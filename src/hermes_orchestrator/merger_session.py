"""Event-scoped ownership of the daemon's Codex App Server connection.

Sol correction 9a512ed7 (P1, Critical): before this module existed the
daemon started the merge flow's ``codex app-server --listen stdio://`` at
process startup and kept the notification listener — and the process-group
lease that comes with it — for the daemon's entire lifetime. While the
bound Sol thread was idle the lease stayed active, so the Codex desktop
app reported the thread as "open in another app" and the operator could
never observe or interact with it.

``MergerSession`` owns the App Server connection's lifecycle instead: it
opens (starts the RPC child, ensures each project's reviewer thread, and
starts a notification listener) only while review work is active, and
releases (cancels the listener, closes the RPC child, which frees the
process lease) the moment nothing is outstanding. "Review work active" is
answered from durable SQLite state only — no RPC call, no model call — so
the check is safe to run on every maintenance tick.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

if TYPE_CHECKING:
    from hermes_orchestrator.merge_flow import MergeFlow

ListenerFactory = Callable[
    ["MergeFlow", Sequence[str], "MergerSession"], Awaitable[None]
]


class MergerSession:
    """Idempotent owner of one daemon's Codex App Server connection.

    ``flow.rpc`` is used only through its ``start``/``close``/
    ``notifications`` surface (duck-typed, so a fake stands in for tests);
    ``review_active`` reads only through ``flow.turns`` (also overridable
    for tests via ``is_review_active``). Every public method is safe to
    call repeatedly: opening an already-open session and releasing an
    already-released one are both no-ops.
    """

    def __init__(
        self,
        flow: MergeFlow,
        projects: Sequence[str],
        *,
        events: EventStore | None = None,
        database: Database | None = None,
        listener_factory: ListenerFactory | None = None,
        is_review_active: Callable[[], bool] | None = None,
    ) -> None:
        self.flow = flow
        self._projects = tuple(projects)
        self._events = events
        self._database = database
        self._listener_factory = listener_factory
        self._is_review_active = is_review_active
        self._open = False
        self._listener: asyncio.Task[None] | None = None

    @property
    def is_open(self) -> bool:
        """Report whether the session currently holds the App Server lease."""

        return self._open

    def review_active(self) -> bool:
        """True iff SQLite shows outstanding review work for any project.

        No RPC, no model call: "active" is either a delivered/admitted
        wake (``MergerTurnService.outstanding_wake``) or a durable
        ``submitted_verdicts`` row still in state ``'submitted'``
        (``MergerTurnService.has_pending_submission``).

        INFRA-223: both helpers repair their own durable state before
        answering -- ``outstanding_wake`` retires a settled or superseded
        wake, and ``has_pending_submission`` settles a submission whose
        own wake and own review are already terminal -- so a purely
        historical row can no longer report live review work and hold
        the pinned Sol transcript open behind an App Server.
        """

        if self._is_review_active is not None:
            return self._is_review_active()
        for project_key in self._projects:
            if self.flow.turns.outstanding_wake(project_key) is not None:
                return True
            if self.flow.turns.has_pending_submission(project_key):
                return True
        return False

    async def open(self, reason: str) -> bool:
        """Start the App Server and the listener; idempotent, fails closed.

        Returns ``False`` without journaling anything when the App Server
        itself cannot start; a per-project ``ensure_thread`` failure is
        logged to stderr but never blocks opening (matching
        ``_start_merge_flow``'s existing tolerance).
        """

        if self._open:
            return True
        try:
            await self.flow.rpc.start()
        except Exception as error:
            print(f"codex app-server unavailable: {error}", file=sys.stderr)
            return False
        for project_key in self._projects:
            try:
                await self.flow.merger.ensure_thread(project_key)
            except Exception as error:
                print(
                    f"merger channel for {project_key} unavailable: {error}",
                    file=sys.stderr,
                )
        self._open = True
        self._listener = asyncio.create_task(self._listen())
        self._journal(
            "merger.session_opened",
            {"reason": reason, "projects": list(self._projects)},
        )
        return True

    async def release(self, reason: str) -> None:
        """Stop the listener and close the App Server; idempotent.

        Called from inside the listener task itself (the terminal-idle
        hook), cancelling and awaiting that same task would deadlock, so
        this only cancels+awaits the listener when it is a *different*
        task; the RPC close still runs inline, which unblocks the
        listener's own ``notifications()`` iteration so it returns on its
        own right after this call.
        """

        if not self._open:
            return
        self._open = False
        listener, self._listener = self._listener, None
        if listener is not None and listener is not asyncio.current_task():
            listener.cancel()
            with suppress(asyncio.CancelledError):
                await listener
        await self.flow.rpc.close()
        self._journal("merger.session_released", {"reason": reason})

    async def reconcile(self, reason: str) -> None:
        """Open when work is active and closed; release when it is not.

        Never raises: every failure is contained and reported to stderr,
        since this runs from the unattended maintenance tick and from
        inside the listener task.
        """

        try:
            active = self.review_active()
        except Exception as error:
            print(f"merger session review check failed: {error}", file=sys.stderr)
            return
        try:
            if (
                self._open
                and self._listener is not None
                and self._listener.done()
                and self._listener is not asyncio.current_task()
            ):
                # The App Server connection ended on its own (child exit
                # or protocol failure): the listener finished while the
                # session still believed it was open. Release first so a
                # still-active review reopens a fresh connection below
                # instead of staying stuck on a dead one.
                await self.release("connection_ended")
            if active and not self._open:
                await self.open(reason)
            elif self._open and not active:
                await self.release(reason)
        except Exception as error:
            print(f"merger session reconcile failed: {error}", file=sys.stderr)

    async def startup(self) -> None:
        """Startup recovery: open, resume settlements, recover, then settle.

        Opens unconditionally to run the two boundary recovery passes
        against a live App Server, then reconciles once so the session
        releases immediately unless that recovery left work outstanding.
        """

        opened = await self.open("startup_recovery")
        if opened:
            await self.flow.reviews.resume_settlements()
            await self.flow.turns.recover_outstanding(self._projects)
        await self.reconcile("startup")

    async def shutdown(self) -> None:
        """Release unconditionally at daemon shutdown."""

        await self.release("daemon_shutdown")

    async def _listen(self) -> None:
        if self._listener_factory is not None:
            await self._listener_factory(self.flow, self._projects, self)
        else:
            await self._default_listen()

    async def _default_listen(self) -> None:
        """Fallback notification loop used when no factory is supplied.

        Settles a durably submitted verdict on ``turn/completed`` and
        resumes a durably submitted verdict on an idle
        ``thread/status/changed`` (never inferring a verdict from the
        thread itself), then reconciles so the session releases the
        instant nothing is left outstanding.
        """

        async for notification in self.flow.rpc.notifications():
            if notification.method == "thread/status/changed":
                status = notification.params.get("status")
                status_type = status.get("type") if isinstance(status, dict) else None
                thread_id = notification.params.get("threadId")
                if status_type == "idle" and isinstance(thread_id, str):
                    await self.flow.turns.settle_idle_thread(self._projects, thread_id)
                    await self.reconcile("terminal_idle")
                continue
            await self.flow.turns.on_notification(notification)

    def _journal(self, event_type: str, payload: dict[str, object]) -> None:
        if self._events is None or self._database is None:
            return
        aggregate_id = ",".join(self._projects)
        with suppress(Exception), self._database.transaction() as connection:
            self._events.append(
                connection,
                EventInput(
                    event_type=event_type,
                    aggregate_type="merger_session",
                    aggregate_id=aggregate_id,
                    payload=payload,
                ),
            )
