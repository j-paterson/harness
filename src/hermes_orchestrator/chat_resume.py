"""Exactly-once durable resume wake for the Orchestrator's chat pane.

INFRA-216 packet W2. The Orchestrator workspace's lower pane runs the
durable Hermes Agent chat (``hermes chat --continue <session>
--create-if-missing``, composed by
:func:`hermes_orchestrator.orchestrator_workspace.hermes_pane_command`).
When the daemon rebuilds the workspace under a successor binding
generation, or respawns a dead/wrong-role lower pane process in place, the
same durable session reattaches — its persisted ``/goal`` row survives —
but nothing resumes it: the reattached chat idles with an active goal.
This module observes each reconciliation's
:class:`~hermes_orchestrator.orchestrator_workspace.OrchestratorWorkspaceState`,
recognizes an ACTUAL recovery, and delivers exactly one durable resume
wake into the named durable session, plus exactly one concise
human-readable journaled event explaining what happened.

Recovery classification (binding, INFRA-216 delegation plan)
--------------------------------------------------------------
An ACTUAL recovery is:

* ``outcome == "created"`` **and** ``generation > 1`` — a rebuild that
  replaced a prior binding (``OrchestratorWorkspaceLifecycle._create``
  called with ``replacing is not None``); or
* ``outcome == "recovered"`` with the lower role
  (:data:`hermes_orchestrator.orchestrator_workspace.LOWER_ROLE`,
  ``"hermes"``) in ``respawned``.

Plain adoption (``outcome == "adopted"``) and true first-ever creation
deliver nothing.

``OrchestratorWorkspaceState`` does not itself distinguish first-ever
creation from a replacement, so this module derives it from the
binding's ``generation``: ``CmuxSurfaceBindings._next_generation`` seeds
the orchestrator role's very first binding at generation 1
(``max(NULL) + 1``) and only ``CmuxSurfaceBindings.replace`` — the path
``_create`` takes when ``replacing is not None`` — ever advances it.
So ``outcome == "created" and generation > 1`` can only be a
replacement; it can never be the first-ever creation.

Delivery surface (verified on this host, 2026-08-31)
--------------------------------------------------------------
``hermes send --help`` and ``hermes sessions --help`` were both run
against the installed Hermes Agent CLI before choosing a transport, per
the delegation plan's requirement to verify the real surface first:

* ``hermes send`` is "Pipe text from any shell script to any messaging
  platform Hermes is already configured for" (Telegram/Discord/Slack/
  Signal/etc, via ``--to platform[:chat_id[:thread_id]]``) — it has no
  concept of a durable *chat* session at all, so it cannot target one.
* ``hermes sessions {list,export,delete,prune,archive,optimize,
  clean-markers,optimize-storage,repair,repair-routing,recover,stats,
  rename,pin,unpin,pinned,retitle-skills,browse,import}`` only
  inspects/manages the session store; none of its subcommands inject a
  turn into a live or durable session either.

So there is no CLI surface that pushes a message into an *already
running* interactive chat process. The only surface that can reach the
named *durable session* at all is the same one the pane itself uses to
attach to it — ``hermes chat --continue <name> --create-if-missing``,
plus the non-interactive single-query flags documented for exactly this
"programmatic caller" use case::

    hermes chat --continue <session> --create-if-missing -q <literal> -Q

(``-q/--query``: "Single query (non-interactive mode)"; ``-Q/--quiet``:
"Quiet mode for programmatic use"; ``--create-if-missing``'s own
``--help`` text: "Programmatic callers that want 'send to this named
thread, making it if needed'.") This is a *new, non-interactive*
process against the same persisted session identity, not a message
typed into the pane's already-attached interactive process — the
delegation plan explicitly forbids inventing an interactive/typing
mechanism, and no other surface exists. :class:`HermesChatResumeTransport`
pins exactly this invocation as the default transport, behind the
:class:`ChatResumeTransport` protocol so it stays swappable if a better
surface ships later.

Because ``-q`` runs the query to full agentic completion synchronously,
a genuine goal resume can run far longer than any short delivery
timeout. :class:`HermesChatResumeTransport` therefore does **not** kill
the child on timeout the way
:class:`hermes_orchestrator.lead_wakes.CommandWakeTransport` does for
its fire-and-acknowledge consumer command: killing a resume turn that
is actually in flight would reproduce the exact SIGHUP-an-active-turn
bug this feature exists to fix. A process still running after the
timeout window is treated as an accepted delivery (``True``) and left
to finish unattended; only a fast, already-exited nonzero return is a
failure.

Grammar-bound argv
--------------------------------------------------------------
The composed argv is bounded exactly like
:func:`hermes_orchestrator.orchestrator_workspace.hermes_pane_command`:
one validated session-name token (the same
``^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`` grammar) plus fixed module-level
literals — :data:`CHAT_RESUME_LITERAL` and the surrounding flags.
Nothing else is parameterizable; the literal instructs the chat to
resume its own persisted active goal if one exists and to do nothing
otherwise — the wake is a signal, never a payload. Durable chat-side
state (the ``/goal`` row) remains the only source of what to resume.

Durable exactly-once
--------------------------------------------------------------
:class:`ChatResumeWakes` owns one row per (``binding_id``,
``generation``), unique-indexed so a repeated claim for the same
recovery is a no-op (``ON CONFLICT ... DO NOTHING``), mirroring
:class:`hermes_orchestrator.lead_wakes.LeadTerminalWakes` and
:class:`hermes_orchestrator.fakechat_signal.FakechatSignalPorts`. Per
observed state: insert-or-ignore the claim; if the row is not yet
``delivered``, attempt delivery; on success, CAS-settle it (``state``
'pending' -> 'delivered', ``delivered_at`` set exactly once, guarded by
``WHERE state = 'pending'`` so two racing settles cannot both succeed).
A crash between claim and settle leaves the row ``pending`` for the
next reconciliation pass to retry; a settled row is never redelivered.
On failure the row stays ``pending`` (only ``last_attempt_at``/
``last_error`` are recorded) so the next pass retries — this is safe by
construction because the delivered literal is idempotent: a chat with
no active goal (already resumed, or already settled) does nothing.

Exactly one concise event is journaled per recovery — on the pass that
*first* inserts the claim row, carrying that attempt's delivery status.
Later retries update the durable row but do not re-journal; the
recovery itself, not each delivery attempt, is the one-time occurrence
being explained.

Wiring
--------------------------------------------------------------
:class:`ObservingOrchestratorWorkspaceOwner` is an
``OrchestratorWorkspaceOwner`` subclass (so ``isinstance`` checks and
existing call sites in ``cli.py`` — which this packet's boundary does
not touch — keep working unchanged) that feeds every ``start()``/
``tick()`` result through :meth:`ChatResumeService.observe` before
returning it. ``runtime.py`` composes it in place of the plain owner.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.orchestrator_workspace import (
    LOWER_ROLE,
    OrchestratorWorkspaceOwner,
    OrchestratorWorkspaceState,
)

#: Same bounded single-token grammar
#: :data:`hermes_orchestrator.orchestrator_workspace._NAME` validates
#: Hermes session names against; redefined here (rather than imported)
#: so this module never depends on that module's private surface.
_SESSION_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

#: The one fixed literal ridden on every resume wake. It is a signal,
#: never a payload: durable chat-side state (the persisted ``/goal``
#: row) remains the sole source of what, if anything, gets resumed.
CHAT_RESUME_LITERAL = (
    "Orchestrator recovery notice: your pane was just rebuilt or "
    "reattached by the daemon. If you have a durable active goal "
    "recorded, resume it now from your own persisted state. If no goal "
    "is active, do nothing and wait as usual."
)

CHAT_RESUME_EVENT_TYPE = "orchestrator.chat_resume"

_TRIGGER_REPLACED = "created_replacement"
_TRIGGER_RESPAWNED = "recovered_lower_respawn"


def _now_utc() -> datetime:
    return datetime.now(UTC)


def classify_recovery(state: OrchestratorWorkspaceState) -> str | None:
    """Return the recovery trigger for ``state``, or ``None`` for a no-op.

    See the module docstring's "Recovery classification" section for the
    binding definition and the ``generation > 1`` derivation.
    """

    if state.outcome == "created" and state.generation > 1:
        return _TRIGGER_REPLACED
    if state.outcome == "recovered" and LOWER_ROLE in state.respawned:
        return _TRIGGER_RESPAWNED
    return None


@dataclass(frozen=True, slots=True)
class ChatResumeWake:
    """One durable pending or delivered resume wake."""

    wake_id: str
    binding_id: str
    generation: int
    workspace_uuid: str
    session_name: str
    trigger: str
    state: str
    created_at: str
    delivered_at: str | None
    last_attempt_at: str | None
    last_error: str | None


def _row_to_wake(row: Any) -> ChatResumeWake:
    return ChatResumeWake(
        wake_id=str(row["wake_id"]),
        binding_id=str(row["binding_id"]),
        generation=int(row["generation"]),
        workspace_uuid=str(row["workspace_uuid"]),
        session_name=str(row["session_name"]),
        trigger=str(row["trigger"]),
        state=str(row["state"]),
        created_at=str(row["created_at"]),
        delivered_at=(
            None if row["delivered_at"] is None else str(row["delivered_at"])
        ),
        last_attempt_at=(
            None
            if row["last_attempt_at"] is None
            else str(row["last_attempt_at"])
        ),
        last_error=(None if row["last_error"] is None else str(row["last_error"])),
    )


class ChatResumeWakes:
    """Durable claim/settle store for resume wakes, one row per recovery.

    ``(binding_id, generation)`` is the sole dedup authority (a unique
    index, migration 0054) — the same insert-or-ignore-then-CAS-settle
    shape as :class:`hermes_orchestrator.lead_wakes.LeadTerminalWakes`.
    """

    def __init__(
        self,
        *,
        database: Database,
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
    ) -> None:
        self._database = database
        self._now = now or _now_utc
        self._ids = ids or (lambda: uuid.uuid4().hex)

    def claim(
        self,
        *,
        binding_id: str,
        generation: int,
        workspace_uuid: str,
        session_name: str,
        trigger: str,
    ) -> tuple[ChatResumeWake, bool]:
        """Insert-or-ignore the claim row; returns ``(row, inserted)``.

        ``inserted`` is ``True`` only on the pass that durably created
        this recovery's row — the caller uses it to journal the
        one-time explanatory event exactly once per recovery.
        """

        wake_id = self._ids()
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "INSERT INTO chat_resume_wakes("
                "wake_id, binding_id, generation, workspace_uuid, "
                "session_name, trigger, state, created_at, delivered_at, "
                "last_attempt_at, last_error"
                ") VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, NULL, NULL, NULL) "
                "ON CONFLICT(binding_id, generation) DO NOTHING",
                (
                    wake_id,
                    binding_id,
                    generation,
                    workspace_uuid,
                    session_name,
                    trigger,
                    stamp,
                ),
            )
            inserted = cursor.rowcount == 1
        if inserted:
            return self.get(wake_id), True
        row = self._database.execute(
            "SELECT wake_id FROM chat_resume_wakes "
            "WHERE binding_id = ? AND generation = ?",
            (binding_id, generation),
        ).fetchone()
        return self.get(str(row["wake_id"])), False

    def record_attempt_failure(self, wake_id: str, *, error: str) -> None:
        """Best-effort bookkeeping for a failed attempt; row stays pending."""

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE chat_resume_wakes SET last_attempt_at = ?, "
                "last_error = ? WHERE wake_id = ? AND state = 'pending'",
                (stamp, error[:500], wake_id),
            )

    def settle(self, wake_id: str) -> ChatResumeWake:
        """CAS the row to delivered; idempotent for an already-settled row."""

        current = self.get(wake_id)
        if current.state != "pending":
            return current
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE chat_resume_wakes SET state = 'delivered', "
                "delivered_at = ?, last_attempt_at = ?, last_error = NULL "
                "WHERE wake_id = ? AND state = 'pending'",
                (stamp, stamp, wake_id),
            )
        return self.get(wake_id)

    def get(self, wake_id: str) -> ChatResumeWake:
        row = self._database.execute(
            "SELECT * FROM chat_resume_wakes WHERE wake_id = ?",
            (wake_id,),
        ).fetchone()
        if row is None:
            raise KeyError(wake_id)
        return _row_to_wake(row)

    def for_binding_generation(
        self, binding_id: str, generation: int
    ) -> ChatResumeWake | None:
        row = self._database.execute(
            "SELECT * FROM chat_resume_wakes "
            "WHERE binding_id = ? AND generation = ?",
            (binding_id, generation),
        ).fetchone()
        return None if row is None else _row_to_wake(row)


class ChatResumeTransport(Protocol):
    """Deliver the fixed resume literal into one named durable session."""

    async def deliver(self, *, session_name: str) -> bool: ...


ProcessFactory = Callable[..., Awaitable[asyncio.subprocess.Process]]


class HermesChatResumeTransport:
    """Default transport: one non-interactive ``hermes chat`` turn.

    Pinned invocation (see the module docstring for the verification
    evidence)::

        hermes chat --continue <session_name> --create-if-missing \\
            -q "<CHAT_RESUME_LITERAL>" -Q

    Grammar-bound: ``session_name`` is validated against the same
    bounded single-token rule the pane command itself uses before it
    ever reaches argv; every other token is a fixed module-level
    literal, never composed from caller data. A malformed name raises
    ``ValueError`` before any process is spawned.

    A nonzero exit or a spawn failure is a bounded ``False`` (delivery
    failed; the caller leaves the durable claim unsettled for retry). A
    process still running after ``timeout_seconds`` is treated as an
    accepted delivery (``True``) rather than killed — see the module
    docstring's "Delivery surface" section for why: ``-q`` runs the
    query to completion synchronously, and killing an in-flight resume
    turn would reproduce the exact SIGHUP-an-active-turn bug INFRA-216
    exists to fix.
    """

    def __init__(
        self,
        *,
        hermes_bin: str = "hermes",
        process_factory: ProcessFactory = asyncio.create_subprocess_exec,
        timeout_seconds: float = 10.0,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("chat resume transport timeout must be positive")
        self._hermes_bin = hermes_bin
        self._process_factory = process_factory
        self._timeout_seconds = timeout_seconds

    async def deliver(self, *, session_name: str) -> bool:
        if _SESSION_NAME.fullmatch(session_name) is None:
            raise ValueError(
                "a Hermes session name must be one bounded token"
            )
        argv = (
            self._hermes_bin,
            "chat",
            "--continue",
            session_name,
            "--create-if-missing",
            "-q",
            CHAT_RESUME_LITERAL,
            "-Q",
        )
        process = await self._process_factory(
            *argv,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            await asyncio.wait_for(
                process.wait(), timeout=self._timeout_seconds
            )
        except TimeoutError:
            # Still running: an in-flight resume turn, not a hang. Never
            # kill it — see class docstring.
            return True
        return process.returncode == 0


class ChatResumeService:
    """Observe reconciliation state; deliver at most one wake per recovery."""

    def __init__(
        self,
        *,
        database: Database,
        events: EventStore,
        session_name: str,
        wakes: ChatResumeWakes | None = None,
        transport: ChatResumeTransport | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._session_name = session_name
        self._wakes = wakes or ChatResumeWakes(database=database, now=now)
        self._transport = transport or HermesChatResumeTransport()

    async def observe(self, state: OrchestratorWorkspaceState) -> None:
        """Feed one reconciliation's state; a no-op unless it is a recovery."""

        trigger = classify_recovery(state)
        if trigger is None:
            return
        row, inserted = self._wakes.claim(
            binding_id=state.binding_id,
            generation=state.generation,
            workspace_uuid=state.workspace_uuid,
            session_name=self._session_name,
            trigger=trigger,
        )
        if row.state == "delivered":
            # Already settled by an earlier pass (or this exact recovery
            # was already observed and delivered): nothing to redeliver,
            # nothing new to explain.
            return
        status = "failed"
        detail = "delivery reported failure"
        try:
            delivered = await self._transport.deliver(
                session_name=self._session_name
            )
        except Exception as error:  # bounded: never raised past this method
            delivered = False
            detail = f"{type(error).__name__}: {error}"[:200]
        if delivered:
            self._wakes.settle(row.wake_id)
            status = "delivered"
        else:
            self._wakes.record_attempt_failure(row.wake_id, error=detail)
        if inserted:
            with self._database.transaction() as connection:
                self._events.append(
                    connection,
                    EventInput(
                        event_type=CHAT_RESUME_EVENT_TYPE,
                        aggregate_type="orchestrator_workspace",
                        aggregate_id=state.binding_id,
                        correlation_id=row.wake_id,
                        payload={
                            "workspace_uuid": state.workspace_uuid,
                            "binding_id": state.binding_id,
                            "generation": state.generation,
                            "outcome": state.outcome,
                            "respawned": list(state.respawned),
                            "trigger": trigger,
                            "session_name": self._session_name,
                            "delivery_status": status,
                        },
                    ),
                )


class ObservingOrchestratorWorkspaceOwner(OrchestratorWorkspaceOwner):
    """``OrchestratorWorkspaceOwner`` that also feeds each result to resume.

    A subclass (not a duck-typed wrapper) so every existing call site —
    including ``isinstance(owner, OrchestratorWorkspaceOwner)`` checks in
    tests this packet's boundary does not touch — keeps working
    unchanged; only ``start()``/``tick()`` gain one extra ``observe``
    step, and only when composed with a resume service.
    """

    def __init__(self, lifecycle: Any, resume: ChatResumeService) -> None:
        super().__init__(lifecycle)
        self._resume = resume

    async def start(self) -> OrchestratorWorkspaceState | None:
        state = await super().start()
        if state is not None:
            await self._resume.observe(state)
        return state

    async def tick(self) -> OrchestratorWorkspaceState | None:
        state = await super().tick()
        if state is not None:
            await self._resume.observe(state)
        return state
