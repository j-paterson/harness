"""Argv-safe local delivery of typed wake events to the Merger queue."""

from __future__ import annotations

import asyncio
import os
import signal
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from hermes_orchestrator.codex_merger import (
    SANDBOX_MODE,
    ReviewerChannel,
    WakeRegistration,
)
from hermes_orchestrator.codex_ponytail_guard import session_guard_config
from hermes_orchestrator.manifests import (
    WAKE_STATUSES as WAKE_STATUSES,
)
from hermes_orchestrator.manifests import (
    ManifestError,
    ManifestSnapshot,
    read_manifest_snapshot,
)
from hermes_orchestrator.manifests import (
    WakeEvent as WakeEvent,
)
from hermes_orchestrator.processes import ProcessRegistry, register_spawned

CODEX_QUEUE_BINARY = "/Applications/Codex.app/Contents/Resources/codex"

#: Delivery reason for a candidate held behind the one-at-a-time reviewer
#: gate (INFRA-221). Not a failure: the candidate is durably queued in
#: ``wake_deliveries`` and is released and woken by the same delivery path
#: once the current candidate's verdict settles.
CANDIDATE_QUEUED = "candidate_queued"

#: INFRA-223: the observed blocker was exactly this -- ``codex queue
#: --thread`` persisted the envelope for the idle pinned Sol thread, but
#: the queued turn never started. These are the App Server methods the
#: bounded start below uses, and the only ones it is allowed to use.
THREAD_QUEUE_LIST = "thread/queue/list"
THREAD_QUEUE_START = "thread/queue/start"

#: INFRA-223 recurrence (INFRA-227): starting the queued head is not
#: enough on its own -- a recovered turn can still be resumed by Codex
#: under a stale read-only/network-restricted sandbox. ``thread/resume``
#: is the same recoverable configuration path the persistent Merger
#: channel already uses (see ``codex_merger._load_persisted_thread``) to
#: reapply the primary Sol task's required write/network/no-prompt
#: policy, and it is sent on this same bounded connection immediately
#: before ``thread/queue/start`` so the queued turn never runs outside
#: that policy.
THREAD_RESUME = "thread/resume"

ProcessFactory = Callable[..., Awaitable[asyncio.subprocess.Process]]


class QueueRpc(Protocol):
    """The short-lived App Server connection the bounded start needs."""

    async def start(self) -> None: ...

    async def request(
        self, method: str, params: dict[str, Any] | None, timeout: float
    ) -> dict[str, Any]: ...

    async def close(self) -> None: ...


#: Builds one short-lived App Server connection. ``None`` means the caller
#: wired no connection, and the bounded start is simply not attempted.
RpcFactory = Callable[[], QueueRpc]


def _own_queue_head(result: object, event_id: str) -> str | None:
    """Return the head item id when the head is provably this wake's.

    The only identity used is one that is ALREADY durable: the wake's own
    event id, which :meth:`WakeEvent.render` writes into the queued
    envelope text as ``event=<event_id> generation=<n>``. Nothing weaker
    counts -- an empty queue, a head with no id, a head whose text is not
    exposed, or a head carrying someone else's event id all return
    ``None`` and nothing is started.
    """

    if not isinstance(result, dict):
        return None
    items = result.get("items")
    if not isinstance(items, list) or not items:
        return None
    head = items[0]
    if not isinstance(head, dict):
        return None
    item_id = head.get("id")
    text = head.get("text")
    if not isinstance(item_id, str) or not item_id:
        return None
    if not isinstance(text, str) or f" event={event_id} " not in text:
        return None
    return item_id


class ChannelStore(Protocol):
    """The reviewer-channel surface the delivery adapter depends on."""

    def read_channel(self, project_key: str) -> ReviewerChannel | None: ...

    def record_delivery_failure(
        self, project_key: str, *, thread_id: str, generation: int
    ) -> bool: ...

    def register_wake(
        self,
        project_key: str,
        event: WakeEvent,
        *,
        manifest: ManifestSnapshot,
    ) -> WakeRegistration: ...

    def record_wake_delivery_success(
        self,
        project_key: str,
        *,
        thread_id: str,
        generation: int,
        event_id: str,
        claim_token: str,
        candidate_sha: str | None = None,
    ) -> bool: ...

    def record_wake_attempt_failed(
        self, project_key: str, event_id: str, *, claim_token: str
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class QueueDeliveryResult:
    """Bounded outcome of one wake delivery; never claims false success."""

    delivered: bool
    attempts: int
    thread_id: str | None
    generation: int | None
    reason: str
    #: INFRA-223 recurrence: whether the bounded queue/start below actually
    #: reapplied the primary Sol task's write/network/no-prompt policy
    #: (``thread/resume`` accepted) before the queued turn was started.
    #: ``False`` on every path that never reached a queued head, including
    #: when nothing needed starting.
    policy_applied: bool = False
    #: The sandbox mode carried by that ``thread/resume``, when attempted.
    policy_sandbox: str | None = None
    #: Outcome of the bounded resume-then-start attempt, when one was
    #: made -- e.g. ``"started"`` or ``"policy_resume_failed: <detail>"``.
    #: ``None`` when ``_start_queued_head`` was never invoked for this
    #: delivery.
    policy_reason: str | None = None
    #: INFRA-223 (generation-132 start-proof blocker): True only when the
    #: bounded queue/start proved the exact owned head started; queue
    #: persistence alone never sets it.
    started: bool = False


@dataclass(frozen=True, slots=True)
class _QueuePolicyStart:
    """Outcome of one bounded resume-then-start attempt on a queue head."""

    started: bool
    policy_applied: bool
    sandbox: str | None
    reason: str


class CodexQueueDelivery:
    """Queue one wake event onto the current Merger thread without a shell.

    The adapter only ever invokes ``codex queue``; it must never invoke
    ``codex exec resume``, which can start a competing execution instead of
    queuing a message onto the existing Merger session.
    """

    def __init__(
        self,
        *,
        channels: ChannelStore,
        manifest_root: Path,
        binary: str = CODEX_QUEUE_BINARY,
        process_factory: ProcessFactory = asyncio.create_subprocess_exec,
        timeout: float = 10.0,
        termination_timeout: float = 5.0,
        processes: ProcessRegistry | None = None,
        rpc_factory: RpcFactory | None = None,
    ) -> None:
        if timeout <= 0:
            raise ValueError("delivery timeout must be positive")
        self._processes = processes
        if termination_timeout <= 0:
            raise ValueError("termination timeout must be positive")
        self._channels = channels
        self._manifest_root = manifest_root
        self._binary = binary
        self._process_factory = process_factory
        self._timeout = timeout
        self._termination_timeout = termination_timeout
        self._rpc_factory = rpc_factory

    async def deliver(
        self, project_key: str, event: WakeEvent
    ) -> QueueDeliveryResult:
        """Deliver one wake with at most one reload-and-rebuild retry.

        Every attempt renders the envelope against the channel generation it
        actually invokes, and success is recorded with a compare-and-swap on
        that exact thread and generation, so a wake that landed on a replaced
        thread is never claimed on the new channel.
        """

        try:
            snapshot = read_manifest_snapshot(
                Path(event.manifest_path),
                root=self._manifest_root,
                expected_digest=event.manifest_digest,
            )
        except ManifestError:
            return QueueDeliveryResult(
                delivered=False,
                attempts=0,
                thread_id=None,
                generation=None,
                reason="manifest_invalid",
            )
        manifest = snapshot.manifest
        if (
            manifest.event_id != event.event_id
            or manifest.status != event.status
            or manifest.candidate_sha != event.candidate_sha
            or manifest.base_sha != event.base_sha
            or event.issue_id not in manifest.linear_issues
        ):
            return QueueDeliveryResult(
                delivered=False,
                attempts=0,
                thread_id=None,
                generation=None,
                reason="manifest_invalid",
            )
        registration = self._channels.register_wake(
            project_key, event, manifest=snapshot
        )
        if registration.state == "conflict":
            return QueueDeliveryResult(
                delivered=False,
                attempts=0,
                thread_id=None,
                generation=None,
                reason="event_conflict",
            )
        start_outcome: _QueuePolicyStart | None = None
        if (
            registration.state in ("delivered", "admitted")
            and registration.thread_id is not None
        ):
            start_outcome = await self._start_queued_head(
                registration.thread_id, event.event_id
            )
        if registration.state in ("delivered", "admitted", "completed"):
            return QueueDeliveryResult(
                delivered=True,
                attempts=0,
                thread_id=None,
                generation=None,
                reason=f"duplicate_{registration.state}",
                policy_applied=bool(
                    start_outcome and start_outcome.policy_applied
                ),
                policy_sandbox=start_outcome.sandbox if start_outcome else None,
                policy_reason=start_outcome.reason if start_outcome else None,
                started=bool(start_outcome and start_outcome.started),
            )
        if registration.state == "queued":
            # INFRA-221: another candidate holds the reviewer with an
            # unsettled verdict. The wake is durably registered and
            # pending -- that row IS the queue -- but nothing is rendered
            # into the thread, so Sol never receives a second candidate
            # while the current verdict is outstanding.
            return QueueDeliveryResult(
                delivered=False,
                attempts=0,
                thread_id=None,
                generation=None,
                reason=CANDIDATE_QUEUED,
            )
        if registration.state == "pending_elsewhere":
            return QueueDeliveryResult(
                delivered=False,
                attempts=0,
                thread_id=None,
                generation=None,
                reason="duplicate_pending",
            )
        if registration.state in ("deferred", "rejected"):
            return QueueDeliveryResult(
                delivered=False,
                attempts=0,
                thread_id=None,
                generation=None,
                reason=f"candidate_{registration.state}",
            )
        if registration.state == "in_flight":
            return QueueDeliveryResult(
                delivered=False,
                attempts=0,
                thread_id=None,
                generation=None,
                reason="delivery_in_flight",
            )
        if (
            registration.channel_state != "ready"
            or registration.thread_id is None
            or registration.generation is None
            or registration.claim_token is None
        ):
            return QueueDeliveryResult(
                delivered=False,
                attempts=0,
                thread_id=None,
                generation=None,
                reason="channel_unavailable",
            )
        thread_id = registration.thread_id
        generation = registration.generation
        claim_token = registration.claim_token
        attempts = 0
        reason = "queue_cli_failed"
        for _ in range(2):
            attempts += 1
            failure = await self._invoke(
                thread_id, event.render(generation), project_key=project_key
            )
            if failure is None:
                recorded = self._channels.record_wake_delivery_success(
                    project_key,
                    thread_id=thread_id,
                    generation=generation,
                    event_id=event.event_id,
                    claim_token=claim_token,
                    candidate_sha=event.candidate_sha,
                )
                if recorded:
                    # INFRA-223: `codex queue` exiting 0 only persisted the
                    # envelope onto the idle pinned thread. Start that exact
                    # queued head now, then let go of the thread again.
                    start_outcome = await self._start_queued_head(
                        thread_id, event.event_id
                    )
                    return QueueDeliveryResult(
                        delivered=True,
                        attempts=attempts,
                        thread_id=thread_id,
                        generation=generation,
                        reason="delivered",
                        policy_applied=start_outcome.policy_applied,
                        policy_sandbox=start_outcome.sandbox,
                        policy_reason=start_outcome.reason,
                        started=start_outcome.started,
                    )
                reason = "redelivery_required"
            else:
                reason = failure
            reloaded = self._ready_channel(project_key)
            if reloaded is None or reloaded.generation == generation:
                break
            thread_id = reloaded.thread_id
            generation = reloaded.generation
        self._channels.record_delivery_failure(
            project_key,
            thread_id=thread_id,
            generation=generation,
        )
        self._channels.record_wake_attempt_failed(
            project_key, event.event_id, claim_token=claim_token
        )
        return QueueDeliveryResult(
            delivered=False,
            attempts=attempts,
            thread_id=thread_id,
            generation=generation,
            reason=reason,
        )

    async def _start_queued_head(
        self, thread_id: str, event_id: str
    ) -> _QueuePolicyStart:
        """Start the exact head just queued, then disconnect immediately.

        One bounded action, no state of its own: connect, list the thread
        queue, reapply the primary Sol task's required write/network/
        no-prompt policy via ``thread/resume`` on the head that provably
        carries this wake's event id, then start it, disconnect. The
        connection is released in ``finally`` on every path, so it can
        never stay attached to an idle Sol.

        INFRA-223 recurrence (INFRA-227): a recovered turn started by the
        bounded head-start alone can still run under a stale read-only/
        network-restricted sandbox, leaving Sol unable to create the PR or
        submit an approved verdict. ``thread/resume`` -- the same
        recoverable configuration path the persistent Merger channel
        already uses -- is sent on this same short-lived connection right
        before ``thread/queue/start`` so the queued turn is never started
        outside the policy the primary Sol task requires. If that resume
        fails, the turn is fail-closed: nothing is started.

        Every other path fails closed and silent, exactly as before --
        no connection wired, a connection that will not start, an empty
        queue, a head that is not this candidate's, an App Server that
        does not implement these methods (a refused call is signal
        enough), or any other RPC error. In each case nothing is started,
        nothing is written, and the caller sees exactly today's
        behaviour, save for the reason now recorded in the returned
        outcome.
        """

        factory = self._rpc_factory
        if factory is None:
            return _QueuePolicyStart(
                started=False,
                policy_applied=False,
                sandbox=None,
                reason="no_rpc_factory",
            )
        try:
            client = factory()
        except Exception as exc:
            return _QueuePolicyStart(
                started=False,
                policy_applied=False,
                sandbox=None,
                reason=f"rpc_connect_failed: {exc}",
            )
        try:
            await client.start()
            listing = await client.request(
                THREAD_QUEUE_LIST, {"threadId": thread_id}, self._timeout
            )
            item_id = _own_queue_head(listing, event_id)
            if item_id is None:
                return _QueuePolicyStart(
                    started=False,
                    policy_applied=False,
                    sandbox=None,
                    reason="no_owned_head",
                )
            try:
                await client.request(
                    THREAD_RESUME,
                    {
                        "threadId": thread_id,
                        "sandbox": SANDBOX_MODE,
                        "approvalPolicy": "never",
                        "config": session_guard_config(),
                    },
                    self._timeout,
                )
            except Exception as exc:
                # Fail closed: never start a turn whose policy could not
                # be reapplied.
                return _QueuePolicyStart(
                    started=False,
                    policy_applied=False,
                    sandbox=SANDBOX_MODE,
                    reason=f"policy_resume_failed: {exc}",
                )
            await client.request(
                THREAD_QUEUE_START,
                {"threadId": thread_id, "queueItemId": item_id},
                self._timeout,
            )
            return _QueuePolicyStart(
                started=True,
                policy_applied=True,
                sandbox=SANDBOX_MODE,
                reason="started",
            )
        except Exception as exc:
            return _QueuePolicyStart(
                started=False,
                policy_applied=False,
                sandbox=None,
                reason=f"rpc_error: {exc}",
            )
        finally:
            with suppress(Exception):
                await client.close()

    def _ready_channel(self, project_key: str) -> ReviewerChannel | None:
        channel = self._channels.read_channel(project_key)
        if channel is None or channel.state != "ready":
            return None
        return channel

    async def _invoke(
        self, thread_id: str, message: str, *, project_key: str = "merger"
    ) -> str | None:
        try:
            process = await self._process_factory(
                self._binary,
                "queue",
                "--thread",
                thread_id,
                "--message",
                message,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )
        except OSError:
            return "spawn_failed"
        try:
            lease_id = await register_spawned(
                self._processes,
                process,
                project_key=project_key,
                kind="codex_queue",
                worker_id=thread_id,
                executable=self._binary,
                terminate=self._terminate_group,
            )
        except RuntimeError:
            return "registration_failed"
        try:
            returncode = await asyncio.wait_for(
                process.wait(), timeout=self._timeout
            )
        except TimeoutError:
            await self._terminate_group(process)
            returncode = process.returncode
            self._settle(lease_id, returncode)
            return "delivery_timeout"
        except asyncio.CancelledError:
            await self._terminate_group(process)
            self._settle(lease_id, process.returncode)
            raise
        self._settle(lease_id, returncode)
        return None if returncode == 0 else "queue_cli_failed"

    def _settle(self, lease_id: str | None, returncode: int | None) -> None:
        if lease_id is not None and self._processes is not None:
            self._processes.mark_exited(lease_id, exit_code=returncode)

    async def _terminate_group(
        self, process: asyncio.subprocess.Process
    ) -> None:
        """Stop the whole owned process group: TERM, bounded wait, then KILL.

        SIGKILL is always sent to the group after the bounded TERM window,
        even when the leader has already exited, so a descendant that ignored
        SIGTERM cannot survive or deliver late.
        """

        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        with suppress(TimeoutError):
            await asyncio.wait_for(
                process.wait(), timeout=self._termination_timeout
            )
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        await process.wait()
