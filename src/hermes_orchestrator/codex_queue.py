"""Argv-safe local delivery of typed wake events to the Merger queue."""

from __future__ import annotations

import asyncio
import os
import signal
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from hermes_orchestrator.codex_merger import ReviewerChannel, WakeRegistration
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

ProcessFactory = Callable[..., Awaitable[asyncio.subprocess.Process]]


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
        if registration.state in ("delivered", "admitted", "completed"):
            return QueueDeliveryResult(
                delivered=True,
                attempts=0,
                thread_id=None,
                generation=None,
                reason=f"duplicate_{registration.state}",
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
                    return QueueDeliveryResult(
                        delivered=True,
                        attempts=attempts,
                        thread_id=thread_id,
                        generation=generation,
                        reason="delivered",
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
