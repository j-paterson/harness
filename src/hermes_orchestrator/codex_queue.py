"""Argv-safe local delivery of typed wake events to the Merger queue."""

from __future__ import annotations

import asyncio
import os
import signal
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Protocol

from hermes_orchestrator.codex_merger import ReviewerChannel

CODEX_QUEUE_BINARY = "/Applications/Codex.app/Contents/Resources/codex"
WAKE_STATUSES = frozenset(
    {"FABLE_READY", "FABLE_REWORK_READY", "FABLE_BLOCKED", "FABLE_COMPLETE"}
)

ProcessFactory = Callable[..., Awaitable[asyncio.subprocess.Process]]


class ChannelStore(Protocol):
    """The reviewer-channel surface the delivery adapter depends on."""

    def read_channel(self, project_key: str) -> ReviewerChannel | None: ...

    def record_delivery_success(
        self,
        project_key: str,
        *,
        thread_id: str,
        generation: int,
        event_id: str | None = None,
        candidate_sha: str | None = None,
    ) -> bool: ...

    def record_delivery_failure(
        self, project_key: str, *, thread_id: str, generation: int
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class WakeEvent:
    """One validated explicit Fable wake; the only shape that can be queued."""

    status: str
    issue_id: str
    candidate_sha: str
    base_sha: str
    manifest_path: str
    event_id: str

    def __post_init__(self) -> None:
        if self.status not in WAKE_STATUSES:
            raise ValueError(f"unknown wake status {self.status}")
        for name in (
            "issue_id",
            "candidate_sha",
            "base_sha",
            "manifest_path",
            "event_id",
        ):
            if not getattr(self, name):
                raise ValueError(f"wake event {name} must not be empty")

    def render(self, generation: int) -> str:
        """Render the wake envelope for the exact channel generation invoked."""

        return (
            f"{self.status} issue={self.issue_id} "
            f"candidate={self.candidate_sha} base={self.base_sha} "
            f"manifest={self.manifest_path} event={self.event_id} "
            f"generation={generation}"
        )


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
        binary: str = CODEX_QUEUE_BINARY,
        process_factory: ProcessFactory = asyncio.create_subprocess_exec,
        timeout: float = 10.0,
        termination_timeout: float = 5.0,
    ) -> None:
        if timeout <= 0:
            raise ValueError("delivery timeout must be positive")
        if termination_timeout <= 0:
            raise ValueError("termination timeout must be positive")
        self._channels = channels
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

        channel = self._ready_channel(project_key)
        if channel is None:
            return QueueDeliveryResult(
                delivered=False,
                attempts=0,
                thread_id=None,
                generation=None,
                reason="channel_unavailable",
            )
        attempts = 0
        reason = "queue_cli_failed"
        for _ in range(2):
            attempts += 1
            failure = await self._invoke(
                channel.thread_id, event.render(channel.generation)
            )
            if failure is None:
                recorded = self._channels.record_delivery_success(
                    project_key,
                    thread_id=channel.thread_id,
                    generation=channel.generation,
                    event_id=event.event_id,
                    candidate_sha=event.candidate_sha,
                )
                if recorded:
                    return QueueDeliveryResult(
                        delivered=True,
                        attempts=attempts,
                        thread_id=channel.thread_id,
                        generation=channel.generation,
                        reason="delivered",
                    )
                reason = "redelivery_required"
            else:
                reason = failure
            reloaded = self._ready_channel(project_key)
            if reloaded is None or reloaded.generation == channel.generation:
                break
            channel = reloaded
        self._channels.record_delivery_failure(
            project_key,
            thread_id=channel.thread_id,
            generation=channel.generation,
        )
        return QueueDeliveryResult(
            delivered=False,
            attempts=attempts,
            thread_id=channel.thread_id,
            generation=channel.generation,
            reason=reason,
        )

    def _ready_channel(self, project_key: str) -> ReviewerChannel | None:
        channel = self._channels.read_channel(project_key)
        if channel is None or channel.state != "ready":
            return None
        return channel

    async def _invoke(self, thread_id: str, message: str) -> str | None:
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
            returncode = await asyncio.wait_for(
                process.wait(), timeout=self._timeout
            )
        except TimeoutError:
            await self._terminate_group(process)
            return "delivery_timeout"
        except asyncio.CancelledError:
            await self._terminate_group(process)
            raise
        return None if returncode == 0 else "queue_cli_failed"

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
