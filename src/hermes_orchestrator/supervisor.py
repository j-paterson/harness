"""Async supervisor loop with ordered, signal-safe shutdown semantics."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any, Protocol


class SupervisorService(Protocol):
    def start(self) -> object: ...

    def tick(self) -> object: ...


AsyncAction = Callable[[], Awaitable[None]]
Dispatcher = Callable[[str], Awaitable[object]]


async def _nothing() -> None:
    return None


class Supervisor:
    """Reconcile, tick, dispatch enabled actions, and stop in a fixed order."""

    def __init__(
        self,
        service: SupervisorService,
        *,
        dispatch: Dispatcher | None = None,
        request_checkpoint: Callable[[str, str], Awaitable[object]] | None = None,
        checkpoint_dispatcher: Any | None = None,
        checkpoint_workers: AsyncAction = _nothing,
        interval_seconds: float = 30.0,
        checkpoint_timeout: float = 30.0,
    ) -> None:
        if interval_seconds <= 0 or checkpoint_timeout <= 0:
            raise ValueError("supervisor intervals must be positive")
        self._service = service
        self._dispatch = dispatch
        self._request_checkpoint = request_checkpoint
        self._checkpoint_dispatcher = checkpoint_dispatcher
        self._checkpoint_workers = checkpoint_workers
        self.checkpoint_requests: list[tuple[str, str]] = []
        self.checkpoint_deliveries: list[tuple[str, str]] = []
        self._interval_seconds = interval_seconds
        self._checkpoint_timeout = checkpoint_timeout
        self._closing = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._reconciled = False
        self._admission_open = False
        self.events: list[str] = []
        self.ticks = 0

    async def start(self) -> None:
        """Reconcile once and begin the periodic loop."""

        if self._task is not None:
            return
        self._ensure_reconciled()
        self._closing.clear()
        self.events.append("supervisor.started")
        self._task = asyncio.create_task(self._loop())

    async def run_once(self) -> None:
        """Perform one reconciled tick and dispatch only enabled actions."""

        self._ensure_reconciled()
        result = self._service.tick()
        self.ticks += 1
        self.events.append("supervisor.ticked")
        # Red pressure: exactly one checkpoint request per tick; the next
        # tick re-samples and continues only while red persists. Every
        # reserved request is delivered through the dispatcher, which
        # settles it durably on any failure before the error propagates.
        if self._checkpoint_dispatcher is not None:
            undelivered = self._checkpoint_dispatcher.undelivered()
            if undelivered is not None:
                outcome = await self._checkpoint_dispatcher.deliver(undelivered)
                self.checkpoint_deliveries.append((undelivered, outcome))
        for action in getattr(result, "resource_actions", ()):
            if getattr(action, "kind", None) != "request_checkpoint" or not getattr(
                action, "target_id", None
            ):
                continue
            request_id = getattr(action, "request_id", None)
            if self._checkpoint_dispatcher is not None and request_id is not None:
                outcome = await self._checkpoint_dispatcher.deliver(request_id)
                self.checkpoint_deliveries.append((request_id, outcome))
            elif self._request_checkpoint is not None:
                self.checkpoint_requests.append((action.target_id, action.reason))
                await self._request_checkpoint(action.target_id, action.reason)
            break
        for action in getattr(result, "planned_actions", ()):
            if (
                getattr(action, "execute", False)
                and self._admission_open
                and getattr(action, "issue_id", None)
                and self._dispatch is not None
            ):
                await self._dispatch(action.issue_id)

    async def shutdown(self) -> None:
        """Close admission, checkpoint workers, and stop the loop in order."""

        self.events.append("admission.closed")
        self._closing.set()
        with suppress(TimeoutError):
            await asyncio.wait_for(
                self._checkpoint_workers(),
                timeout=self._checkpoint_timeout,
            )
        self.events.append("workers.checkpoint_requested")
        if self._task is not None:
            await self._task
            self._task = None
        self.events.append("supervisor.stopped")

    def _ensure_reconciled(self) -> None:
        if not self._reconciled:
            result = self._service.start()
            self._admission_open = bool(
                getattr(result, "admission_open", True)
            )
            self._reconciled = True
            self.events.append("reconciliation.completed")

    async def _loop(self) -> None:
        while not self._closing.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(
                    self._closing.wait(),
                    timeout=self._interval_seconds,
                )
            except TimeoutError:
                continue
