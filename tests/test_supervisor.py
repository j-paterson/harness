from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from hermes_orchestrator.scheduler import PlannedAction
from hermes_orchestrator.supervisor import Supervisor


@dataclass(frozen=True)
class FakeTick:
    planned_actions: tuple[PlannedAction, ...] = ()


@dataclass(frozen=True)
class FakeReconciliation:
    admission_open: bool


class FakeService:
    def __init__(
        self,
        actions: tuple[PlannedAction, ...] = (),
        *,
        admission_open: bool = True,
    ) -> None:
        self.actions = actions
        self.admission_open = admission_open
        self.started = 0
        self.ticks = 0

    def start(self) -> object:
        self.started += 1
        return FakeReconciliation(self.admission_open)

    def tick(self) -> FakeTick:
        self.ticks += 1
        return FakeTick(self.actions)


@pytest.mark.asyncio
async def test_shutdown_stops_admission_before_children() -> None:
    checkpoint_events: list[str] = []

    async def checkpoint() -> None:
        checkpoint_events.append("requested")

    supervisor = Supervisor(
        FakeService(),
        checkpoint_workers=checkpoint,
        interval_seconds=60,
    )

    await supervisor.start()
    await supervisor.shutdown()

    assert checkpoint_events == ["requested"]
    assert supervisor.events[-3:] == [
        "admission.closed",
        "workers.checkpoint_requested",
        "supervisor.stopped",
    ]


@pytest.mark.asyncio
async def test_run_once_dispatches_only_enabled_actions() -> None:
    actions = (
        PlannedAction("start_project_cell", "demo", "ENG-9", "ready", True, {}),
        PlannedAction("start_project_cell", "other", "ENG-10", "dry", False, {}),
    )
    dispatched: list[str] = []

    async def dispatch(issue_id: str) -> None:
        dispatched.append(issue_id)

    supervisor = Supervisor(FakeService(actions), dispatch=dispatch)

    await supervisor.run_once()

    assert dispatched == ["ENG-9"]


@pytest.mark.asyncio
async def test_run_once_does_not_dispatch_when_reconciliation_closes_admission(
) -> None:
    action = PlannedAction(
        "start_project_cell",
        "demo",
        "ENG-9",
        "ready",
        True,
        {},
    )
    dispatched: list[str] = []

    async def dispatch(issue_id: str) -> None:
        dispatched.append(issue_id)

    supervisor = Supervisor(
        FakeService((action,), admission_open=False),
        dispatch=dispatch,
    )

    await supervisor.run_once()

    assert dispatched == []


@pytest.mark.asyncio
async def test_checkpoint_timeout_does_not_abort_shutdown() -> None:
    never = asyncio.Event()

    async def checkpoint() -> None:
        await never.wait()

    supervisor = Supervisor(
        FakeService(),
        checkpoint_workers=checkpoint,
        interval_seconds=60,
        checkpoint_timeout=0.01,
    )
    await supervisor.start()

    await supervisor.shutdown()

    assert supervisor.events[-3:] == [
        "admission.closed",
        "workers.checkpoint_requested",
        "supervisor.stopped",
    ]


@pytest.mark.asyncio
async def test_run_once_requests_exactly_one_checkpoint_per_tick() -> None:
    from hermes_orchestrator.admission import ResourceAction

    class RedService:
        def start(self) -> object:
            return type("R", (), {"admission_open": True})()

        def tick(self) -> object:
            return type(
                "T",
                (),
                {
                    "planned_actions": (),
                    "resource_actions": (
                        ResourceAction("stop_admission", None, "red"),
                        ResourceAction("request_checkpoint", "cell-a", "red"),
                        ResourceAction("request_checkpoint", "cell-b", "red"),
                    ),
                },
            )()

    requested: list[tuple[str, str]] = []

    async def request(cell_id: str, reason: str) -> None:
        requested.append((cell_id, reason))

    supervisor = Supervisor(RedService(), request_checkpoint=request)
    await supervisor.run_once()
    assert requested == [("cell-a", "red")]
    await supervisor.run_once()
    assert requested == [("cell-a", "red"), ("cell-a", "red")]


@pytest.mark.asyncio
async def test_supervisor_delivers_reserved_requests_through_the_dispatcher() -> None:
    from hermes_orchestrator.admission import ResourceAction

    class FakeDispatcher:
        def __init__(self) -> None:
            self.delivered: list[str] = []
            self.pending_before: str | None = "ckpt:old"

        def undelivered(self) -> str | None:
            value, self.pending_before = self.pending_before, None
            return value

        async def deliver(self, request_id: str) -> str:
            self.delivered.append(request_id)
            return "delivered"

    class RedService:
        def start(self) -> object:
            return type("R", (), {"admission_open": True})()

        def tick(self) -> object:
            return type(
                "T",
                (),
                {
                    "planned_actions": (),
                    "resource_actions": (
                        ResourceAction("stop_admission", None, "red"),
                        ResourceAction(
                            "request_checkpoint", "cell-a", "red", request_id="ckpt:a"
                        ),
                    ),
                },
            )()

    dispatcher = FakeDispatcher()
    supervisor = Supervisor(RedService(), checkpoint_dispatcher=dispatcher)
    await supervisor.run_once()
    # The reserved-before-restart request is delivered first, then the
    # tick's own reservation; the legacy callback path is not used.
    assert dispatcher.delivered == ["ckpt:old", "ckpt:a"]
    assert supervisor.checkpoint_deliveries == [
        ("ckpt:old", "delivered"),
        ("ckpt:a", "delivered"),
    ]
