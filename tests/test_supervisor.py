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
