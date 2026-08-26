from __future__ import annotations

from dataclasses import dataclass

import pytest

from hermes_orchestrator.scheduler import PlannedAction
from hermes_orchestrator.supervisor import Supervisor


@dataclass(frozen=True)
class FakeTick:
    planned_actions: tuple[PlannedAction, ...] = ()


class FakeService:
    def __init__(self, actions: tuple[PlannedAction, ...] = ()) -> None:
        self.actions = actions
        self.started = 0
        self.ticks = 0

    def start(self) -> object:
        self.started += 1
        return object()

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
