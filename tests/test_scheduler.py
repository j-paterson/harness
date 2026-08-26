from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.scheduler import Scheduler


@dataclass(frozen=True)
class FakeSnapshot:
    pressure: str
    can_admit: bool


@pytest.fixture
def database(tmp_path: Path):
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def queue_service(database: Database) -> QueueService:
    return QueueService(database, EventStore(database), {"demo", "other"})


def admit(queue: QueueService, issue_id: str, project: str, priority: int) -> None:
    queue.admit(
        AdmissionRequest(
            issue_id=issue_id,
            project_key=project,
            linear_priority=priority,
            admitted_by="operator",
            instruction_id=f"chat-{issue_id}",
        )
    )


def test_observe_mode_never_dispatches(queue_service: QueueService) -> None:
    admit(queue_service, "ENG-7", "demo", 1)
    scheduler = Scheduler(queue_service, mode="observe")

    actions = scheduler.plan(FakeSnapshot(pressure="green", can_admit=True))

    assert actions
    assert all(action.execute is False for action in actions)


def test_one_lead_per_project(queue_service: QueueService) -> None:
    admit(queue_service, "ENG-7", "demo", 1)
    admit(queue_service, "ENG-8", "demo", 2)
    scheduler = Scheduler(queue_service, mode="observe")

    actions = scheduler.plan(FakeSnapshot(pressure="green", can_admit=True))

    starts = [action for action in actions if action.kind == "start_project_cell"]
    assert len(starts) == 1
    assert starts[0].project_key == "demo"
    assert starts[0].issue_id == "ENG-7"


def test_active_project_does_not_plan_second_lead(queue_service: QueueService) -> None:
    admit(queue_service, "ENG-7", "demo", 1)
    scheduler = Scheduler(queue_service, mode="observe", active_projects={"demo"})

    actions = scheduler.plan(FakeSnapshot(pressure="green", can_admit=True))

    assert not any(action.kind == "start_project_cell" for action in actions)


def test_red_pressure_plans_no_new_work(queue_service: QueueService) -> None:
    admit(queue_service, "ENG-7", "demo", 1)
    scheduler = Scheduler(queue_service, mode="observe")

    actions = scheduler.plan(FakeSnapshot(pressure="red", can_admit=False))

    assert not any(action.kind.startswith("start_") for action in actions)
    assert actions[0].kind == "admission_blocked"
    assert actions[0].evidence == {"pressure": "red"}


def test_scheduler_uses_timezone_aware_ranking_time(
    queue_service: QueueService,
) -> None:
    admit(queue_service, "ENG-7", "demo", 1)
    scheduler = Scheduler(
        queue_service,
        mode="observe",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )

    assert scheduler.plan(FakeSnapshot("green", True))[0].issue_id == "ENG-7"
