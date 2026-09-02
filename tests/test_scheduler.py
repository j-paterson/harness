from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest, IssueState
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


def test_active_project_plans_issue_for_existing_lead(
    queue_service: QueueService,
) -> None:
    admit(queue_service, "ENG-7", "demo", 1)
    scheduler = Scheduler(queue_service, mode="active", active_projects={"demo"})

    actions = scheduler.plan(FakeSnapshot(pressure="green", can_admit=True))

    assert [(action.kind, action.issue_id, action.execute) for action in actions] == [
        ("resume_project_cell", "ENG-7", True)
    ]


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


def test_active_project_still_plans_next_ready_issue_with_other_lanes_occupying(
    queue_service: QueueService,
) -> None:
    """INFRA-219 R6 (Sol correction 110ed759): the scheduler's per-project
    dedup within one ``plan()`` cycle (``plan_key`` in :meth:`Scheduler.plan`)
    is a one-action-per-cycle throttle on the SAME lead cell's next turn, not
    a "no more than one issue may ever occupy this project" rule -- that
    occupancy bound now lives, bounded to
    ``hermes_orchestrator.cells.MAX_DEVELOPMENT_ISSUE_LANES``, in
    ``cells.py``'s dispatch/activation path. ``list_ranked`` (``queue.py``)
    already only ever returns QUEUED/BLOCKED issues, so an issue this test
    moves to ``IN_DEVELOPMENT`` -- simulating one of several concurrently
    occupying development-lane issues for project "demo" -- is invisible to
    ranking, and the scheduler must still surface the next ready issue for
    that same active project to resume/dispatch, exactly as it would with
    only one lane in flight.
    """

    admit(queue_service, "ENG-7", "demo", 1)
    queue_service.transition(
        "ENG-7",
        IssueState.IN_DEVELOPMENT,
        actor="operator",
        reason="already occupying one development lane",
    )
    admit(queue_service, "ENG-8", "demo", 2)
    scheduler = Scheduler(queue_service, mode="active", active_projects={"demo"})

    actions = scheduler.plan(FakeSnapshot(pressure="green", can_admit=True))

    assert [(action.kind, action.issue_id, action.execute) for action in actions] == [
        ("resume_project_cell", "ENG-8", True)
    ]


def test_multiple_ready_issues_for_active_project_plan_exactly_one_per_cycle(
    queue_service: QueueService,
) -> None:
    """INFRA-219 R6: cell topology is unchanged by this packet -- one
    development lead cell per project still exists (item 6 of the packet),
    coordinating however many issue lanes are occupying underneath it. A
    single ``plan()`` cycle therefore still proposes exactly one next turn
    per project (the highest-ranked ready issue), never one action per
    queued issue -- planning multiple concurrent lanes happens over
    successive dispatches/cycles, not by fanning out cells per issue.
    """

    admit(queue_service, "ENG-7", "demo", 1)
    admit(queue_service, "ENG-8", "demo", 2)
    admit(queue_service, "ENG-9", "demo", 3)
    scheduler = Scheduler(queue_service, mode="active", active_projects={"demo"})

    actions = scheduler.plan(FakeSnapshot(pressure="green", can_admit=True))

    starts = [action for action in actions if action.kind == "resume_project_cell"]
    assert len(starts) == 1
    assert starts[0].issue_id == "ENG-7"


def test_live_cell_without_ready_pair_is_held(queue_service: QueueService) -> None:
    admit(queue_service, "ENG-7", "demo", 1)
    scheduler = Scheduler(
        queue_service,
        mode="active",
        active_projects={"demo"},
        ready_pairs=frozenset(),
    )

    actions = scheduler.plan(FakeSnapshot(pressure="green", can_admit=True))

    assert [(action.kind, action.execute) for action in actions] == [
        ("pair_not_ready", False)
    ]
    assert not any(
        action.kind in {"resume_project_cell", "start_project_cell"}
        for action in actions
    )
    assert actions[0].project_key == "demo"
    assert actions[0].evidence == {"project_key": "demo"}


def test_ready_pair_resumes_as_before(queue_service: QueueService) -> None:
    admit(queue_service, "ENG-7", "demo", 1)
    scheduler = Scheduler(
        queue_service,
        mode="active",
        active_projects={"demo"},
        ready_pairs={"demo"},
    )

    actions = scheduler.plan(FakeSnapshot(pressure="green", can_admit=True))

    assert [(action.kind, action.issue_id, action.execute) for action in actions] == [
        ("resume_project_cell", "ENG-7", True)
    ]


def test_scheduler_without_ready_pairs_is_unchanged(
    queue_service: QueueService,
) -> None:
    admit(queue_service, "ENG-7", "demo", 1)
    scheduler = Scheduler(queue_service, mode="active", active_projects={"demo"})

    actions = scheduler.plan(FakeSnapshot(pressure="green", can_admit=True))

    assert [(action.kind, action.issue_id, action.execute) for action in actions] == [
        ("resume_project_cell", "ENG-7", True)
    ]


def test_yellow_pressure_admits_only_priority_one_work(
    queue_service: QueueService,
) -> None:
    from dataclasses import dataclass

    from hermes_orchestrator.domain import AdmissionRequest

    @dataclass(frozen=True)
    class YellowSnapshot:
        pressure: str = "yellow"
        can_admit: bool = False
        admission_max_priority: int | None = 1

    queue_service.admit(
        AdmissionRequest("ENG-1", "demo", 1, "operator", "i-1")
    )
    queue_service.admit(
        AdmissionRequest("ENG-2", "other", 3, "operator", "i-2")
    )
    actions = Scheduler(queue_service, mode="active").plan(YellowSnapshot())
    kinds = [(action.kind, action.issue_id) for action in actions]
    assert kinds == [("start_project_cell", "ENG-1"), ("admission_limited", None)]
    assert actions[-1].evidence["held_issues"] == ["ENG-2"]
    assert actions[-1].execute is False
