from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest, IssueState
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.queue import AdmissionDenied, IdempotencyConflict, QueueService


class MutableClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 8, 26, 9, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value

    def advance(self, minutes: int) -> None:
        self.value += timedelta(minutes=minutes)


@pytest.fixture
def database(tmp_path: Path):
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock()


@pytest.fixture
def queue_service(database: Database, clock: MutableClock) -> QueueService:
    return QueueService(
        database=database,
        events=EventStore(database),
        registered_projects={"demo"},
        now=clock.now,
    )


def request(
    issue_id: str,
    instruction_id: str,
    *,
    priority: int = 2,
    admitted_by: str = "operator",
    dependency_ready: bool = True,
) -> AdmissionRequest:
    return AdmissionRequest(
        issue_id=issue_id,
        project_key="demo",
        linear_priority=priority,
        admitted_by=admitted_by,
        instruction_id=instruction_id,
        dependency_ready=dependency_ready,
    )


def test_only_explicit_operator_request_can_admit(queue_service: QueueService) -> None:
    admitted = queue_service.admit(request("ENG-7", "chat-123"))

    assert admitted.issue_id == "ENG-7"
    assert admitted.state is IssueState.QUEUED
    assert queue_service.admit(request("ENG-7", "chat-123")) == admitted


def test_rejects_discovery_source(queue_service: QueueService) -> None:
    with pytest.raises(AdmissionDenied, match="explicit operator"):
        queue_service.admit(
            request("ENG-8", "poll-1", admitted_by="linear_scan")
        )


def test_rejects_reused_instruction_for_different_work(
    queue_service: QueueService,
) -> None:
    queue_service.admit(request("ENG-7", "chat-123"))

    with pytest.raises(IdempotencyConflict, match="chat-123"):
        queue_service.admit(request("ENG-8", "chat-123"))


def test_rank_is_priority_then_readiness_then_age(
    queue_service: QueueService,
    clock: MutableClock,
) -> None:
    queue_service.admit(request("ENG-1", "chat-1", priority=2))
    clock.advance(1)
    queue_service.admit(request("ENG-3", "chat-3", priority=1, dependency_ready=False))
    clock.advance(1)
    queue_service.admit(request("ENG-2", "chat-2", priority=1))

    ranked = queue_service.list_ranked(clock.now())

    assert [item.issue_id for item in ranked] == ["ENG-2", "ENG-3", "ENG-1"]


def test_reprioritize_updates_issue_and_appends_event(
    queue_service: QueueService,
    database: Database,
) -> None:
    queue_service.admit(request("ENG-7", "chat-123", priority=3))

    updated = queue_service.reprioritize("ENG-7", priority=1)

    assert updated.linear_priority == 1
    rows = database.execute("SELECT event_type FROM events ORDER BY sequence")
    event_types = [
        row[0]
        for row in rows
    ]
    assert event_types == ["issue.admitted", "issue.reprioritized"]
