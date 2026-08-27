from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier

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


def test_complete_marks_issue_done_and_removes_it_from_ranked_queue(
    queue_service: QueueService,
    database: Database,
    clock: MutableClock,
) -> None:
    queue_service.admit(request("ENG-7", "chat-123"))

    completed = queue_service.complete(
        "ENG-7",
        reason="linear_completed",
        evidence="https://linear.example/ENG-7",
    )

    assert completed.state is IssueState.DONE
    assert queue_service.list_ranked(clock.now()) == []
    event = database.execute(
        "SELECT event_type, actor, payload_json FROM events ORDER BY sequence DESC"
    ).fetchone()
    assert event is not None
    assert event["event_type"] == "issue.completed"
    assert event["actor"] == "operator"
    assert event["payload_json"] == (
        '{"evidence":"https://linear.example/ENG-7",'
        '"from":"queued","reason":"linear_completed"}'
    )


def test_complete_is_idempotent(
    queue_service: QueueService,
    database: Database,
) -> None:
    queue_service.admit(request("ENG-7", "chat-123"))
    queue_service.complete(
        "ENG-7",
        reason="linear_completed",
        evidence="https://linear.example/ENG-7",
    )

    completed = queue_service.complete(
        "ENG-7",
        reason="linear_completed",
        evidence="https://linear.example/ENG-7",
    )

    assert completed.state is IssueState.DONE
    assert database.scalar(
        "SELECT count(*) FROM events WHERE event_type = 'issue.completed'"
    ) == 1


def test_concurrent_complete_appends_one_event(tmp_path: Path) -> None:
    database_path = tmp_path / "state.db"
    setup_database = Database.open(database_path)
    QueueService(
        database=setup_database,
        events=EventStore(setup_database),
        registered_projects={"demo"},
    ).admit(request("ENG-7", "chat-123"))
    setup_database.close()
    barrier = Barrier(2)

    class SynchronizingQueueService(QueueService):
        def __init__(self, database: Database) -> None:
            super().__init__(database, EventStore(database), {"demo"})
            self._synchronize_next_get = True

        def get(self, issue_id: str):
            issue = super().get(issue_id)
            if self._synchronize_next_get:
                self._synchronize_next_get = False
                barrier.wait(timeout=5)
            return issue

    def complete_once() -> None:
        database = Database.open(database_path)
        try:
            SynchronizingQueueService(database).complete(
                "ENG-7",
                reason="linear_completed",
                evidence="https://linear.example/ENG-7",
            )
        finally:
            database.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(lambda _: complete_once(), range(2)))

    verification_database = Database.open(database_path)
    try:
        assert verification_database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'issue.completed'"
        ) == 1
    finally:
        verification_database.close()


def test_complete_refuses_issue_for_project_with_active_cell(
    queue_service: QueueService,
    database: Database,
) -> None:
    queue_service.admit(request("ENG-7", "chat-123"))
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "cell-demo",
                "demo",
                "starting",
                "max-a",
                "11111111-1111-4111-8111-111111111111",
                "2026-08-26T09:00:00+00:00",
                "2026-08-26T09:00:00+00:00",
            ),
        )

    with pytest.raises(ValueError, match="active project cell"):
        queue_service.complete(
            "ENG-7",
            reason="linear_completed",
            evidence="https://linear.example/ENG-7",
        )

    assert queue_service.get("ENG-7").state is IssueState.QUEUED
