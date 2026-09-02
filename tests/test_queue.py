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


def test_complete_started_issue_keeps_persistent_project_cell(
    queue_service: QueueService,
    database: Database,
) -> None:
    queue_service.admit(request("ENG-7", "chat-123"))
    with database.transaction() as connection:
        connection.execute(
            "UPDATE admitted_issues SET state = 'in_development' "
            "WHERE issue_id = 'ENG-7'"
        )
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "cell-demo",
                "demo",
                "active",
                "max-a",
                "11111111-1111-4111-8111-111111111111",
                "2026-08-26T09:00:00+00:00",
                "2026-08-26T09:00:00+00:00",
            ),
        )

    completed = queue_service.complete(
        "ENG-7",
        reason="pr_merged",
        evidence="https://github.example/pull/7",
    )

    assert completed.state is IssueState.DONE
    assert database.scalar(
        "SELECT state FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == "active"


# -- mark_dependency_ready (INFRA-198 P2) -------------------------------


def test_mark_dependency_ready_flips_only_blocked_not_ready_rows(
    queue_service: QueueService, database: Database
) -> None:
    queue_service.admit(request("ENG-1", "chat-1", dependency_ready=False))
    queue_service.transition(
        "ENG-1", IssueState.BLOCKED, actor="operator", reason="blocked on ENG-0"
    )
    queue_service.admit(request("ENG-2", "chat-2", dependency_ready=True))
    queue_service.transition(
        "ENG-2", IssueState.BLOCKED, actor="operator", reason="already ready"
    )
    queue_service.admit(request("ENG-3", "chat-3", dependency_ready=False))
    queue_service.transition(
        "ENG-3", IssueState.PAUSED, actor="operator", reason="operator hold"
    )
    queue_service.admit(request("ENG-4", "chat-4", dependency_ready=False))
    queue_service.transition(
        "ENG-4", IssueState.IN_DEVELOPMENT, actor="operator", reason="in progress"
    )
    queue_service.admit(request("ENG-5", "chat-5", dependency_ready=True))
    # ENG-5 stays queued.

    flipped = queue_service.mark_dependency_ready(
        "demo", actor="post_merge_advance", reason="merge:review:demo:evt-1"
    )

    assert flipped == ("ENG-1",)
    assert queue_service.get("ENG-1").dependency_ready is True
    assert queue_service.get("ENG-2").dependency_ready is True
    assert queue_service.get("ENG-3").state is IssueState.PAUSED
    assert queue_service.get("ENG-3").dependency_ready is False
    assert queue_service.get("ENG-4").dependency_ready is False
    assert database.scalar(
        "SELECT count(*) FROM events WHERE event_type = 'issue.dependency_ready'"
    ) == 1

    # Idempotent: rerunning after every row is already flipped touches
    # and journals nothing.
    again = queue_service.mark_dependency_ready(
        "demo", actor="post_merge_advance", reason="merge:review:demo:evt-1"
    )
    assert again == ()
    assert database.scalar(
        "SELECT count(*) FROM events WHERE event_type = 'issue.dependency_ready'"
    ) == 1


def test_conditional_transition_refuses_a_changed_source_state(
    queue_service: QueueService, database: Database
) -> None:
    queue_service.admit(request("ENG-9", "chat-9"))
    queue_service.transition(
        "ENG-9", IssueState.DONE, actor="worker", reason="completed concurrently"
    )
    events_before = int(database.scalar("SELECT count(*) FROM events"))

    result = queue_service.transition_if(
        "ENG-9",
        IssueState.QUEUED,
        from_states={IssueState.PAUSED, IssueState.BLOCKED},
        actor="remote-operator",
        reason="remote operator confirmation",
    )

    assert result is None
    assert queue_service.get("ENG-9").state is IssueState.DONE
    assert int(database.scalar("SELECT count(*) FROM events")) == events_before


# -- restore_readiness_after_requeue (INFRA-198) ------------------------


def test_restore_readiness_after_requeue_repairs_a_stuck_queued_row(
    queue_service: QueueService, database: Database
) -> None:
    queue_service.admit(request("ENG-11", "chat-11", dependency_ready=True))
    queue_service.transition(
        "ENG-11", IssueState.PAUSED, actor="orchestrator", reason="resource pressure"
    )
    # The observed production condition: dependency_ready cleared with no
    # journaled event, then the row moved back to queued out of band.
    with database.transaction() as connection:
        connection.execute(
            "UPDATE admitted_issues SET dependency_ready = 0 WHERE issue_id = ?",
            ("ENG-11",),
        )
    queue_service.transition(
        "ENG-11", IssueState.QUEUED, actor="remote-operator", reason="retry"
    )
    assert queue_service.get("ENG-11").dependency_ready is False

    repaired = queue_service.restore_readiness_after_requeue(
        "ENG-11", actor="remote-operator", reason="remote operator confirmation"
    )

    assert repaired is not None
    assert repaired.state is IssueState.QUEUED
    assert repaired.dependency_ready is True
    assert queue_service.get("ENG-11").dependency_ready is True
    assert database.scalar(
        "SELECT count(*) FROM events WHERE event_type = 'issue.dependency_ready' "
        "AND aggregate_id = ?",
        ("ENG-11",),
    ) == 1

    # Nothing left to repair: a healthy queued+ready row is left untouched.
    again = queue_service.restore_readiness_after_requeue(
        "ENG-11", actor="remote-operator", reason="remote operator confirmation"
    )
    assert again is None
    assert database.scalar(
        "SELECT count(*) FROM events WHERE event_type = 'issue.dependency_ready' "
        "AND aggregate_id = ?",
        ("ENG-11",),
    ) == 1


def test_restore_readiness_refuses_a_newly_admitted_dependency_gated_row(
    queue_service: QueueService, database: Database, clock: MutableClock
) -> None:
    # Admission itself permits queued + dependency_ready = 0: that is a
    # legitimate dependency gate, NOT the residue of a prior requeue. With
    # no journaled transition into queued there is no requeue provenance,
    # so the row must be left completely unchanged.
    queue_service.admit(request("ENG-12", "chat-12", dependency_ready=False))
    before = database.execute(
        "SELECT state, dependency_ready, updated_at FROM admitted_issues "
        "WHERE issue_id = ?",
        ("ENG-12",),
    ).fetchone()
    events_before = int(database.scalar("SELECT count(*) FROM events"))
    clock.advance(5)

    assert (
        queue_service.restore_readiness_after_requeue(
            "ENG-12", actor="remote-operator", reason="remote operator confirmation"
        )
        is None
    )

    after = database.execute(
        "SELECT state, dependency_ready, updated_at FROM admitted_issues "
        "WHERE issue_id = ?",
        ("ENG-12",),
    ).fetchone()
    assert tuple(after) == tuple(before)
    assert queue_service.get("ENG-12").dependency_ready is False
    assert int(database.scalar("SELECT count(*) FROM events")) == events_before


def test_restore_readiness_accepts_provenance_from_a_transition_if_requeue(
    queue_service: QueueService, database: Database
) -> None:
    # The retry handler's FIRST call site: transition_if commits the
    # issue.transitioned event before restore_readiness_after_requeue opens
    # its own transaction, so the same provenance predicate is satisfied by
    # the requeue that just happened.
    queue_service.admit(request("ENG-13", "chat-13", dependency_ready=False))
    queue_service.transition(
        "ENG-13", IssueState.PAUSED, actor="orchestrator", reason="resource pressure"
    )

    requeued = queue_service.transition_if(
        "ENG-13",
        IssueState.QUEUED,
        from_states={IssueState.PAUSED, IssueState.BLOCKED},
        actor="remote-operator",
        reason="remote operator confirmation",
    )
    assert requeued is not None
    repaired = queue_service.restore_readiness_after_requeue(
        "ENG-13", actor="remote-operator", reason="remote operator confirmation"
    )

    assert repaired is not None
    assert repaired.dependency_ready is True
    assert queue_service.get("ENG-13").dependency_ready is True


def test_mark_dependency_ready_is_scoped_to_its_project(
    database: Database, clock: MutableClock
) -> None:
    service = QueueService(
        database=database,
        events=EventStore(database),
        registered_projects={"demo", "other"},
        now=clock.now,
    )
    service.admit(
        AdmissionRequest(
            issue_id="OTHER-1",
            project_key="other",
            linear_priority=2,
            admitted_by="operator",
            instruction_id="chat-o1",
            dependency_ready=False,
        )
    )
    service.transition("OTHER-1", IssueState.BLOCKED, actor="operator", reason="x")
    service.admit(request("ENG-6", "chat-6", dependency_ready=False))
    service.transition("ENG-6", IssueState.BLOCKED, actor="operator", reason="x")

    flipped = service.mark_dependency_ready(
        "demo", actor="post_merge_advance", reason="merge:x"
    )

    assert flipped == ("ENG-6",)
    assert service.get("OTHER-1").dependency_ready is False


# -- post_merge_acceptance (INFRA-198 J1) --------------------------------


def test_transition_to_and_from_post_merge_acceptance(
    queue_service: QueueService, database: Database
) -> None:
    queue_service.admit(request("ENG-7", "chat-123"))

    held = queue_service.transition(
        "ENG-7",
        IssueState.POST_MERGE_ACCEPTANCE,
        actor="codex_merger",
        reason="merged abc123; acceptance pending",
    )
    assert held.state is IssueState.POST_MERGE_ACCEPTANCE

    done = queue_service.transition(
        "ENG-7",
        IssueState.DONE,
        actor="codex_merger",
        reason="acceptance satisfied",
    )
    assert done.state is IssueState.DONE
    transitions = database.execute(
        "SELECT payload_json FROM events WHERE event_type = "
        "'issue.transitioned' ORDER BY sequence"
    ).fetchall()
    assert [row["payload_json"] for row in transitions] == [
        '{"from":"queued","reason":"merged abc123; acceptance pending",'
        '"to":"post_merge_acceptance"}',
        '{"from":"post_merge_acceptance","reason":"acceptance satisfied",'
        '"to":"done"}',
    ]


def test_list_ranked_excludes_post_merge_acceptance(
    queue_service: QueueService, clock: MutableClock
) -> None:
    queue_service.admit(request("ENG-7", "chat-123"))
    queue_service.admit(request("ENG-8", "chat-124"))
    queue_service.transition(
        "ENG-7",
        IssueState.POST_MERGE_ACCEPTANCE,
        actor="codex_merger",
        reason="merged abc123; acceptance pending",
    )

    ranked = queue_service.list_ranked(clock.now())

    assert [item.issue_id for item in ranked] == ["ENG-8"]


def _done_issue(
    queue_service: QueueService,
    issue_id: str = "ENG-7",
    instruction_id: str = "chat-123",
) -> None:
    queue_service.admit(request(issue_id, instruction_id))
    queue_service.complete(
        issue_id,
        reason="linear_completed",
        evidence=f"https://linear.example/{issue_id}",
    )


def test_reactivate_done_issue_when_linear_is_non_terminal(
    queue_service: QueueService, database: Database
) -> None:
    _done_issue(queue_service)

    reactivated = queue_service.reactivate(
        request("ENG-7", "chat-456", priority=1, dependency_ready=False),
        linear_status="In Development",
    )

    assert reactivated.state is IssueState.QUEUED
    assert reactivated.instruction_id == "chat-456"
    assert reactivated.linear_priority == 1
    assert reactivated.dependency_ready is False
    event_types = [
        row["event_type"]
        for row in database.execute(
            "SELECT event_type FROM events ORDER BY sequence"
        ).fetchall()
    ]
    assert event_types == ["issue.admitted", "issue.completed", "issue.reactivated"]
    reactivated_event = database.execute(
        "SELECT actor, correlation_id, payload_json FROM events "
        "WHERE event_type = 'issue.reactivated'"
    ).fetchone()
    assert reactivated_event["actor"] == "operator"
    assert reactivated_event["correlation_id"] == "chat-456"
    assert reactivated_event["payload_json"] == (
        '{"dependency_ready":false,"from":"done","linear_status":'
        '"In Development","priority":1,"project_key":"demo","to":"queued"}'
    )


@pytest.mark.parametrize("linear_status", ["Done", "Canceled", "Cancelled", None])
def test_reactivate_refuses_when_linear_is_terminal(
    queue_service: QueueService, linear_status: str | None
) -> None:
    _done_issue(queue_service)

    with pytest.raises(AdmissionDenied):
        queue_service.reactivate(
            request("ENG-7", "chat-456"), linear_status=linear_status
        )


def test_reactivate_refuses_locally_non_terminal_issue(
    queue_service: QueueService,
) -> None:
    queue_service.admit(request("ENG-7", "chat-123"))

    with pytest.raises(AdmissionDenied, match="already admitted"):
        queue_service.reactivate(
            request("ENG-7", "chat-456"), linear_status="In Development"
        )


def test_reactivate_refuses_project_mismatch(
    database: Database, clock: MutableClock
) -> None:
    queue_service = QueueService(
        database=database,
        events=EventStore(database),
        registered_projects={"demo", "other"},
        now=clock.now,
    )
    _done_issue(queue_service)
    mismatched = AdmissionRequest(
        issue_id="ENG-7",
        project_key="other",
        linear_priority=2,
        admitted_by="operator",
        instruction_id="chat-456",
    )

    with pytest.raises(AdmissionDenied):
        queue_service.reactivate(mismatched, linear_status="In Development")


def test_reactivate_is_idempotent_for_the_same_instruction(
    queue_service: QueueService, database: Database
) -> None:
    _done_issue(queue_service)
    first = queue_service.reactivate(
        request("ENG-7", "chat-456", priority=1), linear_status="In Development"
    )

    second = queue_service.reactivate(
        request("ENG-7", "chat-456", priority=1), linear_status="In Development"
    )

    assert second == first
    count = database.execute("SELECT count(*) AS n FROM events").fetchone()
    assert count["n"] == 3


def test_reactivate_conflicts_on_reused_instruction_of_another_issue(
    queue_service: QueueService,
) -> None:
    _done_issue(queue_service, issue_id="ENG-7", instruction_id="chat-123")
    queue_service.admit(request("ENG-8", "chat-999"))

    with pytest.raises(IdempotencyConflict, match="chat-999"):
        queue_service.reactivate(
            request("ENG-7", "chat-999"), linear_status="In Development"
        )
