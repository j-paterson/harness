"""Versioned durable lead assignment packets (INFRA-195)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.lead_assignments import (
    LeadAssignment,
    LeadAssignments,
)

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)
SESSION_A = "11111111-2222-4333-8444-555555555555"
SESSION_B = "66666666-7777-4888-9999-aaaaaaaaaaaa"


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def assignments(database: Database) -> LeadAssignments:
    return LeadAssignments(
        database, events=EventStore(database), now=lambda: NOW
    )


def publish(
    assignments: LeadAssignments,
    database: Database,
    *,
    issue_id: str = "INFRA-1",
    session_id: str = SESSION_A,
) -> LeadAssignment | None:
    with database.transaction() as connection:
        return assignments.publish_in(
            connection,
            project_key="demo",
            issue_id=issue_id,
            cell_id="cell-demo",
            session_id=session_id,
            profile_alias="max-c",
            instruction_id="chat-1",
            queue_transition="queued->in_development",
        )


def test_publish_binds_every_identity(
    assignments: LeadAssignments, database: Database
) -> None:
    record = publish(assignments, database)

    assert record is not None
    assert record.state == "published"
    assert record.project_key == "demo"
    assert record.issue_id == "INFRA-1"
    assert record.cell_id == "cell-demo"
    assert record.session_id == SESSION_A
    assert record.profile_alias == "max-c"
    assert record.instruction_id == "chat-1"
    assert record.queue_transition == "queued->in_development"
    assert assignments.get(record.assignment_id) == record
    journaled = database.execute(
        "SELECT aggregate_id FROM events "
        "WHERE event_type = 'assignment.published'"
    ).fetchall()
    assert [str(row["aggregate_id"]) for row in journaled] == [
        record.assignment_id
    ]


def test_republishing_the_same_binding_is_a_durable_noop(
    assignments: LeadAssignments, database: Database
) -> None:
    first = publish(assignments, database)
    again = publish(assignments, database)

    assert first is not None
    assert again is None
    assert database.scalar("SELECT COUNT(*) FROM lead_assignments") == 1


def test_redispatch_supersedes_a_stale_session(
    assignments: LeadAssignments, database: Database
) -> None:
    stale = publish(assignments, database, session_id=SESSION_A)
    fresh = publish(assignments, database, session_id=SESSION_B)

    assert stale is not None and fresh is not None
    assert assignments.get(stale.assignment_id).state == "superseded"
    assert assignments.get(fresh.assignment_id).state == "published"
    assert assignments.pending_for_session(SESSION_A) == ()
    [pending] = assignments.pending_for_session(SESSION_B)
    assert pending.assignment_id == fresh.assignment_id


def test_acknowledge_binds_the_exact_session_exactly_once(
    assignments: LeadAssignments, database: Database
) -> None:
    record = publish(assignments, database)
    assert record is not None

    assert not assignments.acknowledge(
        record.assignment_id, session_id=SESSION_B
    )
    assert assignments.acknowledge(
        record.assignment_id, session_id=SESSION_A
    )
    assert not assignments.acknowledge(
        record.assignment_id, session_id=SESSION_A
    )
    acknowledged = assignments.get(record.assignment_id)
    assert acknowledged.state == "acknowledged"
    assert acknowledged.acknowledged_at == NOW.isoformat()
    assert assignments.pending_for_session(SESSION_A) == ()


def test_notify_committed_isolates_listener_failures(
    assignments: LeadAssignments, database: Database
) -> None:
    seen: list[LeadAssignment] = []

    def explode(_assignment: LeadAssignment) -> None:
        raise RuntimeError("router down")

    assignments.subscribe(explode)
    assignments.subscribe(seen.append)
    record = publish(assignments, database)
    assert record is not None

    assignments.notify_committed(record)

    assert seen == [record]


def test_a_consumed_packet_is_replaced_by_a_fresh_dispatch_epoch(
    assignments: LeadAssignments, database: Database
) -> None:
    """A live published packet dedups retries; an acknowledged one was
    consumed, so a requeued issue's dispatch supersedes it and wakes
    the idle lead with a fresh packet."""

    first = publish(assignments, database)
    assert first is not None
    assert publish(assignments, database) is None  # live: retry dedups
    assert assignments.acknowledge(
        first.assignment_id, session_id=SESSION_A
    )

    fresh = publish(assignments, database)

    assert fresh is not None
    assert fresh.assignment_id != first.assignment_id
    assert assignments.get(first.assignment_id).state == "superseded"
    [pending] = assignments.pending_for_session(SESSION_A)
    assert pending.assignment_id == fresh.assignment_id
