"""Durable child accounting and exactly-once reactivation (INFRA-195)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.control_operations import ControlOperations
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.lead_children import LeadChildTracker

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)
SESSION = "11111111-2222-4333-8444-555555555555"


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def control(database: Database) -> ControlOperations:
    return ControlOperations(
        database, events=EventStore(database), now=lambda: NOW
    )


@pytest.fixture
def tracker(
    database: Database, control: ControlOperations
) -> LeadChildTracker:
    return LeadChildTracker(database, control=control, now=lambda: NOW)


def seed_active_cell(database: Database) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at) VALUES ('cell-demo', 'demo', "
            "'active', 'max-c', ?, ?, ?)",
            (SESSION, NOW.isoformat(), NOW.isoformat()),
        )


def test_a_stop_with_live_children_records_the_continuation(
    database: Database, tracker: LeadChildTracker
) -> None:
    seed_active_cell(database)
    tracker.child_started(SESSION)
    tracker.child_started(SESSION)
    tracker.child_completed(SESSION)

    continuation = tracker.record_turn_stop(SESSION)

    assert continuation is not None
    assert continuation.state == "waiting"
    assert continuation.cell_id == "cell-demo"
    assert "1 background child(ren) outstanding" in continuation.condition
    # A second Stop at the same boundary refreshes, never duplicates.
    again = tracker.record_turn_stop(SESSION)
    assert again is not None
    assert again.continuation_id == continuation.continuation_id
    assert (
        database.scalar("SELECT COUNT(*) FROM lead_continuations") == 1
    )


def test_a_stop_with_no_outstanding_children_promises_nothing(
    database: Database, tracker: LeadChildTracker
) -> None:
    seed_active_cell(database)
    tracker.child_started(SESSION)
    tracker.child_completed(SESSION)

    assert tracker.record_turn_stop(SESSION) is None
    assert database.scalar("SELECT COUNT(*) FROM lead_continuations") == 0


def test_the_last_child_completion_reactivates_exactly_once(
    database: Database,
    tracker: LeadChildTracker,
    control: ControlOperations,
) -> None:
    seed_active_cell(database)
    tracker.child_started(SESSION)
    tracker.child_started(SESSION)
    continuation = tracker.record_turn_stop(SESSION)
    assert continuation is not None

    assert tracker.child_completed(SESSION) is None
    reactivation = tracker.child_completed(SESSION)

    assert reactivation is not None
    assert reactivation.kind == "children.completed"
    assert reactivation.result["started"] == 2
    assert reactivation.result["completed"] == 2
    state = database.scalar(
        "SELECT state FROM lead_continuations WHERE continuation_id = ?",
        (continuation.continuation_id,),
    )
    assert str(state) == "reactivated"
    # A duplicate completion (a retried hook, a lost ACK) is a no-op:
    # exactly one reactivation exists.
    assert tracker.child_completed(SESSION) is None
    assert (
        database.scalar(
            "SELECT COUNT(*) FROM control_operations "
            "WHERE kind = 'children.completed'"
        )
        == 1
    )


def test_a_completion_with_no_waiting_continuation_stays_silent(
    database: Database, tracker: LeadChildTracker
) -> None:
    seed_active_cell(database)
    tracker.child_started(SESSION)

    assert tracker.child_completed(SESSION) is None
    assert (
        database.scalar("SELECT COUNT(*) FROM control_operations") == 0
    )


def test_a_stale_promise_is_superseded_once_children_settle(
    database: Database, tracker: LeadChildTracker
) -> None:
    """A continuation whose children finished before any reactivation
    could fire (e.g. counters settled during a crash window) is
    superseded at the next Stop instead of reactivating a lead that
    owes nothing."""

    seed_active_cell(database)
    tracker.child_started(SESSION)
    continuation = tracker.record_turn_stop(SESSION)
    assert continuation is not None
    with database.transaction() as connection:
        connection.execute(
            "UPDATE lead_child_activity SET completed = started "
            "WHERE session_id = ?",
            (SESSION,),
        )

    assert tracker.record_turn_stop(SESSION) is None
    state = database.scalar(
        "SELECT state FROM lead_continuations WHERE continuation_id = ?",
        (continuation.continuation_id,),
    )
    assert str(state) == "superseded"
