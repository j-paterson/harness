"""Tests for the INFRA-220 ``target_issue`` transition."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.issue_targeting import (
    IssueTargetingRefused,
    target_issue,
)
from hermes_orchestrator.lead_assignments import LeadAssignments

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)
CELL_ID = "cell-demo"
SESSION_ID = "11111111-2222-4333-8444-555555555555"
OTHER_SESSION_ID = "66666666-7777-4888-9999-aaaaaaaaaaaa"


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def assignments(database: Database) -> LeadAssignments:
    return LeadAssignments(database, events=EventStore(database), now=lambda: NOW)


def _seed_admitted(
    database: Database,
    *,
    issue_id: str,
    project_key: str = "demo",
    priority: int = 2,
    state: str = "queued",
    admitted_at: str = "2026-08-01T00:00:00+00:00",
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO admitted_issues("
            "issue_id, project_key, priority, state, instruction_id, "
            "dependency_ready, overlap_risk, admitted_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, 1, 0, ?, ?)",
            (
                issue_id,
                project_key,
                priority,
                state,
                f"instr-{issue_id}",
                admitted_at,
                admitted_at,
            ),
        )


def _seed_cell(
    database: Database,
    *,
    cell_id: str = CELL_ID,
    project_key: str = "demo",
    session_id: str = SESSION_ID,
    profile_alias: str = "max-c",
    state: str = "active",
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                cell_id,
                project_key,
                state,
                profile_alias,
                session_id,
                "2026-08-01T00:00:00+00:00",
                "2026-08-01T00:00:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO profile_leases("
            "profile_alias, project_key, state, acquired_at) "
            "VALUES (?, ?, 'active', ?)",
            (profile_alias, project_key, "2026-08-01T00:00:00+00:00"),
        )


def _admitted_snapshot(database: Database) -> list[tuple[object, ...]]:
    rows = database.execute(
        "SELECT issue_id, state, priority, admitted_at FROM admitted_issues "
        "ORDER BY issue_id"
    ).fetchall()
    return [tuple(row) for row in rows]


def _assignment_count(database: Database) -> int:
    row = database.execute("SELECT COUNT(*) AS n FROM lead_assignments").fetchone()
    return int(row["n"])


def test_happy_path_publishes_only_the_targeted_issue(
    database: Database, assignments: LeadAssignments
) -> None:
    # Two older, equal-priority queued rows the scheduler preview would
    # otherwise pick first; target-issue must leave both untouched.
    _seed_admitted(
        database,
        issue_id="INFRA-1",
        priority=2,
        admitted_at="2026-07-01T00:00:00+00:00",
    )
    _seed_admitted(
        database,
        issue_id="INFRA-2",
        priority=2,
        admitted_at="2026-07-15T00:00:00+00:00",
    )
    _seed_admitted(
        database,
        issue_id="INFRA-9",
        priority=2,
        admitted_at="2026-08-20T00:00:00+00:00",
    )
    _seed_cell(database)
    before = _admitted_snapshot(database)

    result = target_issue(
        database,
        assignments=assignments,
        issue_id="INFRA-9",
        project_key="demo",
        cell_id=CELL_ID,
        session_id=SESSION_ID,
        instruction="focus on INFRA-9",
        known_projects={"demo"},
    )

    assert result.idempotent is False
    assert result.assignment.issue_id == "INFRA-9"
    assert result.assignment.session_id == SESSION_ID
    assert result.assignment.state == "published"
    assert _assignment_count(database) == 1
    assert _admitted_snapshot(database) == before


def test_repeat_while_pending_is_idempotent(
    database: Database, assignments: LeadAssignments
) -> None:
    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)

    first = target_issue(
        database,
        assignments=assignments,
        issue_id="INFRA-9",
        project_key="demo",
        cell_id=CELL_ID,
        session_id=SESSION_ID,
        instruction="focus on INFRA-9",
        known_projects={"demo"},
    )
    second = target_issue(
        database,
        assignments=assignments,
        issue_id="INFRA-9",
        project_key="demo",
        cell_id=CELL_ID,
        session_id=SESSION_ID,
        instruction="focus on INFRA-9 again",
        known_projects={"demo"},
    )

    assert second.idempotent is True
    assert second.assignment.assignment_id == first.assignment.assignment_id
    assert _assignment_count(database) == 1


def test_refuses_session_mismatch_with_zero_writes(
    database: Database, assignments: LeadAssignments
) -> None:
    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    before = _admitted_snapshot(database)

    with pytest.raises(IssueTargetingRefused):
        target_issue(
            database,
            assignments=assignments,
            issue_id="INFRA-9",
            project_key="demo",
            cell_id=CELL_ID,
            session_id=OTHER_SESSION_ID,
            instruction="focus on INFRA-9",
            known_projects={"demo"},
        )

    assert _assignment_count(database) == 0
    assert _admitted_snapshot(database) == before


def test_refuses_unadmitted_issue_with_zero_writes(
    database: Database, assignments: LeadAssignments
) -> None:
    _seed_cell(database)
    before = _admitted_snapshot(database)

    with pytest.raises(IssueTargetingRefused):
        target_issue(
            database,
            assignments=assignments,
            issue_id="INFRA-404",
            project_key="demo",
            cell_id=CELL_ID,
            session_id=SESSION_ID,
            instruction="focus on INFRA-404",
            known_projects={"demo"},
        )

    assert _assignment_count(database) == 0
    assert _admitted_snapshot(database) == before


def test_refuses_empty_instruction_with_zero_writes(
    database: Database, assignments: LeadAssignments
) -> None:
    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)

    with pytest.raises(IssueTargetingRefused):
        target_issue(
            database,
            assignments=assignments,
            issue_id="INFRA-9",
            project_key="demo",
            cell_id=CELL_ID,
            session_id=SESSION_ID,
            instruction="   ",
            known_projects={"demo"},
        )

    assert _assignment_count(database) == 0
