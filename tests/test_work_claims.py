"""Cell work ownership, separated from packet delivery state (INFRA-222)."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

import hermes_orchestrator.db
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.work_claims import (
    ClaimRefused,
    WorkClaim,
    WorkClaims,
    WorkerBinding,
    WorkerBindings,
)

NOW = datetime(2026, 9, 3, 12, tzinfo=UTC)
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
def claims(database: Database) -> WorkClaims:
    return WorkClaims(database, events=EventStore(database), now=lambda: NOW)


@pytest.fixture
def bindings(database: Database) -> WorkerBindings:
    return WorkerBindings(
        database, events=EventStore(database), now=lambda: NOW
    )


def open_claim(
    claims: WorkClaims,
    database: Database,
    *,
    issue_id: str = "INFRA-1",
    cell_id: str = "cell-demo",
    role: str = "development",
    child_lane: str = "",
    project_key: str = "demo",
) -> WorkClaim:
    with database.transaction() as connection:
        return claims.open_in(
            connection,
            project_key=project_key,
            issue_id=issue_id,
            cell_id=cell_id,
            role=role,
            child_lane=child_lane,
        )


# --- WorkClaims -------------------------------------------------------


def test_development_harness_and_review_claims_coexist_for_one_issue(
    claims: WorkClaims, database: Database
) -> None:
    development = open_claim(claims, database, role="development")
    harness = open_claim(claims, database, role="harness")
    review = open_claim(claims, database, role="review")

    active = claims.active_for_issue("INFRA-1")
    assert {claim.claim_id for claim in active} == {
        development.claim_id,
        harness.claim_id,
        review.claim_id,
    }
    assert all(claim.state == "active" for claim in active)


def test_open_in_is_idempotent(
    claims: WorkClaims, database: Database
) -> None:
    first = open_claim(claims, database)
    second = open_claim(claims, database)

    assert first.claim_id == second.claim_id
    assert database.scalar("SELECT COUNT(*) FROM work_claims") == 1
    journaled = database.execute(
        "SELECT COUNT(*) FROM events WHERE event_type = 'work_claim.opened'"
    ).fetchone()[0]
    assert int(journaled) == 1


def test_superseding_a_lead_assignment_leaves_the_claim_active(
    claims: WorkClaims, database: Database
) -> None:
    claim = open_claim(claims, database)

    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO lead_assignments("
            "assignment_id, schema_version, project_key, issue_id, cell_id, "
            "session_id, profile_alias, instruction_id, queue_transition, "
            "state, created_at, updated_at, acknowledged_at) VALUES "
            "('assign-1', 1, 'demo', 'INFRA-1', 'cell-demo', ?, 'max-c', "
            "'chat-1', 'queued->in_development', 'acknowledged', ?, ?, ?)",
            (SESSION_A, NOW.isoformat(), NOW.isoformat(), NOW.isoformat()),
        )
        connection.execute(
            "INSERT INTO lead_assignments("
            "assignment_id, schema_version, project_key, issue_id, cell_id, "
            "session_id, profile_alias, instruction_id, queue_transition, "
            "state, created_at, updated_at, acknowledged_at) VALUES "
            "('assign-2', 1, 'demo', 'INFRA-1', 'cell-demo', ?, 'max-c', "
            "'chat-2', 'queued->in_development', 'published', ?, ?, NULL)",
            (SESSION_B, NOW.isoformat(), NOW.isoformat()),
        )
        connection.execute(
            "UPDATE lead_assignments SET state = 'superseded' "
            "WHERE assignment_id = 'assign-1'"
        )

    refreshed = claims.get(claim.claim_id)
    assert refreshed.state == "active"
    assert refreshed == claim


def test_close_for_issue_closes_only_that_issues_claims(
    claims: WorkClaims, database: Database
) -> None:
    target = open_claim(claims, database, issue_id="INFRA-1")
    other = open_claim(claims, database, issue_id="INFRA-2")

    with database.transaction() as connection:
        closed = claims.close_for_issue_in(
            connection, issue_id="INFRA-1", reason="issue completed"
        )

    assert [claim.claim_id for claim in closed] == [target.claim_id]
    assert claims.get(target.claim_id).state == "closed"
    assert claims.get(target.claim_id).closed_reason == "issue completed"
    assert claims.get(other.claim_id).state == "active"
    assert claims.active_for_issue("INFRA-1") == []


def test_close_for_issue_can_be_scoped_by_role(
    claims: WorkClaims, database: Database
) -> None:
    development = open_claim(claims, database, role="development")
    harness = open_claim(claims, database, role="harness")

    with database.transaction() as connection:
        closed = claims.close_for_issue_in(
            connection,
            issue_id="INFRA-1",
            reason="harness done",
            roles=("harness",),
        )

    assert [claim.claim_id for claim in closed] == [harness.claim_id]
    assert claims.get(development.claim_id).state == "active"
    assert claims.get(harness.claim_id).state == "closed"


def test_close_in_refuses_unknown_or_already_closed(
    claims: WorkClaims, database: Database
) -> None:
    claim = open_claim(claims, database)

    with pytest.raises(ClaimRefused):
        claims.close(claim_id="does-not-exist", reason="nope")

    claims.close(claim_id=claim.claim_id, reason="done")
    with pytest.raises(ClaimRefused):
        claims.close(claim_id=claim.claim_id, reason="again")


def test_current_for_cell_returns_the_newest_active_claim_by_role(
    claims: WorkClaims, database: Database
) -> None:
    later_now = NOW
    open_claim(claims, database, issue_id="INFRA-1", role="development")
    second = open_claim(
        claims, database, issue_id="INFRA-2", role="development"
    )

    current = claims.current_for_cell("cell-demo", role="development")
    assert current is not None
    assert current.claim_id == second.claim_id
    assert later_now is NOW  # sanity: fixed clock, no time-based ordering


# --- WorkerBindings -----------------------------------------------------


def test_bind_creates_generation_one(
    bindings: WorkerBindings, database: Database
) -> None:
    with database.transaction() as connection:
        binding = bindings.bind_in(
            connection,
            cell_id="cell-demo",
            session_id=SESSION_A,
            profile_alias="max-c",
        )

    assert binding.generation == 1
    assert binding.state == "active"
    assert bindings.active_for_cell("cell-demo") == binding


def test_bind_refuses_when_a_binding_is_already_active(
    bindings: WorkerBindings, database: Database
) -> None:
    with database.transaction() as connection:
        bindings.bind_in(
            connection,
            cell_id="cell-demo",
            session_id=SESSION_A,
            profile_alias="max-c",
        )

    with pytest.raises(ClaimRefused):
        bindings.bind(
            cell_id="cell-demo", session_id=SESSION_B, profile_alias="max-d"
        )


def test_swap_in_changes_only_the_binding_generation(
    claims: WorkClaims,
    bindings: WorkerBindings,
    database: Database,
) -> None:
    claim = open_claim(claims, database)
    with database.transaction() as connection:
        bindings.bind_in(
            connection,
            cell_id="cell-demo",
            session_id=SESSION_A,
            profile_alias="max-c",
        )
    with database.transaction() as connection:
        database.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at, lane_role) VALUES "
            "('cell-demo', 'demo', 'active', 'max-c', ?, ?, ?, "
            "'development')",
            (SESSION_A, NOW.isoformat(), NOW.isoformat()),
        )
    cell_row_before = dict(
        database.execute(
            "SELECT * FROM project_cells WHERE cell_id = 'cell-demo'"
        ).fetchone()
    )

    with database.transaction() as connection:
        swapped = bindings.swap_in(
            connection,
            cell_id="cell-demo",
            expected_session_id=SESSION_A,
            session_id=SESSION_B,
            profile_alias="max-d",
            reason="rotation",
        )

    assert swapped.generation == 2
    assert swapped.session_id == SESSION_B
    history = bindings.history("cell-demo")
    assert [binding.state for binding in history] == ["retired", "active"]
    assert [binding.generation for binding in history] == [1, 2]

    cell_row_after = dict(
        database.execute(
            "SELECT * FROM project_cells WHERE cell_id = 'cell-demo'"
        ).fetchone()
    )
    assert cell_row_after == cell_row_before
    assert claims.get(claim.claim_id) == claim


def test_swap_in_refuses_a_stale_expected_session_id(
    bindings: WorkerBindings, database: Database
) -> None:
    with database.transaction() as connection:
        original = bindings.bind_in(
            connection,
            cell_id="cell-demo",
            session_id=SESSION_A,
            profile_alias="max-c",
        )

    with pytest.raises(ClaimRefused):
        bindings.swap(
            cell_id="cell-demo",
            expected_session_id=SESSION_B,
            session_id="stale-successor",
            profile_alias="max-d",
            reason="rotation",
        )

    assert bindings.active_for_cell("cell-demo") == original
    assert bindings.history("cell-demo") == [original]


# --- migration backfill --------------------------------------------------


def test_migration_0062_backfills_claims_and_bindings(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "state.db"
    migrations = Path(hermes_orchestrator.db.__file__).with_name("migrations")
    connection = sqlite3.connect(database_path)
    stamp = "2026-09-01T00:00:00+00:00"
    try:
        for migration_path in sorted(
            migrations.glob("[0-9][0-9][0-9][0-9]_*.sql")
        ):
            version = int(migration_path.name.split("_", maxsplit=1)[0])
            if version > 60:
                continue
            connection.executescript(migration_path.read_text(encoding="utf-8"))

        # A live development cell and a live harness cell, each with a
        # current worker identity to carry forward as generation 1.
        connection.execute(
            "INSERT INTO project_cells(cell_id, project_key, state, "
            "profile_alias, session_id, created_at, updated_at, lane_role) "
            "VALUES ('dev-cell', 'demo', 'active', 'max-c', ?, ?, ?, "
            "'development')",
            (SESSION_A, stamp, stamp),
        )
        connection.execute(
            "INSERT INTO project_cells(cell_id, project_key, state, "
            "profile_alias, session_id, created_at, updated_at, lane_role) "
            "VALUES ('harness-cell', 'demo', 'active', 'max-d', ?, ?, ?, "
            "'harness')",
            (SESSION_B, stamp, stamp),
        )
        # A terminal cell should NOT get a backfilled binding.
        connection.execute(
            "INSERT INTO project_cells(cell_id, project_key, state, "
            "profile_alias, session_id, created_at, updated_at, lane_role) "
            "VALUES ('retired-cell', 'demo', 'archived', 'max-e', "
            "'session-old', ?, ?, 'development')",
            (stamp, stamp),
        )

        # A non-terminal issue with a live, non-superseded assignment on
        # the development cell -- should backfill an active claim.
        connection.execute(
            "INSERT INTO admitted_issues(issue_id, project_key, priority, "
            "state, instruction_id, dependency_ready, overlap_risk, "
            "admitted_at, updated_at) VALUES ('INFRA-1', 'demo', 1, "
            "'in_development', 'chat-1', 1, 0, ?, ?)",
            (stamp, stamp),
        )
        connection.execute(
            "INSERT INTO lead_assignments(assignment_id, schema_version, "
            "project_key, issue_id, cell_id, session_id, profile_alias, "
            "instruction_id, queue_transition, state, created_at, "
            "updated_at, acknowledged_at) VALUES ('assign-live', 1, "
            "'demo', 'INFRA-1', 'dev-cell', ?, 'max-c', 'chat-1', "
            "'queued->in_development', 'acknowledged', ?, ?, ?)",
            (SESSION_A, stamp, stamp, stamp),
        )
        # A stale, superseded assignment for the same issue/cell -- must
        # NOT backfill a second claim (or the partial unique index would
        # refuse it).
        connection.execute(
            "INSERT INTO lead_assignments(assignment_id, schema_version, "
            "project_key, issue_id, cell_id, session_id, profile_alias, "
            "instruction_id, queue_transition, state, created_at, "
            "updated_at, acknowledged_at) VALUES ('assign-stale', 1, "
            "'demo', 'INFRA-1', 'dev-cell', 'session-stale', 'max-c', "
            "'chat-0', 'queued->in_development', 'superseded', ?, ?, "
            "NULL)",
            (stamp, stamp),
        )

        # A completed (terminal) issue's assignment must NOT backfill a
        # claim.
        connection.execute(
            "INSERT INTO admitted_issues(issue_id, project_key, priority, "
            "state, instruction_id, dependency_ready, overlap_risk, "
            "admitted_at, updated_at) VALUES ('INFRA-2', 'demo', 1, "
            "'done', 'chat-2', 1, 0, ?, ?)",
            (stamp, stamp),
        )
        connection.execute(
            "INSERT INTO lead_assignments(assignment_id, schema_version, "
            "project_key, issue_id, cell_id, session_id, profile_alias, "
            "instruction_id, queue_transition, state, created_at, "
            "updated_at, acknowledged_at) VALUES ('assign-done', 1, "
            "'demo', 'INFRA-2', 'dev-cell', ?, 'max-c', 'chat-2', "
            "'queued->in_development', 'acknowledged', ?, ?, ?)",
            (SESSION_A, stamp, stamp, stamp),
        )

        # A harness-role assignment for a third, non-terminal issue on
        # the harness cell.
        connection.execute(
            "INSERT INTO admitted_issues(issue_id, project_key, priority, "
            "state, instruction_id, dependency_ready, overlap_risk, "
            "admitted_at, updated_at) VALUES ('INFRA-3', 'demo', 1, "
            "'in_development', 'chat-3', 1, 0, ?, ?)",
            (stamp, stamp),
        )
        connection.execute(
            "INSERT INTO lead_assignments(assignment_id, schema_version, "
            "project_key, issue_id, cell_id, session_id, profile_alias, "
            "instruction_id, queue_transition, state, created_at, "
            "updated_at, acknowledged_at) VALUES ('assign-harness', 1, "
            "'demo', 'INFRA-3', 'harness-cell', ?, 'max-d', 'chat-3', "
            "'queued->in_development', 'published', ?, ?, NULL)",
            (SESSION_B, stamp, stamp),
        )
        connection.commit()
    finally:
        connection.close()

    database = Database.open(database_path)
    try:
        # Other migrations may exist ahead of 0062 in this worktree (a
        # sibling INFRA-222 packet); this test only pins that 0062 itself
        # applied and produced the expected backfill.
        assert database.schema_version() >= 62
        assert (
            database.scalar(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 62"
            )
            == 1
        )

        binding_rows = database.execute(
            "SELECT cell_id, generation, session_id, profile_alias, state "
            "FROM worker_bindings ORDER BY cell_id"
        ).fetchall()
        assert [tuple(row) for row in binding_rows] == [
            ("dev-cell", 1, SESSION_A, "max-c", "active"),
            ("harness-cell", 1, SESSION_B, "max-d", "active"),
        ]
        # Duplicate-safe: the partial unique index on cell_id WHERE
        # state='active' held for both rows above.
        active_binding_count = database.scalar(
            "SELECT COUNT(*) FROM worker_bindings WHERE state = 'active'"
        )
        assert active_binding_count == 2

        claim_rows = database.execute(
            "SELECT issue_id, cell_id, role, state FROM work_claims "
            "ORDER BY issue_id"
        ).fetchall()
        assert [tuple(row) for row in claim_rows] == [
            ("INFRA-1", "dev-cell", "development", "active"),
            ("INFRA-3", "harness-cell", "harness", "active"),
        ]
        active_claim_count = database.scalar(
            "SELECT COUNT(*) FROM work_claims WHERE state = 'active'"
        )
        assert active_claim_count == 2
    finally:
        database.close()


def test_worker_binding_dataclass_shape() -> None:
    binding = WorkerBinding(
        binding_id="wb-1",
        cell_id="cell-demo",
        generation=1,
        session_id=SESSION_A,
        profile_alias="max-c",
        cmux_surface_uuid=None,
        state="active",
        bound_at=NOW.isoformat(),
        retired_at=None,
        retired_reason=None,
    )
    assert binding.as_dict()["generation"] == 1
