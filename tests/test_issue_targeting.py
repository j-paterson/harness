"""Tests for the INFRA-220 ``target_issue`` / ``acknowledge_target``
transition (Sol corrections 7ecf4a57, 9944530c and 25689ebd)."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.cells import (
    DEVELOPMENT_LANE,
    HARNESS_LANE,
    MAX_DEVELOPMENT_ISSUE_LANES,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.issue_targeting import (
    IssueTargetingRefused,
    acknowledge_target,
    target_harness_followup,
    target_issue,
)
from hermes_orchestrator.lead_assignments import LeadAssignments

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)
CELL_ID = "cell-demo"
SESSION_ID = "11111111-2222-4333-8444-555555555555"
OTHER_SESSION_ID = "66666666-7777-4888-9999-aaaaaaaaaaaa"


class _RecordingLinear:
    """Minimal ``LinearProjector`` test double: never blocks activation."""

    def __init__(self) -> None:
        self.projected: list[str] = []
        self.effect_ids: list[str] = []

    async def project(self, issue_id: str, target: object, effect_id: str) -> object:
        self.projected.append(issue_id)
        self.effect_ids.append(effect_id)
        return object()


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def events(database: Database) -> EventStore:
    return EventStore(database)


@pytest.fixture
def assignments(database: Database, events: EventStore) -> LeadAssignments:
    return LeadAssignments(database, events=events, now=lambda: NOW)


def _seed_admitted(
    database: Database,
    *,
    issue_id: str,
    project_key: str = "demo",
    priority: int = 2,
    state: str = "queued",
    admitted_at: str = "2026-08-01T00:00:00+00:00",
    dependency_ready: bool = True,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO admitted_issues("
            "issue_id, project_key, priority, state, instruction_id, "
            "dependency_ready, overlap_risk, admitted_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)",
            (
                issue_id,
                project_key,
                priority,
                state,
                f"instr-{issue_id}",
                1 if dependency_ready else 0,
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
    lane_role: str = DEVELOPMENT_LANE,
    lease: bool = True,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "lane_role, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                cell_id,
                project_key,
                state,
                profile_alias,
                session_id,
                lane_role,
                "2026-08-01T00:00:00+00:00",
                "2026-08-01T00:00:00+00:00",
            ),
        )
        if lease:
            connection.execute(
                "INSERT INTO profile_leases("
                "profile_alias, project_key, state, lane_role, acquired_at) "
                "VALUES (?, ?, 'active', ?, ?)",
                (
                    profile_alias,
                    project_key,
                    lane_role,
                    "2026-08-01T00:00:00+00:00",
                ),
            )


def _rotate_cell(
    database: Database,
    *,
    cell_id: str = CELL_ID,
    session_id: str | None = None,
    profile_alias: str | None = None,
    lane_role: str | None = None,
    project_key: str | None = None,
) -> None:
    """Rotate the named cell's durable identity in place.

    Models the cell moving on after a packet named it: a new Claude
    session, a different profile, another lane, another project. The
    packet's own row is left exactly as published.
    """

    columns: list[str] = []
    values: list[object] = []
    for column, value in (
        ("session_id", session_id),
        ("profile_alias", profile_alias),
        ("lane_role", lane_role),
        ("project_key", project_key),
    ):
        if value is not None:
            columns.append(f"{column} = ?")
            values.append(value)
    with database.transaction() as connection:
        connection.execute(
            f"UPDATE project_cells SET {', '.join(columns)} WHERE cell_id = ?",
            (*values, cell_id),
        )


def _set_lease_state(
    database: Database, *, profile_alias: str = "max-c", state: str
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "UPDATE profile_leases SET state = ? WHERE profile_alias = ?",
            (state, profile_alias),
        )


def _assignment_row(database: Database, assignment_id: str) -> tuple[str, object]:
    row = database.execute(
        "SELECT state, acknowledged_at FROM lead_assignments "
        "WHERE assignment_id = ?",
        (assignment_id,),
    ).fetchone()
    return str(row["state"]), row["acknowledged_at"]


def _issue_state(database: Database, issue_id: str) -> str:
    row = database.execute(
        "SELECT state FROM admitted_issues WHERE issue_id = ?", (issue_id,)
    ).fetchone()
    return str(row["state"])


def _event_count(database: Database, event_type: str, aggregate_id: str) -> int:
    row = database.execute(
        "SELECT COUNT(*) AS n FROM events "
        "WHERE event_type = ? AND aggregate_id = ?",
        (event_type, aggregate_id),
    ).fetchone()
    return int(row["n"])


def _cell_touch(database: Database, cell_id: str = CELL_ID) -> tuple[str, str]:
    row = database.execute(
        "SELECT state, updated_at FROM project_cells WHERE cell_id = ?",
        (cell_id,),
    ).fetchone()
    return str(row["state"]), str(row["updated_at"])


def _admitted_snapshot(database: Database) -> list[tuple[object, ...]]:
    rows = database.execute(
        "SELECT issue_id, state, priority, admitted_at FROM admitted_issues "
        "ORDER BY issue_id"
    ).fetchall()
    return [tuple(row) for row in rows]


def _assignment_count(database: Database) -> int:
    row = database.execute("SELECT COUNT(*) AS n FROM lead_assignments").fetchone()
    return int(row["n"])


def _project_cells_count(database: Database) -> int:
    row = database.execute("SELECT COUNT(*) AS n FROM project_cells").fetchone()
    return int(row["n"])


def _seed_pending_decision(
    database: Database,
    *,
    issue_id: str,
    project_key: str = "demo",
    cell_id: str = CELL_ID,
    session_id: str = SESSION_ID,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO operator_decisions("
            "decision_id, issue_id, project_key, cell_id, session_id, "
            "actor, choice, status, recorded_at) "
            "VALUES (?, ?, ?, ?, ?, 'operator', 'hold', 'pending', ?)",
            (
                f"decision-{issue_id}",
                issue_id,
                project_key,
                cell_id,
                session_id,
                "2026-08-01T00:00:00+00:00",
            ),
        )


def _effect_count(database: Database, issue_id: str) -> int:
    """Durable ``In Development`` projection effect rows for the issue."""

    row = database.execute(
        "SELECT COUNT(*) AS n FROM external_effects WHERE target = ?",
        (issue_id,),
    ).fetchone()
    return int(row["n"])


def _consumed_count(database: Database) -> int:
    row = database.execute(
        "SELECT COUNT(*) AS n FROM lead_assignments WHERE state = 'acknowledged'"
    ).fetchone()
    return int(row["n"])


def test_happy_path_publishes_only_the_targeted_issue(
    database: Database, assignments: LeadAssignments,
    events: EventStore
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
        events=events,
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
    database: Database, assignments: LeadAssignments,
    events: EventStore
) -> None:
    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)

    first = target_issue(
        database,
        assignments=assignments,
        events=events,
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
        events=events,
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
    database: Database, assignments: LeadAssignments,
    events: EventStore
) -> None:
    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    before = _admitted_snapshot(database)

    with pytest.raises(IssueTargetingRefused):
        target_issue(
            database,
            assignments=assignments,
            events=events,
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
    database: Database, assignments: LeadAssignments,
    events: EventStore
) -> None:
    _seed_cell(database)
    before = _admitted_snapshot(database)

    with pytest.raises(IssueTargetingRefused):
        target_issue(
            database,
            assignments=assignments,
            events=events,
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
    database: Database, assignments: LeadAssignments,
    events: EventStore
) -> None:
    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)

    with pytest.raises(IssueTargetingRefused):
        target_issue(
            database,
            assignments=assignments,
            events=events,
            issue_id="INFRA-9",
            project_key="demo",
            cell_id=CELL_ID,
            session_id=SESSION_ID,
            instruction="   ",
            known_projects={"demo"},
        )

    assert _assignment_count(database) == 0


# ---------------------------------------------------------------------------
# Sol correction 9944530c packet 1 -- exactly-once confirmation/activation.
# ---------------------------------------------------------------------------


def _publish(
    database: Database,
    assignments: LeadAssignments,
    events: EventStore,
    *,
    issue_id: str = "INFRA-9",
) -> str:
    result = target_issue(
        database,
        assignments=assignments,
        events=events,
        issue_id=issue_id,
        project_key="demo",
        cell_id=CELL_ID,
        session_id=SESSION_ID,
        instruction=f"focus on {issue_id}",
        known_projects={"demo"},
    )
    return result.assignment.assignment_id


@pytest.mark.asyncio
async def test_activation_failure_after_exact_confirmation_stays_retryable(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Packet 1: an activation that refuses must NOT have burned the
    confirmation -- the packet stays published, pending, and retryable."""

    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    assignment_id = _publish(database, assignments, events)
    # The cell's profile lease drops between publication and the ACK, so
    # the canonical activation transaction refuses with zero writes.
    _set_lease_state(database, state="released")

    result = await acknowledge_target(
        database,
        events=events,
        linear=_RecordingLinear(),
        assignments=assignments,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )

    assert result.activated is False
    assert _assignment_row(database, assignment_id) == ("published", None)
    assert _issue_state(database, "INFRA-9") == "queued"
    assert _event_count(database, "issue.started", "INFRA-9") == 0
    pending = assignments.pending_for_session(SESSION_ID)
    assert [row.assignment_id for row in pending] == [assignment_id]


@pytest.mark.asyncio
async def test_retry_after_recovery_activates_exactly_once(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Packet 1: once the transient failure clears, the SAME retryable
    confirmation activates -- once, and is only then consumed."""

    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    assignment_id = _publish(database, assignments, events)
    _set_lease_state(database, state="released")
    linear = _RecordingLinear()

    first = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )
    _set_lease_state(database, state="active")
    second = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )

    assert first.activated is False
    assert second.activated is True
    assert second.issue_id == "INFRA-9"
    assert _issue_state(database, "INFRA-9") == "in_development"
    assert _event_count(database, "issue.started", "INFRA-9") == 1
    state, acknowledged_at = _assignment_row(database, assignment_id)
    assert state == "acknowledged"
    assert acknowledged_at is not None
    assert _assignment_count(database) == 1
    assert assignments.pending_for_session(SESSION_ID) == ()


@pytest.mark.asyncio
async def test_duplicate_confirmation_after_success_has_no_second_effect(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Packet 1: the consumed confirmation can never activate twice --
    the duplicate refuses without even entering activation."""

    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    assignment_id = _publish(database, assignments, events)
    linear = _RecordingLinear()

    first = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )
    after_success = (
        _admitted_snapshot(database),
        _cell_touch(database),
        _assignment_row(database, assignment_id),
    )
    duplicate = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )

    assert first.activated is True
    assert duplicate.activated is False
    assert _event_count(database, "issue.started", "INFRA-9") == 1
    assert _assignment_count(database) == 1
    assert linear.projected == ["INFRA-9"]
    assert (
        _admitted_snapshot(database),
        _cell_touch(database),
        _assignment_row(database, assignment_id),
    ) == after_success


@pytest.mark.asyncio
async def test_requeued_issue_gets_a_new_linear_projection_effect(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """A completed prior run must not suppress the next run's Linear state.

    INFRA-192 was started once in August, returned to Todo, and started
    again in September.  The old per-issue effect id made the second
    projection look completed before it ran.  Each assignment is the
    durable identity of one activation, so the two runs need two effect
    ids while retries of either run remain idempotent.
    """

    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    linear = _RecordingLinear()

    first_assignment = _publish(database, assignments, events)
    first = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id=first_assignment,
        session_id=SESSION_ID,
    )
    with database.transaction() as connection:
        connection.execute(
            "UPDATE admitted_issues SET state = 'queued' WHERE issue_id = ?",
            ("INFRA-9",),
        )
    second_assignment = _publish(database, assignments, events)
    second = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id=second_assignment,
        session_id=SESSION_ID,
    )

    assert first.activated is True
    assert second.activated is True
    assert first_assignment != second_assignment
    assert linear.effect_ids == [
        f"linear:INFRA-9:in-development:{first_assignment}",
        f"linear:INFRA-9:in-development:{second_assignment}",
    ]
    assert database.scalar(
        "SELECT COUNT(*) FROM external_effects WHERE target = 'INFRA-9'"
    ) == 2


@pytest.mark.asyncio
async def test_exact_confirmation_activates_once(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Packet 4: the happy path itself. One exact, session-bound
    confirmation advances the issue and journals its projection exactly
    once -- the positive control the failure-mode tests are measured
    against."""

    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    assignment_id = _publish(database, assignments, events)
    linear = _RecordingLinear()

    result = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )

    assert result.activated is True
    assert _admitted_snapshot(database)[0][1] == "in_development"
    assert _event_count(database, "issue.started", "INFRA-9") == 1
    assert linear.projected == ["INFRA-9"]
    assert _assignment_row(database, assignment_id)[0] == "acknowledged"


@pytest.mark.asyncio
async def test_wrong_confirmation_changes_nothing(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Packet 4: a confirmation that is not the exact bound one leaves
    the world byte-identical -- no activation, no projection, no
    consumed packet -- so the real confirmation stays retryable."""

    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    assignment_id = _publish(database, assignments, events)
    linear = _RecordingLinear()
    before = (
        _admitted_snapshot(database),
        _cell_touch(database),
        _assignment_row(database, assignment_id),
    )

    wrong_session = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id=assignment_id,
        session_id=OTHER_SESSION_ID,
    )
    unknown_packet = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id="assignment-that-does-not-exist",
        session_id=SESSION_ID,
    )

    assert wrong_session.activated is False
    assert unknown_packet.activated is False
    assert linear.projected == []
    assert _event_count(database, "issue.started", "INFRA-9") == 0
    assert (
        _admitted_snapshot(database),
        _cell_touch(database),
        _assignment_row(database, assignment_id),
    ) == before

    # The exact confirmation still works afterwards: nothing was consumed.
    recovered = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )
    assert recovered.activated is True


# ---------------------------------------------------------------------------
# Sol correction 9944530c packet 2 -- the canonical development-lane bound.
# ---------------------------------------------------------------------------


def _seed_occupying(database: Database, count: int) -> None:
    """``count`` OTHER issues already occupying the project's
    development lane, alternating the two occupying states."""

    for index in range(count):
        _seed_admitted(
            database,
            issue_id=f"INFRA-OCC-{index}",
            state="review" if index % 2 else "in_development",
        )


def test_safe_lanes_below_the_canonical_limit_permit_targeting(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Packet 2: the canonical model allows up to
    MAX_DEVELOPMENT_ISSUE_LANES concurrent issue lanes, so existing
    active issues below that bound must NOT refuse a new target."""

    _seed_occupying(database, MAX_DEVELOPMENT_ISSUE_LANES - 1)
    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)

    result = target_issue(
        database,
        assignments=assignments,
        events=events,
        issue_id="INFRA-9",
        project_key="demo",
        cell_id=CELL_ID,
        session_id=SESSION_ID,
        instruction="focus on INFRA-9",
        known_projects={"demo"},
    )

    assert result.idempotent is False
    assert result.assignment.issue_id == "INFRA-9"
    assert _assignment_count(database) == 1


def test_canonical_lane_limit_refuses_an_additional_target_without_writes(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Packet 2: at the canonical bound the target is refused, and the
    refusal writes nothing and reorders nothing in the queue."""

    _seed_occupying(database, MAX_DEVELOPMENT_ISSUE_LANES)
    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    before = _admitted_snapshot(database)

    with pytest.raises(IssueTargetingRefused):
        target_issue(
            database,
            assignments=assignments,
            events=events,
            issue_id="INFRA-9",
            project_key="demo",
            cell_id=CELL_ID,
            session_id=SESSION_ID,
            instruction="focus on INFRA-9",
            known_projects={"demo"},
        )

    assert _assignment_count(database) == 0
    assert _admitted_snapshot(database) == before


# ---------------------------------------------------------------------------
# Sol correction 9944530c packet 3 -- exact named development cell.
# ---------------------------------------------------------------------------


def test_named_development_cell_is_selected_regardless_of_row_order(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Packet 3: the named development cell is found by an exact
    cell_id + lane_role + active-state query, not by whichever project
    row the database happens to return first."""

    _seed_admitted(database, issue_id="INFRA-9")
    # Both of these are inserted BEFORE the named development cell, so
    # an unordered "any active cell for this project" select returns one
    # of them first: a terminal harness-run row, then the live HARNESS
    # lane cell that legitimately coexists with the development lane.
    _seed_cell(
        database,
        cell_id="cell-failed",
        session_id=OTHER_SESSION_ID,
        profile_alias="max-failed",
        state="failed",
        lease=False,
    )
    _seed_cell(
        database,
        cell_id="cell-harness",
        session_id=OTHER_SESSION_ID,
        profile_alias="max-harness",
        state="active",
        lane_role=HARNESS_LANE,
    )
    _seed_cell(database)

    result = target_issue(
        database,
        assignments=assignments,
        events=events,
        issue_id="INFRA-9",
        project_key="demo",
        cell_id=CELL_ID,
        session_id=SESSION_ID,
        instruction="focus on INFRA-9",
        known_projects={"demo"},
    )

    assert result.assignment.cell_id == CELL_ID
    assert result.assignment.profile_alias == "max-c"
    assert result.assignment.session_id == SESSION_ID


def test_harness_cell_is_never_an_eligible_target(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Packet 3: naming the harness lane's own cell must refuse -- the
    lane_role predicate is in the query, not a post-hoc comparison."""

    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(
        database,
        cell_id="cell-harness",
        session_id=OTHER_SESSION_ID,
        profile_alias="max-harness",
        state="active",
        lane_role=HARNESS_LANE,
    )

    with pytest.raises(IssueTargetingRefused):
        target_issue(
            database,
            assignments=assignments,
            events=events,
            issue_id="INFRA-9",
            project_key="demo",
            cell_id="cell-harness",
            session_id=OTHER_SESSION_ID,
            instruction="focus on INFRA-9",
            known_projects={"demo"},
        )

    assert _assignment_count(database) == 0


# ---------------------------------------------------------------------------
# Sol correction 25689ebd packet 1 -- the activation transaction re-proves
# the CELL's own current identity, not merely the assignment row.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rotated_cell_identity_refuses_activation_and_stays_retryable(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Packet 1: the cell rotates to another session on another profile
    between publication and the ACK. The stale session can neither
    activate nor consume its packet -- and the packet stays retryable.

    The profile lease the packet named is deliberately left ACTIVE, so
    the pre-existing lease predicate cannot catch this: only re-proving
    the cell's CURRENT ``session_id``/``profile_alias`` can."""

    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    assignment_id = _publish(database, assignments, events)
    linear = _RecordingLinear()
    _rotate_cell(
        database, session_id=OTHER_SESSION_ID, profile_alias="max-rotated"
    )
    before = _admitted_snapshot(database)

    stale = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )

    assert stale.activated is False
    assert _admitted_snapshot(database) == before
    assert _issue_state(database, "INFRA-9") == "queued"
    assert _event_count(database, "issue.started", "INFRA-9") == 0
    assert _effect_count(database, "INFRA-9") == 0
    assert linear.projected == []
    # Nor was the confirmation consumed: it is still published, still
    # pending for its session, and no ACK was ever journaled.
    assert _assignment_row(database, assignment_id) == ("published", None)
    assert _event_count(database, "assignment.acknowledged", assignment_id) == 0
    pending = assignments.pending_for_session(SESSION_ID)
    assert [row.assignment_id for row in pending] == [assignment_id]

    # The cell's NEW session cannot consume the stale session's packet
    # either -- the packet is bound to the session that was named.
    rotated = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id=assignment_id,
        session_id=OTHER_SESSION_ID,
    )

    assert rotated.activated is False
    assert _assignment_row(database, assignment_id) == ("published", None)
    assert _issue_state(database, "INFRA-9") == "queued"
    assert _event_count(database, "issue.started", "INFRA-9") == 0


@pytest.mark.asyncio
async def test_cell_rotated_out_of_the_development_lane_refuses_activation(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Packet 1: the development lane is part of the cell's identity.
    A cell moved to the harness lane after publication is no longer the
    cell the packet named, and its packet activates nothing."""

    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    assignment_id = _publish(database, assignments, events)
    linear = _RecordingLinear()
    _rotate_cell(database, lane_role=HARNESS_LANE)
    before = _admitted_snapshot(database)

    result = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )

    assert result.activated is False
    assert _admitted_snapshot(database) == before
    assert _event_count(database, "issue.started", "INFRA-9") == 0
    assert _effect_count(database, "INFRA-9") == 0
    assert linear.projected == []
    assert _assignment_row(database, assignment_id) == ("published", None)


@pytest.mark.asyncio
async def test_cell_rotated_to_another_project_refuses_activation(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Packet 1: so is the project. A cell that now belongs to a
    different project is not the cell the packet named."""

    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    assignment_id = _publish(database, assignments, events)
    linear = _RecordingLinear()
    _rotate_cell(database, project_key="other")
    before = _admitted_snapshot(database)

    result = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=assignments,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )

    assert result.activated is False
    assert _admitted_snapshot(database) == before
    assert _event_count(database, "issue.started", "INFRA-9") == 0
    assert _assignment_row(database, assignment_id) == ("published", None)


# ---------------------------------------------------------------------------
# Sol correction 25689ebd packet 2 -- the confirmation is consumed INSIDE
# the canonical activation transaction: both commit, or neither does.
# ---------------------------------------------------------------------------


class _Crash(RuntimeError):
    """A process death at the exact ACK/activation boundary."""


class _SupersedingAssignments(LeadAssignments):
    """Models a supersession landing in the window between activation's
    eligibility checks and the ACK compare-and-swap.

    It supersedes the packet immediately before whichever consume
    primitive the production code uses, so the CAS that follows finds
    no ``published`` row and fails."""

    def __init__(self, database: Database, *, events: EventStore) -> None:
        super().__init__(database, events=events, now=lambda: NOW)
        self.db = database

    @staticmethod
    def _supersede(connection: object, assignment_id: str) -> None:
        connection.execute(  # type: ignore[attr-defined]
            "UPDATE lead_assignments SET state = 'superseded' "
            "WHERE assignment_id = ?",
            (assignment_id,),
        )

    def acknowledge_in(
        self, connection: object, assignment_id: str, *, session_id: str
    ) -> bool:
        self._supersede(connection, assignment_id)
        return super().acknowledge_in(
            connection, assignment_id, session_id=session_id
        )

    def acknowledge(self, assignment_id: str, *, session_id: str) -> bool:
        # The same race for any ordering that consumes in a SECOND
        # transaction after activation has already committed.
        with self.db.transaction() as connection:
            self._supersede(connection, assignment_id)
        return super().acknowledge(assignment_id, session_id=session_id)


class _ObservingAssignments(LeadAssignments):
    """Records, at the moment the confirmation is consumed, both what
    the consuming connection can see and what a SEPARATE connection --
    i.e. the committed database -- can see.

    That pair is the atomicity proof. If the consume runs inside the
    activation transaction, the consuming connection already sees the
    staged transition while the outside observer still sees ``queued``,
    because nothing has committed yet. If the consume runs in a second
    transaction after activation committed, the outside observer sees
    ``in_development`` -- the very window a supersession can exploit."""

    def __init__(self, database: Database, *, events: EventStore) -> None:
        super().__init__(database, events=events, now=lambda: NOW)
        self.db = database
        self.observed: list[tuple[str, str]] = []

    def _outside_view(self, issue_id: str) -> str:
        """The committed issue state, read on its own connection."""

        observer = sqlite3.connect(self.db.path, timeout=1.0)
        try:
            row = observer.execute(
                "SELECT state FROM admitted_issues WHERE issue_id = ?",
                (issue_id,),
            ).fetchone()
        finally:
            observer.close()
        return str(row[0])

    def acknowledge_in(
        self, connection: object, assignment_id: str, *, session_id: str
    ) -> bool:
        row = connection.execute(  # type: ignore[attr-defined]
            "SELECT packet.issue_id AS issue_id, issues.state AS state "
            "FROM lead_assignments packet "
            "JOIN admitted_issues issues ON issues.issue_id = packet.issue_id "
            "WHERE packet.assignment_id = ?",
            (assignment_id,),
        ).fetchone()
        self.observed.append(
            (str(row["state"]), self._outside_view(str(row["issue_id"])))
        )
        return super().acknowledge_in(
            connection, assignment_id, session_id=session_id
        )


class _CrashingAssignments(LeadAssignments):
    """Dies at the ACK boundary while ``armed``, having consumed."""

    def __init__(self, database: Database, *, events: EventStore) -> None:
        super().__init__(database, events=events, now=lambda: NOW)
        self.armed = True

    def acknowledge_in(
        self, connection: object, assignment_id: str, *, session_id: str
    ) -> bool:
        consumed = super().acknowledge_in(
            connection, assignment_id, session_id=session_id
        )
        if self.armed:
            raise _Crash("process died at the ACK boundary")
        return consumed

    def acknowledge(self, assignment_id: str, *, session_id: str) -> bool:
        consumed = super().acknowledge(assignment_id, session_id=session_id)
        if self.armed:
            raise _Crash("process died at the ACK boundary")
        return consumed


@pytest.mark.asyncio
async def test_supersession_racing_the_ack_commits_neither_half(
    database: Database, events: EventStore
) -> None:
    """Packet 2: a supersession that lands after every eligibility check
    but before the ACK CAS must leave NEITHER half durable -- not an
    activated issue whose confirmation was never consumed."""

    racing = _SupersedingAssignments(database, events=events)
    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    assignment_id = _publish(database, racing, events)
    linear = _RecordingLinear()
    before = _admitted_snapshot(database)

    result = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=racing,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )

    assert result.activated is False
    # Neither the activation...
    assert _admitted_snapshot(database) == before
    assert _issue_state(database, "INFRA-9") == "queued"
    assert _event_count(database, "issue.started", "INFRA-9") == 0
    assert _effect_count(database, "INFRA-9") == 0
    assert linear.projected == []
    # ...nor the consume -- and the racing supersession rolled back with
    # them, so the exact packet is still published and still retryable.
    assert _assignment_row(database, assignment_id) == ("published", None)
    assert _consumed_count(database) == 0
    assert _event_count(database, "assignment.acknowledged", assignment_id) == 0


@pytest.mark.asyncio
async def test_confirmation_is_consumed_inside_the_activation_transaction(
    database: Database, events: EventStore
) -> None:
    """Packet 2, the positive half of the same guarantee: when the ACK
    CAS runs, the issue transition is already staged on that very
    connection and NOT yet visible to any other connection -- consume
    and activation are one commit, not two."""

    observing = _ObservingAssignments(database, events=events)
    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    assignment_id = _publish(database, observing, events)
    linear = _RecordingLinear()

    result = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=observing,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )

    assert result.activated is True
    assert observing.observed == [("in_development", "queued")]
    assert _issue_state(database, "INFRA-9") == "in_development"
    assert _assignment_row(database, assignment_id)[0] == "acknowledged"
    assert _event_count(database, "issue.started", "INFRA-9") == 1
    assert _event_count(database, "assignment.acknowledged", assignment_id) == 1


@pytest.mark.asyncio
async def test_retry_across_the_crash_boundary_activates_exactly_once(
    database: Database, events: EventStore
) -> None:
    """Packet 2: a crash at the ACK boundary leaves NOTHING durable, and
    the retry produces exactly one ``issue.started``, one projection
    effect, and one consumed assignment."""

    crashing = _CrashingAssignments(database, events=events)
    _seed_admitted(database, issue_id="INFRA-9")
    _seed_cell(database)
    assignment_id = _publish(database, crashing, events)
    linear = _RecordingLinear()
    before = _admitted_snapshot(database)

    with pytest.raises(_Crash):
        await acknowledge_target(
            database,
            events=events,
            linear=linear,
            assignments=crashing,
            assignment_id=assignment_id,
            session_id=SESSION_ID,
        )

    # The crash took the whole transaction with it: no half-transition.
    assert _admitted_snapshot(database) == before
    assert _issue_state(database, "INFRA-9") == "queued"
    assert _event_count(database, "issue.started", "INFRA-9") == 0
    assert _effect_count(database, "INFRA-9") == 0
    assert _assignment_row(database, assignment_id) == ("published", None)
    assert _consumed_count(database) == 0

    crashing.armed = False
    retry = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=crashing,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )
    duplicate = await acknowledge_target(
        database,
        events=events,
        linear=linear,
        assignments=crashing,
        assignment_id=assignment_id,
        session_id=SESSION_ID,
    )

    assert retry.activated is True
    assert duplicate.activated is False
    assert _issue_state(database, "INFRA-9") == "in_development"
    assert _event_count(database, "issue.started", "INFRA-9") == 1
    assert _effect_count(database, "INFRA-9") == 1
    assert _consumed_count(database) == 1
    assert _assignment_count(database) == 1
    assert _event_count(database, "assignment.acknowledged", assignment_id) == 1
    assert linear.projected == ["INFRA-9"]


# ---------------------------------------------------------------------------
# INFRA-215 -- a harness-bound lead receiving one admitted, dependency-ready
# project follow-up through a durable, lane-preserving transition.
# ---------------------------------------------------------------------------

HARNESS_CELL_ID = "cell-harness-bound"


def _seed_harness_cell(
    database: Database,
    *,
    cell_id: str = HARNESS_CELL_ID,
    project_key: str = "demo",
    session_id: str = SESSION_ID,
    profile_alias: str = "max-harness",
    state: str = "active",
    lease: bool = True,
) -> None:
    _seed_cell(
        database,
        cell_id=cell_id,
        project_key=project_key,
        session_id=session_id,
        profile_alias=profile_alias,
        state=state,
        lane_role=HARNESS_LANE,
        lease=lease,
    )


def test_harness_lead_receives_one_admitted_followup_lane_preserving(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """A harness cell durably bound to its primary (INFRA-198-shaped)
    assignment can receive exactly one admitted, dependency-ready
    project issue as a follow-up -- without a second seat, without
    touching ``project_cells``, and without disturbing the cell's
    session/profile binding."""

    _seed_harness_cell(database)
    _seed_admitted(database, issue_id="INFRA-205")
    cells_before = _project_cells_count(database)

    result = target_harness_followup(
        database,
        assignments=assignments,
        events=events,
        issue_id="INFRA-205",
        project_key="demo",
        cell_id=HARNESS_CELL_ID,
        session_id=SESSION_ID,
        instruction="pick up INFRA-205 as a harness follow-up",
        known_projects={"demo"},
    )

    assert result.idempotent is False
    assert result.assignment.issue_id == "INFRA-205"
    assert result.assignment.cell_id == HARNESS_CELL_ID
    assert result.assignment.session_id == SESSION_ID
    assert result.assignment.profile_alias == "max-harness"
    assert result.assignment.state == "published"
    assert _assignment_count(database) == 1
    assert _project_cells_count(database) == cells_before
    assert _issue_state(database, "INFRA-205") == "in_development"
    assert _event_count(database, "issue.targeted", "INFRA-205") == 1
    # The harness cell's own binding is untouched -- same session, same
    # profile, still the lane it was.
    row = database.execute(
        "SELECT session_id, profile_alias, lane_role FROM project_cells "
        "WHERE cell_id = ?",
        (HARNESS_CELL_ID,),
    ).fetchone()
    assert str(row["session_id"]) == SESSION_ID
    assert str(row["profile_alias"]) == "max-harness"
    assert str(row["lane_role"]) == HARNESS_LANE


def test_second_followup_refused_while_one_is_live(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Exactly one follow-up at a time: a second explicit target while
    the first is still live (published/acknowledged) refuses closed."""

    _seed_harness_cell(database)
    _seed_admitted(database, issue_id="INFRA-205")
    _seed_admitted(database, issue_id="INFRA-206")

    first = target_harness_followup(
        database,
        assignments=assignments,
        events=events,
        issue_id="INFRA-205",
        project_key="demo",
        cell_id=HARNESS_CELL_ID,
        session_id=SESSION_ID,
        instruction="pick up INFRA-205",
        known_projects={"demo"},
    )
    assert first.assignment.issue_id == "INFRA-205"

    with pytest.raises(IssueTargetingRefused):
        target_harness_followup(
            database,
            assignments=assignments,
            events=events,
            issue_id="INFRA-206",
            project_key="demo",
            cell_id=HARNESS_CELL_ID,
            session_id=SESSION_ID,
            instruction="pick up INFRA-206 too",
            known_projects={"demo"},
        )

    assert _assignment_count(database) == 1
    assert _issue_state(database, "INFRA-206") == "queued"
    assert _issue_state(database, "INFRA-205") == "in_development"


def test_followup_refuses_not_ready_or_cross_project_or_pending_decision(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """Three independent predicate misses, each a fail-closed refusal
    with zero durable writes: dependency not ready, the issue admitted
    for a different project, and a pending operator decision."""

    _seed_harness_cell(database)

    # Not dependency-ready.
    _seed_admitted(database, issue_id="INFRA-300", dependency_ready=False)
    with pytest.raises(IssueTargetingRefused):
        target_harness_followup(
            database,
            assignments=assignments,
            events=events,
            issue_id="INFRA-300",
            project_key="demo",
            cell_id=HARNESS_CELL_ID,
            session_id=SESSION_ID,
            instruction="not ready",
            known_projects={"demo"},
        )
    assert _issue_state(database, "INFRA-300") == "queued"
    assert _assignment_count(database) == 0

    # Admitted for a different project than the harness cell's own.
    _seed_admitted(database, issue_id="INFRA-301", project_key="other")
    with pytest.raises(IssueTargetingRefused):
        target_harness_followup(
            database,
            assignments=assignments,
            events=events,
            issue_id="INFRA-301",
            project_key="demo",
            cell_id=HARNESS_CELL_ID,
            session_id=SESSION_ID,
            instruction="cross project",
            known_projects={"demo", "other"},
        )
    assert _issue_state(database, "INFRA-301") == "queued"
    assert _assignment_count(database) == 0

    # A pending operator decision blocks the follow-up exactly as it
    # blocks development-lane targeting.
    _seed_admitted(database, issue_id="INFRA-302")
    _seed_pending_decision(database, issue_id="INFRA-302")
    with pytest.raises(IssueTargetingRefused):
        target_harness_followup(
            database,
            assignments=assignments,
            events=events,
            issue_id="INFRA-302",
            project_key="demo",
            cell_id=HARNESS_CELL_ID,
            session_id=SESSION_ID,
            instruction="pending decision",
            known_projects={"demo"},
        )
    assert _issue_state(database, "INFRA-302") == "queued"
    assert _assignment_count(database) == 0


def test_followup_refuses_dead_or_development_cell_through_harness_path(
    database: Database, assignments: LeadAssignments, events: EventStore
) -> None:
    """The harness follow-up path requires the exact named cell to be a
    LIVE HARNESS-lane cell -- a development-lane cell of the same id
    (impossible in practice, but the predicate must still be a query
    filter) or a dead harness cell must both refuse."""

    _seed_admitted(database, issue_id="INFRA-205")

    # A development-lane cell is never an eligible harness target, even
    # by the same cell_id/session_id.
    _seed_cell(
        database,
        cell_id=HARNESS_CELL_ID,
        session_id=SESSION_ID,
        profile_alias="max-c",
        state="active",
        lane_role=DEVELOPMENT_LANE,
    )
    with pytest.raises(IssueTargetingRefused):
        target_harness_followup(
            database,
            assignments=assignments,
            events=events,
            issue_id="INFRA-205",
            project_key="demo",
            cell_id=HARNESS_CELL_ID,
            session_id=SESSION_ID,
            instruction="wrong lane",
            known_projects={"demo"},
        )
    assert _assignment_count(database) == 0
    assert _issue_state(database, "INFRA-205") == "queued"

    # A dead (failed) harness cell is not live either.
    with database.transaction() as connection:
        connection.execute(
            "UPDATE project_cells SET lane_role = ?, state = 'failed' "
            "WHERE cell_id = ?",
            (HARNESS_LANE, HARNESS_CELL_ID),
        )
    with pytest.raises(IssueTargetingRefused):
        target_harness_followup(
            database,
            assignments=assignments,
            events=events,
            issue_id="INFRA-205",
            project_key="demo",
            cell_id=HARNESS_CELL_ID,
            session_id=SESSION_ID,
            instruction="dead cell",
            known_projects={"demo"},
        )
    assert _assignment_count(database) == 0
    assert _issue_state(database, "INFRA-205") == "queued"
