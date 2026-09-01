"""Tests for the INFRA-220 ``target_issue`` / ``acknowledge_target``
transition (Sol corrections 7ecf4a57 and 9944530c)."""

from __future__ import annotations

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

    async def project(self, issue_id: str, target: object, effect_id: str) -> object:
        self.projected.append(issue_id)
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
