"""Identity-exact child accounting and reactivation (INFRA-195,
Sol correction 032cd4a5)."""

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
FOREIGN_SESSION = "66666666-7777-4888-9999-aaaaaaaaaaaa"


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


def reactivations(database: Database) -> int:
    value = database.scalar(
        "SELECT COUNT(*) FROM control_operations "
        "WHERE kind = 'children.completed'"
    )
    return int(value)  # type: ignore[arg-type]


def durable_snapshot(database: Database) -> dict[str, list[tuple[object, ...]]]:
    """Every row of every table, so 'nothing durable changed' is
    provable — not just the child row."""

    tables = [
        str(row[0])
        for row in database.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    ]
    return {
        table: sorted(
            (
                tuple(row)
                for row in database.execute(
                    f"SELECT * FROM {table}"
                ).fetchall()
            ),
            key=repr,
        )
        for table in tables
    }


def test_a_duplicated_completion_never_reactivates_early(
    database: Database, tracker: LeadChildTracker
) -> None:
    """Two distinct children; the first child's completion arrives
    twice. No reactivation may fire until the second distinct child
    completes."""

    seed_active_cell(database)
    tracker.child_started(SESSION, "child-a")
    tracker.child_started(SESSION, "child-b")
    assert tracker.record_turn_stop(SESSION) is not None

    assert tracker.child_completed(SESSION, "child-a") is None
    assert tracker.child_completed(SESSION, "child-a") is None
    assert reactivations(database) == 0

    reactivation = tracker.child_completed(SESSION, "child-b")

    assert reactivation is not None
    assert reactivation.result["completed_children"] == 2
    assert reactivations(database) == 1


def test_replayed_hooks_record_exactly_one_start_and_completion(
    database: Database, tracker: LeadChildTracker
) -> None:
    seed_active_cell(database)
    assert tracker.child_started(SESSION, "child-a") is True
    assert tracker.child_started(SESSION, "child-a") is False
    assert (
        database.scalar("SELECT COUNT(*) FROM lead_children") == 1
    )

    assert tracker.child_completed(SESSION, "child-a") is None
    state = database.scalar(
        "SELECT state FROM lead_children WHERE child_id = 'child-a'"
    )
    assert str(state) == "completed"
    # The replayed completion is refused without touching anything.
    assert tracker.child_completed(SESSION, "child-a") is None
    assert (
        database.scalar("SELECT COUNT(*) FROM lead_children") == 1
    )


def test_unmatched_completions_are_refused_without_progress(
    database: Database, tracker: LeadChildTracker
) -> None:
    """Completion before start, a foreign session, and an identity-less
    event each fail closed: outstanding work never shrinks."""

    seed_active_cell(database)
    tracker.child_started(SESSION, "child-a")
    assert tracker.record_turn_stop(SESSION) is not None

    assert tracker.child_completed(SESSION, "never-started") is None
    assert tracker.child_completed(FOREIGN_SESSION, "child-a") is None
    assert tracker.child_completed(SESSION, "") is None
    assert tracker.child_started(SESSION, " ") is False

    outstanding = database.scalar(
        "SELECT COUNT(*) FROM lead_children "
        "WHERE session_id = ? AND state = 'started'",
        (SESSION,),
    )
    assert outstanding == 1
    assert reactivations(database) == 0
    continuation_state = database.scalar(
        "SELECT state FROM lead_continuations WHERE session_id = ?",
        (SESSION,),
    )
    assert str(continuation_state) == "waiting"


def test_retried_boundaries_emit_exactly_one_reactivation(
    database: Database, tracker: LeadChildTracker
) -> None:
    """Crash-and-retry at every hook boundary: repeated starts, stops,
    and completions converge on exactly one children.completed, fired
    only once the distinct outstanding set is empty."""

    seed_active_cell(database)
    tracker.child_started(SESSION, "child-a")
    tracker.child_started(SESSION, "child-a")
    first_stop = tracker.record_turn_stop(SESSION)
    second_stop = tracker.record_turn_stop(SESSION)
    assert first_stop is not None and second_stop is not None
    assert first_stop.continuation_id == second_stop.continuation_id

    reactivation = tracker.child_completed(SESSION, "child-a")
    assert reactivation is not None
    # Retries after the boundary settle as durable no-ops.
    assert tracker.child_completed(SESSION, "child-a") is None
    assert reactivations(database) == 1
    state = database.scalar(
        "SELECT state FROM lead_continuations WHERE continuation_id = ?",
        (first_stop.continuation_id,),
    )
    assert str(state) == "reactivated"


def test_a_stop_with_no_outstanding_children_promises_nothing(
    database: Database, tracker: LeadChildTracker
) -> None:
    seed_active_cell(database)
    tracker.child_started(SESSION, "child-a")
    tracker.child_completed(SESSION, "child-a")

    assert tracker.record_turn_stop(SESSION) is None
    assert database.scalar("SELECT COUNT(*) FROM lead_continuations") == 0


def test_a_stale_promise_is_superseded_once_children_settle(
    database: Database, tracker: LeadChildTracker
) -> None:
    seed_active_cell(database)
    tracker.child_started(SESSION, "child-a")
    continuation = tracker.record_turn_stop(SESSION)
    assert continuation is not None
    with database.transaction() as connection:
        connection.execute(
            "UPDATE lead_children SET state = 'completed' "
            "WHERE session_id = ?",
            (SESSION,),
        )

    assert tracker.record_turn_stop(SESSION) is None
    state = database.scalar(
        "SELECT state FROM lead_continuations WHERE continuation_id = ?",
        (continuation.continuation_id,),
    )
    assert str(state) == "superseded"


def lose_the_binding(database: Database, *, rebind: bool) -> None:
    """Take the active cell away from SESSION the two ways it really
    happens: the cell is deactivated, or it is rebound to a new
    session (a rotation)."""

    with database.transaction() as connection:
        if rebind:
            connection.execute(
                "UPDATE project_cells SET session_id = ?, updated_at = ? "
                "WHERE cell_id = 'cell-demo'",
                (FOREIGN_SESSION, NOW.isoformat()),
            )
        else:
            connection.execute(
                "UPDATE project_cells SET state = 'retired', "
                "updated_at = ? WHERE cell_id = 'cell-demo'",
                (NOW.isoformat(),),
            )


@pytest.mark.parametrize("rebind", [False, True])
def test_a_completion_after_the_binding_is_lost_changes_nothing(
    database: Database, tracker: LeadChildTracker, rebind: bool
) -> None:
    """Sol correction 7a810aad: the child row and the waiting
    continuation are created WHILE the session is bound, so both
    outlive the binding and both still match their compare-and-swaps.
    Once the cell is deactivated (or rebound to another session), the
    profile-wide SubagentStop must settle nothing: no child row, no
    continuation row and no control operation may change."""

    seed_active_cell(database)
    assert tracker.child_started(SESSION, "child-a") is True
    assert tracker.record_turn_stop(SESSION) is not None
    lose_the_binding(database, rebind=rebind)
    before = durable_snapshot(database)

    assert tracker.child_completed(SESSION, "child-a") is None

    assert durable_snapshot(database) == before
    assert reactivations(database) == 0
    assert (
        str(
            database.scalar(
                "SELECT state FROM lead_children WHERE session_id = ?",
                (SESSION,),
            )
        )
        == "started"
    )
    assert (
        str(
            database.scalar(
                "SELECT state FROM lead_continuations WHERE session_id = ?",
                (SESSION,),
            )
        )
        == "waiting"
    )


def test_a_still_bound_completion_settles_and_wakes_exactly_once(
    database: Database, tracker: LeadChildTracker
) -> None:
    """The binding check must cost the managed lead nothing: with the
    original session still actively bound, the same completion settles
    the child, reactivates the continuation and wakes it with exactly
    one children.completed — and a replay adds none."""

    seed_active_cell(database)
    assert tracker.child_started(SESSION, "child-a") is True
    continuation = tracker.record_turn_stop(SESSION)
    assert continuation is not None

    reactivation = tracker.child_completed(SESSION, "child-a")

    assert reactivation is not None
    assert reactivation.session_id == SESSION
    assert reactivation.result["completed_children"] == 1
    assert tracker.child_completed(SESSION, "child-a") is None
    assert reactivations(database) == 1
    assert (
        str(
            database.scalar(
                "SELECT state FROM lead_children WHERE session_id = ?",
                (SESSION,),
            )
        )
        == "completed"
    )
    assert (
        str(
            database.scalar(
                "SELECT state FROM lead_continuations "
                "WHERE continuation_id = ?",
                (continuation.continuation_id,),
            )
        )
        == "reactivated"
    )


def run_child_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, object],
    *,
    completed: bool,
) -> int:
    import argparse
    import io
    import json

    from hermes_orchestrator import cli as cli_module

    monkeypatch.setattr(
        cli_module.sys, "stdin", io.StringIO(json.dumps(payload))
    )
    arguments = argparse.Namespace(
        session=None, child=None, state_dir=tmp_path
    )
    return cli_module._child_event(arguments, completed=completed)


def test_the_hook_cli_keys_strictly_on_the_shared_agent_id(
    database: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SubagentStart and SubagentStop share agent_id; a PreToolUse
    tool_use_id can never replace or satisfy the lifecycle identity."""

    seed_active_cell(database)
    start = {"session_id": SESSION, "agent_id": "agent-1"}
    stop = {"session_id": SESSION, "agent_id": "agent-1"}
    assert run_child_event(tmp_path, monkeypatch, start, completed=False) == 0
    assert run_child_event(tmp_path, monkeypatch, stop, completed=True) == 0

    row = database.execute(
        "SELECT child_id, state FROM lead_children WHERE session_id = ?",
        (SESSION,),
    ).fetchone()
    assert str(row["child_id"]) == "agent-1"
    assert str(row["state"]) == "completed"


def test_a_tool_use_id_cannot_satisfy_the_lifecycle_identity(
    database: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    invocation = {"session_id": SESSION, "tool_use_id": "toolu_123"}
    assert (
        run_child_event(tmp_path, monkeypatch, invocation, completed=False)
        == 0
    )
    assert (
        run_child_event(tmp_path, monkeypatch, invocation, completed=True)
        == 0
    )

    assert database.scalar("SELECT COUNT(*) FROM lead_children") == 0


CLEARED_SESSION = "7e6b4849-0000-4000-8000-000000000001"


def seed_live_channel(
    database: Database, cwd: str, *, sessions: tuple[str, ...] = (SESSION,)
) -> None:
    """The durable evidence a live ``/clear``-ed seat already leaves: a
    cwd->project directory row and the connected sidecar registration.
    The lead process lease is deliberately ``stopped`` -- a reboot
    leaves no active lead lease, so it must never be the proof."""

    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO process_leases("
            "lease_id, worker_id, project_key, kind, pid, pgid, "
            "executable, cwd, create_time, state, acquired_at, updated_at"
            ") VALUES ('lease-1', ?, 'demo', 'claude_lead', 1053, 1053, "
            "'claude', ?, 1.0, 'stopped', ?, ?)",
            (SESSION, cwd, NOW.isoformat(), NOW.isoformat()),
        )
        for index, session in enumerate(sessions):
            project = "demo" if session == SESSION else "rival"
            cell = "cell-demo" if session == SESSION else "cell-rival"
            if project == "rival":
                connection.execute(
                    "INSERT INTO process_leases("
                    "lease_id, worker_id, project_key, kind, pid, pgid, "
                    "executable, cwd, create_time, state, acquired_at, "
                    "updated_at) VALUES ('lease-2', ?, 'rival', "
                    "'claude_lead', 9, 9, 'claude', ?, 1.0, 'stopped', "
                    "?, ?)",
                    (session, cwd, NOW.isoformat(), NOW.isoformat()),
                )
                connection.execute(
                    "INSERT INTO project_cells("
                    "cell_id, project_key, state, profile_alias, "
                    "session_id, created_at, updated_at) VALUES "
                    "('cell-rival', 'rival', 'active', 'max-d', ?, ?, ?)",
                    (session, NOW.isoformat(), NOW.isoformat()),
                )
            connection.execute(
                "INSERT INTO channel_registrations("
                "registration_id, project_key, cell_id, session_id, "
                "profile_alias, generation, state, connected_at"
                ") VALUES (?, ?, ?, ?, 'max-c', ?, 'active', ?)",
                (
                    f"reg-{index}",
                    project,
                    cell,
                    session,
                    index + 1,
                    NOW.isoformat(),
                ),
            )


def test_a_cleared_session_canonicalizes_to_the_bound_seat(
    database: Database, tmp_path: Path
) -> None:
    """INFRA-198: ``/clear`` changes only the logical session id. The
    payload session names no active cell, but the seat resolves through
    cwd -> project -> active cell -> its one live registration, and the
    stable id it yields is the one every hook path then uses."""

    from hermes_orchestrator.lead_children import bound_session_at

    seed_active_cell(database)
    seed_live_channel(database, str(tmp_path))
    with database.transaction() as connection:
        assert bound_session_at(connection, str(tmp_path)) == SESSION


@pytest.mark.parametrize(
    "managed_cwd, sessions",
    [
        (False, (SESSION,)),
        (True, ()),
        (True, (SESSION, FOREIGN_SESSION)),
    ],
)
def test_unproven_or_ambiguous_seats_stay_inert(
    database: Database,
    tmp_path: Path,
    managed_cwd: bool,
    sessions: tuple[str, ...],
) -> None:
    """An unmanaged cwd, no connected sidecar, and two live managed
    seats sharing the cwd each resolve to nothing, leaving every hook
    inert."""

    from hermes_orchestrator.lead_children import bound_session_at

    seed_active_cell(database)
    seed_live_channel(database, str(tmp_path), sessions=sessions)
    probe = str(tmp_path) if managed_cwd else str(tmp_path / "elsewhere")
    with database.transaction() as connection:
        assert bound_session_at(connection, probe) is None


def test_the_hook_entry_canonicalizes_once_and_leaves_a_bound_id_alone(
    database: Database, tmp_path: Path
) -> None:
    """The one seam every lead-hook entry uses: a cleared id becomes the
    stable one, an already-bound id is returned untouched, and an
    unresolvable id comes back unchanged so the hook stays inert."""

    from hermes_orchestrator.cli import _canonical_session

    seed_active_cell(database)
    seed_live_channel(database, str(tmp_path))
    cwd = str(tmp_path)
    assert _canonical_session(database, CLEARED_SESSION, cwd) == SESSION
    assert _canonical_session(database, SESSION, cwd) == SESSION
    stray = str(tmp_path / "elsewhere")
    assert _canonical_session(database, CLEARED_SESSION, stray) == (
        CLEARED_SESSION
    )
