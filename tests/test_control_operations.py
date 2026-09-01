"""Versioned durable control-operation events and receipts (INFRA-195)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.control_operations import (
    CONTROL_KINDS,
    LEAD_ACTIONABLE_CONTROL_KINDS,
    LIFECYCLE_CONTROL_KINDS,
    MAINTENANCE_CONTROL_KINDS,
    SILENT_MAINTENANCE_CONTROL_KINDS,
    ControlOperationRefused,
    ControlOperations,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)
SESSION = "11111111-2222-4333-8444-555555555555"
OTHER_SESSION = "66666666-7777-4888-9999-aaaaaaaaaaaa"


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def operations(database: Database) -> ControlOperations:
    return ControlOperations(
        database, events=EventStore(database), now=lambda: NOW
    )


def record(operations: ControlOperations, **overrides: object):
    arguments: dict[str, object] = {
        "kind": "channel.replayed",
        "project_key": "demo",
        "cell_id": "cell-demo",
        "session_id": SESSION,
        "result": {"replay_count": 0},
    }
    arguments.update(overrides)
    return operations.record(**arguments)  # type: ignore[arg-type]


def test_a_zero_valued_absence_result_is_a_recorded_fact(
    operations: ControlOperations, database: Database
) -> None:
    operation = record(operations)

    assert operation is not None
    assert operation.result == {"replay_count": 0}
    assert operation.state == "published"
    stored = operations.get(operation.operation_id)
    assert stored.result == {"replay_count": 0}
    journaled = database.execute(
        "SELECT aggregate_id FROM events "
        "WHERE event_type = 'control_operation.published'"
    ).fetchall()
    assert [str(row["aggregate_id"]) for row in journaled] == [
        operation.operation_id
    ]


def test_free_text_kinds_are_refused(operations: ControlOperations) -> None:
    with pytest.raises(ControlOperationRefused, match="unknown"):
        record(operations, kind="anything.goes")


def test_channel_trust_kinds_record_and_free_text_still_refuses(
    operations: ControlOperations,
) -> None:
    confirmed = record(
        operations,
        kind="channel.auto_confirmed",
        result={"anchor_id": "anc-1"},
    )
    assert confirmed is not None
    assert confirmed.kind == "channel.auto_confirmed"
    assert operations.get(confirmed.operation_id).kind == "channel.auto_confirmed"

    required = record(
        operations,
        kind="channel.approval_required",
        session_id=OTHER_SESSION,
        result={"first_failure": "entry_sha256"},
        reason="CHANNEL APPROVAL REQUIRED: entry_sha256 mismatch",
    )
    assert required is not None
    assert required.kind == "channel.approval_required"
    assert required.reason == "CHANNEL APPROVAL REQUIRED: entry_sha256 mismatch"

    with pytest.raises(ControlOperationRefused, match="unknown"):
        record(operations, kind="channel.anything_else")


def test_a_live_duplicate_is_a_durable_noop(
    operations: ControlOperations, database: Database
) -> None:
    first = record(operations)
    duplicate = record(operations, result={"replay_count": 3})

    assert first is not None
    assert duplicate is None
    assert database.scalar("SELECT COUNT(*) FROM control_operations") == 1

    # Once the live receipt is acknowledged, the next occurrence
    # records fresh — recovery is never permanently silenced.
    assert operations.acknowledge(
        first.operation_id, session_id=SESSION
    )
    again = record(operations, result={"replay_count": 3})
    assert again is not None
    assert again.result == {"replay_count": 3}


def test_acknowledge_binds_the_exact_session_exactly_once(
    operations: ControlOperations,
) -> None:
    operation = record(operations)
    assert operation is not None

    assert not operations.acknowledge(
        operation.operation_id, session_id=OTHER_SESSION
    )
    assert operations.acknowledge(
        operation.operation_id, session_id=SESSION
    )
    assert not operations.acknowledge(
        operation.operation_id, session_id=SESSION
    )
    receipt = operations.get(operation.operation_id)
    assert receipt.state == "acknowledged"
    assert receipt.acknowledged_at == NOW.isoformat()
    assert operations.pending_for_session(SESSION) == ()


def test_record_for_active_cells_receipts_every_active_lead(
    operations: ControlOperations, database: Database
) -> None:
    with database.transaction() as connection:
        for cell, project, session, state in (
            ("cell-a", "demo", SESSION, "active"),
            ("cell-b", "other", OTHER_SESSION, "active"),
            (
                "cell-c",
                "third",
                "77777777-0000-4000-8000-000000000000",
                "failed",
            ),
        ):
            connection.execute(
                "INSERT INTO project_cells("
                "cell_id, project_key, state, profile_alias, session_id, "
                "created_at, updated_at) VALUES (?, ?, ?, 'max-c', "
                "?, ?, ?)",
                (cell, project, state, session, NOW.isoformat(), NOW.isoformat()),
            )

    recorded = operations.record_for_active_cells(
        kind="daemon.restarted", result={"interval_seconds": 30}
    )

    assert sorted(op.session_id for op in recorded) == sorted(
        [SESSION, OTHER_SESSION]
    )
    assert all(op.kind == "daemon.restarted" for op in recorded)
    # The failed cell got nothing, and a second pass is a no-op while
    # the receipts are unacknowledged.
    assert operations.record_for_active_cells(
        kind="daemon.restarted", result={"interval_seconds": 30}
    ) == ()


def test_committed_operations_notify_listeners_best_effort(
    operations: ControlOperations,
) -> None:
    seen: list[object] = []

    def explode(_operation: object) -> None:
        raise RuntimeError("router down")

    operations.subscribe(explode)
    operations.subscribe(seen.append)

    operation = record(operations)

    assert operation is not None
    assert seen == [operation]


def test_confirm_claim_and_ambiguous_kinds_record_and_cas_dedup(
    operations: ControlOperations, database: Database
) -> None:
    """Sol correction b4b545f3 packet 4: the confirmation claim and its
    explicit ambiguous outcome are closed-vocabulary kinds, and the
    live-dedup unique key is the at-most-once claim CAS."""

    claim_key = "channel.confirm:anc-1:ws-1:sf-1:sess-1"
    claim = record(
        operations,
        kind="channel.confirm_claimed",
        result={"anchor_id": "anc-1"},
        dedup_key=claim_key,
    )
    assert claim is not None
    assert claim.kind == "channel.confirm_claimed"
    assert claim.dedup_key == claim_key

    duplicate = record(
        operations,
        kind="channel.confirm_claimed",
        result={"anchor_id": "anc-1"},
        dedup_key=claim_key,
    )
    assert duplicate is None
    assert (
        database.scalar(
            "SELECT COUNT(*) FROM control_operations WHERE dedup_key = ?",
            (claim_key,),
        )
        == 1
    )

    ambiguous = record(
        operations,
        kind="channel.confirm_ambiguous",
        result={"claim_operation_id": claim.operation_id, "stage": "keypress"},
        reason="CHANNEL CONFIRM AMBIGUOUS: keypress failed after the claim",
        dedup_key=f"channel.confirm_ambiguous:{claim.operation_id}",
    )
    assert ambiguous is not None
    assert ambiguous.kind == "channel.confirm_ambiguous"

    with pytest.raises(ControlOperationRefused, match="unknown"):
        record(operations, kind="channel.confirm_pressed")

    # Acknowledging the claim frees the key — recovery is an explicit
    # operator action, never a blind retry.
    assert operations.acknowledge(claim.operation_id, session_id=SESSION)
    again = record(
        operations,
        kind="channel.confirm_claimed",
        result={"anchor_id": "anc-1"},
        dedup_key=claim_key,
    )
    assert again is not None


def test_lead_launch_failed_is_accepted_and_free_text_still_refuses(
    operations: ControlOperations,
) -> None:
    recorded = record(
        operations,
        kind="lead.launch_failed",
        result={"exit_code": 1, "stderr_tail": ""},
    )
    assert recorded is not None
    assert recorded.kind == "lead.launch_failed"
    assert operations.get(recorded.operation_id).kind == "lead.launch_failed"

    with pytest.raises(ControlOperationRefused, match="unknown"):
        record(operations, kind="lead.launch_exploded")


def test_maintenance_and_actionable_kinds_partition_the_vocabulary() -> None:
    """INFRA-201: maintenance receipts are pure transport/lifecycle
    churn; everything else — including any future kind — is
    lead-actionable by default."""

    # INFRA-219 (Sol correction 14bd0c17) narrows INFRA-201's silent set:
    # the runtime-LIFECYCLE kinds are lead-actionable now, because a
    # merged-runtime activation must wake the exact bound idle lead. The
    # partition is therefore against SILENT_MAINTENANCE_CONTROL_KINDS.
    assert MAINTENANCE_CONTROL_KINDS <= CONTROL_KINDS
    assert LIFECYCLE_CONTROL_KINDS <= MAINTENANCE_CONTROL_KINDS
    assert SILENT_MAINTENANCE_CONTROL_KINDS == (
        MAINTENANCE_CONTROL_KINDS - LIFECYCLE_CONTROL_KINDS
    )
    assert LEAD_ACTIONABLE_CONTROL_KINDS | SILENT_MAINTENANCE_CONTROL_KINDS == (
        CONTROL_KINDS
    )
    assert set() == (
        LEAD_ACTIONABLE_CONTROL_KINDS & SILENT_MAINTENANCE_CONTROL_KINDS
    )
    # The three runtime-lifecycle kinds are offered, never silent.
    assert LIFECYCLE_CONTROL_KINDS <= LEAD_ACTIONABLE_CONTROL_KINDS
    assert {
        "daemon.restarted",
        "channel.reregistered",
        "channel.replayed",
    } == LIFECYCLE_CONTROL_KINDS
    assert {
        "daemon.restarted",
        "channel.reregistered",
        "channel.replayed",
        "intake.dedup_repaired",
        "channel.auto_confirmed",
        "channel.confirm_claimed",
    } == MAINTENANCE_CONTROL_KINDS
    assert {
        "intake.dedup_repaired",
        "channel.auto_confirmed",
        "channel.confirm_claimed",
    } == SILENT_MAINTENANCE_CONTROL_KINDS
    # The lead-actionable set now also carries the three lifecycle kinds.
    assert {
        "channel.blocked",
        "children.completed",
        "signal.failed",
        "channel.approval_required",
        "channel.confirm_ambiguous",
        "lead.launch_failed",
        "channel.rebind_refused",
        # INFRA-198: adoption's refusal is lead-actionable for the same
        # reason rebind's is -- the seat could not auto-confirm and an
        # operator must clear the dialog manually.
        "channel.adopt_refused",
        "daemon.restarted",
        "channel.reregistered",
        "channel.replayed",
    } == LEAD_ACTIONABLE_CONTROL_KINDS


def test_settle_maintenance_for_session_acks_only_that_sessions_maintenance(
    operations: ControlOperations, database: Database
) -> None:
    # INFRA-219 Sol correction 14bd0c17: the lifecycle kinds are OFFERED
    # now, so the silent settler no longer touches them. This test is
    # about the settler's session scoping, which is unchanged — it just
    # needs kinds that are still silent.
    maintenance = record(operations, kind="intake.dedup_repaired")
    other_session_maintenance = record(
        operations, kind="channel.auto_confirmed", session_id=OTHER_SESSION
    )
    actionable = record(
        operations,
        kind="children.completed",
        result={"issue_id": "ENG-9"},
    )
    assert maintenance is not None
    assert other_session_maintenance is not None
    assert actionable is not None

    settled = operations.settle_maintenance_for_session(SESSION)

    assert settled == (maintenance.operation_id,)
    assert operations.get(maintenance.operation_id).state == "acknowledged"
    assert operations.get(actionable.operation_id).state == "published"
    assert (
        operations.get(other_session_maintenance.operation_id).state
        == "published"
    )
    journaled = [
        str(row["aggregate_id"])
        for row in database.execute(
            "SELECT aggregate_id FROM events "
            "WHERE event_type = 'control_operation.acknowledged'"
        ).fetchall()
    ]
    assert journaled == [maintenance.operation_id]

    # Idempotent: nothing left to settle.
    assert operations.settle_maintenance_for_session(SESSION) == ()
