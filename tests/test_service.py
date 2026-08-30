from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hermes_orchestrator.admission import AdmissionController, PressureClassifier
from hermes_orchestrator.checkpoints import CheckpointRequests, CheckpointSafetyStore
from hermes_orchestrator.config import PolicyConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.resources import PressureLevel, ResourceSnapshot
from hermes_orchestrator.scheduler import Scheduler
from hermes_orchestrator.service import OrchestratorService


class FakeSampler:
    def sample(self) -> ResourceSnapshot:
        return ResourceSnapshot(
            sampled_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
            pressure=PressureLevel.UNKNOWN,
            can_admit=False,
            available_memory_bytes=8 * 1024**3,
            total_memory_bytes=24 * 1024**3,
            swap_used_bytes=3 * 1024**3,
            load_one=2.5,
            logical_cpus=14,
            disk_free_bytes={"demo": 11 * 1024**3},
            managed_rss_bytes=0,
        )


@pytest.fixture
def database(tmp_path: Path):
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def event_store(database: Database) -> EventStore:
    return EventStore(database)


@pytest.fixture
def service(database: Database, event_store: EventStore) -> OrchestratorService:
    queue = QueueService(database, event_store, {"demo"})
    scheduler = Scheduler(queue, mode="observe")
    return OrchestratorService(
        database=database,
        events=event_store,
        sampler=FakeSampler(),
        scheduler=scheduler,
        policy=PolicyConfig(),
        pid_exists=lambda _pid: False,
    )


def test_startup_reconciles_before_admission(
    service: OrchestratorService,
    event_store: EventStore,
) -> None:
    result = service.start()

    assert result.completed is True
    events = event_store.list_after(0)
    assert [event.event_type for event in events[:2]] == [
        "reconciliation.started",
        "reconciliation.completed",
    ]
    assert service.admission_open is False


def test_startup_expires_missing_worker_lease(
    service: OrchestratorService,
    database: Database,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO worker_leases("
            "lease_id, worker_id, project_key, kind, pid, state, acquired_at"
            ") VALUES ('lease-1', 'worker-1', 'demo', 'claude', 456, 'active', 'now')"
        )

    service.start()

    assert database.scalar(
        "SELECT state FROM worker_leases WHERE lease_id = 'lease-1'"
    ) == "expired"


def test_tick_persists_one_bounded_resource_sample(
    service: OrchestratorService,
    database: Database,
) -> None:
    service.start()

    result = service.tick()

    assert result.snapshot.pressure is PressureLevel.UNKNOWN
    assert result.planned_actions[0].kind == "admission_blocked"
    assert database.scalar("SELECT count(*) FROM resource_samples") == 1


def test_tick_prunes_samples_outside_retention(
    service: OrchestratorService,
    database: Database,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO resource_samples("
            "sample_id, sampled_at, pressure, available_memory_bytes, "
            "total_memory_bytes, swap_used_bytes, load_one, logical_cpus, "
            "disk_json, managed_rss_bytes"
            ") VALUES ('old', '2026-08-24T12:00:00+00:00', 'unknown', "
            "1, 1, 0, 0, 1, '{}', 0)"
        )
    service.start()

    service.tick()

    assert database.scalar("SELECT count(*) FROM resource_samples") == 1
    assert database.scalar(
        "SELECT sample_id FROM resource_samples"
    ) != "old"


def test_tick_journals_resource_actions_for_red_pressure(
    database: Database, event_store: EventStore
) -> None:
    from hermes_orchestrator.admission import AdmissionController, PressureClassifier

    class RedSampler:
        def sample(self) -> ResourceSnapshot:
            return ResourceSnapshot(
                sampled_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
                pressure=PressureLevel.RED,
                can_admit=False,
                available_memory_bytes=3 * 1024**3,
                total_memory_bytes=24 * 1024**3,
                swap_used_bytes=0,
                load_one=1.0,
                logical_cpus=14,
                disk_free_bytes={"demo": 40 * 1024**3},
                managed_rss_bytes=0,
                admission_max_priority=None,
                pressure_reasons=("available memory at or below the red floor",),
            )

    queue = QueueService(database, event_store, {"demo"})
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells(cell_id, project_key, state, profile_alias, "
            "session_id, created_at, updated_at) VALUES ('cell-1', 'demo', 'active', "
            "'max-a', 's', 'now', 'now')"
        )
        connection.execute(
            "INSERT INTO admitted_issues(issue_id, project_key, priority, state, "
            "instruction_id) VALUES ('ENG-9', 'demo', 3, 'in_development', 'i-9')"
        )
    policy = PolicyConfig.model_validate(
        {
            "mode": "active",
            "resource_thresholds": {
                "calibrated": True,
                "yellow_available_memory_gib": 6,
                "red_available_memory_gib": 4,
                "yellow_available_disk_gib": 12,
                "red_available_disk_gib": 8,
            },
        }
    )
    service = OrchestratorService(
        database=database,
        events=event_store,
        sampler=RedSampler(),
        scheduler=Scheduler(queue, mode="active"),
        policy=policy,
        pid_exists=lambda _pid: False,
        admission=AdmissionController(PressureClassifier(policy)),
        queue=queue,
    )
    service.start()
    tick = service.tick()
    kinds = [(action.kind, action.target_id) for action in tick.resource_actions]
    # Without durable safe-boundary evidence no worker is checkpoint-safe:
    # admission closes but nothing is requested.
    assert kinds == [("stop_admission", None), ("pause_worker", None)]
    assert tick.planned_actions[0].kind == "admission_blocked"
    journaled = [
        row["event_type"]
        for row in database.execute(
            "SELECT event_type FROM events WHERE aggregate_type = "
            "'resource_governance' ORDER BY sequence"
        ).fetchall()
    ]
    assert journaled == ["resource.stop_admission", "resource.pause_worker"]


def red_snapshot(at: datetime) -> ResourceSnapshot:
    return ResourceSnapshot(
        sampled_at=at,
        pressure=PressureLevel.RED,
        can_admit=False,
        available_memory_bytes=3 * 1024**3,
        total_memory_bytes=24 * 1024**3,
        swap_used_bytes=0,
        load_one=1.0,
        logical_cpus=14,
        disk_free_bytes={"demo": 40 * 1024**3},
        managed_rss_bytes=0,
        admission_max_priority=None,
        pressure_reasons=("available memory at or below the red floor",),
    )


class ClockedRedSampler:
    def __init__(self) -> None:
        self.at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)

    def advance(self) -> None:
        self.at = self.at + timedelta(minutes=1)

    def sample(self) -> ResourceSnapshot:
        return red_snapshot(self.at)


def governed_service(
    database: Database, event_store: EventStore, sampler: ClockedRedSampler
) -> tuple[OrchestratorService, CheckpointSafetyStore, CheckpointRequests]:
    queue = QueueService(database, event_store, {"demo", "other"})
    policy = PolicyConfig.model_validate(
        {
            "mode": "active",
            "resource_thresholds": {
                "calibrated": True,
                "yellow_available_memory_gib": 6,
                "red_available_memory_gib": 4,
                "yellow_available_disk_gib": 12,
                "red_available_disk_gib": 8,
            },
        }
    )
    safety = CheckpointSafetyStore(database, event_store, now=lambda: sampler.at)
    checkpoints = CheckpointRequests(database, event_store, now=lambda: sampler.at)
    service = OrchestratorService(
        database=database,
        events=event_store,
        sampler=sampler,
        scheduler=Scheduler(queue, mode="active"),
        policy=policy,
        pid_exists=lambda _pid: False,
        admission=AdmissionController(PressureClassifier(policy)),
        queue=queue,
        safety=safety,
        checkpoints=checkpoints,
    )
    service.start()
    return service, safety, checkpoints


def active_cell(database: Database, cell_id: str, project: str, session: str) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells(cell_id, project_key, state, profile_alias, "
            "session_id, created_at, updated_at) VALUES (?, ?, 'active', 'max-a', "
            "?, 'now', 'now')",
            (cell_id, project, session),
        )
        connection.execute(
            "INSERT INTO admitted_issues(issue_id, project_key, priority, state, "
            "instruction_id) VALUES (?, ?, 4, 'in_development', ?)",
            (f"{project}-1", project, f"i-{project}"),
        )


def kinds(tick: object) -> list[tuple[str, str | None]]:
    return [(a.kind, a.target_id) for a in tick.resource_actions]


def test_red_without_proven_safe_boundary_closes_admission_but_requests_nothing(
    database: Database, event_store: EventStore
) -> None:
    sampler = ClockedRedSampler()
    service, _safety, checkpoints = governed_service(database, event_store, sampler)
    active_cell(database, "cell-1", "demo", "s1")
    tick = service.tick()
    assert kinds(tick) == [("stop_admission", None), ("pause_worker", None)]
    assert checkpoints.pending() is None


def test_red_targets_only_current_session_bound_evidence(
    database: Database, event_store: EventStore
) -> None:
    sampler = ClockedRedSampler()
    service, safety, checkpoints = governed_service(database, event_store, sampler)
    active_cell(database, "cell-1", "demo", "s1")
    active_cell(database, "cell-2", "other", "s2")
    # cell-1's evidence belongs to a prior session; cell-2 is current.
    safety.mark_safe("cell-1", "s0", boundary_kind="turn_completed", evidence_id="e0")
    safety.mark_safe("cell-2", "s2", boundary_kind="turn_completed", evidence_id="e2")
    tick = service.tick()
    assert kinds(tick) == [("stop_admission", None), ("request_checkpoint", "cell-2")]
    pending = checkpoints.pending()
    assert pending is not None
    assert (pending.cell_id, pending.session_id, pending.evidence_id) == (
        "cell-2",
        "s2",
        "e2",
    )
    assert tick.resource_actions[1].evidence["request_id"] == pending.request_id


def test_two_red_ticks_yield_one_request_and_suppress_others(
    database: Database, event_store: EventStore
) -> None:
    sampler = ClockedRedSampler()
    service, safety, _checkpoints = governed_service(database, event_store, sampler)
    active_cell(database, "cell-1", "demo", "s1")
    active_cell(database, "cell-2", "other", "s2")
    safety.mark_safe("cell-1", "s1", boundary_kind="turn_completed", evidence_id="e1")
    safety.mark_safe("cell-2", "s2", boundary_kind="turn_completed", evidence_id="e2")
    first = service.tick()
    assert kinds(first) == [("stop_admission", None), ("request_checkpoint", "cell-1")]
    sampler.advance()
    second = service.tick()
    assert kinds(second) == [("stop_admission", None), ("checkpoint_pending", "cell-1")]
    assert database.scalar("SELECT count(*) FROM checkpoint_requests") == 1

    # Restart: the pending request is reconstructed from durable state.
    restarted, _s, restarted_requests = governed_service(
        database, event_store, sampler
    )
    sampler.advance()
    assert kinds(restarted.tick()) == [
        ("stop_admission", None),
        ("checkpoint_pending", "cell-1"),
    ]
    assert database.scalar("SELECT count(*) FROM checkpoint_requests") == 1

    # Completion alone is not enough: a fresh red sample is required.
    pending = restarted_requests.pending()
    assert pending is not None
    restarted_requests.resolve(pending.request_id, outcome="completed")
    stale = restarted.tick()
    assert kinds(stale) == [("stop_admission", None), ("checkpoint_pending", None)]
    sampler.advance()
    nxt = restarted.tick()
    assert kinds(nxt) == [("stop_admission", None), ("request_checkpoint", "cell-2")]
    assert database.scalar("SELECT count(*) FROM checkpoint_requests") == 2


def test_failed_request_must_be_resolved_before_another(
    database: Database, event_store: EventStore
) -> None:
    sampler = ClockedRedSampler()
    service, safety, checkpoints = governed_service(database, event_store, sampler)
    active_cell(database, "cell-1", "demo", "s1")
    active_cell(database, "cell-2", "other", "s2")
    safety.mark_safe("cell-1", "s1", boundary_kind="turn_completed", evidence_id="e1")
    safety.mark_safe("cell-2", "s2", boundary_kind="turn_completed", evidence_id="e2")
    service.tick()
    pending = checkpoints.pending()
    assert pending is not None
    sampler.advance()
    assert kinds(service.tick())[1][0] == "checkpoint_pending"
    assert checkpoints.resolve(pending.request_id, outcome="failed", detail="lead died")
    sampler.advance()
    tick = service.tick()
    assert kinds(tick) == [("stop_admission", None), ("request_checkpoint", "cell-2")]


def test_stale_delivery_requires_fresh_red_then_next_eligible_works(
    database: Database, event_store: EventStore
) -> None:
    sampler = ClockedRedSampler()
    service, safety, checkpoints = governed_service(database, event_store, sampler)
    active_cell(database, "cell-1", "demo", "s1")
    active_cell(database, "cell-2", "other", "s2")
    safety.mark_safe("cell-1", "s1", boundary_kind="turn_completed", evidence_id="e1")
    safety.mark_safe("cell-2", "s2", boundary_kind="turn_completed", evidence_id="e2")
    first = service.tick()
    reserved = first.resource_actions[1]
    assert reserved.kind == "request_checkpoint" and reserved.request_id is not None
    # Delivery finds the session changed: stale, slot released.
    assert checkpoints.resolve(reserved.request_id, outcome="stale", detail="session")
    assert kinds(service.tick()) == [
        ("stop_admission", None),
        ("checkpoint_pending", None),
    ]
    sampler.advance()
    nxt = service.tick()
    assert kinds(nxt) == [("stop_admission", None), ("request_checkpoint", "cell-2")]
    assert nxt.resource_actions[1].request_id is not None


def test_tick_consumes_due_scheduled_resets(
    database: Database, event_store: EventStore
) -> None:
    from hermes_orchestrator.stalls import ScheduledResets

    resets = ScheduledResets(
        database, event_store, now=lambda: datetime(2026, 8, 26, 11, 55, tzinfo=UTC)
    )
    reset_id = resets.schedule("demo", "pk", reason="provider_limit", delay_seconds=60)
    queue = QueueService(database, event_store, {"demo"})
    service = OrchestratorService(
        database=database,
        events=event_store,
        sampler=FakeSampler(),  # samples at 12:00, after the 11:56 due time
        scheduler=Scheduler(queue, mode="observe"),
        policy=PolicyConfig(),
        pid_exists=lambda _pid: False,
        resets=resets,
    )
    service.start()
    service.tick()
    assert resets.pending("demo", "pk") == ()
    assert database.scalar(
        "SELECT state FROM scheduled_resets WHERE reset_id = ?", (reset_id,)
    ) == "consumed"


def test_status_delegation_section_with_packets_and_children(
    database: Database, event_store: EventStore
) -> None:
    queue = QueueService(database, event_store, {"demo"})
    service = OrchestratorService(
        database=database,
        events=event_store,
        sampler=FakeSampler(),
        scheduler=Scheduler(queue, mode="observe"),
        policy=PolicyConfig(),
        pid_exists=lambda _pid: False,
    )
    service.start()

    # Seed subagent_packets with two states and two tiers
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO subagent_packets("
            "packet_id, issue_id, project_key, cell_id, session_id, generation, "
            "model_tier, effort, allowed_files_json, worktree, depends_on_json, "
            "red_test, verification_json, invariants, resource_note, state, "
            "created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "pkt-1",
                "INFRA-186",
                "demo",
                "cell-1",
                "s1",
                1,
                "sonnet",
                "medium",
                "[]",
                "/tmp/wt1",
                "[]",
                "test_red",
                "[]",
                "inv1",
                "note1",
                "accepted",
                "2026-08-29T12:00:00Z",
                "2026-08-29T12:00:00Z",
            ),
        )
        connection.execute(
            "INSERT INTO subagent_packets("
            "packet_id, issue_id, project_key, cell_id, session_id, generation, "
            "model_tier, effort, allowed_files_json, worktree, depends_on_json, "
            "red_test, verification_json, invariants, resource_note, state, "
            "created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "pkt-2",
                "INFRA-186",
                "demo",
                "cell-1",
                "s1",
                1,
                "sonnet",
                "high",
                "[]",
                "/tmp/wt2",
                "[]",
                "test_red2",
                "[]",
                "inv2",
                "note2",
                "reserved",
                "2026-08-29T12:00:00Z",
                "2026-08-29T12:00:00Z",
            ),
        )
        connection.execute(
            "INSERT INTO subagent_packets("
            "packet_id, issue_id, project_key, cell_id, session_id, generation, "
            "model_tier, effort, allowed_files_json, worktree, depends_on_json, "
            "red_test, verification_json, invariants, resource_note, state, "
            "created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "pkt-3",
                "INFRA-186",
                "demo",
                "cell-1",
                "s1",
                1,
                "haiku",
                "low",
                "[]",
                "/tmp/wt3",
                "[]",
                "test_red3",
                "[]",
                "inv3",
                "note3",
                "accepted",
                "2026-08-29T12:00:00Z",
                "2026-08-29T12:00:00Z",
            ),
        )

        # Seed worker_leases: one active claude_subagent, one released
        connection.execute(
            "INSERT INTO worker_leases("
            "lease_id, worker_id, project_key, kind, state, acquired_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                "lease-1",
                "worker-1",
                "demo",
                "claude_subagent",
                "active",
                "2026-08-29T12:00:00Z",
            ),
        )
        connection.execute(
            "INSERT INTO worker_leases("
            "lease_id, worker_id, project_key, kind, state, acquired_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                "lease-2",
                "worker-2",
                "demo",
                "claude_subagent",
                "released",
                "2026-08-29T12:00:00Z",
            ),
        )

        # Seed events with usage payloads: one lead (null parent_tool_use_id),
        # one child (non-null parent_tool_use_id)
        connection.execute(
            "INSERT INTO events("
            "event_id, occurred_at, event_type, aggregate_type, aggregate_id, "
            "correlation_id, actor, payload_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "e-1",
                "2026-08-29T12:00:00Z",
                "stream.message_start",
                "project_cell",
                "cell-1",
                None,
                "orchestrator",
                '{"usage":{"input_tokens":100,"output_tokens":50},"parent_tool_use_id":null}',
            ),
        )
        connection.execute(
            "INSERT INTO events("
            "event_id, occurred_at, event_type, aggregate_type, aggregate_id, "
            "correlation_id, actor, payload_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "e-2",
                "2026-08-29T12:00:00Z",
                "subagent.started",
                "project_cell",
                "cell-1",
                None,
                "orchestrator",
                '{"usage":{"input_tokens":200,"output_tokens":100},"parent_tool_use_id":"tool-123"}',
            ),
        )

    status = service.status()

    # Check delegation section exists
    assert "delegation" in status
    delegation = status["delegation"]

    # Check packets section
    assert "packets" in delegation
    packets = delegation["packets"]
    assert "INFRA-186" in packets
    issue_packets = packets["INFRA-186"]
    assert issue_packets["states"]["accepted"] == 2
    assert issue_packets["states"]["reserved"] == 1
    assert issue_packets["tiers"]["sonnet"] == 2
    assert issue_packets["tiers"]["haiku"] == 1

    # Check children section
    assert "children" in delegation
    children = delegation["children"]
    assert children["active"] == 1
    assert children["released"] == 1

    # Check usage_share section
    assert "usage_share" in delegation
    usage_share = delegation["usage_share"]
    assert usage_share["lead"] == 150  # 100 input + 50 output
    assert usage_share["child"] == 300  # 200 input + 100 output
    assert usage_share["fable_share"] == 0.33  # 150 / (150 + 300) ≈ 0.33


def test_status_delegation_empty_database(
    database: Database, event_store: EventStore
) -> None:
    queue = QueueService(database, event_store, {"demo"})
    service = OrchestratorService(
        database=database,
        events=event_store,
        sampler=FakeSampler(),
        scheduler=Scheduler(queue, mode="observe"),
        policy=PolicyConfig(),
        pid_exists=lambda _pid: False,
    )
    service.start()

    status = service.status()

    # Check delegation section
    assert "delegation" in status
    delegation = status["delegation"]

    # Empty packets
    assert delegation["packets"] == {}

    # Zero children
    assert delegation["children"] == {}

    # fable_share should be 1.0 when no usage
    assert delegation["usage_share"]["lead"] == 0
    assert delegation["usage_share"]["child"] == 0
    assert delegation["usage_share"]["fable_share"] == 1.0
