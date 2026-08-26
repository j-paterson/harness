from datetime import UTC, datetime
from pathlib import Path

import pytest

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
