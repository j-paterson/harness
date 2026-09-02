import uuid
from collections import namedtuple
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from hermes_orchestrator.config import PolicyConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.resources import (
    PressureLevel,
    ResourceSampler,
    bound_process_rss,
    read_fresh_sample,
)

Memory = namedtuple("Memory", "available total")
Swap = namedtuple("Swap", "used")
Disk = namedtuple("Disk", "total used free")


class FakePsutil:
    def virtual_memory(self):
        return Memory(available=8 * 1024**3, total=24 * 1024**3)

    def swap_memory(self):
        return Swap(used=3 * 1024**3)

    def getloadavg(self):
        return (2.5, 2.0, 1.5)

    def cpu_count(self, logical=True):
        assert logical is True
        return 14


def test_uncalibrated_policy_is_observe_only(tmp_path: Path) -> None:
    policy = PolicyConfig()
    sampler = ResourceSampler(
        psutil_module=FakePsutil(),
        policy=policy,
        repository_paths={"demo": tmp_path},
        disk_usage=lambda _path: Disk(100, 60, 40),
    )

    snapshot = sampler.sample()

    assert snapshot.pressure is PressureLevel.UNKNOWN
    assert snapshot.can_admit is False
    assert snapshot.available_memory_bytes == 8 * 1024**3
    assert snapshot.logical_cpus == 14
    assert snapshot.disk_free_bytes == {"demo": 40}


def test_calibrated_policy_classifies_red_memory(tmp_path: Path) -> None:
    policy = PolicyConfig.model_validate(
        {
            "resource_thresholds": {
                "calibrated": True,
                "yellow_available_memory_gib": 10,
                "red_available_memory_gib": 9,
                "yellow_available_disk_gib": 12,
                "red_available_disk_gib": 6,
            }
        }
    )
    sampler = ResourceSampler(
        psutil_module=FakePsutil(),
        policy=policy,
        repository_paths={"demo": tmp_path},
        disk_usage=lambda _path: Disk(100 * 1024**3, 80 * 1024**3, 20 * 1024**3),
    )

    snapshot = sampler.sample()

    assert snapshot.pressure is PressureLevel.RED
    assert snapshot.can_admit is False


def test_vanished_sampling_target_degrades_to_zero_free(tmp_path: Path) -> None:
    # A leased worktree removed by cleanup must not crash the
    # long-running daemon's sampling tick; the vanished target reports
    # zero free bytes, so calibrated admission fails closed instead.
    def disk_usage(path: Path) -> Disk:
        if path.name == "gone":
            raise FileNotFoundError(path)
        return Disk(100, 60, 40)

    sampler = ResourceSampler(
        psutil_module=FakePsutil(),
        policy=PolicyConfig(),
        repository_paths={"demo": tmp_path, "gone": tmp_path / "gone"},
        disk_usage=disk_usage,
    )

    snapshot = sampler.sample()

    assert snapshot.disk_free_bytes == {"demo": 40, "gone": 0}


class FakeManagedProcess:
    def __init__(
        self,
        pid: int,
        *,
        command: tuple[str, ...] = (),
        rss: int = 0,
        children: tuple["FakeManagedProcess", ...] = (),
        inaccessible: bool = False,
    ) -> None:
        self.pid = pid
        self._command = command
        self._rss = rss
        self._children = children
        self._inaccessible = inaccessible

    def cmdline(self) -> tuple[str, ...]:
        if self._inaccessible:
            raise OSError("gone")
        return self._command

    def children(self, *, recursive: bool) -> tuple["FakeManagedProcess", ...]:
        assert recursive is True
        if self._inaccessible:
            raise OSError("gone")
        return self._children

    def memory_info(self) -> SimpleNamespace:
        if self._inaccessible:
            raise OSError("gone")
        return SimpleNamespace(rss=self._rss)


def test_bound_process_rss_counts_daemon_and_exact_active_lead_tree(
    database: Database,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells(cell_id, project_key, state, session_id, "
            "created_at, updated_at) VALUES "
            "('active', 'demo', 'active', 'session-active', 'now', 'now'), "
            "('retired', 'demo', 'retired', 'session-retired', 'now', 'now')"
        )

    sidecar = FakeManagedProcess(12, rss=30)
    daemon = FakeManagedProcess(10, command=("hermes-orchestrator",), rss=100)
    active_lead = FakeManagedProcess(
        11,
        command=("claude", "--resume", "session-active"),
        rss=200,
        children=(sidecar,),
    )
    retired_lead = FakeManagedProcess(
        13,
        command=("claude", "--session-id", "session-retired"),
        rss=400,
    )
    unrelated = FakeManagedProcess(
        14,
        command=("claude", "--resume", "someone-else"),
        rss=500,
    )
    rss = bound_process_rss(
        database,
        process_iter=lambda: iter(
            (daemon, active_lead, sidecar, retired_lead, unrelated)
        ),
        daemon_pid=10,
    )

    assert rss == 330


def test_bound_process_rss_ignores_inaccessible_processes(
    database: Database,
) -> None:
    daemon = FakeManagedProcess(10, rss=100)
    gone = FakeManagedProcess(11, inaccessible=True)
    rss = bound_process_rss(
        database,
        process_iter=lambda: iter((daemon, gone)),
        daemon_pid=10,
    )

    assert rss == 100


# --- read_fresh_sample (INFRA-199 Finding 2) --------------------------------


@pytest.fixture
def database(tmp_path: Path):
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


def _seed_sample(
    database: Database, *, sampled_at: str | None, pressure: str = "green"
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO resource_samples("
            "sample_id, sampled_at, pressure, available_memory_bytes, "
            "total_memory_bytes, swap_used_bytes, load_one, logical_cpus, "
            "disk_json, managed_rss_bytes"
            ") VALUES (?, ?, ?, 1, 2, 0, 0.1, 1, '{}', 1)",
            (str(uuid.uuid4()), sampled_at, pressure),
        )


def test_read_fresh_sample_absent_fails_closed(database: Database) -> None:
    now = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)

    assert read_fresh_sample(database, now=now, max_age=timedelta(minutes=5)) is None


def test_read_fresh_sample_within_bound_is_returned(database: Database) -> None:
    now = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
    sampled_at = now - timedelta(minutes=2)
    _seed_sample(database, sampled_at=sampled_at.isoformat(), pressure="yellow")

    reading = read_fresh_sample(database, now=now, max_age=timedelta(minutes=5))

    assert reading is not None
    assert reading.pressure == "yellow"
    assert reading.sampled_at == sampled_at


def test_read_fresh_sample_older_than_bound_fails_closed(database: Database) -> None:
    now = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
    sampled_at = now - timedelta(minutes=6)
    _seed_sample(database, sampled_at=sampled_at.isoformat())

    assert read_fresh_sample(database, now=now, max_age=timedelta(minutes=5)) is None


def test_read_fresh_sample_exactly_at_bound_is_still_fresh(database: Database) -> None:
    # The boundary itself is inclusive: only STRICTLY older than max_age
    # is stale.
    now = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
    sampled_at = now - timedelta(minutes=5)
    _seed_sample(database, sampled_at=sampled_at.isoformat())

    reading = read_fresh_sample(database, now=now, max_age=timedelta(minutes=5))
    assert reading is not None


def test_read_fresh_sample_malformed_timestamp_fails_closed(database: Database) -> None:
    now = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
    _seed_sample(database, sampled_at="not-a-timestamp")

    assert read_fresh_sample(database, now=now, max_age=timedelta(minutes=5)) is None


def test_read_fresh_sample_future_within_skew_is_fresh(database: Database) -> None:
    now = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
    sampled_at = now + timedelta(seconds=30)
    _seed_sample(database, sampled_at=sampled_at.isoformat())

    reading = read_fresh_sample(
        database,
        now=now,
        max_age=timedelta(minutes=5),
        clock_skew=timedelta(seconds=60),
    )

    assert reading is not None


def test_read_fresh_sample_implausibly_future_fails_closed(database: Database) -> None:
    now = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
    sampled_at = now + timedelta(minutes=10)
    _seed_sample(database, sampled_at=sampled_at.isoformat())

    reading = read_fresh_sample(
        database,
        now=now,
        max_age=timedelta(minutes=5),
        clock_skew=timedelta(seconds=60),
    )

    assert reading is None


def test_read_fresh_sample_works_on_a_connection_inside_a_transaction(
    database: Database,
) -> None:
    # The exact same check must be usable on the raw connection handed
    # out by Database.transaction(), so a caller can recheck freshness
    # atomically alongside its own writes.
    now = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
    _seed_sample(database, sampled_at=(now - timedelta(minutes=1)).isoformat())

    with database.transaction() as connection:
        reading = read_fresh_sample(connection, now=now, max_age=timedelta(minutes=5))

    assert reading is not None
