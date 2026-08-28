"""Verify exact process-group leases and bounded, validated termination."""

from __future__ import annotations

import signal
import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.processes import (
    ProcessLease,
    ProcessLeaseInput,
    ProcessRegistry,
    StopBlocked,
)

NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)


@dataclass
class FakeOs:
    groups: dict[int, int] = field(default_factory=dict)
    killpg_calls: list[tuple[int, int]] = field(default_factory=list)
    # signal -> "exit" | "ignore" | "gone"
    on_kill: dict[int, str] = field(default_factory=dict)
    info: FakeInfo | None = None

    def killpg(self, pgid: int, sig: int) -> None:
        self.killpg_calls.append((pgid, sig))
        behaviour = self.on_kill.get(sig, "exit")
        if behaviour == "gone":
            raise ProcessLookupError
        if behaviour == "exit" and self.info is not None:
            for pid, group in list(self.groups.items()):
                if group == pgid:
                    self.info.running.discard(pid)

    getpgid_calls: int = 0
    on_getpgid: Any = None

    def getpgid(self, pid: int) -> int:
        self.getpgid_calls += 1
        if self.on_getpgid is not None:
            self.on_getpgid(self.getpgid_calls)
        if pid not in self.groups:
            raise ProcessLookupError
        return self.groups[pid]


@dataclass
class FakeInfo:
    create_times: dict[int, float] = field(default_factory=dict)
    running: set[int] = field(default_factory=set)
    rss: dict[int, int] = field(default_factory=dict)
    wait_calls: list[tuple[int, float]] = field(default_factory=list)
    on_wait: Any = None
    running_checks: int = 0
    on_is_running: Any = None

    def create_time(self, pid: int) -> float | None:
        return self.create_times.get(pid)

    def _valid(self, pid: int, create_time: float) -> bool:
        return self.create_times.get(pid) == create_time

    def is_running(self, pid: int, create_time: float) -> bool:
        result = self._valid(pid, create_time) and pid in self.running
        self.running_checks += 1
        if self.on_is_running is not None:
            self.on_is_running(self.running_checks)
        return result

    def tree_rss_bytes(self, pid: int, create_time: float) -> int:
        return self.rss.get(pid, 0) if self._valid(pid, create_time) else 0

    def wait_exit(self, pid: int, create_time: float, timeout: float) -> bool:
        self.wait_calls.append((pid, timeout))
        if self.on_wait is not None:
            self.on_wait(len(self.wait_calls))
        return not self.is_running(pid, create_time)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def fake_info() -> FakeInfo:
    return FakeInfo(create_times={120: 1000.0}, running={120}, rss={120: 4096})


@pytest.fixture
def fake_os(fake_info: FakeInfo) -> FakeOs:
    return FakeOs(groups={120: 120}, info=fake_info)


@pytest.fixture
def process_registry(
    database: Database, fake_os: FakeOs, fake_info: FakeInfo
) -> ProcessRegistry:
    ids = iter(f"lease-{n}" for n in range(1, 20))
    return ProcessRegistry(
        database,
        EventStore(database),
        os_port=fake_os,
        info=fake_info,
        grace_seconds=2.5,
        now=lambda: NOW,
        ids=lambda: next(ids),
    )


def registered_lease(registry: ProcessRegistry, pid: int = 120) -> ProcessLease:
    return registry.register(
        ProcessLeaseInput(
            pid=pid,
            pgid=pid,
            project_key="demo",
            kind="claude",
            worker_id="session-1",
            executable="claude",
            cwd="/repo/demo",
        )
    )


def test_register_requires_process_group(process_registry: ProcessRegistry) -> None:
    with pytest.raises(ValueError, match="process group"):
        process_registry.register(
            ProcessLeaseInput(pid=120, pgid=None, project_key="demo", kind="claude")
        )


def test_register_records_exact_identity(
    process_registry: ProcessRegistry, database: Database
) -> None:
    lease = registered_lease(process_registry)
    assert lease.lease_id == "lease-1"
    assert (lease.pid, lease.pgid, lease.create_time, lease.state) == (
        120,
        120,
        1000.0,
        "active",
    )
    assert (lease.kind, lease.worker_id, lease.executable, lease.cwd) == (
        "claude",
        "session-1",
        "claude",
        "/repo/demo",
    )
    assert database.scalar(
        "SELECT count(*) FROM events WHERE event_type = 'process.registered'"
    ) == 1
    assert process_registry.active("demo") == (lease,)


def test_register_rejects_missing_or_foreign_group(
    process_registry: ProcessRegistry, fake_os: FakeOs, fake_info: FakeInfo
) -> None:
    with pytest.raises(ValueError, match="not running"):
        process_registry.register(
            ProcessLeaseInput(pid=999, pgid=999, project_key="demo", kind="claude")
        )
    fake_info.create_times[130] = 5.0
    fake_os.groups[130] = 120
    with pytest.raises(ValueError, match="process group 120"):
        process_registry.register(
            ProcessLeaseInput(pid=130, pgid=130, project_key="demo", kind="claude")
        )


def test_stop_targets_only_recorded_group(
    process_registry: ProcessRegistry, fake_os: FakeOs
) -> None:
    lease = process_registry.register(
        ProcessLeaseInput(pid=120, pgid=120, project_key="demo", kind="claude")
    )
    result = process_registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert result.signal_sent == signal.SIGTERM
    assert result.exited is True
    assert result.escalated is False
    assert fake_os.killpg_calls == [(120, signal.SIGTERM)]
    stopped = process_registry.get(lease.lease_id)
    assert (stopped.state, stopped.stop_reason, stopped.last_signal) == (
        "stopped",
        "terminated",
        int(signal.SIGTERM),
    )


def test_stop_requires_checkpoint(process_registry: ProcessRegistry) -> None:
    lease = registered_lease(process_registry)
    with pytest.raises(StopBlocked, match="checkpoint"):
        process_registry.request_stop(lease.lease_id, checkpoint_id="")
    with pytest.raises(StopBlocked, match="checkpoint"):
        process_registry.request_stop(lease.lease_id, checkpoint_id="   ")
    assert process_registry.get(lease.lease_id).state == "active"


def test_pid_reuse_is_never_signaled(
    process_registry: ProcessRegistry, fake_os: FakeOs, fake_info: FakeInfo
) -> None:
    lease = registered_lease(process_registry)
    fake_info.create_times[120] = 2000.0  # a different process now owns pid 120
    snapshot = process_registry.snapshot(lease.lease_id)
    assert (snapshot.alive, snapshot.identity_valid, snapshot.rss_bytes) == (
        False,
        False,
        0,
    )
    result = process_registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert (result.signal_sent, result.reason) == (None, "pid_reused")
    assert fake_os.killpg_calls == []
    assert process_registry.get(lease.lease_id).state == "expired"


def test_group_change_is_never_signaled(
    process_registry: ProcessRegistry, fake_os: FakeOs
) -> None:
    lease = registered_lease(process_registry)
    fake_os.groups[120] = 77
    result = process_registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert (result.signal_sent, result.reason) == (None, "pid_reused")
    assert fake_os.killpg_calls == []


def test_already_exited_process_records_without_signal(
    process_registry: ProcessRegistry, fake_os: FakeOs, fake_info: FakeInfo
) -> None:
    lease = registered_lease(process_registry)
    fake_info.running.discard(120)
    result = process_registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert (result.signal_sent, result.exited, result.reason) == (
        None,
        True,
        "already_exited",
    )
    assert fake_os.killpg_calls == []
    assert process_registry.get(lease.lease_id).state == "stopped"


def test_sigterm_timeout_escalates_to_sigkill_on_same_group(
    process_registry: ProcessRegistry,
    fake_os: FakeOs,
    fake_info: FakeInfo,
    database: Database,
) -> None:
    lease = registered_lease(process_registry)
    fake_os.on_kill = {signal.SIGTERM: "ignore", signal.SIGKILL: "exit"}
    result = process_registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert result.signal_sent == signal.SIGKILL
    assert (result.escalated, result.exited, result.reason) == (True, True, "killed")
    assert fake_os.killpg_calls == [(120, signal.SIGTERM), (120, signal.SIGKILL)]
    assert fake_info.wait_calls == [(120, 2.5), (120, 2.5)]
    events = [
        row["event_type"]
        for row in database.execute(
            "SELECT event_type FROM events WHERE aggregate_id = ? ORDER BY sequence",
            (lease.lease_id,),
        ).fetchall()
    ]
    assert events == [
        "process.registered",
        "process.stop_claimed",
        "process.term_sent",
        "process.kill_claimed",
        "process.kill_sent",
        "process.stopped",
    ]
    final = process_registry.get(lease.lease_id)
    assert (final.last_signal, final.stop_reason) == (int(signal.SIGKILL), "killed")


def test_kill_unconfirmed_stays_stopping(
    process_registry: ProcessRegistry, fake_os: FakeOs
) -> None:
    lease = registered_lease(process_registry)
    fake_os.on_kill = {signal.SIGTERM: "ignore", signal.SIGKILL: "ignore"}
    result = process_registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert (result.exited, result.reason) == (False, "kill_unconfirmed")
    assert process_registry.get(lease.lease_id).state == "stopping"
    with pytest.raises(StopBlocked, match="checkpoint"):
        process_registry.request_stop(lease.lease_id, checkpoint_id="")


def test_group_vanishing_between_checks_is_recorded(
    process_registry: ProcessRegistry, fake_os: FakeOs
) -> None:
    lease = registered_lease(process_registry)
    fake_os.on_kill = {signal.SIGTERM: "gone"}
    result = process_registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert (result.signal_sent, result.reason) == (None, "already_exited")
    assert process_registry.get(lease.lease_id).state == "stopped"


def test_tree_rss_only_counts_validated_active_leases(
    process_registry: ProcessRegistry, fake_os: FakeOs, fake_info: FakeInfo
) -> None:
    registered_lease(process_registry)
    fake_info.create_times[140] = 1.0
    fake_info.running.add(140)
    fake_info.rss[140] = 1024
    fake_os.groups[140] = 140
    other = registered_lease(process_registry, pid=140)
    assert process_registry.managed_rss_bytes() == 4096 + 1024
    process_registry.mark_exited(other.lease_id, exit_code=0)
    assert process_registry.managed_rss_bytes() == 4096
    assert process_registry.get(other.lease_id).state == "stopped"


def test_stop_on_inactive_lease_is_blocked(process_registry: ProcessRegistry) -> None:
    lease = registered_lease(process_registry)
    process_registry.mark_exited(lease.lease_id)
    with pytest.raises(StopBlocked, match="not active"):
        process_registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    with pytest.raises(KeyError):
        process_registry.request_stop("missing", checkpoint_id="cp-1")


def test_restart_detects_orphans_and_expires_dead_leases(
    database: Database, fake_os: FakeOs, fake_info: FakeInfo
) -> None:
    first = ProcessRegistry(
        database, EventStore(database), os_port=fake_os, info=fake_info, now=lambda: NOW
    )
    live = registered_lease(first)
    fake_info.create_times[150] = 3.0
    fake_info.running.add(150)
    fake_os.groups[150] = 150
    dead = registered_lease(first, pid=150)
    assert first.find_orphans() == []

    fake_info.running.discard(150)
    restarted = ProcessRegistry(
        database, EventStore(database), os_port=fake_os, info=fake_info, now=lambda: NOW
    )
    assert restarted.expire_dead() == [dead.lease_id]
    assert restarted.get(dead.lease_id).state == "expired"
    orphans = restarted.find_orphans()
    assert [lease.lease_id for lease in orphans] == [live.lease_id]
    assert fake_os.killpg_calls == []


class FailingDatabase:
    """Database proxy whose Nth transaction raises like a broken SQLite."""

    def __init__(self, inner: Database, fail_on: set[int]) -> None:
        self.inner = inner
        self.fail_on = fail_on
        self.transactions = 0

    def transaction(self) -> Any:
        self.transactions += 1
        if self.transactions in self.fail_on:
            raise sqlite3.OperationalError("database is locked")
        return self.inner.transaction()

    def execute(self, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
        return self.inner.execute(sql, parameters)

    def scalar(self, sql: str, parameters: tuple[Any, ...] = ()) -> object:
        return self.inner.scalar(sql, parameters)


def test_claim_persistence_failure_sends_no_signal(
    database: Database, fake_os: FakeOs, fake_info: FakeInfo
) -> None:
    failing = FailingDatabase(database, fail_on={2})  # tx1 register, tx2 claim
    registry = ProcessRegistry(
        failing, EventStore(database), os_port=fake_os, info=fake_info, now=lambda: NOW
    )
    lease = registered_lease(registry)
    with pytest.raises(sqlite3.OperationalError):
        registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert fake_os.killpg_calls == []
    stored = registry.get(lease.lease_id)
    assert (stored.state, stored.stop_owner, stored.stop_phase) == (
        "active",
        None,
        None,
    )


def test_two_stop_callers_only_the_winner_signals(
    database: Database, fake_os: FakeOs, fake_info: FakeInfo
) -> None:
    first = ProcessRegistry(
        database, EventStore(database), os_port=fake_os, info=fake_info, now=lambda: NOW
    )
    second = ProcessRegistry(
        database, EventStore(database), os_port=fake_os, info=fake_info, now=lambda: NOW
    )
    lease = registered_lease(first)
    fake_os.on_kill = {signal.SIGTERM: "ignore", signal.SIGKILL: "exit"}
    loser_results = []

    def race(count: int) -> None:
        if count == 1:
            loser_results.append(second.request_stop(lease.lease_id, "cp-2"))

    fake_info.on_wait = race
    result = first.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert result.reason == "killed"
    assert [r.reason for r in loser_results] == ["stop_in_progress"]
    assert [r.signal_sent for r in loser_results] == [None]
    assert fake_os.killpg_calls == [(120, signal.SIGTERM), (120, signal.SIGKILL)]
    assert first.get(lease.lease_id).stop_checkpoint_id == "cp-1"


def test_result_record_failure_recovers_without_duplicate_signal(
    database: Database, fake_os: FakeOs, fake_info: FakeInfo
) -> None:
    clock = {"now": NOW}
    failing = FailingDatabase(database, fail_on={3})  # tx3 = term_sent record
    registry = ProcessRegistry(
        failing,
        EventStore(database),
        os_port=fake_os,
        info=fake_info,
        grace_seconds=2.0,
        now=lambda: clock["now"],
    )
    lease = registered_lease(registry)
    fake_os.on_kill = {signal.SIGTERM: "ignore", signal.SIGKILL: "exit"}
    with pytest.raises(sqlite3.OperationalError):
        registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert fake_os.killpg_calls == [(120, signal.SIGTERM)]
    crashed = registry.get(lease.lease_id)
    assert (crashed.state, crashed.stop_phase) == ("stopping", "term_claimed")
    assert crashed.stop_owner is not None

    # While the claim lease is live nobody else may act.
    held = registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert (held.signal_sent, held.reason) == (None, "stop_in_progress")
    assert fake_os.killpg_calls == [(120, signal.SIGTERM)]

    # After expiry the recovery never re-sends SIGTERM: it waits, then
    # takes its own KILL claim and re-validates before SIGKILL.
    clock["now"] = NOW + timedelta(seconds=60)
    recovered = registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert (recovered.signal_sent, recovered.reason, recovered.exited) == (
        int(signal.SIGKILL),
        "killed",
        True,
    )
    assert fake_os.killpg_calls == [(120, signal.SIGTERM), (120, signal.SIGKILL)]
    events = [
        row["event_type"]
        for row in database.execute(
            "SELECT event_type FROM events WHERE aggregate_id = ? ORDER BY sequence",
            (lease.lease_id,),
        ).fetchall()
    ]
    assert events == [
        "process.registered",
        "process.stop_claimed",
        "process.stop_recovered",
        "process.kill_claimed",
        "process.kill_sent",
        "process.stopped",
    ]


def test_sigkill_has_its_own_claim_and_revalidation(
    process_registry: ProcessRegistry, fake_os: FakeOs, fake_info: FakeInfo
) -> None:
    lease = registered_lease(process_registry)
    fake_os.on_kill = {signal.SIGTERM: "ignore", signal.SIGKILL: "exit"}

    def reuse_pid_after_kill_claim(count: int) -> None:
        # Liveness checks: TERM revalidation, the grace wait, then the alive
        # check after the wait; the PID is reused right after that check so
        # only the KILL claim's own revalidation can catch it.
        if count == 3:
            fake_info.create_times[120] = 9999.0

    fake_info.on_is_running = reuse_pid_after_kill_claim
    result = process_registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert (result.signal_sent, result.reason) == (None, "pid_reused")
    assert fake_os.killpg_calls == [(120, signal.SIGTERM)]
    final = process_registry.get(lease.lease_id)
    assert (final.state, final.stop_phase) == ("expired", "kill_claimed")


def test_identity_loss_during_grace_never_kills(
    process_registry: ProcessRegistry, fake_os: FakeOs, fake_info: FakeInfo
) -> None:
    lease = registered_lease(process_registry)
    fake_os.on_kill = {signal.SIGTERM: "ignore"}

    def vanish(count: int) -> None:
        fake_info.running.discard(120)

    fake_info.on_wait = vanish
    result = process_registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
    assert (result.signal_sent, result.reason) == (int(signal.SIGTERM), "terminated")
    assert fake_os.killpg_calls == [(120, signal.SIGTERM)]
