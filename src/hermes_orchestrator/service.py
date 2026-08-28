"""Application service for reconciliation and observation-only ticks."""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

import psutil

from hermes_orchestrator.config import PolicyConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.processes import ProcessRegistry
from hermes_orchestrator.resources import ResourceSnapshot
from hermes_orchestrator.scheduler import PlannedAction, Scheduler


class Sampler(Protocol):
    def sample(self) -> ResourceSnapshot: ...


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """Outcome of one startup reconciliation pass."""

    completed: bool
    findings: tuple[str, ...]
    admission_open: bool


@dataclass(frozen=True, slots=True)
class TickResult:
    """One observation sample and its side-effect-free plan."""

    snapshot: ResourceSnapshot
    planned_actions: tuple[PlannedAction, ...]


def _utc_now() -> datetime:
    return datetime.now(UTC)


class OrchestratorService:
    """Coordinate durable reconciliation and observation-only planning."""

    def __init__(
        self,
        database: Database,
        events: EventStore,
        sampler: Sampler,
        scheduler: Scheduler,
        policy: PolicyConfig,
        pid_exists: Callable[[int], bool] = psutil.pid_exists,
        now: Callable[[], datetime] = _utc_now,
        processes: ProcessRegistry | None = None,
    ) -> None:
        self._processes = processes
        self._database = database
        self._events = events
        self._sampler = sampler
        self._scheduler = scheduler
        self._policy = policy
        self._pid_exists = pid_exists
        self._now = now
        self.admission_open = False
        self._started = False

    def start(self) -> ReconciliationResult:
        """Reconcile durable state before allowing any scheduling tick."""

        run_id = str(uuid.uuid4())
        started_at = self._now().astimezone(UTC).isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO reconciliation_runs(run_id, state, started_at) "
                "VALUES (?, 'running', ?)",
                (run_id, started_at),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="reconciliation.started",
                    aggregate_type="reconciliation",
                    aggregate_id=run_id,
                    payload={},
                ),
            )

        findings = self._reconcile_worker_leases()
        findings.extend(self._reconcile_process_leases())
        integrity_ok = self._database.scalar("PRAGMA integrity_check") == "ok"
        if not integrity_ok:
            findings.append("sqlite_integrity_failed")

        completed_at = self._now().astimezone(UTC).isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE reconciliation_runs SET state = ?, completed_at = ?, "
                "findings_json = ? WHERE run_id = ?",
                (
                    "completed" if integrity_ok else "failed",
                    completed_at,
                    json.dumps(findings, separators=(",", ":")),
                    run_id,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="reconciliation.completed",
                    aggregate_type="reconciliation",
                    aggregate_id=run_id,
                    payload={"completed": integrity_ok, "findings": findings},
                ),
            )

        self.admission_open = bool(
            integrity_ok and not findings and self._policy.mode != "observe"
        )
        self._started = integrity_ok
        return ReconciliationResult(
            completed=integrity_ok,
            findings=tuple(findings),
            admission_open=self.admission_open,
        )

    def tick(self) -> TickResult:
        """Persist one bounded sample and return a non-executing plan."""

        if not self._started:
            raise RuntimeError("service must reconcile before ticking")
        snapshot = self._sampler.sample()
        cutoff = snapshot.sampled_at - timedelta(
            hours=self._policy.resource_sample_retention_hours
        )
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO resource_samples("
                "sample_id, sampled_at, pressure, available_memory_bytes, "
                "total_memory_bytes, swap_used_bytes, load_one, logical_cpus, "
                "disk_json, managed_rss_bytes"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(uuid.uuid4()),
                    snapshot.sampled_at.isoformat(),
                    snapshot.pressure.value,
                    snapshot.available_memory_bytes,
                    snapshot.total_memory_bytes,
                    snapshot.swap_used_bytes,
                    snapshot.load_one,
                    snapshot.logical_cpus,
                    json.dumps(snapshot.disk_free_bytes, sort_keys=True),
                    snapshot.managed_rss_bytes,
                ),
            )
            connection.execute(
                "DELETE FROM resource_samples WHERE sampled_at < ?",
                (cutoff.isoformat(),),
            )
        return TickResult(
            snapshot=snapshot,
            planned_actions=tuple(self._scheduler.plan(snapshot)),
        )

    def _reconcile_process_leases(self) -> list[str]:
        """Expire dead process leases; report surviving managed processes.

        Orphans — live, identity-valid managed processes that no launcher
        in this daemon owns — are findings that keep admission closed until
        an explicit, checkpoint-backed stop reclaims them. Nothing is
        signaled here.
        """

        if self._processes is None:
            return []
        expired = self._processes.expire_dead()
        with self._database.transaction() as connection:
            for lease_id in expired:
                self._events.append(
                    connection,
                    EventInput(
                        event_type="process_lease.expired",
                        aggregate_type="process_lease",
                        aggregate_id=lease_id,
                        payload={"reason": "reconciliation"},
                    ),
                )
        return [
            f"orphan_process:{lease.lease_id}"
            for lease in self._processes.find_orphans()
        ]

    def _reconcile_worker_leases(self) -> list[str]:
        rows = self._database.execute(
            "SELECT lease_id, pid FROM worker_leases WHERE state = 'active'"
        ).fetchall()
        findings: list[str] = []
        for row in rows:
            lease_id = str(row["lease_id"])
            pid = row["pid"]
            if pid is None or not self._pid_exists(int(pid)):
                with self._database.transaction() as connection:
                    connection.execute(
                        "UPDATE worker_leases SET state = 'expired' WHERE lease_id = ?",
                        (lease_id,),
                    )
                    self._events.append(
                        connection,
                        EventInput(
                            event_type="worker_lease.expired",
                            aggregate_type="worker_lease",
                            aggregate_id=lease_id,
                            payload={"reason": "process_missing"},
                        ),
                    )
            else:
                findings.append(f"uncertain_live_worker:{lease_id}")
        return findings
