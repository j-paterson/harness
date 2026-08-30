"""Application service for reconciliation and observation-only ticks."""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

import psutil

from hermes_orchestrator.admission import (
    AdmissionController,
    PressureDecision,
    ResourceAction,
    WorkerState,
)
from hermes_orchestrator.checkpoints import CheckpointRequests, CheckpointSafetyStore
from hermes_orchestrator.config import PolicyConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.processes import ProcessRegistry
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.reconcile import Reconciler, ReconciliationReport
from hermes_orchestrator.resources import ResourceSnapshot
from hermes_orchestrator.scheduler import PlannedAction, Scheduler
from hermes_orchestrator.stalls import ScheduledResets


class Sampler(Protocol):
    def sample(self) -> ResourceSnapshot: ...


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """Outcome of one startup reconciliation pass."""

    completed: bool
    findings: tuple[str, ...]
    admission_open: bool
    report: ReconciliationReport | None = None


@dataclass(frozen=True, slots=True)
class TickResult:
    """One observation sample and its side-effect-free plan."""

    snapshot: ResourceSnapshot
    planned_actions: tuple[PlannedAction, ...]
    resource_actions: tuple[ResourceAction, ...] = ()


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
        admission: AdmissionController | None = None,
        queue: QueueService | None = None,
        safety: CheckpointSafetyStore | None = None,
        checkpoints: CheckpointRequests | None = None,
        resets: ScheduledResets | None = None,
        reconciler: Reconciler | None = None,
        startup_report: ReconciliationReport | None = None,
    ) -> None:
        self._reconciler = reconciler
        self._startup_report = startup_report
        self._resets = resets
        self._processes = processes
        self._admission = admission
        self._queue = queue
        self._safety = safety
        self._checkpoints = checkpoints
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
        """Reconcile durable state before allowing any scheduling tick.

        With an ordered :class:`Reconciler` (INFRA-172) the full
        cross-system pass supplies the structured report — either already
        completed at process startup, before profile probes, or run here
        — and admission opens only when that report is clean. Without one
        the legacy worker/process/integrity pass below remains in force.
        """

        if self._startup_report is not None or self._reconciler is not None:
            report = self._startup_report
            if report is None:
                assert self._reconciler is not None
                report = self._reconciler.run()
            self.admission_open = bool(
                report.safe_to_open_admission and self._policy.mode != "observe"
            )
            self._started = report.completed
            return ReconciliationResult(
                completed=report.completed,
                findings=tuple(
                    finding.label for finding in report.findings
                ),
                admission_open=self.admission_open,
                report=report,
            )

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
        if self._resets is not None:
            self._resets.consume_due(snapshot.sampled_at)
        resource_actions = self._govern(snapshot)
        return TickResult(
            snapshot=snapshot,
            planned_actions=tuple(self._scheduler.plan(snapshot)),
            resource_actions=resource_actions,
        )

    def status(self) -> dict:
        """Build operator status view distinguishing lead from child work."""

        delegation = {
            "packets": self._delegation_packets(),
            "children": self._delegation_children(),
            "usage_share": self._delegation_usage_share(),
        }
        return {"delegation": delegation}

    def _delegation_packets(self) -> dict:
        """Count subagent_packets by issue_id, state, and model_tier."""

        packets_by_issue: dict = {}
        rows = self._database.execute(
            "SELECT issue_id, state, model_tier FROM subagent_packets"
        ).fetchall()

        for row in rows:
            issue_id = str(row["issue_id"])
            state = str(row["state"])
            model_tier = str(row["model_tier"])

            if issue_id not in packets_by_issue:
                packets_by_issue[issue_id] = {"states": {}, "tiers": {}}

            packets_by_issue[issue_id]["states"][state] = (
                packets_by_issue[issue_id]["states"].get(state, 0) + 1
            )
            packets_by_issue[issue_id]["tiers"][model_tier] = (
                packets_by_issue[issue_id]["tiers"].get(model_tier, 0) + 1
            )

        return packets_by_issue

    def _delegation_children(self) -> dict:
        """Count worker_leases by state for kind='claude_subagent'."""

        children_by_state: dict = {}
        rows = self._database.execute(
            "SELECT state FROM worker_leases WHERE kind = 'claude_subagent'"
        ).fetchall()

        for row in rows:
            state = str(row["state"])
            children_by_state[state] = children_by_state.get(state, 0) + 1

        return children_by_state

    def _delegation_usage_share(self) -> dict:
        """Compute usage token totals: lead (parent_tool_use_id null) vs child."""

        lead_usage = 0
        child_usage = 0

        rows = self._database.execute(
            "SELECT payload_json FROM events "
            "WHERE event_type LIKE 'stream.%' OR event_type LIKE 'subagent.%'"
        ).fetchall()

        for row in rows:
            payload_str = str(row["payload_json"])
            try:
                payload = json.loads(payload_str)
            except (json.JSONDecodeError, ValueError):
                continue

            # Sum all integer values under "usage" key
            usage = payload.get("usage", {})
            if not isinstance(usage, dict):
                continue

            usage_total = sum(
                v for v in usage.values() if isinstance(v, int)
            )

            # Determine if lead or child based on parent_tool_use_id
            parent_tool_use_id = payload.get("parent_tool_use_id")
            if parent_tool_use_id is None:
                lead_usage += usage_total
            else:
                child_usage += usage_total

        # Calculate fable_share
        total_usage = lead_usage + child_usage
        fable_share = (
            1.0
            if total_usage == 0
            else round(lead_usage / total_usage, 2)
        )

        return {
            "lead": lead_usage,
            "child": child_usage,
            "fable_share": fable_share,
        }

    def _govern(self, snapshot: ResourceSnapshot) -> tuple[ResourceAction, ...]:
        """Evaluate calibrated admission and journal every planned action."""

        if self._admission is None:
            return ()
        queue = self._queue.list_ranked(snapshot.sampled_at) if self._queue else []
        decision = PressureDecision(
            level=snapshot.pressure,
            can_admit=snapshot.can_admit,
            admission_max_priority=snapshot.admission_max_priority,
            reasons=snapshot.pressure_reasons,
        )
        workers = self._worker_states()
        actions = tuple(
            self._admission.evaluate(queue, workers, snapshot, decision=decision)
        )
        actions = self._bind_checkpoint_requests(actions, workers, snapshot)
        if actions:
            with self._database.transaction() as connection:
                for action in actions:
                    self._events.append(
                        connection,
                        EventInput(
                            event_type=f"resource.{action.kind}",
                            aggregate_type="resource_governance",
                            aggregate_id=action.target_id or "host",
                            payload={
                                "reason": action.reason,
                                "pressure": snapshot.pressure.value,
                                "evidence": action.evidence,
                            },
                        ),
                    )
        return actions

    def _bind_checkpoint_requests(
        self,
        actions: tuple[ResourceAction, ...],
        workers: list[WorkerState],
        snapshot: ResourceSnapshot,
    ) -> tuple[ResourceAction, ...]:
        """Enforce one durable outstanding checkpoint request host-wide.

        A ``request_checkpoint`` action survives only when no request is
        pending, the sample is fresher than the last terminal transition,
        and the target's current safe-boundary evidence yields a durable
        (idempotent) request row. Everything else degrades to the
        admission-closing action with the suppression recorded as evidence.
        """

        if self._checkpoints is None:
            return actions
        bound: list[ResourceAction] = []
        for action in actions:
            if action.kind != "request_checkpoint":
                bound.append(action)
                continue
            pending = self._checkpoints.pending()
            if pending is not None:
                bound.append(
                    ResourceAction(
                        kind="checkpoint_pending",
                        target_id=pending.cell_id,
                        reason="red persists; one checkpoint request is outstanding",
                        evidence={"request_id": pending.request_id},
                    )
                )
                continue
            terminal_at = self._checkpoints.last_terminal_at()
            sampled_at = snapshot.sampled_at.isoformat()
            if terminal_at is not None and sampled_at <= terminal_at:
                bound.append(
                    ResourceAction(
                        kind="checkpoint_pending",
                        target_id=None,
                        reason="awaiting a fresh red sample after the last checkpoint",
                        evidence={"last_terminal_at": terminal_at},
                    )
                )
                continue
            # Walk the controller's ordered candidates: a boundary already
            # consumed by a terminal request is skipped for the next one.
            ordered = [action.target_id, *action.evidence.get("candidates", [])]
            by_id = {worker.worker_id: worker for worker in workers}
            for target in dict.fromkeys(ordered):
                worker = by_id.get(target)
                if worker is None or worker.evidence is None:
                    continue
                request = self._checkpoints.request(
                    worker.evidence, reason=action.reason
                )
                if request is None:
                    continue
                bound.append(
                    ResourceAction(
                        kind=action.kind,
                        target_id=target,
                        reason=action.reason,
                        evidence={**action.evidence, "request_id": request.request_id},
                        request_id=request.request_id,
                    )
                )
                break
        return tuple(bound)

    def _worker_states(self) -> list[WorkerState]:
        """Active leads as pause candidates, from durable state only.

        A worker is checkpoint-safe only with current durable evidence bound
        to its exact cell and session; missing, invalidated, or
        prior-session evidence is unsafe.
        """

        rows = self._database.execute(
            "SELECT c.cell_id, c.project_key, c.session_id, "
            "(SELECT min(priority) FROM admitted_issues i "
            " WHERE i.project_key = c.project_key "
            " AND i.state IN ('in_development', 'review')) AS priority, "
            "(SELECT count(*) FROM admitted_issues i "
            " WHERE i.project_key = c.project_key AND i.dependency_ready = 0) "
            " AS blocked_dependents "
            "FROM project_cells c WHERE c.state IN ('starting', 'active')"
        ).fetchall()
        rss: dict[str, int] = {}
        if self._processes is not None:
            for lease in self._processes.active():
                rss[lease.project_key] = rss.get(lease.project_key, 0) + (
                    self._processes.snapshot(lease.lease_id).rss_bytes
                )
        workers: list[WorkerState] = []
        for row in rows:
            evidence = None
            if self._safety is not None and row["session_id"] is not None:
                evidence = self._safety.current(
                    str(row["cell_id"]), str(row["session_id"])
                )
            workers.append(
                WorkerState(
                    worker_id=str(row["cell_id"]),
                    project_key=str(row["project_key"]),
                    priority=int(row["priority"] or 4),
                    checkpoint_safe=evidence is not None,
                    dependency_critical=int(row["blocked_dependents"] or 0) > 0,
                    recent_progress=0.0,
                    rss_bytes=rss.get(str(row["project_key"]), 0),
                    evidence=evidence,
                )
            )
        return workers

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
