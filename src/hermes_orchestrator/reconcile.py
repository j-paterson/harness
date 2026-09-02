"""Ordered cross-system startup reconciliation (INFRA-172).

One :class:`Reconciler` pass runs at every startup, before profile probes
and before any scheduling, and walks the whole recovery surface in a
fixed, documented order: SQLite integrity and incomplete transitions,
process leases, worker sessions, worktrees and their remote proofs, the
GitHub mutation journal, the CircleCI merge ledger, and the Linear
projection. It produces one :class:`ReconciliationReport` of structured
:class:`Finding` entries and fails closed: admission stays shut while any
evidence is conflicting or unknown.

The reconciler observes and gates; it never mutates GitHub, CircleCI,
Linear, or a live process merely to make durable records agree with them.
The only effects it triggers are the already-journaled recovery paths —
``ProcessRegistry.expire_dead`` for vanished processes, the worker-lease
expiry transition, and ``WorktreeCustodian`` convergence for crashed
cleanups — each of which owns its own compare-and-swap guards.

The sqlite stage is the first fail-closed gate: when the integrity check
or the schema-compatibility check fails, every later stage is skipped —
no journaled recovery effect, no process probe, no git call, and no
external read may be authorized by durable state that is already known
to be unreliable. The failed report is persisted from the sqlite
evidence alone and admission stays closed until the state is repaired.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import psutil

from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.worktrees import (
    CleanupBlocked,
    GitPort,
    WorktreeLease,
    WorktreeLeases,
)

STAGE_ORDER = (
    "sqlite",
    "process_leases",
    "worker_sessions",
    "worktrees",
    "github",
    "circleci",
    "linear",
    "admission",
)

_LIVE_CELL_STATES = ("starting", "active", "handoff_required", "paused")


class ProcessEvidencePort(Protocol):
    """Journaled process-lease recovery and orphan detection."""

    def expire_dead(self) -> list[str]: ...

    def find_orphans(self) -> list[Any]: ...


class CustodianPort(Protocol):
    """Worktree convergence through the journaled cleanup claims."""

    def reconcile(self, lease_id: str) -> Any: ...

    def converge_missing(self, lease_id: str) -> Any: ...


class GitHubReadPort(Protocol):
    """Bounded read-only pull-request projection; never merges."""

    def get_pull_request(self, repository: str, number: int) -> Any: ...


class LinearReadPort(Protocol):
    """Bounded read-only Linear issue projection; never mutates."""

    def get_issue(self, issue_id: str) -> Any: ...


class CiKnownStatePort(Protocol):
    """Bounded read-only known CI outcome for one exact merged commit."""

    def known_state(
        self, repository: str, integration_branch: str, merge_sha: str
    ) -> str: ...


class CiStatusCheckPort(Protocol):
    """The existing intake-boundary CI status read reused for projection."""

    def check(
        self, project_slug: str, branch: str, merge_sha: str
    ) -> Any: ...


@dataclass(frozen=True, slots=True)
class LinearExpectations:
    """Exact configured Linear identities the projection may resolve with."""

    team_ids: Mapping[str, str]
    status_ids: Mapping[str, Mapping[str, str]]
    assignee_ids: Mapping[str, str]


class CircleCiKnownState:
    """Project one recorded merge onto CircleCI's known outcome, read-only."""

    def __init__(self, status: CiStatusCheckPort) -> None:
        self._status = status

    def known_state(
        self, repository: str, integration_branch: str, merge_sha: str
    ) -> str:
        check = self._status.check(
            f"gh/{repository}", integration_branch, merge_sha
        )
        return str(check.outcome)


_MANAGED_EXECUTABLES = frozenset({"claude", "codex"})
_MANAGED_MARKER = "hermes_orchestrator"


class ManagedProcessScanner:
    """Bounded read-only discovery of unleased managed-looking processes.

    A process is managed-looking only when both markers hold: its
    executable name is a managed binary (or a Python process running the
    orchestrator package) and its working directory lies inside a managed
    root — a configured project repository or a live worktree lease path.
    Ordinary host processes match neither test and are never reported.

    A managed-looking process is excluded only on proof: the scanning
    process itself, an exact ``(pid, create_time)`` process-lease match,
    or membership in a leased process group. Unprovable exclusions fail
    closed and the process is reported. Every attribute read is
    defensive — a vanished or unreadable process never crashes startup —
    and nothing here ever signals a process. When the scan cannot finish
    inside its bounds it raises instead of returning a partial answer.
    """

    def __init__(
        self,
        database: Database,
        *,
        roots: Sequence[Path] = (),
        iter_processes: Callable[[], Iterable[Any]] | None = None,
        executables: frozenset[str] = _MANAGED_EXECUTABLES,
        max_processes: int = 4096,
        max_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        self_pid: int | None = None,
        getpgid: Callable[[int], int] = os.getpgid,
    ) -> None:
        self._database = database
        self._roots = tuple(Path(root) for root in roots)
        self._iter_processes = iter_processes or self._psutil_processes
        self._executables = executables
        self._max_processes = max_processes
        self._max_seconds = max_seconds
        self._clock = clock
        self._self_pid = self_pid if self_pid is not None else os.getpid()
        self._getpgid = getpgid

    @staticmethod
    def _psutil_processes() -> Iterable[Any]:
        return psutil.process_iter(
            attrs=["pid", "name", "cwd", "cmdline", "create_time"],
            ad_value=None,
        )

    def __call__(self) -> tuple[int, ...]:
        leased, leased_groups = self._leased_identities()
        roots = self._managed_roots()
        try:
            self_pgid: int | None = self._getpgid(self._self_pid)
        except OSError:
            self_pgid = None
        deadline = self._clock() + self._max_seconds
        unknown: list[int] = []
        examined = 0
        for process in self._iter_processes():
            examined += 1
            if examined > self._max_processes or self._clock() > deadline:
                raise RuntimeError("bounded process scan did not complete")
            info = getattr(process, "info", None)
            if not isinstance(info, Mapping):
                continue
            pid = info.get("pid")
            if not isinstance(pid, int) or pid == self._self_pid:
                continue
            if not self._managed_looking(info):
                continue
            cwd = info.get("cwd")
            if not isinstance(cwd, str) or not self._inside(cwd, roots):
                continue
            create_time = info.get("create_time")
            if isinstance(create_time, (int, float)) and any(
                lease_pid == pid and abs(lease_time - float(create_time)) <= 1e-3
                for lease_pid, lease_time in leased
            ):
                continue
            try:
                pgid: int | None = self._getpgid(pid)
            except OSError:
                # Group membership cannot be proven; fail closed and report.
                pgid = None
            if pgid is not None and (
                pgid == self_pgid or pgid in leased_groups
            ):
                continue
            unknown.append(pid)
        return tuple(sorted(unknown))

    def _leased_identities(self) -> tuple[set[tuple[int, float]], set[int]]:
        rows = self._database.execute(
            "SELECT pid, pgid, create_time FROM process_leases "
            "WHERE state IN ('active', 'stopping')"
        ).fetchall()
        leased = {
            (int(row["pid"]), float(row["create_time"])) for row in rows
        }
        groups = {int(row["pgid"]) for row in rows}
        return leased, groups

    def _managed_roots(self) -> tuple[Path, ...]:
        rows = self._database.execute(
            "SELECT path FROM worktree_leases WHERE state != 'reclaimed'"
        ).fetchall()
        return self._roots + tuple(Path(str(row["path"])) for row in rows)

    def _managed_looking(self, info: Mapping[str, Any]) -> bool:
        name = info.get("name")
        base = str(name).lower() if isinstance(name, str) else ""
        if base in self._executables:
            return True
        cmdline = info.get("cmdline")
        joined = (
            " ".join(str(part) for part in cmdline)
            if isinstance(cmdline, (list, tuple))
            else ""
        )
        return base.startswith("python") and _MANAGED_MARKER in joined

    @staticmethod
    def _inside(cwd: str, roots: Sequence[Path]) -> bool:
        path = Path(cwd)
        return any(path.is_relative_to(root) for root in roots)


@dataclass(frozen=True, slots=True)
class Finding:
    """One structured reconciliation observation."""

    kind: str
    subsystem: str
    severity: str  # blocking | warning | info
    aggregate_id: str | None
    recommended_action: str
    evidence: dict[str, Any] = field(default_factory=dict)

    @property
    def blocking(self) -> bool:
        return self.severity == "blocking"

    @property
    def label(self) -> str:
        if self.aggregate_id is None:
            return self.kind
        return f"{self.kind}:{self.aggregate_id}"

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "subsystem": self.subsystem,
            "severity": self.severity,
            "aggregate_id": self.aggregate_id,
            "recommended_action": self.recommended_action,
            "evidence": self.evidence,
        }


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    """The completed ordered pass and its structured findings."""

    run_id: str
    completed: bool
    stages: tuple[str, ...]
    findings: tuple[Finding, ...]

    @property
    def safe_to_open_admission(self) -> bool:
        return self.completed and not any(
            finding.blocking for finding in self.findings
        )

    def blocking(self) -> tuple[Finding, ...]:
        return tuple(finding for finding in self.findings if finding.blocking)


def _expected_schema_version() -> int:
    migrations = Path(__file__).with_name("migrations")
    versions = [
        int(path.name.split("_", maxsplit=1)[0])
        for path in migrations.glob("[0-9][0-9][0-9][0-9]_*.sql")
    ]
    return max(versions)


class Reconciler:
    """Run the fixed-order startup reconciliation over durable state."""

    def __init__(
        self,
        database: Database,
        events: EventStore,
        *,
        projects: Mapping[str, ProjectConfig] | None = None,
        processes: ProcessEvidencePort | None = None,
        worktrees: WorktreeLeases | None = None,
        custodian: CustodianPort | None = None,
        git: GitPort | None = None,
        pid_exists: Callable[[int], bool] = psutil.pid_exists,
        unknown_processes: Callable[[], Sequence[int]] | None = None,
        github_reads: GitHubReadPort | None = None,
        linear_reads: LinearReadPort | None = None,
        linear_expectations: LinearExpectations | None = None,
        ci_states: CiKnownStatePort | None = None,
        expected_schema_version: int | None = None,
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
    ) -> None:
        if worktrees is not None and (custodian is None or git is None):
            raise ValueError(
                "worktree reconciliation requires a custodian and a git port"
            )
        if linear_reads is not None and linear_expectations is None:
            raise ValueError(
                "linear reconciliation reads require the configured "
                "expected identities"
            )
        self._database = database
        self._events = events
        self._projects = dict(projects) if projects is not None else {}
        self._processes = processes
        self._worktrees = worktrees
        self._custodian = custodian
        self._git = git
        self._pid_exists = pid_exists
        self._unknown_processes = unknown_processes
        self._github_reads = github_reads
        self._linear_reads = linear_reads
        self._linear_expectations = linear_expectations
        self._ci_states = ci_states
        self._expected_schema = (
            expected_schema_version
            if expected_schema_version is not None
            else _expected_schema_version()
        )
        self._now = now or (lambda: datetime.now(UTC))
        self._ids = ids or (lambda: str(uuid.uuid4()))

    def run(self) -> ReconciliationReport:
        """Execute every stage in :data:`STAGE_ORDER` and record the pass."""

        run_id = self._ids()
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
                    payload={"stages": list(STAGE_ORDER)},
                ),
            )

        stages: list[str] = []
        findings: list[Finding] = []
        completed = True
        for stage in STAGE_ORDER:
            stages.append(stage)
            if stage == "sqlite":
                completed = self._stage_sqlite(run_id, findings)
                if not completed:
                    # Hard abort: durable state failed its integrity or
                    # schema gate, so it may not authorize any recovery
                    # effect or external read. The stages list honestly
                    # records that nothing after sqlite ran, and only the
                    # sqlite evidence is persisted below.
                    break
            elif stage == "process_leases":
                self._stage_process_leases(findings)
            elif stage == "worker_sessions":
                self._stage_worker_sessions(findings)
            elif stage == "worktrees":
                self._stage_worktrees(findings)
            elif stage == "github":
                self._stage_github(findings)
            elif stage == "circleci":
                self._stage_circleci(findings)
            elif stage == "linear":
                self._stage_linear(findings)

        report = ReconciliationReport(
            run_id=run_id,
            completed=completed,
            stages=tuple(stages),
            findings=tuple(findings),
        )
        completed_at = self._now().astimezone(UTC).isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE reconciliation_runs SET state = ?, completed_at = ?, "
                "findings_json = ? WHERE run_id = ?",
                (
                    "completed" if completed else "failed",
                    completed_at,
                    json.dumps(
                        [finding.as_dict() for finding in findings],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    run_id,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="reconciliation.completed",
                    aggregate_type="reconciliation",
                    aggregate_id=run_id,
                    payload={
                        "completed": completed,
                        "stages": list(report.stages),
                        "findings": len(report.findings),
                        "blocking": len(report.blocking()),
                        "safe_to_open_admission": report.safe_to_open_admission,
                    },
                ),
            )
        return report

    # -- stage 1: sqlite ---------------------------------------------------

    def _stage_sqlite(self, run_id: str, findings: list[Finding]) -> bool:
        integrity_ok = self._database.scalar("PRAGMA integrity_check") == "ok"
        if not integrity_ok:
            findings.append(
                Finding(
                    kind="sqlite_integrity_failed",
                    subsystem="sqlite",
                    severity="blocking",
                    aggregate_id=str(self._database.path),
                    recommended_action=(
                        "restore durable state from a trusted copy before "
                        "reopening admission"
                    ),
                )
            )
        version = self._database.schema_version()
        if version != self._expected_schema:
            findings.append(
                Finding(
                    kind="schema_version_mismatch",
                    subsystem="sqlite",
                    severity="blocking",
                    aggregate_id=str(version),
                    recommended_action=(
                        "run the matching orchestrator build against this "
                        "durable state"
                    ),
                    evidence={
                        "found": version,
                        "expected": self._expected_schema,
                    },
                )
            )
        rows = self._database.execute(
            "SELECT run_id, started_at FROM reconciliation_runs "
            "WHERE state = 'running' AND run_id != ?",
            (run_id,),
        ).fetchall()
        for row in rows:
            findings.append(
                Finding(
                    kind="prior_reconciliation_crashed",
                    subsystem="sqlite",
                    severity="warning",
                    aggregate_id=str(row["run_id"]),
                    recommended_action=(
                        "superseded by this pass; the stale row is kept as an "
                        "audit record"
                    ),
                    evidence={"started_at": str(row["started_at"])},
                )
            )
        return integrity_ok and version == self._expected_schema

    # -- stage 2: process leases -------------------------------------------

    def _stage_process_leases(self, findings: list[Finding]) -> None:
        reported: set[str] = set()
        if self._processes is not None:
            for lease_id in self._processes.expire_dead():
                reported.add(lease_id)
                findings.append(
                    Finding(
                        kind="process_lease_expired",
                        subsystem="process_leases",
                        severity="info",
                        aggregate_id=lease_id,
                        recommended_action=(
                            "none; the vanished process was expired through "
                            "the journaled registry path"
                        ),
                    )
                )
            for lease in self._processes.find_orphans():
                lease_id = str(lease.lease_id)
                reported.add(lease_id)
                if self._expected_applier_lease(lease):
                    findings.append(
                        Finding(
                            kind="expected_applier_lease",
                            subsystem="process_leases",
                            severity="info",
                            aggregate_id=lease_id,
                            recommended_action=(
                                "none; this runtime_applier is bound to a "
                                "live activation intent and is expected to "
                                "survive the kickstart that spawned this "
                                "daemon; the post-merge tick reaps its exit "
                                "by exact identity"
                            ),
                            evidence={
                                "project_key": getattr(
                                    lease, "project_key", None
                                ),
                                "pid": getattr(lease, "pid", None),
                                "cwd": getattr(lease, "cwd", None),
                                "worker_id": getattr(lease, "worker_id", None),
                            },
                        )
                    )
                    continue
                findings.append(
                    Finding(
                        kind="orphan_process",
                        subsystem="process_leases",
                        severity="blocking",
                        aggregate_id=lease_id,
                        recommended_action=(
                            "reclaim the surviving process through an "
                            "explicit checkpoint-backed stop; never signal "
                            "it here"
                        ),
                        evidence={
                            "project_key": getattr(lease, "project_key", None),
                            "pid": getattr(lease, "pid", None),
                            "cwd": getattr(lease, "cwd", None),
                            "state": getattr(lease, "state", None),
                        },
                    )
                )
        rows = self._database.execute(
            "SELECT lease_id, stop_owner, stop_phase, stop_checkpoint_id, "
            "stop_claim_expires_at FROM process_leases WHERE state = 'stopping'"
        ).fetchall()
        for row in rows:
            lease_id = str(row["lease_id"])
            if lease_id in reported:
                continue
            findings.append(
                Finding(
                    kind="incomplete_stop",
                    subsystem="process_leases",
                    severity="blocking",
                    aggregate_id=lease_id,
                    recommended_action=(
                        "resume the stop through "
                        "ProcessRegistry.request_stop with the recorded "
                        "checkpoint; the durable phase prevents any repeated "
                        "signal"
                    ),
                    evidence={
                        "stop_owner": row["stop_owner"],
                        "stop_phase": row["stop_phase"],
                        "stop_checkpoint_id": row["stop_checkpoint_id"],
                        "stop_claim_expires_at": row["stop_claim_expires_at"],
                    },
                )
            )
        if self._unknown_processes is not None:
            try:
                unknown = tuple(self._unknown_processes())
            except Exception as error:
                findings.append(
                    Finding(
                        kind="process_scan_unavailable",
                        subsystem="process_leases",
                        severity="blocking",
                        aggregate_id=None,
                        recommended_action=(
                            "the bounded process discovery did not complete, "
                            "so live managed-looking processes cannot be "
                            "ruled out; admission stays closed"
                        ),
                        evidence={"error": type(error).__name__},
                    )
                )
                unknown = ()
            for pid in unknown:
                findings.append(
                    Finding(
                        kind="unknown_live_process",
                        subsystem="process_leases",
                        severity="blocking",
                        aggregate_id=str(pid),
                        recommended_action=(
                            "identify the managed-looking process and bind "
                            "or stop it through an explicit claim; nothing "
                            "is signaled here"
                        ),
                        evidence={"pid": int(pid)},
                    )
                )

    def _expected_applier_lease(self, lease: Any) -> bool:
        """A live ``runtime_applier`` lease bound to an open activation.

        Sol correction e716a420: a still-verifying applier survives this
        daemon's own kickstart-triggered death; without this, a bare
        ``find_orphans`` would block admission for the applier's entire
        lifetime and never record its exit. Exactly the intent-bound
        lease is excused — every other surviving lease, including any
        other ``runtime_applier``, stays a fail-closed orphan.

        Sol correction c5600e31: "bound" is exact identity, never a
        ``(kind, worker_id)`` scan — two live leases can share a
        worker_id and at most one of them is the spawned applier. The
        lease is expected if and only if ALL of these hold:

        * its kind is ``runtime_applier`` and its ``worker_id`` (the
          merge sha) has an ``activate:<sha>`` intent still
          ``intended``, or the applier's own apply row for that same
          target checkout is ``intended``, ``activated``, or
          ``restarted`` — no intent at all, or a terminal outcome
          already recorded, fails closed;
        * the latest journaled ``activation.applier_spawned`` event for
          ``activate:<sha>`` carries a non-null lease_id equal to this
          lease's lease_id AND a non-null project_key equal to this
          lease's owning project (Sol correction 57c46faa: project
          ownership is part of the exact binding) — a null or absent
          journaled lease_id or project_key excuses NOTHING;
        * the lease's cwd is the intent row's exact target checkout.

        Liveness and identity validity are already proven upstream:
        ``find_orphans`` only ever surfaces live, identity-valid leases,
        and nothing here weakens that.
        """

        if getattr(lease, "kind", None) != "runtime_applier":
            return False
        worker_id = getattr(lease, "worker_id", None)
        if not worker_id:
            return False
        apply_id = f"activate:{worker_id}"
        intent = self._database.execute(
            "SELECT state, target_checkout FROM activation_applies "
            "WHERE apply_id = ?",
            (apply_id,),
        ).fetchone()
        if intent is None:
            return False
        binding = self._journaled_applier_binding(apply_id)
        if binding is None:
            return False
        bound_lease_id, bound_project = binding
        if bound_lease_id != str(getattr(lease, "lease_id", "")):
            return False
        if bound_project != str(getattr(lease, "project_key", "")):
            return False
        if getattr(lease, "cwd", None) != str(intent["target_checkout"]):
            return False
        if str(intent["state"]) == "intended":
            return True
        applier_row = self._database.execute(
            "SELECT 1 FROM activation_applies WHERE apply_id != ? "
            "AND target_checkout = ? "
            "AND state IN ('intended', 'activated', 'restarted')",
            (apply_id, str(intent["target_checkout"])),
        ).fetchone()
        return applier_row is not None

    def _journaled_applier_binding(
        self, apply_id: str
    ) -> tuple[str, str] | None:
        """The (lease_id, project_key) from the latest journaled binding.

        The post-merge advance journals ``activation.applier_spawned``
        on the ``activate:<sha>`` intent atomically with the lease
        registration itself, carrying the exact registered lease_id and
        the owning project_key; that durable record is the only
        activation-to-lease binding, and project ownership is part of it
        (Sol correction 57c46faa). A missing event, an unreadable
        payload, a null lease_id (a spawn with no registry), or a null
        project_key binds nothing — fail closed.
        """

        row = self._database.execute(
            "SELECT payload_json FROM events "
            "WHERE event_type = 'activation.applier_spawned' "
            "AND aggregate_id = ? ORDER BY sequence DESC LIMIT 1",
            (apply_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, ValueError):
            return None
        if not isinstance(payload, dict):
            return None
        lease_id = payload.get("lease_id")
        project_key = payload.get("project_key")
        if (
            isinstance(lease_id, str)
            and lease_id
            and isinstance(project_key, str)
            and project_key
        ):
            return lease_id, project_key
        return None

    # -- stage 3: worker sessions ------------------------------------------

    def _stage_worker_sessions(self, findings: list[Finding]) -> None:
        rows = self._database.execute(
            "SELECT lease_id, pid FROM worker_leases WHERE state = 'active'"
        ).fetchall()
        for row in rows:
            lease_id = str(row["lease_id"])
            pid = row["pid"]
            if pid is None or not self._pid_exists(int(pid)):
                with self._database.transaction() as connection:
                    connection.execute(
                        "UPDATE worker_leases SET state = 'expired' "
                        "WHERE lease_id = ?",
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
                findings.append(
                    Finding(
                        kind="worker_lease_expired",
                        subsystem="worker_sessions",
                        severity="info",
                        aggregate_id=lease_id,
                        recommended_action=(
                            "none; the dead worker lease was expired through "
                            "the journaled transition"
                        ),
                    )
                )
            else:
                findings.append(
                    Finding(
                        kind="uncertain_live_worker",
                        subsystem="worker_sessions",
                        severity="blocking",
                        aggregate_id=lease_id,
                        recommended_action=(
                            "validate the surviving worker's identity and "
                            "reattach or stop it through an explicit claim"
                        ),
                        evidence={"pid": int(pid)},
                    )
                )
        cells = self._database.execute(
            "SELECT cell_id, project_key, state FROM project_cells "
            "WHERE state IN ('starting', 'handoff_required')"
        ).fetchall()
        for cell in cells:
            starting = str(cell["state"]) == "starting"
            findings.append(
                Finding(
                    kind=(
                        "cell_start_incomplete"
                        if starting
                        else "cell_handoff_pending"
                    ),
                    subsystem="worker_sessions",
                    severity="blocking" if starting else "info",
                    aggregate_id=str(cell["cell_id"]),
                    recommended_action=(
                        "resolve the interrupted cell launch through the "
                        "cell service before dispatching this project"
                        if starting
                        else "complete the pending handoff through Hermes"
                    ),
                    evidence={"project_key": str(cell["project_key"])},
                )
            )

    # -- stage 4: worktrees ------------------------------------------------

    def _stage_worktrees(self, findings: list[Finding]) -> None:
        if self._worktrees is None:
            return
        assert self._custodian is not None and self._git is not None
        for lease in self._worktrees.active():
            if lease.state == "reclaiming":
                self._converge(
                    findings,
                    lease,
                    action=self._custodian.reconcile,
                    blocked_kind="worktree_reconcile_blocked",
                )
                continue
            registered = self._git.worktree_list(Path(lease.repo_path))
            if lease.path in registered:
                continue
            if lease.state == "checkpointed":
                self._converge(
                    findings,
                    lease,
                    action=self._custodian.converge_missing,
                    blocked_kind="lost_work_suspected",
                )
            else:
                findings.append(self._lost_work(lease, "never checkpointed"))

    def _converge(
        self,
        findings: list[Finding],
        lease: WorktreeLease,
        *,
        action: Callable[[str], Any],
        blocked_kind: str,
    ) -> None:
        try:
            action(lease.lease_id)
        except CleanupBlocked as error:
            if blocked_kind == "lost_work_suspected":
                findings.append(self._lost_work(lease, str(error)))
            else:
                findings.append(
                    Finding(
                        kind=blocked_kind,
                        subsystem="worktrees",
                        severity="blocking",
                        aggregate_id=lease.lease_id,
                        recommended_action=(
                            "leave the cleanup claim in place; retry once "
                            "the recorded claim provably expired"
                        ),
                        evidence={
                            "path": lease.path,
                            "state": lease.state,
                            "reason": str(error),
                        },
                    )
                )
        else:
            findings.append(
                Finding(
                    kind="worktree_converged",
                    subsystem="worktrees",
                    severity="info",
                    aggregate_id=lease.lease_id,
                    recommended_action=(
                        "resume from the remote branch at the recorded "
                        "checkpoint"
                    ),
                    evidence={
                        "path": lease.path,
                        "branch": lease.branch,
                        "remote": lease.remote,
                        "sha": lease.checkpoint_sha,
                    },
                )
            )

    def _lost_work(self, lease: WorktreeLease, reason: str) -> Finding:
        return Finding(
            kind="lost_work_suspected",
            subsystem="worktrees",
            severity="blocking",
            aggregate_id=lease.lease_id,
            recommended_action=(
                "audit the vanished worktree before any further work on "
                "this project; nothing is reset here"
            ),
            evidence={
                "path": lease.path,
                "state": lease.state,
                "reason": reason,
            },
        )

    # -- stage 5: github ---------------------------------------------------

    def _stage_github(self, findings: list[Finding]) -> None:
        rows = self._database.execute(
            "SELECT effect_id, repository, pr_number, head_sha, state, "
            "claim_expires_at FROM github_merge_effects "
            "WHERE state IN ('attempted', 'pending')"
        ).fetchall()
        now_iso = self._now().astimezone(UTC).isoformat()
        for row in rows:
            if str(row["state"]) == "attempted":
                findings.append(self._project_attempted_merge(row))
            else:
                expires = row["claim_expires_at"]
                findings.append(
                    Finding(
                        kind="merge_claim_stranded",
                        subsystem="github",
                        severity="warning",
                        aggregate_id=str(row["effect_id"]),
                        recommended_action=(
                            "no mutation crossed the boundary; the journal "
                            "claim path recovers it on the next merge attempt"
                        ),
                        evidence={
                            "repository": str(row["repository"]),
                            "pr_number": int(row["pr_number"]),
                            "claim_expired": (
                                expires is None or str(expires) <= now_iso
                            ),
                        },
                    )
                )

    def _project_attempted_merge(self, row: Any) -> Finding:
        """Resolve one fenced merge attempt by an exact read-only projection.

        The durable journal row is never touched here: the ``attempted``
        fence stays in place forever so no duplicate merge can ever be
        authorized; only the finding changes.
        """

        effect_id = str(row["effect_id"])
        repository = str(row["repository"])
        pr_number = int(row["pr_number"])
        head_sha = str(row["head_sha"])
        evidence: dict[str, Any] = {
            "repository": repository,
            "pr_number": pr_number,
            "head_sha": head_sha,
        }
        if self._github_reads is None:
            return Finding(
                kind="merge_outcome_unknown",
                subsystem="github",
                severity="blocking",
                aggregate_id=effect_id,
                recommended_action=(
                    "project the pull request read-only and resolve the "
                    "fenced mutation; the journal fence already prevents a "
                    "duplicate merge"
                ),
                evidence=evidence,
            )
        try:
            pull = self._github_reads.get_pull_request(repository, pr_number)
        except Exception as error:
            return Finding(
                kind="merge_state_unavailable",
                subsystem="github",
                severity="blocking",
                aggregate_id=effect_id,
                recommended_action=(
                    "the pull request could not be read, so the fenced "
                    "mutation outcome stays unknown; admission stays closed"
                ),
                evidence={**evidence, "error": type(error).__name__},
            )
        observed_head = str(pull.head_sha)
        if bool(pull.merged):
            if observed_head == head_sha:
                return Finding(
                    kind="merge_outcome_externally_merged",
                    subsystem="github",
                    severity="info",
                    aggregate_id=effect_id,
                    recommended_action=(
                        "the exact reviewed head is merged; the attempted "
                        "fence stays in place so no duplicate merge is "
                        "possible"
                    ),
                    evidence=evidence,
                )
            return Finding(
                kind="merge_head_mismatch",
                subsystem="github",
                severity="blocking",
                aggregate_id=effect_id,
                recommended_action=(
                    "the merged pull request head is not the reviewed "
                    "head; audit the foreign merge before reopening "
                    "admission"
                ),
                evidence={**evidence, "observed_head_sha": observed_head},
            )
        return Finding(
            kind="merge_outcome_unconfirmed",
            subsystem="github",
            severity="blocking",
            aggregate_id=effect_id,
            recommended_action=(
                "the attempt crossed the mutation boundary but the pull "
                "request is not merged; resolve the fenced mutation "
                "explicitly before reopening admission"
            ),
            evidence={
                **evidence,
                "observed_state": str(pull.state),
                "observed_head_sha": observed_head,
            },
        )

    # -- stage 6: circleci -------------------------------------------------

    def _stage_circleci(self, findings: list[Finding]) -> None:
        for alias, project in self._projects.items():
            if project.ci == "none":
                findings.append(
                    Finding(
                        kind="ci_not_configured",
                        subsystem="circleci",
                        severity="info",
                        aggregate_id=alias,
                        recommended_action=(
                            "none; merged candidates resolve durably as "
                            "ci_not_configured with zero CircleCI calls"
                        ),
                    )
                )
        rows = self._database.execute(
            "SELECT project_key, merge_sha, repository, integration_branch, "
            "state FROM ci_merge_ledger "
            "WHERE state IN ('unresolved', 'failed') "
            "ORDER BY recorded_at ASC, rowid ASC"
        ).fetchall()
        for row in rows:
            project_key = str(row["project_key"])
            merge_sha = str(row["merge_sha"])
            aggregate_id = f"{project_key}:{merge_sha[:12]}"
            project = self._projects.get(project_key)
            known_outcome: str | None = None
            if project is None or project.ci != "none":
                # A circleci-configured (or unknown, hence fail-closed)
                # project must project the recorded merge onto its known
                # CI outcome by exact identity before admission may open.
                if self._ci_states is None:
                    findings.append(
                        self._ci_unavailable(aggregate_id, "port_not_wired")
                    )
                    continue
                try:
                    known_outcome = self._ci_states.known_state(
                        str(row["repository"]),
                        str(row["integration_branch"]),
                        merge_sha,
                    )
                except Exception as error:
                    findings.append(
                        self._ci_unavailable(
                            aggregate_id, type(error).__name__
                        )
                    )
                    continue
            if str(row["state"]) == "unresolved":
                findings.append(
                    Finding(
                        kind="ci_merge_unresolved",
                        subsystem="circleci",
                        severity="warning",
                        aggregate_id=aggregate_id,
                        recommended_action=(
                            "the CI window reconciles this at the next "
                            "intake boundary; nothing is resolved here"
                        ),
                        evidence=(
                            {"known_outcome": known_outcome}
                            if known_outcome is not None
                            else {}
                        ),
                    )
                )
                continue
            if known_outcome is not None and known_outcome != "failure":
                findings.append(
                    Finding(
                        kind="ci_state_mismatch",
                        subsystem="circleci",
                        severity="blocking",
                        aggregate_id=aggregate_id,
                        recommended_action=(
                            "the durable ledger records a terminal CI "
                            "failure but the known CI outcome disagrees; "
                            "audit the ledger before reopening admission"
                        ),
                        evidence={"known_outcome": known_outcome},
                    )
                )
                continue
            routed = self._database.scalar(
                "SELECT count(*) FROM lead_corrections "
                "WHERE project_key = ? AND state IN ('pending', 'acknowledged')",
                (project_key,),
            )
            if int(routed or 0) > 0:
                findings.append(
                    Finding(
                        kind="ci_failure_pending_correction",
                        subsystem="circleci",
                        severity="info",
                        aggregate_id=aggregate_id,
                        recommended_action=(
                            "the failure packet is already routed to the "
                            "lead outbox; intake stays blocked until it is "
                            "corrected"
                        ),
                    )
                )
            else:
                findings.append(
                    Finding(
                        kind="ci_failure_unrouted",
                        subsystem="circleci",
                        severity="blocking",
                        aggregate_id=aggregate_id,
                        recommended_action=(
                            "a stored CI failure blocks intake but no "
                            "correction packet is routed; deliver the packet "
                            "before reopening admission"
                        ),
                    )
                )
        claims = self._database.execute(
            "SELECT project_key, event_id, lease_expires_at "
            "FROM ci_reconciliation_claims WHERE state = 'claimed'"
        ).fetchall()
        for claim in claims:
            findings.append(
                Finding(
                    kind="ci_claim_stranded",
                    subsystem="circleci",
                    severity="warning",
                    aggregate_id=(
                        f"{claim['project_key']}:{claim['event_id']}"
                    ),
                    recommended_action=(
                        "an expired claim permits exactly one atomic "
                        "recovery at the next intake boundary"
                    ),
                    evidence={
                        "lease_expires_at": str(claim["lease_expires_at"]),
                    },
                )
            )

    @staticmethod
    def _ci_unavailable(aggregate_id: str, reason: str) -> Finding:
        return Finding(
            kind="ci_state_unavailable",
            subsystem="circleci",
            severity="blocking",
            aggregate_id=aggregate_id,
            recommended_action=(
                "the known CI outcome for this recorded merge could not "
                "be read; admission stays closed until it can"
            ),
            evidence={"reason": reason},
        )

    # -- stage 7: linear ---------------------------------------------------

    def _stage_linear(self, findings: list[Finding]) -> None:
        rows = self._database.execute(
            "SELECT effect_id, target, request_json FROM external_effects "
            "WHERE adapter = 'linear' AND state = 'pending'"
        ).fetchall()
        for row in rows:
            if self._linear_reads is None:
                findings.append(
                    Finding(
                        kind="linear_effect_ambiguous",
                        subsystem="linear",
                        severity="warning",
                        aggregate_id=str(row["effect_id"]),
                        recommended_action=(
                            "re-read the issue read-only and compare it "
                            "against the journaled request before any "
                            "repeated projection"
                        ),
                        evidence={"target": str(row["target"])},
                    )
                )
            else:
                findings.append(self._project_pending_linear_effect(row))
        placeholders = ",".join("?" for _ in _LIVE_CELL_STATES)
        issues = self._database.execute(
            "SELECT issue_id, project_key FROM admitted_issues "
            "WHERE state IN ('in_development', 'review') "
            "AND project_key NOT IN ("
            "SELECT project_key FROM project_cells "
            f"WHERE state IN ({placeholders}))",
            _LIVE_CELL_STATES,
        ).fetchall()
        for issue in issues:
            findings.append(
                Finding(
                    kind="issue_without_cell",
                    subsystem="linear",
                    severity="info",
                    aggregate_id=str(issue["issue_id"]),
                    recommended_action=(
                        "the scheduler resumes or restarts the project cell "
                        "once admission opens"
                    ),
                    evidence={"project_key": str(issue["project_key"])},
                )
            )


    def _project_pending_linear_effect(self, row: Any) -> Finding:
        """Resolve one pending Linear effect by an exact read-only read.

        The journal row is never completed or mutated here and no Linear
        mutation is ever issued; only the finding changes. Resolution
        requires every configured identity — project, team, state, and
        assignee ids — to be exact. Unprovable effects remain pending and
        visible, but Linear is a projection and cannot close global admission.
        """

        effect_id = str(row["effect_id"])
        assert self._linear_expectations is not None
        expectations = self._linear_expectations
        try:
            request = json.loads(row["request_json"])
        except (TypeError, ValueError):
            request = None
        issue_id = (
            str(request.get("issue_id"))
            if isinstance(request, dict) and request.get("issue_id")
            else str(row["target"])
        )
        target = request.get("target") if isinstance(request, dict) else None
        project_key = self._database.scalar(
            "SELECT project_key FROM admitted_issues WHERE issue_id = ?",
            (issue_id,),
        )
        expected_team = (
            expectations.team_ids.get(str(project_key))
            if project_key is not None
            else None
        )
        status_ids = (
            expectations.status_ids.get(str(project_key), {})
            if project_key is not None
            else {}
        )
        target_status = (
            target.get("status") if isinstance(target, dict) else None
        )
        target_alias = (
            target.get("assignee_alias") if isinstance(target, dict) else None
        )
        expected_state_id = (
            status_ids.get(str(target_status))
            if target_status is not None
            else None
        )
        expected_assignee = expectations.assignee_ids.get(str(target_alias))
        unresolvable = (
            not isinstance(target, dict)
            or expected_team is None
            or expected_assignee is None
            or (target_status is not None and expected_state_id is None)
        )
        if unresolvable:
            return Finding(
                kind="linear_projection_unresolvable",
                subsystem="linear",
                severity="warning",
                aggregate_id=effect_id,
                recommended_action=(
                    "the journaled target cannot be bound to exact "
                    "configured identities; audit the effect before retrying it"
                ),
                evidence={"issue_id": issue_id},
            )
        try:
            issue = self._linear_reads.get_issue(issue_id)  # type: ignore[union-attr]
        except Exception as error:
            return Finding(
                kind="linear_state_unavailable",
                subsystem="linear",
                severity="warning",
                aggregate_id=effect_id,
                recommended_action=(
                    "the issue could not be read, so the pending effect "
                    "outcome stays unknown; unrelated work may continue"
                ),
                evidence={
                    "issue_id": issue_id,
                    "error": type(error).__name__,
                },
            )
        evidence: dict[str, Any] = {"issue_id": issue_id}
        if str(issue.issue_id) != issue_id:
            return Finding(
                kind="linear_identity_mismatch",
                subsystem="linear",
                severity="warning",
                aggregate_id=effect_id,
                recommended_action=(
                    "the read returned a different issue identity; "
                    "nothing may be resolved from it"
                ),
                evidence={**evidence, "observed": str(issue.issue_id)},
            )
        if str(issue.team_id) != expected_team:
            return Finding(
                kind="linear_team_mismatch",
                subsystem="linear",
                severity="warning",
                aggregate_id=effect_id,
                recommended_action=(
                    "the issue is not in the configured Linear team; "
                    "audit the routing before retrying this projection"
                ),
                evidence={**evidence, "observed_team": str(issue.team_id)},
            )
        applied = (
            expected_state_id is None
            or str(issue.state_id) == expected_state_id
        ) and issue.assignee_id == expected_assignee
        if applied:
            return Finding(
                kind="linear_effect_applied",
                subsystem="linear",
                severity="info",
                aggregate_id=effect_id,
                recommended_action=(
                    "the exact journaled target already holds; the "
                    "idempotent projection path completes the journal on "
                    "its next run"
                ),
                evidence=evidence,
            )
        source_revision = (
            request.get("source_revision") if isinstance(request, dict) else None
        )
        if source_revision is not None and str(issue.revision) == str(
            source_revision
        ):
            return Finding(
                kind="linear_effect_unapplied",
                subsystem="linear",
                severity="warning",
                aggregate_id=effect_id,
                recommended_action=(
                    "the issue is provably unchanged since the journaled "
                    "read, so the crash happened before the mutation "
                    "boundary; the idempotent projection path retries it "
                    "safely"
                ),
                evidence={**evidence, "revision": str(issue.revision)},
            )
        return Finding(
            kind="linear_state_mismatch",
            subsystem="linear",
            severity="warning",
            aggregate_id=effect_id,
            recommended_action=(
                "the issue changed after the journaled read without "
                "reaching the target; audit before any repeated projection"
            ),
            evidence={
                **evidence,
                "observed_state_id": str(issue.state_id),
                "observed_revision": str(issue.revision),
            },
        )


def summarize(findings: Iterable[Finding]) -> tuple[str, ...]:
    """Compact per-finding labels for legacy string consumers."""

    return tuple(finding.label for finding in findings)
