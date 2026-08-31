"""Ordered cross-system startup reconciliation (INFRA-172).

The Reconciler runs its stages in one fixed, documented order over durable
SQLite state and read-only projections, produces structured findings, and
fails closed: conflicting or unknown evidence keeps admission shut. Every
effect it triggers goes through an existing journaled recovery path — it
never signals a process, mutates GitHub, CircleCI, or Linear, or resets an
in-flight transition to make records agree.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from hermes_orchestrator.config import PolicyConfig, ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.linear import LinearIssue
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.reconcile import (
    STAGE_ORDER,
    Finding,
    LinearExpectations,
    ManagedProcessScanner,
    Reconciler,
    ReconciliationReport,
)
from hermes_orchestrator.scheduler import Scheduler
from hermes_orchestrator.service import OrchestratorService
from hermes_orchestrator.worktrees import (
    WorktreeCustodian,
    WorktreeLeaseInput,
    WorktreeLeases,
)
from tests.test_service import FakeSampler
from tests.test_worktrees import REPO, SHA_A, WORKTREE, FakeGit, FakeRegistry

NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)
PAST = (NOW - timedelta(hours=2)).isoformat()
FUTURE = (NOW + timedelta(hours=2)).isoformat()


@dataclass
class FakeLease:
    lease_id: str
    state: str = "active"
    cwd: str | None = None
    project_key: str = "demo"
    pid: int = 901
    kind: str = "claude"
    worker_id: str | None = None


@dataclass
class FakeProcesses:
    """Process evidence port: journaled expiry plus orphan detection."""

    dead: list[str] = field(default_factory=list)
    orphans: list[FakeLease] = field(default_factory=list)
    expire_calls: int = 0

    def expire_dead(self) -> list[str]:
        self.expire_calls += 1
        return list(self.dead)

    def find_orphans(self) -> list[FakeLease]:
        return list(self.orphans)


class Clock:
    def __init__(self) -> None:
        self.value = NOW

    def now(self) -> datetime:
        return self.value


class BrokenIntegrityDatabase:
    """Delegate to a real database but fail the integrity pragma."""

    def __init__(self, inner: Database) -> None:
        self._inner = inner

    def scalar(self, sql: str, parameters: tuple[Any, ...] = ()) -> object:
        if "integrity_check" in sql:
            return "row 7 missing from index sqlite_autoindex"
        return self._inner.scalar(sql, parameters)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class RecordingCustodian:
    """Custodian port that must never be reached."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def reconcile(self, lease_id: str) -> None:
        self.calls.append(("reconcile", lease_id))

    def converge_missing(self, lease_id: str) -> None:
        self.calls.append(("converge_missing", lease_id))


class RecordingGit:
    """Wrap a git port and record every method invocation."""

    def __init__(self, inner: object) -> None:
        self._inner = inner
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._inner, name)
        if not callable(attribute):
            return attribute

        def record(*args: Any, **kwargs: Any) -> Any:
            self.calls.append(name)
            return attribute(*args, **kwargs)

        return record


class FakeGitHubReads:
    """Read-only pull-request port with a scripted response."""

    def __init__(
        self, pull: object | None = None, error: Exception | None = None
    ) -> None:
        self.pull = pull
        self.error = error
        self.calls: list[tuple[str, int]] = []

    def get_pull_request(self, repository: str, number: int) -> object:
        self.calls.append((repository, number))
        if self.error is not None:
            raise self.error
        assert self.pull is not None
        return self.pull


class FakeLinearReads:
    """Read-only Linear issue port with a scripted snapshot."""

    def __init__(
        self, issue: LinearIssue | None = None, error: Exception | None = None
    ) -> None:
        self.issue = issue
        self.error = error
        self.calls: list[str] = []

    def get_issue(self, issue_id: str) -> LinearIssue:
        self.calls.append(issue_id)
        if self.error is not None:
            raise self.error
        assert self.issue is not None
        return self.issue


class FakeCiStates:
    """Read-only known-state CI port with a scripted outcome."""

    def __init__(
        self, outcome: str | None = None, error: Exception | None = None
    ) -> None:
        self.outcome = outcome
        self.error = error
        self.calls: list[tuple[str, str, str]] = []

    def known_state(
        self, repository: str, integration_branch: str, merge_sha: str
    ) -> str:
        self.calls.append((repository, integration_branch, merge_sha))
        if self.error is not None:
            raise self.error
        assert self.outcome is not None
        return self.outcome


def expectations() -> LinearExpectations:
    return LinearExpectations(
        team_ids={"demo": "team-eng"},
        status_ids={
            "demo": {
                "Todo": "state-todo",
                "In Development": "state-dev",
                "Review": "state-review",
                "QA": "state-qa",
                "Done": "state-done",
            }
        },
        assignee_ids={"operator": "user-operator", "ryan": "user-ryan"},
    )


def linear_issue(**overrides: Any) -> LinearIssue:
    values: dict[str, Any] = {
        "issue_id": "ENG-431",
        "linear_id": "lin-1",
        "status": "Review",
        "state_id": "state-review",
        "assignee_id": "user-ryan",
        "team_id": "team-eng",
        "revision": "rev-1",
    }
    values.update(overrides)
    return LinearIssue(**values)


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


def build(database: Database, events: EventStore, **overrides: object) -> Reconciler:
    options: dict[str, object] = {
        "pid_exists": lambda _pid: False,
        "now": lambda: NOW,
    }
    options.update(overrides)
    return Reconciler(database, events, **options)


def seed(database: Database, sql: str, parameters: tuple[object, ...]) -> None:
    with database.transaction() as connection:
        connection.execute(sql, parameters)


def project(tmp_path: Path, ci: str) -> ProjectConfig:
    return ProjectConfig(
        linear_team="ENG",
        repo_path=tmp_path,
        integration_branch="main",
        github_repo="acme/demo",
        ci=ci,
    )


def by_kind(report: ReconciliationReport, kind: str) -> list[Finding]:
    return [finding for finding in report.findings if finding.kind == kind]


# -- stage order and report structure ------------------------------------


def test_stages_run_in_documented_order(
    database: Database, events: EventStore
) -> None:
    report = build(database, events).run()
    assert STAGE_ORDER == (
        "sqlite",
        "process_leases",
        "worker_sessions",
        "worktrees",
        "github",
        "circleci",
        "linear",
        "admission",
    )
    assert report.stages == STAGE_ORDER
    row = database.execute(
        "SELECT state, findings_json FROM reconciliation_runs"
    ).fetchone()
    assert row is not None and row["state"] == "completed"
    types = [record.event_type for record in events.list_after(0)]
    assert types[0] == "reconciliation.started"
    assert types[-1] == "reconciliation.completed"


def test_clean_state_opens_admission(
    database: Database, events: EventStore
) -> None:
    report = build(database, events).run()
    assert report.completed is True
    assert report.findings == ()
    assert report.safe_to_open_admission is True


def test_finding_structure_is_complete(
    database: Database, events: EventStore
) -> None:
    processes = FakeProcesses(orphans=[FakeLease("proc-1", cwd="/tmp/wt")])
    report = build(database, events, processes=processes).run()
    finding = by_kind(report, "orphan_process")[0]
    assert finding.subsystem == "process_leases"
    assert finding.severity == "blocking"
    assert finding.aggregate_id == "proc-1"
    assert finding.evidence["cwd"] == "/tmp/wt"
    assert finding.recommended_action
    assert finding.blocking is True
    stored = json.loads(
        database.scalar("SELECT findings_json FROM reconciliation_runs")
    )
    assert stored[0]["kind"] == "orphan_process"


def test_schema_version_mismatch_blocks(
    database: Database, events: EventStore
) -> None:
    report = build(database, events, expected_schema_version=99).run()
    finding = by_kind(report, "schema_version_mismatch")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False


def test_prior_running_reconciliation_reported_not_reset(
    database: Database, events: EventStore
) -> None:
    seed(
        database,
        "INSERT INTO reconciliation_runs(run_id, state, started_at) "
        "VALUES ('run-crashed', 'running', ?)",
        (PAST,),
    )
    report = build(database, events).run()
    finding = by_kind(report, "prior_reconciliation_crashed")[0]
    assert finding.blocking is False
    assert report.safe_to_open_admission is True
    assert (
        database.scalar(
            "SELECT state FROM reconciliation_runs WHERE run_id = 'run-crashed'"
        )
        == "running"
    )


# -- sqlite hard abort ----------------------------------------------------


def unreliable_state_stack(
    database: Database, events: EventStore, tmp_path: Path
) -> dict[str, Any]:
    """Seed every mutation-capable stage and wrap it in recording ports."""

    worker_lease(database, "wl-dead", 4242)
    fake_git = FakeGit(worktree_paths=[REPO])
    clock = Clock()
    leases = WorktreeLeases(database, events, now=clock.now)
    lease_id = register_worktree(leases)
    leases.record_checkpoint(
        lease_id, checkpoint_id="ck-1", sha=SHA_A, message="wip"
    )
    clock.value = NOW - timedelta(hours=2)
    leases.claim_cleanup(lease_id, owner="crashed-owner")
    clock.value = NOW
    merge_effect(database, "gme-1", "attempted", None)
    linear_effect(database)
    admitted_issue(database)
    ledger_row(database, "m" * 40, "failed", '{"reason":"tests"}')
    pid_calls: list[int] = []

    def pid_exists(pid: int) -> bool:
        pid_calls.append(pid)
        return False

    scan_calls: list[str] = []

    def unknown_processes() -> tuple[int, ...]:
        scan_calls.append("scan")
        return (900,)

    return {
        "processes": FakeProcesses(dead=["proc-dead"]),
        "leases": leases,
        "lease_id": lease_id,
        "custodian": RecordingCustodian(),
        "git": RecordingGit(fake_git),
        "pid_exists": pid_exists,
        "pid_calls": pid_calls,
        "unknown_processes": unknown_processes,
        "scan_calls": scan_calls,
        "github_reads": FakeGitHubReads(
            pull=SimpleNamespace(merged=True, head_sha=SHA_A, state="closed")
        ),
        "linear_reads": FakeLinearReads(issue=linear_issue()),
        "ci_states": FakeCiStates(outcome="failure"),
        "projects": {"demo": project(tmp_path, "circleci")},
    }


def assert_no_mutation_capable_stage_ran(
    database: Database,
    events: EventStore,
    report: ReconciliationReport,
    stack: dict[str, Any],
) -> None:
    assert report.completed is False
    assert report.stages == ("sqlite",)
    assert report.safe_to_open_admission is False
    assert report.findings != ()
    assert all(finding.subsystem == "sqlite" for finding in report.findings)
    # No journaled recovery path, git call, process probe, scan, or
    # external adapter read crossed the abort boundary.
    assert stack["processes"].expire_calls == 0
    assert stack["custodian"].calls == []
    assert stack["git"].calls == []
    assert stack["pid_calls"] == []
    assert stack["scan_calls"] == []
    assert stack["github_reads"].calls == []
    assert stack["linear_reads"].calls == []
    assert stack["ci_states"].calls == []
    # Every seeded transition is untouched.
    assert (
        database.scalar(
            "SELECT state FROM worker_leases WHERE lease_id = 'wl-dead'"
        )
        == "active"
    )
    lease = stack["leases"].get(stack["lease_id"])
    assert lease.state == "reclaiming"
    assert lease.cleanup_owner == "crashed-owner"
    types = [record.event_type for record in events.list_after(0)]
    assert "worker_lease.expired" not in types
    # The failed report is persisted from the safe sqlite evidence only.
    row = database.execute(
        "SELECT state, findings_json FROM reconciliation_runs "
        "WHERE run_id = ?",
        (report.run_id,),
    ).fetchone()
    assert row is not None and row["state"] == "failed"
    stored = json.loads(row["findings_json"])
    assert stored and all(item["subsystem"] == "sqlite" for item in stored)


def test_failed_integrity_aborts_all_mutation_capable_stages(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    stack = unreliable_state_stack(database, events, tmp_path)
    reconciler = Reconciler(
        BrokenIntegrityDatabase(database),
        events,
        projects=stack["projects"],
        processes=stack["processes"],
        worktrees=stack["leases"],
        custodian=stack["custodian"],
        git=stack["git"],
        pid_exists=stack["pid_exists"],
        unknown_processes=stack["unknown_processes"],
        github_reads=stack["github_reads"],
        linear_reads=stack["linear_reads"],
        linear_expectations=expectations(),
        ci_states=stack["ci_states"],
        now=lambda: NOW,
    )
    report = reconciler.run()
    assert [f.kind for f in by_kind(report, "sqlite_integrity_failed")]
    assert_no_mutation_capable_stage_ran(database, events, report, stack)


def test_schema_mismatch_aborts_all_mutation_capable_stages(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    stack = unreliable_state_stack(database, events, tmp_path)
    reconciler = Reconciler(
        database,
        events,
        projects=stack["projects"],
        processes=stack["processes"],
        worktrees=stack["leases"],
        custodian=stack["custodian"],
        git=stack["git"],
        pid_exists=stack["pid_exists"],
        unknown_processes=stack["unknown_processes"],
        github_reads=stack["github_reads"],
        linear_reads=stack["linear_reads"],
        linear_expectations=expectations(),
        ci_states=stack["ci_states"],
        expected_schema_version=99,
        now=lambda: NOW,
    )
    report = reconciler.run()
    assert [f.kind for f in by_kind(report, "schema_version_mismatch")]
    assert_no_mutation_capable_stage_ran(database, events, report, stack)


# -- process leases -------------------------------------------------------


def test_orphan_process_keeps_admission_closed(
    database: Database, events: EventStore
) -> None:
    processes = FakeProcesses(
        dead=["proc-dead"], orphans=[FakeLease("proc-1", cwd="/tmp/wt")]
    )
    report = build(database, events, processes=processes).run()
    assert processes.expire_calls == 1
    assert by_kind(report, "process_lease_expired")[0].blocking is False
    assert by_kind(report, "orphan_process")[0].blocking is True
    assert report.safe_to_open_admission is False


def activation_apply(
    database: Database,
    *,
    apply_id: str,
    target_checkout: str,
    state: str,
) -> None:
    seed(
        database,
        "INSERT INTO activation_applies("
        "apply_id, target_checkout, prior_generation, target_generation, "
        "state, reason, created_at, updated_at"
        ") VALUES (?, ?, NULL, NULL, ?, 'test', ?, ?)",
        (apply_id, target_checkout, state, PAST, PAST),
    )


def journal_applier_spawned(
    database: Database,
    events: EventStore,
    *,
    apply_id: str,
    lease_id: str | None,
    pid: int = 901,
) -> None:
    """Journal the durable activation-to-lease binding the way the
    post-merge advance does: ``activation.applier_spawned`` on the
    ``activate:<sha>`` intent, carrying the registered lease_id (null
    when the spawn ran with no registry)."""

    with database.transaction() as connection:
        events.append(
            connection,
            EventInput(
                event_type="activation.applier_spawned",
                aggregate_type="activation_apply",
                aggregate_id=apply_id,
                payload={
                    "apply_id": apply_id,
                    "pid": pid,
                    "lease_id": lease_id,
                },
            ),
        )


def test_expected_applier_lease_is_expected_while_unrelated_lease_blocks(
    database: Database, events: EventStore
) -> None:
    merge_sha = "e" * 40
    target_checkout = f"/state/checkouts/{merge_sha}"
    activation_apply(
        database,
        apply_id=f"activate:{merge_sha}",
        target_checkout=target_checkout,
        state="intended",
    )
    journal_applier_spawned(
        database, events, apply_id=f"activate:{merge_sha}", lease_id="applier-1"
    )
    processes = FakeProcesses(
        orphans=[
            FakeLease(
                "applier-1",
                kind="runtime_applier",
                worker_id=merge_sha,
                cwd=target_checkout,
            ),
            FakeLease("proc-1", cwd="/tmp/wt"),
        ]
    )
    report = build(database, events, processes=processes).run()
    expected = by_kind(report, "expected_applier_lease")[0]
    assert expected.severity == "info"
    assert expected.aggregate_id == "applier-1"
    assert expected.evidence["worker_id"] == merge_sha
    unrelated = by_kind(report, "orphan_process")[0]
    assert unrelated.aggregate_id == "proc-1"
    assert unrelated.blocking is True
    # The unrelated orphan alone keeps admission closed.
    assert report.safe_to_open_admission is False


def test_expected_applier_lease_via_the_applier_own_apply_row(
    database: Database, events: EventStore
) -> None:
    merge_sha = "f" * 40
    target_checkout = f"/state/checkouts/{merge_sha}"
    # The primary intent row already moved past 'intended' (a prior tick
    # marked it verified/etc is not modeled here; only that it no longer
    # reads 'intended'), but the applier's own progress row proves it is
    # still mid-flight.
    activation_apply(
        database,
        apply_id=f"activate:{merge_sha}",
        target_checkout=target_checkout,
        state="activated",
    )
    activation_apply(
        database,
        apply_id="applier-uuid-1",
        target_checkout=target_checkout,
        state="restarted",
    )
    journal_applier_spawned(
        database, events, apply_id=f"activate:{merge_sha}", lease_id="applier-2"
    )
    processes = FakeProcesses(
        orphans=[
            FakeLease(
                "applier-2",
                kind="runtime_applier",
                worker_id=merge_sha,
                cwd=target_checkout,
            ),
        ]
    )
    report = build(database, events, processes=processes).run()
    expected = by_kind(report, "expected_applier_lease")[0]
    assert expected.blocking is False
    assert report.safe_to_open_admission is True


def test_unbound_runtime_applier_lease_stays_a_blocking_orphan(
    database: Database, events: EventStore
) -> None:
    # A runtime_applier lease with no matching activation intent at all
    # (e.g. a stale row from a wholly different life) is never excused.
    processes = FakeProcesses(
        orphans=[
            FakeLease(
                "applier-3", kind="runtime_applier", worker_id="c" * 40
            ),
        ]
    )
    report = build(database, events, processes=processes).run()
    assert by_kind(report, "expected_applier_lease") == []
    finding = by_kind(report, "orphan_process")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False


def test_only_the_journal_bound_applier_lease_is_excused_among_duplicates(
    database: Database, events: EventStore
) -> None:
    """Two live runtime_applier leases share the same worker_id but only
    one lease_id was ever journaled for the activation (Sol correction
    c5600e31): exactly the bound lease is excused; the duplicate — a
    (kind, worker_id) match with a mismatched lease_id — stays a
    blocking orphan."""

    merge_sha = "e" * 40
    target_checkout = f"/state/checkouts/{merge_sha}"
    activation_apply(
        database,
        apply_id=f"activate:{merge_sha}",
        target_checkout=target_checkout,
        state="intended",
    )
    journal_applier_spawned(
        database, events, apply_id=f"activate:{merge_sha}", lease_id="applier-bound"
    )
    processes = FakeProcesses(
        orphans=[
            FakeLease(
                "applier-bound",
                kind="runtime_applier",
                worker_id=merge_sha,
                cwd=target_checkout,
            ),
            FakeLease(
                "applier-duplicate",
                kind="runtime_applier",
                worker_id=merge_sha,
                cwd=target_checkout,
            ),
        ]
    )
    report = build(database, events, processes=processes).run()
    excused = by_kind(report, "expected_applier_lease")
    assert [finding.aggregate_id for finding in excused] == ["applier-bound"]
    blocked = by_kind(report, "orphan_process")
    assert [finding.aggregate_id for finding in blocked] == ["applier-duplicate"]
    assert blocked[0].blocking is True
    assert report.safe_to_open_admission is False


@pytest.mark.parametrize("journaled_lease_id", [None, "some-other-lease"])
def test_applier_lease_without_exact_journaled_binding_stays_blocking(
    database: Database, events: EventStore, journaled_lease_id: str | None
) -> None:
    """A null journaled lease_id (a spawn with no registry) excuses
    nothing, and a journaled lease_id naming a different lease binds
    only that lease — either way this survivor fails closed."""

    merge_sha = "e" * 40
    target_checkout = f"/state/checkouts/{merge_sha}"
    activation_apply(
        database,
        apply_id=f"activate:{merge_sha}",
        target_checkout=target_checkout,
        state="intended",
    )
    journal_applier_spawned(
        database,
        events,
        apply_id=f"activate:{merge_sha}",
        lease_id=journaled_lease_id,
    )
    processes = FakeProcesses(
        orphans=[
            FakeLease(
                "applier-1",
                kind="runtime_applier",
                worker_id=merge_sha,
                cwd=target_checkout,
            ),
        ]
    )
    report = build(database, events, processes=processes).run()
    assert by_kind(report, "expected_applier_lease") == []
    finding = by_kind(report, "orphan_process")[0]
    assert finding.aggregate_id == "applier-1"
    assert finding.blocking is True
    assert report.safe_to_open_admission is False


def test_applier_lease_with_absent_spawn_journal_stays_blocking(
    database: Database, events: EventStore
) -> None:
    # An open intent and a matching (kind, worker_id) alone were enough
    # before Sol correction c5600e31; without the journaled
    # activation.applier_spawned binding they no longer excuse anything.
    merge_sha = "e" * 40
    target_checkout = f"/state/checkouts/{merge_sha}"
    activation_apply(
        database,
        apply_id=f"activate:{merge_sha}",
        target_checkout=target_checkout,
        state="intended",
    )
    processes = FakeProcesses(
        orphans=[
            FakeLease(
                "applier-1",
                kind="runtime_applier",
                worker_id=merge_sha,
                cwd=target_checkout,
            ),
        ]
    )
    report = build(database, events, processes=processes).run()
    assert by_kind(report, "expected_applier_lease") == []
    assert by_kind(report, "orphan_process")[0].blocking is True
    assert report.safe_to_open_admission is False


def test_applier_lease_with_mismatched_cwd_stays_blocking(
    database: Database, events: EventStore
) -> None:
    """The journaled lease_id matches but the lease works somewhere other
    than the intent's exact target checkout: never excused."""

    merge_sha = "e" * 40
    target_checkout = f"/state/checkouts/{merge_sha}"
    activation_apply(
        database,
        apply_id=f"activate:{merge_sha}",
        target_checkout=target_checkout,
        state="intended",
    )
    journal_applier_spawned(
        database, events, apply_id=f"activate:{merge_sha}", lease_id="applier-1"
    )
    processes = FakeProcesses(
        orphans=[
            FakeLease(
                "applier-1",
                kind="runtime_applier",
                worker_id=merge_sha,
                cwd="/somewhere/else",
            ),
        ]
    )
    report = build(database, events, processes=processes).run()
    assert by_kind(report, "expected_applier_lease") == []
    finding = by_kind(report, "orphan_process")[0]
    assert finding.aggregate_id == "applier-1"
    assert finding.blocking is True
    assert report.safe_to_open_admission is False


def test_unknown_live_process_keeps_admission_closed(
    database: Database, events: EventStore
) -> None:
    report = build(database, events, unknown_processes=lambda: (900,)).run()
    finding = by_kind(report, "unknown_live_process")[0]
    assert finding.blocking is True
    assert finding.aggregate_id == "900"
    assert report.safe_to_open_admission is False


def test_incomplete_stop_is_reported_never_reset(
    database: Database, events: EventStore
) -> None:
    seed(
        database,
        "INSERT INTO process_leases("
        "lease_id, project_key, kind, pid, pgid, create_time, state, "
        "stop_owner, stop_phase, stop_claim_expires_at, stop_checkpoint_id, "
        "acquired_at, updated_at"
        ") VALUES ('proc-2', 'demo', 'lead', 500, 500, 1000.0, 'stopping', "
        "'owner-1', 'term_sent', ?, 'ck-9', ?, ?)",
        (PAST, PAST, PAST),
    )
    report = build(database, events).run()
    finding = by_kind(report, "incomplete_stop")[0]
    assert finding.blocking is True
    assert finding.evidence["stop_phase"] == "term_sent"
    assert finding.evidence["stop_checkpoint_id"] == "ck-9"
    row = database.execute(
        "SELECT state, stop_phase FROM process_leases WHERE lease_id = 'proc-2'"
    ).fetchone()
    assert (row["state"], row["stop_phase"]) == ("stopping", "term_sent")


# -- production unknown-process discovery ---------------------------------


def proc(
    pid: int,
    name: str,
    cwd: str | None,
    *,
    create_time: float | None = 100.0,
    cmdline: list[str] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        info={
            "pid": pid,
            "name": name,
            "cwd": cwd,
            "cmdline": cmdline if cmdline is not None else [name],
            "create_time": create_time,
        }
    )


def scanner_for(
    database: Database,
    root: Path,
    procs: list[SimpleNamespace],
    **overrides: Any,
) -> ManagedProcessScanner:
    options: dict[str, Any] = {
        "roots": (root,),
        "iter_processes": lambda: iter(procs),
        "self_pid": 1,
        "getpgid": lambda pid: pid,
    }
    options.update(overrides)
    return ManagedProcessScanner(database, **options)


def process_lease(
    database: Database, lease_id: str, pid: int, pgid: int, create_time: float
) -> None:
    seed(
        database,
        "INSERT INTO process_leases("
        "lease_id, project_key, kind, pid, pgid, executable, cwd, "
        "create_time, state, acquired_at, updated_at"
        ") VALUES (?, 'demo', 'lead', ?, ?, 'claude', NULL, ?, 'active', "
        "?, ?)",
        (lease_id, pid, pgid, create_time, PAST, PAST),
    )


def test_scanner_reports_unleased_managed_looking_process(
    database: Database, tmp_path: Path
) -> None:
    stray = proc(4321, "claude", str(tmp_path / "wt"))
    helper = proc(
        4322,
        "python3.12",
        str(tmp_path / "wt"),
        cmdline=["python3.12", "-m", "hermes_orchestrator.cli", "subagent-gate"],
    )
    scanner = scanner_for(database, tmp_path, [stray, helper])
    assert scanner() == (4321, 4322)


def test_scanner_excludes_unrelated_host_processes(
    database: Database, tmp_path: Path
) -> None:
    procs = [
        # Ordinary host process inside a managed root.
        proc(101, "Safari", str(tmp_path / "wt")),
        # Managed-looking executable working somewhere else entirely.
        proc(102, "claude", "/private/tmp/elsewhere"),
        # Python without the orchestrator marker.
        proc(103, "python3.12", str(tmp_path / "wt"), cmdline=["python3.12"]),
        # Attributes the scan could not read are never guessed at.
        proc(104, "claude", None),
        # The scanning process itself.
        proc(1, "claude", str(tmp_path / "wt")),
    ]
    scanner = scanner_for(database, tmp_path, procs)
    assert scanner() == ()


def test_scanner_excludes_exactly_leased_processes(
    database: Database, tmp_path: Path
) -> None:
    process_lease(database, "proc-1", 4321, 4300, 100.0)
    exact = proc(4321, "claude", str(tmp_path / "wt"), create_time=100.0)
    group_member = proc(4322, "claude", str(tmp_path / "wt"))
    scanner = scanner_for(
        database,
        tmp_path,
        [exact, group_member],
        getpgid=lambda pid: 4300,
    )
    assert scanner() == ()


def test_scanner_reports_leased_pid_with_different_identity(
    database: Database, tmp_path: Path
) -> None:
    process_lease(database, "proc-1", 4321, 4300, 100.0)
    reused = proc(4321, "claude", str(tmp_path / "wt"), create_time=555.0)
    scanner = scanner_for(database, tmp_path, [reused])
    assert scanner() == (4321,)


def test_scanner_treats_worktree_lease_paths_as_managed_roots(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    leases = WorktreeLeases(database, events, now=lambda: NOW)
    register_worktree(leases)
    stray = proc(4321, "claude", str(Path(WORKTREE) / "src"))
    scanner = scanner_for(database, tmp_path / "unrelated", [stray])
    assert scanner() == (4321,)


def test_scanner_bound_overrun_fails_closed(
    database: Database, tmp_path: Path
) -> None:
    procs = [proc(100 + index, "Safari", None) for index in range(3)]
    scanner = scanner_for(database, tmp_path, procs, max_processes=2)
    with pytest.raises(RuntimeError, match="bounded"):
        scanner()


def test_process_scan_failure_blocks_admission(
    database: Database, events: EventStore
) -> None:
    def broken_scan() -> tuple[int, ...]:
        raise RuntimeError("bounded process scan did not complete")

    report = build(database, events, unknown_processes=broken_scan).run()
    finding = by_kind(report, "process_scan_unavailable")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False


# -- worker sessions ------------------------------------------------------


def worker_lease(database: Database, lease_id: str, pid: int) -> None:
    seed(
        database,
        "INSERT INTO worker_leases("
        "lease_id, worker_id, project_key, kind, pid, state, acquired_at"
        ") VALUES (?, 'cell-1', 'demo', 'claude', ?, 'active', ?)",
        (lease_id, pid, PAST),
    )


def test_dead_worker_lease_expires_via_journaled_path(
    database: Database, events: EventStore
) -> None:
    worker_lease(database, "wl-1", 4242)
    report = build(database, events, pid_exists=lambda _pid: False).run()
    assert by_kind(report, "worker_lease_expired")[0].blocking is False
    assert report.safe_to_open_admission is True
    assert (
        database.scalar(
            "SELECT state FROM worker_leases WHERE lease_id = 'wl-1'"
        )
        == "expired"
    )
    types = [record.event_type for record in events.list_after(0)]
    assert "worker_lease.expired" in types


def test_uncertain_live_worker_keeps_admission_closed(
    database: Database, events: EventStore
) -> None:
    worker_lease(database, "wl-2", 4242)
    report = build(database, events, pid_exists=lambda _pid: True).run()
    finding = by_kind(report, "uncertain_live_worker")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False
    assert (
        database.scalar(
            "SELECT state FROM worker_leases WHERE lease_id = 'wl-2'"
        )
        == "active"
    )


def test_starting_cell_is_an_incomplete_transition(
    database: Database, events: EventStore
) -> None:
    seed(
        database,
        "INSERT INTO project_cells(cell_id, project_key, state, created_at, "
        "updated_at) VALUES ('cell-9', 'demo', 'starting', ?, ?)",
        (PAST, PAST),
    )
    report = build(database, events).run()
    finding = by_kind(report, "cell_start_incomplete")[0]
    assert finding.blocking is True
    assert (
        database.scalar(
            "SELECT state FROM project_cells WHERE cell_id = 'cell-9'"
        )
        == "starting"
    )


# -- worktrees ------------------------------------------------------------


def worktree_stack(
    database: Database, events: EventStore, fake_git: FakeGit, clock: Clock
) -> tuple[WorktreeLeases, WorktreeCustodian]:
    leases = WorktreeLeases(database, events, now=clock.now)
    custodian = WorktreeCustodian(
        leases, FakeRegistry(), fake_git, now=clock.now
    )
    return leases, custodian


def register_worktree(leases: WorktreeLeases) -> str:
    return leases.register(
        WorktreeLeaseInput(
            project_key="demo",
            issue_id="ENG-431",
            repo_path=REPO,
            path=WORKTREE,
            branch="feature/eng-431",
            remote="origin",
        )
    ).lease_id


def test_expired_reclaiming_claim_converges_via_custodian(
    database: Database, events: EventStore
) -> None:
    fake_git = FakeGit(worktree_paths=[REPO])
    clock = Clock()
    leases, custodian = worktree_stack(database, events, fake_git, clock)
    lease_id = register_worktree(leases)
    leases.record_checkpoint(
        lease_id, checkpoint_id="ck-1", sha=SHA_A, message="wip"
    )
    clock.value = NOW - timedelta(hours=2)
    leases.claim_cleanup(lease_id, owner="crashed-owner")
    clock.value = NOW
    report = build(
        database,
        events,
        worktrees=leases,
        custodian=custodian,
        git=fake_git,
    ).run()
    assert by_kind(report, "worktree_converged")[0].blocking is False
    assert report.safe_to_open_admission is True
    assert leases.get(lease_id).state == "reclaimed"


def test_live_reclaiming_claim_keeps_admission_closed(
    database: Database, events: EventStore
) -> None:
    fake_git = FakeGit(worktree_paths=[REPO])
    clock = Clock()
    leases, custodian = worktree_stack(database, events, fake_git, clock)
    lease_id = register_worktree(leases)
    leases.record_checkpoint(
        lease_id, checkpoint_id="ck-1", sha=SHA_A, message="wip"
    )
    leases.claim_cleanup(lease_id, owner="live-owner")
    report = build(
        database,
        events,
        worktrees=leases,
        custodian=custodian,
        git=fake_git,
    ).run()
    finding = by_kind(report, "worktree_reconcile_blocked")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False
    lease = leases.get(lease_id)
    assert lease.state == "reclaiming"
    assert lease.cleanup_owner == "live-owner"


def test_missing_worktree_with_durable_proof_converges(
    database: Database, events: EventStore
) -> None:
    fake_git = FakeGit(worktree_paths=[REPO], remote_contains_result=True)
    clock = Clock()
    leases, custodian = worktree_stack(database, events, fake_git, clock)
    lease_id = register_worktree(leases)
    leases.record_checkpoint(
        lease_id, checkpoint_id="ck-1", sha=SHA_A, message="wip"
    )
    leases.record_remote_verified(lease_id, checkpoint_id="ck-1", sha=SHA_A)
    report = build(
        database,
        events,
        worktrees=leases,
        custodian=custodian,
        git=fake_git,
    ).run()
    assert by_kind(report, "worktree_converged")[0].blocking is False
    assert report.safe_to_open_admission is True
    assert leases.get(lease_id).state == "reclaimed"
    # The convergence re-proved the checkpoint on the remote now — it never
    # grandfathered the stored timestamp.
    assert ("fetch", "origin", "feature/eng-431") in fake_git.commands


def test_missing_worktree_without_proof_is_lost_work(
    database: Database, events: EventStore
) -> None:
    fake_git = FakeGit(worktree_paths=[REPO])
    clock = Clock()
    leases, custodian = worktree_stack(database, events, fake_git, clock)
    lease_id = register_worktree(leases)
    leases.record_checkpoint(
        lease_id, checkpoint_id="ck-1", sha=SHA_A, message="wip"
    )
    report = build(
        database,
        events,
        worktrees=leases,
        custodian=custodian,
        git=fake_git,
    ).run()
    finding = by_kind(report, "lost_work_suspected")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False
    lease = leases.get(lease_id)
    assert lease.state == "checkpointed"
    assert lease.cleanup_owner is None


def test_missing_worktree_with_unreachable_remote_is_lost_work(
    database: Database, events: EventStore
) -> None:
    fake_git = FakeGit(worktree_paths=[REPO], remote_contains_result=False)
    clock = Clock()
    leases, custodian = worktree_stack(database, events, fake_git, clock)
    lease_id = register_worktree(leases)
    leases.record_checkpoint(
        lease_id, checkpoint_id="ck-1", sha=SHA_A, message="wip"
    )
    leases.record_remote_verified(lease_id, checkpoint_id="ck-1", sha=SHA_A)
    report = build(
        database,
        events,
        worktrees=leases,
        custodian=custodian,
        git=fake_git,
    ).run()
    assert by_kind(report, "lost_work_suspected")[0].blocking is True
    assert report.safe_to_open_admission is False
    lease = leases.get(lease_id)
    assert lease.state == "checkpointed"
    assert lease.cleanup_owner is None


def test_present_worktree_is_consistent(
    database: Database, events: EventStore
) -> None:
    fake_git = FakeGit()
    clock = Clock()
    leases, custodian = worktree_stack(database, events, fake_git, clock)
    lease_id = register_worktree(leases)
    leases.record_checkpoint(
        lease_id, checkpoint_id="ck-1", sha=SHA_A, message="wip"
    )
    report = build(
        database,
        events,
        worktrees=leases,
        custodian=custodian,
        git=fake_git,
    ).run()
    assert [f for f in report.findings if f.subsystem == "worktrees"] == []
    assert report.safe_to_open_admission is True


# -- github ---------------------------------------------------------------


def merge_effect(
    database: Database, effect_id: str, state: str, expires: str | None
) -> None:
    seed(
        database,
        "INSERT INTO github_merge_effects("
        "effect_id, repository, pr_number, head_sha, head_ref, base_ref, "
        "merge_method, state, request_json, claim_token, claim_expires_at, "
        "created_at, updated_at"
        ") VALUES (?, 'acme/demo', 17, ?, 'feature/x', 'main', 'squash', ?, "
        "'{}', 'tok', ?, ?, ?)",
        (effect_id, SHA_A, state, expires, PAST, PAST),
    )


def test_attempted_merge_effect_keeps_admission_closed(
    database: Database, events: EventStore
) -> None:
    merge_effect(database, "gme-1", "attempted", None)
    report = build(database, events).run()
    finding = by_kind(report, "merge_outcome_unknown")[0]
    assert finding.blocking is True
    assert finding.subsystem == "github"
    assert report.safe_to_open_admission is False


def test_expired_pending_merge_claim_is_recoverable(
    database: Database, events: EventStore
) -> None:
    merge_effect(database, "gme-2", "pending", PAST)
    report = build(database, events).run()
    assert by_kind(report, "merge_claim_stranded")[0].blocking is False
    assert report.safe_to_open_admission is True


def attempted_effect_state(database: Database) -> str:
    return str(
        database.scalar(
            "SELECT state FROM github_merge_effects WHERE effect_id = 'gme-1'"
        )
    )


def test_attempted_merge_externally_merged_with_exact_head_resolves(
    database: Database, events: EventStore
) -> None:
    merge_effect(database, "gme-1", "attempted", None)
    github = FakeGitHubReads(
        pull=SimpleNamespace(merged=True, head_sha=SHA_A, state="closed")
    )
    report = build(database, events, github_reads=github).run()
    finding = by_kind(report, "merge_outcome_externally_merged")[0]
    assert finding.blocking is False
    assert finding.aggregate_id == "gme-1"
    assert report.safe_to_open_admission is True
    assert github.calls == [("acme/demo", 17)]
    # Read-and-report only: the durable mutation fence is never touched.
    assert attempted_effect_state(database) == "attempted"


def test_attempted_merge_externally_open_keeps_admission_closed(
    database: Database, events: EventStore
) -> None:
    merge_effect(database, "gme-1", "attempted", None)
    github = FakeGitHubReads(
        pull=SimpleNamespace(merged=False, head_sha=SHA_A, state="open")
    )
    report = build(database, events, github_reads=github).run()
    finding = by_kind(report, "merge_outcome_unconfirmed")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False
    assert attempted_effect_state(database) == "attempted"


def test_attempted_merge_with_mismatched_head_keeps_admission_closed(
    database: Database, events: EventStore
) -> None:
    merge_effect(database, "gme-1", "attempted", None)
    github = FakeGitHubReads(
        pull=SimpleNamespace(merged=True, head_sha="f" * 40, state="closed")
    )
    report = build(database, events, github_reads=github).run()
    finding = by_kind(report, "merge_head_mismatch")[0]
    assert finding.blocking is True
    assert finding.evidence["observed_head_sha"] == "f" * 40
    assert report.safe_to_open_admission is False
    assert attempted_effect_state(database) == "attempted"


def test_attempted_merge_with_unavailable_read_keeps_admission_closed(
    database: Database, events: EventStore
) -> None:
    merge_effect(database, "gme-1", "attempted", None)
    github = FakeGitHubReads(error=RuntimeError("github unreachable"))
    report = build(database, events, github_reads=github).run()
    finding = by_kind(report, "merge_state_unavailable")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False
    assert attempted_effect_state(database) == "attempted"


# -- circleci -------------------------------------------------------------


def ledger_row(
    database: Database, merge_sha: str, state: str, packet: str | None
) -> None:
    seed(
        database,
        "INSERT INTO ci_merge_ledger("
        "project_key, merge_sha, repository, pr_number, integration_branch, "
        "candidate_sha, candidate_branch, state, packet_json, recorded_at, "
        "updated_at"
        ") VALUES ('demo', ?, 'acme/demo', 17, 'main', ?, 'feature/x', ?, ?, "
        "?, ?)",
        (merge_sha, SHA_A, state, packet, PAST, PAST),
    )


def test_ci_none_project_resolves_ci_not_configured(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    ci_states = FakeCiStates(outcome="success")
    report = build(
        database,
        events,
        projects={"demo": project(tmp_path, "none")},
        ci_states=ci_states,
    ).run()
    finding = by_kind(report, "ci_not_configured")[0]
    assert finding.blocking is False
    assert finding.subsystem == "circleci"
    assert report.safe_to_open_admission is True
    # A ci: none project never consults any CI system.
    assert ci_states.calls == []


def test_ci_none_ledger_rows_never_consult_the_port(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    ledger_row(database, "m" * 40, "unresolved", None)
    ci_states = FakeCiStates(outcome="success")
    report = build(
        database,
        events,
        projects={"demo": project(tmp_path, "none")},
        ci_states=ci_states,
    ).run()
    assert by_kind(report, "ci_merge_unresolved")[0].blocking is False
    assert report.safe_to_open_admission is True
    assert ci_states.calls == []


def test_unresolved_ci_merge_projects_known_state_read_only(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    ledger_row(database, "m" * 40, "unresolved", None)
    ci_states = FakeCiStates(outcome="nonterminal")
    report = build(
        database,
        events,
        projects={"demo": project(tmp_path, "circleci")},
        ci_states=ci_states,
    ).run()
    finding = by_kind(report, "ci_merge_unresolved")[0]
    assert finding.blocking is False
    assert finding.evidence["known_outcome"] == "nonterminal"
    assert report.safe_to_open_admission is True
    assert ci_states.calls == [("acme/demo", "main", "m" * 40)]


def test_circleci_rows_without_a_known_state_port_keep_admission_closed(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    ledger_row(database, "m" * 40, "unresolved", None)
    report = build(
        database, events, projects={"demo": project(tmp_path, "circleci")}
    ).run()
    finding = by_kind(report, "ci_state_unavailable")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False


def test_circleci_unavailable_known_state_keeps_admission_closed(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    ledger_row(database, "m" * 40, "unresolved", None)
    ci_states = FakeCiStates(error=RuntimeError("circleci unreachable"))
    report = build(
        database,
        events,
        projects={"demo": project(tmp_path, "circleci")},
        ci_states=ci_states,
    ).run()
    finding = by_kind(report, "ci_state_unavailable")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False


def test_circleci_known_state_mismatch_keeps_admission_closed(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    ledger_row(database, "m" * 40, "failed", '{"reason":"tests"}')
    ci_states = FakeCiStates(outcome="success")
    report = build(
        database,
        events,
        projects={"demo": project(tmp_path, "circleci")},
        ci_states=ci_states,
    ).run()
    finding = by_kind(report, "ci_state_mismatch")[0]
    assert finding.blocking is True
    assert finding.evidence["known_outcome"] == "success"
    assert report.safe_to_open_admission is False
    assert (
        database.scalar("SELECT state FROM ci_merge_ledger") == "failed"
    )


def test_ci_failure_without_correction_route_blocks(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    ledger_row(database, "m" * 40, "failed", '{"reason":"tests"}')
    report = build(
        database,
        events,
        projects={"demo": project(tmp_path, "circleci")},
        ci_states=FakeCiStates(outcome="failure"),
    ).run()
    assert by_kind(report, "ci_failure_unrouted")[0].blocking is True
    assert report.safe_to_open_admission is False
    seed(
        database,
        "INSERT INTO lead_corrections("
        "correction_id, project_key, issue_id, source, repository, branch, "
        "pr_number, reviewed_sha, packets_json, state, created_at"
        ") VALUES ('corr-1', 'demo', 'ENG-431', 'ci', 'acme/demo', "
        "'feature/x', 17, ?, '[]', 'pending', ?)",
        (SHA_A, PAST),
    )
    routed = build(
        database,
        events,
        projects={"demo": project(tmp_path, "circleci")},
        ci_states=FakeCiStates(outcome="failure"),
    ).run()
    assert routed.safe_to_open_admission is True
    assert by_kind(routed, "ci_failure_pending_correction")[0].blocking is False


# -- linear ---------------------------------------------------------------


def linear_effect(
    database: Database,
    *,
    effect_id: str = "eff-1",
    issue_id: str = "ENG-431",
    status: str = "Review",
    assignee_alias: str = "ryan",
    source_revision: str = "rev-0",
) -> None:
    request = {
        "issue_id": issue_id,
        "target": {"status": status, "assignee_alias": assignee_alias},
        "source_revision": source_revision,
        "changed_fields": ["status"],
    }
    seed(
        database,
        "INSERT INTO external_effects("
        "effect_id, adapter, operation, target, state, request_json, "
        "created_at, updated_at"
        ") VALUES (?, 'linear', 'project', ?, 'pending', ?, ?, ?)",
        (effect_id, issue_id, json.dumps(request), PAST, PAST),
    )


def admitted_issue(
    database: Database,
    *,
    issue_id: str = "ENG-431",
    project_key: str = "demo",
) -> None:
    seed(
        database,
        "INSERT INTO admitted_issues(issue_id, project_key, priority, state) "
        "VALUES (?, ?, 1, 'review')",
        (issue_id, project_key),
    )


def pending_effect_state(database: Database) -> str:
    return str(
        database.scalar(
            "SELECT state FROM external_effects WHERE effect_id = 'eff-1'"
        )
    )


def test_pending_linear_effect_keeps_admission_closed(
    database: Database, events: EventStore
) -> None:
    linear_effect(database)
    report = build(database, events).run()
    finding = by_kind(report, "linear_effect_ambiguous")[0]
    assert finding.blocking is True
    assert finding.subsystem == "linear"
    assert report.safe_to_open_admission is False


def linear_build(
    database: Database,
    events: EventStore,
    reads: FakeLinearReads,
) -> Reconciler:
    return build(
        database,
        events,
        linear_reads=reads,
        linear_expectations=expectations(),
    )


def test_pending_linear_effect_already_applied_resolves(
    database: Database, events: EventStore
) -> None:
    linear_effect(database)
    admitted_issue(database)
    # The issue moved past the journaled read, and the exact configured
    # state and assignee identities match the journaled target.
    reads = FakeLinearReads(issue=linear_issue(revision="rev-9"))
    report = linear_build(database, events, reads).run()
    finding = by_kind(report, "linear_effect_applied")[0]
    assert finding.blocking is False
    assert finding.aggregate_id == "eff-1"
    assert report.safe_to_open_admission is True
    assert reads.calls == ["ENG-431"]
    # Read-and-report only: the journal row is never completed here.
    assert pending_effect_state(database) == "pending"


def test_pending_linear_effect_provably_not_applied_is_reported(
    database: Database, events: EventStore
) -> None:
    linear_effect(database)
    admitted_issue(database)
    # The issue revision still equals the journaled source revision, so
    # the crash provably happened before the mutation boundary.
    reads = FakeLinearReads(
        issue=linear_issue(
            state_id="state-dev",
            status="In Development",
            assignee_id="user-operator",
            revision="rev-0",
        )
    )
    report = linear_build(database, events, reads).run()
    finding = by_kind(report, "linear_effect_unapplied")[0]
    assert finding.blocking is False
    assert finding.severity == "warning"
    assert report.safe_to_open_admission is True
    assert pending_effect_state(database) == "pending"


def test_pending_linear_effect_state_mismatch_keeps_admission_closed(
    database: Database, events: EventStore
) -> None:
    linear_effect(database)
    admitted_issue(database)
    # The issue changed after the journaled read without reaching the
    # target: the outcome cannot be proven either way.
    reads = FakeLinearReads(
        issue=linear_issue(
            state_id="state-dev",
            status="In Development",
            assignee_id="user-operator",
            revision="rev-7",
        )
    )
    report = linear_build(database, events, reads).run()
    finding = by_kind(report, "linear_state_mismatch")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False
    assert pending_effect_state(database) == "pending"


def test_pending_linear_effect_team_mismatch_keeps_admission_closed(
    database: Database, events: EventStore
) -> None:
    linear_effect(database)
    admitted_issue(database)
    reads = FakeLinearReads(issue=linear_issue(team_id="team-other"))
    report = linear_build(database, events, reads).run()
    finding = by_kind(report, "linear_team_mismatch")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False
    assert pending_effect_state(database) == "pending"


def test_pending_linear_effect_unavailable_read_keeps_admission_closed(
    database: Database, events: EventStore
) -> None:
    linear_effect(database)
    admitted_issue(database)
    reads = FakeLinearReads(error=RuntimeError("linear unreachable"))
    report = linear_build(database, events, reads).run()
    finding = by_kind(report, "linear_state_unavailable")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False
    assert pending_effect_state(database) == "pending"


def test_pending_linear_effect_without_exact_identities_keeps_admission_closed(
    database: Database, events: EventStore
) -> None:
    # No admitted issue maps the journaled target onto a configured
    # project, so no exact identity can prove anything.
    linear_effect(database)
    reads = FakeLinearReads(issue=linear_issue())
    report = linear_build(database, events, reads).run()
    finding = by_kind(report, "linear_projection_unresolvable")[0]
    assert finding.blocking is True
    assert report.safe_to_open_admission is False
    assert reads.calls == []


def test_in_flight_issue_without_cell_is_informational(
    database: Database, events: EventStore
) -> None:
    seed(
        database,
        "INSERT INTO admitted_issues(issue_id, project_key, priority, state) "
        "VALUES ('ENG-431', 'demo', 1, 'in_development')",
        (),
    )
    report = build(database, events).run()
    assert by_kind(report, "issue_without_cell")[0].blocking is False
    assert report.safe_to_open_admission is True


# -- service and startup wiring -------------------------------------------


def service_for(
    database: Database,
    events: EventStore,
    *,
    mode: str,
    reconciler: Reconciler | None = None,
    startup_report: ReconciliationReport | None = None,
) -> OrchestratorService:
    queue = QueueService(database, events, {"demo"})
    return OrchestratorService(
        database,
        events,
        FakeSampler(),
        Scheduler(queue, mode="observe"),
        PolicyConfig(mode=mode),
        pid_exists=lambda _pid: False,
        reconciler=reconciler,
        startup_report=startup_report,
    )


def test_service_start_consumes_completed_startup_report(
    database: Database, events: EventStore
) -> None:
    report = build(database, events).run()
    runs_before = database.scalar("SELECT count(*) FROM reconciliation_runs")
    service = service_for(
        database, events, mode="active", startup_report=report
    )
    result = service.start()
    assert result.completed is True
    assert result.admission_open is True
    assert service.admission_open is True
    # The report was consumed, not re-run.
    assert (
        database.scalar("SELECT count(*) FROM reconciliation_runs")
        == runs_before
    )
    service.tick()


def test_service_start_holds_admission_on_blocking_report(
    database: Database, events: EventStore
) -> None:
    report = build(database, events, unknown_processes=lambda: (900,)).run()
    service = service_for(
        database, events, mode="active", startup_report=report
    )
    result = service.start()
    assert result.completed is True
    assert result.admission_open is False
    assert any("unknown_live_process" in item for item in result.findings)


def test_service_start_runs_injected_reconciler(
    database: Database, events: EventStore
) -> None:
    service = service_for(
        database, events, mode="active", reconciler=build(database, events)
    )
    result = service.start()
    assert result.admission_open is True
    assert (
        database.scalar(
            "SELECT count(*) FROM reconciliation_runs WHERE state = 'completed'"
        )
        == 1
    )


def test_observe_mode_never_opens_admission(
    database: Database, events: EventStore
) -> None:
    report = build(database, events).run()
    service = service_for(
        database, events, mode="observe", startup_report=report
    )
    assert service.start().admission_open is False


def test_live_runtime_reconciles_before_profile_probes(tmp_path: Path) -> None:
    import sqlite3

    from hermes_orchestrator.config import load_settings
    from hermes_orchestrator.runtime import open_runtime
    from tests.test_runtime import EligibleProfileCommand, FakeKeychain, active_repo

    class OrderedProbeCommand(EligibleProfileCommand):
        def __init__(self, state_db: Path) -> None:
            super().__init__()
            self.state_db = state_db
            self.reconciled_before_probe: list[bool] = []

        def run_json(
            self, command: list[str], env: dict[str, str]
        ) -> dict[str, object]:
            connection = sqlite3.connect(self.state_db)
            try:
                completed = connection.execute(
                    "SELECT count(*) FROM reconciliation_runs "
                    "WHERE state = 'completed'"
                ).fetchone()[0]
            finally:
                connection.close()
            self.reconciled_before_probe.append(completed > 0)
            return super().run_json(command, env)

    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    probes = OrderedProbeCommand(state_dir / "state.db")
    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=probes,
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        # Reconciliation ran to completion before every profile probe.
        assert len(probes.reconciled_before_probe) == 4
        assert all(probes.reconciled_before_probe)
        assert runtime.reconciliation is not None
        assert runtime.reconciliation.stages == STAGE_ORDER
        assert runtime.reconciliation.safe_to_open_admission is True
        # The service consumes the completed startup report instead of
        # reconciling a second time.
        result = runtime.service.start()
        assert result.admission_open is True
        assert (
            runtime.database.scalar(
                "SELECT count(*) FROM reconciliation_runs"
            )
            == 1
        )
    finally:
        runtime.close()


def test_live_runtime_blocks_admission_on_unknown_managed_process(
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.config import load_settings
    from hermes_orchestrator.runtime import open_runtime
    from tests.test_runtime import EligibleProfileCommand, FakeKeychain, active_repo

    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    # A live managed-looking process working inside the configured project
    # repository without any process lease covering it.
    stray = proc(65001, "claude", str(repo_root), create_time=12345.0)
    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
        process_iter=lambda: iter([stray]),
    )
    try:
        report = runtime.reconciliation
        assert report is not None
        findings = [
            finding
            for finding in report.findings
            if finding.kind == "unknown_live_process"
        ]
        assert findings and findings[0].blocking is True
        assert findings[0].aggregate_id == "65001"
        assert report.safe_to_open_admission is False
        assert runtime.service.start().admission_open is False
    finally:
        runtime.close()
