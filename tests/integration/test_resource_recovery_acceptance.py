"""End-to-end resource-recovery acceptance (INFRA-172).

The red path under crash injection: a dirty leased worktree is WIP
checkpointed, pushed, and proven on a real bare remote; the exact claimed
process group is stopped through fake OS ports; ``git worktree remove``
succeeds and the custodian crashes past the point of no return. The
daemon then "restarts" by reopening the same SQLite file and running the
ordered startup Reconciler, which must converge the crashed cleanup
through the journaled custodian path with no lost work and no duplicate
external action, and must keep admission closed while the crashed claim
is still live.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.git import SubprocessGitRunner, WorktreeGit
from hermes_orchestrator.processes import ProcessLeaseInput, ProcessRegistry
from hermes_orchestrator.reconcile import STAGE_ORDER, Reconciler
from hermes_orchestrator.worktrees import (
    Checkpoint,
    WorktreeCustodian,
    WorktreeLeaseInput,
    WorktreeLeases,
)
from tests.integration.test_worktree_cleanup import (
    CrashOnceGit,
    FakeCrash,
    FakeInfo,
    FakeOs,
)

PID = 120
WIP_MESSAGE = "wip(ENG-431): checkpoint before resource cleanup"


@dataclass
class Scenario:
    repo: Path
    worktree: Path
    origin: Path
    env: dict[str, str]
    state_db: Path
    lease_id: str
    process_lease_id: str
    checkpoint: Checkpoint
    first_killpg_calls: list[tuple[int, int]]

    def git(self, *args: str, cwd: Path) -> str:
        completed = subprocess.run(
            ["git", *args],
            cwd=cwd,
            env=self.env,
            capture_output=True,
            text=True,
            check=True,
        )
        return completed.stdout


@pytest.fixture
def scenario(tmp_path: Path) -> Scenario:
    """Run the live red path up to the injected crash, then release the db."""

    env = {
        "PATH": os.environ["PATH"],
        "HOME": str(tmp_path),
        "GIT_CONFIG_GLOBAL": str(tmp_path / "gitconfig"),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_AUTHOR_NAME": "Test",
        "GIT_AUTHOR_EMAIL": "test@example.invalid",
        "GIT_COMMITTER_NAME": "Test",
        "GIT_COMMITTER_EMAIL": "test@example.invalid",
    }

    def git(*args: str, cwd: Path) -> None:
        subprocess.run(
            ["git", *args],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )

    origin = tmp_path / "origin.git"
    origin.mkdir()
    git("init", "--bare", cwd=origin)
    repo = tmp_path / "repo"
    repo.mkdir()
    git("init", "-b", "main", cwd=repo)
    git("remote", "add", "origin", str(origin), cwd=repo)
    (repo / "README.md").write_text("hello\n", encoding="utf-8")
    git("add", "-A", cwd=repo)
    git("commit", "-m", "initial", cwd=repo)
    git("push", "origin", "main", cwd=repo)
    worktree = tmp_path / "worktrees" / "eng-431"
    git("worktree", "add", "-b", "feature/eng-431", str(worktree), "main", cwd=repo)

    state_db = tmp_path / "state.db"
    database = Database.open(state_db)
    try:
        events = EventStore(database)
        fake_info = FakeInfo(create_times={PID: 1000.0}, running={PID})
        fake_os = FakeOs(groups={PID: PID}, info=fake_info)
        registry = ProcessRegistry(
            database, events, os_port=fake_os, info=fake_info, grace_seconds=0.2
        )
        leases = WorktreeLeases(database, events)
        runner = SubprocessGitRunner(env=env)
        custodian = WorktreeCustodian(leases, registry, WorktreeGit(runner=runner))

        lease_id = leases.register(
            WorktreeLeaseInput(
                project_key="demo",
                issue_id="ENG-431",
                repo_path=str(repo),
                path=str(worktree),
                branch="feature/eng-431",
                remote="origin",
            )
        ).lease_id
        process_lease_id = registry.register(
            ProcessLeaseInput(
                pid=PID,
                pgid=PID,
                project_key="demo",
                kind="subagent",
                cwd=str(worktree),
            )
        ).lease_id

        # Red pressure: pending work is checkpointed, pushed, and proven.
        (worktree / "README.md").write_text("work in progress\n", "utf-8")
        (worktree / "notes.txt").write_text("untracked\n", "utf-8")
        checkpoint = custodian.checkpoint(lease_id, "ENG-431")
        proof = custodian.verify_remote(checkpoint)

        # The cleanup stops the exact claimed group, removes the worktree,
        # then crashes after removal — past the point of no return.
        crashing = WorktreeCustodian(
            leases, registry, CrashOnceGit(runner, "worktree_prune")
        )
        with pytest.raises(FakeCrash):
            crashing.reclaim(lease_id, proof)

        assert registry.get(process_lease_id).state == "stopped"
        assert leases.get(lease_id).state == "reclaiming"
        assert not worktree.exists()
    finally:
        database.close()

    return Scenario(
        repo=repo,
        worktree=worktree,
        origin=origin,
        env=env,
        state_db=state_db,
        lease_id=lease_id,
        process_lease_id=process_lease_id,
        checkpoint=checkpoint,
        first_killpg_calls=list(fake_os.killpg_calls),
    )


def restart(
    scenario: Scenario, *, claim_ttl_seconds: float
) -> tuple[Database, Reconciler, WorktreeLeases, FakeOs]:
    database = Database.open(scenario.state_db)
    events = EventStore(database)
    fake_info = FakeInfo()
    fake_os = FakeOs(info=fake_info)
    registry = ProcessRegistry(
        database, events, os_port=fake_os, info=fake_info, grace_seconds=0.2
    )
    leases = WorktreeLeases(database, events)
    workspace = WorktreeGit(runner=SubprocessGitRunner(env=scenario.env))
    custodian = WorktreeCustodian(
        leases,
        registry,
        workspace,
        cleanup_claim_ttl_seconds=claim_ttl_seconds,
    )
    reconciler = Reconciler(
        database,
        EventStore(database),
        projects={
            "demo": ProjectConfig(
                linear_team="ENG",
                repo_path=scenario.repo,
                integration_branch="main",
                github_repo="acme/demo",
                ci="none",
            )
        },
        processes=registry,
        worktrees=leases,
        custodian=custodian,
        git=workspace,
        pid_exists=lambda _pid: False,
    )
    return database, reconciler, leases, fake_os


def test_restart_reconciles_crashed_cleanup_without_duplicate_action(
    scenario: Scenario,
) -> None:
    time.sleep(0.05)
    database, reconciler, leases, fake_os = restart(
        scenario, claim_ttl_seconds=0.01
    )
    try:
        report = reconciler.run()

        # The full ordered pass completed and recovered the crashed cleanup.
        assert report.stages == STAGE_ORDER
        assert report.completed is True
        converged = [
            finding
            for finding in report.findings
            if finding.kind == "worktree_converged"
        ]
        assert converged and converged[0].aggregate_id == scenario.lease_id
        assert not any(finding.blocking for finding in report.findings)
        assert report.safe_to_open_admission is True
        assert leases.get(scenario.lease_id).state == "reclaimed"

        # No duplicate external action. The remote branch still points at
        # the single WIP checkpoint; no signal was re-sent; the worktree
        # was not removed twice.
        remote_tip = scenario.git(
            "rev-parse", "refs/heads/feature/eng-431", cwd=scenario.origin
        ).strip()
        assert remote_tip == scenario.checkpoint.sha
        subjects = scenario.git(
            "log", "--format=%s", "refs/heads/feature/eng-431",
            cwd=scenario.origin,
        ).splitlines()
        assert subjects.count(WIP_MESSAGE) == 1
        assert scenario.first_killpg_calls == [(PID, signal.SIGTERM)]
        assert fake_os.killpg_calls == []
        listing = scenario.git(
            "worktree", "list", "--porcelain", cwd=scenario.repo
        )
        assert str(scenario.worktree) not in listing

        # The recovered next action is real: a fresh clone of the remote
        # branch restores the checkpointed work exactly.
        recovered = scenario.worktree.parent / "recovered"
        scenario.git(
            "clone", "--branch", "feature/eng-431", str(scenario.origin),
            str(recovered), cwd=scenario.worktree.parent,
        )
        assert (recovered / "README.md").read_text("utf-8") == (
            "work in progress\n"
        )
        assert (recovered / "notes.txt").read_text("utf-8") == "untracked\n"

        # A second startup pass is idempotent: nothing to converge, no new
        # effects, admission stays open.
        second = reconciler.run()
        assert second.safe_to_open_admission is True
        assert [
            finding
            for finding in second.findings
            if finding.subsystem == "worktrees"
        ] == []
        assert leases.get(scenario.lease_id).state == "reclaimed"
    finally:
        database.close()


def test_restart_holds_admission_while_crashed_claim_is_live(
    scenario: Scenario,
) -> None:
    database, reconciler, leases, _fake_os = restart(
        scenario, claim_ttl_seconds=3600.0
    )
    try:
        report = reconciler.run()
        blocked = [
            finding
            for finding in report.findings
            if finding.kind == "worktree_reconcile_blocked"
        ]
        assert blocked and blocked[0].blocking is True
        assert report.safe_to_open_admission is False
        # The claim was not stolen: the lease still belongs to the crashed
        # owner and stays closed to new attachments.
        assert leases.get(scenario.lease_id).state == "reclaiming"
    finally:
        database.close()
