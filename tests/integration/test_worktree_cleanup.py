"""End-to-end worktree checkpoint, remote proof, and reclaim (INFRA-171).

Real git repositories under a temporary directory with a local bare
remote, the real ``WorktreeGit`` adapter, real SQLite durable state, and
the real ``ProcessRegistry`` claimed-stop path driven by fake OS ports so
no real process is ever signaled.
"""

from __future__ import annotations

import os
import signal
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.git import SubprocessGitRunner, WorktreeGit
from hermes_orchestrator.processes import (
    ProcessLeaseInput,
    ProcessRegistry,
)
from hermes_orchestrator.worktrees import (
    CleanupBlocked,
    WorktreeCustodian,
    WorktreeLeaseInput,
    WorktreeLeases,
)

PID = 120


@dataclass
class FakeInfo:
    create_times: dict[int, float] = field(default_factory=dict)
    running: set[int] = field(default_factory=set)

    def create_time(self, pid: int) -> float | None:
        return self.create_times.get(pid)

    def is_running(self, pid: int, create_time: float) -> bool:
        return self.create_times.get(pid) == create_time and pid in self.running

    def tree_rss_bytes(self, pid: int, create_time: float) -> int:
        return 0

    def wait_exit(self, pid: int, create_time: float, timeout: float) -> bool:
        return not self.is_running(pid, create_time)


@dataclass
class FakeOs:
    groups: dict[int, int] = field(default_factory=dict)
    info: FakeInfo | None = None
    killpg_calls: list[tuple[int, int]] = field(default_factory=list)
    ignore_signals: bool = False

    def killpg(self, pgid: int, sig: int) -> None:
        self.killpg_calls.append((pgid, sig))
        if self.ignore_signals or self.info is None:
            return
        for pid, group in list(self.groups.items()):
            if group == pgid:
                self.info.running.discard(pid)

    def getpgid(self, pid: int) -> int:
        if pid not in self.groups:
            raise ProcessLookupError
        return self.groups[pid]


@dataclass
class Harness:
    repo: Path
    worktree: Path
    origin: Path
    env: dict[str, str]
    database: Database
    leases: WorktreeLeases
    registry: ProcessRegistry
    custodian: WorktreeCustodian
    fake_os: FakeOs
    fake_info: FakeInfo

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

    def lease_worktree(self) -> str:
        return self.leases.register(
            WorktreeLeaseInput(
                project_key="demo",
                issue_id="ENG-431",
                repo_path=str(self.repo),
                path=str(self.worktree),
                branch="feature/eng-431",
                remote="origin",
            )
        ).lease_id

    def lease_process(self) -> str:
        return self.registry.register(
            ProcessLeaseInput(
                pid=PID,
                pgid=PID,
                project_key="demo",
                kind="subagent",
                cwd=str(self.worktree),
            )
        ).lease_id


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[Harness]:
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
    git(
        "worktree", "add", "-b", "feature/eng-431", str(worktree), "main", cwd=repo
    )

    database = Database.open(tmp_path / "state.db")
    events = EventStore(database)
    fake_info = FakeInfo(create_times={PID: 1000.0}, running={PID})
    fake_os = FakeOs(groups={PID: PID}, info=fake_info)
    registry = ProcessRegistry(
        database, events, os_port=fake_os, info=fake_info, grace_seconds=0.2
    )
    leases = WorktreeLeases(database, events)
    workspace = WorktreeGit(runner=SubprocessGitRunner(env=env))
    custodian = WorktreeCustodian(leases, registry, workspace)
    try:
        yield Harness(
            repo=repo,
            worktree=worktree,
            origin=origin,
            env=env,
            database=database,
            leases=leases,
            registry=registry,
            custodian=custodian,
            fake_os=fake_os,
            fake_info=fake_info,
        )
    finally:
        database.close()


def test_checkpoint_remote_proof_stop_and_reclaim(harness: Harness) -> None:
    lease_id = harness.lease_worktree()
    process_lease = harness.lease_process()
    (harness.worktree / "README.md").write_text("work in progress\n", "utf-8")
    (harness.worktree / "notes.txt").write_text("untracked\n", "utf-8")

    # Dirty state blocks any direct reclaim.
    with pytest.raises(CleanupBlocked, match="dirty"):
        harness.custodian.reclaim(lease_id, None)

    checkpoint = harness.custodian.checkpoint(lease_id, "ENG-431")
    assert checkpoint.commit_message == (
        "wip(ENG-431): checkpoint before resource cleanup"
    )

    proof = harness.custodian.verify_remote(checkpoint)
    assert proof.sha == checkpoint.sha
    # The exact commit is reachable on the bare remote's branch.
    remote_tip = harness.git(
        "rev-parse", "refs/heads/feature/eng-431", cwd=harness.origin
    ).strip()
    assert remote_tip == checkpoint.sha

    result = harness.custodian.reclaim(lease_id, proof)
    assert result.removed is True
    assert result.stopped_process_leases == (process_lease,)
    # The claimed stop path signaled exactly the recorded group.
    assert harness.fake_os.killpg_calls == [(PID, signal.SIGTERM)]
    assert harness.registry.get(process_lease).state == "stopped"
    # The worktree is gone from git metadata and from disk, immediately.
    listing = harness.git("worktree", "list", "--porcelain", cwd=harness.repo)
    assert str(harness.worktree) not in listing
    assert not harness.worktree.exists()
    assert harness.leases.get(lease_id).state == "reclaimed"

    # Recovery: a fresh clone of the remote restores the checkpointed work.
    recovered = harness.worktree.parent / "recovered"
    harness.git(
        "clone", "--branch", "feature/eng-431", str(harness.origin),
        str(recovered), cwd=harness.worktree.parent,
    )
    assert (recovered / "README.md").read_text("utf-8") == "work in progress\n"
    assert (recovered / "notes.txt").read_text("utf-8") == "untracked\n"


def test_new_work_after_proof_blocks_reclaim(harness: Harness) -> None:
    lease_id = harness.lease_worktree()
    (harness.worktree / "README.md").write_text("work in progress\n", "utf-8")
    checkpoint = harness.custodian.checkpoint(lease_id, "ENG-431")
    proof = harness.custodian.verify_remote(checkpoint)
    (harness.worktree / "fresh.txt").write_text("new work\n", "utf-8")
    with pytest.raises(CleanupBlocked, match="dirty"):
        harness.custodian.reclaim(lease_id, proof)
    assert harness.worktree.exists()
    assert harness.leases.get(lease_id).state == "checkpointed"


def test_cleanup_claim_blocks_new_process_attachment(harness: Harness) -> None:
    lease_id = harness.lease_worktree()
    (harness.worktree / "README.md").write_text("work in progress\n", "utf-8")
    checkpoint = harness.custodian.checkpoint(lease_id, "ENG-431")
    harness.custodian.verify_remote(checkpoint)
    harness.leases.claim_cleanup(lease_id, owner="cleanup-1")
    # The claimed worktree admits no new managed process; the real
    # registry refuses the attachment before any lease row is written.
    with pytest.raises(ValueError, match="cleanup"):
        harness.lease_process()
    assert harness.registry.active() == ()
    harness.leases.release_cleanup(lease_id, owner="cleanup-1", reason="test")
    assert harness.lease_process()


class FakeCrash(RuntimeError):
    """Simulates the custodian process dying mid-pipeline."""


class CrashOnceGit(WorktreeGit):
    """Real git adapter that dies once at a chosen pipeline boundary."""

    def __init__(self, runner: SubprocessGitRunner, method: str) -> None:
        super().__init__(runner=runner)
        self._pending = {method}

    def push(self, path: Path, remote: str, branch: str) -> None:
        if "push" in self._pending:
            self._pending.discard("push")
            raise FakeCrash("crashed before push")
        super().push(path, remote, branch)

    def worktree_prune(self, repo_path: Path) -> None:
        if "worktree_prune" in self._pending:
            self._pending.discard("worktree_prune")
            raise FakeCrash("crashed after removal, before prune")
        super().worktree_prune(repo_path)


def test_crash_between_commit_and_push_recovers_single_wip_commit(
    harness: Harness,
) -> None:
    lease_id = harness.lease_worktree()
    (harness.worktree / "README.md").write_text("work in progress\n", "utf-8")
    crashing = WorktreeCustodian(
        harness.leases,
        harness.registry,
        CrashOnceGit(SubprocessGitRunner(env=harness.env), "push"),
    )
    with pytest.raises(FakeCrash):
        crashing.checkpoint(lease_id, "ENG-431")
    assert harness.leases.get(lease_id).state == "active"

    checkpoint = harness.custodian.checkpoint(lease_id, "ENG-431")
    assert checkpoint.commit_message == (
        "wip(ENG-431): checkpoint before resource cleanup"
    )
    # Exactly one WIP commit ahead of main: never committed twice.
    count = harness.git(
        "rev-list", "--count", "main..feature/eng-431", cwd=harness.repo
    ).strip()
    assert count == "1"
    remote_tip = harness.git(
        "rev-parse", "refs/heads/feature/eng-431", cwd=harness.origin
    ).strip()
    assert remote_tip == checkpoint.sha
    proof = harness.custodian.verify_remote(checkpoint)
    assert harness.custodian.reclaim(lease_id, proof).removed is True


def test_crash_after_removal_is_reconciled_without_duplicate_action(
    harness: Harness,
) -> None:
    lease_id = harness.lease_worktree()
    (harness.worktree / "README.md").write_text("work in progress\n", "utf-8")
    checkpoint = harness.custodian.checkpoint(lease_id, "ENG-431")
    proof = harness.custodian.verify_remote(checkpoint)
    crashing = WorktreeCustodian(
        harness.leases,
        harness.registry,
        CrashOnceGit(SubprocessGitRunner(env=harness.env), "worktree_prune"),
    )
    with pytest.raises(FakeCrash):
        crashing.reclaim(lease_id, proof)
    assert not harness.worktree.exists()
    assert harness.leases.get(lease_id).state == "reclaiming"
    # The crashed claim still refuses new managed attachments.
    with pytest.raises(ValueError, match="cleanup"):
        harness.lease_process()

    def later() -> datetime:
        return datetime.now(UTC) + timedelta(hours=1)

    resumed = WorktreeCustodian(
        harness.leases,
        harness.registry,
        WorktreeGit(runner=SubprocessGitRunner(env=harness.env)),
        now=later,
    )
    result = resumed.reconcile(lease_id)
    assert result.removed is True
    assert harness.leases.get(lease_id).state == "reclaimed"
    listing = harness.git("worktree", "list", "--porcelain", cwd=harness.repo)
    assert str(harness.worktree) not in listing


def test_unstoppable_process_blocks_reclaim_and_keeps_worktree(
    harness: Harness,
) -> None:
    lease_id = harness.lease_worktree()
    process_lease = harness.lease_process()
    harness.fake_os.ignore_signals = True
    (harness.worktree / "README.md").write_text("work in progress\n", "utf-8")
    checkpoint = harness.custodian.checkpoint(lease_id, "ENG-431")
    proof = harness.custodian.verify_remote(checkpoint)
    with pytest.raises(CleanupBlocked, match=process_lease):
        harness.custodian.reclaim(lease_id, proof)
    assert harness.worktree.exists()
    assert harness.leases.get(lease_id).state == "checkpointed"
