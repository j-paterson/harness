"""Verify checkpoint-then-reclaim safety for leased worktrees (INFRA-171)."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.git import GitError, WorktreeStatus
from hermes_orchestrator.processes import StopResult
from hermes_orchestrator.worktrees import (
    CleanupBlocked,
    RemoteProof,
    RemoteVerificationFailed,
    WorktreeCustodian,
    WorktreeLeaseInput,
    WorktreeLeases,
)

NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)
SHA_A = "a" * 40
SHA_B = "b" * 40
WORKTREE = "/repo/.worktrees/eng-431"
REPO = "/repo"


@dataclass
class FakeGit:
    """In-memory git port recording every mutating command in order."""

    status: WorktreeStatus = field(
        default_factory=lambda: WorktreeStatus(modified=(), untracked=())
    )
    head: str = SHA_A
    current_branch: str | None = "feature/eng-431"
    ahead_count: int | None = 0
    remote_contains_result: bool = True
    fetch_error: GitError | None = None
    remove_error: GitError | None = None
    push_error: Exception | None = None
    prune_error: GitError | None = None
    head_message_value: str = "initial"
    worktree_paths: list[str] = field(default_factory=lambda: [REPO, WORKTREE])
    commands: list[tuple[str, ...]] = field(default_factory=list)
    commit_messages: list[str] = field(default_factory=list)

    def status_of(self, path: Path) -> WorktreeStatus:
        return self.status

    def head_sha(self, path: Path) -> str:
        return self.head

    def branch(self, path: Path) -> str | None:
        return self.current_branch

    def ahead(self, path: Path) -> int | None:
        return self.ahead_count

    def add_all(self, path: Path) -> None:
        self.commands.append(("add", "-A"))

    def commit(self, path: Path, message: str) -> str:
        self.commands.append(("commit", message))
        self.commit_messages.append(message)
        self.status = WorktreeStatus(modified=(), untracked=())
        self.head = SHA_B
        self.head_message_value = message
        return self.head

    def head_message(self, path: Path) -> str:
        return self.head_message_value

    def push(self, path: Path, remote: str, branch: str) -> None:
        if self.push_error is not None:
            error = self.push_error
            self.push_error = None
            raise error
        self.commands.append(("push", remote, branch))

    def fetch(self, path: Path, remote: str, branch: str) -> None:
        if self.fetch_error is not None:
            raise self.fetch_error
        self.commands.append(("fetch", remote, branch))

    def remote_contains(
        self, path: Path, sha: str, remote: str, branch: str
    ) -> bool:
        return self.remote_contains_result

    def worktree_remove(self, repo_path: Path, path: Path) -> None:
        if self.remove_error is not None:
            raise self.remove_error
        self.commands.append(("worktree", "remove", str(path)))
        self.worktree_paths = [p for p in self.worktree_paths if p != str(path)]

    def worktree_prune(self, repo_path: Path) -> None:
        if self.prune_error is not None:
            raise self.prune_error
        self.commands.append(("worktree", "prune"))

    def worktree_list(self, repo_path: Path) -> tuple[str, ...]:
        self.commands.append(("worktree", "list"))
        return tuple(self.worktree_paths)


@dataclass
class FakeProcessLease:
    lease_id: str
    cwd: str | None
    project_key: str = "demo"
    state: str = "active"


@dataclass
class FakeRegistry:
    leases: list[FakeProcessLease] = field(default_factory=list)
    stop_results: dict[str, StopResult] = field(default_factory=dict)
    stop_calls: list[tuple[str, str]] = field(default_factory=list)

    def active(self, project_key: str | None = None) -> tuple[FakeProcessLease, ...]:
        return tuple(
            lease
            for lease in self.leases
            if lease.state in ("active", "stopping")
            and (project_key is None or lease.project_key == project_key)
        )

    def request_stop(self, lease_id: str, checkpoint_id: str) -> StopResult:
        self.stop_calls.append((lease_id, checkpoint_id))
        result = self.stop_results.get(
            lease_id,
            StopResult(lease_id, 15, False, True, "terminated"),
        )
        if result.exited:
            for lease in self.leases:
                if lease.lease_id == lease_id:
                    lease.state = "stopped"
        return result


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def leases(database: Database) -> WorktreeLeases:
    ids = iter(f"wt-{n}" for n in range(1, 20))
    return WorktreeLeases(
        database,
        EventStore(database),
        now=lambda: NOW,
        ids=lambda: next(ids),
    )


@pytest.fixture
def fake_git() -> FakeGit:
    return FakeGit()


@pytest.fixture
def registry() -> FakeRegistry:
    return FakeRegistry()


@pytest.fixture
def custodian(
    leases: WorktreeLeases, fake_git: FakeGit, registry: FakeRegistry
) -> WorktreeCustodian:
    return WorktreeCustodian(
        leases,
        registry,
        fake_git,
        now=lambda: NOW,
        max_proof_age_seconds=900.0,
    )


def register(leases: WorktreeLeases) -> str:
    lease = leases.register(
        WorktreeLeaseInput(
            project_key="demo",
            issue_id="ENG-431",
            repo_path=REPO,
            path=WORKTREE,
            branch="feature/eng-431",
            remote="origin",
        )
    )
    return lease.lease_id


def checkpointed_proof(custodian: WorktreeCustodian, lease_id: str):
    checkpoint = custodian.checkpoint(lease_id, "ENG-431")
    return custodian.verify_remote(checkpoint)


def test_reclaim_completed_removes_clean_pushed_issue(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)

    result = custodian.reclaim_completed("demo", {"ENG-431"})

    assert result == {
        "reclaimed": [
            {
                "issue_id": "ENG-431",
                "lease_id": lease_id,
                "path": WORKTREE,
            }
        ],
        "skipped": [],
    }
    assert leases.get(lease_id).state == "reclaimed"
    assert WORKTREE not in fake_git.worktree_paths


def test_reclaim_completed_leaves_unfinished_issue_untouched(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)

    result = custodian.reclaim_completed("demo", set())

    assert result["reclaimed"] == []
    assert result["skipped"] == [
        {
            "issue_id": "ENG-431",
            "lease_id": lease_id,
            "path": WORKTREE,
            "reason": "issue_not_done",
        }
    ]
    assert leases.get(lease_id).state == "active"
    assert fake_git.commands == []


@pytest.mark.parametrize(
    ("status", "ahead", "reason"),
    [
        (WorktreeStatus(modified=("src/a.py",), untracked=()), 0, "dirty"),
        (WorktreeStatus(modified=(), untracked=()), 1, "unpushed"),
        (WorktreeStatus(modified=(), untracked=()), None, "upstream_unavailable"),
    ],
)
def test_reclaim_completed_leaves_unsafe_completed_issue_untouched(
    custodian: WorktreeCustodian,
    leases: WorktreeLeases,
    fake_git: FakeGit,
    status: WorktreeStatus,
    ahead: int | None,
    reason: str,
) -> None:
    lease_id = register(leases)
    fake_git.status = status
    fake_git.ahead_count = ahead

    result = custodian.reclaim_completed("demo", {"ENG-431"})

    assert result["reclaimed"] == []
    assert result["skipped"][0]["reason"] == reason
    assert leases.get(lease_id).state == "active"
    assert not any(command[0] == "worktree" for command in fake_git.commands)


# -- lease registration ----------------------------------------------------


def test_register_and_get_roundtrip(leases: WorktreeLeases) -> None:
    lease_id = register(leases)
    lease = leases.get(lease_id)
    assert lease.state == "active"
    assert lease.path == WORKTREE
    assert lease.branch == "feature/eng-431"
    assert lease.remote == "origin"


def test_register_rejects_duplicate_path(leases: WorktreeLeases) -> None:
    register(leases)
    with pytest.raises(ValueError, match="already leased"):
        register(leases)


def test_unknown_lease_raises_key_error(leases: WorktreeLeases) -> None:
    with pytest.raises(KeyError):
        leases.get("missing")


# -- checkpoint ------------------------------------------------------------


def test_wip_checkpoint_uses_clear_commit_message(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    fake_git.status = WorktreeStatus(modified=("src/a.py",), untracked=())
    checkpoint = custodian.checkpoint(lease_id, "ENG-431")
    assert checkpoint.commit_message == (
        "wip(ENG-431): checkpoint before resource cleanup"
    )
    assert fake_git.commit_messages == [checkpoint.commit_message]
    assert checkpoint.sha == SHA_B
    assert ("push", "origin", "feature/eng-431") in fake_git.commands


def test_checkpoint_of_clean_worktree_pushes_without_commit(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    checkpoint = custodian.checkpoint(lease_id, "ENG-431")
    assert checkpoint.commit_message is None
    assert checkpoint.sha == SHA_A
    assert fake_git.commit_messages == []
    assert ("push", "origin", "feature/eng-431") in fake_git.commands


def test_checkpoint_records_durable_lease_state(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    fake_git.status = WorktreeStatus(modified=("src/a.py",), untracked=())
    checkpoint = custodian.checkpoint(lease_id, "ENG-431")
    lease = leases.get(lease_id)
    assert lease.state == "checkpointed"
    assert lease.checkpoint_sha == checkpoint.sha
    assert lease.checkpoint_id == checkpoint.checkpoint_id


def test_checkpoint_rejects_malformed_issue_id(
    custodian: WorktreeCustodian, leases: WorktreeLeases
) -> None:
    lease_id = register(leases)
    with pytest.raises(ValueError, match="issue id"):
        custodian.checkpoint(lease_id, "eng 431; rm -rf /")


def test_checkpoint_of_reclaimed_lease_is_blocked(
    custodian: WorktreeCustodian, leases: WorktreeLeases
) -> None:
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    custodian.reclaim(lease_id, proof)
    with pytest.raises(CleanupBlocked, match="reclaimed"):
        custodian.checkpoint(lease_id, "ENG-431")


# -- remote proof ----------------------------------------------------------


def test_remote_proof_requires_branch_contains_sha(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    checkpoint = custodian.checkpoint(lease_id, "ENG-431")
    fake_git.remote_contains_result = False
    with pytest.raises(RemoteVerificationFailed):
        custodian.verify_remote(checkpoint)


def test_remote_proof_fails_closed_when_fetch_fails(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    checkpoint = custodian.checkpoint(lease_id, "ENG-431")
    fake_git.fetch_error = GitError("network down")
    with pytest.raises(RemoteVerificationFailed):
        custodian.verify_remote(checkpoint)


def test_remote_proof_rejects_superseded_checkpoint(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    stale = custodian.checkpoint(lease_id, "ENG-431")
    fake_git.status = WorktreeStatus(modified=("src/a.py",), untracked=())
    custodian.checkpoint(lease_id, "ENG-431")
    with pytest.raises(RemoteVerificationFailed, match="not the recorded"):
        custodian.verify_remote(stale)


def test_remote_proof_records_remote_branch_and_sha(
    custodian: WorktreeCustodian, leases: WorktreeLeases
) -> None:
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    assert proof.lease_id == lease_id
    assert proof.remote == "origin"
    assert proof.branch == "feature/eng-431"
    assert proof.sha == SHA_A
    assert proof.fetched_at == NOW.isoformat()


# -- reclaim ---------------------------------------------------------------


def test_dirty_worktree_blocks_direct_reclaim(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    fake_git.status = WorktreeStatus(modified=("src/a.py",), untracked=())
    with pytest.raises(CleanupBlocked, match="dirty"):
        custodian.reclaim(lease_id, None)


def test_untracked_files_block_direct_reclaim(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    fake_git.status = WorktreeStatus(modified=(), untracked=("notes.txt",))
    with pytest.raises(CleanupBlocked, match="dirty"):
        custodian.reclaim(lease_id, proof)


def test_reclaim_without_proof_is_blocked(
    custodian: WorktreeCustodian, leases: WorktreeLeases
) -> None:
    lease_id = register(leases)
    with pytest.raises(CleanupBlocked, match="remote proof"):
        custodian.reclaim(lease_id, None)


def test_reclaim_uses_git_worktree_remove_then_prune(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    result = custodian.reclaim(lease_id, proof)
    assert result.removed is True
    ordered = [c for c in fake_git.commands if c[0] == "worktree"]
    assert ordered == [
        ("worktree", "remove", WORKTREE),
        ("worktree", "prune"),
        ("worktree", "list"),
    ]
    assert leases.get(lease_id).state == "reclaimed"


def test_reclaim_blocked_when_head_moved_after_proof(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    fake_git.head = SHA_B
    with pytest.raises(CleanupBlocked, match="HEAD"):
        custodian.reclaim(lease_id, proof)


def test_reclaim_blocked_when_proof_is_stale(
    leases: WorktreeLeases, fake_git: FakeGit, registry: FakeRegistry
) -> None:
    clock = {"now": NOW}
    custodian = WorktreeCustodian(
        leases,
        registry,
        fake_git,
        now=lambda: clock["now"],
        max_proof_age_seconds=900.0,
    )
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    clock["now"] = datetime(2026, 8, 28, 13, tzinfo=UTC)
    with pytest.raises(CleanupBlocked, match="stale"):
        custodian.reclaim(lease_id, proof)


def test_reclaim_blocked_for_mismatched_lease_proof(
    custodian: WorktreeCustodian, leases: WorktreeLeases
) -> None:
    lease_id = register(leases)
    other = leases.register(
        WorktreeLeaseInput(
            project_key="demo",
            issue_id="ENG-432",
            repo_path=REPO,
            path="/repo/.worktrees/eng-432",
            branch="feature/eng-432",
            remote="origin",
        )
    )
    proof = checkpointed_proof(custodian, other.lease_id)
    with pytest.raises(CleanupBlocked, match="lease"):
        custodian.reclaim(lease_id, proof)


def test_active_process_lease_is_stopped_through_claimed_path(
    custodian: WorktreeCustodian,
    leases: WorktreeLeases,
    fake_git: FakeGit,
    registry: FakeRegistry,
) -> None:
    lease_id = register(leases)
    registry.leases.append(
        FakeProcessLease("proc-1", cwd=f"{WORKTREE}/src")
    )
    proof = checkpointed_proof(custodian, lease_id)
    result = custodian.reclaim(lease_id, proof)
    assert registry.stop_calls == [("proc-1", proof.checkpoint_id)]
    assert result.stopped_process_leases == ("proc-1",)


def test_unconfirmed_process_stop_blocks_reclaim(
    custodian: WorktreeCustodian,
    leases: WorktreeLeases,
    registry: FakeRegistry,
) -> None:
    lease_id = register(leases)
    registry.leases.append(FakeProcessLease("proc-1", cwd=WORKTREE))
    registry.stop_results["proc-1"] = StopResult(
        "proc-1", 9, True, False, "kill_unconfirmed"
    )
    proof = checkpointed_proof(custodian, lease_id)
    with pytest.raises(CleanupBlocked, match="proc-1"):
        custodian.reclaim(lease_id, proof)
    assert leases.get(lease_id).state == "checkpointed"


def test_unrelated_process_leases_are_never_stopped(
    custodian: WorktreeCustodian,
    leases: WorktreeLeases,
    registry: FakeRegistry,
) -> None:
    lease_id = register(leases)
    registry.leases.append(FakeProcessLease("proc-9", cwd="/elsewhere/repo"))
    registry.leases.append(FakeProcessLease("proc-8", cwd=None))
    proof = checkpointed_proof(custodian, lease_id)
    result = custodian.reclaim(lease_id, proof)
    assert registry.stop_calls == []
    assert result.stopped_process_leases == ()


def test_failed_worktree_remove_blocks_and_keeps_lease(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    fake_git.remove_error = GitError("worktree is locked")
    with pytest.raises(CleanupBlocked, match="remove"):
        custodian.reclaim(lease_id, proof)
    assert leases.get(lease_id).state == "checkpointed"
    assert ("worktree", "prune") not in fake_git.commands


def test_reclaim_keeps_claim_when_path_survives_prune(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    """Past an apparently-successful remove, the claim is never released:
    the worktree may be half-removed, so only reconciliation may proceed."""

    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)

    def keep_path(repo_path: Path, path: Path) -> None:
        fake_git.commands.append(("worktree", "remove", str(path)))

    fake_git.worktree_remove = keep_path  # type: ignore[method-assign]
    with pytest.raises(CleanupBlocked, match="still registered"):
        custodian.reclaim(lease_id, proof)
    assert leases.get(lease_id).state == "reclaiming"


def test_reclaim_of_reclaimed_lease_is_blocked(
    custodian: WorktreeCustodian, leases: WorktreeLeases
) -> None:
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    custodian.reclaim(lease_id, proof)
    with pytest.raises(CleanupBlocked, match="reclaimed"):
        custodian.reclaim(lease_id, proof)


def test_lifecycle_events_are_journaled(
    custodian: WorktreeCustodian, leases: WorktreeLeases, database: Database
) -> None:
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    custodian.reclaim(lease_id, proof)
    rows = database.execute(
        "SELECT event_type FROM events WHERE aggregate_id = ? ORDER BY sequence",
        (lease_id,),
    ).fetchall()
    assert [row["event_type"] for row in rows] == [
        "worktree.registered",
        "worktree.checkpointed",
        "worktree.remote_verified",
        "worktree.cleanup_claimed",
        "worktree.reclaimed",
    ]


# -- durable remote proof (correction e17c9a84, packet 1) ------------------


def hand_built_proof(leases: WorktreeLeases, lease_id: str) -> RemoteProof:
    """A proof forged from the recorded checkpoint, never verified."""

    lease = leases.get(lease_id)
    assert lease.checkpoint_id is not None
    assert lease.checkpoint_sha is not None
    return RemoteProof(
        lease_id=lease_id,
        checkpoint_id=lease.checkpoint_id,
        remote=lease.remote,
        branch=lease.branch,
        sha=lease.checkpoint_sha,
        fetched_at=NOW.isoformat(),
    )


def test_hand_built_proof_without_verify_remote_is_blocked(
    custodian: WorktreeCustodian, leases: WorktreeLeases
) -> None:
    lease_id = register(leases)
    custodian.checkpoint(lease_id, "ENG-431")
    forged = hand_built_proof(leases, lease_id)
    with pytest.raises(CleanupBlocked, match="durable remote verification"):
        custodian.reclaim(lease_id, forged)


def test_proof_remote_mismatch_is_blocked(
    custodian: WorktreeCustodian, leases: WorktreeLeases
) -> None:
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    with pytest.raises(CleanupBlocked, match="remote"):
        custodian.reclaim(lease_id, replace(proof, remote="fork"))


def test_proof_branch_mismatch_is_blocked(
    custodian: WorktreeCustodian, leases: WorktreeLeases
) -> None:
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    with pytest.raises(CleanupBlocked, match="branch"):
        custodian.reclaim(lease_id, replace(proof, branch="main"))


def test_future_durable_verification_is_blocked(
    leases: WorktreeLeases, fake_git: FakeGit, registry: FakeRegistry
) -> None:
    clock = {"now": NOW}
    custodian = WorktreeCustodian(
        leases, registry, fake_git, now=lambda: clock["now"]
    )
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    clock["now"] = datetime(2026, 8, 28, 11, tzinfo=UTC)
    with pytest.raises(CleanupBlocked, match="future"):
        custodian.reclaim(lease_id, proof)


def test_recheckpoint_clears_durable_verification(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    checkpointed_proof(custodian, lease_id)
    fake_git.status = WorktreeStatus(modified=("src/a.py",), untracked=())
    custodian.checkpoint(lease_id, "ENG-431")
    lease = leases.get(lease_id)
    assert lease.remote_verified_at is None
    assert lease.verified_sha is None
    assert lease.verified_checkpoint_id is None
    forged = hand_built_proof(leases, lease_id)
    with pytest.raises(CleanupBlocked, match="durable remote verification"):
        custodian.reclaim(lease_id, forged)


def test_fresh_caller_timestamp_cannot_mask_stale_durable_proof(
    database: Database, fake_git: FakeGit, registry: FakeRegistry
) -> None:
    clock = {"now": NOW}
    ids = iter(f"wt-{n}" for n in range(1, 20))
    leases = WorktreeLeases(
        database,
        EventStore(database),
        now=lambda: clock["now"],
        ids=lambda: next(ids),
    )
    custodian = WorktreeCustodian(
        leases, registry, fake_git, now=lambda: clock["now"]
    )
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    clock["now"] = datetime(2026, 8, 28, 14, tzinfo=UTC)
    freshened = replace(proof, fetched_at=clock["now"].isoformat())
    with pytest.raises(CleanupBlocked, match="stale"):
        custodian.reclaim(lease_id, freshened)


def test_durable_proof_survives_restart_within_age_limit(
    tmp_path: Path, fake_git: FakeGit, registry: FakeRegistry
) -> None:
    db_path = tmp_path / "restart.db"
    first = Database.open(db_path)
    ids = iter(f"wt-{n}" for n in range(1, 20))
    leases = WorktreeLeases(
        first, EventStore(first), now=lambda: NOW, ids=lambda: next(ids)
    )
    custodian = WorktreeCustodian(leases, registry, fake_git, now=lambda: NOW)
    lease_id = register(leases)
    custodian.verify_remote(custodian.checkpoint(lease_id, "ENG-431"))
    first.close()

    reopened = Database.open(db_path)
    try:
        leases2 = WorktreeLeases(reopened, EventStore(reopened), now=lambda: NOW)
        custodian2 = WorktreeCustodian(
            leases2, registry, fake_git, now=lambda: NOW
        )
        lease = leases2.get(lease_id)
        assert lease.remote_verified_at == NOW.isoformat()
        assert lease.verified_remote == "origin"
        assert lease.verified_branch == "feature/eng-431"
        proof = RemoteProof(
            lease_id=lease_id,
            checkpoint_id=lease.verified_checkpoint_id,
            remote=lease.verified_remote,
            branch=lease.verified_branch,
            sha=lease.verified_sha,
            fetched_at=lease.remote_verified_at,
        )
        result = custodian2.reclaim(lease_id, proof)
        assert result.removed is True
        assert leases2.get(lease_id).state == "reclaimed"
    finally:
        reopened.close()


# -- atomic cleanup claim (correction e17c9a84, packet 2) ------------------


def test_late_binding_process_blocks_removal(
    custodian: WorktreeCustodian,
    leases: WorktreeLeases,
    fake_git: FakeGit,
    registry: FakeRegistry,
) -> None:
    lease_id = register(leases)
    registry.leases.append(FakeProcessLease("proc-1", cwd=WORKTREE))
    original = registry.request_stop

    def stop_and_attach(process_lease_id: str, checkpoint_id: str) -> StopResult:
        result = original(process_lease_id, checkpoint_id)
        registry.leases.append(
            FakeProcessLease("proc-late", cwd=f"{WORKTREE}/src")
        )
        return result

    registry.request_stop = stop_and_attach  # type: ignore[method-assign]
    proof = checkpointed_proof(custodian, lease_id)
    with pytest.raises(CleanupBlocked, match="proc-late"):
        custodian.reclaim(lease_id, proof)
    assert ("worktree", "remove", WORKTREE) not in fake_git.commands
    assert leases.get(lease_id).state == "checkpointed"


def test_worktree_is_claimed_reclaiming_during_cleanup(
    custodian: WorktreeCustodian,
    leases: WorktreeLeases,
    registry: FakeRegistry,
) -> None:
    lease_id = register(leases)
    registry.leases.append(FakeProcessLease("proc-1", cwd=WORKTREE))
    observed: list[str] = []
    original = registry.request_stop

    def observe(process_lease_id: str, checkpoint_id: str) -> StopResult:
        observed.append(leases.get(lease_id).state)
        return original(process_lease_id, checkpoint_id)

    registry.request_stop = observe  # type: ignore[method-assign]
    proof = checkpointed_proof(custodian, lease_id)
    custodian.reclaim(lease_id, proof)
    assert observed == ["reclaiming"]
    assert leases.get(lease_id).state == "reclaimed"


def test_existing_cleanup_claim_blocks_concurrent_reclaim(
    custodian: WorktreeCustodian, leases: WorktreeLeases
) -> None:
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    leases.claim_cleanup(lease_id, owner="other-custodian")
    with pytest.raises(CleanupBlocked, match="claimed"):
        custodian.reclaim(lease_id, proof)
    leases.release_cleanup(
        lease_id, owner="other-custodian", reason="test handback"
    )
    assert custodian.reclaim(lease_id, proof).removed is True


def test_failed_stop_releases_cleanup_claim(
    custodian: WorktreeCustodian,
    leases: WorktreeLeases,
    registry: FakeRegistry,
    database: Database,
) -> None:
    lease_id = register(leases)
    registry.leases.append(FakeProcessLease("proc-1", cwd=WORKTREE))
    registry.stop_results["proc-1"] = StopResult(
        "proc-1", 9, True, False, "kill_unconfirmed"
    )
    proof = checkpointed_proof(custodian, lease_id)
    with pytest.raises(CleanupBlocked, match="proc-1"):
        custodian.reclaim(lease_id, proof)
    lease = leases.get(lease_id)
    assert lease.state == "checkpointed"
    assert lease.cleanup_owner is None
    rows = database.execute(
        "SELECT event_type FROM events WHERE aggregate_id = ? ORDER BY sequence",
        (lease_id,),
    ).fetchall()
    assert [row["event_type"] for row in rows][-2:] == [
        "worktree.cleanup_claimed",
        "worktree.cleanup_released",
    ]
    registry.stop_results.pop("proc-1")
    assert custodian.reclaim(lease_id, proof).removed is True


def test_failed_remove_releases_cleanup_claim(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    fake_git.remove_error = GitError("worktree is locked")
    with pytest.raises(CleanupBlocked, match="remove"):
        custodian.reclaim(lease_id, proof)
    lease = leases.get(lease_id)
    assert lease.state == "checkpointed"
    assert lease.cleanup_owner is None
    fake_git.remove_error = None
    assert custodian.reclaim(lease_id, proof).removed is True


def test_stale_cleanup_claim_is_reconciled_after_restart(
    database: Database, fake_git: FakeGit, registry: FakeRegistry
) -> None:
    clock = {"now": NOW}
    ids = iter(f"wt-{n}" for n in range(1, 20))
    leases = WorktreeLeases(
        database,
        EventStore(database),
        now=lambda: clock["now"],
        ids=lambda: next(ids),
    )
    custodian = WorktreeCustodian(
        leases, registry, fake_git, now=lambda: clock["now"]
    )
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    # A custodian that crashed mid-cleanup left its claim behind.
    leases.claim_cleanup(lease_id, owner="crashed-custodian")
    clock["now"] = datetime(2026, 8, 28, 14, tzinfo=UTC)
    # The stale claim is reconciled; the stale durable proof still blocks.
    with pytest.raises(CleanupBlocked, match="stale"):
        custodian.reclaim(lease_id, proof)
    lease = leases.get(lease_id)
    assert lease.state == "checkpointed"
    assert lease.cleanup_owner is None
    # Recovery: prove the checkpoint on the remote again, then reclaim.
    fresh = custodian.verify_remote(custodian.checkpoint(lease_id, "ENG-431"))
    assert custodian.reclaim(lease_id, fresh).removed is True


# -- crash reconciliation (INFRA-172) --------------------------------------


class FakeCrash(RuntimeError):
    """Simulates the process dying mid-pipeline; nothing may handle it."""


WIP_MESSAGE = "wip(ENG-431): checkpoint before resource cleanup"


def _never(path: Path) -> WorktreeStatus:
    raise AssertionError("a removed worktree path must never be inspected")


def make_stack(
    database: Database,
    fake_git: FakeGit,
    registry: FakeRegistry,
    clock: dict[str, datetime],
    **custodian_options: float,
) -> tuple[WorktreeLeases, WorktreeCustodian]:
    ids = iter(f"wt-{n}" for n in range(1, 20))
    leases = WorktreeLeases(
        database,
        EventStore(database),
        now=lambda: clock["now"],
        ids=lambda: next(ids),
    )
    custodian = WorktreeCustodian(
        leases, registry, fake_git, now=lambda: clock["now"], **custodian_options
    )
    return leases, custodian


def test_crash_between_commit_and_push_resumes_without_second_commit(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    fake_git.status = WorktreeStatus(modified=("src/a.py",), untracked=())
    fake_git.push_error = FakeCrash("crashed before push")
    with pytest.raises(FakeCrash):
        custodian.checkpoint(lease_id, "ENG-431")
    assert leases.get(lease_id).state == "active"
    assert fake_git.commit_messages == [WIP_MESSAGE]

    checkpoint = custodian.checkpoint(lease_id, "ENG-431")
    # The already-committed WIP is recognized, never committed twice.
    assert fake_git.commit_messages == [WIP_MESSAGE]
    assert checkpoint.commit_message == WIP_MESSAGE
    assert checkpoint.sha == SHA_B
    lease = leases.get(lease_id)
    assert lease.state == "checkpointed"
    assert lease.checkpoint_message == WIP_MESSAGE


def test_crash_between_push_and_record_mints_identity_exactly_once(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    fake_git.status = WorktreeStatus(modified=("src/a.py",), untracked=())
    original = leases.record_checkpoint
    crashes = iter([FakeCrash("crashed before the durable record")])

    def crash_once(*args: object, **kwargs: object) -> None:
        crash = next(crashes, None)
        if crash is not None:
            raise crash
        original(*args, **kwargs)  # type: ignore[arg-type]

    leases.record_checkpoint = crash_once  # type: ignore[method-assign]
    with pytest.raises(FakeCrash):
        custodian.checkpoint(lease_id, "ENG-431")
    assert leases.get(lease_id).state == "active"

    checkpoint = custodian.checkpoint(lease_id, "ENG-431")
    assert fake_git.commit_messages == [WIP_MESSAGE]
    # The push is repeat-safe and repeated; the identity is minted once.
    pushes = [c for c in fake_git.commands if c[0] == "push"]
    assert pushes == [("push", "origin", "feature/eng-431")] * 2
    lease = leases.get(lease_id)
    assert lease.state == "checkpointed"
    assert lease.checkpoint_id == checkpoint.checkpoint_id
    assert lease.checkpoint_sha == SHA_B
    assert lease.checkpoint_message == WIP_MESSAGE


def test_checkpoint_reuses_recorded_identity_when_nothing_changed(
    custodian: WorktreeCustodian, leases: WorktreeLeases, fake_git: FakeGit
) -> None:
    lease_id = register(leases)
    first = custodian.checkpoint(lease_id, "ENG-431")
    proof = custodian.verify_remote(first)

    again = custodian.checkpoint(lease_id, "ENG-431")
    assert again.checkpoint_id == first.checkpoint_id
    assert again.sha == first.sha
    # No second push, no re-record: the durable verification survives.
    assert [c for c in fake_git.commands if c[0] == "push"] == [
        ("push", "origin", "feature/eng-431")
    ]
    assert leases.get(lease_id).remote_verified_at == NOW.isoformat()
    assert custodian.reclaim(lease_id, proof).removed is True


def test_checkpoint_on_cleanup_claimed_lease_is_blocked(
    custodian: WorktreeCustodian, leases: WorktreeLeases
) -> None:
    lease_id = register(leases)
    checkpointed_proof(custodian, lease_id)
    leases.claim_cleanup(lease_id, owner="cleanup-1")
    with pytest.raises(CleanupBlocked, match="cleanup"):
        custodian.checkpoint(lease_id, "ENG-431")


def test_crash_after_removal_is_converged_without_second_removal(
    database: Database, fake_git: FakeGit, registry: FakeRegistry
) -> None:
    clock = {"now": NOW}
    leases, custodian = make_stack(
        database, fake_git, registry, clock, cleanup_claim_ttl_seconds=60.0
    )
    lease_id = register(leases)
    checkpointed_proof(custodian, lease_id)
    # The crashed custodian removed the worktree but never recorded it.
    leases.claim_cleanup(lease_id, owner="crashed-custodian")
    fake_git.worktree_paths = [REPO]
    fake_git.status_of = _never  # type: ignore[method-assign]
    fake_git.head_sha = _never  # type: ignore[method-assign]

    clock["now"] = NOW + timedelta(seconds=120)
    result = custodian.reconcile(lease_id)
    assert result.removed is True
    assert result.stopped_process_leases == ()
    assert leases.get(lease_id).state == "reclaimed"
    assert all(c[:2] != ("worktree", "remove") for c in fake_git.commands)
    assert ("worktree", "prune") in fake_git.commands


def test_reconcile_resumes_expired_claim_and_finishes_cleanup(
    database: Database, fake_git: FakeGit, registry: FakeRegistry
) -> None:
    clock = {"now": NOW}
    leases, custodian = make_stack(
        database, fake_git, registry, clock, cleanup_claim_ttl_seconds=60.0
    )
    lease_id = register(leases)
    registry.leases.append(FakeProcessLease("proc-1", cwd=WORKTREE))
    checkpointed_proof(custodian, lease_id)
    recorded = leases.get(lease_id).checkpoint_id
    assert recorded is not None
    leases.claim_cleanup(lease_id, owner="crashed-custodian")

    clock["now"] = NOW + timedelta(seconds=120)
    result = custodian.reconcile(lease_id)
    # The stop reuses the recorded checkpoint id; nothing is re-derived.
    assert registry.stop_calls == [("proc-1", recorded)]
    assert result.removed is True
    assert leases.get(lease_id).state == "reclaimed"
    events = [
        row["event_type"]
        for row in database.execute(
            "SELECT event_type FROM events WHERE aggregate_id = ? "
            "ORDER BY sequence",
            (lease_id,),
        ).fetchall()
    ]
    # The claim is taken over, never released to 'checkpointed' mid-resume,
    # so the registry's attachment refusal holds throughout.
    assert "worktree.cleanup_recovered" in events
    assert "worktree.cleanup_released" not in events
    assert events[-1] == "worktree.reclaimed"


def test_reconcile_blocked_while_claim_is_live(
    custodian: WorktreeCustodian, leases: WorktreeLeases
) -> None:
    lease_id = register(leases)
    checkpointed_proof(custodian, lease_id)
    leases.claim_cleanup(lease_id, owner="live-custodian")
    with pytest.raises(CleanupBlocked, match="claimed"):
        custodian.reconcile(lease_id)
    lease = leases.get(lease_id)
    assert lease.state == "reclaiming"
    assert lease.cleanup_owner == "live-custodian"


def test_reconcile_of_unclaimed_lease_is_blocked(
    custodian: WorktreeCustodian, leases: WorktreeLeases
) -> None:
    lease_id = register(leases)
    with pytest.raises(CleanupBlocked, match="not claimed"):
        custodian.reconcile(lease_id)


def test_reconcile_releases_claim_when_durable_proof_is_stale(
    database: Database, fake_git: FakeGit, registry: FakeRegistry
) -> None:
    clock = {"now": NOW}
    leases, custodian = make_stack(
        database, fake_git, registry, clock, cleanup_claim_ttl_seconds=60.0
    )
    lease_id = register(leases)
    checkpointed_proof(custodian, lease_id)
    leases.claim_cleanup(lease_id, owner="crashed-custodian")

    clock["now"] = NOW + timedelta(seconds=2000)
    # Stale proof is never grandfathered: the resume releases and demands
    # a fresh verification.
    with pytest.raises(CleanupBlocked, match="stale"):
        custodian.reconcile(lease_id)
    lease = leases.get(lease_id)
    assert lease.state == "checkpointed"
    assert lease.cleanup_owner is None
    fresh = custodian.verify_remote(custodian.checkpoint(lease_id, "ENG-431"))
    assert custodian.reclaim(lease_id, fresh).removed is True


def test_reconcile_releases_claim_when_new_work_appeared(
    database: Database, fake_git: FakeGit, registry: FakeRegistry
) -> None:
    clock = {"now": NOW}
    leases, custodian = make_stack(
        database, fake_git, registry, clock, cleanup_claim_ttl_seconds=60.0
    )
    lease_id = register(leases)
    checkpointed_proof(custodian, lease_id)
    leases.claim_cleanup(lease_id, owner="crashed-custodian")
    fake_git.status = WorktreeStatus(modified=("src/a.py",), untracked=())

    clock["now"] = NOW + timedelta(seconds=120)
    with pytest.raises(CleanupBlocked, match="dirty"):
        custodian.reconcile(lease_id)
    lease = leases.get(lease_id)
    assert lease.state == "checkpointed"
    assert lease.cleanup_owner is None


def test_prune_failure_after_removal_keeps_claim_for_reconciliation(
    database: Database, fake_git: FakeGit, registry: FakeRegistry
) -> None:
    clock = {"now": NOW}
    leases, custodian = make_stack(
        database, fake_git, registry, clock, cleanup_claim_ttl_seconds=60.0
    )
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    fake_git.prune_error = GitError("disk went away")
    with pytest.raises(CleanupBlocked, match="halted after removal"):
        custodian.reclaim(lease_id, proof)
    lease = leases.get(lease_id)
    assert lease.state == "reclaiming"
    assert WORKTREE not in fake_git.worktree_paths

    fake_git.prune_error = None
    clock["now"] = NOW + timedelta(seconds=120)
    result = custodian.reconcile(lease_id)
    assert result.removed is True
    assert leases.get(lease_id).state == "reclaimed"
    removes = [c for c in fake_git.commands if c[:2] == ("worktree", "remove")]
    assert removes == [("worktree", "remove", WORKTREE)]


def test_reclaim_on_crashed_claim_with_removed_path_converges(
    database: Database, fake_git: FakeGit, registry: FakeRegistry
) -> None:
    clock = {"now": NOW}
    leases, custodian = make_stack(
        database, fake_git, registry, clock, cleanup_claim_ttl_seconds=60.0
    )
    lease_id = register(leases)
    proof = checkpointed_proof(custodian, lease_id)
    leases.claim_cleanup(lease_id, owner="crashed-custodian")
    fake_git.worktree_paths = [REPO]

    clock["now"] = NOW + timedelta(seconds=120)
    result = custodian.reclaim(lease_id, proof)
    assert result.removed is True
    assert leases.get(lease_id).state == "reclaimed"
    assert all(c[:2] != ("worktree", "remove") for c in fake_git.commands)


def test_crash_after_removal_survives_restart_and_reconciles(
    tmp_path: Path, fake_git: FakeGit, registry: FakeRegistry
) -> None:
    db_path = tmp_path / "restart.db"
    first = Database.open(db_path)
    clock = {"now": NOW}
    leases, custodian = make_stack(
        first, fake_git, registry, clock, cleanup_claim_ttl_seconds=60.0
    )
    lease_id = register(leases)
    checkpointed_proof(custodian, lease_id)
    leases.claim_cleanup(lease_id, owner="crashed-custodian")
    fake_git.worktree_paths = [REPO]
    first.close()

    reopened = Database.open(db_path)
    try:
        clock2 = {"now": NOW + timedelta(seconds=120)}
        leases2, custodian2 = make_stack(
            reopened, fake_git, registry, clock2, cleanup_claim_ttl_seconds=60.0
        )
        result = custodian2.reconcile(lease_id)
        assert result.removed is True
        assert leases2.get(lease_id).state == "reclaimed"
        assert all(c[:2] != ("worktree", "remove") for c in fake_git.commands)
    finally:
        reopened.close()


# -- inspection ------------------------------------------------------------


def test_inspect_reports_status_head_and_bound_processes(
    custodian: WorktreeCustodian,
    leases: WorktreeLeases,
    fake_git: FakeGit,
    registry: FakeRegistry,
) -> None:
    register(leases)
    fake_git.status = WorktreeStatus(modified=("src/a.py",), untracked=("n.txt",))
    fake_git.ahead_count = 2
    registry.leases.append(FakeProcessLease("proc-1", cwd=WORKTREE))
    inspection = custodian.inspect(Path(WORKTREE))
    assert inspection.clean is False
    assert inspection.modified == ("src/a.py",)
    assert inspection.untracked == ("n.txt",)
    assert inspection.head_sha == SHA_A
    assert inspection.branch == "feature/eng-431"
    assert inspection.ahead == 2
    assert inspection.process_lease_ids == ("proc-1",)


class _RecordingWorktreeGit:
    """Records dedicated-worktree creation without touching a real repo."""

    def __init__(self) -> None:
        self.added: list[tuple[Path, Path, str]] = []
        self.created: list[tuple[Path, str, str]] = []
        self.fetched: list[tuple[str, str]] = []
        self.existing_branches: set[str] = set()

    def worktree_add_branch(
        self, repo_path: Path, path: Path, branch: str
    ) -> None:
        self.added.append((repo_path, path, branch))
        path.mkdir(parents=True, exist_ok=True)

    def local_branch_exists(self, repo_path: Path, branch: str) -> bool:
        return branch in self.existing_branches

    def fetch(self, repo_path: Path, remote: str, branch: str) -> None:
        self.fetched.append((remote, branch))

    def worktree_add_new_branch(
        self, repo_path: Path, path: Path, branch: str, start_point: str
    ) -> None:
        self.created.append((path, branch, start_point))
        path.mkdir(parents=True, exist_ok=True)


def test_two_admitted_issues_resolve_distinct_bound_paths(
    leases: WorktreeLeases, tmp_path: Path
) -> None:
    """The decisive regression for reopened INFRA-219.

    Publication resolves ONE exact issue checkout per admitted lane. A
    shared checkout could not: ``worktree_leases_live_path_idx`` is
    UNIQUE on live ``path``, so two issues sharing one path is not even
    representable, and every lane that did resolve would return the same
    checkout — the wrong-head hazard the guard exists to prevent.
    """

    from hermes_orchestrator.emission import resolve_lane
    from hermes_orchestrator.worktrees import bind_issue_worktree

    repo = tmp_path / "project"
    repo.mkdir()
    git = _RecordingWorktreeGit()
    for issue, branch in (("INFRA-1", "feature/one"), ("INFRA-2", "feature/two")):
        bind_issue_worktree(
            leases,
            git,
            project_key="demo",
            issue_id=issue,
            repo_path=repo,
            branch=branch,
        )

    first = resolve_lane(leases, "demo", "INFRA-1")
    second = resolve_lane(leases, "demo", "INFRA-2")

    assert first.path != second.path
    assert first.branch == "feature/one"
    assert second.branch == "feature/two"
    # First assignment: no local branch, so each lane is CREATED from
    # the fetched integration head rather than reusing a stale branch.
    assert len(git.created) == 2
    assert git.fetched == [("origin", "main"), ("origin", "main")]


def test_bind_issue_worktree_is_idempotent(
    leases: WorktreeLeases, tmp_path: Path
) -> None:
    """``resolve_lane`` refuses zero live leases AND refuses more than
    one, so a repeated bind must add neither a second lease nor a second
    worktree — otherwise publication breaks exactly as hard as before."""

    from hermes_orchestrator.emission import resolve_lane
    from hermes_orchestrator.worktrees import bind_issue_worktree

    repo = tmp_path / "project"
    repo.mkdir()
    git = _RecordingWorktreeGit()
    first = bind_issue_worktree(
        leases, git, project_key="demo", issue_id="INFRA-1",
        repo_path=repo, branch="feature/one",
    )
    again = bind_issue_worktree(
        leases, git, project_key="demo", issue_id="INFRA-1",
        repo_path=repo, branch="feature/one",
    )

    assert again.lease_id == first.lease_id
    assert len(git.created) == 1
    assert str(resolve_lane(leases, "demo", "INFRA-1").path) == first.path


def test_forbidden_checkouts_are_refused_fail_closed(
    leases: WorktreeLeases, tmp_path: Path
) -> None:
    """The coordinator CWD, the harness checkout and the primary
    checkout are never leaseable, and a branchless checkout is refused
    before any git call — refusals write nothing (INFRA-214)."""

    from hermes_orchestrator.worktrees import (
        IssueWorktreeRefused,
        bind_issue_worktree,
        dedicated_issue_path,
    )

    repo = tmp_path / "project"
    repo.mkdir()
    git = _RecordingWorktreeGit()

    with pytest.raises(IssueWorktreeRefused, match="no branch"):
        bind_issue_worktree(
            leases, git, project_key="demo", issue_id="INFRA-1",
            repo_path=repo, branch="   ",
        )

    # The derived path can never equal a forbidden checkout, so the
    # explicit refusal is belt-and-braces: prove it still fires.
    derived = dedicated_issue_path(repo, "INFRA-1")
    with pytest.raises(IssueWorktreeRefused, match="not leaseable"):
        bind_issue_worktree(
            leases, git, project_key="demo", issue_id="INFRA-1",
            repo_path=repo, branch="feature/one", forbidden=(derived,),
        )

    assert git.added == [] and git.created == []
    assert leases.active("demo") == ()


def test_first_assignment_creates_the_branch_from_the_fetched_head(
    leases: WorktreeLeases, tmp_path: Path
) -> None:
    """INFRA-214 live-path requirement: a newly admitted issue has NO
    local feature branch, so a worktree add that assumes one fails. The
    lane's branch is created from the project's FETCHED
    ``origin/<integration_branch>`` — never a stale local main, which
    may lag the remote by many merges and would silently base the lane
    on an old head."""

    from hermes_orchestrator.worktrees import bind_issue_worktree

    repo = tmp_path / "project"
    repo.mkdir()
    git = _RecordingWorktreeGit()

    bind_issue_worktree(
        leases, git, project_key="demo", issue_id="INFRA-1",
        repo_path=repo, branch="feature/infra-1",
        integration_branch="release",
    )

    assert git.added == []
    assert git.fetched == [("origin", "release")]
    [(path, branch, start_point)] = git.created
    assert branch == "feature/infra-1"
    assert start_point == "origin/release"
    assert path.name == "project-issue-INFRA-1"


def test_an_existing_issue_branch_is_reused_not_recreated(
    leases: WorktreeLeases, tmp_path: Path
) -> None:
    """Only a validated EXISTING issue branch is reused; reuse must not
    fetch or re-create it (INFRA-214)."""

    from hermes_orchestrator.worktrees import bind_issue_worktree

    repo = tmp_path / "project"
    repo.mkdir()
    git = _RecordingWorktreeGit()
    git.existing_branches.add("feature/infra-1")

    bind_issue_worktree(
        leases, git, project_key="demo", issue_id="INFRA-1",
        repo_path=repo, branch="feature/infra-1",
    )

    assert git.created == []
    assert git.fetched == []
    [(_repo, _path, branch)] = git.added
    assert branch == "feature/infra-1"
