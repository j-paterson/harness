"""Checkpoint-verified worktree reclamation (INFRA-171).

A leased worktree is reclaimed only after its pending work is committed
as one clearly labeled WIP checkpoint, pushed to the configured remote,
and proven reachable there (fetch plus ancestry against the exact SHA).
Managed processes bound to the worktree are stopped only through the
``ProcessRegistry`` claimed-stop path carrying the checkpoint id; nothing
is ever signaled by name or path. Removal is ``git worktree remove``
(never ``--force``) followed by ``git worktree prune`` and a verification
that the path is no longer registered; ``rm`` is never invoked. There is
no retention delay: a proven checkpoint makes reclaim immediate.
"""

from __future__ import annotations

import re
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.git import GitError, WorktreeStatus

_ISSUE_PATTERN = re.compile(r"^[A-Z][A-Z0-9]*-[0-9]+$")

WIP_MESSAGE_TEMPLATE = "wip({issue}): checkpoint before resource cleanup"


class CleanupBlocked(RuntimeError):
    """A reclaim lacks its required safety evidence and changes nothing."""


class RemoteVerificationFailed(RuntimeError):
    """The checkpoint could not be proven reachable on the remote."""


class GitPort(Protocol):
    """Worktree git evidence and mutation boundary."""

    def status_of(self, path: Path) -> WorktreeStatus: ...

    def head_sha(self, path: Path) -> str: ...

    def branch(self, path: Path) -> str | None: ...

    def ahead(self, path: Path) -> int | None: ...

    def add_all(self, path: Path) -> None: ...

    def commit(self, path: Path, message: str) -> str: ...

    def push(self, path: Path, remote: str, branch: str) -> None: ...

    def fetch(self, path: Path, remote: str, branch: str) -> None: ...

    def remote_contains(
        self, path: Path, sha: str, remote: str, branch: str
    ) -> bool: ...

    def worktree_remove(self, repo_path: Path, path: Path) -> None: ...

    def worktree_prune(self, repo_path: Path) -> None: ...

    def worktree_list(self, repo_path: Path) -> tuple[str, ...]: ...


class RegistryPort(Protocol):
    """Exact leased-process boundary; stops go through the claimed path."""

    def active(self, project_key: str | None = None) -> tuple[Any, ...]: ...

    def request_stop(self, lease_id: str, checkpoint_id: str) -> Any: ...


@dataclass(frozen=True, slots=True)
class WorktreeLeaseInput:
    project_key: str
    issue_id: str
    repo_path: str
    path: str
    branch: str
    remote: str


@dataclass(frozen=True, slots=True)
class WorktreeLease:
    lease_id: str
    project_key: str
    issue_id: str
    repo_path: str
    path: str
    branch: str
    remote: str
    state: str
    checkpoint_id: str | None
    checkpoint_sha: str | None
    checkpoint_message: str | None
    checkpointed_at: str | None
    remote_verified_at: str | None
    verified_remote: str | None
    verified_branch: str | None
    verified_sha: str | None
    verified_checkpoint_id: str | None
    cleanup_owner: str | None
    cleanup_claimed_at: str | None
    reclaimed_at: str | None
    acquired_at: str


@dataclass(frozen=True, slots=True)
class WorktreeInspection:
    path: str
    branch: str | None
    head_sha: str
    modified: tuple[str, ...]
    untracked: tuple[str, ...]
    ahead: int | None
    process_lease_ids: tuple[str, ...]

    @property
    def clean(self) -> bool:
        return not self.modified and not self.untracked


@dataclass(frozen=True, slots=True)
class Checkpoint:
    checkpoint_id: str
    lease_id: str
    issue_id: str
    path: str
    branch: str
    remote: str
    sha: str
    commit_message: str | None
    created_at: str


@dataclass(frozen=True, slots=True)
class RemoteProof:
    lease_id: str
    checkpoint_id: str
    remote: str
    branch: str
    sha: str
    fetched_at: str


@dataclass(frozen=True, slots=True)
class CleanupResult:
    lease_id: str
    path: str
    removed: bool
    pruned: bool
    stopped_process_leases: tuple[str, ...]


class WorktreeLeases:
    """Durable worktree leases; every transition is journaled."""

    def __init__(
        self,
        database: Database,
        events: EventStore,
        *,
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._now = now or (lambda: datetime.now(UTC))
        self._ids = ids or (lambda: uuid.uuid4().hex)

    def register(self, request: WorktreeLeaseInput) -> WorktreeLease:
        for label, value in (
            ("project key", request.project_key),
            ("issue id", request.issue_id),
            ("repo path", request.repo_path),
            ("path", request.path),
            ("branch", request.branch),
            ("remote", request.remote),
        ):
            if not value.strip():
                raise ValueError(f"a worktree lease requires a {label}")
        lease_id = self._ids()
        stamp = self._now().isoformat()
        try:
            with self._database.transaction() as connection:
                connection.execute(
                    "INSERT INTO worktree_leases("
                    "lease_id, project_key, issue_id, repo_path, path, branch, "
                    "remote, state, acquired_at, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)",
                    (
                        lease_id,
                        request.project_key,
                        request.issue_id,
                        request.repo_path,
                        request.path,
                        request.branch,
                        request.remote,
                        stamp,
                        stamp,
                    ),
                )
                self._events.append(
                    connection,
                    EventInput(
                        event_type="worktree.registered",
                        aggregate_type="worktree_lease",
                        aggregate_id=lease_id,
                        payload={
                            "project_key": request.project_key,
                            "issue_id": request.issue_id,
                            "path": request.path,
                            "branch": request.branch,
                            "remote": request.remote,
                        },
                    ),
                )
        except sqlite3.IntegrityError as error:
            raise ValueError(
                f"worktree path {request.path} is already leased"
            ) from error
        return self.get(lease_id)

    def get(self, lease_id: str) -> WorktreeLease:
        row = self._database.execute(
            "SELECT * FROM worktree_leases WHERE lease_id = ?", (lease_id,)
        ).fetchone()
        if row is None:
            raise KeyError(lease_id)
        return _row_to_lease(row)

    def active(self, project_key: str | None = None) -> tuple[WorktreeLease, ...]:
        if project_key is None:
            rows = self._database.execute(
                "SELECT * FROM worktree_leases WHERE state != 'reclaimed' "
                "ORDER BY acquired_at ASC, rowid ASC"
            ).fetchall()
        else:
            rows = self._database.execute(
                "SELECT * FROM worktree_leases WHERE state != 'reclaimed' "
                "AND project_key = ? ORDER BY acquired_at ASC, rowid ASC",
                (project_key,),
            ).fetchall()
        return tuple(_row_to_lease(row) for row in rows)

    def record_checkpoint(
        self,
        lease_id: str,
        *,
        checkpoint_id: str,
        sha: str,
        message: str | None,
    ) -> None:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE worktree_leases SET state = 'checkpointed', "
                "checkpoint_id = ?, checkpoint_sha = ?, checkpoint_message = ?, "
                "checkpointed_at = ?, remote_verified_at = NULL, "
                "verified_remote = NULL, verified_branch = NULL, "
                "verified_sha = NULL, verified_checkpoint_id = NULL, "
                "updated_at = ? "
                "WHERE lease_id = ? AND state IN ('active', 'checkpointed')",
                (checkpoint_id, sha, message, stamp, stamp, lease_id),
            )
            if cursor.rowcount != 1:
                raise CleanupBlocked(
                    f"worktree lease {lease_id} cannot be checkpointed"
                )
            self._events.append(
                connection,
                EventInput(
                    event_type="worktree.checkpointed",
                    aggregate_type="worktree_lease",
                    aggregate_id=lease_id,
                    payload={
                        "checkpoint_id": checkpoint_id,
                        "sha": sha,
                        "message": message,
                    },
                ),
            )

    def record_remote_verified(
        self, lease_id: str, *, checkpoint_id: str, sha: str
    ) -> None:
        """Bind the verification to the lease's own recorded identity.

        The verified remote, branch, SHA, and checkpoint are copied from
        the lease row inside the compare-and-swap, and the timestamp is
        this store's clock: nothing here is caller-supplied evidence.
        """

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE worktree_leases SET remote_verified_at = ?, "
                "verified_remote = remote, verified_branch = branch, "
                "verified_sha = checkpoint_sha, "
                "verified_checkpoint_id = checkpoint_id, updated_at = ? "
                "WHERE lease_id = ? AND state = 'checkpointed' "
                "AND checkpoint_sha = ? AND checkpoint_id = ?",
                (stamp, stamp, lease_id, sha, checkpoint_id),
            )
            if cursor.rowcount != 1:
                raise RemoteVerificationFailed(
                    f"worktree lease {lease_id} has no current checkpoint {sha}"
                )
            self._events.append(
                connection,
                EventInput(
                    event_type="worktree.remote_verified",
                    aggregate_type="worktree_lease",
                    aggregate_id=lease_id,
                    payload={"sha": sha, "checkpoint_id": checkpoint_id},
                ),
            )

    def claim_cleanup(self, lease_id: str, *, owner: str) -> None:
        """Atomically claim a checkpointed lease for cleanup.

        While the lease is 'reclaiming' no new worker or process may
        attach to its path; the registry refuses such registrations.
        """

        if not owner.strip():
            raise ValueError("a cleanup claim requires an owner token")
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE worktree_leases SET state = 'reclaiming', "
                "cleanup_owner = ?, cleanup_claimed_at = ?, updated_at = ? "
                "WHERE lease_id = ? AND state = 'checkpointed'",
                (owner, stamp, stamp, lease_id),
            )
            if cursor.rowcount != 1:
                raise CleanupBlocked(
                    f"worktree lease {lease_id} cannot be claimed for cleanup"
                )
            self._events.append(
                connection,
                EventInput(
                    event_type="worktree.cleanup_claimed",
                    aggregate_type="worktree_lease",
                    aggregate_id=lease_id,
                    payload={"owner": owner},
                ),
            )

    def release_cleanup(self, lease_id: str, *, owner: str, reason: str) -> None:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE worktree_leases SET state = 'checkpointed', "
                "cleanup_owner = NULL, cleanup_claimed_at = NULL, "
                "updated_at = ? WHERE lease_id = ? AND state = 'reclaiming' "
                "AND cleanup_owner = ?",
                (stamp, lease_id, owner),
            )
            if cursor.rowcount != 1:
                raise CleanupBlocked(
                    f"cleanup claim on worktree lease {lease_id} was lost"
                )
            self._events.append(
                connection,
                EventInput(
                    event_type="worktree.cleanup_released",
                    aggregate_type="worktree_lease",
                    aggregate_id=lease_id,
                    payload={"owner": owner, "reason": reason},
                ),
            )

    def reconcile_cleanup(self, lease_id: str, *, stale_before: str) -> bool:
        """Release a crashed cleanup's claim once it has provably expired."""

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE worktree_leases SET state = 'checkpointed', "
                "cleanup_owner = NULL, cleanup_claimed_at = NULL, "
                "updated_at = ? WHERE lease_id = ? AND state = 'reclaiming' "
                "AND cleanup_claimed_at <= ?",
                (stamp, lease_id, stale_before),
            )
            if cursor.rowcount != 1:
                return False
            self._events.append(
                connection,
                EventInput(
                    event_type="worktree.cleanup_reconciled",
                    aggregate_type="worktree_lease",
                    aggregate_id=lease_id,
                    payload={"stale_before": stale_before},
                ),
            )
        return True

    def record_reclaimed(self, lease_id: str, *, owner: str) -> None:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE worktree_leases SET state = 'reclaimed', "
                "reclaimed_at = ?, cleanup_owner = NULL, "
                "cleanup_claimed_at = NULL, updated_at = ? "
                "WHERE lease_id = ? AND state = 'reclaiming' "
                "AND cleanup_owner = ?",
                (stamp, stamp, lease_id, owner),
            )
            if cursor.rowcount != 1:
                raise CleanupBlocked(
                    f"worktree lease {lease_id} cannot be marked reclaimed"
                )
            self._events.append(
                connection,
                EventInput(
                    event_type="worktree.reclaimed",
                    aggregate_type="worktree_lease",
                    aggregate_id=lease_id,
                    payload={"owner": owner},
                ),
            )


class WorktreeCustodian:
    """Checkpoint, prove, and only then reclaim one leased worktree."""

    def __init__(
        self,
        leases: WorktreeLeases,
        registry: RegistryPort,
        git: GitPort,
        *,
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
        max_proof_age_seconds: float = 900.0,
        cleanup_claim_ttl_seconds: float = 900.0,
    ) -> None:
        if max_proof_age_seconds <= 0:
            raise ValueError("max_proof_age_seconds must be positive")
        if cleanup_claim_ttl_seconds <= 0:
            raise ValueError("cleanup_claim_ttl_seconds must be positive")
        self._leases = leases
        self._registry = registry
        self._git = git
        self._now = now or (lambda: datetime.now(UTC))
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._max_proof_age = max_proof_age_seconds
        self._cleanup_claim_ttl = cleanup_claim_ttl_seconds

    def inspect(self, path: Path) -> WorktreeInspection:
        status = self._git.status_of(path)
        return WorktreeInspection(
            path=str(path),
            branch=self._git.branch(path),
            head_sha=self._git.head_sha(path),
            modified=status.modified,
            untracked=status.untracked,
            ahead=self._git.ahead(path),
            process_lease_ids=tuple(
                lease.lease_id for lease in self._bound_processes(path)
            ),
        )

    def checkpoint(self, lease_id: str, issue_id: str) -> Checkpoint:
        lease = self._leases.get(lease_id)
        if lease.state == "reclaimed":
            raise CleanupBlocked(f"worktree lease {lease_id} is reclaimed")
        if _ISSUE_PATTERN.match(issue_id) is None:
            raise ValueError(
                "issue id must be an identifier like ENG-431"
            )
        path = Path(lease.path)
        status = self._git.status_of(path)
        if status.clean:
            message = None
            sha = self._git.head_sha(path)
        else:
            message = WIP_MESSAGE_TEMPLATE.format(issue=issue_id)
            self._git.add_all(path)
            sha = self._git.commit(path, message)
        self._git.push(path, lease.remote, lease.branch)
        checkpoint_id = f"wtck-{self._ids()}"
        self._leases.record_checkpoint(
            lease_id, checkpoint_id=checkpoint_id, sha=sha, message=message
        )
        return Checkpoint(
            checkpoint_id=checkpoint_id,
            lease_id=lease_id,
            issue_id=issue_id,
            path=lease.path,
            branch=lease.branch,
            remote=lease.remote,
            sha=sha,
            commit_message=message,
            created_at=self._now().isoformat(),
        )

    def verify_remote(self, checkpoint: Checkpoint) -> RemoteProof:
        lease = self._leases.get(checkpoint.lease_id)
        if (
            lease.checkpoint_id != checkpoint.checkpoint_id
            or lease.checkpoint_sha != checkpoint.sha
        ):
            raise RemoteVerificationFailed(
                f"checkpoint {checkpoint.checkpoint_id} is not the recorded "
                f"checkpoint for lease {checkpoint.lease_id}"
            )
        path = Path(lease.path)
        try:
            self._git.fetch(path, lease.remote, lease.branch)
        except GitError as error:
            raise RemoteVerificationFailed(
                f"fetch of {lease.remote}/{lease.branch} failed: {error}"
            ) from error
        if not self._git.remote_contains(
            path, checkpoint.sha, lease.remote, lease.branch
        ):
            raise RemoteVerificationFailed(
                f"commit {checkpoint.sha[:12]} is not reachable on "
                f"{lease.remote}/{lease.branch}"
            )
        self._leases.record_remote_verified(
            lease.lease_id,
            checkpoint_id=checkpoint.checkpoint_id,
            sha=checkpoint.sha,
        )
        return RemoteProof(
            lease_id=lease.lease_id,
            checkpoint_id=checkpoint.checkpoint_id,
            remote=lease.remote,
            branch=lease.branch,
            sha=checkpoint.sha,
            fetched_at=self._now().isoformat(),
        )

    def reclaim(self, lease_id: str, proof: RemoteProof | None) -> CleanupResult:
        lease = self._leases.get(lease_id)
        if lease.state == "reclaimed":
            raise CleanupBlocked(f"worktree lease {lease_id} is reclaimed")
        if lease.state == "reclaiming":
            stale_before = (
                self._now() - timedelta(seconds=self._cleanup_claim_ttl)
            ).isoformat()
            if not self._leases.reconcile_cleanup(
                lease_id, stale_before=stale_before
            ):
                raise CleanupBlocked(
                    f"worktree lease {lease_id} is claimed for cleanup "
                    f"by {lease.cleanup_owner}"
                )
            lease = self._leases.get(lease_id)
        path = Path(lease.path)
        status = self._git.status_of(path)
        if not status.clean:
            raise CleanupBlocked(
                f"worktree {lease.path} is dirty "
                f"({len(status.modified)} modified, "
                f"{len(status.untracked)} untracked); checkpoint it first"
            )
        if proof is None:
            raise CleanupBlocked(
                "a reclaim requires a verified remote proof"
            )
        if proof.lease_id != lease_id:
            raise CleanupBlocked(
                f"remote proof belongs to lease {proof.lease_id}, "
                f"not {lease_id}"
            )
        if (
            lease.checkpoint_id != proof.checkpoint_id
            or lease.checkpoint_sha != proof.sha
        ):
            raise CleanupBlocked(
                "remote proof does not match the recorded checkpoint"
            )
        if proof.remote != lease.remote:
            raise CleanupBlocked(
                f"remote proof names remote {proof.remote}, "
                f"not the leased remote {lease.remote}"
            )
        if proof.branch != lease.branch:
            raise CleanupBlocked(
                f"remote proof names branch {proof.branch}, "
                f"not the leased branch {lease.branch}"
            )
        # Authorization comes only from the durable verification record
        # written by verify_remote; the caller's proof object is routing.
        if (
            lease.remote_verified_at is None
            or lease.verified_checkpoint_id != lease.checkpoint_id
            or lease.verified_sha != lease.checkpoint_sha
            or lease.verified_remote != lease.remote
            or lease.verified_branch != lease.branch
        ):
            raise CleanupBlocked(
                f"worktree lease {lease_id} has no durable remote "
                f"verification for its recorded checkpoint"
            )
        now = self._now()
        verified_at = datetime.fromisoformat(lease.remote_verified_at)
        if verified_at > now:
            raise CleanupBlocked(
                f"durable remote verification is in the future "
                f"({lease.remote_verified_at})"
            )
        age = (now - verified_at).total_seconds()
        if age > self._max_proof_age:
            raise CleanupBlocked(
                f"remote proof is stale ({age:.0f}s old)"
            )
        head = self._git.head_sha(path)
        if head != lease.checkpoint_sha:
            raise CleanupBlocked(
                f"HEAD moved after the remote proof "
                f"({head[:12]} != {lease.checkpoint_sha[:12]})"
            )
        checkpoint_id = lease.checkpoint_id
        owner = self._ids()
        self._leases.claim_cleanup(lease_id, owner=owner)
        try:
            stopped: list[str] = []
            for process_lease in self._bound_processes(path):
                result = self._registry.request_stop(
                    process_lease.lease_id, checkpoint_id
                )
                if not result.exited:
                    raise CleanupBlocked(
                        f"process lease {process_lease.lease_id} did not exit "
                        f"({result.reason})"
                    )
                stopped.append(process_lease.lease_id)
            lingering = self._bound_processes(path)
            if lingering:
                names = ", ".join(item.lease_id for item in lingering)
                raise CleanupBlocked(
                    f"process leases bound to {lease.path} after the "
                    f"claimed stops: {names}"
                )
            repo_path = Path(lease.repo_path)
            try:
                self._git.worktree_remove(repo_path, path)
            except GitError as error:
                raise CleanupBlocked(
                    f"git worktree remove refused {lease.path}: {error}"
                ) from error
            self._git.worktree_prune(repo_path)
            if lease.path in self._git.worktree_list(repo_path):
                raise CleanupBlocked(
                    f"worktree {lease.path} is still registered after prune"
                )
        except BaseException as error:
            self._leases.release_cleanup(
                lease_id, owner=owner, reason=type(error).__name__
            )
            raise
        self._leases.record_reclaimed(lease_id, owner=owner)
        return CleanupResult(
            lease_id=lease_id,
            path=lease.path,
            removed=True,
            pruned=True,
            stopped_process_leases=tuple(stopped),
        )

    def _bound_processes(self, path: Path) -> tuple[Any, ...]:
        """Active process leases whose recorded cwd lies inside ``path``."""

        return tuple(
            lease
            for lease in self._registry.active()
            if lease.cwd is not None and Path(lease.cwd).is_relative_to(path)
        )


def _row_to_lease(row: Any) -> WorktreeLease:
    return WorktreeLease(
        lease_id=str(row["lease_id"]),
        project_key=str(row["project_key"]),
        issue_id=str(row["issue_id"]),
        repo_path=str(row["repo_path"]),
        path=str(row["path"]),
        branch=str(row["branch"]),
        remote=str(row["remote"]),
        state=str(row["state"]),
        checkpoint_id=row["checkpoint_id"],
        checkpoint_sha=row["checkpoint_sha"],
        checkpoint_message=row["checkpoint_message"],
        checkpointed_at=row["checkpointed_at"],
        remote_verified_at=row["remote_verified_at"],
        verified_remote=row["verified_remote"],
        verified_branch=row["verified_branch"],
        verified_sha=row["verified_sha"],
        verified_checkpoint_id=row["verified_checkpoint_id"],
        cleanup_owner=row["cleanup_owner"],
        cleanup_claimed_at=row["cleanup_claimed_at"],
        reclaimed_at=row["reclaimed_at"],
        acquired_at=str(row["acquired_at"]),
    )
