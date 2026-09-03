"""Verify the Fable -> Sol issue-worktree lease writer transfer (INFRA-222).

The immutable review artifact is the recorded candidate Git SHA; the
worktree directory is an operational resource whose EXCLUSIVE writer may
change. These tests prove the writer-transfer compare-and-swap on
``worktree_leases``: a fresh lease starts as Fable's; a fable -> sol
hand-over only succeeds when the tree is clean and the observed HEAD
equals the candidate SHA being submitted; a sol -> fable return (rework)
only succeeds when the worktree is unchanged from what was submitted;
every refusal leaves the row byte-identical; the generation-keyed
compare-and-swap defeats a concurrent/stale transfer attempt; and a
transaction that fails after the UPDATE rolls back cleanly so the
recorded owner survives a crash mid-transfer.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.worktrees import (
    CleanupBlocked,
    WorktreeLeaseInput,
    WorktreeLeases,
)

NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)
SHA_A = "a" * 40
SHA_B = "b" * 40
SHA_C = "c" * 40
WORKTREE = "/repo/.worktrees/eng-431"
REPO = "/repo"


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


def register(leases: WorktreeLeases, issue_id: str = "ENG-431") -> str:
    lease = leases.register(
        WorktreeLeaseInput(
            project_key="demo",
            issue_id=issue_id,
            repo_path=REPO,
            path=WORKTREE,
            branch="feature/eng-431",
            remote="origin",
        )
    )
    return lease.lease_id


def row_snapshot(database: Database, lease_id: str) -> dict[str, object]:
    row = database.execute(
        "SELECT * FROM worktree_leases WHERE lease_id = ?", (lease_id,)
    ).fetchone()
    return dict(row)


def test_fresh_lease_is_writer_fable_generation_one(leases: WorktreeLeases) -> None:
    lease_id = register(leases)
    lease = leases.get(lease_id)
    assert lease.writer_role == "fable"
    assert lease.writer_ref is None
    assert lease.writer_generation == 1
    assert lease.submitted_candidate_sha is None
    assert lease.transferred_at is None


def test_fable_to_sol_transfer_records_candidate_and_bumps_generation(
    leases: WorktreeLeases,
) -> None:
    lease_id = register(leases)
    lease = leases.transfer_writer(
        lease_id,
        expected_writer_role="fable",
        expected_writer_ref=None,
        to_writer_role="sol",
        to_writer_ref="sol-gen-1",
        observed_head=SHA_A,
        tree_clean=True,
        submitted_candidate_sha=SHA_A,
    )
    assert lease.writer_role == "sol"
    assert lease.writer_ref == "sol-gen-1"
    assert lease.writer_generation == 2
    assert lease.submitted_candidate_sha == SHA_A
    assert lease.transferred_at == NOW.isoformat()


def test_transfer_refuses_dirty_tree_and_leaves_row_unchanged(
    leases: WorktreeLeases, database: Database
) -> None:
    lease_id = register(leases)
    before = row_snapshot(database, lease_id)
    with pytest.raises(CleanupBlocked):
        leases.transfer_writer(
            lease_id,
            expected_writer_role="fable",
            expected_writer_ref=None,
            to_writer_role="sol",
            to_writer_ref="sol-gen-1",
            observed_head=SHA_A,
            tree_clean=False,
            submitted_candidate_sha=SHA_A,
        )
    assert row_snapshot(database, lease_id) == before


def test_transfer_refuses_head_mismatch_with_candidate_sha(
    leases: WorktreeLeases, database: Database
) -> None:
    lease_id = register(leases)
    before = row_snapshot(database, lease_id)
    with pytest.raises(CleanupBlocked):
        leases.transfer_writer(
            lease_id,
            expected_writer_role="fable",
            expected_writer_ref=None,
            to_writer_role="sol",
            to_writer_ref="sol-gen-1",
            observed_head=SHA_B,
            tree_clean=True,
            submitted_candidate_sha=SHA_A,
        )
    assert row_snapshot(database, lease_id) == before


def test_transfer_refuses_wrong_expected_writer(
    leases: WorktreeLeases, database: Database
) -> None:
    lease_id = register(leases)
    before = row_snapshot(database, lease_id)
    with pytest.raises(CleanupBlocked):
        leases.transfer_writer(
            lease_id,
            expected_writer_role="sol",
            expected_writer_ref=None,
            to_writer_role="fable",
            to_writer_ref=None,
            observed_head=SHA_A,
            tree_clean=True,
        )
    assert row_snapshot(database, lease_id) == before


def test_transfer_refuses_same_role_transfer(
    leases: WorktreeLeases, database: Database
) -> None:
    lease_id = register(leases)
    before = row_snapshot(database, lease_id)
    with pytest.raises(CleanupBlocked):
        leases.transfer_writer(
            lease_id,
            expected_writer_role="fable",
            expected_writer_ref=None,
            to_writer_role="fable",
            to_writer_ref=None,
            observed_head=SHA_A,
            tree_clean=True,
        )
    assert row_snapshot(database, lease_id) == before


def test_sol_to_fable_return_requires_unchanged_head(
    leases: WorktreeLeases, database: Database
) -> None:
    lease_id = register(leases)
    leases.transfer_writer(
        lease_id,
        expected_writer_role="fable",
        expected_writer_ref=None,
        to_writer_role="sol",
        to_writer_ref="sol-gen-1",
        observed_head=SHA_A,
        tree_clean=True,
        submitted_candidate_sha=SHA_A,
    )
    before = row_snapshot(database, lease_id)
    # A rework return whose observed HEAD does not equal the recorded
    # submitted candidate SHA must be refused: rework makes no edits.
    with pytest.raises(CleanupBlocked):
        leases.transfer_writer(
            lease_id,
            expected_writer_role="sol",
            expected_writer_ref="sol-gen-1",
            to_writer_role="fable",
            to_writer_ref=None,
            observed_head=SHA_B,
            tree_clean=True,
        )
    assert row_snapshot(database, lease_id) == before

    lease = leases.transfer_writer(
        lease_id,
        expected_writer_role="sol",
        expected_writer_ref="sol-gen-1",
        to_writer_role="fable",
        to_writer_ref=None,
        observed_head=SHA_A,
        tree_clean=True,
    )
    assert lease.writer_role == "fable"
    assert lease.writer_ref is None
    assert lease.writer_generation == 3
    # The candidate SHA is unchanged by a rework return.
    assert lease.submitted_candidate_sha == SHA_A


def test_generation_keeps_increasing_across_repeated_round_trips(
    leases: WorktreeLeases,
) -> None:
    lease_id = register(leases)
    leases.transfer_writer(
        lease_id,
        expected_writer_role="fable",
        expected_writer_ref=None,
        to_writer_role="sol",
        to_writer_ref="sol-gen-1",
        observed_head=SHA_A,
        tree_clean=True,
        submitted_candidate_sha=SHA_A,
    )
    leases.transfer_writer(
        lease_id,
        expected_writer_role="sol",
        expected_writer_ref="sol-gen-1",
        to_writer_role="fable",
        to_writer_ref=None,
        observed_head=SHA_A,
        tree_clean=True,
    )
    # A NEW candidate SHA after rework: a fresh fable -> sol transfer
    # must work and the generation must keep climbing monotonically.
    lease = leases.transfer_writer(
        lease_id,
        expected_writer_role="fable",
        expected_writer_ref=None,
        to_writer_role="sol",
        to_writer_ref="sol-gen-2",
        observed_head=SHA_B,
        tree_clean=True,
        submitted_candidate_sha=SHA_B,
    )
    assert lease.writer_role == "sol"
    assert lease.writer_ref == "sol-gen-2"
    assert lease.writer_generation == 4
    assert lease.submitted_candidate_sha == SHA_B


def test_stale_concurrent_transfer_attempt_refused(
    leases: WorktreeLeases, database: Database
) -> None:
    """A second caller racing on the pre-transfer state loses (INFRA-222).

    Both callers observe the lease as fable-owned before either commits.
    The first transfer_writer call wins and bumps the generation; the
    second, still carrying its now-stale expected writer role, is
    refused by the same compare-and-swap that keys the UPDATE on
    (lease_id, writer_role, writer_generation) — exactly the mechanism
    that defeats a genuinely concurrent transfer.
    """

    lease_id = register(leases)
    winner = leases.transfer_writer(
        lease_id,
        expected_writer_role="fable",
        expected_writer_ref=None,
        to_writer_role="sol",
        to_writer_ref="sol-winner",
        observed_head=SHA_A,
        tree_clean=True,
        submitted_candidate_sha=SHA_A,
    )
    assert winner.writer_generation == 2
    before = row_snapshot(database, lease_id)
    with pytest.raises(CleanupBlocked):
        leases.transfer_writer(
            lease_id,
            expected_writer_role="fable",
            expected_writer_ref=None,
            to_writer_role="sol",
            to_writer_ref="sol-loser",
            observed_head=SHA_A,
            tree_clean=True,
            submitted_candidate_sha=SHA_A,
        )
    after = row_snapshot(database, lease_id)
    assert after == before
    assert after["writer_generation"] == 2
    assert after["writer_ref"] == "sol-winner"


def test_transfer_rolls_back_cleanly_on_crash_after_update(
    leases: WorktreeLeases, database: Database
) -> None:
    """A failure after the UPDATE inside the same transaction rolls back.

    Crash recovery must never leave a half-transferred lease: the
    recorded owner before the crash is the owner that survives.
    """

    lease_id = register(leases)
    before = row_snapshot(database, lease_id)

    class Boom(RuntimeError):
        pass

    with pytest.raises(Boom), database.transaction() as connection:
        leases.transfer_writer_in(
            connection,
            lease_id,
            expected_writer_role="fable",
            expected_writer_ref=None,
            to_writer_role="sol",
            to_writer_ref="sol-gen-1",
            observed_head=SHA_A,
            tree_clean=True,
            submitted_candidate_sha=SHA_A,
        )
        raise Boom("simulated crash after the UPDATE")

    after = row_snapshot(database, lease_id)
    assert after == before
    lease = leases.get(lease_id)
    assert lease.writer_role == "fable"
    assert lease.writer_generation == 1
    assert lease.submitted_candidate_sha is None


def test_writer_of_returns_live_triple(leases: WorktreeLeases) -> None:
    lease_id = register(leases)
    assert leases.writer_of("ENG-431") == ("fable", None, 1)
    leases.transfer_writer(
        lease_id,
        expected_writer_role="fable",
        expected_writer_ref=None,
        to_writer_role="sol",
        to_writer_ref="sol-abc",
        observed_head=SHA_A,
        tree_clean=True,
        submitted_candidate_sha=SHA_A,
    )
    assert leases.writer_of("ENG-431") == ("sol", "sol-abc", 2)


def test_writer_of_returns_none_for_unknown_issue(leases: WorktreeLeases) -> None:
    register(leases)
    assert leases.writer_of("ENG-999") is None
