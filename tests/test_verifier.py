"""Trusted verification runner and receipt tests (INFRA-186)."""

from __future__ import annotations

import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.verifier import Verifier

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)
GATE_ID = "demo-gate"
PASS_COMMAND = "python3 -c \"print('1 passed')\""
PASS_COMMAND_EXTRA_WHITESPACE = "python3    -c   \"print('1 passed')\""
OTHER_COMMAND = "python3 -c \"print('2 passed')\""
FAIL_COMMAND = "python3 -c \"import sys; sys.exit(1)\""


def git(checkout: Path, *args: str) -> None:
    subprocess.run(
        ("git", "-C", str(checkout), *args),
        check=True,
        capture_output=True,
        env={
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.invalid",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.invalid",
            "PATH": "/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin",
            "HOME": str(checkout),
        },
    )


def build_repo(root: Path) -> Path:
    """A tiny real git repo with a tracked README/pyproject and an
    UNTRACKED uv.lock (so editing the lock file never shows up in
    ``git diff HEAD``, isolating dependency staleness from tree
    staleness)."""

    repo = root / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text("[project]\nname = 'demo'\n")
    (repo / "README.md").write_text("hello\n")
    (repo / "uv.lock").write_text("version = 1\n")
    git(repo, "init", "-q")
    git(repo, "add", "pyproject.toml", "README.md")
    git(repo, "commit", "-qm", "init")
    return repo


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    return build_repo(tmp_path)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def verifier(database: Database, tmp_path: Path) -> Verifier:
    return Verifier(
        database,
        events=EventStore(database),
        state_dir=tmp_path / "state",
        now=lambda: NOW,
    )


def receipt_count(database: Database) -> int:
    return int(database.scalar("SELECT count(*) FROM verification_receipts"))


def test_identical_runs_on_an_unchanged_tree_reuse_the_same_receipt(
    verifier: Verifier, repo: Path, database: Database
) -> None:
    first = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)
    second = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    assert first.receipt_id == second.receipt_id
    assert receipt_count(database) == 1
    assert first.exit_code == 0
    assert first.test_counts == {"passed": 1}


def test_a_fresh_receipt_validates_true(verifier: Verifier, repo: Path) -> None:
    receipt = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    outcome = verifier.validate(
        receipt.receipt_id, cwd=repo, gate_id=GATE_ID, command=PASS_COMMAND
    )

    assert outcome == (True, "fresh")


def test_a_forged_row_fails_signature_validation(
    verifier: Verifier, repo: Path, database: Database
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO verification_receipts("
            "receipt_id, schema_version, gate_id, command, tree_hash, "
            "dirty_patch_hash, dependency_hash, runner_fingerprint, "
            "exit_code, test_counts, duration_seconds, output_hash, "
            "signature, created_at) "
            "VALUES ('forged', 1, ?, ?, 'deadbeef', '', 'cafe', 'runner', "
            "0, '{}', 0.1, 'output', 'not-a-real-signature', ?)",
            (GATE_ID, PASS_COMMAND, NOW.isoformat()),
        )

    outcome = verifier.validate(
        "forged", cwd=repo, gate_id=GATE_ID, command=PASS_COMMAND
    )

    assert outcome == (False, "signature invalid")


def test_validate_reports_unknown_receipt(verifier: Verifier, repo: Path) -> None:
    outcome = verifier.validate(
        "does-not-exist", cwd=repo, gate_id=GATE_ID, command=PASS_COMMAND
    )

    assert outcome == (False, "unknown receipt")


def test_a_new_commit_makes_the_receipt_stale(
    verifier: Verifier, repo: Path
) -> None:
    receipt = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    (repo / "README.md").write_text("changed\n")
    git(repo, "add", "README.md")
    git(repo, "commit", "-qm", "change")

    outcome = verifier.validate(
        receipt.receipt_id, cwd=repo, gate_id=GATE_ID, command=PASS_COMMAND
    )

    assert outcome == (False, "stale: tree changed")


def test_an_uncommitted_edit_makes_the_receipt_stale(
    verifier: Verifier, repo: Path
) -> None:
    receipt = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    (repo / "README.md").write_text("dirty\n")

    outcome = verifier.validate(
        receipt.receipt_id, cwd=repo, gate_id=GATE_ID, command=PASS_COMMAND
    )

    assert outcome == (False, "stale: tree changed")


def test_an_edited_dependency_lockfile_makes_the_receipt_stale(
    verifier: Verifier, repo: Path
) -> None:
    receipt = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    # uv.lock is untracked in this fixture, so this edit is invisible to
    # `git diff HEAD` and cannot change tree_hash or dirty_patch_hash.
    (repo / "uv.lock").write_text("version = 2\n")

    outcome = verifier.validate(
        receipt.receipt_id, cwd=repo, gate_id=GATE_ID, command=PASS_COMMAND
    )

    assert outcome == (False, "stale: dependencies changed")


def test_a_different_command_makes_the_receipt_stale(
    verifier: Verifier, repo: Path
) -> None:
    receipt = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    outcome = verifier.validate(
        receipt.receipt_id, cwd=repo, gate_id=GATE_ID, command=OTHER_COMMAND
    )

    assert outcome == (False, "stale: command changed")


def test_extra_whitespace_in_the_command_is_not_a_change(
    verifier: Verifier, repo: Path
) -> None:
    receipt = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    outcome = verifier.validate(
        receipt.receipt_id,
        cwd=repo,
        gate_id=GATE_ID,
        command=PASS_COMMAND_EXTRA_WHITESPACE,
    )

    assert outcome == (True, "fresh")


def test_a_changed_runner_fingerprint_makes_the_receipt_stale(
    verifier: Verifier, repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    receipt = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    monkeypatch.setattr("hermes_orchestrator.verifier.RUNNER_VERSION", 999)

    outcome = verifier.validate(
        receipt.receipt_id, cwd=repo, gate_id=GATE_ID, command=PASS_COMMAND
    )

    assert outcome == (False, "stale: runner or environment changed")


def test_a_failing_command_records_its_exit_code_and_validate_refuses_it(
    verifier: Verifier, repo: Path
) -> None:
    receipt = verifier.run_verified(repo, gate_id=GATE_ID, command=FAIL_COMMAND)

    assert receipt.exit_code == 1

    outcome = verifier.validate(
        receipt.receipt_id, cwd=repo, gate_id=GATE_ID, command=FAIL_COMMAND
    )

    assert outcome == (False, "receipt records a failing run")


def test_find_fresh_returns_none_on_a_changed_tree_and_the_receipt_otherwise(
    verifier: Verifier, repo: Path
) -> None:
    receipt = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    found = verifier.find_fresh(repo, gate_id=GATE_ID, command=PASS_COMMAND)
    assert found is not None
    assert found.receipt_id == receipt.receipt_id

    (repo / "README.md").write_text("changed\n")
    git(repo, "add", "README.md")
    git(repo, "commit", "-qm", "change")

    assert verifier.find_fresh(repo, gate_id=GATE_ID, command=PASS_COMMAND) is None


def test_the_signing_key_is_created_once_with_restrictive_permissions(
    verifier: Verifier, repo: Path, tmp_path: Path
) -> None:
    verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    key_path = tmp_path / "state" / "verifier.key"
    assert key_path.exists()
    mode = key_path.stat().st_mode & 0o777
    assert mode == 0o600
    key_bytes = bytes.fromhex(key_path.read_text().strip())
    assert len(key_bytes) == 32


# --- validate_for_tree (INFRA-186 P9: the any-agent transition validator) ---


def test_validate_for_tree_is_fresh_on_an_unchanged_tree_without_a_command(
    verifier: Verifier, repo: Path
) -> None:
    receipt = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    outcome = verifier.validate_for_tree(
        receipt.receipt_id, cwd=repo, gate_id=GATE_ID
    )

    assert outcome == (True, "fresh")


def test_validate_for_tree_reports_gate_mismatch(
    verifier: Verifier, repo: Path
) -> None:
    receipt = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    outcome = verifier.validate_for_tree(
        receipt.receipt_id, cwd=repo, gate_id="some-other-gate"
    )

    assert outcome == (False, "gate mismatch")


def test_validate_for_tree_is_stale_on_a_tree_change(
    verifier: Verifier, repo: Path
) -> None:
    receipt = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    (repo / "README.md").write_text("changed\n")
    git(repo, "add", "README.md")
    git(repo, "commit", "-qm", "change")

    outcome = verifier.validate_for_tree(
        receipt.receipt_id, cwd=repo, gate_id=GATE_ID
    )

    assert outcome == (False, "stale: tree changed")


def test_validate_for_tree_reports_a_forged_row_invalid(
    verifier: Verifier, repo: Path, database: Database
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO verification_receipts("
            "receipt_id, schema_version, gate_id, command, tree_hash, "
            "dirty_patch_hash, dependency_hash, runner_fingerprint, "
            "exit_code, test_counts, duration_seconds, output_hash, "
            "signature, created_at) "
            "VALUES ('forged-for-tree', 1, ?, ?, 'deadbeef', '', 'cafe', "
            "'runner', 0, '{}', 0.1, 'output', 'not-a-real-signature', ?)",
            (GATE_ID, PASS_COMMAND, NOW.isoformat()),
        )

    outcome = verifier.validate_for_tree(
        "forged-for-tree", cwd=repo, gate_id=GATE_ID
    )

    assert outcome == (False, "signature invalid")


def test_validate_for_tree_refuses_a_failing_run(
    verifier: Verifier, repo: Path
) -> None:
    receipt = verifier.run_verified(repo, gate_id=GATE_ID, command=FAIL_COMMAND)

    outcome = verifier.validate_for_tree(
        receipt.receipt_id, cwd=repo, gate_id=GATE_ID
    )

    assert outcome == (False, "receipt records a failing run")


def test_validate_for_tree_reports_unknown_receipt(
    verifier: Verifier, repo: Path
) -> None:
    outcome = verifier.validate_for_tree(
        "does-not-exist", cwd=repo, gate_id=GATE_ID
    )

    assert outcome == (False, "unknown receipt")


def test_receipt_ids_for_gate_orders_newest_first(
    verifier: Verifier, repo: Path
) -> None:
    first = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    (repo / "README.md").write_text("changed\n")
    git(repo, "add", "README.md")
    git(repo, "commit", "-qm", "change")
    second = verifier.run_verified(repo, gate_id=GATE_ID, command=PASS_COMMAND)

    assert verifier.receipt_ids_for_gate(GATE_ID) == (
        second.receipt_id,
        first.receipt_id,
    )
    assert verifier.receipt_ids_for_gate("no-such-gate") == ()
