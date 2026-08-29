"""Durable runtime activation with safe rollback (INFRA-195)."""

from __future__ import annotations

import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.activation import (
    ActivationRefused,
    RuntimeActivator,
    checkout_migration_max,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def activator(database: Database) -> RuntimeActivator:
    return RuntimeActivator(
        database, events=EventStore(database), now=lambda: NOW
    )


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


def build_checkout(root: Path, *, schema: int) -> Path:
    checkout = root / "checkout"
    migrations = checkout / "src" / "hermes_orchestrator" / "migrations"
    migrations.mkdir(parents=True)
    for version in (1, schema):
        (migrations / f"{version:04d}_x.sql").write_text("-- x\n")
    (checkout / "pyproject.toml").write_text("[project]\nname='x'\n")
    git(checkout, "init", "-q")
    git(checkout, "add", "-A")
    git(checkout, "commit", "-qm", "init")
    return checkout


def test_a_clean_matched_checkout_activates_with_full_identity(
    database: Database, activator: RuntimeActivator, tmp_path: Path
) -> None:
    checkout = build_checkout(tmp_path, schema=database.schema_version())

    activation = activator.activate(
        checkout_root=checkout, binary_path=Path("/usr/local/bin/hermes")
    )

    assert activation.state == "active"
    assert activation.generation == 1
    assert len(activation.git_sha) == 40
    assert activation.checkout_root == str(checkout)
    assert activation.binary_path == "/usr/local/bin/hermes"
    assert activation.database_schema == database.schema_version()
    assert activator.current() == activation


def test_a_new_activation_supersedes_the_prior_one(
    database: Database, activator: RuntimeActivator, tmp_path: Path
) -> None:
    checkout = build_checkout(tmp_path, schema=database.schema_version())
    first = activator.activate(
        checkout_root=checkout, binary_path=Path("/bin/a")
    )
    second = activator.activate(
        checkout_root=checkout, binary_path=Path("/bin/b")
    )

    assert second.generation == first.generation + 1
    current = activator.current()
    assert current is not None
    assert current.activation_id == second.activation_id
    superseded = database.scalar(
        "SELECT state FROM runtime_activations WHERE activation_id = ?",
        (first.activation_id,),
    )
    assert str(superseded) == "superseded"


def test_a_dirty_checkout_is_refused_and_rolls_back_safely(
    database: Database, activator: RuntimeActivator, tmp_path: Path
) -> None:
    checkout = build_checkout(tmp_path, schema=database.schema_version())
    good = activator.activate(
        checkout_root=checkout, binary_path=Path("/bin/a")
    )
    (checkout / "src" / "hermes_orchestrator" / "extra.py").write_text("x\n")

    with pytest.raises(ActivationRefused, match="untracked"):
        activator.activate(checkout_root=checkout, binary_path=Path("/bin/b"))

    # The safe rollback: the prior activation is still the active one,
    # and the failed attempt is a durable record.
    current = activator.current()
    assert current is not None
    assert current.activation_id == good.activation_id
    failed = database.scalar(
        "SELECT COUNT(*) FROM runtime_activations WHERE state = 'failed'"
    )
    assert failed == 1


def test_a_schema_mismatched_checkout_is_refused(
    database: Database, activator: RuntimeActivator, tmp_path: Path
) -> None:
    checkout = build_checkout(
        tmp_path, schema=database.schema_version() + 5
    )

    with pytest.raises(ActivationRefused, match="mismatched"):
        activator.activate(checkout_root=checkout, binary_path=Path("/bin/a"))

    assert activator.current() is None


def test_a_stale_binary_without_git_is_refused(
    database: Database, activator: RuntimeActivator, tmp_path: Path
) -> None:
    checkout = tmp_path / "not-a-repo"
    checkout.mkdir()

    with pytest.raises(ActivationRefused, match="git HEAD"):
        activator.activate(checkout_root=checkout, binary_path=Path("/bin/a"))


def test_checkout_migration_max_reads_filename_prefixes(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "src" / "hermes_orchestrator" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0001_a.sql").write_text("-- a\n")
    (migrations / "0042_b.sql").write_text("-- b\n")
    (migrations / "notes.txt").write_text("ignored\n")

    assert checkout_migration_max(tmp_path) == 42
    assert checkout_migration_max(tmp_path / "missing") == 0
