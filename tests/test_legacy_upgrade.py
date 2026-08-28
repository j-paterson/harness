"""Verify forward-only upgrade of legacy durable databases (INFRA-166).

The production database recorded migration 3 from an older implementation
that created ``merger_threads`` and never created ``reviewer_channels``.
SQLite never re-runs a recorded migration, so the upgrade must come from a
new version that works on both legacy and current databases.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database

MIGRATIONS = Path("src/hermes_orchestrator/migrations")

LEGACY_0003 = """
BEGIN IMMEDIATE;
CREATE TABLE merger_threads (
    project_key TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO schema_migrations(version, applied_at) VALUES (3, CURRENT_TIMESTAMP);
COMMIT;
"""


def build_legacy_database(
    path: Path, *, legacy_rows: tuple[tuple[str, str, str], ...] = ()
) -> None:
    """Replay the production migration history: 1, 2, legacy 3, 4-7."""

    connection = sqlite3.connect(path, isolation_level=None)
    try:
        for name in ("0001_initial.sql", "0002_handoffs.sql"):
            connection.executescript((MIGRATIONS / name).read_text())
        connection.executescript(LEGACY_0003)
        for name in sorted(MIGRATIONS.glob("000[4-7]_*.sql")):
            connection.executescript(name.read_text())
        for project_key, thread_id, state in legacy_rows:
            connection.execute(
                "INSERT INTO merger_threads VALUES (?, ?, ?, 't0', 't0')",
                (project_key, thread_id, state),
            )
    finally:
        connection.close()


def tables(database: Database) -> set[str]:
    rows = database.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    return {str(row["name"]) for row in rows}


def test_legacy_history_matches_production_shape(tmp_path: Path) -> None:
    path = tmp_path / "legacy.db"
    build_legacy_database(path)
    connection = sqlite3.connect(path)
    try:
        versions = [
            row[0]
            for row in connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            )
        ]
        names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    finally:
        connection.close()
    assert versions == [1, 2, 3, 4, 5, 6, 7]
    assert "merger_threads" in names
    assert "reviewer_channels" not in names


def test_legacy_database_upgrades_to_current_schema(tmp_path: Path) -> None:
    path = tmp_path / "legacy.db"
    build_legacy_database(path)

    database = Database.open(path)
    try:
        assert database.schema_version() == 14
        names = tables(database)
        assert "reviewer_channels" in names
        assert "merger_threads" not in names
        assert "lead_corrections" in names
        assert database.scalar("PRAGMA integrity_check") == "ok"
        assert database.scalar("SELECT count(*) FROM reviewer_channels") == 0
    finally:
        database.close()


def test_legacy_thread_rows_are_preserved_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "legacy.db"
    build_legacy_database(
        path,
        legacy_rows=(("demo", "thr_legacy", "ready"), ("other", "", "creating")),
    )

    database = Database.open(path)
    try:
        rows = {
            str(row["project_key"]): row
            for row in database.execute(
                "SELECT * FROM reviewer_channels ORDER BY project_key"
            ).fetchall()
        }
        demo = rows["demo"]
        assert demo["thread_id"] == "thr_legacy"
        assert demo["generation"] == 0
        assert demo["state"] == "configuring"
        assert demo["replacement_reason"] == "legacy_merger_threads:ready"
        assert demo["heartbeat_enabled"] == 0
        assert demo["created_at"] == "t0"
        other = rows["other"]
        assert other["state"] == "uncertain"
        assert other["replacement_reason"] == "legacy_merger_threads:creating"
    finally:
        database.close()


def test_upgrade_never_replaces_an_existing_current_channel(tmp_path: Path) -> None:
    """A current-shaped database at version 8 keeps its channel untouched."""

    path = tmp_path / "current.db"
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        for name in sorted(MIGRATIONS.glob("000[1-8]_*.sql")):
            connection.executescript(name.read_text())
        connection.execute(
            "INSERT INTO reviewer_channels("
            "project_key, thread_id, generation, state, integration_branch, "
            "created_at, updated_at) VALUES ('demo', 'thr_current', 3, "
            "'ready', 'main', 't1', 't1')"
        )
    finally:
        connection.close()

    database = Database.open(path)
    try:
        assert database.schema_version() == 14
        row = database.execute(
            "SELECT thread_id, generation, state FROM reviewer_channels"
        ).fetchone()
        assert (row["thread_id"], row["generation"], row["state"]) == (
            "thr_current",
            3,
            "ready",
        )
        assert "merger_threads" not in tables(database)
    finally:
        database.close()


def test_upgrade_is_idempotent_across_reopen(tmp_path: Path) -> None:
    path = tmp_path / "legacy.db"
    build_legacy_database(path, legacy_rows=(("demo", "thr_legacy", "ready"),))
    Database.open(path).close()
    reopened = Database.open(path)
    try:
        assert reopened.schema_version() == 14
        assert reopened.scalar("SELECT count(*) FROM reviewer_channels") == 1
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_legacy_channel_requires_live_reverification(tmp_path: Path) -> None:
    """The carried-forward thread becomes ready only after live reconfiguration."""

    from hermes_orchestrator.codex_merger import CodexMerger
    from hermes_orchestrator.config import ProjectConfig
    from tests.test_codex_merger import FakeRpc

    path = tmp_path / "legacy.db"
    build_legacy_database(path, legacy_rows=(("demo", "thr_legacy", "ready"),))
    database = Database.open(path)
    try:
        rpc = FakeRpc()
        merger = CodexMerger(
            rpc=rpc,
            database=database,
            projects={
                "demo": ProjectConfig(
                    linear_team="infrastructure",
                    repo_path=tmp_path,
                    integration_branch="main",
                    github_repo="j-paterson/demo",
                )
            },
            prompt_file=Path("prompts/codex-merger.md"),
        )
        before = merger.read_channel("demo")
        assert before is not None and before.state == "configuring"
        assert merger.read_status("demo").state == "configuring"

        thread = await merger.ensure_thread("demo")

        assert thread.thread_id == "thr_legacy"
        after = merger.read_channel("demo")
        assert after is not None
        assert (after.state, after.generation, after.integration_branch) == (
            "ready",
            1,
            "main",
        )
        assert "thread/start" not in rpc.methods
        assert rpc.request_for("thread/name/set")["params"]["threadId"] == (
            "thr_legacy"
        )
    finally:
        database.close()
