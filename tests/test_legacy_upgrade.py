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

# Deliberate explicit pin of the current schema version (keep as a literal).
CURRENT_SCHEMA = 60

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
        assert database.schema_version() == CURRENT_SCHEMA
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
        assert database.schema_version() == CURRENT_SCHEMA
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
        assert reopened.schema_version() == CURRENT_SCHEMA
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


def test_pending_commands_survive_the_claim_state_recreate(tmp_path: Path) -> None:
    """Migration 0021 recreates remote_pending_commands; rows must survive."""

    path = tmp_path / "schema20.db"
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        for migration in sorted(MIGRATIONS.glob("[0-9]*.sql")):
            if int(migration.name.split("_", maxsplit=1)[0]) > 20:
                continue
            connection.executescript(migration.read_text())
        connection.execute(
            "INSERT INTO remote_pending_commands("
            "confirmation_id, session_id, intent, target, confirmation_phrase, "
            "impact_summary, prepared_at, expires_at, state"
            ") VALUES ('c-1', 's-1', 'pause', 'project:demo', 'PAUSE DEMO', "
            "'impact', 't0', 't1', 'executed')"
        )
        connection.execute(
            "INSERT INTO remote_command_results("
            "idempotency_key, session_id, confirmation_id, code, "
            "correlation_id, state_json, executed_at"
            ") VALUES ('k-1', 's-1', 'c-1', 'accepted', 'corr-1', '{}', 't1')"
        )
    finally:
        connection.close()

    database = Database.open(path)
    try:
        assert database.schema_version() == CURRENT_SCHEMA
        row = database.execute(
            "SELECT * FROM remote_pending_commands WHERE confirmation_id = 'c-1'"
        ).fetchone()
        assert row["state"] == "executed"
        assert row["confirmation_phrase"] == "PAUSE DEMO"
        assert row["claimed_at"] is None
        assert row["idempotency_key"] is None
        assert row["parameters_json"] == "{}"
        database.execute(
            "SELECT 1 FROM remote_pending_commands WHERE state = 'claimed'"
        ).fetchone()
        result = database.execute(
            "SELECT code FROM remote_command_results WHERE idempotency_key = 'k-1'"
        ).fetchone()
        assert result["code"] == "accepted"
        assert database.scalar("PRAGMA integrity_check") == "ok"
    finally:
        database.close()


def test_wake_repair_floor_excludes_pre_upgrade_terminal_evidence(
    tmp_path: Path,
) -> None:
    """Upgrading a lived-in database never manufactures historical wakes."""

    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.lead_wakes import (
        LeadTerminalWakes,
        LeadWakeReconciler,
    )

    path = tmp_path / "legacy.db"
    build_legacy_database(path)

    # Terminal evidence journaled long before the wake outbox existed.
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at) VALUES ('cell-old', 'demo', "
            "'handoff_required', 'max-a', "
            "'44444444-4444-4444-8444-444444444444', 't0', 't0')"
        )
        connection.execute(
            "INSERT INTO events(event_id, occurred_at, event_type, "
            "aggregate_type, aggregate_id, correlation_id, actor, "
            "payload_json) VALUES ('evt-old', 't0', "
            "'project_cell.handoff_required', 'project_cell', 'cell-old', "
            "NULL, 'orchestrator', '{\"reason\": \"context_rotation\", "
            "\"session_id\": \"44444444-4444-4444-8444-444444444444\"}')"
        )
    finally:
        connection.close()

    database = Database.open(path)
    try:
        floor = database.scalar(
            "SELECT floor_sequence FROM lead_wake_repair WHERE id = 1"
        )
        assert int(floor) >= 1
        wakes = LeadTerminalWakes(
            database=database, events=EventStore(database)
        )
        reconciler = LeadWakeReconciler(
            database=database, events=EventStore(database), wakes=wakes
        )
        assert reconciler.reconcile() == ()
        assert database.scalar(
            "SELECT count(*) FROM lead_terminal_wakes"
        ) == 0
    finally:
        database.close()
