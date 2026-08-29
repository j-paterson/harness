from pathlib import Path

from hermes_orchestrator.db import Database


def test_database_applies_migration_once(tmp_path: Path) -> None:
    database_path = tmp_path / "state.db"

    database = Database.open(database_path)
    assert database.schema_version() == 41
    database.close()

    reopened = Database.open(database_path)
    try:
        assert reopened.schema_version() == 41
        assert reopened.scalar("PRAGMA integrity_check") == "ok"
        assert reopened.scalar("PRAGMA foreign_keys") == 1
        assert reopened.scalar("PRAGMA journal_mode") == "wal"
        assert reopened.scalar("PRAGMA busy_timeout") == 5000
    finally:
        reopened.close()
