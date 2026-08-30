import sqlite3
from pathlib import Path

import hermes_orchestrator.db
from hermes_orchestrator.db import Database


def test_database_applies_migration_once(tmp_path: Path) -> None:
    database_path = tmp_path / "state.db"

    database = Database.open(database_path)
    assert database.schema_version() == 50
    database.close()

    reopened = Database.open(database_path)
    try:
        assert reopened.schema_version() == 50
        assert reopened.scalar("PRAGMA integrity_check") == "ok"
        assert reopened.scalar("PRAGMA foreign_keys") == 1
        assert reopened.scalar("PRAGMA journal_mode") == "wal"
        assert reopened.scalar("PRAGMA busy_timeout") == 5000
    finally:
        reopened.close()


def test_migration_0050_pins_the_capacity_observation_table(
    tmp_path: Path,
) -> None:
    # INFRA-197 correction C2: durable fable-capacity evidence. Current
    # evidence per (profile_alias, model) is the newest row; the shape is
    # pinned so the fail-closed selection gate can rely on it.
    database = Database.open(tmp_path / "state.db")
    try:
        columns = {
            str(row["name"]): (str(row["type"]), int(row["notnull"]))
            for row in database.execute(
                "PRAGMA table_info(profile_capacity_observations)"
            ).fetchall()
        }
        assert columns == {
            "observation_id": ("INTEGER", 0),
            "profile_alias": ("TEXT", 1),
            "model": ("TEXT", 1),
            "state": ("TEXT", 1),
            "source": ("TEXT", 1),
            "observed_at": ("TEXT", 1),
            "resets_at": ("TEXT", 0),
            "detail": ("TEXT", 0),
        }
        with database.transaction() as connection:
            with_horizon = (
                "INSERT INTO profile_capacity_observations("
                "profile_alias, model, state, source, observed_at, resets_at"
                ") VALUES ('max-a', 'fable', ?, 'provider_limit', "
                "'2026-08-26T00:00:00+00:00', '2026-09-02T00:00:00+00:00')"
            )
            connection.execute(with_horizon.replace("?", "'capped'"))
        try:
            with database.transaction() as connection:
                connection.execute(with_horizon.replace("?", "'exhausted'"))
            raise AssertionError("state outside available/capped was accepted")
        except sqlite3.IntegrityError:
            pass
    finally:
        database.close()


def test_migration_0050_applies_on_a_schema_49_database(tmp_path: Path) -> None:
    # A live database left at the pre-correction schema (49) must upgrade
    # additively: no existing table is altered, only the new one appears.
    database_path = tmp_path / "state.db"
    migrations = Path(hermes_orchestrator.db.__file__).with_name("migrations")
    connection = sqlite3.connect(database_path)
    try:
        for migration_path in sorted(migrations.glob("[0-9][0-9][0-9][0-9]_*.sql")):
            version = int(migration_path.name.split("_", maxsplit=1)[0])
            if version >= 50:
                continue
            connection.executescript(migration_path.read_text(encoding="utf-8"))
    finally:
        connection.close()

    database = Database.open(database_path)
    try:
        assert database.schema_version() == 50
        assert database.scalar("PRAGMA integrity_check") == "ok"
        assert (
            database.scalar(
                "SELECT count(*) FROM sqlite_master "
                "WHERE type = 'table' AND name = 'profile_capacity_observations'"
            )
            == 1
        )
    finally:
        database.close()
