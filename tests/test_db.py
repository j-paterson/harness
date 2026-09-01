import sqlite3
from pathlib import Path

import hermes_orchestrator.db
from hermes_orchestrator.db import Database


def test_database_applies_migration_once(tmp_path: Path) -> None:
    database_path = tmp_path / "state.db"

    database = Database.open(database_path)
    assert database.schema_version() == 57
    database.close()

    reopened = Database.open(database_path)
    try:
        assert reopened.schema_version() == 57
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
        assert database.schema_version() == 57
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


def test_migration_0051_pins_handoff_replacement_identity(tmp_path: Path) -> None:
    # Sol correction b4b545f3 P2: rotation recovery reconstructs the exact
    # replacement identities persisted at acknowledgement, so the handoffs
    # row carries the selected profile alongside the selected session.
    database = Database.open(tmp_path / "state.db")
    try:
        columns = {
            str(row["name"]): (str(row["type"]), int(row["notnull"]))
            for row in database.execute("PRAGMA table_info(handoffs)").fetchall()
        }
        assert columns == {
            "handoff_id": ("TEXT", 0),
            "cell_id": ("TEXT", 1),
            "state": ("TEXT", 1),
            "document_json": ("TEXT", 1),
            "markdown": ("TEXT", 1),
            "replacement_session_id": ("TEXT", 0),
            "restated_next_action": ("TEXT", 0),
            "created_at": ("TEXT", 1),
            "updated_at": ("TEXT", 1),
            "replacement_profile_alias": ("TEXT", 0),
        }
    finally:
        database.close()


def test_migration_0051_applies_on_a_schema_50_database(tmp_path: Path) -> None:
    # A live database left at schema 50 must upgrade additively, keeping
    # every acknowledged handoff row (its new column simply reads NULL).
    database_path = tmp_path / "state.db"
    migrations = Path(hermes_orchestrator.db.__file__).with_name("migrations")
    connection = sqlite3.connect(database_path)
    try:
        for migration_path in sorted(migrations.glob("[0-9][0-9][0-9][0-9]_*.sql")):
            version = int(migration_path.name.split("_", maxsplit=1)[0])
            if version >= 51:
                continue
            connection.executescript(migration_path.read_text(encoding="utf-8"))
        connection.executescript(
            "INSERT INTO handoffs("
            "handoff_id, cell_id, state, document_json, markdown, "
            "replacement_session_id, restated_next_action, created_at, updated_at"
            ") VALUES ('handoff-legacy', 'cell-demo', 'acknowledged', '{}', '# h', "
            "'22222222-2222-4222-8222-222222222222', 'Run the failing test.', "
            "'2026-08-26T12:00:00+00:00', '2026-08-26T12:00:00+00:00');"
        )
    finally:
        connection.close()

    database = Database.open(database_path)
    try:
        assert database.schema_version() == 57
        assert database.scalar("PRAGMA integrity_check") == "ok"
        row = database.execute(
            "SELECT state, replacement_session_id, replacement_profile_alias "
            "FROM handoffs WHERE handoff_id = 'handoff-legacy'"
        ).fetchone()
        assert str(row["state"]) == "acknowledged"
        assert str(row["replacement_session_id"]).startswith("22222222")
        assert row["replacement_profile_alias"] is None
    finally:
        database.close()


def test_migration_0052_pins_the_submitted_verdicts_table(tmp_path: Path) -> None:
    # Operator correction ec1f6bdf: the explicit Sol submit-review boundary
    # persists each bound verdict exactly once. event_id is the CAS key,
    # the state enum is closed, and the shape is pinned so idempotent
    # duplicate detection can rely on it.
    database = Database.open(tmp_path / "state.db")
    try:
        columns = {
            str(row["name"]): (str(row["type"]), int(row["notnull"]))
            for row in database.execute(
                "PRAGMA table_info(submitted_verdicts)"
            ).fetchall()
        }
        assert columns == {
            "event_id": ("TEXT", 1),
            "project_key": ("TEXT", 1),
            "issue_id": ("TEXT", 1),
            "candidate_sha": ("TEXT", 1),
            "reviewed_thread_id": ("TEXT", 1),
            "reviewed_generation": ("INTEGER", 1),
            "verdict_json": ("TEXT", 1),
            "state": ("TEXT", 1),
            "result_json": ("TEXT", 0),
            "created_at": ("TEXT", 1),
            "updated_at": ("TEXT", 1),
        }
        def insert(event_id: str, state: str) -> str:
            return (
                "INSERT INTO submitted_verdicts("
                "event_id, project_key, issue_id, candidate_sha, "
                "reviewed_thread_id, reviewed_generation, verdict_json, "
                f"state, created_at, updated_at) VALUES ('{event_id}', "
                f"'demo', 'ENG-9', 'aa', 'thr', 1, '{{}}', '{state}', "
                "'2026-08-28T12:00:00+00:00', '2026-08-28T12:00:00+00:00')"
            )

        with database.transaction() as connection:
            connection.execute(insert("evt-1", "submitted"))
        violations = (
            insert("evt-1", "settled"),  # duplicate event: the CAS key holds
            insert("evt-2", "pending"),  # state outside the closed enum
        )
        for violating in violations:
            try:
                with database.transaction() as connection:
                    connection.execute(violating)
                raise AssertionError("constraint violation was accepted")
            except sqlite3.IntegrityError:
                pass
    finally:
        database.close()


def test_migration_0052_applies_on_a_schema_51_database(tmp_path: Path) -> None:
    # A live database left at schema 51 must upgrade additively: no
    # existing table is altered, only the new table appears.
    database_path = tmp_path / "state.db"
    migrations = Path(hermes_orchestrator.db.__file__).with_name("migrations")
    connection = sqlite3.connect(database_path)
    try:
        for migration_path in sorted(migrations.glob("[0-9][0-9][0-9][0-9]_*.sql")):
            version = int(migration_path.name.split("_", maxsplit=1)[0])
            if version >= 52:
                continue
            connection.executescript(migration_path.read_text(encoding="utf-8"))
    finally:
        connection.close()

    database = Database.open(database_path)
    try:
        assert database.schema_version() == 57
        assert database.scalar("PRAGMA integrity_check") == "ok"
        assert (
            database.scalar(
                "SELECT count(*) FROM sqlite_master "
                "WHERE type = 'table' AND name = 'submitted_verdicts'"
            )
            == 1
        )
    finally:
        database.close()
