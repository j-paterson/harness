"""SQLite lifecycle, migrations, and explicit transactions."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any


class Database:
    """A single SQLite connection configured for durable local state."""

    def __init__(self, path: Path, connection: sqlite3.Connection) -> None:
        self.path = path
        self._connection = connection

    @classmethod
    def open(cls, path: Path) -> Database:
        """Open a database, configure it, and apply pending migrations."""

        resolved = path.expanduser().resolve(strict=False)
        resolved.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(resolved, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA synchronous = NORMAL")
        database = cls(resolved, connection)
        database._apply_migrations()
        return database

    def _apply_migrations(self) -> None:
        exists = self.scalar(
            "SELECT count(*) FROM sqlite_master "
            "WHERE type = 'table' AND name = 'schema_migrations'"
        )
        current = self.schema_version() if exists else 0
        migrations_dir = Path(__file__).with_name("migrations")
        for migration_path in sorted(migrations_dir.glob("[0-9][0-9][0-9][0-9]_*.sql")):
            version = int(migration_path.name.split("_", maxsplit=1)[0])
            if version > current:
                self._connection.executescript(migration_path.read_text(encoding="utf-8"))
                current = version

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Run writes atomically using an immediate transaction."""

        self._connection.execute("BEGIN IMMEDIATE")
        try:
            yield self._connection
        except BaseException:
            self._connection.execute("ROLLBACK")
            raise
        else:
            self._connection.execute("COMMIT")

    def schema_version(self) -> int:
        """Return the highest applied migration version."""

        value = self.scalar("SELECT coalesce(max(version), 0) FROM schema_migrations")
        return int(value)

    def scalar(self, sql: str, parameters: tuple[Any, ...] = ()) -> object:
        """Return the first column of the first result row."""

        row = self._connection.execute(sql, parameters).fetchone()
        if row is None:
            return None
        return row[0]

    def execute(
        self,
        sql: str,
        parameters: tuple[Any, ...] = (),
    ) -> sqlite3.Cursor:
        """Execute a read statement outside an explicit write transaction."""

        return self._connection.execute(sql, parameters)

    def close(self) -> None:
        """Close the underlying SQLite connection."""

        self._connection.close()
