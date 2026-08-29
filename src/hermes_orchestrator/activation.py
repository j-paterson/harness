"""Durable runtime activation: exact binary, checkout, schema, generation.

INFRA-195: the resolution of "which checkout may run against this
database" used to live only in duplicated out-of-repo shell scripts.
It is now a first-class, durable, validated record. An activation
proves the checkout before it becomes authoritative — a full git HEAD
SHA, a clean implementation tree (``src``, ``pyproject.toml``,
``uv.lock``, no untracked files under ``src``), and a migration
maximum equal to the live database schema — then supersedes the prior
active row in one transaction. A failed validation is recorded as a
durable ``failed`` attempt while the prior activation stays active:
that is the safe rollback, and the daemon keeps running the identity
that last proved out.
"""

from __future__ import annotations

import re
import subprocess
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

ACTIVATION_SCHEMA_VERSION = 1

_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_MIGRATION_NAME = re.compile(r"^(\d{4})_.+\.sql$")


class ActivationRefused(RuntimeError):
    """The checkout failed validation; the prior activation stays active."""


@dataclass(frozen=True, slots=True)
class RuntimeActivation:
    """One durable runtime identity record."""

    activation_id: str
    schema_version: int
    generation: int
    binary_path: str
    checkout_root: str
    git_sha: str
    database_schema: int
    state: str
    reason: str | None
    activated_at: str


def checkout_migration_max(checkout_root: Path) -> int:
    """The highest migration a checkout carries; 0 when it has none."""

    migrations = checkout_root / "src" / "hermes_orchestrator" / "migrations"
    highest = 0
    if migrations.is_dir():
        for entry in migrations.iterdir():
            match = _MIGRATION_NAME.match(entry.name)
            if match is not None:
                highest = max(highest, int(match.group(1)))
    return highest


class RuntimeActivator:
    """Validate, record, and supersede durable runtime activations."""

    def __init__(
        self,
        database: Database,
        *,
        events: EventStore,
        ids: Callable[[], str] | None = None,
        now: Callable[[], datetime] | None = None,
        run: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._now = now or (lambda: datetime.now(UTC))
        self._run = run or subprocess.run

    def current(self) -> RuntimeActivation | None:
        row = self._database.execute(
            "SELECT * FROM runtime_activations WHERE state = 'active'"
        ).fetchone()
        return None if row is None else self._record(row)

    def activate(
        self, *, checkout_root: Path, binary_path: Path
    ) -> RuntimeActivation:
        """Prove the checkout, then make it the active runtime identity.

        Any validation failure records a durable ``failed`` attempt and
        raises :class:`ActivationRefused`; the prior active row is
        untouched — the safe rollback.
        """

        try:
            git_sha = self._validate(checkout_root)
        except ActivationRefused as refusal:
            self._record_attempt(
                state="failed",
                binary_path=binary_path,
                checkout_root=checkout_root,
                git_sha="0" * 40,
                reason=str(refusal),
            )
            raise
        return self._record_attempt(
            state="active",
            binary_path=binary_path,
            checkout_root=checkout_root,
            git_sha=git_sha,
            reason=None,
        )

    # -- internals ---------------------------------------------------------

    def _validate(self, checkout_root: Path) -> str:
        head = self._git(checkout_root, "rev-parse", "HEAD")
        if head is None or _SHA_PATTERN.match(head.strip()) is None:
            raise ActivationRefused(
                f"{checkout_root} has no readable git HEAD"
            )
        clean = self._git(
            checkout_root,
            "diff",
            "--quiet",
            "--",
            "src",
            "pyproject.toml",
            "uv.lock",
        )
        if clean is None:
            raise ActivationRefused(
                f"{checkout_root} carries uncommitted implementation "
                "changes; refusing to activate a mutable runtime"
            )
        untracked = self._git(
            checkout_root,
            "ls-files",
            "--others",
            "--exclude-standard",
            "--",
            "src",
        )
        if untracked is None or untracked.strip():
            raise ActivationRefused(
                f"{checkout_root} carries untracked implementation "
                "files; refusing to activate a mutable runtime"
            )
        highest = checkout_migration_max(checkout_root)
        live_schema = self._database.schema_version()
        if highest != live_schema:
            raise ActivationRefused(
                f"{checkout_root} carries migrations through {highest} "
                f"but the live database is at schema {live_schema}; "
                "refusing the mismatched runtime"
            )
        return head.strip()

    def _git(self, checkout_root: Path, *args: str) -> str | None:
        try:
            completed = self._run(
                ("git", "-C", str(checkout_root), *args),
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
        except Exception:
            return None
        if completed.returncode != 0:
            return None
        return completed.stdout

    def _record_attempt(
        self,
        *,
        state: str,
        binary_path: Path,
        checkout_root: Path,
        git_sha: str,
        reason: str | None,
    ) -> RuntimeActivation:
        stamp = self._now().isoformat()
        activation_id = self._ids()
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT coalesce(max(generation), 0) AS generation "
                "FROM runtime_activations"
            ).fetchone()
            generation = int(row["generation"]) + 1
            if state == "active":
                connection.execute(
                    "UPDATE runtime_activations SET state = 'superseded', "
                    "updated_at = ? WHERE state = 'active'",
                    (stamp,),
                )
            connection.execute(
                "INSERT INTO runtime_activations("
                "activation_id, schema_version, generation, binary_path, "
                "checkout_root, git_sha, database_schema, state, reason, "
                "activated_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    activation_id,
                    ACTIVATION_SCHEMA_VERSION,
                    generation,
                    str(binary_path),
                    str(checkout_root),
                    git_sha,
                    self._database.schema_version(),
                    state,
                    reason,
                    stamp,
                    stamp,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type=f"runtime_activation.{state}",
                    aggregate_type="runtime_activation",
                    aggregate_id=activation_id,
                    payload={
                        "generation": generation,
                        "binary_path": str(binary_path),
                        "checkout_root": str(checkout_root),
                        "git_sha": git_sha,
                        "reason": reason,
                    },
                ),
            )
        return RuntimeActivation(
            activation_id=activation_id,
            schema_version=ACTIVATION_SCHEMA_VERSION,
            generation=generation,
            binary_path=str(binary_path),
            checkout_root=str(checkout_root),
            git_sha=git_sha,
            database_schema=self._database.schema_version(),
            state=state,
            reason=reason,
            activated_at=stamp,
        )

    @staticmethod
    def _record(row: object) -> RuntimeActivation:
        return RuntimeActivation(
            activation_id=str(row["activation_id"]),  # type: ignore[index]
            schema_version=int(row["schema_version"]),  # type: ignore[index]
            generation=int(row["generation"]),  # type: ignore[index]
            binary_path=str(row["binary_path"]),  # type: ignore[index]
            checkout_root=str(row["checkout_root"]),  # type: ignore[index]
            git_sha=str(row["git_sha"]),  # type: ignore[index]
            database_schema=int(row["database_schema"]),  # type: ignore[index]
            state=str(row["state"]),  # type: ignore[index]
            reason=(
                None
                if row["reason"] is None  # type: ignore[index]
                else str(row["reason"])  # type: ignore[index]
            ),
            activated_at=str(row["activated_at"]),  # type: ignore[index]
        )
