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

import os
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

APPLY_VERIFY_TIMEOUT_SECONDS = 60.0
APPLY_POLL_SECONDS = 1.0

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


def materialize_artifact(
    *,
    state_dir: Path,
    checkout_root: Path,
    git_sha: str,
    run: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> Path:
    """Materialize one immutable runtime artifact for an exact commit.

    The exact tree of ``git_sha`` is exported into
    ``<state_dir>/runtimes/<sha>/`` with a ``RUNTIME_SHA`` marker,
    built in a temporary sibling and atomically renamed, so the
    artifact either exists completely or not at all; an existing
    complete artifact is reused (idempotent). Artifacts never depend
    on the disposable checkout they were exported from.
    """

    runner = run or subprocess.run
    runtimes = state_dir / "runtimes"
    target = runtimes / git_sha
    marker = target / "RUNTIME_SHA"
    if marker.is_file():
        if marker.read_text(encoding="ascii").strip() != git_sha:
            raise ActivationRefused(
                f"runtime artifact {target} carries a foreign RUNTIME_SHA"
            )
        return target
    runtimes.mkdir(parents=True, exist_ok=True)
    staging = runtimes / f".{git_sha}.tmp-{os.getpid()}"
    archive = runtimes / f".{git_sha}.tar-{os.getpid()}"
    try:
        staging.mkdir()
        exported = runner(
            (
                "git",
                "-C",
                str(checkout_root),
                "archive",
                "--format=tar",
                "-o",
                str(archive),
                git_sha,
            ),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        if exported.returncode != 0:
            raise ActivationRefused(
                f"could not export {git_sha[:12]} from {checkout_root}"
            )
        extracted = runner(
            ("tar", "-xf", str(archive), "-C", str(staging)),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        if extracted.returncode != 0:
            raise ActivationRefused(
                f"could not extract the {git_sha[:12]} artifact"
            )
        (staging / "RUNTIME_SHA").write_text(
            git_sha + "\n", encoding="ascii"
        )
        try:
            staging.rename(target)
        except OSError:
            # A concurrent materialization won the rename; the complete
            # existing artifact is authoritative.
            if not marker.is_file():
                raise
    finally:
        archive.unlink(missing_ok=True)
        if staging.is_dir() and staging != target:
            import shutil as _shutil

            _shutil.rmtree(staging, ignore_errors=True)
    return target


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

    @staticmethod
    def _write_active_pointer(artifact: Path) -> None:
        """Refresh the derived ACTIVE pointer beside the artifacts.

        The database stays the authority (runtime-exec re-resolves it
        read-only); the pointer is what the rendered shell bootstrap
        reads, atomically replaced so a crash can never leave it
        half-written. A WAL database cannot be opened by a read-only
        shell client, which is why the shell never touches SQL.
        """

        pointer = artifact.parent / "ACTIVE"
        staging = artifact.parent / f".ACTIVE.tmp-{os.getpid()}"
        staging.write_text(str(artifact) + "\n", encoding="utf-8")
        staging.rename(pointer)

    def observed_sha(self, checkout_root: Path) -> str | None:
        """The running code's commit identity: an immutable artifact's
        RUNTIME_SHA marker, or the checkout's git HEAD."""

        marker = checkout_root / "RUNTIME_SHA"
        if marker.is_file():
            value = marker.read_text(encoding="ascii").strip()
            return value if _SHA_PATTERN.match(value) else None
        head = self._git(checkout_root, "rev-parse", "HEAD")
        if head is None:
            return None
        value = head.strip()
        return value if _SHA_PATTERN.match(value) else None

    def confirm_startup(
        self, *, checkout_root: Path, binary_path: Path, pid: int
    ) -> RuntimeActivation | None:
        """Journal the daemon's observed identity against the ledger.

        The started daemon never self-activates over an existing
        activation (that would silently undo a rollback): with no
        activation at all it bootstraps one; otherwise it confirms only
        when the COMPLETE immutable identity matches the active row —
        checkout root, exact commit SHA (a mutable checkout that
        advanced past the activated commit fails), resolved binary
        path, and the live database schema. A mismatch journals the
        observation and returns None so the caller fails closed. Every
        outcome appends a durable ``daemon.started`` event carrying the
        pid and generation — the health signal the apply protocol
        verifies.
        """

        current = self.current()
        if current is None:
            try:
                current = self.activate(
                    checkout_root=checkout_root, binary_path=binary_path
                )
            except ActivationRefused:
                current = None
        observed = self.observed_sha(checkout_root)
        mismatches: list[str] = []
        if current is None:
            mismatches.append("no active activation")
        else:
            if current.checkout_root != str(checkout_root):
                mismatches.append("checkout_root")
            if observed is None or observed != current.git_sha:
                mismatches.append("git_sha")
            if current.binary_path != str(binary_path):
                mismatches.append("binary_path")
            if current.database_schema != self._database.schema_version():
                mismatches.append("database_schema")
        matches = current is not None and not mismatches
        with self._database.transaction() as connection:
            self._events.append(
                connection,
                EventInput(
                    event_type="daemon.started",
                    aggregate_type="runtime_activation",
                    aggregate_id=(
                        current.activation_id if current else "unactivated"
                    ),
                    payload={
                        "pid": pid,
                        "checkout_root": str(checkout_root),
                        "binary_path": str(binary_path),
                        "observed_sha": observed,
                        "activation_generation": (
                            current.generation if matches and current else None
                        ),
                        "matches_active": matches,
                        "mismatches": mismatches,
                    },
                ),
            )
        return current if matches else None

    def activate_artifact(
        self,
        *,
        source_checkout: Path,
        state_dir: Path,
        prepare: Callable[[Path], None] | None = None,
    ) -> RuntimeActivation:
        """Prove the source checkout, then activate its immutable artifact.

        The runtime identity that becomes active is the exported
        artifact under the state directory — independent of the
        disposable checkout it came from — with the artifact's own
        interpreter entry point as the recorded binary. ``prepare``
        runs between materialization and the ledger write, so an
        artifact that cannot be built or proven runnable never becomes
        active (the failure is a durable ``failed`` attempt).
        """

        git_sha = self._validate(source_checkout)
        artifact = materialize_artifact(
            state_dir=state_dir,
            checkout_root=source_checkout,
            git_sha=git_sha,
            run=self._run,
        )
        if prepare is not None:
            try:
                prepare(artifact)
            except Exception as error:
                refusal = ActivationRefused(
                    f"artifact preparation failed: {error}"
                )
                self._record_attempt(
                    state="failed",
                    binary_path=artifact
                    / ".venv"
                    / "bin"
                    / "hermes-orchestrator",
                    checkout_root=artifact,
                    git_sha=git_sha,
                    reason=str(refusal),
                )
                raise refusal from error
        self._write_active_pointer(artifact)
        return self._record_attempt(
            state="active",
            binary_path=artifact / ".venv" / "bin" / "hermes-orchestrator",
            checkout_root=artifact,
            git_sha=git_sha,
            reason=f"artifact of {source_checkout}",
        )

    def reactivate(self, prior: RuntimeActivation) -> RuntimeActivation:
        """Reinstate a prior activation's identity as a new generation.

        An immutable artifact revalidates by its marker; a plain
        checkout revalidates fully. Either way the exact commit must
        still be observable and the live database schema must still be
        the one the prior runtime proved against — a rollback across a
        schema advance needs the matching database snapshot restored
        first, and fails closed here.
        """

        prior_root = Path(prior.checkout_root)
        observed = self.observed_sha(prior_root)
        if observed != prior.git_sha:
            raise ActivationRefused(
                f"{prior.checkout_root} no longer carries the prior "
                f"activation's commit {prior.git_sha[:12]}"
            )
        if prior.database_schema != self._database.schema_version():
            raise ActivationRefused(
                f"the prior runtime proved against schema "
                f"{prior.database_schema} but the live database is at "
                f"{self._database.schema_version()}; restore the matching "
                "database snapshot before rolling back"
            )
        if not (prior_root / "RUNTIME_SHA").is_file():
            self._validate(prior_root)
        else:
            self._write_active_pointer(prior_root)
        return self._record_attempt(
            state="active",
            binary_path=Path(prior.binary_path),
            checkout_root=prior_root,
            git_sha=prior.git_sha,
            reason=f"rollback to generation {prior.generation}",
        )

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

    def find_daemon_start(
        self, *, generation: int, since: str
    ) -> dict[str, object] | None:
        """The durable proof a supervised daemon started on a generation."""

        rows = self._database.execute(
            "SELECT payload_json FROM events "
            "WHERE event_type = 'daemon.started' AND occurred_at >= ? "
            "ORDER BY sequence DESC LIMIT 20",
            (since,),
        ).fetchall()
        import json as _json

        for row in rows:
            payload = _json.loads(str(row["payload_json"]))
            if payload.get("activation_generation") == generation:
                return payload
        return None

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


class ApplyFailed(RuntimeError):
    """The apply protocol could not prove a healthy outcome."""


@dataclass(frozen=True, slots=True)
class ApplyReport:
    """The proven outcome of one activation apply."""

    apply_id: str
    state: str
    target_generation: int | None
    verified_pid: int | None
    reason: str | None


class ActivationApplier:
    """Journaled activation apply with proven process rollback.

    Intent is durable before any process changes; the supervised job is
    restarted only after the ledger holds the new activation; success
    is proven exclusively by the freshly started daemon's own durable
    ``daemon.started`` event carrying the exact target generation. A
    startup that never proves out triggers a rollback that reinstates
    the prior activation, restarts the job again, and health-verifies
    the prior identity the same way before the rollback is recorded
    successful — anything unprovable is journaled ``ambiguous`` and
    fails closed.
    """

    def __init__(
        self,
        activator: RuntimeActivator,
        database: Database,
        *,
        kickstart: Callable[[], None],
        ids: Callable[[], str] | None = None,
        now: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] | None = None,
        verify_timeout_seconds: float = APPLY_VERIFY_TIMEOUT_SECONDS,
        poll_seconds: float = APPLY_POLL_SECONDS,
    ) -> None:
        self._activator = activator
        self._database = database
        self._kickstart = kickstart
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._now = now or (lambda: datetime.now(UTC))
        self._sleep = sleep or (lambda seconds: __import__("time").sleep(seconds))
        self._verify_timeout = verify_timeout_seconds
        self._poll = poll_seconds

    def apply(
        self,
        *,
        checkout_root: Path,
        binary_path: Path | None = None,
        artifact_state_dir: Path | None = None,
        prepare: Callable[[Path], None] | None = None,
    ) -> ApplyReport:
        apply_id = self._ids()
        prior = self._activator.current()
        self._journal(
            apply_id,
            state="intended",
            target_checkout=str(checkout_root),
            prior_generation=None if prior is None else prior.generation,
            target_generation=None,
            reason=None,
            insert=True,
        )
        try:
            if artifact_state_dir is not None:
                # The activated identity is the immutable exported
                # artifact, never the disposable source checkout; it
                # must build and prove runnable BEFORE it becomes
                # active.
                activation = self._activator.activate_artifact(
                    source_checkout=checkout_root,
                    state_dir=artifact_state_dir,
                    prepare=prepare,
                )
            else:
                if binary_path is None:
                    raise ActivationRefused(
                        "a direct activation requires the binary path"
                    )
                activation = self._activator.activate(
                    checkout_root=checkout_root, binary_path=binary_path
                )
                if prepare is not None:
                    prepare(Path(activation.checkout_root))
        except ActivationRefused as refusal:
            self._journal(
                apply_id, state="refused", reason=str(refusal)
            )
            raise
        self._journal(
            apply_id,
            state="activated",
            target_generation=activation.generation,
        )
        started_at = self._now().isoformat()
        self._kickstart()
        self._journal(apply_id, state="restarted")
        proof = self._await_daemon(activation.generation, since=started_at)
        if proof is not None:
            self._journal(apply_id, state="verified")
            return ApplyReport(
                apply_id=apply_id,
                state="verified",
                target_generation=activation.generation,
                verified_pid=int(proof.get("pid", 0)) or None,
                reason=None,
            )
        # The new runtime never proved out: reinstate the prior
        # activation and prove the prior executable is actually
        # running again before calling the rollback successful.
        if prior is None:
            self._journal(
                apply_id,
                state="ambiguous",
                reason="no prior activation exists to roll back to",
            )
            raise ApplyFailed(
                "the new runtime never reported healthy and no prior "
                "activation exists; the apply is ambiguous"
            )
        try:
            restored = self._activator.reactivate(prior)
        except ActivationRefused as refusal:
            self._journal(
                apply_id,
                state="ambiguous",
                reason=f"rollback target refused revalidation: {refusal}",
            )
            raise ApplyFailed(
                "the new runtime never reported healthy and the prior "
                "activation no longer validates; the apply is ambiguous"
            ) from refusal
        rollback_started = self._now().isoformat()
        self._kickstart()
        proof = self._await_daemon(
            restored.generation, since=rollback_started
        )
        if proof is None:
            self._journal(
                apply_id,
                state="ambiguous",
                reason="the rolled-back runtime never reported healthy",
            )
            raise ApplyFailed(
                "neither the new nor the restored runtime reported "
                "healthy; the apply is ambiguous"
            )
        self._journal(
            apply_id,
            state="rolled_back",
            reason=(
                f"generation {restored.generation} restored prior "
                f"identity {prior.git_sha[:12]}"
            ),
        )
        return ApplyReport(
            apply_id=apply_id,
            state="rolled_back",
            target_generation=restored.generation,
            verified_pid=int(proof.get("pid", 0)) or None,
            reason="the new runtime never reported healthy",
        )

    def _await_daemon(
        self, generation: int, *, since: str
    ) -> dict[str, object] | None:
        waited = 0.0
        while True:
            proof = self._activator.find_daemon_start(
                generation=generation, since=since
            )
            if proof is not None:
                return proof
            if waited >= self._verify_timeout:
                return None
            self._sleep(self._poll)
            waited += self._poll

    def _journal(
        self,
        apply_id: str,
        *,
        state: str,
        target_checkout: str | None = None,
        prior_generation: int | None = None,
        target_generation: int | None = None,
        reason: str | None = None,
        insert: bool = False,
    ) -> None:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            if insert:
                connection.execute(
                    "INSERT INTO activation_applies("
                    "apply_id, target_checkout, prior_generation, "
                    "target_generation, state, reason, created_at, "
                    "updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        apply_id,
                        target_checkout,
                        prior_generation,
                        target_generation,
                        state,
                        reason,
                        stamp,
                        stamp,
                    ),
                )
            else:
                connection.execute(
                    "UPDATE activation_applies SET state = ?, "
                    "target_generation = COALESCE(?, target_generation), "
                    "reason = COALESCE(?, reason), updated_at = ? "
                    "WHERE apply_id = ?",
                    (state, target_generation, reason, stamp, apply_id),
                )
