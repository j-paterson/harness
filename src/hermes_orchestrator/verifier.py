"""Trusted verification runner and content-addressed receipts (INFRA-186).

Receipts come from THIS runner's own measurements of the tree, the
environment, and the command's execution — never from anything a model
says happened. A model can request that a gate command be run, but it
can never author, forge, or backdate a receipt that validates: the
receipt's identity (``receipt_id``) is the sha256 of a canonical
binding document built entirely from facts this runner measured
itself (the git tree, any uncommitted diff, dependency lockfile
bytes, the runner's own fingerprint, and the command's actual exit
code / output), and the receipt's ``signature`` is an hmac-sha256 over
that same document keyed by a secret held only by this runner's state
directory. Nothing a prompt or model output supplies is ever trusted
as-is; it is either recomputed by the runner or ignored.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import platform
import re
import secrets
import shlex
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

RUNNER_VERSION = 1

_DEPENDENCY_FILES = ("uv.lock", "pyproject.toml")
_ABSENT_MARKER = b"<absent>"

_COUNT_PATTERNS = {
    "passed": re.compile(r"(\d+) passed"),
    "failed": re.compile(r"(\d+) failed"),
    "error": re.compile(r"(\d+) error"),
}

_BINDING_FIELDS = (
    "schema_version",
    "gate_id",
    "command",
    "tree_hash",
    "dirty_patch_hash",
    "dependency_hash",
    "runner_fingerprint",
    "exit_code",
    "test_counts",
    "output_hash",
)


@dataclass(frozen=True, slots=True)
class VerificationReceipt:
    """One content-addressed, signed record of a runner-measured gate run."""

    receipt_id: str
    schema_version: int
    gate_id: str
    command: str
    tree_hash: str
    dirty_patch_hash: str
    dependency_hash: str
    runner_fingerprint: str
    exit_code: int
    test_counts: dict[str, int]
    duration_seconds: float
    output_hash: str
    signature: str
    created_at: str


def _parse_test_counts(output: str) -> dict[str, int]:
    """Best-effort pytest-style counts from the tail of combined output."""

    lines = [line for line in output.strip().splitlines() if line.strip()]
    tail = "\n".join(lines[-10:])
    counts: dict[str, int] = {}
    for label, pattern in _COUNT_PATTERNS.items():
        match = pattern.search(tail)
        if match is not None:
            counts[label] = int(match.group(1))
    return counts


def _dependency_hash(cwd: Path) -> str:
    """sha256 over the bytes of the dependency files; a missing file
    hashes as an absent-marker rather than being skipped."""

    hasher = hashlib.sha256()
    for name in _DEPENDENCY_FILES:
        path = cwd / name
        if path.exists():
            hasher.update(path.read_bytes())
        else:
            hasher.update(_ABSENT_MARKER)
        hasher.update(b"\x00")
    return hasher.hexdigest()


def _runner_fingerprint() -> str:
    material = f"{RUNNER_VERSION}:{sys.version}:{platform.platform()}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _git(cwd: Path, *args: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(cwd), *args),
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


class Verifier:
    """Runs gate commands and records/validates trusted receipts."""

    def __init__(
        self,
        database: Database,
        *,
        events: EventStore,
        state_dir: Path,
        run: Callable[..., subprocess.CompletedProcess[str]] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._state_dir = state_dir
        self._run = run or subprocess.run
        self._now = now or (lambda: datetime.now(UTC))
        self._key: bytes | None = None

    def _signing_key(self) -> bytes:
        """Read (or create, once) the 32-byte hex signing key this runner
        alone holds. The key file is created mode 0600 so nothing but
        this runner can ever produce a valid signature."""

        if self._key is not None:
            return self._key
        self._state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = self._state_dir / "verifier.key"
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            key_hex = path.read_text(encoding="ascii").strip()
        else:
            key_hex = secrets.token_bytes(32).hex()
            try:
                os.write(descriptor, key_hex.encode("ascii"))
            finally:
                os.close(descriptor)
        self._key = bytes.fromhex(key_hex)
        return self._key

    def _context(self, cwd: Path, gate_id: str, command: str) -> dict[str, Any]:
        """Facts about the tree, dependencies, environment, and command,
        measured BY THIS RUNNER right now — never taken from a caller's
        say-so."""

        tree_hash = _git(cwd, "rev-parse", "HEAD^{tree}").strip()
        diff_output = _git(cwd, "diff", "HEAD")
        dirty_patch_hash = (
            ""
            if diff_output == ""
            else hashlib.sha256(diff_output.encode("utf-8")).hexdigest()
        )
        return {
            "gate_id": gate_id,
            "command": " ".join(shlex.split(command)),
            "tree_hash": tree_hash,
            "dirty_patch_hash": dirty_patch_hash,
            "dependency_hash": _dependency_hash(cwd),
            "runner_fingerprint": _runner_fingerprint(),
        }

    def run_verified(
        self,
        cwd: Path,
        *,
        gate_id: str,
        command: str,
        timeout: float = 1800,
    ) -> VerificationReceipt:
        """Execute ``command`` under ``cwd`` and record a signed,
        content-addressed receipt. Reruning the identical command against
        an unchanged tree yields the identical receipt (same
        ``receipt_id``) rather than a new row."""

        context = self._context(cwd, gate_id, command)
        argv = shlex.split(command)
        started = time.monotonic()
        try:
            completed = self._run(
                argv,
                cwd=str(cwd),
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            exit_code = int(completed.returncode)
            combined_output = (completed.stdout or "") + (completed.stderr or "")
        except subprocess.TimeoutExpired as expired:
            exit_code = -1
            stdout = expired.stdout or ""
            stderr = expired.stderr or ""
            if isinstance(stdout, bytes):
                stdout = stdout.decode("utf-8", errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", errors="replace")
            combined_output = stdout + stderr
        duration_seconds = time.monotonic() - started

        test_counts = _parse_test_counts(combined_output)
        output_hash = hashlib.sha256(combined_output.encode("utf-8")).hexdigest()

        binding = {
            "schema_version": RUNNER_VERSION,
            "gate_id": context["gate_id"],
            "command": context["command"],
            "tree_hash": context["tree_hash"],
            "dirty_patch_hash": context["dirty_patch_hash"],
            "dependency_hash": context["dependency_hash"],
            "runner_fingerprint": context["runner_fingerprint"],
            "exit_code": exit_code,
            "test_counts": test_counts,
            "output_hash": output_hash,
        }
        canonical = json.dumps(binding, sort_keys=True)
        receipt_id = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        signature = hmac.new(
            self._signing_key(), canonical.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        stamp = self._now().isoformat()

        with self._database.transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO verification_receipts("
                "receipt_id, schema_version, gate_id, command, tree_hash, "
                "dirty_patch_hash, dependency_hash, runner_fingerprint, "
                "exit_code, test_counts, duration_seconds, output_hash, "
                "signature, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    receipt_id,
                    binding["schema_version"],
                    binding["gate_id"],
                    binding["command"],
                    binding["tree_hash"],
                    binding["dirty_patch_hash"],
                    binding["dependency_hash"],
                    binding["runner_fingerprint"],
                    binding["exit_code"],
                    json.dumps(test_counts, sort_keys=True),
                    duration_seconds,
                    binding["output_hash"],
                    signature,
                    stamp,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="verification_receipt.recorded",
                    aggregate_type="verification_receipt",
                    aggregate_id=receipt_id,
                    payload={
                        "receipt_id": receipt_id,
                        "gate_id": gate_id,
                        "exit_code": exit_code,
                        "tree_hash": context["tree_hash"],
                    },
                ),
            )
        return self.get(receipt_id)

    def validate(
        self, receipt_id: str, *, cwd: Path, gate_id: str, command: str
    ) -> tuple[bool, str]:
        """Check a receipt's signature and freshness against the CURRENT,
        runner-measured state of ``cwd``. A hand-inserted or tampered row
        fails signature verification structurally — it was never signed
        by this runner's key."""

        row = self._database.execute(
            "SELECT * FROM verification_receipts WHERE receipt_id = ?",
            (receipt_id,),
        ).fetchone()
        if row is None:
            return False, "unknown receipt"

        stored_test_counts = json.loads(str(row["test_counts"]))
        stored_binding = {
            "schema_version": int(row["schema_version"]),
            "gate_id": str(row["gate_id"]),
            "command": str(row["command"]),
            "tree_hash": str(row["tree_hash"]),
            "dirty_patch_hash": str(row["dirty_patch_hash"]),
            "dependency_hash": str(row["dependency_hash"]),
            "runner_fingerprint": str(row["runner_fingerprint"]),
            "exit_code": int(row["exit_code"]),
            "test_counts": stored_test_counts,
            "output_hash": str(row["output_hash"]),
        }
        canonical = json.dumps(stored_binding, sort_keys=True)
        expected_signature = hmac.new(
            self._signing_key(), canonical.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected_signature, str(row["signature"])):
            return False, "signature invalid"

        context = self._context(cwd, gate_id, command)
        if (
            context["tree_hash"] != stored_binding["tree_hash"]
            or context["dirty_patch_hash"] != stored_binding["dirty_patch_hash"]
        ):
            return False, "stale: tree changed"
        if context["dependency_hash"] != stored_binding["dependency_hash"]:
            return False, "stale: dependencies changed"
        if context["command"] != stored_binding["command"]:
            return False, "stale: command changed"
        if context["runner_fingerprint"] != stored_binding["runner_fingerprint"]:
            return False, "stale: runner or environment changed"
        if stored_binding["exit_code"] != 0:
            return False, "receipt records a failing run"
        return True, "fresh"

    def find_fresh(
        self, cwd: Path, *, gate_id: str, command: str
    ) -> VerificationReceipt | None:
        """The newest receipt for ``gate_id`` at the current tree that
        still validates fresh, or ``None``."""

        context = self._context(cwd, gate_id, command)
        rows = self._database.execute(
            "SELECT receipt_id FROM verification_receipts "
            "WHERE gate_id = ? AND tree_hash = ? "
            "ORDER BY created_at DESC, rowid DESC",
            (gate_id, context["tree_hash"]),
        ).fetchall()
        for row in rows:
            receipt_id = str(row["receipt_id"])
            is_fresh, _ = self.validate(
                receipt_id, cwd=cwd, gate_id=gate_id, command=command
            )
            if is_fresh:
                return self.get(receipt_id)
        return None

    def get(self, receipt_id: str) -> VerificationReceipt:
        row = self._database.execute(
            "SELECT * FROM verification_receipts WHERE receipt_id = ?",
            (receipt_id,),
        ).fetchone()
        if row is None:
            raise KeyError(receipt_id)
        return VerificationReceipt(
            receipt_id=str(row["receipt_id"]),
            schema_version=int(row["schema_version"]),
            gate_id=str(row["gate_id"]),
            command=str(row["command"]),
            tree_hash=str(row["tree_hash"]),
            dirty_patch_hash=str(row["dirty_patch_hash"]),
            dependency_hash=str(row["dependency_hash"]),
            runner_fingerprint=str(row["runner_fingerprint"]),
            exit_code=int(row["exit_code"]),
            test_counts=dict(json.loads(str(row["test_counts"]))),
            duration_seconds=float(row["duration_seconds"]),
            output_hash=str(row["output_hash"]),
            signature=str(row["signature"]),
            created_at=str(row["created_at"]),
        )
