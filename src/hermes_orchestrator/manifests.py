"""Immutable candidate manifests and typed Fable wake events."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

MANIFEST_VERSION = 1
WAKE_STATUSES = frozenset(
    {"FABLE_READY", "FABLE_REWORK_READY", "FABLE_BLOCKED", "FABLE_COMPLETE"}
)
_READY_STATUSES = frozenset({"FABLE_READY", "FABLE_REWORK_READY"})
_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_EVENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_LINEAR_ISSUE_PATTERN = re.compile(r"^[A-Z][A-Z0-9]{1,9}-[1-9][0-9]{0,5}$")

# Deterministic seams so identity-race tests can interleave an attacker
# between syscalls; production behavior is the plain os call.
_fchmod = os.fchmod
_stat_path = os.stat
_MANIFEST_KEYS = frozenset(
    {
        "manifestVersion",
        "eventId",
        "status",
        "candidateSha",
        "baseSha",
        "branch",
        "linearIssues",
        "changedFiles",
        "verification",
        "blockers",
        "createdAt",
    }
)


class ManifestError(ValueError):
    """Raised when a candidate manifest is invalid; nothing may proceed."""


@dataclass(frozen=True, slots=True)
class WakeEvent:
    """One validated explicit Fable wake; the only shape that can be queued.

    Construction fails closed on anything that is not a real FABLE_* event
    with strict event-ID, Linear-issue, SHA, and digest grammar. The event
    is still never sufficient authority on its own: delivery re-reads the
    confined immutable manifest and compares it against these fields.
    """

    status: str
    issue_id: str
    candidate_sha: str
    base_sha: str
    manifest_path: str
    event_id: str
    manifest_digest: str

    def __post_init__(self) -> None:
        if self.status not in WAKE_STATUSES:
            raise ValueError(f"unknown wake status {self.status}")
        if _EVENT_ID_PATTERN.match(self.event_id or "") is None:
            raise ValueError(f"wake event id {self.event_id!r} is invalid")
        if _LINEAR_ISSUE_PATTERN.match(self.issue_id or "") is None:
            raise ValueError(
                f"wake event issue {self.issue_id!r} is not a Linear issue"
            )
        for name in ("candidate_sha", "base_sha"):
            if _SHA_PATTERN.match(getattr(self, name) or "") is None:
                raise ValueError(f"wake event {name} is not a full sha")
        if _DIGEST_PATTERN.match(self.manifest_digest or "") is None:
            raise ValueError("wake event manifest_digest is invalid")
        if not self.manifest_path:
            raise ValueError("wake event manifest_path must not be empty")

    def render(self, generation: int) -> str:
        """Render the wake envelope for the exact channel generation invoked."""

        return (
            f"{self.status} issue={self.issue_id} "
            f"candidate={self.candidate_sha} base={self.base_sha} "
            f"manifest={self.manifest_path} event={self.event_id} "
            f"generation={generation}"
        )


@dataclass(frozen=True, slots=True)
class ManifestIdentity:
    """Durable file identity of one published manifest."""

    device: int
    inode: int
    size: int
    mtime_ns: int
    mode: int


@dataclass(frozen=True, slots=True)
class CandidateManifest:
    """One validated, immutable candidate emitted at a freeze boundary."""

    manifest_version: int
    event_id: str
    status: str
    candidate_sha: str
    base_sha: str
    branch: str
    linear_issues: tuple[str, ...]
    changed_files: tuple[str, ...]
    verification: tuple[tuple[str, str], ...]
    blockers: tuple[str, ...]
    created_at: str


def write_manifest(
    root: Path, manifest: CandidateManifest, *, head_sha: str
) -> Path:
    """Atomically publish one immutable manifest inside the state root.

    The document is fully validated first, the candidate must equal the
    proven repository HEAD, the payload is written through an exclusive
    unpredictable temporary file that is fsynced and made read-only, and
    publication uses an exclusive hard link so an existing manifest can
    never be replaced. The directory is fsynced after publication.
    """

    document = _serialize(manifest)
    _validate(document)
    if manifest.candidate_sha != head_sha:
        raise ManifestError(
            "candidateSha does not equal the repository HEAD at the freeze "
            "boundary"
        )
    resolved_root = _resolved_root(root)
    final_name = f"{manifest.event_id}.json"
    final_path = resolved_root / final_name
    directory_descriptor = os.open(
        resolved_root, os.O_RDONLY | os.O_DIRECTORY
    )
    try:
        descriptor, temporary = tempfile.mkstemp(
            prefix=".manifest-", suffix=".tmp", dir=resolved_root
        )
        temp_name = Path(temporary).name
        try:
            payload = json.dumps(document, sort_keys=True, indent=1).encode(
                "utf-8"
            )
            os.write(descriptor, payload)
            os.fsync(descriptor)
            _fchmod(descriptor, 0o444)
            os.fsync(descriptor)
            published = os.fstat(descriptor)
            named = os.stat(
                temp_name, dir_fd=directory_descriptor, follow_symlinks=False
            )
            if (named.st_dev, named.st_ino) != (
                published.st_dev,
                published.st_ino,
            ):
                raise ManifestError(
                    f"manifest {final_name} temporary was replaced during "
                    "publication"
                )
            try:
                os.link(
                    temp_name,
                    final_name,
                    src_dir_fd=directory_descriptor,
                    dst_dir_fd=directory_descriptor,
                    follow_symlinks=False,
                )
            except FileExistsError:
                raise ManifestError(
                    f"manifest {final_name} is immutable"
                ) from None
            linked = os.stat(
                final_name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
            if (linked.st_dev, linked.st_ino) != (
                published.st_dev,
                published.st_ino,
            ):
                os.unlink(final_name, dir_fd=directory_descriptor)
                raise ManifestError(
                    f"manifest {final_name} was replaced during publication"
                )
        finally:
            os.close(descriptor)
            with suppress(FileNotFoundError):
                os.unlink(temp_name, dir_fd=directory_descriptor)
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
    return final_path


def manifest_digest_for(manifest: CandidateManifest) -> str:
    """Compute the content digest of one manifest's canonical serialization."""

    document = _serialize(manifest)
    payload = json.dumps(document, sort_keys=True, indent=1).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def manifest_document(manifest: CandidateManifest) -> dict[str, Any]:
    """The canonical serialized document of one validated manifest."""

    document = _serialize(manifest)
    _validate(document)
    return document


def manifest_from_document(value: dict[str, Any]) -> CandidateManifest:
    """Validate and rebuild one manifest from its canonical document."""

    _validate(value)
    return CandidateManifest(
        manifest_version=value["manifestVersion"],
        event_id=value["eventId"],
        status=value["status"],
        candidate_sha=value["candidateSha"],
        base_sha=value["baseSha"],
        branch=value["branch"],
        linear_issues=tuple(value["linearIssues"]),
        changed_files=tuple(value["changedFiles"]),
        verification=tuple(
            (entry["command"], entry["outcome"])
            for entry in value["verification"]
        ),
        blockers=tuple(value["blockers"]),
        created_at=value["createdAt"],
    )


@dataclass(frozen=True, slots=True)
class ManifestSnapshot:
    """One validated manifest with its content digest and file identity."""

    manifest: CandidateManifest
    digest: str
    identity: ManifestIdentity


def read_manifest(
    path: Path, *, root: Path, expected_digest: str | None = None
) -> CandidateManifest:
    """Read one confined immutable manifest; see read_manifest_snapshot."""

    return read_manifest_snapshot(
        path, root=root, expected_digest=expected_digest
    ).manifest


def read_manifest_snapshot(
    path: Path,
    *,
    root: Path,
    expected_digest: str | None = None,
    expected_identity: ManifestIdentity | None = None,
) -> ManifestSnapshot:
    """Strictly read one immutable manifest confined to the state root.

    The file must live directly inside the resolved root, must not be a
    symlink, must not be writable (checked on the opened descriptor), its
    complete stat identity (device, inode, size, mtime, mode) must be
    stable before and after the read and match what the path currently
    names, and — when a durable digest or file identity recorded at
    publication or delivery is supplied — the content digest and the
    complete identity must match exactly. Callers receive the validated
    snapshot so they never restat the path themselves.
    """

    resolved_root = _resolved_root(root)
    name = path.name
    if not name.endswith(".json") or (
        _EVENT_ID_PATTERN.match(name[: -len(".json")]) is None
    ):
        raise ManifestError(f"manifest filename {name!r} is invalid")
    parent = Path(os.path.realpath(path.parent))
    if parent != resolved_root:
        raise ManifestError(
            f"manifest {name} is outside the configured state root"
        )
    target = parent / name
    try:
        descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as error:
        raise ManifestError(
            f"manifest {name} is unreadable or a symlink"
        ) from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ManifestError(f"manifest {name} is not a regular file")
        if before.st_mode & 0o222:
            raise ManifestError(f"manifest {name} is mutable")
        raw = os.read(descriptor, before.st_size + 1)
        after = os.fstat(descriptor)
        current = _stat_path(target, follow_symlinks=False)
        before_identity = _full_identity(before)
        if (
            before_identity != _full_identity(after)
            or before_identity != _full_identity(current)
        ):
            raise ManifestError(
                f"manifest {name} changed identity during the read"
            )
    finally:
        os.close(descriptor)
    digest = hashlib.sha256(raw).hexdigest()
    if expected_digest is not None and digest != expected_digest:
        raise ManifestError(
            f"manifest {name} content digest does not match the durable "
            "identity recorded at publication"
        )
    identity = ManifestIdentity(*_full_identity(before))
    if expected_identity is not None and identity != expected_identity:
        raise ManifestError(
            f"manifest {name} file identity does not match the durable "
            "identity recorded at registration"
        )
    try:
        value = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ManifestError(f"manifest {name} is unreadable") from error
    if not isinstance(value, dict):
        raise ManifestError("manifest root must be an object")
    manifest = manifest_from_document(value)
    if value["eventId"] != name[: -len(".json")]:
        raise ManifestError(
            f"manifest {name} filename does not match its eventId"
        )
    return ManifestSnapshot(
        manifest=manifest, digest=digest, identity=identity
    )


def wake_event_for(manifest: CandidateManifest, path: Path) -> WakeEvent:
    """Derive the typed wake envelope for one validated manifest."""

    return WakeEvent(
        status=manifest.status,
        issue_id=manifest.linear_issues[0],
        candidate_sha=manifest.candidate_sha,
        base_sha=manifest.base_sha,
        manifest_path=str(path),
        event_id=manifest.event_id,
        manifest_digest=manifest_digest_for(manifest),
    )


def _full_identity(entry: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        entry.st_dev,
        entry.st_ino,
        entry.st_size,
        entry.st_mtime_ns,
        stat.S_IMODE(entry.st_mode),
    )


def _resolved_root(root: Path) -> Path:
    try:
        resolved = root.resolve(strict=True)
    except OSError as error:
        raise ManifestError("manifest state root does not exist") from error
    if not resolved.is_dir():
        raise ManifestError("manifest state root is not a directory")
    return resolved


def _serialize(manifest: CandidateManifest) -> dict[str, Any]:
    return {
        "manifestVersion": manifest.manifest_version,
        "eventId": manifest.event_id,
        "status": manifest.status,
        "candidateSha": manifest.candidate_sha,
        "baseSha": manifest.base_sha,
        "branch": manifest.branch,
        "linearIssues": list(manifest.linear_issues),
        "changedFiles": list(manifest.changed_files),
        "verification": [
            {"command": command, "outcome": outcome}
            for command, outcome in manifest.verification
        ],
        "blockers": list(manifest.blockers),
        "createdAt": manifest.created_at,
    }


def _validate(document: dict[str, Any]) -> None:
    supplied = set(document)
    missing = _MANIFEST_KEYS - supplied
    if missing:
        raise ManifestError(f"manifest is missing {sorted(missing)[0]}")
    extra = supplied - _MANIFEST_KEYS
    if extra:
        raise ManifestError(
            f"manifest carries unknown field {sorted(extra)[0]}"
        )
    version = document["manifestVersion"]
    if version != MANIFEST_VERSION:
        raise ManifestError(f"unknown manifest version {version!r}")
    status = document["status"]
    if status not in WAKE_STATUSES:
        raise ManifestError(f"unknown manifest status {status!r}")
    event_id = document["eventId"]
    if not isinstance(event_id, str) or (
        _EVENT_ID_PATTERN.match(event_id) is None
    ):
        raise ManifestError(f"manifest eventId {event_id!r} is invalid")
    branch = document["branch"]
    if not isinstance(branch, str) or not branch or branch != branch.strip():
        raise ManifestError("manifest branch is invalid")
    for field in ("candidateSha", "baseSha"):
        value = document[field]
        if not isinstance(value, str) or _SHA_PATTERN.match(value) is None:
            raise ManifestError(f"manifest {field} is not a full sha")
    created_at = document["createdAt"]
    if not isinstance(created_at, str):
        raise ManifestError("manifest createdAt is invalid")
    try:
        datetime.fromisoformat(created_at)
    except ValueError as error:
        raise ManifestError("manifest createdAt is invalid") from error
    issues = _string_list(document, "linearIssues")
    for issue in issues:
        if _LINEAR_ISSUE_PATTERN.match(issue) is None:
            raise ManifestError(
                f"manifest linearIssues entry {issue!r} is not a Linear "
                "issue identifier"
            )
    files = _string_list(document, "changedFiles")
    _string_list(document, "blockers")
    for candidate_file in files:
        parts = Path(candidate_file).parts
        if Path(candidate_file).is_absolute() or ".." in parts:
            raise ManifestError(
                f"manifest changed file {candidate_file!r} escapes the "
                "repository path"
            )
    verification = document["verification"]
    if not isinstance(verification, list):
        raise ManifestError("manifest verification is invalid")
    for entry in verification:
        if (
            not isinstance(entry, dict)
            or set(entry) != {"command", "outcome"}
            or not all(
                isinstance(entry[key], str) and entry[key]
                for key in ("command", "outcome")
            )
        ):
            raise ManifestError("manifest verification entries are invalid")
    if not issues:
        raise ManifestError(
            "every wakeable manifest requires at least one entry in "
            "linearIssues"
        )
    if status in _READY_STATUSES:
        if not files:
            raise ManifestError(
                "ready manifests require at least one entry in changedFiles"
            )
        if not verification:
            raise ManifestError(
                "ready manifests require exact verification entries"
            )
    if status == "FABLE_BLOCKED" and not files and not document["blockers"]:
        raise ManifestError(
            "blocked manifests without changed files require blockers"
        )
    if status == "FABLE_COMPLETE" and not files and not verification:
        raise ManifestError(
            "complete manifests without changed files require verification"
        )


def _string_list(document: dict[str, Any], field: str) -> list[str]:
    value = document[field]
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise ManifestError(f"manifest {field} is invalid")
    if len(set(value)) != len(value):
        raise ManifestError(f"manifest {field} contains duplicate entries")
    return value
