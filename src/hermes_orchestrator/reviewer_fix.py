"""The bounded reviewer-fix helper: ACCEPT_WITH_REVIEWER_FIX.

INFRA-194 (operator ruling, hardened by Sol corrections 2e1ddcce and
48ac5268): Sol's write scope is the exact approved merge plus this
helper. An eligible fix is mechanical, unambiguous, and auditable — at
most two files and thirty changed lines, with every added, modified,
deleted, and untracked file contributing its complete textual delta
(the whole fix is staged before eligibility is decided) — and never
touches migrations, schemas, generated artifacts, or the categorically
excluded judgment surfaces, which return to the Fable lead.

Publication is intent-journaled behind a single durable owner: every
locally provable gate — the staged eligibility budget, the labeled
commit, focused verification, the shared mandatory candidate gate
(:func:`run_freeze_gate`, the same gate the candidate emitter runs) —
completes first, then the intent row is committed carrying an owner
token, a lease, and the complete prevalidated manifest (event id,
canonical payload, digest). The owner crosses the ``attempted``
boundary durably before the exact expected-remote-head
compare-and-push, so a definitely unattempted intent is always
distinguishable from an attempted or unknown outcome. Reconciliation
adopts an intent only after its lease expires: an expired ``intended``
row was provably never pushed and aborts retryably; an expired
``attempted`` row is resolved from the remote truth — exact final-SHA
identity or proven ancestry finalizes the intent-bound manifest
exactly once, an exactly-unchanged expected head aborts retryably, and
anything unprovable parks durably as ``reconciliation_required``,
which keeps blocking further fixes and never aborts.
"""

from __future__ import annotations

import json
import re
import shlex
import subprocess
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.emission import EmissionBlocked, run_freeze_gate
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.git import GitRunner
from hermes_orchestrator.manifests import (
    MANIFEST_VERSION,
    CandidateManifest,
    ManifestError,
    manifest_digest_for,
    manifest_document,
    manifest_from_document,
    read_manifest_snapshot,
    write_manifest,
)

FIX_LABEL = "ACCEPT_WITH_REVIEWER_FIX"

MAX_FILES = 2
MAX_CHANGED_LINES = 30

# How long one recorded publication intent remains exclusively owned.
# While the lease is live only the owner may cross or settle the
# boundary; reconciliation adopts the intent only after expiry.
LEASE_SECONDS = 900

# Every state that still blocks further fixes for the project.
_BLOCKING_STATES = "('intended', 'attempted', 'reconciliation_required')"

# The mechanically enforceable subset of the categorical exclusions;
# the judgment-bearing remainder (pricing, public APIs, ownership
# architecture, broad consumer changes) is enforced by contract and
# review, not by path matching.
_EXCLUDED_PATH_PATTERN = re.compile(
    r"(^|/)(migrations?|schemas?|generated)(/|$)|\.sql$"
)

_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class ReviewerFixRefused(RuntimeError):
    """The fix is ineligible or unprovable; the guarded step did not run."""


@dataclass(frozen=True, slots=True)
class ReviewerFixReceipt:
    """One durable, auditable bounded-fix receipt."""

    fix_id: str
    project_key: str
    issue_id: str
    submitted_sha: str
    final_sha: str
    event_id: str | None
    message: str
    files: tuple[str, ...]
    changed_lines: int
    verification: tuple[tuple[str, str], ...]
    state: str
    reason: str | None


# Runs one focused verification command; returns (exit_code, output).
CommandRunner = Callable[[str, Path], tuple[int, str]]


def _subprocess_command_runner(command: str, cwd: Path) -> tuple[int, str]:
    completed = subprocess.run(
        shlex.split(command),
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=1200,
        check=False,
    )
    return completed.returncode, completed.stdout + completed.stderr


class ReviewerFixHelper:
    """Create, verify, intent-journal, and durably receipt one fix."""

    def __init__(
        self,
        *,
        database: Database,
        events: EventStore,
        projects: Mapping[str, ProjectConfig],
        git: GitRunner,
        manifest_root: Path,
        commands: CommandRunner = _subprocess_command_runner,
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
        lease_seconds: int = LEASE_SECONDS,
    ) -> None:
        self._database = database
        self._events = events
        self._projects = dict(projects)
        self._git = git
        self._manifest_root = manifest_root
        self._commands = commands
        self._now = now or (lambda: datetime.now(UTC))
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._lease_seconds = lease_seconds

    # -- the one fix -------------------------------------------------------

    def apply(
        self,
        project_key: str,
        issue_id: str,
        *,
        submitted_sha: str,
        message: str,
        verification: tuple[str, ...],
    ) -> ReviewerFixReceipt:
        project = self._projects.get(project_key)
        if project is None:
            raise ReviewerFixRefused(f"unknown project {project_key!r}")
        if not message.strip():
            raise ReviewerFixRefused("a reviewer fix requires a message")
        if not verification:
            raise ReviewerFixRefused(
                "a reviewer fix requires focused verification commands"
            )
        if _SHA_PATTERN.match(submitted_sha) is None:
            raise ReviewerFixRefused(
                "the submitted sha must be one full commit sha"
            )
        # An unresolved publication boundary blocks every further fix;
        # reconciliation only adopts what it can safely prove.
        self.reconcile(project_key)
        pending = self._database.execute(
            "SELECT fix_id FROM reviewer_fixes "
            f"WHERE project_key = ? AND state IN {_BLOCKING_STATES}",
            (project_key,),
        ).fetchone()
        if pending is not None:
            raise ReviewerFixRefused(
                "an unresolved reviewer-fix publication boundary exists; "
                "no further fix is permitted until it reconciles"
            )
        existing = self._database.execute(
            "SELECT fix_id FROM reviewer_fixes "
            "WHERE project_key = ? AND submitted_sha = ? "
            "AND state != 'aborted'",
            (project_key, submitted_sha),
        ).fetchone()
        if existing is not None:
            raise ReviewerFixRefused(
                "this submitted candidate already carries a reviewer fix"
            )
        repo = project.repo_path

        # The fix must sit as uncommitted work atop the exact immutable
        # submitted SHA — never atop anything else.
        head = self._run(repo, "rev-parse", "HEAD").strip()
        if head != submitted_sha:
            raise ReviewerFixRefused(
                "HEAD is not the immutable submitted candidate"
            )
        if not self._run(repo, "status", "--porcelain").strip():
            raise ReviewerFixRefused("there is no fix in the working tree")

        # Stage the COMPLETE fix first, so untracked files contribute
        # their full textual delta; then decide eligibility on the
        # index. A refusal unstages and leaves the worktree untouched.
        self._run(repo, "add", "--all")
        try:
            files, changed_lines = self._eligibility(repo)
        except ReviewerFixRefused:
            self._run(repo, "reset")
            raise

        # One clearly labeled commit atop the submitted SHA.
        self._run(repo, "commit", "-m", f"{FIX_LABEL}: {message.strip()}")
        final_sha = self._run(repo, "rev-parse", "HEAD").strip()

        # Focused verification proves the fix before it is published;
        # a failure keeps the labeled commit local for human review.
        results: list[tuple[str, str]] = []
        for command in verification:
            code, _output = self._commands(command, repo)
            if code != 0:
                raise ReviewerFixRefused(
                    f"focused verification failed ({command!r}); the "
                    "labeled fix commit remains local and unpublished"
                )
            results.append((command, "passed"))

        # The mandatory candidate gate — the exact same gate the
        # candidate emitter runs — proves everything locally provable
        # before any remote mutation.
        try:
            facts = run_freeze_gate(
                lambda *args: self._run(repo, *args),
                project,
                expect_pushed=False,
            )
        except EmissionBlocked as error:
            raise ReviewerFixRefused(
                f"the mandatory candidate gate refused: {error}"
            ) from error
        if facts.head != final_sha:
            raise ReviewerFixRefused(
                "the gate proved a different HEAD than the fix commit"
            )

        # The complete manifest — event id, canonical payload, digest —
        # is prevalidated and bound into the durable intent BEFORE the
        # push, so every later finalization writes or verifies exactly
        # this immutable snapshot and never regenerates identity from
        # mutable state.
        manifest = self._manifest(
            issue_id, facts.branch, facts.base, final_sha,
            facts.changed_files, tuple(results),
        )
        document = manifest_document(manifest)
        digest = manifest_digest_for(manifest)
        fix_id = self._record_intent(
            project_key,
            issue_id,
            submitted_sha=submitted_sha,
            final_sha=final_sha,
            branch=facts.branch,
            message=message,
            files=files,
            changed_lines=changed_lines,
            results=tuple(results),
            event_id=manifest.event_id,
            manifest_json=json.dumps(document, sort_keys=True),
            manifest_digest=digest,
        )

        # The attempted boundary is crossed durably BEFORE the push, so
        # a crash from here on can never be mistaken for an unattempted
        # intent.
        self._transition(fix_id, "intended", "attempted")

        # Exact expected-remote-head compare-and-push: the remote must
        # still be the immutable submitted candidate, or nothing moves.
        lease = f"refs/heads/{facts.branch}:{submitted_sha}"
        push = self._git.run(
            (
                "git", "push", f"--force-with-lease={lease}",
                "--", "origin", facts.branch,
            ),
            repo,
        )
        if push.returncode == 0:
            return self._finalize(project, fix_id, "attempted")
        return self._settle_owned_attempt(project, fix_id)

    # -- deterministic reconciliation --------------------------------------

    def reconcile(self, project_key: str) -> tuple[ReviewerFixReceipt, ...]:
        """Resolve what the remote truth can prove; never guess.

        A live lease is exclusively the owner's: its intent is left
        untouched and keeps blocking. An expired ``intended`` row was
        provably never pushed and aborts retryably. An expired
        ``attempted`` row (or a parked ``reconciliation_required`` row,
        whose push is by then provably not in flight) resolves from the
        remote: exact final-SHA identity or proven ancestry finalizes
        the intent-bound manifest exactly once, an exactly-unchanged
        expected head aborts retryably, and anything unprovable parks
        (or stays) as ``reconciliation_required``. A legacy schema-36
        row (empty owner token) left no attempted-boundary evidence, so
        an unchanged expected head never auto-aborts it — it stays
        blocking until identity or ancestry proves the outcome. An
        unreadable remote leaves the intent blocking.
        """

        project = self._projects.get(project_key)
        if project is None:
            raise ReviewerFixRefused(f"unknown project {project_key!r}")
        outcomes: list[ReviewerFixReceipt] = []
        rows = self._database.execute(
            "SELECT * FROM reviewer_fixes "
            f"WHERE project_key = ? AND state IN {_BLOCKING_STATES} "
            "ORDER BY created_at ASC, rowid ASC",
            (project_key,),
        ).fetchall()
        for row in rows:
            fix_id = str(row["fix_id"])
            state = str(row["state"])
            if state == "intended":
                if self._lease_live(row):
                    continue
                # The attempted boundary was never crossed: the push is
                # provably unattempted and the intent aborts retryably.
                self._transition(
                    fix_id,
                    "intended",
                    "aborted",
                    reason="the owner lease expired before the push "
                    "boundary; the fix was provably unattempted",
                )
                outcomes.append(self._receipt(fix_id))
                continue
            if state == "attempted" and self._lease_live(row):
                # The owner's push may still be in flight; only the
                # owner or lease expiry may settle it.
                continue
            repo = project.repo_path
            branch = str(row["branch"])
            self._run(repo, "fetch", "--", "origin", branch)
            remote = self._run(repo, "rev-parse", f"origin/{branch}").strip()
            outcome = self._classify(
                repo,
                remote,
                str(row["final_sha"]),
                str(row["expected_remote_sha"]),
            )
            legacy = str(row["owner_token"]) == ""
            if outcome == "landed":
                outcomes.append(self._finalize(project, fix_id, state))
            elif outcome == "not_landed" and not legacy:
                self._transition(
                    fix_id,
                    state,
                    "aborted",
                    reason=f"push did not land; remote head {remote[:12]}",
                )
                outcomes.append(self._receipt(fix_id))
            elif state == "attempted":
                self._transition(
                    fix_id,
                    "attempted",
                    "reconciliation_required",
                    reason="push outcome unproven; remote head "
                    f"{remote[:12]}",
                )
                outcomes.append(self._receipt(fix_id))
            # A reconciliation_required row that is still unprovable —
            # and every legacy row short of a landed proof — stays
            # parked and blocking, without a state change.
        return tuple(outcomes)

    def pending_intents(self, project_key: str) -> int:
        value = self._database.scalar(
            "SELECT COUNT(*) FROM reviewer_fixes "
            f"WHERE project_key = ? AND state IN {_BLOCKING_STATES}",
            (project_key,),
        )
        return int(value)  # type: ignore[arg-type]

    # -- internals ---------------------------------------------------------

    def _settle_owned_attempt(
        self, project: ProjectConfig, fix_id: str
    ) -> ReviewerFixReceipt:
        """The owner settles its own completed-but-failed push call."""

        row = self._database.execute(
            "SELECT * FROM reviewer_fixes WHERE fix_id = ?", (fix_id,)
        ).fetchone()
        assert row is not None
        repo = project.repo_path
        branch = str(row["branch"])
        fetch = self._git.run(("git", "fetch", "--", "origin", branch), repo)
        if fetch.returncode != 0:
            self._transition(
                fix_id,
                "attempted",
                "reconciliation_required",
                reason="the remote was unreadable after the push; the "
                "outcome is unproven",
            )
            raise ReviewerFixRefused(
                "the push outcome is unprovable (remote unreadable); the "
                "publication intent is parked as reconciliation_required "
                "and blocks further fixes"
            )
        remote = self._run(repo, "rev-parse", f"origin/{branch}").strip()
        outcome = self._classify(
            repo,
            remote,
            str(row["final_sha"]),
            str(row["expected_remote_sha"]),
        )
        if outcome == "landed":
            return self._finalize(project, fix_id, "attempted")
        if outcome == "not_landed":
            self._transition(
                fix_id,
                "attempted",
                "aborted",
                reason=f"push did not land; remote head {remote[:12]}",
            )
            raise ReviewerFixRefused(
                "the expected-head push did not land; the publication "
                "intent was reconciled as aborted"
            )
        self._transition(
            fix_id,
            "attempted",
            "reconciliation_required",
            reason=f"push outcome unproven; remote head {remote[:12]}",
        )
        raise ReviewerFixRefused(
            "the push outcome is unprovable; the publication intent is "
            "parked as reconciliation_required and blocks further fixes"
        )

    def _classify(
        self, repo: Path, remote: str, final_sha: str, expected: str
    ) -> str:
        """Prove landed, not_landed, or unknown — never guess.

        Exact final-SHA identity proves landed. An exactly-unchanged
        expected head proves the lease push never landed. Otherwise the
        branch may have advanced past the fix, so proven ancestry of
        the final SHA also proves landed; an ancestry check that cannot
        be established, or any other head, is unknown.
        """

        if remote == final_sha:
            return "landed"
        if remote == expected:
            return "not_landed"
        ancestry = self._git.run(
            ("git", "merge-base", "--is-ancestor", final_sha, remote), repo
        )
        if ancestry.returncode == 0:
            return "landed"
        return "unknown"

    def _lease_live(self, row: object) -> bool:
        raw = str(row["lease_expires_at"])  # type: ignore[index]
        try:
            expires = datetime.fromisoformat(raw)
        except ValueError:
            # An unparseable lease can never justify adopting a
            # possibly in-flight boundary.
            return True
        return self._now() < expires

    def _manifest(
        self,
        issue_id: str,
        branch: str,
        base: str,
        final_sha: str,
        changed: tuple[str, ...],
        verification: tuple[tuple[str, str], ...],
    ) -> CandidateManifest:
        stamp = self._now().astimezone(UTC)
        event_id = (
            "fable_rework_ready-"
            f"{issue_id.lower()}-{final_sha[:12]}-"
            f"{stamp.strftime('%Y%m%dT%H%M%S')}"
        )
        return CandidateManifest(
            manifest_version=MANIFEST_VERSION,
            event_id=event_id,
            status="FABLE_REWORK_READY",
            candidate_sha=final_sha,
            base_sha=base,
            branch=branch,
            linear_issues=(issue_id,),
            changed_files=changed,
            verification=verification,
            blockers=(),
            created_at=stamp.isoformat(),
        )

    def _record_intent(
        self,
        project_key: str,
        issue_id: str,
        *,
        submitted_sha: str,
        final_sha: str,
        branch: str,
        message: str,
        files: tuple[str, ...],
        changed_lines: int,
        results: tuple[tuple[str, str], ...],
        event_id: str,
        manifest_json: str,
        manifest_digest: str,
    ) -> str:
        fix_id = self._ids()
        stamp = self._now().isoformat()
        lease_expires = (
            self._now() + timedelta(seconds=self._lease_seconds)
        ).isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO reviewer_fixes("
                "fix_id, project_key, issue_id, submitted_sha, final_sha, "
                "branch, expected_remote_sha, owner_token, "
                "lease_expires_at, event_id, manifest_json, "
                "manifest_digest, message, files_json, changed_lines, "
                "verification_json, state, created_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, 'intended', ?, ?)",
                (
                    fix_id,
                    project_key,
                    issue_id,
                    submitted_sha,
                    final_sha,
                    branch,
                    submitted_sha,
                    self._ids(),
                    lease_expires,
                    event_id,
                    manifest_json,
                    manifest_digest,
                    message.strip(),
                    json.dumps(list(files)),
                    changed_lines,
                    json.dumps(list(results)),
                    stamp,
                    stamp,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="reviewer_fix.intended",
                    aggregate_type="reviewer_fix",
                    aggregate_id=fix_id,
                    actor="codex_merger",
                    payload={
                        "issue_id": issue_id,
                        "submitted_sha": submitted_sha,
                        "final_sha": final_sha,
                        "event_id": event_id,
                        "manifest_digest": manifest_digest,
                        "files": list(files),
                        "changed_lines": changed_lines,
                    },
                ),
            )
        return fix_id

    def _finalize(
        self, project: ProjectConfig, fix_id: str, from_state: str
    ) -> ReviewerFixReceipt:
        """Finalize exactly the intent-bound manifest, idempotently.

        The manifest identity, payload, and digest were bound before
        the push; nothing is regenerated here. A legacy schema-36 row
        predates that snapshot, so its manifest is rebuilt
        deterministically from durable row fields alone. An existing
        file must carry exactly the bound digest (a crash between the
        file write and the receipt transition resumes here);
        conflicting content fails closed without overwriting.
        """

        row = self._database.execute(
            "SELECT * FROM reviewer_fixes WHERE fix_id = ?", (fix_id,)
        ).fetchone()
        assert row is not None
        manifest_json = row["manifest_json"]
        digest = row["manifest_digest"]
        try:
            if manifest_json is None or digest is None:
                manifest = self._legacy_manifest(project, row)
                digest = manifest_digest_for(manifest)
            else:
                manifest = manifest_from_document(
                    json.loads(str(manifest_json))
                )
            path = self._manifest_root / f"{manifest.event_id}.json"
            if path.exists():
                read_manifest_snapshot(
                    path,
                    root=self._manifest_root,
                    expected_digest=str(digest),
                )
            else:
                write_manifest(
                    self._manifest_root,
                    manifest,
                    head_sha=manifest.candidate_sha,
                )
        except ManifestError as error:
            raise ReviewerFixRefused(
                f"the intent-bound manifest could not be finalized: {error}"
            ) from error
        self._transition(
            fix_id, from_state, "recorded", event_id=manifest.event_id
        )
        return self._receipt(fix_id)

    def _legacy_manifest(
        self, project: ProjectConfig, row: object
    ) -> CandidateManifest:
        """Deterministically rebuild the manifest for a schema-36 row.

        Legacy intents predate the intent-bound snapshot, so identity
        derives only from durable row fields — the intent's own
        timestamp, issue, final SHA, receipted files, and verification
        — plus the proven integration base. Repeated reconciliation
        therefore always converges on exactly one event; the current
        clock never mints a new identity.
        """

        final_sha = str(row["final_sha"])  # type: ignore[index]
        issue_id = str(row["issue_id"])  # type: ignore[index]
        stamp = datetime.fromisoformat(
            str(row["created_at"])  # type: ignore[index]
        ).astimezone(UTC)
        event_id = row["event_id"] or (  # type: ignore[index]
            "fable_rework_ready-"
            f"{issue_id.lower()}-{final_sha[:12]}-"
            f"{stamp.strftime('%Y%m%dT%H%M%S')}"
        )
        base = self._run(
            project.repo_path,
            "merge-base",
            final_sha,
            f"origin/{project.integration_branch}",
        ).strip()
        return CandidateManifest(
            manifest_version=MANIFEST_VERSION,
            event_id=str(event_id),
            status="FABLE_REWORK_READY",
            candidate_sha=final_sha,
            base_sha=base,
            branch=str(row["branch"]),  # type: ignore[index]
            linear_issues=(issue_id,),
            changed_files=tuple(
                json.loads(str(row["files_json"]))  # type: ignore[index]
            ),
            verification=tuple(
                (str(command), str(outcome))
                for command, outcome in json.loads(
                    str(row["verification_json"])  # type: ignore[index]
                )
            ),
            blockers=(),
            created_at=stamp.isoformat(),
        )

    def _transition(
        self,
        fix_id: str,
        from_state: str,
        to_state: str,
        *,
        event_id: str | None = None,
        reason: str | None = None,
    ) -> None:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_fixes SET state = ?, "
                "event_id = COALESCE(?, event_id), "
                "reason = COALESCE(?, reason), updated_at = ? "
                "WHERE fix_id = ? AND state = ?",
                (to_state, event_id, reason, stamp, fix_id, from_state),
            )
            if cursor.rowcount != 1:
                raise ReviewerFixRefused(
                    f"reviewer fix {fix_id} changed state concurrently"
                )
            self._events.append(
                connection,
                EventInput(
                    event_type=f"reviewer_fix.{to_state}",
                    aggregate_type="reviewer_fix",
                    aggregate_id=fix_id,
                    payload={"reason": reason, "event_id": event_id},
                ),
            )

    def _receipt(self, fix_id: str) -> ReviewerFixReceipt:
        row = self._database.execute(
            "SELECT * FROM reviewer_fixes WHERE fix_id = ?", (fix_id,)
        ).fetchone()
        assert row is not None
        return ReviewerFixReceipt(
            fix_id=str(row["fix_id"]),
            project_key=str(row["project_key"]),
            issue_id=str(row["issue_id"]),
            submitted_sha=str(row["submitted_sha"]),
            final_sha=str(row["final_sha"]),
            event_id=(
                None if row["event_id"] is None else str(row["event_id"])
            ),
            message=str(row["message"]),
            files=tuple(json.loads(str(row["files_json"]))),
            changed_lines=int(row["changed_lines"]),
            verification=tuple(
                (str(cmd), str(state))
                for cmd, state in json.loads(str(row["verification_json"]))
            ),
            state=str(row["state"]),
            reason=None if row["reason"] is None else str(row["reason"]),
        )

    def _eligibility(self, repo: Path) -> tuple[tuple[str, ...], int]:
        """Refuse anything beyond the bounded mechanical budget.

        Runs against the fully staged index, so added, modified,
        deleted, and previously untracked files all contribute their
        complete textual delta. Binary, rename, or malformed entries
        fail closed.
        """

        numstat = self._run(repo, "diff", "--numstat", "--cached", "HEAD")
        files: list[str] = []
        total = 0
        for line in numstat.splitlines():
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) != 3 or not parts[2].strip():
                raise ReviewerFixRefused(
                    f"unparseable diff entry ({line!r}); refusing"
                )
            added, deleted, path = parts
            if "=>" in path:
                raise ReviewerFixRefused(
                    f"renames are never eligible ({path})"
                )
            if added == "-" or deleted == "-":
                raise ReviewerFixRefused(
                    f"binary or generated content is never eligible ({path})"
                )
            files.append(path)
            total += int(added) + int(deleted)
        if not files:
            raise ReviewerFixRefused("the staged fix contains no files")
        if len(files) > MAX_FILES:
            raise ReviewerFixRefused(
                f"{len(files)} files exceed the bounded budget of "
                f"{MAX_FILES}; return this to the Fable lead"
            )
        if total > MAX_CHANGED_LINES:
            raise ReviewerFixRefused(
                f"{total} changed lines exceed the bounded budget of "
                f"{MAX_CHANGED_LINES}; return this to the Fable lead"
            )
        for path in files:
            if _EXCLUDED_PATH_PATTERN.search(path):
                raise ReviewerFixRefused(
                    f"{path} is categorically excluded from reviewer "
                    "fixes; return this to the Fable lead"
                )
        return tuple(files), total

    def _run(self, repo: Path, *args: str) -> str:
        result = self._git.run(("git", *args), repo)
        if result.returncode != 0:
            raise ReviewerFixRefused(
                f"git {args[0]} failed: {result.stderr.strip()[:200]}"
            )
        return result.stdout
