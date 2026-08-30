"""ACCEPT_WITH_REVIEWER_FIX: staged budgets, owner-leased publication.

Sol correction 2e1ddcce regressions: every locally provable gate runs
before any remote mutation, the push is an exact expected-remote-head
compare-and-push behind a durable publication intent, uncertain
outcomes reconcile deterministically to exactly one receipt, and
untracked files contribute their complete textual delta to the budget.

Sol correction 48ac5268 regressions: exactly one owner may cross or
reconcile the publication boundary — a live lease is never adopted, an
expired ``intended`` row is provably unattempted and aborts retryably,
an attempted push resolves only from exact identity or proven
ancestry, anything unprovable parks as blocking
``reconciliation_required``, and the intent binds the complete
manifest identity before the push so finalization never regenerates it
from mutable state.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hermes_orchestrator.codex_ponytail_guard import GuardError
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.git import GitResult
from hermes_orchestrator.manifests import (
    MANIFEST_VERSION,
    CandidateManifest,
    manifest_digest_for,
    manifest_document,
    read_manifest,
    write_manifest,
)
from hermes_orchestrator.reviewer_fix import (
    ReviewerFixHelper,
    ReviewerFixRefused,
)

SUBMITTED = "5" * 40
FINAL = "6" * 40
FOREIGN = "7" * 40
BASE = "4" * 40
NOW = datetime(2026, 8, 29, 12, tzinfo=UTC)
BRANCH = "feature/eng-9"
LEASE = f"--force-with-lease=refs/heads/{BRANCH}:{SUBMITTED}"

LIVE_LEASE = (NOW + timedelta(seconds=900)).isoformat()
EXPIRED_LEASE = (NOW - timedelta(seconds=1)).isoformat()

# The exact manifest a production intent would bind for the scripted
# fix: identity, payload, and digest are all fixed at intent time.
EVENT_ID = (
    f"fable_rework_ready-eng-9-{FINAL[:12]}-{NOW.strftime('%Y%m%dT%H%M%S')}"
)
BOUND_MANIFEST = CandidateManifest(
    manifest_version=MANIFEST_VERSION,
    event_id=EVENT_ID,
    status="FABLE_REWORK_READY",
    candidate_sha=FINAL,
    base_sha=BASE,
    branch=BRANCH,
    linear_issues=("ENG-9",),
    changed_files=("src/app.py",),
    verification=(("cmd", "passed"),),
    blockers=(),
    created_at=NOW.isoformat(),
)
BOUND_MANIFEST_JSON = json.dumps(
    manifest_document(BOUND_MANIFEST), sort_keys=True
)
BOUND_MANIFEST_DIGEST = manifest_digest_for(BOUND_MANIFEST)


@dataclass
class Clock:
    """A settable clock so tests can expire leases and advance time."""

    value: datetime = NOW

    def __call__(self) -> datetime:
        return self.value


@dataclass
class ScriptedGit:
    """State-aware git fake: staging, committing, and a lease push."""

    numstat_cached: str = "3\t2\tsrc/app.py\n"
    status: str = " M src/app.py\n"
    post_commit_status: str = ""
    branch: str = BRANCH
    committed: bool = False
    remote_head: str = SUBMITTED
    push_exit: int = 0
    push_lands: bool = True
    remote_descends_from_final: bool = False
    ancestry_check_error: bool = False
    fail_fetch_after_push: bool = False
    on_push: Callable[[], None] | None = None
    fail_prefixes: list[tuple[str, ...]] = field(default_factory=list)
    pushes: list[tuple[str, ...]] = field(default_factory=list)
    commits: list[tuple[str, ...]] = field(default_factory=list)
    resets: list[tuple[str, ...]] = field(default_factory=list)

    def run(self, args: tuple[str, ...], cwd: Path) -> GitResult:
        key = args[1:]
        for prefix in self.fail_prefixes:
            if key[: len(prefix)] == prefix:
                return GitResult(returncode=1, stdout="", stderr="boom")
        if key == ("rev-parse", "HEAD"):
            return _ok((FINAL if self.committed else SUBMITTED) + "\n")
        if key == ("status", "--porcelain"):
            return _ok(
                self.post_commit_status if self.committed else self.status
            )
        if key == ("add", "--all"):
            return _ok("")
        if key == ("reset",):
            self.resets.append(key)
            return _ok("")
        if key == ("diff", "--numstat", "--cached", "HEAD"):
            return _ok(self.numstat_cached)
        if key[0] == "commit":
            self.commits.append(key)
            self.committed = True
            return _ok("")
        if key == ("rev-parse", "--abbrev-ref", "HEAD"):
            return _ok(self.branch + "\n")
        if key[0] == "fetch":
            if self.fail_fetch_after_push and self.pushes:
                return GitResult(returncode=1, stdout="", stderr="boom")
            return _ok("")
        if key == ("rev-parse", f"origin/{BRANCH}"):
            return _ok(self.remote_head + "\n")
        if key[:2] == ("merge-base", "--is-ancestor"):
            if self.ancestry_check_error:
                return GitResult(
                    returncode=128, stdout="", stderr="bad object"
                )
            return GitResult(
                returncode=0 if self.remote_descends_from_final else 1,
                stdout="",
                stderr="",
            )
        if key[0] == "merge-base":
            return _ok(BASE + "\n")
        if key[:2] == ("diff", "--name-only"):
            return _ok("src/app.py\n")
        if key[0] == "push":
            if self.on_push is not None:
                self.on_push()
            self.pushes.append(key)
            if self.push_lands:
                self.remote_head = FINAL
            return GitResult(
                returncode=self.push_exit,
                stdout="",
                stderr="" if self.push_exit == 0 else "rejected",
            )
        raise AssertionError(f"unexpected git invocation {key}")


def _ok(stdout: str) -> GitResult:
    return GitResult(returncode=0, stdout=stdout, stderr="")


@dataclass
class ScriptedPonytail:
    """Scripted Ponytail snapshot decision for both mutation boundaries.

    ``snapshots`` and ``reviews`` are consumed one value per call (the
    last value repeats once exhausted), so a test can script the commit
    boundary and the push-boundary revalidation independently. The
    defaults model a review recorded for exactly the current candidate.
    """

    snapshots: list[str] = field(default_factory=lambda: ["snap-a"])
    reviews: list[str | None] = field(default_factory=lambda: ["snap-a"])
    snapshot_calls: int = 0
    review_calls: int = 0

    def snapshot_hash(self, repo: Path) -> str:
        index = min(self.snapshot_calls, len(self.snapshots) - 1)
        self.snapshot_calls += 1
        return self.snapshots[index]

    def reviewed_hash(self, repo: Path) -> str | None:
        index = min(self.review_calls, len(self.reviews) - 1)
        self.review_calls += 1
        return self.reviews[index]


@dataclass
class RecordingCommands:
    exit_code: int = 0
    calls: list[str] = field(default_factory=list)

    def __call__(self, command: str, cwd: Path) -> tuple[int, str]:
        self.calls.append(command)
        return self.exit_code, "output"


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def git() -> ScriptedGit:
    return ScriptedGit()


@pytest.fixture
def commands() -> RecordingCommands:
    return RecordingCommands()


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def ponytail() -> ScriptedPonytail:
    return ScriptedPonytail()


@pytest.fixture
def helper(
    database: Database,
    git: ScriptedGit,
    commands: RecordingCommands,
    clock: Clock,
    ponytail: ScriptedPonytail,
    tmp_path: Path,
) -> ReviewerFixHelper:
    manifest_root = tmp_path / "manifests"
    manifest_root.mkdir()
    return ReviewerFixHelper(
        database=database,
        events=EventStore(database),
        projects={
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=tmp_path / "repo",
                integration_branch="main",
                github_repo="j-paterson/demo",
            )
        },
        git=git,
        manifest_root=manifest_root,
        commands=commands,
        now=clock,
        snapshot_hash=ponytail.snapshot_hash,
        reviewed_hash=ponytail.reviewed_hash,
    )


def apply(helper: ReviewerFixHelper, **overrides: object) -> object:
    arguments: dict[str, object] = {
        "submitted_sha": SUBMITTED,
        "message": "align the retry constant with the contract",
        "verification": ("uv run pytest -q tests/test_app.py",),
    }
    arguments.update(overrides)
    return helper.apply("demo", "ENG-9", **arguments)  # type: ignore[arg-type]


def insert_intent(
    database: Database,
    *,
    state: str,
    lease_expires_at: str,
    fix_id: str = "fix-1",
) -> None:
    """One durable row exactly as a production intent would bind it."""

    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO reviewer_fixes("
            "fix_id, project_key, issue_id, submitted_sha, final_sha, "
            "branch, expected_remote_sha, owner_token, lease_expires_at, "
            "event_id, manifest_json, manifest_digest, message, "
            "files_json, changed_lines, verification_json, state, "
            "created_at, updated_at) VALUES (?, 'demo', 'ENG-9', ?, ?, ?, "
            "?, 'other-owner', ?, ?, ?, ?, 'msg', '[\"src/app.py\"]', 5, "
            "'[[\"cmd\", \"passed\"]]', ?, ?, ?)",
            (
                fix_id,
                SUBMITTED,
                FINAL,
                BRANCH,
                SUBMITTED,
                lease_expires_at,
                EVENT_ID,
                BOUND_MANIFEST_JSON,
                BOUND_MANIFEST_DIGEST,
                state,
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )


def state_of(database: Database, fix_id: str = "fix-1") -> str:
    row = database.execute(
        "SELECT state FROM reviewer_fixes WHERE fix_id = ?", (fix_id,)
    ).fetchone()
    assert row is not None
    return str(row["state"])


class TestStagedEligibility:
    """Every file — untracked included — contributes its full delta."""

    def refuse(
        self, helper: ReviewerFixHelper, git: ScriptedGit, match: str
    ) -> None:
        with pytest.raises(ReviewerFixRefused, match=match):
            apply(helper)
        # Refusal happened before anything irreversible: no commit, no
        # push, and the staged snapshot was unstaged again.
        assert git.commits == []
        assert git.pushes == []
        assert git.resets == [("reset",)]

    def test_an_untracked_31_line_file_is_refused_before_commit(
        self, helper: ReviewerFixHelper, git: ScriptedGit
    ) -> None:
        git.numstat_cached = "31\t0\tsrc/new_helper.py\n"
        self.refuse(helper, git, "changed lines exceed")

    def test_tracked_and_untracked_aggregate_over_budget_refuses(
        self, helper: ReviewerFixHelper, git: ScriptedGit
    ) -> None:
        git.numstat_cached = "20\t0\tsrc/app.py\n15\t0\tsrc/new.py\n"
        self.refuse(helper, git, "changed lines exceed")

    def test_more_than_two_files_refuses(
        self, helper: ReviewerFixHelper, git: ScriptedGit
    ) -> None:
        git.numstat_cached = "1\t0\ta.py\n1\t0\tb.py\n1\t0\tc.py\n"
        self.refuse(helper, git, "files exceed")

    def test_binary_content_refuses(
        self, helper: ReviewerFixHelper, git: ScriptedGit
    ) -> None:
        git.numstat_cached = "-\t-\tassets/logo.png\n"
        self.refuse(helper, git, "binary or generated")

    def test_renames_refuse(
        self, helper: ReviewerFixHelper, git: ScriptedGit
    ) -> None:
        git.numstat_cached = "1\t1\tsrc/{old.py => new.py}\n"
        self.refuse(helper, git, "renames are never eligible")

    @pytest.mark.parametrize(
        "entry",
        ["3\t2\n", "3\t2\t\n", "3\t2\t \n", "3\tsrc/app.py\n"],
    )
    def test_malformed_entries_refuse(
        self, helper: ReviewerFixHelper, git: ScriptedGit, entry: str
    ) -> None:
        git.numstat_cached = entry
        self.refuse(helper, git, "unparseable diff entry")

    @pytest.mark.parametrize(
        "path",
        [
            "src/hermes_orchestrator/migrations/0099_x.sql",
            "schema/accounts.yml",
            "generated/client.py",
        ],
    )
    def test_categorically_excluded_paths_refuse(
        self, helper: ReviewerFixHelper, git: ScriptedGit, path: str
    ) -> None:
        git.numstat_cached = f"1\t1\t{path}\n"
        self.refuse(helper, git, "categorically excluded")

    def test_untracked_full_line_count_lands_in_the_receipt(
        self, helper: ReviewerFixHelper, git: ScriptedGit
    ) -> None:
        git.numstat_cached = "12\t0\tsrc/new file.py\n0\t8\tsrc/gone.py\n"

        receipt = apply(helper)

        assert receipt.changed_lines == 20
        assert receipt.files == ("src/new file.py", "src/gone.py")

    def test_exactly_two_files_and_thirty_lines_stays_eligible(
        self, helper: ReviewerFixHelper, git: ScriptedGit
    ) -> None:
        git.numstat_cached = "10\t5\tsrc/app.py\n10\t5\tsrc/new.py\n"

        receipt = apply(helper)

        assert receipt.state == "recorded"
        assert receipt.changed_lines == 30
        assert receipt.files == ("src/app.py", "src/new.py")

    def test_an_empty_new_file_counts_zero_but_is_receipted(
        self, helper: ReviewerFixHelper, git: ScriptedGit
    ) -> None:
        git.numstat_cached = "0\t0\tsrc/empty.py\n"

        receipt = apply(helper)

        assert receipt.changed_lines == 0
        assert receipt.files == ("src/empty.py",)


def build_helper(
    database: Database,
    git: ScriptedGit,
    tmp_path: Path,
    **seam: object,
) -> ReviewerFixHelper:
    """One helper with default wiring plus explicit seam overrides."""

    manifest_root = tmp_path / "manifests"
    manifest_root.mkdir(exist_ok=True)
    return ReviewerFixHelper(
        database=database,
        events=EventStore(database),
        projects={
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=tmp_path / "repo",
                integration_branch="main",
                github_repo="j-paterson/demo",
            )
        },
        git=git,
        manifest_root=manifest_root,
        commands=RecordingCommands(),
        now=Clock(),
        **seam,  # type: ignore[arg-type]
    )


class TestPonytailBoundaries:
    """Sol packet 165f5ee6/2: the helper runs as an outer command the
    session hook cannot see, so the Ponytail snapshot decision is
    enforced inside the helper itself — the complete staged/worktree
    candidate needs a matching review marker before the labeled commit,
    and the committed publish state is revalidated before the push."""

    def test_an_unreviewed_eligible_fix_is_refused_before_commit(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        ponytail: ScriptedPonytail,
        database: Database,
        tmp_path: Path,
    ) -> None:
        """No recorded review: refused before Git commit, with no push
        and no publication intent of any kind."""

        ponytail.reviews = [None]

        with pytest.raises(
            ReviewerFixRefused, match="commit boundary has no recorded"
        ):
            apply(helper)

        assert git.commits == []
        assert git.pushes == []
        assert git.resets == [("reset",)]
        assert database.scalar("SELECT COUNT(*) FROM reviewer_fixes") == 0
        assert list((tmp_path / "manifests").iterdir()) == []

    def test_recording_the_exact_snapshot_permits_the_labeled_commit(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        ponytail: ScriptedPonytail,
    ) -> None:
        """A review marker matching the exact current candidate
        snapshot authorizes the labeled commit and the publication."""

        receipt = apply(helper)

        assert receipt.state == "recorded"
        [commit] = git.commits
        assert commit[2].startswith("ACCEPT_WITH_REVIEWER_FIX: ")
        [push] = git.pushes
        assert push[1] == LEASE
        # Both mutation boundaries consulted the snapshot decision.
        assert ponytail.snapshot_calls == 2
        assert ponytail.review_calls == 2

    def test_a_changed_snapshot_after_review_prevents_commit(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        ponytail: ScriptedPonytail,
        database: Database,
    ) -> None:
        """The candidate changed after its review was recorded: the
        stale marker no longer authorizes the commit boundary."""

        ponytail.snapshots = ["snap-b"]
        ponytail.reviews = ["snap-a"]

        with pytest.raises(
            ReviewerFixRefused,
            match="commit boundary changed after its ponytail review",
        ):
            apply(helper)

        assert git.commits == []
        assert git.pushes == []
        assert git.resets == [("reset",)]
        assert database.scalar("SELECT COUNT(*) FROM reviewer_fixes") == 0

    @pytest.mark.parametrize("scenario", ["absent", "stale"])
    def test_the_push_boundary_revalidates_and_refuses_publication(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        ponytail: ScriptedPonytail,
        database: Database,
        tmp_path: Path,
        scenario: str,
    ) -> None:
        """The commit boundary passed, but at the push boundary the
        required reviewed state is absent or stale: the labeled commit
        stays local and nothing is journaled or published."""

        if scenario == "absent":
            ponytail.reviews = ["snap-a", None]
        else:
            ponytail.snapshots = ["snap-a", "snap-b"]

        with pytest.raises(ReviewerFixRefused, match="push boundary"):
            apply(helper)

        assert len(git.commits) == 1
        assert git.pushes == []
        assert database.scalar("SELECT COUNT(*) FROM reviewer_fixes") == 0
        assert list((tmp_path / "manifests").iterdir()) == []
        assert ponytail.snapshot_calls == 2
        assert ponytail.review_calls == 2

    def test_unverifiable_review_state_fails_closed_before_commit(
        self,
        database: Database,
        git: ScriptedGit,
        tmp_path: Path,
    ) -> None:
        def broken(repo: Path) -> str:
            raise GuardError("git rev-parse failed: boom")

        helper = build_helper(database, git, tmp_path, snapshot_hash=broken)

        with pytest.raises(
            ReviewerFixRefused, match="could not be verified"
        ):
            apply(helper)

        assert git.commits == []
        assert git.pushes == []
        assert database.scalar("SELECT COUNT(*) FROM reviewer_fixes") == 0

    def test_the_default_seam_reuses_the_real_guard_functions(
        self,
        database: Database,
        git: ScriptedGit,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Without injected callables the helper binds the guard
        module's own snapshot_hash and reviewed_hash."""

        calls: list[str] = []

        def fake_snapshot(repo: Path) -> str:
            calls.append(f"snapshot:{repo.name}")
            return "snap"

        def fake_reviewed(repo: Path) -> str | None:
            calls.append(f"reviewed:{repo.name}")
            return "snap"

        monkeypatch.setattr(
            "hermes_orchestrator.codex_ponytail_guard.snapshot_hash",
            fake_snapshot,
        )
        monkeypatch.setattr(
            "hermes_orchestrator.codex_ponytail_guard.reviewed_hash",
            fake_reviewed,
        )

        receipt = apply(build_helper(database, git, tmp_path))

        assert receipt.state == "recorded"
        assert calls == ["snapshot:repo", "reviewed:repo"] * 2


class TestIntentJournalledPublication:
    """No remote mutation before every local proof and a durable intent."""

    def test_the_happy_path_orders_gate_intent_push_finalize(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        commands: RecordingCommands,
        database: Database,
        tmp_path: Path,
    ) -> None:
        receipt = apply(helper)

        assert receipt.state == "recorded"
        assert receipt.final_sha == FINAL
        assert receipt.event_id is not None
        [push] = git.pushes
        assert push[1] == LEASE
        [commit] = git.commits
        assert commit[2].startswith("ACCEPT_WITH_REVIEWER_FIX: ")
        assert commands.calls == ["uv run pytest -q tests/test_app.py"]
        manifest = read_manifest(
            tmp_path / "manifests" / f"{receipt.event_id}.json",
            root=tmp_path / "manifests",
        )
        assert manifest.candidate_sha == FINAL
        assert manifest.base_sha == BASE
        assert helper.pending_intents("demo") == 0

    def test_a_dirty_tree_after_the_commit_never_pushes(
        self, helper: ReviewerFixHelper, git: ScriptedGit, database: Database
    ) -> None:
        git.post_commit_status = " M src/other.py\n"

        with pytest.raises(ReviewerFixRefused, match="not clean"):
            apply(helper)

        assert git.pushes == []
        assert database.scalar("SELECT COUNT(*) FROM reviewer_fixes") == 0

    @pytest.mark.parametrize(
        "prefix",
        [("fetch", "--", "origin", "main"), ("merge-base",)],
    )
    def test_gate_failures_never_push(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        database: Database,
        prefix: tuple[str, ...],
    ) -> None:
        git.fail_prefixes = [prefix]

        with pytest.raises(ReviewerFixRefused):
            apply(helper)

        assert git.pushes == []
        assert database.scalar("SELECT COUNT(*) FROM reviewer_fixes") == 0

    def test_passing_trivial_commands_cannot_bypass_the_real_gate(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        commands: RecordingCommands,
    ) -> None:
        """The mandatory candidate gate is invoked, not simulated."""

        git.branch = "main"  # the gate refuses the integration branch

        with pytest.raises(ReviewerFixRefused, match="candidate gate refused"):
            apply(helper)

        assert commands.calls  # focused verification passed trivially
        assert git.pushes == []

    def test_failed_verification_publishes_nothing(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        commands: RecordingCommands,
        database: Database,
    ) -> None:
        commands.exit_code = 1

        with pytest.raises(ReviewerFixRefused, match="verification failed"):
            apply(helper)

        assert len(git.commits) == 1
        assert git.pushes == []
        assert database.scalar("SELECT COUNT(*) FROM reviewer_fixes") == 0

    def test_the_attempted_boundary_and_manifest_precede_the_push(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        database: Database,
    ) -> None:
        """At the instant the push begins, the durable row has already
        crossed the attempted boundary and carries the complete bound
        manifest identity — a crash mid-push can never look
        unattempted, and finalization never regenerates identity."""

        seen: list[object] = []
        git.on_push = lambda: seen.append(
            database.execute(
                "SELECT state, event_id, manifest_digest "
                "FROM reviewer_fixes"
            ).fetchone()
        )

        receipt = apply(helper)

        assert receipt.state == "recorded"
        [row] = seen
        assert row["state"] == "attempted"
        assert row["event_id"] == receipt.event_id
        assert row["manifest_digest"] is not None

    def test_a_raced_remote_head_parks_as_reconciliation_required(
        self, helper: ReviewerFixHelper, git: ScriptedGit, database: Database
    ) -> None:
        """A rejected lease against a foreign head proves nothing
        either way: never overwrite, never abort, keep blocking."""

        git.remote_head = FOREIGN
        git.push_exit = 1
        git.push_lands = False

        with pytest.raises(
            ReviewerFixRefused, match="reconciliation_required"
        ):
            apply(helper)

        [push] = git.pushes
        assert push[1] == LEASE
        # The foreign head was never overwritten or appended to.
        assert git.remote_head == FOREIGN
        assert helper.pending_intents("demo") == 1
        assert helper.reconcile("demo") == ()
        with pytest.raises(ReviewerFixRefused, match="no further fix"):
            apply(helper)
        assert len(git.pushes) == 1

    def test_an_ambiguous_push_that_landed_finalizes_exactly_once(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        database: Database,
    ) -> None:
        git.push_exit = 1
        git.push_lands = True

        receipt = apply(helper)

        assert receipt.state == "recorded"
        assert receipt.final_sha == FINAL
        assert (
            database.scalar(
                "SELECT COUNT(*) FROM reviewer_fixes WHERE state = 'recorded'"
            )
            == 1
        )

    def test_an_unchanged_remote_after_a_failed_push_aborts_retryably(
        self, helper: ReviewerFixHelper, git: ScriptedGit
    ) -> None:
        git.push_exit = 1
        git.push_lands = False

        with pytest.raises(ReviewerFixRefused, match="reconciled as aborted"):
            apply(helper)

        assert helper.pending_intents("demo") == 0
        git.committed = False
        git.push_exit = 0
        git.push_lands = True
        fresh = apply(helper)
        assert fresh.state == "recorded"

    def test_an_unreadable_remote_after_an_ambiguous_push_stays_resumable(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        tmp_path: Path,
    ) -> None:
        """Push exit is ambiguous and the remote cannot be read: the
        intent parks as reconciliation_required, blocks every further
        fix, nothing is pushed twice, and a later readable remote
        reconciles to exactly one receipt."""

        git.push_exit = 1
        git.push_lands = True
        git.fail_fetch_after_push = True

        with pytest.raises(ReviewerFixRefused, match="unprovable"):
            apply(helper)

        assert helper.pending_intents("demo") == 1
        with pytest.raises(ReviewerFixRefused, match="fetch failed"):
            apply(helper)
        assert len(git.pushes) == 1

        git.fail_fetch_after_push = False
        [receipt] = helper.reconcile("demo")
        assert receipt.state == "recorded"
        assert receipt.final_sha == FINAL
        assert (
            tmp_path / "manifests" / f"{receipt.event_id}.json"
        ).exists()
        assert helper.reconcile("demo") == ()

    def test_a_finalize_failure_after_a_landed_push_stays_resumable(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        database: Database,
        clock: Clock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The push landed but finalization died: the attempted row
        survives under its live lease, only lease expiry lets a
        resumer adopt it, and reconciliation finalizes the exact bound
        event identity without a second push."""

        def refuse_write(*args: object, **kwargs: object) -> None:
            raise RuntimeError("manifest write failed")

        monkeypatch.setattr(
            "hermes_orchestrator.reviewer_fix.write_manifest", refuse_write
        )

        with pytest.raises(RuntimeError, match="manifest write failed"):
            apply(helper)

        bound = database.execute(
            "SELECT event_id FROM reviewer_fixes"
        ).fetchone()
        assert helper.pending_intents("demo") == 1
        monkeypatch.undo()

        # The owner's lease is still live: nobody may adopt the row.
        assert helper.reconcile("demo") == ()
        assert helper.pending_intents("demo") == 1

        clock.value = NOW + timedelta(seconds=901)
        [receipt] = helper.reconcile("demo")
        assert receipt.state == "recorded"
        assert receipt.event_id == bound["event_id"]
        assert len(git.pushes) == 1
        assert (
            database.scalar(
                "SELECT COUNT(*) FROM reviewer_fixes WHERE state = 'recorded'"
            )
            == 1
        )

    def test_one_live_fix_per_submitted_candidate(
        self, helper: ReviewerFixHelper, git: ScriptedGit
    ) -> None:
        apply(helper)
        git.committed = False

        with pytest.raises(ReviewerFixRefused, match="already carries"):
            apply(helper)


class TestOwnerLease:
    """Exactly one owner may cross or reconcile the boundary."""

    def test_a_live_intended_lease_is_never_adopted(
        self, helper: ReviewerFixHelper, git: ScriptedGit, database: Database
    ) -> None:
        """Process A paused between intent and push: B cannot abort,
        push, or authorize another fix while A owns a live lease."""

        insert_intent(database, state="intended", lease_expires_at=LIVE_LEASE)

        assert helper.reconcile("demo") == ()
        assert state_of(database) == "intended"
        assert helper.pending_intents("demo") == 1
        with pytest.raises(ReviewerFixRefused, match="no further fix"):
            apply(helper)
        assert git.pushes == []

    @pytest.mark.parametrize("remote_readable", [True, False])
    def test_a_live_attempted_lease_is_never_aborted(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        database: Database,
        remote_readable: bool,
    ) -> None:
        """Process A paused mid-push with the remote unchanged or
        unreadable: concurrent reconciliation stays blocked and never
        classifies the live attempt as not landed."""

        insert_intent(
            database, state="attempted", lease_expires_at=LIVE_LEASE
        )
        git.remote_head = SUBMITTED
        if not remote_readable:
            git.fail_prefixes = [("fetch",)]

        assert helper.reconcile("demo") == ()
        assert state_of(database) == "attempted"
        assert helper.pending_intents("demo") == 1
        assert git.pushes == []

    def test_an_expired_unattempted_intent_aborts_exactly_once(
        self, helper: ReviewerFixHelper, git: ScriptedGit, database: Database
    ) -> None:
        """The attempted boundary was never crossed, so after lease
        expiry one resumer aborts retryably — exactly once."""

        insert_intent(
            database, state="intended", lease_expires_at=EXPIRED_LEASE
        )

        [receipt] = helper.reconcile("demo")

        assert receipt.state == "aborted"
        assert "provably unattempted" in str(receipt.reason)
        assert helper.reconcile("demo") == ()
        assert helper.pending_intents("demo") == 0
        fresh = apply(helper)
        assert fresh.state == "recorded"

    def test_an_expired_attempted_push_reconciles_without_pushing(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        database: Database,
        tmp_path: Path,
    ) -> None:
        """After an attempted ambiguous push the lease expires: one
        resumer reconciles from the remote truth and never issues a
        second push."""

        insert_intent(
            database, state="attempted", lease_expires_at=EXPIRED_LEASE
        )
        git.remote_head = FINAL

        [receipt] = helper.reconcile("demo")

        assert receipt.state == "recorded"
        assert receipt.final_sha == FINAL
        assert receipt.event_id == EVENT_ID
        assert (
            tmp_path / "manifests" / f"{receipt.event_id}.json"
        ).exists()
        assert git.pushes == []
        assert helper.reconcile("demo") == ()
        assert (
            database.scalar("SELECT COUNT(*) FROM reviewer_fixes") == 1
        )

    def test_an_expired_intent_with_an_unchanged_remote_aborts(
        self, helper: ReviewerFixHelper, git: ScriptedGit, database: Database
    ) -> None:
        insert_intent(
            database, state="attempted", lease_expires_at=EXPIRED_LEASE
        )
        git.remote_head = SUBMITTED

        [receipt] = helper.reconcile("demo")

        assert receipt.state == "aborted"
        assert "did not land" in str(receipt.reason)
        assert helper.pending_intents("demo") == 0
        fresh = apply(helper)
        assert fresh.state == "recorded"

    def test_a_remote_that_advanced_past_the_fix_proves_it_landed(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        database: Database,
        tmp_path: Path,
    ) -> None:
        """The branch moved on to a descendant of the fix commit:
        ancestry proves the fix landed and the bound manifest is
        finalized exactly once."""

        insert_intent(
            database, state="attempted", lease_expires_at=EXPIRED_LEASE
        )
        git.remote_head = FOREIGN
        git.remote_descends_from_final = True

        [receipt] = helper.reconcile("demo")

        assert receipt.state == "recorded"
        assert receipt.final_sha == FINAL
        assert (
            tmp_path / "manifests" / f"{receipt.event_id}.json"
        ).exists()

    def test_unprovable_ancestry_fails_closed_and_never_aborts(
        self, helper: ReviewerFixHelper, git: ScriptedGit, database: Database
    ) -> None:
        insert_intent(
            database, state="attempted", lease_expires_at=EXPIRED_LEASE
        )
        git.remote_head = FOREIGN
        git.ancestry_check_error = True

        [receipt] = helper.reconcile("demo")

        assert receipt.state == "reconciliation_required"
        assert helper.pending_intents("demo") == 1
        # Still unprovable on the next pass: it stays parked.
        assert helper.reconcile("demo") == ()
        assert state_of(database) == "reconciliation_required"

    def test_an_unreadable_remote_blocks_further_fixes(
        self, helper: ReviewerFixHelper, git: ScriptedGit, database: Database
    ) -> None:
        insert_intent(
            database, state="attempted", lease_expires_at=EXPIRED_LEASE
        )
        git.fail_prefixes = [("fetch", "--", "origin", BRANCH)]

        with pytest.raises(ReviewerFixRefused):
            apply(helper)

        assert git.pushes == []
        assert helper.pending_intents("demo") == 1


class TestIntentBoundManifest:
    """Finalization reuses the immutable intent-bound snapshot."""

    def test_a_crash_between_manifest_write_and_receipt_resumes_idempotently(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        database: Database,
        tmp_path: Path,
    ) -> None:
        """The manifest file was published but the process died before
        the receipt transition: resumption verifies the exact bound
        digest, writes nothing new, and finalizes the same event."""

        insert_intent(
            database, state="attempted", lease_expires_at=EXPIRED_LEASE
        )
        git.remote_head = FINAL
        manifest_root = tmp_path / "manifests"
        write_manifest(manifest_root, BOUND_MANIFEST, head_sha=FINAL)

        [receipt] = helper.reconcile("demo")

        assert receipt.state == "recorded"
        assert receipt.event_id == EVENT_ID
        assert [entry.name for entry in manifest_root.iterdir()] == [
            f"{EVENT_ID}.json"
        ]

    def test_a_later_clock_never_generates_a_new_event_identity(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        database: Database,
        clock: Clock,
        tmp_path: Path,
    ) -> None:
        insert_intent(
            database, state="attempted", lease_expires_at=EXPIRED_LEASE
        )
        git.remote_head = FINAL
        clock.value = NOW + timedelta(days=3)

        [receipt] = helper.reconcile("demo")

        assert receipt.event_id == EVENT_ID
        manifest = read_manifest(
            tmp_path / "manifests" / f"{EVENT_ID}.json",
            root=tmp_path / "manifests",
        )
        assert manifest.created_at == NOW.isoformat()

    def test_mutable_state_after_push_never_reaches_the_manifest(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        database: Database,
        tmp_path: Path,
    ) -> None:
        """Reconciliation must not recompute the base, changed files,
        or verification from the current repository: recomputation is
        forced to fail, and the finalized manifest still carries the
        intent-bound content."""

        insert_intent(
            database, state="attempted", lease_expires_at=EXPIRED_LEASE
        )
        git.remote_head = FINAL
        git.fail_prefixes = [("merge-base",), ("diff", "--name-only")]

        [receipt] = helper.reconcile("demo")

        assert receipt.state == "recorded"
        manifest = read_manifest(
            tmp_path / "manifests" / f"{EVENT_ID}.json",
            root=tmp_path / "manifests",
        )
        assert manifest.base_sha == BASE
        assert manifest.changed_files == ("src/app.py",)
        assert manifest.verification == (("cmd", "passed"),)

    def test_conflicting_precreated_manifest_content_fails_closed(
        self,
        helper: ReviewerFixHelper,
        git: ScriptedGit,
        database: Database,
        tmp_path: Path,
    ) -> None:
        insert_intent(
            database, state="attempted", lease_expires_at=EXPIRED_LEASE
        )
        git.remote_head = FINAL
        path = tmp_path / "manifests" / f"{EVENT_ID}.json"
        path.write_text('{"forged": true}')
        path.chmod(0o444)

        with pytest.raises(
            ReviewerFixRefused, match="could not be finalized"
        ):
            helper.reconcile("demo")

        assert path.read_text() == '{"forged": true}'
        assert [
            entry.name for entry in (tmp_path / "manifests").iterdir()
        ] == [f"{EVENT_ID}.json"]
        assert state_of(database) == "attempted"
        assert helper.pending_intents("demo") == 1


MIGRATIONS = (
    Path(__file__).parent.parent / "src" / "hermes_orchestrator" / "migrations"
)


def build_schema36_database(
    path: Path,
    rows: tuple[tuple[str, str, str | None, str | None], ...],
) -> None:
    """Replay migrations through 0036 and seed reviewer-fix rows.

    ``rows`` entries are ``(fix_id, state, event_id, reason)`` in the
    exact schema-36 shape: no owner token, lease, or bound manifest.
    """

    connection = sqlite3.connect(path, isolation_level=None)
    try:
        for script in sorted(MIGRATIONS.glob("[0-9][0-9][0-9][0-9]_*.sql")):
            if int(script.name.split("_", maxsplit=1)[0]) > 36:
                continue
            connection.executescript(script.read_text(encoding="utf-8"))
        for fix_id, state, event_id, reason in rows:
            connection.execute(
                "INSERT INTO reviewer_fixes("
                "fix_id, project_key, issue_id, submitted_sha, final_sha, "
                "branch, expected_remote_sha, event_id, message, "
                "files_json, changed_lines, verification_json, state, "
                "reason, created_at, updated_at) VALUES (?, 'demo', "
                "'ENG-9', ?, ?, ?, ?, ?, 'msg', '[\"src/app.py\"]', 5, "
                "'[[\"cmd\", \"passed\"]]', ?, ?, ?, ?)",
                (
                    fix_id,
                    SUBMITTED,
                    FINAL,
                    BRANCH,
                    SUBMITTED,
                    event_id,
                    state,
                    reason,
                    NOW.isoformat(),
                    NOW.isoformat(),
                ),
            )
    finally:
        connection.close()


def upgraded_helper(
    tmp_path: Path, git: ScriptedGit
) -> tuple[Database, ReviewerFixHelper]:
    """Open (and thereby upgrade) the legacy database and wire a helper."""

    database = Database.open(tmp_path / "legacy.db")
    manifest_root = tmp_path / "manifests"
    manifest_root.mkdir(exist_ok=True)
    helper = ReviewerFixHelper(
        database=database,
        events=EventStore(database),
        projects={
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=tmp_path / "repo",
                integration_branch="main",
                github_repo="j-paterson/demo",
            )
        },
        git=git,
        manifest_root=manifest_root,
        commands=RecordingCommands(),
        now=Clock(),
    )
    return database, helper


class TestLegacySchema36Upgrade:
    """Sol correction f732f35b: upgrading a schema-36 nonterminal
    intent preserves uncertainty — remote identity and ancestry decide,
    never the absence of the newer attempted-state evidence."""

    def test_a_legacy_intent_whose_push_landed_finalizes(
        self, git: ScriptedGit, tmp_path: Path
    ) -> None:
        build_schema36_database(
            tmp_path / "legacy.db", (("fix-1", "intended", None, None),)
        )
        git.remote_head = FINAL
        database, helper = upgraded_helper(tmp_path, git)
        try:
            # The upgrade itself is conservative: never the stronger
            # 'intended' state, and never a blind abort.
            assert state_of(database) == "reconciliation_required"

            [receipt] = helper.reconcile("demo")

            assert receipt.state == "recorded"
            assert receipt.final_sha == FINAL
            assert receipt.event_id == EVENT_ID
            manifest = read_manifest(
                tmp_path / "manifests" / f"{EVENT_ID}.json",
                root=tmp_path / "manifests",
            )
            assert manifest.candidate_sha == FINAL
            assert manifest.created_at == NOW.isoformat()
            assert git.pushes == []
        finally:
            database.close()

    def test_a_legacy_intent_with_a_descendant_remote_records_landed(
        self, git: ScriptedGit, tmp_path: Path
    ) -> None:
        build_schema36_database(
            tmp_path / "legacy.db", (("fix-1", "intended", None, None),)
        )
        git.remote_head = FOREIGN
        git.remote_descends_from_final = True
        database, helper = upgraded_helper(tmp_path, git)
        try:
            [receipt] = helper.reconcile("demo")

            assert receipt.state == "recorded"
            assert receipt.event_id == EVENT_ID
            assert git.pushes == []
        finally:
            database.close()

    @pytest.mark.parametrize(
        "scenario", ["unrelated", "unchanged", "unreadable"]
    )
    def test_an_unprovable_legacy_intent_stays_blocking(
        self, git: ScriptedGit, tmp_path: Path, scenario: str
    ) -> None:
        """No remote proof either way — an unrelated head, an exactly
        unchanged expected head (the legacy protocol left no evidence
        that a push could not still have occurred), or an unreadable
        remote — keeps the legacy intent blocking, never aborted."""

        build_schema36_database(
            tmp_path / "legacy.db", (("fix-1", "intended", None, None),)
        )
        if scenario == "unrelated":
            git.remote_head = FOREIGN
        elif scenario == "unchanged":
            git.remote_head = SUBMITTED
        else:
            git.fail_prefixes = [("fetch",)]
        database, helper = upgraded_helper(tmp_path, git)
        try:
            if scenario == "unreadable":
                with pytest.raises(ReviewerFixRefused):
                    helper.reconcile("demo")
            else:
                assert helper.reconcile("demo") == ()

            assert state_of(database) == "reconciliation_required"
            assert helper.pending_intents("demo") == 1
            with pytest.raises(ReviewerFixRefused):
                apply(helper)
            assert git.pushes == []
        finally:
            database.close()

    def test_legacy_terminal_rows_survive_upgrade_unchanged(
        self, git: ScriptedGit, tmp_path: Path
    ) -> None:
        build_schema36_database(
            tmp_path / "legacy.db",
            (
                ("fix-r", "recorded", "evt-recorded-1", None),
                ("fix-a", "aborted", None, "push did not land"),
            ),
        )
        database, helper = upgraded_helper(tmp_path, git)
        try:
            assert helper.reconcile("demo") == ()
            assert helper.pending_intents("demo") == 0
            recorded = database.execute(
                "SELECT state, event_id FROM reviewer_fixes "
                "WHERE fix_id = 'fix-r'"
            ).fetchone()
            assert recorded["state"] == "recorded"
            assert recorded["event_id"] == "evt-recorded-1"
            aborted = database.execute(
                "SELECT state, reason FROM reviewer_fixes "
                "WHERE fix_id = 'fix-a'"
            ).fetchone()
            assert aborted["state"] == "aborted"
            assert aborted["reason"] == "push did not land"
        finally:
            database.close()

    def test_reopening_the_upgraded_database_stays_idempotent(
        self, git: ScriptedGit, tmp_path: Path
    ) -> None:
        build_schema36_database(
            tmp_path / "legacy.db", (("fix-1", "intended", None, None),)
        )
        git.remote_head = FINAL
        database, helper = upgraded_helper(tmp_path, git)
        try:
            [receipt] = helper.reconcile("demo")
            assert receipt.state == "recorded"
        finally:
            database.close()

        reopened, resumed = upgraded_helper(tmp_path, git)
        try:
            assert reopened.schema_version() >= 38
            assert resumed.reconcile("demo") == ()
            assert [
                entry.name
                for entry in (tmp_path / "manifests").iterdir()
            ] == [f"{EVENT_ID}.json"]
            assert git.pushes == []
            assert (
                reopened.scalar(
                    "SELECT COUNT(*) FROM reviewer_fixes "
                    "WHERE state = 'recorded'"
                )
                == 1
            )
            # The recorded legacy fix still owns its submitted
            # candidate: no second fix is authorized for it.
            git.status = " M src/app.py\n"
            with pytest.raises(ReviewerFixRefused, match="already carries"):
                apply(resumed)
        finally:
            reopened.close()
