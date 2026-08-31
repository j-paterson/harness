"""Verify the approved-merge state machine and the GitHub intake gate."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.git import GitError
from hermes_orchestrator.github import (
    GitHubError,
    MergeAmbiguous,
    MergeBlocked,
    MergeResult,
    PullRequest,
    PullRequestSummary,
)
from hermes_orchestrator.manifests import MANIFEST_VERSION, CandidateManifest
from hermes_orchestrator.merge import (
    GitHubIntakeGate,
    IntegrationMerge,
    ProvenMerge,
    ReconciliationRequired,
)
from hermes_orchestrator.review_intake import CandidateRejected
from hermes_orchestrator.verdicts import ReviewVerdict

REPOSITORY = "j-paterson/demo"
CANDIDATE = "1" * 40
MERGE_SHA = "3" * 40
BASE = "4" * 40
TREE = "5" * 40
REPO_PATH = Path("/repo/demo")

PROJECTS = {
    "demo": ProjectConfig(
        linear_team="infrastructure",
        repo_path=REPO_PATH,
        integration_branch="main",
        github_repo=REPOSITORY,
    )
}


def approved_verdict(**overrides: Any) -> ReviewVerdict:
    arguments: dict[str, Any] = {
        "verdict": "approved",
        "repository": REPOSITORY,
        "branch": "feature/eng-9",
        "pr_number": 14,
        "reviewed_sha": CANDIDATE,
        "packets": (),
    }
    arguments.update(overrides)
    return ReviewVerdict(**arguments)


def open_pull(**overrides: Any) -> PullRequest:
    arguments: dict[str, Any] = {
        "repository": REPOSITORY,
        "number": 14,
        "state": "open",
        "draft": False,
        "merged": False,
        "mergeable": True,
        "merge_commit_sha": None,
        "head_sha": CANDIDATE,
        "head_ref": "feature/eng-9",
        "head_repository": REPOSITORY,
        "base_ref": "main",
        "base_repository": REPOSITORY,
    }
    arguments.update(overrides)
    return PullRequest(**arguments)


def open_summary(**overrides: Any) -> PullRequestSummary:
    arguments: dict[str, Any] = {
        "repository": REPOSITORY,
        "number": 14,
        "state": "open",
        "draft": False,
        "head_sha": CANDIDATE,
        "head_ref": "feature/eng-9",
        "head_repository": REPOSITORY,
        "base_ref": "main",
        "base_repository": REPOSITORY,
    }
    arguments.update(overrides)
    return PullRequestSummary(**arguments)


@dataclass
class FakeGitHub:
    """Recording stand-in for the deterministic GitHub client."""

    merge_result: MergeResult | None = None
    merge_error: Exception | None = None
    open_pulls: tuple[PullRequestSummary, ...] = ()
    full_pulls: dict[int, PullRequest] = field(default_factory=dict)
    merge_calls: list[dict[str, Any]] = field(default_factory=list)
    list_calls: list[tuple[str, str]] = field(default_factory=list)
    on_list: Any = None
    get_calls: list[tuple[str, int]] = field(default_factory=list)

    def merge(
        self,
        repository: str,
        number: int,
        *,
        expected_head_sha: str,
        expected_head_ref: str,
        expected_base: str,
        effect_id: str,
        merge_method: str = "squash",
    ) -> MergeResult:
        self.merge_calls.append(
            {
                "repository": repository,
                "number": number,
                "expected_head_sha": expected_head_sha,
                "expected_head_ref": expected_head_ref,
                "expected_base": expected_base,
                "effect_id": effect_id,
                "merge_method": merge_method,
            }
        )
        if self.merge_error is not None:
            raise self.merge_error
        assert self.merge_result is not None
        return self.merge_result

    def list_open_pulls(
        self, repository: str, *, base: str
    ) -> tuple[PullRequestSummary, ...]:
        self.list_calls.append((repository, base))
        if self.on_list is not None:
            self.on_list(len(self.list_calls))
        return self.open_pulls

    def get_pull_request(self, repository: str, number: int) -> PullRequest:
        self.get_calls.append((repository, number))
        pull = self.full_pulls.get(number)
        if pull is None:
            raise GitHubError("GitHub pull-request read returned status 404")
        return pull


@dataclass
class FakeGit:
    """Recording ancestry verifier with programmable proof outcomes."""

    ancestor: dict[tuple[str, str], bool] = field(default_factory=dict)
    trees: dict[str, str] = field(default_factory=dict)
    fetch_error: GitError | None = None
    calls: list[tuple[str, ...]] = field(default_factory=list)
    parents: dict[str, str] = field(default_factory=dict)
    paths: dict[tuple[str, str], tuple[str, ...]] = field(default_factory=dict)
    delta_digests: dict[tuple[str, str], str] = field(default_factory=dict)
    first_parent_error: GitError | None = None
    changed_paths_error: GitError | None = None
    delta_digest_error: GitError | None = None

    def fetch(self, repo_path: Path, remote: str, branch: str) -> None:
        self.calls.append(("fetch", str(repo_path), remote, branch))
        if self.fetch_error is not None:
            raise self.fetch_error

    def is_ancestor(self, repo_path: Path, commit: str, ref: str) -> bool:
        self.calls.append(("is_ancestor", str(repo_path), commit, ref))
        return self.ancestor[(commit, ref)]

    def tree_of(self, repo_path: Path, commit: str) -> str:
        self.calls.append(("tree_of", str(repo_path), commit))
        return self.trees[commit]

    def first_parent(self, repo_path: Path, commit: str) -> str:
        self.calls.append(("first_parent", str(repo_path), commit))
        if self.first_parent_error is not None:
            raise self.first_parent_error
        return self.parents[commit]

    def changed_paths(
        self, repo_path: Path, base: str, head: str
    ) -> tuple[str, ...]:
        self.calls.append(("changed_paths", str(repo_path), base, head))
        if self.changed_paths_error is not None:
            raise self.changed_paths_error
        return self.paths[(base, head)]

    def delta_digest(self, repo_path: Path, base: str, head: str) -> str:
        self.calls.append(("delta_digest", str(repo_path), base, head))
        if self.delta_digest_error is not None:
            raise self.delta_digest_error
        return self.delta_digests[(base, head)]


@pytest.fixture
def github() -> FakeGitHub:
    return FakeGitHub(merge_result=MergeResult(MERGE_SHA, already_merged=False))


@pytest.fixture
def git() -> FakeGit:
    return FakeGit(
        ancestor={
            (MERGE_SHA, "origin/main"): True,
            (CANDIDATE, MERGE_SHA): True,
        }
    )


@pytest.fixture
def executor(github: FakeGitHub, git: FakeGit) -> IntegrationMerge:
    return IntegrationMerge(projects=PROJECTS, github=github, git=git)


def test_rejects_unknown_project(executor: IntegrationMerge) -> None:
    with pytest.raises(MergeBlocked, match="project"):
        executor.merge_approved("ghost", approved_verdict(), effect_id="effect-1")


def test_rejects_unapproved_verdict(
    executor: IntegrationMerge, github: FakeGitHub
) -> None:
    verdict = approved_verdict(verdict="corrections_required")
    with pytest.raises(MergeBlocked, match="approved"):
        executor.merge_approved("demo", verdict, effect_id="effect-1")
    assert github.merge_calls == []


def test_rejects_foreign_repository_approval(
    executor: IntegrationMerge, github: FakeGitHub
) -> None:
    verdict = approved_verdict(repository="someone/else")
    with pytest.raises(MergeBlocked, match="repository"):
        executor.merge_approved("demo", verdict, effect_id="effect-1")
    assert github.merge_calls == []


def test_merge_binds_exact_candidate_identity(
    executor: IntegrationMerge, github: FakeGitHub
) -> None:
    executor.merge_approved("demo", approved_verdict(), effect_id="effect-1")
    assert github.merge_calls == [
        {
            "repository": REPOSITORY,
            "number": 14,
            "expected_head_sha": CANDIDATE,
            "expected_head_ref": "feature/eng-9",
            "expected_base": "main",
            "effect_id": "effect-1",
            "merge_method": "squash",
        }
    ]


def test_proven_merge_fetches_before_ancestry(
    executor: IntegrationMerge, git: FakeGit
) -> None:
    outcome = executor.merge_approved("demo", approved_verdict(), effect_id="e-1")
    assert isinstance(outcome, ProvenMerge)
    assert outcome.merge_sha == MERGE_SHA
    assert outcome.candidate_branch == "feature/eng-9"
    assert outcome.relation == "candidate_reachable"
    assert git.calls == [
        ("fetch", str(REPO_PATH), "origin", "main"),
        ("is_ancestor", str(REPO_PATH), MERGE_SHA, "origin/main"),
        ("is_ancestor", str(REPO_PATH), CANDIDATE, MERGE_SHA),
    ]


def test_merge_commit_equal_to_candidate_is_exact(
    github: FakeGitHub, git: FakeGit
) -> None:
    github.merge_result = MergeResult(CANDIDATE, already_merged=False)
    git.ancestor = {(CANDIDATE, "origin/main"): True}
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    outcome = executor.merge_approved("demo", approved_verdict(), effect_id="e-1")
    assert outcome.relation == "merge_commit_is_candidate"


def test_squash_merge_proves_reviewed_tree_equivalence(
    github: FakeGitHub, git: FakeGit
) -> None:
    git.ancestor = {
        (MERGE_SHA, "origin/main"): True,
        (CANDIDATE, MERGE_SHA): False,
    }
    git.trees = {MERGE_SHA: TREE, CANDIDATE: TREE}
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    outcome = executor.merge_approved("demo", approved_verdict(), effect_id="e-1")
    assert outcome.relation == "reviewed_tree_equivalent"


@pytest.mark.parametrize(
    "method,ancestor,trees_equal,expected_relation",
    [
        ("merge", True, False, "candidate_reachable"),
        ("squash", True, False, "candidate_reachable"),
        ("rebase", True, False, "candidate_reachable"),
        ("squash", False, True, "reviewed_tree_equivalent"),
        ("rebase", False, True, "reviewed_tree_equivalent"),
        ("merge", False, True, None),
        ("merge", False, False, None),
        ("squash", False, False, None),
        ("rebase", False, False, None),
    ],
)
def test_proof_relation_is_bound_to_the_merge_method(
    github: FakeGitHub,
    git: FakeGit,
    method: str,
    ancestor: bool,
    trees_equal: bool,
    expected_relation: str | None,
) -> None:
    """Tree equivalence is proof only for squash/rebase, never for merge."""

    git.ancestor = {
        (MERGE_SHA, "origin/main"): True,
        (CANDIDATE, MERGE_SHA): ancestor,
    }
    git.trees = {MERGE_SHA: TREE, CANDIDATE: TREE if trees_equal else "6" * 40}
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    if expected_relation is None:
        with pytest.raises(ReconciliationRequired):
            executor.merge_approved(
                "demo", approved_verdict(), effect_id="e-1", merge_method=method
            )
    else:
        outcome = executor.merge_approved(
            "demo", approved_verdict(), effect_id="e-1", merge_method=method
        )
        assert outcome.relation == expected_relation
    assert github.merge_calls[0]["merge_method"] == method


def test_merge_method_with_equal_tree_never_consults_trees(
    github: FakeGitHub, git: FakeGit
) -> None:
    """The exact review probe: method merge, equal tree, no ancestry."""

    git.ancestor = {
        (MERGE_SHA, "origin/main"): True,
        (CANDIDATE, MERGE_SHA): False,
    }
    git.trees = {MERGE_SHA: TREE, CANDIDATE: TREE}
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    with pytest.raises(ReconciliationRequired, match="ancestry"):
        executor.merge_approved(
            "demo", approved_verdict(), effect_id="e-1", merge_method="merge"
        )
    assert all(call[0] != "tree_of" for call in git.calls)


def test_unrelated_merge_result_requires_reconciliation(
    github: FakeGitHub, git: FakeGit
) -> None:
    git.ancestor = {
        (MERGE_SHA, "origin/main"): True,
        (CANDIDATE, MERGE_SHA): False,
    }
    git.trees = {MERGE_SHA: TREE, CANDIDATE: "6" * 40}
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    with pytest.raises(ReconciliationRequired, match="reviewed candidate"):
        executor.merge_approved("demo", approved_verdict(), effect_id="e-1")


def test_fetch_failure_requires_reconciliation(
    github: FakeGitHub, git: FakeGit
) -> None:
    git.fetch_error = GitError("git fetch failed")
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    with pytest.raises(ReconciliationRequired, match="fetch"):
        executor.merge_approved("demo", approved_verdict(), effect_id="e-1")


def test_ancestry_failure_requires_reconciliation(
    github: FakeGitHub, git: FakeGit
) -> None:
    git.ancestor = {(MERGE_SHA, "origin/main"): False}
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    with pytest.raises(ReconciliationRequired, match="not reachable"):
        executor.merge_approved("demo", approved_verdict(), effect_id="e-1")


def test_pre_merge_block_is_not_reconciliation(
    github: FakeGitHub, git: FakeGit
) -> None:
    """A refusal before the mutation must surface as MergeBlocked, not repair."""

    github.merge_error = MergeBlocked("pull request head changed")
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    with pytest.raises(MergeBlocked, match="head changed"):
        executor.merge_approved("demo", approved_verdict(), effect_id="e-1")
    assert git.calls == []


def test_ambiguous_ownership_requires_reconciliation(
    github: FakeGitHub, git: FakeGit
) -> None:
    """Unprovable merge ownership must never look like a clean block."""

    github.merge_error = MergeAmbiguous(
        "reconciliation required: pending merge intent"
    )
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    with pytest.raises(ReconciliationRequired, match="ambiguous"):
        executor.merge_approved("demo", approved_verdict(), effect_id="e-1")
    assert git.calls == []


PARENT = "7" * 40
CANDIDATE_PATHS = ("A\tsrc/new.py", "M\tsrc/app.py")


def _advanced_base_git(**overrides: Any) -> FakeGit:
    """A FakeGit wired so the three existing relations fail but the fourth,
    patch equivalence against an advanced base, succeeds."""

    arguments: dict[str, Any] = {
        "ancestor": {
            (MERGE_SHA, "origin/main"): True,
            (CANDIDATE, MERGE_SHA): False,
            (BASE, PARENT): True,
        },
        "trees": {MERGE_SHA: TREE, CANDIDATE: "6" * 40},
        "parents": {MERGE_SHA: PARENT},
        "paths": {
            (BASE, CANDIDATE): CANDIDATE_PATHS,
            (PARENT, MERGE_SHA): CANDIDATE_PATHS,
        },
        "delta_digests": {
            (BASE, CANDIDATE): "8" * 40,
            (PARENT, MERGE_SHA): "8" * 40,
        },
    }
    arguments.update(overrides)
    return FakeGit(**arguments)


def test_prove_landed_proves_advanced_base_patch_equivalence(
    github: FakeGitHub,
) -> None:
    git = _advanced_base_git()
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    outcome = executor.prove_landed(
        "demo",
        candidate_sha=CANDIDATE,
        candidate_branch="feature/eng-9",
        pr_number=14,
        merge_sha=MERGE_SHA,
        base_sha=BASE,
    )
    assert outcome.relation == "patch_equivalent"
    assert outcome.base_sha == BASE
    assert outcome.merge_parent_sha == PARENT
    assert outcome.delta_digest == "8" * 40
    assert outcome.changed_paths == CANDIDATE_PATHS


def test_merge_approved_proves_advanced_base_patch_equivalence(
    github: FakeGitHub,
) -> None:
    git = _advanced_base_git()
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    outcome = executor.merge_approved(
        "demo", approved_verdict(), effect_id="e-1", base_sha=BASE
    )
    assert outcome.relation == "patch_equivalent"
    assert outcome.base_sha == BASE
    assert outcome.merge_parent_sha == PARENT
    assert outcome.delta_digest == "8" * 40
    assert outcome.changed_paths == CANDIDATE_PATHS


def test_patch_equivalence_rejects_content_difference(
    github: FakeGitHub,
) -> None:
    """Same changed paths, different patch id: one changed hunk."""

    git = _advanced_base_git(
        delta_digests={
            (BASE, CANDIDATE): "8" * 40,
            (PARENT, MERGE_SHA): "9" * 40,
        }
    )
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    with pytest.raises(ReconciliationRequired, match="content differs"):
        executor.prove_landed(
            "demo",
            candidate_sha=CANDIDATE,
            candidate_branch="feature/eng-9",
            pr_number=14,
            merge_sha=MERGE_SHA,
            base_sha=BASE,
        )


@pytest.mark.parametrize(
    "merge_paths",
    [
        ("A\tsrc/new.py", "A\tsrc/extra.py", "M\tsrc/app.py"),  # added path
        ("M\tsrc/app.py",),  # deleted path
        ("A\tsrc/new.py", "M\tsrc/app.py", "R100\told.py\tnew.py"),  # renamed path
    ],
    ids=["added", "deleted", "renamed"],
)
def test_patch_equivalence_rejects_changed_path_difference(
    github: FakeGitHub, merge_paths: tuple[str, ...]
) -> None:
    git = _advanced_base_git(
        paths={
            (BASE, CANDIDATE): CANDIDATE_PATHS,
            (PARENT, MERGE_SHA): merge_paths,
        }
    )
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    with pytest.raises(ReconciliationRequired, match="changed paths differ"):
        executor.prove_landed(
            "demo",
            candidate_sha=CANDIDATE,
            candidate_branch="feature/eng-9",
            pr_number=14,
            merge_sha=MERGE_SHA,
            base_sha=BASE,
        )


def test_patch_equivalence_rejects_base_not_ancestor_of_parent(
    github: FakeGitHub,
) -> None:
    git = _advanced_base_git(
        ancestor={
            (MERGE_SHA, "origin/main"): True,
            (CANDIDATE, MERGE_SHA): False,
            (BASE, PARENT): False,
        }
    )
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    with pytest.raises(ReconciliationRequired, match="candidate base is not reachable"):
        executor.prove_landed(
            "demo",
            candidate_sha=CANDIDATE,
            candidate_branch="feature/eng-9",
            pr_number=14,
            merge_sha=MERGE_SHA,
            base_sha=BASE,
        )
    assert all(call[0] not in ("changed_paths", "delta_digest") for call in git.calls)


def test_patch_equivalence_not_attempted_without_base_sha(
    github: FakeGitHub,
) -> None:
    """base_sha None preserves today's unrelated-merge behaviour exactly."""

    git = _advanced_base_git()
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    with pytest.raises(ReconciliationRequired, match="unrelated"):
        executor.prove_landed(
            "demo",
            candidate_sha=CANDIDATE,
            candidate_branch="feature/eng-9",
            pr_number=14,
            merge_sha=MERGE_SHA,
        )
    assert all(
        call[0] not in ("first_parent", "changed_paths", "delta_digest")
        for call in git.calls
    )


def test_merge_method_merge_never_tries_patch_equivalence(
    github: FakeGitHub,
) -> None:
    git = _advanced_base_git()
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    with pytest.raises(ReconciliationRequired, match="ancestry"):
        executor.prove_landed(
            "demo",
            candidate_sha=CANDIDATE,
            candidate_branch="feature/eng-9",
            pr_number=14,
            merge_sha=MERGE_SHA,
            merge_method="merge",
            base_sha=BASE,
        )
    assert all(
        call[0] not in ("first_parent", "changed_paths", "delta_digest")
        for call in git.calls
    )


@pytest.mark.parametrize(
    "error_field",
    ["first_parent_error", "changed_paths_error", "delta_digest_error"],
)
def test_git_error_in_patch_equivalence_primitive_requires_reconciliation(
    github: FakeGitHub, error_field: str
) -> None:
    git = _advanced_base_git(**{error_field: GitError("git invocation failed")})
    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    with pytest.raises(ReconciliationRequired, match="merge ancestry proof failed"):
        executor.prove_landed(
            "demo",
            candidate_sha=CANDIDATE,
            candidate_branch="feature/eng-9",
            pr_number=14,
            merge_sha=MERGE_SHA,
            base_sha=BASE,
        )


def test_existing_relations_win_before_patch_equivalence_is_tried(
    github: FakeGitHub, git: FakeGit
) -> None:
    """The three existing proofs pre-empt the fourth even when a base is
    supplied: candidate_reachable here, with no first_parent/patch calls."""

    executor = IntegrationMerge(projects=PROJECTS, github=github, git=git)
    outcome = executor.prove_landed(
        "demo",
        candidate_sha=CANDIDATE,
        candidate_branch="feature/eng-9",
        pr_number=14,
        merge_sha=MERGE_SHA,
        base_sha=BASE,
    )
    assert outcome.relation == "candidate_reachable"
    assert outcome.base_sha is None
    assert all(
        call[0] not in ("first_parent", "changed_paths", "delta_digest")
        for call in git.calls
    )


def candidate_manifest(**overrides: Any) -> CandidateManifest:
    arguments: dict[str, Any] = {
        "manifest_version": MANIFEST_VERSION,
        "event_id": "evt-1",
        "status": "FABLE_READY",
        "candidate_sha": CANDIDATE,
        "base_sha": BASE,
        "branch": "feature/eng-9",
        "linear_issues": ("ENG-9",),
        "changed_files": ("src/app.py",),
        "verification": (("uv run pytest -q", "passed"),),
        "blockers": (),
        "created_at": "2026-08-28T00:00:00+00:00",
    }
    arguments.update(overrides)
    return CandidateManifest(**arguments)


@pytest.fixture
def gate(github: FakeGitHub) -> GitHubIntakeGate:
    return GitHubIntakeGate(projects=PROJECTS, github=github)


def test_gate_admits_single_matching_open_pull(
    gate: GitHubIntakeGate, github: FakeGitHub
) -> None:
    github.open_pulls = (open_summary(),)
    github.full_pulls = {14: open_pull()}
    gate.validate("demo", candidate_manifest())
    assert github.list_calls == [(REPOSITORY, "main")]
    assert github.get_calls == [(REPOSITORY, 14)]
    assert github.merge_calls == []


def test_gate_admits_a_clean_zero_pull_candidate(
    gate: GitHubIntakeGate, github: FakeGitHub
) -> None:
    """INFRA-202: Sol owns the sole PR, so a candidate pushed with no open
    pull request is admissible on the freeze gate's proof alone."""

    github.open_pulls = ()
    gate.validate("demo", candidate_manifest())
    assert github.list_calls == [(REPOSITORY, "main")]
    assert github.get_calls == []
    assert github.merge_calls == []


def test_gate_rejects_second_pull_toward_integration(
    gate: GitHubIntakeGate, github: FakeGitHub
) -> None:
    github.open_pulls = (
        open_summary(),
        open_summary(number=15, head_ref="feature/x"),
    )
    with pytest.raises(CandidateRejected, match="at most one open pull request"):
        gate.validate("demo", candidate_manifest())
    assert github.get_calls == []


def test_gate_rereads_full_pull_before_eligibility(
    gate: GitHubIntakeGate, github: FakeGitHub
) -> None:
    """A matching list summary is never enough: the full PR decides."""

    github.open_pulls = (open_summary(),)
    github.full_pulls = {14: open_pull(head_sha="9" * 40)}
    with pytest.raises(CandidateRejected, match="head"):
        gate.validate("demo", candidate_manifest())
    assert github.get_calls == [(REPOSITORY, 14)]


def test_gate_rejects_full_pull_no_longer_open(
    gate: GitHubIntakeGate, github: FakeGitHub
) -> None:
    github.open_pulls = (open_summary(),)
    github.full_pulls = {
        14: open_pull(state="closed", merged=True, merge_commit_sha=MERGE_SHA)
    }
    with pytest.raises(CandidateRejected, match="open"):
        gate.validate("demo", candidate_manifest())


def test_gate_rejects_stale_candidate_head(
    gate: GitHubIntakeGate, github: FakeGitHub
) -> None:
    github.open_pulls = (open_summary(head_sha="9" * 40),)
    github.full_pulls = {14: open_pull(head_sha="9" * 40)}
    with pytest.raises(CandidateRejected, match="head"):
        gate.validate("demo", candidate_manifest())


def test_gate_rejects_branch_mismatch(
    gate: GitHubIntakeGate, github: FakeGitHub
) -> None:
    github.open_pulls = (open_summary(head_ref="feature/other"),)
    github.full_pulls = {14: open_pull(head_ref="feature/other")}
    with pytest.raises(CandidateRejected, match="branch"):
        gate.validate("demo", candidate_manifest())


def test_gate_rejects_draft_pull(
    gate: GitHubIntakeGate, github: FakeGitHub
) -> None:
    github.open_pulls = (open_summary(),)
    github.full_pulls = {14: open_pull(draft=True)}
    with pytest.raises(CandidateRejected, match="draft"):
        gate.validate("demo", candidate_manifest())


def test_gate_admits_only_cleanly_mergeable_true(
    gate: GitHubIntakeGate, github: FakeGitHub
) -> None:
    github.open_pulls = (open_summary(),)
    github.full_pulls = {14: open_pull(mergeable=True)}
    gate.validate("demo", candidate_manifest())


def test_gate_rejects_unmergeable_full_pull(
    gate: GitHubIntakeGate, github: FakeGitHub
) -> None:
    github.open_pulls = (open_summary(),)
    github.full_pulls = {14: open_pull(mergeable=False)}
    with pytest.raises(CandidateRejected, match="mergeable"):
        gate.validate("demo", candidate_manifest())


def test_gate_rejects_unknown_mergeability(
    gate: GitHubIntakeGate, github: FakeGitHub
) -> None:
    """GitHub still computing mergeability is unknown, not eligible."""

    github.open_pulls = (open_summary(),)
    github.full_pulls = {14: open_pull(mergeable=None)}
    with pytest.raises(CandidateRejected, match="mergeable"):
        gate.validate("demo", candidate_manifest())


def test_gate_rejects_fork_head_repository(
    gate: GitHubIntakeGate, github: FakeGitHub
) -> None:
    github.open_pulls = (open_summary(),)
    github.full_pulls = {14: open_pull(head_repository="fork/demo")}
    with pytest.raises(CandidateRejected, match="repository"):
        gate.validate("demo", candidate_manifest())


def test_gate_fails_closed_when_full_read_fails(
    gate: GitHubIntakeGate, github: FakeGitHub
) -> None:
    github.open_pulls = (open_summary(),)
    github.full_pulls = {}
    with pytest.raises(CandidateRejected, match="pull request"):
        gate.validate("demo", candidate_manifest())


def test_gate_rejects_unknown_project(gate: GitHubIntakeGate) -> None:
    with pytest.raises(CandidateRejected, match="project"):
        gate.validate("ghost", candidate_manifest())
