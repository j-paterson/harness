"""Approved-merge state machine and the GitHub candidate intake gate.

INFRA-164: the only path from an approved exact-SHA verdict to a Linear Done
projection runs through :class:`IntegrationMerge`. It validates the approval
binding against the project configuration, performs the deterministic GitHub
merge, then fetches the integration branch and proves the merge result is
reachable there and carries the reviewed work before returning a
:class:`ProvenMerge` — the only token that permits projecting Done. CircleCI
is never consulted here; CI reconciliation happens at later intake
boundaries (INFRA-165).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

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
from hermes_orchestrator.manifests import CandidateManifest
from hermes_orchestrator.review_intake import CandidateRejected
from hermes_orchestrator.verdicts import ReviewVerdict


class ReconciliationRequired(RuntimeError):
    """The merge mutation may exist but its proof failed; keep Review open.

    Raised only after the merge call: fetch failure, unreachable merge
    commit, or a merge result unrelated to the reviewed candidate. The issue
    must stay in Review and an operator must reconcile before any retry.
    """


class MergeClient(Protocol):
    """The deterministic GitHub surface the state machine depends on."""

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
    ) -> MergeResult: ...

    def list_open_pulls(
        self, repository: str, *, base: str
    ) -> tuple[PullRequestSummary, ...]: ...

    def get_pull_request(self, repository: str, number: int) -> PullRequest: ...


class AncestryVerifier(Protocol):
    """The local Git evidence surface used for post-merge proofs."""

    def fetch(self, repo_path: Path, remote: str, branch: str) -> None: ...

    def is_ancestor(self, repo_path: Path, commit: str, ref: str) -> bool: ...

    def tree_of(self, repo_path: Path, commit: str) -> str: ...


@dataclass(frozen=True, slots=True)
class ProvenMerge:
    """A merge whose result is proven reachable from the integration branch.

    ``relation`` records how the reviewed candidate relates to the merge
    commit: ``merge_commit_is_candidate`` (fast-forward), ``candidate_reachable``
    (merge commit), or ``reviewed_tree_equivalent`` (squash or rebase whose
    final tree is byte-identical to the reviewed tree). This object is the
    only permit for a Linear Done projection.
    """

    project_key: str
    repository: str
    pr_number: int
    candidate_sha: str
    candidate_branch: str
    merge_sha: str
    integration_branch: str
    relation: str


class IntegrationMerge:
    """Merge exactly one approved candidate and prove the result landed."""

    def __init__(
        self,
        *,
        projects: Mapping[str, ProjectConfig],
        github: MergeClient,
        git: AncestryVerifier,
    ) -> None:
        self._projects = projects
        self._github = github
        self._git = git

    def merge_approved(
        self,
        project_key: str,
        verdict: ReviewVerdict,
        *,
        effect_id: str,
        merge_method: str = "squash",
    ) -> ProvenMerge:
        """Merge the approved exact-SHA verdict and prove integration.

        Every precondition failure raises :class:`MergeBlocked` before any
        mutation; every post-merge proof failure raises
        :class:`ReconciliationRequired` and never returns a Done permit.
        """

        project = self._projects.get(project_key)
        if project is None:
            raise MergeBlocked(f"unknown project {project_key!r}")
        if verdict.verdict != "approved" or verdict.packets:
            raise MergeBlocked("merge requires an approved verdict")
        if verdict.repository != project.github_repo:
            raise MergeBlocked(
                "approval repository does not match the project repository"
            )
        try:
            result = self._github.merge(
                project.github_repo,
                verdict.pr_number,
                expected_head_sha=verdict.reviewed_sha,
                expected_head_ref=verdict.branch,
                expected_base=project.integration_branch,
                effect_id=effect_id,
                merge_method=merge_method,
            )
        except MergeAmbiguous as error:
            raise ReconciliationRequired(
                f"merge ownership is ambiguous: {error}"
            ) from error
        relation = self._prove(
            project, verdict.reviewed_sha, result.merge_sha, merge_method
        )
        return ProvenMerge(
            project_key=project_key,
            repository=project.github_repo,
            pr_number=verdict.pr_number,
            candidate_sha=verdict.reviewed_sha,
            candidate_branch=verdict.branch,
            merge_sha=result.merge_sha,
            integration_branch=project.integration_branch,
            relation=relation,
        )

    def prove_landed(
        self,
        project_key: str,
        *,
        candidate_sha: str,
        candidate_branch: str,
        pr_number: int,
        merge_sha: str,
        merge_method: str = "squash",
    ) -> ProvenMerge:
        """Prove an externally performed merge landed the reviewed work.

        INFRA-194 reconciliation: no mutation happens here — the same
        ancestry and tree proofs that gate a guarded merge run against
        the already-existing merge commit, and only a proof yields the
        ProvenMerge permit. Any failure raises
        :class:`ReconciliationRequired` and nothing may be
        reconstructed from it.
        """

        project = self._projects.get(project_key)
        if project is None:
            raise MergeBlocked(f"unknown project {project_key!r}")
        relation = self._prove(project, candidate_sha, merge_sha, merge_method)
        return ProvenMerge(
            project_key=project_key,
            repository=project.github_repo,
            pr_number=pr_number,
            candidate_sha=candidate_sha,
            candidate_branch=candidate_branch,
            merge_sha=merge_sha,
            integration_branch=project.integration_branch,
            relation=relation,
        )

    def _prove(
        self,
        project: ProjectConfig,
        candidate_sha: str,
        merge_sha: str,
        merge_method: str,
    ) -> str:
        """Prove the merge result carries the reviewed work, per method.

        A merge-method merge preserves commit identity, so only the exact
        candidate or candidate ancestry is proof; tree equivalence is
        accepted only for squash and rebase, whose merge commit is a new
        identity carrying the reviewed tree.
        """

        integration_ref = f"origin/{project.integration_branch}"
        try:
            self._git.fetch(project.repo_path, "origin", project.integration_branch)
        except GitError as error:
            raise ReconciliationRequired(
                f"integration fetch failed after merge: {error}"
            ) from error
        try:
            if not self._git.is_ancestor(project.repo_path, merge_sha, integration_ref):
                raise ReconciliationRequired(
                    "merge commit is not reachable from the integration branch"
                )
            if merge_sha == candidate_sha:
                return "merge_commit_is_candidate"
            if self._git.is_ancestor(project.repo_path, candidate_sha, merge_sha):
                return "candidate_reachable"
            if merge_method == "merge":
                raise ReconciliationRequired(
                    "merge-method merge requires candidate ancestry: tree "
                    "equivalence is not accepted as proof"
                )
            if self._git.tree_of(project.repo_path, merge_sha) == self._git.tree_of(
                project.repo_path, candidate_sha
            ):
                return "reviewed_tree_equivalent"
        except GitError as error:
            raise ReconciliationRequired(
                f"merge ancestry proof failed: {error}"
            ) from error
        raise ReconciliationRequired(
            "merge result is unrelated to the reviewed candidate"
        )


class GitHubIntakeGate:
    """INFRA-164 implementation of the review-intake validation port.

    Runs after local candidate validation and before the admission
    compare-and-swap. It is strictly read-only: it proves the candidate is
    the head of at most one open pull request toward the integration branch
    — the one-PR-at-a-time invariant — and fails closed by raising
    :class:`CandidateRejected` on any mismatch. A clean pushed candidate
    with zero open pull requests is admitted without further checks: the
    freeze gate already proved the pushed head, branch, and base, and the
    Merger (Sol) creates and owns the sole pull request (INFRA-202).
    """

    def __init__(
        self,
        *,
        projects: Mapping[str, ProjectConfig],
        github: MergeClient,
    ) -> None:
        self._projects = projects
        self._github = github

    def validate(self, project_key: str, candidate: CandidateManifest) -> None:
        project = self._projects.get(project_key)
        if project is None:
            raise CandidateRejected(f"unknown project {project_key!r}")
        summaries = self._github.list_open_pulls(
            project.github_repo, base=project.integration_branch
        )
        if len(summaries) == 0:
            return
        if len(summaries) > 1:
            raise CandidateRejected(
                "at most one open pull request toward the integration "
                f"branch is permitted, found {len(summaries)}"
            )
        # The list schema carries no merge-decision fields; eligibility is
        # always decided on a fresh full read of the one listed pull request.
        try:
            pull = self._github.get_pull_request(
                project.github_repo, summaries[0].number
            )
        except GitHubError as error:
            raise CandidateRejected(
                f"could not read the open pull request: {error}"
            ) from error
        if pull.state != "open" or pull.merged:
            raise CandidateRejected("the pull request is no longer open")
        if pull.draft:
            raise CandidateRejected("the open pull request is a draft")
        if pull.head_ref != candidate.branch:
            raise CandidateRejected(
                "the open pull request branch does not match the candidate"
            )
        if pull.head_sha != candidate.candidate_sha:
            raise CandidateRejected(
                "the open pull request head is not the candidate SHA"
            )
        if pull.base_ref != project.integration_branch:
            raise CandidateRejected(
                "the open pull request base is not the integration branch"
            )
        if pull.mergeable is not True:
            raise CandidateRejected(
                "the open pull request is not proven cleanly mergeable"
            )
        if (
            pull.head_repository != project.github_repo
            or pull.base_repository != project.github_repo
        ):
            raise CandidateRejected(
                "the open pull request repository does not match the project"
            )
