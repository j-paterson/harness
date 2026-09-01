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
from hermes_orchestrator.git import AmbiguousHunkError, GitError
from hermes_orchestrator.github import (
    DiscoveredPull,
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

    def discover_pull_request(
        self, repository: str, *, branch: str, head_sha: str
    ) -> DiscoveredPull | None: ...


class AncestryVerifier(Protocol):
    """The local Git evidence surface used for post-merge proofs."""

    def fetch(self, repo_path: Path, remote: str, branch: str) -> None: ...

    def is_ancestor(self, repo_path: Path, commit: str, ref: str) -> bool: ...

    def tree_of(self, repo_path: Path, commit: str) -> str: ...

    def first_parent(self, repo_path: Path, commit: str) -> str: ...

    def changed_paths(
        self, repo_path: Path, base: str, head: str
    ) -> tuple[str, ...]: ...

    def apply_to_tree(
        self, repo_path: Path, base: str, head: str, parent: str
    ) -> str: ...


@dataclass(frozen=True, slots=True)
class ProvenMerge:
    """A merge whose result is proven reachable from the integration branch.

    ``relation`` records how the reviewed candidate relates to the merge
    commit: ``merge_commit_is_candidate`` (fast-forward), ``candidate_reachable``
    (merge commit), ``reviewed_tree_equivalent`` (squash or rebase whose
    final tree is byte-identical to the reviewed tree), or
    ``patch_equivalent`` (squash or rebase onto an advanced base, proven by
    an exact positional/preimage application proof: every hunk of the
    recorded candidate's exact reviewed patch is first proven to land at
    its mapped reviewed target on the merge's first parent -- shifted only
    by unrelated edits strictly above it, with any target drift, ambiguity,
    fuzz, or conflict failing closed -- and only then is the patch applied,
    in a throwaway index, reproducing the merge commit's own tree
    byte-for-byte; never a digest comparison, and never just "the context
    matched somewhere"), or ``exact_binding_ambiguous_patch`` (INFRA-198,
    external reconciliation only: the same fourth-relation reconstruction
    hit the one narrow ``AmbiguousHunkError`` class -- a duplicated block
    makes the positional proof inconclusive -- after the caller had
    already independently proven the exact GitHub PR binding: reviewed
    head SHA equal to the candidate SHA, the pull request merged, and the
    merge commit reachable from the integration branch; that binding
    stands on its own as proof for a squash/rebase merge, and the failed
    hunk reconstruction is recorded honestly as corroboration that could
    not complete, never silently upgraded to ``patch_equivalent``).
    This object is the only permit for a Linear Done projection.
    """

    project_key: str
    repository: str
    pr_number: int
    candidate_sha: str
    candidate_branch: str
    merge_sha: str
    integration_branch: str
    relation: str
    base_sha: str | None = None
    merge_parent_sha: str | None = None
    applied_tree_sha: str | None = None
    merge_tree_sha: str | None = None
    changed_paths: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _Proof:
    """Internal outcome of :meth:`IntegrationMerge._prove`.

    Carries the relation plus whatever facts the fourth relation
    (``patch_equivalent``) binds; the first three relations leave the
    bound-fact fields at their defaults.
    """

    relation: str
    base_sha: str | None = None
    merge_parent_sha: str | None = None
    applied_tree_sha: str | None = None
    merge_tree_sha: str | None = None
    changed_paths: tuple[str, ...] = ()


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
        base_sha: str | None = None,
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
        proof = self._prove(
            project,
            verdict.reviewed_sha,
            result.merge_sha,
            merge_method,
            base_sha=base_sha,
        )
        return ProvenMerge(
            project_key=project_key,
            repository=project.github_repo,
            pr_number=verdict.pr_number,
            candidate_sha=verdict.reviewed_sha,
            candidate_branch=verdict.branch,
            merge_sha=result.merge_sha,
            integration_branch=project.integration_branch,
            relation=proof.relation,
            base_sha=proof.base_sha,
            merge_parent_sha=proof.merge_parent_sha,
            applied_tree_sha=proof.applied_tree_sha,
            merge_tree_sha=proof.merge_tree_sha,
            changed_paths=proof.changed_paths,
        )

    def is_ancestor_commit(
        self, project_key: str, *, ancestor: str, descendant: str
    ) -> bool:
        """True iff ``ancestor`` is reachable from ``descendant``.

        INFRA-218 (S2 plumbing): the one narrow read-only ancestry
        question wake supersession needs, answered by the SAME
        ``AncestryVerifier`` port and project checkout this class
        already proves merges with (see ``prove_landed``'s reachability
        checks). Read-only: no merge, no mutation, no new git surface.
        An unknown project fails closed as ``False``; a git failure
        raises ``GitError`` for the caller to fail closed on.
        """

        project = self._projects.get(project_key)
        if project is None:
            return False
        return self._git.is_ancestor(project.repo_path, ancestor, descendant)

    def prove_landed(
        self,
        project_key: str,
        *,
        candidate_sha: str,
        candidate_branch: str,
        pr_number: int,
        merge_sha: str,
        merge_method: str = "squash",
        base_sha: str | None = None,
        exact_pr_binding: bool = True,
    ) -> ProvenMerge:
        """Prove an externally performed merge landed the reviewed work.

        INFRA-194 reconciliation: no mutation happens here — the same
        ancestry and tree proofs that gate a guarded merge run against
        the already-existing merge commit, and only a proof yields the
        ProvenMerge permit. Any failure raises
        :class:`ReconciliationRequired` and nothing may be
        reconstructed from it.

        Callers normally prove the exact GitHub PR binding before reaching
        here, so the narrow ambiguous-duplicate-hunk fallback is enabled by
        default. A caller proving a submitted SHA behind a later PR head
        passes ``exact_pr_binding=False``; ambiguity then fails closed.
        """

        project = self._projects.get(project_key)
        if project is None:
            raise MergeBlocked(f"unknown project {project_key!r}")
        proof = self._prove(
            project,
            candidate_sha,
            merge_sha,
            merge_method,
            base_sha=base_sha,
            trust_exact_binding=exact_pr_binding,
        )
        return ProvenMerge(
            project_key=project_key,
            repository=project.github_repo,
            pr_number=pr_number,
            candidate_sha=candidate_sha,
            candidate_branch=candidate_branch,
            merge_sha=merge_sha,
            integration_branch=project.integration_branch,
            relation=proof.relation,
            base_sha=proof.base_sha,
            merge_parent_sha=proof.merge_parent_sha,
            applied_tree_sha=proof.applied_tree_sha,
            merge_tree_sha=proof.merge_tree_sha,
            changed_paths=proof.changed_paths,
        )

    def _prove(
        self,
        project: ProjectConfig,
        candidate_sha: str,
        merge_sha: str,
        merge_method: str,
        *,
        base_sha: str | None = None,
        trust_exact_binding: bool = False,
    ) -> _Proof:
        """Prove the merge result carries the reviewed work, per method.

        A merge-method merge preserves commit identity, so only the exact
        candidate or candidate ancestry is proof; tree equivalence is
        accepted only for squash and rebase, whose merge commit is a new
        identity carrying the reviewed tree. When those three relations all
        fail, merge_method is squash or rebase, and the recorded candidate
        base is supplied, a fourth relation — patch equivalence against an
        advanced base — is tried before giving up. That fourth relation is
        never a digest comparison and never just "the reviewed context
        matched somewhere": it is an exact positional/preimage application
        proof. Every reviewed hunk must first be proven to land at its
        mapped reviewed target on the merge's first parent -- its mapped
        position shifted only by unrelated edits strictly above it, with a
        changed target, an ambiguous or relocated preimage, apply fuzz, or
        an unexplained applied offset all failing closed -- before the
        exact reviewed patch is applied, in a throwaway index that never
        touches the user's worktree or real index, onto the merge's first
        parent; accepted only when the resulting tree is byte-identical to
        the merge commit's own tree.

        ``trust_exact_binding`` (INFRA-198) is set only by callers that
        have already independently proven the exact GitHub PR binding
        (reviewed head SHA == candidate SHA, merged, base matched, merge
        commit present) before this method ever runs -- it never applies
        to the guarded live-merge gate in ``merge_approved``. Even then it
        changes nothing about the ancestry proof above (a merge commit
        that is not reachable from the integration branch, or a candidate
        base that is not reachable from the merge parent, still refuses
        unconditionally): it only permits the fourth relation to accept
        the merge on the exact binding alone when the hunk reconstruction
        fails with exactly the narrow :class:`AmbiguousHunkError` class --
        a duplicated block making the positional proof inconclusive, not
        a proven mismatch. A genuine reconstruction failure (a changed
        target, fuzz, an unexplained offset, or the applied tree not
        matching the merge tree) still refuses closed regardless.
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
                return _Proof(relation="merge_commit_is_candidate")
            if self._git.is_ancestor(project.repo_path, candidate_sha, merge_sha):
                return _Proof(relation="candidate_reachable")
            if merge_method == "merge":
                raise ReconciliationRequired(
                    "merge-method merge requires candidate ancestry: tree "
                    "equivalence is not accepted as proof"
                )
            if self._git.tree_of(project.repo_path, merge_sha) == self._git.tree_of(
                project.repo_path, candidate_sha
            ):
                return _Proof(relation="reviewed_tree_equivalent")
            if merge_method in ("squash", "rebase") and base_sha is not None:
                parent = self._git.first_parent(project.repo_path, merge_sha)
                if not self._git.is_ancestor(project.repo_path, base_sha, parent):
                    raise ReconciliationRequired(
                        "candidate base is not reachable from the merge "
                        "commit's first parent"
                    )
                candidate_paths = self._git.changed_paths(
                    project.repo_path, base_sha, candidate_sha
                )
                merge_paths = self._git.changed_paths(
                    project.repo_path, parent, merge_sha
                )
                if not candidate_paths or candidate_paths != merge_paths:
                    raise ReconciliationRequired(
                        "merge changed paths differ from the reviewed candidate"
                    )
                try:
                    applied = self._git.apply_to_tree(
                        project.repo_path, base_sha, candidate_sha, parent
                    )
                except AmbiguousHunkError:
                    if not trust_exact_binding:
                        raise
                    # The exact GitHub PR binding is already durably
                    # established by the caller and by the ancestry proof
                    # above; the hunk reconstruction is corroboration, not
                    # a gate, so an inconclusive (not mismatched) hunk
                    # proof does not park this in reconciliation_required.
                    return _Proof(
                        relation="exact_binding_ambiguous_patch",
                        base_sha=base_sha,
                        merge_parent_sha=parent,
                        merge_tree_sha=self._git.tree_of(
                            project.repo_path, merge_sha
                        ),
                        changed_paths=merge_paths,
                    )
                merge_tree = self._git.tree_of(project.repo_path, merge_sha)
                if applied != merge_tree:
                    raise ReconciliationRequired(
                        "applying the reviewed candidate delta to the merge "
                        "parent does not reproduce the merge tree"
                    )
                return _Proof(
                    relation="patch_equivalent",
                    base_sha=base_sha,
                    merge_parent_sha=parent,
                    applied_tree_sha=applied,
                    merge_tree_sha=merge_tree,
                    changed_paths=merge_paths,
                )
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
