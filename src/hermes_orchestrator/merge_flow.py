"""Compose the live Codex merge flow from real adapters (INFRA-166).

Credentials are read from the profile-isolated macOS Keychain only when the
flow is built; nothing here starts a process or performs a network call.
The Codex App Server client is returned unstarted so the caller controls
its lifecycle. CircleCI is consulted only inside ``CiWindow`` at intake
boundaries; there is no polling loop anywhere in this graph.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from hermes_orchestrator.ci_window import CircleCiIntakeGate, CiWindow
from hermes_orchestrator.circleci import (
    CircleCiClient,
    CircleCiStatusAdapter,
    HttpxCircleCiTransport,
)
from hermes_orchestrator.codex_merger import CodexMerger
from hermes_orchestrator.codex_queue import CodexQueueDelivery
from hermes_orchestrator.codex_rpc import CodexRpcClient, app_server_command
from hermes_orchestrator.config import Settings
from hermes_orchestrator.db import Database
from hermes_orchestrator.emission import CandidateEmitter
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.git import GitRunner, GitVerifier, SubprocessGitRunner
from hermes_orchestrator.github import (
    GitHubClient,
    HttpxGitHubTransport,
    MergeEffectJournal,
)
from hermes_orchestrator.lead_outbox import LeadCorrectionOutbox
from hermes_orchestrator.merge import GitHubIntakeGate, IntegrationMerge
from hermes_orchestrator.merger_turns import CodexThreadReports, MergerTurnService
from hermes_orchestrator.qa import QaRouter
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.review_intake import (
    CandidateAdmission,
    CompositeIntakeGate,
)
from hermes_orchestrator.reviews import LinearProjector, ReviewService

GITHUB_KEYCHAIN_SERVICE = "hermes-orchestrator-github"
CIRCLECI_KEYCHAIN_SERVICE = "hermes-orchestrator-circleci"


_PACKAGE_PROMPTS = Path(__file__).resolve().parents[2] / "prompts"


def merger_contract_path(
    repo_root: Path, *, package_prompts: Path | None = None
) -> Path:
    """Locate the immutable Merger contract, failing closed when absent.

    The contract is versioned with the orchestrator code: the configuration
    root is consulted first, then the source tree of the running package,
    so a configuration checkout that predates the contract never blocks a
    live flow and a missing contract is never silently substituted.
    """

    candidates = (
        repo_root / "prompts" / "codex-merger.md",
        (package_prompts or _PACKAGE_PROMPTS) / "codex-merger.md",
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise ValueError("live merge flow requires prompts/codex-merger.md")


class KeychainReader(Protocol):
    def read(self, service: str, account: str) -> str: ...


@dataclass(slots=True)
class MergeFlow:
    """The composed live merge graph for every configured project."""

    rpc: CodexRpcClient
    merger: CodexMerger
    delivery: CodexQueueDelivery
    emitter: CandidateEmitter
    admission: CandidateAdmission
    window: CiWindow
    reviews: ReviewService
    turns: MergerTurnService
    outbox: LeadCorrectionOutbox
    qa: QaRouter
    manifest_root: Path


def build_merge_flow(
    settings: Settings,
    *,
    database: Database,
    events: EventStore,
    queue: QueueService,
    linear: LinearProjector,
    keychain: KeychainReader,
    base_env: Mapping[str, str],
    git_runner: GitRunner | None = None,
    queue_process_factory: Callable[..., Any] | None = None,
) -> MergeFlow:
    """Wire the production merge flow; fails closed on missing inputs."""

    prompt_path = merger_contract_path(settings.repo_root)
    manifest_root = settings.state_dir / "manifests"
    manifest_root.mkdir(parents=True, exist_ok=True)
    runner = git_runner if git_runner is not None else SubprocessGitRunner()

    github_token = keychain.read(GITHUB_KEYCHAIN_SERVICE, "default")
    circleci_token = keychain.read(CIRCLECI_KEYCHAIN_SERVICE, "default")
    github = GitHubClient(
        transport=HttpxGitHubTransport(github_token),
        journal=MergeEffectJournal(database),
    )
    circleci = CircleCiStatusAdapter(
        CircleCiClient(transport=HttpxCircleCiTransport(circleci_token))
    )

    rpc = CodexRpcClient(app_server_command(), base_env=base_env)
    merger = CodexMerger(
        rpc=rpc,
        database=database,
        projects=settings.projects,
        prompt_file=prompt_path,
    )
    delivery_kwargs: dict[str, Any] = {}
    if queue_process_factory is not None:
        delivery_kwargs["process_factory"] = queue_process_factory
    delivery = CodexQueueDelivery(
        channels=merger, manifest_root=manifest_root, **delivery_kwargs
    )
    emitter = CandidateEmitter(
        projects=settings.projects,
        git=runner,
        manifest_root=manifest_root,
        delivery=delivery,
    )
    window = CiWindow(
        database=database,
        status=circleci,
        max_unresolved=settings.policy.max_unresolved_ci_merges,
    )
    outbox = LeadCorrectionOutbox(
        database=database,
        events=events,
        project_for_issue=lambda issue_id: queue.get(issue_id).project_key,
    )
    admission = CandidateAdmission(
        channels=merger,
        manifest_root=manifest_root,
        branch_head=_branch_head(settings, github),
        base_policy=_base_policy(settings, GitVerifier(runner=runner)),
        intake_gate=CompositeIntakeGate(
            (
                CircleCiIntakeGate(window, corrections=outbox),
                GitHubIntakeGate(projects=settings.projects, github=github),
            )
        ),
    )
    qa = QaRouter(database=database, events=events)
    reviews = ReviewService(
        database=database,
        events=events,
        projects=settings.projects,
        queue=queue,
        github=github,
        merge=IntegrationMerge(
            projects=settings.projects,
            github=github,
            git=GitVerifier(runner=runner),
        ),
        window=window,
        linear=linear,
        qa=qa,
        lead=outbox,
    )
    turns = MergerTurnService(
        database=database,
        projects=settings.projects,
        merger=merger,
        admission=admission,
        reviews=reviews,
        reports=CodexThreadReports(rpc),
        github=github,
        lead=outbox,
        window=window,
        manifest_root=manifest_root,
    )
    return MergeFlow(
        rpc=rpc,
        merger=merger,
        delivery=delivery,
        emitter=emitter,
        admission=admission,
        window=window,
        reviews=reviews,
        turns=turns,
        outbox=outbox,
        qa=qa,
        manifest_root=manifest_root,
    )


def _branch_head(settings: Settings, github: GitHubClient) -> Callable[[str, str], str]:
    """Resolve the remote branch head from the one open pull request."""

    def head(project_key: str, branch: str) -> str:
        project = settings.projects[project_key]
        for summary in github.list_open_pulls(
            project.github_repo, base=project.integration_branch
        ):
            if summary.head_ref == branch:
                return summary.head_sha
        return ""

    return head


def _base_policy(settings: Settings, git: GitVerifier) -> Callable[[str, str], bool]:
    """A candidate base must be reachable from the fetched integration branch."""

    def policy(project_key: str, base_sha: str) -> bool:
        project = settings.projects[project_key]
        try:
            git.fetch(project.repo_path, "origin", project.integration_branch)
            return git.is_ancestor(
                project.repo_path, base_sha, f"origin/{project.integration_branch}"
            )
        except Exception:
            return False

    return policy
