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

from hermes_orchestrator.acceptance import AcceptanceGates
from hermes_orchestrator.ci_window import CircleCiIntakeGate, CiStatusPort, CiWindow
from hermes_orchestrator.circleci import (
    CiCheck,
    CircleCiClient,
    CircleCiError,
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
from hermes_orchestrator.lead_assignments import LeadAssignments
from hermes_orchestrator.lead_outbox import LeadCorrectionOutbox
from hermes_orchestrator.merge import GitHubIntakeGate, IntegrationMerge
from hermes_orchestrator.merger_turns import CodexThreadReports, MergerTurnService
from hermes_orchestrator.processes import ProcessRegistry
from hermes_orchestrator.qa import QaRouter
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.review_intake import (
    CandidateAdmission,
    CompositeIntakeGate,
)
from hermes_orchestrator.reviews import LinearProjector, ReviewService
from hermes_orchestrator.settlement import MergeSettlements

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


class NoCiStatusPort:
    """Status port for deployments with no CircleCI project at all.

    Installed only when every configured project declares ``ci: none``;
    any call is a contract violation and fails closed.
    """

    def check(self, project_slug: str, branch: str, merge_sha: str) -> CiCheck:
        raise CircleCiError("no configured project uses CircleCI")


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
    settlements: MergeSettlements | None = None
    acceptance: AcceptanceGates | None = None


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
    processes: ProcessRegistry | None = None,
    assignments: LeadAssignments | None = None,
) -> MergeFlow:
    """Wire the production merge flow; fails closed on missing inputs.

    INFRA-198 J1: the acceptance-gate policy is composed here for every
    caller (like :class:`QaRouter` and :class:`MergeSettlements`), so a
    gated issue holds after a proven merge no matter which entry built
    the flow. ``assignments`` is the daemon's durable lead-assignment
    store; when omitted, the acceptance hold still applies but the
    acceptance action packet is dispatched by the reconciliation repair
    instead of at settlement time.
    """

    prompt_path = merger_contract_path(settings.repo_root)
    manifest_root = settings.state_dir / "manifests"
    manifest_root.mkdir(parents=True, exist_ok=True)
    runner = git_runner if git_runner is not None else SubprocessGitRunner()

    github_token = keychain.read(GITHUB_KEYCHAIN_SERVICE, "default")
    merge_journal = MergeEffectJournal(database)
    github = GitHubClient(
        transport=HttpxGitHubTransport(github_token),
        journal=merge_journal,
    )
    circleci: CiStatusPort
    if any(project.ci == "circleci" for project in settings.projects.values()):
        circleci_token = keychain.read(CIRCLECI_KEYCHAIN_SERVICE, "default")
        circleci = CircleCiStatusAdapter(
            CircleCiClient(transport=HttpxCircleCiTransport(circleci_token))
        )
    else:
        circleci = NoCiStatusPort()

    rpc = CodexRpcClient(
        app_server_command(), base_env=base_env, processes=processes
    )
    merger = CodexMerger(
        rpc=rpc,
        database=database,
        projects=settings.projects,
        prompt_file=prompt_path,
    )
    delivery_kwargs: dict[str, Any] = {"processes": processes}
    if queue_process_factory is not None:
        delivery_kwargs["process_factory"] = queue_process_factory
    delivery = CodexQueueDelivery(
        channels=merger, manifest_root=manifest_root, **delivery_kwargs
    )
    window = CiWindow(
        database=database,
        status=circleci,
        max_unresolved=settings.policy.max_unresolved_ci_merges,
        ci_policy=lambda project_key: settings.projects[project_key].ci,
    )
    outbox = LeadCorrectionOutbox(
        database=database,
        events=events,
        project_for_issue=lambda issue_id: queue.get(issue_id).project_key,
    )
    intake_gate = CompositeIntakeGate(
        (
            CircleCiIntakeGate(window, corrections=outbox),
            GitHubIntakeGate(projects=settings.projects, github=github),
        )
    )
    admission = CandidateAdmission(
        channels=merger,
        manifest_root=manifest_root,
        branch_head=_branch_head(settings, GitVerifier(runner=runner)),
        base_policy=_base_policy(settings, GitVerifier(runner=runner)),
        intake_gate=intake_gate,
    )
    qa = QaRouter(database=database, events=events)
    settlements = MergeSettlements(database, events)
    acceptance = AcceptanceGates(database, events=events)
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
        settlements=settlements,
        merge_journal=merge_journal,
        acceptance=acceptance,
        assignments=assignments,
    )
    emitter = CandidateEmitter(
        projects=settings.projects,
        git=runner,
        manifest_root=manifest_root,
        delivery=delivery,
        intake_gate=intake_gate,
        lead=outbox,
        issue_for_failure=reviews.issue_for_candidate,
        # Operator directive (2026-08-30, admission relaxation):
        # candidate admission requires only the freeze gate (clean
        # pushed sha), the immutable manifest, and Sol intake — no
        # delegation-evidence ledger and no mandatory verifier
        # receipt. Test results are advisory; Sol and CI own
        # verification.
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
        settlements=settlements,
        acceptance=acceptance,
    )


def _branch_head(settings: Settings, git: GitVerifier) -> Callable[[str, str], str]:
    """Resolve the remote branch head from git, never from open pull requests.

    Fetches the branch from ``origin`` and resolves ``origin/<branch>`` to
    its commit SHA; any fetch or resolution failure returns "" rather than
    raising, since this feeds an admission comparison, not a proof gate
    (INFRA-202).
    """

    def head(project_key: str, branch: str) -> str:
        project = settings.projects[project_key]
        try:
            git.fetch(project.repo_path, "origin", branch)
            return git.head_of(project.repo_path, f"origin/{branch}")
        except Exception:
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
