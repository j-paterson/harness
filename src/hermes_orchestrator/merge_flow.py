"""Compose the live Codex merge flow from real adapters (INFRA-166).

Credentials are read from the profile-isolated macOS Keychain at the first
POINT OF USE of the collaborator that needs them (INFRA-212), never while
the flow is built; nothing here starts a process or performs a network
call. The Codex App Server client is returned unstarted so the caller
controls its lifecycle. CircleCI is consulted only inside ``CiWindow`` at
intake boundaries; there is no polling loop anywhere in this graph.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

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
from hermes_orchestrator.git import (
    GitRunner,
    GitVerifier,
    SubprocessGitRunner,
)
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
    BranchHeadUnknown,
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


class DeferredCollaborator:
    """One credentialed collaborator, composed at its first point of use.

    INFRA-212: ``build_merge_flow`` used to read every Keychain
    credential it might need while it composed the graph, so a strictly
    local command that touches none of them still failed closed on a
    Keychain read -- the observed ``submit-review`` failure inside the
    Codex workspace, where ``security`` exited 44 before the submission
    could be validated at all.

    This is a transparent forwarding proxy, NOT an alternative
    implementation: it holds the exact factory that builds the exact
    production collaborator and runs it once, on the first attribute
    access, then delegates every attribute to that one instance
    thereafter. There is no second code path to drift from the
    credentialed one -- a caller that touches the collaborator composes
    and uses the real client, and a caller that never touches it never
    reads a credential. A composition failure (an unreadable Keychain
    item, missing routing config) is raised at the point of use, so a
    path that genuinely needs the credential still fails closed with the
    same exception it always raised, merely later.

    ``__getattr__`` forwarding is deliberate: it can never fall behind
    the wrapped client's surface the way a hand-written stub with an
    enumerated method list would.

    ``surface`` names the methods a consumer may PROBE (``hasattr``)
    while wiring the graph -- ``CiWindow`` checks ``hasattr(status,
    "check")`` in its constructor, and a bare forwarding proxy would
    compose the client just to answer that. Those names are answered
    with a forwarding callable that composes on CALL instead. It is an
    optimisation only: a name outside ``surface`` still resolves
    correctly, merely by composing sooner, so the list can never make
    the proxy behave differently from the client it wraps.
    """

    __slots__ = ("_factory", "_instance", "_surface")

    def __init__(
        self, factory: Callable[[], Any], *, surface: tuple[str, ...] = ()
    ) -> None:
        self._factory = factory
        self._instance: Any | None = None
        self._surface = frozenset(surface)

    def _resolve(self) -> Any:
        instance = self._instance
        if instance is None:
            instance = self._factory()
            self._instance = instance
        return instance

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__") or name in ("_factory", "_instance", "_surface"):
            # Never forward this proxy's own slots: an unset slot would
            # otherwise re-enter here and recurse.
            raise AttributeError(name)
        if name in self._surface and self._instance is None:

            def call(*args: Any, **kwargs: Any) -> Any:
                return getattr(self._resolve(), name)(*args, **kwargs)

            return call
        return getattr(self._resolve(), name)


class DatabaseDurableWakeReader:
    """Read-only ``wake_deliveries`` lookup satisfying ``DurableWakePort``.

    Sol correction 110ed759 (INFRA-219 R3): packet L5 added the
    ``durable_wake`` port to :class:`CandidateEmitter` precisely so a
    manifest file surviving on disk is never adopted for reuse without a
    matching durable wake row for the exact event -- but the port was left
    optional and ``build_merge_flow`` composed the production emitter
    without wiring it, so the stale-manifest gate never actually bound in
    production. ``wake_deliveries`` is keyed ``PRIMARY KEY (project_key,
    event_id)`` (migration 0004), which is exactly the existence question
    the port asks; nothing already exposed on :class:`CodexMerger` answers
    it as a plain read (every existing ``wake_deliveries`` query there is
    folded into a write's compare-and-swap or a delivery-state scan), so
    this adapter reads the table directly through the same ``Database``
    handle ``build_merge_flow`` already holds rather than growing a new
    write-oriented method on the Merger's channel surface for a read.
    """

    def __init__(self, database: Database) -> None:
        self._database = database

    def exists(self, project_key: str, event_id: str) -> bool:
        return bool(
            self._database.scalar(
                "SELECT EXISTS(SELECT 1 FROM wake_deliveries "
                "WHERE project_key = ? AND event_id = ?)",
                (project_key, event_id),
            )
        )


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

    merge_journal = MergeEffectJournal(database)
    # INFRA-212: the GitHub and CircleCI clients are the graph's only
    # credentialed collaborators, and both are now composed at their
    # first point of use rather than here. The durable, local half of
    # the flow (manifests, wake/submission identity, correction
    # delivery) is therefore fully usable with no Keychain read at all,
    # while every caller that does cross an external boundary gets the
    # identical client -- and the identical fail-closed error when the
    # credential cannot be read.
    github = cast(
        GitHubClient,
        DeferredCollaborator(
            lambda: GitHubClient(
                transport=HttpxGitHubTransport(
                    keychain.read(GITHUB_KEYCHAIN_SERVICE, "default")
                ),
                journal=merge_journal,
            ),
            surface=(
                "merge",
                "list_open_pulls",
                "get_pull_request",
                "discover_pull_request",
            ),
        ),
    )
    circleci: CiStatusPort
    if any(project.ci == "circleci" for project in settings.projects.values()):
        circleci = cast(
            CiStatusPort,
            DeferredCollaborator(
                lambda: CircleCiStatusAdapter(
                    CircleCiClient(
                        transport=HttpxCircleCiTransport(
                            keychain.read(CIRCLECI_KEYCHAIN_SERVICE, "default")
                        )
                    )
                ),
                surface=("check",),
            ),
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
        merged_candidate_proof=_merged_candidate_proof(settings, github),
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
        # Sol correction 110ed759 (INFRA-219 R3): wire the durable-wake
        # reader so the stale-manifest gate L5 added actually binds in
        # production — see DatabaseDurableWakeReader above.
        durable_wake=DatabaseDurableWakeReader(database),
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
    """Resolve the remote branch head via a typed ref query, never a fetch.

    INFRA-217, Sol correction 43152bf8: production previously used
    ``git fetch -- origin <branch>`` and treated ANY fetch failure as
    authoritative absence, but a genuinely deleted remote branch makes
    that exact command exit 128 with ``fatal: couldn't find remote ref``
    -- indistinguishable from a network, auth, or invocation failure at
    the process boundary, so the required already-merged/deleted-branch
    case never reached the GitHub proof below. The prior ``head_of``-
    based fallback made the opposite mistake: every
    :class:`~hermes_orchestrator.git.GitError` from local resolution was
    read as authoritative absence, even though
    :class:`~hermes_orchestrator.git.GitVerifier` raises that identical
    exception for missing refs, corrupt repositories, invalid output, and
    other local failures alike.

    :meth:`~hermes_orchestrator.git.GitVerifier.remote_head` replaces
    both with one typed remote-ref query (``git ls-remote --heads``)
    whose three outcomes stay distinct end to end:

    - AUTHORITATIVE ABSENCE: the remote was reached and its ref
      namespace is authoritative, and it reports zero matching refs.
      This is the normal state AFTER a merge, since GitHub deletes the
      branch, and is represented by returning ``""`` exactly as before.
    - EXISTS: the remote reports exactly one matching
      ``refs/heads/<branch>`` entry. Its SHA is returned.
    - EVERY OTHER FAILURE: transport, authentication, invocation,
      malformed-output, or local-repository failures. This is NEVER
      branch absence, so it raises
      :class:`~hermes_orchestrator.review_intake.BranchHeadUnknown`
      instead of returning ``""``, and callers must fail closed on it.
    """

    def head(project_key: str, branch: str) -> str:
        project = settings.projects[project_key]
        try:
            sha = git.remote_head(project.repo_path, "origin", branch)
        except Exception as error:
            raise BranchHeadUnknown(
                f"remote ref query for {branch!r} failed for "
                f"{project_key!r}: {error}"
            ) from error
        return sha if sha is not None else ""

    return head


def _merged_candidate_proof(
    settings: Settings, github: GitHubClient
) -> Callable[[str, str, str], bool]:
    """Prove a reviewed SHA is an already-merged pull request (INFRA-217).

    GitHub deletes a candidate's branch when its pull request merges, so
    ``origin/<branch>`` stops resolving and admission would reject a
    candidate whose merge is already proven -- the production defect Sol
    found. This is the ONLY thing permitted to excuse an unresolvable
    remote branch, and it excuses nothing else: the discovery must find
    exactly one pull request at the exact reviewed head whose repository
    AND head repository are the project repository, whose base is the
    integration branch, and which is genuinely ``merged``. Discovery is
    already exact on (branch, head_sha) by construction, so a different
    commit can never satisfy it. Any ambiguity or GitHub failure answers
    ``False`` and admission fails closed exactly as before.
    """

    def proof(project_key: str, branch: str, candidate_sha: str) -> bool:
        project = settings.projects.get(project_key)
        if project is None:
            return False
        try:
            discovered = github.discover_pull_request(
                project.github_repo, branch=branch, head_sha=candidate_sha
            )
        except Exception:
            return False
        if discovered is None or not discovered.merged:
            return False
        return (
            discovered.repository == project.github_repo
            and discovered.head_repository == project.github_repo
            and discovered.base_ref == project.integration_branch
        )

    return proof


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
