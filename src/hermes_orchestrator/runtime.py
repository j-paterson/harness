"""Assemble observation-only or live orchestration components."""

from __future__ import annotations

import fcntl
import os
import shutil
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TextIO

from hermes_orchestrator.admission import AdmissionController, PressureClassifier
from hermes_orchestrator.cells import (
    DispatchResult,
    ProfileCapacityEvidence,
    ProjectCellService,
)
from hermes_orchestrator.channel_hub import (
    ChannelCapabilities,
    ChannelHub,
    ChannelLauncher,
    ChannelPacketRouter,
    hub_socket_path,
)
from hermes_orchestrator.checkpoints import CheckpointRequests, CheckpointSafetyStore
from hermes_orchestrator.circleci import (
    CircleCiClient,
    CircleCiStatusAdapter,
    HttpxCircleCiTransport,
)
from hermes_orchestrator.claude import (
    ClaudeRunner,
    control_launch_failure_recorder,
)
from hermes_orchestrator.cmux import (
    CMUX_KEYCHAIN_SERVICE,
    CmuxCliAdapter,
    CmuxControlPort,
)
from hermes_orchestrator.cmux_surfaces import (
    ChannelTrustConfirmer,
    CmuxHibernationDriver,
    CmuxHibernationGate,
    CmuxLeadSeater,
    CmuxSurfaceBindings,
    CmuxSurfaceReconciler,
    CmuxWakeAnnouncer,
    RegistryProfileDirectory,
)
from hermes_orchestrator.config import Settings
from hermes_orchestrator.context import ActiveTimeTracker, ContextMonitor
from hermes_orchestrator.control_operations import ControlOperations
from hermes_orchestrator.dashboard_refresh import DashboardRefreshAction
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.fakechat_router import FakechatWakeRouter
from hermes_orchestrator.git import WorktreeGit
from hermes_orchestrator.github import (
    GitHubClient,
    HttpxGitHubTransport,
    MergeEffectJournal,
)
from hermes_orchestrator.handoffs import HandoffService
from hermes_orchestrator.keychain import Keychain
from hermes_orchestrator.lead_assignments import LeadAssignments
from hermes_orchestrator.lead_intake import (
    LeadIntakeRouter,
    LeadIntakeTransport,
)
from hermes_orchestrator.lead_wakes import LeadTerminalWakes
from hermes_orchestrator.linear import (
    ExternalEffectStore,
    LinearClient,
    LinearGraphQLTransport,
    LinearIssueReader,
    ProjectLinearRouter,
)
from hermes_orchestrator.merge_flow import (
    CIRCLECI_KEYCHAIN_SERVICE,
    GITHUB_KEYCHAIN_SERVICE,
    MergeFlow,
    build_merge_flow,
)
from hermes_orchestrator.operator_decisions import OperatorDecisions
from hermes_orchestrator.orchestrator_workspace import (
    SEAT_ENV,
    OrchestratorWorkspaceLifecycle,
    OrchestratorWorkspaceOwner,
    WorkspaceRefused,
)
from hermes_orchestrator.post_merge import PostMergeAdvance
from hermes_orchestrator.processes import ProcessRegistry
from hermes_orchestrator.profiles import (
    ClaudeProfileProbe,
    JsonCommand,
    ProfileHealth,
    ProfilePool,
    ProfileRegistry,
)
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.reconcile import (
    CircleCiKnownState,
    LinearExpectations,
    ManagedProcessScanner,
    Reconciler,
    ReconciliationReport,
)
from hermes_orchestrator.resources import ResourceSampler
from hermes_orchestrator.scheduler import Scheduler
from hermes_orchestrator.service import OrchestratorService
from hermes_orchestrator.stalls import ScheduledResets
from hermes_orchestrator.subagent_packets import SubagentPackets
from hermes_orchestrator.worktrees import WorktreeCustodian, WorktreeLeases

Dispatch = Callable[[str], Awaitable[DispatchResult]]


class DaemonAlreadyRunning(RuntimeError):
    """Raised when another live runtime owns the durable daemon state."""


class _DaemonLock:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._handle: TextIO | None = None

    def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        handle = self._path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            handle.close()
            raise DaemonAlreadyRunning(
                "another hermes-orchestrator daemon is already running"
            ) from error
        except BaseException:
            handle.close()
            raise
        self._handle = handle

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle = None
            handle.close()


def acquire_workspace_fence(state_dir: Path) -> _DaemonLock:
    """Take the ONE exclusive ownership fence for this state directory.

    Sol L2: every workspace-mutating entry — the daemon's lifecycle
    owner and the manual ``orchestrator-workspace ensure``/``smoke``
    CLI paths — serializes through the same flock the daemon holds
    (``<state-dir>/daemon.lock``). Non-blocking: if the production
    daemon (or any other mutator) owns it, :class:`DaemonAlreadyRunning`
    is raised and the caller must refuse with zero binding/cmux
    effects. A smoke against its own isolated state directory takes
    that directory's own fence, so temp-state smokes stay permitted.
    flock releases on process death, so a crashed owner transfers the
    fence deterministically to the next acquirer.
    """

    lock = _DaemonLock(state_dir / "daemon.lock")
    lock.acquire()
    return lock


class KeychainReader(Protocol):
    """Narrow credential boundary used during live assembly."""

    def read(self, service: str, account: str) -> str: ...


class _CachingKeychain:
    """Read each credential exactly once during one runtime assembly.

    The startup reconciler and the merge flow share the same tokens; the
    cache keeps every credential read single and in its documented order
    regardless of which component asks first.
    """

    def __init__(self, inner: KeychainReader) -> None:
        self._inner = inner
        self._cache: dict[tuple[str, str], str] = {}

    def read(self, service: str, account: str) -> str:
        key = (service, account)
        if key not in self._cache:
            self._cache[key] = self._inner.read(service, account)
        return self._cache[key]


@dataclass(slots=True)
class Runtime:
    """Owned runtime graph and its single database lifecycle."""

    database: Database
    queue: QueueService
    service: OrchestratorService
    dispatch: Dispatch | None
    cells: ProjectCellService | None
    profile_health: tuple[ProfileHealth, ...]
    merge_flow: MergeFlow | None = None
    post_merge: PostMergeAdvance | None = None
    processes: ProcessRegistry | None = None
    checkpoints: CheckpointRequests | None = None
    resets: ScheduledResets | None = None
    reconciliation: ReconciliationReport | None = None
    lead_wakes: LeadTerminalWakes | None = None
    cmux_bindings: CmuxSurfaceBindings | None = None
    cmux_reconciler: CmuxSurfaceReconciler | None = None
    cmux_hibernation: CmuxHibernationDriver | None = None
    orchestrator_workspace: OrchestratorWorkspaceOwner | None = None
    lead_intake: LeadIntakeRouter | None = None
    channel_hub: ChannelHub | None = None
    channel_capabilities: ChannelCapabilities | None = None
    control_operations: ControlOperations | None = None
    dashboard_refresh: DashboardRefreshAction | None = None
    # Sol correction b4b545f3 (v5): production composition no longer
    # builds a fakechat wake plane, so this is always None from
    # open_runtime; the field survives only because the CLI supervisor
    # threads it through and treats None as "no fakechat routing".
    fakechat_router: FakechatWakeRouter | None = None
    _daemon_lock: _DaemonLock | None = None

    def close(self) -> None:
        """Close durable local state after all workers have stopped."""

        try:
            self.database.close()
        finally:
            if self._daemon_lock is not None:
                self._daemon_lock.release()


def cmux_password_source(
    keychain: KeychainReader,
) -> Callable[[], str | None]:
    """Read the optional cmux socket password from the Keychain once.

    A process seated inside cmux needs no password, so absence is normal:
    any read failure resolves to no password and the socket's own denial
    is the fail-closed boundary. The result is memoized so a missing item
    is not re-probed on every cmux call.
    """

    cache: list[str | None] = []

    def read() -> str | None:
        if not cache:
            try:
                cache.append(keychain.read(CMUX_KEYCHAIN_SERVICE, "default"))
            except Exception:
                cache.append(None)
        return cache[0]

    return read


def resolve_sidecar_entry(*, repo_root: Path, state_dir: Path) -> Path:
    """Resolve the hermes-control sidecar's launch entry point.

    The sidecar build (``channels/hermes-control/dist/``) is compiled
    JS output that Git never tracks, so a plain ``repo_root`` checkout
    may carry none — the historical location the daemon has always
    launched from when it runs directly out of a checkout. Runtime
    activation (see ``activation.materialize_artifact``) copies a
    proven sidecar build into every immutable runtime artifact, read
    via the ``runtimes/ACTIVE`` pointer the same way the shell
    bootstrap does.

    Sol correction f0a5a403 (packet 2): once an ACTIVE runtime is
    recorded (the pointer is readable and non-empty), the artifact-
    derived entry path is returned REGARDLESS of whether the file
    exists — an ACTIVE artifact missing or carrying an incomplete
    sidecar must make
    :class:`~hermes_orchestrator.channel_hub.ChannelLauncher` refuse
    the missing entry (the durable ``channel.blocked`` receipt), never
    silently execute mutable gitignored checkout bytes unrelated to
    the active artifact identity. The ``repo_root`` fallback exists
    only for the documented pre-activation state, when no ACTIVE
    runtime is recorded; a resolution where that path does not exist
    is not itself an error here — the caller's ``shutil.which("node")``
    guard and the launcher's file-exists check remain the fail-closed
    boundary.
    """

    fallback = (
        repo_root / "channels" / "hermes-control" / "dist" / "src" / "main.js"
    )
    pointer = state_dir / "runtimes" / "ACTIVE"
    try:
        recorded = pointer.read_text(encoding="utf-8").strip()
    except OSError:
        return fallback
    if not recorded:
        return fallback
    return (
        Path(recorded)
        / "channels"
        / "hermes-control"
        / "dist"
        / "src"
        / "main.js"
    )


def build_channel_collaborators(
    *,
    settings: Settings,
    database: Database,
    events: EventStore,
    control_operations: ControlOperations,
    cmux_port: CmuxControlPort,
    capabilities: ChannelCapabilities | None = None,
) -> tuple[ChannelLauncher | None, ChannelTrustConfirmer | None]:
    """Compose the hermes-control channel launcher and trust confirmer.

    Shared by ``open_runtime``'s live daemon composition and the
    one-shot ``rotate-lead`` CLI command's collaborators, so a
    Hermes-managed lead rotation seats the replacement with exactly the
    same channel launch and trust-confirmation path the daemon uses —
    never a plain classic seat missing the hermes-control channel.
    """

    channel_capabilities = (
        capabilities
        if capabilities is not None
        else ChannelCapabilities(database=database, state_dir=settings.state_dir)
    )
    # Sol correction b4b545f3 (v5): the fakechat wake plane (the v4
    # substitute) is retired from production composition — the
    # hermes-control channel is the wake accelerator, and the
    # announcements plus the Stop-hook poll remain the automatic
    # fallback.
    node_binary = shutil.which("node")
    channel_launcher = (
        None
        if node_binary is None
        else ChannelLauncher(
            state_dir=settings.state_dir,
            capabilities=channel_capabilities,
            sidecar_entry=resolve_sidecar_entry(
                repo_root=settings.repo_root,
                state_dir=settings.state_dir,
            ),
            node_binary=Path(node_binary),
        )
    )
    # Sol correction f0a5a403 (packet 4): the trust gate is part of the
    # production managed-seat lifecycle — the seater triggers one
    # bounded gate evaluation for each newly created channel-launched
    # binding, so a trusted launch auto-confirms without the manual
    # channel-trust-confirm command. The entry path is re-resolved per
    # trigger so the gate always measures the ACTIVE artifact identity
    # of that moment.
    channel_confirmer = (
        None
        if channel_launcher is None
        else ChannelTrustConfirmer(
            database=database,
            events=events,
            control=control_operations,
            port=cmux_port,
            entry_resolver=lambda: resolve_sidecar_entry(
                repo_root=settings.repo_root,
                state_dir=settings.state_dir,
            ),
        )
    )
    return channel_launcher, channel_confirmer


def build_linear_router(
    settings: Settings,
    *,
    database: Database,
    queue: QueueService,
    keychain: KeychainReader,
) -> ProjectLinearRouter:
    """Build project-scoped Linear clients from validated routing config."""

    linear_config = settings.linear
    if linear_config is None:
        raise ValueError("live runtime requires Linear routing")
    token = keychain.read("hermes-orchestrator-linear", "default")
    transport = LinearGraphQLTransport(token)
    effects = ExternalEffectStore(database)
    clients = {
        project_key: LinearClient(
            transport=transport,
            effects=effects,
            status_ids=linear_config.teams[
                project.linear_team
            ].status_ids.as_mapping(),
            assignee_ids=linear_config.assignee_ids,
            expected_team_id=linear_config.teams[project.linear_team].team_id,
        )
        for project_key, project in settings.projects.items()
    }
    return ProjectLinearRouter(
        clients=clients,
        project_for_issue=lambda issue_id: queue.get(issue_id).project_key,
    )


def open_runtime(
    settings: Settings,
    *,
    enable_live: bool,
    profile_command: JsonCommand | None = None,
    keychain: KeychainReader | None = None,
    base_env: Mapping[str, str] | None = None,
    process_iter: Callable[[], Iterable[Any]] | None = None,
) -> Runtime:
    """Build a runtime, loading credentials only for explicitly active operation."""

    if enable_live and settings.policy.mode != "active":
        raise ValueError("live runtime requires active policy mode")

    daemon_lock = (
        _DaemonLock(settings.state_dir / "daemon.lock") if enable_live else None
    )
    if daemon_lock is not None:
        daemon_lock.acquire()
    try:
        database = Database.open(settings.state_dir / "state.db")
    except BaseException:
        if daemon_lock is not None:
            daemon_lock.release()
        raise
    try:
        events = EventStore(database)
        processes = ProcessRegistry(database, events)
        safety = CheckpointSafetyStore(database, events)
        checkpoints = CheckpointRequests(database, events)
        resets = ScheduledResets(database, events)
        lead_wakes = LeadTerminalWakes(database=database, events=events)
        lead_assignments = LeadAssignments(database, events=events)
        control_operations = ControlOperations(database, events=events)
        subagent_packets = SubagentPackets(database, events=events)
        cmux_bindings = CmuxSurfaceBindings(database=database, events=events)
        queue = QueueService(database, events, settings.projects)
        worktree_git = WorktreeGit()
        worktree_leases = WorktreeLeases(database, events)
        custodian = WorktreeCustodian(worktree_leases, processes, worktree_git)

        reader: KeychainReader | None = None
        reconciler_ports: dict[str, Any] = {}
        if enable_live:
            linear_config = settings.linear
            if linear_config is None:
                raise ValueError("live runtime requires Linear routing")
            profile_path = settings.repo_root / "config" / "profiles.yaml"
            if not profile_path.exists():
                raise ValueError("live runtime requires config/profiles.yaml")
            prompt_path = settings.repo_root / "prompts" / "claude-lead.md"
            if not prompt_path.is_file():
                raise ValueError("live runtime requires prompts/claude-lead.md")
            missing_repositories = sorted(
                alias
                for alias, project in settings.projects.items()
                if not project.repo_path.is_dir()
            )
            if missing_repositories:
                raise ValueError(
                    "live runtime project paths do not exist: "
                    + ", ".join(missing_repositories)
                )

            # The reconciler owns its own bounded read-only external
            # ports; credentials are cached so every keychain item is
            # still read exactly once per assembly, in order.
            reader = _CachingKeychain(
                keychain if keychain is not None else Keychain()
            )
            linear_token = reader.read("hermes-orchestrator-linear", "default")
            github_token = reader.read(GITHUB_KEYCHAIN_SERVICE, "default")
            ci_states: CircleCiKnownState | None = None
            if any(
                project.ci == "circleci"
                for project in settings.projects.values()
            ):
                circleci_token = reader.read(
                    CIRCLECI_KEYCHAIN_SERVICE, "default"
                )
                ci_states = CircleCiKnownState(
                    CircleCiStatusAdapter(
                        CircleCiClient(
                            transport=HttpxCircleCiTransport(circleci_token)
                        )
                    )
                )
            reconciler_ports = {
                "unknown_processes": ManagedProcessScanner(
                    database,
                    roots=tuple(
                        project.repo_path
                        for project in settings.projects.values()
                    ),
                    iter_processes=process_iter,
                ),
                "github_reads": GitHubClient(
                    transport=HttpxGitHubTransport(github_token),
                    journal=MergeEffectJournal(database),
                ),
                "linear_reads": LinearIssueReader(linear_token),
                "linear_expectations": LinearExpectations(
                    team_ids={
                        alias: linear_config.teams[project.linear_team].team_id
                        for alias, project in settings.projects.items()
                    },
                    status_ids={
                        alias: dict(
                            linear_config.teams[
                                project.linear_team
                            ].status_ids.as_mapping()
                        )
                        for alias, project in settings.projects.items()
                    },
                    assignee_ids=dict(linear_config.assignee_ids),
                ),
                "ci_states": ci_states,
            }

        reconciler = Reconciler(
            database,
            events,
            projects=settings.projects,
            processes=processes,
            worktrees=worktree_leases,
            custodian=custodian,
            git=worktree_git,
            **reconciler_ports,
        )
        reconciliation: ReconciliationReport | None = None
        cells: ProjectCellService | None = None
        dispatch: Dispatch | None = None
        merge_flow: MergeFlow | None = None
        post_merge: PostMergeAdvance | None = None
        profile_health: tuple[ProfileHealth, ...] = ()
        cmux_reconciler: CmuxSurfaceReconciler | None = None
        cmux_hibernation: CmuxHibernationDriver | None = None
        orchestrator_workspace: OrchestratorWorkspaceOwner | None = None
        lead_intake: LeadIntakeRouter | None = None
        channel_hub: ChannelHub | None = None
        channel_capabilities: ChannelCapabilities | None = None
        dashboard_refresh: DashboardRefreshAction | None = None

        if enable_live:
            assert reader is not None

            # The ordered startup reconciliation completes before any
            # profile probe runs and before scheduling can begin; its
            # report alone decides whether admission may open.
            reconciliation = reconciler.run()

            environment = dict(os.environ if base_env is None else base_env)
            registry = ProfileRegistry.load(profile_path)
            # INFRA-205: replacement selection consults the durable
            # capacity observations, so a Fable-capped profile is never
            # chosen on auth health alone.
            pool = ProfilePool(
                registry, capacity_evidence=ProfileCapacityEvidence(database)
            )
            probe = ClaudeProfileProbe(
                registry,
                command=profile_command,
                base_env=environment,
            )
            checked: list[ProfileHealth] = []
            for profile in registry.profiles:
                health = probe.check(profile.alias)
                pool.record_health(health)
                checked.append(health)
            profile_health = tuple(checked)

            # The dashboard reads only durable rows and the loaded
            # registry; every other collaborator stays defaulted per the
            # Optional=None convention. Without a registry (observe
            # mode) the field stays None: no dashboard, never a crash.
            dashboard_refresh = DashboardRefreshAction(
                database=database, registry=registry
            )

            linear = build_linear_router(
                settings, database=database, queue=queue, keychain=reader
            )
            merge_flow = build_merge_flow(
                settings,
                database=database,
                events=events,
                queue=queue,
                linear=linear,
                keychain=reader,
                base_env=environment,
                processes=processes,
            )
            # INFRA-198 P2: composed only in live/active mode, where a
            # durable database exists. ReviewService is built inside
            # ``build_merge_flow`` (out of reach here), so the fast
            # ``on_merged`` accelerator is attached to the already-built
            # instance as a public attribute; the daemon's 30s tick is
            # the restart-safe, always-correct discovery path regardless.
            post_merge = PostMergeAdvance(
                database=database,
                events=events,
                projects=settings.projects,
                queue=queue,
                repo_root=settings.repo_root,
                state_dir=settings.state_dir,
                registry=processes,
            )
            merge_flow.reviews.on_merged = post_merge.on_merged
            runner = ClaudeRunner(
                registry,
                prompt_file=prompt_path,
                base_env=environment,
                processes=processes,
                freeze_dir=settings.state_dir / "freezes",
                launch_failure_recorder=control_launch_failure_recorder(
                    control_operations
                ),
            )
            cmux_seater: CmuxLeadSeater | None = None
            if settings.cmux is not None:
                # Terminal visibility is observational: the port speaks
                # only allow-listed metadata commands, the socket password
                # flows Keychain → environment and nowhere else, and a
                # denied or absent cmux never blocks orchestration.
                cmux_port = CmuxCliAdapter(
                    settings.cmux.cli,
                    base_env=environment,
                    password_source=cmux_password_source(reader),
                )
                cmux_project_paths = {
                    alias: project.repo_path
                    for alias, project in settings.projects.items()
                }
                cmux_profile_dirs = RegistryProfileDirectory(registry)
                # The dedicated channel is an accelerator over the
                # same durable packets: freshly committed wakes and
                # corrections route to a registered hermes-control
                # channel the moment they commit, while the metadata
                # announcements and the Stop-hook poll remain the
                # automatic fallback.
                channel_capabilities = ChannelCapabilities(
                    database=database, state_dir=settings.state_dir
                )
                channel_hub = ChannelHub(
                    database=database,
                    bindings=cmux_bindings,
                    capabilities=channel_capabilities,
                    socket_path=hub_socket_path(settings.state_dir),
                    control=control_operations,
                )
                channel_router = ChannelPacketRouter(channel_hub)
                channel_router.attach(lead_wakes)
                channel_router.attach(lead_assignments)
                channel_router.attach(control_operations)
                channel_router.attach(merge_flow.outbox)
                channel_launcher, channel_confirmer = build_channel_collaborators(
                    settings=settings,
                    database=database,
                    events=events,
                    control_operations=control_operations,
                    cmux_port=cmux_port,
                    capabilities=channel_capabilities,
                )
                cmux_seater = CmuxLeadSeater(
                    bindings=cmux_bindings,
                    port=cmux_port,
                    project_paths=cmux_project_paths,
                    profile_dirs=cmux_profile_dirs,
                    # The read-only auth-status probe under the leased
                    # profile's exact CLAUDE_CONFIG_DIR: a seat is
                    # refused before creation unless the account is a
                    # logged-in first-party Max subscription.
                    auth_probe=lambda alias: probe.check(alias).eligible,
                    channel_launch=channel_launcher,
                    control=control_operations,
                    channel_trust=channel_confirmer,
                )
                cmux_reconciler = CmuxSurfaceReconciler(
                    bindings=cmux_bindings,
                    port=cmux_port,
                    project_paths=cmux_project_paths,
                    profile_dirs=cmux_profile_dirs,
                    environ=environment,
                )
                cmux_hibernation = CmuxHibernationDriver(
                    gate=CmuxHibernationGate(
                        database=database,
                        bindings=cmux_bindings,
                        safety=safety,
                    ),
                    port=cmux_port,
                    database=database,
                    events=events,
                )
                CmuxWakeAnnouncer(
                    bindings=cmux_bindings, port=cmux_port
                ).attach(lead_wakes)
                # The lead-intake router derives pending correction
                # and wake envelopes from durable state every pass,
                # so publication and typing can never be separated
                # permanently by a crash.
                lead_intake = LeadIntakeRouter(
                    database=database,
                    transport=LeadIntakeTransport(
                        database=database,
                        bindings=cmux_bindings,
                        port=cmux_port,
                    ),
                )
                # INFRA-191 (Sol K1): the one lock-holding daemon owns
                # the two-pane Orchestrator workspace autonomously;
                # the upper pane runs the lock-free read-only
                # `dashboard` entry, never a second daemon. A daemon
                # somehow launched inside a marked pane composes a
                # lifecycle that refuses everything, fail closed. An
                # unconfigurable repo/state path refuses composition
                # instead of crashing live startup: cmux visibility
                # stays optional.
                try:
                    orchestrator_workspace = OrchestratorWorkspaceOwner(
                        OrchestratorWorkspaceLifecycle(
                            port=cmux_port,
                            bindings=cmux_bindings,
                            repo_root=settings.repo_root,
                            state_dir=settings.state_dir,
                            inside_marked_pane=bool(
                                environment.get(SEAT_ENV)
                            ),
                        )
                    )
                except WorkspaceRefused:
                    orchestrator_workspace = None

            cells = ProjectCellService(
                database=database,
                events=events,
                queue=queue,
                profiles=pool,
                runner=runner,
                linear=linear,
                project_paths={
                    alias: project.repo_path
                    for alias, project in settings.projects.items()
                },
                handoffs=HandoffService(database),
                safety=safety,
                checkpoints=checkpoints,
                context=ContextMonitor(database, events, policy=settings.policy),
                active_time=ActiveTimeTracker(database),
                context_window_tokens=settings.policy.context_window_tokens,
                completion_sink=lead_wakes,
                surfaces=cmux_seater,
                classic_seats=(
                    settings.cmux is not None
                    and settings.cmux.classic_leads
                    and cmux_seater is not None
                ),
                decisions=OperatorDecisions(database),
                assignments=lead_assignments,
                packets=subagent_packets,
            )
            dispatch = cells.dispatch

        scheduler = Scheduler(
            queue,
            mode="active" if enable_live else "observe",
            active_projects=(cells.active_projects if cells is not None else ()),
        )
        repository_paths = {
            alias: project.repo_path
            for alias, project in settings.projects.items()
        }
        repository_paths["orchestrator_state"] = settings.state_dir
        classifier = PressureClassifier(settings.policy)
        sampler = ResourceSampler(
            policy=settings.policy,
            repository_paths=repository_paths,
            managed_rss=processes.managed_rss_bytes,
            classifier=classifier,
        )
        service = OrchestratorService(
            database=database,
            events=events,
            sampler=sampler,
            scheduler=scheduler,
            policy=settings.policy,
            processes=processes,
            admission=AdmissionController(classifier),
            queue=queue,
            safety=safety,
            checkpoints=checkpoints,
            resets=resets,
            reconciler=reconciler,
            startup_report=reconciliation,
        )
        return Runtime(
            database=database,
            queue=queue,
            service=service,
            dispatch=dispatch,
            cells=cells,
            profile_health=profile_health,
            merge_flow=merge_flow,
            post_merge=post_merge,
            processes=processes,
            checkpoints=checkpoints,
            resets=resets,
            reconciliation=reconciliation,
            lead_wakes=lead_wakes,
            cmux_bindings=cmux_bindings,
            cmux_reconciler=cmux_reconciler,
            cmux_hibernation=cmux_hibernation,
            orchestrator_workspace=orchestrator_workspace,
            lead_intake=lead_intake,
            channel_hub=channel_hub,
            channel_capabilities=channel_capabilities,
            control_operations=control_operations,
            dashboard_refresh=dashboard_refresh,
            fakechat_router=None,
            _daemon_lock=daemon_lock,
        )
    except BaseException:
        try:
            database.close()
        finally:
            if daemon_lock is not None:
                daemon_lock.release()
        raise
