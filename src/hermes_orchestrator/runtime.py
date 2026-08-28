"""Assemble observation-only or live orchestration components."""

from __future__ import annotations

import fcntl
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TextIO

from hermes_orchestrator.admission import AdmissionController, PressureClassifier
from hermes_orchestrator.cells import DispatchResult, ProjectCellService
from hermes_orchestrator.checkpoints import CheckpointRequests, CheckpointSafetyStore
from hermes_orchestrator.claude import ClaudeRunner
from hermes_orchestrator.config import Settings
from hermes_orchestrator.context import ActiveTimeTracker, ContextMonitor
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.handoffs import HandoffService
from hermes_orchestrator.keychain import Keychain
from hermes_orchestrator.linear import (
    ExternalEffectStore,
    LinearClient,
    LinearGraphQLTransport,
    ProjectLinearRouter,
)
from hermes_orchestrator.merge_flow import MergeFlow, build_merge_flow
from hermes_orchestrator.processes import ProcessRegistry
from hermes_orchestrator.profiles import (
    ClaudeProfileProbe,
    JsonCommand,
    ProfileHealth,
    ProfilePool,
    ProfileRegistry,
)
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.resources import ResourceSampler
from hermes_orchestrator.scheduler import Scheduler
from hermes_orchestrator.service import OrchestratorService
from hermes_orchestrator.stalls import ScheduledResets

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


class KeychainReader(Protocol):
    """Narrow credential boundary used during live assembly."""

    def read(self, service: str, account: str) -> str: ...


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
    processes: ProcessRegistry | None = None
    checkpoints: CheckpointRequests | None = None
    resets: ScheduledResets | None = None
    _daemon_lock: _DaemonLock | None = None

    def close(self) -> None:
        """Close durable local state after all workers have stopped."""

        try:
            self.database.close()
        finally:
            if self._daemon_lock is not None:
                self._daemon_lock.release()


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
        queue = QueueService(database, events, settings.projects)
        cells: ProjectCellService | None = None
        dispatch: Dispatch | None = None
        merge_flow: MergeFlow | None = None
        profile_health: tuple[ProfileHealth, ...] = ()

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

            environment = dict(os.environ if base_env is None else base_env)
            registry = ProfileRegistry.load(profile_path)
            pool = ProfilePool(registry)
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

            reader = keychain or Keychain()
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
            runner = ClaudeRunner(
                registry,
                prompt_file=prompt_path,
                base_env=environment,
                processes=processes,
                freeze_dir=settings.state_dir / "freezes",
            )
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
        )
        return Runtime(
            database=database,
            queue=queue,
            service=service,
            dispatch=dispatch,
            cells=cells,
            profile_health=profile_health,
            merge_flow=merge_flow,
            processes=processes,
            checkpoints=checkpoints,
            resets=resets,
            _daemon_lock=daemon_lock,
        )
    except BaseException:
        try:
            database.close()
        finally:
            if daemon_lock is not None:
                daemon_lock.release()
        raise
