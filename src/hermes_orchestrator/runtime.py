"""Assemble observation-only or live orchestration components."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

from hermes_orchestrator.cells import DispatchResult, ProjectCellService
from hermes_orchestrator.claude import ClaudeRunner
from hermes_orchestrator.config import Settings
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

Dispatch = Callable[[str], Awaitable[DispatchResult]]


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

    def close(self) -> None:
        """Close durable local state after all workers have stopped."""

        self.database.close()


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

    database = Database.open(settings.state_dir / "state.db")
    try:
        events = EventStore(database)
        queue = QueueService(database, events, settings.projects)
        cells: ProjectCellService | None = None
        dispatch: Dispatch | None = None
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

            token = (keychain or Keychain()).read(
                "hermes-orchestrator-linear",
                "default",
            )
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
                    expected_team_id=linear_config.teams[
                        project.linear_team
                    ].team_id,
                )
                for project_key, project in settings.projects.items()
            }
            linear = ProjectLinearRouter(
                clients=clients,
                project_for_issue=lambda issue_id: queue.get(issue_id).project_key,
            )
            runner = ClaudeRunner(
                registry,
                prompt_file=prompt_path,
                base_env=environment,
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
        sampler = ResourceSampler(
            policy=settings.policy,
            repository_paths=repository_paths,
        )
        service = OrchestratorService(
            database=database,
            events=events,
            sampler=sampler,
            scheduler=scheduler,
            policy=settings.policy,
        )
        return Runtime(
            database=database,
            queue=queue,
            service=service,
            dispatch=dispatch,
            cells=cells,
            profile_health=profile_health,
        )
    except BaseException:
        database.close()
        raise
