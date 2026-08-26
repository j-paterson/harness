from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from hermes_orchestrator.cells import ProjectCellService
from hermes_orchestrator.claude import ClaudeEvent, LeadTurnRequest
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.linear import LinearProjection
from hermes_orchestrator.profiles import (
    ProfileHealth,
    ProfilePool,
    ProfileRegistry,
)
from hermes_orchestrator.queue import QueueService

SESSION_ID = UUID("11111111-1111-4111-8111-111111111111")


class RecordingRunner:
    def __init__(self) -> None:
        self.start_count = 0
        self.resume_count = 0
        self.emit_session_started = True
        self.emit_limit = False

    def start_lead(self, request: LeadTurnRequest) -> AsyncIterator[ClaudeEvent]:
        self.start_count += 1
        return self._events(request)

    def resume_lead(
        self,
        session_id: UUID,
        request: LeadTurnRequest,
    ) -> AsyncIterator[ClaudeEvent]:
        assert session_id == request.session_id
        self.resume_count += 1
        return self._events(request)

    async def _events(self, request: LeadTurnRequest) -> AsyncIterator[ClaudeEvent]:
        if self.emit_session_started:
            yield ClaudeEvent(
                kind="session.started",
                original_type="system",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:00:00Z",
                usage={},
            )
        if self.emit_limit:
            yield ClaudeEvent(
                kind="provider.limit",
                original_type="result",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:01:00Z",
                usage={},
                error_code="subscription_limit",
            )


class RecordingLinear:
    def __init__(self) -> None:
        self.targets: list[tuple[str, str, str]] = []

    async def project(
        self,
        issue_id: str,
        target: LinearProjection,
        effect_id: str,
    ) -> object:
        assert effect_id == f"linear:{issue_id}:in-development"
        self.targets.append((issue_id, target.status, target.assignee_alias))
        return object()


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def queue(database: Database) -> QueueService:
    return QueueService(database, EventStore(database), {"demo"})


@pytest.fixture
def registry(tmp_path: Path) -> ProfileRegistry:
    path = tmp_path / "profiles.yaml"
    path.write_text(
        "profiles:\n"
        "  - alias: max-a\n"
        f"    config_dir: {tmp_path / '.claude-max-a'}\n"
        "  - alias: max-b\n"
        f"    config_dir: {tmp_path / '.claude-max-b'}\n"
        "  - alias: max-c\n"
        f"    config_dir: {tmp_path / '.claude-max-c'}\n"
        "  - alias: max-d\n"
        f"    config_dir: {tmp_path / '.claude-max-d'}\n",
        encoding="utf-8",
    )
    return ProfileRegistry.load(path)


@pytest.fixture
def profiles(registry: ProfileRegistry) -> ProfilePool:
    pool = ProfilePool(registry)
    for profile in registry.profiles:
        pool.record_health(
            ProfileHealth(
                profile_alias=profile.alias,
                eligible=True,
                reason="eligible",
                last_checked_at=datetime(2026, 8, 26, tzinfo=UTC),
            )
        )
    return pool


def admit(queue: QueueService, issue_id: str) -> None:
    queue.admit(
        AdmissionRequest(
            issue_id=issue_id,
            project_key="demo",
            linear_priority=1,
            admitted_by="operator",
            instruction_id=f"chat-{issue_id}",
        )
    )


@pytest.fixture
def runner() -> RecordingRunner:
    return RecordingRunner()


@pytest.fixture
def linear() -> RecordingLinear:
    return RecordingLinear()


@pytest.fixture
def cell_service(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> ProjectCellService:
    return ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_two_issues_share_one_project_lead(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
) -> None:
    admit(queue, "ENG-9")
    admit(queue, "ENG-10")

    first = await cell_service.dispatch("ENG-9")
    second = await cell_service.dispatch("ENG-10")

    assert first.cell_id == second.cell_id
    assert first.session_id == second.session_id == SESSION_ID
    assert runner.start_count == 1
    assert runner.resume_count == 1


@pytest.mark.asyncio
async def test_working_issue_projects_in_development(
    cell_service: ProjectCellService,
    queue: QueueService,
    linear: RecordingLinear,
) -> None:
    admit(queue, "ENG-9")

    await cell_service.dispatch("ENG-9")

    assert linear.targets == [("ENG-9", "In Development", "operator")]


@pytest.mark.asyncio
async def test_no_profile_available_keeps_issue_queued(
    database: Database,
    queue: QueueService,
    registry: ProfileRegistry,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    service = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=ProfilePool(registry),
        runner=runner,
        linear=linear,
        project_paths={"demo": tmp_path},
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "waiting_for_profile"
    assert queue.get("ENG-9").state.value == "queued"
    assert linear.targets == []


@pytest.mark.asyncio
async def test_projection_waits_for_session_started(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
    linear: RecordingLinear,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_session_started = False

    result = await cell_service.dispatch("ENG-9")

    assert result.status == "start_unconfirmed"
    assert queue.get("ENG-9").state.value == "queued"
    assert linear.targets == []


@pytest.mark.asyncio
async def test_provider_limit_requires_handoff(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
    database: Database,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_limit = True

    result = await cell_service.dispatch("ENG-9")

    assert result.status == "handoff_required"
    assert database.scalar(
        "SELECT state FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == "handoff_required"


@pytest.mark.asyncio
async def test_handoff_required_cell_does_not_resume(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_limit = True
    await cell_service.dispatch("ENG-9")
    admit(queue, "ENG-10")
    runner.emit_limit = False

    result = await cell_service.dispatch("ENG-10")

    assert result.status == "handoff_required"
    assert runner.resume_count == 0
    assert queue.get("ENG-10").state.value == "queued"
