from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from hermes_orchestrator.cells import ProjectCellService
from hermes_orchestrator.claude import (
    ClaudeEvent,
    ClaudeEventParser,
    LeadTurnRequest,
)
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
        self.emit_handoff_ack = False
        self.raise_on_start = False
        self.retired_sessions: list[UUID] = []
        self.start_requests: list[LeadTurnRequest] = []

    def start_lead(self, request: LeadTurnRequest) -> AsyncIterator[ClaudeEvent]:
        if self.raise_on_start:
            raise RuntimeError("replacement start failed")
        self.start_count += 1
        self.start_requests.append(request)
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
        if self.emit_handoff_ack:
            yield ClaudeEvent(
                kind="handoff.acknowledged",
                original_type="result",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:02:00Z",
                usage={},
                restated_next_action=("Run the failing unit test and correct ENG-9."),
            )

    async def retire_session(self, session_id: UUID) -> None:
        self.retired_sessions.append(session_id)


class RecordingLinear:
    def __init__(self) -> None:
        self.targets: list[tuple[str, str | None, str]] = []
        self.validations: list[tuple[str, str]] = []

    async def validate(self, project_key: str, issue_id: str) -> object:
        self.validations.append((project_key, issue_id))
        return object()

    async def project(
        self,
        issue_id: str,
        target: LinearProjection,
        effect_id: str,
    ) -> object:
        assert effect_id == f"linear:{issue_id}:in-development:v2"
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
    pool = ProfilePool(
        registry,
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )
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
async def test_linear_team_is_validated_before_claude_starts(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
    linear: RecordingLinear,
) -> None:
    admit(queue, "ENG-9")

    await cell_service.dispatch("ENG-9")

    assert linear.validations == [("demo", "ENG-9")]
    assert runner.start_count == 1


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
async def test_dispatch_does_not_resurrect_issue_completed_during_validation(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")

    class CompletingLinear(RecordingLinear):
        async def validate(self, project_key: str, issue_id: str) -> object:
            result = await super().validate(project_key, issue_id)
            queue.complete(
                issue_id,
                reason="linear_completed",
                evidence="https://linear.example/ENG-9",
            )
            return result

    linear = CompletingLinear()
    service = ProjectCellService(
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

    result = await service.dispatch("ENG-9")

    assert result.status == "already_completed"
    assert queue.get("ENG-9").state.value == "done"
    assert runner.start_count == 0
    assert linear.targets == []


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
async def test_failed_initial_lead_start_releases_cell_and_profile(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
    database: Database,
) -> None:
    admit(queue, "ENG-9")
    runner.raise_on_start = True

    with pytest.raises(RuntimeError, match="replacement start failed"):
        await cell_service.dispatch("ENG-9")

    assert (
        database.scalar(
            "SELECT count(*) FROM project_cells WHERE state IN "
            "('starting', 'active', 'handoff_required', 'paused')"
        )
        == 0
    )
    assert (
        database.scalar("SELECT state FROM project_cells WHERE cell_id = 'cell-demo'")
        == "failed"
    )
    assert database.scalar("SELECT count(*) FROM profile_leases") == 0
    assert queue.get("ENG-9").state.value == "queued"


@pytest.mark.asyncio
async def test_failed_start_cleanup_preserves_a_cell_confirmed_concurrently(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")

    class ConfirmingRunner(RecordingRunner):
        def start_lead(
            self,
            request: LeadTurnRequest,
        ) -> AsyncIterator[ClaudeEvent]:
            return self._confirm_then_fail()

        async def _confirm_then_fail(self) -> AsyncIterator[ClaudeEvent]:
            database.execute(
                "UPDATE project_cells SET state = 'active' WHERE cell_id = 'cell-demo'"
            )
            if False:
                yield ClaudeEvent("unused", "unused", None, None, None, {})
            raise RuntimeError("stream failed after concurrent confirmation")

    service = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=ConfirmingRunner(),
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )

    with pytest.raises(RuntimeError, match="concurrent confirmation"):
        await service.dispatch("ENG-9")

    assert (
        database.scalar("SELECT state FROM project_cells WHERE cell_id = 'cell-demo'")
        == "active"
    )
    assert database.scalar("SELECT count(*) FROM profile_leases") == 1
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'project_cell.start_failed'"
        )
        == 0
    )


@pytest.mark.asyncio
async def test_projection_waits_for_session_started(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
    linear: RecordingLinear,
    database: Database,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_session_started = False

    result = await cell_service.dispatch("ENG-9")

    assert result.status == "start_unconfirmed"
    assert queue.get("ENG-9").state.value == "queued"
    assert linear.targets == []
    assert (
        database.scalar("SELECT state FROM project_cells WHERE cell_id = 'cell-demo'")
        == "failed"
    )
    assert database.scalar("SELECT count(*) FROM profile_leases") == 0


@pytest.mark.asyncio
async def test_linear_projection_failure_leaves_new_issue_retryable(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    tmp_path: Path,
) -> None:
    class FailingLinear(RecordingLinear):
        async def project(
            self,
            issue_id: str,
            target: LinearProjection,
            effect_id: str,
        ) -> object:
            raise TimeoutError("Linear is unavailable")

    service = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=FailingLinear(),
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )
    admit(queue, "ENG-9")

    with pytest.raises(TimeoutError, match="Linear is unavailable"):
        await service.dispatch("ENG-9")

    assert queue.get("ENG-9").state.value == "queued"
    assert (
        database.scalar("SELECT state FROM project_cells WHERE cell_id = 'cell-demo'")
        == "failed"
    )
    assert database.scalar("SELECT count(*) FROM profile_leases") == 0


@pytest.mark.asyncio
async def test_restart_preserves_unconfirmed_cell_until_process_reconciliation(
    database: Database,
    queue: QueueService,
    registry: ProfileRegistry,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    started_at = datetime(2026, 8, 26, tzinfo=UTC).isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at) VALUES (?, ?, 'starting', ?, ?, ?, ?)",
            ("cell-demo", "demo", "max-a", str(SESSION_ID), started_at, started_at),
        )
        connection.execute(
            "INSERT INTO profile_leases("
            "profile_alias, project_key, state, acquired_at) "
            "VALUES ('max-a', 'demo', 'active', ?)",
            (started_at,),
        )
    restarted_profiles = ProfilePool(registry)
    for profile in registry.profiles:
        restarted_profiles.record_health(
            ProfileHealth(
                profile_alias=profile.alias,
                eligible=True,
                reason="eligible",
                last_checked_at=datetime(2026, 8, 26, tzinfo=UTC),
            )
        )
    restarted_runner = RecordingRunner()

    restarted = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=restarted_profiles,
        runner=restarted_runner,
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: UUID("22222222-2222-4222-8222-222222222222"),
        cell_ids=lambda: "cell-retry",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )
    result = await restarted.dispatch("ENG-9")

    assert result.status == "start_reconciliation_required"
    assert result.cell_id == "cell-demo"
    assert restarted_runner.start_count == 0
    assert restarted_runner.resume_count == 0
    assert (
        database.scalar("SELECT state FROM project_cells WHERE cell_id = 'cell-demo'")
        == "starting"
    )
    assert database.scalar("SELECT count(*) FROM profile_leases") == 1


def test_restart_restores_capped_lease_for_handoff_required_cell(
    database: Database,
    queue: QueueService,
    registry: ProfileRegistry,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 26, 12, tzinfo=UTC)
    profiles = ProfilePool(registry, now=lambda: now)
    for profile in registry.profiles:
        profiles.record_health(
            ProfileHealth(
                profile_alias=profile.alias,
                eligible=True,
                reason="eligible",
                last_checked_at=now,
            )
        )
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at) "
            "VALUES ('cell-demo', 'demo', 'handoff_required', 'max-a', ?, ?, ?)",
            (str(SESSION_ID), now.isoformat(), now.isoformat()),
        )
        connection.execute(
            "INSERT INTO profile_leases("
            "profile_alias, project_key, state, acquired_at, cooldown_until) "
            "VALUES ('max-a', 'demo', 'capped', ?, ?)",
            (now.isoformat(), (now + timedelta(hours=1)).isoformat()),
        )

    ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=linear,
        project_paths={"demo": tmp_path},
        now=lambda: now,
    )

    restored = profiles.acquire("demo")
    assert restored is not None
    assert restored.profile_alias == "max-a"


@pytest.mark.asyncio
async def test_creation_conflict_resumes_the_existing_cell(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")

    class ConflictService(ProjectCellService):
        hide_existing = True

        def _find_active_cell(self, project_key: str):  # type: ignore[no-untyped-def]
            if self.hide_existing:
                self.hide_existing = False
                return None
            return super()._find_active_cell(project_key)

    service = ConflictService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: UUID("22222222-2222-4222-8222-222222222222"),
        cell_ids=lambda: "cell-conflict",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )
    started_at = datetime(2026, 8, 26, tzinfo=UTC).isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at) VALUES (?, ?, 'active', ?, ?, ?, ?)",
            ("cell-existing", "demo", "max-b", str(SESSION_ID), started_at, started_at),
        )
        connection.execute(
            "INSERT INTO profile_leases("
            "profile_alias, project_key, state, acquired_at) "
            "VALUES ('max-b', 'demo', 'active', ?)",
            (started_at,),
        )

    result = await service.dispatch("ENG-9")

    assert result.status == "working"
    assert result.cell_id == "cell-existing"
    assert runner.start_count == 0
    assert runner.resume_count == 1


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
    assert (
        database.scalar("SELECT state FROM project_cells WHERE cell_id = 'cell-demo'")
        == "handoff_required"
    )


@pytest.mark.asyncio
async def test_provider_limit_before_session_start_releases_unconfirmed_cell(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
    database: Database,
    profiles: ProfilePool,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_session_started = False
    runner.emit_limit = True

    result = await cell_service.dispatch("ENG-9")

    assert result.status == "start_unconfirmed"
    assert (
        database.scalar("SELECT state FROM project_cells WHERE cell_id = 'cell-demo'")
        == "failed"
    )
    assert (
        database.scalar(
            "SELECT state FROM profile_leases WHERE profile_alias = 'max-a'"
        )
        == "capped"
    )
    assert (
        database.scalar(
            "SELECT cooldown_until FROM profile_leases WHERE profile_alias = 'max-a'"
        )
        is not None
    )
    replacement = profiles.acquire("demo")
    assert replacement is not None
    assert replacement.profile_alias == "max-b"


@pytest.mark.asyncio
async def test_expired_capped_profile_can_be_leased_again(
    database: Database,
    queue: QueueService,
    registry: ProfileRegistry,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 26, 12, tzinfo=UTC)
    profiles = ProfilePool(registry, now=lambda: now)
    for profile in registry.profiles:
        profiles.record_health(
            ProfileHealth(
                profile_alias=profile.alias,
                eligible=True,
                reason="eligible",
                last_checked_at=now,
            )
        )
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO profile_leases("
            "profile_alias, project_key, state, acquired_at, cooldown_until"
            ") VALUES ('max-a', 'old-project', 'capped', ?, ?)",
            (
                datetime(2026, 8, 26, 10, tzinfo=UTC).isoformat(),
                datetime(2026, 8, 26, 11, tzinfo=UTC).isoformat(),
            ),
        )
    service = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: now,
    )
    admit(queue, "ENG-9")

    result = await service.dispatch("ENG-9")

    assert result.status == "working"
    assert result.profile_alias == "max-a"
    assert (
        database.scalar(
            "SELECT state FROM profile_leases WHERE profile_alias = 'max-a'"
        )
        == "active"
    )


@pytest.mark.asyncio
async def test_running_service_reuses_profile_after_cooldown_expires(
    database: Database,
    queue: QueueService,
    registry: ProfileRegistry,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    current = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    profiles = ProfilePool(registry, now=lambda: current[0])
    for profile in registry.profiles:
        profiles.record_health(
            ProfileHealth(
                profile_alias=profile.alias,
                eligible=True,
                reason="eligible",
                last_checked_at=current[0],
            )
        )
    runner = RecordingRunner()
    runner.emit_session_started = False
    runner.emit_limit = True
    cell_ids = iter(("cell-first", "cell-retry"))
    session_ids = iter(
        (
            UUID("11111111-1111-4111-8111-111111111111"),
            UUID("22222222-2222-4222-8222-222222222222"),
        )
    )
    service = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: next(session_ids),
        cell_ids=lambda: next(cell_ids),
        now=lambda: current[0],
    )
    admit(queue, "ENG-9")
    first = await service.dispatch("ENG-9")
    current[0] += timedelta(hours=2)
    runner.emit_session_started = True
    runner.emit_limit = False

    retry = await service.dispatch("ENG-9")

    assert first.status == "start_unconfirmed"
    assert retry.status == "working"
    assert retry.profile_alias == "max-a"
    assert (
        database.scalar(
            "SELECT state FROM profile_leases WHERE profile_alias = 'max-a'"
        )
        == "active"
    )


@pytest.mark.asyncio
async def test_provider_limit_explicitly_closes_the_lead_stream(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")

    class LimitStream:
        def __init__(self) -> None:
            self.emitted = False
            self.closed = False

        def __aiter__(self):  # type: ignore[no-untyped-def]
            return self

        async def __anext__(self) -> ClaudeEvent:
            if self.emitted:
                raise StopAsyncIteration
            self.emitted = True
            return ClaudeEvent(
                kind="provider.limit",
                original_type="result",
                session_id=SESSION_ID,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:00:00Z",
                usage={},
                error_code="subscription_limit",
            )

        async def aclose(self) -> None:
            self.closed = True

    stream = LimitStream()

    class LimitRunner(RecordingRunner):
        def start_lead(
            self,
            request: LeadTurnRequest,
        ) -> AsyncIterator[ClaudeEvent]:
            return stream

    service = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=LimitRunner(),
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "start_unconfirmed"
    assert stream.closed is True


@pytest.mark.asyncio
async def test_false_limit_records_do_not_cool_down_or_hand_off(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    false_records = [
        {
            "type": "rate_limit_event",
            "session_id": str(SESSION_ID),
            "rate_limit": {
                "status": "allowed_warning",
                "summary": "You've reached your weekly limit for Fable 5.",
                "resetsAt": 1767225600,
            },
        },
        {
            "type": "user",
            "session_id": str(SESSION_ID),
            "message": {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu-1",
                        "content": [
                            {
                                "type": "text",
                                "text": "You've hit your usage limit "
                                "for this sandbox.",
                            }
                        ],
                    }
                ],
            },
        },
        {
            "type": "assistant",
            "session_id": str(SESSION_ID),
            "parent_tool_use_id": "toolu-child-1",
            "message": {
                "role": "assistant",
                "content": [
                    {
                        "type": "text",
                        "text": "You've hit your concurrent agents limit. "
                        "Wait for a free subagent slot.",
                    }
                ],
            },
        },
        {
            "type": "result",
            "subtype": "error_during_execution",
            "session_id": str(SESSION_ID),
            "parent_tool_use_id": "toolu-child-1",
            "errors": ["You've hit your usage limit; retry after reset."],
        },
        {
            "type": "assistant",
            "session_id": str(SESSION_ID),
            "message": {
                "role": "assistant",
                "model": "claude-fable-5",
                "content": [
                    {
                        "type": "text",
                        "text": "You've hit your concurrent agents limit. "
                        "Wait for a free subagent slot.",
                    }
                ],
            },
        },
        {
            "type": "result",
            "subtype": "error_during_execution",
            "session_id": str(SESSION_ID),
            "errors": [
                "You've reached your disk usage limit for this workspace."
            ],
        },
        {
            "type": "assistant",
            "session_id": str(SESSION_ID),
            "message": {
                "role": "assistant",
                "model": "claude-fable-5",
                "content": [
                    {
                        "type": "text",
                        "text": "You've reached your limit of the partial "
                        "sums: the series converges to 1.",
                    }
                ],
            },
        },
    ]
    non_subscription_caps = [
        "You've reached your Fable concurrent agents limit. "
        "Wait for a free slot.",
        "You've reached your Fable disk usage limit for this workspace.",
        "You've reached your Fable sandbox limit.",
        "You've reached your Fable tool concurrency limit.",
    ]
    for cap_text in non_subscription_caps:
        false_records.append(
            {
                "type": "assistant",
                "session_id": str(SESSION_ID),
                "message": {
                    "role": "assistant",
                    "model": "<synthetic>",
                    "content": [{"type": "text", "text": cap_text}],
                },
            }
        )
        false_records.append(
            {
                "type": "result",
                "subtype": "error_during_execution",
                "session_id": str(SESSION_ID),
                "errors": [cap_text],
            }
        )

    class FalseLimitRunner(RecordingRunner):
        async def _events(
            self,
            request: LeadTurnRequest,
        ) -> AsyncIterator[ClaudeEvent]:
            yield ClaudeEvent(
                kind="session.started",
                original_type="system",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:00:00Z",
                usage={},
            )
            parser = ClaudeEventParser()
            for record in false_records:
                yield parser.feed(json.dumps(record).encode())

    service = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=FalseLimitRunner(),
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "working"
    assert (
        database.scalar("SELECT state FROM project_cells WHERE cell_id = 'cell-demo'")
        == "active"
    )
    assert (
        database.scalar(
            "SELECT state FROM profile_leases WHERE profile_alias = 'max-a'"
        )
        == "active"
    )
    assert (
        database.scalar(
            "SELECT cooldown_until FROM profile_leases WHERE profile_alias = 'max-a'"
        )
        is None
    )
    assert (
        database.scalar(
            "SELECT count(*) FROM events "
            "WHERE event_type IN "
            "('provider.limit', 'project_cell.handoff_required')"
        )
        == 0
    )


@pytest.mark.asyncio
async def test_duplicate_provider_limit_requires_one_handoff(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")

    class DuplicateLimitRunner(RecordingRunner):
        async def _events(
            self,
            request: LeadTurnRequest,
        ) -> AsyncIterator[ClaudeEvent]:
            yield ClaudeEvent(
                kind="session.started",
                original_type="system",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:00:00Z",
                usage={},
            )
            for _ in range(2):
                yield ClaudeEvent(
                    kind="provider.limit",
                    original_type="result",
                    session_id=request.session_id,
                    parent_tool_use_id=None,
                    timestamp="2026-08-26T12:01:00Z",
                    usage={},
                    error_code="subscription_limit",
                )

    service = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=DuplicateLimitRunner(),
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "handoff_required"
    assert database.scalar(
        "SELECT count(*) FROM events "
        "WHERE event_type = 'project_cell.handoff_required'"
    ) == 1


@pytest.mark.asyncio
async def test_provider_limit_before_session_start_cannot_activate_later(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")

    class LimitThenStartRunner(RecordingRunner):
        async def _events(
            self,
            request: LeadTurnRequest,
        ) -> AsyncIterator[ClaudeEvent]:
            yield ClaudeEvent(
                kind="provider.limit",
                original_type="result",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:00:00Z",
                usage={},
                error_code="subscription_limit",
            )
            yield ClaudeEvent(
                kind="session.started",
                original_type="system",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:01:00Z",
                usage={},
            )

    service = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=LimitThenStartRunner(),
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "start_unconfirmed"
    assert queue.get("ENG-9").state.value == "queued"
    assert linear.targets == []
    assert (
        database.scalar("SELECT state FROM project_cells WHERE cell_id = 'cell-demo'")
        == "failed"
    )


@pytest.mark.asyncio
async def test_concurrent_dispatch_waits_for_initial_session_confirmation(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    start_entered = asyncio.Event()
    resume_entered = asyncio.Event()
    emit_session = asyncio.Event()

    class ConcurrentRunner(RecordingRunner):
        def start_lead(
            self,
            request: LeadTurnRequest,
        ) -> AsyncIterator[ClaudeEvent]:
            return self._delayed_start(request)

        def resume_lead(
            self,
            session_id: UUID,
            request: LeadTurnRequest,
        ) -> AsyncIterator[ClaudeEvent]:
            return self._late_resume(request)

        async def _delayed_start(
            self,
            request: LeadTurnRequest,
        ) -> AsyncIterator[ClaudeEvent]:
            start_entered.set()
            await emit_session.wait()
            yield ClaudeEvent(
                kind="session.started",
                original_type="system",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:00:00Z",
                usage={},
            )

        async def _late_resume(
            self,
            request: LeadTurnRequest,
        ) -> AsyncIterator[ClaudeEvent]:
            resume_entered.set()
            yield ClaudeEvent(
                kind="session.started",
                original_type="system",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:01:00Z",
                usage={},
            )

    service = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=ConcurrentRunner(),
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )
    admit(queue, "ENG-9")
    first = asyncio.create_task(service.dispatch("ENG-9"))
    await start_entered.wait()
    admit(queue, "ENG-10")
    second = asyncio.create_task(service.dispatch("ENG-10"))
    await asyncio.sleep(0)

    assert resume_entered.is_set() is False
    emit_session.set()
    first_result = await first
    second_result = await second

    assert first_result.status == "working"
    assert second_result.status == "working"
    assert resume_entered.is_set() is True
    assert queue.get("ENG-10").state.value == "in_development"
    assert (
        database.scalar("SELECT state FROM project_cells WHERE cell_id = 'cell-demo'")
        == "active"
    )
    assert database.scalar("SELECT count(*) FROM profile_leases") == 1


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
