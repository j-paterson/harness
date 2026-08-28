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
from hermes_orchestrator.handoffs import HandoffService
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


@pytest.mark.asyncio
async def test_turn_completion_records_session_bound_safe_boundary(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    from hermes_orchestrator.checkpoints import (
        CheckpointRequests,
        CheckpointSafetyStore,
    )

    events = EventStore(database)
    safety = CheckpointSafetyStore(database, events)
    checkpoints = CheckpointRequests(database, events)
    admit(queue, "ENG-9")
    cells = ProjectCellService(
        database=database,
        events=events,
        queue=queue,
        profiles=profiles,
        runner=RecordingRunner(),
        linear=RecordingLinear(),
        project_paths={"demo": Path("/tmp/demo")},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        safety=safety,
        checkpoints=checkpoints,
    )
    result = await cells.dispatch("ENG-9")
    assert result.status == "working"
    evidence = safety.current("cell-demo", str(SESSION_ID))
    assert evidence is not None
    assert evidence.boundary_kind == "turn_completed"
    assert database.scalar(
        "SELECT count(*) FROM events WHERE event_id = ?", (evidence.evidence_id,)
    ) == 1

    # New work on the same cell invalidates the boundary until the turn ends.
    admit(queue, "ENG-10")
    seen: list[bool] = []
    original = cells.record

    def spy(cell: object, event: object) -> str:
        seen.append(safety.current("cell-demo", str(SESSION_ID)) is not None)
        return original(cell, event)  # type: ignore[arg-type]

    cells.record = spy  # type: ignore[method-assign]
    await cells.dispatch("ENG-10")
    assert seen and not any(seen)
    assert safety.current("cell-demo", str(SESSION_ID)) is not None


class ContextRunner(RecordingRunner):
    """Emits usage-bearing assistant events and optional context signals."""

    def __init__(self) -> None:
        super().__init__()
        self.usage_tokens: list[int] = []
        self.compact = False
        self.context_error = False

    async def _events(self, request: LeadTurnRequest) -> AsyncIterator[ClaudeEvent]:
        yield ClaudeEvent(
            kind="session.started",
            original_type="system",
            session_id=request.session_id,
            parent_tool_use_id=None,
            timestamp="2026-08-26T12:00:00Z",
            usage={},
        )
        for tokens in self.usage_tokens:
            yield ClaudeEvent(
                kind="stream.assistant",
                original_type="assistant",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:00:01Z",
                usage={"input_tokens": tokens},
            )
        if self.compact:
            yield ClaudeEvent(
                kind="context.compacted",
                original_type="system",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:00:02Z",
                usage={},
            )
        if self.context_error:
            yield ClaudeEvent(
                kind="context.error",
                original_type="result",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:00:03Z",
                usage={},
                error_code="context_window",
            )


def context_cells(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: ContextRunner,
    *,
    clock: list[datetime],
) -> tuple[ProjectCellService, HandoffService]:
    from hermes_orchestrator.config import PolicyConfig
    from hermes_orchestrator.context import ActiveTimeTracker, ContextMonitor

    events = EventStore(database)
    handoffs = HandoffService(database, request_ids=lambda: f"req-{len(clock)}")
    cells = ProjectCellService(
        database=database,
        events=events,
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=RecordingLinear(),
        project_paths={"demo": Path("/tmp/demo")},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        handoffs=handoffs,
        context=ContextMonitor(database, events, policy=PolicyConfig()),
        active_time=ActiveTimeTracker(database),
        context_window_tokens=100_000,
        now=lambda: clock[-1],
    )
    return cells, handoffs


def handoff_reasons(database: Database) -> list[str]:
    return [
        str(row["reason"])
        for row in database.execute(
            "SELECT reason FROM handoff_requests ORDER BY requested_at, rowid"
        ).fetchall()
    ]


@pytest.mark.asyncio
async def test_context_prepare_requests_a_draft_without_stopping_work(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = ContextRunner()
    runner.usage_tokens = [72_000]  # 72% of a 100k window
    admit(queue, "ENG-9")
    cells, _ = context_cells(database, queue, profiles, runner, clock=clock)
    result = await cells.dispatch("ENG-9")
    assert result.status == "working"
    assert handoff_reasons(database) == ["context_prepare"]
    assert database.scalar(
        "SELECT state FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == "active"


@pytest.mark.asyncio
async def test_rotation_pending_waits_for_the_turn_boundary_then_rotates(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = ContextRunner()
    runner.usage_tokens = [85_000]
    admit(queue, "ENG-9")
    cells, _ = context_cells(database, queue, profiles, runner, clock=clock)
    result = await cells.dispatch("ENG-9")
    # The turn completed normally: that is the safe boundary.
    assert result.status == "handoff_required"
    assert handoff_reasons(database) == ["context_rotation"]
    assert database.scalar(
        "SELECT state FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == "handoff_required"


@pytest.mark.asyncio
async def test_context_error_requires_an_emergency_handoff(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = ContextRunner()
    runner.context_error = True
    admit(queue, "ENG-9")
    cells, _ = context_cells(database, queue, profiles, runner, clock=clock)
    result = await cells.dispatch("ENG-9")
    assert result.status == "handoff_required"
    assert handoff_reasons(database) == ["context_error"]


@pytest.mark.asyncio
async def test_six_active_hours_counts_execution_not_wall_time(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    from hermes_orchestrator.context import ActiveTimeTracker

    clock = [datetime(2026, 8, 26, 8, tzinfo=UTC)]
    runner = ContextRunner()
    admit(queue, "ENG-9")
    cells, _ = context_cells(database, queue, profiles, runner, clock=clock)
    # Simulate 5.5 active hours of earlier turns for this session, then a
    # 10-hour idle gap that must not count.
    tracker = ActiveTimeTracker(database)
    tracker.open(f"cell-demo:{SESSION_ID}", clock[0])
    tracker.idle(f"cell-demo:{SESSION_ID}", clock[0] + timedelta(hours=5, minutes=30))
    clock.append(clock[0] + timedelta(hours=15, minutes=30))
    result = await cells.dispatch("ENG-9")
    assert result.status == "working"
    assert handoff_reasons(database) == []
    # A further turn pushes active time past six hours; rotation waits for
    # the turn boundary and then requires the handoff.
    clock.append(clock[-1] + timedelta(minutes=40))
    admit(queue, "ENG-10")
    tracker.open(f"cell-demo:{SESSION_ID}", clock[-2])
    tracker.idle(f"cell-demo:{SESSION_ID}", clock[-1])
    result = await cells.dispatch("ENG-10")
    assert result.status == "handoff_required"
    assert handoff_reasons(database) == ["context_rotation"]


class FreezingContextRunner(ContextRunner):
    """Crosses the rotate threshold mid-turn, then tries to start subwork."""

    def __init__(self) -> None:
        super().__init__()
        self.freezes: list[tuple[UUID, str]] = []
        self.thaws: list[UUID] = []
        self.markers: set[UUID] = set()
        self.subagents_after: int = 1
        self.frozen_at_first_event: list[bool] = []

    def freeze_assignments(self, session_id: UUID, reason: str) -> None:
        self.freezes.append((session_id, reason))
        self.markers.add(session_id)

    def thaw_assignments(self, session_id: UUID) -> None:
        self.thaws.append(session_id)
        self.markers.discard(session_id)

    def assignments_frozen(self, session_id: UUID) -> bool:
        return session_id in self.markers

    async def _events(self, request: LeadTurnRequest) -> AsyncIterator[ClaudeEvent]:
        self.frozen_at_first_event.append(request.session_id in self.markers)
        yield ClaudeEvent(
            kind="session.started",
            original_type="system",
            session_id=request.session_id,
            parent_tool_use_id=None,
            timestamp="2026-08-26T12:00:00Z",
            usage={},
        )
        yield ClaudeEvent(
            kind="subagent.started",
            original_type="assistant",
            session_id=request.session_id,
            parent_tool_use_id="toolu-before",
            timestamp="2026-08-26T12:00:01Z",
            usage={"input_tokens": 40_000},
        )
        for tokens in self.usage_tokens:
            yield ClaudeEvent(
                kind="stream.assistant",
                original_type="assistant",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:00:02Z",
                usage={"input_tokens": tokens},
            )
        for index in range(self.subagents_after):
            yield ClaudeEvent(
                kind="subagent.started",
                original_type="assistant",
                session_id=request.session_id,
                parent_tool_use_id=f"toolu-after-{index}",
                timestamp="2026-08-26T12:00:03Z",
                usage={"input_tokens": 86_000},
            )
        if self.emit_handoff_ack:
            yield ClaudeEvent(
                kind="handoff.acknowledged",
                original_type="result",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:02:00Z",
                usage={},
                restated_next_action="Run the failing unit test and correct ENG-9.",
            )


@pytest.mark.asyncio
async def test_rotation_pending_freezes_new_subwork_until_the_boundary(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = FreezingContextRunner()
    runner.usage_tokens = [85_000]  # crosses 80% of the 100k window mid-turn
    admit(queue, "ENG-9")
    cells, _ = context_cells(database, queue, profiles, runner, clock=clock)

    result = await cells.dispatch("ENG-9")

    # The subagent that started before the threshold is leased; the one
    # attempted after the transition is refused and gets no lease.
    leases = [
        str(row["lease_id"])
        for row in database.execute(
            "SELECT lease_id FROM worker_leases ORDER BY acquired_at, rowid"
        ).fetchall()
    ]
    assert leases == ["toolu-before"]
    rejected = database.execute(
        "SELECT payload_json FROM events WHERE event_type = 'subagent.rejected'"
    ).fetchall()
    assert len(rejected) == 1
    assert "toolu-after-0" in str(rejected[0]["payload_json"])
    # The runner's control path was told to freeze this exact session, once.
    assert [session for session, _ in runner.freezes] == [SESSION_ID]
    assert runner.freezes[0][1].startswith("rotation_pending:")
    frozen_events = database.scalar(
        "SELECT count(*) FROM events WHERE event_type = 'assignments.frozen'"
    )
    assert frozen_events == 1
    # Existing work reached its safe boundary (the turn completed), so the
    # acknowledged handoff flow is invoked.
    assert result.status == "handoff_required"
    assert handoff_reasons(database) == ["context_rotation"]
    assert database.scalar(
        "SELECT state FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == "handoff_required"


@pytest.mark.asyncio
async def test_rotation_thaws_the_retired_session(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    from hermes_orchestrator.handoffs import HandoffDocument, HandoffTest

    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = FreezingContextRunner()
    runner.usage_tokens = [85_000]
    admit(queue, "ENG-9")
    cells, handoffs = context_cells(database, queue, profiles, runner, clock=clock)
    assert (await cells.dispatch("ENG-9")).status == "handoff_required"
    document = HandoffDocument(
        cell_id="cell-demo",
        objective="o",
        status="s",
        decisions=["d"],
        branch="b",
        commits=["c"],
        pull_request="pr",
        modified_files=["f"],
        tests=[HandoffTest(command="t", outcome="ok")],
        blockers=[],
        remaining_steps=["r"],
        commands=["cmd"],
        environment_notes=["e"],
        risks=[],
        next_action="n",
    )
    record = handoffs.submit(document)
    runner.emit_handoff_ack = True
    runner.usage_tokens = []
    runner.subagents_after = 0
    rotated = await cells.rotate("cell-demo", record.handoff_id)
    assert rotated.session_id != SESSION_ID
    assert runner.thaws == [SESSION_ID]


def event_count(database: Database, event_type: str) -> int:
    return int(
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = ?", (event_type,)
        )  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_restart_reconstructs_the_freeze_marker_before_any_assignment(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    """Durable rotation_pending with a missing marker is restored on resume."""

    from hermes_orchestrator.config import PolicyConfig
    from hermes_orchestrator.context import ContextMonitor, ContextSignal

    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = FreezingContextRunner()
    runner.usage_tokens = []
    admit(queue, "ENG-9")
    cells, _ = context_cells(database, queue, profiles, runner, clock=clock)
    # A prior process persisted rotation_pending for this session and
    # stopped before the marker was written: no marker, no freeze event.
    monitor = ContextMonitor(database, EventStore(database), policy=PolicyConfig())
    decision = monitor.record(
        ContextSignal(
            worker_id=f"cell-demo:{SESSION_ID}", at=clock[0], percent=85.0
        )
    )
    assert decision.state == "rotation_pending"
    assert runner.markers == set()
    assert event_count(database, "assignments.frozen") == 0

    result = await cells.dispatch("ENG-9")

    # The marker existed before the lead emitted its first event, so the
    # PreToolUse gate blocks Agent calls from the very start; the
    # subagent attempted afterwards is refused with no lease.
    assert runner.frozen_at_first_event == [True]
    assert [session for session, _ in runner.freezes] == [SESSION_ID]
    assert runner.freezes[0][1].startswith("rotation_pending:")
    leases = [
        str(row["lease_id"])
        for row in database.execute("SELECT lease_id FROM worker_leases").fetchall()
    ]
    assert leases == []
    assert event_count(database, "subagent.rejected") == 2
    assert event_count(database, "assignments.frozen") == 1
    # Existing work still reaches its safe boundary, then hands off.
    assert result.status == "handoff_required"
    assert handoff_reasons(database) == ["context_rotation"]


@pytest.mark.asyncio
async def test_freeze_reconciliation_is_idempotent(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    from hermes_orchestrator.cells import ProjectCell
    from hermes_orchestrator.config import PolicyConfig
    from hermes_orchestrator.context import ContextMonitor, ContextSignal

    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = FreezingContextRunner()
    admit(queue, "ENG-9")
    cells, _ = context_cells(database, queue, profiles, runner, clock=clock)
    cell = ProjectCell(
        cell_id="cell-demo",
        project_key="demo",
        state="active",
        profile_alias="max-a",
        session_id=SESSION_ID,
    )
    # Healthy sessions are never frozen by reconciliation.
    assert cells.reconcile_freeze(cell) is False
    assert runner.freezes == []
    monitor = ContextMonitor(database, EventStore(database), policy=PolicyConfig())
    monitor.record(
        ContextSignal(worker_id=f"cell-demo:{SESSION_ID}", at=clock[0], percent=85.0)
    )
    for _ in range(3):
        assert cells.reconcile_freeze(cell) is True
    assert len(runner.freezes) == 1
    assert event_count(database, "assignments.frozen") == 1
    assert event_count(database, "context.rotation_pending") == 1
    assert handoff_reasons(database) == []
    # A lost marker is restored exactly once more, again without duplicating
    # transition events or handoff requests.
    runner.markers.clear()
    assert cells.reconcile_freeze(cell) is True
    assert cells.reconcile_freeze(cell) is True
    assert len(runner.freezes) == 2
    assert event_count(database, "assignments.frozen") == 2
    assert event_count(database, "context.rotation_pending") == 1
    assert handoff_reasons(database) == []


# INFRA-188: only the latest eligible individual assistant invocation may
# drive the context percentage — never a cumulative top-level result and
# never forwarded child usage. A false percent poisons the sticky monitor
# into a false rotation, so eligibility must be structural.


class ScriptedContextRunner(ContextRunner):
    """Emits session.started, then an exact scripted event sequence."""

    def __init__(self) -> None:
        super().__init__()
        self.script: list[ClaudeEvent] = []

    async def _events(self, request: LeadTurnRequest) -> AsyncIterator[ClaudeEvent]:
        yield ClaudeEvent(
            kind="session.started",
            original_type="system",
            session_id=request.session_id,
            parent_tool_use_id=None,
            timestamp="2026-08-26T12:00:00Z",
            usage={},
        )
        for event in self.script:
            yield event


def _stream_event(
    *,
    kind: str,
    original_type: str,
    parent_tool_use_id: str | None = None,
    usage: dict[str, int] | None = None,
    error_code: str | None = None,
) -> ClaudeEvent:
    return ClaudeEvent(
        kind=kind,
        original_type=original_type,
        session_id=SESSION_ID,
        parent_tool_use_id=parent_tool_use_id,
        timestamp="2026-08-26T12:00:01Z",
        usage=usage or {},
        error_code=error_code,
    )


def _context_evidence(database: Database) -> tuple[str, float | None]:
    row = database.execute(
        "SELECT state, last_percent FROM context_evidence "
        f"WHERE worker_id = 'cell-demo:{SESSION_ID}'"
    ).fetchone()
    assert row is not None
    percent = None if row["last_percent"] is None else float(row["last_percent"])
    return str(row["state"]), percent


@pytest.mark.asyncio
async def test_cumulative_result_usage_never_drives_rotation(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = ScriptedContextRunner()
    # One modest real invocation, then the terminal result whose usage is
    # the cumulative run total (all invocations plus children): far beyond
    # the window, but it says nothing about live context occupancy.
    runner.script = [
        _stream_event(
            kind="stream.assistant",
            original_type="assistant",
            usage={"input_tokens": 30_000},
        ),
        _stream_event(
            kind="stream.result",
            original_type="result",
            usage={
                "input_tokens": 40_000,
                "cache_read_input_tokens": 500_000,
                "output_tokens": 9_000,
            },
        ),
    ]
    admit(queue, "ENG-9")
    cells, _ = context_cells(database, queue, profiles, runner, clock=clock)
    result = await cells.dispatch("ENG-9")
    assert result.status == "working"
    assert handoff_reasons(database) == []
    assert _context_evidence(database) == ("healthy", 30.0)


@pytest.mark.asyncio
async def test_forwarded_child_usage_never_drives_rotation(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = ScriptedContextRunner()
    # A child worker's final usage forwarded inside a top-level tool-result
    # record: it describes the child's context, never this session's.
    runner.script = [
        _stream_event(
            kind="stream.user",
            original_type="user",
            usage={"input_tokens": 95_000, "cache_read_input_tokens": 4_000},
        ),
    ]
    admit(queue, "ENG-9")
    cells, _ = context_cells(database, queue, profiles, runner, clock=clock)
    result = await cells.dispatch("ENG-9")
    assert result.status == "working"
    assert handoff_reasons(database) == []
    assert _context_evidence(database) == ("healthy", None)


@pytest.mark.asyncio
async def test_child_assistant_usage_never_drives_rotation(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = ScriptedContextRunner()
    runner.script = [
        _stream_event(
            kind="stream.assistant",
            original_type="assistant",
            parent_tool_use_id="toolu-child",
            usage={"input_tokens": 95_000},
        ),
    ]
    admit(queue, "ENG-9")
    cells, _ = context_cells(database, queue, profiles, runner, clock=clock)
    result = await cells.dispatch("ENG-9")
    assert result.status == "working"
    assert handoff_reasons(database) == []
    assert _context_evidence(database) == ("healthy", None)


@pytest.mark.asyncio
async def test_latest_assistant_invocation_drives_percent(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = ScriptedContextRunner()
    # The latest eligible individual invocation (72%) sets the percentage;
    # the cumulative terminal result must not escalate it to a rotation.
    runner.script = [
        _stream_event(
            kind="stream.assistant",
            original_type="assistant",
            usage={"input_tokens": 40_000},
        ),
        _stream_event(
            kind="stream.assistant",
            original_type="assistant",
            usage={"input_tokens": 72_000},
        ),
        _stream_event(
            kind="stream.result",
            original_type="result",
            usage={"input_tokens": 400_000, "cache_read_input_tokens": 90_000},
        ),
    ]
    admit(queue, "ENG-9")
    cells, _ = context_cells(database, queue, profiles, runner, clock=clock)
    result = await cells.dispatch("ENG-9")
    assert result.status == "working"
    assert handoff_reasons(database) == ["context_prepare"]
    assert _context_evidence(database) == ("prepare", 72.0)


@pytest.mark.asyncio
async def test_synthetic_limit_assistant_usage_never_drives_rotation(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = ScriptedContextRunner()
    # A synthetic subscription-limit notice is not a real invocation even
    # when it arrives shaped as an assistant record with usage attached.
    runner.script = [
        _stream_event(
            kind="provider.limit",
            original_type="assistant",
            usage={"input_tokens": 90_000},
            error_code="subscription_limit",
        ),
    ]
    admit(queue, "ENG-9")
    cells, _ = context_cells(database, queue, profiles, runner, clock=clock)
    result = await cells.dispatch("ENG-9")
    # The limit itself requires a handoff (journaled with its own reason,
    # without a rotation handoff request); context evidence must stay clean
    # so the replacement session does not inherit a false rotation state.
    assert result.status == "handoff_required"
    assert handoff_reasons(database) == []
    reasons = [
        json.loads(str(row["payload_json"]))["reason"]
        for row in database.execute(
            "SELECT payload_json FROM events "
            "WHERE event_type = 'project_cell.handoff_required'"
        ).fetchall()
    ]
    assert reasons == ["subscription_limit"]
    assert _context_evidence(database) == ("healthy", None)


@pytest.mark.asyncio
async def test_top_level_subagent_start_usage_still_counts(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = ScriptedContextRunner()
    # An assistant invocation that starts a subagent is still this
    # session's own invocation: its usage is real context occupancy and
    # the 80% rotation behavior must be preserved.
    runner.script = [
        _stream_event(
            kind="subagent.started",
            original_type="assistant",
            usage={"input_tokens": 86_000},
        ),
    ]
    admit(queue, "ENG-9")
    cells, _ = context_cells(database, queue, profiles, runner, clock=clock)
    result = await cells.dispatch("ENG-9")
    assert result.status == "handoff_required"
    assert handoff_reasons(database) == ["context_rotation"]
    assert _context_evidence(database)[1] == 86.0
