from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from hermes_orchestrator.cells import (
    ProfileCapacityEvidence,
    ProjectCell,
    ProjectCellService,
    RotationBlocked,
    activate_admitted_issue,
)
from hermes_orchestrator.claude import (
    ClaudeEvent,
    ClaudeEventParser,
    LeadTurnRequest,
)
from hermes_orchestrator.cmux_surfaces import SKIP_PERMISSIONS_FLAG
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest, IssueState
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.handoffs import HandoffDocument, HandoffService, HandoffTest
from hermes_orchestrator.lead_assignments import LeadAssignments
from hermes_orchestrator.lead_wakes import TerminalWakeInput
from hermes_orchestrator.linear import LinearProjection
from hermes_orchestrator.operator_decisions import OperatorDecisions
from hermes_orchestrator.profiles import (
    ProfileHealth,
    ProfilePool,
    ProfileRegistry,
)
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.subagent_packets import SubagentPacket

SESSION_ID = UUID("11111111-1111-4111-8111-111111111111")
REPLACEMENT_SESSION = UUID("22222222-2222-4222-8222-222222222222")
THIRD_SESSION = UUID("33333333-3333-4333-8333-333333333333")
FOURTH_SESSION = UUID("44444444-4444-4444-8444-444444444444")


class RecordingRunner:
    def __init__(self) -> None:
        self.start_count = 0
        self.resume_count = 0
        self.emit_session_started = True
        self.emit_limit = False
        self.limit_kind: str | None = None
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
                limit_kind=self.limit_kind,
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
    """A ``LinearProjector`` test double.

    INFRA-199 v2: Linear is never consulted before the local activation
    commit, so ``validate`` is no longer part of the shared contract
    (``cells.LinearProjector``) — this fake keeps the method only so
    any test that still constructs it directly compiles; nothing in
    ``cells.py`` calls it any more, so ``validations`` should stay
    empty across every dispatch path exercised here.
    """

    def __init__(self) -> None:
        self.targets: list[tuple[str, str | None, str]] = []
        self.validations: list[tuple[str, str]] = []
        self.effect_ids: list[str] = []

    async def validate(self, project_key: str, issue_id: str) -> object:
        self.validations.append((project_key, issue_id))
        return object()

    async def project(
        self,
        issue_id: str,
        target: LinearProjection,
        effect_id: str,
    ) -> object:
        # One stable, permanent effect id per issue — no epoch suffix:
        # local activation is authoritative and never re-projects under
        # a bumped id (the removed compensation machinery used to).
        assert effect_id == f"linear:{issue_id}:in-development:v2"
        self.effect_ids.append(effect_id)
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
    # One issue in_development per project (INFRA-199): ENG-9 must leave
    # in_development before ENG-10 may start. Sharing the lead cell/session
    # sequentially is still legal; only concurrent in_development is not.
    queue.complete("ENG-9", reason="shipped", evidence="pr-merged")
    second = await cell_service.dispatch("ENG-10")

    assert first.cell_id == second.cell_id
    assert first.session_id == second.session_id == SESSION_ID
    assert runner.start_count == 1
    assert runner.resume_count == 1


@pytest.mark.asyncio
async def test_second_issue_refused_while_first_in_development(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
    linear: RecordingLinear,
    database: Database,
) -> None:
    admit(queue, "ENG-9")
    admit(queue, "ENG-10")

    first = await cell_service.dispatch("ENG-9")
    assert queue.get("ENG-9").state == IssueState.IN_DEVELOPMENT

    events_before = database.execute(
        "SELECT COUNT(*) AS n FROM events"
    ).fetchone()["n"]

    second = await cell_service.dispatch("ENG-10")

    assert second.status == "project_busy"
    assert second.cell_id is None
    assert second.session_id is None
    assert queue.get("ENG-10").state == IssueState.QUEUED
    assert ("demo", "ENG-10") not in linear.validations
    assert runner.start_count == 1
    assert runner.resume_count == 0
    events_after = database.execute(
        "SELECT COUNT(*) AS n FROM events"
    ).fetchone()["n"]
    assert events_after == events_before

    # Dispatching (resuming) the ALREADY in_development issue itself is
    # untouched by the guard: only a DIFFERENT queued issue is refused.
    resumed = await cell_service.dispatch("ENG-9")
    assert resumed.cell_id == first.cell_id
    assert resumed.session_id == first.session_id
    assert runner.resume_count == 1


@pytest.mark.asyncio
async def test_second_issue_refused_while_first_in_review(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
) -> None:
    """INFRA-211 failure reproduction: a lead that has already handed
    off to review still occupies the project — a distinct queued issue
    must never start or deliver an assignment while the first sits in
    review."""

    admit(queue, "ENG-9")
    admit(queue, "ENG-10")

    await cell_service.dispatch("ENG-9")
    queue.transition(
        "ENG-9", IssueState.REVIEW, actor="lead", reason="handed off"
    )

    second = await cell_service.dispatch("ENG-10")

    assert second.status == "project_busy"
    assert second.cell_id is None
    assert second.session_id is None
    assert queue.get("ENG-10").state == IssueState.QUEUED
    assert runner.resume_count == 0


@pytest.mark.asyncio
async def test_second_issue_dispatches_once_first_leaves_in_development(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
) -> None:
    admit(queue, "ENG-9")
    admit(queue, "ENG-10")

    await cell_service.dispatch("ENG-9")
    queue.complete("ENG-9", reason="shipped", evidence="pr-merged")

    second = await cell_service.dispatch("ENG-10")

    assert second.status != "project_busy"
    assert queue.get("ENG-10").state == IssueState.IN_DEVELOPMENT
    assert runner.resume_count == 1


@pytest.mark.asyncio
async def test_dispatch_never_pre_validates_linear_before_claude_starts(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
    linear: RecordingLinear,
) -> None:
    """INFRA-199 v2 (flipped O-era test): Linear never authorizes
    activation, so dispatch never consults it before starting the
    Claude lead — team/routing correctness is proven only by the
    post-commit projection, once the session and the local activation
    have already gone through."""

    admit(queue, "ENG-9")

    await cell_service.dispatch("ENG-9")

    assert linear.validations == []
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
async def test_dispatch_does_not_resurrect_an_already_completed_issue(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    tmp_path: Path,
) -> None:
    """``_create_cell``'s own transactional check refuses to resurrect
    an issue that completed by any other means before dispatch — e.g.
    a merge or an operator action recorded between admission and this
    dispatch pass — never a local activation racing ahead of Linear
    (INFRA-199 v2: there is no pre-commit Linear call left to race)."""

    admit(queue, "ENG-9")
    queue.complete(
        "ENG-9",
        reason="linear_completed",
        evidence="https://linear.example/ENG-9",
    )

    linear = RecordingLinear()
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
async def test_dispatch_survives_a_linear_projection_failure_after_local_activation(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    tmp_path: Path,
) -> None:
    """INFRA-199 v2 (flipped O-era test): the Linear projection is
    attempted only AFTER the shared activation transaction commits, so
    a failure there never rolls back the newly created cell/session or
    the issue's transition to ``in_development`` — Linear never
    authorizes activation. The durable pending trace this leaves in
    ``external_effects`` (not exercised by this in-memory fake) is
    reconciliation's job to surface, never a reason to fail the lead
    start or strand the queue."""

    class FailingLinear(RecordingLinear):
        async def project(
            self,
            issue_id: str,
            target: LinearProjection,
            effect_id: str,
        ) -> object:
            raise TimeoutError("Linear is unavailable")

    linear = FailingLinear()
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
    admit(queue, "ENG-9")

    result = await service.dispatch("ENG-9")

    assert result.status == "working"
    assert queue.get("ENG-9").state == IssueState.IN_DEVELOPMENT
    assert (
        database.scalar("SELECT state FROM project_cells WHERE cell_id = 'cell-demo'")
        == "active"
    )
    assert database.scalar("SELECT count(*) FROM profile_leases") == 1
    assert _issue_started_count(database) == 1
    # The fake raised before ever recording a target: the projection
    # attempt happened, but nothing about it is locally visible.
    assert linear.targets == []


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

        def _find_active_cell(  # type: ignore[no-untyped-def]
            self, project_key: str, lane_role: str = "development"
        ):
            if self.hide_existing:
                self.hide_existing = False
                return None
            return super()._find_active_cell(project_key, lane_role)

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
    # A second, in-flight dispatch of the SAME issue (e.g. a racing resume
    # after restart) must still be served once the lock frees up (INFRA-199
    # forbids starting a DIFFERENT queued issue while one is in_development,
    # not re-dispatching the one already in flight).
    second = asyncio.create_task(service.dispatch("ENG-9"))
    await asyncio.sleep(0)

    assert resume_entered.is_set() is False
    emit_session.set()
    first_result = await first
    second_result = await second

    assert first_result.status == "working"
    assert second_result.status == "working"
    assert resume_entered.is_set() is True
    assert queue.get("ENG-9").state.value == "in_development"
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
    runner.emit_limit = False

    # Re-dispatching the SAME (still in_development) issue is the resume
    # path the guard must leave untouched; it should still see the cell's
    # own handoff_required gate rather than a project-busy refusal.
    result = await cell_service.dispatch("ENG-9")

    assert result.status == "handoff_required"
    assert runner.resume_count == 0
    assert queue.get("ENG-9").state.value == "in_development"


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

    # New work on the same cell invalidates the boundary until the turn
    # ends. ENG-9 must leave in_development first (INFRA-199: one issue
    # in_development per project) before ENG-10 may start on this cell.
    queue.complete("ENG-9", reason="shipped", evidence="pr-merged")
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


class ReplacementSequenceRunner(RecordingRunner):
    """Return profile-specific replacement acknowledgement outcomes."""

    def __init__(
        self,
        *,
        capped: set[str],
        acknowledged: set[str],
        close_fails: set[str] | None = None,
    ) -> None:
        super().__init__()
        self._capped = capped
        self._acknowledged = acknowledged
        self._close_fails = close_fails or set()

    def start_lead(self, request: LeadTurnRequest) -> AsyncIterator[ClaudeEvent]:
        stream = super().start_lead(request)
        if request.profile_alias not in self._close_fails:
            return stream

        class CloseFailingStream:
            def __aiter__(self) -> AsyncIterator[ClaudeEvent]:
                return self

            async def __anext__(self) -> ClaudeEvent:
                return await stream.__anext__()

            async def aclose(self) -> None:
                await stream.aclose()  # type: ignore[attr-defined]
                raise RuntimeError("replacement stream cleanup failed")

        return CloseFailingStream()

    async def _events(self, request: LeadTurnRequest) -> AsyncIterator[ClaudeEvent]:
        yield ClaudeEvent(
            kind="session.started",
            original_type="system",
            session_id=request.session_id,
            parent_tool_use_id=None,
            timestamp="2026-08-26T12:00:00Z",
            usage={},
        )
        if request.output_schema is None:
            return
        if request.profile_alias in self._capped:
            yield ClaudeEvent(
                kind="provider.limit",
                original_type="result",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:01:00Z",
                usage={},
                error_code="subscription_limit",
                limit_kind="fable",
            )
            raise RuntimeError("replacement exited 1 after provider.limit")
        if request.profile_alias in self._acknowledged:
            yield ClaudeEvent(
                kind="handoff.acknowledged",
                original_type="result",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-26T12:02:00Z",
                usage={},
                restated_next_action="Continue the same INFRA-198 rotation.",
            )


def rotation_profiles(
    database: Database,
    registry: ProfileRegistry,
    *,
    now: datetime,
) -> ProfilePool:
    evidence = ProfileCapacityEvidence(database)
    pool = ProfilePool(registry, capacity_evidence=evidence, now=lambda: now)
    for profile in registry.profiles:
        pool.record_health(
            ProfileHealth(
                profile_alias=profile.alias,
                eligible=True,
                reason="eligible",
                last_checked_at=now,
            )
        )
    with database.transaction() as connection:
        for alias in ("max-b", "max-c", "max-d"):
            connection.execute(
                "INSERT INTO profile_capacity_observations("
                "profile_alias, model, state, source, observed_at, resets_at, detail"
                ") VALUES (?, 'fable', ?, 'operator_attestation', ?, ?, ?)",
                (
                    alias,
                    "capped" if alias == "max-b" else "available",
                    (now - timedelta(hours=1)).isoformat(),
                    (now - timedelta(minutes=1)).isoformat(),
                    "reset horizon passed" if alias == "max-b" else "available",
                ),
            )
    return pool


def submit_rotation_handoff(handoffs: HandoffService) -> str:
    return handoffs.submit(
        HandoffDocument(
            cell_id="cell-demo",
            objective="Complete INFRA-198 rotation acceptance",
            status="ready",
            decisions=["continue the same handoff across capped profiles"],
            branch="sol/infra-198-capped-replacement",
            commits=["pending"],
            pull_request="pending",
            modified_files=["src/hermes_orchestrator/cells.py"],
            tests=[
                HandoffTest(
                    command="pytest tests/test_cells.py",
                    outcome="pending",
                )
            ],
            blockers=[],
            remaining_steps=["acknowledge and transfer"],
            commands=["pytest tests/test_cells.py"],
            environment_notes=["test fixture"],
            risks=[],
            next_action="continue the same rotation",
        )
    ).handoff_id


def replacement_rotation_service(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    tmp_path: Path,
) -> tuple[ProjectCellService, HandoffService]:
    handoffs = HandoffService(database)
    sessions = iter((REPLACEMENT_SESSION, THIRD_SESSION, FOURTH_SESSION))
    cells = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=RecordingLinear(),
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        handoffs=handoffs,
        replacement_session_ids=lambda: next(sessions),
        now=lambda: datetime(2026, 8, 26, 12, tzinfo=UTC),
    )
    return cells, handoffs


def context_cells(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: ContextRunner,
    *,
    clock: list[datetime],
    completion_sink: object | None = None,
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
        completion_sink=completion_sink,
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
    # the turn boundary and then requires the handoff. ENG-9 must leave
    # in_development first (INFRA-199: one issue in_development per
    # project) before ENG-10 may start on this cell.
    queue.complete("ENG-9", reason="shipped", evidence="pr-merged")
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


@pytest.mark.asyncio
async def test_rotation_continues_to_the_next_profile_after_replacement_cap(
    database: Database,
    queue: QueueService,
    registry: ProfileRegistry,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 26, 12, tzinfo=UTC)
    profiles = rotation_profiles(database, registry, now=now)
    runner = ReplacementSequenceRunner(capped={"max-b"}, acknowledged={"max-c"})
    cells, handoffs = replacement_rotation_service(
        database, queue, profiles, runner, tmp_path
    )
    admit(queue, "ENG-9")
    assert (await cells.dispatch("ENG-9")).status == "working"
    handoff_id = submit_rotation_handoff(handoffs)

    rotated = await cells.rotate("cell-demo", handoff_id)

    assert rotated.profile_alias == "max-c"
    assert [
        request.profile_alias
        for request in runner.start_requests
        if request.output_schema is not None
    ] == ["max-b", "max-c"]
    observation = database.execute(
        "SELECT state, source, resets_at FROM profile_capacity_observations "
        "WHERE profile_alias = 'max-b' ORDER BY observation_id DESC LIMIT 1"
    ).fetchone()
    assert observation is not None
    assert (observation["state"], observation["source"]) == (
        "capped",
        "provider_limit",
    )
    assert datetime.fromisoformat(str(observation["resets_at"])) > now
    assert database.scalar(
        "SELECT COUNT(*) FROM events WHERE event_type = 'provider.limit' "
        "AND aggregate_id = 'cell-demo'"
    ) == 1


@pytest.mark.asyncio
async def test_immediate_rotation_retry_skips_the_durably_capped_profile(
    database: Database,
    queue: QueueService,
    registry: ProfileRegistry,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 26, 12, tzinfo=UTC)
    profiles = rotation_profiles(database, registry, now=now)
    for alias in ("max-c", "max-d"):
        profiles.record_health(
            ProfileHealth(
                profile_alias=alias,
                eligible=False,
                reason="temporarily unavailable",
                last_checked_at=now,
            )
        )
    runner = ReplacementSequenceRunner(capped={"max-b"}, acknowledged={"max-c"})
    cells, handoffs = replacement_rotation_service(
        database, queue, profiles, runner, tmp_path
    )
    admit(queue, "ENG-9")
    assert (await cells.dispatch("ENG-9")).status == "working"
    handoff_id = submit_rotation_handoff(handoffs)

    with pytest.raises(RotationBlocked, match=r"max-b.*capped"):
        await cells.rotate("cell-demo", handoff_id)
    restarted_profiles = ProfilePool(
        registry,
        capacity_evidence=ProfileCapacityEvidence(database),
        now=lambda: now,
    )
    for alias in ("max-a", "max-b", "max-c", "max-d"):
        restarted_profiles.record_health(
            ProfileHealth(
                profile_alias=alias,
                eligible=alias != "max-d",
                reason="available" if alias != "max-d" else "unavailable",
                last_checked_at=now,
            )
        )
    retry_runner = ReplacementSequenceRunner(capped=set(), acknowledged={"max-c"})
    retry_cells, _ = replacement_rotation_service(
        database, queue, restarted_profiles, retry_runner, tmp_path
    )

    rotated = await retry_cells.rotate("cell-demo", handoff_id)

    assert rotated.profile_alias == "max-c"
    assert [
        request.profile_alias
        for request in retry_runner.start_requests
        if request.output_schema is not None
    ] == ["max-c"]
    assert database.scalar(
        "SELECT COUNT(*) FROM profile_capacity_observations "
        "WHERE profile_alias = 'max-b' AND source = 'provider_limit'"
    ) == 1


@pytest.mark.asyncio
async def test_rotation_reports_every_cap_when_the_pool_is_exhausted(
    database: Database,
    queue: QueueService,
    registry: ProfileRegistry,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 26, 12, tzinfo=UTC)
    profiles = rotation_profiles(database, registry, now=now)
    runner = ReplacementSequenceRunner(
        capped={"max-b", "max-c", "max-d"}, acknowledged=set()
    )
    cells, handoffs = replacement_rotation_service(
        database, queue, profiles, runner, tmp_path
    )
    admit(queue, "ENG-9")
    assert (await cells.dispatch("ENG-9")).status == "working"
    handoff_id = submit_rotation_handoff(handoffs)

    with pytest.raises(RotationBlocked) as blocked:
        await cells.rotate("cell-demo", handoff_id)

    assert all(alias in str(blocked.value) for alias in ("max-b", "max-c", "max-d"))
    assert "capped" in str(blocked.value)
    assert [
        request.profile_alias
        for request in runner.start_requests
        if request.output_schema is not None
    ] == ["max-b", "max-c", "max-d"]
    assert database.scalar(
        "SELECT COUNT(*) FROM profile_capacity_observations "
        "WHERE source = 'provider_limit'"
    ) == 3
    assert handoffs.get(handoff_id).state == "submitted"


@pytest.mark.asyncio
async def test_replacement_cleanup_failure_stops_after_persisting_the_cap(
    database: Database,
    queue: QueueService,
    registry: ProfileRegistry,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 26, 12, tzinfo=UTC)
    profiles = rotation_profiles(database, registry, now=now)
    runner = ReplacementSequenceRunner(
        capped={"max-b"},
        acknowledged={"max-c"},
        close_fails={"max-b"},
    )
    cells, handoffs = replacement_rotation_service(
        database, queue, profiles, runner, tmp_path
    )
    admit(queue, "ENG-9")
    assert (await cells.dispatch("ENG-9")).status == "working"
    handoff_id = submit_rotation_handoff(handoffs)

    with pytest.raises(RuntimeError, match="stream cleanup failed"):
        await cells.rotate("cell-demo", handoff_id)

    assert [
        request.profile_alias
        for request in runner.start_requests
        if request.output_schema is not None
    ] == ["max-b"]
    assert database.scalar(
        "SELECT COUNT(*) FROM profile_capacity_observations "
        "WHERE profile_alias = 'max-b' AND source = 'provider_limit'"
    ) == 1
    assert handoffs.get(handoff_id).state == "submitted"
    assert database.scalar(
        "SELECT profile_alias FROM profile_leases WHERE project_key = 'demo'"
    ) == "max-a"


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


class RecordingSink:
    """Collects committed terminal wakes without any transport."""

    def __init__(self) -> None:
        self.wakes: list[TerminalWakeInput] = []

    def commit(self, wake: TerminalWakeInput) -> TerminalWakeInput:
        self.wakes.append(wake)
        return wake


def sink_cells(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
    *,
    cell_ids: object | None = None,
    completion_sink: object | None = None,
) -> tuple[ProjectCellService, RecordingSink]:
    sink = completion_sink if completion_sink is not None else RecordingSink()
    service = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=cell_ids or (lambda: "cell-demo"),
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
        completion_sink=sink,
    )
    return service, sink


def last_cell_event_id(database: Database) -> str:
    row = database.execute(
        "SELECT event_id FROM events WHERE aggregate_type = 'project_cell' "
        "ORDER BY sequence DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    return str(row["event_id"])


@pytest.mark.asyncio
async def test_completed_turn_commits_one_completed_wake(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    service, sink = sink_cells(database, queue, profiles, runner, linear, tmp_path)

    result = await service.dispatch("ENG-9")

    assert result.status == "working"
    assert len(sink.wakes) == 1
    wake = sink.wakes[0]
    assert wake.kind == "completed"
    assert wake.reason == "turn_completed"
    assert wake.project_key == "demo"
    assert wake.issue_id == "ENG-9"
    assert wake.cell_id == "cell-demo"
    assert wake.session_id == SESSION_ID
    assert wake.profile_alias == "max-a"
    assert wake.reset_at is None
    # The wake binds the turn's terminal evidence event.
    assert wake.turn_key == last_cell_event_id(database)


@pytest.mark.asyncio
async def test_each_turn_commits_a_distinct_wake(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    admit(queue, "ENG-10")
    service, sink = sink_cells(database, queue, profiles, runner, linear, tmp_path)

    await service.dispatch("ENG-9")
    # ENG-9 must leave in_development first (INFRA-199: one issue
    # in_development per project) before ENG-10 may start on this cell.
    queue.complete("ENG-9", reason="shipped", evidence="pr-merged")
    await service.dispatch("ENG-10")

    assert [wake.kind for wake in sink.wakes] == ["completed", "completed"]
    assert sink.wakes[0].turn_key != sink.wakes[1].turn_key


@pytest.mark.asyncio
async def test_provider_limit_commits_provider_capped_wake_with_reset(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_limit = True
    service, sink = sink_cells(database, queue, profiles, runner, linear, tmp_path)

    result = await service.dispatch("ENG-9")

    assert result.status == "handoff_required"
    assert len(sink.wakes) == 1
    wake = sink.wakes[0]
    assert wake.kind == "provider_capped"
    assert wake.reason == "subscription_limit"
    # The reset metadata is the durable lease cooldown, not a recomputation.
    assert wake.reset_at == "2026-08-26T01:00:00+00:00"


@pytest.mark.asyncio
async def test_start_capped_commits_provider_capped_wake(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_session_started = False
    runner.emit_limit = True
    service, sink = sink_cells(database, queue, profiles, runner, linear, tmp_path)

    result = await service.dispatch("ENG-9")

    assert result.status == "start_unconfirmed"
    assert [wake.kind for wake in sink.wakes] == ["provider_capped"]
    assert sink.wakes[0].reason == "subscription_limit"
    assert sink.wakes[0].reset_at == "2026-08-26T01:00:00+00:00"


@pytest.mark.asyncio
async def test_unconfirmed_start_commits_blocked_wake(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_session_started = False
    service, sink = sink_cells(database, queue, profiles, runner, linear, tmp_path)

    result = await service.dispatch("ENG-9")

    assert result.status == "start_unconfirmed"
    assert [wake.kind for wake in sink.wakes] == ["blocked"]
    assert sink.wakes[0].reason == "start_unconfirmed"
    assert sink.wakes[0].reset_at is None
    # The wake binds the durable start-failure evidence event, so a lost
    # wake reconstructs into exactly this identity.
    assert sink.wakes[0].turn_key == f"start_failed:{last_cell_event_id(database)}"


@pytest.mark.asyncio
async def test_repeated_failed_starts_each_commit_a_distinct_wake(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_session_started = False
    counter = iter(range(1, 10))
    service, sink = sink_cells(
        database,
        queue,
        profiles,
        runner,
        linear,
        tmp_path,
        cell_ids=lambda: f"cell-{next(counter)}",
    )

    await service.dispatch("ENG-9")
    await service.dispatch("ENG-9")

    # Every stuck attempt must wake Hermes; a constant fallback key would
    # deduplicate the second failure into invisibility.
    assert [wake.kind for wake in sink.wakes] == ["blocked", "blocked"]
    assert sink.wakes[0].turn_key != sink.wakes[1].turn_key


@pytest.mark.asyncio
async def test_sink_failure_does_not_destroy_the_turn_result(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    class ExplodingSink:
        def commit(self, wake: TerminalWakeInput) -> TerminalWakeInput:
            raise RuntimeError("outbox is unavailable")

    admit(queue, "ENG-9")
    service, _ = sink_cells(
        database,
        queue,
        profiles,
        runner,
        linear,
        tmp_path,
        completion_sink=ExplodingSink(),
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "working"
    failures = database.execute(
        "SELECT aggregate_id FROM events "
        "WHERE event_type = 'lead_wake.publish_failed'"
    ).fetchall()
    assert [str(row["aggregate_id"]) for row in failures] == ["cell-demo"]


@pytest.mark.asyncio
async def test_issue_completed_during_turn_commits_completed_wake(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    tmp_path: Path,
) -> None:
    """An explicit terminal reconciliation wins the race between the
    lead process starting and dispatch confirming its session — the
    local activation transaction re-proves the CAS at commit time and
    refuses, zero writes, exactly as if the candidate had never been
    runnable (INFRA-199 v2: there is no Linear call left in this path
    to race against, so the injection point moves to the lead stream
    itself, ahead of the very first ``session.started`` event)."""

    admit(queue, "ENG-9")

    service, sink = sink_cells(
        database,
        queue,
        profiles,
        _CompletingRunner(database),
        RecordingLinear(),
        tmp_path,
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "already_completed"
    assert [wake.kind for wake in sink.wakes] == ["completed"]
    assert sink.wakes[0].reason == "issue_already_completed"


@pytest.mark.asyncio
async def test_issue_completed_before_dispatch_commits_no_wake(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    tmp_path: Path,
) -> None:
    """INFRA-199 v2 (flipped O-era test): with no pre-commit Linear call
    left before cell creation, an issue completed by any other means
    before this dispatch pass is simply already done when
    ``_create_cell`` makes its own transactional check — no lead turn
    ever runs, so there is no terminal boundary to wake on."""

    admit(queue, "ENG-9")
    queue.complete(
        "ENG-9",
        reason="linear_completed",
        evidence="https://linear.example/ENG-9",
    )

    service, sink = sink_cells(
        database, queue, profiles, runner, RecordingLinear(), tmp_path
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "already_completed"
    assert sink.wakes == []


@pytest.mark.asyncio
async def test_context_rotation_commits_handoff_required_wake(
    database: Database, queue: QueueService, profiles: ProfilePool
) -> None:
    clock = [datetime(2026, 8, 26, 12, tzinfo=UTC)]
    runner = ContextRunner()
    runner.usage_tokens = [85_000]
    admit(queue, "ENG-9")
    sink = RecordingSink()
    cells, _ = context_cells(
        database, queue, profiles, runner, clock=clock, completion_sink=sink
    )

    result = await cells.dispatch("ENG-9")

    assert result.status == "handoff_required"
    assert [wake.kind for wake in sink.wakes] == ["handoff_required"]
    assert sink.wakes[0].reason == "context_rotation"
    assert sink.wakes[0].reset_at is None


@pytest.mark.asyncio
async def test_completed_turn_persists_one_durable_deduplicated_wake_row(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.lead_wakes import LeadTerminalWakes

    admit(queue, "ENG-9")
    events = EventStore(database)
    wakes = LeadTerminalWakes(database=database, events=events)
    service = ProjectCellService(
        database=database,
        events=events,
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
        completion_sink=wakes,
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "working"
    pending = wakes.pending("demo")
    assert len(pending) == 1
    assert pending[0].kind == "completed"
    assert pending[0].session_id == str(SESSION_ID)
    assert pending[0].state == "pending"


class CrashingSink:
    """Simulates losing the wake insert after the terminal state committed."""

    def commit(self, wake: TerminalWakeInput) -> TerminalWakeInput:
        raise RuntimeError("crashed before the outbox insert")


def repair_cells(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
    *,
    sink: object,
) -> ProjectCellService:
    from hermes_orchestrator.checkpoints import CheckpointSafetyStore

    events = EventStore(database)
    return ProjectCellService(
        database=database,
        events=events,
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
        safety=CheckpointSafetyStore(database, events),
        completion_sink=sink,
    )


def repair_pair(database: Database) -> tuple[object, object]:
    from hermes_orchestrator.lead_wakes import (
        LeadTerminalWakes,
        LeadWakeReconciler,
    )

    wakes = LeadTerminalWakes(database=database, events=EventStore(database))
    reconciler = LeadWakeReconciler(
        database=database, events=EventStore(database), wakes=wakes
    )
    return reconciler, wakes


def wake_rows(database: Database) -> list[dict[str, object]]:
    return [
        dict(row)
        for row in database.execute(
            "SELECT * FROM lead_terminal_wakes ORDER BY created_at, rowid"
        ).fetchall()
    ]


@pytest.mark.asyncio
async def test_lost_completed_wake_reconstructs_from_safety_evidence(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    service = repair_cells(
        database, queue, profiles, runner, linear, tmp_path, sink=CrashingSink()
    )

    result = await service.dispatch("ENG-9")

    # The wake insert failed, but the terminal result survived durably.
    assert result.status == "working"
    reconciler, wakes = repair_pair(database)
    assert wakes.pending() == ()

    reconstructed = reconciler.reconcile()

    assert [wake.kind for wake in reconstructed] == ["completed"]
    wake = reconstructed[0]
    evidence = database.execute(
        "SELECT evidence_id, session_id FROM checkpoint_safety "
        "WHERE cell_id = 'cell-demo'"
    ).fetchone()
    assert wake.turn_key == str(evidence["evidence_id"])
    assert wake.reason == "turn_completed"
    assert wake.project_key == "demo"
    assert wake.issue_id == "ENG-9"
    assert wake.cell_id == "cell-demo"
    assert wake.session_id == str(SESSION_ID)
    assert wake.profile_alias == "max-a"
    assert wake.state == "pending"
    # Repeated repair stays deduplicated on the same turn identity.
    assert reconciler.reconcile() == ()
    assert len(wake_rows(database)) == 1


@pytest.mark.asyncio
async def test_crash_before_wake_insert_recovers_exactly_once_on_restart(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.lead_wakes import LeadWakeDelivery

    class CrashingRunner(RecordingRunner):
        async def _events(
            self, request: LeadTurnRequest
        ) -> AsyncIterator[ClaudeEvent]:
            async for event in super()._events(request):
                yield event
            raise RuntimeError("lead process crashed")

    runner = CrashingRunner()
    runner.emit_limit = True
    admit(queue, "ENG-9")
    _, wakes = repair_pair(database)
    service = repair_cells(
        database, queue, profiles, runner, linear, tmp_path, sink=wakes
    )

    with pytest.raises(RuntimeError, match="lead process crashed"):
        await service.dispatch("ENG-9")

    # The terminal transition committed; the wake never reached the outbox.
    assert database.scalar(
        "SELECT state FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == "handoff_required"
    assert wake_rows(database) == []

    # Restart: a fresh runtime reconstructs and delivers exactly one wake.
    reopened = Database.open(tmp_path / "state.db")
    try:
        restart_reconciler, restart_wakes = repair_pair(reopened)
        reconstructed = restart_reconciler.reconcile()
        assert [wake.kind for wake in reconstructed] == ["provider_capped"]
        wake = reconstructed[0]
        evidence = reopened.execute(
            "SELECT event_id FROM events "
            "WHERE event_type = 'project_cell.handoff_required'"
        ).fetchone()
        assert wake.turn_key == f"handoff:{evidence['event_id']}"
        assert wake.reason == "subscription_limit"
        assert wake.reset_at == "2026-08-26T01:00:00+00:00"
        assert wake.issue_id == "ENG-9"

        transport = _RecordingWakeTransport()
        delivery = LeadWakeDelivery(restart_wakes, transport=transport)
        assert delivery.replay_startup() == 1
        assert await delivery.drain() == (wake.wake_id,)
        assert transport.delivered == [wake.wake_id]
        # Repeated repair and replay after delivery stay deduplicated.
        assert restart_reconciler.reconcile() == ()
        assert delivery.replay_startup() == 0
        assert len(wake_rows(reopened)) == 1
    finally:
        reopened.close()


class _RecordingWakeTransport:
    def __init__(self) -> None:
        self.delivered: list[str] = []

    async def __call__(self, wake: object) -> bool:
        self.delivered.append(wake.wake_id)  # type: ignore[attr-defined]
        return True


@pytest.mark.asyncio
async def test_lost_blocked_wake_reconstructs_from_start_failure_evidence(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_session_started = False
    service = repair_cells(
        database, queue, profiles, runner, linear, tmp_path, sink=CrashingSink()
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "start_unconfirmed"
    reconciler, _ = repair_pair(database)

    reconstructed = reconciler.reconcile()

    assert [wake.kind for wake in reconstructed] == ["blocked"]
    wake = reconstructed[0]
    evidence = database.execute(
        "SELECT event_id FROM events "
        "WHERE event_type = 'project_cell.start_failed'"
    ).fetchone()
    assert wake.turn_key == f"start_failed:{evidence['event_id']}"
    assert wake.reason == "start_unconfirmed"
    assert wake.issue_id == "ENG-9"
    assert wake.session_id == str(SESSION_ID)
    assert reconciler.reconcile() == ()
    assert len(wake_rows(database)) == 1


@pytest.mark.asyncio
async def test_reconcile_after_direct_commit_is_a_stable_noop(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    reconciler, wakes = repair_pair(database)
    service = repair_cells(
        database, queue, profiles, runner, linear, tmp_path, sink=wakes
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "working"
    assert len(wake_rows(database)) == 1
    # The direct path already published this boundary; repair must derive
    # the same turn identity and deduplicate into the existing row.
    assert reconciler.reconcile() == ()
    assert reconciler.reconcile() == ()
    assert len(wake_rows(database)) == 1


@pytest.mark.asyncio
async def test_reconcile_skips_evidence_from_a_rotated_session(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_limit = True
    service = repair_cells(
        database, queue, profiles, runner, linear, tmp_path, sink=CrashingSink()
    )
    result = await service.dispatch("ENG-9")
    assert result.status == "handoff_required"

    # The consumer already drove a rotation past this boundary: the cell
    # carries a replacement session, so the lost wake must stay lost.
    database.execute(
        "UPDATE project_cells SET state = 'active', "
        "session_id = '22222222-2222-4222-8222-222222222222' "
        "WHERE cell_id = 'cell-demo'"
    )
    reconciler, _ = repair_pair(database)

    assert reconciler.reconcile() == ()
    assert wake_rows(database) == []


@pytest.mark.asyncio
async def test_reconcile_never_resurrects_evidence_below_the_floor(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_limit = True
    service = repair_cells(
        database, queue, profiles, runner, linear, tmp_path, sink=CrashingSink()
    )
    await service.dispatch("ENG-9")

    # Evidence journaled before the repair schema existed sits below the
    # migration-recorded floor and is never manufactured into a wake.
    database.execute(
        "UPDATE lead_wake_repair SET floor_sequence = "
        "(SELECT coalesce(max(sequence), 0) FROM events)"
    )
    reconciler, _ = repair_pair(database)

    assert reconciler.reconcile() == ()
    assert wake_rows(database) == []


class _CompletingRunner(RecordingRunner):
    """Completes the issue via direct SQL before its ``session.started``
    is ever observed by dispatch — the local activation transaction's
    own CAS then refuses cleanly (INFRA-199 v2: there is no Linear call
    left in the activation path to race against any more, so the
    injection point is the lead stream itself, ahead of the first
    event dispatch ever reads)."""

    def __init__(self, database: Database, issue_id: str = "ENG-9") -> None:
        super().__init__()
        self._database = database
        self._issue_id = issue_id

    async def _events(
        self, request: LeadTurnRequest
    ) -> AsyncIterator[ClaudeEvent]:
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE admitted_issues SET state = 'done' WHERE issue_id = ?",
                (self._issue_id,),
            )
        async for event in super()._events(request):
            yield event


def already_completed_evidence_key(database: Database) -> str:
    row = database.execute(
        "SELECT event_id FROM events "
        "WHERE event_type = 'project_cell.issue_already_completed' "
        "ORDER BY sequence DESC LIMIT 1"
    ).fetchone()
    assert row is not None, "already_completed terminal evidence was journaled"
    return f"already_completed:{row['event_id']}"


@pytest.mark.asyncio
async def test_lost_already_completed_wake_reconstructs_and_pushes_on_restart(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.lead_wakes import LeadWakeDelivery

    admit(queue, "ENG-9")
    service = repair_cells(
        database,
        queue,
        profiles,
        _CompletingRunner(database),
        RecordingLinear(),
        tmp_path,
        sink=CrashingSink(),
    )

    result = await service.dispatch("ENG-9")

    # The already_completed result is authoritative; only the wake was lost.
    assert result.status == "already_completed"
    assert wake_rows(database) == []

    # Restart: repair derives the wake from durable terminal evidence alone,
    # without any redispatch, and pushes exactly one completed wake.
    reopened = Database.open(tmp_path / "state.db")
    try:
        restart_reconciler, restart_wakes = repair_pair(reopened)
        reconstructed = restart_reconciler.reconcile()
        assert [wake.kind for wake in reconstructed] == ["completed"]
        wake = reconstructed[0]
        assert wake.reason == "issue_already_completed"
        assert wake.turn_key == already_completed_evidence_key(reopened)
        assert wake.project_key == "demo"
        assert wake.issue_id == "ENG-9"
        assert wake.cell_id == "cell-demo"
        assert wake.session_id == str(SESSION_ID)
        assert wake.profile_alias == "max-a"
        assert wake.state == "pending"

        transport = _RecordingWakeTransport()
        delivery = LeadWakeDelivery(restart_wakes, transport=transport)
        assert delivery.replay_startup() == 1
        assert await delivery.drain() == (wake.wake_id,)
        assert transport.delivered == [wake.wake_id]
        # Repeated repair and replay after acknowledgement neither
        # duplicate the wake nor redeliver it.
        assert restart_reconciler.reconcile() == ()
        assert delivery.replay_startup() == 0
        assert await delivery.drain() == ()
        assert transport.delivered == [wake.wake_id]
        assert len(wake_rows(reopened)) == 1
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_direct_already_completed_wake_matches_reconstruction_identity(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    reconciler, wakes = repair_pair(database)
    service = repair_cells(
        database,
        queue,
        profiles,
        _CompletingRunner(database),
        RecordingLinear(),
        tmp_path,
        sink=wakes,
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "already_completed"
    rows = wake_rows(database)
    assert [str(row["kind"]) for row in rows] == ["completed"]
    assert str(rows[0]["reason"]) == "issue_already_completed"
    # The direct wake binds the same durable evidence identity repair
    # derives, so the two paths deduplicate into one row.
    assert str(rows[0]["turn_key"]) == already_completed_evidence_key(database)
    assert reconciler.reconcile() == ()
    assert reconciler.reconcile() == ()
    assert len(wake_rows(database)) == 1


@pytest.mark.asyncio
async def test_resumed_cell_already_completed_wake_reconstructs_after_crash(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    admit(queue, "ENG-9")
    reconciler, wakes = repair_pair(database)
    first = repair_cells(
        database, queue, profiles, runner, linear, tmp_path, sink=wakes
    )
    assert (await first.dispatch("ENG-9")).status == "working"
    assert [str(row["kind"]) for row in wake_rows(database)] == ["completed"]
    queue.complete(
        "ENG-9",
        reason="linear_completed",
        evidence="https://linear.example/ENG-9",
    )

    second = repair_cells(
        database, queue, profiles, runner, linear, tmp_path, sink=CrashingSink()
    )
    result = await second.dispatch("ENG-9")

    # The resumed turn observed the completed issue but lost its wake.
    assert result.status == "already_completed"
    assert len(wake_rows(database)) == 1

    reconstructed = reconciler.reconcile()

    assert [wake.kind for wake in reconstructed] == ["completed"]
    wake = reconstructed[0]
    assert wake.reason == "issue_already_completed"
    assert wake.turn_key == already_completed_evidence_key(database)
    assert wake.issue_id == "ENG-9"
    assert wake.session_id == str(SESSION_ID)
    # The first turn's delivered boundary stays deduplicated; only the
    # lost already_completed wake is reconstructed.
    assert reconciler.reconcile() == ()
    assert len(wake_rows(database)) == 2


@pytest.mark.asyncio
async def test_confirmed_session_gets_exactly_one_lead_seat(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    class RecordingSeater:
        def __init__(self) -> None:
            self.ensured: list[dict[str, object]] = []

        async def ensure(self, **identity: object) -> object:
            self.ensured.append(dict(identity))
            return object()

    seater = RecordingSeater()
    admit(queue, "ENG-9")
    events = EventStore(database)
    service = ProjectCellService(
        database=database,
        events=events,
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
        surfaces=seater,
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "working"
    # The seat activates exactly once, before the lead process launch.
    assert seater.ensured == [
        {
            "project_key": "demo",
            "cell_id": "cell-demo",
            "session_id": str(SESSION_ID),
            "profile_alias": "max-a",
            "issue_id": "ENG-9",
            "classic_command": None,
        }
    ]


@pytest.mark.asyncio
async def test_failed_seat_activation_never_launches_a_hidden_lead(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    class ExplodingSeater:
        async def ensure(self, **identity: str) -> None:
            raise RuntimeError("cmux socket denied this process")

    admit(queue, "ENG-9")
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
        surfaces=ExplodingSeater(),
    )

    result = await service.dispatch("ENG-9")

    # With terminal visibility configured, a lead without its visible
    # seat would be a hidden process: the launch is refused entirely and
    # the failed start releases the claim.
    assert result.status == "seat_failed"
    assert runner.start_count == 0
    assert runner.resume_count == 0
    failures = database.execute(
        "SELECT aggregate_id FROM events WHERE event_type = 'cmux.seat_failed'"
    ).fetchall()
    assert [str(row["aggregate_id"]) for row in failures] == ["cell-demo"]
    cell_state = database.scalar(
        "SELECT state FROM project_cells WHERE cell_id = 'cell-demo'"
    )
    assert str(cell_state) == "failed"


@pytest.mark.asyncio
async def test_classic_seat_dispatch_never_spawns_a_stream_shadow(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    class RecordingSeater:
        def __init__(self) -> None:
            self.ensured: list[dict[str, object]] = []

        async def ensure(self, **identity: object) -> object:
            self.ensured.append(dict(identity))
            return object()

    seater = RecordingSeater()
    admit(queue, "ENG-9")
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
        surfaces=seater,
        classic_seats=True,
    )

    result = await service.dispatch("ENG-9")

    # The pane itself runs the classic interactive lead: the daemon
    # projects Linear, activates the issue, and launches nothing.
    assert result.status == "seated"
    assert runner.start_count == 0
    assert runner.resume_count == 0
    [ensured] = seater.ensured
    assert ensured["classic_command"] == (
        f"claude --session-id {SESSION_ID} {SKIP_PERMISSIONS_FLAG}"
    )
    assert ("ENG-9", "In Development", "operator") in linear.targets
    cell_state = database.scalar(
        "SELECT state FROM project_cells WHERE cell_id = 'cell-demo'"
    )
    assert str(cell_state) == "active"


@pytest.mark.asyncio
async def test_classic_dispatch_commits_a_bound_assignment_packet(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    """INFRA-195: the queue transition and the versioned assignment
    packet commit in one transaction, bound to the exact project,
    issue, cell, session, profile, instruction id, and transition; a
    re-dispatch is a durable no-op with no duplicate packet."""

    class RecordingSeater:
        async def ensure(self, **identity: object) -> object:
            return object()

    events = EventStore(database)
    assignments = LeadAssignments(database, events=events)
    committed: list[object] = []
    assignments.subscribe(committed.append)
    admit(queue, "ENG-9")
    service = ProjectCellService(
        database=database,
        events=events,
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
        surfaces=RecordingSeater(),
        classic_seats=True,
        assignments=assignments,
    )

    result = await service.dispatch("ENG-9")

    assert result.status == "seated"
    [packet] = assignments.pending_for_session(str(SESSION_ID))
    assert packet.project_key == "demo"
    assert packet.issue_id == "ENG-9"
    assert packet.cell_id == "cell-demo"
    assert packet.profile_alias == "max-a"
    assert packet.instruction_id == "chat-ENG-9"
    assert packet.queue_transition == "queued->in_development"
    assert committed == [packet]

    again = await service.dispatch("ENG-9")

    assert again.status == "seated"
    assert database.scalar("SELECT COUNT(*) FROM lead_assignments") == 1
    assert committed == [packet]


@pytest.mark.asyncio
async def test_dispatch_waits_while_an_operator_decision_is_pending(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    """awaiting_operator_decision is enforceable state, not narrative.

    While a decision is pending for the issue, dispatch performs zero
    work — no lead start, no Linear projection, no cell row. An
    explicit application command lifts the block; nothing else does.
    """

    decisions = OperatorDecisions(database)
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
        decisions=decisions,
    )
    admit(queue, "ENG-90")
    decisions.record_pending(
        decision_id="dec-arch-1",
        issue_id="ENG-90",
        project_key="demo",
        cell_id="cell-demo",
        session_id=str(SESSION_ID),
        choice="channel_pilot",
    )

    blocked = await service.dispatch("ENG-90")

    assert blocked.status == "awaiting_operator_decision"
    assert runner.start_count == 0
    assert linear.targets == []
    assert database.scalar("SELECT COUNT(*) FROM project_cells") == 0

    decisions.apply(
        decision_id="dec-arch-1",
        status="approved",
        source_message="Let's try that dedicated channel.",
    )
    resumed = await service.dispatch("ENG-90")

    assert resumed.status == "working"
    assert runner.start_count == 1


# INFRA-186 P4: exactly-once worker-lease release and packet settlement
# when a subagent reaches a terminal event (subagent.completed/failed).


def _terminal_cell() -> ProjectCell:
    return ProjectCell(
        cell_id="cell-demo",
        project_key="demo",
        state="active",
        profile_alias="max-a",
        session_id=SESSION_ID,
    )


def _started_event(tool_use_id: str) -> ClaudeEvent:
    return ClaudeEvent(
        kind="subagent.started",
        original_type="assistant",
        session_id=SESSION_ID,
        parent_tool_use_id=tool_use_id,
        timestamp="2026-08-29T12:00:00Z",
        usage={},
    )


def _terminal_event(
    kind: str,
    tool_use_id: str | None,
    *,
    packet_id: str | None = None,
) -> ClaudeEvent:
    return ClaudeEvent(
        kind=kind,
        original_type="result",
        session_id=SESSION_ID,
        parent_tool_use_id=tool_use_id,
        timestamp="2026-08-29T12:00:01Z",
        usage={},
        packet_id=packet_id,
    )


def _lease_state(database: Database, lease_id: str) -> str | None:
    row = database.execute(
        "SELECT state FROM worker_leases WHERE lease_id = ?",
        (lease_id,),
    ).fetchone()
    return None if row is None else str(row["state"])


def _reserved_packet(database: Database, *, tool_use_id: str) -> SubagentPacket:
    from hermes_orchestrator.subagent_packets import SubagentPackets

    packets = SubagentPackets(database, events=EventStore(database))
    packet = packets.create(
        issue_id="INFRA-186",
        project_key="demo",
        cell_id="cell-demo",
        session_id=str(SESSION_ID),
        generation=1,
        model_tier="sonnet",
        effort="medium",
        allowed_files=("src/hermes_orchestrator/foo.py",),
        worktree="/work/demo",
        red_test="tests/test_foo.py::test_red",
        verification=("uv run pytest tests/test_foo.py -q",),
        invariants="never touch bar.py",
        resource_note="single worker",
    )
    return packets.reserve(
        packet.packet_id, session_id=str(SESSION_ID), tool_use_id=tool_use_id
    )


def test_subagent_completed_releases_the_lease_exactly_once(
    cell_service: ProjectCellService, database: Database
) -> None:
    cell = _terminal_cell()
    cell_service.record(cell, _started_event("toolu-1"))
    assert _lease_state(database, "toolu-1") == "active"

    cell_service.record(cell, _terminal_event("subagent.completed", "toolu-1"))
    assert _lease_state(database, "toolu-1") == "released"

    # A duplicate completed event finds rowcount 0 and does nothing further.
    cell_service.record(cell, _terminal_event("subagent.completed", "toolu-1"))
    assert _lease_state(database, "toolu-1") == "released"


def test_subagent_failed_releases_the_lease_and_settles_failed(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.subagent_packets import SubagentPackets

    packets = SubagentPackets(database, events=EventStore(database))
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
        packets=packets,
    )
    reserved = _reserved_packet(database, tool_use_id="toolu-1")
    cell = _terminal_cell()
    service.record(cell, _started_event("toolu-1"))

    service.record(
        cell,
        _terminal_event("subagent.failed", "toolu-1", packet_id=reserved.packet_id),
    )

    assert _lease_state(database, "toolu-1") == "released"
    assert packets.get(reserved.packet_id).state == "failed"


def test_completed_with_wired_packets_settles_a_reserved_packet(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.subagent_packets import SubagentPackets

    packets = SubagentPackets(database, events=EventStore(database))
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
        packets=packets,
    )
    reserved = _reserved_packet(database, tool_use_id="toolu-1")
    cell = _terminal_cell()
    service.record(cell, _started_event("toolu-1"))

    service.record(
        cell,
        _terminal_event(
            "subagent.completed", "toolu-1", packet_id=reserved.packet_id
        ),
    )

    assert _lease_state(database, "toolu-1") == "released"
    assert packets.get(reserved.packet_id).state == "returned"


def test_replayed_completed_leaves_a_settled_packet_unchanged(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.subagent_packets import SubagentPackets

    packets = SubagentPackets(database, events=EventStore(database))
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
        packets=packets,
    )
    reserved = _reserved_packet(database, tool_use_id="toolu-1")
    cell = _terminal_cell()
    service.record(cell, _started_event("toolu-1"))
    service.record(
        cell,
        _terminal_event(
            "subagent.completed", "toolu-1", packet_id=reserved.packet_id
        ),
    )
    assert packets.get(reserved.packet_id).state == "returned"

    # A replayed terminal event (duplicate delivery) is a durable no-op:
    # no exception, and the already-settled packet is left untouched.
    service.record(
        cell,
        _terminal_event(
            "subagent.completed", "toolu-1", packet_id=reserved.packet_id
        ),
    )

    assert packets.get(reserved.packet_id).state == "returned"


def test_completed_without_packets_only_releases_the_lease(
    cell_service: ProjectCellService, database: Database
) -> None:
    # cell_service is wired with packets=None (the default).
    cell = _terminal_cell()
    cell_service.record(cell, _started_event("toolu-1"))
    cell_service.record(cell, _terminal_event("subagent.completed", "toolu-1"))
    assert _lease_state(database, "toolu-1") == "released"


def test_completed_with_wired_packets_but_no_packet_id_only_releases_the_lease(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.subagent_packets import SubagentPackets

    packets = SubagentPackets(database, events=EventStore(database))
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
        packets=packets,
    )
    cell = _terminal_cell()
    service.record(cell, _started_event("toolu-2"))

    # packet_id is None even though packets is wired: only the lease releases.
    service.record(
        cell, _terminal_event("subagent.completed", "toolu-2", packet_id=None)
    )

    assert _lease_state(database, "toolu-2") == "released"
    assert database.scalar("SELECT COUNT(*) FROM subagent_packets") == 0


# --- INFRA-197 correction C2: fable caps leave durable capacity evidence ---
#
# A provider.limit with limit_kind="fable" is a weekly-budget exhaustion:
# it must write a durable capacity observation and a cooldown honoring the
# recorded horizon. The sanitized stream exposes no reset time, so the
# conservative fallback is one full weekly cycle (7 days) — never the flat
# 1 hour that session/monthly_spend caps keep.


def _capacity_rows(database: Database) -> list[dict[str, object]]:
    rows = database.execute(
        "SELECT profile_alias, model, state, source, observed_at, "
        "resets_at, detail FROM profile_capacity_observations "
        "ORDER BY observation_id"
    ).fetchall()
    return [dict(row) for row in rows]


@pytest.mark.asyncio
async def test_fable_limit_records_capacity_observation_and_weekly_cooldown(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
    database: Database,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_limit = True
    runner.limit_kind = "fable"

    result = await cell_service.dispatch("ENG-9")

    assert result.status == "handoff_required"
    expected_reset = "2026-09-02T00:00:00+00:00"
    assert (
        database.scalar(
            "SELECT cooldown_until FROM profile_leases "
            "WHERE profile_alias = 'max-a'"
        )
        == expected_reset
    )
    rows = _capacity_rows(database)
    assert len(rows) == 1
    observation = rows[0]
    assert observation["profile_alias"] == "max-a"
    assert observation["model"] == "fable"
    assert observation["state"] == "capped"
    assert observation["source"] == "provider_limit"
    assert observation["observed_at"] == "2026-08-26T00:00:00+00:00"
    # No reset horizon reaches the sanitized stream, so the recorded
    # horizon is the documented worst case and the cooldown honors it.
    assert observation["resets_at"] == expected_reset
    assert "no reset horizon" in str(observation["detail"])


@pytest.mark.asyncio
async def test_fable_limit_before_session_start_records_weekly_capped_lease(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
    database: Database,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_session_started = False
    runner.emit_limit = True
    runner.limit_kind = "fable"

    result = await cell_service.dispatch("ENG-9")

    assert result.status == "start_unconfirmed"
    expected_reset = "2026-09-02T00:00:00+00:00"
    assert (
        database.scalar(
            "SELECT state FROM profile_leases WHERE profile_alias = 'max-a'"
        )
        == "capped"
    )
    assert (
        database.scalar(
            "SELECT cooldown_until FROM profile_leases "
            "WHERE profile_alias = 'max-a'"
        )
        == expected_reset
    )
    rows = _capacity_rows(database)
    assert len(rows) == 1
    assert rows[0]["state"] == "capped"
    assert rows[0]["source"] == "provider_limit"
    assert rows[0]["resets_at"] == expected_reset


@pytest.mark.asyncio
@pytest.mark.parametrize("limit_kind", [None, "session", "monthly_spend"])
async def test_non_fable_limits_keep_the_flat_hour_cooldown(
    cell_service: ProjectCellService,
    queue: QueueService,
    runner: RecordingRunner,
    database: Database,
    limit_kind: str | None,
) -> None:
    admit(queue, "ENG-9")
    runner.emit_limit = True
    runner.limit_kind = limit_kind

    result = await cell_service.dispatch("ENG-9")

    assert result.status == "handoff_required"
    assert (
        database.scalar(
            "SELECT cooldown_until FROM profile_leases "
            "WHERE profile_alias = 'max-a'"
        )
        == "2026-08-26T01:00:00+00:00"
    )
    # Session and spend caps are not fable-capacity evidence: no
    # observation row is written for them.
    assert _capacity_rows(database) == []


# --- activate_admitted_issue: the shared idle-dispatch activation ---------
# ---------------------------------------------------------------- service --
# (INFRA-199 correction: the Stop-hook idle dispatcher must never bypass
# Linear or reimplement the activation state machine — it calls this
# exact function, the same one ProjectCellService._activate_issue is
# built on.)


def _seed_active_cell(
    database: Database,
    *,
    cell_id: str = "cell-demo",
    project_key: str = "demo",
    profile_alias: str = "max-a",
    session_id: UUID = SESSION_ID,
) -> None:
    now = datetime(2026, 8, 26, tzinfo=UTC).isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at) VALUES (?, ?, 'active', ?, ?, ?, ?)",
            (cell_id, project_key, profile_alias, str(session_id), now, now),
        )
        connection.execute(
            "INSERT INTO profile_leases("
            "profile_alias, project_key, state, acquired_at) "
            "VALUES (?, ?, 'active', ?)",
            (profile_alias, project_key, now),
        )


def _issue_started_count(database: Database) -> int:
    return int(
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'issue.started'"
        )
    )


def _assignment_count(database: Database) -> int:
    return int(database.scalar("SELECT count(*) FROM lead_assignments"))


_LINEAR_STATE_NAMES = {"state-todo": "Todo", "state-in-dev": "In Development"}


class _ToggleableIssueUpdateTransport:
    """A fake ``LinearTransport`` (see ``linear.py``) for exercising the
    REAL ``LinearClient``/``ExternalEffectStore``: ``Issue`` always
    reads back the current fake state; ``IssueUpdate`` raises while
    ``fail_updates`` is True and otherwise applies the update, modeling
    an outage that later recovers without ever losing the durable
    pending effect row ``ExternalEffectStore.begin`` wrote before the
    first failed mutation attempt."""

    def __init__(self) -> None:
        self.fail_updates = True
        self.update_calls = 0
        self._state_id = "state-todo"
        self._assignee_id: str | None = None

    async def execute(
        self, operation: str, query: str, variables: dict[str, object]
    ) -> dict[str, object]:
        if operation == "Issue":
            return {
                "issue": {
                    "id": "issue-uuid-1",
                    "identifier": variables["id"],
                    "updatedAt": "2026-08-30T00:00:00.000Z",
                    "state": {
                        "id": self._state_id,
                        "name": _LINEAR_STATE_NAMES[self._state_id],
                    },
                    "assignee": (
                        {"id": self._assignee_id}
                        if self._assignee_id is not None
                        else None
                    ),
                    "team": {"id": "team-1"},
                }
            }
        assert operation == "IssueUpdate"
        self.update_calls += 1
        if self.fail_updates:
            raise RuntimeError("Linear is unreachable")
        update = variables["input"]
        if "stateId" in update:
            self._state_id = update["stateId"]
        if "assigneeId" in update:
            self._assignee_id = update["assigneeId"]
        return {
            "issueUpdate": {
                "success": True,
                "issue": {
                    "id": "issue-uuid-1",
                    "updatedAt": "2026-08-30T00:00:01.000Z",
                },
            }
        }


async def _activate(
    database: Database,
    linear: RecordingLinear,
    *,
    issue_id: str = "ENG-9",
    guard: object = None,
) -> tuple[bool, object]:
    events = EventStore(database)
    return await activate_admitted_issue(
        database=database,
        events=events,
        linear=linear,
        assignments=LeadAssignments(database, events=events),
        cell_id="cell-demo",
        project_key="demo",
        profile_alias="max-a",
        session_id=str(SESSION_ID),
        issue_id=issue_id,
        guard=guard,
    )


# --- INFRA-199 v2: local-commit-first activation ---------------------------
#
# Hermes' durable local lifecycle is authoritative; Linear is an
# eventually consistent workflow projection only. ``activate_admitted_issue``
# commits its LOCAL activation transaction first and never consults Linear
# before that commit; only once it succeeds is the idempotent "In
# Development" projection attempted, and a projection failure is swallowed
# rather than rolling back or retrying the already-durable local work (see
# ``cells._project_in_development``). The tests below replace the P-era
# activation-intent/compensation suite this superseded — there is nothing
# left to converge, since Linear can no longer strand local state.


@pytest.mark.asyncio
async def test_activate_admitted_issue_commits_locally_then_projects(
    database: Database, queue: QueueService
) -> None:
    """The local activation transaction must land BEFORE the Linear
    projection: a fake that inspects durable state from inside
    ``project`` observes the issue already ``in_development`` and its
    assignment already committed."""

    admit(queue, "ENG-9")
    _seed_active_cell(database)
    events = EventStore(database)

    class OrderingLinear(RecordingLinear):
        def __init__(self) -> None:
            super().__init__()
            self.state_at_projection: IssueState | None = None
            self.assignments_at_projection: int | None = None

        async def project(
            self, issue_id: str, target: LinearProjection, effect_id: str
        ) -> object:
            self.state_at_projection = queue.get(issue_id).state
            self.assignments_at_projection = _assignment_count(database)
            return await super().project(issue_id, target, effect_id)

    linear = OrderingLinear()
    assignments = LeadAssignments(database, events=events)

    activated, assignment = await activate_admitted_issue(
        database=database,
        events=events,
        linear=linear,
        assignments=assignments,
        cell_id="cell-demo",
        project_key="demo",
        profile_alias="max-a",
        session_id=str(SESSION_ID),
        issue_id="ENG-9",
    )

    assert activated is True
    assert linear.validations == []
    assert linear.state_at_projection == IssueState.IN_DEVELOPMENT
    assert linear.assignments_at_projection == 1
    assert linear.targets == [("ENG-9", "In Development", "operator")]
    assert queue.get("ENG-9").state == IssueState.IN_DEVELOPMENT
    assert assignment is not None
    assert _issue_started_count(database) == 1
    assert _assignment_count(database) == 1


@pytest.mark.asyncio
async def test_activate_admitted_issue_refuses_transactionally_without_touching_linear(
    database: Database, queue: QueueService
) -> None:
    """A commit-time refusal (here: the guard) leaves zero local writes
    AND never even attempts Linear — there is nothing to project for a
    candidate that did not activate."""

    admit(queue, "ENG-9")
    _seed_active_cell(database)
    linear = RecordingLinear()

    refused, assignment = await _activate(
        database, linear, guard=lambda connection: False
    )

    assert refused is False
    assert assignment is None
    assert queue.get("ENG-9").state == IssueState.QUEUED
    assert linear.targets == []
    assert linear.validations == []
    assert _issue_started_count(database) == 0
    assert _assignment_count(database) == 0


@pytest.mark.asyncio
async def test_activate_admitted_issue_projection_failure_leaves_durable_pending_trace(
    database: Database, queue: QueueService
) -> None:
    """INFRA-199 v2 (flipped O-era test): a Linear projection failure —
    an unreachable/deleted issue, a network error, a disallowed
    transition, anything — never blocks or undoes the local activation
    that already committed. The real ``LinearClient``/
    ``ExternalEffectStore`` (``linear.py``) leave exactly the durable
    ``pending`` ``external_effects`` row that reconciliation's
    ``_project_pending_linear_effect`` already surfaces — never
    nothing — and a replay of the same issue (its cell's session
    resuming) retries that SAME effect id rather than minting a new
    one or duplicating any local work. Once Linear recovers, that same
    replay completes the pending effect exactly once."""

    from hermes_orchestrator.linear import ExternalEffectStore, LinearClient

    admit(queue, "ENG-9")
    _seed_active_cell(database)
    events = EventStore(database)
    assignments = LeadAssignments(database, events=events)
    effects = ExternalEffectStore(database)
    transport = _ToggleableIssueUpdateTransport()
    linear = LinearClient(
        transport=transport,
        effects=effects,
        status_ids={"Todo": "state-todo", "In Development": "state-in-dev"},
        assignee_ids={"operator": "assignee-op", "ryan": "assignee-ryan"},
        expected_team_id="team-1",
    )
    effect_id = "linear:ENG-9:in-development:v2"

    def _activate_via_client() -> object:
        return activate_admitted_issue(
            database=database,
            events=events,
            linear=linear,
            assignments=assignments,
            cell_id="cell-demo",
            project_key="demo",
            profile_alias="max-a",
            session_id=str(SESSION_ID),
            issue_id="ENG-9",
        )

    activated, assignment = await _activate_via_client()

    # Local activation stands even though the projection failed.
    assert activated is True
    assert assignment is not None
    assert queue.get("ENG-9").state == IssueState.IN_DEVELOPMENT
    assert _issue_started_count(database) == 1
    assert _assignment_count(database) == 1

    effect = effects.get(effect_id)
    assert effect is not None
    assert effect.state == "pending"
    assert transport.update_calls == 1

    # A replay (the issue's own project cell resuming) never rolls back
    # or duplicates the local work, and retries the SAME pending effect.
    replayed, replayed_assignment = await _activate_via_client()
    assert replayed is True
    assert replayed_assignment is None  # unconsumed packet: durable no-op
    assert _issue_started_count(database) == 1
    assert _assignment_count(database) == 1
    assert transport.update_calls == 2
    assert effects.get(effect_id).state == "pending"
    assert (
        int(
            database.scalar(
                "SELECT count(*) FROM external_effects WHERE effect_id = ?",
                (effect_id,),
            )
        )
        == 1
    )

    # Linear recovers: the next replay completes the SAME effect exactly
    # once — no re-mint, no duplicate local work.
    transport.fail_updates = False
    recovered, recovered_assignment = await _activate_via_client()
    assert recovered is True
    assert recovered_assignment is None
    assert effects.get(effect_id).state == "completed"
    assert transport.update_calls == 3
    assert _issue_started_count(database) == 1
    assert _assignment_count(database) == 1


@pytest.mark.parametrize(
    "occupying_state",
    [IssueState.IN_DEVELOPMENT, IssueState.REVIEW],
    ids=["in_development", "review"],
)
@pytest.mark.asyncio
async def test_activate_admitted_issue_refuses_when_project_is_occupied(
    database: Database, queue: QueueService, occupying_state: IssueState
) -> None:
    """Project occupancy (INFRA-199 v2 / INFRA-211): a DIFFERENT issue
    already in ``in_development`` OR ``review`` refuses this candidate
    transactionally, with zero local writes and zero Linear traffic."""

    admit(queue, "ENG-9")
    admit(queue, "ENG-10")
    _seed_active_cell(database)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE admitted_issues SET state = ? WHERE issue_id = 'ENG-10'",
            (occupying_state.value,),
        )
    linear = RecordingLinear()

    refused, assignment = await _activate(database, linear)

    assert refused is False
    assert assignment is None
    assert queue.get("ENG-9").state == IssueState.QUEUED
    assert _issue_started_count(database) == 0
    assert _assignment_count(database) == 0
    assert linear.targets == []

    # The occupying issue clears; the candidate now activates normally.
    with database.transaction() as connection:
        connection.execute(
            "UPDATE admitted_issues SET state = 'done' WHERE issue_id = 'ENG-10'"
        )
    activated, _ = await _activate(database, linear)
    assert activated is True
    assert queue.get("ENG-9").state == IssueState.IN_DEVELOPMENT
    assert linear.targets == [("ENG-9", "In Development", "operator")]


@pytest.mark.asyncio
async def test_activate_admitted_issue_refuses_a_dependency_not_ready_issue(
    database: Database, queue: QueueService
) -> None:
    admit(queue, "ENG-9")
    _seed_active_cell(database)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE admitted_issues SET dependency_ready = 0 "
            "WHERE issue_id = 'ENG-9'"
        )
    linear = RecordingLinear()

    refused, assignment = await _activate(database, linear)

    assert refused is False
    assert assignment is None
    assert queue.get("ENG-9").state == IssueState.QUEUED
    assert _issue_started_count(database) == 0
    assert _assignment_count(database) == 0
    assert linear.targets == []


@pytest.mark.asyncio
async def test_activate_admitted_issue_refuses_a_pending_operator_decision(
    database: Database, queue: QueueService
) -> None:
    admit(queue, "ENG-9")
    _seed_active_cell(database)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO operator_decisions("
            "decision_id, issue_id, project_key, cell_id, session_id, "
            "actor, choice, status, recorded_at"
            ") VALUES ('dec-1', 'ENG-9', 'demo', 'cell-demo', ?, "
            "'operator', 'hold', 'pending', ?)",
            (str(SESSION_ID), datetime.now(UTC).isoformat()),
        )
    linear = RecordingLinear()

    refused, assignment = await _activate(database, linear)

    assert refused is False
    assert assignment is None
    assert queue.get("ENG-9").state == IssueState.QUEUED
    assert _issue_started_count(database) == 0
    assert _assignment_count(database) == 0
    assert linear.targets == []


@pytest.mark.asyncio
async def test_activate_admitted_issue_refuses_a_non_runnable_source_state(
    database: Database, queue: QueueService
) -> None:
    """The CAS accepts only the exact observed runnable state
    (queued/blocked) — never a permissive ``!= done`` check that could
    silently drag a review/paused issue back into ``in_development``."""

    admit(queue, "ENG-9")
    _seed_active_cell(database)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE admitted_issues SET state = ? WHERE issue_id = 'ENG-9'",
            (IssueState.REVIEW.value,),
        )
    linear = RecordingLinear()

    refused, assignment = await _activate(database, linear)

    assert refused is False
    assert assignment is None
    assert queue.get("ENG-9").state == IssueState.REVIEW
    assert _issue_started_count(database) == 0
    assert _assignment_count(database) == 0
    assert linear.targets == []


@pytest.mark.asyncio
async def test_activate_admitted_issue_refuses_a_stale_profile_lease(
    database: Database, queue: QueueService
) -> None:
    """The ``project_cells``/``profile_leases`` CAS guard: an active
    cell whose profile lease is no longer active (released, rotated,
    capped) refuses activation transactionally."""

    admit(queue, "ENG-9")
    _seed_active_cell(database)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE profile_leases SET state = 'capped' "
            "WHERE project_key = 'demo' AND profile_alias = 'max-a'"
        )
    linear = RecordingLinear()

    refused, assignment = await _activate(database, linear)

    assert refused is False
    assert assignment is None
    assert queue.get("ENG-9").state == IssueState.QUEUED
    assert _issue_started_count(database) == 0
    assert _assignment_count(database) == 0
    assert linear.targets == []


def _pending_projection_rows(database: Database) -> list[tuple[str, dict]]:
    """Pending Linear effects, read EXACTLY the way reconciliation's
    ``_stage_linear`` / ``_project_pending_linear_effect`` read them."""

    rows = database.execute(
        "SELECT effect_id, target, request_json FROM external_effects "
        "WHERE adapter = 'linear' AND state = 'pending'"
    ).fetchall()
    return [
        (str(row["effect_id"]), json.loads(row["request_json"]))
        for row in rows
    ]


_IN_DEV_REQUEST = {
    "issue_id": "ENG-9",
    "target": {"status": "In Development", "assignee_alias": "operator"},
}


@pytest.mark.asyncio
async def test_activation_commit_journals_the_stable_pending_projection_row(
    database: Database, queue: QueueService
) -> None:
    """Sol ec0ed7fe gap 2: the stable TARGET-ONLY ``In Development``
    projection record is journaled by the local activation transaction
    itself — no live Linear client involved — so it exists even when
    the projector is a fake that never touches the effect store. A
    transactional refusal journals nothing."""

    admit(queue, "ENG-9")
    _seed_active_cell(database)
    linear = RecordingLinear()

    refused, _ = await _activate(database, linear, guard=lambda connection: False)
    assert refused is False
    assert (
        int(database.scalar("SELECT count(*) FROM external_effects")) == 0
    )

    activated, _ = await _activate(database, linear)

    assert activated is True
    assert _pending_projection_rows(database) == [
        ("linear:ENG-9:in-development:v2", _IN_DEV_REQUEST)
    ]
    # The fake projector ran after the commit but never completed the
    # journal entry — exactly the durable trace reconciliation reads.
    assert linear.targets == [("ENG-9", "In Development", "operator")]

    # Replay adopts the existing row: still exactly one.
    replayed, _ = await _activate(database, linear)
    assert replayed is True
    assert _pending_projection_rows(database) == [
        ("linear:ENG-9:in-development:v2", _IN_DEV_REQUEST)
    ]


@pytest.mark.parametrize("effect_state", ["pending", "completed"])
@pytest.mark.asyncio
async def test_activation_adopts_legacy_revision_bearing_projection_row(
    database: Database,
    queue: QueueService,
    effect_state: str,
) -> None:
    """An upgrade preserves a matching row written by current main."""

    admit(queue, "ENG-9")
    _seed_active_cell(database)
    legacy_request = {
        **_IN_DEV_REQUEST,
        "source_revision": "legacy-r1",
        "changed_fields": ["status"],
    }
    response = (
        {
            "issue_id": "ENG-9",
            "changed_fields": ["status"],
            "source_revision": "legacy-r1",
            "response_revision": "legacy-r2",
        }
        if effect_state == "completed"
        else None
    )
    now = datetime.now(UTC).isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO external_effects("
            "effect_id, adapter, operation, target, state, request_json, "
            "response_json, created_at, updated_at"
            ") VALUES (?, 'linear', 'project', 'ENG-9', ?, ?, ?, ?, ?)",
            (
                "linear:ENG-9:in-development:v2",
                effect_state,
                json.dumps(legacy_request, sort_keys=True, separators=(",", ":")),
                (
                    json.dumps(response, sort_keys=True, separators=(",", ":"))
                    if response is not None
                    else None
                ),
                now,
                now,
            ),
        )

    activated, assignment = await _activate(database, RecordingLinear())

    assert activated is True
    assert assignment is not None
    assert queue.get("ENG-9").state == IssueState.IN_DEVELOPMENT
    assert _issue_started_count(database) == 1
    assert _assignment_count(database) == 1
    row = database.execute(
        "SELECT state, request_json FROM external_effects WHERE effect_id = ?",
        ("linear:ENG-9:in-development:v2",),
    ).fetchone()
    assert row is not None
    assert str(row["state"]) == effect_state
    assert json.loads(row["request_json"]) == legacy_request


class _ReadOutageTransport:
    """A fake ``LinearTransport`` whose ``Issue`` read fails while
    ``fail_reads`` is True — the early-failure class that used to leave
    NO pending row at all — and otherwise behaves like a healthy
    Todo-state issue that accepts updates."""

    def __init__(self) -> None:
        self.fail_reads = True
        self.update_calls = 0
        self._state_id = "state-todo"
        self._assignee_id: str | None = None

    async def execute(
        self, operation: str, query: str, variables: dict[str, object]
    ) -> dict[str, object]:
        if operation == "Issue":
            if self.fail_reads:
                raise TimeoutError("Linear is unreachable")
            return {
                "issue": {
                    "id": "issue-uuid-1",
                    "identifier": variables["id"],
                    "updatedAt": "2026-08-30T00:00:00.000Z",
                    "state": {
                        "id": self._state_id,
                        "name": _LINEAR_STATE_NAMES[self._state_id],
                    },
                    "assignee": (
                        {"id": self._assignee_id}
                        if self._assignee_id is not None
                        else None
                    ),
                    "team": {"id": "team-1"},
                }
            }
        assert operation == "IssueUpdate"
        self.update_calls += 1
        update = variables["input"]
        if "stateId" in update:
            self._state_id = update["stateId"]
        if "assigneeId" in update:
            self._assignee_id = update["assigneeId"]
        return {
            "issueUpdate": {
                "success": True,
                "issue": {
                    "id": "issue-uuid-1",
                    "updatedAt": "2026-08-30T00:00:01.000Z",
                },
            }
        }


@pytest.mark.asyncio
async def test_initial_read_failure_leaves_the_pending_effect_for_reconciliation(
    database: Database, queue: QueueService
) -> None:
    """Sol ec0ed7fe gap 2: an outage on the projection's very FIRST
    Linear read — before this correction, the one failure class that
    left no ``external_effects`` row at all, invisible to
    ``reconcile._project_pending_linear_effect`` — now leaves exactly
    the stable pending row the activation transaction journaled. Once
    Linear recovers, a replay (real ``LinearClient`` over the fake
    transport) ADOPTS that same row and completes the effect exactly
    once, with no second ``issue.started`` and no second assignment."""

    from hermes_orchestrator.linear import ExternalEffectStore, LinearClient

    admit(queue, "ENG-9")
    _seed_active_cell(database)
    events = EventStore(database)
    assignments = LeadAssignments(database, events=events)
    effects = ExternalEffectStore(database)
    transport = _ReadOutageTransport()
    linear = LinearClient(
        transport=transport,
        effects=effects,
        status_ids={"Todo": "state-todo", "In Development": "state-in-dev"},
        assignee_ids={"operator": "assignee-op", "ryan": "assignee-ryan"},
        expected_team_id="team-1",
    )
    effect_id = "linear:ENG-9:in-development:v2"

    def _activate_via_client() -> object:
        return activate_admitted_issue(
            database=database,
            events=events,
            linear=linear,
            assignments=assignments,
            cell_id="cell-demo",
            project_key="demo",
            profile_alias="max-a",
            session_id=str(SESSION_ID),
            issue_id="ENG-9",
        )

    activated, assignment = await _activate_via_client()

    # Local activation stands; the failed initial read left the stable
    # pending record — visible to reconciliation — not nothing.
    assert activated is True
    assert assignment is not None
    assert queue.get("ENG-9").state == IssueState.IN_DEVELOPMENT
    assert transport.update_calls == 0
    assert _pending_projection_rows(database) == [(effect_id, _IN_DEV_REQUEST)]
    assert _issue_started_count(database) == 1
    assert _assignment_count(database) == 1

    # Linear recovers: the replay adopts and completes the SAME effect
    # exactly once — one mutation, no second issue.started/assignment.
    transport.fail_reads = False
    recovered, recovered_assignment = await _activate_via_client()
    assert recovered is True
    assert recovered_assignment is None
    assert effects.get(effect_id).state == "completed"
    assert transport.update_calls == 1
    assert _issue_started_count(database) == 1
    assert _assignment_count(database) == 1
    assert (
        int(
            database.scalar(
                "SELECT count(*) FROM external_effects WHERE effect_id = ?",
                (effect_id,),
            )
        )
        == 1
    )


# --- Sol ec0ed7fe gap 3: the daemon dispatch capacity guard ----------------


_GUARD_NOW = datetime(2026, 8, 26, tzinfo=UTC)


def _insert_resource_sample(
    database: Database,
    *,
    pressure: str,
    sampled_at: datetime,
    sample_id: str = "sample-1",
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO resource_samples("
            "sample_id, sampled_at, pressure, available_memory_bytes, "
            "total_memory_bytes, swap_used_bytes, load_one, logical_cpus, "
            "disk_json, managed_rss_bytes"
            ") VALUES (?, ?, ?, 1, 2, 0, 0.1, 1, '{}', 1)",
            (sample_id, sampled_at.isoformat(), pressure),
        )


@pytest.mark.parametrize(
    "pressure, sample_age, priority, expect_active",
    [
        pytest.param("green", timedelta(0), 1, True, id="fresh-green"),
        pytest.param("green", timedelta(minutes=6), 1, False, id="stale-green"),
        pytest.param("red", timedelta(0), 1, False, id="fresh-red"),
        pytest.param("yellow", timedelta(0), 1, True, id="fresh-yellow-p1"),
        pytest.param(
            "yellow", timedelta(0), 2, False, id="yellow-priority-too-high"
        ),
        pytest.param(None, timedelta(0), 1, False, id="no-sample"),
    ],
)
@pytest.mark.asyncio
async def test_armed_daemon_dispatch_reproves_capacity_inside_the_transaction(
    cell_service: ProjectCellService,
    queue: QueueService,
    database: Database,
    linear: RecordingLinear,
    pressure: str | None,
    sample_age: timedelta,
    priority: int,
    expect_active: bool,
) -> None:
    """Sol ec0ed7fe gap 3: with the capacity guard armed (the live
    daemon), the shared activation transaction re-proves the IDENTICAL
    freshness/priority predicate the idle path supplies — on its own
    connection — so a missing, stale, red, or priority-refusing sample
    fails a fresh activation closed with zero issue writes and zero
    Linear traffic."""

    cell_service.require_dispatch_capacity_guard(5)
    queue.admit(
        AdmissionRequest(
            issue_id="ENG-9",
            project_key="demo",
            linear_priority=priority,
            admitted_by="operator",
            instruction_id="chat-ENG-9",
        )
    )
    if pressure is not None:
        _insert_resource_sample(
            database, pressure=pressure, sampled_at=_GUARD_NOW - sample_age
        )

    result = await cell_service.dispatch("ENG-9")

    if expect_active:
        assert result.status == "working"
        assert queue.get("ENG-9").state == IssueState.IN_DEVELOPMENT
        assert _issue_started_count(database) == 1
        assert linear.targets == [("ENG-9", "In Development", "operator")]
    else:
        assert result.status == "start_unconfirmed"
        assert queue.get("ENG-9").state == IssueState.QUEUED
        assert _issue_started_count(database) == 0
        assert linear.targets == []
        assert _pending_projection_rows(database) == []


@pytest.mark.asyncio
async def test_armed_daemon_dispatch_fails_closed_on_a_superseding_red_sample(
    cell_service: ProjectCellService,
    queue: QueueService,
    database: Database,
    linear: RecordingLinear,
) -> None:
    """A green sample authorized planning, but a NEWER red sample landed
    before the activation commit: the transaction-local guard reads the
    newest sample and refuses — delayed capacity evidence never
    authorizes activation."""

    cell_service.require_dispatch_capacity_guard(5)
    admit(queue, "ENG-9")
    _insert_resource_sample(
        database,
        pressure="green",
        sampled_at=_GUARD_NOW - timedelta(minutes=2),
        sample_id="sample-green",
    )
    _insert_resource_sample(
        database,
        pressure="red",
        sampled_at=_GUARD_NOW + timedelta(seconds=30),
        sample_id="sample-red",
    )

    result = await cell_service.dispatch("ENG-9")

    assert result.status == "start_unconfirmed"
    assert queue.get("ENG-9").state == IssueState.QUEUED
    assert _issue_started_count(database) == 0
    assert linear.targets == []


@pytest.mark.asyncio
async def test_armed_daemon_dispatch_rechecks_reprioritization_in_transaction(
    cell_service: ProjectCellService,
    queue: QueueService,
    database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transaction-local guard must see a post-selection priority change."""

    cell_service.require_dispatch_capacity_guard(5)
    admit(queue, "ENG-9")
    _insert_resource_sample(database, pressure="yellow", sampled_at=_GUARD_NOW)
    original_guard = cell_service._capacity_guard

    def reprioritize_after_guard_snapshot(issue_id: str) -> object:
        guard = original_guard(issue_id)
        queue.reprioritize(issue_id, 2)
        return guard

    monkeypatch.setattr(
        cell_service, "_capacity_guard", reprioritize_after_guard_snapshot
    )

    result = await cell_service.dispatch("ENG-9")

    assert result.status == "start_unconfirmed"
    assert queue.get("ENG-9").state == IssueState.QUEUED
    assert _issue_started_count(database) == 0


@pytest.mark.asyncio
async def test_armed_guard_never_blocks_the_replay_of_in_flight_work(
    cell_service: ProjectCellService,
    queue: QueueService,
    database: Database,
    runner: RecordingRunner,
) -> None:
    """The guard admits NEW work only: an issue already
    ``in_development`` (a resume after restart, when no fresh sample
    may exist yet) replays through the same transaction untouched by
    the capacity guard — recovery is not a new admission."""

    _insert_resource_sample(database, pressure="green", sampled_at=_GUARD_NOW)
    admit(queue, "ENG-9")
    first = await cell_service.dispatch("ENG-9")
    assert first.status == "working"

    # The sample goes stale; the armed service resumes the in-flight
    # issue anyway.
    with database.transaction() as connection:
        connection.execute(
            "UPDATE resource_samples SET sampled_at = ?",
            ((_GUARD_NOW - timedelta(minutes=10)).isoformat(),),
        )
    cell_service.require_dispatch_capacity_guard(5)

    resumed = await cell_service.dispatch("ENG-9")

    assert resumed.status == "working"
    assert runner.resume_count == 1
    assert queue.get("ENG-9").state == IssueState.IN_DEVELOPMENT
    assert _issue_started_count(database) == 1


@pytest.mark.asyncio
async def test_daemon_and_idle_racing_activate_at_most_one_issue(
    cell_service: ProjectCellService,
    queue: QueueService,
    database: Database,
) -> None:
    """Concurrent daemon dispatch and idle activation for the same
    project: the existing project-wide lock, ``BEGIN IMMEDIATE``
    transaction, and occupancy CAS still admit at most one issue —
    exactly one ``issue.started``, one issue ``in_development``."""

    admit(queue, "ENG-9")
    admit(queue, "ENG-10")
    _seed_active_cell(database)
    idle_linear = RecordingLinear()

    await asyncio.gather(
        cell_service.dispatch("ENG-9"),
        _activate(database, idle_linear, issue_id="ENG-10"),
    )

    activated = [
        issue_id
        for issue_id in ("ENG-9", "ENG-10")
        if queue.get(issue_id).state is IssueState.IN_DEVELOPMENT
    ]
    assert len(activated) == 1
    assert _issue_started_count(database) == 1
    assert (
        int(
            database.scalar(
                "SELECT count(*) FROM admitted_issues "
                "WHERE state = 'in_development'"
            )
        )
        == 1
    )


@pytest.mark.asyncio
async def test_paused_issue_is_never_touched_by_a_dispatch_of_a_different_issue(
    cell_service: ProjectCellService,
    queue: QueueService,
    linear: RecordingLinear,
) -> None:
    """INFRA-199 v2: there is no convergence sweep any more. A paused
    issue is simply not ranked (``queue.list_ranked`` only ever returns
    queued/blocked issues), so dispatch never touches it — no Linear
    traffic, no local writes — while a distinct ranked issue in the
    same project dispatches normally."""

    admit(queue, "ENG-9")
    admit(queue, "ENG-10")
    queue.transition("ENG-9", IssueState.PAUSED, actor="operator", reason="hold")

    result = await cell_service.dispatch("ENG-10")

    assert result.status == "working"
    assert queue.get("ENG-9").state == IssueState.PAUSED
    assert queue.get("ENG-10").state == IssueState.IN_DEVELOPMENT
    assert all(target[0] != "ENG-9" for target in linear.targets)


@pytest.mark.asyncio
async def test_harness_dispatch_is_still_blocked_by_project_occupancy(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    """INFRA-219 L2 RECORDED GAP: lane-scoped cells are not enough.

    L1 made cell uniqueness per (project, lane) and L2 made dispatch,
    worktrees, and the dashboard lane-aware — but ISSUE OCCUPANCY is
    still tracked per project in ``admitted_issues`` (no lane
    dimension), and both the coarse ``project_busy`` pre-check and the
    transactional activation predicate read it. So while the
    development lead occupies the project with its issue, a harness
    dispatch for a different issue is refused as ``project_busy``.

    This test pins that ACTUAL behavior rather than the desired one:
    INFRA-219's acceptance ("with the development lead actively
    working an issue, one Hermes command starts the visible harness
    lead") cannot pass until occupancy itself grows a lane dimension,
    which is a durable-model change beyond L2's boundary. The
    development lane's rows must at least stay untouched by the
    refusal, which is what the assertions below prove.
    """

    harness_cwd = tmp_path / "harness"
    harness_cwd.mkdir()
    sessions = iter(
        [
            UUID("22222222-2222-4222-8222-222222222222"),
            UUID("33333333-3333-4333-8333-333333333333"),
        ]
    )
    cells = iter(["cell-dev", "cell-harness"])
    service = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=linear,
        project_paths={"demo": tmp_path},
        lane_project_paths={("demo", "harness"): harness_cwd},
        session_ids=lambda: next(sessions),
        cell_ids=lambda: next(cells),
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )

    admit(queue, "ENG-9")
    development = await service.dispatch("ENG-9")
    assert development.status == "working"
    dev_cell = service.active_cell("demo")
    assert dev_cell is not None
    dev_before = database.execute(
        "SELECT cell_id, session_id, state FROM project_cells "
        "WHERE lane_role = 'development'"
    ).fetchall()

    admit(queue, "ENG-11")
    harness = await service.dispatch("ENG-11", lane_role="harness")

    # The recorded gap: per-project occupancy refuses the harness lane.
    assert harness.status == "project_busy"
    assert service.active_cell("demo", "harness") is None
    # The refusal is a plain read: the development lane is untouched and
    # no harness process was ever launched.
    dev_after = database.execute(
        "SELECT cell_id, session_id, state FROM project_cells "
        "WHERE lane_role = 'development'"
    ).fetchall()
    assert [tuple(row) for row in dev_after] == [tuple(row) for row in dev_before]
    assert dev_cell is not None
    assert all(
        request.cwd != harness_cwd for request in runner.start_requests
    )


@pytest.mark.asyncio
async def test_second_harness_dispatch_resumes_rather_than_duplicating(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    linear: RecordingLinear,
    tmp_path: Path,
) -> None:
    """INFRA-219 L2: starting the harness lane twice adopts the live cell
    (L1's per-lane uniqueness), never a second row."""

    sessions = iter(
        [
            UUID("44444444-4444-4444-8444-444444444444"),
            UUID("55555555-5555-4555-8555-555555555555"),
        ]
    )
    cells = iter(["cell-harness", "cell-extra"])
    service = ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=linear,
        project_paths={"demo": tmp_path},
        session_ids=lambda: next(sessions),
        cell_ids=lambda: next(cells),
        now=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )

    admit(queue, "ENG-9")
    first = await service.dispatch("ENG-9", lane_role="harness")
    admit(queue, "ENG-11")
    second = await service.dispatch("ENG-11", lane_role="harness")

    assert first.status == "working"
    # The lane is already occupied by its own live cell: the second
    # dispatch is refused as busy rather than creating a second harness
    # cell (L1's per-lane uniqueness), and never touches the first.
    assert second.status == "project_busy"
    rows = database.execute(
        "SELECT cell_id FROM project_cells WHERE lane_role = 'harness'"
    ).fetchall()
    assert [str(row["cell_id"]) for row in rows] == ["cell-harness"]
