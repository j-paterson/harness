from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from hermes_orchestrator.cells import ProjectCellService, RotationBlocked
from hermes_orchestrator.claude import ClaudeEvent, LeadTurnRequest
from hermes_orchestrator.cmux import CmuxSurfaceRef
from hermes_orchestrator.cmux_surfaces import (
    CmuxSurfaceBindings,
    classic_resume_command,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.handoffs import (
    HandoffDocument,
    HandoffService,
    HandoffTest,
    derived_handoff_document,
)
from hermes_orchestrator.lead_rotation import LeadRotation, WorktreeState
from hermes_orchestrator.linear import LinearProjection
from hermes_orchestrator.profiles import ProfileHealth, ProfilePool, ProfileRegistry
from hermes_orchestrator.queue import QueueService

SESSION_ID = UUID("11111111-1111-4111-8111-111111111111")
REPLACEMENT_SESSION = UUID("22222222-2222-4222-8222-222222222222")
THIRD_SESSION = UUID("33333333-3333-4333-8333-333333333333")
NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)


class RecordingRunner:
    """Minimal lead runner boundary, in the style of test_cells.py."""

    def __init__(self) -> None:
        self.start_count = 0
        self.resume_count = 0
        self.emit_session_started = True
        self.emit_handoff_ack = False
        self.retired_sessions: list[UUID] = []

    def start_lead(self, request: LeadTurnRequest) -> AsyncIterator[ClaudeEvent]:
        self.start_count += 1
        return self._events(request)

    def resume_lead(
        self, session_id: UUID, request: LeadTurnRequest
    ) -> AsyncIterator[ClaudeEvent]:
        self.resume_count += 1
        return self._events(request)

    async def _events(self, request: LeadTurnRequest) -> AsyncIterator[ClaudeEvent]:
        if self.emit_session_started:
            yield ClaudeEvent(
                kind="session.started",
                original_type="system",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-30T12:00:00Z",
                usage={},
            )
        if self.emit_handoff_ack:
            yield ClaudeEvent(
                kind="handoff.acknowledged",
                original_type="result",
                session_id=request.session_id,
                parent_tool_use_id=None,
                timestamp="2026-08-30T12:00:01Z",
                usage={},
                restated_next_action="Run the failing test and finish INFRA-197.",
            )

    async def retire_session(self, session_id: UUID) -> None:
        self.retired_sessions.append(session_id)


class RecordingLinear:
    async def validate(self, project_key: str, issue_id: str) -> object:
        return object()

    async def project(
        self, issue_id: str, target: LinearProjection, effect_id: str
    ) -> object:
        return object()


def insert_active_registration(
    database: Database,
    *,
    session_id: str,
    project_key: str = "demo",
    cell_id: str = "cell-demo",
    profile_alias: str = "max-b",
) -> None:
    """Durably record one active ``channel_registrations`` row, exactly
    as the hermes-control sidecar does when it registers a freshly
    seated session (migration 0033_channel_hub.sql)."""

    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO channel_registrations("
            "registration_id, project_key, cell_id, session_id, "
            "profile_alias, generation, state, connected_at"
            ") VALUES (?, ?, ?, ?, ?, 1, 'active', ?)",
            (
                f"registration-{session_id}",
                project_key,
                cell_id,
                session_id,
                profile_alias,
                NOW.isoformat(),
            ),
        )


class RecordingSeater:
    """A managed-seat fake that durably retires/binds through the REAL
    CmuxSurfaceBindings, so LeadRotation's own idempotency read of
    ``bindings.active_lead`` reflects genuine seat state across calls —
    exactly what the real CmuxLeadSeater.ensure() would leave behind."""

    def __init__(
        self,
        bindings: CmuxSurfaceBindings,
        database: Database,
        *,
        registers_channel: bool = True,
    ) -> None:
        self._bindings = bindings
        self._database = database
        self._refs = iter(range(1, 1000))
        self.calls: list[dict[str, object]] = []
        self.fail_next = False
        self.registers_channel = registers_channel

    async def ensure(self, **identity: object) -> object:
        self.calls.append(dict(identity))
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("cmux seat activation crashed")
        cell_id = str(identity["cell_id"])
        session_id = str(identity["session_id"])
        existing = self._bindings.active_lead(cell_id)
        if existing is not None:
            if existing.session_id == session_id:
                return existing
            self._bindings.mark_closed(existing.binding_id, reason="session_rotated")
        n = next(self._refs)
        ref = CmuxSurfaceRef(workspace_uuid=f"ws-{n}", surface_uuid=f"sf-{n}")
        bound = self._bindings.bind_lead(
            project_key=str(identity["project_key"]),
            cell_id=cell_id,
            session_id=session_id,
            profile_alias=str(identity["profile_alias"]),
            ref=ref,
        )
        if self.registers_channel:
            # Simulates the sidecar registering on the hermes-control
            # channel the moment the seat's pane comes up.
            insert_active_registration(
                self._database,
                session_id=session_id,
                project_key=str(identity["project_key"]),
                cell_id=cell_id,
                profile_alias=str(identity["profile_alias"]),
            )
        return bound


def make_worktree_state(
    *,
    dirty: bool = False,
    head: str = "deadbeef",
    origin_head: str = "deadbeef",
    head_is_integration_ancestor: bool = False,
) -> WorktreeState:
    return WorktreeState(
        branch="feature/infra-197",
        head=head,
        origin_head=origin_head,
        dirty=dirty,
        head_is_integration_ancestor=head_is_integration_ancestor,
    )


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
    pool = ProfilePool(registry, now=lambda: NOW)
    for profile in registry.profiles:
        pool.record_health(
            ProfileHealth(
                profile_alias=profile.alias,
                eligible=True,
                reason="eligible",
                last_checked_at=NOW,
            )
        )
    return pool


@pytest.fixture
def runner() -> RecordingRunner:
    return RecordingRunner()


class CountingHandoffs(HandoffService):
    """The real service, plus an acknowledgement-call counter so tests can
    assert recovery performs ZERO additional acknowledgement calls."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self.acknowledge_calls = 0

    def acknowledge(self, *args: object, **kwargs: object) -> object:  # type: ignore[override]
        self.acknowledge_calls += 1
        return super().acknowledge(*args, **kwargs)  # type: ignore[arg-type]


@pytest.fixture
def handoffs(database: Database) -> CountingHandoffs:
    counter = iter(range(1, 1000))
    return CountingHandoffs(
        database,
        handoff_ids=lambda: f"handoff-{next(counter)}",
        now=lambda: NOW,
    )


@pytest.fixture
def cells(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    handoffs: HandoffService,
    tmp_path: Path,
) -> ProjectCellService:
    return ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=RecordingLinear(),
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: NOW,
        handoffs=handoffs,
        replacement_session_ids=lambda: REPLACEMENT_SESSION,
    )


@pytest.fixture
def bindings(database: Database) -> CmuxSurfaceBindings:
    counter = iter(range(1, 1000))
    return CmuxSurfaceBindings(
        database=database,
        events=EventStore(database),
        now=lambda: NOW,
        ids=lambda: f"binding-{next(counter)}",
    )


@pytest.fixture
def seater(bindings: CmuxSurfaceBindings, database: Database) -> RecordingSeater:
    return RecordingSeater(bindings, database)


def make_rotation(
    database: Database,
    handoffs: HandoffService,
    cells: ProjectCellService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    *,
    worktree: WorktreeState | None = None,
    registration_wait_seconds: float = 0,
) -> LeadRotation:
    state = worktree or make_worktree_state()
    return LeadRotation(
        database=database,
        handoffs=handoffs,
        cells=cells,
        bindings=bindings,
        seater=seater,
        worktree_state=lambda project_key: state,
        registration_wait_seconds=registration_wait_seconds,
    )


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


def submit_handoff(
    handoffs: HandoffService,
    *,
    branch: str = "feature/infra-197",
    commits: list[str] | None = None,
) -> str:
    """Submit a handoff document whose ``branch``/``commits`` match
    ``make_worktree_state()``'s default (branch ``feature/infra-197``,
    head ``deadbeef``) unless overridden — so this stays the "fresh"
    shape for the staleness gate (INFRA-184) in every test that does not
    explicitly want a stale one."""

    document = HandoffDocument(
        cell_id="cell-demo",
        objective="Finish INFRA-197 packet P1",
        status="ready to rotate",
        decisions=["compose from existing primitives"],
        branch=branch,
        commits=commits or ["deadbeef"],
        pull_request="https://example.invalid/pr/1",
        modified_files=["src/hermes_orchestrator/lead_rotation.py"],
        tests=[HandoffTest(command="pytest", outcome="passed")],
        blockers=[],
        remaining_steps=["seat the replacement"],
        commands=["pytest"],
        environment_notes=["none"],
        risks=[],
        next_action="run the seat phase",
    )
    return handoffs.submit(document).handoff_id


def submit_derived_handoff(
    handoffs: HandoffService,
    *,
    issue_id: str = "ENG-9",
    issue_state: str,
    branch: str = "feature/infra-197",
    head: str = "deadbeef",
    next_action: str = "run the seat phase",
) -> str:
    """Submit a handoff through the REAL ``derived_handoff_document``
    producer (the shape rotate-lead sees in production), so its
    ``status`` line is genuinely ``"issue {id} is {state}; ..."`` —
    :func:`submit_handoff`'s hand-authored ``status="ready to rotate"``
    never parses, and only a document built this way can exercise the
    staleness gate's issue criterion (INFRA-184 correction)."""

    document = derived_handoff_document(
        cell_id="cell-demo",
        project_key="demo",
        session_id=str(SESSION_ID),
        profile_alias="max-a",
        issue_id=issue_id,
        issue_state=issue_state,
        branch=branch,
        head=head,
        decisions=["derived"],
        caveats=[],
        risks=[],
        next_action=next_action,
    )
    return handoffs.submit(document).handoff_id


async def start_cell(cells: ProjectCellService, queue: QueueService) -> None:
    admit(queue, "ENG-9")
    result = await cells.dispatch("ENG-9")
    assert result.status == "working"


@pytest.mark.asyncio
async def test_no_submitted_handoff_blocks_precondition(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
) -> None:
    await start_cell(cells, queue)
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is False
    assert report.phase == "precondition"
    assert report.handoff_id is None
    assert "handoff" in (report.failure or "").lower()
    assert seater.calls == []


@pytest.mark.asyncio
async def test_dirty_worktree_blocks_precondition(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
) -> None:
    await start_cell(cells, queue)
    submit_handoff(handoffs)
    rotation = make_rotation(
        database,
        handoffs,
        cells,
        bindings,
        seater,
        worktree=make_worktree_state(dirty=True),
    )

    report = await rotation.rotate("cell-demo")

    assert report.ok is False
    assert report.phase == "precondition"
    assert "uncommitted" in (report.failure or "")
    assert seater.calls == []


@pytest.mark.asyncio
async def test_head_behind_origin_blocks_precondition(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
) -> None:
    await start_cell(cells, queue)
    submit_handoff(handoffs)
    rotation = make_rotation(
        database,
        handoffs,
        cells,
        bindings,
        seater,
        worktree=make_worktree_state(head="local1", origin_head="origin1"),
    )

    report = await rotation.rotate("cell-demo")

    assert report.ok is False
    assert report.phase == "precondition"
    assert "origin" in (report.failure or "").lower()
    assert seater.calls == []


@pytest.mark.asyncio
async def test_zero_work_head_is_integration_ancestor_passes_precondition(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """A leased seat with no remote issue branch (``origin_head == ""``)
    whose HEAD is a proven ancestor of the fetched integration head
    proves zero implementation delta — the gate must let it proceed
    past the worktree precondition rather than refuse for an unpushed
    head. The producer maps BOTH "HEAD is strictly behind the
    integration head" (generation 97's clean, older-integration-based
    worktree) and "HEAD exactly equals the integration head" (the
    trivial ancestor case) to ``head_is_integration_ancestor=True``, so
    this one case covers both."""
    await start_cell(cells, queue)
    # commits[0] matches the zero-work worktree's HEAD, so the staleness
    # gate (INFRA-184) reads this as fresh, not stale.
    handoff_id = submit_handoff(handoffs, commits=["integration1"])
    runner.emit_handoff_ack = True
    rotation = make_rotation(
        database,
        handoffs,
        cells,
        bindings,
        seater,
        worktree=make_worktree_state(
            head="integration1", origin_head="", head_is_integration_ancestor=True
        ),
    )

    report = await rotation.rotate("cell-demo")

    assert report.ok is True
    assert report.phase == "complete"
    assert report.handoff_id == handoff_id


@pytest.mark.asyncio
async def test_zero_work_head_not_integration_ancestor_blocks(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
) -> None:
    """No remote issue branch, but local HEAD is NOT a proven ancestor
    of the fetched integration head — either it carries unpushed local
    implementation commits (which are never ancestors of
    ``origin/HEAD``), or the ancestor probe itself could not prove the
    relationship. Either way this is not the zero-work shape, so the
    strict refusal still applies."""
    await start_cell(cells, queue)
    submit_handoff(handoffs)
    rotation = make_rotation(
        database,
        handoffs,
        cells,
        bindings,
        seater,
        worktree=make_worktree_state(
            head="local1", origin_head="", head_is_integration_ancestor=False
        ),
    )

    report = await rotation.rotate("cell-demo")

    assert report.ok is False
    assert report.phase == "precondition"
    assert "origin" in (report.failure or "").lower()
    assert seater.calls == []


@pytest.mark.asyncio
async def test_remote_issue_branch_present_still_blocks_despite_ancestor_proof(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
) -> None:
    """A remote issue branch exists but disagrees with local HEAD: the
    zero-work exception only ever applies when ``origin_head == ""``,
    so this keeps today's strict refusal verbatim regardless of the
    ancestor proof."""
    await start_cell(cells, queue)
    submit_handoff(handoffs)
    rotation = make_rotation(
        database,
        handoffs,
        cells,
        bindings,
        seater,
        worktree=make_worktree_state(
            head="local1",
            origin_head="origin1",
            head_is_integration_ancestor=True,
        ),
    )

    report = await rotation.rotate("cell-demo")

    assert report.ok is False
    assert report.phase == "precondition"
    assert "origin" in (report.failure or "").lower()
    assert seater.calls == []


@pytest.mark.asyncio
async def test_dirty_zero_work_shaped_worktree_still_blocks_precondition(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
) -> None:
    """The dirty check runs before the zero-work exception is even
    considered: uncommitted changes refuse rotation even when the rest
    of the state otherwise matches the zero-work shape."""
    await start_cell(cells, queue)
    submit_handoff(handoffs)
    rotation = make_rotation(
        database,
        handoffs,
        cells,
        bindings,
        seater,
        worktree=make_worktree_state(
            dirty=True,
            head="integration1",
            origin_head="",
            head_is_integration_ancestor=True,
        ),
    )

    report = await rotation.rotate("cell-demo")

    assert report.ok is False
    assert report.phase == "precondition"
    assert "uncommitted" in (report.failure or "")
    assert seater.calls == []


@pytest.mark.asyncio
async def test_happy_path_runs_ack_transfer_and_seat_in_order(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """Also exercises the INFRA-184 staleness gate's fresh path: the
    submitted handoff's branch/HEAD match the current worktree and the
    issue is still in-progress, so rotation proceeds exactly as before —
    a matching submitted handoff is never treated as stale."""
    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    runner.emit_handoff_ack = True
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is True
    assert report.phase == "complete"
    assert report.cell_id == "cell-demo"
    assert report.handoff_id == handoff_id
    assert report.replacement_session == str(REPLACEMENT_SESSION)
    assert report.profile == "max-b"
    assert report.binding_id is not None

    # dispatch's own start + rotate's replacement ack turn.
    assert runner.start_count == 2
    assert runner.retired_sessions == [SESSION_ID]
    acknowledged = handoffs.get(handoff_id)
    assert acknowledged.state == "acknowledged"
    # Sol b4b545f3 P2: the selected identities became durable with the
    # acknowledgement, so a crash from here on is recoverable.
    assert acknowledged.replacement_session_id == REPLACEMENT_SESSION
    assert acknowledged.replacement_profile_alias == "max-b"
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(REPLACEMENT_SESSION)

    [call] = seater.calls
    assert call["project_key"] == "demo"
    assert call["cell_id"] == "cell-demo"
    assert call["session_id"] == str(REPLACEMENT_SESSION)
    assert call["profile_alias"] == "max-b"
    assert call["classic_command"] == classic_resume_command(
        str(REPLACEMENT_SESSION), resume=True
    )
    binding = bindings.active_lead("cell-demo")
    assert binding is not None
    assert binding.session_id == str(REPLACEMENT_SESSION)


@pytest.mark.asyncio
async def test_transfer_phase_propagates_rotation_blocked(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    profiles: ProfilePool,
) -> None:
    await start_cell(cells, queue)
    submit_handoff(handoffs)
    for alias in ("max-b", "max-c", "max-d"):
        profiles.record_health(
            ProfileHealth(
                profile_alias=alias, eligible=False, reason="down", last_checked_at=NOW
            )
        )
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is False
    assert report.phase == "transfer"
    assert "healthy profile" in (report.failure or "")
    assert seater.calls == []


@pytest.mark.asyncio
async def test_resume_after_crash_handoff_acknowledged_cell_not_yet_rotated(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """A prior run durably acknowledged the handoff (persisting the selected
    replacement session AND profile) but crashed before cells.rotate ever
    committed its transactional transfer: cell.session_id is still the
    incumbent. Recovery reconstructs the persisted identities and completes
    the transfer with ZERO additional runner or acknowledgement calls
    (Sol correction b4b545f3 packet 2, required test 1)."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    handoffs.acknowledge(
        handoff_id,
        REPLACEMENT_SESSION,
        "Run the failing test and finish INFRA-197.",
        profile_alias="max-b",
    )
    assert runner.start_count == 1  # only the initial dispatch so far
    ack_calls_after_durable = handoffs.acknowledge_calls
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is True
    assert report.phase == "complete"
    assert report.replacement_session == str(REPLACEMENT_SESSION)
    assert report.profile == "max-b"
    # ZERO additional runner or acknowledgement calls during recovery.
    assert runner.start_count == 1
    assert runner.resume_count == 0
    assert handoffs.acknowledge_calls == ack_calls_after_durable
    assert runner.retired_sessions == [SESSION_ID]
    assert len(seater.calls) == 1
    binding = bindings.active_lead("cell-demo")
    assert binding is not None
    assert binding.session_id == str(REPLACEMENT_SESSION)
    assert database.scalar(
        "SELECT profile_alias FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == "max-b"


@pytest.mark.asyncio
async def test_recovery_preserves_the_originally_selected_profile(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: CountingHandoffs,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
    profiles: ProfilePool,
) -> None:
    """Sol b4b545f3 P2, required test 2: recovery must complete with the
    profile persisted at acknowledgement even when current availability and
    selection ordering have changed since — a fresh selection today would
    pick max-b and would refuse the now-ineligible max-c."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    handoffs.acknowledge(
        handoff_id,
        REPLACEMENT_SESSION,
        "Run the failing test and finish INFRA-197.",
        profile_alias="max-c",
    )
    # Availability changed after the durable selection.
    profiles.record_health(
        ProfileHealth(
            profile_alias="max-c",
            eligible=False,
            reason="auth probe failed after selection",
            last_checked_at=NOW,
        )
    )
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is True
    assert report.phase == "complete"
    assert report.profile == "max-c"
    assert report.replacement_session == str(REPLACEMENT_SESSION)
    assert runner.start_count == 1  # no reselection turn, no new runner call
    assert database.scalar(
        "SELECT profile_alias FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == "max-c"
    assert database.scalar(
        "SELECT profile_alias FROM profile_leases WHERE project_key = 'demo'"
    ) == "max-c"


@pytest.mark.asyncio
async def test_repeated_recovery_produces_one_replacement_and_one_execution(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: CountingHandoffs,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """Sol b4b545f3 P2, required test 3: repeated recovery across the
    acknowledgement, transfer, and seating boundaries yields exactly one
    replacement lead and one effective handoff execution."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    # Boundary 1: acknowledgement is durable, transfer never ran.
    handoffs.acknowledge(
        handoff_id,
        REPLACEMENT_SESSION,
        "Run the failing test and finish INFRA-197.",
        profile_alias="max-b",
    )
    ack_calls_after_durable = handoffs.acknowledge_calls
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    # Boundary 2: the transfer commits but the process dies at seating.
    seater.fail_next = True
    first = await rotation.rotate("cell-demo")
    assert first.ok is False
    assert first.phase == "seat"
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(REPLACEMENT_SESSION)

    # Boundary 3: recovery completes seating. A further call is a NEW
    # rotation request; the consumed handoff never satisfies it — the
    # rotation files the fresh-handoff request and reports awaiting
    # instead of transferring anything (Sol 3c1651df / 52d15493).
    second = await rotation.rotate("cell-demo")
    assert second.ok is True
    assert second.phase == "complete"
    third = await rotation.rotate("cell-demo")
    assert third.ok is False
    assert third.phase == "awaiting_handoff"
    assert third.failure is None
    assert third.request_id is not None

    # One replacement lead, one effective handoff execution.
    assert runner.start_count == 1  # the initial dispatch only
    assert handoffs.acknowledge_calls == ack_calls_after_durable
    assert runner.retired_sessions == [SESSION_ID]
    assert database.scalar(
        "SELECT count(*) FROM events WHERE event_type = 'project_cell.rotated'"
    ) == 1
    lease_rows = database.execute(
        "SELECT profile_alias, project_key FROM profile_leases"
    ).fetchall()
    assert [
        (str(row["profile_alias"]), str(row["project_key"])) for row in lease_rows
    ] == [("max-b", "demo")]
    binding = bindings.active_lead("cell-demo")
    assert binding is not None
    assert binding.session_id == str(REPLACEMENT_SESSION)


@pytest.mark.asyncio
async def test_resume_after_crash_cell_rotated_seat_still_incumbent(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """A prior run completed cells.rotate() in full (the transactional
    transfer committed) but crashed before the seat phase: the incumbent
    session's seat is still active. Only the seat phase may run."""

    await start_cell(cells, queue)
    bindings.bind_lead(
        project_key="demo",
        cell_id="cell-demo",
        session_id=str(SESSION_ID),
        profile_alias="max-a",
        ref=CmuxSurfaceRef(workspace_uuid="ws-incumbent", surface_uuid="sf-incumbent"),
    )
    handoff_id = submit_handoff(handoffs)
    runner.emit_handoff_ack = True
    journal_rotation_attempt(database, handoff_id)
    rotated = await cells.rotate("cell-demo", handoff_id)
    assert rotated.session_id == REPLACEMENT_SESSION
    start_count_after_transfer = runner.start_count
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is True
    assert report.phase == "complete"
    assert report.replacement_session == str(REPLACEMENT_SESSION)
    # No second ack turn / transfer: cells.rotate() was not invoked again.
    assert runner.start_count == start_count_after_transfer
    assert len(seater.calls) == 1
    incumbent_binding = bindings.get("binding-1")
    assert incumbent_binding.state == "closed"
    active = bindings.active_lead("cell-demo")
    assert active is not None
    assert active.session_id == str(REPLACEMENT_SESSION)


@pytest.mark.asyncio
async def test_completed_rotation_handoff_never_satisfies_a_new_request(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """Sol 3c1651df: after a rotation reports ``complete``, its handoff is
    consumed. A later rotate-lead call is a NEW request: nothing may
    transfer off the stale handoff — and instead of a terminal operator
    instruction the rotation files the fresh durable handoff request and
    reports awaiting (Sol 52d15493)."""

    await start_cell(cells, queue)
    submit_handoff(handoffs)
    runner.emit_handoff_ack = True
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    first = await rotation.rotate("cell-demo")
    assert first.ok is True
    assert first.phase == "complete"
    start_count_after_first = runner.start_count
    calls_after_first = len(seater.calls)

    second = await rotation.rotate("cell-demo")

    assert second.ok is False
    assert second.phase == "awaiting_handoff"
    assert second.failure is None
    assert second.request_id is not None
    # Nothing moved: no second ack turn, lease, or seat activation.
    assert runner.start_count == start_count_after_first
    assert len(seater.calls) == calls_after_first
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(REPLACEMENT_SESSION)


@pytest.mark.asyncio
async def test_pre_journal_stale_handoff_fails_closed(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """The live-defect shape itself: an acknowledged handoff whose
    replacement already equals the durable incumbent, with NO journaled
    attempt (it predates the attempt journal — e.g. handoff c194833f).
    rotate-lead must not report ok and must not transfer off the stale
    handoff; it files the fresh-handoff request and reports awaiting
    (Sol 52d15493)."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    handoffs.acknowledge(
        handoff_id,
        SESSION_ID,
        "stale: replacement is the incumbent",
        profile_alias="max-a",
    )
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is False
    assert report.phase == "awaiting_handoff"
    assert report.failure is None
    assert report.request_id is not None
    assert runner.start_count == 1  # only the initial dispatch
    assert seater.calls == []
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(SESSION_ID)


def journal_rotation_attempt(database: Database, handoff_id: str) -> None:
    """Lay down the write-ahead ``lead_rotation.attempt`` a crashed real
    run would have journaled before its transactional transfer."""

    from hermes_orchestrator.events import EventInput, EventStore

    events = EventStore(database)
    with database.transaction() as connection:
        events.append(
            connection,
            EventInput(
                event_type="lead_rotation.attempt",
                aggregate_type="handoff",
                aggregate_id=handoff_id,
                payload={"cell_id": "cell-demo"},
            ),
        )


def null_out_replacement_profile(database: Database, handoff_id: str) -> None:
    """Regress an acknowledged row to its migration-era shape: rows
    acknowledged before migration 0051 carry no replacement profile."""

    with database.transaction() as connection:
        connection.execute(
            "UPDATE handoffs SET replacement_profile_alias = NULL "
            "WHERE handoff_id = ?",
            (handoff_id,),
        )


@pytest.mark.asyncio
async def test_legacy_acknowledged_handoff_without_profile_makes_zero_runner_calls(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: CountingHandoffs,
    runner: RecordingRunner,
) -> None:
    """Sol f0a5a403 P1, required test 1: an acknowledged handoff whose
    replacement_profile_alias is NULL (migration-era row) must fail closed
    with an actionable backfill message — never repeat selection and
    acknowledgement by relaunching a lead."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    handoffs.acknowledge(
        handoff_id,
        REPLACEMENT_SESSION,
        "Run the failing test and finish INFRA-197.",
        profile_alias="max-b",
    )
    null_out_replacement_profile(database, handoff_id)
    ack_calls_after_durable = handoffs.acknowledge_calls
    assert runner.start_count == 1  # only the initial dispatch so far

    with pytest.raises(RotationBlocked, match="re-acknowledg"):
        await cells.rotate("cell-demo", handoff_id)

    # ZERO runner calls: no relaunch, no resume, no retirement.
    assert runner.start_count == 1
    assert runner.resume_count == 0
    assert runner.retired_sessions == []
    assert handoffs.acknowledge_calls == ack_calls_after_durable
    # The cell is untouched: still the incumbent session and profile.
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(SESSION_ID)
    assert database.scalar(
        "SELECT profile_alias FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == "max-a"


@pytest.mark.asyncio
async def test_legacy_acknowledged_handoff_reserves_no_capacity_and_moves_no_lease(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: CountingHandoffs,
    profiles: ProfilePool,
) -> None:
    """Sol f0a5a403 P1, required test 2: the same legacy row must produce
    zero replacement-profile reservations and zero lease changes."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    handoffs.acknowledge(
        handoff_id,
        REPLACEMENT_SESSION,
        "Run the failing test and finish INFRA-197.",
        profile_alias="max-b",
    )
    null_out_replacement_profile(database, handoff_id)
    reserve_calls: list[tuple[str, str]] = []
    original_reserve = profiles.reserve_replacement

    def counting_reserve(project_key: str, exclude_alias: str) -> object:
        reserve_calls.append((project_key, exclude_alias))
        return original_reserve(project_key, exclude_alias)

    profiles.reserve_replacement = counting_reserve  # type: ignore[method-assign]

    with pytest.raises(RotationBlocked):
        await cells.rotate("cell-demo", handoff_id)

    assert reserve_calls == []
    lease_rows = database.execute(
        "SELECT profile_alias, project_key FROM profile_leases"
    ).fetchall()
    assert [
        (str(row["profile_alias"]), str(row["project_key"])) for row in lease_rows
    ] == [("max-a", "demo")]
    assert profiles.acquire("demo").profile_alias == "max-a"


@pytest.mark.asyncio
async def test_fully_identified_acknowledged_handoff_still_recovers_without_relaunch(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: CountingHandoffs,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """Sol f0a5a403 P1, required test 3: an acknowledged handoff with BOTH
    persisted identities keeps the direct transfer-and-seating recovery
    path — zero relaunches, exactly one seat activation."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    handoffs.acknowledge(
        handoff_id,
        REPLACEMENT_SESSION,
        "Run the failing test and finish INFRA-197.",
        profile_alias="max-b",
    )
    ack_calls_after_durable = handoffs.acknowledge_calls
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is True
    assert report.phase == "complete"
    assert report.replacement_session == str(REPLACEMENT_SESSION)
    assert report.profile == "max-b"
    assert runner.start_count == 1  # the initial dispatch only — no relaunch
    assert runner.resume_count == 0
    assert handoffs.acknowledge_calls == ack_calls_after_durable
    assert runner.retired_sessions == [SESSION_ID]
    assert len(seater.calls) == 1
    binding = bindings.active_lead("cell-demo")
    assert binding is not None
    assert binding.session_id == str(REPLACEMENT_SESSION)
    assert database.scalar(
        "SELECT profile_alias FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == "max-b"


@pytest.mark.asyncio
async def test_replacement_without_channel_registration_blocks_until_it_lands(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    runner: RecordingRunner,
) -> None:
    """INFRA-207: a seat with no active hermes-control registration is
    reported blocked (never complete); transfer, handoff, and the bound
    seat are untouched, and a rerun after the registration lands
    completes idempotently with no extra seater calls or runner starts."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    runner.emit_handoff_ack = True
    seater = RecordingSeater(bindings, database, registers_channel=False)
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    first = await rotation.rotate("cell-demo")

    assert first.ok is False
    assert first.phase == "channel_registration"
    assert first.cell_id == "cell-demo"
    assert first.handoff_id == handoff_id
    assert first.replacement_session == str(REPLACEMENT_SESSION)
    assert first.profile == "max-b"
    assert first.binding_id is not None
    assert str(REPLACEMENT_SESSION) in (first.failure or "")
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(REPLACEMENT_SESSION)
    assert handoffs.get(handoff_id).state == "acknowledged"
    binding = bindings.active_lead("cell-demo")
    assert binding is not None
    assert binding.session_id == str(REPLACEMENT_SESSION)
    assert binding.binding_id == first.binding_id
    calls_after_first = len(seater.calls)
    start_count_after_first = runner.start_count

    insert_active_registration(database, session_id=str(REPLACEMENT_SESSION))
    second = await rotation.rotate("cell-demo")

    assert second.ok is True
    assert second.phase == "complete"
    assert second.binding_id == first.binding_id
    assert len(seater.calls) == calls_after_first
    assert runner.start_count == start_count_after_first


# -- atomic rotation attempt claim (Sol 524a38ed finding 3) ---------------


def rotation_event_count(database: Database, event_type: str) -> int:
    return int(
        database.scalar(
            "SELECT COUNT(*) FROM events WHERE event_type = ?", (event_type,)
        )
    )


def backdate_rotation_attempts(database: Database, *, seconds: float) -> None:
    """Age every journaled rotation attempt past the claim-age bound, as
    if its claimant crashed long ago."""

    stamp = (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()
    with database.transaction() as connection:
        connection.execute(
            "UPDATE events SET occurred_at = ? "
            "WHERE event_type = 'lead_rotation.attempt'",
            (stamp,),
        )


class CountingCells:
    """Duck-typed pass-through over the real cell service that can stall
    exactly one transfer between the atomic claim and ``cells.rotate``,
    so a test deterministically interleaves a second rotate-lead while
    the first durably holds the journaled claim."""

    def __init__(self, inner: ProjectCellService) -> None:
        self._inner = inner
        self.rotate_calls = 0
        self.stall_next = False
        self.release = asyncio.Event()

    async def rotate(self, cell_id: str, handoff_id: str) -> object:
        self.rotate_calls += 1
        if self.stall_next:
            self.stall_next = False
            await self.release.wait()
        return await self._inner.rotate(cell_id, handoff_id)


@pytest.mark.asyncio
async def test_concurrent_rotations_one_claim_one_launch_loser_fails_closed(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """Sol 524a38ed finding 3, required test 5: two concurrent
    rotate-lead calls over one handoff produce exactly one claim, one
    launch, one transfer, and one completion; the loser's atomic claim
    sees the open attempt it did not create and fails closed with no
    side effects."""

    await start_cell(cells, queue)
    submit_handoff(handoffs)
    runner.emit_handoff_ack = True
    winner_cells = CountingCells(cells)
    winner_cells.stall_next = True
    winner = make_rotation(database, handoffs, winner_cells, bindings, seater)
    loser_cells = CountingCells(cells)
    loser_seater = RecordingSeater(bindings, database)
    loser = make_rotation(
        database, handoffs, loser_cells, bindings, loser_seater
    )

    task = asyncio.create_task(winner.rotate("cell-demo"))
    while winner_cells.rotate_calls == 0:
        await asyncio.sleep(0)
    # The winner committed its atomic claim and stalls BEFORE the
    # transactional transfer — the exact window the old outside-the-
    # transaction check let a second process race through.
    assert rotation_event_count(database, "lead_rotation.attempt") == 1

    blocked = await loser.rotate("cell-demo")

    assert blocked.ok is False
    assert blocked.phase == "claim"
    assert "rotation attempt already in progress" in (blocked.failure or "")
    # The loser performed NO transfer, NO launch, and journaled nothing.
    assert loser_cells.rotate_calls == 0
    assert loser_seater.calls == []
    assert rotation_event_count(database, "lead_rotation.attempt") == 1
    assert rotation_event_count(database, "lead_rotation.completed") == 0
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(SESSION_ID)

    winner_cells.release.set()
    report = await task

    assert report.ok is True
    assert report.phase == "complete"
    assert report.replacement_session == str(REPLACEMENT_SESSION)
    # One claim, one transfer, one launch, one completion — total.
    assert rotation_event_count(database, "lead_rotation.attempt") == 1
    assert rotation_event_count(database, "lead_rotation.completed") == 1
    assert winner_cells.rotate_calls == 1
    assert len(seater.calls) == 1
    assert database.scalar(
        "SELECT COUNT(*) FROM events WHERE event_type = 'project_cell.rotated'"
    ) == 1
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(REPLACEMENT_SESSION)


@pytest.mark.asyncio
async def test_a_live_claim_blocks_a_new_request_until_it_ages(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """An open attempt YOUNGER than the claim-age bound presumes a live
    claimant: a competing rotate-lead fails closed — no transfer, no
    launch, no second attempt journaled."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    journal_rotation_attempt(database, handoff_id)  # fresh: presumed live
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is False
    assert report.phase == "claim"
    assert "rotation attempt already in progress" in (report.failure or "")
    assert seater.calls == []
    assert runner.start_count == 1  # the initial dispatch only
    assert rotation_event_count(database, "lead_rotation.attempt") == 1
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(SESSION_ID)


@pytest.mark.asyncio
async def test_aged_claim_from_a_crashed_winner_still_rotates(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """Crash-resume survives the claim: the winner journaled its attempt
    and died before the transfer committed. A retry past the expiry
    bound presumes the claimant dead and adopts ownership through the
    atomic takeover CAS (Sol 04d013b0 finding 4) — exactly one takeover
    event references the expired claim, never a second attempt — then
    completes the rotation."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    runner.emit_handoff_ack = True
    journal_rotation_attempt(database, handoff_id)
    backdate_rotation_attempts(database, seconds=3600.0)
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is True
    assert report.phase == "complete"
    assert report.replacement_session == str(REPLACEMENT_SESSION)
    # Takeover, not duplication: one attempt, one takeover, one close.
    assert rotation_event_count(database, "lead_rotation.attempt") == 1
    assert rotation_event_count(database, "lead_rotation.takeover") == 1
    assert rotation_event_count(database, "lead_rotation.completed") == 1
    assert len(seater.calls) == 1
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(REPLACEMENT_SESSION)


# -- durable exclusive ownership over the claim (Sol 04d013b0 f4/f5) ------


@pytest.mark.asyncio
async def test_concurrent_recovery_over_one_expired_attempt_admits_one_adopter(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """Sol 04d013b0 finding 4, required test 4 (first half): two
    concurrent recovery callers over one EXPIRED attempt race the
    takeover CAS — exactly one appends the takeover and crosses into
    cells.rotate; the loser's serialized transaction observes the fresh
    takeover it did not create and fails closed with zero side effects
    (the old age-only heuristic let both pass, rotate_calls 1/1)."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    runner.emit_handoff_ack = True
    journal_rotation_attempt(database, handoff_id)
    backdate_rotation_attempts(database, seconds=3600.0)
    winner_cells = CountingCells(cells)
    winner_cells.stall_next = True
    winner = make_rotation(database, handoffs, winner_cells, bindings, seater)
    loser_cells = CountingCells(cells)
    loser_seater = RecordingSeater(bindings, database)
    loser = make_rotation(database, handoffs, loser_cells, bindings, loser_seater)

    task = asyncio.create_task(winner.rotate("cell-demo"))
    while winner_cells.rotate_calls == 0:
        await asyncio.sleep(0)
    # The winner's takeover CAS committed ownership of the expired
    # claim and it stalls BEFORE the transactional transfer — the exact
    # window the age-only heuristic let a second adopter race through.
    assert rotation_event_count(database, "lead_rotation.takeover") == 1

    blocked = await loser.rotate("cell-demo")

    assert blocked.ok is False
    assert blocked.phase == "claim"
    assert "rotation attempt already in progress" in (blocked.failure or "")
    # The loser never crossed into cells.rotate and journaled nothing.
    assert loser_cells.rotate_calls == 0
    assert loser_seater.calls == []
    assert rotation_event_count(database, "lead_rotation.attempt") == 1
    assert rotation_event_count(database, "lead_rotation.takeover") == 1
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(SESSION_ID)

    winner_cells.release.set()
    report = await task

    assert report.ok is True
    assert report.phase == "complete"
    assert winner_cells.rotate_calls == 1
    assert rotation_event_count(database, "lead_rotation.takeover") == 1
    assert rotation_event_count(database, "lead_rotation.completed") == 1
    assert database.scalar(
        "SELECT COUNT(*) FROM events WHERE event_type = 'project_cell.rotated'"
    ) == 1
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(REPLACEMENT_SESSION)


@pytest.mark.asyncio
async def test_a_live_slow_acknowledgement_stream_is_not_stolen_on_age(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """Sol 04d013b0 finding 4, required test 4 (second half): a live
    owner stalled inside the unbounded acknowledgement stream keeps its
    claim through the in-transfer heartbeat renewals — expiry is
    measured from the NEWEST renewal by the current owner, so wall-clock
    age of the original claim alone can never justify a steal."""

    await start_cell(cells, queue)
    submit_handoff(handoffs)
    runner.emit_handoff_ack = True
    owner_cells = CountingCells(cells)
    owner_cells.stall_next = True
    owner = LeadRotation(
        database=database,
        handoffs=handoffs,
        cells=owner_cells,
        bindings=bindings,
        seater=seater,
        worktree_state=lambda project_key: make_worktree_state(),
        registration_wait_seconds=0,
        renewal_interval_seconds=0.005,
    )
    thief_cells = CountingCells(cells)
    thief_seater = RecordingSeater(bindings, database)
    thief = make_rotation(database, handoffs, thief_cells, bindings, thief_seater)

    task = asyncio.create_task(owner.rotate("cell-demo"))
    while rotation_event_count(database, "lead_rotation.renewed") == 0:
        await asyncio.sleep(0.005)
    # Age the CLAIM far past the expiry bound while the owner is alive
    # (stalled in the acknowledgement stream) and heartbeating.
    backdate_rotation_attempts(database, seconds=3600.0)

    blocked = await thief.rotate("cell-demo")

    assert blocked.ok is False
    assert blocked.phase == "claim"
    assert "rotation attempt already in progress" in (blocked.failure or "")
    assert thief_cells.rotate_calls == 0
    assert thief_seater.calls == []
    assert rotation_event_count(database, "lead_rotation.takeover") == 0
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(SESSION_ID)

    owner_cells.release.set()
    report = await task

    assert report.ok is True
    assert report.phase == "complete"
    assert rotation_event_count(database, "lead_rotation.attempt") == 1
    assert rotation_event_count(database, "lead_rotation.completed") == 1
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(REPLACEMENT_SESSION)


@pytest.mark.asyncio
async def test_concurrent_post_transfer_recovery_completes_exactly_once(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    runner: RecordingRunner,
) -> None:
    """Sol 04d013b0 finding 5, required test 5: two concurrent
    post-transfer recovery calls converge on ONE ``lead_rotation.
    completed`` event and ONE complete report — the atomic
    check-and-complete admits exactly one closer — with no duplicate
    seat, launch, or transfer side effects (the seat phase's durable
    active-binding read keeps the second caller off the seater)."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    runner.emit_handoff_ack = True
    journal_rotation_attempt(database, handoff_id)
    rotated = await cells.rotate("cell-demo", handoff_id)
    assert rotated.session_id == REPLACEMENT_SESSION
    start_count_after_transfer = runner.start_count

    gate = asyncio.Event()
    parked = {"count": 0}

    async def parked_sleep(seconds: float) -> None:
        parked["count"] += 1
        await gate.wait()

    def recovery(seat: RecordingSeater) -> LeadRotation:
        return LeadRotation(
            database=database,
            handoffs=handoffs,
            cells=cells,
            bindings=bindings,
            seater=seat,
            worktree_state=lambda project_key: make_worktree_state(),
            registration_wait_seconds=5,
            sleep=parked_sleep,
        )

    seater_a = RecordingSeater(bindings, database, registers_channel=False)
    seater_b = RecordingSeater(bindings, database, registers_channel=False)
    task_a = asyncio.create_task(recovery(seater_a).rotate("cell-demo"))
    while parked["count"] == 0:
        await asyncio.sleep(0)
    task_b = asyncio.create_task(recovery(seater_b).rotate("cell-demo"))
    while parked["count"] < 2:
        await asyncio.sleep(0)
    # Both recovery calls observed the OPEN attempt and are parked in
    # the registration wait; the registration lands and both race the
    # atomic completion.
    insert_active_registration(database, session_id=str(REPLACEMENT_SESSION))
    gate.set()
    report_a = await task_a
    report_b = await task_b

    complete = [r for r in (report_a, report_b) if r.ok]
    assert len(complete) == 1
    assert complete[0].phase == "complete"
    assert complete[0].replacement_session == str(REPLACEMENT_SESSION)
    [lost] = [r for r in (report_a, report_b) if not r.ok]
    assert lost.phase == "completed_elsewhere"
    assert "concurrent recovery call" in (lost.failure or "")
    assert rotation_event_count(database, "lead_rotation.completed") == 1
    # No duplicate side effects: one seat activation total, no new
    # runner launches, one transactional transfer.
    assert len(seater_a.calls) + len(seater_b.calls) == 1
    assert runner.start_count == start_count_after_transfer
    assert database.scalar(
        "SELECT COUNT(*) FROM events WHERE event_type = 'project_cell.rotated'"
    ) == 1
    binding = bindings.active_lead("cell-demo")
    assert binding is not None
    assert binding.session_id == str(REPLACEMENT_SESSION)


# -- fresh-handoff request and event-driven continuation (Sol 52d15493) ----


def make_cells_with_replacements(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    runner: RecordingRunner,
    handoffs: HandoffService,
    tmp_path: Path,
    replacement_ids: Iterator[UUID],
) -> ProjectCellService:
    """The fixture cell service, but with a replacement-session SEQUENCE:
    the continuation's second rotation must mint a session different from
    the first rotation's replacement (now the incumbent)."""

    from hermes_orchestrator.events import EventStore

    return ProjectCellService(
        database=database,
        events=EventStore(database),
        queue=queue,
        profiles=profiles,
        runner=runner,
        linear=RecordingLinear(),
        project_paths={"demo": tmp_path},
        session_ids=lambda: SESSION_ID,
        cell_ids=lambda: "cell-demo",
        now=lambda: NOW,
        handoffs=handoffs,
        replacement_session_ids=lambda: next(replacement_ids),
    )


async def consume_one_full_rotation(
    cells: ProjectCellService,
    queue: QueueService,
    handoffs: HandoffService,
    runner: RecordingRunner,
    rotation: LeadRotation,
) -> str:
    """Dispatch, submit, and fully rotate once: the handoff is consumed
    and the incumbent is now the first rotation's replacement."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)
    runner.emit_handoff_ack = True
    first = await rotation.rotate("cell-demo")
    assert first.ok is True
    assert first.phase == "complete"
    return handoff_id


@pytest.mark.asyncio
async def test_consumed_handoff_files_the_fresh_request_with_derived_fields(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """INFRA-198 required test 1: a rotate-lead over the consumed newest
    handoff files the durable fresh-handoff request through the existing
    channel — the ``handoff_requests`` row and the deduplicated
    ``handoff_required`` wake carry the mechanically derived facts, the
    incumbent is asked ONLY for the non-derivable content — and the
    report is awaiting/requested, never the terminal operator
    instruction. A repeat call duplicates nothing."""

    rotation = make_rotation(database, handoffs, cells, bindings, seater)
    consumed = await consume_one_full_rotation(
        cells, queue, handoffs, runner, rotation
    )
    start_count_after_first = runner.start_count
    calls_after_first = len(seater.calls)

    report = await rotation.rotate("cell-demo")

    assert report.ok is False
    assert report.phase == "awaiting_handoff"
    assert report.failure is None
    assert report.handoff_id == consumed
    assert report.request_id is not None

    # The durable request row, on the EXISTING handoff_requests channel,
    # carries every mechanically derived fact ...
    request = database.execute(
        "SELECT request_id, state, reason FROM handoff_requests "
        "WHERE cell_id = 'cell-demo'"
    ).fetchone()
    assert request is not None
    assert str(request["request_id"]) == report.request_id
    assert str(request["state"]) == "requested"
    reason = str(request["reason"])
    assert f"consumed={consumed}" in reason
    assert "cell=cell-demo" in reason
    assert "project=demo" in reason
    assert f"session={REPLACEMENT_SESSION}" in reason
    assert "profile=max-b" in reason
    assert "issue=ENG-9" in reason
    assert "branch=feature/infra-197" in reason
    assert "head=deadbeef" in reason
    # ... and asks the incumbent ONLY for the non-derivable content.
    assert (
        "provide only decisions, caveats/blockers, risks, "
        "and the exact next action" in reason
    )

    # The incumbent is woken through the existing terminal-wake outbox,
    # bound to the handoff_required evidence identity.
    wake = database.execute(
        "SELECT kind, session_id, profile_alias, reason, state, turn_key "
        "FROM lead_terminal_wakes WHERE cell_id = 'cell-demo'"
    ).fetchall()
    assert len(wake) == 1
    assert str(wake[0]["kind"]) == "handoff_required"
    assert str(wake[0]["session_id"]) == str(REPLACEMENT_SESSION)
    assert str(wake[0]["profile_alias"]) == "max-b"
    assert str(wake[0]["reason"]) == reason
    assert str(wake[0]["turn_key"]).startswith("handoff:")
    assert database.scalar(
        "SELECT state FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == "handoff_required"

    # Nothing transferred or launched, and a repeat call is idempotent:
    # same awaiting report, no duplicate request, wake, or marker.
    repeat = await rotation.rotate("cell-demo")
    assert repeat.phase == "awaiting_handoff"
    assert repeat.request_id == report.request_id
    assert runner.start_count == start_count_after_first
    assert len(seater.calls) == calls_after_first
    assert database.scalar(
        "SELECT count(*) FROM handoff_requests WHERE cell_id = 'cell-demo'"
    ) == 1
    assert database.scalar(
        "SELECT count(*) FROM lead_terminal_wakes WHERE cell_id = 'cell-demo'"
    ) == 1
    assert rotation_event_count(database, "lead_rotation.awaiting_handoff") == 1


@pytest.mark.asyncio
async def test_fresh_submission_resumes_the_same_rotation_changing_both(
    database: Database,
    queue: QueueService,
    profiles: ProfilePool,
    handoffs: CountingHandoffs,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
    tmp_path: Path,
) -> None:
    """INFRA-198 required test 2: once the incumbent submits the fresh
    handoff, the SAME awaiting rotation resumes automatically through the
    post-commit listener continuation — exactly as the submission path
    drives it — and the completed rotation changes BOTH the profile and
    the session; the incumbent is retired."""

    from hermes_orchestrator.handoffs import HandoffRecord

    replacement_ids = iter([REPLACEMENT_SESSION, THIRD_SESSION])
    cells = make_cells_with_replacements(
        database, queue, profiles, runner, handoffs, tmp_path, replacement_ids
    )
    rotation = make_rotation(database, handoffs, cells, bindings, seater)
    await consume_one_full_rotation(cells, queue, handoffs, runner, rotation)

    awaiting = await rotation.rotate("cell-demo")
    assert awaiting.phase == "awaiting_handoff"

    # The event-driven continuation, wired the way the submission path
    # wires it: the post-commit signal hands the durable record to
    # resume_on_submission.
    submitted: list[HandoffRecord] = []
    handoffs.subscribe(submitted.append)
    fresh_id = submit_handoff(handoffs)
    assert [record.handoff_id for record in submitted] == [fresh_id]

    resumed = await rotation.resume_on_submission(submitted[0])

    assert resumed is not None
    assert resumed.ok is True
    assert resumed.phase == "complete"
    assert resumed.handoff_id == fresh_id
    # BOTH identities changed relative to the incumbent the awaiting
    # rotation found (the first rotation's replacement, max-b).
    assert resumed.replacement_session == str(THIRD_SESSION)
    assert resumed.replacement_session != str(REPLACEMENT_SESSION)
    assert resumed.profile is not None
    assert resumed.profile != "max-b"
    cell = database.execute(
        "SELECT state, session_id, profile_alias FROM project_cells "
        "WHERE cell_id = 'cell-demo'"
    ).fetchone()
    assert str(cell["state"]) == "active"
    assert str(cell["session_id"]) == str(THIRD_SESSION)
    assert str(cell["profile_alias"]) == resumed.profile
    binding = bindings.active_lead("cell-demo")
    assert binding is not None
    assert binding.session_id == str(THIRD_SESSION)
    assert runner.retired_sessions == [SESSION_ID, REPLACEMENT_SESSION]

    # Ownership-claim invariants held across both rotations: one open
    # attempt per handoff, each closed by exactly one completion; the
    # one awaiting marker was resumed exactly once.
    assert rotation_event_count(database, "lead_rotation.attempt") == 2
    assert rotation_event_count(database, "lead_rotation.completed") == 2
    assert rotation_event_count(database, "lead_rotation.awaiting_handoff") == 1
    assert rotation_event_count(database, "lead_rotation.awaiting_resumed") == 1

    # A replayed signal for the same record never re-drives anything.
    assert await rotation.resume_on_submission(submitted[0]) is None


@pytest.mark.asyncio
async def test_ordinary_handoff_submission_never_triggers_the_continuation(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """An ordinary handoff request (stall remedy / red pressure) followed
    by a submission carries NO awaiting marker: the continuation returns
    None, journals nothing, and rotates nothing."""

    await start_cell(cells, queue)
    assert cells.request_checkpoint("cell-demo", "stall:no_material_change")
    fresh_id = submit_handoff(handoffs)
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    assert await rotation.resume_on_submission(handoffs.get(fresh_id)) is None

    assert rotation_event_count(database, "lead_rotation.attempt") == 0
    assert rotation_event_count(database, "lead_rotation.awaiting_resumed") == 0
    assert seater.calls == []
    assert runner.start_count == 1  # the initial dispatch only
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(SESSION_ID)


# -- session vs. account rotation kind, derived end to end (INFRA-184) -----


@pytest.mark.asyncio
async def test_provider_limit_cause_derives_account_rotation_kind(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """A provider-limit-caused handoff episode (``request_checkpoint``
    with the ``"subscription_limit"`` reason ``_require_handoff``'s own
    ``provider.limit`` callers write) derives the ACCOUNT rotation kind
    end to end: the completed rotation lands on a genuinely different
    profile from the incumbent's, exactly as before this issue."""

    await start_cell(cells, queue)
    assert cells.request_checkpoint("cell-demo", "subscription_limit") is True
    submit_handoff(handoffs)
    runner.emit_handoff_ack = True
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is True
    assert report.phase == "complete"
    assert report.profile == "max-b"


@pytest.mark.asyncio
async def test_context_and_ordinary_handoff_causes_derive_session_rotation_kind(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """A context-threshold cause derives the SESSION rotation kind end to
    end: the completed rotation stays on the incumbent's own healthy
    profile with only a fresh session. An ordinary operator handoff
    request (a ``request_checkpoint`` reason that names neither a
    provider limit nor the rotation's own internal fresh-handoff
    bookkeeping -- e.g. a six-hour-renewal or stall-remedy request)
    derives the very same session kind."""

    await start_cell(cells, queue)
    assert cells.request_checkpoint("cell-demo", "context_rotation") is True
    submit_handoff(handoffs)
    runner.emit_handoff_ack = True
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is True
    assert report.phase == "complete"
    assert report.profile == "max-a"
    assert report.replacement_session == str(REPLACEMENT_SESSION)


# -- submitted-but-stale handoff staleness gate (INFRA-184 / INFRA-192) ----


def handoff_request_reason(database: Database) -> str:
    return str(
        database.execute(
            "SELECT reason FROM handoff_requests WHERE cell_id = 'cell-demo'"
        ).fetchone()["reason"]
    )


@pytest.mark.asyncio
async def test_submitted_but_stale_handoff_requests_fresh_handoff(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """Reproduces the observed INFRA-192 regression: rotate-lead consumed
    a still-``submitted`` handoff whose recorded status named an issue
    that was ``in_development`` at submission, but the issue has since
    merged to ``done`` while the handoff's next action still presumes
    ongoing implementation. Branch and HEAD otherwise MATCH the current
    worktree — isolating the issue criterion (INFRA-184 correction: this
    must be detected by looking up the SPECIFIC issue the handoff
    recorded, never "the project's most recently updated admitted
    issue"). rotate-lead must never transfer the cell onto it: it routes
    into the SAME fresh-handoff request path the already-consumed
    handoff uses, journals the stale evidence on the existing durable
    request/wake channel, and leaves the stale handoff untouched and
    reusable (still ``submitted``)."""

    await start_cell(cells, queue)
    handoff_id = submit_derived_handoff(handoffs, issue_state="in_development")
    queue.complete("ENG-9", reason="merged upstream", evidence="pr merged")
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is False
    assert report.phase == "awaiting_handoff"
    assert report.failure is None
    assert report.handoff_id == handoff_id
    assert report.request_id is not None

    # Nothing transferred, launched, or acknowledged.
    assert runner.start_count == 1  # only the initial dispatch
    assert runner.resume_count == 0
    assert seater.calls == []
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(SESSION_ID)

    # The stale handoff stays reusable: never acknowledged or superseded.
    stale = handoffs.get(handoff_id)
    assert stale.state == "submitted"
    assert stale.replacement_session_id is None

    # The compact evidence dict is durably journaled through the
    # existing request/wake channel — branch/HEAD matched, so only the
    # issue criterion (recorded in_development, now done) is why.
    reason = handoff_request_reason(database)
    assert "lead_rotation:stale_handoff_detected" in reason
    assert "handoff_head=deadbeef" in reason
    assert "current_head=deadbeef" in reason
    assert "handoff_branch=feature/infra-197" in reason
    assert "current_branch=feature/infra-197" in reason
    assert "issue_state=done" in reason
    assert rotation_event_count(database, "lead_rotation.attempt") == 0


@pytest.mark.asyncio
async def test_completed_issue_handoff_with_matching_head_is_fresh(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """INFRA-184 correction: a legitimate handoff whose status correctly
    records the issue as ALREADY ``done`` (a cell can carry several
    issue lanes — "issue X is done; next action: dispatch the next
    admitted issue" is exactly the shape that rotated successfully in
    production) must never be treated as stale. The recorded state was
    never in-progress, so the issue criterion does not apply at all;
    branch and HEAD match, so rotation proceeds through the normal path
    with no fresh-handoff request filed."""

    await start_cell(cells, queue)
    queue.complete("ENG-9", reason="merged upstream", evidence="pr merged")
    handoff_id = submit_derived_handoff(
        handoffs,
        issue_state="done",
        next_action="dispatch the next admitted issue",
    )
    runner.emit_handoff_ack = True
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is True
    assert report.phase == "complete"
    assert report.handoff_id == handoff_id
    assert report.replacement_session == str(REPLACEMENT_SESSION)
    # No fresh-handoff request was ever filed: the normal path ran.
    assert database.scalar(
        "SELECT COUNT(*) FROM handoff_requests WHERE cell_id = 'cell-demo'"
    ) == 0


@pytest.mark.asyncio
async def test_newly_admitted_sibling_issue_does_not_mark_handoff_stale(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """INFRA-184 correction: the issue criterion must look up the
    SPECIFIC issue the handoff's status recorded, by id — never "the
    project's most recently updated admitted_issues row". A newly
    admitted sibling issue (state ``queued``, admitted and thus updated
    AFTER the recorded issue) must never poison that lookup: the
    recorded issue (ENG-9) is genuinely still ``in_development``, branch
    and HEAD match, so rotation proceeds through the normal path."""

    await start_cell(cells, queue)
    handoff_id = submit_derived_handoff(handoffs, issue_state="in_development")
    admit(queue, "ENG-10")  # a later-admitted, later-updated sibling issue
    assert (
        database.scalar(
            "SELECT state FROM admitted_issues WHERE issue_id = 'ENG-10'"
        )
        == "queued"
    )
    runner.emit_handoff_ack = True
    rotation = make_rotation(database, handoffs, cells, bindings, seater)

    report = await rotation.rotate("cell-demo")

    assert report.ok is True
    assert report.phase == "complete"
    assert report.handoff_id == handoff_id
    assert report.replacement_session == str(REPLACEMENT_SESSION)
    assert database.scalar(
        "SELECT COUNT(*) FROM handoff_requests WHERE cell_id = 'cell-demo'"
    ) == 0


@pytest.mark.asyncio
async def test_stale_head_alone_with_issue_in_progress_requests_fresh_handoff(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
    """A stale HEAD alone is enough, even while the issue is still
    genuinely in progress: the branch matches and the issue is
    ``in_development``, but the submitted handoff's HEAD no longer
    matches the current worktree HEAD (the worktree moved on since
    submission) — that alone routes into the fresh-handoff request path,
    exactly like the already-consumed case."""

    await start_cell(cells, queue)
    handoff_id = submit_handoff(handoffs)  # commits=["deadbeef"] by default
    rotation = make_rotation(
        database,
        handoffs,
        cells,
        bindings,
        seater,
        worktree=make_worktree_state(head="movedon", origin_head="movedon"),
    )

    report = await rotation.rotate("cell-demo")

    assert report.ok is False
    assert report.phase == "awaiting_handoff"
    assert report.failure is None
    assert report.request_id is not None
    assert seater.calls == []
    assert runner.start_count == 1  # only the initial dispatch
    assert database.scalar(
        "SELECT session_id FROM project_cells WHERE cell_id = 'cell-demo'"
    ) == str(SESSION_ID)

    stale = handoffs.get(handoff_id)
    assert stale.state == "submitted"

    reason = handoff_request_reason(database)
    assert "lead_rotation:stale_handoff_detected" in reason
    assert "handoff_head=deadbeef" in reason
    assert "current_head=movedon" in reason
    # The issue itself is genuinely still in progress — the HEAD
    # mismatch alone is what made this stale.
    assert "issue_state=in_development" in reason
