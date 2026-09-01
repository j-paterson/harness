from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
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
from hermes_orchestrator.handoffs import HandoffDocument, HandoffService, HandoffTest
from hermes_orchestrator.lead_rotation import LeadRotation, WorktreeState
from hermes_orchestrator.linear import LinearProjection
from hermes_orchestrator.profiles import ProfileHealth, ProfilePool, ProfileRegistry
from hermes_orchestrator.queue import QueueService

SESSION_ID = UUID("11111111-1111-4111-8111-111111111111")
REPLACEMENT_SESSION = UUID("22222222-2222-4222-8222-222222222222")
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
    *, dirty: bool = False, head: str = "deadbeef", origin_head: str = "deadbeef"
) -> WorktreeState:
    return WorktreeState(
        branch="feature/infra-197", head=head, origin_head=origin_head, dirty=dirty
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


def submit_handoff(handoffs: HandoffService) -> str:
    document = HandoffDocument(
        cell_id="cell-demo",
        objective="Finish INFRA-197 packet P1",
        status="ready to rotate",
        decisions=["compose from existing primitives"],
        branch="feature/infra-197",
        commits=["abc123"],
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
async def test_happy_path_runs_ack_transfer_and_seat_in_order(
    database: Database,
    queue: QueueService,
    cells: ProjectCellService,
    handoffs: HandoffService,
    bindings: CmuxSurfaceBindings,
    seater: RecordingSeater,
    runner: RecordingRunner,
) -> None:
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
    # rotation request; the consumed handoff refuses it (Sol 3c1651df).
    second = await rotation.rotate("cell-demo")
    assert second.ok is True
    assert second.phase == "complete"
    third = await rotation.rotate("cell-demo")
    assert third.ok is False
    assert third.phase == "precondition"
    assert "stale acknowledged handoff" in (third.failure or "")

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
    consumed. A later rotate-lead call is a NEW request and must fail
    closed instead of returning a free no-op success — the live defect
    reused exactly such a stale handoff while nothing rotated."""

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
    assert second.phase == "precondition"
    assert "stale acknowledged handoff" in (second.failure or "")
    assert "fresh handoff" in (second.failure or "")
    # Nothing moved: no second ack turn, lease, or seat activation.
    assert runner.start_count == start_count_after_first
    assert len(seater.calls) == calls_after_first


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
    rotate-lead must refuse, not report ok, and must not transfer."""

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
    assert report.phase == "precondition"
    assert "stale acknowledged handoff" in (report.failure or "")
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
