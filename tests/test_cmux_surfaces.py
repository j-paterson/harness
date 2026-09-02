from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.channel_trust import (
    APPROVED_PROMPT_PATTERN,
    ChannelTrustAnchors,
    ChannelTrustGate,
    TrustVerdict,
)
from hermes_orchestrator.cmux import (
    CmuxAccessDenied,
    CmuxError,
    CmuxProtocolError,
    CmuxSurfaceRef,
    CmuxUnavailable,
)
from hermes_orchestrator.cmux_surfaces import (
    SKIP_PERMISSIONS_FLAG,
    ChannelTrustConfirmer,
    CmuxBindingConflict,
    CmuxDeadLeadSweep,
    CmuxHibernationGate,
    CmuxSurfaceBindings,
    CmuxSurfaceReconciler,
    HibernationDecision,
    classic_channel_command,
    managed_claude_worker_alive,
)
from hermes_orchestrator.control_operations import ControlOperations
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore

NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)

ORCH = CmuxSurfaceRef(
    workspace_uuid="11111111-1111-4111-8111-111111111111",
    surface_uuid="11111111-1111-4111-8111-222222222222",
)
LEAD = CmuxSurfaceRef(
    workspace_uuid="33333333-3333-4333-8333-333333333333",
    surface_uuid="33333333-3333-4333-8333-444444444444",
)
FRESH = CmuxSurfaceRef(
    workspace_uuid="55555555-5555-4555-8555-555555555555",
    surface_uuid="55555555-5555-4555-8555-666666666666",
)
THIRD = CmuxSurfaceRef(
    workspace_uuid="77777777-7777-4777-8777-777777777777",
    surface_uuid="77777777-7777-4777-8777-888888888888",
)

#: The second visible cell of the same project -- the harness lane's
#: seat, which start-lane creates with no trust anchor of its own.
HARNESS = CmuxSurfaceRef(
    workspace_uuid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    surface_uuid="aaaaaaaa-aaaa-4aaa-8aaa-bbbbbbbbbbbb",
)

SESSION = "99999999-9999-4999-8999-999999999999"
# The replacement session a managed rotation seats.
SESSION_2 = "88888888-8888-4888-8888-888888888888"
# The harness lane's own session.
HARNESS_SESSION = "77777777-7777-4777-8777-999999999999"


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def bindings(database: Database) -> CmuxSurfaceBindings:
    counter = iter(range(1, 100))
    return CmuxSurfaceBindings(
        database=database,
        events=EventStore(database),
        now=lambda: NOW,
        ids=lambda: f"binding-{next(counter)}",
    )


def bind_demo_lead(bindings: CmuxSurfaceBindings, ref: CmuxSurfaceRef = LEAD) -> object:
    return bindings.bind_lead(
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        profile_alias="max-a",
        ref=ref,
    )


def bind_harness_lead(bindings: CmuxSurfaceBindings) -> object:
    return bindings.bind_lead(
        project_key="demo",
        cell_id="cell-harness",
        session_id=HARNESS_SESSION,
        profile_alias="max-a",
        ref=HARNESS,
    )


def event_types(database: Database) -> list[str]:
    rows = database.execute(
        "SELECT event_type FROM events "
        "WHERE aggregate_type = 'cmux_binding' ORDER BY sequence"
    ).fetchall()
    return [str(row["event_type"]) for row in rows]


def intent_event_types(database: Database) -> list[str]:
    rows = database.execute(
        "SELECT event_type FROM events "
        "WHERE aggregate_type = 'cmux_intent' ORDER BY sequence"
    ).fetchall()
    return [str(row["event_type"]) for row in rows]


class SimulatedCrash(BaseException):
    """Process death at an exact point: no except-Exception handler,
    compensation, or context manager in production code may observe it."""


@dataclass
class FakePort:
    live: set[CmuxSurfaceRef] = field(default_factory=set)
    created: list[dict[str, object]] = field(default_factory=list)
    resumes: list[tuple[CmuxSurfaceRef, str]] = field(default_factory=list)
    statuses: list[tuple[str, str, str]] = field(default_factory=list)
    closed: list[str] = field(default_factory=list)
    renames: list[tuple[str, str]] = field(default_factory=list)
    titles: dict[str, str] = field(default_factory=dict)
    deny: bool = False
    next_refs: list[CmuxSurfaceRef] = field(default_factory=list)
    fail: dict[str, CmuxError] = field(default_factory=dict)
    crash: str | None = None
    on_create: Callable[[str], None] | None = None
    screen: str = ""
    screen_reads: int = 0
    confirmed: list[CmuxSurfaceRef] = field(default_factory=list)

    def _maybe_fail(self, operation: str) -> None:
        error = self.fail.get(operation)
        if error is not None:
            raise error

    async def ping(self) -> None:
        if self.deny:
            raise CmuxAccessDenied("cmux socket denied this process")

    async def create_workspace(
        self,
        *,
        title: str,
        cwd: Path,
        command: str | None = None,
        env: dict[str, str] | None = None,
        resolve_marker: str | None = None,
    ) -> CmuxSurfaceRef:
        self._maybe_fail("create_workspace")
        if self.on_create is not None:
            self.on_create(title)
        if self.crash == "before_create":
            raise SimulatedCrash("process died before the external create")
        self.created.append(
            {
                "title": title,
                "cwd": cwd,
                "command": command,
                "env": env,
                "resolve_marker": resolve_marker,
            }
        )
        ref = self.next_refs.pop(0)
        self.live.add(ref)
        self.titles[ref.workspace_uuid] = title
        if self.crash == "after_create":
            # The workspace exists externally, but the caller never
            # observed the returned identities.
            raise SimulatedCrash(
                "process died before the returned identities were persisted"
            )
        return ref

    async def live_workspace_uuids(self) -> frozenset[str]:
        self._maybe_fail("live_workspace_uuids")
        return frozenset(ref.workspace_uuid for ref in self.live)

    async def surface_alive(self, ref: CmuxSurfaceRef) -> bool:
        self._maybe_fail("surface_alive")
        return ref in self.live

    async def close_workspace(self, workspace_uuid: str) -> None:
        self._maybe_fail("close_workspace")
        self.closed.append(workspace_uuid)
        self.live = {ref for ref in self.live if ref.workspace_uuid != workspace_uuid}

    async def set_surface_resume(self, ref: CmuxSurfaceRef, command: str) -> None:
        self._maybe_fail("set_surface_resume")
        self.resumes.append((ref, command))

    async def set_status(self, workspace_uuid: str, key: str, value: str) -> None:
        self.statuses.append((workspace_uuid, key, value))

    async def rename_workspace(self, workspace_uuid: str, title: str) -> None:
        self._maybe_fail("rename_workspace")
        self.renames.append((workspace_uuid, title))
        self.titles[workspace_uuid] = title

    async def read_screen(self, ref: CmuxSurfaceRef, *, lines: int = 60) -> str:
        self._maybe_fail("read_screen")
        self.screen_reads += 1
        return self.screen

    async def confirm_channel_dialog(self, ref: CmuxSurfaceRef) -> None:
        self._maybe_fail("confirm_channel_dialog")
        self.confirmed.append(ref)

    async def find_workspace_uuids(self, *, title_marker: str) -> frozenset[str]:
        self._maybe_fail("find_workspace_uuids")
        # Mirrors the real adapter: the marker matches only as an exact
        # whitespace-delimited token, never as a bare substring.
        pattern = re.compile(rf"(?<!\S){re.escape(title_marker)}(?!\S)")
        live_uuids = {ref.workspace_uuid for ref in self.live}
        return frozenset(
            workspace_uuid
            for workspace_uuid, title in self.titles.items()
            if workspace_uuid in live_uuids and pattern.search(title)
        )


class FakeProfileDirs:
    def __init__(self, dirs: dict[str, Path]) -> None:
        self._dirs = dirs

    def config_dir(self, alias: str) -> Path:
        return self._dirs[alias]


class FakeChannelLaunch:
    """Records launch-material generation and retirement."""

    def __init__(
        self,
        config: Path | None = None,
        error: Exception | None = None,
    ) -> None:
        self.config = config
        self.error = error
        self.generated: list[dict[str, object]] = []
        self.cleaned: list[str] = []

    def generate(self, **kwargs: object) -> Path:
        self.generated.append(kwargs)
        if self.error is not None:
            raise self.error
        assert self.config is not None
        return self.config

    def cleanup(self, session_id: str) -> None:
        self.cleaned.append(session_id)


def reconciler(
    bindings: CmuxSurfaceBindings,
    port: FakePort,
    *,
    environ: dict[str, str] | None = None,
    profiles: dict[str, Path] | None = None,
    prompt_files: dict[str, Path] | None = None,
    channel_launch: FakeChannelLaunch | None = None,
    control: ControlOperations | None = None,
    channel_trust: object | None = None,
) -> CmuxSurfaceReconciler:
    return CmuxSurfaceReconciler(
        bindings=bindings,
        port=port,
        project_paths={"demo": Path("/repos/demo")},
        prompt_files=prompt_files,
        profile_dirs=FakeProfileDirs(
            profiles if profiles is not None else {"max-a": Path("/profiles/max-a")}
        ),
        environ=environ or {},
        # Sol correction a06cbce0: restart recovery relaunches through
        # the channel launcher, so the tests compose one by default.
        channel_launch=(
            channel_launch
            if channel_launch is not None
            else FakeChannelLaunch(
                config=Path(f"/state/channels/{SESSION}.mcp.json")
            )
        ),
        control=control,
        channel_trust=channel_trust,
    )


def test_duplicate_orchestrator_activation_reuses_the_binding(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    first = bindings.bind_orchestrator(ORCH)
    second = bindings.bind_orchestrator(ORCH)

    assert second.binding_id == first.binding_id
    assert second.generation == 1
    assert event_types(database) == ["cmux_binding.bound"]


def test_orchestrator_ownership_never_transfers_silently(
    bindings: CmuxSurfaceBindings,
) -> None:
    bindings.bind_orchestrator(ORCH)

    with pytest.raises(CmuxBindingConflict):
        bindings.bind_orchestrator(FRESH)


def test_duplicate_lead_activation_reuses_the_binding(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    first = bind_demo_lead(bindings)
    second = bind_demo_lead(bindings)

    assert second.binding_id == first.binding_id
    assert event_types(database) == ["cmux_binding.bound"]
    with pytest.raises(CmuxBindingConflict):
        bind_demo_lead(bindings, FRESH)


def test_replace_increments_generation_and_journals_lifecycle(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    first = bind_demo_lead(bindings)

    successor = bindings.replace(first.binding_id, FRESH, reason="surface_missing")

    assert successor.generation == 2
    assert successor.cell_id == "cell-demo"
    assert successor.session_id == SESSION
    assert bindings.get(first.binding_id).state == "stale"
    assert bindings.active_lead("cell-demo").binding_id == (successor.binding_id)
    assert event_types(database) == [
        "cmux_binding.bound",
        "cmux_binding.replaced",
        "cmux_binding.bound",
    ]


def test_closure_records_an_explicit_terminal_lifecycle_event(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seat = bindings.bind_orchestrator(ORCH)

    closed = bindings.mark_closed(seat.binding_id, reason="operator_closed")

    assert closed.state == "closed"
    assert bindings.active_orchestrator() is None
    # Idempotent re-close changes nothing; reviving a terminal row is a
    # conflict, never a silent transfer.
    assert (
        bindings.mark_closed(seat.binding_id, reason="operator_closed").state
        == "closed"
    )
    with pytest.raises(CmuxBindingConflict):
        bindings.mark_lost(seat.binding_id, reason="late")
    payload_reason = database.scalar(
        "SELECT json_extract(payload_json, '$.reason') FROM events "
        "WHERE event_type = 'cmux_binding.closed'"
    )
    assert str(payload_reason) == "operator_closed"


@pytest.mark.asyncio
async def test_socket_denial_fails_closed_leaving_bindings_untouched(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    bind_demo_lead(bindings)
    port = FakePort(deny=True)

    report = await reconciler(bindings, port).reconcile()

    assert report.available is False
    assert bindings.active_lead("cell-demo").state == "active"
    assert port.created == []
    assert event_types(database) == ["cmux_binding.bound"]


@pytest.mark.asyncio
async def test_live_surfaces_verify_without_any_mutation(
    bindings: CmuxSurfaceBindings,
) -> None:
    binding = bind_demo_lead(bindings)
    port = FakePort(live={LEAD})

    report = await reconciler(bindings, port).reconcile()

    assert report.verified == (binding.binding_id,)
    assert report.replaced == () and report.lost == ()
    assert port.created == []


@pytest.mark.asyncio
async def test_stale_lead_surface_is_replaced_with_exact_identity(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    first = bind_demo_lead(bindings)
    port = FakePort(next_refs=[FRESH])
    launch = FakeChannelLaunch(config=Path(f"/state/channels/{SESSION}.mcp.json"))
    # Sol correction c5600e31: restart recovery now fails closed on
    # channel trust — a trigger that confirms is composed here so this
    # test still exercises the surface-replacement mechanics; the
    # trust-gating contract itself is covered by
    # ``TestReconcilerChannelTrust``.
    trust = FakeTrustTrigger(verdict=TrustVerdict(confirmed=True))
    prompt = Path("/runtime/prompts/claude-lead.md")

    report = await reconciler(
        bindings,
        port,
        prompt_files={"development": prompt},
        channel_launch=launch,
        channel_trust=trust,
    ).reconcile()

    successor = bindings.active_lead("cell-demo")
    assert report.replaced == (successor.binding_id,)
    assert successor.generation == first.generation + 1
    assert successor.ref == FRESH
    # One replacement journals the write-ahead row, then the old and new
    # generations atomically; generation increments exactly once.
    assert bindings.get(first.binding_id).state == "stale"
    assert event_types(database) == [
        "cmux_binding.bound",
        "cmux_binding.residual",
        "cmux_binding.replaced",
        "cmux_binding.bound",
    ]
    # Sol correction a06cbce0: the replacement workspace is created from
    # durable identity alone — recorded project cwd, the profile's exact
    # CLAUDE_CONFIG_DIR, and the exact channel-enabled classic launch
    # command normal seating composes for this Claude session; never a
    # blank terminal. The requested title carries the write-ahead
    # intent's unique marker; the visible title is restored once the
    # identities are durably bound.
    channel_command = classic_channel_command(
        SESSION,
        resume=True,
        channel_config=Path(f"/state/channels/{SESSION}.mcp.json"),
        prompt_file=prompt,
    )
    [created] = port.created
    assert created["cwd"] == Path("/repos/demo")
    assert created["command"] == channel_command
    assert created["env"] == {"CLAUDE_CONFIG_DIR": "/profiles/max-a"}
    assert str(created["title"]).startswith("demo lead")
    assert port.titles[FRESH.workspace_uuid] == "demo lead"
    assert port.resumes == [(FRESH, channel_command)]
    # The config is generation-stamped for the successor binding, so the
    # hub's registration check accepts exactly the reseated lead.
    [generated] = launch.generated
    assert generated["session_id"] == SESSION
    assert generated["generation"] == successor.generation
    # Classic-seat evidence is recorded exactly as normal seating
    # records it; the lead-intake transport and hub both require it.
    assert bindings.is_classic(successor.binding_id, SESSION) is True


@pytest.mark.asyncio
async def test_failed_channel_generation_fails_the_replacement_closed(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    """Sol correction a06cbce0: a replacement that cannot carry the
    channel-enabled classic launch command never binds an active seat
    over a blank terminal — the seat is recorded lost with one durable,
    actionable channel.blocked receipt."""

    first = bind_demo_lead(bindings)
    port = FakePort(next_refs=[FRESH])
    launch = FakeChannelLaunch(error=FileNotFoundError("no build"))
    control = ControlOperations(database, events=EventStore(database))

    report = await reconciler(
        bindings, port, channel_launch=launch, control=control
    ).reconcile()

    assert report.completed is True
    assert report.replaced == ()
    assert report.lost == (first.binding_id,)
    assert bindings.get(first.binding_id).state == "lost"
    assert bindings.active_lead("cell-demo") is None
    # No workspace is created and no resume is set: nothing visible
    # exists for the unlaunched seat.
    assert port.created == []
    assert port.resumes == []
    [receipt] = control.pending_for_session(SESSION)
    assert receipt.kind == "channel.blocked"
    assert receipt.result["launcher_error"] == "no build"
    assert "blank terminal" in str(receipt.reason)


@pytest.mark.asyncio
async def test_reconciler_without_a_launcher_never_seats_a_blank_terminal(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    first = bind_demo_lead(bindings)
    port = FakePort(next_refs=[FRESH])
    control = ControlOperations(database, events=EventStore(database))
    bare = CmuxSurfaceReconciler(
        bindings=bindings,
        port=port,
        project_paths={"demo": Path("/repos/demo")},
        profile_dirs=FakeProfileDirs({"max-a": Path("/profiles/max-a")}),
        environ={},
        control=control,
    )

    report = await bare.reconcile()

    assert report.replaced == ()
    assert report.lost == (first.binding_id,)
    assert bindings.get(first.binding_id).state == "lost"
    assert port.created == []
    [receipt] = control.pending_for_session(SESSION)
    assert receipt.kind == "channel.blocked"


@pytest.mark.asyncio
async def test_missing_profile_marks_the_seat_lost_instead_of_adopting(
    bindings: CmuxSurfaceBindings,
) -> None:
    binding = bind_demo_lead(bindings)
    port = FakePort(next_refs=[FRESH])

    report = await reconciler(
        bindings, port, profiles={"max-b": Path("/profiles/max-b")}
    ).reconcile()

    assert report.lost == (binding.binding_id,)
    assert bindings.get(binding.binding_id).state == "lost"
    assert port.created == []
    assert port.resumes == []


@pytest.mark.asyncio
async def test_orchestrator_reseats_only_into_its_own_cmux_seat(
    bindings: CmuxSurfaceBindings,
) -> None:
    binding = bindings.bind_orchestrator(ORCH)
    port = FakePort()
    seated = reconciler(
        bindings,
        port,
        environ={
            "CMUX_WORKSPACE_ID": FRESH.workspace_uuid,
            "CMUX_SURFACE_ID": FRESH.surface_uuid,
        },
    )

    report = await seated.reconcile()

    successor = bindings.active_orchestrator()
    assert report.replaced == (successor.binding_id,)
    assert successor.ref == FRESH
    assert successor.generation == binding.generation + 1
    assert port.created == []


@pytest.mark.asyncio
async def test_lost_orchestrator_seat_is_never_recreated_silently(
    bindings: CmuxSurfaceBindings,
) -> None:
    binding = bindings.bind_orchestrator(ORCH)
    port = FakePort()

    report = await reconciler(bindings, port).reconcile()

    assert report.lost == (binding.binding_id,)
    assert bindings.active_orchestrator() is None
    assert port.created == []


@pytest.mark.asyncio
async def test_seated_process_binds_the_missing_orchestrator_seat(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakePort()
    seated = reconciler(
        bindings,
        port,
        environ={
            "CMUX_WORKSPACE_ID": ORCH.workspace_uuid,
            "CMUX_SURFACE_ID": ORCH.surface_uuid,
        },
    )

    await seated.reconcile()

    assert bindings.active_orchestrator().ref == ORCH


class FakeSafety:
    def __init__(self, safe: set[tuple[str, str]]) -> None:
        self._safe = safe

    def current(self, cell_id: str, session_id: str) -> object | None:
        if (cell_id, session_id) in self._safe:
            return object()
        return None


def seed_cell(database: Database, state: str = "active") -> None:
    database.execute(
        "INSERT INTO project_cells("
        "cell_id, project_key, state, profile_alias, session_id, "
        "created_at, updated_at) VALUES "
        "('cell-demo', 'demo', ?, 'max-a', ?, ?, ?)",
        (state, SESSION, NOW.isoformat(), NOW.isoformat()),
    )


def seed_harness_cell(database: Database, state: str = "active") -> None:
    """A second cell of the SAME project -- the shape start-lane creates
    for the harness lane, which has no trust anchor of its own."""

    database.execute(
        "INSERT INTO project_cells("
        "cell_id, project_key, lane_role, state, profile_alias, "
        "session_id, created_at, updated_at) VALUES "
        "('cell-harness', 'demo', 'harness', ?, 'max-a', ?, ?, ?)",
        (state, HARNESS_SESSION, NOW.isoformat(), NOW.isoformat()),
    )


def seed_running_lease(database: Database) -> None:
    database.execute(
        "INSERT INTO process_leases("
        "lease_id, worker_id, project_key, kind, pid, pgid, executable, "
        "cwd, create_time, state, acquired_at, updated_at) VALUES "
        "('lease-1', ?, 'demo', 'claude_lead', 4242, 4242, 'claude', "
        "'/repos/demo', 1.0, 'active', ?, ?)",
        (SESSION, NOW.isoformat(), NOW.isoformat()),
    )


def gate(
    database: Database,
    bindings: CmuxSurfaceBindings,
    *,
    safe: bool,
) -> CmuxHibernationGate:
    evidence = {("cell-demo", SESSION)} if safe else set()
    return CmuxHibernationGate(
        database=database,
        bindings=bindings,
        safety=FakeSafety(evidence),
    )


def test_hibernation_clears_only_an_idle_safe_restorable_lead(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_cell(database)
    bind_demo_lead(bindings)

    assert gate(database, bindings, safe=True).decide() == (
        HibernationDecision(clear=True)
    )


def test_running_lead_blocks_hibernation(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_cell(database)
    seed_running_lease(database)
    bind_demo_lead(bindings)

    decision = gate(database, bindings, safe=True).decide()

    assert decision.clear is False
    assert decision.blockers == ("cell-demo:running",)


def test_uncheckpointed_lead_blocks_hibernation(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_cell(database)
    bind_demo_lead(bindings)

    decision = gate(database, bindings, safe=False).decide()

    assert decision.blockers == ("cell-demo:uncheckpointed",)


def test_unrestorable_cell_state_blocks_hibernation(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_cell(database, state="handoff_required")
    bind_demo_lead(bindings)

    decision = gate(database, bindings, safe=True).decide()

    assert decision.blockers == ("cell-demo:unrestorable",)


def seater(
    bindings: CmuxSurfaceBindings,
    port: FakePort,
    *,
    profiles: dict[str, Path] | None = None,
    prompt_files: dict[str, Path] | None = None,
    channel_launch: object | None = None,
    control: object | None = None,
    channel_trust: object | None = None,
) -> object:
    from hermes_orchestrator.cmux_surfaces import CmuxLeadSeater

    return CmuxLeadSeater(
        bindings=bindings,
        port=port,
        project_paths={"demo": Path("/repos/demo")},
        profile_dirs=FakeProfileDirs(
            profiles if profiles is not None else {"max-a": Path("/profiles/max-a")}
        ),
        prompt_files=prompt_files,
        channel_launch=channel_launch,
        control=control,
        channel_trust=channel_trust,
    )


@pytest.mark.asyncio
async def test_seater_creates_one_seat_and_reuses_it(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    port = FakePort(next_refs=[LEAD])
    ensure = seater(bindings, port)

    first = await ensure.ensure(
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        profile_alias="max-a",
        issue_id="ENG-9",
    )
    second = await ensure.ensure(
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        profile_alias="max-a",
        issue_id="ENG-10",
    )

    # Duplicate activation reuses the exact bound identity.
    assert second.binding_id == first.binding_id
    assert len(port.created) == 1
    assert port.created[0]["env"] == {"CLAUDE_CONFIG_DIR": "/profiles/max-a"}
    assert port.resumes == [
        (LEAD, f"claude --resume {SESSION} {SKIP_PERMISSIONS_FLAG}")
    ]
    # The displayed issue follows dispatch; the durable binding does not.
    assert port.statuses == [
        (LEAD.workspace_uuid, "issue", "ENG-9"),
        (LEAD.workspace_uuid, "issue", "ENG-10"),
    ]
    # Write-ahead activation journals ownership of the created workspace
    # before it is promoted to the active seat.
    assert event_types(database) == [
        "cmux_binding.residual",
        "cmux_binding.bound",
    ]


@pytest.mark.asyncio
async def test_seater_retires_the_old_seat_on_session_rotation(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    port = FakePort(next_refs=[LEAD, FRESH])
    ensure = seater(bindings, port)
    rotated = "88888888-8888-4888-8888-888888888888"

    first = await ensure.ensure(
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        profile_alias="max-a",
    )
    second = await ensure.ensure(
        project_key="demo",
        cell_id="cell-demo",
        session_id=rotated,
        profile_alias="max-a",
    )

    # The exact old workspace is closed before the seat leaves 'active';
    # only the rotated session's workspace remains live and bound.
    assert port.closed == [LEAD.workspace_uuid]
    assert port.live == {FRESH}
    assert bindings.get(first.binding_id).state == "closed"
    assert bindings.active_lead("cell-demo").binding_id == second.binding_id
    assert second.session_id == rotated
    assert second.generation == first.generation + 1
    assert port.resumes[-1] == (
        FRESH, f"claude --resume {rotated} {SKIP_PERMISSIONS_FLAG}"
    )
    assert event_types(database) == [
        "cmux_binding.residual",
        "cmux_binding.bound",
        "cmux_binding.closed",
        "cmux_binding.residual",
        "cmux_binding.bound",
    ]


@pytest.mark.asyncio
async def test_uncertain_rotation_close_holds_the_seat_residual(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    port = FakePort(next_refs=[LEAD, FRESH])
    ensure = seater(bindings, port)
    rotated = "88888888-8888-4888-8888-888888888888"

    first = await ensure.ensure(
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        profile_alias="max-a",
    )
    port.fail["close_workspace"] = CmuxUnavailable("cmux command timed out")
    with pytest.raises(CmuxUnavailable):
        await ensure.ensure(
            project_key="demo",
            cell_id="cell-demo",
            session_id=rotated,
            profile_alias="max-a",
        )

    # cmux never confirmed the close, so ownership evidence survives as a
    # residual binding and no replacement seat is created meanwhile.
    held = bindings.get(first.binding_id)
    assert held.state == "residual"
    assert held.workspace_uuid == LEAD.workspace_uuid
    assert bindings.active_lead("cell-demo") is None
    assert len(port.created) == 1
    assert event_types(database) == [
        "cmux_binding.residual",
        "cmux_binding.bound",
        "cmux_binding.residual",
    ]
    decision = gate(database, bindings, safe=True).decide()
    assert decision.clear is False
    assert "cell-demo:residual" in decision.blockers


@pytest.mark.asyncio
async def test_failed_resume_setup_closes_the_created_workspace(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    port = FakePort(
        next_refs=[LEAD],
        fail={"set_surface_resume": CmuxUnavailable("cmux command timed out")},
    )

    with pytest.raises(CmuxUnavailable):
        await seater(bindings, port).ensure(
            project_key="demo",
            cell_id="cell-demo",
            session_id=SESSION,
            profile_alias="max-a",
        )

    # The half-activated workspace is compensated by exact identity; its
    # write-ahead row is closed and no live seat remains.
    assert port.closed == [LEAD.workspace_uuid]
    assert port.live == set()
    assert bindings.active_lead("cell-demo") is None
    assert bindings.residual() == ()
    assert event_types(database) == [
        "cmux_binding.residual",
        "cmux_binding.closed",
    ]


@pytest.mark.asyncio
async def test_failed_resume_and_uncertain_close_record_a_residual(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    port = FakePort(
        next_refs=[LEAD],
        fail={
            "set_surface_resume": CmuxUnavailable("cmux command timed out"),
            "close_workspace": CmuxUnavailable("cmux command timed out"),
        },
    )

    with pytest.raises(CmuxUnavailable):
        await seater(bindings, port).ensure(
            project_key="demo",
            cell_id="cell-demo",
            session_id=SESSION,
            profile_alias="max-a",
        )

    # Compensation itself failed: the created workspace stays durably
    # owned as a residual binding for startup reconciliation.
    residuals = bindings.residual()
    assert [held.workspace_uuid for held in residuals] == [LEAD.workspace_uuid]
    assert residuals[0].session_id == SESSION
    assert residuals[0].cell_id == "cell-demo"
    assert bindings.active_lead("cell-demo") is None
    assert event_types(database) == ["cmux_binding.residual"]
    decision = gate(database, bindings, safe=True).decide()
    assert decision.clear is False
    assert "cell-demo:residual" in decision.blockers


@pytest.mark.asyncio
async def test_failed_durable_bind_closes_the_created_workspace(
    database: Database,
    bindings: CmuxSurfaceBindings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    port = FakePort(next_refs=[LEAD])

    def explode(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("durable bind failed")

    monkeypatch.setattr(bindings, "activate_residual", explode)
    with pytest.raises(RuntimeError, match="durable bind failed"):
        await seater(bindings, port).ensure(
            project_key="demo",
            cell_id="cell-demo",
            session_id=SESSION,
            profile_alias="max-a",
        )

    # No untracked workspace remains after the durable bind failure: the
    # write-ahead row kept ownership until the compensating close.
    assert port.closed == [LEAD.workspace_uuid]
    assert port.live == set()
    assert bindings.active_lead("cell-demo") is None
    assert bindings.residual() == ()
    assert event_types(database) == [
        "cmux_binding.residual",
        "cmux_binding.closed",
    ]


@pytest.mark.asyncio
async def test_seater_creates_nothing_for_an_unknown_profile(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakePort(next_refs=[LEAD])
    ensure = seater(bindings, port, profiles={})

    seat = await ensure.ensure(
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        profile_alias="max-a",
    )

    assert seat is None
    assert port.created == []
    assert bindings.active_lead("cell-demo") is None


def record_demo_residual(
    bindings: CmuxSurfaceBindings, ref: CmuxSurfaceRef = LEAD
) -> object:
    return bindings.record_residual(
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        profile_alias="max-a",
        ref=ref,
        reason="activation_close_uncertain",
    )


@pytest.mark.asyncio
async def test_startup_reclaims_live_residual_workspaces(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    residual = record_demo_residual(bindings)
    port = FakePort(live={LEAD})

    report = await reconciler(bindings, port).reconcile()

    # The exact residual workspace is closed and its lifecycle journaled.
    assert report.reclaimed == (residual.binding_id,)
    assert port.closed == [LEAD.workspace_uuid]
    assert bindings.get(residual.binding_id).state == "closed"
    assert bindings.residual() == ()
    assert event_types(database) == [
        "cmux_binding.residual",
        "cmux_binding.closed",
    ]


@pytest.mark.asyncio
async def test_vanished_residual_workspace_is_recorded_lost(
    bindings: CmuxSurfaceBindings,
) -> None:
    residual = record_demo_residual(bindings)
    port = FakePort()

    report = await reconciler(bindings, port).reconcile()

    assert report.lost == (residual.binding_id,)
    assert bindings.get(residual.binding_id).state == "lost"
    assert port.closed == []


@pytest.mark.asyncio
async def test_uncertain_residual_close_stays_residual(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    residual = record_demo_residual(bindings)
    port = FakePort(
        live={LEAD},
        fail={"close_workspace": CmuxUnavailable("cmux command timed out")},
    )

    report = await reconciler(bindings, port).reconcile()

    # The close is still unconfirmed: ownership evidence stays residual,
    # hibernation stays blocked, and the next startup retries.
    assert report.available is True
    assert report.completed is False
    assert bindings.get(residual.binding_id).state == "residual"
    decision = gate(database, bindings, safe=True).decide()
    assert decision.clear is False


@pytest.mark.asyncio
async def test_failed_replacement_resume_setup_compensates_the_workspace(
    bindings: CmuxSurfaceBindings,
) -> None:
    first = bind_demo_lead(bindings)
    port = FakePort(
        next_refs=[FRESH],
        fail={"set_surface_resume": CmuxUnavailable("cmux command timed out")},
    )

    report = await reconciler(bindings, port).reconcile()

    # The replacement workspace is compensated and the original binding is
    # left active and recoverable; daemon startup is never aborted.
    assert report.completed is False
    assert port.closed == [FRESH.workspace_uuid]
    assert bindings.active_lead("cell-demo").binding_id == first.binding_id
    assert bindings.residual() == ()


@pytest.mark.asyncio
async def test_post_ping_surface_check_failure_leaves_binding_untouched(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    binding = bind_demo_lead(bindings)
    port = FakePort(fail={"surface_alive": CmuxUnavailable("cmux command timed out")})

    report = await reconciler(bindings, port).reconcile()

    assert report.available is True
    assert report.completed is False
    assert bindings.get(binding.binding_id).state == "active"
    assert port.created == []
    assert event_types(database) == ["cmux_binding.bound"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        CmuxAccessDenied("cmux socket denied this process"),
        CmuxUnavailable("cmux command timed out"),
        CmuxProtocolError("cmux did not return a workspace identity"),
    ],
)
async def test_post_ping_replacement_failures_never_escape(
    bindings: CmuxSurfaceBindings, error: CmuxError
) -> None:
    binding = bind_demo_lead(bindings)
    port = FakePort(fail={"create_workspace": error})

    report = await reconciler(bindings, port).reconcile()

    # Denial, timeout, and protocol failure after a successful ping all
    # stay inside the optional cmux boundary; the binding stays active
    # and recoverable for the next reconciliation.
    assert report.available is True
    assert report.completed is False
    assert bindings.get(binding.binding_id).state == "active"


def test_residual_seat_blocks_hibernation(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_cell(database)
    bind_demo_lead(bindings)
    record_demo_residual(bindings, FRESH)

    decision = gate(database, bindings, safe=True).decide()

    assert decision.clear is False
    assert decision.blockers == ("cell-demo:residual",)


@pytest.mark.asyncio
async def test_ensure_refuses_replacement_while_residual_unresolved(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    port = FakePort(next_refs=[LEAD, FRESH])
    ensure = seater(bindings, port)
    rotated = "88888888-8888-4888-8888-888888888888"

    await ensure.ensure(
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        profile_alias="max-a",
    )
    port.fail["close_workspace"] = CmuxUnavailable("cmux command timed out")
    with pytest.raises(CmuxUnavailable):
        await ensure.ensure(
            project_key="demo",
            cell_id="cell-demo",
            session_id=rotated,
            profile_alias="max-a",
        )

    # The residual close is still unconfirmed, so a re-dispatch before
    # reconciliation must not seat a replacement workspace.
    with pytest.raises(CmuxUnavailable):
        await ensure.ensure(
            project_key="demo",
            cell_id="cell-demo",
            session_id=rotated,
            profile_alias="max-a",
        )

    assert len(port.created) == 1
    assert len(bindings.residual()) == 1
    assert bindings.active_lead("cell-demo") is None
    decision = gate(database, bindings, safe=True).decide()
    assert decision.clear is False
    assert "cell-demo:residual" in decision.blockers


@pytest.mark.asyncio
async def test_resolved_residual_permits_exactly_one_replacement(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakePort(next_refs=[LEAD, FRESH])
    ensure = seater(bindings, port)
    rotated = "88888888-8888-4888-8888-888888888888"

    await ensure.ensure(
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        profile_alias="max-a",
    )
    port.fail["close_workspace"] = CmuxUnavailable("cmux command timed out")
    with pytest.raises(CmuxUnavailable):
        await ensure.ensure(
            project_key="demo",
            cell_id="cell-demo",
            session_id=rotated,
            profile_alias="max-a",
        )
    del port.fail["close_workspace"]

    report = await reconciler(bindings, port).reconcile()
    seat = await ensure.ensure(
        project_key="demo",
        cell_id="cell-demo",
        session_id=rotated,
        profile_alias="max-a",
    )

    # Reconciliation closed the exact residual workspace; the following
    # dispatch seats exactly one replacement.
    assert len(report.reclaimed) == 1
    assert port.closed == [LEAD.workspace_uuid]
    assert len(port.created) == 2
    assert seat is not None and seat.ref == FRESH
    assert bindings.residual() == ()
    assert port.live == {FRESH}


@pytest.mark.asyncio
async def test_repeated_dispatches_never_accumulate_workspaces(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakePort(next_refs=[LEAD, FRESH])
    ensure = seater(bindings, port)
    rotated = "88888888-8888-4888-8888-888888888888"

    await ensure.ensure(
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        profile_alias="max-a",
    )
    port.fail["close_workspace"] = CmuxUnavailable("cmux command timed out")
    for _ in range(3):
        with pytest.raises(CmuxUnavailable):
            await ensure.ensure(
                project_key="demo",
                cell_id="cell-demo",
                session_id=rotated,
                profile_alias="max-a",
            )

    # However often dispatch retries, the cell owns at most the one
    # unresolved workspace: no active-plus-residual accumulation.
    assert len(port.created) == 1
    assert port.live == {LEAD}
    assert len(bindings.residual()) == 1
    assert bindings.active_lead("cell-demo") is None


def test_replace_is_atomic_when_successor_insert_fails(
    database: Database,
) -> None:
    import sqlite3

    bindings = CmuxSurfaceBindings(
        database=database,
        events=EventStore(database),
        now=lambda: NOW,
        ids=lambda: "binding-1",
    )
    first = bind_demo_lead(bindings)

    with pytest.raises(sqlite3.IntegrityError):
        bindings.replace(first.binding_id, FRESH, reason="surface_missing")

    # The duplicate successor id failed the insert; the old generation's
    # stale transition rolled back with it, so ownership is never lost.
    assert bindings.get(first.binding_id).state == "active"
    assert bindings.active_lead("cell-demo").binding_id == first.binding_id
    assert event_types(database) == ["cmux_binding.bound"]


@pytest.mark.asyncio
async def test_interrupted_replacement_recovers_one_seat(
    bindings: CmuxSurfaceBindings,
) -> None:
    # Durable state as a crash would leave it: the old seat is still
    # active (its workspace already gone), and the created replacement is
    # live but only write-ahead owned — never promoted.
    old = bind_demo_lead(bindings)
    bindings.record_residual(
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        profile_alias="max-a",
        ref=FRESH,
        reason="activation_pending",
    )
    port = FakePort(live={FRESH}, next_refs=[THIRD])
    # Sol correction c5600e31: restart recovery fails closed on channel
    # trust; a confirming trigger keeps this test focused on the
    # write-ahead reclaim mechanics it exercises.
    trust = FakeTrustTrigger(verdict=TrustVerdict(confirmed=True))

    report = await reconciler(bindings, port, channel_trust=trust).reconcile()

    # Startup reconciliation closes the write-ahead workspace by exact
    # identity and reseats the still-active old generation once: one
    # owned seat, no orphaned workspace.
    assert len(report.reclaimed) == 1
    assert port.closed == [FRESH.workspace_uuid]
    assert bindings.get(old.binding_id).state == "stale"
    successor = bindings.active_lead("cell-demo")
    assert successor.ref == THIRD
    assert report.replaced == (successor.binding_id,)
    assert bindings.residual() == ()
    assert port.live == {THIRD}


def demo_seat(cell_id: str = "cell-demo", session_id: str = SESSION) -> dict:
    return {
        "project_key": "demo",
        "cell_id": cell_id,
        "session_id": session_id,
        "profile_alias": "max-a",
    }


def seed_unrelated_workspace(port: FakePort) -> None:
    """A live workspace Hermes never created, with a look-alike title."""

    port.live.add(THIRD)
    port.titles[THIRD.workspace_uuid] = "demo lead"


@pytest.mark.asyncio
async def test_activation_intent_is_durable_before_the_external_create(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakePort(next_refs=[LEAD])
    observed: list[tuple[tuple, str]] = []
    port.on_create = lambda title: observed.append((bindings.pending_intents(), title))

    seat = await seater(bindings, port).ensure(**demo_seat())

    # At the instant the external create ran, the intent was already
    # committed and its unique identity travelled inside the requested
    # title, so no crash after this point can orphan the workspace.
    [(pending, title)] = observed
    assert len(pending) == 1
    assert pending[0].state == "pending"
    assert pending[0].title_marker in title
    assert title.startswith("demo lead")
    # The same durable marker resolves a cmux short mutation
    # acknowledgement to exactly one workspace inside the adapter.
    assert port.created[0]["resolve_marker"] == pending[0].title_marker
    # Successful activation binds the returned identities to that exact
    # intent and drops the cosmetic marker from the visible title.
    bound = bindings.get_intent(pending[0].intent_id)
    assert bound.state == "bound"
    assert bound.binding_id == seat.binding_id
    assert bindings.pending_intents() == ()
    assert port.titles[LEAD.workspace_uuid] == "demo lead"


@pytest.mark.asyncio
async def test_interruption_after_create_returns_recovers_exact_workspace(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    port = FakePort(next_refs=[LEAD], crash="after_create")
    seed_unrelated_workspace(port)

    with pytest.raises(SimulatedCrash):
        await seater(bindings, port).ensure(**demo_seat())

    # The crash window's durable truth: a live workspace whose returned
    # identities were never recorded, no binding row at all, and the
    # write-ahead intent as the only evidence. Hibernation must wait.
    assert bindings.active_lead("cell-demo") is None
    assert bindings.residual() == ()
    [intent] = bindings.pending_intents()
    decision = gate(database, bindings, safe=True).decide()
    assert decision.clear is False
    assert "cell-demo:activation_intent" in decision.blockers

    port.crash = None
    report = await reconciler(bindings, port).reconcile()

    # Restart reconciliation correlates through the intent's unique title
    # marker: the exact created workspace is reclaimed, the unrelated
    # look-alike stays untouched, and nothing remains pending.
    assert report.intents_reclaimed == (intent.intent_id,)
    assert port.closed == [LEAD.workspace_uuid]
    assert port.live == {THIRD}
    assert bindings.pending_intents() == ()
    assert intent_event_types(database) == [
        "cmux_intent.recorded",
        "cmux_intent.reclaimed",
    ]
    assert gate(database, bindings, safe=True).decide().clear is True


@pytest.mark.asyncio
async def test_interruption_before_create_adopts_no_unrelated_workspace(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    port = FakePort(next_refs=[LEAD], crash="before_create")
    seed_unrelated_workspace(port)

    with pytest.raises(SimulatedCrash):
        await seater(bindings, port).ensure(**demo_seat())

    assert port.created == []
    [intent] = bindings.pending_intents()

    port.crash = None
    report = await reconciler(bindings, port).reconcile()

    # No live workspace carries this intent's identity, so the intent is
    # aborted without closing or adopting anything.
    assert report.intents_aborted == (intent.intent_id,)
    assert report.intents_reclaimed == ()
    assert port.closed == []
    assert port.live == {THIRD}
    assert bindings.pending_intents() == ()
    assert intent_event_types(database) == [
        "cmux_intent.recorded",
        "cmux_intent.aborted",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("crash_point", ["before_create", "after_create"])
async def test_repeated_reconciliation_after_interruption_is_idempotent(
    bindings: CmuxSurfaceBindings, crash_point: str
) -> None:
    port = FakePort(next_refs=[LEAD, FRESH], crash=crash_point)
    ensure = seater(bindings, port)

    with pytest.raises(SimulatedCrash):
        await ensure.ensure(**demo_seat())
    port.crash = None

    first = await reconciler(bindings, port).reconcile()
    closed_after_first = list(port.closed)
    second = await reconciler(bindings, port).reconcile()

    # The second pass finds nothing pending and mutates nothing.
    assert len(first.intents_reclaimed) + len(first.intents_aborted) == 1
    assert second.intents_reclaimed == () and second.intents_aborted == ()
    assert port.closed == closed_after_first

    seat = await ensure.ensure(**demo_seat())

    # After either interruption point, at most one active seat results.
    assert seat is not None and seat.state == "active"
    assert bindings.active_lead("cell-demo").binding_id == seat.binding_id
    assert port.live == {seat.ref}
    assert bindings.pending_intents() == ()
    assert bindings.residual() == ()


@pytest.mark.asyncio
async def test_dispatch_retry_reclaims_the_unbound_workspace_first(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakePort(next_refs=[LEAD, FRESH], crash="after_create")
    ensure = seater(bindings, port)

    with pytest.raises(SimulatedCrash):
        await ensure.ensure(**demo_seat())
    port.crash = None

    seat = await ensure.ensure(**demo_seat())

    # A same-process retry resolves the pending intent before creating a
    # replacement: the orphaned workspace is closed by exact identity and
    # the cell never operates two live seats.
    assert port.closed == [LEAD.workspace_uuid]
    assert seat is not None and seat.ref == FRESH
    assert port.live == {FRESH}
    assert bindings.pending_intents() == ()


@pytest.mark.asyncio
async def test_uncertain_intent_reclaim_stays_pending(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    port = FakePort(next_refs=[LEAD], crash="after_create")
    with pytest.raises(SimulatedCrash):
        await seater(bindings, port).ensure(**demo_seat())
    port.crash = None
    port.fail["close_workspace"] = CmuxUnavailable("cmux command timed out")

    report = await reconciler(bindings, port).reconcile()

    # cmux never confirmed the close: the intent stays pending — still
    # owned, still blocking hibernation — and the next startup retries.
    assert report.available is True
    assert report.completed is False
    assert report.intents_reclaimed == ()
    assert len(bindings.pending_intents()) == 1
    decision = gate(database, bindings, safe=True).decide()
    assert decision.clear is False
    assert "cell-demo:activation_intent" in decision.blockers


def test_pending_intent_blocks_hibernation(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_cell(database)
    bind_demo_lead(bindings)
    bindings.record_intent(**demo_seat())

    decision = gate(database, bindings, safe=True).decide()

    assert decision.clear is False
    assert "cell-demo:activation_intent" in decision.blockers


@pytest.mark.asyncio
async def test_ambiguous_marker_matches_close_nothing_and_stay_pending(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    port = FakePort(next_refs=[LEAD], crash="after_create")
    with pytest.raises(SimulatedCrash):
        await seater(bindings, port).ensure(**demo_seat())
    port.crash = None
    [intent] = bindings.pending_intents()
    # A second live workspace carries the same exact marker (operator
    # duplication or copied metadata): the intent's single create can no
    # longer prove which workspace it owns.
    port.live.add(FRESH)
    port.titles[FRESH.workspace_uuid] = port.titles[LEAD.workspace_uuid]

    first = await reconciler(bindings, port).reconcile()
    second = await reconciler(bindings, port).reconcile()

    # Ambiguous ownership fails closed: neither workspace is closed, the
    # intent stays pending for operator resolution, hibernation stays
    # blocked, and both passes surface the ambiguity as incomplete.
    for report in (first, second):
        assert report.available is True
        assert report.completed is False
        assert report.intents_ambiguous == (intent.intent_id,)
        assert report.intents_reclaimed == ()
        assert report.intents_aborted == ()
    assert port.closed == []
    assert port.live == {LEAD, FRESH}
    assert bindings.get_intent(intent.intent_id).state == "pending"
    decision = gate(database, bindings, safe=True).decide()
    assert decision.clear is False
    assert "cell-demo:activation_intent" in decision.blockers


@pytest.mark.asyncio
async def test_operator_resolved_ambiguity_reclaims_the_single_match(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakePort(next_refs=[LEAD], crash="after_create")
    with pytest.raises(SimulatedCrash):
        await seater(bindings, port).ensure(**demo_seat())
    port.crash = None
    [intent] = bindings.pending_intents()
    port.live.add(FRESH)
    port.titles[FRESH.workspace_uuid] = port.titles[LEAD.workspace_uuid]

    ambiguous = await reconciler(bindings, port).reconcile()
    # The operator resolves the ambiguity by closing the duplicate; the
    # remaining single exact match is again provably this intent's.
    port.live.discard(FRESH)
    resolved = await reconciler(bindings, port).reconcile()

    assert ambiguous.intents_ambiguous == (intent.intent_id,)
    assert resolved.intents_ambiguous == ()
    assert resolved.intents_reclaimed == (intent.intent_id,)
    assert port.closed == [LEAD.workspace_uuid]
    assert bindings.pending_intents() == ()


@pytest.mark.asyncio
async def test_ambiguous_intent_refuses_a_replacement_seat(
    bindings: CmuxSurfaceBindings,
) -> None:
    from hermes_orchestrator.cmux_surfaces import CmuxBindingConflict

    port = FakePort(next_refs=[LEAD, FRESH], crash="after_create")
    ensure = seater(bindings, port)
    with pytest.raises(SimulatedCrash):
        await ensure.ensure(**demo_seat())
    port.crash = None
    port.live.add(THIRD)
    port.titles[THIRD.workspace_uuid] = port.titles[LEAD.workspace_uuid]

    # A dispatch retry must not close either candidate nor seat a
    # replacement while ownership is ambiguous.
    with pytest.raises(CmuxBindingConflict):
        await ensure.ensure(**demo_seat())

    assert port.closed == []
    assert len(port.created) == 1
    assert port.live == {LEAD, THIRD}
    assert len(bindings.pending_intents()) == 1
    assert bindings.active_lead("cell-demo") is None


@pytest.mark.asyncio
async def test_marker_superstrings_and_substrings_never_match(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    port = FakePort(next_refs=[LEAD], crash="before_create")
    with pytest.raises(SimulatedCrash):
        await seater(bindings, port).ensure(**demo_seat())
    port.crash = None
    [intent] = bindings.pending_intents()
    marker = intent.title_marker
    # Titles carrying the marker only embedded in a longer token, or a
    # marker whose identity merely starts with this intent's id, are not
    # this intent's workspace.
    port.live.update({FRESH, THIRD})
    port.titles[FRESH.workspace_uuid] = f"demo lead x{marker}y"
    port.titles[THIRD.workspace_uuid] = f"demo lead {marker[:-1]}-suffix]"

    report = await reconciler(bindings, port).reconcile()

    # No exact match exists, so the intent aborts and neither look-alike
    # workspace is touched.
    assert report.intents_aborted == (intent.intent_id,)
    assert report.intents_ambiguous == ()
    assert port.closed == []
    assert port.live == {FRESH, THIRD}
    assert bindings.pending_intents() == ()


def test_intent_binds_only_once_and_never_revives(
    bindings: CmuxSurfaceBindings,
) -> None:
    from hermes_orchestrator.cmux_surfaces import CmuxBindingConflict

    intent = bindings.record_intent(**demo_seat())
    bound = bindings.bind_intent(intent.intent_id, ref=LEAD)

    assert bound.state == "residual"
    assert bound.ref == LEAD
    assert bindings.get_intent(intent.intent_id).binding_id == (bound.binding_id)
    # A resolved intent can never bind again, be aborted, or be
    # reclaimed: the durable operation identity is single-use.
    with pytest.raises(CmuxBindingConflict):
        bindings.bind_intent(intent.intent_id, ref=FRESH)
    with pytest.raises(CmuxBindingConflict):
        bindings.abort_intent(intent.intent_id, reason="late")
    with pytest.raises(CmuxBindingConflict):
        bindings.reclaim_intent(
            intent.intent_id,
            workspace_uuids=(LEAD.workspace_uuid,),
            reason="late",
        )


class TestClassicSeats:
    def test_classic_resume_command_adds_the_project_lead_contract(self) -> None:
        from hermes_orchestrator.cmux_surfaces import classic_resume_command

        prompt = Path("/runtime/prompts/claude-lead.md")

        assert classic_resume_command(
            SESSION, resume=False, prompt_file=prompt
        ) == (
            f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG} "
            f"--append-system-prompt-file {prompt}"
        )
        with pytest.raises(CmuxBindingConflict):
            classic_resume_command(
                SESSION,
                resume=False,
                prompt_file=Path("/runtime/operator-supplied.md"),
            )

    def test_classic_resume_command_is_sanitized(self) -> None:
        from hermes_orchestrator.cmux_surfaces import classic_resume_command

        assert classic_resume_command(SESSION, resume=True) == (
            f"claude --resume {SESSION} {SKIP_PERMISSIONS_FLAG}"
        )
        assert classic_resume_command(SESSION, resume=False) == (
            f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
        )
        with pytest.raises(ValueError):
            classic_resume_command("nonsense; rm -rf /", resume=True)

    @pytest.mark.asyncio
    async def test_classic_seat_runs_the_tui_and_records_evidence(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        port = FakePort(next_refs=[LEAD])

        seat = await seater(bindings, port).ensure(
            **demo_seat(),
            classic_command=f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}",
        )

        assert seat is not None
        [created] = port.created
        # The pane runs exactly the sanitized native TUI command, and it
        # carries the fixed INFRA-197 flag immediately after the UUID.
        assert created["command"] == (
            f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
        )
        assert bindings.is_classic(seat.binding_id, SESSION) is True
        # The restore path still carries the sanitized resume command,
        # itself carrying the fixed flag.
        assert port.resumes == [
            (LEAD, f"claude --resume {SESSION} {SKIP_PERMISSIONS_FLAG}")
        ]

    @pytest.mark.asyncio
    async def test_arbitrary_classic_commands_are_refused_before_create(
        self, bindings: CmuxSurfaceBindings
    ) -> None:
        port = FakePort(next_refs=[LEAD])
        rogue = "88888888-8888-4888-8888-888888888888"

        for command in (
            "claude --resume abc; rm -rf /",
            "claude -p --output-format=stream-json",
            f"claude --resume {rogue} {SKIP_PERMISSIONS_FLAG}",
            f"bash -c 'claude --resume {SESSION} {SKIP_PERMISSIONS_FLAG}'",
            # The pre-INFRA-197 shape for the exact right session is no
            # longer sufficient: the fixed flag is now mandatory, not
            # optional, so a command missing it must still refuse.
            f"claude --session-id {SESSION}",
            # Nor can a caller supply anything in place of the fixed
            # literal, or repeat/relocate it.
            f"claude --session-id {SESSION} --dangerously-skip-something-else",
            (
                f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG} "
                f"{SKIP_PERMISSIONS_FLAG}"
            ),
        ):
            with pytest.raises(CmuxBindingConflict):
                await seater(bindings, port).ensure(
                    **demo_seat(), classic_command=command
                )
        assert port.created == []

    @pytest.mark.asyncio
    async def test_failed_auth_probe_refuses_the_seat_before_create(
        self, bindings: CmuxSurfaceBindings
    ) -> None:
        from hermes_orchestrator.cmux_surfaces import (
            CmuxLeadSeater,
            SeatAuthRefused,
        )

        port = FakePort(next_refs=[LEAD])
        probed: list[str] = []

        def probe(alias: str) -> bool:
            probed.append(alias)
            return False

        ensure = CmuxLeadSeater(
            bindings=bindings,
            port=port,
            project_paths={"demo": Path("/repos/demo")},
            profile_dirs=FakeProfileDirs({"max-a": Path("/profiles/max-a")}),
            auth_probe=probe,
        )

        with pytest.raises(SeatAuthRefused):
            await ensure.ensure(
                **demo_seat(),
                classic_command=(
                    f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
                ),
            )

        # The read-only probe ran under the leased profile and nothing
        # was created or launched for the unproven account.
        assert probed == ["max-a"]
        assert port.created == []
        assert bindings.active_lead("cell-demo") is None


class TestChannelLaunch:
    @pytest.mark.asyncio
    async def test_a_visible_lead_loads_its_role_contract(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        port = FakePort(next_refs=[LEAD])
        prompt = Path("/runtime/prompts/claude-lead.md")
        launch = FakeChannelLaunch(config=Path(f"/state/channels/{SESSION}.mcp.json"))

        await seater(
            bindings,
            port,
            prompt_files={"development": prompt},
            channel_launch=launch,
        ).ensure(
            **demo_seat(),
            classic_command=f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}",
        )

        [created] = port.created
        assert created["command"] == (
            f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG} "
            f"--append-system-prompt-file {prompt} "
            f"--mcp-config /state/channels/{SESSION}.mcp.json "
            "--dangerously-load-development-channels server:hermes-control"
        )

    def test_the_extended_command_is_exact_and_grammar_bound(self) -> None:
        from hermes_orchestrator.cmux_surfaces import (
            classic_channel_command,
        )

        config = Path(f"/state/channels/{SESSION}.mcp.json")

        command = classic_channel_command(SESSION, resume=False, channel_config=config)

        assert command == (
            f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG} "
            f"--mcp-config {config} "
            "--dangerously-load-development-channels server:hermes-control"
        )

    def test_foreign_config_paths_are_refused(self) -> None:
        from hermes_orchestrator.cmux_surfaces import (
            classic_channel_command,
        )

        rogue = "88888888-8888-4888-8888-888888888888"
        for config in (
            Path(f"/state/evil dir/{SESSION}.mcp.json"),
            Path(f"/state/channels/{rogue}.mcp.json"),
            Path(f"state/channels/{SESSION}.mcp.json"),
            Path(f"/state/channels/{SESSION}.mcp.json;rm"),
            Path("/state/channels/settings.json"),
        ):
            with pytest.raises(CmuxBindingConflict):
                classic_channel_command(SESSION, resume=False, channel_config=config)

    @pytest.mark.asyncio
    async def test_a_new_seat_launches_with_its_generated_channel(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        port = FakePort(next_refs=[LEAD])
        launch = FakeChannelLaunch(config=Path(f"/state/channels/{SESSION}.mcp.json"))

        seat = await seater(bindings, port, channel_launch=launch).ensure(
            **demo_seat(),
            classic_command=f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}",
        )

        assert seat is not None
        [created] = port.created
        assert created["command"] == (
            f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG} "
            f"--mcp-config /state/channels/{SESSION}.mcp.json "
            "--dangerously-load-development-channels server:hermes-control"
        )
        [generated] = launch.generated
        assert generated["session_id"] == SESSION
        assert generated["generation"] == 1
        assert bindings.is_classic(seat.binding_id, SESSION) is True
        # INFRA-207: the surface's native resume keeps the launched
        # channel extension (as ``--resume``), so a cmux-restored pane
        # does not silently drop the hermes-control channel.
        assert port.resumes == [
            (
                LEAD,
                f"claude --resume {SESSION} {SKIP_PERMISSIONS_FLAG} "
                f"--mcp-config /state/channels/{SESSION}.mcp.json "
                "--dangerously-load-development-channels server:hermes-control",
            )
        ]

    @pytest.mark.asyncio
    async def test_the_channel_seat_carries_no_fakechat_material(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        """Sol correction b4b545f3 (v5): the production classic-seat
        path adds no fakechat channel command and no fakechat port
        environment — hermes-control is the only channel extension."""

        port = FakePort(next_refs=[LEAD])
        launch = FakeChannelLaunch(config=Path(f"/state/channels/{SESSION}.mcp.json"))

        seat = await seater(bindings, port, channel_launch=launch).ensure(
            **demo_seat(),
            classic_command=f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}",
        )

        assert seat is not None
        [created] = port.created
        assert "fakechat" not in str(created["command"])
        assert created["env"] == {"CLAUDE_CONFIG_DIR": "/profiles/max-a"}

    @pytest.mark.asyncio
    async def test_a_launcher_failure_falls_back_to_the_bare_command(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        port = FakePort(next_refs=[LEAD])
        launch = FakeChannelLaunch(error=FileNotFoundError("no build"))

        seat = await seater(bindings, port, channel_launch=launch).ensure(
            **demo_seat(),
            classic_command=f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}",
        )

        assert seat is not None
        [created] = port.created
        assert created["command"] == (
            f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
        )

    @pytest.mark.asyncio
    async def test_a_launcher_failure_records_a_blocked_receipt(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        """INFRA-195: a channel-less seat is never a silent fallback —
        one durable, actionable channel.blocked receipt says why."""

        from hermes_orchestrator.control_operations import (
            ControlOperations,
        )
        from hermes_orchestrator.events import EventStore

        port = FakePort(next_refs=[LEAD])
        launch = FakeChannelLaunch(error=FileNotFoundError("no build"))
        control = ControlOperations(database, events=EventStore(database))

        seat = await seater(
            bindings, port, channel_launch=launch, control=control
        ).ensure(
            **demo_seat(),
            classic_command=f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}",
        )

        assert seat is not None
        [receipt] = control.pending_for_session(SESSION)
        assert receipt.kind == "channel.blocked"
        assert receipt.result["launcher_error"] == "no build"
        assert "Stop-hook" in str(receipt.reason)

    @pytest.mark.asyncio
    async def test_rotation_retires_the_old_sessions_channel_material(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        replacement = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        port = FakePort(next_refs=[LEAD, FRESH])
        launch = FakeChannelLaunch(config=Path(f"/state/channels/{SESSION}.mcp.json"))
        ensure = seater(bindings, port, channel_launch=launch)
        await ensure.ensure(
            **demo_seat(),
            classic_command=f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}",
        )
        launch.config = Path(f"/state/channels/{replacement}.mcp.json")

        await ensure.ensure(
            **demo_seat(session_id=replacement),
            classic_command=(
                f"claude --session-id {replacement} {SKIP_PERMISSIONS_FLAG}"
            ),
        )

        # The rotated-away session's config and capability were
        # removed only after its workspace was confirmed closed.
        assert launch.cleaned == [SESSION]


# --------------------------------------------------------------------
# Channel-trust confirmation rides the managed-seat lifecycle
# (Sol correction f0a5a403, packet 4)
# --------------------------------------------------------------------

DIALOG_TEXT = (
    "Loading development channels\n"
    "  - server:hermes-control\n"
    "  [x] I am using this for local development\n"
    "  Press Enter to confirm, Esc to cancel"
)

CHANNEL_ARGV = [
    "claude",
    "--session-id",
    SESSION,
    SKIP_PERMISSIONS_FLAG,
    "--mcp-config",
    f"/state/channels/{SESSION}.mcp.json",
    "--dangerously-load-development-channels",
    "server:hermes-control",
]


def trust_package(tmp_path: Path) -> Path:
    """A sidecar package laid out exactly as production resolves it:
    ``<root>/dist/src/main.js`` with the manifest at ``<root>`` — so
    ``entry.parents[2]`` is the package root, as in the real
    ``channels/hermes-control`` layout."""

    root = tmp_path / "artifact" / "channels" / "hermes-control"
    (root / "dist" / "src").mkdir(parents=True)
    (root / "package.json").write_text(
        json.dumps({"name": "hermes-control-channel", "version": "9.9.9"}),
        encoding="utf-8",
    )
    entry = root / "dist" / "src" / "main.js"
    entry.write_text("console.log('hermes-control');\n", encoding="utf-8")
    return entry


def capture_trust_anchor(
    database: Database,
    entry: Path,
    *,
    surface_uuid: str = LEAD.surface_uuid,
    prompt_pattern: str = APPROVED_PROMPT_PATTERN,
) -> object:
    events = EventStore(database)
    return ChannelTrustAnchors(database, events=events).capture(
        cell_id="cell-demo",
        profile_alias="max-a",
        entry_path=entry,
        package_root=entry.parents[2],
        channel_entry="server:hermes-control",
        launch_argv_template=CHANNEL_ARGV,
        workspace_uuid=LEAD.workspace_uuid,
        surface_uuid=surface_uuid,
        session_id=SESSION,
        prompt_pattern=prompt_pattern,
    )


async def _no_sleep(_seconds: float) -> None:
    return None


def trust_confirmer(
    database: Database,
    port: FakePort,
    entry: Path,
    *,
    control: ControlOperations,
    wait_seconds: int = 90,
    argv: list[str] | None = None,
) -> ChannelTrustConfirmer:
    ticks = iter(float(tick) for tick in range(10_000))
    live = list(CHANNEL_ARGV) if argv is None else list(argv)
    return ChannelTrustConfirmer(
        database=database,
        events=EventStore(database),
        control=control,
        port=port,
        entry_resolver=lambda: entry,
        live_argv=lambda _session: list(live),
        wait_seconds=wait_seconds,
        clock=lambda: next(ticks),
        sleep=_no_sleep,
    )


def control_operation_kinds(database: Database) -> list[str]:
    rows = database.execute(
        "SELECT kind FROM control_operations ORDER BY rowid ASC"
    ).fetchall()
    return [str(row["kind"]) for row in rows]


class FakeTrustTrigger:
    def __init__(
        self,
        error: Exception | None = None,
        verdict: TrustVerdict | None = None,
    ) -> None:
        self.calls: list[object] = []
        self.error = error
        self.verdict = verdict

    async def confirm_seat(self, binding: object) -> object | None:
        self.calls.append(binding)
        if self.error is not None:
            raise self.error
        return self.verdict


@dataclass
class SequencedScreenPort(FakePort):
    """A FakePort whose ``read_screen`` serves a scripted sequence:
    entries are consumed in order (the last one repeats), and an
    exception entry raises instead of returning — so the pane can
    change, or the surface vanish, between the watcher's detection
    read and the gate's last-moment re-read."""

    screen_sequence: list[object] = field(default_factory=list)

    async def read_screen(self, ref: CmuxSurfaceRef, *, lines: int = 60) -> str:
        self.screen_reads += 1
        step = (
            self.screen_sequence.pop(0)
            if len(self.screen_sequence) > 1
            else self.screen_sequence[0]
        )
        if isinstance(step, Exception):
            raise step
        return str(step)


class TestChannelTrustLifecycle:
    """The bounded watcher and trust gate run automatically for the
    exact newly created channel-launched binding — the manually invoked
    channel-trust-confirm CLI command is no longer the only path."""

    @pytest.mark.asyncio
    async def test_a_trusted_seat_auto_confirms_one_enter_without_the_cli(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        seed_cell(database)
        entry = trust_package(tmp_path)
        capture_trust_anchor(database, entry)
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(next_refs=[LEAD], screen=f"...\n{DIALOG_TEXT}\n")
        launch = FakeChannelLaunch(
            config=Path(f"/state/channels/{SESSION}.mcp.json")
        )
        confirmer = trust_confirmer(database, port, entry, control=control)

        seat = await seater(
            bindings,
            port,
            channel_launch=launch,
            control=control,
            channel_trust=confirmer,
        ).ensure(
            **demo_seat(),
            classic_command=(
                f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
            ),
        )

        assert seat is not None
        # Exactly one Enter, sent to the exact bound surface, with no
        # manual channel-trust-confirm invocation anywhere.
        assert port.confirmed == [LEAD]
        assert control_operation_kinds(database) == [
            "channel.confirm_claimed",
            "channel.auto_confirmed",
        ]
        # The watch detection read plus the gate's last-moment live
        # re-read of the exact surface immediately before the Enter
        # (Sol correction a9cc6d5f packet 3).
        assert port.screen_reads >= 2

    @pytest.mark.asyncio
    async def test_repeated_and_concurrent_triggers_send_at_most_one_enter(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        seed_cell(database)
        entry = trust_package(tmp_path)
        capture_trust_anchor(database, entry)
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(next_refs=[LEAD], screen=f"...\n{DIALOG_TEXT}\n")
        launch = FakeChannelLaunch(
            config=Path(f"/state/channels/{SESSION}.mcp.json")
        )
        confirmer = trust_confirmer(database, port, entry, control=control)
        await seater(
            bindings,
            port,
            channel_launch=launch,
            control=control,
            channel_trust=confirmer,
        ).ensure(
            **demo_seat(),
            classic_command=(
                f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
            ),
        )
        binding = bindings.active_lead("cell-demo")
        assert binding is not None

        first, second = await asyncio.gather(
            confirmer.confirm_seat(binding),
            confirmer.confirm_seat(binding),
        )

        # The lifecycle trigger already pressed the one Enter; the
        # durable claim CAS refuses every later or concurrent trigger
        # for the same launch.
        assert port.confirmed == [LEAD]
        for verdict in (first, second):
            assert verdict is not None
            assert verdict.confirmed is False
            assert verdict.first_failure == "confirm_already_claimed"

    @pytest.mark.asyncio
    async def test_prompt_mismatch_sends_zero_keys(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        seed_cell(database)
        entry = trust_package(tmp_path)
        capture_trust_anchor(database, entry)
        control = ControlOperations(database, events=EventStore(database))
        # The watcher sees a dialog-shaped screen, but the full approved
        # four-marker sequence is not on it.
        port = FakePort(
            screen=(
                "Loading development channels\n"
                "  - server:hermes-control\n"
                "  Press Y to do something unexpected"
            )
        )
        binding = bind_demo_lead(bindings)
        confirmer = trust_confirmer(database, port, entry, control=control)

        verdict = await confirmer.confirm_seat(binding)

        assert port.confirmed == []
        assert verdict is not None
        assert verdict.confirmed is False
        assert verdict.first_failure == "prompt_match"
        assert control_operation_kinds(database) == [
            "channel.approval_required"
        ]

    @pytest.mark.asyncio
    async def test_surface_drift_with_matching_content_rebinds_and_confirms(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        """INFRA-208: before the rebind trigger existed, an anchor bound
        to a different surface than the live seat could only refuse —
        this is exactly the shape a managed rotation leaves behind. The
        content re-measured at the (unchanged) entry path is
        byte-identical to what the anchor already proved, so
        ``confirm_seat`` now carries the anchor forward to the live
        surface itself, and the gate then matches fully and presses the
        one Enter — zero manual dialog clicks."""

        seed_cell(database)
        entry = trust_package(tmp_path)
        # The anchor was trusted for a different surface than the one
        # this seat is bound to — the live identity a rotation leaves.
        predecessor = capture_trust_anchor(
            database, entry, surface_uuid=THIRD.surface_uuid
        )
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(screen=f"...\n{DIALOG_TEXT}\n")
        binding = bind_demo_lead(bindings)
        confirmer = trust_confirmer(database, port, entry, control=control)

        verdict = await confirmer.confirm_seat(binding)

        assert port.confirmed == [LEAD]
        assert verdict is not None
        assert verdict.confirmed is True
        assert control_operation_kinds(database) == [
            "channel.confirm_claimed",
            "channel.auto_confirmed",
        ]
        successor = ChannelTrustAnchors(
            database, events=EventStore(database)
        ).active_for_cell("cell-demo")
        assert successor is not None
        assert successor.anchor_id != predecessor.anchor_id
        assert successor.surface_uuid == LEAD.surface_uuid
        assert successor.prompt_pattern == predecessor.prompt_pattern

    @pytest.mark.asyncio
    async def test_a_rebind_refusal_records_a_receipt_and_still_reaches_approval(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        """A rebind attempt can itself refuse — here, the build at the
        live entry path changed after the anchor was trusted, so the
        content re-measured no longer matches: a genuinely new trust
        decision, not a rotation. That refusal is recorded durably as
        ``channel.rebind_refused``, and the seat still falls through to
        the gate's own existing approval-required path exactly as it
        did before INFRA-208 — the dialog path and its receipts are
        unchanged."""

        seed_cell(database)
        entry = trust_package(tmp_path)
        capture_trust_anchor(database, entry, surface_uuid=THIRD.surface_uuid)
        # The build at this same path changed after the anchor was
        # trusted.
        entry.write_text("console.log('a different build');\n", encoding="utf-8")
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(screen=f"...\n{DIALOG_TEXT}\n")
        binding = bind_demo_lead(bindings)
        confirmer = trust_confirmer(database, port, entry, control=control)

        verdict = await confirmer.confirm_seat(binding)

        assert port.confirmed == []
        assert verdict is not None
        assert verdict.confirmed is False
        assert control_operation_kinds(database) == [
            "channel.rebind_refused",
            "channel.approval_required",
        ]
        row = database.execute(
            "SELECT reason, result_json FROM control_operations "
            "WHERE kind = 'channel.rebind_refused'"
        ).fetchone()
        assert row is not None
        assert "REBIND REFUSED" in str(row["reason"])
        assert "error" in json.loads(str(row["result_json"]))
        # The predecessor anchor is untouched by the refused rebind —
        # the gate below evaluates against it directly.
        anchors = ChannelTrustAnchors(database, events=EventStore(database))
        anchor = anchors.active_for_cell("cell-demo")
        assert anchor is not None
        assert anchor.surface_uuid == THIRD.surface_uuid

    @pytest.mark.asyncio
    async def test_an_anchorless_cell_adopts_the_projects_proven_anchor(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        """INFRA-198 blocker 1 (observed live 2026-09-01): ``start-lane``
        created visible harness cell 8369559d with NO anchor of its own,
        while every existing anchor belonged to the development cell of
        the same project. Trust was cell-scoped, so ``anchor_present``
        refused on every attempt and no retry could ever succeed --
        ``capture`` records a MANUAL trust event, so the new cell had no
        path to acquire one.

        The operator's proof is a PROGRAM identity, not a property of a
        cell, so the sibling's proven anchor is carried onto this cell
        before the gate runs and the seat auto-confirms with zero manual
        dialog clicks. This drives the real ``confirm_seat`` path: it is
        the assertion that the production caller exists at all."""

        seed_cell(database)
        seed_harness_cell(database)
        entry = trust_package(tmp_path)
        # The one proven anchor belongs to the DEVELOPMENT cell.
        source = capture_trust_anchor(database, entry)
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(screen=f"...\n{DIALOG_TEXT}\n")
        binding = bind_harness_lead(bindings)
        confirmer = trust_confirmer(database, port, entry, control=control)

        verdict = await confirmer.confirm_seat(binding)

        assert verdict is not None
        assert verdict.confirmed is True
        assert port.confirmed == [HARNESS]
        assert control_operation_kinds(database) == [
            "channel.confirm_claimed",
            "channel.auto_confirmed",
        ]
        anchors = ChannelTrustAnchors(database, events=EventStore(database))
        adopted = anchors.active_for_cell("cell-harness")
        assert adopted is not None
        assert adopted.anchor_id != source.anchor_id
        assert adopted.prompt_pattern == source.prompt_pattern
        # Adoption COPIES: the development cell keeps its own trust.
        kept = anchors.active_for_cell("cell-demo")
        assert kept is not None
        assert kept.anchor_id == source.anchor_id

    @pytest.mark.asyncio
    async def test_an_adopt_refusal_records_a_receipt_and_still_reaches_approval(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        """An adoption can itself refuse -- here the build at the live
        entry path changed after the sibling's anchor was trusted, so
        the content re-measured no longer matches: a genuinely new trust
        decision. It is recorded as ``channel.adopt_refused`` and the
        seat still falls through to the gate's existing
        approval-required path, leaving the manual dialog and its
        receipts exactly as they were."""

        seed_cell(database)
        seed_harness_cell(database)
        entry = trust_package(tmp_path)
        source = capture_trust_anchor(database, entry)
        entry.write_text("console.log('a different build');\n", encoding="utf-8")
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(screen=f"...\n{DIALOG_TEXT}\n")
        binding = bind_harness_lead(bindings)
        confirmer = trust_confirmer(database, port, entry, control=control)

        verdict = await confirmer.confirm_seat(binding)

        assert port.confirmed == []
        assert verdict is not None
        assert verdict.confirmed is False
        assert control_operation_kinds(database) == [
            "channel.adopt_refused",
            "channel.approval_required",
        ]
        row = database.execute(
            "SELECT reason, result_json FROM control_operations "
            "WHERE kind = 'channel.adopt_refused'"
        ).fetchone()
        assert row is not None
        assert "ADOPT REFUSED" in str(row["reason"])
        # Nothing was minted for the anchorless cell.
        anchors = ChannelTrustAnchors(database, events=EventStore(database))
        assert anchors.active_for_cell("cell-harness") is None
        assert anchors.active_for_cell("cell-demo").anchor_id == source.anchor_id

    @pytest.mark.asyncio
    async def test_a_same_session_and_surface_re_ensure_never_rebinds(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        """No rotation happened — the active anchor already binds this
        exact session and surface — so ``confirm_seat`` never attempts
        a rebind at all: no predecessor is retired, no successor is
        minted, and the single anchor captured for this seat is the
        exact one the gate matches against."""

        seed_cell(database)
        entry = trust_package(tmp_path)
        original = capture_trust_anchor(database, entry)
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(screen=f"...\n{DIALOG_TEXT}\n")
        binding = bind_demo_lead(bindings)
        confirmer = trust_confirmer(database, port, entry, control=control)

        verdict = await confirmer.confirm_seat(binding)

        assert verdict is not None
        assert verdict.confirmed is True
        anchors = ChannelTrustAnchors(database, events=EventStore(database))
        active = anchors.active_for_cell("cell-demo")
        assert active is not None
        assert active.anchor_id == original.anchor_id
        rows = database.execute(
            "SELECT event_type FROM events "
            "WHERE event_type LIKE 'channel_trust_anchor.%' ORDER BY sequence"
        ).fetchall()
        assert [str(row["event_type"]) for row in rows] == [
            "channel_trust_anchor.captured"
        ]
        assert "channel.rebind_refused" not in control_operation_kinds(database)

    @pytest.mark.asyncio
    async def test_managed_rotation_to_the_selected_profile_rebinds_and_confirms(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        """Sol correction 531484d2: the realistic managed account
        rotation. Trust was proven on profile max-a (session SESSION,
        surface THIRD). Hermes selects a different healthy profile
        max-b and seats the replacement exactly as production does —
        write-ahead intent naming (SESSION_2, max-b), bound to the new
        workspace as a residual row, promoted to the cell's active lead
        binding — and the replacement launches with the same shared
        command shape, differing only at the session slots. The seat
        trigger carries the anchor forward to max-b/SESSION_2/LEAD and
        the gate presses the one Enter: zero manual dialog clicks."""

        seed_cell(database)
        entry = trust_package(tmp_path)
        predecessor = capture_trust_anchor(
            database, entry, surface_uuid=THIRD.surface_uuid
        )
        intent = bindings.record_intent(
            project_key="demo",
            cell_id="cell-demo",
            session_id=SESSION_2,
            profile_alias="max-b",
        )
        residual = bindings.bind_intent(intent.intent_id, ref=LEAD)
        binding = bindings.activate_residual(residual.binding_id)
        rotated_argv = [token.replace(SESSION, SESSION_2) for token in CHANNEL_ARGV]
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(screen=f"...\n{DIALOG_TEXT}\n")
        confirmer = trust_confirmer(
            database, port, entry, control=control, argv=rotated_argv
        )

        verdict = await confirmer.confirm_seat(binding)

        assert port.confirmed == [LEAD]
        assert verdict is not None
        assert verdict.confirmed is True
        assert control_operation_kinds(database) == [
            "channel.confirm_claimed",
            "channel.auto_confirmed",
        ]
        anchors = ChannelTrustAnchors(database, events=EventStore(database))
        successor = anchors.active_for_cell("cell-demo")
        assert successor is not None
        assert successor.anchor_id != predecessor.anchor_id
        assert successor.profile_alias == "max-b"
        assert successor.session_id == SESSION_2
        assert successor.surface_uuid == LEAD.surface_uuid
        assert successor.workspace_uuid == LEAD.workspace_uuid
        # Content, prompt evidence, and the trusted launch template are
        # the predecessor's — only the seat identity moved.
        assert successor.prompt_pattern == predecessor.prompt_pattern
        assert successor.launch_argv_template == tuple(CHANNEL_ARGV)
        assert anchors.get(predecessor.anchor_id).state == "retired"
        rebound = database.execute(
            "SELECT payload_json FROM events "
            "WHERE event_type = 'channel_trust_anchor.rebound'"
        ).fetchone()
        payload = json.loads(str(rebound["payload_json"]))
        assert payload["predecessor_anchor_id"] == predecessor.anchor_id
        assert payload["predecessor_profile_alias"] == "max-a"
        assert payload["selected_binding_id"] == binding.binding_id
        assert payload["selected_intent_id"] == intent.intent_id

    @pytest.mark.asyncio
    async def test_a_profile_no_durable_selection_names_sends_zero_keys(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        """The seat claims a different profile than the anchor's, but
        it was minted outside Hermes's write-ahead intent path — there
        is no durable selection proving max-b was chosen for this
        session. The rebind refuses (recorded as
        ``channel.rebind_refused``), the anchor is untouched, and the
        gate's own approval-required path follows with zero keys."""

        seed_cell(database)
        entry = trust_package(tmp_path)
        predecessor = capture_trust_anchor(
            database, entry, surface_uuid=THIRD.surface_uuid
        )
        binding = bindings.bind_lead(
            project_key="demo",
            cell_id="cell-demo",
            session_id=SESSION_2,
            profile_alias="max-b",
            ref=LEAD,
        )
        rotated_argv = [token.replace(SESSION, SESSION_2) for token in CHANNEL_ARGV]
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(screen=f"...\n{DIALOG_TEXT}\n")
        confirmer = trust_confirmer(
            database, port, entry, control=control, argv=rotated_argv
        )

        verdict = await confirmer.confirm_seat(binding)

        assert port.confirmed == []
        assert verdict is not None
        assert verdict.confirmed is False
        assert control_operation_kinds(database) == [
            "channel.rebind_refused",
            "channel.approval_required",
        ]
        row = database.execute(
            "SELECT result_json FROM control_operations "
            "WHERE kind = 'channel.rebind_refused'"
        ).fetchone()
        assert "Hermes-selected replacement binding" in json.loads(
            str(row["result_json"])
        )["error"]
        anchors = ChannelTrustAnchors(database, events=EventStore(database))
        active = anchors.active_for_cell("cell-demo")
        assert active is not None
        assert active.anchor_id == predecessor.anchor_id
        assert active.profile_alias == "max-a"
        assert active.surface_uuid == THIRD.surface_uuid

    @pytest.mark.asyncio
    async def test_watcher_timeout_sends_zero_keys_and_no_receipt(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        seed_cell(database)
        entry = trust_package(tmp_path)
        capture_trust_anchor(database, entry)
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(screen="just a shell prompt, no dialog")
        binding = bind_demo_lead(bindings)
        confirmer = trust_confirmer(
            database, port, entry, control=control, wait_seconds=3
        )

        verdict = await confirmer.confirm_seat(binding)

        # An absent dialog is not a trust refusal: zero keys and no
        # durable trust receipt of any kind.
        assert verdict is None
        assert port.confirmed == []
        assert port.screen_reads >= 1
        assert control_operation_kinds(database) == []

    @pytest.mark.asyncio
    async def test_claim_failure_sends_zero_keys(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        class _ClaimFails(ControlOperations):
            def record(self, **kwargs: object) -> object:
                if kwargs.get("kind") == "channel.confirm_claimed":
                    raise RuntimeError("claim store down")
                return super().record(**kwargs)

        seed_cell(database)
        entry = trust_package(tmp_path)
        capture_trust_anchor(database, entry)
        control = _ClaimFails(database, events=EventStore(database))
        port = FakePort(screen=f"...\n{DIALOG_TEXT}\n")
        binding = bind_demo_lead(bindings)
        confirmer = trust_confirmer(database, port, entry, control=control)

        verdict = await confirmer.confirm_seat(binding)

        assert port.confirmed == []
        assert verdict is not None
        assert verdict.confirmed is False
        assert verdict.first_failure == "confirm_claim_failed"

    @pytest.mark.asyncio
    async def test_pane_change_between_detection_and_confirmation_sends_zero_keys(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        """Sol correction a9cc6d5f packet 3, required test (1): the
        watcher detects the exact approved dialog, but the pane changes
        before confirmation — the gate's last-moment live re-read of
        the exact surface sees the change and ZERO Enter goes out; the
        durable non-success refusal records with the retained claim."""

        seed_cell(database)
        entry = trust_package(tmp_path)
        capture_trust_anchor(database, entry)
        control = ControlOperations(database, events=EventStore(database))
        port = SequencedScreenPort(
            screen_sequence=[
                f"...\n{DIALOG_TEXT}\n",  # the watcher's detection read
                "$ user typed something; the dialog is gone",  # boundary
            ]
        )
        binding = bind_demo_lead(bindings)
        confirmer = trust_confirmer(database, port, entry, control=control)

        verdict = await confirmer.confirm_seat(binding)

        assert port.confirmed == []
        assert verdict is not None
        assert verdict.confirmed is False
        assert verdict.first_failure == "final_prompt_missing"
        assert control_operation_kinds(database) == [
            "channel.confirm_claimed",
            "channel.approval_required",
        ]

    @pytest.mark.asyncio
    async def test_surface_loss_at_the_final_boundary_sends_zero_keys(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        """Sol correction a9cc6d5f packet 3, required test (3): the
        exact bound surface is replaced or vanishes between detection
        and the keypress boundary, so the last-moment bounded re-read
        fails — zero keys and a durable non-success result."""

        seed_cell(database)
        entry = trust_package(tmp_path)
        capture_trust_anchor(database, entry)
        control = ControlOperations(database, events=EventStore(database))
        port = SequencedScreenPort(
            screen_sequence=[
                f"...\n{DIALOG_TEXT}\n",  # the watcher's detection read
                CmuxUnavailable("the exact surface no longer exists"),
            ]
        )
        binding = bind_demo_lead(bindings)
        confirmer = trust_confirmer(database, port, entry, control=control)

        verdict = await confirmer.confirm_seat(binding)

        assert port.confirmed == []
        assert verdict is not None
        assert verdict.confirmed is False
        assert verdict.first_failure == "final_read_failed"
        assert control_operation_kinds(database) == [
            "channel.confirm_claimed",
            "channel.approval_required",
        ]

    @pytest.mark.asyncio
    async def test_a_seat_composed_without_the_collaborator_behaves_as_today(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        port = FakePort(next_refs=[LEAD], screen=f"...\n{DIALOG_TEXT}\n")
        launch = FakeChannelLaunch(
            config=Path(f"/state/channels/{SESSION}.mcp.json")
        )

        seat = await seater(bindings, port, channel_launch=launch).ensure(
            **demo_seat(),
            classic_command=(
                f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
            ),
        )

        assert seat is not None
        assert port.screen_reads == 0
        assert port.confirmed == []

    @pytest.mark.asyncio
    async def test_the_trigger_fires_once_per_new_channel_binding(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        port = FakePort(next_refs=[LEAD])
        launch = FakeChannelLaunch(
            config=Path(f"/state/channels/{SESSION}.mcp.json")
        )
        trigger = FakeTrustTrigger()
        ensure = seater(
            bindings, port, channel_launch=launch, channel_trust=trigger
        )

        first = await ensure.ensure(
            **demo_seat(),
            classic_command=(
                f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
            ),
        )
        second = await ensure.ensure(
            **demo_seat(),
            classic_command=(
                f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
            ),
        )

        # One trigger, carrying the exact newly created binding; the
        # reuse path never re-triggers.
        assert second.binding_id == first.binding_id
        assert [b.binding_id for b in trigger.calls] == [first.binding_id]

    @pytest.mark.asyncio
    async def test_a_trigger_failure_never_breaks_the_seat(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        port = FakePort(next_refs=[LEAD])
        launch = FakeChannelLaunch(
            config=Path(f"/state/channels/{SESSION}.mcp.json")
        )
        trigger = FakeTrustTrigger(error=RuntimeError("watcher exploded"))

        seat = await seater(
            bindings, port, channel_launch=launch, channel_trust=trigger
        ).ensure(
            **demo_seat(),
            classic_command=(
                f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
            ),
        )

        assert seat is not None
        assert len(trigger.calls) == 1
        assert bindings.active_lead("cell-demo") is not None

    @pytest.mark.asyncio
    async def test_a_channel_less_seat_never_triggers_the_gate(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        trigger = FakeTrustTrigger()
        # A launcher failure drains to the bare classic command; the
        # bare seat carries no dev-channel dialog to confirm.
        port = FakePort(next_refs=[LEAD, FRESH])
        failing = FakeChannelLaunch(error=FileNotFoundError("no build"))
        await seater(
            bindings, port, channel_launch=failing, channel_trust=trigger
        ).ensure(
            **demo_seat(),
            classic_command=(
                f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
            ),
        )
        # And a plain classic seat with no launcher composed at all.
        rotated = "88888888-8888-4888-8888-888888888888"
        await seater(bindings, port, channel_trust=trigger).ensure(
            **demo_seat(cell_id="cell-other", session_id=rotated),
            classic_command=(
                f"claude --session-id {rotated} {SKIP_PERMISSIONS_FLAG}"
            ),
        )

        assert trigger.calls == []
        assert port.confirmed == []

    @pytest.mark.asyncio
    async def test_an_active_artifact_missing_its_entry_blocks_the_channel(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        """Sol correction f0a5a403 (packet 2), launch side: with an
        ACTIVE runtime recorded but its sidecar entry missing, the seat
        launches channel-less with the existing actionable
        channel.blocked receipt — the mutable repo_root checkout build
        is present and still never executed."""

        from hermes_orchestrator.channel_hub import ChannelLauncher
        from hermes_orchestrator.runtime import resolve_sidecar_entry

        repo_root = tmp_path / "repo"
        mutable = repo_root / "channels/hermes-control/dist/src/main.js"
        mutable.parent.mkdir(parents=True)
        mutable.write_text("// mutable checkout bytes\n", encoding="utf-8")
        state_dir = tmp_path / "state"
        artifact = state_dir / "runtimes" / "cafe"
        artifact.mkdir(parents=True)  # no sidecar inside this artifact
        (state_dir / "runtimes" / "ACTIVE").write_text(
            str(artifact), encoding="utf-8"
        )

        launcher = ChannelLauncher(
            state_dir=state_dir,
            capabilities=None,  # unreachable: the entry check fails first
            sidecar_entry=resolve_sidecar_entry(
                repo_root=repo_root, state_dir=state_dir
            ),
            node_binary=Path("/usr/bin/true"),
        )
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(next_refs=[LEAD])

        seat = await seater(
            bindings, port, channel_launch=launcher, control=control
        ).ensure(
            **demo_seat(),
            classic_command=(
                f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
            ),
        )

        assert seat is not None
        [created] = port.created
        assert created["command"] == (
            f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
        )
        assert str(mutable) not in str(created["command"])
        [receipt] = control.pending_for_session(SESSION)
        assert receipt.kind == "channel.blocked"
        assert "sidecar build is missing" in str(
            receipt.result["launcher_error"]
        )


# --------------------------------------------------------------------
# Restart recovery fails closed on channel trust (Sol correction
# c5600e31): unlike the seater's best-effort one-shot trigger, a
# restart-recovered lead may not sit active/classic in durable state
# until it completes the same bounded channel-trust confirmation and
# registration path normal seating runs.
# --------------------------------------------------------------------


class TestReconcilerChannelTrust:
    @pytest.mark.asyncio
    async def test_restart_recovery_confirms_trust_before_going_active(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        """The exact same bounded watch-then-gate path normal seating
        triggers now runs for the restart-recovered successor: the
        anchor trusted on the dying seat's own surface (LEAD) is
        carried forward to the freshly created replacement surface
        (FRESH) — the INFRA-208 rebind-and-confirm mechanics
        ``ChannelTrustConfirmer`` already provides — and the seat only
        stays active/classic once that gate presses the one Enter."""

        seed_cell(database)
        bind_demo_lead(bindings)
        entry = trust_package(tmp_path)
        capture_trust_anchor(database, entry)  # anchored to LEAD's surface
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(next_refs=[FRESH], screen=f"...\n{DIALOG_TEXT}\n")
        launch = FakeChannelLaunch(
            config=Path(f"/state/channels/{SESSION}.mcp.json")
        )
        confirmer = trust_confirmer(database, port, entry, control=control)

        report = await reconciler(
            bindings,
            port,
            channel_launch=launch,
            control=control,
            channel_trust=confirmer,
        ).reconcile()

        successor = bindings.active_lead("cell-demo")
        assert successor is not None
        assert successor.state == "active"
        assert successor.ref == FRESH
        assert bindings.is_classic(successor.binding_id, SESSION) is True
        assert report.replaced == (successor.binding_id,)
        assert report.lost == ()
        # The Enter landed on the exact bound (new) surface, never the
        # dead predecessor's.
        assert port.confirmed == [FRESH]
        assert control_operation_kinds(database) == [
            "channel.confirm_claimed",
            "channel.auto_confirmed",
        ]
        anchors = ChannelTrustAnchors(database, events=EventStore(database))
        active_anchor = anchors.active_for_cell("cell-demo")
        assert active_anchor is not None
        assert active_anchor.surface_uuid == FRESH.surface_uuid
        assert active_anchor.session_id == SESSION

    @pytest.mark.asyncio
    async def test_a_timed_out_confirmation_leaves_no_active_binding(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
    ) -> None:
        """The bounded watch never sees the dialog (``confirm_seat``
        returns ``None``): exactly as unproven as an explicit refusal,
        so the successor is recorded lost, never active/classic, with
        one durable actionable ``channel.blocked`` receipt."""

        bind_demo_lead(bindings)
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(next_refs=[FRESH])
        launch = FakeChannelLaunch(
            config=Path(f"/state/channels/{SESSION}.mcp.json")
        )
        trigger = FakeTrustTrigger()  # always returns None

        report = await reconciler(
            bindings,
            port,
            channel_launch=launch,
            control=control,
            channel_trust=trigger,
        ).reconcile()

        assert report.replaced == ()
        [lost_id] = report.lost
        assert bindings.get(lost_id).state == "lost"
        assert bindings.active_lead("cell-demo") is None
        # No stale classic-seat evidence survives an unproven
        # confirmation, and the exact replacement workspace — never the
        # dead predecessor's — is closed through the port.
        assert bindings.is_classic(lost_id, SESSION) is False
        assert port.closed == [FRESH.workspace_uuid]
        # The trust trigger saw the exact successor binding — the new
        # surface, not the dead predecessor's.
        [seen] = trigger.calls
        assert seen.binding_id == lost_id
        assert seen.ref == FRESH
        [receipt] = control.pending_for_session(SESSION)
        assert receipt.kind == "channel.blocked"
        assert "CHANNEL TRUST UNCONFIRMED" in str(receipt.reason)
        assert receipt.result["successor_binding_id"] == lost_id

    @pytest.mark.asyncio
    async def test_a_raising_trigger_leaves_no_active_binding(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
    ) -> None:
        bind_demo_lead(bindings)
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(next_refs=[FRESH])
        launch = FakeChannelLaunch(
            config=Path(f"/state/channels/{SESSION}.mcp.json")
        )
        trigger = FakeTrustTrigger(error=RuntimeError("watcher exploded"))

        report = await reconciler(
            bindings,
            port,
            channel_launch=launch,
            control=control,
            channel_trust=trigger,
        ).reconcile()

        assert report.replaced == ()
        [lost_id] = report.lost
        assert bindings.active_lead("cell-demo") is None
        assert bindings.is_classic(lost_id, SESSION) is False
        assert port.closed == [FRESH.workspace_uuid]
        [receipt] = control.pending_for_session(SESSION)
        assert receipt.kind == "channel.blocked"
        assert receipt.result["trigger_error"] == "watcher exploded"

    @pytest.mark.asyncio
    async def test_a_refused_verdict_leaves_no_active_binding(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        tmp_path: Path,
    ) -> None:
        """The dialog appears but the gate's own full re-derivation
        refuses it (content drift here): a non-confirmed
        ``TrustVerdict``, not ``None`` — handled identically."""

        seed_cell(database)
        bind_demo_lead(bindings)
        entry = trust_package(tmp_path)
        capture_trust_anchor(database, entry)
        # The build at this same path changed after the anchor was
        # trusted, so the gate's content re-derivation refuses.
        entry.write_text("console.log('a different build');\n", encoding="utf-8")
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(next_refs=[FRESH], screen=f"...\n{DIALOG_TEXT}\n")
        launch = FakeChannelLaunch(
            config=Path(f"/state/channels/{SESSION}.mcp.json")
        )
        confirmer = trust_confirmer(database, port, entry, control=control)

        report = await reconciler(
            bindings,
            port,
            channel_launch=launch,
            control=control,
            channel_trust=confirmer,
        ).reconcile()

        assert report.replaced == ()
        [lost_id] = report.lost
        assert bindings.active_lead("cell-demo") is None
        assert port.confirmed == []
        assert bindings.is_classic(lost_id, SESSION) is False
        assert port.closed == [FRESH.workspace_uuid]
        # The gate's own refusal receipt still records; the reconciler
        # adds its own seat-level channel.blocked receipt on top.
        assert "channel.approval_required" in control_operation_kinds(database)
        assert "channel.blocked" in control_operation_kinds(database)

    @pytest.mark.asyncio
    async def test_a_launcher_without_a_trust_trigger_fails_closed(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
    ) -> None:
        """A reconciler composed with a channel launcher but no
        ``channel_trust`` collaborator can never obtain proof of trust
        either, so restart recovery fails exactly the same way as a
        timed-out or refused confirmation."""

        bind_demo_lead(bindings)
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(next_refs=[FRESH])
        launch = FakeChannelLaunch(
            config=Path(f"/state/channels/{SESSION}.mcp.json")
        )

        report = await reconciler(
            bindings, port, channel_launch=launch, control=control
        ).reconcile()

        assert report.replaced == ()
        [lost_id] = report.lost
        assert bindings.active_lead("cell-demo") is None
        assert bindings.is_classic(lost_id, SESSION) is False
        assert port.closed == [FRESH.workspace_uuid]
        [receipt] = control.pending_for_session(SESSION)
        assert receipt.kind == "channel.blocked"
        assert "CHANNEL TRUST UNCONFIRMED" in str(receipt.reason)

    @pytest.mark.asyncio
    async def test_classic_evidence_is_recorded_only_after_a_confirmed_verdict(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
    ) -> None:
        """Sol correction 57c46faa (packet 2): classic evidence must
        never exist before the trust trigger returns a confirmed
        verdict — recording it up front would let a failed or
        unproven confirmation leave stale classic evidence behind."""

        bind_demo_lead(bindings)
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(next_refs=[FRESH])
        launch = FakeChannelLaunch(
            config=Path(f"/state/channels/{SESSION}.mcp.json")
        )
        observed: dict[str, object] = {}

        class OrderingTrigger:
            async def confirm_seat(self, binding: object) -> TrustVerdict:
                # The successor is already the active binding by the
                # time trust is checked, but classic evidence must not
                # exist yet.
                observed["binding_id"] = binding.binding_id
                observed["active_before_confirm"] = (
                    bindings.active_lead("cell-demo").binding_id
                    == binding.binding_id
                )
                observed["classic_before_confirm"] = bindings.is_classic(
                    binding.binding_id, SESSION
                )
                return TrustVerdict(confirmed=True, anchor_id="anchor-1")

        report = await reconciler(
            bindings,
            port,
            channel_launch=launch,
            control=control,
            channel_trust=OrderingTrigger(),
        ).reconcile()

        successor_id = observed["binding_id"]
        assert report.replaced == (successor_id,)
        assert observed["active_before_confirm"] is True
        assert observed["classic_before_confirm"] is False
        assert bindings.is_classic(successor_id, SESSION) is True
        assert bindings.get(successor_id).state == "active"

    @pytest.mark.asyncio
    async def test_a_workspace_closure_failure_retains_residual_ownership(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
    ) -> None:
        """When trust is unconfirmed AND the compensated close cannot
        be confirmed either, the binding must not be marked lost
        outright — it is held as residual ownership evidence, exposing
        no active or classic seat, until a later reconciliation pass
        reclaims the exact surface."""

        bind_demo_lead(bindings)
        control = ControlOperations(database, events=EventStore(database))
        port = FakePort(
            next_refs=[FRESH],
            fail={"close_workspace": CmuxUnavailable("cmux socket busy")},
        )
        launch = FakeChannelLaunch(
            config=Path(f"/state/channels/{SESSION}.mcp.json")
        )
        trigger = FakeTrustTrigger()  # always returns None (unconfirmed)

        report = await reconciler(
            bindings,
            port,
            channel_launch=launch,
            control=control,
            channel_trust=trigger,
        ).reconcile()

        assert report.replaced == ()
        assert report.lost == ()
        assert bindings.active_lead("cell-demo") is None
        [seen] = trigger.calls
        successor_id = seen.binding_id
        successor = bindings.get(successor_id)
        assert successor.state == "residual"
        assert successor.ref == FRESH
        assert bindings.is_classic(successor_id, SESSION) is False
        # The close was attempted but never confirmed — the workspace
        # is not recorded closed.
        assert port.closed == []

        # A later reconciliation pass, once cmux can confirm the close,
        # reclaims the exact residual surface instead of leaking it.
        port.fail.pop("close_workspace")
        port.live.add(FRESH)
        second_report = await reconciler(
            bindings,
            port,
            channel_launch=launch,
            control=control,
            channel_trust=FakeTrustTrigger(),
        ).reconcile()

        assert successor_id in second_report.reclaimed
        assert bindings.get(successor_id).state == "closed"
        assert port.closed == [FRESH.workspace_uuid]

    @pytest.mark.asyncio
    async def test_normal_seating_and_restart_recovery_bind_identical_identity(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
    ) -> None:
        """Both paths hand the exact same trigger the exact same
        binding-identity fields — cell, session, and profile — for the
        one launch this cell owns; only the surface identity differs
        because restart recovery necessarily creates a new workspace."""

        normal_trigger = FakeTrustTrigger()
        seat_port = FakePort(next_refs=[LEAD])
        seated = await seater(
            bindings,
            seat_port,
            channel_launch=FakeChannelLaunch(
                config=Path(f"/state/channels/{SESSION}.mcp.json")
            ),
            control=ControlOperations(database, events=EventStore(database)),
            channel_trust=normal_trigger,
        ).ensure(
            **demo_seat(),
            classic_command=(
                f"claude --session-id {SESSION} {SKIP_PERMISSIONS_FLAG}"
            ),
        )
        [normal_seen] = normal_trigger.calls
        assert normal_seen.binding_id == seated.binding_id

        recovery_trigger = FakeTrustTrigger()
        recovery_port = FakePort(next_refs=[FRESH])
        await reconciler(
            bindings,
            recovery_port,
            channel_launch=FakeChannelLaunch(
                config=Path(f"/state/channels/{SESSION}.mcp.json")
            ),
            control=ControlOperations(database, events=EventStore(database)),
            channel_trust=recovery_trigger,
        ).reconcile()
        [recovery_seen] = recovery_trigger.calls

        assert normal_seen.cell_id == recovery_seen.cell_id == "cell-demo"
        assert normal_seen.session_id == recovery_seen.session_id == SESSION
        assert normal_seen.profile_alias == recovery_seen.profile_alias == "max-a"
        assert normal_seen.project_key == recovery_seen.project_key == "demo"
        # Only the surface identity differs — a genuinely new workspace.
        assert normal_seen.ref == LEAD
        assert recovery_seen.ref == FRESH


def _dead_lead_sweep(
    database: Database,
    bindings: CmuxSurfaceBindings,
    port: FakePort,
    *,
    control: ControlOperations | None = None,
    leases: object | None = None,
    worker_alive: bool | None = True,
) -> CmuxDeadLeadSweep:
    """The sweep with its worker probe injected, never a live ``ps``.

    ``worker_alive`` is the tri-state answer the injected probe returns
    for every session: ``True`` (running), ``False`` (definitively
    absent) or ``None`` (unknown). It defaults to ``True`` so the
    surface-only tests keep measuring exactly the surface probe.
    """

    return CmuxDeadLeadSweep(
        bindings=bindings,
        port=port,
        database=database,
        events=EventStore(database),
        control=control,
        leases=leases,
        worker_alive=lambda _session_id: worker_alive,
    )


def _cell_state(database: Database, cell_id: str) -> str:
    row = database.execute(
        "SELECT state FROM project_cells WHERE cell_id = ?", (cell_id,)
    ).fetchone()
    return str(row["state"])


@pytest.mark.asyncio
async def test_dead_lead_sweep_retires_the_exact_dead_seat(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    """INFRA-198 (observed live 2026-09-01): harness workspace
    87B8378D was closed and its Claude PID exited, yet more than two
    daemon ticks later cell 8369559d and its binding were still
    ``active``, so no replacement could be launched. The cmux
    reconciler probes for exactly this, but runs ONCE at daemon
    startup, so a lead that dies mid-run stays active until the next
    restart. The per-tick sweep retires the exact dead identity."""

    seed_cell(database)
    control = ControlOperations(database, events=EventStore(database))
    port = FakePort()
    binding = bind_demo_lead(bindings)
    # The surface is absent: the workspace was closed and the PID exited.
    assert LEAD not in port.live

    retired = await _dead_lead_sweep(
        database, bindings, port, control=control
    ).tick()

    assert retired == (binding.binding_id,)
    assert bindings.active_lead("cell-demo") is None
    assert _cell_state(database, "cell-demo") == "failed"
    assert control_operation_kinds(database) == ["lead.dead_worker_retired"]


@pytest.mark.asyncio
async def test_dead_lead_sweep_retires_nothing_when_cmux_is_unreachable(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    """The regression that matters most: absence must be a SUCCESSFUL
    probe's answer. A denied or unreachable socket must never read as
    "every worker died" -- that would tear down healthy seats on a
    transient cmux hiccup, on every tick."""

    seed_cell(database)
    control = ControlOperations(database, events=EventStore(database))
    port = FakePort(deny=True)
    binding = bind_demo_lead(bindings)

    retired = await _dead_lead_sweep(
        database, bindings, port, control=control
    ).tick()

    assert retired == ()
    live = bindings.active_lead("cell-demo")
    assert live is not None
    assert live.binding_id == binding.binding_id
    assert _cell_state(database, "cell-demo") == "active"
    assert control_operation_kinds(database) == []


@pytest.mark.asyncio
async def test_dead_lead_sweep_leaves_a_live_seat_untouched(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    """A seat whose surface answers the probe is healthy and must be
    left exactly as found -- the sweep repairs residue, it does not
    recycle working leads."""

    seed_cell(database)
    control = ControlOperations(database, events=EventStore(database))
    port = FakePort(live={LEAD})
    binding = bind_demo_lead(bindings)

    retired = await _dead_lead_sweep(
        database, bindings, port, control=control
    ).tick()

    assert retired == ()
    live = bindings.active_lead("cell-demo")
    assert live is not None
    assert live.binding_id == binding.binding_id
    assert _cell_state(database, "cell-demo") == "active"
    assert control_operation_kinds(database) == []


@pytest.mark.parametrize(
    ("lines", "expected"),
    [
        # Exactly one managed process for this session: alive.
        ([f"claude --resume {SESSION} --dangerously-skip-permissions"], True),
        # ``ps`` succeeded and nothing matched: the ONLY definitive
        # absence, and the only answer a caller may retire on.
        (["/bin/zsh -l", f"claude --resume {SESSION_2}"], False),
        # Two matches: ambiguous, so no single root can be identified.
        (
            [
                f"claude --resume {SESSION}",
                f"claude --session-id {SESSION} --print",
            ],
            None,
        ),
    ],
)
def test_managed_claude_worker_alive_is_tri_state(
    monkeypatch: pytest.MonkeyPatch, lines: list[str], expected: bool | None
) -> None:
    """The primitive the sweep and ``start-lane`` both decide on.
    ``live_claude_argv`` collapses "absent" and "unmeasurable" into the
    same empty list; retirement destroys durable state, so it needs the
    two kept apart."""

    import subprocess

    from hermes_orchestrator import cmux_surfaces

    monkeypatch.setattr(
        cmux_surfaces.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=["ps"], returncode=0, stdout="\n".join(lines)
        ),
    )

    assert managed_claude_worker_alive(SESSION) is expected


def test_managed_claude_worker_alive_is_unknown_when_ps_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A measurement that could not run is not evidence of a dead
    worker -- it must read as unknown, never as a definitive absence."""

    from hermes_orchestrator import cmux_surfaces

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("ps unavailable")

    monkeypatch.setattr(cmux_surfaces.subprocess, "run", _boom)

    assert managed_claude_worker_alive(SESSION) is None


@pytest.mark.asyncio
async def test_dead_lead_sweep_retires_a_live_surface_whose_worker_is_gone(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    """The exact observed INFRA-198 failure: cmux workspace C39EBCDC /
    surface F5B1BD55 still EXISTED, but ``claude --resume 9b539c86``
    had exited with "No conversation found" and the pane was a bare
    shell prompt. The sweep's only liveness test was
    ``surface_alive``, which answered True forever, so the corpse was
    never retired and ``start-lane`` kept reporting ``already_running``
    for it. A definitively absent worker must condemn the seat even
    while its surface answers."""

    seed_cell(database)
    control = ControlOperations(database, events=EventStore(database))
    # The surface is alive -- exactly as observed live.
    port = FakePort(live={LEAD})
    binding = bind_demo_lead(bindings)

    retired = await _dead_lead_sweep(
        database, bindings, port, control=control, worker_alive=False
    ).tick()

    assert retired == (binding.binding_id,)
    assert bindings.active_lead("cell-demo") is None
    assert _cell_state(database, "cell-demo") == "failed"
    assert control_operation_kinds(database) == ["lead.dead_worker_retired"]
    # The receipt names WHICH signal condemned the seat, so an operator
    # can tell a closed workspace from a dead worker inside a live one.
    row = database.execute(
        "SELECT result_json FROM control_operations "
        "WHERE kind = 'lead.dead_worker_retired'"
    ).fetchone()
    assert json.loads(str(row["result_json"]))["condemning_signal"] == "worker_absent"


@pytest.mark.asyncio
async def test_dead_lead_sweep_keeps_a_seat_whose_worker_liveness_is_unknown(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    """The regression that guards the fix: the worker probe is
    tri-state, and only its definitive ``False`` may retire anything.
    ``None`` means ``ps`` failed or matched ambiguously -- retiring on
    that would tear down every healthy seat at once on a transient
    hiccup, on every tick."""

    seed_cell(database)
    control = ControlOperations(database, events=EventStore(database))
    port = FakePort(live={LEAD})
    binding = bind_demo_lead(bindings)

    retired = await _dead_lead_sweep(
        database, bindings, port, control=control, worker_alive=None
    ).tick()

    assert retired == ()
    live = bindings.active_lead("cell-demo")
    assert live is not None
    assert live.binding_id == binding.binding_id
    assert _cell_state(database, "cell-demo") == "active"
    assert control_operation_kinds(database) == []


@pytest.mark.asyncio
async def test_dead_lead_sweep_stops_on_a_mid_pass_cmux_failure(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    """cmux failing partway through is not evidence about anything it
    never probed: the pass ends and every unprobed binding keeps its
    durable state for the next tick."""

    seed_cell(database)
    control = ControlOperations(database, events=EventStore(database))
    port = FakePort(fail={"surface_alive": CmuxError("cmux went away")})
    binding = bind_demo_lead(bindings)

    retired = await _dead_lead_sweep(
        database, bindings, port, control=control
    ).tick()

    assert retired == ()
    live = bindings.active_lead("cell-demo")
    assert live is not None
    assert live.binding_id == binding.binding_id
    assert _cell_state(database, "cell-demo") == "active"


def test_retire_dead_lead_cell_retires_a_stale_lead_binding(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    """INFRA-219 (observed live): the harness lead's cmux pane
    survived while its Claude process exited. Seat-restore correctly
    refused to reactivate the dead worker, leaving the binding
    ``stale`` -- which made the cell invisible to ``tick()``, whose
    sweep only iterates ACTIVE lead bindings. ``retire_dead_lead_cell``
    is the cell-scoped retirement ``start-lane`` calls instead, after
    measuring the dead worker itself; it must never touch
    ``lead_assignments``."""

    seed_cell(database)
    control = ControlOperations(database, events=EventStore(database))
    binding = bind_demo_lead(bindings)
    database.execute(
        "UPDATE cmux_surface_bindings SET state = 'stale' "
        "WHERE binding_id = ?",
        (binding.binding_id,),
    )
    database.execute(
        "INSERT INTO profile_leases("
        "profile_alias, project_key, lane_role, state, acquired_at) "
        "VALUES ('max-a', 'demo', 'development', 'active', ?)",
        (NOW.isoformat(),),
    )
    database.execute(
        "INSERT INTO lead_assignments("
        "assignment_id, schema_version, project_key, issue_id, cell_id, "
        "session_id, profile_alias, instruction_id, queue_transition, "
        "state, created_at, updated_at) VALUES ("
        "'assign-1', 1, 'demo', 'issue-1', 'cell-demo', ?, 'max-a', "
        "'instr-1', 'dispatch', 'published', ?, ?)",
        (SESSION, NOW.isoformat(), NOW.isoformat()),
    )

    retired = _dead_lead_sweep(
        database, bindings, FakePort(), control=control
    ).retire_dead_lead_cell("cell-demo")

    assert retired is True
    # Only an ACTIVE binding is closed; this one was already 'stale'
    # (seat-restore's own refusal) and is left exactly as found.
    assert bindings.get(binding.binding_id).state == "stale"
    assert _cell_state(database, "cell-demo") == "failed"
    lease_row = database.execute(
        "SELECT 1 FROM profile_leases "
        "WHERE project_key = 'demo' AND lane_role = 'development'"
    ).fetchone()
    assert lease_row is None
    assert control_operation_kinds(database) == ["lead.dead_worker_retired"]
    result = json.loads(
        str(
            database.execute(
                "SELECT result_json FROM control_operations "
                "WHERE kind = 'lead.dead_worker_retired'"
            ).fetchone()["result_json"]
        )
    )
    assert result["condemning_signal"] == "worker_absent"
    # The versioned assignment packet is untouched -- this is a seat
    # retirement, never a durable-assignment mutation.
    assignment_row = database.execute(
        "SELECT state FROM lead_assignments WHERE assignment_id = 'assign-1'"
    ).fetchone()
    assert assignment_row is not None
    assert str(assignment_row["state"]) == "published"


def test_retire_dead_lead_cell_returns_false_when_cell_is_not_active(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    """A cell that has already moved on (or never existed) is left
    exactly as found -- zero writes, no receipt."""

    seed_cell(database, state="failed")
    control = ControlOperations(database, events=EventStore(database))
    binding = bind_demo_lead(bindings)

    retired = _dead_lead_sweep(
        database, bindings, FakePort(), control=control
    ).retire_dead_lead_cell("cell-demo")

    assert retired is False
    assert bindings.get(binding.binding_id).state == "active"
    assert _cell_state(database, "cell-demo") == "failed"
    assert control_operation_kinds(database) == []


# -- relaunched trusted seat restoration (INFRA-198) ---------------------


def restoring_reconciler(
    bindings: CmuxSurfaceBindings,
    port: FakePort,
    *,
    worker_alive: Callable[[str], bool | None],
) -> CmuxSurfaceReconciler:
    return CmuxSurfaceReconciler(
        bindings=bindings,
        port=port,
        project_paths={"demo": Path("/repos/demo")},
        profile_dirs=FakeProfileDirs({"max-a": Path("/profiles/max-a")}),
        environ={},
        worker_alive=worker_alive,
    )


def relaunched_seat_shape(
    database: Database, bindings: CmuxSurfaceBindings
) -> tuple[str, str]:
    """The observed post-reboot shape (2026-09-01): the relaunched
    seat's own binding is stale, the recovery attempt's successor is
    lost as channel_trust_unconfirmed, and no active lead remains --
    while the cell still names exactly this session and profile."""

    seed_cell(database)
    original = bind_demo_lead(bindings)
    successor = bindings.replace(
        original.binding_id, FRESH, reason="surface_missing"
    )
    bindings.mark_lost(
        successor.binding_id, reason="channel_trust_unconfirmed"
    )
    return str(original.binding_id), str(successor.binding_id)


#: The reboot re-mints only the workspace uuid around the surviving
#: surface (observed live 2026-09-01: workspace 29DBF6DA... became
#: A6011199... while surface A9289997... survived).
REBOOTED_WORKSPACE = CmuxSurfaceRef(
    workspace_uuid="bbbbbbbb-cccc-4ccc-8ccc-cccccccccccc",
    surface_uuid=LEAD.surface_uuid,
)


@pytest.mark.asyncio
async def test_reconcile_restores_relaunched_trusted_seat(
    database: Database, bindings: CmuxSurfaceBindings, tmp_path: Path
) -> None:
    """A reboot-relaunched seat whose pane, process, and proven anchor
    all still hold is restored to active without any relaunch, new
    workspace, or capability regeneration — even though the reboot
    re-minted the workspace uuid around the surviving surface — so its
    existing sidecar registers with the generation it already carries."""

    entry = trust_package(tmp_path)
    capture_trust_anchor(database, entry)
    original_id, lost_id = relaunched_seat_shape(database, bindings)
    port = FakePort(live={REBOOTED_WORKSPACE})
    report = await restoring_reconciler(
        bindings, port, worker_alive=lambda _session: True
    ).reconcile()
    active = bindings.active_lead("cell-demo")
    assert active is not None
    assert active.binding_id == original_id
    assert active.generation == 1
    assert active.ref == REBOOTED_WORKSPACE
    assert original_id in report.replaced
    assert bindings.get(lost_id).state == "lost"
    assert port.created == []


@pytest.mark.asyncio
async def test_relaunched_seat_stays_stale_without_live_worker(
    database: Database, bindings: CmuxSurfaceBindings, tmp_path: Path
) -> None:
    entry = trust_package(tmp_path)
    capture_trust_anchor(database, entry)
    original_id, _lost_id = relaunched_seat_shape(database, bindings)
    port = FakePort(live={LEAD})
    for verdict in (False, None):
        await restoring_reconciler(
            bindings, port, worker_alive=lambda _session, v=verdict: v
        ).reconcile()
        assert bindings.active_lead("cell-demo") is None
    def raising(_session: str) -> bool:
        raise RuntimeError("ps unavailable")
    await restoring_reconciler(
        bindings, port, worker_alive=raising
    ).reconcile()
    assert bindings.active_lead("cell-demo") is None
    assert bindings.get(original_id).state == "stale"


@pytest.mark.asyncio
async def test_relaunched_seat_stays_stale_without_matching_proven_anchor(
    database: Database, bindings: CmuxSurfaceBindings, tmp_path: Path
) -> None:
    """An anchor bound to a DIFFERENT surface (or a dead pane) is not
    this seat's proof: nothing is restored."""

    entry = trust_package(tmp_path)
    capture_trust_anchor(database, entry, surface_uuid=FRESH.surface_uuid)
    original_id, _lost_id = relaunched_seat_shape(database, bindings)
    port = FakePort(live={LEAD})
    await restoring_reconciler(
        bindings, port, worker_alive=lambda _session: True
    ).reconcile()
    assert bindings.active_lead("cell-demo") is None
    assert bindings.get(original_id).state == "stale"


@pytest.mark.asyncio
async def test_relaunched_seat_stays_stale_when_its_pane_is_gone(
    database: Database, bindings: CmuxSurfaceBindings, tmp_path: Path
) -> None:
    entry = trust_package(tmp_path)
    capture_trust_anchor(database, entry)
    original_id, _lost_id = relaunched_seat_shape(database, bindings)
    port = FakePort(live=set())
    await restoring_reconciler(
        bindings, port, worker_alive=lambda _session: True
    ).reconcile()
    assert bindings.active_lead("cell-demo") is None
    assert bindings.get(original_id).state == "stale"


@pytest.mark.asyncio
async def test_restored_seat_registers_at_its_rebooted_workspace(
    database: Database, bindings: CmuxSurfaceBindings, tmp_path: Path
) -> None:
    """Sol finding 1: restoration re-mints the binding's workspace, so
    the proven anchor must follow it -- otherwise the trust gate keeps
    comparing against the pre-reboot workspace and the restored seat is
    unregisterable by either route."""

    entry = trust_package(tmp_path)
    capture_trust_anchor(database, entry)
    relaunched_seat_shape(database, bindings)
    port = FakePort(live={REBOOTED_WORKSPACE})
    await restoring_reconciler(
        bindings, port, worker_alive=lambda _session: True
    ).reconcile()

    gate = ChannelTrustGate(
        database,
        EventStore(database),
        ChannelTrustAnchors(database, events=EventStore(database)),
        ControlOperations(database, events=EventStore(database)),
        lambda: DIALOG_TEXT,
        lambda: None,
    )
    verdict = gate.evaluate(
        cell_id="cell-demo",
        session_id=SESSION,
        workspace_uuid=REBOOTED_WORKSPACE.workspace_uuid,
        surface_uuid=REBOOTED_WORKSPACE.surface_uuid,
        profile_alias="max-a",
        entry_path=entry,
        package_root=entry.parents[2],
        launch_argv=list(CHANNEL_ARGV),
        screen_text=DIALOG_TEXT,
    )
    assert verdict.confirmed is True


@pytest.mark.asyncio
async def test_restored_seat_refuses_any_other_workspace(
    database: Database, bindings: CmuxSurfaceBindings, tmp_path: Path
) -> None:
    """The re-mint narrows trust rather than widening it: the pre-reboot
    workspace and any unrelated one are both refused afterwards."""

    entry = trust_package(tmp_path)
    capture_trust_anchor(database, entry)
    relaunched_seat_shape(database, bindings)
    port = FakePort(live={REBOOTED_WORKSPACE})
    await restoring_reconciler(
        bindings, port, worker_alive=lambda _session: True
    ).reconcile()

    gate = ChannelTrustGate(
        database,
        EventStore(database),
        ChannelTrustAnchors(database, events=EventStore(database)),
        ControlOperations(database, events=EventStore(database)),
        lambda: DIALOG_TEXT,
        lambda: None,
    )
    for workspace_uuid in (LEAD.workspace_uuid, FRESH.workspace_uuid):
        verdict = gate.evaluate(
            cell_id="cell-demo",
            session_id=SESSION,
            workspace_uuid=workspace_uuid,
            surface_uuid=REBOOTED_WORKSPACE.surface_uuid,
            profile_alias="max-a",
            entry_path=entry,
            package_root=entry.parents[2],
            launch_argv=list(CHANNEL_ARGV),
            screen_text=DIALOG_TEXT,
        )
        assert verdict.confirmed is False
        assert verdict.first_failure == "workspace_uuid"


@pytest.mark.asyncio
async def test_relaunched_seat_stays_stale_when_its_cell_moved_on(
    database: Database, bindings: CmuxSurfaceBindings, tmp_path: Path
) -> None:
    """Sol finding 2: a stale anchored session and its live process can
    outlive the cell that named them. A cell that has rotated to another
    session/profile/lane, or gone inactive, must not have an obsolete
    binding restored as its active owner."""

    drifts = (
        "UPDATE project_cells SET session_id = "
        "'11111111-2222-4222-8222-333333333333' WHERE cell_id = 'cell-demo'",
        "UPDATE project_cells SET profile_alias = 'max-b' "
        "WHERE cell_id = 'cell-demo'",
        "UPDATE project_cells SET lane_role = 'harness' "
        "WHERE cell_id = 'cell-demo'",
        "UPDATE project_cells SET state = 'archived' "
        "WHERE cell_id = 'cell-demo'",
    )
    for index, drift in enumerate(drifts):
        entry = trust_package(tmp_path / str(index))
        capture_trust_anchor(database, entry)
        original_id, _lost_id = relaunched_seat_shape(database, bindings)
        database.execute(drift)
        port = FakePort(live={REBOOTED_WORKSPACE})
        await restoring_reconciler(
            bindings, port, worker_alive=lambda _session: True
        ).reconcile()
        assert bindings.active_lead("cell-demo") is None
        assert bindings.get(original_id).state == "stale"
        assert (
            ChannelTrustAnchors(database, events=EventStore(database))
            .active_for_cell("cell-demo")
            .workspace_uuid
            == LEAD.workspace_uuid
        )
        database.execute("DELETE FROM project_cells")
        database.execute("DELETE FROM cmux_surface_bindings")
        database.execute("DELETE FROM channel_trust_anchors")


def test_restore_lead_refuses_a_racing_owner_or_rotation(
    database: Database, bindings: CmuxSurfaceBindings, tmp_path: Path
) -> None:
    """A change committed between selection and the restore write is
    re-proved under the write lock, so restoration can never produce two
    active owners or an obsolete one -- and the anchor never moves."""

    entry = trust_package(tmp_path)
    capture_trust_anchor(database, entry)
    original_id, _lost_id = relaunched_seat_shape(database, bindings)
    [(selected, anchor_id)] = bindings.restorable_relaunched_leads()
    assert selected.binding_id == original_id
    racing = bind_demo_lead(bindings, FRESH)
    with pytest.raises(CmuxBindingConflict):
        bindings.restore_lead(
            original_id,
            ref=REBOOTED_WORKSPACE,
            reason="relaunched",
            anchor_id=anchor_id,
        )
    bindings.mark_lost(str(racing.binding_id), reason="raced")
    database.execute(
        "UPDATE project_cells SET profile_alias = 'max-b' "
        "WHERE cell_id = 'cell-demo'"
    )
    with pytest.raises(CmuxBindingConflict):
        bindings.restore_lead(
            original_id,
            ref=REBOOTED_WORKSPACE,
            reason="relaunched",
            anchor_id=anchor_id,
        )
    assert bindings.get(original_id).state == "stale"
    anchor = ChannelTrustAnchors(
        database, events=EventStore(database)
    ).active_for_cell("cell-demo")
    assert anchor is not None
    assert anchor.workspace_uuid == LEAD.workspace_uuid


def test_restore_lead_refuses_when_the_selected_anchor_was_replaced(
    database: Database, bindings: CmuxSurfaceBindings, tmp_path: Path
) -> None:
    """The restore must consume the exact anchor selected with the stale seat."""

    entry = trust_package(tmp_path)
    anchors = ChannelTrustAnchors(database, events=EventStore(database))
    selected_anchor = capture_trust_anchor(database, entry)
    original_id, _lost_id = relaunched_seat_shape(database, bindings)
    anchors.rebind(
        cell_id="cell-demo",
        profile_alias="max-a",
        entry_path=entry,
        package_root=entry.parents[2],
        channel_entry="server:hermes-control",
        launch_argv_template=CHANNEL_ARGV,
        workspace_uuid=FRESH.workspace_uuid,
        surface_uuid=FRESH.surface_uuid,
        session_id=SESSION,
    )

    with pytest.raises(CmuxBindingConflict):
        bindings.restore_lead(
            original_id,
            ref=REBOOTED_WORKSPACE,
            reason="relaunched",
            anchor_id=selected_anchor.anchor_id,
        )

    assert bindings.get(original_id).state == "stale"
    active_anchor = anchors.active_for_cell("cell-demo")
    assert active_anchor is not None
    assert active_anchor.anchor_id != selected_anchor.anchor_id
    assert active_anchor.workspace_uuid == FRESH.workspace_uuid


def test_an_anchor_remint_rolls_back_with_its_caller(
    database: Database, tmp_path: Path
) -> None:
    """Sol 7f7fd46f: the restore path has to commit the anchor re-mint
    and the binding write TOGETHER, so the re-mint rides the caller's
    transaction. When that transaction rolls back the anchor must still
    name its original workspace — otherwise a failure after the re-mint
    leaves the anchor moved and the binding stale, which is exactly the
    disagreement this correction exists to prevent."""

    entry = trust_package(tmp_path)
    capture_trust_anchor(database, entry)
    anchors = ChannelTrustAnchors(database, events=EventStore(database))
    before = anchors.active_for_cell("cell-demo")
    assert before is not None

    with (
        pytest.raises(RuntimeError),
        database.transaction() as connection,
    ):
            anchors._rebind_in(
                connection,
                cell_id="cell-demo",
                profile_alias=before.profile_alias,
                entry_path=entry,
                package_root=entry.parents[2],
                channel_entry=before.channel_entry,
                launch_argv_template=before.launch_argv_template,
                workspace_uuid=REBOOTED_WORKSPACE.workspace_uuid,
                surface_uuid=before.surface_uuid,
                session_id=before.session_id,
            )
            raise RuntimeError("the caller's own write failed")

    after = anchors.active_for_cell("cell-demo")
    assert after is not None
    assert after.anchor_id == before.anchor_id
    assert after.workspace_uuid == before.workspace_uuid
