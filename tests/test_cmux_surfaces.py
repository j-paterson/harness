from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.cmux import (
    CmuxAccessDenied,
    CmuxError,
    CmuxProtocolError,
    CmuxSurfaceRef,
    CmuxUnavailable,
)
from hermes_orchestrator.cmux_surfaces import (
    SKIP_PERMISSIONS_FLAG,
    CmuxBindingConflict,
    CmuxHibernationGate,
    CmuxSurfaceBindings,
    CmuxSurfaceReconciler,
    HibernationDecision,
)
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

SESSION = "99999999-9999-4999-8999-999999999999"


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


def reconciler(
    bindings: CmuxSurfaceBindings,
    port: FakePort,
    *,
    environ: dict[str, str] | None = None,
    profiles: dict[str, Path] | None = None,
) -> CmuxSurfaceReconciler:
    return CmuxSurfaceReconciler(
        bindings=bindings,
        port=port,
        project_paths={"demo": Path("/repos/demo")},
        profile_dirs=FakeProfileDirs(
            profiles if profiles is not None else {"max-a": Path("/profiles/max-a")}
        ),
        environ=environ or {},
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

    report = await reconciler(bindings, port).reconcile()

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
    # The replacement workspace is created from durable identity alone:
    # recorded project cwd, the profile's exact CLAUDE_CONFIG_DIR, and the
    # sanitized native resume command for the exact Claude session. The
    # requested title carries the write-ahead intent's unique marker; the
    # visible title is restored once the identities are durably bound.
    [created] = port.created
    assert created["cwd"] == Path("/repos/demo")
    assert created["command"] is None
    assert created["env"] == {"CLAUDE_CONFIG_DIR": "/profiles/max-a"}
    assert str(created["title"]).startswith("demo lead")
    assert port.titles[FRESH.workspace_uuid] == "demo lead"
    assert port.resumes == [
        (FRESH, f"claude --resume {SESSION} {SKIP_PERMISSIONS_FLAG}")
    ]


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
    channel_launch: object | None = None,
    control: object | None = None,
) -> object:
    from hermes_orchestrator.cmux_surfaces import CmuxLeadSeater

    return CmuxLeadSeater(
        bindings=bindings,
        port=port,
        project_paths={"demo": Path("/repos/demo")},
        profile_dirs=FakeProfileDirs(
            profiles if profiles is not None else {"max-a": Path("/profiles/max-a")}
        ),
        channel_launch=channel_launch,
        control=control,
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

    report = await reconciler(bindings, port).reconcile()

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


class TestChannelLaunch:
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
