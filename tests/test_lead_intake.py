from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.cmux import INTAKE_ENVELOPE_PATTERN, CmuxSurfaceRef
from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.lead_intake import (
    CORRECTION_READY,
    WORK_READY,
    IntakeRefused,
    LeadIntakeTransport,
)

NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)
SESSION = "99999999-9999-4999-8999-999999999999"
OTHER_SESSION = "88888888-8888-4888-8888-888888888888"
LEAD = CmuxSurfaceRef(
    workspace_uuid="33333333-3333-4333-8333-333333333333",
    surface_uuid="33333333-3333-4333-8333-444444444444",
)
WAKE_ID = "a" * 32
CORRECTION_ID = "b" * 32


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def bindings(database: Database) -> CmuxSurfaceBindings:
    return CmuxSurfaceBindings(database=database, events=EventStore(database))


class RecordingPort:
    def __init__(self) -> None:
        self.envelopes: list[tuple[CmuxSurfaceRef, str]] = []
        self.alive = True
        self.focused: str | None = None
        self.probe_error: Exception | None = None

    async def surface_alive(self, ref: CmuxSurfaceRef) -> bool:
        if self.probe_error is not None:
            raise self.probe_error
        return self.alive

    async def focused_workspace_uuid(self) -> str | None:
        return self.focused

    async def deliver_intake_envelope(
        self, ref: CmuxSurfaceRef, envelope: str
    ) -> None:
        # The adapter-side grammar is the last line of defence; the
        # transport must never hand it anything else.
        assert INTAKE_ENVELOPE_PATTERN.fullmatch(envelope)
        self.envelopes.append((ref, envelope))


def seed_packets(database: Database) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO lead_terminal_wakes("
            "wake_id, schema_version, project_key, issue_id, cell_id, "
            "session_id, profile_alias, turn_key, kind, reason, state, "
            "created_at) VALUES (?, 1, 'demo', 'ENG-9', 'cell-demo', ?, "
            "'max-a', 'turn-1', 'completed', 'turn completed', "
            "'delivered', ?)",
            (WAKE_ID, SESSION, NOW.isoformat()),
        )
        connection.execute(
            "INSERT INTO lead_corrections("
            "correction_id, project_key, issue_id, source, repository, "
            "branch, pr_number, reviewed_sha, packets_json, state, "
            "created_at) VALUES (?, 'demo', 'ENG-9', 'codex_review', "
            "'owner/demo', 'feature/x', 7, 'abc', '[]', 'pending', ?)",
            (CORRECTION_ID, NOW.isoformat()),
        )


def seat(bindings: CmuxSurfaceBindings, *, classic: bool = True) -> object:
    binding = bindings.bind_lead(
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        profile_alias="max-a",
        ref=LEAD,
    )
    if classic:
        bindings.record_classic(binding.binding_id, SESSION)
    return binding


class FakeBoundary:
    """Mutable lead-owned prompt-boundary evidence."""

    def __init__(self, recorded_at: str | None = NOW.isoformat()) -> None:
        self.recorded_at = recorded_at

    def current(self, cell_id: str, session_id: str) -> object | None:
        if self.recorded_at is None:
            return None
        evidence = type("Evidence", (), {})()
        evidence.recorded_at = self.recorded_at
        return evidence


def transport(
    database: Database,
    bindings: CmuxSurfaceBindings,
    port: RecordingPort,
    *,
    safety: FakeBoundary | None = None,
) -> LeadIntakeTransport:
    return LeadIntakeTransport(
        database=database,
        bindings=bindings,
        port=port,
        safety=safety if safety is not None else FakeBoundary(),
    )


@pytest.mark.asyncio
async def test_delivers_exactly_one_envelope_to_the_exact_surface(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    seat(bindings)
    port = RecordingPort()

    delivery = await transport(database, bindings, port).deliver(
        kind=WORK_READY,
        packet_id=WAKE_ID,
        cell_id="cell-demo",
        session_id=SESSION,
    )

    assert delivery.status == "delivered"
    assert port.envelopes == [(LEAD, f"HERMES_WORK_READY {WAKE_ID}")]
    assert delivery.surface_uuid == LEAD.surface_uuid


@pytest.mark.asyncio
async def test_repeated_delivery_is_deduplicated_durably(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    seat(bindings)
    port = RecordingPort()
    lane = transport(database, bindings, port)

    first = await lane.deliver(
        kind=CORRECTION_READY,
        packet_id=CORRECTION_ID,
        cell_id="cell-demo",
        session_id=SESSION,
    )
    second = await lane.deliver(
        kind=CORRECTION_READY,
        packet_id=CORRECTION_ID,
        cell_id="cell-demo",
        session_id=SESSION,
    )

    assert first.status == "delivered"
    assert second.status == "deduplicated"
    assert len(port.envelopes) == 1


@pytest.mark.asyncio
async def test_unknown_packets_and_kinds_are_refused(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    seat(bindings)
    port = RecordingPort()
    lane = transport(database, bindings, port)

    with pytest.raises(IntakeRefused, match="only HERMES"):
        await lane.deliver(
            kind="HERMES_RUN_ANYTHING",
            packet_id=WAKE_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )
    with pytest.raises(IntakeRefused, match="32-hex"):
        await lane.deliver(
            kind=WORK_READY,
            packet_id="not-a-packet",
            cell_id="cell-demo",
            session_id=SESSION,
        )
    with pytest.raises(IntakeRefused, match="no durable packet"):
        await lane.deliver(
            kind=WORK_READY,
            packet_id="c" * 32,
            cell_id="cell-demo",
            session_id=SESSION,
        )
    # A correction id is not deliverable as a work envelope: the id
    # must exist in the exact durable table its kind names.
    with pytest.raises(IntakeRefused, match="no durable packet"):
        await lane.deliver(
            kind=WORK_READY,
            packet_id=CORRECTION_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )
    assert port.envelopes == []


@pytest.mark.asyncio
async def test_stale_mismatched_and_nonclassic_bindings_are_refused(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    port = RecordingPort()
    lane = transport(database, bindings, port)

    # No binding at all.
    with pytest.raises(IntakeRefused, match="no active seat"):
        await lane.deliver(
            kind=WORK_READY,
            packet_id=WAKE_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )

    binding = seat(bindings, classic=False)
    # Session mismatch: the seat rotated away from the requested lead.
    with pytest.raises(IntakeRefused, match="different session"):
        await lane.deliver(
            kind=WORK_READY,
            packet_id=WAKE_ID,
            cell_id="cell-demo",
            session_id=OTHER_SESSION,
        )
    # No classic evidence: nothing may ever be typed into it.
    with pytest.raises(IntakeRefused, match="non-classic"):
        await lane.deliver(
            kind=WORK_READY,
            packet_id=WAKE_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )
    bindings.record_classic(binding.binding_id, SESSION)
    # A dead surface is a stale binding.
    port.alive = False
    with pytest.raises(IntakeRefused, match="no longer live"):
        await lane.deliver(
            kind=WORK_READY,
            packet_id=WAKE_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )
    assert port.envelopes == []


def seed_active_cell(database: Database) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at) VALUES "
            "('cell-demo', 'demo', 'active', 'max-a', ?, ?, ?)",
            (SESSION, NOW.isoformat(), NOW.isoformat()),
        )


def delivery_rows(database: Database) -> list[tuple[str, str, int]]:
    rows = database.execute(
        "SELECT kind, state, attempts FROM lead_intake_deliveries "
        "ORDER BY rowid"
    ).fetchall()
    return [
        (str(r["kind"]), str(r["state"]), int(r["attempts"])) for r in rows
    ]


@pytest.mark.asyncio
async def test_racing_deliveries_produce_one_external_sequence(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    import asyncio

    seed_packets(database)
    seat(bindings)

    class HoldingPort(RecordingPort):
        def __init__(self) -> None:
            super().__init__()
            self.gate = asyncio.Event()

        async def deliver_intake_envelope(self, ref, envelope):  # type: ignore[no-untyped-def]
            await self.gate.wait()
            await super().deliver_intake_envelope(ref, envelope)

    port = HoldingPort()
    lane = transport(database, bindings, port)
    kwargs = dict(
        kind=WORK_READY,
        packet_id=WAKE_ID,
        cell_id="cell-demo",
        session_id=SESSION,
    )

    first = asyncio.ensure_future(lane.deliver(**kwargs))
    await asyncio.sleep(0)
    # The second caller finds the fresh claim and types nothing.
    second = await lane.deliver(**kwargs)
    port.gate.set()
    first_result = await first

    assert second.status == "pending"
    assert first_result.status == "delivered"
    assert len(port.envelopes) == 1


@pytest.mark.asyncio
async def test_crash_after_claim_is_retried_after_the_window(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    from datetime import timedelta

    seed_packets(database)
    seat(bindings)

    class Died(BaseException):
        pass

    class CrashingPort(RecordingPort):
        async def deliver_intake_envelope(self, ref, envelope):  # type: ignore[no-untyped-def]
            raise Died("process died before typing")

    crashing = LeadIntakeTransport(
        database=database,
        bindings=bindings,
        port=CrashingPort(),
        safety=FakeBoundary(),
        now=lambda: NOW,
    )
    with pytest.raises(Died):
        await crashing.deliver(
            kind=WORK_READY,
            packet_id=WAKE_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )
    assert delivery_rows(database) == [(WORK_READY, "attempted", 1)]

    # Restart: a new transport takes over the stale attempt once the
    # retry window has passed and completes exactly one delivery.
    port = RecordingPort()
    later = LeadIntakeTransport(
        database=database,
        bindings=bindings,
        port=port,
        # A lead-owned boundary recorded after the uncertain attempt is
        # what re-arms recovery; the wall clock alone never does.
        safety=FakeBoundary(
            (NOW + timedelta(seconds=60)).isoformat()
        ),
        now=lambda: NOW + timedelta(seconds=120),
    )
    result = await later.deliver(
        kind=WORK_READY,
        packet_id=WAKE_ID,
        cell_id="cell-demo",
        session_id=SESSION,
    )

    assert result.status == "delivered"
    assert len(port.envelopes) == 1
    assert delivery_rows(database) == [(WORK_READY, "delivered", 2)]


@pytest.mark.asyncio
async def test_failed_attempt_stays_durable_and_distinguishable(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    from datetime import timedelta

    from hermes_orchestrator.cmux import CmuxUnavailable

    seed_packets(database)
    seat(bindings)

    class FailingPort(RecordingPort):
        async def deliver_intake_envelope(self, ref, envelope):  # type: ignore[no-untyped-def]
            raise CmuxUnavailable("cmux command timed out")

    failing = LeadIntakeTransport(
        database=database,
        bindings=bindings,
        port=FailingPort(),
        safety=FakeBoundary(),
        now=lambda: NOW,
    )
    outcome = await failing.deliver(
        kind=WORK_READY,
        packet_id=WAKE_ID,
        cell_id="cell-demo",
        session_id=SESSION,
    )

    # The uncertain external effect is durably an 'attempted' row —
    # never recorded as delivered, never lost.
    assert outcome.status == "attempt_failed"
    assert delivery_rows(database) == [(WORK_READY, "attempted", 1)]

    # A fresh retry inside the window is refused (the owner may still
    # be live); after the window it recovers to exactly one delivery.
    within = await failing.deliver(
        kind=WORK_READY,
        packet_id=WAKE_ID,
        cell_id="cell-demo",
        session_id=SESSION,
    )
    assert within.status == "pending"

    port = RecordingPort()
    later = LeadIntakeTransport(
        database=database,
        bindings=bindings,
        port=port,
        # A lead-owned boundary recorded after the uncertain attempt is
        # what re-arms recovery; the wall clock alone never does.
        safety=FakeBoundary(
            (NOW + timedelta(seconds=60)).isoformat()
        ),
        now=lambda: NOW + timedelta(seconds=120),
    )
    result = await later.deliver(
        kind=WORK_READY,
        packet_id=WAKE_ID,
        cell_id="cell-demo",
        session_id=SESSION,
    )
    assert result.status == "delivered"
    assert delivery_rows(database) == [(WORK_READY, "delivered", 2)]


@pytest.mark.asyncio
async def test_crash_after_return_recovers_without_losing_the_packet(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    from datetime import timedelta

    seed_packets(database)
    seat(bindings)
    port = RecordingPort()
    lane = transport(database, bindings, port)
    kwargs = dict(
        kind=WORK_READY,
        packet_id=WAKE_ID,
        cell_id="cell-demo",
        session_id=SESSION,
    )
    await lane.deliver(**kwargs)
    # Simulate the crash window between the acknowledged Return and the
    # delivered record: the row is still 'attempted' on restart.
    with database.transaction() as connection:
        connection.execute(
            "UPDATE lead_intake_deliveries SET state = 'attempted', "
            "delivered_at = NULL, updated_at = ?",
            (NOW.isoformat(),),
        )

    later = LeadIntakeTransport(
        database=database,
        bindings=bindings,
        port=port,
        # A lead-owned boundary recorded after the uncertain attempt is
        # what re-arms recovery; the wall clock alone never does.
        safety=FakeBoundary(
            (NOW + timedelta(seconds=60)).isoformat()
        ),
        now=lambda: NOW + timedelta(seconds=120),
    )
    result = await later.deliver(**kwargs)

    # Recovery may repeat the envelope (the id-based packet fetch is
    # idempotent on the lead side) but processing is never lost and
    # never duplicated durably: one row, delivered.
    assert result.status == "delivered"
    rows = delivery_rows(database)
    assert len(rows) == 1
    assert rows[0][1] == "delivered"
    assert len(port.envelopes) == 2


@pytest.mark.asyncio
async def test_router_routes_each_pending_packet_exactly_once(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    from hermes_orchestrator.lead_intake import LeadIntakeRouter

    seed_packets(database)
    seed_active_cell(database)
    seat(bindings)
    port = RecordingPort()
    router = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )

    first = await router.tick()
    second = await router.tick()

    # One CORRECTION envelope for the pending correction, one WORK
    # envelope for the wake; the second pass types nothing.
    assert sorted(first) == sorted([CORRECTION_ID, WAKE_ID])
    assert second == ()
    envelopes = sorted(text for _, text in port.envelopes)
    assert envelopes == [
        f"HERMES_CORRECTION_READY {CORRECTION_ID}",
        f"HERMES_WORK_READY {WAKE_ID}",
    ]


@pytest.mark.asyncio
async def test_restart_between_publication_and_delivery_still_delivers(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    from hermes_orchestrator.lead_intake import LeadIntakeRouter

    seed_packets(database)
    seed_active_cell(database)
    seat(bindings)
    # The publication happened; the process crashed before any routing
    # pass ran. A freshly constructed router derives the pending work
    # from durable state alone.
    port = RecordingPort()
    restarted = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )

    delivered = await restarted.tick()

    assert sorted(delivered) == sorted([CORRECTION_ID, WAKE_ID])
    assert len(port.envelopes) == 2


@pytest.mark.asyncio
async def test_refused_seats_retain_pending_packets_without_typing(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    from hermes_orchestrator.lead_intake import LeadIntakeRouter

    seed_packets(database)
    seed_active_cell(database)
    port = RecordingPort()
    router = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )

    # No seat at all: nothing typed, everything stays pending.
    assert await router.tick() == ()
    assert port.envelopes == []
    assert delivery_rows(database) == []

    # A non-classic seat is refused the same way.
    binding = seat(bindings, classic=False)
    assert await router.tick() == ()
    assert port.envelopes == []

    # Once the seat carries classic evidence, the retained packets
    # deliver.
    bindings.record_classic(binding.binding_id, SESSION)
    delivered = await router.tick()
    assert sorted(delivered) == sorted([CORRECTION_ID, WAKE_ID])


@pytest.mark.asyncio
async def test_superseded_backfill_rows_are_never_typed(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    from hermes_orchestrator.lead_intake import LeadIntakeRouter

    seed_packets(database)
    seed_active_cell(database)
    seat(bindings)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO lead_intake_deliveries("
            "delivery_id, kind, packet_id, cell_id, session_id, "
            "surface_uuid, state, attempts, claimed_at, updated_at) "
            "VALUES ('backfill', 'HERMES_WORK_READY', ?, 'cell-demo', ?, "
            "'', 'superseded', 0, ?, ?)",
            (WAKE_ID, SESSION, NOW.isoformat(), NOW.isoformat()),
        )
    port = RecordingPort()
    router = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )

    delivered = await router.tick()

    # Only the correction routes; the superseded wake is terminal.
    assert delivered == (CORRECTION_ID,)
    assert [text for _, text in port.envelopes] == [
        f"HERMES_CORRECTION_READY {CORRECTION_ID}"
    ]


@pytest.mark.asyncio
async def test_operator_owned_prompt_is_never_touched(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    # The operator is focused in the lead workspace with an unsent
    # draft in its prompt. Keyboard input reaches only the focused
    # surface, so focus IS the ownership signal: delivery stays
    # pending, no keystroke of any kind is emitted, and the draft is
    # untouched byte for byte.
    seed_packets(database)
    seat(bindings)
    port = RecordingPort()
    port.focused = LEAD.workspace_uuid

    with pytest.raises(IntakeRefused, match="safe boundary"):
        await transport(database, bindings, port).deliver(
            kind=WORK_READY,
            packet_id=WAKE_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )

    assert port.envelopes == []
    assert delivery_rows(database) == []


@pytest.mark.asyncio
async def test_unproven_prompt_boundary_holds_delivery_pending(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    seat(bindings)
    port = RecordingPort()

    with pytest.raises(IntakeRefused, match="prompt boundary"):
        await transport(
            database, bindings, port, safety=FakeBoundary(None)
        ).deliver(
            kind=WORK_READY,
            packet_id=WAKE_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )

    assert port.envelopes == []
    assert delivery_rows(database) == []


@pytest.mark.asyncio
async def test_uncertain_attempt_retry_waits_for_a_fresh_boundary(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    from datetime import timedelta

    from hermes_orchestrator.cmux import CmuxUnavailable

    seed_packets(database)
    seat(bindings)

    class FailingPort(RecordingPort):
        async def deliver_intake_envelope(self, ref, envelope):  # type: ignore[no-untyped-def]
            raise CmuxUnavailable("cmux command timed out")

    failing = LeadIntakeTransport(
        database=database,
        bindings=bindings,
        port=FailingPort(),
        safety=FakeBoundary(),
        now=lambda: NOW,
    )
    outcome = await failing.deliver(
        kind=WORK_READY,
        packet_id=WAKE_ID,
        cell_id="cell-demo",
        session_id=SESSION,
    )
    assert outcome.status == "attempt_failed"

    # Even far past any wall-clock window, without a boundary recorded
    # AFTER the attempt no keystroke is emitted: the partial line may
    # coexist with operator input and only the lead's own turn cycle
    # proves it was consumed.
    port = RecordingPort()
    stale_boundary = LeadIntakeTransport(
        database=database,
        bindings=bindings,
        port=port,
        safety=FakeBoundary(NOW.isoformat()),
        now=lambda: NOW + timedelta(seconds=3600),
    )
    held = await stale_boundary.deliver(
        kind=WORK_READY,
        packet_id=WAKE_ID,
        cell_id="cell-demo",
        session_id=SESSION,
    )
    assert held.status == "pending"
    assert port.envelopes == []
    assert delivery_rows(database) == [(WORK_READY, "attempted", 1)]


@pytest.mark.asyncio
async def test_probe_failures_are_contained_and_recovery_delivers_once(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    from hermes_orchestrator.cmux import CmuxUnavailable
    from hermes_orchestrator.lead_intake import LeadIntakeRouter

    seed_packets(database)
    seed_active_cell(database)
    seat(bindings)
    port = RecordingPort()
    port.probe_error = CmuxUnavailable("cmux command timed out")
    router = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )

    # A liveness-probe failure never escapes the tick, types nothing,
    # and loses nothing.
    assert await router.tick() == ()
    assert port.envelopes == []
    assert delivery_rows(database) == []

    # cmux recovers: the retained packets deliver exactly once.
    port.probe_error = None
    delivered = await router.tick()
    assert sorted(delivered) == sorted([CORRECTION_ID, WAKE_ID])
    assert await router.tick() == ()
    assert len(port.envelopes) == 2
