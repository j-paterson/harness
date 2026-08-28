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

    async def surface_alive(self, ref: CmuxSurfaceRef) -> bool:
        return self.alive

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


def transport(
    database: Database, bindings: CmuxSurfaceBindings, port: RecordingPort
) -> LeadIntakeTransport:
    return LeadIntakeTransport(
        database=database, bindings=bindings, port=port
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
