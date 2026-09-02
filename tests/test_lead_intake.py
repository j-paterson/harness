from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hermes_orchestrator.cmux import CmuxSurfaceRef, CmuxUnavailable
from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
from hermes_orchestrator.control_operations import ControlOperations
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.lead_intake import (
    INTAKE_ENVELOPE_PATTERN,
    WORK_READY,
    IntakeRefused,
    LeadIntakePoll,
    LeadIntakeRouter,
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
ASSIGNMENT_ID = "c" * 32
OPERATION_ID = "d" * 32


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
    """Records the bounded signal plus the supplemental metadata."""

    def __init__(self) -> None:
        self.signals: list[tuple[CmuxSurfaceRef, str]] = []
        self.statuses: list[tuple[str, str, str]] = []
        self.notifications: list[tuple[str, str, str]] = []
        self.alive = True
        self.probe_error: Exception | None = None
        self.signal_error: Exception | None = None

    async def surface_alive(self, ref: CmuxSurfaceRef) -> bool:
        if self.probe_error is not None:
            raise self.probe_error
        return self.alive

    async def deliver_intake_envelope(
        self, ref: CmuxSurfaceRef, envelope: str
    ) -> None:
        from hermes_orchestrator.cmux import INTAKE_SIGNAL_PATTERN

        if self.signal_error is not None:
            raise self.signal_error
        # The adapter-side grammar is the last line of defence; the
        # transport must never hand it anything else.
        assert INTAKE_SIGNAL_PATTERN.fullmatch(envelope)
        self.signals.append((ref, envelope))

    async def set_status(
        self, workspace_uuid: str, key: str, value: str
    ) -> None:
        self.statuses.append((workspace_uuid, key, value))

    async def notify(
        self, workspace_uuid: str, title: str, body: str
    ) -> None:
        self.notifications.append((workspace_uuid, title, body))


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


def seed_active_cell(database: Database) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at) VALUES "
            "('cell-demo', 'demo', 'active', 'max-a', ?, ?, ?)",
            (SESSION, NOW.isoformat(), NOW.isoformat()),
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
    database: Database,
    bindings: CmuxSurfaceBindings,
    port: RecordingPort,
    **kwargs: object,
) -> LeadIntakeTransport:
    return LeadIntakeTransport(
        database=database, bindings=bindings, port=port, **kwargs
    )


def delivery_rows(database: Database) -> list[tuple[str, str, int]]:
    rows = database.execute(
        "SELECT kind, state, attempts FROM lead_intake_deliveries "
        "ORDER BY rowid"
    ).fetchall()
    return [
        (str(r["kind"]), str(r["state"]), int(r["attempts"])) for r in rows
    ]


DELIVER_KWARGS = dict(
    kind=WORK_READY,
    packet_id=WAKE_ID,
    cell_id="cell-demo",
    session_id=SESSION,
)


@pytest.mark.asyncio
async def test_delivery_signals_the_surface_with_supplemental_metadata(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    seat(bindings)
    port = RecordingPort()

    outcome = await transport(database, bindings, port).deliver(
        **DELIVER_KWARGS
    )

    assert outcome.status == "announced"
    envelope = f"HERMES_WORK_READY {WAKE_ID}"
    # The automatic path announces with metadata only and never types.
    assert port.signals == []
    assert port.statuses == [(LEAD.workspace_uuid, "intake", envelope)]
    assert port.notifications == [
        (LEAD.workspace_uuid, "Hermes intake pending", envelope)
    ]
    assert delivery_rows(database) == [(WORK_READY, "announced", 1)]


@pytest.mark.asyncio
async def test_router_performs_zero_interactive_sends(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    # Sol's staged-draft proofs, by construction: whatever the target
    # prompt buffer holds — a focused draft, an unfocused draft, or
    # nothing — a router tick performs zero interactive sends, so any
    # draft is preserved byte-for-byte. Only metadata announcements
    # happen, one per packet.
    seed_packets(database)
    seed_active_cell(database)
    seat(bindings)
    port = RecordingPort()
    router = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )

    announced = await router.tick()

    assert sorted(announced) == sorted([CORRECTION_ID, WAKE_ID])
    assert port.signals == []
    assert len(port.statuses) == 2
    assert len(port.notifications) == 2


@pytest.mark.asyncio
async def test_failed_metadata_announcement_stays_uncertain(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    seat(bindings)

    class FailingNotify(RecordingPort):
        async def notify(self, workspace_uuid, title, body):  # type: ignore[no-untyped-def]
            raise CmuxUnavailable("cmux command timed out")

    outcome = await transport(
        database, bindings, FailingNotify()
    ).deliver(**DELIVER_KWARGS)

    assert outcome.status == "attempt_failed"
    assert delivery_rows(database) == [(WORK_READY, "attempted", 1)]


@pytest.mark.asyncio
async def test_post_stop_race_wakes_through_polling_not_injection(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    # The live acceptance race: the lead's final scan is empty, the
    # lead goes idle, and only then does a correction commit. The
    # automatic wake is announcement plus the lead's own poll — never
    # prompt injection — and restart never duplicates effective intake.
    seed_active_cell(database)
    seat(bindings)
    port = RecordingPort()
    router = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )
    assert await router.tick() == ()

    seed_packets(database)
    announced = await router.tick()
    assert sorted(announced) == sorted([CORRECTION_ID, WAKE_ID])
    assert port.signals == []

    # The lead drains through the offer/ack poll at its next boundary.
    poll = LeadIntakePoll(database=database)
    drained = []
    while (offer := poll.next_offer(SESSION)) is not None:
        assert poll.acknowledge(
            session_id=SESSION,
            packet_id=offer.packet_id,
            offer_token=offer.offer_token,
        )
        drained.append(offer.packet_id)
    assert sorted(drained) == sorted([CORRECTION_ID, WAKE_ID])

    # Restart: a fresh router derives everything from durable state,
    # re-announces nothing, and still types nothing.
    restarted = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )
    assert await restarted.tick() == ()
    assert port.signals == []


def seed_assignment(database: Database) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO lead_assignments("
            "assignment_id, schema_version, project_key, issue_id, "
            "cell_id, session_id, profile_alias, instruction_id, "
            "queue_transition, state, created_at, updated_at, "
            "acknowledged_at) VALUES (?, 1, 'demo', 'ENG-9', "
            "'cell-demo', ?, 'max-b', 'chat-ENG-9', "
            "'queued->in_development', 'published', ?, ?, NULL)",
            (ASSIGNMENT_ID, SESSION, NOW.isoformat(), NOW.isoformat()),
        )


def test_the_poll_offers_a_published_assignment_and_ack_settles_it(
    database: Database,
) -> None:
    """INFRA-195: the fallback drain offers the assignment envelope,
    and the lead's exact acknowledgement settles both the delivery row
    and the assignment ledger in one transaction."""

    seed_active_cell(database)
    seed_assignment(database)
    poll = LeadIntakePoll(database=database)

    offer = poll.next_offer(SESSION)

    assert offer is not None
    assert offer.kind == "HERMES_ASSIGNMENT_READY"
    assert offer.envelope == f"HERMES_ASSIGNMENT_READY {ASSIGNMENT_ID}"
    assert poll.acknowledge(
        session_id=SESSION,
        packet_id=offer.packet_id,
        offer_token=offer.offer_token,
    )
    state = database.scalar(
        "SELECT state FROM lead_assignments WHERE assignment_id = ?",
        (ASSIGNMENT_ID,),
    )
    assert str(state) == "acknowledged"
    # Settled means gone: nothing further is offered.
    assert poll.next_offer(SESSION) is None


def test_the_poll_offers_a_control_operation_and_ack_settles_it(
    database: Database,
) -> None:
    seed_active_cell(database)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO control_operations("
            "operation_id, schema_version, kind, project_key, cell_id, "
            "session_id, dedup_key, result_json, reason, state, "
            "created_at, updated_at, acknowledged_at) VALUES "
            "(?, 1, 'children.completed', 'demo', 'cell-demo', ?, "
            "'children.completed:' || ?, '{\"issue_id\": \"ENG-9\"}', NULL, "
            "'published', ?, ?, NULL)",
            (OPERATION_ID, SESSION, SESSION, NOW.isoformat(), NOW.isoformat()),
        )
    poll = LeadIntakePoll(database=database)

    offer = poll.next_offer(SESSION)

    assert offer is not None
    assert offer.envelope == f"HERMES_CONTROL_READY {OPERATION_ID}"
    assert poll.acknowledge(
        session_id=SESSION,
        packet_id=offer.packet_id,
        offer_token=offer.offer_token,
    )
    state = database.scalar(
        "SELECT state FROM control_operations WHERE operation_id = ?",
        (OPERATION_ID,),
    )
    assert str(state) == "acknowledged"
    assert poll.next_offer(SESSION) is None


def test_the_poll_never_offers_a_maintenance_receipt(
    database: Database,
) -> None:
    """INFRA-201: a SILENT maintenance receipt is never offered through
    the poll. INFRA-219 (Sol correction 14bd0c17) narrowed that set --
    the runtime-lifecycle kinds ARE offered now -- so this uses a kind
    that is still genuinely silent churn.

    Original wording: a maintenance receipt is never offered through the
    Stop-hook poll; it is settled silently by
    ``settle_maintenance_for_session`` instead."""

    seed_active_cell(database)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO control_operations("
            "operation_id, schema_version, kind, project_key, cell_id, "
            "session_id, dedup_key, result_json, reason, state, "
            "created_at, updated_at, acknowledged_at) VALUES "
            "(?, 1, 'intake.dedup_repaired', 'demo', 'cell-demo', ?, "
            "'intake.dedup_repaired:' || ?, '{\"replay_count\": 0}', NULL, "
            "'published', ?, ?, NULL)",
            (OPERATION_ID, SESSION, SESSION, NOW.isoformat(), NOW.isoformat()),
        )
    poll = LeadIntakePoll(database=database)

    assert poll.next_offer(SESSION) is None

    operations = ControlOperations(database, events=EventStore(database))
    settled = operations.settle_maintenance_for_session(SESSION)

    assert settled == (OPERATION_ID,)
    state = database.scalar(
        "SELECT state FROM control_operations WHERE operation_id = ?",
        (OPERATION_ID,),
    )
    assert str(state) == "acknowledged"


@pytest.mark.asyncio
async def test_the_router_announces_a_published_assignment_once(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_active_cell(database)
    seed_assignment(database)
    seat(bindings)
    port = RecordingPort()
    router = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )

    announced = await router.tick()

    assert ASSIGNMENT_ID in announced
    assert ASSIGNMENT_ID not in await router.tick()


@pytest.mark.asyncio
async def test_the_router_never_announces_a_maintenance_receipt(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    """INFRA-201: a maintenance receipt is never announced on the
    seat while an actionable one is."""

    seed_active_cell(database)
    seat(bindings)
    maintenance_id = "e" * 32
    actionable_id = "f" * 32
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO control_operations("
            "operation_id, schema_version, kind, project_key, cell_id, "
            "session_id, dedup_key, result_json, reason, state, "
            "created_at, updated_at, acknowledged_at) VALUES "
            "(?, 1, 'intake.dedup_repaired', 'demo', 'cell-demo', ?, "
            "'intake.dedup_repaired:' || ?, '{\"interval_seconds\": 30}', "
            "NULL, 'published', ?, ?, NULL)",
            (
                maintenance_id,
                SESSION,
                SESSION,
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
        connection.execute(
            "INSERT INTO control_operations("
            "operation_id, schema_version, kind, project_key, cell_id, "
            "session_id, dedup_key, result_json, reason, state, "
            "created_at, updated_at, acknowledged_at) VALUES "
            "(?, 1, 'children.completed', 'demo', 'cell-demo', ?, "
            "'children.completed:' || ?, '{\"issue_id\": \"ENG-9\"}', "
            "NULL, 'published', ?, ?, NULL)",
            (
                actionable_id,
                SESSION,
                SESSION,
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
    port = RecordingPort()
    router = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )

    announced = await router.tick()

    assert actionable_id in announced
    assert maintenance_id not in announced


@pytest.mark.asyncio
async def test_repeated_announcement_is_deduplicated_durably(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    seat(bindings)
    port = RecordingPort()
    lane = transport(database, bindings, port)

    first = await lane.deliver(**DELIVER_KWARGS)
    second = await lane.deliver(**DELIVER_KWARGS)

    assert first.status == "announced"
    assert second.status == "deduplicated"
    assert len(port.notifications) == 1


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
    # A correction id is not announceable as a work envelope: the id
    # must exist in the exact durable table its kind names.
    with pytest.raises(IntakeRefused, match="no durable packet"):
        await lane.deliver(
            kind=WORK_READY,
            packet_id=CORRECTION_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )
    assert port.notifications == []


@pytest.mark.asyncio
async def test_stale_mismatched_and_nonclassic_bindings_are_refused(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    port = RecordingPort()
    lane = transport(database, bindings, port)

    with pytest.raises(IntakeRefused, match="no active seat"):
        await lane.deliver(**DELIVER_KWARGS)

    binding = seat(bindings, classic=False)
    with pytest.raises(IntakeRefused, match="different session"):
        await lane.deliver(
            kind=WORK_READY,
            packet_id=WAKE_ID,
            cell_id="cell-demo",
            session_id=OTHER_SESSION,
        )
    with pytest.raises(IntakeRefused, match="non-classic"):
        await lane.deliver(**DELIVER_KWARGS)
    bindings.record_classic(binding.binding_id, SESSION)
    port.alive = False
    with pytest.raises(IntakeRefused, match="no longer live"):
        await lane.deliver(**DELIVER_KWARGS)
    assert port.notifications == []
    assert port.statuses == []


@pytest.mark.asyncio
async def test_racing_announcements_produce_one_external_effect(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    import asyncio

    seed_packets(database)
    seat(bindings)

    class HoldingPort(RecordingPort):
        def __init__(self) -> None:
            super().__init__()
            self.gate = asyncio.Event()

        async def set_status(self, workspace_uuid, key, value):  # type: ignore[no-untyped-def]
            await self.gate.wait()
            await super().set_status(workspace_uuid, key, value)

    port = HoldingPort()
    lane = transport(database, bindings, port)

    first = asyncio.ensure_future(lane.deliver(**DELIVER_KWARGS))
    await asyncio.sleep(0)
    second = await lane.deliver(**DELIVER_KWARGS)
    port.gate.set()
    first_result = await first

    assert second.status == "pending"
    assert first_result.status == "announced"
    assert len(port.notifications) == 1


@pytest.mark.asyncio
async def test_uncertain_attempt_stays_durable_and_retries_after_window(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    seat(bindings)

    class FailingNotifyPort(RecordingPort):
        async def notify(self, workspace_uuid, title, body):  # type: ignore[no-untyped-def]
            raise CmuxUnavailable("cmux command timed out")

    failing = transport(
        database, bindings, FailingNotifyPort(), now=lambda: NOW
    )
    outcome = await failing.deliver(**DELIVER_KWARGS)
    assert outcome.status == "attempt_failed"
    assert delivery_rows(database) == [(WORK_READY, "attempted", 1)]

    # Inside the retry window another announcer does nothing external.
    held = await failing.deliver(**DELIVER_KWARGS)
    assert held.status == "pending"

    # After the window the retry repeats the harmless metadata and
    # completes.
    port = RecordingPort()
    later = transport(
        database,
        bindings,
        port,
        now=lambda: NOW + timedelta(seconds=120),
    )
    result = await later.deliver(**DELIVER_KWARGS)
    assert result.status == "announced"
    assert delivery_rows(database) == [(WORK_READY, "announced", 2)]


@pytest.mark.asyncio
async def test_probe_failures_are_contained_and_recovery_announces_once(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    seed_active_cell(database)
    seat(bindings)
    port = RecordingPort()
    port.probe_error = CmuxUnavailable("cmux command timed out")
    router = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )

    assert await router.tick() == ()
    assert port.notifications == []
    assert delivery_rows(database) == []

    port.probe_error = None
    announced = await router.tick()
    assert sorted(announced) == sorted([CORRECTION_ID, WAKE_ID])
    assert await router.tick() == ()
    assert len(port.notifications) == 2


@pytest.mark.asyncio
async def test_restart_between_publication_and_announcement_recovers(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    seed_active_cell(database)
    seat(bindings)
    port = RecordingPort()
    restarted = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )

    announced = await restarted.tick()

    assert sorted(announced) == sorted([CORRECTION_ID, WAKE_ID])


@pytest.mark.asyncio
async def test_refused_seats_retain_pending_packets(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    seed_active_cell(database)
    port = RecordingPort()
    router = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )

    assert await router.tick() == ()
    assert port.notifications == []

    binding = seat(bindings, classic=False)
    assert await router.tick() == ()

    bindings.record_classic(binding.binding_id, SESSION)
    announced = await router.tick()
    assert sorted(announced) == sorted([CORRECTION_ID, WAKE_ID])


class TestOfferAcknowledgementLease:
    def poll(
        self, database: Database, **kwargs: object
    ) -> LeadIntakePoll:
        return LeadIntakePoll(database=database, **kwargs)

    def test_only_one_poll_owns_an_offer_at_a_time(
        self, database: Database
    ) -> None:
        seed_packets(database)
        seed_active_cell(database)
        poll = self.poll(database)

        first = poll.next_offer(SESSION)
        second = poll.next_offer(SESSION)
        third = poll.next_offer(SESSION)

        # Two packets exist, so two racing polls each lease a distinct
        # one; a third finds every offer owned and gets nothing. No
        # packet is ever offered to two owners inside a lease.
        assert first is not None and second is not None
        assert first.packet_id != second.packet_id
        assert INTAKE_ENVELOPE_PATTERN.fullmatch(first.envelope)
        assert third is None
        states = [row[1] for row in delivery_rows(database)]
        assert states == ["offered", "offered"]

    def test_offer_without_acknowledgement_is_never_delivered(
        self, database: Database
    ) -> None:
        # A crash after the offer transaction, a broken stdout pipe, or
        # a hook-host rejection all look identical: the offer was
        # leased but never acknowledged. Nothing is delivered.
        seed_packets(database)
        seed_active_cell(database)
        poll = self.poll(database, now=lambda: NOW)

        offer = poll.next_offer(SESSION)

        assert offer is not None
        assert "delivered" not in [
            row[1] for row in delivery_rows(database)
        ]

    def test_expired_offer_is_recovered_under_a_fresh_token(
        self, database: Database
    ) -> None:
        seed_packets(database)
        seed_active_cell(database)
        early = self.poll(database, now=lambda: NOW)
        lost = early.next_offer(SESSION)
        assert lost is not None

        # Within the lease the packet stays owned.
        held = self.poll(database, now=lambda: NOW).next_offer(SESSION)
        assert held is None or held.packet_id != lost.packet_id

        # After expiry (a restart later), the packet is re-offered
        # under a fresh token; the lost token is dead.
        later = self.poll(
            database, now=lambda: NOW + timedelta(seconds=600)
        )
        recovered = later.next_offer(SESSION)
        assert recovered is not None
        assert recovered.packet_id in (lost.packet_id, held and held.packet_id)
        assert recovered.offer_token != lost.offer_token
        assert (
            later.acknowledge(
                session_id=SESSION,
                packet_id=lost.packet_id,
                offer_token=lost.offer_token,
            )
            is False
        )

    def test_exact_acknowledgement_delivers_and_polls_skip_it(
        self, database: Database
    ) -> None:
        seed_packets(database)
        seed_active_cell(database)
        poll = self.poll(database)

        offer = poll.next_offer(SESSION)
        assert offer is not None
        accepted = poll.acknowledge(
            session_id=SESSION,
            packet_id=offer.packet_id,
            offer_token=offer.offer_token,
        )

        assert accepted is True
        remaining = poll.next_offer(SESSION)
        assert remaining is None or remaining.packet_id != offer.packet_id
        row = database.execute(
            "SELECT state, offer_token FROM lead_intake_deliveries "
            "WHERE packet_id = ?",
            (offer.packet_id,),
        ).fetchone()
        assert str(row["state"]) == "delivered"
        assert row["offer_token"] is None

    def test_invalid_acknowledgements_change_nothing(
        self, database: Database
    ) -> None:
        seed_packets(database)
        seed_active_cell(database)
        poll = self.poll(database)
        offer = poll.next_offer(SESSION)
        assert offer is not None

        # Foreign session, wrong packet, unknown token: all rejected.
        assert not poll.acknowledge(
            session_id=OTHER_SESSION,
            packet_id=offer.packet_id,
            offer_token=offer.offer_token,
        )
        assert not poll.acknowledge(
            session_id=SESSION,
            packet_id="f" * 32,
            offer_token=offer.offer_token,
        )
        assert not poll.acknowledge(
            session_id=SESSION,
            packet_id=offer.packet_id,
            offer_token="0" * 32,
        )
        # The real acknowledgement still works exactly once; a
        # duplicate is rejected.
        assert poll.acknowledge(
            session_id=SESSION,
            packet_id=offer.packet_id,
            offer_token=offer.offer_token,
        )
        assert not poll.acknowledge(
            session_id=SESSION,
            packet_id=offer.packet_id,
            offer_token=offer.offer_token,
        )

    def test_superseded_rows_are_never_offered_or_acknowledged(
        self, database: Database
    ) -> None:
        seed_packets(database)
        seed_active_cell(database)
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO lead_intake_deliveries("
                "delivery_id, kind, packet_id, cell_id, session_id, "
                "surface_uuid, state, attempts, claimed_at, updated_at) "
                "VALUES ('backfill', 'HERMES_WORK_READY', ?, "
                "'cell-demo', ?, '', 'superseded', 0, ?, ?)",
                (WAKE_ID, SESSION, NOW.isoformat(), NOW.isoformat()),
            )
        poll = self.poll(database)

        offer = poll.next_offer(SESSION)

        assert offer is not None
        assert offer.packet_id == CORRECTION_ID
        assert not poll.acknowledge(
            session_id=SESSION,
            packet_id=WAKE_ID,
            offer_token="0" * 32,
        )

    def test_announced_rows_are_offerable_and_announcer_backs_off(
        self, database: Database
    ) -> None:
        seed_packets(database)
        seed_active_cell(database)
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO lead_intake_deliveries("
                "delivery_id, kind, packet_id, cell_id, session_id, "
                "surface_uuid, state, attempts, claimed_at, updated_at) "
                "VALUES ('announced-row', 'HERMES_WORK_READY', ?, "
                "'cell-demo', ?, '', 'announced', 1, ?, ?)",
                (WAKE_ID, SESSION, NOW.isoformat(), NOW.isoformat()),
            )
        poll = self.poll(database)

        offers = {
            offer.packet_id
            for offer in (
                poll.next_offer(SESSION),
                poll.next_offer(SESSION),
            )
            if offer is not None
        }

        assert offers == {CORRECTION_ID, WAKE_ID}
        row = database.execute(
            "SELECT state FROM lead_intake_deliveries "
            "WHERE delivery_id = 'announced-row'"
        ).fetchone()
        assert str(row["state"]) == "offered"


def test_the_lead_contract_documents_the_signal_protocol() -> None:
    from pathlib import Path as _Path

    contract = (
        _Path(__file__).parent.parent / "prompts" / "claude-lead.md"
    ).read_text()

    # Signals are wake-only; SQLite is authoritative; the Stop-hook
    # offer flow is the fallback, not the primary wake.
    assert "HERMES_CORRECTION_READY <id>" in contract
    assert "HERMES_WORK_READY <id>" in contract
    assert "signal only" in contract
    assert "only the durable SQLite record is" in contract
    assert "fallback" in contract


class TestManualEmergencySignal:
    """The operator-only bounded cmux signal (emergency recovery)."""

    def signal(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        port: RecordingPort,
    ):
        from hermes_orchestrator.lead_intake import ManualIntakeSignal

        return ManualIntakeSignal(
            database=database, bindings=bindings, port=port
        )

    @pytest.mark.asyncio
    async def test_operator_signal_sends_one_bounded_envelope(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        seed_packets(database)
        seed_active_cell(database)
        seat(bindings)
        port = RecordingPort()

        envelope = await self.signal(database, bindings, port).send(
            kind=WORK_READY,
            packet_id=WAKE_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )

        assert envelope == f"HERMES_WORK_READY {WAKE_ID}"
        assert port.signals == [(LEAD, envelope + "\n")]
        # The manual signal records into the same durable machine, so
        # the automatic announcer never re-announces it.
        assert delivery_rows(database) == [(WORK_READY, "announced", 1)]
        router = LeadIntakeRouter(
            database=database,
            transport=transport(database, bindings, port),
        )
        # Only the correction remains to announce; the signalled wake
        # is deduplicated.
        announced = await router.tick()
        assert announced == (CORRECTION_ID,)
        assert len(port.signals) == 1

    @pytest.mark.asyncio
    async def test_manual_signal_fails_closed_everywhere(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        seed_packets(database)
        port = RecordingPort()
        lane = self.signal(database, bindings, port)

        with pytest.raises(IntakeRefused, match="no active seat"):
            await lane.send(
                kind=WORK_READY,
                packet_id=WAKE_ID,
                cell_id="cell-demo",
                session_id=SESSION,
            )
        binding = seat(bindings, classic=False)
        with pytest.raises(IntakeRefused, match="non-classic"):
            await lane.send(
                kind=WORK_READY,
                packet_id=WAKE_ID,
                cell_id="cell-demo",
                session_id=SESSION,
            )
        bindings.record_classic(binding.binding_id, SESSION)
        with pytest.raises(IntakeRefused, match="no durable packet"):
            await lane.send(
                kind=WORK_READY,
                packet_id="c" * 32,
                cell_id="cell-demo",
                session_id=SESSION,
            )
        port.alive = False
        with pytest.raises(IntakeRefused, match="no longer live"):
            await lane.send(
                kind=WORK_READY,
                packet_id=WAKE_ID,
                cell_id="cell-demo",
                session_id=SESSION,
            )
        assert port.signals == []

    @pytest.mark.asyncio
    async def test_consumed_packets_are_never_resignalled(
        self, database: Database, bindings: CmuxSurfaceBindings
    ) -> None:
        seed_packets(database)
        seed_active_cell(database)
        seat(bindings)
        port = RecordingPort()
        poll = LeadIntakePoll(database=database)
        offer = poll.next_offer(SESSION)
        while offer is not None and offer.packet_id != WAKE_ID:
            offer = poll.next_offer(SESSION)
        assert offer is not None
        assert poll.acknowledge(
            session_id=SESSION,
            packet_id=WAKE_ID,
            offer_token=offer.offer_token,
        )

        with pytest.raises(IntakeRefused, match="already consumed"):
            await self.signal(database, bindings, port).send(
                kind=WORK_READY,
                packet_id=WAKE_ID,
                cell_id="cell-demo",
                session_id=SESSION,
            )
        assert port.signals == []


@pytest.mark.asyncio
async def test_the_router_fails_closed_residual_deliveries(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    """A non-terminal delivery row whose packet was consumed through
    another path is durable residue: the tick supersedes it against
    the packet ledger so it can never be offered again."""

    seed_active_cell(database)
    seed_assignment(database)
    seat(bindings)
    port = RecordingPort()
    router = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )
    assert ASSIGNMENT_ID in await router.tick()  # row becomes 'announced'
    with database.transaction() as connection:
        connection.execute(
            "UPDATE lead_assignments SET state = 'acknowledged' "
            "WHERE assignment_id = ?",
            (ASSIGNMENT_ID,),
        )

    await router.tick()

    state = database.scalar(
        "SELECT state FROM lead_intake_deliveries WHERE packet_id = ?",
        (ASSIGNMENT_ID,),
    )
    assert str(state) == "superseded"
    assert LeadIntakePoll(database=database).next_offer(SESSION) is None


def test_runtime_lifecycle_receipts_are_silent_maintenance(
    database: Database,
) -> None:
    """Restart/re-register/replay never changes Fable's next action."""

    seed_active_cell(database)
    operations = ControlOperations(database, events=EventStore(database))
    lifecycle = (
        ("daemon.restarted", "a" * 32),
        ("channel.reregistered", "b" * 32),
        ("channel.replayed", "c" * 32),
    )
    for kind, operation_id in lifecycle:
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO control_operations("
                "operation_id, schema_version, kind, project_key, cell_id, "
                "session_id, dedup_key, result_json, reason, state, "
                "created_at, updated_at, acknowledged_at) VALUES "
                "(?, 1, ?, 'demo', 'cell-demo', ?, ? || ':' || ?, "
                "'{\"activation\": {\"git_sha\": \"8ac98e27\"}}', NULL, "
                "'published', ?, ?, NULL)",
                (
                    operation_id,
                    kind,
                    SESSION,
                    kind,
                    SESSION,
                    NOW.isoformat(),
                    NOW.isoformat(),
                ),
            )
    poll = LeadIntakePoll(database=database)
    assert poll.next_offer(SESSION) is None
    assert poll.next_offer(OTHER_SESSION) is None
    assert set(operations.settle_maintenance_for_session(SESSION)) == {
        operation_id for _, operation_id in lifecycle
    }
