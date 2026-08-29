from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hermes_orchestrator.cmux import CmuxSurfaceRef, CmuxUnavailable
from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
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
    # The primary delivery is one bounded signal to the exact surface;
    # metadata is supplemental visibility.
    assert port.signals == [(LEAD, envelope + "\n")]
    assert port.statuses == [(LEAD.workspace_uuid, "intake", envelope)]
    assert port.notifications == [
        (LEAD.workspace_uuid, "Hermes intake pending", envelope)
    ]
    assert delivery_rows(database) == [(WORK_READY, "announced", 1)]


@pytest.mark.asyncio
async def test_router_signals_each_packet_to_its_exact_surface(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    # The post-idle wake: for every pending packet with a valid classic
    # binding, exactly one bounded signal reaches the exact surface —
    # nothing outside the closed grammar can ever reach a terminal.
    seed_packets(database)
    seed_active_cell(database)
    seat(bindings)
    port = RecordingPort()
    router = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )

    announced = await router.tick()

    assert sorted(announced) == sorted([CORRECTION_ID, WAKE_ID])
    assert len(port.signals) == 2
    assert all(ref == LEAD for ref, _ in port.signals)
    assert {text for _, text in port.signals} == {
        f"HERMES_CORRECTION_READY {CORRECTION_ID}\n",
        f"HERMES_WORK_READY {WAKE_ID}\n",
    }


@pytest.mark.asyncio
async def test_metadata_alone_is_never_treated_as_delivery(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    seed_packets(database)
    seat(bindings)
    port = RecordingPort()
    port.signal_error = CmuxUnavailable("cmux command timed out")

    outcome = await transport(database, bindings, port).deliver(
        **DELIVER_KWARGS
    )

    # The signal failed, so the delivery is an uncertain attempt even
    # though the metadata channel was healthy; nothing is announced.
    assert outcome.status == "attempt_failed"
    assert delivery_rows(database) == [(WORK_READY, "attempted", 1)]
    assert port.signals == []


@pytest.mark.asyncio
async def test_post_stop_correction_race_wakes_once_across_restart(
    database: Database, bindings: CmuxSurfaceBindings
) -> None:
    # The live acceptance failure: the lead's final scan is empty, the
    # lead goes idle, and only then does a correction commit.
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
    assert len(port.signals) == 2

    # The woken lead retrieves the durable packets by id and
    # acknowledges the correction through the existing application
    # APIs.
    with database.transaction() as connection:
        connection.execute(
            "UPDATE lead_corrections SET state = 'acknowledged' "
            "WHERE correction_id = ?",
            (CORRECTION_ID,),
        )

    # Restart: a fresh router derives everything from durable state and
    # signals nothing again — one effective intake per packet.
    restarted = LeadIntakeRouter(
        database=database, transport=transport(database, bindings, port)
    )
    assert await restarted.tick() == ()
    assert len(port.signals) == 2


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

    failing_port = RecordingPort()
    failing_port.signal_error = CmuxUnavailable("cmux command timed out")
    failing = transport(database, bindings, failing_port, now=lambda: NOW)
    outcome = await failing.deliver(**DELIVER_KWARGS)
    assert outcome.status == "attempt_failed"
    assert delivery_rows(database) == [(WORK_READY, "attempted", 1)]

    # Inside the retry window another announcer does nothing external.
    held = await failing.deliver(**DELIVER_KWARGS)
    assert held.status == "pending"

    # After the window the retry re-signals and completes; a repeated
    # signal is dedup-safe on the lead side because the packet fetch by
    # id is idempotent.
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
    assert "Terminal text is never authoritative" in contract
    assert "fallback" in contract
