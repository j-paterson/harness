"""The channel hub fails closed and delivers exactly-once effective."""

from __future__ import annotations

import asyncio
import json
import stat
import tempfile
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

import pytest
import pytest_asyncio

from hermes_orchestrator.channel_hub import (
    ChannelCapabilities,
    ChannelHub,
)
from hermes_orchestrator.cmux_surfaces import (
    CmuxSurfaceBindings,
    CmuxSurfaceRef,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore

SESSION = "11111111-2222-4333-8444-555555555555"
WAKE_ID = "a" * 32
CORRECTION_ID = "b" * 32
NOW = datetime(2026, 8, 29, 12, 0, tzinfo=UTC)
LEAD = CmuxSurfaceRef(workspace_uuid="w" * 8, surface_uuid="s" * 8)


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


@pytest.fixture
def state_dir(tmp_path: Path) -> Path:
    return tmp_path / "state"


@pytest.fixture
def capabilities(database: Database, state_dir: Path) -> ChannelCapabilities:
    return ChannelCapabilities(database=database, state_dir=state_dir)


@pytest.fixture
def socket_path() -> Iterator[Path]:
    # Unix socket paths have a hard ~104-byte limit on macOS; pytest
    # tmp_path can exceed it, so the socket lives in a short tempdir.
    with tempfile.TemporaryDirectory() as short:
        yield Path(short) / "hub.sock"


def hub_for(
    database: Database,
    bindings: CmuxSurfaceBindings,
    capabilities: ChannelCapabilities,
    socket_path: Path,
) -> ChannelHub:
    return ChannelHub(
        database=database,
        bindings=bindings,
        capabilities=capabilities,
        socket_path=socket_path,
    )


@pytest_asyncio.fixture
async def hub(
    database: Database,
    bindings: CmuxSurfaceBindings,
    capabilities: ChannelCapabilities,
    socket_path: Path,
) -> AsyncIterator[ChannelHub]:
    value = hub_for(database, bindings, capabilities, socket_path)
    await value.start()
    try:
        yield value
    finally:
        await value.stop()


def seat(bindings: CmuxSurfaceBindings, *, classic: bool = True) -> object:
    binding = bindings.bind_lead(
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        profile_alias="max-b",
        ref=LEAD,
    )
    if classic:
        bindings.record_classic(binding.binding_id, SESSION)
    return binding


def seed_packets(database: Database) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO lead_terminal_wakes("
            "wake_id, schema_version, project_key, issue_id, cell_id, "
            "session_id, profile_alias, turn_key, kind, reason, state, "
            "created_at) VALUES (?, 1, 'demo', 'ENG-9', 'cell-demo', ?, "
            "'max-b', 'turn-1', 'completed', 'turn completed', "
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
            "('cell-demo', 'demo', 'active', 'max-b', ?, ?, ?)",
            (SESSION, NOW.isoformat(), NOW.isoformat()),
        )


class Sidecar:
    """A protocol-exact fake of the hermes-control sidecar."""

    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.reader = reader
        self.writer = writer

    @classmethod
    async def connect(cls, path: Path) -> Sidecar:
        reader, writer = await asyncio.open_unix_connection(str(path))
        return cls(reader, writer)

    async def send(self, message: dict[str, object]) -> None:
        self.writer.write((json.dumps(message) + "\n").encode())
        await self.writer.drain()

    async def send_raw(self, payload: bytes) -> None:
        self.writer.write(payload)
        await self.writer.drain()

    async def receive(self) -> dict[str, object]:
        line = await asyncio.wait_for(self.reader.readline(), timeout=2)
        assert line, "connection closed while a reply was expected"
        return json.loads(line)

    async def expect_closed(self) -> None:
        line = await asyncio.wait_for(self.reader.readline(), timeout=2)
        assert line == b""

    async def close(self) -> None:
        self.writer.close()
        with suppress(BrokenPipeError, ConnectionResetError):
            await self.writer.wait_closed()


def register_message(token: str, **overrides: object) -> dict[str, object]:
    message: dict[str, object] = {
        "op": "register",
        "proto": 1,
        "project": "demo",
        "cell_id": "cell-demo",
        "session_id": SESSION,
        "profile": "max-b",
        "generation": 1,
        "capability": token,
    }
    message.update(overrides)
    return message


def issued_capability(
    capabilities: ChannelCapabilities, session_id: str = SESSION
) -> str:
    path = capabilities.issue(session_id)
    return path.read_text(encoding="ascii")


async def registered_sidecar(
    hub: ChannelHub, capabilities: ChannelCapabilities
) -> Sidecar:
    sidecar = await Sidecar.connect(hub.socket_path)
    await sidecar.send(register_message(issued_capability(capabilities)))
    reply = await sidecar.receive()
    assert reply == {"op": "registered", "proto": 1}
    return sidecar


class TestCapabilities:
    def test_the_token_lives_only_in_a_0600_file(
        self, database: Database, capabilities: ChannelCapabilities
    ) -> None:
        path = capabilities.issue(SESSION)

        mode = stat.S_IMODE(path.stat().st_mode)
        token = path.read_text(encoding="ascii")
        row = database.execute(
            "SELECT capability_sha256 FROM channel_capabilities WHERE session_id = ?",
            (SESSION,),
        ).fetchone()

        assert mode == 0o600
        assert len(token) == 64
        assert row is not None
        assert token not in str(row["capability_sha256"])
        assert capabilities.verify(SESSION, token)
        assert not capabilities.verify(SESSION, "f" * 64)
        assert not capabilities.verify(SESSION, "not hex at all")

    def test_retire_revokes_and_removes_the_file(
        self, capabilities: ChannelCapabilities
    ) -> None:
        path = capabilities.issue(SESSION)
        token = path.read_text(encoding="ascii")

        capabilities.retire(SESSION)

        assert not path.exists()
        assert not capabilities.verify(SESSION, token)


@pytest.mark.asyncio
class TestRegistration:
    async def test_a_valid_identity_registers_durably(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings)

        sidecar = await registered_sidecar(hub, capabilities)

        row = database.execute(
            "SELECT project_key, cell_id, profile_alias, generation "
            "FROM channel_registrations "
            "WHERE session_id = ? AND state = 'active'",
            (SESSION,),
        ).fetchone()
        assert row is not None
        assert str(row["project_key"]) == "demo"
        assert int(row["generation"]) == 1
        assert hub.registered_sessions() == frozenset({SESSION})
        await sidecar.close()

    @pytest.mark.parametrize(
        ("overrides", "reason_fragment"),
        [
            ({"proto": 2}, "protocol"),
            ({"project": ""}, "incomplete"),
            ({"generation": "1"}, "integer"),
            ({"capability": "f" * 64}, "capability"),
            ({"cell_id": "cell-other"}, "no active seat"),
            ({"session_id": "66666666-7777-4888-9999-000000000000"}, "capability"),
            ({"project": "other"}, "identity mismatch"),
            ({"profile": "max-c"}, "identity mismatch"),
            ({"generation": 7}, "stale"),
        ],
    )
    async def test_mismatched_identities_are_refused_and_closed(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
        overrides: dict[str, object],
        reason_fragment: str,
    ) -> None:
        seat(bindings)
        capability = issued_capability(capabilities)

        sidecar = await Sidecar.connect(hub.socket_path)
        await sidecar.send(register_message(capability, **overrides))
        reply = await sidecar.receive()

        assert reply["op"] == "refused"
        assert reason_fragment in str(reply["reason"])
        assert capability not in json.dumps(reply)
        await sidecar.expect_closed()
        assert hub.registered_sessions() == frozenset()
        active = database.scalar(
            "SELECT COUNT(*) FROM channel_registrations WHERE state = 'active'"
        )
        assert active == 0
        await sidecar.close()

    async def test_a_non_classic_seat_is_refused(
        self,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings, classic=False)

        sidecar = await Sidecar.connect(hub.socket_path)
        await sidecar.send(register_message(issued_capability(capabilities)))
        reply = await sidecar.receive()

        assert reply["op"] == "refused"
        assert "classic" in str(reply["reason"])
        await sidecar.close()

    async def test_free_form_and_oversized_lines_close_the_connection(
        self,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings)

        chatty = await Sidecar.connect(hub.socket_path)
        await chatty.send({"op": "chat", "text": "hello hub"})
        await chatty.expect_closed()
        await chatty.close()

        oversized = await Sidecar.connect(hub.socket_path)
        await oversized.send_raw(b"x" * 5000 + b"\n")
        await oversized.expect_closed()
        await oversized.close()

        assert hub.registered_sessions() == frozenset()


@pytest.mark.asyncio
class TestDelivery:
    async def test_a_publish_with_no_channel_stays_durably_pending(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seed_packets(database)

        status = await hub.publish(
            kind="HERMES_CORRECTION_READY",
            packet_id=CORRECTION_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )

        assert status == "pending"
        state = database.scalar(
            "SELECT state FROM channel_events WHERE packet_id = ?",
            (CORRECTION_ID,),
        )
        assert str(state) == "pending"

    async def test_registration_replays_the_pending_event_and_ack_lands(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings)
        seed_packets(database)
        await hub.publish(
            kind="HERMES_CORRECTION_READY",
            packet_id=CORRECTION_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )

        sidecar = await registered_sidecar(hub, capabilities)
        event = await sidecar.receive()

        assert event["op"] == "event"
        assert event["kind"] == "HERMES_CORRECTION_READY"
        assert event["packet_id"] == CORRECTION_ID
        assert event["session_id"] == SESSION

        await sidecar.send(
            {
                "op": "ack",
                "event_id": event["event_id"],
                "packet_id": CORRECTION_ID,
                "session_id": SESSION,
            }
        )
        reply = await sidecar.receive()
        assert reply == {"op": "ack_ok", "event_id": event["event_id"]}
        state = database.scalar(
            "SELECT state FROM channel_events WHERE event_id = ?",
            (event["event_id"],),
        )
        assert str(state) == "acked"
        await sidecar.close()

    async def test_a_live_channel_receives_the_publish_immediately(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings)
        seed_packets(database)
        sidecar = await registered_sidecar(hub, capabilities)

        status = await hub.publish(
            kind="HERMES_CORRECTION_READY",
            packet_id=CORRECTION_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )
        event = await sidecar.receive()

        assert status == "published"
        assert event["packet_id"] == CORRECTION_ID
        await sidecar.close()

    async def test_duplicate_and_foreign_acks_change_nothing(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings)
        seed_packets(database)
        await hub.publish(
            kind="HERMES_CORRECTION_READY",
            packet_id=CORRECTION_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )
        sidecar = await registered_sidecar(hub, capabilities)
        event = await sidecar.receive()
        event_id = event["event_id"]

        wrong_session = {
            "op": "ack",
            "event_id": event_id,
            "packet_id": CORRECTION_ID,
            "session_id": "66666666-7777-4888-9999-000000000000",
        }
        wrong_packet = {
            "op": "ack",
            "event_id": event_id,
            "packet_id": "c" * 32,
            "session_id": SESSION,
        }
        for refusal in (wrong_session, wrong_packet):
            await sidecar.send(refusal)
            reply = await sidecar.receive()
            assert reply["op"] == "ack_refused"
        state = database.scalar(
            "SELECT state FROM channel_events WHERE event_id = ?",
            (event_id,),
        )
        assert str(state) == "published"

        good = {
            "op": "ack",
            "event_id": event_id,
            "packet_id": CORRECTION_ID,
            "session_id": SESSION,
        }
        await sidecar.send(good)
        assert (await sidecar.receive())["op"] == "ack_ok"
        await sidecar.send(good)
        duplicate = await sidecar.receive()
        assert duplicate["op"] == "ack_refused"
        acked_at = database.scalar(
            "SELECT acked_at FROM channel_events WHERE event_id = ?",
            (event_id,),
        )
        assert acked_at is not None
        await sidecar.close()

    async def test_hub_restart_replays_only_unacknowledged_events(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        socket_path: Path,
    ) -> None:
        seat(bindings)
        seed_packets(database)

        first = hub_for(database, bindings, capabilities, socket_path)
        await first.start()
        assert (
            await first.publish(
                kind="HERMES_WORK_READY",
                packet_id=WAKE_ID,
                cell_id="cell-demo",
                session_id=SESSION,
            )
            == "pending"
        )
        sidecar = await registered_sidecar(first, capabilities)
        wake_event = await sidecar.receive()
        assert wake_event["kind"] == "HERMES_WORK_READY"
        await sidecar.send(
            {
                "op": "ack",
                "event_id": wake_event["event_id"],
                "packet_id": wake_event["packet_id"],
                "session_id": SESSION,
            }
        )
        assert (await sidecar.receive())["op"] == "ack_ok"
        await first.publish(
            kind="HERMES_CORRECTION_READY",
            packet_id=CORRECTION_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )
        unacked = await sidecar.receive()
        await sidecar.close()
        await first.stop()

        second = hub_for(database, bindings, capabilities, socket_path)
        await second.start()
        try:
            reconnected = await registered_sidecar(second, capabilities)
            replayed = await reconnected.receive()

            # Only the unacknowledged correction returns; the same
            # durable event identity makes the delivery deduplicable.
            assert replayed["event_id"] == unacked["event_id"]
            assert replayed["packet_id"] == CORRECTION_ID
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(reconnected.reader.readline(), timeout=0.3)
            await reconnected.close()
        finally:
            await second.stop()

    async def test_a_nudge_routes_committed_packets_to_the_channel(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings)
        seed_active_cell(database)
        sidecar = await registered_sidecar(hub, capabilities)
        seed_packets(database)

        nudger = await Sidecar.connect(hub.socket_path)
        await nudger.send({"op": "nudge"})
        reply = await nudger.receive()
        await nudger.expect_closed()
        await nudger.close()

        assert reply["op"] == "nudged"
        assert reply["published"] == 2
        kinds = {
            (await sidecar.receive())["kind"],
            (await sidecar.receive())["kind"],
        }
        assert kinds == {
            "HERMES_CORRECTION_READY",
            "HERMES_WORK_READY",
        }
        await sidecar.close()

    async def test_invalid_publish_inputs_raise_before_any_effect(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        with pytest.raises(ValueError):
            await hub.publish(
                kind="HERMES_CHAT",
                packet_id=CORRECTION_ID,
                cell_id="cell-demo",
                session_id=SESSION,
            )
        with pytest.raises(ValueError):
            await hub.publish(
                kind="HERMES_CORRECTION_READY",
                packet_id="short",
                cell_id="cell-demo",
                session_id=SESSION,
            )
        assert database.scalar("SELECT COUNT(*) FROM channel_events") == 0
