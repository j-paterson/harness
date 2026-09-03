"""The channel hub fails closed and delivers exactly-once effective."""

from __future__ import annotations

import asyncio
import json
import stat
import tempfile
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress
from datetime import UTC, datetime, timedelta
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
from hermes_orchestrator.control_operations import ControlOperations
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore

SESSION = "11111111-2222-4333-8444-555555555555"
WAKE_ID = "a" * 32
CORRECTION_ID = "b" * 32
ASSIGNMENT_ID = "c" * 32
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
            "'pending', ?)",
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

    async def test_repeated_publish_pass_does_not_repeat_visible_event(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings)
        seed_packets(database)
        sidecar = await registered_sidecar(hub, capabilities)

        first = await hub.publish(
            kind="HERMES_CORRECTION_READY",
            packet_id=CORRECTION_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )
        event = await sidecar.receive()
        second = await hub.publish(
            kind="HERMES_CORRECTION_READY",
            packet_id=CORRECTION_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )

        assert first == "published"
        assert second == "deduplicated"
        assert event["packet_id"] == CORRECTION_ID
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(sidecar.reader.readline(), timeout=0.3)
        assert database.scalar(
            "SELECT attempts FROM channel_events WHERE event_id = ?",
            (event["event_id"],),
        ) == 1
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


@pytest.mark.asyncio
class TestDirectRouting:
    async def test_a_committed_wake_reaches_the_channel_without_a_tick(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        from uuid import UUID

        from hermes_orchestrator.channel_hub import ChannelPacketRouter
        from hermes_orchestrator.lead_wakes import (
            LeadTerminalWakes,
            TerminalWakeInput,
        )

        seat(bindings)
        wakes = LeadTerminalWakes(database=database, events=EventStore(database))
        ChannelPacketRouter(hub).attach(wakes)
        sidecar = await registered_sidecar(hub, capabilities)

        wakes.commit(
            TerminalWakeInput(
                project_key="demo",
                issue_id="ENG-9",
                cell_id="cell-demo",
                session_id=UUID(SESSION),
                profile_alias="max-b",
                turn_key="turn-9",
                kind="completed",
                reason="turn completed",
            )
        )
        event = await sidecar.receive()

        assert event["op"] == "event"
        assert event["kind"] == "HERMES_WORK_READY"
        assert event["session_id"] == SESSION
        await sidecar.close()

    async def test_a_correction_never_targets_a_more_recent_harness_cell(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        """INFRA-198 (observed live 2026-09-01): correction cde70842 for
        a product candidate was announced to the DEAD HARNESS session.

        The target was the most recently updated active cell of ANY
        lane, and the harness cell's row happened to be touched last, so
        the harness lane won a correction it neither owns nor can act
        on. Only the development lane publishes product candidates, so
        only it can answer a correction about one. The harness cell here
        is deliberately the FRESHER row -- under the old ordering it
        won."""

        seat(bindings)
        seed_active_cell(database)
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO project_cells("
                "cell_id, project_key, lane_role, state, profile_alias, "
                "session_id, created_at, updated_at) VALUES "
                "('cell-harness', 'demo', 'harness', 'active', 'max-b', "
                "?, ?, ?)",
                (
                    "99999999-9999-4999-8999-999999999999",
                    NOW.isoformat(),
                    # Strictly newer than the development cell.
                    (NOW + timedelta(minutes=5)).isoformat(),
                ),
            )
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO lead_corrections("
                "correction_id, project_key, issue_id, source, repository, "
                "branch, pr_number, reviewed_sha, packets_json, state, "
                "created_at) VALUES (?, 'demo', 'ENG-9', 'codex_review', "
                "'owner/demo', 'feature/x', 7, 'abc', '[]', 'pending', ?)",
                (CORRECTION_ID, NOW.isoformat()),
            )
        sidecar = await registered_sidecar(hub, capabilities)

        published = await hub.publish_pending()

        assert published == (f"HERMES_CORRECTION_READY {CORRECTION_ID}",)
        event = await sidecar.receive()
        assert event["kind"] == "HERMES_CORRECTION_READY"
        # The development lead, never the fresher harness seat.
        assert event["session_id"] == SESSION
        row = database.execute(
            "SELECT session_id FROM channel_events WHERE packet_id = ?",
            (CORRECTION_ID,),
        ).fetchone()
        assert str(row["session_id"]) == SESSION
        await sidecar.close()

    async def test_a_journalled_correction_reaches_the_channel_directly(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        from hermes_orchestrator.channel_hub import ChannelPacketRouter
        from hermes_orchestrator.lead_outbox import LeadCorrectionOutbox
        from hermes_orchestrator.verdicts import CorrectionPacket

        seat(bindings)
        seed_active_cell(database)
        outbox = LeadCorrectionOutbox(
            database=database,
            events=EventStore(database),
            project_for_issue=lambda issue_id: "demo",
        )
        ChannelPacketRouter(hub).attach(outbox)
        sidecar = await registered_sidecar(hub, capabilities)

        outbox.deliver(
            "ENG-9",
            (
                CorrectionPacket(
                    severity="Critical",
                    repository="owner/demo",
                    branch="feature/x",
                    pr_number=7,
                    # INFRA-193: the stored packet is validated against the
                    # authoritative CorrectionPacket schema on every read,
                    # which requires a real 40-hex reviewed_sha. Every one
                    # of the 193 packets in live durable state satisfies
                    # this; "abc" was only ever an unrealistic placeholder.
                    reviewed_sha="a" * 40,
                    evidence="the defect",
                    acceptance_criterion="it works",
                    required_correction="fix it",
                    required_tests=("pytest",),
                ),
            ),
        )
        event = await sidecar.receive()

        assert event["kind"] == "HERMES_CORRECTION_READY"
        await sidecar.close()

    async def test_the_nudge_client_triggers_routing_or_reports_failure(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        from hermes_orchestrator.channel_hub import nudge

        seat(bindings)
        seed_active_cell(database)
        sidecar = await registered_sidecar(hub, capabilities)
        seed_packets(database)

        nudged = await asyncio.to_thread(nudge, hub.socket_path)

        assert nudged is True
        kinds = {
            (await sidecar.receive())["kind"],
            (await sidecar.receive())["kind"],
        }
        assert kinds == {
            "HERMES_CORRECTION_READY",
            "HERMES_WORK_READY",
        }
        await sidecar.close()

        missing = hub.socket_path.with_name("absent.sock")
        assert await asyncio.to_thread(nudge, missing) is False


class TestLauncher:
    def launcher(
        self,
        capabilities: ChannelCapabilities,
        state_dir: Path,
        tmp_path: Path,
    ) -> object:
        from hermes_orchestrator.channel_hub import ChannelLauncher

        entry = tmp_path / "sidecar" / "main.js"
        entry.parent.mkdir(parents=True, exist_ok=True)
        entry.write_text("// sidecar", encoding="utf-8")
        node = tmp_path / "node"
        node.write_text("#!/bin/sh", encoding="utf-8")
        return ChannelLauncher(
            state_dir=state_dir,
            capabilities=capabilities,
            sidecar_entry=entry,
            node_binary=node,
        )

    def test_the_config_carries_identity_but_never_the_token(
        self,
        capabilities: ChannelCapabilities,
        state_dir: Path,
        tmp_path: Path,
    ) -> None:
        launcher = self.launcher(capabilities, state_dir, tmp_path)

        config_path = launcher.generate(
            project_key="demo",
            cell_id="cell-demo",
            session_id=SESSION,
            profile_alias="max-b",
            generation=3,
        )

        assert config_path.name == f"{SESSION}.mcp.json"
        assert stat.S_IMODE(config_path.stat().st_mode) == 0o600
        raw = config_path.read_text(encoding="utf-8")
        config = json.loads(raw)
        server = config["mcpServers"]["hermes-control"]
        env = server["env"]
        assert env["HERMES_CONTROL_PROJECT"] == "demo"
        assert env["HERMES_CONTROL_CELL"] == "cell-demo"
        assert env["HERMES_CONTROL_SESSION"] == SESSION
        assert env["HERMES_CONTROL_PROFILE"] == "max-b"
        assert env["HERMES_CONTROL_GENERATION"] == "3"
        assert env["HERMES_CONTROL_SOCKET"].endswith("channels/hub.sock")
        capability_file = Path(env["HERMES_CONTROL_CAPABILITY_FILE"])
        token = capability_file.read_text(encoding="ascii")
        # The capability rides only in its 0600 file — never in the
        # config, and the launcher issued a verifiable token.
        assert token not in raw
        assert capabilities.verify(SESSION, token)

    def test_a_missing_sidecar_build_refuses_before_any_write(
        self,
        capabilities: ChannelCapabilities,
        state_dir: Path,
        tmp_path: Path,
    ) -> None:
        from hermes_orchestrator.channel_hub import ChannelLauncher

        launcher = ChannelLauncher(
            state_dir=state_dir,
            capabilities=capabilities,
            sidecar_entry=tmp_path / "absent" / "main.js",
            node_binary=tmp_path / "node",
        )

        with pytest.raises(FileNotFoundError):
            launcher.generate(
                project_key="demo",
                cell_id="cell-demo",
                session_id=SESSION,
                profile_alias="max-b",
                generation=1,
            )

        assert not (state_dir / "channels" / f"{SESSION}.mcp.json").exists()

    def test_cleanup_removes_config_and_retires_the_capability(
        self,
        capabilities: ChannelCapabilities,
        state_dir: Path,
        tmp_path: Path,
    ) -> None:
        launcher = self.launcher(capabilities, state_dir, tmp_path)
        config_path = launcher.generate(
            project_key="demo",
            cell_id="cell-demo",
            session_id=SESSION,
            profile_alias="max-b",
            generation=1,
        )
        capability_file = capabilities.path_for(SESSION)
        token = capability_file.read_text(encoding="ascii")

        launcher.cleanup(SESSION)

        assert not config_path.exists()
        assert not capability_file.exists()
        assert not capabilities.verify(SESSION, token)


def seed_assignment(database: Database, *, state: str = "published") -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO lead_assignments("
            "assignment_id, schema_version, project_key, issue_id, "
            "cell_id, session_id, profile_alias, instruction_id, "
            "queue_transition, state, created_at, updated_at, "
            "acknowledged_at) VALUES (?, 1, 'demo', 'ENG-9', "
            "'cell-demo', ?, 'max-b', 'chat-ENG-9', "
            "'queued->in_development', ?, ?, ?, NULL)",
            (ASSIGNMENT_ID, SESSION, state, NOW.isoformat(), NOW.isoformat()),
        )


@pytest.mark.asyncio
class TestAssignmentEvents:
    """INFRA-195: assignment packets ride the channel with exact ACK."""

    async def test_a_published_assignment_reaches_the_exact_session(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings)
        seed_active_cell(database)
        seed_assignment(database)
        sidecar = await registered_sidecar(hub, capabilities)

        published = await hub.publish_pending()

        assert published == (f"HERMES_ASSIGNMENT_READY {ASSIGNMENT_ID}",)
        event = await sidecar.receive()
        assert event["op"] == "event"
        assert event["kind"] == "HERMES_ASSIGNMENT_READY"
        assert event["packet_id"] == ASSIGNMENT_ID
        assert event["session_id"] == SESSION
        await sidecar.close()

    async def test_the_exact_channel_ack_acknowledges_the_ledger(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings)
        seed_active_cell(database)
        seed_assignment(database)
        sidecar = await registered_sidecar(hub, capabilities)
        await hub.publish_pending()
        event = await sidecar.receive()

        await sidecar.send(
            {
                "op": "ack",
                "event_id": event["event_id"],
                "packet_id": ASSIGNMENT_ID,
                "session_id": SESSION,
            }
        )
        reply = await sidecar.receive()

        assert reply == {"op": "ack_ok", "event_id": event["event_id"]}
        row = database.execute(
            "SELECT state, acknowledged_at FROM lead_assignments "
            "WHERE assignment_id = ?",
            (ASSIGNMENT_ID,),
        ).fetchone()
        assert str(row["state"]) == "acknowledged"
        assert row["acknowledged_at"] is not None
        await sidecar.close()

    async def test_an_unacked_assignment_event_survives_the_repair_sweep(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        """The bootstrap defect, inverted: only the assignment ledger's
        own terminal state may supersede its channel event — a delivery
        marked elsewhere never consumes the exact-ACK contract."""

        seat(bindings)
        seed_active_cell(database)
        seed_assignment(database)
        await hub.publish(
            kind="HERMES_ASSIGNMENT_READY",
            packet_id=ASSIGNMENT_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )

        await hub.publish_pending()

        state = database.scalar(
            "SELECT state FROM channel_events WHERE packet_id = ?",
            (ASSIGNMENT_ID,),
        )
        assert str(state) != "superseded"

        # Once the ledger itself is terminal (acknowledged through the
        # fallback drain), the event supersedes and never replays.
        with database.transaction() as connection:
            connection.execute(
                "UPDATE lead_assignments SET state = 'acknowledged' "
                "WHERE assignment_id = ?",
                (ASSIGNMENT_ID,),
            )
        await hub.publish_pending()
        state = database.scalar(
            "SELECT state FROM channel_events WHERE packet_id = ?",
            (ASSIGNMENT_ID,),
        )
        assert str(state) == "superseded"


@pytest.mark.asyncio
class TestControlOperationEvents:
    """INFRA-195: recovery is provable through ACKable receipts."""

    async def test_reregistration_retries_confirmation_anchor_capture(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        socket_path: Path,
    ) -> None:
        class FlakyAnchors:
            attempts = 0
            captured = False

            def capture_after_confirmation(self, **_: object) -> None:
                self.attempts += 1
                if self.attempts == 1:
                    raise RuntimeError("transient capture failure")
                self.captured = True

        anchors = FlakyAnchors()
        hub = ChannelHub(
            database=database,
            bindings=bindings,
            capabilities=capabilities,
            socket_path=socket_path,
            anchors=anchors,  # type: ignore[arg-type]
        )
        await hub.start()
        try:
            seat(bindings)
            first = await registered_sidecar(hub, capabilities)
            await first.close()
            second = await registered_sidecar(hub, capabilities)
            await second.close()
        finally:
            await hub.stop()

        assert anchors.attempts == 2
        assert anchors.captured

    async def test_a_lifecycle_receipt_stays_off_the_model_channel(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        socket_path: Path,
    ) -> None:
        """Runtime bookkeeping is durable but never starts a Fable turn."""

        operations = ControlOperations(database, events=EventStore(database))
        hub = ChannelHub(
            database=database,
            bindings=bindings,
            capabilities=capabilities,
            socket_path=socket_path,
            control=operations,
        )
        await hub.start()
        try:
            seat(bindings)
            recorded = operations.record(
                kind="daemon.restarted",
                project_key="demo",
                cell_id="cell-demo",
                session_id=SESSION,
                result={"interval_seconds": 30},
            )
            assert recorded is not None
            sidecar = await registered_sidecar(hub, capabilities)

            await hub.publish_pending()

            assert database.scalar(
                "SELECT COUNT(*) FROM channel_events WHERE packet_id = ?",
                (recorded.operation_id,),
            ) == 0
            await sidecar.close()
        finally:
            await hub.stop()

        assert operations.get(recorded.operation_id).state == "published"
        settled = operations.settle_maintenance_for_session(SESSION)
        assert recorded.operation_id in settled
        assert operations.get(recorded.operation_id).state == "acknowledged"

    async def test_a_replay_receipts_its_exact_count_including_zero(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        socket_path: Path,
    ) -> None:
        operations = ControlOperations(database, events=EventStore(database))
        hub = ChannelHub(
            database=database,
            bindings=bindings,
            capabilities=capabilities,
            socket_path=socket_path,
            control=operations,
        )
        await hub.start()
        try:
            seat(bindings)
            sidecar = await registered_sidecar(hub, capabilities)
            await sidecar.close()
        finally:
            await hub.stop()

        [receipt] = operations.pending_for_session(SESSION)
        assert receipt.kind == "channel.replayed"
        assert receipt.result == {"replay_count": 0}

    async def test_a_reregistration_gets_a_durable_receipt(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        socket_path: Path,
    ) -> None:
        operations = ControlOperations(database, events=EventStore(database))
        hub = ChannelHub(
            database=database,
            bindings=bindings,
            capabilities=capabilities,
            socket_path=socket_path,
            control=operations,
        )
        await hub.start()
        try:
            seat(bindings)
            first = await registered_sidecar(hub, capabilities)
            await first.close()
            second = await registered_sidecar(hub, capabilities)
            await second.close()
        finally:
            await hub.stop()

        kinds = {
            receipt.kind: receipt
            for receipt in operations.pending_for_session(SESSION)
        }
        assert "channel.reregistered" in kinds
        assert kinds["channel.reregistered"].result["prior_registrations"] >= 1

    async def test_a_refused_registration_records_one_blocked_receipt(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        socket_path: Path,
    ) -> None:
        """Routine recovery must never require /mcp or a relaunch by
        hand: a refusal becomes one durable, actionable receipt, and a
        later successful registration supersedes it."""

        operations = ControlOperations(database, events=EventStore(database))
        hub = ChannelHub(
            database=database,
            bindings=bindings,
            capabilities=capabilities,
            socket_path=socket_path,
            control=operations,
        )
        await hub.start()
        try:
            # No seat binding exists yet: the registration is refused.
            sidecar = await Sidecar.connect(hub.socket_path)
            await sidecar.send(
                register_message(issued_capability(capabilities))
            )
            reply = await sidecar.receive()
            assert reply["op"] == "refused"
            await sidecar.close()

            [receipt] = operations.pending_for_session(SESSION)
            assert receipt.kind == "channel.blocked"
            assert receipt.result["refusal"] == reply["reason"]
            assert "reissued capability" in str(receipt.reason)

            # A duplicate refusal never floods: one live receipt.
            second = await Sidecar.connect(hub.socket_path)
            await second.send(
                register_message(issued_capability(capabilities))
            )
            await second.receive()
            await second.close()
            assert len(operations.pending_for_session(SESSION)) == 1

            # Confirmation does not make the same unresolved incident
            # newly actionable on the sidecar's next bounded retry.
            assert operations.acknowledge(
                receipt.operation_id, session_id=SESSION
            )
            third = await Sidecar.connect(hub.socket_path)
            await third.send(
                register_message(issued_capability(capabilities))
            )
            await third.receive()
            await third.close()
            blocked_count = database.scalar(
                "SELECT COUNT(*) FROM control_operations "
                "WHERE session_id = ? AND kind = 'channel.blocked'",
                (SESSION,),
            )
            assert blocked_count == 1

            # The seat lands and registration succeeds: the blocked
            # receipt is healed by the recovery it was blocking on.
            seat(bindings)
            healed = await registered_sidecar(hub, capabilities)
            await healed.close()
        finally:
            await hub.stop()

        kinds = [
            receipt.kind
            for receipt in operations.pending_for_session(SESSION)
        ]
        assert "channel.blocked" not in kinds

    async def test_the_exact_channel_ack_settles_the_receipt(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        socket_path: Path,
    ) -> None:
        operations = ControlOperations(database, events=EventStore(database))
        hub = ChannelHub(
            database=database,
            bindings=bindings,
            capabilities=capabilities,
            socket_path=socket_path,
            control=operations,
        )
        await hub.start()
        try:
            seat(bindings)
            recorded = operations.record(
                kind="children.completed",
                project_key="demo",
                cell_id="cell-demo",
                session_id=SESSION,
                result={"issue_id": "ENG-9"},
            )
            assert recorded is not None
            sidecar = await registered_sidecar(hub, capabilities)
            await hub.publish_pending()
            target = await sidecar.receive()
            assert target["packet_id"] == recorded.operation_id
            assert target["kind"] == "HERMES_CONTROL_READY"
            await sidecar.send(
                {
                    "op": "ack",
                    "event_id": target["event_id"],
                    "packet_id": recorded.operation_id,
                    "session_id": SESSION,
                }
            )
            reply = await sidecar.receive()
            assert reply["op"] == "ack_ok"
            await sidecar.close()
        finally:
            await hub.stop()

        assert operations.get(recorded.operation_id).state == "acknowledged"

    async def test_silent_maintenance_receipts_never_derive_a_channel_event(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        socket_path: Path,
    ) -> None:
        """Maintenance stays off-channel; actionable facts still ride it."""

        operations = ControlOperations(database, events=EventStore(database))
        hub = ChannelHub(
            database=database,
            bindings=bindings,
            capabilities=capabilities,
            socket_path=socket_path,
            control=operations,
        )
        await hub.start()
        try:
            seat(bindings)
            seed_active_cell(database)
            sidecar = await registered_sidecar(hub, capabilities)
            await hub.publish_pending()
            silent = operations.record(
                kind="intake.dedup_repaired",
                project_key="demo",
                cell_id="cell-demo",
                session_id=SESSION,
                result={"superseded_events": 1},
            )
            actionable = operations.record(
                kind="children.completed",
                project_key="demo",
                cell_id="cell-demo",
                session_id=SESSION,
                result={"issue_id": "ENG-9"},
            )
            assert silent is not None
            assert actionable is not None

            await hub.publish_pending()

            target = await sidecar.receive()
            assert target["packet_id"] == actionable.operation_id
            assert target["kind"] == "HERMES_CONTROL_READY"
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(sidecar.reader.readline(), timeout=0.3)
            await sidecar.close()
        finally:
            await hub.stop()

        maintenance_event = database.scalar(
            "SELECT COUNT(*) FROM channel_events WHERE packet_id = ?",
            (silent.operation_id,),
        )
        assert int(maintenance_event) == 0


@pytest.mark.asyncio
class TestConsumedPacketRepair:
    """Live regression: consumed packets must never replay as events."""

    async def test_consumed_packets_are_never_derived_and_get_superseded(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings)
        seed_active_cell(database)
        seed_packets(database)
        # Both packets grew channel events while still pending...
        await hub.publish(
            kind="HERMES_WORK_READY",
            packet_id=WAKE_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )
        await hub.publish(
            kind="HERMES_CORRECTION_READY",
            packet_id=CORRECTION_ID,
            cell_id="cell-demo",
            session_id=SESSION,
        )
        # ...and were then consumed through another path (the
        # Stop-hook poll, an operator signal) before any channel ACK.
        with database.transaction() as connection:
            connection.execute(
                "UPDATE lead_terminal_wakes SET state = 'delivered' WHERE wake_id = ?",
                (WAKE_ID,),
            )
            connection.execute(
                "UPDATE lead_corrections SET state = 'acknowledged' "
                "WHERE correction_id = ?",
                (CORRECTION_ID,),
            )

        published = await hub.publish_pending()

        assert published == ()
        states = {
            str(row["packet_id"]): str(row["state"])
            for row in database.execute(
                "SELECT packet_id, state FROM channel_events"
            ).fetchall()
        }
        assert states == {
            WAKE_ID: "superseded",
            CORRECTION_ID: "superseded",
        }

        # A registration after the repair replays nothing: the day-old
        # consumed wake can never wake the lead again.
        sidecar = await registered_sidecar(hub, capabilities)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(sidecar.reader.readline(), timeout=0.3)
        await sidecar.close()


def active_registration_id(database: Database, session_id: str = SESSION) -> str:
    value = database.scalar(
        "SELECT registration_id FROM channel_registrations "
        "WHERE session_id = ? AND state = 'active' "
        "ORDER BY connected_at DESC, rowid DESC LIMIT 1",
        (session_id,),
    )
    assert value is not None
    return str(value)


def registration_row(database: Database, registration_id: str) -> object:
    row = database.execute(
        "SELECT state, close_reason FROM channel_registrations "
        "WHERE registration_id = ?",
        (registration_id,),
    ).fetchone()
    assert row is not None
    return row


def close_events(database: Database, registration_id: str) -> list[dict[str, object]]:
    rows = database.execute(
        "SELECT payload_json FROM events "
        "WHERE event_type = 'channel.closed' AND aggregate_id = ?",
        (registration_id,),
    ).fetchall()
    return [json.loads(str(row["payload_json"])) for row in rows]


class _DeadWriter:
    """A minimal stand-in for a StreamWriter whose transport already
    reports closing — used only to reproduce, deterministically, the
    race where a new registration arrives before the hub has finished
    processing an old peer's death (no real socket or task involved,
    so there is nothing left to race)."""

    def is_closing(self) -> bool:
        return True

    def close(self) -> None:
        pass


@pytest.mark.asyncio
class TestCloseReasons:
    """INFRA-197: durable, precise lifecycle truth on every close.

    ``close_reason`` used to be a single free-text 'disconnected' for
    every closed registration, and every close was silent — no
    journaled event. Live forensics needs to tell a peer's own
    disconnect apart from a supersede, a dead-transport reconnect, and
    a daemon shutdown, from the durable record alone.
    """

    async def test_a_peer_disconnect_records_disconnected_and_journals(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings)
        sidecar = await registered_sidecar(hub, capabilities)
        registration_id = active_registration_id(database)

        await sidecar.close()
        # The disconnect is processed by the hub's own connection task,
        # asynchronously with respect to this test — poll for it
        # rather than assume any particular number of loop iterations.
        deadline = asyncio.get_event_loop().time() + 2
        row = registration_row(database, registration_id)
        while str(row["state"]) != "closed":
            assert asyncio.get_event_loop().time() < deadline, (
                "the disconnect was never processed"
            )
            await asyncio.sleep(0.01)
            row = registration_row(database, registration_id)

        assert str(row["close_reason"]) == "disconnected"
        [payload] = close_events(database, registration_id)
        assert payload == {
            "session_id": SESSION,
            "cell_id": "cell-demo",
            "generation": 1,
            "close_reason": "disconnected",
        }

    async def test_a_live_supersede_records_superseded_and_closes_the_old_writer(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings)
        first = await registered_sidecar(hub, capabilities)
        first_id = active_registration_id(database)

        second = await registered_sidecar(hub, capabilities)

        # The still-live prior transport is the one the hub itself
        # terminates as part of the supersede.
        await first.expect_closed()
        row = registration_row(database, first_id)
        assert str(row["state"]) == "superseded"
        assert str(row["close_reason"]) == "superseded"
        [payload] = close_events(database, first_id)
        assert payload["close_reason"] == "superseded"
        assert payload["session_id"] == SESSION
        assert payload["cell_id"] == "cell-demo"
        assert payload["generation"] == 1
        await second.close()

    async def test_a_replace_over_a_dead_transport_records_replaced_dead(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        """The common healthy reconnect: the old socket already died,
        but the hub's own disconnect task for it has not yet run, so
        the row is still 'active' and the hub's tracked writer is the
        only place the transport's death is visible. Swapping in a
        writer whose transport reports already-closing, without
        touching the real connection or its task at all, reproduces
        this exact race deterministically instead of depending on
        socket-close scheduling."""

        seat(bindings)
        first = await registered_sidecar(hub, capabilities)
        first_id = active_registration_id(database)

        hub._connections[SESSION] = _DeadWriter()  # type: ignore[assignment]

        second = await registered_sidecar(hub, capabilities)

        row = registration_row(database, first_id)
        assert str(row["state"]) == "superseded"
        assert str(row["close_reason"]) == "replaced_dead"
        [payload] = close_events(database, first_id)
        assert payload["close_reason"] == "replaced_dead"
        assert payload["session_id"] == SESSION

        await second.close()
        await first.close()

    async def test_stop_marks_open_registrations_hub_stopped_then_unlinks(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        hub: ChannelHub,
    ) -> None:
        seat(bindings)
        await registered_sidecar(hub, capabilities)
        registration_id = active_registration_id(database)

        await hub.stop()

        row = registration_row(database, registration_id)
        assert str(row["state"]) == "closed"
        assert str(row["close_reason"]) == "hub_stopped"
        [payload] = close_events(database, registration_id)
        assert payload["close_reason"] == "hub_stopped"
        assert not hub.socket_path.exists()

    async def test_pending_events_replay_after_a_superseded_close(
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
        first = await registered_sidecar(hub, capabilities)
        pending = await first.receive()
        assert pending["packet_id"] == CORRECTION_ID

        second = await registered_sidecar(hub, capabilities)
        await first.expect_closed()

        replayed = await second.receive()
        assert replayed["event_id"] == pending["event_id"]
        assert replayed["packet_id"] == CORRECTION_ID

        close_reason = database.scalar(
            "SELECT close_reason FROM channel_registrations "
            "WHERE session_id = ? AND state = 'superseded'",
            (SESSION,),
        )
        assert str(close_reason) == "superseded"
        await second.close()


@pytest.mark.asyncio
class TestRepairReceiptChurn:
    """INFRA-197: the repair sweep never feeds on its own receipts.

    Live evidence: with no publishable channel, every consumed
    ``intake.dedup_repaired`` receipt left behind a stale channel
    event the next sweep counted as a fresh repair, minting a new
    receipt each pass — one spurious wake per maintenance tick,
    indefinitely.
    """

    async def test_a_consumed_repair_receipt_never_mints_another(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        socket_path: Path,
    ) -> None:
        operations = ControlOperations(database, events=EventStore(database))
        hub = ChannelHub(
            database=database,
            bindings=bindings,
            capabilities=capabilities,
            socket_path=socket_path,
            control=operations,
        )
        await hub.start()
        try:
            seat(bindings)
            seed_active_cell(database)
            recorded = operations.record(
                kind="intake.dedup_repaired",
                project_key="demo",
                cell_id="cell-demo",
                session_id=SESSION,
                result={"superseded_events": 1},
            )
            assert recorded is not None
            await hub.publish(
                kind="HERMES_CONTROL_READY",
                packet_id=recorded.operation_id,
                cell_id="cell-demo",
                session_id=SESSION,
            )
            assert operations.acknowledge(
                recorded.operation_id, session_id=SESSION
            )

            await hub.publish_pending()

            # The stale event is still swept terminally...
            state = database.scalar(
                "SELECT state FROM channel_events WHERE packet_id = ?",
                (recorded.operation_id,),
            )
            assert str(state) == "superseded"
            # ...but the sweep of pure receipt churn mints nothing.
            kinds = [
                receipt.kind
                for receipt in operations.pending_for_session(SESSION)
            ]
            assert "intake.dedup_repaired" not in kinds
        finally:
            await hub.stop()

    async def test_a_consumed_ordinary_receipt_still_counts_as_repair(
        self,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        socket_path: Path,
    ) -> None:
        """The INFRA-185 protection is intact: a stale event for any
        other consumed packet still supersedes and still receipts."""

        operations = ControlOperations(database, events=EventStore(database))
        hub = ChannelHub(
            database=database,
            bindings=bindings,
            capabilities=capabilities,
            socket_path=socket_path,
            control=operations,
        )
        await hub.start()
        try:
            seat(bindings)
            seed_active_cell(database)
            recorded = operations.record(
                kind="daemon.restarted",
                project_key="demo",
                cell_id="cell-demo",
                session_id=SESSION,
                result={"interval_seconds": 30},
            )
            assert recorded is not None
            await hub.publish(
                kind="HERMES_CONTROL_READY",
                packet_id=recorded.operation_id,
                cell_id="cell-demo",
                session_id=SESSION,
            )
            assert operations.acknowledge(
                recorded.operation_id, session_id=SESSION
            )

            await hub.publish_pending()

            state = database.scalar(
                "SELECT state FROM channel_events WHERE packet_id = ?",
                (recorded.operation_id,),
            )
            assert str(state) == "superseded"
            repairs = [
                receipt
                for receipt in operations.pending_for_session(SESSION)
                if receipt.kind == "intake.dedup_repaired"
            ]
            assert len(repairs) == 1
            assert repairs[0].result == {"superseded_events": 1}
        finally:
            await hub.stop()
