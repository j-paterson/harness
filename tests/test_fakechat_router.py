"""FakechatWakeRouter: signal announced, unleased deliveries only."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.fakechat_router import FakechatWakeRouter

CELL = "c" * 8
BINDING = "b" * 8
SURFACE = "s" * 8


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@dataclass
class _Outcome:
    status: str
    detail: str = ""


@dataclass
class _Call:
    session_id: str
    kind: str
    packet_id: str
    delivery_id: str


@dataclass
class _StubSender:
    """Records every call; returns a scripted outcome (or raises) per id."""

    outcomes: dict[str, str] = field(default_factory=dict)
    raise_for: frozenset[str] = frozenset()
    calls: list[_Call] = field(default_factory=list)

    def send(
        self,
        *,
        session_id: str,
        kind: str,
        packet_id: str,
        delivery_id: str,
    ) -> _Outcome:
        self.calls.append(
            _Call(
                session_id=session_id,
                kind=kind,
                packet_id=packet_id,
                delivery_id=delivery_id,
            )
        )
        if delivery_id in self.raise_for:
            raise RuntimeError(f"boom for {delivery_id}")
        status = self.outcomes.get(delivery_id, "delivered")
        return _Outcome(status=status)


def _insert_port(
    database: Database,
    *,
    session_id: str,
    state: str = "active",
    cell_id: str = CELL,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO fakechat_signal_ports("
            "session_id, cell_id, binding_id, generation, port, "
            "state, created_at, retired_at"
            ") VALUES (?, ?, ?, 1, 4000, ?, '2026-08-29T00:00:00+00:00', "
            "?)",
            (
                session_id,
                cell_id,
                BINDING,
                state,
                None if state == "active" else "2026-08-29T00:00:00+00:00",
            ),
        )


def _insert_delivery(
    database: Database,
    *,
    delivery_id: str,
    session_id: str,
    kind: str = "HERMES_WORK_READY",
    packet_id: str = "a" * 32,
    state: str = "announced",
    updated_at: str = "2026-08-29T00:00:00+00:00",
    cell_id: str = CELL,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO lead_intake_deliveries("
            "delivery_id, kind, packet_id, cell_id, session_id, "
            "surface_uuid, state, attempts, claimed_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
            (
                delivery_id,
                kind,
                packet_id,
                cell_id,
                session_id,
                SURFACE,
                state,
                updated_at,
                updated_at,
            ),
        )


def _delivery_state(database: Database, delivery_id: str) -> str:
    row = database.execute(
        "SELECT state FROM lead_intake_deliveries WHERE delivery_id = ?",
        (delivery_id,),
    ).fetchone()
    assert row is not None
    return str(row["state"])


class TestSignalPending:
    def test_announced_with_active_port_is_signalled(
        self, database: Database
    ) -> None:
        session_id = "sess-1"
        delivery_id = "d" * 32
        packet_id = "e" * 32
        _insert_port(database, session_id=session_id)
        _insert_delivery(
            database,
            delivery_id=delivery_id,
            session_id=session_id,
            kind="HERMES_WORK_READY",
            packet_id=packet_id,
        )
        sender = _StubSender()
        router = FakechatWakeRouter(database=database, sender=sender)

        result = router.signal_pending()

        assert len(sender.calls) == 1
        call = sender.calls[0]
        assert call.session_id == session_id
        assert call.kind == "HERMES_WORK_READY"
        assert call.packet_id == packet_id
        assert call.delivery_id == delivery_id
        assert result == (delivery_id,)

    @pytest.mark.parametrize("state", ["offered", "delivered", "superseded"])
    def test_terminal_or_leased_states_are_not_signalled(
        self, database: Database, state: str
    ) -> None:
        session_id = "sess-1"
        delivery_id = "d" * 32
        _insert_port(database, session_id=session_id)
        _insert_delivery(
            database,
            delivery_id=delivery_id,
            session_id=session_id,
            state=state,
        )
        sender = _StubSender()
        router = FakechatWakeRouter(database=database, sender=sender)

        result = router.signal_pending()

        assert sender.calls == []
        assert result == ()

    def test_no_active_port_row_is_not_signalled(
        self, database: Database
    ) -> None:
        session_id = "sess-1"
        delivery_id = "d" * 32
        _insert_delivery(
            database, delivery_id=delivery_id, session_id=session_id
        )
        sender = _StubSender()
        router = FakechatWakeRouter(database=database, sender=sender)

        assert router.signal_pending() == ()
        assert sender.calls == []

    def test_retired_port_row_is_not_signalled(
        self, database: Database
    ) -> None:
        session_id = "sess-1"
        delivery_id = "d" * 32
        _insert_port(database, session_id=session_id, state="retired")
        _insert_delivery(
            database, delivery_id=delivery_id, session_id=session_id
        )
        sender = _StubSender()
        router = FakechatWakeRouter(database=database, sender=sender)

        assert router.signal_pending() == ()
        assert sender.calls == []

    @pytest.mark.parametrize("status", ["failed", "refused"])
    def test_failed_or_refused_outcome_leaves_row_untouched(
        self, database: Database, status: str
    ) -> None:
        session_id = "sess-1"
        delivery_id = "d" * 32
        _insert_port(database, session_id=session_id)
        _insert_delivery(
            database, delivery_id=delivery_id, session_id=session_id
        )
        sender = _StubSender(outcomes={delivery_id: status})
        router = FakechatWakeRouter(database=database, sender=sender)

        result = router.signal_pending()

        assert result == ()
        assert len(sender.calls) == 1
        assert _delivery_state(database, delivery_id) == "announced"

    def test_a_raising_sender_does_not_block_other_rows(
        self, database: Database
    ) -> None:
        session_a, session_b = "sess-a", "sess-b"
        delivery_a, delivery_b = "a" * 32, "b" * 32
        _insert_port(database, session_id=session_a, cell_id="cell-a")
        _insert_port(database, session_id=session_b, cell_id="cell-b")
        _insert_delivery(
            database,
            delivery_id=delivery_a,
            session_id=session_a,
            updated_at="2026-08-29T00:00:00+00:00",
        )
        _insert_delivery(
            database,
            delivery_id=delivery_b,
            session_id=session_b,
            updated_at="2026-08-29T00:00:01+00:00",
        )
        sender = _StubSender(raise_for=frozenset({delivery_a}))
        router = FakechatWakeRouter(database=database, sender=sender)

        result = router.signal_pending()

        assert len(sender.calls) == 2
        assert result == (delivery_b,)
        assert _delivery_state(database, delivery_a) == "announced"
        assert _delivery_state(database, delivery_b) == "announced"

    def test_pass_is_bounded_oldest_updated_at_first(
        self, database: Database
    ) -> None:
        total = 40
        delivery_ids = [f"{i:032x}" for i in range(total)]
        for index, delivery_id in enumerate(delivery_ids):
            session_id = f"sess-{index}"
            _insert_port(database, session_id=session_id, cell_id=f"cell-{index}")
            _insert_delivery(
                database,
                delivery_id=delivery_id,
                session_id=session_id,
                packet_id="a" * 32,
                updated_at=f"2026-08-29T00:00:{index:02d}+00:00",
            )
        sender = _StubSender()
        router = FakechatWakeRouter(database=database, sender=sender)

        result = router.signal_pending()

        assert len(sender.calls) == 32
        assert len(result) == 32
        assert [call.delivery_id for call in sender.calls] == delivery_ids[:32]


class _Outbox:
    def __init__(self) -> None:
        self._listeners: list = []

    def subscribe(self, listener) -> None:
        self._listeners.append(listener)

    def commit(self, packet: object = None) -> None:
        for listener in self._listeners:
            listener(packet)


class TestAttachAndCommitSignal:
    @pytest.mark.asyncio
    async def test_a_commit_signal_triggers_exactly_one_routed_pass(
        self, database: Database
    ) -> None:
        session_id = "sess-1"
        delivery_id = "d" * 32
        _insert_port(database, session_id=session_id)
        _insert_delivery(
            database, delivery_id=delivery_id, session_id=session_id
        )
        sender = _StubSender()
        router = FakechatWakeRouter(database=database, sender=sender)
        outbox = _Outbox()
        router.attach(outbox)

        outbox.commit()
        assert router._tasks
        await asyncio.gather(*router._tasks)

        assert len(sender.calls) == 1
        assert sender.calls[0].delivery_id == delivery_id

    def test_on_commit_outside_a_running_loop_is_a_no_op(
        self, database: Database
    ) -> None:
        sender = _StubSender()
        router = FakechatWakeRouter(database=database, sender=sender)
        outbox = _Outbox()
        router.attach(outbox)

        outbox.commit()

        assert sender.calls == []
        assert router._tasks == set()
