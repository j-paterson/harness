from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.lead_wakes import (
    WAKE_SCHEMA_VERSION,
    LeadTerminalWakes,
    TerminalWake,
    TerminalWakeInput,
)

SESSION_ID = UUID("11111111-1111-4111-8111-111111111111")


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


def build_wakes(
    database: Database, *, ids: Iterator[str] | None = None
) -> LeadTerminalWakes:
    generator = ids or iter(f"wake-{index}" for index in range(1, 100))
    return LeadTerminalWakes(
        database=database,
        events=EventStore(database),
        now=lambda: datetime(2026, 8, 28, 12, 0, tzinfo=UTC),
        ids=lambda: next(generator),
    )


def completed_turn(**overrides: object) -> TerminalWakeInput:
    fields: dict[str, object] = {
        "project_key": "demo",
        "issue_id": "INFRA-181",
        "cell_id": "cell-demo",
        "session_id": SESSION_ID,
        "profile_alias": "max-a",
        "turn_key": "evt-terminal-1",
        "kind": "completed",
        "reason": "turn_completed",
    }
    fields.update(overrides)
    return TerminalWakeInput(**fields)  # type: ignore[arg-type]


def wake_events(database: Database, event_type: str) -> list[str]:
    rows = database.execute(
        "SELECT aggregate_id FROM events WHERE event_type = ? ORDER BY sequence",
        (event_type,),
    ).fetchall()
    return [str(row["aggregate_id"]) for row in rows]


def test_completed_turn_commits_one_durable_deduplicated_wake(
    database: Database,
) -> None:
    wakes = build_wakes(database)
    first = wakes.commit(completed_turn())
    duplicate = wakes.commit(completed_turn())

    assert first.wake_id == duplicate.wake_id
    assert first.state == "pending"
    assert first.schema_version == WAKE_SCHEMA_VERSION
    assert first.kind == "completed"
    assert first.session_id == str(SESSION_ID)
    rows = database.execute("SELECT * FROM lead_terminal_wakes").fetchall()
    assert len(rows) == 1
    assert wake_events(database, "lead_wake.committed") == [first.wake_id]


def test_same_turn_distinct_kind_commits_a_second_wake(database: Database) -> None:
    wakes = build_wakes(database)
    completed = wakes.commit(completed_turn())
    capped = wakes.commit(
        completed_turn(
            kind="provider_capped",
            reason="subscription_limit:fable",
            reset_at="2026-08-28T13:00:00+00:00",
        )
    )

    assert completed.wake_id != capped.wake_id
    assert capped.reset_at == "2026-08-28T13:00:00+00:00"
    assert len(wakes.pending("demo")) == 2


def test_commit_rejects_an_unknown_kind(database: Database) -> None:
    wakes = build_wakes(database)
    with pytest.raises(ValueError):
        wakes.commit(completed_turn(kind="celebrated"))


def test_commit_signals_each_new_wake_exactly_once(database: Database) -> None:
    wakes = build_wakes(database)
    delivered: list[TerminalWake] = []
    wakes.subscribe(delivered.append)

    first = wakes.commit(completed_turn())
    wakes.commit(completed_turn())

    assert [wake.wake_id for wake in delivered] == [first.wake_id]
    # The signal fires only after the durable commit: the row already exists.
    assert delivered[0].state == "pending"
    assert len(wakes.pending("demo")) == 1


def test_listener_failure_does_not_lose_the_durable_wake(
    database: Database,
) -> None:
    wakes = build_wakes(database)

    def explode(wake: TerminalWake) -> None:
        raise RuntimeError("delivery target is unavailable")

    wakes.subscribe(explode)
    wake = wakes.commit(completed_turn())

    assert wake.state == "pending"
    assert [pending.wake_id for pending in wakes.replay_undelivered()] == [
        wake.wake_id
    ]


def test_undelivered_wake_replays_after_restart_exactly_once(
    tmp_path: Path,
) -> None:
    first_database = Database.open(tmp_path / "state.db")
    try:
        wakes = build_wakes(first_database)
        committed = wakes.commit(completed_turn())
    finally:
        first_database.close()

    reopened = Database.open(tmp_path / "state.db")
    try:
        replayer = build_wakes(reopened)
        replayed = replayer.replay_undelivered()
        assert [wake.wake_id for wake in replayed] == [committed.wake_id]

        delivered = replayer.mark_delivered(committed.wake_id)
        assert delivered.state == "delivered"
        assert delivered.delivered_at is not None
        assert replayer.replay_undelivered() == ()
        assert wake_events(reopened, "lead_wake.delivered") == [committed.wake_id]
    finally:
        reopened.close()


def test_mark_delivered_is_idempotent(database: Database) -> None:
    wakes = build_wakes(database)
    wake = wakes.commit(completed_turn())

    first = wakes.mark_delivered(wake.wake_id)
    second = wakes.mark_delivered(wake.wake_id)

    assert first.state == second.state == "delivered"
    assert first.delivered_at == second.delivered_at
    assert wake_events(database, "lead_wake.delivered") == [wake.wake_id]


def test_mark_delivered_rejects_an_unknown_wake(database: Database) -> None:
    wakes = build_wakes(database)
    with pytest.raises(KeyError):
        wakes.mark_delivered("wake-missing")


def test_pending_filters_by_project(database: Database) -> None:
    wakes = build_wakes(database)
    demo = wakes.commit(completed_turn())
    wakes.commit(
        completed_turn(project_key="other", cell_id="cell-other", turn_key="evt-9")
    )

    assert [wake.wake_id for wake in wakes.pending("demo")] == [demo.wake_id]
    assert len(wakes.pending()) == 2


class RecordingTransport:
    """Async delivery transport with scriptable per-call outcomes."""

    def __init__(self, outcomes: list[bool] | None = None) -> None:
        self.delivered: list[str] = []
        self.outcomes = outcomes

    async def __call__(self, wake: TerminalWake) -> bool:
        outcome = True if not self.outcomes else self.outcomes.pop(0)
        if outcome:
            self.delivered.append(wake.wake_id)
        return outcome


@pytest.mark.asyncio
async def test_commit_sets_the_delivery_signal(database: Database) -> None:
    from hermes_orchestrator.lead_wakes import LeadWakeDelivery

    wakes = build_wakes(database)
    delivery = LeadWakeDelivery(wakes)

    assert not delivery.signal.is_set()
    wakes.commit(completed_turn())
    assert delivery.signal.is_set()


@pytest.mark.asyncio
async def test_drain_delivers_pending_wakes_and_acknowledges(
    database: Database,
) -> None:
    from hermes_orchestrator.lead_wakes import LeadWakeDelivery

    wakes = build_wakes(database)
    transport = RecordingTransport()
    delivery = LeadWakeDelivery(wakes, transport=transport)
    first = wakes.commit(completed_turn())
    second = wakes.commit(completed_turn(turn_key="evt-terminal-2"))

    delivered = await delivery.drain()

    assert delivered == (first.wake_id, second.wake_id)
    assert not delivery.signal.is_set()
    assert wakes.pending() == ()
    assert await delivery.drain() == ()


@pytest.mark.asyncio
async def test_drain_without_transport_keeps_rows_pending(
    database: Database,
) -> None:
    from hermes_orchestrator.lead_wakes import LeadWakeDelivery

    wakes = build_wakes(database)
    delivery = LeadWakeDelivery(wakes)
    wake = wakes.commit(completed_turn())

    assert await delivery.drain() == ()
    assert not delivery.signal.is_set()
    assert [pending.wake_id for pending in wakes.pending()] == [wake.wake_id]


@pytest.mark.asyncio
async def test_failed_transport_leaves_the_row_pending_for_retry(
    database: Database,
) -> None:
    from hermes_orchestrator.lead_wakes import LeadWakeDelivery

    wakes = build_wakes(database)
    transport = RecordingTransport(outcomes=[False, True])
    delivery = LeadWakeDelivery(wakes, transport=transport)
    wake = wakes.commit(completed_turn())

    assert await delivery.drain() == ()
    assert [pending.wake_id for pending in wakes.pending()] == [wake.wake_id]
    assert await delivery.drain() == (wake.wake_id,)
    assert wakes.pending() == ()


@pytest.mark.asyncio
async def test_replay_startup_signals_undelivered_rows_once(
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.lead_wakes import LeadWakeDelivery

    first_database = Database.open(tmp_path / "state.db")
    try:
        build_wakes(first_database).commit(completed_turn())
    finally:
        first_database.close()

    reopened = Database.open(tmp_path / "state.db")
    try:
        wakes = build_wakes(reopened)
        transport = RecordingTransport()
        delivery = LeadWakeDelivery(wakes, transport=transport)

        assert delivery.replay_startup() == 1
        assert delivery.signal.is_set()
        assert len(await delivery.drain()) == 1
        assert delivery.replay_startup() == 0
        assert not delivery.signal.is_set()
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_blocked_and_capped_commits_do_not_arm_the_signal(
    database: Database,
) -> None:
    from hermes_orchestrator.lead_wakes import LeadWakeDelivery

    wakes = build_wakes(database)
    delivery = LeadWakeDelivery(wakes)

    wakes.commit(completed_turn(kind="blocked", reason="start_unconfirmed"))
    wakes.commit(
        completed_turn(
            kind="provider_capped",
            reason="subscription_limit:fable",
            turn_key="evt-terminal-2",
        )
    )

    # A fast-failing lead start must keep the supervisor interval as its
    # retry backoff; only forward progress ends the wait early.
    assert not delivery.signal.is_set()
    assert len(wakes.pending()) == 2
    wakes.commit(completed_turn(turn_key="evt-terminal-3"))
    assert delivery.signal.is_set()


@pytest.mark.asyncio
async def test_raising_transport_ends_the_pass_without_escaping(
    database: Database,
) -> None:
    from hermes_orchestrator.lead_wakes import LeadWakeDelivery

    wakes = build_wakes(database)

    async def explode(wake: TerminalWake) -> bool:
        raise RuntimeError("transport is down")

    delivery = LeadWakeDelivery(wakes, transport=explode)
    wake = wakes.commit(completed_turn())

    assert await delivery.drain() == ()
    assert [pending.wake_id for pending in wakes.pending()] == [wake.wake_id]


class FakeNotifyProcess:
    """Scriptable stand-in for the spawned Hermes consumer command."""

    def __init__(self, *, returncode: int = 0, hang: bool = False) -> None:
        self.returncode = returncode
        self._hang = hang
        self.stdin_payload: bytes | None = None
        self.killed = False

    async def communicate(
        self, data: bytes | None = None
    ) -> tuple[bytes, bytes]:
        self.stdin_payload = data
        if self._hang:
            import asyncio

            await asyncio.sleep(3600)
        return b"", b""

    def kill(self) -> None:
        self.killed = True

    async def wait(self) -> int:
        return self.returncode


def notify_factory(process: FakeNotifyProcess) -> tuple[object, list[tuple]]:
    calls: list[tuple] = []

    async def factory(*argv: str, **kwargs: object) -> FakeNotifyProcess:
        calls.append(argv)
        return process

    return factory, calls


@pytest.mark.asyncio
async def test_command_transport_pushes_wake_id_and_accepts_zero_exit(
    database: Database,
) -> None:
    import json

    from hermes_orchestrator.lead_wakes import CommandWakeTransport

    wake = build_wakes(database).commit(completed_turn())
    process = FakeNotifyProcess(returncode=0)
    factory, calls = notify_factory(process)
    transport = CommandWakeTransport(
        ("hermes-notify", "--wake"), process_factory=factory
    )

    assert await transport(wake) is True
    assert calls == [("hermes-notify", "--wake")]
    assert process.stdin_payload is not None
    payload = json.loads(process.stdin_payload.decode("utf-8"))
    # wake_id is the consumer's idempotency key; the payload carries
    # orchestration metadata only.
    assert payload["wake_id"] == wake.wake_id
    assert payload["kind"] == "completed"
    assert payload["turn_key"] == "evt-terminal-1"
    assert set(payload) == set(wake.as_dict())


@pytest.mark.asyncio
async def test_command_transport_rejects_nonzero_exit(
    database: Database,
) -> None:
    from hermes_orchestrator.lead_wakes import CommandWakeTransport

    wake = build_wakes(database).commit(completed_turn())
    factory, _ = notify_factory(FakeNotifyProcess(returncode=1))
    transport = CommandWakeTransport(("hermes-notify",), process_factory=factory)

    assert await transport(wake) is False


@pytest.mark.asyncio
async def test_command_transport_timeout_kills_and_reports_unaccepted(
    database: Database,
) -> None:
    from hermes_orchestrator.lead_wakes import CommandWakeTransport

    wake = build_wakes(database).commit(completed_turn())
    process = FakeNotifyProcess(hang=True)
    factory, _ = notify_factory(process)
    transport = CommandWakeTransport(
        ("hermes-notify",), process_factory=factory, timeout_seconds=0.01
    )

    assert await transport(wake) is False
    assert process.killed


def test_command_transport_requires_a_command() -> None:
    from hermes_orchestrator.lead_wakes import CommandWakeTransport

    with pytest.raises(ValueError):
        CommandWakeTransport(())
    with pytest.raises(ValueError):
        CommandWakeTransport(("hermes-notify",), timeout_seconds=0)
