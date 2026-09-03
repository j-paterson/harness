from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
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


def test_decision_resolved_is_an_accepted_kind(database: Database) -> None:
    """INFRA-224: an operator resolution is a terminal boundary for
    whichever lane paused on it, so ``decision_resolved`` is a WAKE_KINDS
    member accepted by ``commit`` like every other terminal kind."""

    wakes = build_wakes(database)

    wake = wakes.commit(
        completed_turn(
            kind="decision_resolved",
            turn_key="decision:dec-1",
            reason="decision dec-1 resolved approved: go ahead (next: resume)",
        )
    )

    assert wake.kind == "decision_resolved"
    assert wake.state == "pending"
    assert len(wakes.pending("demo")) == 1


def test_decision_resolved_commit_dedups_on_the_same_turn_key(
    database: Database,
) -> None:
    wakes = build_wakes(database)

    first = wakes.commit(
        completed_turn(kind="decision_resolved", turn_key="decision:dec-1")
    )
    duplicate = wakes.commit(
        completed_turn(
            kind="decision_resolved",
            turn_key="decision:dec-1",
            reason="a second, differently worded resolution attempt",
        )
    )

    assert first.wake_id == duplicate.wake_id
    assert duplicate.reason == first.reason
    assert len(wakes.pending("demo")) == 1
    assert wake_events(database, "lead_wake.committed") == [first.wake_id]


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


WAKE_NOW = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)


STOPPED_AT = (WAKE_NOW + timedelta(minutes=1)).isoformat()


def seed_work_ready(database: Database, **overrides: object) -> None:
    """An ACTIVE development seat, IDLE by its own durable proof (its
    latest assignment acknowledged, no child or continuation
    outstanding), one queued admitted issue, and a fresh green resource
    sample: every predicate of ``commit_work_ready`` satisfied."""

    now = WAKE_NOW.isoformat()
    seed: dict[str, object] = {
        "lane_role": "development",
        "cell_state": "active",
        "assignment_state": "acknowledged",
        "issue_id": "INFRA-211",
        "priority": 1,
        "dependency_ready": 1,
        "pressure": "green",
        # A Stop strictly newer than the acknowledgement above is the
        # only positive proof the turn ended; None models a foreground
        # turn still running.
        "stopped_at": STOPPED_AT,
    }
    seed.update(overrides)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells(cell_id, project_key, lane_role, state, "
            "profile_alias, session_id, created_at, updated_at) VALUES "
            "('cell-demo', 'demo', ?, ?, 'max-a', ?, ?, ?)",
            (seed["lane_role"], seed["cell_state"], str(SESSION_ID), now, now),
        )
        connection.execute(
            "INSERT INTO admitted_issues(issue_id, project_key, priority, state, "
            "instruction_id, dependency_ready, overlap_risk, admitted_at, "
            "updated_at) VALUES (?, 'demo', ?, 'queued', 'chat-1', ?, 0, ?, ?)",
            (seed["issue_id"], seed["priority"], seed["dependency_ready"], now, now),
        )
        connection.execute(
            "INSERT INTO lead_assignments(assignment_id, schema_version, "
            "project_key, issue_id, cell_id, session_id, profile_alias, "
            "instruction_id, queue_transition, state, created_at, updated_at, "
            "acknowledged_at) VALUES ('assign-prior', 1, 'demo', 'INFRA-210', "
            "'cell-demo', ?, 'max-a', 'chat-0', 'queued->in_development', ?, "
            "?, ?, ?)",
            (
                str(SESSION_ID),
                seed["assignment_state"],
                now,
                now,
                # A published packet is handed over but NOT yet consumed,
                # so it carries no acknowledgement stamp.
                now if seed["assignment_state"] == "acknowledged" else None,
            ),
        )
        connection.execute(
            "INSERT INTO resource_samples(sample_id, sampled_at, pressure, "
            "available_memory_bytes, total_memory_bytes, swap_used_bytes, "
            "load_one, logical_cpus, disk_json, managed_rss_bytes) "
            "VALUES ('sample-1', ?, ?, 1, 2, 0, 0.1, 8, '{}', 0)",
            (now, seed["pressure"]),
        )
        if seed["stopped_at"] is not None:
            connection.execute(
                "INSERT INTO events(event_id, event_type, aggregate_type, "
                "aggregate_id, occurred_at, actor, payload_json) VALUES "
                "('evt-stop', 'lead_turn.stopped', 'lead_session', ?, ?, "
                "'lead', '{}')",
                (str(SESSION_ID), seed["stopped_at"]),
            )




def test_an_idle_seat_with_runnable_work_wakes_exactly_once(
    database: Database,
) -> None:
    """Every other kind fires because a turn ENDED, so an idle seat was
    never told safe work remained. The turn key is the runnable SET, so
    re-evaluating an unchanged condition commits nothing."""

    seed_work_ready(database)
    wakes = build_wakes(database)

    first = wakes.commit_work_ready("demo", freshness_minutes=5)
    again = wakes.commit_work_ready("demo", freshness_minutes=5)

    assert first is not None
    assert first.kind == "work_ready"
    assert again is not None
    assert again.wake_id == first.wake_id
    assert wake_events(database, "lead_wake.committed") == ["wake-1"]


def test_work_ready_wake_lists_every_runnable_issue_up_to_headroom(
    database: Database,
) -> None:
    """INFRA-215: an idle seat with more admitted runnable work than one
    lane can hold is told about EVERY issue safe to pick up right now,
    capped at the development-lane headroom -- not merely the first
    runnable issue it finds."""

    seed_work_ready(database, issue_id="INFRA-211")
    now = WAKE_NOW.isoformat()
    with database.transaction() as connection:
        # Six more queued, dependency-ready issues -- seven runnable in
        # total together with INFRA-211 above.
        for index in range(212, 218):
            connection.execute(
                "INSERT INTO admitted_issues(issue_id, project_key, "
                "priority, state, instruction_id, dependency_ready, "
                "overlap_risk, admitted_at, updated_at) VALUES "
                "(?, 'demo', 1, 'queued', ?, 1, 0, ?, ?)",
                (f"INFRA-{index}", f"chat-{index}", now, now),
            )
        # One issue already occupying a development lane: headroom is
        # therefore 6 - 1 = 5, strictly less than the 7 runnable issues.
        connection.execute(
            "INSERT INTO admitted_issues(issue_id, project_key, "
            "priority, state, instruction_id, dependency_ready, "
            "overlap_risk, admitted_at, updated_at) VALUES "
            "('INFRA-200', 'demo', 1, 'in_development', 'chat-200', 1, 0, "
            "?, ?)",
            (now, now),
        )
    wakes = build_wakes(database)

    wake = wakes.commit_work_ready("demo", freshness_minutes=5)

    assert wake is not None
    assert wake.kind == "work_ready"
    expected = ("INFRA-211", "INFRA-212", "INFRA-213", "INFRA-214", "INFRA-215")
    assert wake.issue_id == expected[0]
    assert tuple(wake.turn_key.removeprefix("work-ready:").split(",")) == expected


def _start_a_child(database: Database) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO lead_children(session_id, child_id, state, started_at) "
            "VALUES (?, 'child-1', 'started', ?)",
            (str(SESSION_ID), WAKE_NOW.isoformat()),
        )


def _promise_a_continuation(database: Database) -> None:
    now = WAKE_NOW.isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO lead_continuations(continuation_id, session_id, "
            "project_key, cell_id, condition, state, created_at, updated_at) "
            "VALUES ('cont-1', ?, 'demo', 'cell-demo', '1 outstanding', "
            "'waiting', ?, ?)",
            (str(SESSION_ID), now, now),
        )


def test_an_active_foreground_turn_is_never_treated_as_idle(
    database: Database,
) -> None:
    """Sol acce71fc: a lead running its OWN turn has an acknowledged
    assignment and zero child/continuation rows -- the same durable
    shape as an idle seat, because those tables track only background
    work around Stop. Without a Stop newer than that turn's start,
    nothing may be committed."""

    seed_work_ready(database, stopped_at=None)
    wakes = build_wakes(database)

    assert wakes.commit_work_ready("demo", freshness_minutes=5) is None
    assert wake_events(database, "lead_wake.committed") == []


def test_the_same_turn_wakes_once_after_it_reaches_stop(
    database: Database,
) -> None:
    """The other half: once that identical seat records the existing
    unconditional Stop, the otherwise-unchanged runnable work commits
    and delivers exactly one wake."""

    seed_work_ready(database)
    wakes = build_wakes(database)

    wake = wakes.commit_work_ready("demo", freshness_minutes=5)

    assert wake is not None
    assert wake.kind == "work_ready"
    assert wake_events(database, "lead_wake.committed") == [wake.wake_id]


# Sol 6c08a91e: a channel event created BEFORE the Stop in
# ``seed_work_ready``, so only its durable publication/acknowledgement
# transition can order it after that Stop.
CHANNEL_CONSUMED_AT = (WAKE_NOW + timedelta(minutes=2)).isoformat()
STOPPED_AGAIN_AT = (WAKE_NOW + timedelta(minutes=3)).isoformat()


def _seed_channel_event(
    database: Database,
    *,
    state: str,
    published_at: str | None = None,
    acked_at: str | None = None,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO channel_events(event_id, kind, packet_id, cell_id, "
            "session_id, state, attempts, created_at, updated_at, "
            "published_at, acked_at) VALUES ('chan-1', "
            "'HERMES_CORRECTION_READY', 'packet-1', 'cell-demo', ?, ?, 1, "
            "?, ?, ?, ?)",
            (
                str(SESSION_ID),
                state,
                WAKE_NOW.isoformat(),
                acked_at or published_at or WAKE_NOW.isoformat(),
                published_at,
                acked_at,
            ),
        )


def _record_a_newer_stop(database: Database) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO events(event_id, event_type, aggregate_type, "
            "aggregate_id, occurred_at, actor, payload_json) VALUES "
            "('evt-stop-2', 'lead_turn.stopped', 'lead_session', ?, ?, "
            "'lead', '{}')",
            (str(SESSION_ID), STOPPED_AGAIN_AT),
        )


def test_a_channel_event_published_after_stop_keeps_the_seat_busy(
    database: Database,
) -> None:
    """Sol 6c08a91e: ``created_at`` is when the row was written, not
    when the input reached a lead. An event created before the last
    Stop but PUBLISHED after it starts a turn that Stop cannot have
    ended, so ordering by creation let a stale Stop look newest while a
    live turn ran."""

    seed_work_ready(database)
    _seed_channel_event(
        database, state="published", published_at=CHANNEL_CONSUMED_AT
    )
    wakes = build_wakes(database)

    assert wakes.commit_work_ready("demo", freshness_minutes=5) is None
    assert wake_events(database, "lead_wake.committed") == []


def test_an_acknowledged_channel_event_keeps_the_seat_busy(
    database: Database,
) -> None:
    """Acknowledgement is what hands the packet to a foreground turn,
    yet it used to drop the event out of a ``state = 'published'``
    query at exactly that moment -- erasing the newest turn start on
    the strongest evidence one had occurred. It stays authoritative
    until a Stop newer than the acknowledgement arrives."""

    seed_work_ready(database)
    _seed_channel_event(database, state="acked", acked_at=CHANNEL_CONSUMED_AT)
    wakes = build_wakes(database)

    assert wakes.commit_work_ready("demo", freshness_minutes=5) is None
    assert wake_events(database, "lead_wake.committed") == []


def test_a_stop_newer_than_the_acknowledgement_wakes_once(
    database: Database,
) -> None:
    """The other half: the otherwise-identical seat, once it records a
    Stop newer than that acknowledgement, is genuinely idle and commits
    exactly one ``work_ready`` wake."""

    seed_work_ready(database)
    _seed_channel_event(database, state="acked", acked_at=CHANNEL_CONSUMED_AT)
    _record_a_newer_stop(database)
    wakes = build_wakes(database)

    wake = wakes.commit_work_ready("demo", freshness_minutes=5)

    assert wake is not None
    assert wake.kind == "work_ready"
    assert wake_events(database, "lead_wake.committed") == [wake.wake_id]


def test_an_unacknowledged_published_assignment_blocks_the_wake(
    database: Database,
) -> None:
    """Sol 3c73856e: a ``published`` assignment is work already handed
    to this lead and not yet consumed. Until it is acknowledged it
    contributes no start timestamp, and neither does its channel event
    while that event is still ``pending`` -- so an OLDER Stop won the
    ordering and a second ``work_ready`` landed on a seat that already
    held unconsumed work."""

    seed_work_ready(database, assignment_state="published")
    _seed_channel_event(database, state="pending")
    wakes = build_wakes(database)

    assert wakes.commit_work_ready("demo", freshness_minutes=5) is None
    assert wake_events(database, "lead_wake.committed") == []


def test_an_acknowledged_assignment_and_newer_stop_wakes_once(
    database: Database,
) -> None:
    """The other half: once that same assignment is acknowledged and
    the foreground turn it started reaches a Stop newer than the
    consumption, the otherwise-identical runnable work commits and
    delivers exactly one wake."""

    seed_work_ready(database, assignment_state="acknowledged")
    _seed_channel_event(database, state="acked", acked_at=CHANNEL_CONSUMED_AT)
    _record_a_newer_stop(database)
    wakes = build_wakes(database)

    wake = wakes.commit_work_ready("demo", freshness_minutes=5)

    assert wake is not None
    assert wake.kind == "work_ready"
    assert wake_events(database, "lead_wake.committed") == [wake.wake_id]


# Older than the assignment `seed_work_ready` writes, so recency
# ordering puts this one second and a LIMIT 1 guard never sees it.
ASSIGNED_EARLIER_AT = (WAKE_NOW - timedelta(minutes=1)).isoformat()


def _seed_an_older_assignment(
    database: Database, *, state: str, acknowledged_at: str | None = None
) -> None:
    """A second LIVE assignment on the same session, for a different
    issue. ``lead_assignments_live`` is unique by ``(issue_id,
    session_id)``, so this is an ordinary multi-lane seat, not a
    conflict with the row ``seed_work_ready`` already wrote."""

    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO lead_assignments(assignment_id, schema_version, "
            "project_key, issue_id, cell_id, session_id, profile_alias, "
            "instruction_id, queue_transition, state, created_at, "
            "updated_at, acknowledged_at) VALUES ('assign-older', 1, "
            "'demo', 'INFRA-209', 'cell-demo', ?, 'max-a', 'chat-x', "
            "'queued->in_development', ?, ?, ?, ?)",
            (
                str(SESSION_ID),
                state,
                ASSIGNED_EARLIER_AT,
                acknowledged_at or ASSIGNED_EARLIER_AT,
                acknowledged_at,
            ),
        )


def test_an_older_published_assignment_is_not_hidden_by_a_newer_ack(
    database: Database,
) -> None:
    """Sol 62424477: one session holds several live assignments at once,
    so asking only for its NEWEST row let a newer acknowledged
    assignment hide an older still-unconsumed published packet, and the
    seat looked idle while holding work it had never taken up."""

    seed_work_ready(database, assignment_state="acknowledged")
    _seed_an_older_assignment(database, state="published")
    wakes = build_wakes(database)

    assert wakes.commit_work_ready("demo", freshness_minutes=5) is None
    assert wake_events(database, "lead_wake.committed") == []


def test_every_live_assignment_acknowledged_then_a_newer_stop_wakes_once(
    database: Database,
) -> None:
    """The other half: with no published assignment left anywhere on the
    session and a Stop newer than EVERY consumption timestamp, the
    otherwise-identical runnable work commits exactly one wake."""

    seed_work_ready(database, assignment_state="acknowledged")
    _seed_an_older_assignment(
        database, state="acknowledged", acknowledged_at=CHANNEL_CONSUMED_AT
    )
    _record_a_newer_stop(database)
    wakes = build_wakes(database)

    wake = wakes.commit_work_ready("demo", freshness_minutes=5)

    assert wake is not None
    assert wake.kind == "work_ready"
    assert wake_events(database, "lead_wake.committed") == [wake.wake_id]
