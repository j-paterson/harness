"""Verify durable checkpoint-safety evidence and single outstanding requests."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hermes_orchestrator.checkpoints import (
    CheckpointRequests,
    CheckpointSafetyStore,
    SafetyEvidence,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore

NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def safety(database: Database) -> CheckpointSafetyStore:
    return CheckpointSafetyStore(database, EventStore(database), now=lambda: NOW)


@pytest.fixture
def requests(database: Database) -> CheckpointRequests:
    return CheckpointRequests(database, EventStore(database), now=lambda: NOW)


def evidence(
    cell: str = "cell-1", session: str = "s1", ev: str = "evt-1"
) -> SafetyEvidence:
    return SafetyEvidence(cell, session, "safe", "turn_completed", ev, NOW.isoformat())


def test_evidence_is_bound_to_cell_and_session(safety: CheckpointSafetyStore) -> None:
    assert safety.current("cell-1", "s1") is None
    recorded = safety.mark_safe(
        "cell-1", "s1", boundary_kind="turn_completed", evidence_id="evt-1"
    )
    assert safety.current("cell-1", "s1") == recorded
    assert safety.current("cell-1", "s2") is None  # prior/other session rejected
    assert safety.current("cell-2", "s1") is None


def test_invalidation_makes_evidence_unsafe_until_a_new_boundary(
    safety: CheckpointSafetyStore, database: Database
) -> None:
    safety.mark_safe("cell-1", "s1", boundary_kind="turn_completed", evidence_id="e1")
    assert safety.invalidate("cell-1", reason="dispatch:ENG-1") is True
    assert safety.current("cell-1", "s1") is None
    assert safety.invalidate("cell-1", reason="again") is False
    safety.mark_safe("cell-1", "s1", boundary_kind="turn_completed", evidence_id="e2")
    current = safety.current("cell-1", "s1")
    assert current is not None and current.evidence_id == "e2"
    kinds = [
        row["event_type"]
        for row in database.execute(
            "SELECT event_type FROM events WHERE aggregate_id = 'cell-1' "
            "ORDER BY sequence"
        ).fetchall()
    ]
    assert kinds == [
        "checkpoint_safety.safe",
        "checkpoint_safety.invalidated",
        "checkpoint_safety.safe",
    ]
    with pytest.raises(ValueError, match="evidence id"):
        safety.mark_safe("cell-1", "s1", boundary_kind="turn_completed", evidence_id="")


def test_only_one_request_may_be_pending_host_wide(
    requests: CheckpointRequests,
) -> None:
    first = requests.request(evidence(), reason="red")
    assert first is not None and first.state == "pending"
    assert requests.request(evidence(), reason="red") == first  # idempotent
    assert requests.request(evidence("cell-2", "s9", "e9"), reason="red") is None
    assert requests.pending() == first


def test_terminal_transitions_are_explicit_and_unlock_the_next(
    requests: CheckpointRequests,
) -> None:
    first = requests.request(evidence(), reason="red")
    assert first is not None
    with pytest.raises(ValueError, match="outcome"):
        requests.resolve(first.request_id, outcome="done")
    assert requests.resolve(first.request_id, outcome="completed", detail="h1")
    assert requests.resolve(first.request_id, outcome="failed") is False
    assert requests.pending() is None
    assert requests.last_terminal_at() == NOW.isoformat()
    # The same proven boundary can never be requested twice.
    assert requests.request(evidence(), reason="red") is None
    nxt = requests.request(evidence("cell-2", "s2", "e2"), reason="red")
    assert nxt is not None and nxt.state == "pending"
    assert requests.resolve_for_cell("cell-9", outcome="failed") is False
    assert requests.resolve_for_cell("cell-2", outcome="stale", detail="rotated")
    assert requests.get(nxt.request_id).state == "stale"  # type: ignore[union-attr]


def test_pending_request_survives_restart(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    database = Database.open(path)
    try:
        CheckpointRequests(database, EventStore(database)).request(
            evidence(), reason="red"
        )
    finally:
        database.close()
    reopened = Database.open(path)
    try:
        restarted = CheckpointRequests(reopened, EventStore(reopened))
        pending = restarted.pending()
        assert pending is not None and pending.cell_id == "cell-1"
        assert restarted.request(evidence("cell-2", "s2", "e2"), reason="red") is None
    finally:
        reopened.close()


def test_fresh_sample_rule_uses_last_terminal_time(
    requests: CheckpointRequests,
) -> None:
    assert requests.last_terminal_at() is None
    first = requests.request(evidence(), reason="red")
    assert first is not None
    requests.resolve(first.request_id, outcome="completed")
    later = NOW + timedelta(seconds=30)
    assert requests.last_terminal_at() < later.isoformat()  # type: ignore[operator]


# --- delivery: a reserved request never strands the pending slot -----------


import asyncio as _asyncio  # noqa: E402

from hermes_orchestrator.checkpoints import CheckpointDispatcher  # noqa: E402


class Sessions:
    def __init__(self, sessions: dict[str, str | None]) -> None:
        self.sessions = sessions

    def current(self, cell_id: str) -> str | None:
        return self.sessions.get(cell_id)


@pytest.mark.asyncio
async def test_false_after_cell_state_change_resolves_stale(
    requests: CheckpointRequests,
) -> None:
    reserved = requests.request(evidence(), reason="red")
    assert reserved is not None
    calls: list[tuple[str, str]] = []

    def callback(cell_id: str, reason: str) -> bool:
        calls.append((cell_id, reason))
        return False  # the cell left the active state before delivery

    dispatcher = CheckpointDispatcher(
        requests, callback=callback, current_session=Sessions({"cell-1": "s1"}).current
    )
    assert await dispatcher.deliver(reserved.request_id) == "stale"
    assert calls == [("cell-1", "red")]
    assert requests.pending() is None
    stored = requests.get(reserved.request_id)
    assert stored is not None
    assert (stored.state, stored.resolution) == ("stale", "cell_not_active")


@pytest.mark.asyncio
async def test_callback_exception_is_recorded_failed_before_propagating(
    requests: CheckpointRequests,
) -> None:
    reserved = requests.request(evidence(), reason="red")
    assert reserved is not None

    async def callback(cell_id: str, reason: str) -> bool:
        raise RuntimeError("profile token expired: secret")

    dispatcher = CheckpointDispatcher(
        requests, callback=callback, current_session=Sessions({"cell-1": "s1"}).current
    )
    with pytest.raises(RuntimeError):
        await dispatcher.deliver(reserved.request_id)
    stored = requests.get(reserved.request_id)
    assert stored is not None
    assert (stored.state, stored.resolution) == (
        "failed",
        "callback_error:RuntimeError",
    )
    assert requests.pending() is None


@pytest.mark.asyncio
async def test_cancellation_is_recorded_failed_before_propagating(
    requests: CheckpointRequests,
) -> None:
    reserved = requests.request(evidence(), reason="red")
    assert reserved is not None

    async def callback(cell_id: str, reason: str) -> bool:
        raise _asyncio.CancelledError

    dispatcher = CheckpointDispatcher(
        requests, callback=callback, current_session=Sessions({"cell-1": "s1"}).current
    )
    with pytest.raises(_asyncio.CancelledError):
        await dispatcher.deliver(reserved.request_id)
    stored = requests.get(reserved.request_id)
    assert stored is not None and (stored.state, stored.resolution) == (
        "failed",
        "cancelled",
    )


@pytest.mark.asyncio
async def test_missing_callback_never_strands_the_slot(
    requests: CheckpointRequests,
) -> None:
    reserved = requests.request(evidence(), reason="red")
    assert reserved is not None
    dispatcher = CheckpointDispatcher(
        requests, callback=None, current_session=Sessions({"cell-1": "s1"}).current
    )
    assert await dispatcher.deliver(reserved.request_id) == "failed"
    assert requests.pending() is None
    assert requests.get(reserved.request_id).resolution == "no_checkpoint_callback"  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_session_change_at_execution_resolves_stale_without_calling(
    requests: CheckpointRequests,
) -> None:
    reserved = requests.request(evidence(), reason="red")
    assert reserved is not None
    called = []
    dispatcher = CheckpointDispatcher(
        requests,
        callback=lambda cell, reason: called.append(cell) or True,
        current_session=Sessions({"cell-1": "s2"}).current,
    )
    assert await dispatcher.deliver(reserved.request_id) == "stale"
    assert called == []
    assert requests.get(reserved.request_id).resolution == "session_changed:other"  # type: ignore[union-attr]
    gone = requests.request(evidence("cell-3", "s3", "e3"), reason="red")
    assert gone is not None
    dispatcher = CheckpointDispatcher(
        requests, callback=lambda c, r: True, current_session=Sessions({}).current
    )
    assert await dispatcher.deliver(gone.request_id) == "stale"
    assert requests.get(gone.request_id).resolution == "session_changed:none"  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_delivery_is_idempotent_and_survives_restart(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    database = Database.open(path)
    try:
        CheckpointRequests(database, EventStore(database)).request(
            evidence(), reason="red"
        )
    finally:
        database.close()
    # Restart after reserve-before-callback: exactly one delivery, no duplicate.
    reopened = Database.open(path)
    try:
        requests = CheckpointRequests(reopened, EventStore(reopened))
        delivered: list[str] = []
        dispatcher = CheckpointDispatcher(
            requests,
            callback=lambda cell, reason: delivered.append(cell) or True,
            current_session=Sessions({"cell-1": "s1"}).current,
        )
        pending_id = dispatcher.undelivered()
        assert pending_id is not None
        assert await dispatcher.deliver(pending_id) == "delivered"
        assert await dispatcher.deliver(pending_id) == "already_delivered"
        assert dispatcher.undelivered() is None
        assert delivered == ["cell-1"]
        pending = requests.pending()
        assert pending is not None and pending.delivered_at is not None
        assert reopened.scalar("SELECT count(*) FROM checkpoint_requests") == 1
        # Still occupying the slot until a terminal transition.
        assert requests.request(evidence("cell-2", "s2", "e2"), reason="red") is None
        assert requests.resolve_for_cell("cell-1", outcome="completed")
        assert await dispatcher.deliver(pending_id) == "not_pending"
    finally:
        reopened.close()
