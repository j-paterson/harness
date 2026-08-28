"""Verify context-pressure thresholds and active-time accounting."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.config import PolicyConfig
from hermes_orchestrator.context import (
    ActiveTimeTracker,
    ContextMonitor,
    ContextSignal,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore

DAY = datetime(2026, 8, 28, tzinfo=UTC)


class Clock:
    def at(self, hhmm: str) -> datetime:
        hours, minutes = hhmm.split(":")
        return DAY.replace(hour=int(hours), minute=int(minutes))


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def active_time(database: Database) -> ActiveTimeTracker:
    return ActiveTimeTracker(database)


@pytest.fixture
def context_monitor(database: Database) -> ContextMonitor:
    return ContextMonitor(
        database,
        EventStore(database),
        policy=PolicyConfig(),
        now=lambda: DAY.replace(hour=12),
    )


def signal(**overrides: Any) -> ContextSignal:
    values: dict[str, Any] = {"worker_id": "worker-1", "at": DAY.replace(hour=12)}
    values.update(overrides)
    return ContextSignal(**values)


def test_context_thresholds(context_monitor: ContextMonitor) -> None:
    assert context_monitor.record(signal(percent=69)).state == "healthy"
    assert context_monitor.record(signal(percent=70)).state == "prepare"
    assert context_monitor.record(signal(percent=81)).state == "rotation_pending"


def test_idle_time_does_not_count(active_time: ActiveTimeTracker, clock: Clock) -> None:
    active_time.open("worker-1", clock.at("08:00"))
    active_time.idle("worker-1", clock.at("10:00"))
    active_time.open("worker-1", clock.at("16:00"))
    assert active_time.total("worker-1", clock.at("18:00")) == timedelta(hours=4)
    # Re-opening an open interval is idempotent; other workers are isolated.
    active_time.open("worker-1", clock.at("17:00"))
    assert active_time.total("worker-1", clock.at("18:00")) == timedelta(hours=4)
    assert active_time.total("worker-2", clock.at("18:00")) == timedelta()


def test_six_active_hours_waits_for_safe_boundary(
    context_monitor: ContextMonitor,
) -> None:
    decision = context_monitor.record(signal(active_hours=6.2, safe_boundary=False))
    assert decision.state == "rotation_pending"
    assert any("safe boundary" in reason for reason in decision.reasons)
    decision = context_monitor.record(signal(active_hours=6.2, safe_boundary=True))
    assert decision.state == "rotate_now"


def test_repeated_compaction_is_strong_evidence(
    context_monitor: ContextMonitor,
) -> None:
    assert context_monitor.record(signal(compaction=True)).state == "prepare"
    decision = context_monitor.record(signal(compaction=True, rapid_refill=True))
    assert decision.state == "rotation_pending"


def test_behavioral_warning_alone_never_forces_rotation(
    context_monitor: ContextMonitor,
) -> None:
    for _ in range(5):
        decision = context_monitor.record(signal(behavioral_warning=True))
    assert decision.state == "healthy"
    assert any("cannot force rotation" in reason for reason in decision.reasons)
    assert decision.evidence["warnings"] == 5


def test_context_error_is_an_emergency_that_bypasses_the_boundary(
    context_monitor: ContextMonitor,
) -> None:
    decision = context_monitor.record(signal(context_error=True, safe_boundary=False))
    assert decision.state == "rotate_now"
    assert "emergency" in decision.reasons[0]


def test_decision_is_sticky_and_durable(
    database: Database, context_monitor: ContextMonitor
) -> None:
    assert context_monitor.record(signal(percent=85)).state == "rotation_pending"
    assert context_monitor.record(signal(percent=10)).state == "rotation_pending"
    reopened = ContextMonitor(database, EventStore(database), policy=PolicyConfig())
    assert reopened.state("worker-1") == "rotation_pending"
    assert reopened.record(signal(safe_boundary=True)).state == "rotate_now"
    reopened.reset("worker-1", reason="rotated")
    assert reopened.state("worker-1") == "healthy"
    kinds = [
        row["event_type"]
        for row in database.execute(
            "SELECT event_type FROM events WHERE aggregate_id = 'worker-1' "
            "ORDER BY sequence"
        ).fetchall()
    ]
    assert kinds == ["context.rotation_pending", "context.rotate_now", "context.reset"]


def test_naive_clock_is_rejected(active_time: ActiveTimeTracker) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        active_time.open("worker-1", datetime(2026, 8, 28, 8))
