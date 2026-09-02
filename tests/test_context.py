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
    derive_context_occupancy,
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


# -- derive_context_occupancy (INFRA-184) --------------------------------


def test_derive_context_occupancy_authoritative_percent_wins() -> None:
    # A cumulative-shaped usage dict is supplied alongside an
    # authoritative reading; the authoritative figure always wins and the
    # cumulative fields are never touched.
    measurement = derive_context_occupancy(
        {"input_tokens": 900_000, "cache_read_input_tokens": 5_000_000},
        window_tokens=200_000,
        authoritative_percent=42.5,
    )
    assert measurement.percent == 42.5
    assert measurement.source == "authoritative"
    assert measurement.uncertainty is None
    assert measurement.occupied_tokens == 85_000


def test_derive_context_occupancy_out_of_range_authoritative_percent_falls_back() -> (
    None
):
    measurement = derive_context_occupancy(
        {"input_tokens": 20_000},
        window_tokens=100_000,
        authoritative_percent=142.0,
    )
    assert measurement.source == "lead_estimate"
    assert measurement.percent == 20.0


def test_derive_context_occupancy_single_call_estimate() -> None:
    # input + cache_creation + cache_read for ONE call is legitimately
    # that call's own prompt size.
    measurement = derive_context_occupancy(
        {"input_tokens": 40_000, "cache_creation_input_tokens": 10_000},
        window_tokens=100_000,
    )
    assert measurement.percent == 50.0
    assert measurement.occupied_tokens == 50_000
    assert measurement.source == "lead_estimate"
    assert measurement.uncertainty is None


def test_derive_context_occupancy_clamps_impossible_percent() -> None:
    # The observed shape: a monotonically-growing cache_read_input_tokens
    # counter read back as though it were this call's own live window
    # occupancy -- exactly what produced the false "1704% full" rotation.
    measurement = derive_context_occupancy(
        {"cache_read_input_tokens": 3_408_000},
        window_tokens=200_000,
    )
    assert measurement.percent == 100.0
    assert measurement.source == "lead_estimate"
    assert measurement.uncertainty == "impossible_percent:1704.0"


def test_derive_context_occupancy_negative_field_is_dropped_and_flagged() -> None:
    measurement = derive_context_occupancy(
        {"input_tokens": -5, "cache_read_input_tokens": 30_000},
        window_tokens=100_000,
    )
    assert measurement.percent == 30.0
    assert measurement.occupied_tokens == 30_000
    assert measurement.source == "lead_estimate"
    assert measurement.uncertainty == "negative_field:input_tokens:-5"


# -- uncertain signals never rotate alone (INFRA-184) --------------------


def test_uncertain_signal_never_satisfies_rotation_threshold_alone(
    context_monitor: ContextMonitor,
) -> None:
    decision = context_monitor.record(
        signal(percent=100.0, uncertainty="impossible_percent:1704.0")
    )
    assert decision.state == "healthy"
    assert any("uncertain" in reason for reason in decision.reasons)
    assert decision.evidence["uncertainty"] == "impossible_percent:1704.0"


def test_uncertain_signal_does_not_become_durable_fallback_percent(
    context_monitor: ContextMonitor,
) -> None:
    context_monitor.record(signal(percent=50.0))
    decision = context_monitor.record(
        signal(percent=100.0, uncertainty="impossible_percent:1704.0")
    )
    assert decision.evidence["percent"] == 50.0
    # A later signal-less turn (no fresh percent reported at all) must
    # inherit the last TRUSTED percent, never the discarded uncertain one.
    decision = context_monitor.record(signal(percent=None))
    assert decision.evidence["percent"] == 50.0
    assert decision.state == "healthy"


def test_six_active_hours_still_fires_alongside_uncertain_measurement(
    context_monitor: ContextMonitor,
) -> None:
    context_monitor.record(
        signal(percent=100.0, uncertainty="impossible_percent:1704.0")
    )
    decision = context_monitor.record(signal(active_hours=6.2, safe_boundary=True))
    assert decision.state == "rotate_now"


def test_context_error_still_fires_alongside_uncertain_measurement(
    context_monitor: ContextMonitor,
) -> None:
    context_monitor.record(
        signal(percent=100.0, uncertainty="impossible_percent:1704.0")
    )
    decision = context_monitor.record(signal(context_error=True, safe_boundary=False))
    assert decision.state == "rotate_now"
    assert "emergency" in decision.reasons[0]


def test_compaction_and_rapid_refill_still_fire_alongside_uncertain_measurement(
    context_monitor: ContextMonitor,
) -> None:
    context_monitor.record(
        signal(percent=100.0, uncertainty="impossible_percent:1704.0")
    )
    assert context_monitor.record(signal(compaction=True)).state == "prepare"
    decision = context_monitor.record(signal(compaction=True, rapid_refill=True))
    assert decision.state == "rotation_pending"
