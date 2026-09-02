"""Context pressure and six-active-hour rotation (INFRA-169).

Active execution time is tracked per worker session as durable intervals,
so idle time between turns never counts. Context evidence — the context
percentage when available, compaction, rapid refill after compaction,
explicit context errors, and behavioural warnings — is aggregated durably
into one sticky decision per worker: ``healthy`` → ``prepare`` (request a
handoff draft, keep working) → ``rotation_pending`` (assign no new subwork,
wait for a safe boundary) → ``rotate_now`` (invoke the acknowledged handoff
flow). A behavioural warning alone can never force rotation, and a context
error bypasses the safe-boundary wait after an emergency checkpoint.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from hermes_orchestrator.config import PolicyConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

STATES = ("healthy", "prepare", "rotation_pending", "rotate_now")
_RANK = {state: index for index, state in enumerate(STATES)}
# INFRA-184: the fields a single top-level invocation's usage dict may
# carry that, together, describe THAT call's own prompt size. Summing
# these for one record is legitimate; summing them across records is
# exactly the "1704% full" bug this module exists to prevent.
_OCCUPANCY_FIELDS = (
    "input_tokens",
    "cache_creation_input_tokens",
    "cache_read_input_tokens",
)


@dataclass(frozen=True, slots=True)
class ContextMeasurement:
    """One occupancy measurement scoped to a single context window.

    ``source`` is ``"authoritative"`` when a real current-window percent
    was supplied (a provider-reported figure, never derived by us) and
    ``"lead_estimate"`` when it was derived from one individual model
    invocation's own usage. ``uncertainty`` is ``None`` for a trustworthy
    reading and a short machine-parsable reason (e.g.
    ``"impossible_percent:1704.0"``) when the telemetry had to be
    clamped -- callers must journal that reason but must never rotate on
    the clamped percentage alone.
    """

    percent: float
    occupied_tokens: int
    source: str
    uncertainty: str | None = None


def derive_context_occupancy(
    usage: Mapping[str, int],
    *,
    window_tokens: int,
    authoritative_percent: float | None = None,
) -> ContextMeasurement:
    """Derive one current-window occupancy percent for a single invocation.

    Never call this with usage summed or accumulated across calls,
    records, or subagents -- ``usage`` must be exactly one individual
    top-level model invocation's own usage dict. An authoritative
    current-window percent, when supplied and within ``[0, 100]``,
    always wins. Otherwise this estimates from the three prompt-size
    fields of THIS ONE invocation; a negative or non-integer field is
    dropped (treated as zero) and flagged, and a computed percent over
    100 -- impossible for a single call against its own window -- is
    clamped to 100 and flagged rather than trusted.
    """

    if authoritative_percent is not None and 0.0 <= authoritative_percent <= 100.0:
        occupied = (
            round(window_tokens * authoritative_percent / 100.0)
            if window_tokens > 0
            else 0
        )
        return ContextMeasurement(
            percent=float(authoritative_percent),
            occupied_tokens=occupied,
            source="authoritative",
            uncertainty=None,
        )

    uncertainty: str | None = None
    occupied = 0
    for name in _OCCUPANCY_FIELDS:
        raw = usage.get(name, 0)
        if not isinstance(raw, int) or isinstance(raw, bool):
            uncertainty = uncertainty or f"non_integer_field:{name}"
            continue
        if raw < 0:
            uncertainty = uncertainty or f"negative_field:{name}:{raw}"
            continue
        occupied += raw

    if window_tokens <= 0:
        return ContextMeasurement(
            percent=0.0,
            occupied_tokens=0,
            source="lead_estimate",
            uncertainty=uncertainty or "non_positive_window_tokens",
        )

    percent = 100.0 * occupied / window_tokens
    if percent > 100.0:
        uncertainty = uncertainty or f"impossible_percent:{round(percent, 1)}"
        percent = 100.0
    elif percent < 0.0:
        uncertainty = uncertainty or f"impossible_percent:{round(percent, 1)}"
        percent = 0.0

    return ContextMeasurement(
        percent=percent,
        occupied_tokens=occupied,
        source="lead_estimate",
        uncertainty=uncertainty,
    )


@dataclass(frozen=True, slots=True)
class ContextSignal:
    """One observation about a worker's context and active time."""

    worker_id: str
    at: datetime
    percent: float | None = None
    compaction: bool = False
    rapid_refill: bool = False
    context_error: bool = False
    behavioral_warning: bool = False
    active_hours: float | None = None
    safe_boundary: bool = False
    # INFRA-184: a short machine-parsable reason (see
    # ``ContextMeasurement.uncertainty``) when ``percent`` came from
    # telemetry that had to be clamped. Such a signal is still journaled
    # -- it is visible measurement uncertainty -- but it can never, on
    # its own, satisfy a prepare/rotate threshold or become the durable
    # fallback percent for a later signal that carries none.
    uncertainty: str | None = None


@dataclass(frozen=True, slots=True)
class ContextDecision:
    state: str
    reasons: tuple[str, ...]
    evidence: dict[str, Any] = field(default_factory=dict)


class ActiveTimeTracker:
    """Durable active-interval accounting; idle time never counts."""

    def __init__(self, database: Database) -> None:
        self._database = database

    def open(self, worker_id: str, at: datetime) -> None:
        stamp = _aware(at).isoformat()
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT interval_id FROM active_intervals WHERE worker_id = ? "
                "AND closed_at IS NULL",
                (worker_id,),
            ).fetchone()
            if row is not None:
                return
            connection.execute(
                "INSERT INTO active_intervals(worker_id, opened_at, closed_at) "
                "VALUES (?, ?, NULL)",
                (worker_id, stamp),
            )

    def idle(self, worker_id: str, at: datetime) -> None:
        stamp = _aware(at).isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE active_intervals SET closed_at = ? WHERE worker_id = ? "
                "AND closed_at IS NULL AND opened_at <= ?",
                (stamp, worker_id, stamp),
            )

    def total(self, worker_id: str, at: datetime) -> timedelta:
        now = _aware(at)
        rows = self._database.execute(
            "SELECT opened_at, closed_at FROM active_intervals WHERE worker_id = ?",
            (worker_id,),
        ).fetchall()
        total = timedelta()
        for row in rows:
            opened = datetime.fromisoformat(str(row["opened_at"]))
            closed = (
                now
                if row["closed_at"] is None
                else min(datetime.fromisoformat(str(row["closed_at"])), now)
            )
            if closed > opened:
                total += closed - opened
        return total


class ContextMonitor:
    """Aggregate durable context evidence into a sticky rotation decision."""

    def __init__(
        self,
        database: Database,
        events: EventStore,
        *,
        policy: PolicyConfig,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._policy = policy
        self._now = now or (lambda: datetime.now(UTC))

    def state(self, worker_id: str) -> str:
        row = self._database.execute(
            "SELECT state FROM context_evidence WHERE worker_id = ?", (worker_id,)
        ).fetchone()
        return "healthy" if row is None else str(row["state"])

    def reset(self, worker_id: str, *, reason: str) -> None:
        with self._database.transaction() as connection:
            connection.execute(
                "DELETE FROM context_evidence WHERE worker_id = ?", (worker_id,)
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="context.reset",
                    aggregate_type="worker",
                    aggregate_id=worker_id,
                    payload={"reason": reason},
                ),
            )

    def record(self, signal: ContextSignal) -> ContextDecision:
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM context_evidence WHERE worker_id = ?",
                (signal.worker_id,),
            ).fetchone()
            compactions = int(row["compactions"]) if row else 0
            rapid_refills = int(row["rapid_refills"]) if row else 0
            context_errors = int(row["context_errors"]) if row else 0
            warnings = int(row["warnings"]) if row else 0
            previous_state = str(row["state"]) if row else "healthy"
            last_percent = (
                float(row["last_percent"])
                if row and row["last_percent"] is not None
                else None
            )
            stored_hours = float(row["active_hours"]) if row else 0.0
            compactions += int(signal.compaction)
            rapid_refills += int(signal.rapid_refill)
            context_errors += int(signal.context_error)
            warnings += int(signal.behavioral_warning)
            # INFRA-184: an uncertain measurement (impossible/clamped
            # telemetry) is journaled below but must never itself become
            # a usable occupancy percent -- it can't satisfy a threshold
            # this call, and it can't become the durable fallback that a
            # later, signal-less turn would otherwise inherit.
            percent = (
                last_percent
                if signal.uncertainty is not None
                else (signal.percent if signal.percent is not None else last_percent)
            )
            active_hours = (
                signal.active_hours if signal.active_hours is not None else stored_hours
            )

            prepare = float(self._policy.context_prepare_percent)
            rotate = float(self._policy.context_rotate_percent)
            max_hours = float(self._policy.max_active_session_hours)
            reasons: list[str] = []
            pending = False
            emergency = False
            if signal.uncertainty is not None:
                reasons.append(f"context measurement uncertain: {signal.uncertainty}")
            if context_errors > 0:
                emergency = True
                reasons.append("context error reported; emergency checkpoint")
            if percent is not None and percent >= rotate:
                pending = True
                reasons.append(f"context use {percent:.0f}% >= rotate {rotate:.0f}%")
            if active_hours >= max_hours:
                pending = True
                reasons.append(
                    f"{active_hours:.1f} active hours >= {max_hours:.0f} hour limit"
                )
            if compactions >= 2 and rapid_refills >= 1:
                pending = True
                reasons.append("repeated compaction with rapid refill")
            elif compactions >= 1 and rapid_refills >= 1:
                pending = True
                reasons.append("compaction followed by rapid refill")
            prepare_now = False
            if not pending and percent is not None and percent >= prepare:
                prepare_now = True
                reasons.append(f"context use {percent:.0f}% >= prepare {prepare:.0f}%")
            if not pending and compactions >= 1 and not prepare_now:
                prepare_now = True
                reasons.append("context was compacted")
            if warnings and not (pending or prepare_now or emergency):
                reasons.append(
                    "behavioural warning alone cannot force rotation without "
                    "context, compaction, active-age, or error evidence"
                )

            if emergency:
                computed = "rotate_now"
            elif pending:
                computed = "rotation_pending"
            elif prepare_now:
                computed = "prepare"
            else:
                computed = "healthy"
            # Sticky: never relax on a quieter signal; rotate_now requires a
            # safe boundary unless a context error made it an emergency.
            state = (
                computed if _RANK[computed] > _RANK[previous_state] else previous_state
            )
            if state == "rotate_now" and not emergency:
                state = "rotation_pending"
            if state == "rotation_pending" and signal.safe_boundary:
                state = "rotate_now"
                reasons.append("safe boundary reached")
            if state == "rotation_pending" and not signal.safe_boundary:
                reasons.append("waiting for a safe boundary before rotating")
            if not reasons:
                reasons.append("context healthy")
            evidence = {
                "percent": percent,
                "compactions": compactions,
                "rapid_refills": rapid_refills,
                "context_errors": context_errors,
                "warnings": warnings,
                "active_hours": round(active_hours, 3),
                "safe_boundary": signal.safe_boundary,
                "uncertainty": signal.uncertainty,
            }
            connection.execute(
                "INSERT INTO context_evidence("
                "worker_id, last_percent, compactions, rapid_refills, "
                "context_errors, warnings, active_hours, state, reasons_json, "
                "updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(worker_id) DO UPDATE SET "
                "last_percent = excluded.last_percent, "
                "compactions = excluded.compactions, "
                "rapid_refills = excluded.rapid_refills, "
                "context_errors = excluded.context_errors, "
                "warnings = excluded.warnings, "
                "active_hours = excluded.active_hours, state = excluded.state, "
                "reasons_json = excluded.reasons_json, "
                "updated_at = excluded.updated_at",
                (
                    signal.worker_id,
                    percent,
                    compactions,
                    rapid_refills,
                    context_errors,
                    warnings,
                    active_hours,
                    state,
                    json.dumps(reasons),
                    self._now().isoformat(),
                ),
            )
            if state != previous_state:
                self._events.append(
                    connection,
                    EventInput(
                        event_type=f"context.{state}",
                        aggregate_type="worker",
                        aggregate_id=signal.worker_id,
                        payload={
                            "from": previous_state,
                            "reasons": reasons,
                            **evidence,
                        },
                    ),
                )
            if signal.uncertainty is not None:
                # INFRA-184: journaled unconditionally -- independent of
                # any state transition -- so a measurement anomaly that
                # (correctly) never moves the sticky state stays visible
                # in the durable event log rather than only in a
                # context_evidence row a later call can overwrite.
                self._events.append(
                    connection,
                    EventInput(
                        event_type="context.measurement_uncertain",
                        aggregate_type="worker",
                        aggregate_id=signal.worker_id,
                        payload={
                            "uncertainty": signal.uncertainty,
                            "reasons": reasons,
                            **evidence,
                        },
                    ),
                )
        return ContextDecision(state=state, reasons=tuple(reasons), evidence=evidence)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("clock must return a timezone-aware datetime")
    return value.astimezone(UTC)
