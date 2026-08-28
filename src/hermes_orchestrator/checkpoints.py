"""Durable checkpoint-safety evidence and the one-outstanding-request rule.

INFRA-168 correction: a red-pressure checkpoint may target only a worker
whose safe boundary is durably proven and current — bound to the exact
project cell and lead session and invalidated the moment new work starts.
Checkpoint requests are durable with an idempotent identity; at most one
may be pending host-wide (enforced by a partial unique index), new
requests are suppressed while one is pending, and a fresh red sample after
a terminal transition is required before the next request.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

TERMINAL_STATES = ("completed", "failed", "stale")


@dataclass(frozen=True, slots=True)
class SafetyEvidence:
    cell_id: str
    session_id: str
    state: str
    boundary_kind: str
    evidence_id: str
    recorded_at: str


@dataclass(frozen=True, slots=True)
class CheckpointRequest:
    request_id: str
    cell_id: str
    session_id: str
    evidence_id: str
    reason: str
    state: str
    requested_at: str
    resolved_at: str | None
    resolution: str | None
    delivered_at: str | None = None


class CheckpointSafetyStore:
    """Authoritative, durable safe-boundary evidence per project cell."""

    def __init__(
        self,
        database: Database,
        events: EventStore,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._now = now or (lambda: datetime.now(UTC))

    def mark_safe(
        self,
        cell_id: str,
        session_id: str,
        *,
        boundary_kind: str,
        evidence_id: str,
    ) -> SafetyEvidence:
        if not evidence_id.strip():
            raise ValueError("safe-boundary evidence requires an evidence id")
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO checkpoint_safety("
                "cell_id, session_id, state, boundary_kind, evidence_id, "
                "recorded_at, invalidated_at, invalidation_reason"
                ") VALUES (?, ?, 'safe', ?, ?, ?, NULL, NULL) "
                "ON CONFLICT(cell_id) DO UPDATE SET session_id = excluded.session_id, "
                "state = 'safe', boundary_kind = excluded.boundary_kind, "
                "evidence_id = excluded.evidence_id, "
                "recorded_at = excluded.recorded_at, "
                "invalidated_at = NULL, invalidation_reason = NULL",
                (cell_id, session_id, boundary_kind, evidence_id, stamp),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="checkpoint_safety.safe",
                    aggregate_type="project_cell",
                    aggregate_id=cell_id,
                    payload={
                        "session_id": session_id,
                        "boundary_kind": boundary_kind,
                        "evidence_id": evidence_id,
                    },
                ),
            )
        evidence = self.current(cell_id, session_id)
        assert evidence is not None
        return evidence

    def invalidate(self, cell_id: str, *, reason: str) -> bool:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE checkpoint_safety SET state = 'invalidated', "
                "invalidated_at = ?, invalidation_reason = ? "
                "WHERE cell_id = ? AND state = 'safe'",
                (stamp, reason, cell_id),
            )
            if cursor.rowcount == 1:
                self._events.append(
                    connection,
                    EventInput(
                        event_type="checkpoint_safety.invalidated",
                        aggregate_type="project_cell",
                        aggregate_id=cell_id,
                        payload={"reason": reason},
                    ),
                )
        return cursor.rowcount == 1

    def current(self, cell_id: str, session_id: str) -> SafetyEvidence | None:
        """Evidence is current only if safe and bound to this exact session."""

        row = self._database.execute(
            "SELECT * FROM checkpoint_safety WHERE cell_id = ? AND state = 'safe' "
            "AND session_id = ?",
            (cell_id, session_id),
        ).fetchone()
        if row is None:
            return None
        return SafetyEvidence(
            cell_id=str(row["cell_id"]),
            session_id=str(row["session_id"]),
            state=str(row["state"]),
            boundary_kind=str(row["boundary_kind"]),
            evidence_id=str(row["evidence_id"]),
            recorded_at=str(row["recorded_at"]),
        )


class CheckpointRequests:
    """Durable, globally single-outstanding checkpoint requests."""

    def __init__(
        self,
        database: Database,
        events: EventStore,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._now = now or (lambda: datetime.now(UTC))

    @staticmethod
    def identity(evidence: SafetyEvidence) -> str:
        return f"ckpt:{evidence.cell_id}:{evidence.session_id}:{evidence.evidence_id}"

    def pending(self) -> CheckpointRequest | None:
        row = self._database.execute(
            "SELECT * FROM checkpoint_requests WHERE state = 'pending'"
        ).fetchone()
        return None if row is None else _row_to_request(row)

    def last_terminal_at(self) -> str | None:
        placeholders = ",".join("?" for _ in TERMINAL_STATES)
        value = self._database.scalar(
            "SELECT max(resolved_at) FROM checkpoint_requests "
            f"WHERE state IN ({placeholders})",
            TERMINAL_STATES,
        )
        return None if value is None else str(value)

    def request(
        self, evidence: SafetyEvidence, *, reason: str
    ) -> CheckpointRequest | None:
        """Create the one pending request; None if any request is pending.

        Idempotent per (cell, session, evidence): re-requesting the same
        proven boundary returns the existing row instead of a duplicate.
        """

        request_id = self.identity(evidence)
        existing = self.get(request_id)
        if existing is not None:
            return existing if existing.state == "pending" else None
        stamp = self._now().isoformat()
        try:
            with self._database.transaction() as connection:
                connection.execute(
                    "INSERT INTO checkpoint_requests("
                    "request_id, cell_id, session_id, evidence_id, reason, state, "
                    "requested_at, resolved_at, resolution"
                    ") VALUES (?, ?, ?, ?, ?, 'pending', ?, NULL, NULL)",
                    (
                        request_id,
                        evidence.cell_id,
                        evidence.session_id,
                        evidence.evidence_id,
                        reason,
                        stamp,
                    ),
                )
                self._events.append(
                    connection,
                    EventInput(
                        event_type="checkpoint_request.pending",
                        aggregate_type="checkpoint_request",
                        aggregate_id=request_id,
                        payload={
                            "cell_id": evidence.cell_id,
                            "session_id": evidence.session_id,
                            "reason": reason,
                        },
                    ),
                )
        except sqlite3.IntegrityError:
            return None
        return self.get(request_id)

    def resolve(self, request_id: str, *, outcome: str, detail: str = "") -> bool:
        if outcome not in TERMINAL_STATES:
            raise ValueError(f"unknown checkpoint outcome {outcome!r}")
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE checkpoint_requests SET state = ?, resolved_at = ?, "
                "resolution = ? WHERE request_id = ? AND state = 'pending'",
                (outcome, stamp, detail or outcome, request_id),
            )
            if cursor.rowcount == 1:
                self._events.append(
                    connection,
                    EventInput(
                        event_type=f"checkpoint_request.{outcome}",
                        aggregate_type="checkpoint_request",
                        aggregate_id=request_id,
                        payload={"detail": detail},
                    ),
                )
        return cursor.rowcount == 1

    def mark_delivered(self, request_id: str, *, evidence: str) -> bool:
        """Record one successful, revalidated delivery of a pending request."""

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE checkpoint_requests SET delivered_at = ?, "
                "delivery_evidence = ? WHERE request_id = ? AND state = 'pending' "
                "AND delivered_at IS NULL",
                (stamp, evidence, request_id),
            )
            if cursor.rowcount == 1:
                self._events.append(
                    connection,
                    EventInput(
                        event_type="checkpoint_request.delivered",
                        aggregate_type="checkpoint_request",
                        aggregate_id=request_id,
                        payload={"evidence": evidence},
                    ),
                )
        return cursor.rowcount == 1

    def undelivered(self) -> CheckpointRequest | None:
        """The pending request reserved but not yet delivered, if any."""

        pending = self.pending()
        if pending is None or pending.delivered_at is not None:
            return None
        return pending

    def resolve_for_cell(self, cell_id: str, *, outcome: str, detail: str = "") -> bool:
        pending = self.pending()
        if pending is None or pending.cell_id != cell_id:
            return False
        return self.resolve(pending.request_id, outcome=outcome, detail=detail)

    def get(self, request_id: str) -> CheckpointRequest | None:
        row = self._database.execute(
            "SELECT * FROM checkpoint_requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        return None if row is None else _row_to_request(row)


def _row_to_request(row: Any) -> CheckpointRequest:
    return CheckpointRequest(
        request_id=str(row["request_id"]),
        cell_id=str(row["cell_id"]),
        session_id=str(row["session_id"]),
        evidence_id=str(row["evidence_id"]),
        reason=str(row["reason"]),
        state=str(row["state"]),
        requested_at=str(row["requested_at"]),
        resolved_at=row["resolved_at"],
        resolution=row["resolution"],
        delivered_at=row["delivered_at"],
    )


CheckpointCallback = Callable[[str, str], Any]


class CheckpointDispatcher:
    """Deliver exactly one reserved request to its exact current cell/session.

    Every non-delivery — missing callback, target/session mismatch at
    execution time, a callback that reports the cell is no longer active,
    cancellation, or any exception — transitions that exact request to a
    terminal ``failed``/``stale`` state with bounded evidence before the
    error propagates or the loop continues, so a failed delivery can never
    hold the single pending slot. Delivery is idempotent across retries and
    restarts: an already-delivered request is never re-sent.
    """

    def __init__(
        self,
        requests: CheckpointRequests,
        *,
        callback: CheckpointCallback | None,
        current_session: Callable[[str], str | None],
    ) -> None:
        self._requests = requests
        self._callback = callback
        self._current_session = current_session

    def undelivered(self) -> str | None:
        """Request id reserved before a restart or crash and never delivered."""

        pending = self._requests.undelivered()
        return None if pending is None else pending.request_id

    async def deliver(self, request_id: str) -> str:
        """Return ``delivered``, ``already_delivered``, ``stale``, or ``failed``."""

        request = self._requests.get(request_id)
        if request is None or request.state != "pending":
            return "not_pending"
        if request.delivered_at is not None:
            return "already_delivered"
        if self._callback is None:
            self._requests.resolve(
                request_id, outcome="failed", detail="no_checkpoint_callback"
            )
            return "failed"
        current = self._current_session(request.cell_id)
        if current != request.session_id:
            self._requests.resolve(
                request_id,
                outcome="stale",
                detail=f"session_changed:{'none' if current is None else 'other'}",
            )
            return "stale"
        try:
            accepted = self._callback(request.cell_id, request.reason)
            if hasattr(accepted, "__await__"):
                accepted = await accepted
        except BaseException as error:
            detail = (
                "cancelled"
                if isinstance(error, asyncio.CancelledError)
                else f"callback_error:{type(error).__name__}"
            )
            self._requests.resolve(request_id, outcome="failed", detail=detail)
            raise
        if not accepted:
            self._requests.resolve(
                request_id, outcome="stale", detail="cell_not_active"
            )
            return "stale"
        self._requests.mark_delivered(request_id, evidence="callback_accepted")
        return "delivered"
