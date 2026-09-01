"""Versioned durable control-operation events and receipts.

INFRA-195: lifecycle recovery must be provable without watching a
terminal. Every recorded operation carries an explicit result — an
absence is a recorded fact ({"replay_count": 0}), never silence. The
exact lead is woken through the dedicated channel, fetches the durable
row, and ACKs it; at most one live operation exists per dedup key so
repeated recovery never floods the channel while an earlier receipt is
still unacknowledged.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

CONTROL_READY = "HERMES_CONTROL_READY"
CONTROL_SCHEMA_VERSION = 1

# The lifecycle vocabulary. Growth is a code change here, reviewed
# like any other contract change; free-text kinds are refused.
CONTROL_KINDS = frozenset(
    {
        "daemon.restarted",
        "channel.reregistered",
        "channel.replayed",
        "intake.dedup_repaired",
        "channel.blocked",
        "children.completed",
        "signal.failed",
        "channel.auto_confirmed",
        "channel.approval_required",
        "channel.confirm_claimed",
        "channel.confirm_ambiguous",
        "lead.launch_failed",
        "channel.rebind_refused",
        # INFRA-198: the adoption sibling of rebind_refused -- this cell
        # has no anchor and the project's proven one could not be
        # carried onto it, so the operator must confirm manually.
        "channel.adopt_refused",
        # INFRA-198: a lead whose cmux surface is provably gone was
        # retired mid-run by the per-tick dead-lead sweep -- the seat,
        # its cell and its lane are released, and a deliberate
        # start-lane seats the replacement.
        "lead.dead_worker_retired",
    }
)

# INFRA-201: the vocabulary splits into two disjoint classes. A
# maintenance receipt reports pure transport/lifecycle churn — a
# reconnect, a repaired duplicate, a routine restart — whose only lead
# action is to acknowledge it; delivering 91 wakes for 10 such
# receipts in one live session was the measured problem. A
# maintenance receipt stays exactly as durable as before: the same
# ``record``/``record_in``, the same dedup key, the same exact-session
# ``acknowledge`` and its ``control_operation.acknowledged`` event. The
# only thing that changes is who settles it and how visibly — the
# lead's own Stop hook now settles it silently
# (``settle_maintenance_for_session``) instead of a dedicated wake
# reaching the primary view through the channel, the seat announce, or
# the Stop-hook offer. Everything else — every kind not listed here,
# and any kind added to CONTROL_KINDS in the future — is
# lead-actionable by default and keeps reaching the primary view
# exactly as before.
MAINTENANCE_CONTROL_KINDS = frozenset(
    {
        "daemon.restarted",
        "channel.reregistered",
        "channel.replayed",
        "intake.dedup_repaired",
        "channel.auto_confirmed",
        "channel.confirm_claimed",
    }
)
#: INFRA-219 (Sol correction 14bd0c17): the runtime-lifecycle subset of
#: the maintenance kinds. INFRA-201 correctly classified transport churn
#: as silent, but a MERGED-RUNTIME activation is not churn: it is the one
#: moment an ACTIVE lead must be told its runtime changed. Observed live
#: — after PR #52 activated runtime 8ac98e2757ba these three sat
#: ``published``, bound to the exact idle lead, and the lead had to be
#: woken manually through cmux. They are therefore OFFERED through the
#: existing offer/ACK path (no new transport), while the genuinely churny
#: remainder stays silent exactly as INFRA-201 intended.
LIFECYCLE_CONTROL_KINDS = frozenset(
    {
        "daemon.restarted",
        "channel.reregistered",
        "channel.replayed",
    }
)

#: The maintenance kinds still settled silently by the lead's own Stop
#: hook: pure transport/dedup churn with nothing for a lead to act on.
SILENT_MAINTENANCE_CONTROL_KINDS = (
    MAINTENANCE_CONTROL_KINDS - LIFECYCLE_CONTROL_KINDS
)

LEAD_ACTIONABLE_CONTROL_KINDS = (
    CONTROL_KINDS - SILENT_MAINTENANCE_CONTROL_KINDS
)


def is_maintenance_kind(kind: str) -> bool:
    """Whether ``kind`` is pure transport/maintenance churn (INFRA-201)."""

    return kind in MAINTENANCE_CONTROL_KINDS


class ControlOperationRefused(ValueError):
    """The operation is outside the durable lifecycle vocabulary."""


@dataclass(frozen=True, slots=True)
class ControlOperation:
    """One durable control-operation event awaiting its exact receipt."""

    operation_id: str
    schema_version: int
    kind: str
    project_key: str
    cell_id: str
    session_id: str
    dedup_key: str
    result: dict[str, object]
    reason: str | None
    state: str
    created_at: str
    updated_at: str
    acknowledged_at: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "operation_id": self.operation_id,
            "schema_version": self.schema_version,
            "kind": self.kind,
            "project_key": self.project_key,
            "cell_id": self.cell_id,
            "session_id": self.session_id,
            "dedup_key": self.dedup_key,
            "result": dict(self.result),
            "reason": self.reason,
            "state": self.state,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "acknowledged_at": self.acknowledged_at,
        }


class ControlOperations:
    """Record, read, and receipt durable control operations."""

    def __init__(
        self,
        database: Database,
        *,
        events: EventStore,
        ids: Callable[[], str] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._now = now or (lambda: datetime.now(UTC))
        self._listeners: list[Callable[[ControlOperation], None]] = []

    def subscribe(self, listener: Callable[[ControlOperation], None]) -> None:
        """Register a post-commit listener (e.g. the channel router)."""

        self._listeners.append(listener)

    def notify_committed(self, operation: ControlOperation) -> None:
        for listener in self._listeners:
            with suppress(Exception):
                listener(operation)

    def record(
        self,
        *,
        kind: str,
        project_key: str,
        cell_id: str,
        session_id: str,
        result: Mapping[str, object],
        reason: str | None = None,
        dedup_key: str | None = None,
    ) -> ControlOperation | None:
        """Durably record one operation; a live duplicate is a no-op.

        The default dedup key is ``kind:session``, so at most one
        unacknowledged receipt per kind can wait on any one lead. The
        result mapping is stored verbatim — an empty or zero-valued
        result is exactly the point.
        """

        with self._database.transaction() as connection:
            record = self.record_in(
                connection,
                kind=kind,
                project_key=project_key,
                cell_id=cell_id,
                session_id=session_id,
                result=result,
                reason=reason,
                dedup_key=dedup_key,
            )
        if record is not None:
            self.notify_committed(record)
        return record

    def record_in(
        self,
        connection: sqlite3.Connection,
        *,
        kind: str,
        project_key: str,
        cell_id: str,
        session_id: str,
        result: Mapping[str, object],
        reason: str | None = None,
        dedup_key: str | None = None,
    ) -> ControlOperation | None:
        """Record inside the caller's transaction; the caller notifies
        listeners (``notify_committed``) only after its commit."""

        if kind not in CONTROL_KINDS:
            raise ControlOperationRefused(
                f"unknown control-operation kind {kind!r}"
            )
        key = dedup_key or f"{kind}:{session_id}"
        stamp = self._now().isoformat()
        operation_id = self._ids()
        live = connection.execute(
            "SELECT operation_id FROM control_operations "
            "WHERE dedup_key = ? AND state = 'published'",
            (key,),
        ).fetchone()
        if live is not None:
            return None
        connection.execute(
            "INSERT INTO control_operations("
            "operation_id, schema_version, kind, project_key, "
            "cell_id, session_id, dedup_key, result_json, reason, "
            "state, created_at, updated_at, acknowledged_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'published', ?, ?, "
            "NULL)",
            (
                operation_id,
                CONTROL_SCHEMA_VERSION,
                kind,
                project_key,
                cell_id,
                session_id,
                key,
                json.dumps(dict(result), sort_keys=True),
                reason,
                stamp,
                stamp,
            ),
        )
        self._events.append(
            connection,
            EventInput(
                event_type="control_operation.published",
                aggregate_type="control_operation",
                aggregate_id=operation_id,
                payload={
                    "kind": kind,
                    "project_key": project_key,
                    "cell_id": cell_id,
                    "session_id": session_id,
                    "result": dict(result),
                    "reason": reason,
                },
            ),
        )
        return ControlOperation(
            operation_id=operation_id,
            schema_version=CONTROL_SCHEMA_VERSION,
            kind=kind,
            project_key=project_key,
            cell_id=cell_id,
            session_id=session_id,
            dedup_key=key,
            result=dict(result),
            reason=reason,
            state="published",
            created_at=stamp,
            updated_at=stamp,
            acknowledged_at=None,
        )

    def record_for_active_cells(
        self,
        *,
        kind: str,
        result: Mapping[str, object],
        reason: str | None = None,
    ) -> tuple[ControlOperation, ...]:
        """Receipt one lifecycle fact to every active lead, deduplicated."""

        rows = self._database.execute(
            "SELECT project_key, cell_id, session_id FROM project_cells "
            "WHERE state = 'active'"
        ).fetchall()
        recorded: list[ControlOperation] = []
        for row in rows:
            operation = self.record(
                kind=kind,
                project_key=str(row["project_key"]),
                cell_id=str(row["cell_id"]),
                session_id=str(row["session_id"]),
                result=result,
                reason=reason,
            )
            if operation is not None:
                recorded.append(operation)
        return tuple(recorded)

    def acknowledge(self, operation_id: str, *, session_id: str) -> bool:
        """Record the bound session's exact receipt, exactly once."""

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE control_operations SET state = 'acknowledged', "
                "acknowledged_at = ?, updated_at = ? "
                "WHERE operation_id = ? AND session_id = ? "
                "AND state = 'published'",
                (stamp, stamp, operation_id, session_id),
            )
            if cursor.rowcount != 1:
                return False
            self._events.append(
                connection,
                EventInput(
                    event_type="control_operation.acknowledged",
                    aggregate_type="control_operation",
                    aggregate_id=operation_id,
                    payload={"session_id": session_id},
                ),
            )
        return True

    def get(self, operation_id: str) -> ControlOperation:
        row = self._database.execute(
            "SELECT * FROM control_operations WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        if row is None:
            raise KeyError(operation_id)
        return self._record(row)

    def pending_for_session(
        self, session_id: str
    ) -> tuple[ControlOperation, ...]:
        rows = self._database.execute(
            "SELECT * FROM control_operations "
            "WHERE session_id = ? AND state = 'published' "
            "ORDER BY created_at ASC, rowid ASC",
            (session_id,),
        ).fetchall()
        return tuple(self._record(row) for row in rows)

    def settle_maintenance_for_session(
        self, session_id: str
    ) -> tuple[str, ...]:
        """Acknowledge this session's published maintenance receipts.

        Scoped to exactly ``session_id`` and to
        ``SILENT_MAINTENANCE_CONTROL_KINDS`` (INFRA-219 Sol
        correction 14bd0c17: the runtime-lifecycle kinds are OFFERED
        to the lead now, never settled silently here, so a
        merged-runtime activation cannot leave an ACTIVE lead idle):
        an actionable receipt, or a
        maintenance receipt bound to another session, is never
        touched. Settlement goes through the same exact-session
        :meth:`acknowledge` used everywhere else, so the durable row
        and its ``control_operation.acknowledged`` event are unchanged
        in shape — only who settles them, and when, differs
        (INFRA-201). Idempotent: a second call finds nothing left
        ``published`` and returns an empty tuple.
        """

        placeholders = ",".join(
            "?" * len(SILENT_MAINTENANCE_CONTROL_KINDS)
        )
        rows = self._database.execute(
            "SELECT operation_id FROM control_operations "
            "WHERE session_id = ? AND state = 'published' "
            f"AND kind IN ({placeholders}) "
            "ORDER BY created_at ASC, rowid ASC",
            (session_id, *SILENT_MAINTENANCE_CONTROL_KINDS),
        ).fetchall()
        settled: list[str] = []
        for row in rows:
            operation_id = str(row["operation_id"])
            if self.acknowledge(operation_id, session_id=session_id):
                settled.append(operation_id)
        return tuple(settled)

    @staticmethod
    def _record(row: sqlite3.Row) -> ControlOperation:
        acknowledged = row["acknowledged_at"]
        reason = row["reason"]
        return ControlOperation(
            operation_id=str(row["operation_id"]),
            schema_version=int(row["schema_version"]),
            kind=str(row["kind"]),
            project_key=str(row["project_key"]),
            cell_id=str(row["cell_id"]),
            session_id=str(row["session_id"]),
            dedup_key=str(row["dedup_key"]),
            result=dict(json.loads(str(row["result_json"]))),
            reason=None if reason is None else str(reason),
            state=str(row["state"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            acknowledged_at=(
                None if acknowledged is None else str(acknowledged)
            ),
        )
