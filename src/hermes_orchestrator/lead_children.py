"""Durable, identity-exact child accounting and lead reactivation.

INFRA-195 (hardened by Sol correction 032cd4a5): a classic lead turn
that ends while background children are still running must resume
without an operator prompt — and never resume early. Child activity is
keyed by the stable hook-provided child identity, so a start records
exactly once per (session, child), a completion compare-and-swaps that
exact identity (a duplicate, a reordered delivery, a foreign session,
or an identity-less event is refused without advancing progress), and
outstanding work is the set of distinct started-but-uncompleted
children. The Stop hook records the continuation condition durably;
the completion that empties the outstanding set compare-and-swaps the
waiting continuation and queues exactly one ``children.completed``
control operation (deduplicated by the continuation id), which wakes
the exact lead through the dedicated channel.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from hermes_orchestrator.control_operations import (
    ControlOperation,
    ControlOperations,
)
from hermes_orchestrator.db import Database


@dataclass(frozen=True, slots=True)
class LeadContinuation:
    """One durable promise that a stopped lead still owes work."""

    continuation_id: str
    session_id: str
    project_key: str
    cell_id: str
    condition: str
    state: str


class LeadChildTracker:
    """Track child identities; reactivate a waiting lead exactly once."""

    def __init__(
        self,
        database: Database,
        *,
        control: ControlOperations,
        ids: Callable[[], str] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._control = control
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._now = now or (lambda: datetime.now(UTC))

    def child_started(self, session_id: str, child_id: str) -> bool:
        """Record one distinct child start; a duplicate is a no-op.

        An identity-less start fails closed: nothing is recorded, so it
        can never create outstanding work no completion could settle.
        """

        if not child_id.strip():
            return False
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO lead_children("
                "session_id, child_id, state, started_at, completed_at) "
                "VALUES (?, ?, 'started', ?, NULL)",
                (session_id, child_id.strip(), stamp),
            )
        return cursor.rowcount == 1

    def child_completed(
        self, session_id: str, child_id: str
    ) -> ControlOperation | None:
        """Settle one exact child; fire the one reactivation when the
        distinct outstanding set becomes empty.

        The completion is a compare-and-swap on the exact (session,
        child) identity in the ``started`` state: a duplicate, a
        completion that precedes its start, a foreign session, or an
        identity-less event changes nothing and never advances
        progress. The reactivation control operation and the
        continuation's compare-and-swap commit in one transaction, so a
        crash can never separate them, and the continuation-id dedup
        key makes any retry a durable no-op.
        """

        if not child_id.strip():
            return None
        stamp = self._now().isoformat()
        operation: ControlOperation | None = None
        with self._database.transaction() as connection:
            settled = connection.execute(
                "UPDATE lead_children SET state = 'completed', "
                "completed_at = ? "
                "WHERE session_id = ? AND child_id = ? "
                "AND state = 'started'",
                (stamp, session_id, child_id.strip()),
            )
            if settled.rowcount != 1:
                return None
            outstanding = connection.execute(
                "SELECT COUNT(*) AS outstanding FROM lead_children "
                "WHERE session_id = ? AND state = 'started'",
                (session_id,),
            ).fetchone()
            if int(outstanding["outstanding"]) > 0:
                return None
            waiting = connection.execute(
                "SELECT continuation_id, project_key, cell_id, condition "
                "FROM lead_continuations "
                "WHERE session_id = ? AND state = 'waiting'",
                (session_id,),
            ).fetchone()
            if waiting is None:
                return None
            reactivated = connection.execute(
                "UPDATE lead_continuations SET state = 'reactivated', "
                "updated_at = ? "
                "WHERE continuation_id = ? AND state = 'waiting'",
                (stamp, str(waiting["continuation_id"])),
            )
            if reactivated.rowcount != 1:
                return None
            completed = connection.execute(
                "SELECT COUNT(*) AS completed FROM lead_children "
                "WHERE session_id = ? AND state = 'completed'",
                (session_id,),
            ).fetchone()
            operation = self._control.record_in(
                connection,
                kind="children.completed",
                project_key=str(waiting["project_key"]),
                cell_id=str(waiting["cell_id"]),
                session_id=session_id,
                result={
                    "completed_children": int(completed["completed"]),
                    "continuation": str(waiting["condition"]),
                },
                dedup_key=(
                    f"children.completed:{waiting['continuation_id']}"
                ),
            )
        if operation is not None:
            self._control.notify_committed(operation)
        return operation

    def record_turn_stop(self, session_id: str) -> LeadContinuation | None:
        """At a Stop boundary, durably promise the outstanding work.

        With distinct children still outstanding, one waiting
        continuation per session is recorded (idempotently); with none,
        any stale waiting continuation is superseded so it can never
        reactivate a lead that owes nothing.
        """

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            counts = connection.execute(
                "SELECT "
                "SUM(CASE WHEN state = 'started' THEN 1 ELSE 0 END) "
                "AS outstanding, "
                "COUNT(*) AS total FROM lead_children "
                "WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            outstanding = int(counts["outstanding"] or 0)
            total = int(counts["total"] or 0)
            if outstanding <= 0:
                connection.execute(
                    "UPDATE lead_continuations SET state = 'superseded', "
                    "updated_at = ? "
                    "WHERE session_id = ? AND state = 'waiting'",
                    (stamp, session_id),
                )
                return None
            cell = connection.execute(
                "SELECT cell_id, project_key FROM project_cells "
                "WHERE session_id = ? AND state = 'active' "
                "ORDER BY updated_at DESC, rowid DESC LIMIT 1",
                (session_id,),
            ).fetchone()
            if cell is None:
                return None
            existing = connection.execute(
                "SELECT continuation_id, project_key, cell_id, condition, "
                "state FROM lead_continuations "
                "WHERE session_id = ? AND state = 'waiting'",
                (session_id,),
            ).fetchone()
            condition = (
                f"{outstanding} background child(ren) outstanding "
                f"({total - outstanding}/{total} complete)"
            )
            if existing is not None:
                connection.execute(
                    "UPDATE lead_continuations SET condition = ?, "
                    "updated_at = ? WHERE continuation_id = ?",
                    (condition, stamp, str(existing["continuation_id"])),
                )
                return LeadContinuation(
                    continuation_id=str(existing["continuation_id"]),
                    session_id=session_id,
                    project_key=str(existing["project_key"]),
                    cell_id=str(existing["cell_id"]),
                    condition=condition,
                    state="waiting",
                )
            continuation_id = self._ids()
            connection.execute(
                "INSERT INTO lead_continuations("
                "continuation_id, session_id, project_key, cell_id, "
                "condition, state, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 'waiting', ?, ?)",
                (
                    continuation_id,
                    session_id,
                    str(cell["project_key"]),
                    str(cell["cell_id"]),
                    condition,
                    stamp,
                    stamp,
                ),
            )
            return LeadContinuation(
                continuation_id=continuation_id,
                session_id=session_id,
                project_key=str(cell["project_key"]),
                cell_id=str(cell["cell_id"]),
                condition=condition,
                state="waiting",
            )
