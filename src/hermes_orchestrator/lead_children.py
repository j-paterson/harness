"""Durable child accounting and exactly-once lead reactivation.

INFRA-195: a classic lead turn that ends while background children are
still running must resume without an operator prompt. The
PreToolUse(Agent) hook counts each child start, the SubagentStop hook
counts each completion, and the Stop hook records the continuation
condition durably when children are still outstanding. The completion
that settles the last outstanding child compare-and-swaps the waiting
continuation and queues exactly one ``children.completed`` control
operation (deduplicated by the continuation id), which wakes the exact
lead through the dedicated channel.
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
    """Count child starts/stops; reactivate a waiting lead exactly once."""

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

    def child_started(self, session_id: str) -> int:
        """Durably count one child start; returns the started total."""

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO lead_child_activity("
                "session_id, started, completed, updated_at) "
                "VALUES (?, 1, 0, ?) "
                "ON CONFLICT(session_id) DO UPDATE SET "
                "started = started + 1, updated_at = excluded.updated_at",
                (session_id, stamp),
            )
            row = connection.execute(
                "SELECT started FROM lead_child_activity "
                "WHERE session_id = ?",
                (session_id,),
            ).fetchone()
        return int(row["started"])

    def child_completed(self, session_id: str) -> ControlOperation | None:
        """Durably count one completion; fire the one reactivation.

        The reactivation control operation and the continuation's
        compare-and-swap commit in a single transaction, so a crash can
        never separate them, and the continuation-id dedup key makes a
        duplicate completion (or a lost-ACK retry) a durable no-op.
        """

        stamp = self._now().isoformat()
        operation: ControlOperation | None = None
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO lead_child_activity("
                "session_id, started, completed, updated_at) "
                "VALUES (?, 0, 1, ?) "
                "ON CONFLICT(session_id) DO UPDATE SET "
                "completed = completed + 1, "
                "updated_at = excluded.updated_at",
                (session_id, stamp),
            )
            activity = connection.execute(
                "SELECT started, completed FROM lead_child_activity "
                "WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if int(activity["completed"]) < int(activity["started"]):
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
            operation = self._control.record_in(
                connection,
                kind="children.completed",
                project_key=str(waiting["project_key"]),
                cell_id=str(waiting["cell_id"]),
                session_id=session_id,
                result={
                    "started": int(activity["started"]),
                    "completed": int(activity["completed"]),
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

        With children still outstanding, one waiting continuation per
        session is recorded (idempotently); with none, any stale
        waiting continuation is superseded so it can never reactivate a
        lead that owes nothing.
        """

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            activity = connection.execute(
                "SELECT started, completed FROM lead_child_activity "
                "WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            outstanding = 0 if activity is None else (
                int(activity["started"]) - int(activity["completed"])
            )
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
                f"({activity['completed']}/{activity['started']} complete)"
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
