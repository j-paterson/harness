"""Versioned durable lead assignment packets.

INFRA-195: dispatching a queued issue to an already-idle classic lead
must leave a durable, exactly-bound contract — not just a queue
transition. The assignment packet is committed inside the same
transaction as the queue transition, bound to the exact project,
issue, cell, session, profile, instruction id, and transition; the
dedicated channel then wakes that exact session, which fetches the
packet from durable state and ACKs it before work begins. Linear and
the Hermes database remain the payload sources of truth: the packet
carries binding and provenance, never an authoritative issue body.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

ASSIGNMENT_READY = "HERMES_ASSIGNMENT_READY"
ASSIGNMENT_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class LeadAssignment:
    """One durable, exactly-bound assignment packet."""

    assignment_id: str
    schema_version: int
    project_key: str
    issue_id: str
    cell_id: str
    session_id: str
    profile_alias: str
    instruction_id: str
    queue_transition: str
    state: str
    created_at: str
    updated_at: str
    acknowledged_at: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "assignment_id": self.assignment_id,
            "schema_version": self.schema_version,
            "project_key": self.project_key,
            "issue_id": self.issue_id,
            "cell_id": self.cell_id,
            "session_id": self.session_id,
            "profile_alias": self.profile_alias,
            "instruction_id": self.instruction_id,
            "queue_transition": self.queue_transition,
            "state": self.state,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "acknowledged_at": self.acknowledged_at,
        }


class LeadAssignments:
    """Publish, read, acknowledge, and supersede assignment packets."""

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
        self._listeners: list[Callable[[LeadAssignment], None]] = []

    def subscribe(self, listener: Callable[[LeadAssignment], None]) -> None:
        """Register a post-commit listener (e.g. the channel router)."""

        self._listeners.append(listener)

    def notify_committed(self, assignment: LeadAssignment) -> None:
        """Fan a committed assignment out to listeners, best-effort.

        The durable row is already the truth; a listener failure never
        propagates into the dispatch path.
        """

        for listener in self._listeners:
            with suppress(Exception):
                listener(assignment)

    def publish_in(
        self,
        connection: sqlite3.Connection,
        *,
        project_key: str,
        issue_id: str,
        cell_id: str,
        session_id: str,
        profile_alias: str,
        instruction_id: str,
        queue_transition: str,
    ) -> LeadAssignment | None:
        """Publish one packet inside the caller's dispatch transaction.

        Any live assignment for the same issue bound to a different
        (stale) session is superseded first. For this exact issue and
        session, an UNCONSUMED ``published`` packet makes the dispatch
        a durable no-op (retries never duplicate the live contract),
        while an already-acknowledged packet was consumed by a
        completed delivery: a fresh dispatch epoch supersedes it and
        publishes a new packet, so a requeued issue wakes the idle
        lead again.
        """

        stamp = self._now().isoformat()
        stale = connection.execute(
            "UPDATE lead_assignments SET state = 'superseded', "
            "updated_at = ? WHERE issue_id = ? AND session_id != ? "
            "AND state != 'superseded'",
            (stamp, issue_id, session_id),
        )
        if stale.rowcount:
            self._events.append(
                connection,
                EventInput(
                    event_type="assignment.superseded",
                    aggregate_type="lead_assignment",
                    aggregate_id=issue_id,
                    payload={
                        "issue_id": issue_id,
                        "reason": "stale session replaced by re-dispatch",
                        "session_id": session_id,
                    },
                ),
            )
        existing = connection.execute(
            "SELECT assignment_id, state FROM lead_assignments "
            "WHERE issue_id = ? AND session_id = ? AND state != 'superseded'",
            (issue_id, session_id),
        ).fetchone()
        if existing is not None:
            if str(existing["state"]) == "published":
                return None
            consumed = connection.execute(
                "UPDATE lead_assignments SET state = 'superseded', "
                "updated_at = ? WHERE assignment_id = ? "
                "AND state = 'acknowledged'",
                (stamp, str(existing["assignment_id"])),
            )
            if consumed.rowcount != 1:
                return None
            self._events.append(
                connection,
                EventInput(
                    event_type="assignment.superseded",
                    aggregate_type="lead_assignment",
                    aggregate_id=str(existing["assignment_id"]),
                    payload={
                        "issue_id": issue_id,
                        "reason": "consumed packet replaced by a fresh "
                        "dispatch epoch",
                        "session_id": session_id,
                    },
                ),
            )
        assignment_id = self._ids()
        connection.execute(
            "INSERT INTO lead_assignments("
            "assignment_id, schema_version, project_key, issue_id, "
            "cell_id, session_id, profile_alias, instruction_id, "
            "queue_transition, state, created_at, updated_at, "
            "acknowledged_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'published', ?, ?, NULL)",
            (
                assignment_id,
                ASSIGNMENT_SCHEMA_VERSION,
                project_key,
                issue_id,
                cell_id,
                session_id,
                profile_alias,
                instruction_id,
                queue_transition,
                stamp,
                stamp,
            ),
        )
        self._events.append(
            connection,
            EventInput(
                event_type="assignment.published",
                aggregate_type="lead_assignment",
                aggregate_id=assignment_id,
                payload={
                    "project_key": project_key,
                    "issue_id": issue_id,
                    "cell_id": cell_id,
                    "session_id": session_id,
                    "profile_alias": profile_alias,
                    "instruction_id": instruction_id,
                    "queue_transition": queue_transition,
                },
            ),
        )
        return LeadAssignment(
            assignment_id=assignment_id,
            schema_version=ASSIGNMENT_SCHEMA_VERSION,
            project_key=project_key,
            issue_id=issue_id,
            cell_id=cell_id,
            session_id=session_id,
            profile_alias=profile_alias,
            instruction_id=instruction_id,
            queue_transition=queue_transition,
            state="published",
            created_at=stamp,
            updated_at=stamp,
            acknowledged_at=None,
        )

    def acknowledge(self, assignment_id: str, *, session_id: str) -> bool:
        """Record the bound session's exact acknowledgement, once.

        The compare-and-swap requires the exact assignment id, the
        exact bound session, and the ``published`` state; anything
        else — a foreign session, a superseded packet, a duplicate
        acknowledgement — changes nothing and returns False.
        """

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE lead_assignments SET state = 'acknowledged', "
                "acknowledged_at = ?, updated_at = ? "
                "WHERE assignment_id = ? AND session_id = ? "
                "AND state = 'published'",
                (stamp, stamp, assignment_id, session_id),
            )
            if cursor.rowcount != 1:
                return False
            self._events.append(
                connection,
                EventInput(
                    event_type="assignment.acknowledged",
                    aggregate_type="lead_assignment",
                    aggregate_id=assignment_id,
                    payload={"session_id": session_id},
                ),
            )
        return True

    def get(self, assignment_id: str) -> LeadAssignment:
        row = self._database.execute(
            "SELECT * FROM lead_assignments WHERE assignment_id = ?",
            (assignment_id,),
        ).fetchone()
        if row is None:
            raise KeyError(assignment_id)
        return self._record(row)

    def pending_for_session(
        self, session_id: str
    ) -> tuple[LeadAssignment, ...]:
        rows = self._database.execute(
            "SELECT * FROM lead_assignments "
            "WHERE session_id = ? AND state = 'published' "
            "ORDER BY created_at ASC, rowid ASC",
            (session_id,),
        ).fetchall()
        return tuple(self._record(row) for row in rows)

    @staticmethod
    def _record(row: sqlite3.Row) -> LeadAssignment:
        acknowledged = row["acknowledged_at"]
        return LeadAssignment(
            assignment_id=str(row["assignment_id"]),
            schema_version=int(row["schema_version"]),
            project_key=str(row["project_key"]),
            issue_id=str(row["issue_id"]),
            cell_id=str(row["cell_id"]),
            session_id=str(row["session_id"]),
            profile_alias=str(row["profile_alias"]),
            instruction_id=str(row["instruction_id"]),
            queue_transition=str(row["queue_transition"]),
            state=str(row["state"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            acknowledged_at=(
                None if acknowledged is None else str(acknowledged)
            ),
        )
