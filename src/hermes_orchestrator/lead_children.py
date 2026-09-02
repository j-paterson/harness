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

import os
import sqlite3
import sys
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


def _bound_cell(
    connection: sqlite3.Connection, session_id: str
) -> sqlite3.Row | None:
    """The active managed cell this Claude session is bound to, if any.

    The ONE binding predicate every lead-hook path shares (it mirrors
    ``LeadIntakePoll.next_offer`` and the idle dispatch exactly): a
    session with no active ``project_cells`` row is not a managed lead,
    and every hook must be inert for it.
    """

    return connection.execute(
        "SELECT cell_id, project_key FROM project_cells "
        "WHERE session_id = ? AND state = 'active' "
        "ORDER BY updated_at DESC, rowid DESC LIMIT 1",
        (session_id,),
    ).fetchone()


def bound_session_at(
    connection: sqlite3.Connection,
    cwd: str | None,
    workspace_uuid: str | None,
    surface_uuid: str | None,
) -> str | None:
    """The stable session id of the managed seat running in ``cwd``.

    INFRA-198: ``/clear`` gives the live Claude session a new LOGICAL id
    while the process, its channel sidecar, and every durable Hermes row
    keep the seat's original one. The hook payload then carries an id
    ``_bound_cell`` cannot resolve, so every lead hook goes inert.
    Nothing is rebound to the logical id -- durable identity must keep
    agreeing with the running process and the launch argv the trust
    anchor persists. Instead each hook ENTRY canonicalizes its session
    ONCE, from evidence that already exists, persisting nothing:

    cwd -> its ONE project -> that project's ONE active development cell
    -> that cell's ONE active ``channel_registrations`` row, whose
    ``session_id`` IS the bound session. The registration is the
    positive proof the sidecar is connected right now; a lead process
    lease is NOT that proof (a reboot leaves every lead lease stopped
    while the seat is live) and serves only as the cwd->project
    directory. Zero or more than one match at any step returns ``None``
    and the hook stays inert exactly as today.
    """

    rows = connection.execute(
        "SELECT DISTINCT r.session_id FROM channel_registrations r "
        "JOIN project_cells c ON c.cell_id = r.cell_id "
        "AND c.state = 'active' AND c.lane_role = 'development' "
        "AND c.project_key = r.project_key "
        "AND c.session_id = r.session_id "
        "WHERE r.state = 'active' AND r.project_key IN ("
        "SELECT project_key FROM process_leases WHERE cwd = ?)",
        (str(cwd or os.getcwd()),),
    ).fetchall()
    if len(rows) != 1 or not workspace_uuid or not surface_uuid:
        return None
    session_id = str(rows[0]["session_id"])
    anchor = connection.execute(
        "SELECT 1 FROM channel_trust_anchors WHERE state = 'active' "
        "AND session_id = ? AND workspace_uuid = ? AND surface_uuid = ?",
        (session_id, workspace_uuid, surface_uuid),
    ).fetchone()
    return session_id if anchor is not None else None


class LeadChildTracker:
    """Track child identities; reactivate a waiting lead exactly once."""

    def __init__(
        self,
        database: Database,
        *,
        control: ControlOperations,
        ids: Callable[[], str] | None = None,
        now: Callable[[], datetime] | None = None,
        replenish: Callable[[str], None] | None = None,
    ) -> None:
        self._database = database
        self._control = control
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._now = now or (lambda: datetime.now(UTC))
        # INFRA-215: an injectable immediate-replenishment hook, called
        # after a child settles for a project that may now have
        # runnable work. ``None`` is the ordinary shape everywhere this
        # tracker is built ad hoc from a hook payload; the daemon's
        # per-tick sweep still discovers the same work, just not
        # immediately.
        self._replenish = replenish

    def child_started(self, session_id: str, child_id: str) -> bool:
        """Record one distinct child start; a duplicate is a no-op.

        An identity-less start fails closed: nothing is recorded, so it
        can never create outstanding work no completion could settle.

        INFRA-204: so does a start from a session bound to NO active
        managed cell. The SubagentStart hook is installed profile-wide,
        so it fires for every Claude session using that profile,
        including ordinary human sessions the orchestrator does not
        manage. Those must leave nothing behind — an unbound session's
        row would be outstanding work no continuation could ever settle
        and no lead could ever be woken for. The binding predicate is
        the same active-cell lookup :meth:`record_turn_stop`,
        ``LeadIntakePoll.next_offer`` and the idle dispatch already
        use, and it is read inside the insert's own transaction, so a
        cell that deactivates concurrently cannot admit a row.
        """

        if not child_id.strip():
            return False
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            if _bound_cell(connection, session_id) is None:
                return False
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

        INFRA-204 (Sol correction 7a810aad): so does a completion from
        a session bound to NO active managed cell. The compare-and-swap
        alone only covers a session that was NEVER bound (no child row
        matches it); a child row created *while* the session was bound
        outlives the binding, so a later profile-wide SubagentStop
        would still match it, settle the continuation and enqueue
        ``children.completed`` for a lead nothing manages any more. The
        binding is therefore re-read first, inside this same
        transaction, using the one predicate :meth:`child_started`,
        :meth:`record_turn_stop`, ``LeadIntakePoll.next_offer`` and the
        idle dispatch already share; an unbound session changes no
        child row, no continuation row and no control operation, and
        returns the same "no effect" ``None`` the CAS-miss path does.

        INFRA-215: once the child row itself is durably settled (this
        method's own compare-and-swap succeeded), :attr:`_replenish`
        (when wired) is called with the session's project exactly once
        more, AFTER the transaction commits -- whether or not this
        exact completion also happened to empty the outstanding set and
        reactivate a continuation. Child completion is one of the
        events the durable project-completion contract lists as
        needing a fresh eligibility check, independent of whether a
        lead is waiting on it. A failed replenish signal never raises
        here; the daemon's own per-tick sweep repairs it.
        """

        if not child_id.strip():
            return None
        stamp = self._now().isoformat()
        outcome: dict[str, object | None] = {"operation": None, "project_key": None}
        try:
            with self._database.transaction() as connection:
                bound = _bound_cell(connection, session_id)
                if bound is None:
                    return None
                settled = connection.execute(
                    "UPDATE lead_children SET state = 'completed', "
                    "completed_at = ? "
                    "WHERE session_id = ? AND child_id = ? "
                    "AND state = 'started'",
                    (stamp, session_id, child_id.strip()),
                )
                if settled.rowcount != 1:
                    return None
                outcome["project_key"] = str(bound["project_key"])
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
                outcome["operation"] = operation
                return operation
        finally:
            operation = outcome["operation"]
            if operation is not None:
                self._control.notify_committed(operation)
            project_key = outcome["project_key"]
            if project_key is not None and self._replenish is not None:
                try:
                    self._replenish(str(project_key))
                except Exception as error:
                    print(
                        "lead_children: immediate replenish failed for "
                        f"{project_key!r}: {type(error).__name__}: {error}",
                        file=sys.stderr,
                    )

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
            cell = _bound_cell(connection, session_id)
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
