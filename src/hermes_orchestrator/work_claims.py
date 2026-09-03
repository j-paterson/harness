"""Cell work ownership, separated from packet delivery state.

INFRA-222: ``lead_assignments`` is a versioned packet-delivery record
(published / acknowledged / superseded) that today also gets read as
"what is this cell working on", and ``project_cells`` carries the
current worker identity (session_id, profile_alias) directly on the
cell row, so rotating the worker means overwriting the only durable
record of who is bound to the cell. This module lands the two
concerns delivery conflated together:

* :class:`WorkClaims` -- which cell owns which (issue, role) work.
  A claim opens when work starts and survives any number of
  superseded / re-published assignment packets; it closes only on an
  explicit ownership event (issue completion, reassignment), never as
  a side effect of packet lifecycle. A development claim, a harness
  claim, and a review claim for the SAME issue coexist by design.
* :class:`WorkerBindings` -- which concrete worker (session, profile,
  cmux surface) currently occupies a cell, as a monotonic generation
  sequence rather than a single mutable pair of columns. Rotating the
  worker retires generation N and binds generation N+1 in one
  compare-and-swap; the cell and any open work claims are untouched.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

WORK_CLAIM_ROLES = frozenset({"development", "harness", "review"})


class ClaimRefused(ValueError):
    """Raised when a durable predicate fails; no write has occurred."""


@dataclass(frozen=True, slots=True)
class WorkClaim:
    """One durable record of a cell owning (issue, role) work."""

    claim_id: str
    project_key: str
    issue_id: str
    cell_id: str
    role: str
    child_lane: str
    state: str
    opened_at: str
    closed_at: str | None
    closed_reason: str | None
    updated_at: str

    def as_dict(self) -> dict[str, object]:
        return {
            "claim_id": self.claim_id,
            "project_key": self.project_key,
            "issue_id": self.issue_id,
            "cell_id": self.cell_id,
            "role": self.role,
            "child_lane": self.child_lane,
            "state": self.state,
            "opened_at": self.opened_at,
            "closed_at": self.closed_at,
            "closed_reason": self.closed_reason,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True, slots=True)
class WorkerBinding:
    """One durable record of a worker occupying a cell generation."""

    binding_id: str
    cell_id: str
    generation: int
    session_id: str
    profile_alias: str
    cmux_surface_uuid: str | None
    state: str
    bound_at: str
    retired_at: str | None
    retired_reason: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "binding_id": self.binding_id,
            "cell_id": self.cell_id,
            "generation": self.generation,
            "session_id": self.session_id,
            "profile_alias": self.profile_alias,
            "cmux_surface_uuid": self.cmux_surface_uuid,
            "state": self.state,
            "bound_at": self.bound_at,
            "retired_at": self.retired_at,
            "retired_reason": self.retired_reason,
        }


class WorkClaims:
    """Open, close, and read durable cell work-ownership claims."""

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

    def open_in(
        self,
        connection: sqlite3.Connection,
        *,
        project_key: str,
        issue_id: str,
        cell_id: str,
        role: str,
        child_lane: str = "",
    ) -> WorkClaim:
        """Open one ownership claim, idempotently.

        An existing active claim for the exact same (issue_id,
        cell_id, role, child_lane) key is returned unchanged -- no
        insert, no event -- so a retried "start work" call never
        duplicates the live claim.
        """

        if role not in WORK_CLAIM_ROLES:
            raise ClaimRefused(f"unknown work claim role {role!r}")

        existing = connection.execute(
            "SELECT * FROM work_claims WHERE issue_id = ? AND cell_id = ? "
            "AND role = ? AND child_lane = ? AND state = 'active'",
            (issue_id, cell_id, role, child_lane),
        ).fetchone()
        if existing is not None:
            return self._record(existing)

        stamp = self._now().isoformat()
        claim_id = self._ids()
        connection.execute(
            "INSERT INTO work_claims("
            "claim_id, project_key, issue_id, cell_id, role, child_lane, "
            "state, opened_at, closed_at, closed_reason, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 'active', ?, NULL, NULL, ?)",
            (
                claim_id,
                project_key,
                issue_id,
                cell_id,
                role,
                child_lane,
                stamp,
                stamp,
            ),
        )
        self._events.append(
            connection,
            EventInput(
                event_type="work_claim.opened",
                aggregate_type="work_claim",
                aggregate_id=claim_id,
                payload={
                    "project_key": project_key,
                    "issue_id": issue_id,
                    "cell_id": cell_id,
                    "role": role,
                    "child_lane": child_lane,
                },
            ),
        )
        return WorkClaim(
            claim_id=claim_id,
            project_key=project_key,
            issue_id=issue_id,
            cell_id=cell_id,
            role=role,
            child_lane=child_lane,
            state="active",
            opened_at=stamp,
            closed_at=None,
            closed_reason=None,
            updated_at=stamp,
        )

    def open(
        self,
        *,
        project_key: str,
        issue_id: str,
        cell_id: str,
        role: str,
        child_lane: str = "",
    ) -> WorkClaim:
        """Standalone wrapper around :meth:`open_in` for callers with
        no transaction of their own."""

        with self._database.transaction() as connection:
            return self.open_in(
                connection,
                project_key=project_key,
                issue_id=issue_id,
                cell_id=cell_id,
                role=role,
                child_lane=child_lane,
            )

    def close_in(
        self,
        connection: sqlite3.Connection,
        *,
        claim_id: str,
        reason: str,
    ) -> WorkClaim:
        """Close one active claim; refuses unknown or already-closed."""

        stamp = self._now().isoformat()
        cursor = connection.execute(
            "UPDATE work_claims SET state = 'closed', closed_at = ?, "
            "closed_reason = ?, updated_at = ? "
            "WHERE claim_id = ? AND state = 'active'",
            (stamp, reason, stamp, claim_id),
        )
        if cursor.rowcount != 1:
            raise ClaimRefused(
                f"claim {claim_id!r} is not an active work claim"
            )
        self._events.append(
            connection,
            EventInput(
                event_type="work_claim.closed",
                aggregate_type="work_claim",
                aggregate_id=claim_id,
                payload={"reason": reason},
            ),
        )
        row = connection.execute(
            "SELECT * FROM work_claims WHERE claim_id = ?", (claim_id,)
        ).fetchone()
        assert row is not None
        return self._record(row)

    def close(self, *, claim_id: str, reason: str) -> WorkClaim:
        """Standalone wrapper around :meth:`close_in`."""

        with self._database.transaction() as connection:
            return self.close_in(connection, claim_id=claim_id, reason=reason)

    def close_for_issue_in(
        self,
        connection: sqlite3.Connection,
        *,
        issue_id: str,
        reason: str,
        roles: Sequence[str] | None = None,
    ) -> list[WorkClaim]:
        """Close every active claim for ``issue_id``, journaling each.

        For explicit issue completion / reassignment -- never called
        as a side effect of delivery-packet lifecycle. ``roles``
        narrows which active claims close; ``None`` closes all roles.
        """

        if roles is None:
            rows = connection.execute(
                "SELECT claim_id FROM work_claims "
                "WHERE issue_id = ? AND state = 'active' "
                "ORDER BY opened_at ASC, rowid ASC",
                (issue_id,),
            ).fetchall()
        else:
            placeholders = ", ".join("?" for _ in roles)
            rows = connection.execute(
                "SELECT claim_id FROM work_claims "
                "WHERE issue_id = ? AND state = 'active' "
                f"AND role IN ({placeholders}) "
                "ORDER BY opened_at ASC, rowid ASC",
                (issue_id, *roles),
            ).fetchall()
        return [
            self.close_in(
                connection, claim_id=str(row["claim_id"]), reason=reason
            )
            for row in rows
        ]

    def active_for_cell(self, cell_id: str) -> list[WorkClaim]:
        rows = self._database.execute(
            "SELECT * FROM work_claims WHERE cell_id = ? AND state = 'active' "
            "ORDER BY opened_at ASC, rowid ASC",
            (cell_id,),
        ).fetchall()
        return [self._record(row) for row in rows]

    def current_for_cell(
        self, cell_id: str, *, role: str | None = None
    ) -> WorkClaim | None:
        """The newest active claim for ``cell_id``, optionally scoped
        to one role."""

        if role is None:
            row = self._database.execute(
                "SELECT * FROM work_claims WHERE cell_id = ? "
                "AND state = 'active' "
                "ORDER BY opened_at DESC, rowid DESC LIMIT 1",
                (cell_id,),
            ).fetchone()
        else:
            row = self._database.execute(
                "SELECT * FROM work_claims WHERE cell_id = ? AND role = ? "
                "AND state = 'active' "
                "ORDER BY opened_at DESC, rowid DESC LIMIT 1",
                (cell_id, role),
            ).fetchone()
        return None if row is None else self._record(row)

    def active_for_issue(self, issue_id: str) -> list[WorkClaim]:
        rows = self._database.execute(
            "SELECT * FROM work_claims WHERE issue_id = ? "
            "AND state = 'active' "
            "ORDER BY opened_at ASC, rowid ASC",
            (issue_id,),
        ).fetchall()
        return [self._record(row) for row in rows]

    def get(self, claim_id: str) -> WorkClaim:
        row = self._database.execute(
            "SELECT * FROM work_claims WHERE claim_id = ?", (claim_id,)
        ).fetchone()
        if row is None:
            raise KeyError(claim_id)
        return self._record(row)

    @staticmethod
    def _record(row: sqlite3.Row) -> WorkClaim:
        return WorkClaim(
            claim_id=str(row["claim_id"]),
            project_key=str(row["project_key"]),
            issue_id=str(row["issue_id"]),
            cell_id=str(row["cell_id"]),
            role=str(row["role"]),
            child_lane=str(row["child_lane"]),
            state=str(row["state"]),
            opened_at=str(row["opened_at"]),
            closed_at=(
                None if row["closed_at"] is None else str(row["closed_at"])
            ),
            closed_reason=(
                None
                if row["closed_reason"] is None
                else str(row["closed_reason"])
            ),
            updated_at=str(row["updated_at"]),
        )


class WorkerBindings:
    """Bind, swap, and read the durable worker identity for a cell."""

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

    def bind_in(
        self,
        connection: sqlite3.Connection,
        *,
        cell_id: str,
        session_id: str,
        profile_alias: str,
        cmux_surface_uuid: str | None = None,
    ) -> WorkerBinding:
        """Bind a fresh generation-1-or-next worker identity.

        Refuses when an active binding already exists for the cell --
        use :meth:`swap_in` to replace a live worker.
        """

        existing = connection.execute(
            "SELECT 1 FROM worker_bindings WHERE cell_id = ? "
            "AND state = 'active'",
            (cell_id,),
        ).fetchone()
        if existing is not None:
            raise ClaimRefused(
                f"cell {cell_id!r} already has an active worker binding"
            )
        return self._bind_next_generation(
            connection,
            cell_id=cell_id,
            session_id=session_id,
            profile_alias=profile_alias,
            cmux_surface_uuid=cmux_surface_uuid,
        )

    def bind(
        self,
        *,
        cell_id: str,
        session_id: str,
        profile_alias: str,
        cmux_surface_uuid: str | None = None,
    ) -> WorkerBinding:
        """Standalone wrapper around :meth:`bind_in`."""

        with self._database.transaction() as connection:
            return self.bind_in(
                connection,
                cell_id=cell_id,
                session_id=session_id,
                profile_alias=profile_alias,
                cmux_surface_uuid=cmux_surface_uuid,
            )

    def swap_in(
        self,
        connection: sqlite3.Connection,
        *,
        cell_id: str,
        expected_session_id: str,
        session_id: str,
        profile_alias: str,
        cmux_surface_uuid: str | None = None,
        reason: str,
    ) -> WorkerBinding:
        """Retire the active binding and bind the next generation, as
        one compare-and-swap.

        The retirement only takes effect if the currently active
        binding's ``session_id`` matches ``expected_session_id``;
        anything else -- no active binding, or a foreign/stale session
        -- refuses and leaves both the old and (absent) new binding
        untouched.
        """

        stamp = self._now().isoformat()
        active = connection.execute(
            "SELECT binding_id FROM worker_bindings WHERE cell_id = ? "
            "AND state = 'active'",
            (cell_id,),
        ).fetchone()
        if active is None:
            raise ClaimRefused(
                f"cell {cell_id!r} has no active worker binding to swap"
            )
        binding_id = str(active["binding_id"])
        cursor = connection.execute(
            "UPDATE worker_bindings SET state = 'retired', retired_at = ?, "
            "retired_reason = ? "
            "WHERE binding_id = ? AND session_id = ? AND state = 'active'",
            (stamp, reason, binding_id, expected_session_id),
        )
        if cursor.rowcount != 1:
            raise ClaimRefused(
                f"cell {cell_id!r}'s active binding session does not match "
                f"expected_session_id {expected_session_id!r}"
            )
        self._events.append(
            connection,
            EventInput(
                event_type="worker_binding.retired",
                aggregate_type="worker_binding",
                aggregate_id=binding_id,
                payload={
                    "cell_id": cell_id,
                    "session_id": expected_session_id,
                    "reason": reason,
                },
            ),
        )
        return self._bind_next_generation(
            connection,
            cell_id=cell_id,
            session_id=session_id,
            profile_alias=profile_alias,
            cmux_surface_uuid=cmux_surface_uuid,
        )

    def swap(
        self,
        *,
        cell_id: str,
        expected_session_id: str,
        session_id: str,
        profile_alias: str,
        cmux_surface_uuid: str | None = None,
        reason: str,
    ) -> WorkerBinding:
        """Standalone wrapper around :meth:`swap_in`."""

        with self._database.transaction() as connection:
            return self.swap_in(
                connection,
                cell_id=cell_id,
                expected_session_id=expected_session_id,
                session_id=session_id,
                profile_alias=profile_alias,
                cmux_surface_uuid=cmux_surface_uuid,
                reason=reason,
            )

    def active_for_cell(self, cell_id: str) -> WorkerBinding | None:
        row = self._database.execute(
            "SELECT * FROM worker_bindings WHERE cell_id = ? "
            "AND state = 'active'",
            (cell_id,),
        ).fetchone()
        return None if row is None else self._record(row)

    def history(self, cell_id: str) -> list[WorkerBinding]:
        rows = self._database.execute(
            "SELECT * FROM worker_bindings WHERE cell_id = ? "
            "ORDER BY generation ASC",
            (cell_id,),
        ).fetchall()
        return [self._record(row) for row in rows]

    def _bind_next_generation(
        self,
        connection: sqlite3.Connection,
        *,
        cell_id: str,
        session_id: str,
        profile_alias: str,
        cmux_surface_uuid: str | None,
    ) -> WorkerBinding:
        stamp = self._now().isoformat()
        next_generation = int(
            connection.execute(
                "SELECT coalesce(max(generation), 0) + 1 "
                "FROM worker_bindings WHERE cell_id = ?",
                (cell_id,),
            ).fetchone()[0]
        )
        binding_id = self._ids()
        connection.execute(
            "INSERT INTO worker_bindings("
            "binding_id, cell_id, generation, session_id, profile_alias, "
            "cmux_surface_uuid, state, bound_at, retired_at, "
            "retired_reason) "
            "VALUES (?, ?, ?, ?, ?, ?, 'active', ?, NULL, NULL)",
            (
                binding_id,
                cell_id,
                next_generation,
                session_id,
                profile_alias,
                cmux_surface_uuid,
                stamp,
            ),
        )
        self._events.append(
            connection,
            EventInput(
                event_type="worker_binding.bound",
                aggregate_type="worker_binding",
                aggregate_id=binding_id,
                payload={
                    "cell_id": cell_id,
                    "generation": next_generation,
                    "session_id": session_id,
                    "profile_alias": profile_alias,
                    "cmux_surface_uuid": cmux_surface_uuid,
                },
            ),
        )
        return WorkerBinding(
            binding_id=binding_id,
            cell_id=cell_id,
            generation=next_generation,
            session_id=session_id,
            profile_alias=profile_alias,
            cmux_surface_uuid=cmux_surface_uuid,
            state="active",
            bound_at=stamp,
            retired_at=None,
            retired_reason=None,
        )

    @staticmethod
    def _record(row: sqlite3.Row) -> WorkerBinding:
        return WorkerBinding(
            binding_id=str(row["binding_id"]),
            cell_id=str(row["cell_id"]),
            generation=int(row["generation"]),
            session_id=str(row["session_id"]),
            profile_alias=str(row["profile_alias"]),
            cmux_surface_uuid=(
                None
                if row["cmux_surface_uuid"] is None
                else str(row["cmux_surface_uuid"])
            ),
            state=str(row["state"]),
            bound_at=str(row["bound_at"]),
            retired_at=(
                None if row["retired_at"] is None else str(row["retired_at"])
            ),
            retired_reason=(
                None
                if row["retired_reason"] is None
                else str(row["retired_reason"])
            ),
        )
