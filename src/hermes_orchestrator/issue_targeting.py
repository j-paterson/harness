"""Target an explicitly admitted issue at a named eligible cell/session.

INFRA-220: the scheduler preview (``QueueService.list_ranked``) always
picks the oldest equal-priority admitted row, and there was no
supported command to bind a named existing Fable session to one
explicitly selected admitted issue instead. ``target_issue`` is that
one strict transition. It validates entirely against durable state —
the issue is admitted for the project, the project is known, the
named cell is that project's active cell, the named session is
exactly that cell's current session, the cell's profile lease is
active, and a non-empty operator instruction was supplied — and fails
closed with zero writes on any single failed predicate.

On success it publishes the exact assignment packet through the
EXISTING ``LeadAssignments.publish_in`` machinery (one transaction);
it never invents a second offer/ACK path and never fakes an ACK. No
other admitted row is reordered, paused, reprioritized, or deleted.

Projection: at this base revision ``LeadAssignments.acknowledge`` has
no production caller anywhere in this codebase (only test fixtures
call it) — there is no generic ACK-driven "In Development" projection
to reuse yet. This module therefore does not add a projection call of
its own; the durable published packet is the complete, correct output
of this transition, and In Development projection remains future work
for whatever consumes the ACK.

Idempotency: ``publish_in`` treats an unconsumed ``published`` packet
for the same issue+session as a durable no-op and returns ``None``;
this module reads back and returns that existing pending packet, so a
repeat call while a targeting is still pending is idempotent. A repeat
call after the prior packet was acknowledged is NOT idempotent —
``publish_in`` supersedes the consumed packet and publishes a fresh
one, which is the correct behaviour for a deliberate re-target of an
issue whose earlier assignment was already delivered.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass

from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import IssueState
from hermes_orchestrator.lead_assignments import LeadAssignment, LeadAssignments

_ACTIVE_CELL_STATES = ("starting", "active", "handoff_required", "paused")
_ADMITTED_STATES = (IssueState.QUEUED.value, IssueState.BLOCKED.value)
_CURRENT_SESSION_STATES = ("starting", "active")


class IssueTargetingRefused(ValueError):
    """Raised when any durable predicate fails; no write has occurred."""


@dataclass(frozen=True, slots=True)
class TargetIssueResult:
    """Outcome of one ``target_issue`` call."""

    assignment: LeadAssignment
    idempotent: bool


def target_issue(
    database: Database,
    *,
    assignments: LeadAssignments,
    issue_id: str,
    project_key: str,
    cell_id: str,
    session_id: str,
    instruction: str,
    known_projects: Collection[str],
) -> TargetIssueResult:
    """Validate against durable state, then publish one assignment packet.

    Every predicate is read and checked before any transaction opens;
    a single failed predicate raises :class:`IssueTargetingRefused`
    with zero durable writes.
    """

    if not instruction.strip():
        raise IssueTargetingRefused("an operator instruction is required")
    if project_key not in known_projects:
        raise IssueTargetingRefused(f"unknown project {project_key!r}")

    issue_row = database.execute(
        "SELECT state FROM admitted_issues WHERE issue_id = ? AND project_key = ?",
        (issue_id, project_key),
    ).fetchone()
    if issue_row is None or str(issue_row["state"]) not in _ADMITTED_STATES:
        raise IssueTargetingRefused(
            f"issue {issue_id!r} is not admitted for project {project_key!r}"
        )
    issue_state = str(issue_row["state"])

    cell_row = database.execute(
        "SELECT cell_id, session_id, profile_alias FROM project_cells "
        "WHERE project_key = ? AND state IN "
        f"({','.join('?' for _ in _ACTIVE_CELL_STATES)})",
        (project_key, *_ACTIVE_CELL_STATES),
    ).fetchone()
    if cell_row is None or str(cell_row["cell_id"]) != cell_id:
        raise IssueTargetingRefused(
            f"cell {cell_id!r} is not the active cell for project {project_key!r}"
        )
    profile_alias = str(cell_row["profile_alias"])

    session_row = database.execute(
        "SELECT session_id FROM project_cells WHERE cell_id = ? "
        "AND state IN (?, ?)",
        (cell_id, *_CURRENT_SESSION_STATES),
    ).fetchone()
    current_session_id = None if session_row is None else str(session_row["session_id"])
    if current_session_id is None or current_session_id != str(session_id):
        raise IssueTargetingRefused(
            f"session {session_id!r} is not cell {cell_id!r}'s current session"
        )

    lease_row = database.execute(
        "SELECT 1 FROM profile_leases WHERE project_key = ? "
        "AND profile_alias = ? AND state = 'active'",
        (project_key, profile_alias),
    ).fetchone()
    if lease_row is None:
        raise IssueTargetingRefused(
            f"cell {cell_id!r}'s profile lease is not active"
        )

    queue_transition = f"{issue_state}->{IssueState.IN_DEVELOPMENT.value}"
    with database.transaction() as connection:
        published = assignments.publish_in(
            connection,
            project_key=project_key,
            issue_id=issue_id,
            cell_id=cell_id,
            session_id=str(session_id),
            profile_alias=profile_alias,
            instruction_id=instruction,
            queue_transition=queue_transition,
        )
    if published is not None:
        return TargetIssueResult(assignment=published, idempotent=False)

    pending = tuple(
        row
        for row in assignments.pending_for_session(str(session_id))
        if row.issue_id == issue_id
    )
    if not pending:
        # publish_in returned None but the packet was not the pending,
        # unconsumed no-op case (e.g. a same-epoch race lost a CAS);
        # there is nothing live to hand back.
        raise IssueTargetingRefused(
            f"targeting {issue_id!r} could not be published or reconciled"
        )
    return TargetIssueResult(assignment=pending[0], idempotent=True)
