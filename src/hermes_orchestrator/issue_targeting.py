"""Target an explicitly admitted issue at a named eligible cell/session,
and consume that exact session's ACK to advance it (INFRA-220).

Sol correction 7ecf4a57 on the base revision found that this module
implemented packet PUBLICATION only, mislabeled as the transition
itself: it constructed a ``queued->in_development`` label and returned
success while leaving the issue queued, never required the exact-
session intake ACK, never performed the durable In Development
projection, bypassed dependency readiness / occupancy / pending
operator decisions / resource-and-child limits, stored arbitrary CLI
text as ``instruction_id`` instead of the admitted issue's durable
one, and read every mutable predicate BEFORE ``BEGIN IMMEDIATE`` —
letting issue, session, cell, or lease state drift before publication.

This revision:

* :func:`target_issue` re-proves every mutable predicate — issue
  admission state, dependency readiness, the canonical
  development-lane capacity bound, a pending operator decision, the
  named cell's exact identity/lane/liveness, the named session's
  currency, and the cell's active profile lease — INSIDE the exact
  ``BEGIN IMMEDIATE`` transaction (``Database.transaction()``) that
  publishes the packet through the EXISTING
  ``LeadAssignments.publish_in``. It still does NOT advance
  ``admitted_issues`` or journal any projection: publication alone is
  not the transition (that was the base-revision bug), and premature
  advancement before the exact-session ACK would be a new one.
* :func:`acknowledge_target` is the ACK-gated transition, ordered so
  the confirmation is CONSUMED ONLY AFTER activation has committed
  (Sol correction 9944530c packet 1 — see that function's docstring
  for why this is recoverable rather than atomic).

Sol correction 9944530c (this revision) landed three fixes:

1. **Exactly-once transition.** The previous revision consumed the
   ACK in its own committed transaction and only then called the
   canonical activation in a second one: an activation failure — or
   eligibility that changed in between — left a permanently consumed
   confirmation that duplicate-ACK refusal could never retry. The
   consume now runs LAST; see :func:`acknowledge_target`.
2. **Occupancy.** The publish-time occupancy check was a duplicated
   "any other active issue refuses" rule, contradicting the merged
   canonical development model (up to
   :data:`cells.MAX_DEVELOPMENT_ISSUE_LANES` concurrent issue lanes
   per project). The duplicate is deleted; this module now calls the
   canonical predicate, ``cells.development_lane_saturated`` — the
   same bounded COUNT over ``_OCCUPYING_ISSUE_STATES`` that
   ``_activate_issue_transaction`` re-proves at commit time.
3. **Cell selection.** The active-cell query selected an arbitrary
   project cell and compared the id afterwards, so the dual-lane model
   could reject the valid named development cell purely on row order
   (a harness cell, or a stale failed row, coming back first). The
   exact ``cell_id``, the required ``lane_role = 'development'``, and
   the active-state predicate are all in the SQL now.

Provenance (INFRA-220 requirement 3): the published packet's
``instruction_id`` is always the ADMITTED ISSUE's own durable
``admitted_issues.instruction_id`` — never the operator's CLI text.
The operator's free-text ``instruction`` is journaled as a separate
human-readable event (``issue_targeting.instruction``) in the same
transaction, bound to the published assignment id, rather than being
given a durable column of its own — there is no migration in scope
for this correction (the HARD BOUNDARY forbids one) to add one to
``lead_assignments``.

Lane role: INFRA-219's durable dual-lane model IS merged on this base
(``project_cells.lane_role``, lane-scoped profile leases), so the
named cell must be the project's DEVELOPMENT-lane cell — a harness
cell never claims a product issue and can never be targeted. Visible-
session identity (the lane-scoped cmux binding) remains outstanding:
targeting still proves only that the named session is the named
development cell's current session.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Collection
from dataclasses import dataclass

from hermes_orchestrator.cells import (
    DEVELOPMENT_LANE,
    MAX_DEVELOPMENT_ISSUE_LANES,
    LinearProjector,
    activate_admitted_issue,
    development_lane_saturated,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import IssueState
from hermes_orchestrator.events import EventInput, EventStore
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


@dataclass(frozen=True, slots=True)
class TargetAckResult:
    """Outcome of one ``acknowledge_target`` call."""

    assignment_id: str
    issue_id: str
    activated: bool


def target_issue(
    database: Database,
    *,
    events: EventStore,
    assignments: LeadAssignments,
    issue_id: str,
    project_key: str,
    cell_id: str,
    session_id: str,
    instruction: str,
    known_projects: Collection[str],
) -> TargetIssueResult:
    """Publish one assignment packet for an admitted issue — publication
    only; the transition itself happens at :func:`acknowledge_target`.

    Every mutable predicate is re-proved INSIDE the one ``BEGIN
    IMMEDIATE`` transaction that publishes (``Database.transaction()``
    opens it); a single failed predicate raises
    :class:`IssueTargetingRefused` and the whole transaction rolls
    back — zero durable writes.
    """

    if not instruction.strip():
        raise IssueTargetingRefused("an operator instruction is required")
    if project_key not in known_projects:
        raise IssueTargetingRefused(f"unknown project {project_key!r}")

    with database.transaction() as connection:
        issue_row = connection.execute(
            "SELECT state, instruction_id, dependency_ready "
            "FROM admitted_issues WHERE issue_id = ? AND project_key = ?",
            (issue_id, project_key),
        ).fetchone()
        if issue_row is None or str(issue_row["state"]) not in _ADMITTED_STATES:
            raise IssueTargetingRefused(
                f"issue {issue_id!r} is not admitted for project {project_key!r}"
            )
        issue_state = str(issue_row["state"])
        durable_instruction_id = str(issue_row["instruction_id"])

        if not int(issue_row["dependency_ready"]):
            raise IssueTargetingRefused(
                f"issue {issue_id!r} is not dependency-ready"
            )

        # Packet 2: the CANONICAL development-lane capacity predicate,
        # re-proven on this transaction's own connection. NOT a local
        # "is any other issue active" rule — the merged development
        # model permits up to MAX_DEVELOPMENT_ISSUE_LANES concurrent
        # issue lanes per project, and this is the very predicate
        # ``_activate_issue_transaction`` will re-prove again when the
        # ACK is consumed.
        if development_lane_saturated(
            connection, project_key=project_key, issue_id=issue_id
        ):
            raise IssueTargetingRefused(
                f"project {project_key!r}'s development lane already holds "
                f"{MAX_DEVELOPMENT_ISSUE_LANES} other occupying issues"
            )

        pending_decision = connection.execute(
            "SELECT 1 FROM operator_decisions WHERE issue_id = ? "
            "AND status = 'pending' LIMIT 1",
            (issue_id,),
        ).fetchone()
        if pending_decision is not None:
            raise IssueTargetingRefused(
                f"issue {issue_id!r} has a pending operator decision"
            )

        # Packet 3: the EXACT named cell, its required development
        # lane_role, and its liveness are all predicates of this one
        # query. Selecting an arbitrary project row and comparing the
        # id afterwards let row order decide the answer: under the
        # dual-lane model a harness cell (or a stale terminal row)
        # could come back first and the valid named development cell
        # was refused.
        cell_row = connection.execute(
            "SELECT profile_alias, session_id, state FROM project_cells "
            "WHERE cell_id = ? AND project_key = ? AND lane_role = ? "
            f"AND state IN ({','.join('?' for _ in _ACTIVE_CELL_STATES)})",
            (cell_id, project_key, DEVELOPMENT_LANE, *_ACTIVE_CELL_STATES),
        ).fetchone()
        if cell_row is None:
            raise IssueTargetingRefused(
                f"cell {cell_id!r} is not a live development cell for "
                f"project {project_key!r}"
            )
        profile_alias = str(cell_row["profile_alias"])

        current_session_id = (
            None
            if cell_row["session_id"] is None
            or str(cell_row["state"]) not in _CURRENT_SESSION_STATES
            else str(cell_row["session_id"])
        )
        if current_session_id is None or current_session_id != str(session_id):
            raise IssueTargetingRefused(
                f"session {session_id!r} is not cell {cell_id!r}'s current session"
            )

        lease_row = connection.execute(
            "SELECT 1 FROM profile_leases WHERE project_key = ? "
            "AND profile_alias = ? AND state = 'active'",
            (project_key, profile_alias),
        ).fetchone()
        if lease_row is None:
            raise IssueTargetingRefused(
                f"cell {cell_id!r}'s profile lease is not active"
            )

        queue_transition = f"{issue_state}->{IssueState.IN_DEVELOPMENT.value}"
        published = assignments.publish_in(
            connection,
            project_key=project_key,
            issue_id=issue_id,
            cell_id=cell_id,
            session_id=str(session_id),
            profile_alias=profile_alias,
            instruction_id=durable_instruction_id,
            queue_transition=queue_transition,
        )
        if published is not None:
            events.append(
                connection,
                EventInput(
                    event_type="issue_targeting.instruction",
                    aggregate_type="lead_assignment",
                    aggregate_id=published.assignment_id,
                    payload={
                        "issue_id": issue_id,
                        "operator_instruction": instruction,
                    },
                ),
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


async def acknowledge_target(
    database: Database,
    *,
    events: EventStore,
    linear: LinearProjector,
    assignments: LeadAssignments,
    assignment_id: str,
    session_id: str,
) -> TargetAckResult:
    """Consume the exact-session ACK for one published targeting packet
    and advance the target — exactly once, and never consuming the
    confirmation before the activation it gates has committed.

    Sol correction 9944530c packet 1. The previous revision ran the
    ``LeadAssignments.acknowledge`` compare-and-swap in its OWN
    committed transaction and only then called activation in a second
    one. Anything that made activation fail in between — a flipped
    dependency, a saturated lane, a pending operator decision, a lost
    cell lease, a crash — left the packet permanently ``acknowledged``
    with the issue still queued, and every retry then died on the
    duplicate-ACK refusal: an unrecoverable half-transition.

    RECOVERABLE, not atomic, and deliberately so. The canonical
    activation transaction (``cells._activate_issue_transaction``, via
    :func:`cells.activate_admitted_issue`) owns its own ``BEGIN
    IMMEDIATE`` on the single shared connection; SQLite has no nesting
    here, so the only ways to make the two writes one commit would be
    to re-implement the activation state machine inside this module —
    exactly the divergence this module must never introduce — or to
    let the confirmation be consumed by a hook that runs BEFORE the
    activation predicates and therefore commits even when they refuse,
    which is the very bug. Instead the ORDER carries the guarantee:

    1. Re-read the packet and refuse anything that is not the exact,
       still-``published``, exactly-session-bound confirmation. Zero
       writes, and a duplicate ACK after a successful transition stops
       here (the packet is ``acknowledged`` by then) — so the
       activation transaction is not even entered a second time.
    2. Call the CANONICAL activation, ``activate_admitted_issue``,
       with ``assignments=None`` (it advances ``admitted_issues``,
       journals ``issue.started``, and begins the existing idempotent
       In Development projection effect without publishing a second
       packet for this same offer) and with a ``guard`` that re-proves
       the confirmation's exactness TRANSACTIONALLY, on the activation
       transaction's own connection, alongside every dependency /
       lane-capacity / operator-decision / cell-lease predicate. A
       refusal there leaves ZERO writes — including the packet, still
       ``published`` and still pending for its session, hence
       retryable.
    3. Only once activation has COMMITTED is the confirmation consumed,
       through the EXISTING ``LeadAssignments.acknowledge`` CAS.

    Exactly-once across the crash window between (2) and (3): the
    packet is still ``published`` and the issue is already
    ``in_development``, so a retry re-enters activation, which takes
    its replay branch — the cell lease is re-confirmed but
    ``admitted_issues`` is NOT re-CASed, no second ``issue.started`` is
    journaled, and the existing projection effect row is adopted, not
    re-begun — and the ACK CAS then lands. One transition, one event,
    one effect, one consumed confirmation, however many times this is
    retried. Concurrent duplicates race on that single CAS: the loser
    reports ``activated=False`` and has added no effect of its own.
    """

    try:
        assignment = assignments.get(assignment_id)
    except KeyError:
        return TargetAckResult(
            assignment_id=assignment_id, issue_id="", activated=False
        )
    if (
        assignment.session_id != str(session_id)
        or assignment.state != "published"
    ):
        return TargetAckResult(
            assignment_id=assignment_id,
            issue_id=assignment.issue_id,
            activated=False,
        )

    def confirmation_is_exact(connection: sqlite3.Connection) -> bool:
        """Re-prove the exact confirmation inside the activation
        transaction, before it writes anything."""

        row = connection.execute(
            "SELECT 1 FROM lead_assignments WHERE assignment_id = ? "
            "AND session_id = ? AND project_key = ? AND issue_id = ? "
            "AND cell_id = ? AND profile_alias = ? AND state = 'published'",
            (
                assignment_id,
                str(session_id),
                assignment.project_key,
                assignment.issue_id,
                assignment.cell_id,
                assignment.profile_alias,
            ),
        ).fetchone()
        return row is not None

    activated, _ = await activate_admitted_issue(
        database=database,
        events=events,
        linear=linear,
        assignments=None,
        cell_id=assignment.cell_id,
        project_key=assignment.project_key,
        profile_alias=assignment.profile_alias,
        session_id=str(session_id),
        issue_id=assignment.issue_id,
        guard=confirmation_is_exact,
    )
    if not activated:
        # The packet was NOT consumed: it is still published and still
        # pending for its session, so this exact target stays retryable.
        return TargetAckResult(
            assignment_id=assignment_id,
            issue_id=assignment.issue_id,
            activated=False,
        )

    consumed = assignments.acknowledge(
        assignment_id, session_id=str(session_id)
    )
    return TargetAckResult(
        assignment_id=assignment_id,
        issue_id=assignment.issue_id,
        activated=consumed,
    )
