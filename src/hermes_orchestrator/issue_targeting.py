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
* :func:`acknowledge_target` is the ACK-gated transition, in which the
  confirmation is CONSUMED INSIDE the canonical activation transaction
  — after every eligibility predicate, before the commit — so the
  consume and the transition are one commit (Sol correction 25689ebd
  packet 2; see that function's docstring).

Sol correction 9944530c (this revision) landed three fixes:

1. **Exactly-once transition.** The previous revision consumed the
   ACK in its own committed transaction and only then called the
   canonical activation in a second one: an activation failure — or
   eligibility that changed in between — left a permanently consumed
   confirmation that duplicate-ACK refusal could never retry. The
   consume now runs LAST; see :func:`acknowledge_target`. Sol
   correction 25689ebd packet 2 then closed the remaining gap — it
   ran last, but in a SECOND transaction after activation had already
   committed — by moving it inside the activation transaction as a
   post-eligibility callback (``cells`` ``on_eligible``).
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
    HARNESS_LANE,
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


def target_harness_followup(
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
    """Attach one explicitly admitted, dependency-ready project issue to
    a lead already durably bound to a HARNESS-lane primary assignment
    (INFRA-215), as a second, lane-preserving work item — never a
    second seat, and never a new protocol.

    Why this is a single-shot transition, unlike :func:`target_issue`:
    the DEVELOPMENT lane's ``target_issue``/``acknowledge_target`` pair
    exists because that lane's canonical activation
    (``cells._activate_issue_transaction``) is an exact-session-ACK
    protocol — publication and activation are deliberately two
    transactions so the exact confirming session can be proven before
    the queue transitions. The HARNESS lane has no such protocol for
    its own primary activation: ``cells._activate_issue_body`` with
    ``harness=True`` activates only the cell's lease and returns —  it
    never CASes ``admitted_issues``, never journals ``issue.started``,
    and (see below) never calls ``assignments.publish_in`` for the
    issue it was launched against. INFRA-215 is explicit that no new
    protocol is to be introduced for the harness lane, so this function
    does not invent an ACK step either: it re-proves every mutable
    predicate, publishes the assignment through the EXISTING
    ``LeadAssignments.publish_in``, CASes the admitted issue
    ``queued``/``blocked`` -> ``in_development``, and journals
    ``issue.targeted`` — all inside the one ``BEGIN IMMEDIATE``
    transaction ``Database.transaction()`` opens. A single failed
    predicate raises :class:`IssueTargetingRefused` and the whole
    transaction rolls back — zero durable writes.

    How the harness PRIMARY assignment is represented (and therefore
    how "at most zero live follow-ups" is counted): it is represented
    by NOTHING in ``lead_assignments``. ``_activate_issue_body``'s
    publish step is gated ``if not harness and assignments is not
    None`` — the harness path never reaches it. A harness cell's
    primary issue lives only in ``project_cells`` (the cell's own
    ``session_id``/``profile_alias`` binding) and in the launch prompt
    text; it never gets a durable assignment packet of its own. So
    "the harness cell currently has at most ZERO live follow-up
    assignments" reduces to a plain count: zero ``lead_assignments``
    rows in a state other than ``superseded`` exist for this exact
    ``cell_id``/``session_id`` pair — there is no primary row to
    exclude from that count, because one is never written. Exactly one
    follow-up may be live at a time; calling this again while the
    first follow-up is still ``published`` or ``acknowledged`` refuses
    closed with zero writes.

    Capacity: the project's canonical development-lane bound
    (:func:`cells.development_lane_saturated`) is re-proven here too —
    once this follow-up transitions to ``in_development`` it occupies
    that same project-wide, lane-agnostic count
    (``_OCCUPYING_ISSUE_STATES`` is keyed on ``admitted_issues.state``
    alone, not on which cell/lane owns the issue), so admitting it must
    not itself push the project over the bound the development lane's
    own dispatch relies on.

    Never inserts a ``project_cells`` row, and never opens the Linear
    "In Development" projection effect that the canonical development
    activation opens — the harness lane's product-issue work stays as
    invisible to Linear as the harness lane's own primary work always
    has been. The issue's normal Sol/candidate path is driven off
    ``admitted_issues`` state independent of that projection and is
    unaffected; it may still run to completion the ordinary way once
    Fable produces a change for it.
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

        # The exact named cell, its required HARNESS lane_role, and its
        # liveness are all predicates of this one query — the same
        # shape as ``target_issue``'s development-lane lookup, just
        # bound to the other lane.
        cell_row = connection.execute(
            "SELECT profile_alias, session_id, state FROM project_cells "
            "WHERE cell_id = ? AND project_key = ? AND lane_role = ? "
            f"AND state IN ({','.join('?' for _ in _ACTIVE_CELL_STATES)})",
            (cell_id, project_key, HARNESS_LANE, *_ACTIVE_CELL_STATES),
        ).fetchone()
        if cell_row is None:
            raise IssueTargetingRefused(
                f"cell {cell_id!r} is not a live harness cell for "
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
                f"session {session_id!r} is not harness cell {cell_id!r}'s "
                "current session"
            )

        lease_row = connection.execute(
            "SELECT 1 FROM profile_leases WHERE project_key = ? "
            "AND profile_alias = ? AND state = 'active'",
            (project_key, profile_alias),
        ).fetchone()
        if lease_row is None:
            raise IssueTargetingRefused(
                f"harness cell {cell_id!r}'s profile lease is not active"
            )

        # Exactly one follow-up at a time. There is no primary row to
        # exclude from this count -- see the docstring above -- so any
        # live (non-superseded) packet at all for this cell/session is
        # a live follow-up already occupying the one seat.
        live_followup = connection.execute(
            "SELECT 1 FROM lead_assignments WHERE cell_id = ? "
            "AND session_id = ? AND state != 'superseded' LIMIT 1",
            (cell_id, str(session_id)),
        ).fetchone()
        if live_followup is not None:
            raise IssueTargetingRefused(
                f"harness cell {cell_id!r} already has a live follow-up "
                "assignment"
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
        if published is None:
            raise IssueTargetingRefused(
                f"targeting {issue_id!r} could not be published as a "
                "harness follow-up"
            )

        updated = connection.execute(
            "UPDATE admitted_issues SET state = ?, updated_at = ? "
            "WHERE issue_id = ? AND state = ?",
            (
                IssueState.IN_DEVELOPMENT.value,
                published.updated_at,
                issue_id,
                issue_state,
            ),
        )
        if updated.rowcount == 0:
            raise IssueTargetingRefused(
                f"issue {issue_id!r} changed state before it could be "
                "activated as a harness follow-up"
            )

        events.append(
            connection,
            EventInput(
                event_type="issue.targeted",
                aggregate_type="issue",
                aggregate_id=issue_id,
                payload={
                    "cell_id": cell_id,
                    "session_id": str(session_id),
                    "profile_alias": profile_alias,
                    "lane_role": HARNESS_LANE,
                    "assignment_id": published.assignment_id,
                },
            ),
        )
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

    return TargetIssueResult(assignment=published, idempotent=False)


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
    and advance the target — ATOMICALLY: the confirmation and the
    canonical activation commit together or neither commits.

    Sol correction 25689ebd packet 2. The previous revision consumed
    the confirmation AFTER the activation transaction had already
    committed, arguing that SQLite's lack of nested transactions made
    atomicity impossible without either re-implementing the activation
    state machine here or consuming inside the PRE-eligibility
    ``guard`` (which commits even when eligibility later refuses — the
    original bug). Both alternatives are still refused; the way
    through is a POST-ELIGIBILITY callback that runs INSIDE the
    canonical activation's own ``BEGIN IMMEDIATE``, on that
    transaction's own connection:

    1. Re-read the packet and refuse anything that is not the exact,
       still-``published``, exactly-session-bound confirmation. Zero
       writes, and a duplicate ACK after a successful transition stops
       here (the packet is ``acknowledged`` by then) — so the
       activation transaction is not even entered a second time.
    2. Call the CANONICAL activation, ``activate_admitted_issue``,
       with ``assignments=None`` (it advances ``admitted_issues``,
       journals ``issue.started``, and begins the existing idempotent
       In Development projection effect without publishing a second
       packet for this same offer), with a ``guard`` that re-proves the
       confirmation's exactness transactionally BEFORE any write, and
       with ``on_eligible`` — the post-eligibility hook — that consumes
       the exact confirmation through the in-transaction primitive
       ``LeadAssignments.acknowledge_in``.
    3. That hook runs LAST inside the activation transaction: after
       every eligibility predicate (dependency readiness, the canonical
       development-lane bound, operator decisions, the cell's current
       identity and its active lease, the ``admitted_issues`` CAS) has
       passed, and before the commit. If its CAS fails — a supersession
       that landed in exactly the window the previous ordering left
       open — it returns False and the WHOLE transaction rolls back:
       the issue never transitions, ``issue.started`` is never
       journaled, no projection effect is opened, and the packet is
       left exactly as it was.

    Sol correction 25689ebd packet 1 is the other half of step (3):
    "the cell's current identity" is now literally that. The canonical
    activation's ``project_cells`` CAS re-proves the cell's CURRENT
    ``project_key``, ``lane_role``, ``profile_alias`` and ``session_id``
    against the ones the packet named, so a cell that rotated to a new
    session or profile — or moved lanes — between publication and the
    ACK can no longer be activated on behalf of the stale identity that
    named it. Validating this module's own assignment row is not the
    same thing as proving the cell still IS what the packet named.

    So there is no longer any gap: the exact confirmation is consumed
    in the same commit that transitions the issue. A refusal anywhere —
    the guard, any eligibility predicate, or the ACK CAS — leaves ZERO
    writes, including the packet, still ``published`` and still pending
    for its session, hence retryable.

    Exactly-once across a crash: a crash before the commit leaves
    nothing durable and the retry does the whole thing once; a crash
    after it leaves the packet ``acknowledged``, so the retry stops at
    step (1) and never re-enters activation. One transition, one
    ``issue.started``, one projection effect, one consumed
    confirmation, however many times this is retried. Concurrent
    duplicates race on that single in-transaction CAS: the loser
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

    def consume_confirmation(connection: sqlite3.Connection) -> bool:
        """The post-eligibility hook: consume THIS exact confirmation
        inside the activation transaction, after every eligibility
        predicate has passed and before the commit.

        Its CAS is the EXISTING one (``LeadAssignments.acknowledge_in``
        — the same compare-and-swap ``acknowledge`` runs, on the
        caller's connection instead of its own), so a supersession that
        landed in this window makes it fail here, and the activation
        that it gates rolls back with it.
        """

        return assignments.acknowledge_in(
            connection, assignment_id, session_id=str(session_id)
        )

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
        on_eligible=consume_confirmation,
        projection_key=assignment_id,
    )
    # ``activated`` is now one answer for both writes: the transition
    # and the consume shared a single commit.
    return TargetAckResult(
        assignment_id=assignment_id,
        issue_id=assignment.issue_id,
        activated=activated,
    )
