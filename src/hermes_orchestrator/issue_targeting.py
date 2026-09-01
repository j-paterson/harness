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
  admission state, dependency readiness, project occupancy (another
  issue already ``in_development``/``review``), a pending operator
  decision, the named cell's identity/liveness, the named session's
  currency, and the cell's active profile lease — INSIDE the exact
  ``BEGIN IMMEDIATE`` transaction (``Database.transaction()``) that
  publishes the packet through the EXISTING
  ``LeadAssignments.publish_in``. It still does NOT advance
  ``admitted_issues`` or journal any projection: publication alone is
  not the transition (that was the base-revision bug), and premature
  advancement before the exact-session ACK would be a new one.
* :func:`acknowledge_target` is the ACK-gated transition. It consumes
  the exact-session ACK through the EXISTING
  ``LeadAssignments.acknowledge`` compare-and-swap (bound to the exact
  assignment id and session; anything else — wrong session, wrong
  packet, a stale/superseded offer, a duplicate ACK — changes nothing
  and refuses). Only once that CAS lands does it call the CANONICAL
  activation transaction, ``cells.activate_admitted_issue`` — the same
  shared service backing ``ProjectCellService`` explicit dispatch and
  the Stop-hook idle dispatcher — passing ``assignments=None`` so it
  advances ``admitted_issues``, journals ``issue.started``, and begins
  the existing idempotent In Development projection effect WITHOUT
  re-publishing a second packet for the same offer. Every dependency/
  occupancy/decision/lease predicate is therefore re-proven a SECOND
  time, transactionally, at ACK-consumption, exactly as required.

Dependency-eligibility reuse gap (report to the coordinator): the
non-mutating predicate block ``cells.py`` uses at commit time
(``prior_state in _RUNNABLE_ISSUE_STATES``, ``dependency_ready``,
occupancy, pending operator decision — see
``_activate_issue_transaction``, cells.py ~lines 396-422) is NOT
exposed as a public, side-effect-free callable; only the private
module function and the public-but-MUTATING
``activate_admitted_issue`` exist. Because publication must not
mutate ``admitted_issues`` (ACK-gating), :func:`target_issue` cannot
call either one for its publish-time recheck and instead re-reads the
identical predicates directly (duplicated below). If the coordinator
exposes a public ``cells.activation_eligible(connection, issue_id,
project_key) -> bool`` factored out of that block, this duplication
goes away.

Provenance (INFRA-220 requirement 3): the published packet's
``instruction_id`` is always the ADMITTED ISSUE's own durable
``admitted_issues.instruction_id`` — never the operator's CLI text.
The operator's free-text ``instruction`` is journaled as a separate
human-readable event (``issue_targeting.instruction``) in the same
transaction, bound to the published assignment id, rather than being
given a durable column of its own — there is no migration in scope
for this correction (the HARD BOUNDARY forbids one) to add one to
``lead_assignments``.

Outstanding: lane-role / visible-session eligibility (INFRA-219) is
explicitly OUT of scope here. INFRA-219's durable development-lane and
visible-session identity (``project_cells.lane_role``, lane-scoped
leases and cmux bindings) lives only on the unmerged
``feature/infra-219`` branch and is not available on this base; no
local lane model is invented in its place. Once INFRA-219 merges, both
:func:`target_issue` and the canonical activation path it defers to
must additionally require the named session to be that project's
lane-scoped, visible session.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass

from hermes_orchestrator.cells import LinearProjector, activate_admitted_issue
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import IssueState
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.lead_assignments import LeadAssignment, LeadAssignments

_ACTIVE_CELL_STATES = ("starting", "active", "handoff_required", "paused")
_ADMITTED_STATES = (IssueState.QUEUED.value, IssueState.BLOCKED.value)
_CURRENT_SESSION_STATES = ("starting", "active")
_OCCUPYING_ISSUE_STATES = (IssueState.IN_DEVELOPMENT.value, IssueState.REVIEW.value)


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

        occupied = connection.execute(
            "SELECT 1 FROM admitted_issues WHERE project_key = ? "
            "AND state IN (?, ?) AND issue_id != ? LIMIT 1",
            (project_key, *_OCCUPYING_ISSUE_STATES, issue_id),
        ).fetchone()
        if occupied is not None:
            raise IssueTargetingRefused(
                f"project {project_key!r} is already occupied by another issue"
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

        cell_row = connection.execute(
            "SELECT cell_id, profile_alias FROM project_cells "
            "WHERE project_key = ? AND state IN "
            f"({','.join('?' for _ in _ACTIVE_CELL_STATES)})",
            (project_key, *_ACTIVE_CELL_STATES),
        ).fetchone()
        if cell_row is None or str(cell_row["cell_id"]) != cell_id:
            raise IssueTargetingRefused(
                f"cell {cell_id!r} is not the active cell for project {project_key!r}"
            )
        profile_alias = str(cell_row["profile_alias"])

        session_row = connection.execute(
            "SELECT session_id FROM project_cells WHERE cell_id = ? "
            "AND state IN (?, ?)",
            (cell_id, *_CURRENT_SESSION_STATES),
        ).fetchone()
        current_session_id = (
            None if session_row is None else str(session_row["session_id"])
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
    and, only then, advance the target and journal the projection.

    Step 1 — the ACK itself runs through the EXISTING
    ``LeadAssignments.acknowledge`` compare-and-swap: it must bind the
    exact assignment id, the exact bound session, and the ``published``
    state. A missing assignment, the wrong session, a stale/superseded
    offer, or a duplicate ACK all change nothing and this function
    returns with ``activated=False`` — zero further writes are
    attempted.

    Step 2 — only once that CAS lands does this call the CANONICAL
    activation transaction, ``cells.activate_admitted_issue`` (the same
    shared service backing explicit dispatch and the Stop-hook idle
    dispatcher), with ``assignments=None`` so it advances
    ``admitted_issues`` to ``in_development``, journals
    ``issue.started``, and begins the existing idempotent In
    Development projection effect WITHOUT publishing a second packet
    for this same offer. Every dependency/occupancy/decision/lease
    predicate is re-proved a second time there, transactionally.

    A retried ACK for an assignment already ``acknowledged`` fails the
    step-1 CAS and never reaches step 2 — no second effective
    assignment or activation is ever created by a retry.
    """

    acked = assignments.acknowledge(assignment_id, session_id=session_id)
    if not acked:
        return TargetAckResult(
            assignment_id=assignment_id, issue_id="", activated=False
        )

    assignment = assignments.get(assignment_id)
    activated, _ = await activate_admitted_issue(
        database=database,
        events=events,
        linear=linear,
        assignments=None,
        cell_id=assignment.cell_id,
        project_key=assignment.project_key,
        profile_alias=assignment.profile_alias,
        session_id=session_id,
        issue_id=assignment.issue_id,
    )
    return TargetAckResult(
        assignment_id=assignment_id,
        issue_id=assignment.issue_id,
        activated=activated,
    )
