"""The durable project-completion contract (INFRA-215).

Hermes owns an event-driven project driver until every explicitly
admitted issue of a project is terminal (done or cancelled) or waiting
on a real operator decision or external dependency. This module is the
PURE read side of that contract: given only durable rows already
committed by other subsystems, it derives what state a project's batch
of admitted work is in and what Hermes should do next. Nothing here
writes anything; every caller that acts on :func:`batch_status` or
:func:`replenishment_targets` performs its own durable write through
its own existing, independently tested path (``QueueService``,
``LeadTerminalWakes``, ...).

Table choices, and why each was picked over a sibling table that looks
similar:

* ``admitted_issues`` is the ONLY source of admitted issue identity and
  state. A project's batch is exactly its rows here -- never Linear,
  never an in-memory queue snapshot.
* Terminal issue states: :class:`~hermes_orchestrator.domain.IssueState`
  defines only ``done`` as a genuinely finished completion state today;
  there is no ``cancelled`` member and no admission/queue path in this
  codebase currently writes one. ``admitted_issues.state`` is a plain
  TEXT column, not a foreign key onto the enum, so :data:`TERMINAL_ISSUE_STATES`
  recognizes the literal string ``"cancelled"`` defensively alongside
  ``IssueState.DONE`` -- ready for an operator-cancellation path the
  issue text names but that does not exist yet, without requiring a
  schema or enum change to land this contract.
* ``runnable`` is ``queued`` AND ``dependency_ready = 1`` AND not
  presently the subject of a pending ``operator_decisions`` row -- an
  issue awaiting an explicit operator choice is not safe to dispatch
  even though its dependency is otherwise satisfied.
* ``pending_candidates`` reads ``wake_deliveries``: a row's ``state``
  walks ``pending -> claimed -> delivered -> admitted -> completed``
  (``codex_merger.py``), with ``rejected``/``deferred`` also terminal
  for that exact candidate identity. Anything short of
  ``completed``/``rejected``/``deferred`` is a candidate genuinely
  in flight through the reviewer pipeline right now.
* ``pending_corrections`` reads ``lead_corrections`` (state ``pending``
  vs. ``acknowledged``, per ``lead_outbox.py``): a correction packet
  the lead has not yet acknowledged and acted on.
* ``pending_merges`` unions two tables that both represent "a verdict
  exists and settlement has not finished": ``merge_settlements`` rows
  short of the terminal ``settled``/``failed`` states (``recorded``,
  ``merging``, ``merged`` -- the verdict is recorded through the
  GitHub-crossing mutation but the durable window is not yet
  journaled), and ``submitted_verdicts`` rows still ``submitted`` (a
  durably persisted verdict whose settlement transaction has not yet
  run). Both are transient by construction; a batch is never complete
  while either is nonzero.
* ``live_leases`` reads ``worktree_leases`` with ``state = 'active'``
  per the caller's exact instruction -- the lease's own teardown states
  (``checkpointed``, ``reclaiming``) are transient waypoints toward
  ``reclaimed`` and are expected to drain on the very next
  reconciliation pass, so counting only ``active`` keeps this a cheap,
  single-predicate read exactly as specified.
* ``operator_decisions`` with ``status = 'pending'`` is the only durable
  record of "waiting on a real operator decision" this codebase has
  today; a concrete external dependency (an outage, a third-party
  approval) has no table of its own yet, so :attr:`BatchStatus.blocker`
  only ever names a pending operator decision.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from hermes_orchestrator.cells import MAX_DEVELOPMENT_ISSUE_LANES
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import IssueState

#: Issue states this batch treats as genuinely finished. See the module
#: docstring for why ``"cancelled"`` is included even though
#: :class:`IssueState` defines no such member yet.
TERMINAL_ISSUE_STATES = frozenset({IssueState.DONE.value, "cancelled"})

#: The two ``admitted_issues`` states that occupy a development lane,
#: mirroring ``cells._OCCUPYING_ISSUE_STATES`` -- kept as an independent
#: literal here rather than importing that private tuple, since this
#: module owns a read-only aggregate view, not lane dispatch itself.
_OCCUPYING_ISSUE_STATES = (IssueState.IN_DEVELOPMENT.value, IssueState.REVIEW.value)


@dataclass(frozen=True, slots=True)
class BatchStatus:
    """The durable, point-in-time shape of one project's admitted batch."""

    project_key: str
    admitted: tuple[str, ...]
    terminal: tuple[str, ...]
    active: tuple[tuple[str, str], ...]
    runnable: tuple[str, ...]
    pending_candidates: int
    pending_corrections: int
    pending_merges: int
    live_leases: int
    next_action: str
    blocker: str | None
    complete: bool


def _occupying_count(database: Database, project_key: str) -> int:
    row = database.execute(
        "SELECT COUNT(*) AS occupying_count FROM admitted_issues "
        "WHERE project_key = ? AND state IN (?, ?)",
        (project_key, *_OCCUPYING_ISSUE_STATES),
    ).fetchone()
    return int(row["occupying_count"]) if row is not None else 0


def _lane_headroom(database: Database, project_key: str) -> int:
    return max(0, MAX_DEVELOPMENT_ISSUE_LANES - _occupying_count(database, project_key))


def _ranked_runnable(
    database: Database,
    project_key: str,
    *,
    priority_ceiling: int | None = None,
) -> tuple[str, ...]:
    """Runnable issue ids, ranked the way the queue ranks them.

    Mirrors ``QueueService.list_ranked``'s ordering (priority ASC,
    admitted_at ASC, overlap_risk ASC, issue_id ASC) but, unlike that
    method, is scoped to exactly one project and to genuinely
    dispatchable rows -- ``list_ranked`` returns every project's
    ``queued``/``blocked`` rows regardless of readiness, which is the
    right shape for its own callers but the wrong shape here.
    """

    query = (
        "SELECT issue_id FROM admitted_issues WHERE project_key = ? "
        "AND state = ? AND dependency_ready = 1 "
        "AND issue_id NOT IN ("
        "SELECT issue_id FROM operator_decisions "
        "WHERE project_key = ? AND status = 'pending')"
    )
    params: list[object] = [project_key, IssueState.QUEUED.value, project_key]
    if priority_ceiling is not None:
        query += " AND priority <= ?"
        params.append(priority_ceiling)
    query += " ORDER BY priority ASC, admitted_at ASC, overlap_risk ASC, issue_id ASC"
    rows = database.execute(query, tuple(params)).fetchall()
    return tuple(str(row["issue_id"]) for row in rows)


def replenishment_targets(
    database: Database,
    project_key: str,
    *,
    priority_ceiling: int | None = None,
) -> tuple[str, ...]:
    """Runnable issue ids ready to dispatch right now, up to lane headroom.

    Ranked exactly as the queue ranks them. Excludes any issue already
    bound to a live (non-``reclaimed``) ``worktree_leases`` row -- that
    is work already in progress, not work to (re-)dispatch. An optional
    ``priority_ceiling`` additionally excludes issues this call's
    resource evidence does not yet authorize; omitted, every runnable
    issue up to headroom is a target (the shape :func:`batch_status`
    needs, which has no resource-sample concept of its own).
    """

    headroom = _lane_headroom(database, project_key)
    if headroom <= 0:
        return ()
    runnable = _ranked_runnable(
        database, project_key, priority_ceiling=priority_ceiling
    )
    if not runnable:
        return ()
    leased = {
        str(row["issue_id"])
        for row in database.execute(
            "SELECT issue_id FROM worktree_leases WHERE project_key = ? "
            "AND state != 'reclaimed'",
            (project_key,),
        ).fetchall()
    }
    targets = tuple(issue_id for issue_id in runnable if issue_id not in leased)
    return targets[:headroom]


def _pending_operator_decision_issue(
    database: Database, project_key: str
) -> str | None:
    row = database.execute(
        "SELECT issue_id FROM operator_decisions WHERE project_key = ? "
        "AND status = 'pending' ORDER BY recorded_at ASC, decision_id ASC LIMIT 1",
        (project_key,),
    ).fetchone()
    return str(row["issue_id"]) if row is not None else None


def batch_status(database: Database, project_key: str, *, now: datetime) -> BatchStatus:
    """Derive one project's batch status from durable rows only.

    See the module docstring for exactly which table backs each field.
    """

    if now.tzinfo is None:
        raise ValueError("batch status time must be timezone-aware")

    admitted_rows = database.execute(
        "SELECT issue_id, state FROM admitted_issues WHERE project_key = ? "
        "ORDER BY issue_id ASC",
        (project_key,),
    ).fetchall()
    admitted = tuple(str(row["issue_id"]) for row in admitted_rows)
    terminal = tuple(
        str(row["issue_id"])
        for row in admitted_rows
        if str(row["state"]) in TERMINAL_ISSUE_STATES
    )
    active = tuple(
        (str(row["issue_id"]), str(row["state"]))
        for row in admitted_rows
        if str(row["state"]) not in TERMINAL_ISSUE_STATES
    )
    runnable = _ranked_runnable(database, project_key)

    pending_candidates = int(
        database.scalar(
            "SELECT COUNT(*) FROM wake_deliveries WHERE project_key = ? "
            "AND state IN ('pending', 'claimed', 'delivered', 'admitted')",
            (project_key,),
        )
        or 0
    )
    pending_corrections = int(
        database.scalar(
            "SELECT COUNT(*) FROM lead_corrections WHERE project_key = ? "
            "AND state = 'pending'",
            (project_key,),
        )
        or 0
    )
    pending_merges = int(
        database.scalar(
            "SELECT COUNT(*) FROM merge_settlements WHERE project_key = ? "
            "AND state IN ('recorded', 'merging', 'merged')",
            (project_key,),
        )
        or 0
    ) + int(
        database.scalar(
            "SELECT COUNT(*) FROM submitted_verdicts WHERE project_key = ? "
            "AND state = 'submitted'",
            (project_key,),
        )
        or 0
    )
    live_leases = int(
        database.scalar(
            "SELECT COUNT(*) FROM worktree_leases WHERE project_key = ? "
            "AND state = 'active'",
            (project_key,),
        )
        or 0
    )

    complete = (
        bool(admitted)
        and not active
        and pending_candidates == 0
        and pending_corrections == 0
        and pending_merges == 0
        and live_leases == 0
    )

    decision_issue = _pending_operator_decision_issue(database, project_key)
    blocker = (
        f"operator_decision:{decision_issue}" if decision_issue is not None else None
    )

    if not admitted:
        next_action = "idle:no_admitted_work"
    elif complete:
        next_action = "complete"
    else:
        headroom = _lane_headroom(database, project_key)
        if runnable and headroom > 0:
            targets = replenishment_targets(database, project_key)
            next_action = "replenish:" + ",".join(targets)
        elif (
            pending_candidates + pending_corrections + pending_merges > 0
            and not runnable
        ):
            next_action = "await_review"
        elif decision_issue is not None:
            next_action = f"await_operator_decision:{decision_issue}"
        else:
            next_action = "await_children"

    return BatchStatus(
        project_key=project_key,
        admitted=admitted,
        terminal=terminal,
        active=active,
        runnable=runnable,
        pending_candidates=pending_candidates,
        pending_corrections=pending_corrections,
        pending_merges=pending_merges,
        live_leases=live_leases,
        next_action=next_action,
        blocker=blocker,
        complete=complete,
    )
