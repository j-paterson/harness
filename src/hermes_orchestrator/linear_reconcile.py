"""Reconcile stale local queue projections from live Linear reads (INFRA-230).

``admitted_issues`` is durable local state that can drift from Linear: an
issue can be marked Done in Linear while its local row is still stuck in
``review`` (or any other non-terminal state) because the settlement path
that would normally close it never ran or ran before this local row
existed. A stale row keeps counting as active work everywhere occupancy
and lane saturation are computed from ``admitted_issues.state``
(``cells.development_lane_saturated``, ``project_driver.batch_status``),
which starves rotation and worktree selection on work that is already
finished.

:class:`LinearQueueReconciler` is a cheap, read-only-except-for-repair
pass: for each admitted issue that is not already locally terminal, it
takes exactly one bounded Linear read and, only when that read proves the
issue is genuinely finished, drives the SAME atomic compare-and-swap
transition (:meth:`QueueService.transition_if`) that every other
completion path uses. It never mutates Linear, never re-derives a status
Linear did not report, and never regresses a row that is already done.
A transient read failure isolates exactly the one issue it happened to
and never stops the pass; every other issue -- including ones in other
projects -- is still reconciled in the same run.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import IssueState
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.project_driver import TERMINAL_ISSUE_STATES
from hermes_orchestrator.queue import _LINEAR_TERMINAL_STATUSES, QueueService
from hermes_orchestrator.reconcile import LinearReadPort
from hermes_orchestrator.worktrees import WorktreeCustodian, WorktreeLeases

#: Every locally non-terminal state a Linear-terminal read may complete
#: from, mirroring the source states the merge-settlement path already
#: transitions through on the way to ``done``.
_RECLAIMABLE_FROM_STATES = frozenset(
    {
        IssueState.QUEUED,
        IssueState.BLOCKED,
        IssueState.PAUSED,
        IssueState.IN_DEVELOPMENT,
        IssueState.REVIEW,
        IssueState.QA,
        IssueState.POST_MERGE_ACCEPTANCE,
    }
)

IssueReconcileAction = Literal["completed", "unchanged", "unavailable", "refused"]


@dataclass(frozen=True, slots=True)
class IssueReconcileOutcome:
    """The structured result of reconciling one admitted issue."""

    issue_id: str
    project_key: str
    local_state: str
    linear_status: str | None
    action: IssueReconcileAction
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "issue_id": self.issue_id,
            "project_key": self.project_key,
            "local_state": self.local_state,
            "linear_status": self.linear_status,
            "action": self.action,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class LinearReconcileReport:
    """The completed pass and its per-issue outcomes."""

    outcomes: tuple[IssueReconcileOutcome, ...]
    completed: int
    unchanged: int
    unavailable: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "outcomes": [outcome.as_dict() for outcome in self.outcomes],
            "completed": self.completed,
            "unchanged": self.unchanged,
            "unavailable": self.unavailable,
        }


def _utc_now() -> datetime:
    return datetime.now(UTC)


class LinearQueueReconciler:
    """Reconcile stale local admitted-issue projections against Linear.

    Every effect goes through an already-tested, independently-guarded
    path: the state transition is :meth:`QueueService.transition_if`'s
    atomic compare-and-swap (which itself closes any open work claim in
    the same transaction when the target is ``done``), and the worktree
    lease release -- when a custodian is supplied -- is the existing
    checkpoint / verify-remote / reclaim custodian chain, never a forced
    release. A lease or a Linear read that cannot be proven safe is left
    exactly as found and reported, never forced.
    """

    def __init__(
        self,
        database: Database,
        *,
        queue: QueueService,
        linear_reads: LinearReadPort,
        events: EventStore,
        custodian: WorktreeCustodian | None = None,
        leases: WorktreeLeases | None = None,
        now: Callable[[], datetime] | None = None,
        max_reads: int | None = None,
    ) -> None:
        self._database = database
        self._queue = queue
        self._linear_reads = linear_reads
        self._events = events
        self._custodian = custodian
        self._leases = leases
        self._now = now or _utc_now
        self._max_reads = max_reads

    def run(
        self,
        project_key: str | None = None,
        *,
        issue_ids: Sequence[str] | None = None,
    ) -> LinearReconcileReport:
        """Reconcile every non-terminal admitted issue in one bounded pass.

        Optionally narrowed to one project and/or an explicit set of
        issue ids. Never raises: every per-issue failure is isolated to
        that issue's own outcome so unrelated projects keep moving.
        """

        rows = self._candidates(project_key, issue_ids)
        outcomes: list[IssueReconcileOutcome] = []
        completed = 0
        unchanged = 0
        unavailable = 0
        reads_used = 0

        for row in rows:
            issue_id = str(row["issue_id"])
            row_project_key = str(row["project_key"])
            local_state = str(row["state"])

            if self._max_reads is not None and reads_used >= self._max_reads:
                outcomes.append(
                    IssueReconcileOutcome(
                        issue_id=issue_id,
                        project_key=row_project_key,
                        local_state=local_state,
                        linear_status=None,
                        action="unchanged",
                        detail="read budget exhausted",
                    )
                )
                unchanged += 1
                continue

            try:
                reads_used += 1
                status = str(self._linear_reads.get_issue(issue_id).status)

                if status not in _LINEAR_TERMINAL_STATUSES:
                    outcomes.append(
                        IssueReconcileOutcome(
                            issue_id=issue_id,
                            project_key=row_project_key,
                            local_state=local_state,
                            linear_status=status,
                            action="unchanged",
                            detail=f"linear status {status!r} is not terminal",
                        )
                    )
                    unchanged += 1
                    continue

                updated = self._queue.transition_if(
                    issue_id,
                    IssueState.DONE,
                    from_states=_RECLAIMABLE_FROM_STATES,
                    actor="linear_reconcile",
                    reason=f"linear status {status}",
                )
                if updated is None:
                    # Already settled done by another path between the
                    # candidate read and this transition attempt.
                    outcomes.append(
                        IssueReconcileOutcome(
                            issue_id=issue_id,
                            project_key=row_project_key,
                            local_state=local_state,
                            linear_status=status,
                            action="unchanged",
                            detail="issue already reconciled",
                        )
                    )
                    unchanged += 1
                    continue

                self._release_leases(row_project_key, issue_id)
                self._journal(
                    "issue.reconciled_from_linear",
                    issue_id=issue_id,
                    payload={
                        "project_key": row_project_key,
                        "from_state": local_state,
                        "linear_status": status,
                    },
                )
                outcomes.append(
                    IssueReconcileOutcome(
                        issue_id=issue_id,
                        project_key=row_project_key,
                        local_state=local_state,
                        linear_status=status,
                        action="completed",
                        detail=f"linear status {status}",
                    )
                )
                completed += 1
            except Exception as error:  # isolate this issue only
                detail = f"{type(error).__name__}: {error}"
                self._journal(
                    "issue.linear_unavailable",
                    issue_id=issue_id,
                    payload={"project_key": row_project_key, "error": detail},
                )
                outcomes.append(
                    IssueReconcileOutcome(
                        issue_id=issue_id,
                        project_key=row_project_key,
                        local_state=local_state,
                        linear_status=None,
                        action="unavailable",
                        detail=detail,
                    )
                )
                unavailable += 1

        return LinearReconcileReport(
            outcomes=tuple(outcomes),
            completed=completed,
            unchanged=unchanged,
            unavailable=unavailable,
        )

    def _candidates(
        self, project_key: str | None, issue_ids: Sequence[str] | None
    ) -> list[Any]:
        clauses = [
            "state NOT IN ({})".format(
                ",".join("?" for _ in TERMINAL_ISSUE_STATES)
            )
        ]
        params: list[Any] = list(TERMINAL_ISSUE_STATES)
        if project_key is not None:
            clauses.append("project_key = ?")
            params.append(project_key)
        if issue_ids is not None:
            ids = list(issue_ids)
            if not ids:
                return []
            clauses.append("issue_id IN ({})".format(",".join("?" for _ in ids)))
            params.extend(ids)
        query = (
            "SELECT issue_id, project_key, state FROM admitted_issues "
            "WHERE " + " AND ".join(clauses) + " "
            "ORDER BY project_key ASC, admitted_at ASC, rowid ASC"
        )
        return self._database.execute(query, tuple(params)).fetchall()

    def _release_leases(self, project_key: str, issue_id: str) -> None:
        """Release the issue's live worktree lease(s), fail-soft.

        Mirrors ``reviews.py``'s ``_settle_development_lane``: the
        existing checkpoint -> verify-remote -> reclaim custodian chain
        is the ONLY release path, and a lease whose proof cannot be
        established is left exactly as found. The Linear-driven
        completion above already happened and is never undone by a
        lease that stays active.
        """

        if self._custodian is None or self._leases is None:
            return
        for lease in self._leases.active(project_key):
            if lease.issue_id != issue_id:
                continue
            try:
                checkpoint = self._custodian.checkpoint(lease.lease_id, issue_id)
                self._custodian.reclaim(
                    lease.lease_id, self._custodian.verify_remote(checkpoint)
                )
            except Exception as error:  # unprovable: leave the lease put
                print(
                    "worktree lease "
                    f"{lease.lease_id} for {issue_id!r} was not released "
                    f"after Linear reconciliation; lease left active: "
                    f"{type(error).__name__}: {error}",
                    file=sys.stderr,
                )

    def _journal(
        self, event_type: str, *, issue_id: str, payload: dict[str, Any]
    ) -> None:
        with self._database.transaction() as connection:
            self._events.append(
                connection,
                EventInput(
                    event_type=event_type,
                    aggregate_type="issue",
                    aggregate_id=issue_id,
                    actor="linear_reconcile",
                    payload=payload,
                ),
            )


def build_linear_queue_reconciler(
    database: Database,
    *,
    linear_reads: LinearReadPort,
    custodian: WorktreeCustodian | None = None,
) -> LinearQueueReconciler:
    """Construct a reconciler the same way the CLI wires ``QueueService``.

    Mirrors ``cli.py``'s ``QueueService(database, events,
    registered_projects=())`` composition -- this pass never admits, so
    it needs no registered-project validation -- and always builds its
    own :class:`WorktreeLeases` so lease lookups work even when no
    custodian is supplied (in which case release is simply skipped).
    """

    events = EventStore(database)
    queue = QueueService(database, events, registered_projects=())
    leases = WorktreeLeases(database, events)
    return LinearQueueReconciler(
        database,
        queue=queue,
        linear_reads=linear_reads,
        events=events,
        custodian=custodian,
        leases=leases,
    )
