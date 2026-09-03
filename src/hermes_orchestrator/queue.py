"""Explicit private queue admission and ranking."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterable
from datetime import UTC, datetime

from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest, IssueState, QueuedIssue
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.work_claims import WorkClaims


class AdmissionDenied(ValueError):
    """The request is outside the explicit operator-admission contract."""


class IdempotencyConflict(ValueError):
    """An instruction identifier was reused for different work."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


# INFRA-227: Linear workflow states that mean the issue is genuinely
# finished. Any other status (including one this orchestrator does not
# yet recognize) is treated as non-terminal, but ``None`` (the read
# failed or the issue could not be resolved) is never treated as
# non-terminal -- reactivation fails closed on an unknown status.
_LINEAR_TERMINAL_STATUSES = frozenset({"Done", "Canceled", "Cancelled", "Duplicate"})


class QueueService:
    """Own explicit admission, reprioritization, and stable queue ordering."""

    def __init__(
        self,
        database: Database,
        events: EventStore,
        registered_projects: Iterable[str],
        now: Callable[[], datetime] = _utc_now,
        claims: WorkClaims | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._registered_projects = frozenset(registered_projects)
        self._now = now
        # INFRA-222: a caller may hand in a shared instance; otherwise
        # one is constructed lazily here, so every existing
        # zero-``claims`` composition (production and tests alike)
        # keeps working with no call-site change required.
        self._claims = (
            claims if claims is not None else WorkClaims(database, events=events)
        )

    def admit(self, request: AdmissionRequest) -> QueuedIssue:
        """Admit one explicitly supplied issue, idempotently."""

        self._validate_admission(request)
        existing_instruction = self._database.execute(
            "SELECT * FROM admitted_issues WHERE instruction_id = ?",
            (request.instruction_id,),
        ).fetchone()
        if existing_instruction is not None:
            existing = self._row_to_issue(existing_instruction)
            if self._matches_request(existing, request):
                return existing
            raise IdempotencyConflict(
                f"instruction id {request.instruction_id} was already used"
            )

        existing_issue = self._database.execute(
            "SELECT * FROM admitted_issues WHERE issue_id = ?",
            (request.issue_id,),
        ).fetchone()
        if existing_issue is not None:
            raise AdmissionDenied(f"issue {request.issue_id} is already admitted")

        now = self._aware_now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO admitted_issues("
                "issue_id, project_key, priority, state, instruction_id, "
                "dependency_ready, overlap_risk, admitted_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    request.issue_id,
                    request.project_key,
                    request.linear_priority,
                    IssueState.QUEUED.value,
                    request.instruction_id,
                    int(request.dependency_ready),
                    request.overlap_risk,
                    now,
                    now,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="issue.admitted",
                    aggregate_type="issue",
                    aggregate_id=request.issue_id,
                    correlation_id=request.instruction_id,
                    actor="operator",
                    payload={
                        "project_key": request.project_key,
                        "priority": request.linear_priority,
                        "dependency_ready": request.dependency_ready,
                        "overlap_risk": request.overlap_risk,
                    },
                ),
            )
        return self.get(request.issue_id)

    def reactivate(
        self, request: AdmissionRequest, *, linear_status: str | None
    ) -> QueuedIssue:
        """Reactivate an already-admitted ``done`` row an operator reopened.

        INFRA-227: Linear can be reopened to a non-terminal workflow
        state while the local row still reads ``done`` from a prior
        explicit completion (``complete``). Plain ``admit`` refuses this
        outright ("already admitted"), and there is no supported
        transition to move a ``done`` row back to ``queued``. This reuses
        the existing admission machinery -- the same
        :class:`AdmissionRequest` an operator would use to admit the
        issue -- gated by the Linear status the caller already read, so
        no new intent, protocol, or workflow state is introduced.

        Every precondition fails closed with :class:`AdmissionDenied`
        before any write. Idempotent: replaying the exact same
        ``instruction_id`` for this issue after a successful reactivation
        returns the row unchanged and journals nothing new; reusing that
        instruction id for a *different* issue raises
        :class:`IdempotencyConflict`, exactly as ``admit`` does.
        """

        self._validate_admission(request)

        existing_instruction = self._database.execute(
            "SELECT * FROM admitted_issues WHERE instruction_id = ?",
            (request.instruction_id,),
        ).fetchone()
        if existing_instruction is not None:
            existing = self._row_to_issue(existing_instruction)
            if existing.issue_id != request.issue_id:
                raise IdempotencyConflict(
                    f"instruction id {request.instruction_id} was already used"
                )
            if existing.state is not IssueState.DONE:
                return existing

        row = self._database.execute(
            "SELECT * FROM admitted_issues WHERE issue_id = ?",
            (request.issue_id,),
        ).fetchone()
        if row is None:
            raise AdmissionDenied(
                f"issue {request.issue_id} is not admitted; nothing to reactivate"
            )
        current = self._row_to_issue(row)
        if current.project_key != request.project_key:
            raise AdmissionDenied(
                f"issue {request.issue_id} does not belong to project "
                f"{request.project_key}"
            )
        if current.state is not IssueState.DONE:
            raise AdmissionDenied(f"issue {request.issue_id} is already admitted")
        if linear_status is None or linear_status in _LINEAR_TERMINAL_STATUSES:
            raise AdmissionDenied(
                f"issue {request.issue_id} is not reactivatable while Linear "
                f"reports status {linear_status!r}"
            )

        now = self._aware_now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE admitted_issues SET state = ?, instruction_id = ?, "
                "priority = ?, dependency_ready = ?, overlap_risk = ?, "
                "updated_at = ? WHERE issue_id = ? AND state = ?",
                (
                    IssueState.QUEUED.value,
                    request.instruction_id,
                    request.linear_priority,
                    int(request.dependency_ready),
                    request.overlap_risk,
                    now,
                    request.issue_id,
                    IssueState.DONE.value,
                ),
            )
            if cursor.rowcount != 1:
                raise AdmissionDenied(
                    f"issue {request.issue_id} is already admitted"
                )
            self._events.append(
                connection,
                EventInput(
                    event_type="issue.reactivated",
                    aggregate_type="issue",
                    aggregate_id=request.issue_id,
                    correlation_id=request.instruction_id,
                    actor="operator",
                    payload={
                        "from": "done",
                        "to": "queued",
                        "linear_status": linear_status,
                        "project_key": request.project_key,
                        "priority": request.linear_priority,
                        "dependency_ready": request.dependency_ready,
                    },
                ),
            )
        return self.get(request.issue_id)

    def reprioritize(self, issue_id: str, priority: int) -> QueuedIssue:
        """Change queue priority and record the change atomically."""

        self._validate_priority(priority)
        current = self.get(issue_id)
        if current.linear_priority == priority:
            return current
        now = self._aware_now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE admitted_issues SET priority = ?, updated_at = ? "
                "WHERE issue_id = ?",
                (priority, now, issue_id),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="issue.reprioritized",
                    aggregate_type="issue",
                    aggregate_id=issue_id,
                    actor="operator",
                    payload={"from": current.linear_priority, "to": priority},
                ),
            )
        return self.get(issue_id)

    def complete(self, issue_id: str, *, reason: str, evidence: str) -> QueuedIssue:
        """Mark externally completed work done and journal the operator evidence."""

        if not reason.strip():
            raise ValueError("completion reason is required")
        if not evidence.strip():
            raise ValueError("completion evidence is required")
        now = self._aware_now().isoformat()
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM admitted_issues WHERE issue_id = ?",
                (issue_id,),
            ).fetchone()
            if row is None:
                raise KeyError(issue_id)
            current = self._row_to_issue(row)
            if current.state is not IssueState.DONE:
                if current.state is IssueState.QUEUED:
                    active_cell = connection.execute(
                        "SELECT 1 FROM project_cells WHERE project_key = ? "
                        "AND state IN ("
                        "'starting', 'active', 'handoff_required', 'paused'"
                        ") LIMIT 1",
                        (current.project_key,),
                    ).fetchone()
                    if active_cell is not None:
                        raise ValueError(
                            f"issue {issue_id} belongs to a project "
                            "with an active project cell"
                        )
                connection.execute(
                    "UPDATE admitted_issues SET state = ?, updated_at = ? "
                    "WHERE issue_id = ?",
                    (IssueState.DONE.value, now, issue_id),
                )
                self._events.append(
                    connection,
                    EventInput(
                        event_type="issue.completed",
                        aggregate_type="issue",
                        aggregate_id=issue_id,
                        actor="operator",
                        payload={
                            "from": current.state.value,
                            "reason": reason,
                            "evidence": evidence,
                        },
                    ),
                )
                # INFRA-222: explicit issue completion closes every
                # active work claim for this issue -- development,
                # harness, and review alike -- in the SAME transaction
                # as the DONE transition. This is an explicit ownership
                # event, never a side effect of any lead_assignments
                # packet lifecycle.
                self._claims.close_for_issue_in(
                    connection, issue_id=issue_id, reason="issue.completed"
                )
        return self.get(issue_id)

    def transition(
        self,
        issue_id: str,
        state: IssueState,
        *,
        actor: str,
        reason: str,
    ) -> QueuedIssue:
        """Record one review-flow lifecycle transition for an admitted issue."""

        result = self._transition(
            issue_id,
            state,
            actor=actor,
            reason=reason,
            from_states=None,
        )
        assert result is not None
        return result

    def transition_if(
        self,
        issue_id: str,
        state: IssueState,
        *,
        from_states: set[IssueState] | frozenset[IssueState],
        actor: str,
        reason: str,
    ) -> QueuedIssue | None:
        """Transition only from an allowed source state, atomically."""

        return self._transition(
            issue_id,
            state,
            actor=actor,
            reason=reason,
            from_states=frozenset(from_states),
        )

    def _transition(
        self,
        issue_id: str,
        state: IssueState,
        *,
        actor: str,
        reason: str,
        from_states: frozenset[IssueState] | None,
    ) -> QueuedIssue | None:

        if not reason.strip():
            raise ValueError("transition reason is required")
        now = self._aware_now().isoformat()
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM admitted_issues WHERE issue_id = ?",
                (issue_id,),
            ).fetchone()
            if row is None:
                raise KeyError(issue_id)
            current = self._row_to_issue(row)
            if from_states is not None and current.state not in from_states:
                return None
            if current.state is state:
                return current
            connection.execute(
                "UPDATE admitted_issues SET state = ?, updated_at = ? "
                "WHERE issue_id = ?",
                (state.value, now, issue_id),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="issue.transitioned",
                    aggregate_type="issue",
                    aggregate_id=issue_id,
                    actor=actor,
                    payload={
                        "from": current.state.value,
                        "to": state.value,
                        "reason": reason,
                    },
                ),
            )
            if state is IssueState.DONE:
                # INFRA-222: the review-flow completion path (Sol's
                # merge settlement transitions land here through
                # ``transition``/``transition_if``) is exactly as much
                # an explicit issue-completion event as the operator's
                # ``complete`` -- every active work claim closes in the
                # SAME transaction as this DONE transition, never as a
                # side effect of a delivery packet's own lifecycle.
                self._claims.close_for_issue_in(
                    connection, issue_id=issue_id, reason="issue.completed"
                )
        return self.get(issue_id)

    def mark_dependency_ready(
        self, project_key: str, *, actor: str, reason: str
    ) -> tuple[str, ...]:
        """Flip one project's blocked, not-yet-ready issues to ready.

        INFRA-198 P2: called after a merge settles (directly, or from
        the daemon's post-merge tick) so the existing ranked dispatch
        can admit the next explicitly admitted successor. Only rows
        with ``state = 'blocked'`` and ``dependency_ready = 0`` are
        touched; ``paused`` issues are an operator hold and are never
        auto-advanced. Idempotent: a rerun after every row is already
        flipped touches and journals nothing.
        """

        if not reason.strip():
            raise ValueError("reason is required")
        now = self._aware_now().isoformat()
        flipped: list[str] = []
        with self._database.transaction() as connection:
            rows = connection.execute(
                "SELECT issue_id FROM admitted_issues WHERE project_key = ? "
                "AND state = ? AND dependency_ready = 0 ORDER BY issue_id ASC",
                (project_key, IssueState.BLOCKED.value),
            ).fetchall()
            for row in rows:
                issue_id = str(row["issue_id"])
                connection.execute(
                    "UPDATE admitted_issues SET dependency_ready = 1, "
                    "updated_at = ? WHERE issue_id = ?",
                    (now, issue_id),
                )
                self._events.append(
                    connection,
                    EventInput(
                        event_type="issue.dependency_ready",
                        aggregate_type="issue",
                        aggregate_id=issue_id,
                        actor=actor,
                        payload={"reason": reason},
                    ),
                )
                flipped.append(issue_id)
        return tuple(flipped)

    def restore_readiness_after_requeue(
        self, issue_id: str, *, actor: str, reason: str
    ) -> QueuedIssue | None:
        """Repair a queued row left not-ready by an out-of-band clear.

        INFRA-198: ``dependency_ready`` can be cleared on a ``paused`` row
        outside any journaled event (an operational fixup, not a supported
        transition). When that row is later moved back to ``queued`` by an
        explicit operator requeue (see ``_retry_handler``), the clear
        survives and dispatch refuses it forever — ``mark_dependency_ready``
        deliberately never touches non-``blocked`` rows, so nothing else
        can repair it. An explicit operator requeue is the readiness
        authority once a pause hold is cleared, so this method is scoped
        tightly: it only ever touches a row that is ALREADY ``queued`` with
        ``dependency_ready = 0``, and it always journals what it did.
        Returns ``None`` when there is nothing to repair.

        That state shape alone is NOT sufficient, because admission itself
        permits it: an issue admitted with ``dependency_ready = False`` sits
        ``queued`` and not-ready as a legitimate dependency gate, and
        repairing it would dispatch work whose dependency never settled. So
        the repair also requires durable provenance of a prior requeue —
        a journaled ``issue.transitioned`` event for this exact issue whose
        payload ``to`` is ``queued`` (see ``_has_requeue_provenance``).
        Admission journals ``issue.admitted``, never ``issue.transitioned``,
        so a never-requeued row has no such provenance and is left
        completely unchanged.
        """

        if not reason.strip():
            raise ValueError("reason is required")
        now = self._aware_now().isoformat()
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM admitted_issues WHERE issue_id = ?",
                (issue_id,),
            ).fetchone()
            if row is None:
                raise KeyError(issue_id)
            current = self._row_to_issue(row)
            if current.state is not IssueState.QUEUED or current.dependency_ready:
                return None
            if not self._has_requeue_provenance(connection, issue_id):
                return None
            connection.execute(
                "UPDATE admitted_issues SET dependency_ready = 1, "
                "updated_at = ? WHERE issue_id = ?",
                (now, issue_id),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="issue.dependency_ready",
                    aggregate_type="issue",
                    aggregate_id=issue_id,
                    actor=actor,
                    payload={"reason": reason},
                ),
            )
        return self.get(issue_id)

    def _has_requeue_provenance(
        self, connection: sqlite3.Connection, issue_id: str
    ) -> bool:
        """Whether a prior transition INTO ``queued`` was journaled here.

        ``_transition`` appends an ``issue.transitioned`` event carrying
        ``from``/``to``/``reason`` for every state change it makes, and
        ``queued`` is only ever a transition TARGET for an operator requeue
        (the retry command). Admission inserts the initial ``queued`` row
        directly and journals ``issue.admitted`` instead, so this predicate
        separates "residue of a prior requeue" from "legitimately
        dependency-gated brand-new admission" using events that already
        exist. Read on the caller's connection so the check and the repair
        it guards share one transaction.
        """

        found = connection.execute(
            "SELECT 1 FROM events WHERE aggregate_type = 'issue' "
            "AND aggregate_id = ? AND event_type = 'issue.transitioned' "
            "AND json_extract(payload_json, '$.to') = ? LIMIT 1",
            (issue_id, IssueState.QUEUED.value),
        ).fetchone()
        return found is not None

    def get(self, issue_id: str) -> QueuedIssue:
        """Read one admitted issue."""

        row = self._database.execute(
            "SELECT * FROM admitted_issues WHERE issue_id = ?",
            (issue_id,),
        ).fetchone()
        if row is None:
            raise KeyError(issue_id)
        return self._row_to_issue(row)

    def list_ranked(self, now: datetime) -> list[QueuedIssue]:
        """Return active queue items in deterministic scheduling order."""

        if now.tzinfo is None:
            raise ValueError("ranking time must be timezone-aware")
        rows = self._database.execute(
            "SELECT * FROM admitted_issues "
            "WHERE state IN (?, ?) "
            "ORDER BY priority ASC, dependency_ready DESC, admitted_at ASC, "
            "overlap_risk ASC, issue_id ASC",
            (IssueState.QUEUED.value, IssueState.BLOCKED.value),
        ).fetchall()
        return [self._row_to_issue(row) for row in rows]

    def _validate_admission(self, request: AdmissionRequest) -> None:
        if request.admitted_by != "operator":
            raise AdmissionDenied("admission requires an explicit operator instruction")
        if not request.instruction_id.strip():
            raise AdmissionDenied("operator instruction id is required")
        if not request.issue_id.strip():
            raise AdmissionDenied("issue id is required")
        if request.project_key not in self._registered_projects:
            raise AdmissionDenied(f"project {request.project_key} is not registered")
        self._validate_priority(request.linear_priority)
        if request.overlap_risk < 0:
            raise AdmissionDenied("overlap risk cannot be negative")

    @staticmethod
    def _validate_priority(priority: int) -> None:
        if priority not in range(1, 5):
            raise AdmissionDenied("priority must be from 1 through 4")

    def _aware_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return value.astimezone(UTC)

    @staticmethod
    def _matches_request(issue: QueuedIssue, request: AdmissionRequest) -> bool:
        return (
            issue.issue_id == request.issue_id
            and issue.project_key == request.project_key
            and issue.linear_priority == request.linear_priority
            and issue.dependency_ready is request.dependency_ready
            and issue.overlap_risk == request.overlap_risk
        )

    @staticmethod
    def _row_to_issue(row: sqlite3.Row) -> QueuedIssue:
        return QueuedIssue(
            issue_id=str(row["issue_id"]),
            project_key=str(row["project_key"]),
            linear_priority=int(row["priority"]),
            state=IssueState(row["state"]),
            instruction_id=str(row["instruction_id"]),
            dependency_ready=bool(row["dependency_ready"]),
            overlap_risk=int(row["overlap_risk"]),
            admitted_at=datetime.fromisoformat(row["admitted_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )
