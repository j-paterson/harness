"""Explicit private queue admission and ranking."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterable
from datetime import UTC, datetime

from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest, IssueState, QueuedIssue
from hermes_orchestrator.events import EventInput, EventStore


class AdmissionDenied(ValueError):
    """The request is outside the explicit operator-admission contract."""


class IdempotencyConflict(ValueError):
    """An instruction identifier was reused for different work."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


class QueueService:
    """Own explicit admission, reprioritization, and stable queue ordering."""

    def __init__(
        self,
        database: Database,
        events: EventStore,
        registered_projects: Iterable[str],
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._database = database
        self._events = events
        self._registered_projects = frozenset(registered_projects)
        self._now = now

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
