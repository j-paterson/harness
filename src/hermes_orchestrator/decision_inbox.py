"""The global Hermes operator-decision inbox.

INFRA-224: a resolution must durably record the operator's answer, wake
the exact waiting lane exactly once, and let the caller resume from the
named next action -- all without inventing a second mutation surface
alongside the two primitives that already own this state:

* :class:`hermes_orchestrator.operator_decisions.OperatorDecisions` owns
  the decision row itself -- authority-owned refusal, request_key
  dedup, and the single-shot compare-and-swap resolution.
* :class:`hermes_orchestrator.lead_wakes.LeadTerminalWakes` owns the
  exactly-once terminal wake outbox for one project/cell/session/turn
  identity.

:class:`DecisionInbox` composes them: a resolved decision's
``(project_key, cell_id, session_id)`` is the waiting lane, and
``turn_key=f"decision:{decision_id}"`` with ``kind="decision_resolved"``
is the turn identity a resolution wakes -- so a decision can never wake
its lane twice, and a duplicate ``resolve()`` call is refused upstream
by :meth:`OperatorDecisions.resolve` before any wake is attempted.

Decision wakes are rebuilt on :meth:`DecisionInbox.reconcile` directly
from already-resolved ``operator_decisions`` rows, never from a
journaled evidence event the way
:class:`hermes_orchestrator.lead_wakes.LeadWakeReconciler` rebuilds a
terminal-turn wake: a resolved row already IS the durable proof a wake
is owed.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.lead_wakes import (
    LeadTerminalWakes,
    TerminalWake,
    TerminalWakeInput,
)
from hermes_orchestrator.operator_decisions import (
    DecisionRequest,
    OperatorDecision,
    OperatorDecisions,
)

#: Terminal statuses a resolved decision may carry -- the only ones
#: `reconcile()` treats as "a wake is owed" for.
_RESOLVED_STATUSES = ("approved", "rejected")

_URGENCY_WORDS = {0: "critical", 1: "high", 2: "normal", 3: "low"}

_SUMMARY_QUESTION_LIMIT = 80


@dataclass(frozen=True, slots=True)
class ResolutionResult:
    """The outcome of resolving one operator decision."""

    decision: OperatorDecision
    wake_id: str | None
    woke: bool
    next: OperatorDecision | None


class DecisionInbox:
    """Raise, list, and resolve operator decisions across every project."""

    def __init__(
        self,
        database: Database,
        *,
        decisions: OperatorDecisions,
        wakes: LeadTerminalWakes,
        events: EventStore | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._decisions = decisions
        self._wakes = wakes
        self._events = events
        self._now = now or (lambda: datetime.now(UTC))

    def raise_request(
        self, request: DecisionRequest
    ) -> tuple[OperatorDecision, bool]:
        """Raise one authority-worthy request into the global inbox.

        Agent-owned-category refusal and request_key dedup are both
        enforced by :meth:`OperatorDecisions.raise_request`; this only
        journals the raise when it actually created a new row.
        """

        decision, created = self._decisions.raise_request(request)
        if created and self._events is not None:
            with self._database.transaction() as connection:
                self._events.append(
                    connection,
                    EventInput(
                        event_type="operator_decision.raised",
                        aggregate_type="operator_decision",
                        aggregate_id=decision.decision_id,
                        correlation_id=decision.request_key,
                        payload={
                            "project_key": decision.project_key,
                            "issue_id": decision.issue_id,
                            "cell_id": decision.cell_id,
                            "session_id": decision.session_id,
                            "category": decision.category,
                            "urgency": decision.urgency,
                        },
                    ),
                )
        return decision, created

    def list(self, project_key: str | None = None) -> list[OperatorDecision]:
        """Pending requests across every project, or one project.

        Ordered urgency ascending then oldest first -- the order an
        operator should work them in.
        """

        return self._decisions.inbox(project_key)

    def next(self, project_key: str | None = None) -> OperatorDecision | None:
        """The single highest-priority pending request, if any."""

        return self._decisions.next_pending(project_key)

    def count(self, project_key: str | None = None) -> int:
        """How many requests are presently pending."""

        return self._decisions.pending_count(project_key)

    def summary(self, decision: OperatorDecision) -> str:
        """One concise dashboard/chat line: issue, urgency, question."""

        urgency_word = _URGENCY_WORDS.get(decision.urgency, "normal")
        question = " ".join((decision.question or "").split())
        if len(question) > _SUMMARY_QUESTION_LIMIT:
            question = question[: _SUMMARY_QUESTION_LIMIT - 1].rstrip() + "…"
        return f"[{decision.issue_id}] ({urgency_word}) {question}"

    def resolve(
        self,
        decision_id: str,
        *,
        status: str,
        answer: str,
        source_message: str,
        next_action: str | None = None,
    ) -> ResolutionResult:
        """Resolve one decision, wake its exact lane once, and advance.

        Delegates the actual compare-and-swap to
        :meth:`OperatorDecisions.resolve`: a second call for the same
        decision id refuses there, before any wake is attempted, so no
        second wake is ever committed for one decision.
        """

        resolved = self._decisions.resolve(
            decision_id,
            status=status,
            answer=answer,
            source_message=source_message,
            next_action=next_action,
        )
        wake, woke = self._commit_resolution_wake(resolved)
        return ResolutionResult(
            decision=resolved,
            wake_id=wake.wake_id,
            woke=woke,
            next=self.next(),
        )

    def reconcile(self, project_key: str | None = None) -> int:
        """Re-commit any missing ``decision_resolved`` wake; idempotent.

        A resolved decision whose wake row already exists costs
        nothing here and commits nothing again -- both the pre-check
        and the underlying unique index make this safe to call as
        often as a repair sweep likes. Returns how many wakes this
        call actually committed.
        """

        placeholders = ",".join("?" for _ in _RESOLVED_STATUSES)
        if project_key is None:
            rows = self._database.execute(
                f"SELECT decision_id FROM operator_decisions "
                f"WHERE status IN ({placeholders}) "
                "ORDER BY recorded_at ASC, rowid ASC",
                _RESOLVED_STATUSES,
            ).fetchall()
        else:
            rows = self._database.execute(
                f"SELECT decision_id FROM operator_decisions "
                f"WHERE status IN ({placeholders}) AND project_key = ? "
                "ORDER BY recorded_at ASC, rowid ASC",
                (*_RESOLVED_STATUSES, project_key),
            ).fetchall()
        committed = 0
        for row in rows:
            decision = self._decisions.get(str(row["decision_id"]))
            _, woke = self._commit_resolution_wake(decision)
            if woke:
                committed += 1
        return committed

    def _commit_resolution_wake(
        self, decision: OperatorDecision
    ) -> tuple[TerminalWake, bool]:
        """Commit (or return) the one wake owed for a resolved decision."""

        already = self._existing_wake(decision)
        next_action = decision.next_action or "resume"
        reason = (
            f"decision {decision.decision_id} resolved {decision.status}: "
            f"{decision.answer} (next: {next_action})"
        )
        wake = self._wakes.commit(
            TerminalWakeInput(
                project_key=decision.project_key,
                issue_id=decision.issue_id,
                cell_id=decision.cell_id,
                session_id=UUID(str(decision.session_id)),
                profile_alias=self._profile_alias(decision.cell_id),
                turn_key=f"decision:{decision.decision_id}",
                kind="decision_resolved",
                reason=reason,
            )
        )
        return wake, already is None

    def _existing_wake(self, decision: OperatorDecision) -> TerminalWake | None:
        row = self._database.execute(
            "SELECT wake_id FROM lead_terminal_wakes WHERE project_key = ? "
            "AND cell_id = ? AND session_id = ? AND turn_key = ? AND kind = ?",
            (
                decision.project_key,
                decision.cell_id,
                str(decision.session_id),
                f"decision:{decision.decision_id}",
                "decision_resolved",
            ),
        ).fetchone()
        return None if row is None else self._wakes.get(str(row["wake_id"]))

    def _profile_alias(self, cell_id: str) -> str:
        row = self._database.execute(
            "SELECT profile_alias FROM project_cells WHERE cell_id = ?",
            (cell_id,),
        ).fetchone()
        if row is None or row["profile_alias"] is None:
            return ""
        return str(row["profile_alias"])
