"""Durable QA origin persistence and post-merge Linear routing.

INFRA-166: ordinary merged work projects Done to the operator; QA-designated
work projects QA. Work that Ryan assigned from QA returns to Ryan in QA after
the merge, and a QA rejection returns it to the operator in In Development
without rewriting the durable origin, so the corrected work returns to Ryan
again. The origin is recorded only at admission or by explicit designation;
it is never inferred from later assignment changes.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.linear import LinearProjection

QA_ORIGIN_KINDS = ("ordinary", "ryan_assigned", "operator_designated")


@dataclass(frozen=True, slots=True)
class QaOrigin:
    """How an issue entered the queue with respect to QA."""

    kind: str

    def __post_init__(self) -> None:
        if self.kind not in QA_ORIGIN_KINDS:
            raise ValueError(f"unknown QA origin {self.kind!r}")

    @property
    def routes_to_qa(self) -> bool:
        return self.kind != "ordinary"


class QaRouter:
    """Persist QA origins and derive the exact post-merge projection."""

    def __init__(
        self,
        *,
        database: Database,
        events: EventStore,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._now = now or (lambda: datetime.now(UTC))

    def record_origin(self, issue_id: str, origin: QaOrigin) -> None:
        """Durably record an explicit origin; same-kind re-records are no-ops."""

        if not issue_id.strip():
            raise ValueError("issue id is required")
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT kind FROM qa_origins WHERE issue_id = ?", (issue_id,)
            ).fetchone()
            if row is not None and row["kind"] == origin.kind:
                return
            if row is None:
                connection.execute(
                    "INSERT INTO qa_origins(issue_id, kind, recorded_at, updated_at) "
                    "VALUES (?, ?, ?, ?)",
                    (issue_id, origin.kind, stamp, stamp),
                )
            else:
                connection.execute(
                    "UPDATE qa_origins SET kind = ?, updated_at = ? "
                    "WHERE issue_id = ?",
                    (origin.kind, stamp, issue_id),
                )
            self._events.append(
                connection,
                EventInput(
                    event_type="qa_origin.recorded",
                    aggregate_type="issue",
                    aggregate_id=issue_id,
                    actor="operator",
                    payload={
                        "kind": origin.kind,
                        "previous": None if row is None else row["kind"],
                    },
                ),
            )

    def origin_of(self, issue_id: str) -> QaOrigin:
        row = self._database.execute(
            "SELECT kind FROM qa_origins WHERE issue_id = ?", (issue_id,)
        ).fetchone()
        return QaOrigin("ordinary" if row is None else str(row["kind"]))

    def after_merge(self, issue_id: str) -> LinearProjection:
        """The only approved projection after a proven merge."""

        origin = self.origin_of(issue_id)
        if origin.kind == "ryan_assigned":
            return LinearProjection(status="QA", assignee_alias="ryan")
        if origin.kind == "operator_designated":
            return LinearProjection(status="QA", assignee_alias="operator")
        return LinearProjection(status="Done", assignee_alias="operator")

    def after_rejection(self, issue_id: str) -> LinearProjection:
        """A QA rejection always returns the work to the Claude lead."""

        self.origin_of(issue_id)
        return LinearProjection(status="In Development", assignee_alias="operator")
