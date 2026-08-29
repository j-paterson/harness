"""Durable operator decisions as enforceable orchestration state.

INFRA-190: an architectural choice awaiting the operator is not a
transcript nuance — it is durable state. While a decision is pending
for an issue, implementation dispatch and candidate publication are
blocked. Resolution requires an explicit, nonempty, schema-valid
application command naming the exact pending decision id; a blank
message, hook firing, transcript replay, system event, or model
assertion mutates nothing (the observed false-approval inference is a
regression test). A recorded receipt imports idempotently with its
exact SHA-256 bound.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from hermes_orchestrator.db import Database

_STATUSES = ("approved", "rejected")

_REQUIRED_RECEIPT_FIELDS = (
    "decision_id",
    "issue_id",
    "project_key",
    "cell_id",
    "session_id",
    "actor",
    "choice",
    "status",
    "recorded_at",
    "source_message",
)


class DecisionRefused(RuntimeError):
    """The command may not resolve any decision; nothing changed."""


@dataclass(frozen=True, slots=True)
class OperatorDecision:
    decision_id: str
    issue_id: str
    project_key: str
    cell_id: str
    session_id: str
    actor: str
    choice: str
    status: str
    receipt_sha256: str | None
    source_message: str | None
    recorded_at: str
    applied_at: str | None


class OperatorDecisions:
    """Append-only decision records with one enforced transition."""

    def __init__(
        self,
        database: Database,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._now = now or (lambda: datetime.now(UTC))

    def record_pending(
        self,
        *,
        decision_id: str,
        issue_id: str,
        project_key: str,
        cell_id: str,
        session_id: str,
        choice: str,
    ) -> OperatorDecision:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO operator_decisions("
                "decision_id, issue_id, project_key, cell_id, "
                "session_id, actor, choice, status, recorded_at"
                ") VALUES (?, ?, ?, ?, ?, 'operator', ?, 'pending', ?)",
                (
                    decision_id,
                    issue_id,
                    project_key,
                    cell_id,
                    session_id,
                    choice,
                    stamp,
                ),
            )
        return self.get(decision_id)

    def apply(
        self,
        *,
        decision_id: str,
        status: str,
        source_message: str,
        actor: str = "operator",
    ) -> OperatorDecision:
        """Resolve one exact pending decision, or refuse untouched.

        The command must be affirmatively formed: a real decision id,
        an explicit approved/rejected status, a nonempty operator
        message, and the operator actor. Anything less — notably the
        blank inbound event observed live — refuses with zero
        mutation.
        """

        if not str(decision_id).strip():
            raise DecisionRefused(
                "a decision command requires the exact pending decision id"
            )
        if status not in _STATUSES:
            raise DecisionRefused(
                "a decision command must state approved or rejected explicitly"
            )
        if not str(source_message).strip():
            raise DecisionRefused("a blank message can never resolve a decision")
        if actor != "operator":
            raise DecisionRefused("only the operator may resolve an operator decision")
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE operator_decisions SET status = ?, "
                "source_message = ?, applied_at = ? "
                "WHERE decision_id = ? AND status = 'pending'",
                (status, source_message, stamp, decision_id),
            )
            if cursor.rowcount != 1:
                raise DecisionRefused(
                    "no pending decision carries this id; nothing changed"
                )
        return self.get(decision_id)

    def import_receipt(self, receipt: dict, *, receipt_sha256: str) -> OperatorDecision:
        """Import one recorded operator receipt, idempotently.

        The receipt must carry every identity field, a nonempty
        operator source message, and an explicit approved/rejected
        status. A receipt already imported with the same SHA-256 is a
        no-op returning the existing row; a conflicting existing row
        refuses.
        """

        missing = [
            field
            for field in _REQUIRED_RECEIPT_FIELDS
            if not str(receipt.get(field, "")).strip()
        ]
        if missing:
            raise DecisionRefused(
                "the receipt is missing required fields: " + ", ".join(missing)
            )
        if receipt["actor"] != "operator":
            raise DecisionRefused("only an operator receipt may be imported")
        if receipt["status"] not in _STATUSES:
            raise DecisionRefused("the receipt must record approved or rejected")
        decision_id = str(receipt["decision_id"])
        existing = self._find(decision_id)
        if existing is not None:
            if existing.receipt_sha256 == receipt_sha256:
                return existing
            raise DecisionRefused("a different record already carries this decision id")
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO operator_decisions("
                "decision_id, issue_id, project_key, cell_id, "
                "session_id, actor, choice, status, receipt_sha256, "
                "source_message, recorded_at, applied_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    decision_id,
                    str(receipt["issue_id"]),
                    str(receipt["project_key"]),
                    str(receipt["cell_id"]),
                    str(receipt["session_id"]),
                    "operator",
                    str(receipt["choice"]),
                    str(receipt["status"]),
                    receipt_sha256,
                    str(receipt["source_message"]),
                    str(receipt["recorded_at"]),
                    stamp,
                ),
            )
        return self.get(decision_id)

    def pending_for_issue(self, issue_id: str) -> tuple[OperatorDecision, ...]:
        rows = self._database.execute(
            "SELECT * FROM operator_decisions "
            "WHERE issue_id = ? AND status = 'pending' "
            "ORDER BY recorded_at ASC, rowid ASC",
            (issue_id,),
        ).fetchall()
        return tuple(_row_to_decision(row) for row in rows)

    def get(self, decision_id: str) -> OperatorDecision:
        decision = self._find(decision_id)
        if decision is None:
            raise KeyError(decision_id)
        return decision

    def _find(self, decision_id: str) -> OperatorDecision | None:
        row = self._database.execute(
            "SELECT * FROM operator_decisions WHERE decision_id = ?",
            (decision_id,),
        ).fetchone()
        return None if row is None else _row_to_decision(row)


def _row_to_decision(row: object) -> OperatorDecision:
    def text(key: str) -> str:
        return str(row[key])  # type: ignore[index]

    def optional(key: str) -> str | None:
        value = row[key]  # type: ignore[index]
        return None if value is None else str(value)

    return OperatorDecision(
        decision_id=text("decision_id"),
        issue_id=text("issue_id"),
        project_key=text("project_key"),
        cell_id=text("cell_id"),
        session_id=text("session_id"),
        actor=text("actor"),
        choice=text("choice"),
        status=text("status"),
        receipt_sha256=optional("receipt_sha256"),
        source_message=optional("source_message"),
        recorded_at=text("recorded_at"),
        applied_at=optional("applied_at"),
    )
