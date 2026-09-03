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

import hashlib
import json
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
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

#: Urgency scale for the global operator-decision inbox. Lower sorts
#: first: a critical question is worked before a normal one even if it
#: was raised later.
URGENCY_CRITICAL = 0
URGENCY_HIGH = 1
URGENCY_NORMAL = 2
URGENCY_LOW = 3

#: INFRA-224: routine, reversible implementation choices an agent must
#: make on its own -- these can never enter the operator inbox. Their
#: presence on a DecisionRequest.category always refuses raise_request
#: with zero mutation, regardless of how the request is otherwise
#: shaped.
AGENT_OWNED_CATEGORIES = frozenset(
    {
        "lead_vs_child_routing",
        "retry_or_resume",
        "focused_test_selection",
        "small_fix",
        "branch_alignment",
    }
)


class DecisionRefused(RuntimeError):
    """The command may not resolve any decision; nothing changed."""


@dataclass(frozen=True, slots=True)
class DecisionOption:
    """One choice offered to the operator, with its tradeoffs."""

    label: str
    tradeoffs: str


@dataclass(frozen=True, slots=True)
class DecisionRequest:
    """An agent's request for an operator's authority-worthy decision.

    Carries the full context an operator needs to decide without
    reopening a session: the question, why it exceeds agent authority,
    the facts and options on the table, a recommendation, and what is
    paused while the operator decides.
    """

    issue_id: str
    project_key: str
    cell_id: str
    session_id: str
    requesting_role: str
    question: str
    authority_reason: str
    facts: tuple[str, ...]
    options: tuple[DecisionOption, ...]
    recommendation: str
    delay_impact: str
    paused_scope: str
    urgency: int = URGENCY_NORMAL
    category: str = "authority"
    request_key: str | None = None

    def derive_request_key(self) -> str:
        """A deterministic dedup key for this request.

        Two raises for the same project, issue, and category that ask
        the same question (modulo whitespace and case) collide on
        this key, so raise_request treats the second as the same
        outstanding request rather than a new one.
        """

        normalized_question = " ".join(self.question.split()).lower()
        digest_input = "\x1f".join(
            (self.project_key, self.issue_id, self.category, normalized_question)
        )
        return hashlib.sha256(digest_input.encode("utf-8")).hexdigest()

    def effective_request_key(self) -> str:
        """The request_key to dedup on: explicit if given, else derived."""

        return self.request_key or self.derive_request_key()


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
    question: str | None = None
    authority_reason: str | None = None
    requesting_role: str | None = None
    facts: tuple[str, ...] = field(default_factory=tuple)
    options: tuple[DecisionOption, ...] = field(default_factory=tuple)
    recommendation: str | None = None
    delay_impact: str | None = None
    paused_scope: str | None = None
    urgency: int = URGENCY_NORMAL
    request_key: str | None = None
    category: str | None = None
    answer: str | None = None
    next_action: str | None = None


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

        return self._resolve_pending(
            decision_id=decision_id,
            status=status,
            source_message=source_message,
            actor=actor,
        )

    def resolve(
        self,
        decision_id: str,
        *,
        status: str,
        answer: str,
        source_message: str,
        next_action: str | None = None,
        actor: str = "operator",
    ) -> OperatorDecision:
        """Resolve one exact pending inbox request, recording the answer.

        Reuses apply()'s compare-and-swap discipline unchanged --
        pending only, exactly once, operator actor only, fail-closed
        on any blank -- and additionally stores the operator's answer
        and next action in the same UPDATE.
        """

        if not str(answer).strip():
            raise DecisionRefused("a blank answer can never resolve a decision")
        return self._resolve_pending(
            decision_id=decision_id,
            status=status,
            source_message=source_message,
            actor=actor,
            answer=answer,
            next_action=next_action,
        )

    def _resolve_pending(
        self,
        *,
        decision_id: str,
        status: str,
        source_message: str,
        actor: str,
        answer: str | None = None,
        next_action: str | None = None,
    ) -> OperatorDecision:
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
                "source_message = ?, applied_at = ?, answer = ?, next_action = ? "
                "WHERE decision_id = ? AND status = 'pending'",
                (status, source_message, stamp, answer, next_action, decision_id),
            )
            if cursor.rowcount != 1:
                raise DecisionRefused(
                    "no pending decision carries this id; nothing changed"
                )
        return self.get(decision_id)

    def raise_request(self, request: DecisionRequest) -> tuple[OperatorDecision, bool]:
        """Raise an operator-authority request into the global inbox.

        Refuses with zero mutation when the category is agent-owned
        (a routine reversible implementation choice must never reach
        the operator) or when any required field is blank. Dedupes by
        request_key: a pending row already carrying the same key is
        returned unchanged with ``created=False`` instead of a second
        row being inserted -- including under a concurrent duplicate
        insert racing on the partial unique index.
        """

        if request.category in AGENT_OWNED_CATEGORIES:
            raise DecisionRefused(
                f"{request.category} is an agent-owned routine choice; "
                "it may not enter the operator inbox"
            )
        required = {
            "question": request.question,
            "authority_reason": request.authority_reason,
            "requesting_role": request.requesting_role,
            "recommendation": request.recommendation,
            "delay_impact": request.delay_impact,
            "paused_scope": request.paused_scope,
        }
        missing = [name for name, value in required.items() if not str(value).strip()]
        if missing:
            raise DecisionRefused(
                "an operator decision request requires: " + ", ".join(missing)
            )
        if not request.options:
            raise DecisionRefused(
                "an operator decision request requires at least one option"
            )

        request_key = request.effective_request_key()
        existing = self._pending_by_request_key(request_key)
        if existing is not None:
            return existing, False

        decision_id = str(uuid.uuid4())
        stamp = self._now().isoformat()
        facts_json = json.dumps(list(request.facts))
        options_json = json.dumps(
            [
                {"label": option.label, "tradeoffs": option.tradeoffs}
                for option in request.options
            ]
        )
        try:
            with self._database.transaction() as connection:
                connection.execute(
                    "INSERT INTO operator_decisions("
                    "decision_id, issue_id, project_key, cell_id, session_id, "
                    "actor, choice, status, recorded_at, question, "
                    "authority_reason, requesting_role, facts_json, "
                    "options_json, recommendation, delay_impact, paused_scope, "
                    "urgency, request_key, category"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, "
                    "?, ?, ?, ?, ?, ?, ?)",
                    (
                        decision_id,
                        request.issue_id,
                        request.project_key,
                        request.cell_id,
                        request.session_id,
                        request.requesting_role,
                        request.recommendation,
                        stamp,
                        request.question,
                        request.authority_reason,
                        request.requesting_role,
                        facts_json,
                        options_json,
                        request.recommendation,
                        request.delay_impact,
                        request.paused_scope,
                        request.urgency,
                        request_key,
                        request.category,
                    ),
                )
        except sqlite3.IntegrityError:
            existing = self._pending_by_request_key(request_key)
            if existing is not None:
                return existing, False
            raise
        return self.get(decision_id), True

    def inbox(self, project_key: str | None = None) -> list[OperatorDecision]:
        """Pending requests across all projects, or one project.

        Ordered urgency ascending, then oldest first -- the order an
        operator should work them in.
        """

        if project_key is None:
            rows = self._database.execute(
                "SELECT * FROM operator_decisions WHERE status = 'pending' "
                "ORDER BY urgency ASC, recorded_at ASC, rowid ASC"
            ).fetchall()
        else:
            rows = self._database.execute(
                "SELECT * FROM operator_decisions "
                "WHERE status = 'pending' AND project_key = ? "
                "ORDER BY urgency ASC, recorded_at ASC, rowid ASC",
                (project_key,),
            ).fetchall()
        return [_row_to_decision(row) for row in rows]

    def next_pending(self, project_key: str | None = None) -> OperatorDecision | None:
        """The single highest-priority pending request, if any."""

        inbox = self.inbox(project_key)
        return inbox[0] if inbox else None

    def pending_count(self, project_key: str | None = None) -> int:
        """How many requests are presently pending."""

        if project_key is None:
            value = self._database.scalar(
                "SELECT COUNT(*) FROM operator_decisions WHERE status = 'pending'"
            )
        else:
            value = self._database.scalar(
                "SELECT COUNT(*) FROM operator_decisions "
                "WHERE status = 'pending' AND project_key = ?",
                (project_key,),
            )
        return int(value)  # type: ignore[arg-type]

    def _pending_by_request_key(self, request_key: str) -> OperatorDecision | None:
        row = self._database.execute(
            "SELECT * FROM operator_decisions "
            "WHERE request_key = ? AND status = 'pending'",
            (request_key,),
        ).fetchone()
        return None if row is None else _row_to_decision(row)

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

    def json_list(key: str) -> list[object]:
        raw = row[key]  # type: ignore[index]
        return [] if not raw else json.loads(raw)

    facts = tuple(str(item) for item in json_list("facts_json"))
    options = tuple(
        DecisionOption(label=str(item["label"]), tradeoffs=str(item["tradeoffs"]))
        for item in json_list("options_json")
    )
    urgency_value = row["urgency"]  # type: ignore[index]
    urgency = URGENCY_NORMAL if urgency_value is None else int(urgency_value)

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
        question=optional("question"),
        authority_reason=optional("authority_reason"),
        requesting_role=optional("requesting_role"),
        facts=facts,
        options=options,
        recommendation=optional("recommendation"),
        delay_impact=optional("delay_impact"),
        paused_scope=optional("paused_scope"),
        urgency=urgency,
        request_key=optional("request_key"),
        category=optional("category"),
        answer=optional("answer"),
        next_action=optional("next_action"),
    )
