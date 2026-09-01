"""Durable post-merge acceptance gates.

INFRA-198 (acceptance-completion policy root fix, packet J1): a merge
records implementation completion, not operator acceptance. An
acceptance-gated issue owns exactly one durable gate row binding the
operator instruction that required it to the named predicates that must
be covered by evidence before the issue may complete. While the gate is
``pending`` the post-merge projection holds the issue short of Done;
``satisfy`` stores the covering evidence and restores the ordinary
completion path. Packet J2 drives ``require``/``satisfy`` from
hermes-command intents, and packet K repairs premature Done projections
at restart; both build on exactly this API.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

ACCEPTANCE_PENDING = "pending"
ACCEPTANCE_SATISFIED = "satisfied"


class AcceptanceGateConflict(ValueError):
    """The request would rewrite a gate outside its one-way contract."""


@dataclass(frozen=True, slots=True)
class AcceptanceGate:
    """One durable acceptance gate for an admitted issue.

    ``evidence`` is the stored predicate-to-evidence mapping as sorted
    key/value pairs; ``None`` while the gate is pending.
    """

    issue_id: str
    instruction_id: str
    predicates: tuple[str, ...]
    state: str
    evidence: tuple[tuple[str, str], ...] | None
    created_at: str
    updated_at: str

    def as_dict(self) -> dict[str, object]:
        return {
            "issue_id": self.issue_id,
            "instruction_id": self.instruction_id,
            "predicates": list(self.predicates),
            "state": self.state,
            "evidence": (
                None if self.evidence is None else dict(self.evidence)
            ),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class AcceptanceGates:
    """Create, satisfy, and read durable acceptance gates, journaled."""

    def __init__(
        self,
        database: Database,
        *,
        events: EventStore,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._now = now or (lambda: datetime.now(UTC))

    def require(
        self,
        issue_id: str,
        *,
        instruction_id: str,
        predicates: Sequence[str],
    ) -> AcceptanceGate:
        """Create or refresh one pending gate; refuse touching a satisfied one.

        Idempotent replay: re-requiring a pending gate with the same
        instruction id and predicates returns the stored gate and
        journals nothing. A pending gate refreshes to the new
        instruction and predicates (journaling ``acceptance.required``);
        a satisfied gate is immutable and refuses with
        :class:`AcceptanceGateConflict` — acceptance already happened,
        so a new requirement needs a new explicit operator decision, not
        a silent reopen.
        """

        if not issue_id.strip():
            raise ValueError("issue id is required")
        if not instruction_id.strip():
            raise ValueError("operator instruction id is required")
        cleaned = tuple(dict.fromkeys(predicates))
        if not cleaned or any(not predicate.strip() for predicate in cleaned):
            raise ValueError("at least one non-blank predicate is required")
        stamp = self._now().isoformat()
        predicates_json = json.dumps(list(cleaned))
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM acceptance_gates WHERE issue_id = ?",
                (issue_id,),
            ).fetchone()
            if row is not None and str(row["state"]) == ACCEPTANCE_SATISFIED:
                raise AcceptanceGateConflict(
                    f"acceptance gate for {issue_id} is already satisfied; "
                    "it cannot be re-required"
                )
            if (
                row is not None
                and str(row["instruction_id"]) == instruction_id
                and str(row["predicates_json"]) == predicates_json
            ):
                return _row_to_gate(row)
            if row is None:
                connection.execute(
                    "INSERT INTO acceptance_gates("
                    "issue_id, instruction_id, predicates_json, state, "
                    "evidence_json, created_at, updated_at"
                    ") VALUES (?, ?, ?, ?, NULL, ?, ?)",
                    (
                        issue_id,
                        instruction_id,
                        predicates_json,
                        ACCEPTANCE_PENDING,
                        stamp,
                        stamp,
                    ),
                )
            else:
                connection.execute(
                    "UPDATE acceptance_gates SET instruction_id = ?, "
                    "predicates_json = ?, updated_at = ? "
                    "WHERE issue_id = ? AND state = ?",
                    (
                        instruction_id,
                        predicates_json,
                        stamp,
                        issue_id,
                        ACCEPTANCE_PENDING,
                    ),
                )
            self._events.append(
                connection,
                EventInput(
                    event_type="acceptance.required",
                    aggregate_type="issue",
                    aggregate_id=issue_id,
                    correlation_id=instruction_id,
                    actor="operator",
                    payload={
                        "instruction_id": instruction_id,
                        "predicates": list(cleaned),
                        "refreshed": row is not None,
                    },
                ),
            )
        gate = self.get(issue_id)
        assert gate is not None
        return gate

    def satisfy(
        self, issue_id: str, *, evidence: Mapping[str, str]
    ) -> AcceptanceGate:
        """Satisfy one pending gate with evidence covering every predicate.

        The evidence keys must cover every configured predicate; a
        partial mapping refuses with :class:`AcceptanceGateConflict`
        and changes nothing. The pending-to-satisfied advance is a
        compare-and-set that journals exactly one
        ``acceptance.satisfied`` event.

        Replay contract: satisfying an already-satisfied gate with the
        byte-identical normalized evidence returns the stored gate and
        journals no second event; any different evidence refuses —
        satisfied evidence is an immutable operator record.
        """

        if not issue_id.strip():
            raise ValueError("issue id is required")
        normalized = tuple(sorted((str(k), str(v)) for k, v in evidence.items()))
        if any(not key.strip() or not value.strip() for key, value in normalized):
            raise ValueError("evidence keys and values must be non-blank")
        evidence_json = json.dumps([list(pair) for pair in normalized])
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM acceptance_gates WHERE issue_id = ?",
                (issue_id,),
            ).fetchone()
            if row is None:
                raise KeyError(issue_id)
            gate = _row_to_gate(row)
            missing = [
                predicate
                for predicate in gate.predicates
                if predicate not in dict(normalized)
            ]
            if missing:
                raise AcceptanceGateConflict(
                    "evidence does not cover every configured predicate; "
                    f"missing: {', '.join(missing)}"
                )
            if gate.state == ACCEPTANCE_SATISFIED:
                if gate.evidence == normalized:
                    return gate
                raise AcceptanceGateConflict(
                    f"acceptance gate for {issue_id} is already satisfied "
                    "with different evidence"
                )
            cursor = connection.execute(
                "UPDATE acceptance_gates SET state = ?, evidence_json = ?, "
                "updated_at = ? WHERE issue_id = ? AND state = ?",
                (
                    ACCEPTANCE_SATISFIED,
                    evidence_json,
                    stamp,
                    issue_id,
                    ACCEPTANCE_PENDING,
                ),
            )
            if cursor.rowcount != 1:
                raise AcceptanceGateConflict(
                    f"acceptance gate for {issue_id} changed state concurrently"
                )
            self._events.append(
                connection,
                EventInput(
                    event_type="acceptance.satisfied",
                    aggregate_type="issue",
                    aggregate_id=issue_id,
                    correlation_id=gate.instruction_id,
                    actor="operator",
                    payload={
                        "instruction_id": gate.instruction_id,
                        "predicates": list(gate.predicates),
                        "evidence": dict(normalized),
                    },
                ),
            )
        satisfied = self.get(issue_id)
        assert satisfied is not None
        return satisfied

    def get(self, issue_id: str) -> AcceptanceGate | None:
        """Read one gate, or ``None`` when the issue is not gated."""

        row = self._database.execute(
            "SELECT * FROM acceptance_gates WHERE issue_id = ?",
            (issue_id,),
        ).fetchone()
        return None if row is None else _row_to_gate(row)

    def all_gates(self) -> tuple[AcceptanceGate, ...]:
        """Read every durable gate, oldest first.

        INFRA-198 packet K: the acceptance reconciliation pass walks the
        whole table at every recovery boundary — the gate rows are the
        durable truth that survives any crash between gate satisfaction
        and the queue/Linear completion it should have driven.
        """

        rows = self._database.execute(
            "SELECT * FROM acceptance_gates ORDER BY created_at ASC, rowid ASC"
        ).fetchall()
        return tuple(_row_to_gate(row) for row in rows)

    def pending(self, issue_id: str) -> bool:
        """True while the issue's gate exists and awaits acceptance."""

        gate = self.get(issue_id)
        return gate is not None and gate.state == ACCEPTANCE_PENDING


def _row_to_gate(row: sqlite3.Row) -> AcceptanceGate:
    evidence_json = row["evidence_json"]
    return AcceptanceGate(
        issue_id=str(row["issue_id"]),
        instruction_id=str(row["instruction_id"]),
        predicates=tuple(
            str(item) for item in json.loads(str(row["predicates_json"]))
        ),
        state=str(row["state"]),
        evidence=(
            None
            if evidence_json is None
            else tuple(
                (str(pair[0]), str(pair[1]))
                for pair in json.loads(str(evidence_json))
            )
        ),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )
