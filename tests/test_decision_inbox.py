"""The global Hermes operator-decision inbox.

INFRA-224: a resolution must durably record the operator's answer, wake
the exact waiting lane exactly once, and let the caller resume from the
named next action; a duplicate raise or a duplicate resolve must never
create a second row or a second wake, and a routine reversible
implementation choice must never reach the inbox at all.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.decision_inbox import DecisionInbox, ResolutionResult
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.lead_wakes import LeadTerminalWakes
from hermes_orchestrator.operator_decisions import (
    DecisionOption,
    DecisionRefused,
    DecisionRequest,
    OperatorDecisions,
)

SESSION_ID = UUID("11111111-1111-4111-8111-111111111111")
OTHER_SESSION_ID = UUID("22222222-2222-4222-8222-222222222222")

NOW = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


def seed_cell(
    database: Database,
    *,
    cell_id: str = "cell-1",
    project_key: str = "demo",
    session_id: UUID = SESSION_ID,
    profile_alias: str = "max-a",
) -> None:
    stamp = NOW.isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells(cell_id, project_key, state, "
            "profile_alias, session_id, created_at, updated_at) VALUES "
            "(?, ?, 'active', ?, ?, ?, ?)",
            (cell_id, project_key, profile_alias, str(session_id), stamp, stamp),
        )


def build_inbox(
    database: Database, *, events: EventStore | None = None
) -> DecisionInbox:
    decisions = OperatorDecisions(database, now=lambda: NOW)
    wakes = LeadTerminalWakes(
        database=database,
        events=EventStore(database),
        now=lambda: NOW,
    )
    return DecisionInbox(
        database,
        decisions=decisions,
        wakes=wakes,
        events=events,
        now=lambda: NOW,
    )


def authority_request(**overrides: object) -> DecisionRequest:
    defaults: dict[str, object] = {
        "issue_id": "INFRA-224",
        "project_key": "demo",
        "cell_id": "cell-1",
        "session_id": str(SESSION_ID),
        "requesting_role": "lead",
        "question": "Should we roll back the schema change?",
        "authority_reason": "Irreversible data migration risk.",
        "facts": ("Migration touched 40k rows.", "No backup taken."),
        "options": (
            DecisionOption(label="roll back", tradeoffs="loses new writes"),
            DecisionOption(label="roll forward", tradeoffs="risk of corruption"),
        ),
        "recommendation": "roll back",
        "delay_impact": "Blocks INFRA-224 until answered.",
        "paused_scope": "issue:INFRA-224",
    }
    defaults.update(overrides)
    return DecisionRequest(**defaults)  # type: ignore[arg-type]


def wakes_for(database: Database, kind: str) -> list[object]:
    return database.execute(
        "SELECT * FROM lead_terminal_wakes WHERE kind = ? ORDER BY rowid",
        (kind,),
    ).fetchall()


class TestRaiseRequest:
    def test_two_projects_raise_independent_decisions(
        self, database: Database
    ) -> None:
        seed_cell(database, cell_id="cell-1", project_key="demo")
        seed_cell(
            database,
            cell_id="cell-2",
            project_key="alpha",
            session_id=OTHER_SESSION_ID,
        )
        inbox = build_inbox(database)

        demo, demo_created = inbox.raise_request(
            authority_request(project_key="demo", question="Demo question?")
        )
        alpha, alpha_created = inbox.raise_request(
            authority_request(
                project_key="alpha",
                cell_id="cell-2",
                session_id=str(OTHER_SESSION_ID),
                question="Alpha question?",
            )
        )

        assert demo_created is True
        assert alpha_created is True
        assert {d.decision_id for d in inbox.list()} == {
            demo.decision_id,
            alpha.decision_id,
        }
        assert [d.decision_id for d in inbox.list("demo")] == [demo.decision_id]
        assert [d.decision_id for d in inbox.list("alpha")] == [alpha.decision_id]

    def test_list_is_global_and_ordered_by_urgency_then_oldest(
        self, database: Database
    ) -> None:
        seed_cell(database)
        inbox = build_inbox(database)

        low, _ = inbox.raise_request(
            authority_request(urgency=3, question="Low urgency question?")
        )
        critical, _ = inbox.raise_request(
            authority_request(urgency=0, question="Critical urgency question?")
        )

        assert [d.decision_id for d in inbox.list()] == [
            critical.decision_id,
            low.decision_id,
        ]
        assert inbox.next() is not None
        assert inbox.next().decision_id == critical.decision_id  # type: ignore[union-attr]
        assert inbox.count() == 2

    def test_a_duplicate_raise_returns_the_same_decision_and_inserts_nothing(
        self, database: Database
    ) -> None:
        seed_cell(database)
        inbox = build_inbox(database)

        first, created_first = inbox.raise_request(authority_request())
        second, created_second = inbox.raise_request(authority_request())

        assert created_first is True
        assert created_second is False
        assert first == second
        count = database.scalar(
            "SELECT COUNT(*) FROM operator_decisions WHERE issue_id = ?",
            ("INFRA-224",),
        )
        assert count == 1

    def test_raise_journals_an_event_only_when_created(
        self, database: Database
    ) -> None:
        seed_cell(database)
        events = EventStore(database)
        inbox = build_inbox(database, events=events)

        decision, created = inbox.raise_request(authority_request())
        inbox.raise_request(authority_request())

        assert created is True
        rows = database.execute(
            "SELECT aggregate_id FROM events "
            "WHERE event_type = 'operator_decision.raised'"
        ).fetchall()
        assert [str(row["aggregate_id"]) for row in rows] == [decision.decision_id]

    def test_an_agent_owned_category_is_refused_and_inserts_nothing(
        self, database: Database
    ) -> None:
        seed_cell(database)
        inbox = build_inbox(database)

        with pytest.raises(DecisionRefused):
            inbox.raise_request(authority_request(category="small_fix"))

        assert database.scalar("SELECT COUNT(*) FROM operator_decisions") == 0

    def test_summary_shape(self, database: Database) -> None:
        seed_cell(database)
        inbox = build_inbox(database)
        decision, _ = inbox.raise_request(
            authority_request(
                urgency=0,
                question=(
                    "Should we roll back the schema change given the "
                    "irreversible migration already touched forty "
                    "thousand rows in production tonight?"
                ),
            )
        )

        line = inbox.summary(decision)

        assert line.startswith("[INFRA-224] (critical) ")
        assert len(line) <= len("[INFRA-224] (critical) ") + 80
        assert "…" in line or "rows" in line


class TestResolve:
    def test_resolve_records_answer_and_commits_exactly_one_wake(
        self, database: Database
    ) -> None:
        seed_cell(database, cell_id="cell-1", session_id=SESSION_ID)
        inbox = build_inbox(database)
        decision, _ = inbox.raise_request(authority_request())

        result = inbox.resolve(
            decision.decision_id,
            status="approved",
            answer="Roll back the migration.",
            source_message="Operator: roll back.",
            next_action="rollback_migration",
        )

        assert isinstance(result, ResolutionResult)
        assert result.decision.status == "approved"
        assert result.decision.answer == "Roll back the migration."
        assert result.decision.next_action == "rollback_migration"
        assert result.woke is True
        assert result.wake_id is not None
        assert result.next is None

        rows = wakes_for(database, "decision_resolved")
        assert len(rows) == 1
        [row] = rows
        assert row["wake_id"] == result.wake_id
        assert row["project_key"] == "demo"
        assert row["cell_id"] == "cell-1"
        assert row["session_id"] == str(SESSION_ID)
        assert row["turn_key"] == f"decision:{decision.decision_id}"
        assert decision.decision_id in row["reason"]
        assert "approved" in row["reason"]
        assert "Roll back the migration." in row["reason"]
        assert "rollback_migration" in row["reason"]

    def test_resolve_returns_the_next_pending_decision(
        self, database: Database
    ) -> None:
        seed_cell(database)
        inbox = build_inbox(database)
        first, _ = inbox.raise_request(
            authority_request(question="First question?", urgency=1)
        )
        second, _ = inbox.raise_request(
            authority_request(question="Second question?", urgency=2)
        )

        result = inbox.resolve(
            first.decision_id,
            status="approved",
            answer="Go ahead.",
            source_message="Operator: go ahead.",
        )

        assert result.next is not None
        assert result.next.decision_id == second.decision_id

    def test_resolving_again_refuses_and_creates_no_second_wake(
        self, database: Database
    ) -> None:
        seed_cell(database)
        inbox = build_inbox(database)
        decision, _ = inbox.raise_request(authority_request())
        inbox.resolve(
            decision.decision_id,
            status="approved",
            answer="Roll back.",
            source_message="Operator: roll back.",
        )

        with pytest.raises(DecisionRefused):
            inbox.resolve(
                decision.decision_id,
                status="rejected",
                answer="Changed my mind.",
                source_message="Operator: nope.",
            )

        rows = wakes_for(database, "decision_resolved")
        assert len(rows) == 1

    def test_reconcile_recommits_a_missing_wake_and_is_idempotent(
        self, database: Database
    ) -> None:
        seed_cell(database)
        inbox = build_inbox(database)
        decision, _ = inbox.raise_request(authority_request())
        result = inbox.resolve(
            decision.decision_id,
            status="approved",
            answer="Roll back.",
            source_message="Operator: roll back.",
        )

        with database.transaction() as connection:
            connection.execute(
                "DELETE FROM lead_terminal_wakes WHERE wake_id = ?",
                (result.wake_id,),
            )
        assert wakes_for(database, "decision_resolved") == []

        first_pass = inbox.reconcile()
        assert first_pass == 1
        rows = wakes_for(database, "decision_resolved")
        assert len(rows) == 1

        second_pass = inbox.reconcile()
        assert second_pass == 0
        assert len(wakes_for(database, "decision_resolved")) == 1

    def test_reconcile_scopes_by_project(self, database: Database) -> None:
        seed_cell(database, cell_id="cell-1", project_key="demo")
        seed_cell(
            database,
            cell_id="cell-2",
            project_key="alpha",
            session_id=OTHER_SESSION_ID,
        )
        inbox = build_inbox(database)
        demo_decision, _ = inbox.raise_request(
            authority_request(project_key="demo", question="Demo question?")
        )
        alpha_decision, _ = inbox.raise_request(
            authority_request(
                project_key="alpha",
                cell_id="cell-2",
                session_id=str(OTHER_SESSION_ID),
                question="Alpha question?",
            )
        )
        demo_result = inbox.resolve(
            demo_decision.decision_id,
            status="approved",
            answer="Demo answer.",
            source_message="Operator: demo.",
        )
        alpha_result = inbox.resolve(
            alpha_decision.decision_id,
            status="approved",
            answer="Alpha answer.",
            source_message="Operator: alpha.",
        )
        with database.transaction() as connection:
            connection.execute(
                "DELETE FROM lead_terminal_wakes WHERE wake_id IN (?, ?)",
                (demo_result.wake_id, alpha_result.wake_id),
            )

        committed = inbox.reconcile("demo")

        assert committed == 1
        rows = wakes_for(database, "decision_resolved")
        assert [str(row["project_key"]) for row in rows] == ["demo"]
