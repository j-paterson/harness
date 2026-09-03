"""The operator-decision gate: durable, explicit, and fail-closed.

INFRA-190 regression territory: a blank inbound event once read as
approval and triggered implementation. These tests prove a decision is
durable state that only a nonempty schema-valid operator command can
resolve, and that pending decisions block candidate publication.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.hermes_tools import HermesCommandService
from hermes_orchestrator.operator_decisions import (
    URGENCY_CRITICAL,
    URGENCY_LOW,
    URGENCY_NORMAL,
    DecisionOption,
    DecisionRefused,
    DecisionRequest,
    OperatorDecisions,
)
from hermes_orchestrator.queue import QueueService
from tests.test_cli import base_arguments, invoke


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def decisions(database: Database) -> OperatorDecisions:
    return OperatorDecisions(database)


def record_channel_pilot(decisions: OperatorDecisions) -> None:
    decisions.record_pending(
        decision_id="dec-1",
        issue_id="INFRA-190",
        project_key="demo",
        cell_id="cell-1",
        session_id="session-1",
        choice="claude_code_channel_pilot",
    )


def receipt_payload() -> dict[str, str]:
    return {
        "decision_id": "receipt-dec",
        "issue_id": "INFRA-190",
        "project_key": "demo",
        "cell_id": "cell-1",
        "session_id": "session-1",
        "actor": "operator",
        "choice": "claude_code_channel_pilot",
        "status": "approved",
        "recorded_at": "2026-08-29T00:00:00+00:00",
        "source_message": "Let's try that dedicated channel.",
    }


class TestDecisionLifecycle:
    def test_a_recorded_decision_is_pending_for_its_issue(
        self, decisions: OperatorDecisions
    ) -> None:
        record_channel_pilot(decisions)

        [pending] = decisions.pending_for_issue("INFRA-190")

        assert pending.decision_id == "dec-1"
        assert pending.status == "pending"
        assert pending.actor == "operator"
        assert pending.applied_at is None

    def test_an_explicit_command_resolves_exactly_one_decision(
        self, decisions: OperatorDecisions
    ) -> None:
        record_channel_pilot(decisions)

        applied = decisions.apply(
            decision_id="dec-1",
            status="approved",
            source_message="Let's try that dedicated channel.",
        )

        assert applied.status == "approved"
        assert applied.applied_at is not None
        assert decisions.pending_for_issue("INFRA-190") == ()

    def test_a_resolved_decision_cannot_be_resolved_again(
        self, decisions: OperatorDecisions
    ) -> None:
        record_channel_pilot(decisions)
        decisions.apply(
            decision_id="dec-1",
            status="rejected",
            source_message="Not this way.",
        )

        with pytest.raises(DecisionRefused):
            decisions.apply(
                decision_id="dec-1",
                status="approved",
                source_message="Actually yes.",
            )

        assert decisions.get("dec-1").status == "rejected"


class TestBlankEventsAuthorizeNothing:
    """The observed live failure: a blank event must mutate nothing."""

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"decision_id": "", "status": "approved", "source_message": "x"},
            {"decision_id": "   ", "status": "approved", "source_message": "x"},
            {"decision_id": "dec-1", "status": "approved", "source_message": ""},
            {"decision_id": "dec-1", "status": "approved", "source_message": " \n"},
            {"decision_id": "dec-1", "status": "", "source_message": "x"},
            {"decision_id": "dec-1", "status": "pending", "source_message": "x"},
            {
                "decision_id": "dec-1",
                "status": "approved",
                "source_message": "x",
                "actor": "model",
            },
            {
                "decision_id": "dec-1",
                "status": "approved",
                "source_message": "x",
                "actor": "hook",
            },
        ],
    )
    def test_malformed_commands_refuse_with_zero_mutation(
        self, decisions: OperatorDecisions, kwargs: dict[str, str]
    ) -> None:
        record_channel_pilot(decisions)

        with pytest.raises(DecisionRefused):
            decisions.apply(**kwargs)

        assert decisions.get("dec-1").status == "pending"
        assert len(decisions.pending_for_issue("INFRA-190")) == 1

    def test_a_command_for_an_unknown_decision_changes_nothing(
        self, decisions: OperatorDecisions
    ) -> None:
        record_channel_pilot(decisions)

        with pytest.raises(DecisionRefused):
            decisions.apply(
                decision_id="dec-999",
                status="approved",
                source_message="approve whatever is pending",
            )

        assert decisions.get("dec-1").status == "pending"

    def test_blank_hermes_events_never_reach_decision_state(
        self, database: Database, decisions: OperatorDecisions
    ) -> None:
        """Replay of the observed failure at the command boundary.

        A blank message, an empty hook event, and a payload without an
        explicit decision id all fail schema validation before any
        handler runs; the pending decision survives untouched.
        """

        record_channel_pilot(decisions)
        queue = QueueService(database, EventStore(database), {"demo"})

        def handler(command: object) -> dict[str, object]:
            applied = decisions.apply(
                decision_id=command.decision_id,  # type: ignore[attr-defined]
                status=command.status,  # type: ignore[attr-defined]
                source_message=command.source_message,  # type: ignore[attr-defined]
            )
            return {"status": applied.status}

        service = HermesCommandService(
            queue, handlers={"apply_operator_decision": handler}
        )

        blank_events: list[object] = [
            "",
            None,
            {},
            {"intent": "apply_operator_decision"},
            {
                "intent": "apply_operator_decision",
                "decision_id": "dec-1",
                "status": "approved",
                "source_message": "",
            },
            {
                "intent": "apply_operator_decision",
                "decision_id": "",
                "status": "approved",
                "source_message": "yes",
            },
            {
                "intent": "apply_operator_decision",
                "decision_id": "dec-1",
                "status": "yes please",
                "source_message": "yes",
            },
        ]
        for event in blank_events:
            result = service.execute(event)
            assert result.code in ("invalid_command", "intent_not_allowed")

        assert decisions.get("dec-1").status == "pending"

        accepted = service.execute(
            {
                "intent": "apply_operator_decision",
                "decision_id": "dec-1",
                "status": "approved",
                "source_message": "Let's try that dedicated channel.",
            }
        )

        assert accepted.code == "accepted"
        assert decisions.get("dec-1").status == "approved"


class TestReceiptImport:
    def test_import_records_the_receipt_with_its_exact_hash(
        self, decisions: OperatorDecisions
    ) -> None:
        imported = decisions.import_receipt(receipt_payload(), receipt_sha256="a" * 64)

        assert imported.status == "approved"
        assert imported.receipt_sha256 == "a" * 64
        assert decisions.pending_for_issue("INFRA-190") == ()

    def test_repeated_import_is_idempotent(
        self, database: Database, decisions: OperatorDecisions
    ) -> None:
        first = decisions.import_receipt(receipt_payload(), receipt_sha256="a" * 64)
        second = decisions.import_receipt(receipt_payload(), receipt_sha256="a" * 64)

        assert first == second
        count = database.scalar("SELECT COUNT(*) FROM operator_decisions")
        assert count == 1

    def test_a_conflicting_record_under_the_same_id_refuses(
        self, decisions: OperatorDecisions
    ) -> None:
        decisions.import_receipt(receipt_payload(), receipt_sha256="a" * 64)

        with pytest.raises(DecisionRefused):
            decisions.import_receipt(receipt_payload(), receipt_sha256="b" * 64)

    @pytest.mark.parametrize(
        "mutation",
        [
            {"source_message": ""},
            {"actor": "model"},
            {"status": "pending"},
            {"decision_id": ""},
            {"session_id": ""},
        ],
    )
    def test_incomplete_receipts_refuse_without_mutation(
        self,
        database: Database,
        decisions: OperatorDecisions,
        mutation: dict[str, str],
    ) -> None:
        receipt = {**receipt_payload(), **mutation}

        with pytest.raises(DecisionRefused):
            decisions.import_receipt(receipt, receipt_sha256="a" * 64)

        assert database.scalar("SELECT COUNT(*) FROM operator_decisions") == 0


class TestCandidatePublicationGate:
    def test_candidate_ready_refuses_while_a_decision_is_pending(
        self, configured_repo: tuple[Path, Path]
    ) -> None:
        _, state_dir = configured_repo
        assert invoke([*base_arguments(configured_repo), "init"]).exit_code == 0
        store = Database.open(state_dir / "state.db")
        try:
            record_channel_pilot(OperatorDecisions(store))
        finally:
            store.close()

        result = invoke(
            [
                *base_arguments(configured_repo),
                "candidate-ready",
                "INFRA-190",
                "--project",
                "demo",
                "--verified",
                "pytest=green",
            ]
        )

        assert result.exit_code == 1
        assert "awaiting operator decision dec-1" in result.stderr
        assert "blocked" in result.stderr

    def test_decision_import_command_is_hash_bound_and_idempotent(
        self, configured_repo: tuple[Path, Path], tmp_path: Path
    ) -> None:
        assert invoke([*base_arguments(configured_repo), "init"]).exit_code == 0
        receipt_path = tmp_path / "receipt.json"
        receipt_path.write_text(json.dumps(receipt_payload()), encoding="utf-8")
        digest = hashlib.sha256(receipt_path.read_bytes()).hexdigest()

        wrong = invoke(
            [
                *base_arguments(configured_repo),
                "decision-import",
                "--receipt",
                str(receipt_path),
                "--sha256",
                "0" * 64,
            ]
        )
        assert wrong.exit_code == 1
        assert "does not match" in wrong.output

        for _ in range(2):
            result = invoke(
                [
                    *base_arguments(configured_repo),
                    "decision-import",
                    "--receipt",
                    str(receipt_path),
                    "--sha256",
                    digest,
                    "--json",
                ]
            )
            assert result.exit_code == 0
            payload = json.loads(result.stdout)
            assert payload["decision_id"] == "receipt-dec"
            assert payload["status"] == "approved"
            assert payload["receipt_sha256"] == digest


def authority_request(**overrides: object) -> DecisionRequest:
    defaults: dict[str, object] = {
        "issue_id": "INFRA-224",
        "project_key": "demo",
        "cell_id": "cell-1",
        "session_id": "session-1",
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


class TestDecisionInbox:
    """INFRA-224: a global operator inbox across every project.

    An authority-worthy question is durable, deduplicated, fully
    contextual state; a routine reversible implementation choice must
    never reach it at all.
    """

    def test_raise_request_creates_a_pending_row_with_full_context(
        self, decisions: OperatorDecisions
    ) -> None:
        decision, created = decisions.raise_request(authority_request())

        assert created is True
        assert decision.status == "pending"
        assert decision.issue_id == "INFRA-224"
        assert decision.project_key == "demo"
        assert decision.requesting_role == "lead"
        assert decision.question == "Should we roll back the schema change?"
        assert decision.authority_reason == "Irreversible data migration risk."
        assert decision.facts == (
            "Migration touched 40k rows.",
            "No backup taken.",
        )
        assert decision.options == (
            DecisionOption(label="roll back", tradeoffs="loses new writes"),
            DecisionOption(label="roll forward", tradeoffs="risk of corruption"),
        )
        assert decision.recommendation == "roll back"
        assert decision.delay_impact == "Blocks INFRA-224 until answered."
        assert decision.paused_scope == "issue:INFRA-224"
        assert decision.urgency == URGENCY_NORMAL
        assert decision.category == "authority"
        assert decision.request_key is not None
        assert decision.answer is None
        assert decision.next_action is None

    def test_a_duplicate_raise_returns_the_same_decision_and_inserts_nothing(
        self, database: Database, decisions: OperatorDecisions
    ) -> None:
        first, created_first = decisions.raise_request(authority_request())
        second, created_second = decisions.raise_request(authority_request())

        assert created_first is True
        assert created_second is False
        assert first == second
        count = database.scalar(
            "SELECT COUNT(*) FROM operator_decisions WHERE issue_id = ?",
            ("INFRA-224",),
        )
        assert count == 1

    def test_two_projects_raise_independent_decisions_ordered_by_urgency(
        self, decisions: OperatorDecisions
    ) -> None:
        low, _ = decisions.raise_request(
            authority_request(
                project_key="alpha",
                urgency=URGENCY_LOW,
                question="Low urgency question?",
            )
        )
        critical, _ = decisions.raise_request(
            authority_request(
                project_key="beta",
                urgency=URGENCY_CRITICAL,
                question="Critical urgency question?",
            )
        )

        assert [d.decision_id for d in decisions.inbox()] == [
            critical.decision_id,
            low.decision_id,
        ]
        assert [d.decision_id for d in decisions.inbox("alpha")] == [low.decision_id]
        assert [d.decision_id for d in decisions.inbox("beta")] == [
            critical.decision_id
        ]

    def test_an_agent_owned_category_is_refused_and_inserts_nothing(
        self, database: Database, decisions: OperatorDecisions
    ) -> None:
        with pytest.raises(DecisionRefused):
            decisions.raise_request(authority_request(category="small_fix"))

        assert database.scalar("SELECT COUNT(*) FROM operator_decisions") == 0

    def test_resolve_records_answer_and_next_action_and_is_single_shot(
        self, decisions: OperatorDecisions
    ) -> None:
        decision, _ = decisions.raise_request(authority_request())

        resolved = decisions.resolve(
            decision.decision_id,
            status="approved",
            answer="Roll back the migration.",
            source_message="Operator: roll back.",
            next_action="rollback_migration",
        )

        assert resolved.status == "approved"
        assert resolved.answer == "Roll back the migration."
        assert resolved.next_action == "rollback_migration"
        assert resolved.applied_at is not None
        assert decisions.pending_count() == 0

        with pytest.raises(DecisionRefused):
            decisions.resolve(
                decision.decision_id,
                status="rejected",
                answer="Changed my mind.",
                source_message="Operator: nope.",
            )

        assert decisions.get(decision.decision_id).status == "approved"

    def test_resolve_refuses_a_blank_answer_without_mutation(
        self, decisions: OperatorDecisions
    ) -> None:
        decision, _ = decisions.raise_request(authority_request())

        with pytest.raises(DecisionRefused):
            decisions.resolve(
                decision.decision_id,
                status="approved",
                answer="   ",
                source_message="Operator: roll back.",
            )

        assert decisions.get(decision.decision_id).status == "pending"

    def test_pending_count_and_next_pending(
        self, decisions: OperatorDecisions
    ) -> None:
        assert decisions.pending_count() == 0
        assert decisions.next_pending() is None

        low, _ = decisions.raise_request(
            authority_request(urgency=URGENCY_LOW, question="Question one?")
        )
        critical, _ = decisions.raise_request(
            authority_request(
                project_key="alpha",
                urgency=URGENCY_CRITICAL,
                question="Question two?",
            )
        )

        assert decisions.pending_count() == 2
        assert decisions.pending_count("demo") == 1
        assert decisions.pending_count("alpha") == 1
        next_overall = decisions.next_pending()
        assert next_overall is not None
        assert next_overall.decision_id == critical.decision_id
        next_demo = decisions.next_pending("demo")
        assert next_demo is not None
        assert next_demo.decision_id == low.decision_id

    def test_legacy_rows_without_new_columns_still_load(
        self, decisions: OperatorDecisions
    ) -> None:
        record_channel_pilot(decisions)

        legacy = decisions.get("dec-1")

        assert legacy.question is None
        assert legacy.authority_reason is None
        assert legacy.requesting_role is None
        assert legacy.facts == ()
        assert legacy.options == ()
        assert legacy.recommendation is None
        assert legacy.delay_impact is None
        assert legacy.paused_scope is None
        assert legacy.urgency == URGENCY_NORMAL
        assert legacy.request_key is None
        assert legacy.category is None
        assert legacy.answer is None
        assert legacy.next_action is None
