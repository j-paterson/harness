from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.hermes_tools import HermesCommandService
from hermes_orchestrator.queue import QueueService


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def command_service(database: Database) -> HermesCommandService:
    queue = QueueService(database, EventStore(database), {"demo"})
    return HermesCommandService(queue)


def test_hermes_cannot_discover_work(command_service: HermesCommandService) -> None:
    result = command_service.execute({"intent": "scan_linear"})

    assert result.code == "intent_not_allowed"


def test_queue_intent_requires_issue_and_instruction(
    command_service: HermesCommandService,
) -> None:
    result = command_service.execute(
        {"intent": "queue_issue", "issue_id": "ENG-9", "project_key": "demo"}
    )

    assert result.code == "operator_instruction_required"


def test_queue_intent_admits_only_supplied_issue(
    command_service: HermesCommandService,
) -> None:
    result = command_service.execute(
        {
            "intent": "queue_issue",
            "issue_id": "ENG-9",
            "project_key": "demo",
            "priority": 1,
            "operator_instruction_id": "chat-9",
        }
    )

    assert result.code == "queued"
    assert result.state == {
        "issue_id": "ENG-9",
        "project_key": "demo",
        "priority": 1,
        "state": "queued",
    }


def test_unknown_command_fields_fail_closed(
    command_service: HermesCommandService,
) -> None:
    result = command_service.execute(
        {"intent": "status", "include_account_emails": True}
    )

    assert result.code == "invalid_command"


def test_queue_intent_records_explicit_qa_origin(database: Database) -> None:
    from hermes_orchestrator.qa import QaRouter

    queue = QueueService(database, EventStore(database), {"demo"})
    router = QaRouter(database=database, events=EventStore(database))
    service = HermesCommandService(queue, qa=router)

    result = service.execute(
        {
            "intent": "queue_issue",
            "issue_id": "ENG-10",
            "project_key": "demo",
            "priority": 1,
            "operator_instruction_id": "chat-10",
            "qa_origin": "ryan_assigned",
        }
    )

    assert result.code == "queued"
    assert result.state["qa_origin"] == "ryan_assigned"
    assert router.origin_of("ENG-10").kind == "ryan_assigned"


def test_qa_origin_without_router_fails_closed(
    command_service: HermesCommandService,
) -> None:
    result = command_service.execute(
        {
            "intent": "queue_issue",
            "issue_id": "ENG-10",
            "project_key": "demo",
            "priority": 1,
            "operator_instruction_id": "chat-10",
            "qa_origin": "operator_designated",
        }
    )
    assert result.code == "qa_routing_unavailable"


def test_qa_origin_is_never_inferred(command_service: HermesCommandService) -> None:
    result = command_service.execute(
        {
            "intent": "queue_issue",
            "issue_id": "ENG-10",
            "project_key": "demo",
            "priority": 1,
            "operator_instruction_id": "chat-10",
            "qa_origin": "assignee_is_ryan",
        }
    )
    assert result.code == "invalid_command"


def test_new_review_intents_route_to_handlers(database: Database) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    calls: list[tuple[str, object]] = []

    def pending(command: object) -> dict[str, object]:
        calls.append(("pending", command))
        return {"corrections": []}

    def rejecting(command: object) -> dict[str, object]:
        raise ValueError("issue ENG-9 has no merged review to reject")

    service = HermesCommandService(
        queue, handlers={"pending_corrections": pending, "qa_reject": rejecting}
    )
    listed = service.execute({"intent": "pending_corrections"})
    assert listed.code == "accepted"
    assert listed.state == {"corrections": []}
    rejected = service.execute(
        {"intent": "qa_reject", "issue_id": "ENG-9", "reason": "r", "evidence": "e"}
    )
    assert rejected.code == "rejected"
    assert "no merged review" in str(rejected.state["reason"])
    missing = service.execute({"intent": "ack_correction", "correction_id": "c"})
    assert missing.code == "intent_unavailable"
    invalid = service.execute({"intent": "qa_reject", "issue_id": "ENG-9"})
    assert invalid.code == "invalid_command"


def test_stall_intents_are_strict_and_routed(database: Database) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    seen: list[object] = []

    def report(command: object) -> dict[str, object]:
        seen.append(command)
        return {"stalled": True, "mode": "ask_operator"}

    service = HermesCommandService(queue, handlers={"report_stall": report})
    result = service.execute(
        {
            "intent": "report_stall",
            "project_key": "demo",
            "repeated_failure": "uv run pytest -q",
        }
    )
    assert result.code == "accepted" and result.state["mode"] == "ask_operator"
    assert seen[0].repeated_failure == "uv run pytest -q"
    invalid = service.execute(
        {
            "intent": "approve_playbook",
            "consultation_id": "c",
            "actions": [],
            "verification": "v",
            "timeout_seconds": 10,
            "rollback": "r",
        }
    )
    assert invalid.code == "invalid_command"
    unavailable = service.execute({"intent": "pending_consultations"})
    assert unavailable.code == "intent_unavailable"
