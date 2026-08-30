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


def test_remote_capability_intents_are_strict_and_routed(database: Database) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    seen: list[object] = []

    def record(command: object) -> dict[str, object]:
        seen.append(command)
        return {"handled": True}

    service = HermesCommandService(
        queue,
        handlers={
            "approve_stall": record,
            "request_checkpoint": record,
            "request_cleanup": record,
        },
    )
    for intent in ("approve_stall", "request_checkpoint", "request_cleanup"):
        result = service.execute({"intent": intent, "project_key": "demo"})
        assert result.code == "accepted", intent
        assert result.state == {"handled": True}
        invalid = service.execute(
            {"intent": intent, "project_key": "demo", "extra": "field"}
        )
        assert invalid.code == "invalid_command", intent
        missing = service.execute({"intent": intent})
        assert missing.code == "invalid_command", intent
    assert [command.project_key for command in seen] == ["demo", "demo", "demo"]


def test_remote_capability_intents_without_handlers_are_unavailable(
    command_service: HermesCommandService,
) -> None:
    for intent in ("approve_stall", "request_checkpoint", "request_cleanup"):
        result = command_service.execute({"intent": intent, "project_key": "demo"})
        assert result.code == "intent_unavailable", intent


def test_pending_wakes_and_ack_wake_are_strict_and_routed(database: Database) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    calls: list[tuple[str, object]] = []

    def pending(command: object) -> dict[str, object]:
        calls.append(("pending", command))
        return {"wakes": []}

    def ack(command: object) -> dict[str, object]:
        calls.append(("ack", command))
        return {"acknowledged": True}

    service = HermesCommandService(
        queue, handlers={"pending_wakes": pending, "ack_wake": ack}
    )
    listed = service.execute({"intent": "pending_wakes"})
    assert listed.code == "accepted"
    assert listed.state == {"wakes": []}
    assert calls[0][1].project_key is None

    acked = service.execute({"intent": "ack_wake", "wake_id": "w-1"})
    assert acked.code == "accepted"
    assert acked.state == {"acknowledged": True}
    assert calls[1][1].wake_id == "w-1"

    invalid = service.execute({"intent": "ack_wake"})
    assert invalid.code == "invalid_command"


def test_pending_wakes_and_ack_wake_without_handlers_are_unavailable(
    command_service: HermesCommandService,
) -> None:
    missing_pending = command_service.execute({"intent": "pending_wakes"})
    assert missing_pending.code == "intent_unavailable"
    missing_ack = command_service.execute({"intent": "ack_wake", "wake_id": "w-1"})
    assert missing_ack.code == "intent_unavailable"


def test_supports_reflects_pending_wakes_and_ack_wake(database: Database) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    bare = HermesCommandService(queue)
    assert not bare.supports("pending_wakes")
    assert not bare.supports("ack_wake")

    wired = HermesCommandService(
        queue, handlers={"pending_wakes": lambda c: {}, "ack_wake": lambda c: {}}
    )
    assert wired.supports("pending_wakes")
    assert wired.supports("ack_wake")


def test_supports_reflects_inline_and_wired_intents(database: Database) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    bare = HermesCommandService(queue)
    assert bare.supports("queue_issue")
    assert bare.supports("status")
    assert bare.supports("reprioritize")
    assert not bare.supports("approve_stall")
    assert not bare.supports("shell")
    assert not bare.supports("scan_linear")

    wired = HermesCommandService(queue, handlers={"approve_stall": lambda c: {}})
    assert wired.supports("approve_stall")
    assert not wired.supports("request_cleanup")


def test_packet_intents_are_strict_and_routed(database: Database) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    seen: list[object] = []

    def created(command: object) -> dict[str, object]:
        seen.append(command)
        return {"packet_id": "p-1", "state": "planned"}

    def accepted(command: object) -> dict[str, object]:
        seen.append(command)
        return {"packet_id": command.packet_id, "state": "accepted"}

    def rejected(command: object) -> dict[str, object]:
        seen.append(command)
        return {"packet_id": command.packet_id, "state": "rejected"}

    def direct_exception(command: object) -> dict[str, object]:
        seen.append(command)
        if len(command.expected_files) > 2 or command.expected_lines > 30:
            raise ValueError("direct-work exception exceeds the reviewer-fix scale")
        return {"packet_id": "p-2", "state": "accepted"}

    service = HermesCommandService(
        queue,
        handlers={
            "create_packet": created,
            "accept_packet": accepted,
            "reject_packet": rejected,
            "record_direct_exception": direct_exception,
        },
    )

    create_result = service.execute(
        {
            "intent": "create_packet",
            "issue_id": "ENG-9",
            "model_tier": "sonnet",
            "effort": "high",
            "allowed_files": ["src/app.py"],
            "worktree": "/repo",
            "red_test": "pytest -k red",
            "verification": ["pytest -q"],
            "invariants": "no other files change",
            "resource_note": "one sonnet slot",
        }
    )
    assert create_result.code == "accepted"
    assert create_result.state == {"packet_id": "p-1", "state": "planned"}
    assert seen[0].allowed_files == ["src/app.py"]
    assert seen[0].depends_on == []

    invalid_create = service.execute(
        {
            "intent": "create_packet",
            "issue_id": "ENG-9",
            "model_tier": "sonnet",
            "effort": "high",
            "allowed_files": ["src/app.py"],
            "worktree": "/repo",
            "red_test": "pytest -k red",
            "verification": ["pytest -q"],
            "invariants": "no other files change",
            "resource_note": "one sonnet slot",
            "extra_field": "nope",
        }
    )
    assert invalid_create.code == "invalid_command"

    accept_result = service.execute(
        {"intent": "accept_packet", "packet_id": "p-1", "evidence": {"diff": "clean"}}
    )
    assert accept_result.code == "accepted"
    assert accept_result.state == {"packet_id": "p-1", "state": "accepted"}

    reject_result = service.execute(
        {"intent": "reject_packet", "packet_id": "p-1", "reason": "scope creep"}
    )
    assert reject_result.code == "accepted"
    assert reject_result.state == {"packet_id": "p-1", "state": "rejected"}

    within_scale = service.execute(
        {
            "intent": "record_direct_exception",
            "issue_id": "ENG-9",
            "reason": "one-line typo fix",
            "expected_files": ["src/app.py"],
            "expected_lines": 5,
            "verification": "pytest -q",
        }
    )
    assert within_scale.code == "accepted"
    assert within_scale.state == {"packet_id": "p-2", "state": "accepted"}

    over_scale = service.execute(
        {
            "intent": "record_direct_exception",
            "issue_id": "ENG-9",
            "reason": "too much for a direct exception",
            "expected_files": ["a.py", "b.py", "c.py"],
            "expected_lines": 5,
            "verification": "pytest -q",
        }
    )
    assert over_scale.code == "rejected"
    assert "reviewer-fix scale" in str(over_scale.state["reason"])

    over_lines = service.execute(
        {
            "intent": "record_direct_exception",
            "issue_id": "ENG-9",
            "reason": "too many lines for a direct exception",
            "expected_files": ["a.py"],
            "expected_lines": 31,
            "verification": "pytest -q",
        }
    )
    assert over_lines.code == "rejected"
    assert "reviewer-fix scale" in str(over_lines.state["reason"])

    invalid_lines = service.execute(
        {
            "intent": "record_direct_exception",
            "issue_id": "ENG-9",
            "reason": "zero lines is not a real exception",
            "expected_files": ["a.py"],
            "expected_lines": 0,
            "verification": "pytest -q",
        }
    )
    assert invalid_lines.code == "invalid_command"

    invalid_files = service.execute(
        {
            "intent": "record_direct_exception",
            "issue_id": "ENG-9",
            "reason": "no files at all",
            "expected_files": [],
            "expected_lines": 5,
            "verification": "pytest -q",
        }
    )
    assert invalid_files.code == "invalid_command"


def test_packet_intents_without_handlers_are_unavailable(
    command_service: HermesCommandService,
) -> None:
    for intent, payload in (
        (
            "create_packet",
            {
                "issue_id": "ENG-9",
                "model_tier": "sonnet",
                "effort": "high",
                "allowed_files": ["a.py"],
                "worktree": "/repo",
                "red_test": "r",
                "verification": ["v"],
                "invariants": "i",
                "resource_note": "n",
            },
        ),
        ("accept_packet", {"packet_id": "p-1", "evidence": {}}),
        ("reject_packet", {"packet_id": "p-1", "reason": "r"}),
        (
            "record_direct_exception",
            {
                "issue_id": "ENG-9",
                "reason": "r",
                "expected_files": ["a.py"],
                "expected_lines": 5,
                "verification": "v",
            },
        ),
    ):
        result = command_service.execute({"intent": intent, **payload})
        assert result.code == "intent_unavailable", intent
