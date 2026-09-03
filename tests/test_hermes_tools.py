from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest
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
    missing = service.execute(
        {
            "intent": "ack_correction",
            "correction_id": "c",
            "observed_count": 4,
            "payload_sha256": "0" * 64,
        }
    )
    assert missing.code == "intent_unavailable"
    invalid = service.execute({"intent": "qa_reject", "issue_id": "ENG-9"})
    assert invalid.code == "invalid_command"


def test_correction_intake_intents_are_strict(database: Database) -> None:
    """INFRA-193: confirmation must carry proof of a complete read.

    ``fetch_correction`` is a routed intent, and ``ack_correction``
    cannot even parse without both ``observed_count`` and a nonblank
    ``payload_sha256`` — a truncated glance has no shape that validates.
    """

    queue = QueueService(database, EventStore(database), {"demo"})
    seen: list[object] = []

    def fetch(command: object) -> dict[str, object]:
        seen.append(command)
        return {"declared_count": 4, "payload_sha256": "a" * 64}

    def ack(command: object) -> dict[str, object]:
        seen.append(command)
        return {"state": "acknowledged"}

    service = HermesCommandService(
        queue, handlers={"fetch_correction": fetch, "ack_correction": ack}
    )
    assert service.supports("fetch_correction")

    fetched = service.execute(
        {"intent": "fetch_correction", "correction_id": "corr-1"}
    )
    assert fetched.code == "accepted"
    assert fetched.state["declared_count"] == 4
    assert seen[0].correction_id == "corr-1"

    # The old id-alone confirmation no longer validates at all.
    assert (
        service.execute(
            {"intent": "ack_correction", "correction_id": "corr-1"}
        ).code
        == "invalid_command"
    )
    for omitted in ("observed_count", "payload_sha256"):
        raw = {
            "intent": "ack_correction",
            "correction_id": "corr-1",
            "observed_count": 4,
            "payload_sha256": "a" * 64,
        }
        del raw[omitted]
        assert service.execute(raw).code == "invalid_command"
    blank = service.execute(
        {
            "intent": "ack_correction",
            "correction_id": "corr-1",
            "observed_count": 4,
            "payload_sha256": "   ",
        }
    )
    assert blank.code == "invalid_command"

    accepted = service.execute(
        {
            "intent": "ack_correction",
            "correction_id": "corr-1",
            "observed_count": 4,
            "payload_sha256": "a" * 64,
        }
    )
    assert accepted.code == "accepted"
    assert seen[-1].observed_count == 4
    assert seen[-1].payload_sha256 == "a" * 64


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
    assert seen[-1].worktree is None

    with_worktree = service.execute(
        {
            "intent": "record_direct_exception",
            "issue_id": "ENG-9",
            "reason": "one-line typo fix in a named worktree",
            "expected_files": ["src/app.py"],
            "expected_lines": 5,
            "verification": "pytest -q",
            "worktree": "/repo/worktrees/eng-9",
        }
    )
    assert with_worktree.code == "accepted"
    assert seen[-1].worktree == "/repo/worktrees/eng-9"

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


def test_acceptance_intents_are_strict_and_routed(database: Database) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    seen: list[object] = []

    def required(command: object) -> dict[str, object]:
        seen.append(command)
        return {"issue_id": command.issue_id, "state": "pending"}

    def satisfied(command: object) -> dict[str, object]:
        seen.append(command)
        return {"issue_id": command.issue_id, "state": "satisfied"}

    service = HermesCommandService(
        queue,
        handlers={"require_acceptance": required, "satisfy_acceptance": satisfied},
    )

    require_result = service.execute(
        {
            "intent": "require_acceptance",
            "issue_id": "ENG-9",
            "instruction_id": "chat-9",
            "predicates": ["tests pass"],
        }
    )
    assert require_result.code == "accepted"
    assert require_result.state == {"issue_id": "ENG-9", "state": "pending"}
    assert seen[0].predicates == ["tests pass"]

    satisfy_result = service.execute(
        {
            "intent": "satisfy_acceptance",
            "issue_id": "ENG-9",
            "evidence": {"tests pass": "pytest output attached"},
        }
    )
    assert satisfy_result.code == "accepted"
    assert satisfy_result.state == {"issue_id": "ENG-9", "state": "satisfied"}
    assert seen[1].evidence == {"tests pass": "pytest output attached"}

    invalid_require_empty = service.execute(
        {
            "intent": "require_acceptance",
            "issue_id": "ENG-9",
            "instruction_id": "chat-9",
            "predicates": [],
        }
    )
    assert invalid_require_empty.code == "invalid_command"

    invalid_require_blank = service.execute(
        {
            "intent": "require_acceptance",
            "issue_id": "ENG-9",
            "instruction_id": "chat-9",
            "predicates": [""],
        }
    )
    assert invalid_require_blank.code == "invalid_command"

    invalid_satisfy_empty = service.execute(
        {"intent": "satisfy_acceptance", "issue_id": "ENG-9", "evidence": {}}
    )
    assert invalid_satisfy_empty.code == "invalid_command"

    invalid_satisfy_blank = service.execute(
        {
            "intent": "satisfy_acceptance",
            "issue_id": "ENG-9",
            "evidence": {"tests pass": ""},
        }
    )
    assert invalid_satisfy_blank.code == "invalid_command"


def test_acceptance_conflict_surfaces_as_a_rejection(database: Database) -> None:
    from hermes_orchestrator.acceptance import AcceptanceGateConflict

    queue = QueueService(database, EventStore(database), {"demo"})

    def refusing(command: object) -> dict[str, object]:
        raise AcceptanceGateConflict(
            f"acceptance gate for {command.issue_id} is already satisfied; "
            "it cannot be re-required"
        )

    service = HermesCommandService(queue, handlers={"require_acceptance": refusing})
    result = service.execute(
        {
            "intent": "require_acceptance",
            "issue_id": "ENG-9",
            "instruction_id": "chat-9",
            "predicates": ["tests pass"],
        }
    )
    assert result.code == "rejected"
    assert "already satisfied" in str(result.state["reason"])


def test_acceptance_intents_without_handlers_are_unavailable(
    command_service: HermesCommandService,
) -> None:
    missing_require = command_service.execute(
        {
            "intent": "require_acceptance",
            "issue_id": "ENG-9",
            "instruction_id": "chat-9",
            "predicates": ["tests pass"],
        }
    )
    assert missing_require.code == "intent_unavailable"

    missing_satisfy = command_service.execute(
        {
            "intent": "satisfy_acceptance",
            "issue_id": "ENG-9",
            "evidence": {"tests pass": "evidence"},
        }
    )
    assert missing_satisfy.code == "intent_unavailable"


def test_supports_reflects_acceptance_intents(database: Database) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    bare = HermesCommandService(queue)
    assert not bare.supports("require_acceptance")
    assert not bare.supports("satisfy_acceptance")

    wired = HermesCommandService(
        queue,
        handlers={
            "require_acceptance": lambda c: {},
            "satisfy_acceptance": lambda c: {},
        },
    )
    assert wired.supports("require_acceptance")
    assert wired.supports("satisfy_acceptance")


class _FakeLinearReads:
    """A stand-in for ``LinearReadPort`` that returns a fixed status."""

    def __init__(self, status: str) -> None:
        self._status = status
        self.calls: list[str] = []

    def get_issue(self, issue_id: str) -> object:
        self.calls.append(issue_id)
        return SimpleNamespace(status=self._status)


def _done_issue(queue: QueueService, issue_id: str = "ENG-9") -> None:
    queue.admit(
        AdmissionRequest(
            issue_id=issue_id,
            project_key="demo",
            linear_priority=2,
            admitted_by="operator",
            instruction_id=f"chat-{issue_id}-original",
        )
    )
    queue.complete(
        issue_id,
        reason="linear_completed",
        evidence=f"https://linear.example/{issue_id}",
    )


def test_queue_issue_reactivates_done_issue_with_non_terminal_linear(
    database: Database,
) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    _done_issue(queue)
    linear = _FakeLinearReads("In Development")
    service = HermesCommandService(queue, linear_reads=linear)

    result = service.execute(
        {
            "intent": "queue_issue",
            "issue_id": "ENG-9",
            "project_key": "demo",
            "priority": 1,
            "operator_instruction_id": "chat-reopen",
        }
    )

    assert result.code == "queued"
    assert result.state == {
        "issue_id": "ENG-9",
        "project_key": "demo",
        "priority": 1,
        "state": "queued",
        "reactivated": True,
    }
    assert linear.calls == ["ENG-9"]


def test_queue_issue_on_done_issue_without_linear_reads_is_denied(
    database: Database,
) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    _done_issue(queue)
    service = HermesCommandService(queue)

    result = service.execute(
        {
            "intent": "queue_issue",
            "issue_id": "ENG-9",
            "project_key": "demo",
            "priority": 1,
            "operator_instruction_id": "chat-reopen",
        }
    )

    assert result.code == "admission_denied"
    assert result.state == {"reason": "reactivation requires a Linear read"}


def test_queue_issue_reactivation_is_idempotent(database: Database) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    _done_issue(queue)
    service = HermesCommandService(
        queue, linear_reads=_FakeLinearReads("In Development")
    )
    command = {
        "intent": "queue_issue",
        "issue_id": "ENG-9",
        "project_key": "demo",
        "priority": 1,
        "operator_instruction_id": "chat-reopen",
    }

    first = service.execute(command)
    second = service.execute(command)

    # The first call denies plain ``admit`` ("already admitted") and
    # reactivates instead. The replay is idempotent end-to-end: the row
    # it left behind now satisfies plain ``admit``'s own idempotency
    # check directly, so the second call never touches reactivation at
    # all -- it is simply, correctly, already exactly what was asked
    # for. Either way the reported queue state is identical.
    assert first.code == second.code == "queued"
    assert first.state["reactivated"] is True
    for state in (first.state, second.state):
        assert state["issue_id"] == "ENG-9"
        assert state["project_key"] == "demo"
        assert state["priority"] == 1
        assert state["state"] == "queued"
    count = database.execute(
        "SELECT count(*) AS n FROM events WHERE event_type = 'issue.reactivated'"
    ).fetchone()
    assert count["n"] == 1


def test_queue_issue_on_active_issue_still_denied(database: Database) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    queue.admit(
        AdmissionRequest(
            issue_id="ENG-9",
            project_key="demo",
            linear_priority=2,
            admitted_by="operator",
            instruction_id="chat-9",
        )
    )
    service = HermesCommandService(
        queue, linear_reads=_FakeLinearReads("In Development")
    )

    result = service.execute(
        {
            "intent": "queue_issue",
            "issue_id": "ENG-9",
            "project_key": "demo",
            "priority": 1,
            "operator_instruction_id": "chat-again",
        }
    )

    assert result.code == "admission_denied"
    assert result.state == {"reason": "issue ENG-9 is already admitted"}


def _raise_decision_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "intent": "raise_operator_decision",
        "issue_id": "ENG-9",
        "requesting_role": "lead",
        "question": "Rename the public API or keep the deprecated alias?",
        "authority_reason": "breaks a documented external contract",
        "facts": ["three external callers use the old name"],
        "options": [
            {"label": "rename", "tradeoffs": "breaks callers immediately"},
            {"label": "alias", "tradeoffs": "carries dead code forward"},
        ],
        "recommendation": "keep the alias for one release",
        "delay_impact": "blocks the migration packet",
        "paused_scope": "the migration packet",
    }
    payload.update(overrides)
    return payload


def test_raise_operator_decision_is_strict_and_routed(database: Database) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    seen: list[object] = []

    def raised(command: object) -> dict[str, object]:
        seen.append(command)
        return {"decision_id": "dec-1", "created": True, "pending": 1, "summary": "s"}

    service = HermesCommandService(
        queue, handlers={"raise_operator_decision": raised}
    )

    result = service.execute(_raise_decision_payload())
    assert result.code == "accepted"
    assert result.state == {
        "decision_id": "dec-1",
        "created": True,
        "pending": 1,
        "summary": "s",
    }
    assert seen[0].urgency == 2
    assert seen[0].category == "authority"
    assert seen[0].request_key is None
    assert seen[0].facts == ["three external callers use the old name"]
    assert seen[0].options[0].label == "rename"

    with_overrides = service.execute(
        _raise_decision_payload(urgency=0, category="authority", request_key="k-1")
    )
    assert with_overrides.code == "accepted"
    assert seen[1].urgency == 0
    assert seen[1].request_key == "k-1"


def test_raise_operator_decision_refuses_malformed_payloads(
    database: Database,
) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    service = HermesCommandService(
        queue, handlers={"raise_operator_decision": lambda c: {}}
    )

    urgency_too_high = service.execute(_raise_decision_payload(urgency=4))
    assert urgency_too_high.code == "invalid_command"

    urgency_negative = service.execute(_raise_decision_payload(urgency=-1))
    assert urgency_negative.code == "invalid_command"

    empty_options = service.execute(_raise_decision_payload(options=[]))
    assert empty_options.code == "invalid_command"

    blank_question = service.execute(_raise_decision_payload(question="   "))
    assert blank_question.code == "invalid_command"

    blank_option_label = service.execute(
        _raise_decision_payload(options=[{"label": "  ", "tradeoffs": "x"}])
    )
    assert blank_option_label.code == "invalid_command"

    extra_field = service.execute(_raise_decision_payload(unexpected="nope"))
    assert extra_field.code == "invalid_command"


def test_pending_and_next_operator_decisions_are_strict_and_routed(
    database: Database,
) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    calls: list[tuple[str, object]] = []

    def pending(command: object) -> dict[str, object]:
        calls.append(("pending", command))
        return {"decisions": []}

    def nxt(command: object) -> dict[str, object]:
        calls.append(("next", command))
        return {"decision": None, "pending": 0}

    service = HermesCommandService(
        queue,
        handlers={
            "pending_operator_decisions": pending,
            "next_operator_decision": nxt,
        },
    )

    pending_result = service.execute({"intent": "pending_operator_decisions"})
    assert pending_result.code == "accepted"
    assert pending_result.state == {"decisions": []}
    assert calls[0][1].project_key is None

    scoped = service.execute(
        {"intent": "pending_operator_decisions", "project_key": "demo"}
    )
    assert scoped.code == "accepted"
    assert calls[1][1].project_key == "demo"

    next_result = service.execute({"intent": "next_operator_decision"})
    assert next_result.code == "accepted"
    assert next_result.state == {"decision": None, "pending": 0}
    assert calls[2][1].project_key is None

    invalid = service.execute(
        {"intent": "pending_operator_decisions", "unexpected": "nope"}
    )
    assert invalid.code == "invalid_command"


def test_operator_decision_intents_without_handlers_are_unavailable(
    command_service: HermesCommandService,
) -> None:
    for intent, payload in (
        ("raise_operator_decision", _raise_decision_payload()),
        ("pending_operator_decisions", {}),
        ("next_operator_decision", {}),
    ):
        result = command_service.execute({**payload, "intent": intent})
        assert result.code == "intent_unavailable", intent


def test_supports_reflects_operator_decision_intents(database: Database) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    bare = HermesCommandService(queue)
    assert not bare.supports("raise_operator_decision")
    assert not bare.supports("pending_operator_decisions")
    assert not bare.supports("next_operator_decision")

    wired = HermesCommandService(
        queue,
        handlers={
            "raise_operator_decision": lambda c: {},
            "pending_operator_decisions": lambda c: {},
            "next_operator_decision": lambda c: {},
        },
    )
    assert wired.supports("raise_operator_decision")
    assert wired.supports("pending_operator_decisions")
    assert wired.supports("next_operator_decision")


def test_apply_operator_decision_answer_and_next_action_are_optional_but_strict(
    database: Database,
) -> None:
    queue = QueueService(database, EventStore(database), {"demo"})
    seen: list[object] = []

    def applied(command: object) -> dict[str, object]:
        seen.append(command)
        return {"decision_id": command.decision_id, "status": command.status}

    service = HermesCommandService(
        queue, handlers={"apply_operator_decision": applied}
    )

    legacy = service.execute(
        {
            "intent": "apply_operator_decision",
            "decision_id": "dec-1",
            "status": "approved",
            "source_message": "go ahead",
        }
    )
    assert legacy.code == "accepted"
    assert seen[0].answer is None
    assert seen[0].next_action is None

    with_answer = service.execute(
        {
            "intent": "apply_operator_decision",
            "decision_id": "dec-1",
            "status": "approved",
            "source_message": "go ahead",
            "answer": "use the alias",
            "next_action": "resume the migration packet",
        }
    )
    assert with_answer.code == "accepted"
    assert seen[1].answer == "use the alias"
    assert seen[1].next_action == "resume the migration packet"

    blank_answer = service.execute(
        {
            "intent": "apply_operator_decision",
            "decision_id": "dec-1",
            "status": "approved",
            "source_message": "go ahead",
            "answer": "   ",
        }
    )
    assert blank_answer.code == "invalid_command"
