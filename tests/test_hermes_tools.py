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
