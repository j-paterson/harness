from __future__ import annotations

import json
from pathlib import Path

from tests.test_cli import base_arguments, invoke


def test_acceptance_restart_is_idempotent(configured_repo: tuple[Path, Path]) -> None:
    arguments = [
        *base_arguments(configured_repo),
        "queue-add",
        "ENG-7",
        "--project",
        "demo",
        "--priority",
        "2",
        "--operator-instruction",
        "chat-123",
        "--json",
    ]

    assert invoke(arguments).exit_code == 0
    assert invoke(arguments).exit_code == 0
    reconciled = invoke(
        [*base_arguments(configured_repo), "reconcile", "--json"]
    )
    assert reconciled.exit_code == 0
    status = invoke([*base_arguments(configured_repo), "status", "--json"])

    assert status.exit_code == 0
    payload = json.loads(status.stdout)
    assert payload["queue_count"] == 1
    assert payload["mode"] == "observe"
    assert payload["admission_open"] is False


def test_queue_list_returns_only_explicitly_admitted_issue(
    configured_repo: tuple[Path, Path],
) -> None:
    add = [
        *base_arguments(configured_repo),
        "queue-add",
        "ENG-9",
        "--project",
        "demo",
        "--priority",
        "1",
        "--operator-instruction",
        "chat-999",
        "--json",
    ]
    assert invoke(add).exit_code == 0

    listed = invoke([*base_arguments(configured_repo), "queue-list", "--json"])

    assert listed.exit_code == 0
    assert [item["issue_id"] for item in json.loads(listed.stdout)] == ["ENG-9"]
