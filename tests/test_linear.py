from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.linear import (
    ExternalEffectStore,
    LinearClient,
    LinearGraphQLTransport,
    LinearProjection,
    ProjectLinearRouter,
)


class RecordingLinearTransport:
    def __init__(self) -> None:
        self.operations: list[str] = []
        self.variables: list[dict[str, Any]] = []
        self.status = "Todo"
        self.state_id = "state-todo"
        self.assignee_id = "user-operator"
        self.team_id = "team-engineering"
        self.revision = "2026-08-26T12:00:00Z"

    def issue(
        self,
        *,
        status: str,
        state_id: str,
        assignee_id: str,
        revision: str,
    ) -> None:
        self.status = status
        self.state_id = state_id
        self.assignee_id = assignee_id
        self.revision = revision

    async def execute(
        self,
        operation: str,
        query: str,
        variables: dict[str, Any],
    ) -> dict[str, Any]:
        self.operations.append(operation)
        self.variables.append(variables)
        if operation == "Issue":
            return {
                "issue": {
                    "id": "linear-eng-9",
                    "identifier": variables["id"],
                    "updatedAt": self.revision,
                    "state": {"id": self.state_id, "name": self.status},
                    "assignee": {"id": self.assignee_id},
                    "team": {"id": self.team_id},
                }
            }
        update = variables["input"]
        if "stateId" in update:
            self.state_id = update["stateId"]
        if "assigneeId" in update:
            self.assignee_id = update["assigneeId"]
        self.revision = "2026-08-26T12:01:00Z"
        return {
            "issueUpdate": {
                "success": True,
                "issue": {"id": variables["id"], "updatedAt": self.revision},
            }
        }


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def transport() -> RecordingLinearTransport:
    return RecordingLinearTransport()


@pytest.fixture
def linear_client(
    database: Database,
    transport: RecordingLinearTransport,
) -> LinearClient:
    return LinearClient(
        transport=transport,
        effects=ExternalEffectStore(database),
        status_ids={
            "Todo": "state-todo",
            "In Development": "state-development",
            "Review": "state-review",
            "QA": "state-qa",
            "Done": "state-done",
        },
        assignee_ids={"operator": "user-operator", "ryan": "user-ryan"},
        expected_team_id="team-engineering",
    )


@pytest.mark.asyncio
async def test_projection_updates_assignee_without_mutating_status(
    linear_client: LinearClient,
    transport: RecordingLinearTransport,
) -> None:
    result = await linear_client.project(
        "ENG-9",
        LinearProjection(assignee_alias="ryan"),
        effect_id="effect-1",
    )

    assert result.changed_fields == ("assignee",)
    assert transport.operations == ["Issue", "IssueUpdate"]
    assert transport.variables[1] == {
        "id": "linear-eng-9",
        "input": {"assigneeId": "user-ryan"},
    }


@pytest.mark.asyncio
async def test_projection_refuses_non_atomic_status_transition(
    linear_client: LinearClient,
    transport: RecordingLinearTransport,
) -> None:
    transport.issue(
        status="Done",
        state_id="state-done",
        assignee_id="user-operator",
        revision="done-r1",
    )

    with pytest.raises(ValueError, match="status mutation requires compare-and-set"):
        await linear_client.project(
            "ENG-9",
            LinearProjection(status="In Development", assignee_alias="operator"),
            effect_id="effect-terminal-guard",
        )

    assert transport.operations == ["Issue"]


@pytest.mark.asyncio
async def test_projection_is_noop_when_target_matches(
    linear_client: LinearClient,
    transport: RecordingLinearTransport,
) -> None:
    transport.issue(
        status="Review",
        state_id="state-review",
        assignee_id="user-operator",
        revision="r2",
    )

    result = await linear_client.project(
        "ENG-9",
        LinearProjection(status="Review", assignee_alias="operator"),
        effect_id="effect-2",
    )

    assert result.changed_fields == ()
    assert transport.operations == ["Issue"]


@pytest.mark.asyncio
async def test_completed_effect_retry_does_not_touch_linear(
    linear_client: LinearClient,
    transport: RecordingLinearTransport,
) -> None:
    target = LinearProjection(assignee_alias="ryan")
    first = await linear_client.project("ENG-9", target, effect_id="effect-3")
    transport.operations.clear()
    transport.variables.clear()

    second = await linear_client.project("ENG-9", target, effect_id="effect-3")

    assert second == first
    assert transport.operations == []
    assert transport.variables == []


def test_allowed_projection_has_no_comment_or_label_fields() -> None:
    assert set(LinearProjection.model_fields) == {"status", "assignee_alias"}


@pytest.mark.asyncio
async def test_project_validation_rejects_issue_from_another_team(
    linear_client: LinearClient,
    transport: RecordingLinearTransport,
) -> None:
    transport.team_id = "team-other"

    with pytest.raises(ValueError, match="configured Linear team"):
        await linear_client.validate_issue("ENG-9")

    assert transport.operations == ["Issue"]


@pytest.mark.asyncio
async def test_qa_projection_fails_closed_when_team_has_no_qa_state(
    database: Database,
    transport: RecordingLinearTransport,
) -> None:
    client = LinearClient(
        transport=transport,
        effects=ExternalEffectStore(database),
        status_ids={
            "Todo": "state-todo",
            "In Development": "state-progress",
            "Review": "state-review",
            "Done": "state-done",
        },
        assignee_ids={"operator": "user-operator", "ryan": "user-ryan"},
        expected_team_id="team-engineering",
    )

    with pytest.raises(ValueError, match=r"QA.*not configured"):
        await client.project(
            "ENG-9",
            LinearProjection(status="QA", assignee_alias="ryan"),
            effect_id="effect-missing-qa",
        )

    assert transport.operations == ["Issue"]


@pytest.mark.asyncio
async def test_project_router_validates_before_routing_projection(
    linear_client: LinearClient,
) -> None:
    router = ProjectLinearRouter(
        clients={"demo": linear_client},
        project_for_issue=lambda issue_id: "demo" if issue_id == "ENG-9" else "other",
    )

    issue = await router.validate("demo", "ENG-9")
    result = await router.project(
        "ENG-9",
        LinearProjection(assignee_alias="ryan"),
        "effect-router",
    )

    assert issue.issue_id == "ENG-9"
    assert result.changed_fields == ("assignee",)


@pytest.mark.asyncio
async def test_graphql_transport_sends_token_and_operation() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers["Authorization"]
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"data": {"issue": {"id": "issue-1"}}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        transport = LinearGraphQLTransport("linear-token", client=http)
        result = await transport.execute(
            "Issue",
            "query Issue($id: String!) { issue(id: $id) { id } }",
            {"id": "ENG-9"},
        )

    assert result == {"issue": {"id": "issue-1"}}
    assert captured == {
        "authorization": "linear-token",
        "body": {
            "operationName": "Issue",
            "query": "query Issue($id: String!) { issue(id: $id) { id } }",
            "variables": {"id": "ENG-9"},
        },
    }
