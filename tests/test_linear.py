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
    projection_request,
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
    assert transport.operations == ["Issue", "IssueUpdate", "Issue"]
    assert transport.variables[1] == {
        "id": "linear-eng-9",
        "input": {"assigneeId": "user-ryan"},
    }


@pytest.mark.asyncio
async def test_projection_updates_allowed_status_transition(
    linear_client: LinearClient,
    transport: RecordingLinearTransport,
) -> None:
    result = await linear_client.project(
        "ENG-9",
        LinearProjection(status="In Development", assignee_alias="operator"),
        effect_id="effect-status",
    )

    assert result.changed_fields == ("status",)
    assert transport.operations == ["Issue", "IssueUpdate", "Issue"]
    assert transport.variables[1] == {
        "id": "linear-eng-9",
        "input": {"stateId": "state-development"},
    }


@pytest.mark.asyncio
async def test_done_to_in_development_accepted_for_acceptance_projection(
    linear_client: LinearClient,
    transport: RecordingLinearTransport,
) -> None:
    """INFRA-198 (Sol 52d15493, required test 3): the post-merge
    acceptance projection — the gate machinery moves a merged issue
    done -> post_merge_acceptance and projects the hold back to Linear
    as Done -> In Development — is an allowed transition (it was
    refused live on merged e8d5757)."""

    transport.issue(
        status="Done",
        state_id="state-done",
        assignee_id="user-operator",
        revision="done-r1",
    )

    result = await linear_client.project(
        "ENG-9",
        LinearProjection(status="In Development", assignee_alias="operator"),
        effect_id="effect-acceptance-hold",
    )

    assert result.changed_fields == ("status",)
    assert transport.operations == ["Issue", "IssueUpdate", "Issue"]
    assert transport.variables[1] == {
        "id": "linear-eng-9",
        "input": {"stateId": "state-development"},
    }


@pytest.mark.asyncio
async def test_projection_refuses_every_other_done_regression(
    linear_client: LinearClient,
    transport: RecordingLinearTransport,
) -> None:
    """The acceptance carve-out is exactly one transition: every other
    regression out of Done still fails closed with no update sent."""

    for index, target in enumerate(("Todo", "Review", "QA")):
        transport.issue(
            status="Done",
            state_id="state-done",
            assignee_id="user-operator",
            revision=f"done-r{index}",
        )
        transport.operations.clear()

        with pytest.raises(ValueError, match=r"status transition.*not allowed"):
            await linear_client.project(
                "ENG-9",
                LinearProjection(status=target, assignee_alias="operator"),
                effect_id=f"effect-terminal-guard-{index}",
            )

        assert transport.operations == ["Issue"]


def _pending_linear_rows(database: Database) -> list[dict[str, Any]]:
    """Read pending Linear effects EXACTLY the way reconciliation does
    (``reconcile._stage_linear`` /
    ``_project_pending_linear_effect``): same SELECT, same
    ``request_json`` fields."""

    rows = database.execute(
        "SELECT effect_id, target, request_json FROM external_effects "
        "WHERE adapter = 'linear' AND state = 'pending'"
    ).fetchall()
    return [
        {
            "effect_id": str(row["effect_id"]),
            "target": str(row["target"]),
            "request": json.loads(row["request_json"]),
        }
        for row in rows
    ]


@pytest.mark.asyncio
async def test_replay_adopts_the_pending_row_and_completes_it_exactly_once(
    database: Database,
) -> None:
    """Sol ec0ed7fe gap 2 (flipped revision-guard test): the journal
    row is the stable TARGET-ONLY record, so a replay after a lost
    response ADOPTS that same pending row, re-reads current state, and
    completes the effect exactly once — never a second mutation for a
    target that already holds, never a second row, never a refusal of
    its own journal entry."""

    class LostResponseTransport(RecordingLinearTransport):
        fail_after_update = True

        async def execute(
            self,
            operation: str,
            query: str,
            variables: dict[str, Any],
        ) -> dict[str, Any]:
            result = await super().execute(operation, query, variables)
            if operation == "IssueUpdate" and self.fail_after_update:
                self.fail_after_update = False
                raise TimeoutError("Linear response was lost")
            return result

    transport = LostResponseTransport()
    effects = ExternalEffectStore(database)
    client = LinearClient(
        transport=transport,
        effects=effects,
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
    target = LinearProjection(
        status="In Development",
        assignee_alias="operator",
    )
    with database.transaction() as connection:
        ExternalEffectStore.begin_in(
            connection,
            "effect-race",
            target="ENG-9",
            request=projection_request("ENG-9", target),
        )

    # The mutation applied on Linear's side but the response was lost:
    # exactly one pending row survives the crash boundary.
    with pytest.raises(TimeoutError, match="response was lost"):
        await client.project("ENG-9", target, effect_id="effect-race")
    assert transport.state_id == "state-development"
    assert [row["effect_id"] for row in _pending_linear_rows(database)] == [
        "effect-race"
    ]

    result = await client.project("ENG-9", target, effect_id="effect-race")

    # The replay adopted the same row, saw the target already holding,
    # and completed without a second IssueUpdate.
    assert result.changed_fields == ()
    assert transport.operations == ["Issue", "IssueUpdate", "Issue"]
    assert effects.get("effect-race").state == "completed"
    assert _pending_linear_rows(database) == []
    assert (
        int(
            database.scalar(
                "SELECT count(*) FROM external_effects "
                "WHERE effect_id = 'effect-race'"
            )
        )
        == 1
    )

    # Every further retry is answered from the completed journal entry.
    transport.operations.clear()
    replayed = await client.project("ENG-9", target, effect_id="effect-race")
    assert replayed == result
    assert transport.operations == []


@pytest.mark.asyncio
async def test_non_prejournaled_projection_preserves_revision_conflict_guard(
    database: Database,
) -> None:
    """A lost response must not let a retry overwrite a newer Linear edit."""

    class LostResponseTransport(RecordingLinearTransport):
        fail_after_update = True

        async def execute(
            self,
            operation: str,
            query: str,
            variables: dict[str, Any],
        ) -> dict[str, Any]:
            result = await super().execute(operation, query, variables)
            if operation == "IssueUpdate" and self.fail_after_update:
                self.fail_after_update = False
                raise TimeoutError("Linear response was lost")
            return result

    transport = LostResponseTransport()
    client = LinearClient(
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
    target = LinearProjection(
        status="In Development",
        assignee_alias="operator",
    )

    with pytest.raises(TimeoutError, match="response was lost"):
        await client.project("ENG-9", target, effect_id="effect-review-flow")
    transport.issue(
        status="Review",
        state_id="state-review",
        assignee_id="user-operator",
        revision="human-r3",
    )

    with pytest.raises(RuntimeError, match="changed after projection began"):
        await client.project("ENG-9", target, effect_id="effect-review-flow")

    assert transport.state_id == "state-review"
    assert transport.operations.count("IssueUpdate") == 1


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
async def test_initial_read_failure_still_leaves_the_pending_row(
    database: Database,
) -> None:
    """The activation journal survives an outage on the first read."""

    class UnreachableTransport:
        async def execute(
            self, operation: str, query: str, variables: dict[str, Any]
        ) -> dict[str, Any]:
            raise TimeoutError("Linear is unreachable")

    effects = ExternalEffectStore(database)
    client = LinearClient(
        transport=UnreachableTransport(),
        effects=effects,
        status_ids={"Todo": "state-todo", "In Development": "state-development"},
        assignee_ids={"operator": "user-operator", "ryan": "user-ryan"},
        expected_team_id="team-engineering",
    )
    target = LinearProjection(status="In Development", assignee_alias="operator")
    with database.transaction() as connection:
        ExternalEffectStore.begin_in(
            connection,
            "effect-read-outage",
            target="ENG-9",
            request=projection_request("ENG-9", target),
        )

    with pytest.raises(TimeoutError, match="unreachable"):
        await client.project("ENG-9", target, effect_id="effect-read-outage")

    [row] = _pending_linear_rows(database)
    assert row["effect_id"] == "effect-read-outage"
    assert row["target"] == "ENG-9"
    # The exact fields reconcile._project_pending_linear_effect binds.
    assert row["request"] == {
        "issue_id": "ENG-9",
        "target": {"status": "In Development", "assignee_alias": "operator"},
    }


@pytest.mark.parametrize(
    "client_overrides, issue_overrides, match",
    [
        pytest.param(
            {"expected_team_id": "team-other"},
            {},
            "configured Linear team",
            id="team-mismatch",
        ),
        pytest.param(
            {
                "status_ids": {
                    "Todo": "state-todo",
                    "Review": "state-review",
                }
            },
            {},
            r"In Development.*not configured",
            id="missing-status-mapping",
        ),
        pytest.param(
            {"assignee_ids": {"ryan": "user-ryan"}},
            {},
            r"operator.*not configured",
            id="missing-assignee-mapping",
        ),
        pytest.param(
            {},
            {
                "status": "Done",
                "state_id": "state-done",
                "assignee_id": "user-operator",
                "revision": "done-r1",
            },
            r"status transition.*not allowed",
            id="disallowed-transition",
        ),
    ],
)
@pytest.mark.asyncio
async def test_validation_failures_preserve_one_pending_row(
    database: Database,
    transport: RecordingLinearTransport,
    client_overrides: dict[str, Any],
    issue_overrides: dict[str, Any],
    match: str,
) -> None:
    """Validation failures preserve the activation's pending row."""

    if issue_overrides:
        transport.issue(**issue_overrides)
    arguments: dict[str, Any] = {
        "transport": transport,
        "effects": ExternalEffectStore(database),
        "status_ids": {
            "Todo": "state-todo",
            "In Development": "state-development",
            "Review": "state-review",
            "QA": "state-qa",
            "Done": "state-done",
        },
        "assignee_ids": {"operator": "user-operator", "ryan": "user-ryan"},
        "expected_team_id": "team-engineering",
    }
    arguments.update(client_overrides)
    client = LinearClient(**arguments)
    target = LinearProjection(status="In Development", assignee_alias="operator")
    with database.transaction() as connection:
        ExternalEffectStore.begin_in(
            connection,
            "effect-validation",
            target="ENG-9",
            request=projection_request("ENG-9", target),
        )

    with pytest.raises(ValueError, match=match):
        await client.project("ENG-9", target, effect_id="effect-validation")

    assert "IssueUpdate" not in transport.operations
    [row] = _pending_linear_rows(database)
    assert row["effect_id"] == "effect-validation"

    # A retry adopts the same row, fails the same validation, and the
    # journal still holds exactly one record.
    with pytest.raises(ValueError, match=match):
        await client.project("ENG-9", target, effect_id="effect-validation")
    assert [r["effect_id"] for r in _pending_linear_rows(database)] == [
        "effect-validation"
    ]
    assert (
        int(
            database.scalar(
                "SELECT count(*) FROM external_effects "
                "WHERE effect_id = 'effect-validation'"
            )
        )
        == 1
    )


@pytest.mark.asyncio
async def test_project_adopts_a_row_begun_without_the_live_client(
    linear_client: LinearClient,
    database: Database,
    transport: RecordingLinearTransport,
) -> None:
    """Sol ec0ed7fe gap 2: ``ExternalEffectStore.begin_in`` writes the
    pending row on a plain local transaction — no live client — exactly
    the way the activation transaction journals it, and ``project``
    then ADOPTS that row (same effect id, byte-identical request)
    instead of double-beginning or refusing; the same single row is
    completed."""

    target = LinearProjection(status="In Development", assignee_alias="operator")
    with database.transaction() as connection:
        begun = ExternalEffectStore.begin_in(
            connection,
            "effect-prebegun",
            target="ENG-9",
            request=projection_request("ENG-9", target),
        )
    assert begun.state == "pending"

    result = await linear_client.project(
        "ENG-9", target, effect_id="effect-prebegun"
    )

    assert result.changed_fields == ("status",)
    assert ExternalEffectStore(database).get("effect-prebegun").state == "completed"
    assert (
        int(
            database.scalar(
                "SELECT count(*) FROM external_effects "
                "WHERE effect_id = 'effect-prebegun'"
            )
        )
        == 1
    )
    # A different request for the same effect id is still refused, by
    # the journal itself.
    with pytest.raises(ValueError, match="already used for another request"):
        await linear_client.project(
            "ENG-9",
            LinearProjection(status="Review", assignee_alias="ryan"),
            effect_id="effect-prebegun",
        )


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
