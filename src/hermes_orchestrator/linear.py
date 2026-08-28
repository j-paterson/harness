"""Minimal, idempotent Linear workflow projection."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

import httpx
from pydantic import BaseModel, ConfigDict

from hermes_orchestrator.db import Database

_ISSUE_QUERY = """
query Issue($id: String!) {
  issue(id: $id) {
    id
    identifier
    updatedAt
    state { id name }
    assignee { id }
    team { id }
  }
}
""".strip()

_ISSUE_UPDATE_MUTATION = """
mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) {
  issueUpdate(id: $id, input: $input) {
    success
    issue { id updatedAt }
  }
}
""".strip()

_ALLOWED_STATUS_TRANSITIONS = frozenset(
    {
        ("Todo", "In Development"),
        ("In Development", "Review"),
        ("Review", "In Development"),
        ("Review", "QA"),
        ("Review", "Done"),
        ("QA", "In Development"),
        ("QA", "Done"),
    }
)


class LinearProjection(BaseModel):
    """The complete set of workflow fields Hermes may change."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["Todo", "In Development", "Review", "QA", "Done"] | None = None
    assignee_alias: Literal["operator", "ryan"]


@dataclass(frozen=True, slots=True)
class LinearIssue:
    """Current Linear fields required to calculate a minimal update."""

    issue_id: str
    linear_id: str
    status: str
    state_id: str
    assignee_id: str | None
    team_id: str
    revision: str


@dataclass(frozen=True, slots=True)
class ProjectionResult:
    """Journal-safe outcome of one requested projection."""

    issue_id: str
    changed_fields: tuple[str, ...]
    source_revision: str
    response_revision: str

    def as_record(self) -> dict[str, Any]:
        """Return a JSON-compatible result."""

        return {
            "issue_id": self.issue_id,
            "changed_fields": list(self.changed_fields),
            "source_revision": self.source_revision,
            "response_revision": self.response_revision,
        }

    @classmethod
    def from_record(cls, value: Mapping[str, Any]) -> ProjectionResult:
        """Rehydrate a completed effect result."""

        return cls(
            issue_id=str(value["issue_id"]),
            changed_fields=tuple(str(item) for item in value["changed_fields"]),
            source_revision=str(value["source_revision"]),
            response_revision=str(value["response_revision"]),
        )


class LinearTransport(Protocol):
    """GraphQL execution boundary used by the projection client."""

    async def execute(
        self,
        operation: str,
        query: str,
        variables: dict[str, Any],
    ) -> dict[str, Any]: ...


class LinearGraphQLTransport:
    """HTTP transport for Linear's GraphQL endpoint."""

    def __init__(
        self,
        token: str,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not token:
            raise ValueError("Linear token is required")
        self._token = token
        self._client = client

    async def execute(
        self,
        operation: str,
        query: str,
        variables: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute one named operation and return its data object."""

        owns_client = self._client is None
        client = self._client or httpx.AsyncClient()
        try:
            response = await client.post(
                "https://api.linear.app/graphql",
                headers={"Authorization": self._token},
                json={
                    "operationName": operation,
                    "query": query,
                    "variables": variables,
                },
            )
            response.raise_for_status()
            payload = response.json()
        finally:
            if owns_client:
                await client.aclose()
        return _graphql_data(payload)


def _graphql_data(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Linear returned a non-object response")
    if payload.get("errors"):
        raise RuntimeError("Linear GraphQL operation failed")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError("Linear response is missing data")
    return data


def _parse_issue(issue_id: str, data: dict[str, Any]) -> LinearIssue:
    issue = data.get("issue")
    if not isinstance(issue, dict):
        raise ValueError(f"Linear issue not found: {issue_id}")
    state = issue.get("state")
    assignee = issue.get("assignee")
    team = issue.get("team")
    if not isinstance(state, dict):
        raise ValueError("Linear issue is missing state")
    if not isinstance(team, dict):
        raise ValueError("Linear issue is missing team")
    return LinearIssue(
        issue_id=str(issue["identifier"]),
        linear_id=str(issue["id"]),
        status=str(state["name"]),
        state_id=str(state["id"]),
        assignee_id=(str(assignee["id"]) if isinstance(assignee, dict) else None),
        team_id=str(team["id"]),
        revision=str(issue["updatedAt"]),
    )


class LinearIssueReader:
    """Bounded synchronous read-only issue reads for startup reconciliation.

    Runs the same strict ``Issue`` query as the projection client but over
    a plain synchronous transport, so the startup reconciler can project
    pending effects without an event loop. It can only read; there is no
    mutation surface here at all.
    """

    def __init__(
        self,
        token: str,
        *,
        client: httpx.Client | None = None,
        timeout: float = 10.0,
    ) -> None:
        if not token:
            raise ValueError("Linear token is required")
        self._token = token
        self._client = client
        self._timeout = timeout

    def get_issue(self, issue_id: str) -> LinearIssue:
        owns_client = self._client is None
        client = self._client or httpx.Client(timeout=self._timeout)
        try:
            response = client.post(
                "https://api.linear.app/graphql",
                headers={"Authorization": self._token},
                json={
                    "operationName": "Issue",
                    "query": _ISSUE_QUERY,
                    "variables": {"id": issue_id},
                },
            )
            response.raise_for_status()
            payload = response.json()
        finally:
            if owns_client:
                client.close()
        return _parse_issue(issue_id, _graphql_data(payload))


@dataclass(frozen=True, slots=True)
class ExternalEffect:
    """One durable external effect journal entry."""

    effect_id: str
    state: str
    request: dict[str, Any]
    response: dict[str, Any] | None


class ExternalEffectStore:
    """Durably suppress duplicate Linear effects across retries and restarts."""

    def __init__(self, database: Database) -> None:
        self._database = database

    def begin(
        self,
        effect_id: str,
        *,
        target: str,
        request: dict[str, Any],
    ) -> ExternalEffect:
        """Create a pending effect or return the matching existing entry."""

        existing = self.get(effect_id)
        if existing is not None:
            if existing.request != request:
                raise ValueError("effect_id was already used for another request")
            return existing

        now = datetime.now(UTC).isoformat()
        request_json = json.dumps(request, sort_keys=True, separators=(",", ":"))
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO external_effects("
                "effect_id, adapter, operation, target, state, request_json, "
                "response_json, created_at, updated_at"
                ") VALUES (?, 'linear', 'project', ?, 'pending', ?, NULL, ?, ?)",
                (effect_id, target, request_json, now, now),
            )
        return ExternalEffect(effect_id, "pending", request, None)

    def complete(
        self,
        effect_id: str,
        response: dict[str, Any],
    ) -> ExternalEffect:
        """Record a completed response for future idempotent retries."""

        response_json = json.dumps(response, sort_keys=True, separators=(",", ":"))
        now = datetime.now(UTC).isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE external_effects SET state = 'completed', "
                "response_json = ?, updated_at = ? WHERE effect_id = ?",
                (response_json, now, effect_id),
            )
        completed = self.get(effect_id)
        if completed is None:
            raise RuntimeError("completed effect disappeared")
        return completed

    def get(self, effect_id: str) -> ExternalEffect | None:
        """Read one effect from the durable journal."""

        row = self._database.execute(
            "SELECT effect_id, state, request_json, response_json "
            "FROM external_effects WHERE effect_id = ?",
            (effect_id,),
        ).fetchone()
        if row is None:
            return None
        return ExternalEffect(
            effect_id=str(row["effect_id"]),
            state=str(row["state"]),
            request=json.loads(row["request_json"]),
            response=(
                json.loads(row["response_json"])
                if row["response_json"] is not None
                else None
            ),
        )


class LinearClient:
    """Read current Linear state and apply the smallest approved projection."""

    def __init__(
        self,
        *,
        transport: LinearTransport,
        effects: ExternalEffectStore,
        status_ids: Mapping[str, str],
        assignee_ids: Mapping[str, str],
        expected_team_id: str | None = None,
    ) -> None:
        self._transport = transport
        self._effects = effects
        self._status_ids = dict(status_ids)
        self._assignee_ids = dict(assignee_ids)
        self._expected_team_id = expected_team_id

    async def get_issue(self, issue_id: str) -> LinearIssue:
        """Read the fields required for an idempotent projection."""

        data = await self._transport.execute(
            "Issue",
            _ISSUE_QUERY,
            {"id": issue_id},
        )
        return _parse_issue(issue_id, data)

    async def validate_issue(self, issue_id: str) -> LinearIssue:
        """Read an issue and fail closed when it belongs to another team."""

        issue = await self.get_issue(issue_id)
        self._validate_team(issue)
        return issue

    async def project(
        self,
        issue_id: str,
        target: LinearProjection,
        effect_id: str,
    ) -> ProjectionResult:
        """Apply only differing state or assignee values exactly once."""

        requested_target = {
            "issue_id": issue_id,
            "target": target.model_dump(mode="json"),
        }
        effect = self._effects.get(effect_id)
        if effect is not None:
            recorded_target = {
                "issue_id": effect.request.get("issue_id"),
                "target": effect.request.get("target"),
            }
            if recorded_target != requested_target:
                raise ValueError("effect_id was already used for another request")
            if effect.state == "completed" and effect.response is not None:
                return ProjectionResult.from_record(effect.response)

        issue = await self.validate_issue(issue_id)
        update: dict[str, str] = {}
        changed_fields: list[str] = []
        target_state_id: str | None = None
        if target.status is not None:
            try:
                target_state_id = self._status_ids[target.status]
            except KeyError as error:
                raise ValueError(
                    f"Linear status {target.status} is not configured for this team"
                ) from error
        target_assignee_id = self._assignee_ids[target.assignee_alias]
        target_matches = (
            target_state_id is None or issue.state_id == target_state_id
        ) and issue.assignee_id == target_assignee_id
        if effect is not None and target_matches:
            result = ProjectionResult(
                issue_id=issue_id,
                changed_fields=tuple(
                    str(field) for field in effect.request.get("changed_fields", ())
                ),
                source_revision=str(
                    effect.request.get("source_revision", issue.revision)
                ),
                response_revision=issue.revision,
            )
            self._effects.complete(effect_id, result.as_record())
            return result
        if (
            effect is not None
            and effect.request.get("source_revision") != issue.revision
        ):
            raise RuntimeError("Linear issue changed after projection began")

        if target_state_id is not None and issue.state_id != target_state_id:
            current_status = self._logical_status(issue.state_id)
            if (current_status, target.status) not in _ALLOWED_STATUS_TRANSITIONS:
                raise ValueError(
                    f"Linear status transition {current_status} -> "
                    f"{target.status} is not allowed"
                )
            update["stateId"] = target_state_id
            changed_fields.append("status")
        if issue.assignee_id != target_assignee_id:
            update["assigneeId"] = target_assignee_id
            changed_fields.append("assignee")

        if effect is None:
            request = {
                **requested_target,
                "source_revision": issue.revision,
                "changed_fields": changed_fields,
            }
            effect = self._effects.begin(effect_id, target=issue_id, request=request)

        response_revision = issue.revision
        if update:
            data = await self._transport.execute(
                "IssueUpdate",
                _ISSUE_UPDATE_MUTATION,
                {"id": issue.linear_id, "input": update},
            )
            update_result = data.get("issueUpdate")
            if (
                not isinstance(update_result, dict)
                or update_result.get("success") is not True
            ):
                raise RuntimeError("Linear issue update did not succeed")
            updated_issue = update_result.get("issue")
            if not isinstance(updated_issue, dict):
                raise ValueError("Linear issue update is missing its issue")
            verified = await self.validate_issue(issue_id)
            if target_state_id is not None and verified.state_id != target_state_id:
                raise RuntimeError("Linear status projection verification failed")
            if verified.assignee_id != target_assignee_id:
                raise RuntimeError("Linear assignee projection verification failed")
            response_revision = verified.revision

        result = ProjectionResult(
            issue_id=issue_id,
            changed_fields=tuple(changed_fields),
            source_revision=issue.revision,
            response_revision=response_revision,
        )
        self._effects.complete(effect_id, result.as_record())
        return result

    def _logical_status(self, state_id: str) -> str:
        matches = [
            name
            for name, configured_id in self._status_ids.items()
            if configured_id == state_id
        ]
        if len(matches) != 1:
            raise ValueError(
                "Linear issue has an unknown or ambiguous configured status"
            )
        return matches[0]

    def _validate_team(self, issue: LinearIssue) -> None:
        if (
            self._expected_team_id is not None
            and issue.team_id != self._expected_team_id
        ):
            raise ValueError(
                f"Linear issue {issue.issue_id} is not in the configured Linear team"
            )


class ProjectLinearRouter:
    """Route validated issue projections through project-scoped Linear clients."""

    def __init__(
        self,
        *,
        clients: Mapping[str, LinearClient],
        project_for_issue: Callable[[str], str],
    ) -> None:
        self._clients = dict(clients)
        self._project_for_issue = project_for_issue

    async def validate(self, project_key: str, issue_id: str) -> LinearIssue:
        """Prove issue/team routing before a Claude lead can start."""

        return await self._client(project_key).validate_issue(issue_id)

    async def project(
        self,
        issue_id: str,
        target: LinearProjection,
        effect_id: str,
    ) -> ProjectionResult:
        """Project using the issue's already-admitted project registration."""

        project_key = self._project_for_issue(issue_id)
        return await self._client(project_key).project(issue_id, target, effect_id)

    def _client(self, project_key: str) -> LinearClient:
        try:
            return self._clients[project_key]
        except KeyError as error:
            raise ValueError(f"unregistered Linear project: {project_key}") from error
