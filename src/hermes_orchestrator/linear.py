"""Minimal, idempotent Linear workflow projection."""

from __future__ import annotations

import json
from collections.abc import Mapping
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


class LinearProjection(BaseModel):
    """The complete set of workflow fields Hermes may change."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["Todo", "In Development", "Review", "QA", "Done"]
    assignee_alias: Literal["operator", "ryan"]


@dataclass(frozen=True, slots=True)
class LinearIssue:
    """Current Linear fields required to calculate a minimal update."""

    issue_id: str
    linear_id: str
    status: str
    state_id: str
    assignee_id: str | None
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
        if not isinstance(payload, dict):
            raise ValueError("Linear returned a non-object response")
        if payload.get("errors"):
            raise RuntimeError("Linear GraphQL operation failed")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError("Linear response is missing data")
        return data


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
    ) -> None:
        self._transport = transport
        self._effects = effects
        self._status_ids = dict(status_ids)
        self._assignee_ids = dict(assignee_ids)

    async def get_issue(self, issue_id: str) -> LinearIssue:
        """Read the fields required for an idempotent projection."""

        data = await self._transport.execute(
            "Issue",
            _ISSUE_QUERY,
            {"id": issue_id},
        )
        issue = data.get("issue")
        if not isinstance(issue, dict):
            raise ValueError(f"Linear issue not found: {issue_id}")
        state = issue.get("state")
        assignee = issue.get("assignee")
        if not isinstance(state, dict):
            raise ValueError("Linear issue is missing state")
        return LinearIssue(
            issue_id=str(issue["identifier"]),
            linear_id=str(issue["id"]),
            status=str(state["name"]),
            state_id=str(state["id"]),
            assignee_id=(
                str(assignee["id"]) if isinstance(assignee, dict) else None
            ),
            revision=str(issue["updatedAt"]),
        )

    async def project(
        self,
        issue_id: str,
        target: LinearProjection,
        effect_id: str,
    ) -> ProjectionResult:
        """Apply only differing state or assignee values exactly once."""

        request = {"issue_id": issue_id, "target": target.model_dump(mode="json")}
        effect = self._effects.begin(effect_id, target=issue_id, request=request)
        if effect.state == "completed" and effect.response is not None:
            return ProjectionResult.from_record(effect.response)

        issue = await self.get_issue(issue_id)
        update: dict[str, str] = {}
        changed_fields: list[str] = []
        target_state_id = self._status_ids[target.status]
        target_assignee_id = self._assignee_ids[target.assignee_alias]
        if issue.state_id != target_state_id:
            update["stateId"] = target_state_id
            changed_fields.append("status")
        if issue.assignee_id != target_assignee_id:
            update["assigneeId"] = target_assignee_id
            changed_fields.append("assignee")

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
            response_revision = str(updated_issue["updatedAt"])

        result = ProjectionResult(
            issue_id=issue_id,
            changed_fields=tuple(changed_fields),
            source_revision=issue.revision,
            response_revision=response_revision,
        )
        self._effects.complete(effect_id, result.as_record())
        return result
