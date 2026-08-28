"""Strict, bounded CircleCI v2 reads for merge reconciliation.

This surface is read-only and event-driven: it is invoked exactly once per
eligible candidate intake boundary by the merge window. Nothing here polls,
sleeps, or watches a pipeline to completion, and no response is ever
interpreted optimistically as success.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx

_SLUG_PATTERN = re.compile(
    r"^(gh|bb)/[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$"
)
_UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_BRANCH_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")

PIPELINE_STATES = frozenset(
    {"created", "errored", "setup-pending", "setup", "pending"}
)
WORKFLOW_SUCCESS = frozenset({"success"})
WORKFLOW_FAILURE = frozenset({"failed", "error", "canceled", "unauthorized"})
WORKFLOW_NONTERMINAL = frozenset({"running", "on_hold", "failing", "not_run"})
WORKFLOW_STATUSES = WORKFLOW_SUCCESS | WORKFLOW_FAILURE | WORKFLOW_NONTERMINAL
_JOB_FAILURE = frozenset(
    {"failed", "error", "canceled", "unauthorized", "infrastructure_fail",
     "timedout"}
)


class CircleCiError(RuntimeError):
    """Raised when CircleCI state cannot be established deterministically."""


@dataclass(frozen=True, slots=True)
class CircleCiResponse:
    """One transport-level CircleCI response."""

    status: int
    payload: Any


class CircleCiTransport(Protocol):
    """Narrow injected REST boundary with bounded timeouts."""

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, str] | None = None,
    ) -> CircleCiResponse: ...


class HttpxCircleCiTransport:
    """HTTPS transport for circleci.com/api/v2 with a bounded timeout.

    The token is sent per request and never stored anywhere else, logged,
    or echoed into errors.
    """

    def __init__(
        self,
        token: str,
        *,
        client: httpx.Client | None = None,
        timeout: float = 10.0,
    ) -> None:
        if not token:
            raise ValueError("CircleCI token is required")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self._token = token
        self._client = client
        self._timeout = timeout

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, str] | None = None,
    ) -> CircleCiResponse:
        owns_client = self._client is None
        client = self._client or httpx.Client(
            base_url="https://circleci.com/api/v2", timeout=self._timeout
        )
        try:
            response = client.request(
                method,
                path,
                params=dict(query) if query is not None else None,
                headers={
                    "Circle-Token": self._token,
                    "Accept": "application/json",
                },
                timeout=self._timeout,
            )
        except (httpx.HTTPError, OSError) as error:
            # Normalize unavailability to an identity-free error: no URL,
            # token, or response fragments may leak into diagnostics.
            raise CircleCiError(
                "CircleCI transport request failed: "
                f"{type(error).__name__}"
            ) from error
        finally:
            if owns_client:
                client.close()
        try:
            payload = response.json()
        except ValueError:
            payload = None
        return CircleCiResponse(response.status_code, payload)


@dataclass(frozen=True, slots=True)
class Pipeline:
    """One validated pipeline bound to an exact revision and branch."""

    id: str
    number: int
    state: str
    created_at: str
    revision: str
    branch: str


@dataclass(frozen=True, slots=True)
class Workflow:
    """One validated workflow bound to its exact pipeline."""

    id: str
    name: str
    status: str
    pipeline_id: str


@dataclass(frozen=True, slots=True)
class Job:
    """One validated job inside a workflow."""

    name: str
    status: str
    job_number: int | None


@dataclass(frozen=True, slots=True)
class CiCheck:
    """The classified CI outcome for exactly one merged commit.

    ``outcome`` is ``success`` only when every workflow of the newest
    pipeline for the exact revision succeeded, ``failure`` when any workflow
    terminally failed, and ``nonterminal`` otherwise — including missing
    pipelines and anything ambiguous. Evidence lines contain only pipeline
    numbers, workflow/job names, and statuses; never credentials.
    """

    outcome: str
    reason: str
    evidence: tuple[str, ...] = field(default=())


class CircleCiClient:
    """Bounded strict reads of pipelines, workflows, and jobs."""

    def __init__(
        self, *, transport: CircleCiTransport, max_pages: int = 3
    ) -> None:
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        self._transport = transport
        self._max_pages = max_pages

    def pipelines_for_sha(
        self, project_slug: str, *, branch: str, revision: str
    ) -> tuple[Pipeline, ...]:
        """List pipelines on one branch whose revision is exactly ``revision``.

        Pagination is bounded by ``max_pages``; every returned item is
        strictly validated and any malformed or stale entry fails closed.
        """

        _require_slug(project_slug)
        _require_branch(branch)
        _require_sha(revision)
        items = self._fetch_complete(
            f"/project/{project_slug}/pipeline",
            "pipeline list",
            base_query={"branch": branch},
        )
        matches: list[Pipeline] = []
        for item in items:
            pipeline = _parse_pipeline(item, expected_branch=branch)
            if pipeline.revision == revision:
                matches.append(pipeline)
        return tuple(matches)

    def workflows_for_pipeline(self, pipeline_id: str) -> tuple[Workflow, ...]:
        """List all of one pipeline's workflows with strict identity echoes."""

        _require_uuid(pipeline_id, "pipeline id")
        items = self._fetch_complete(
            f"/pipeline/{pipeline_id}/workflow", "workflow list"
        )
        return tuple(
            _parse_workflow(item, expected_pipeline=pipeline_id)
            for item in items
        )

    def jobs_for_workflow(self, workflow_id: str) -> tuple[Job, ...]:
        """List all of one workflow's jobs for failure evidence."""

        _require_uuid(workflow_id, "workflow id")
        items = self._fetch_complete(f"/workflow/{workflow_id}/job", "job list")
        return tuple(_parse_job(item) for item in items)

    def _fetch_complete(
        self,
        path: str,
        label: str,
        *,
        base_query: Mapping[str, str] | None = None,
    ) -> list[Any]:
        """Fetch a complete bounded snapshot of one paginated collection.

        If the page bound is exhausted while a continuation token remains,
        the snapshot is incomplete and this fails closed: partial data must
        never be interpreted — a hidden later page could contain a failed
        workflow or a newer pipeline.
        """

        items: list[Any] = []
        page_token: str | None = None
        for _ in range(self._max_pages):
            query: dict[str, str] = dict(base_query) if base_query else {}
            if page_token is not None:
                query["page-token"] = page_token
            response = self._transport.request(
                "GET", path, query=query or None
            )
            page_items, page_token = _parse_page(response, label)
            items.extend(page_items)
            if not page_token:
                return items
        raise CircleCiError(
            f"CircleCI {label} page bound exhausted with a continuation "
            "remaining; snapshot is incomplete"
        )


class CircleCiStatusAdapter:
    """Classify one merged commit's CI outcome from bounded strict reads."""

    def __init__(self, client: CircleCiClient) -> None:
        self._client = client

    def check(
        self, project_slug: str, branch: str, merge_sha: str
    ) -> CiCheck:
        pipelines = self._client.pipelines_for_sha(
            project_slug, branch=branch, revision=merge_sha
        )
        if not pipelines:
            return CiCheck(
                outcome="nonterminal",
                reason="no pipeline found for the merge commit",
            )
        newest = max(pipelines, key=lambda item: item.created_at)
        if newest.state == "errored":
            # A terminally errored pipeline (e.g. broken configuration)
            # never produces workflows; treating it as pending would hold
            # the window open forever without a correction.
            return CiCheck(
                outcome="failure",
                reason=sanitize_text(
                    f"pipeline {newest.number} errored before workflows ran"
                ),
                evidence=(
                    sanitize_text(
                        f"pipeline {newest.number} state errored "
                        "(configuration or setup error)"
                    ),
                ),
            )
        workflows = self._client.workflows_for_pipeline(newest.id)
        if not workflows:
            return CiCheck(
                outcome="nonterminal",
                reason=f"pipeline {newest.number} has no workflows yet",
            )
        failed = tuple(w for w in workflows if w.status in WORKFLOW_FAILURE)
        if failed:
            evidence: list[str] = []
            for workflow in failed:
                name = sanitize_text(workflow.name, 120)
                evidence.append(
                    sanitize_text(
                        f"pipeline {newest.number} workflow {name} "
                        f"{workflow.status}"
                    )
                )
                try:
                    jobs = self._client.jobs_for_workflow(workflow.id)
                except CircleCiError:
                    # Incomplete evidence must never suppress a terminal
                    # failure verdict from the complete workflow snapshot.
                    evidence.append("job details unavailable")
                    continue
                for job in jobs:
                    if job.status in _JOB_FAILURE:
                        evidence.append(
                            sanitize_text(
                                f"job {sanitize_text(job.name, 120)} "
                                f"{job.status}"
                            )
                        )
            return CiCheck(
                outcome="failure",
                reason=sanitize_text(
                    f"workflow {sanitize_text(failed[0].name, 120)} "
                    f"{failed[0].status}"
                ),
                evidence=tuple(evidence),
            )
        if all(w.status in WORKFLOW_SUCCESS for w in workflows):
            return CiCheck(outcome="success", reason="all workflows succeeded")
        pending = next(w for w in workflows if w.status not in WORKFLOW_SUCCESS)
        return CiCheck(
            outcome="nonterminal",
            reason=sanitize_text(
                f"workflow {sanitize_text(pending.name, 120)} {pending.status}"
            ),
        )


# Credential-shaped key/value content that must never be persisted or
# displayed, however it arrives inside remote-controlled names or reasons.
# An Authorization value is opaque: schemes are multi-token and cannot be
# enumerated or parsed safely (Digest carries realm/nonce/response pairs,
# custom schemes carry arbitrary payloads), so everything from the
# Authorization key through the end of the untrusted field is redacted.
# Callers therefore sanitize each remote-controlled name as its own field
# before composing trusted context around it.
_AUTHORIZATION_PATTERN = re.compile(r"(?i)\bauthorization\b\s*[:=].*")
# Non-Authorization credential keys carry single-token values; a leading
# scheme word (e.g. "token: Bearer x") is consumed with its payload.
_AUTH_SCHEMES = (
    "basic|bearer|concealed|digest|dpop|gnap|hoba|mutual|negotiate|ntlm|"
    "oauth|privatetoken|scram-sha-1|scram-sha-256|vapid|aws4-hmac-sha256"
)
_SECRET_PATTERN = re.compile(
    r"(?i)(?:\b(?:circle-token|x-api-key|api[-_]?key|token|"
    r"secret|password|passwd)\b\s*[:=]\s*(?:(?:" + _AUTH_SCHEMES + r")\s+)?"
    r"\S+|\bbearer\s+\S+)"
)


def sanitize_text(value: str, limit: int = 200) -> str:
    """Normalize untrusted text to bounded, printable, credential-free text.

    Control characters (including newlines and tabs) become spaces, runs of
    whitespace collapse, credential-shaped content is redacted, and the
    result is truncated to ``limit``. An Authorization field is treated as
    opaque: everything from the key through the end of this field is
    redacted, because scheme payloads (Basic, Digest parameter lists,
    custom schemes) cannot be parsed safely. Other credential keys
    (Circle-Token, token/secret/password/API-key forms, bare bearer; case
    insensitive) redact their single value. Used for every CI-provided
    name or reason before it enters persisted evidence or any display
    path; callers compose trusted context around already-sanitized fields
    so safe actionable names and statuses are kept.
    """

    cleaned = "".join(ch if ch.isprintable() else " " for ch in value)
    cleaned = " ".join(cleaned.split())
    cleaned = _AUTHORIZATION_PATTERN.sub("[redacted]", cleaned)
    cleaned = _SECRET_PATTERN.sub("[redacted]", cleaned)
    if len(cleaned) > limit:
        cleaned = cleaned[: limit - 1] + "…"
    return cleaned or "unavailable"


def _parse_page(
    response: CircleCiResponse, label: str
) -> tuple[list[Any], str | None]:
    if response.status != 200:
        raise CircleCiError(
            f"CircleCI {label} returned status {response.status}"
        )
    payload = response.payload
    if not isinstance(payload, dict) or not isinstance(
        payload.get("items"), list
    ):
        raise CircleCiError(f"CircleCI {label} payload is malformed")
    token = payload.get("next_page_token")
    if token is not None and not isinstance(token, str):
        raise CircleCiError(f"CircleCI {label} page token is malformed")
    return payload["items"], token or None


def _parse_pipeline(item: Any, *, expected_branch: str) -> Pipeline:
    if not isinstance(item, dict):
        raise CircleCiError("pipeline entry is not an object")
    pipeline_id = item.get("id")
    if not isinstance(pipeline_id, str) or _UUID_PATTERN.match(pipeline_id) is None:
        raise CircleCiError("pipeline id is invalid")
    number = item.get("number")
    if not isinstance(number, int) or isinstance(number, bool) or number < 1:
        raise CircleCiError("pipeline number is invalid")
    state = item.get("state")
    if state not in PIPELINE_STATES:
        raise CircleCiError(f"pipeline state {state!r} is invalid")
    created_at = item.get("created_at")
    if not isinstance(created_at, str) or not created_at:
        raise CircleCiError("pipeline created_at is invalid")
    vcs = item.get("vcs")
    if not isinstance(vcs, dict):
        raise CircleCiError("pipeline vcs is invalid")
    revision = vcs.get("revision")
    if not isinstance(revision, str) or _SHA_PATTERN.match(revision) is None:
        raise CircleCiError("pipeline revision is invalid")
    branch = vcs.get("branch")
    if branch != expected_branch:
        raise CircleCiError(
            "stale CircleCI response: pipeline branch does not match the query"
        )
    return Pipeline(
        id=pipeline_id,
        number=number,
        state=state,
        created_at=created_at,
        revision=revision,
        branch=branch,
    )


def _parse_workflow(item: Any, *, expected_pipeline: str) -> Workflow:
    if not isinstance(item, dict):
        raise CircleCiError("workflow entry is not an object")
    workflow_id = item.get("id")
    if not isinstance(workflow_id, str) or _UUID_PATTERN.match(workflow_id) is None:
        raise CircleCiError("workflow id is invalid")
    name = item.get("name")
    if not isinstance(name, str) or not name:
        raise CircleCiError("workflow name is invalid")
    status = item.get("status")
    if status not in WORKFLOW_STATUSES:
        raise CircleCiError(f"workflow status {status!r} is invalid")
    if item.get("pipeline_id") != expected_pipeline:
        raise CircleCiError(
            "stale CircleCI response: workflow belongs to another pipeline"
        )
    return Workflow(
        id=workflow_id,
        name=name,
        status=status,
        pipeline_id=expected_pipeline,
    )


def _parse_job(item: Any) -> Job:
    if not isinstance(item, dict):
        raise CircleCiError("job entry is not an object")
    name = item.get("name")
    if not isinstance(name, str) or not name:
        raise CircleCiError("job name is invalid")
    status = item.get("status")
    if not isinstance(status, str) or not status:
        raise CircleCiError("job status is invalid")
    job_number = item.get("job_number")
    if job_number is not None and (
        not isinstance(job_number, int) or isinstance(job_number, bool)
    ):
        raise CircleCiError("job number is invalid")
    return Job(name=name, status=status, job_number=job_number)


def _require_slug(value: str) -> None:
    if not isinstance(value, str) or _SLUG_PATTERN.match(value) is None:
        raise CircleCiError("project slug must be a safe vcs/owner/name id")


def _require_uuid(value: str, label: str) -> None:
    if not isinstance(value, str) or _UUID_PATTERN.match(value) is None:
        raise CircleCiError(f"{label} must be a UUID")


def _require_sha(value: str) -> None:
    if not isinstance(value, str) or _SHA_PATTERN.match(value) is None:
        raise CircleCiError("revision must be a full lowercase hexadecimal SHA")


def _require_branch(value: str) -> None:
    if (
        not isinstance(value, str)
        or _BRANCH_PATTERN.match(value) is None
        or ".." in value
    ):
        raise CircleCiError("branch contains unsafe characters")
