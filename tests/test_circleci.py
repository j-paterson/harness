"""Verify strict, bounded CircleCI reads and merge-status classification."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import pytest

from hermes_orchestrator.circleci import (
    CircleCiClient,
    CircleCiError,
    CircleCiResponse,
    CircleCiStatusAdapter,
)

SLUG = "gh/j-paterson/demo"
MERGE_SHA = "3" * 40
OTHER_SHA = "9" * 40
PIPELINE_ID = "11111111-2222-3333-4444-555555555555"
WORKFLOW_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
PIPELINE_PATH = f"/project/{SLUG}/pipeline"
WORKFLOW_PATH = f"/pipeline/{PIPELINE_ID}/workflow"
JOB_PATH = f"/workflow/{WORKFLOW_ID}/job"
TOKEN = "super-secret-token"


def pipeline_item(**overrides: Any) -> dict[str, Any]:
    item: dict[str, Any] = {
        "id": PIPELINE_ID,
        "number": 42,
        "state": "created",
        "created_at": "2026-08-28T00:00:00Z",
        "vcs": {"revision": MERGE_SHA, "branch": "main"},
    }
    item.update(overrides)
    return item


def workflow_item(**overrides: Any) -> dict[str, Any]:
    item: dict[str, Any] = {
        "id": WORKFLOW_ID,
        "name": "build-test",
        "status": "success",
        "pipeline_id": PIPELINE_ID,
    }
    item.update(overrides)
    return item


def job_item(**overrides: Any) -> dict[str, Any]:
    item: dict[str, Any] = {
        "name": "pytest",
        "status": "failed",
        "job_number": 7,
    }
    item.update(overrides)
    return item


def page(items: list[dict[str, Any]], token: str | None = None) -> dict[str, Any]:
    return {"items": items, "next_page_token": token}


@dataclass
class FakeTransport:
    """Recording transport returning queued responses per method and path."""

    responses: dict[tuple[str, str], list[CircleCiResponse]] = field(
        default_factory=dict
    )
    calls: list[tuple[str, str, Mapping[str, str] | None]] = field(
        default_factory=list
    )

    def queue(self, method: str, path: str, response: CircleCiResponse) -> None:
        self.responses.setdefault((method, path), []).append(response)

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, str] | None = None,
    ) -> CircleCiResponse:
        self.calls.append((method, path, query))
        queued = self.responses.get((method, path))
        if not queued:
            raise AssertionError(f"unexpected CircleCI call {method} {path}")
        return queued.pop(0)


@pytest.fixture
def transport() -> FakeTransport:
    return FakeTransport()


@pytest.fixture
def client(transport: FakeTransport) -> CircleCiClient:
    return CircleCiClient(transport=transport, max_pages=3)


@pytest.fixture
def adapter(client: CircleCiClient) -> CircleCiStatusAdapter:
    return CircleCiStatusAdapter(client)


def test_pipelines_for_sha_filters_exact_revision(
    client: CircleCiClient, transport: FakeTransport
) -> None:
    foreign = pipeline_item(
        id="99999999-2222-3333-4444-555555555555",
        vcs={"revision": OTHER_SHA, "branch": "main"},
    )
    transport.queue(
        "GET",
        PIPELINE_PATH,
        CircleCiResponse(200, page([foreign, pipeline_item()])),
    )
    pipelines = client.pipelines_for_sha(SLUG, branch="main", revision=MERGE_SHA)
    assert [p.id for p in pipelines] == [PIPELINE_ID]
    assert transport.calls == [("GET", PIPELINE_PATH, {"branch": "main"})]


def test_pipelines_fail_closed_when_bound_exhausts_with_continuation(
    client: CircleCiClient, transport: FakeTransport
) -> None:
    """Partial data with a remaining continuation must never look complete."""

    for _ in range(3):
        transport.queue(
            "GET", PIPELINE_PATH, CircleCiResponse(200, page([], token="more"))
        )
    with pytest.raises(CircleCiError, match="continuation"):
        client.pipelines_for_sha(SLUG, branch="main", revision=MERGE_SHA)
    assert len(transport.calls) == 3
    assert transport.calls[1][2] == {"branch": "main", "page-token": "more"}


def test_pipelines_follow_pagination_to_completion(
    client: CircleCiClient, transport: FakeTransport
) -> None:
    transport.queue(
        "GET", PIPELINE_PATH, CircleCiResponse(200, page([], token="more"))
    )
    transport.queue(
        "GET", PIPELINE_PATH, CircleCiResponse(200, page([pipeline_item()]))
    )
    pipelines = client.pipelines_for_sha(SLUG, branch="main", revision=MERGE_SHA)
    assert [p.id for p in pipelines] == [PIPELINE_ID]


def test_workflow_pagination_reveals_later_page_failure(
    adapter: CircleCiStatusAdapter, transport: FakeTransport
) -> None:
    """A failed workflow on page 2 must not be hidden by a page-1 success."""

    other_workflow = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
    transport.queue(
        "GET", PIPELINE_PATH, CircleCiResponse(200, page([pipeline_item()]))
    )
    transport.queue(
        "GET",
        WORKFLOW_PATH,
        CircleCiResponse(200, page([workflow_item()], token="w2")),
    )
    transport.queue(
        "GET",
        WORKFLOW_PATH,
        CircleCiResponse(
            200,
            page([workflow_item(id=other_workflow, name="deploy", status="failed")]),
        ),
    )
    transport.queue(
        "GET",
        f"/workflow/{other_workflow}/job",
        CircleCiResponse(200, page([job_item()])),
    )
    check = adapter.check(SLUG, branch="main", merge_sha=MERGE_SHA)
    assert check.outcome == "failure"
    assert any("deploy" in line for line in check.evidence)
    workflow_calls = [c for c in transport.calls if c[1] == WORKFLOW_PATH]
    assert workflow_calls[1][2] == {"page-token": "w2"}


def test_workflows_fail_closed_when_bound_exhausts_with_continuation(
    client: CircleCiClient, transport: FakeTransport
) -> None:
    for _ in range(3):
        transport.queue(
            "GET",
            WORKFLOW_PATH,
            CircleCiResponse(200, page([workflow_item()], token="w2")),
        )
    with pytest.raises(CircleCiError, match="continuation"):
        client.workflows_for_pipeline(PIPELINE_ID)


def test_jobs_follow_pagination_to_completion(
    client: CircleCiClient, transport: FakeTransport
) -> None:
    transport.queue(
        "GET",
        JOB_PATH,
        CircleCiResponse(200, page([job_item(status="success")], token="j2")),
    )
    transport.queue("GET", JOB_PATH, CircleCiResponse(200, page([job_item()])))
    jobs = client.jobs_for_workflow(WORKFLOW_ID)
    assert [job.status for job in jobs] == ["success", "failed"]


def test_incomplete_job_evidence_keeps_the_failure_verdict(
    adapter: CircleCiStatusAdapter, transport: FakeTransport
) -> None:
    """Truncated job evidence must never suppress a terminal failure."""

    transport.queue(
        "GET", PIPELINE_PATH, CircleCiResponse(200, page([pipeline_item()]))
    )
    transport.queue(
        "GET",
        WORKFLOW_PATH,
        CircleCiResponse(200, page([workflow_item(status="failed")])),
    )
    for _ in range(3):
        transport.queue(
            "GET", JOB_PATH, CircleCiResponse(200, page([job_item()], token="j2"))
        )
    check = adapter.check(SLUG, branch="main", merge_sha=MERGE_SHA)
    assert check.outcome == "failure"
    assert any("job details unavailable" in line for line in check.evidence)


def test_pipelines_rejects_stale_branch_echo(
    client: CircleCiClient, transport: FakeTransport
) -> None:
    stale = pipeline_item(vcs={"revision": MERGE_SHA, "branch": "develop"})
    transport.queue("GET", PIPELINE_PATH, CircleCiResponse(200, page([stale])))
    with pytest.raises(CircleCiError, match="branch"):
        client.pipelines_for_sha(SLUG, branch="main", revision=MERGE_SHA)


@pytest.mark.parametrize(
    "overrides",
    [
        {"id": "not-a-uuid"},
        {"number": 0},
        {"state": "exploded"},
        {"vcs": {"revision": "short", "branch": "main"}},
        {"vcs": None},
    ],
)
def test_pipelines_fail_closed_on_malformed_items(
    client: CircleCiClient,
    transport: FakeTransport,
    overrides: dict[str, Any],
) -> None:
    transport.queue(
        "GET", PIPELINE_PATH, CircleCiResponse(200, page([pipeline_item(**overrides)]))
    )
    with pytest.raises(CircleCiError):
        client.pipelines_for_sha(SLUG, branch="main", revision=MERGE_SHA)


def test_pipelines_rejects_non_200(
    client: CircleCiClient, transport: FakeTransport
) -> None:
    transport.queue("GET", PIPELINE_PATH, CircleCiResponse(500, {"message": "oops"}))
    with pytest.raises(CircleCiError, match="500"):
        client.pipelines_for_sha(SLUG, branch="main", revision=MERGE_SHA)


@pytest.mark.parametrize(
    "slug", ["", "gh", "gh/only-owner", "-gh/a/b", "gh/a/b/c", "gh/a b/c"]
)
def test_project_slug_is_validated_before_any_call(
    client: CircleCiClient, transport: FakeTransport, slug: str
) -> None:
    with pytest.raises(CircleCiError):
        client.pipelines_for_sha(slug, branch="main", revision=MERGE_SHA)
    assert transport.calls == []


def test_workflows_rejects_foreign_pipeline_echo(
    client: CircleCiClient, transport: FakeTransport
) -> None:
    foreign = workflow_item(pipeline_id="99999999-2222-3333-4444-555555555555")
    transport.queue("GET", WORKFLOW_PATH, CircleCiResponse(200, page([foreign])))
    with pytest.raises(CircleCiError, match="pipeline"):
        client.workflows_for_pipeline(PIPELINE_ID)


def test_workflows_reject_unknown_status(
    client: CircleCiClient, transport: FakeTransport
) -> None:
    transport.queue(
        "GET",
        WORKFLOW_PATH,
        CircleCiResponse(200, page([workflow_item(status="mystery")])),
    )
    with pytest.raises(CircleCiError, match="status"):
        client.workflows_for_pipeline(PIPELINE_ID)


def test_client_requires_positive_page_bound(transport: FakeTransport) -> None:
    with pytest.raises(ValueError, match="max_pages"):
        CircleCiClient(transport=transport, max_pages=0)


def test_adapter_success_requires_all_workflows_successful(
    adapter: CircleCiStatusAdapter, transport: FakeTransport
) -> None:
    transport.queue(
        "GET", PIPELINE_PATH, CircleCiResponse(200, page([pipeline_item()]))
    )
    transport.queue(
        "GET",
        WORKFLOW_PATH,
        CircleCiResponse(
            200,
            page(
                [
                    workflow_item(),
                    workflow_item(
                        id="bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee",
                        name="lint",
                    ),
                ]
            ),
        ),
    )
    check = adapter.check(SLUG, branch="main", merge_sha=MERGE_SHA)
    assert check.outcome == "success"


def test_adapter_failure_collects_sanitized_evidence(
    adapter: CircleCiStatusAdapter, transport: FakeTransport
) -> None:
    transport.queue(
        "GET", PIPELINE_PATH, CircleCiResponse(200, page([pipeline_item()]))
    )
    transport.queue(
        "GET",
        WORKFLOW_PATH,
        CircleCiResponse(200, page([workflow_item(status="failed")])),
    )
    transport.queue("GET", JOB_PATH, CircleCiResponse(200, page([job_item()])))
    check = adapter.check(SLUG, branch="main", merge_sha=MERGE_SHA)
    assert check.outcome == "failure"
    joined = " ".join(check.evidence)
    assert "build-test" in joined
    assert "pytest" in joined
    assert "42" in joined
    assert TOKEN not in joined


@pytest.mark.parametrize("status", ["running", "on_hold", "failing", "not_run"])
def test_adapter_nonterminal_statuses_stay_unresolved(
    adapter: CircleCiStatusAdapter, transport: FakeTransport, status: str
) -> None:
    transport.queue(
        "GET", PIPELINE_PATH, CircleCiResponse(200, page([pipeline_item()]))
    )
    transport.queue(
        "GET",
        WORKFLOW_PATH,
        CircleCiResponse(200, page([workflow_item(status=status)])),
    )
    check = adapter.check(SLUG, branch="main", merge_sha=MERGE_SHA)
    assert check.outcome == "nonterminal"


def test_adapter_missing_pipeline_is_nonterminal_not_success(
    adapter: CircleCiStatusAdapter, transport: FakeTransport
) -> None:
    transport.queue("GET", PIPELINE_PATH, CircleCiResponse(200, page([])))
    check = adapter.check(SLUG, branch="main", merge_sha=MERGE_SHA)
    assert check.outcome == "nonterminal"
    assert "no pipeline" in check.reason


def test_adapter_empty_workflow_list_is_nonterminal(
    adapter: CircleCiStatusAdapter, transport: FakeTransport
) -> None:
    transport.queue(
        "GET", PIPELINE_PATH, CircleCiResponse(200, page([pipeline_item()]))
    )
    transport.queue("GET", WORKFLOW_PATH, CircleCiResponse(200, page([])))
    check = adapter.check(SLUG, branch="main", merge_sha=MERGE_SHA)
    assert check.outcome == "nonterminal"


def test_errored_pipeline_without_workflows_is_terminal_failure(
    adapter: CircleCiStatusAdapter, transport: FakeTransport
) -> None:
    """A terminally errored pipeline is a failure, not an optimistic hold."""

    transport.queue(
        "GET",
        PIPELINE_PATH,
        CircleCiResponse(200, page([pipeline_item(state="errored")])),
    )
    check = adapter.check(SLUG, branch="main", merge_sha=MERGE_SHA)
    assert check.outcome == "failure"
    assert "errored" in check.reason
    assert all(call[1] == PIPELINE_PATH for call in transport.calls)


def test_evidence_is_single_line_bounded_printable_text(
    adapter: CircleCiStatusAdapter, transport: FakeTransport
) -> None:
    dirty_workflow = "deploy\r\n\x1b[31mevil\x00" + "x" * 400
    dirty_job = "job\nname\twith\x07controls" + "y" * 400
    transport.queue(
        "GET", PIPELINE_PATH, CircleCiResponse(200, page([pipeline_item()]))
    )
    transport.queue(
        "GET",
        WORKFLOW_PATH,
        CircleCiResponse(
            200, page([workflow_item(status="failed", name=dirty_workflow)])
        ),
    )
    transport.queue(
        "GET", JOB_PATH, CircleCiResponse(200, page([job_item(name=dirty_job)]))
    )
    check = adapter.check(SLUG, branch="main", merge_sha=MERGE_SHA)
    assert check.outcome == "failure"
    for line in (check.reason, *check.evidence):
        assert "\n" not in line and "\r" not in line and "\t" not in line
        assert "\x1b" not in line and "\x00" not in line and "\x07" not in line
        assert len(line) <= 220
        assert TOKEN not in line


def test_httpx_transport_normalizes_unavailability_without_identity() -> None:
    import httpx

    from hermes_orchestrator.circleci import HttpxCircleCiTransport

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    transport = HttpxCircleCiTransport(
        TOKEN,
        client=httpx.Client(
            transport=httpx.MockTransport(handler),
            base_url="https://circleci.com/api/v2",
        ),
    )
    with pytest.raises(CircleCiError) as excinfo:
        transport.request("GET", f"/project/{SLUG}/pipeline")
    text = str(excinfo.value)
    assert TOKEN not in text
    assert "circleci.com" not in text
    assert SLUG not in text


def test_httpx_transport_applies_timeout_to_injected_client() -> None:
    import httpx

    from hermes_orchestrator.circleci import HttpxCircleCiTransport

    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["timeout"] = request.extensions.get("timeout")
        return httpx.Response(200, json={"items": [], "next_page_token": None})

    transport = HttpxCircleCiTransport(
        TOKEN,
        client=httpx.Client(
            transport=httpx.MockTransport(handler),
            base_url="https://circleci.com/api/v2",
        ),
        timeout=7.5,
    )
    response = transport.request("GET", f"/pipeline/{PIPELINE_ID}/workflow")
    assert response.status == 200
    assert captured["timeout"]["read"] == 7.5
    assert captured["timeout"]["connect"] == 7.5


def test_adapter_uses_newest_pipeline_for_the_sha(
    adapter: CircleCiStatusAdapter, transport: FakeTransport
) -> None:
    older = pipeline_item(
        id="99999999-2222-3333-4444-555555555555",
        number=41,
        created_at="2026-08-27T00:00:00Z",
    )
    transport.queue(
        "GET",
        PIPELINE_PATH,
        CircleCiResponse(200, page([pipeline_item(), older])),
    )
    transport.queue(
        "GET", WORKFLOW_PATH, CircleCiResponse(200, page([workflow_item()]))
    )
    check = adapter.check(SLUG, branch="main", merge_sha=MERGE_SHA)
    assert check.outcome == "success"


@pytest.mark.parametrize(
    "dirty,secret",
    [
        ("Circle-Token: controlled-secret-1", "controlled-secret-1"),
        ("circle-token=controlled-secret-2", "controlled-secret-2"),
        ("bearer controlled-bare-5", "controlled-bare-5"),
        ("token: controlled-token-6", "controlled-token-6"),
        ("secret=controlled-secret-7", "controlled-secret-7"),
        ("password: controlled-pass-8", "controlled-pass-8"),
        ("api_key=controlled-key-9", "controlled-key-9"),
        ("API-KEY: controlled-key-10", "controlled-key-10"),
    ],
)
def test_sanitize_text_redacts_credential_shaped_content(
    dirty: str, secret: str
) -> None:
    from hermes_orchestrator.circleci import sanitize_text

    cleaned = sanitize_text(f"workflow deploy {dirty} failed")
    assert secret not in cleaned
    assert "[redacted]" in cleaned
    assert "deploy" in cleaned
    assert "failed" in cleaned


def test_sanitize_text_keeps_safe_actionable_names() -> None:
    from hermes_orchestrator.circleci import sanitize_text

    text = "pipeline 42 workflow build-and-test failed on job pytest-unit"
    assert sanitize_text(text) == text


def test_adapter_redacts_credentials_from_remote_names(
    adapter: CircleCiStatusAdapter, transport: FakeTransport
) -> None:
    """A remote-controlled name must never persist its secret verbatim."""

    transport.queue(
        "GET", PIPELINE_PATH, CircleCiResponse(200, page([pipeline_item()]))
    )
    transport.queue(
        "GET",
        WORKFLOW_PATH,
        CircleCiResponse(
            200,
            page(
                [
                    workflow_item(
                        status="failed",
                        name="deploy Circle-Token: controlled-secret end",
                    )
                ]
            ),
        ),
    )
    transport.queue(
        "GET",
        JOB_PATH,
        CircleCiResponse(
            200,
            page([job_item(name="job Authorization: Bearer controlled.jwt")]),
        ),
    )
    check = adapter.check(SLUG, branch="main", merge_sha=MERGE_SHA)
    assert check.outcome == "failure"
    joined = " ".join((check.reason, *check.evidence))
    assert "controlled-secret" not in joined
    assert "controlled.jwt" not in joined
    assert "[redacted]" in joined
    assert "deploy" in joined


@pytest.mark.parametrize(
    "scheme,payload",
    [
        ("Basic", "dXNlcjpwYXNz"),
        ("Bearer", "controlled.jwt.value"),
        ("Digest", "controlled-digest-response"),
        ("Negotiate", "controlled-negotiate-blob"),
        ("DPoP", "controlled-dpop-proof"),
        ("OAuth", "controlled-oauth-value"),
        ("X-Custom-Scheme", "controlled-custom-value"),
    ],
)
def test_sanitize_text_treats_authorization_values_as_opaque(
    scheme: str, payload: str
) -> None:
    """The Authorization value is opaque through the end of the field."""

    from hermes_orchestrator.circleci import sanitize_text

    cleaned = sanitize_text(
        f"job deploy Authorization: {scheme} {payload} trailing-context"
    )
    assert payload not in cleaned
    assert scheme not in cleaned
    assert "trailing-context" not in cleaned
    assert "[redacted]" in cleaned
    assert "deploy" in cleaned


def test_sanitize_text_redacts_multi_parameter_digest_authorization() -> None:
    """The reviewed reproduction: every Digest component must disappear."""

    from hermes_orchestrator.circleci import sanitize_text

    cleaned = sanitize_text(
        'job deploy Authorization: Digest username="Mufasa", realm="test", '
        'nonce="abc123nonce", response="deadbeef"'
    )
    for leaked in ("Mufasa", "realm", "abc123nonce", "deadbeef", "Digest"):
        assert leaked not in cleaned
    assert "[redacted]" in cleaned
    assert "deploy" in cleaned


def test_sanitize_text_redacts_multi_token_custom_scheme() -> None:
    from hermes_orchestrator.circleci import sanitize_text

    cleaned = sanitize_text(
        "job deploy Authorization=CustomScheme part-one part-two part-three"
    )
    for leaked in ("CustomScheme", "part-one", "part-two", "part-three"):
        assert leaked not in cleaned
    assert "[redacted]" in cleaned
    assert "deploy" in cleaned


def test_adapter_redacts_basic_authorization_payload(
    adapter: CircleCiStatusAdapter, transport: FakeTransport
) -> None:
    transport.queue(
        "GET", PIPELINE_PATH, CircleCiResponse(200, page([pipeline_item()]))
    )
    transport.queue(
        "GET",
        WORKFLOW_PATH,
        CircleCiResponse(
            200,
            page(
                [
                    workflow_item(
                        status="failed",
                        name="deploy Authorization: Basic dXNlcjpwYXNz end",
                    )
                ]
            ),
        ),
    )
    transport.queue("GET", JOB_PATH, CircleCiResponse(200, page([job_item()])))
    check = adapter.check(SLUG, branch="main", merge_sha=MERGE_SHA)
    assert check.outcome == "failure"
    joined = " ".join((check.reason, *check.evidence))
    assert "dXNlcjpwYXNz" not in joined
    assert "Basic" not in joined
    assert "[redacted]" in joined
    assert "deploy" in joined


def test_adapter_keeps_trusted_context_around_opaque_authorization(
    adapter: CircleCiStatusAdapter, transport: FakeTransport
) -> None:
    """Remote names are sanitized as their own field, so trusted pipeline
    and status context composed around them stays actionable."""

    digest_name = (
        'deploy Authorization: Digest username="Mufasa", realm="test", '
        'nonce="abc123nonce", response="deadbeef"'
    )
    transport.queue(
        "GET", PIPELINE_PATH, CircleCiResponse(200, page([pipeline_item()]))
    )
    transport.queue(
        "GET",
        WORKFLOW_PATH,
        CircleCiResponse(
            200, page([workflow_item(status="failed", name=digest_name)])
        ),
    )
    transport.queue(
        "GET",
        JOB_PATH,
        CircleCiResponse(
            200,
            page(
                [
                    job_item(
                        name="job Authorization=CustomScheme part-one part-two"
                    )
                ]
            ),
        ),
    )
    check = adapter.check(SLUG, branch="main", merge_sha=MERGE_SHA)
    assert check.outcome == "failure"
    joined = " ".join((check.reason, *check.evidence))
    for leaked in (
        "Mufasa",
        "abc123nonce",
        "deadbeef",
        "Digest",
        "CustomScheme",
        "part-one",
        "part-two",
    ):
        assert leaked not in joined
    assert "[redacted]" in joined
    assert "deploy" in joined
    # Trusted pipeline/status context composed after the sanitized remote
    # name remains actionable.
    assert "pipeline 42" in joined
    assert any(line.endswith("failed") for line in check.evidence)
