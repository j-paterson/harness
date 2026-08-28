"""Phone-safe status views with fail-closed redaction (INFRA-174)."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from hermes_orchestrator.events import EventInput
from hermes_orchestrator.remote.views import (
    ContextBand,
    OperationsSummary,
    ProjectStatusInput,
    ProjectView,
    QueueItemInput,
    QueueItemView,
    RedactionError,
    ResourceBand,
    ResourceInput,
    ResourceView,
    ReviewInput,
    ReviewView,
    StallInput,
    StallView,
    StatusViewService,
    WorkerStatusInput,
    WorkerView,
)

FORBIDDEN_TERMS = (
    # Plan-mandated floor.
    "email",
    "phone",
    "token",
    "secret",
    "raw_output",
    "environment",
    # Extended local floor: credential vocabulary and real Keychain names.
    "cookie",
    "credential",
    "password",
    "passwd",
    "authorization",
    "api_key",
    "apikey",
    "keychain",
    "signing",
    "private_key",
    "bearer",
    "argv",
    "env_var",
    "hermes-orchestrator-remote",
    "hermes-orchestrator-linear",
    "hermes-orchestrator-github",
    "hermes-orchestrator-circleci",
)


# The per-project repositories the tests configure for review-link
# provenance; each alias's canonical PR URL is the only value _guard_pr_url
# may accept for that alias. Deliberately not this project's real
# repository: provenance must be config-driven, per project.
CONFIGURED_GITHUB_REPOS = {
    "project-1": "acme/widgets",
    "project-2": "beta/rockets",
}
CANONICAL_PR_URL = "https://github.com/acme/widgets/pull/19"
SECOND_CANONICAL_PR_URL = "https://github.com/beta/rockets/pull/23"


class FakeAuditSink:
    def __init__(self) -> None:
        self.events: list[EventInput] = []

    def append(self, event: EventInput) -> None:
        self.events.append(event)


class FakeStatusSource:
    def __init__(
        self,
        *,
        projects: list[ProjectStatusInput] | None = None,
        workers: list[WorkerStatusInput] | None = None,
        queue_items: list[QueueItemInput] | None = None,
        resources_input: ResourceInput | None = None,
        reviews: list[ReviewInput] | None = None,
        stalls: list[StallInput] | None = None,
    ) -> None:
        self._projects = projects or []
        self._workers = workers or []
        self._queue_items = queue_items or []
        self._resources = resources_input
        self._reviews = reviews or []
        self._stalls = stalls or []

    def projects(self) -> list[ProjectStatusInput]:
        return self._projects

    def workers(self) -> list[WorkerStatusInput]:
        return self._workers

    def queue_items(self) -> list[QueueItemInput]:
        return self._queue_items

    def resources(self) -> ResourceInput | None:
        return self._resources

    def reviews(self) -> list[ReviewInput]:
        return self._reviews

    def stalls(self) -> list[StallInput]:
        return self._stalls


def _benign_source() -> FakeStatusSource:
    return FakeStatusSource(
        projects=[
            ProjectStatusInput(
                project_alias="project-1",
                state="active",
                age_seconds=5400,
            )
        ],
        workers=[
            WorkerStatusInput(
                worker_alias="worker-1",
                profile_alias="profile-a",
                project_alias="project-1",
                state="working",
                age_seconds=1800,
                context_band=ContextBand.OK,
            )
        ],
        queue_items=[
            QueueItemInput(
                issue_alias="issue-101",
                project_alias="project-1",
                state="queued",
                age_seconds=600,
            )
        ],
        resources_input=ResourceInput(
            band=ResourceBand.GREEN,
            available_memory_gib=32.0,
            total_memory_gib=64.0,
            load_one=2.5,
            logical_cpus=10,
            disk_free_gib=120.0,
        ),
        reviews=[
            ReviewInput(
                issue_alias="issue-99",
                project_alias="project-1",
                state="awaiting_verdict",
                pr_url=CANONICAL_PR_URL,
                ci_status="none",
                age_seconds=900,
            )
        ],
        stalls=[
            StallInput(
                worker_alias="worker-2",
                project_alias="project-1",
                stall_summary="no activity for 45 minutes",
                age_seconds=2700,
                checkpoint_eligible=True,
            )
        ],
    )


def _service(
    source: FakeStatusSource | None = None,
    sink: FakeAuditSink | None = None,
) -> StatusViewService:
    return StatusViewService(
        source=source or _benign_source(),
        audit=sink or FakeAuditSink(),
        github_repos=CONFIGURED_GITHUB_REPOS,
    )


def test_summary_contains_no_forbidden_terms() -> None:
    payload = _service().summary().model_dump()
    serialized = json.dumps(payload).lower()
    for forbidden in FORBIDDEN_TERMS:
        assert forbidden not in serialized, forbidden


def test_summary_renders_benign_content() -> None:
    summary = _service().summary()
    assert summary.projects[0].project_alias == "project-1"
    assert summary.workers[0].context_band == ContextBand.OK
    assert summary.queue[0].issue_alias == "issue-101"
    assert summary.resources is not None
    assert summary.resources.band == ResourceBand.GREEN
    assert summary.reviews[0].pr_url == CANONICAL_PR_URL
    assert summary.stalls[0].checkpoint_eligible is True


def test_summary_renders_without_resources() -> None:
    source = _benign_source()
    source._resources = None
    summary = _service(source).summary()
    assert summary.resources is None


@pytest.mark.parametrize(
    ("model", "expected"),
    [
        (ProjectView, {"project_alias", "state", "age_seconds"}),
        (
            WorkerView,
            {
                "worker_alias",
                "profile_alias",
                "project_alias",
                "state",
                "age_seconds",
                "context_band",
            },
        ),
        (QueueItemView, {"issue_alias", "project_alias", "state", "age_seconds"}),
        (
            ResourceView,
            {
                "band",
                "available_memory_gib",
                "total_memory_gib",
                "load_one",
                "logical_cpus",
                "disk_free_gib",
            },
        ),
        (
            ReviewView,
            {
                "issue_alias",
                "project_alias",
                "state",
                "pr_url",
                "ci_status",
                "age_seconds",
            },
        ),
        (
            StallView,
            {
                "worker_alias",
                "project_alias",
                "stall_summary",
                "age_seconds",
                "checkpoint_eligible",
            },
        ),
        (
            OperationsSummary,
            {"projects", "workers", "queue", "resources", "reviews", "stalls"},
        ),
    ],
)
def test_view_models_expose_exactly_allowlisted_fields(model, expected) -> None:
    assert set(model.model_fields.keys()) == expected


def test_serialized_summary_has_exact_key_sets() -> None:
    payload = _service().summary().model_dump()
    assert set(payload.keys()) == {
        "projects",
        "workers",
        "queue",
        "resources",
        "reviews",
        "stalls",
    }
    assert set(payload["projects"][0].keys()) == {
        "project_alias",
        "state",
        "age_seconds",
    }
    assert set(payload["stalls"][0].keys()) == {
        "worker_alias",
        "project_alias",
        "stall_summary",
        "age_seconds",
        "checkpoint_eligible",
    }


def test_view_models_reject_unexpected_fields() -> None:
    with pytest.raises(ValidationError):
        ProjectView(
            project_alias="project-1",
            state="active",
            age_seconds=1,
            internal_note="db-row-leak",
        )
    with pytest.raises(ValidationError):
        OperationsSummary(
            projects=[],
            workers=[],
            queue=[],
            resources=None,
            reviews=[],
            stalls=[],
            debug="leak",
        )


def test_view_models_are_frozen() -> None:
    view = ProjectView(project_alias="project-1", state="active", age_seconds=1)
    with pytest.raises(ValidationError):
        view.state = "changed"


@pytest.mark.parametrize(
    "suspect",
    [
        "token=abc123DEF",
        "Bearer TOKEN abc123DEF",
        "SECRET abc123DEF",
        "session-signing abc123DEF",
        "api_key abc123DEF",
    ],
)
def test_suspect_stall_summary_fails_closed(suspect: str) -> None:
    sink = FakeAuditSink()
    source = _benign_source()
    source._stalls = [
        StallInput(
            worker_alias="worker-2",
            project_alias="project-1",
            stall_summary=suspect,
            age_seconds=2700,
            checkpoint_eligible=False,
        )
    ]
    service = _service(source, sink)
    with pytest.raises(RedactionError) as excinfo:
        service.summary()
    # The suspect value must never leak: not in the error, not in the event.
    assert "abc123DEF" not in str(excinfo.value)
    assert len(sink.events) == 1
    event = sink.events[0]
    assert event.event_type == "redaction_failure"
    assert event.aggregate_type == "remote_view"
    assert "abc123DEF" not in json.dumps(event.payload)
    assert event.payload["field"] == "stall_summary"
    assert event.payload["matched_term"]


def test_suspect_pr_url_fails_closed() -> None:
    sink = FakeAuditSink()
    source = _benign_source()
    source._reviews = [
        ReviewInput(
            issue_alias="issue-99",
            project_alias="project-1",
            state="awaiting_verdict",
            pr_url="https://github.example/org/repo/pull/19?access_token=abc123DEF",
            ci_status="none",
            age_seconds=900,
        )
    ]
    with pytest.raises(RedactionError):
        _service(source, sink).summary()
    assert [event.event_type for event in sink.events] == ["redaction_failure"]


def _review_source(
    pr_url: str, project_alias: str = "project-1"
) -> FakeStatusSource:
    source = _benign_source()
    source._reviews = [
        ReviewInput(
            issue_alias="issue-99",
            project_alias=project_alias,
            state="awaiting_verdict",
            pr_url=pr_url,
            ci_status="none",
            age_seconds=900,
        )
    ]
    return source


def test_canonical_pr_url_for_the_configured_repository_renders() -> None:
    sink = FakeAuditSink()
    summary = _service(_review_source(CANONICAL_PR_URL), sink).summary()
    assert summary.reviews[0].pr_url == CANONICAL_PR_URL
    assert sink.events == []


def test_each_alias_canonical_pr_url_renders_for_its_own_project() -> None:
    sink = FakeAuditSink()
    source = _benign_source()
    source._reviews = [
        ReviewInput(
            issue_alias="issue-99",
            project_alias="project-1",
            state="awaiting_verdict",
            pr_url=CANONICAL_PR_URL,
            ci_status="none",
            age_seconds=900,
        ),
        ReviewInput(
            issue_alias="issue-23",
            project_alias="project-2",
            state="awaiting_verdict",
            pr_url=SECOND_CANONICAL_PR_URL,
            ci_status="none",
            age_seconds=300,
        ),
    ]
    summary = _service(source, sink).summary()
    assert [review.pr_url for review in summary.reviews] == [
        CANONICAL_PR_URL,
        SECOND_CANONICAL_PR_URL,
    ]
    assert sink.events == []


@pytest.mark.parametrize(
    ("project_alias", "pr_url", "reason"),
    [
        # Cross-project: each alias carrying the OTHER project's canonical
        # URL must fail even though the URL is canonical somewhere.
        ("project-2", CANONICAL_PR_URL, "unverified_pr_link"),
        ("project-1", SECOND_CANONICAL_PR_URL, "unverified_pr_link"),
        # Unknown or unmapped aliases fail closed even with canonical-looking
        # URLs for configured repositories.
        ("project-3", CANONICAL_PR_URL, "unmapped_project"),
        ("project-3", SECOND_CANONICAL_PR_URL, "unmapped_project"),
        ("", CANONICAL_PR_URL, "unmapped_project"),
    ],
)
def test_pr_url_not_bound_to_its_project_fails_closed(
    project_alias: str, pr_url: str, reason: str
) -> None:
    sink = FakeAuditSink()
    service = _service(_review_source(pr_url, project_alias), sink)
    with pytest.raises(RedactionError) as excinfo:
        service.summary()
    # Neither the URL nor the alias leaks: not in the error, not in the event.
    assert pr_url not in str(excinfo.value)
    if project_alias:
        assert project_alias not in str(excinfo.value)
    assert len(sink.events) == 1
    event = sink.events[0]
    assert event.event_type == "redaction_failure"
    assert event.aggregate_type == "remote_view"
    assert event.payload == {
        "view": "review",
        "field": "pr_url",
        "reason": reason,
    }
    serialized = json.dumps(event.payload)
    assert pr_url not in serialized
    if project_alias:
        assert project_alias not in serialized


def test_repository_mapping_is_copied_at_construction() -> None:
    mapping = dict(CONFIGURED_GITHUB_REPOS)
    sink = FakeAuditSink()
    service = _service(_review_source(CANONICAL_PR_URL), sink)
    hostile_service = StatusViewService(
        source=_review_source(CANONICAL_PR_URL),
        audit=sink,
        github_repos=mapping,
    )
    # Mutating the caller's mapping after construction must not redirect
    # validation: the service holds its own immutable copy.
    mapping["project-1"] = "mallory/widgets"
    mapping.pop("project-2")
    assert service.summary().reviews[0].pr_url == CANONICAL_PR_URL
    assert hostile_service.summary().reviews[0].pr_url == CANONICAL_PR_URL
    assert sink.events == []


@pytest.mark.parametrize(
    "pr_url",
    [
        # Foreign and lookalike hosts.
        "https://phishing.example/acme/widgets/pull/19",
        "https://github.com.evil.test/acme/widgets/pull/19",
        "https://gíthub.com/acme/widgets/pull/19",
        "https://GitHub.com/acme/widgets/pull/19",
        # Scheme and authority tricks.
        "//github.com/acme/widgets/pull/19",
        "http://github.com/acme/widgets/pull/19",
        "HTTPS://github.com/acme/widgets/pull/19",
        "javascript:alert(1)",
        "data:text/html,unverified",
        "https://user@github.com/acme/widgets/pull/19",
        "https://github.com:443/acme/widgets/pull/19",
        "https://github.com\\acme/widgets/pull/19",
        # Wrong repository.
        "https://github.com/mallory/widgets/pull/19",
        "https://github.com/acme/gadgets/pull/19",
        # Unexpected path components.
        "https://github.com/acme/widgets/pull/19/files",
        "https://github.com/acme/widgets/pull/19/",
        "https://github.com/acme/widgets/pulls/19",
        "https://github.com/acme/widgets/pull/%31%39",
        "https://github.com/acme/widgets/pull/19?tab=diff",
        "https://github.com/acme/widgets/pull/19#top",
        # Malformed pull numbers.
        "https://github.com/acme/widgets/pull/0",
        "https://github.com/acme/widgets/pull/019",
        "https://github.com/acme/widgets/pull/-19",
        "https://github.com/acme/widgets/pull/nineteen",
        "https://github.com/acme/widgets/pull/١٩",
        "https://github.com/acme/widgets/pull/",
        # Not a URL at all.
        "not a url",
        "",
    ],
)
def test_unverified_pr_url_fails_closed_without_leaking(pr_url: str) -> None:
    sink = FakeAuditSink()
    service = _service(_review_source(pr_url), sink)
    with pytest.raises(RedactionError) as excinfo:
        service.summary()
    # The rejected value never leaks: not in the error, not in the event.
    if pr_url:
        assert pr_url not in str(excinfo.value)
    assert len(sink.events) == 1
    event = sink.events[0]
    assert event.event_type == "redaction_failure"
    assert event.aggregate_type == "remote_view"
    assert event.payload["view"] == "review"
    assert event.payload["field"] == "pr_url"
    assert event.payload["reason"] == "unverified_pr_link"
    if pr_url:
        assert pr_url not in json.dumps(event.payload)


def test_suspect_alias_fails_closed_case_insensitively() -> None:
    sink = FakeAuditSink()
    source = _benign_source()
    source._workers = [
        WorkerStatusInput(
            worker_alias="worker-PASSWORD-1",
            profile_alias="profile-a",
            project_alias="project-1",
            state="working",
            age_seconds=1800,
            context_band=ContextBand.OK,
        )
    ]
    with pytest.raises(RedactionError):
        _service(source, sink).summary()
    assert len(sink.events) == 1
    assert sink.events[0].payload["field"] == "worker_alias"


def test_band_enums_are_closed() -> None:
    assert {band.value for band in ResourceBand} == {
        "unknown",
        "green",
        "yellow",
        "red",
    }
    assert {band.value for band in ContextBand} == {
        "unknown",
        "ok",
        "prepare",
        "rotate",
    }
    with pytest.raises(ValueError):
        ResourceBand("critical")
    with pytest.raises(ValueError):
        ContextBand("high")
