"""Phone-safe status read models with fail-closed redaction (INFRA-174).

Every view field is constructed explicitly by name from a narrow input
contract — database rows and internal dataclasses never reach a view. String
values are screened against known secret field names before rendering; a
suspect value aborts the whole summary with :class:`RedactionError` after
emitting a ``redaction_failure`` audit event that carries the view, field,
and matched term but never the value itself.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from pydantic import BaseModel, ConfigDict

from hermes_orchestrator.events import EventInput


class ResourceBand(StrEnum):
    """Closed phone-safe resource pressure vocabulary."""

    UNKNOWN = "unknown"
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"


class ContextBand(StrEnum):
    """Closed phone-safe context pressure vocabulary."""

    UNKNOWN = "unknown"
    OK = "ok"
    PREPARE = "prepare"
    ROTATE = "rotate"


# Known secret field names; matching is case-insensitive substring over every
# string value. A false positive fails closed by design. The first six terms
# are the plan-mandated floor; the rest cover credential vocabulary and the
# real Keychain service names this host uses.
FORBIDDEN_VALUE_TERMS: tuple[str, ...] = (
    "email",
    "phone",
    "token",
    "secret",
    "raw_output",
    "environment",
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


class RedactionError(RuntimeError):
    """A value carried a known secret field name; the view was not rendered."""


class AuditEventSink(Protocol):
    """Destination for audit events, bound to the event store by the wiring."""

    def append(self, event: EventInput) -> None: ...


class _StrictView(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ProjectView(_StrictView):
    project_alias: str
    state: str
    age_seconds: int


class WorkerView(_StrictView):
    worker_alias: str
    profile_alias: str
    project_alias: str
    state: str
    age_seconds: int
    context_band: ContextBand


class QueueItemView(_StrictView):
    issue_alias: str
    project_alias: str
    state: str
    age_seconds: int


class ResourceView(_StrictView):
    band: ResourceBand
    available_memory_gib: float
    total_memory_gib: float
    load_one: float
    logical_cpus: int
    disk_free_gib: float


class ReviewView(_StrictView):
    issue_alias: str
    project_alias: str
    state: str
    pr_url: str
    ci_status: str
    age_seconds: int


class StallView(_StrictView):
    worker_alias: str
    project_alias: str
    stall_summary: str
    age_seconds: int
    checkpoint_eligible: bool


class OperationsSummary(_StrictView):
    projects: tuple[ProjectView, ...]
    workers: tuple[WorkerView, ...]
    queue: tuple[QueueItemView, ...]
    resources: ResourceView | None
    reviews: tuple[ReviewView, ...]
    stalls: tuple[StallView, ...]


@dataclass(frozen=True, slots=True)
class ProjectStatusInput:
    project_alias: str
    state: str
    age_seconds: int


@dataclass(frozen=True, slots=True)
class WorkerStatusInput:
    worker_alias: str
    profile_alias: str
    project_alias: str
    state: str
    age_seconds: int
    context_band: ContextBand


@dataclass(frozen=True, slots=True)
class QueueItemInput:
    issue_alias: str
    project_alias: str
    state: str
    age_seconds: int


@dataclass(frozen=True, slots=True)
class ResourceInput:
    band: ResourceBand
    available_memory_gib: float
    total_memory_gib: float
    load_one: float
    logical_cpus: int
    disk_free_gib: float


@dataclass(frozen=True, slots=True)
class ReviewInput:
    issue_alias: str
    project_alias: str
    state: str
    pr_url: str
    ci_status: str
    age_seconds: int


@dataclass(frozen=True, slots=True)
class StallInput:
    worker_alias: str
    project_alias: str
    stall_summary: str
    age_seconds: int
    checkpoint_eligible: bool


class StatusSource(Protocol):
    """Narrow input contract the wiring adapts real stores onto."""

    def projects(self) -> Sequence[ProjectStatusInput]: ...

    def workers(self) -> Sequence[WorkerStatusInput]: ...

    def queue_items(self) -> Sequence[QueueItemInput]: ...

    def resources(self) -> ResourceInput | None: ...

    def reviews(self) -> Sequence[ReviewInput]: ...

    def stalls(self) -> Sequence[StallInput]: ...


class StatusViewService:
    """Assemble the phone-safe operations summary field by field."""

    def __init__(self, source: StatusSource, audit: AuditEventSink) -> None:
        self._source = source
        self._audit = audit

    def summary(self) -> OperationsSummary:
        return OperationsSummary(
            projects=tuple(
                ProjectView(
                    project_alias=self._guard(
                        "project", "project_alias", item.project_alias
                    ),
                    state=self._guard("project", "state", item.state),
                    age_seconds=item.age_seconds,
                )
                for item in self._source.projects()
            ),
            workers=tuple(
                WorkerView(
                    worker_alias=self._guard(
                        "worker", "worker_alias", item.worker_alias
                    ),
                    profile_alias=self._guard(
                        "worker", "profile_alias", item.profile_alias
                    ),
                    project_alias=self._guard(
                        "worker", "project_alias", item.project_alias
                    ),
                    state=self._guard("worker", "state", item.state),
                    age_seconds=item.age_seconds,
                    context_band=item.context_band,
                )
                for item in self._source.workers()
            ),
            queue=tuple(
                QueueItemView(
                    issue_alias=self._guard(
                        "queue_item", "issue_alias", item.issue_alias
                    ),
                    project_alias=self._guard(
                        "queue_item", "project_alias", item.project_alias
                    ),
                    state=self._guard("queue_item", "state", item.state),
                    age_seconds=item.age_seconds,
                )
                for item in self._source.queue_items()
            ),
            resources=self._resource_view(self._source.resources()),
            reviews=tuple(
                ReviewView(
                    issue_alias=self._guard("review", "issue_alias", item.issue_alias),
                    project_alias=self._guard(
                        "review", "project_alias", item.project_alias
                    ),
                    state=self._guard("review", "state", item.state),
                    pr_url=self._guard("review", "pr_url", item.pr_url),
                    ci_status=self._guard("review", "ci_status", item.ci_status),
                    age_seconds=item.age_seconds,
                )
                for item in self._source.reviews()
            ),
            stalls=tuple(
                StallView(
                    worker_alias=self._guard(
                        "stall", "worker_alias", item.worker_alias
                    ),
                    project_alias=self._guard(
                        "stall", "project_alias", item.project_alias
                    ),
                    stall_summary=self._guard(
                        "stall", "stall_summary", item.stall_summary
                    ),
                    age_seconds=item.age_seconds,
                    checkpoint_eligible=item.checkpoint_eligible,
                )
                for item in self._source.stalls()
            ),
        )

    def _resource_view(self, item: ResourceInput | None) -> ResourceView | None:
        if item is None:
            return None
        return ResourceView(
            band=item.band,
            available_memory_gib=item.available_memory_gib,
            total_memory_gib=item.total_memory_gib,
            load_one=item.load_one,
            logical_cpus=item.logical_cpus,
            disk_free_gib=item.disk_free_gib,
        )

    def _guard(self, view: str, field_name: str, value: str) -> str:
        lowered = value.lower()
        for term in FORBIDDEN_VALUE_TERMS:
            if term in lowered:
                self._audit.append(
                    EventInput(
                        event_type="redaction_failure",
                        aggregate_type="remote_view",
                        aggregate_id=view,
                        payload={
                            "view": view,
                            "field": field_name,
                            "matched_term": term,
                        },
                        actor="remote",
                    )
                )
                raise RedactionError(
                    f"{view}.{field_name} matched forbidden term {term!r}; "
                    "view not rendered"
                )
        return value
