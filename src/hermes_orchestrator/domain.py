"""Shared immutable domain records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class IssueState(StrEnum):
    """Private issue lifecycle states."""

    QUEUED = "queued"
    IN_DEVELOPMENT = "in_development"
    REVIEW = "review"
    QA = "qa"
    DONE = "done"
    PAUSED = "paused"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class AdmissionRequest:
    """An operator's explicit request to add one issue to the private queue."""

    issue_id: str
    project_key: str
    linear_priority: int
    admitted_by: str
    instruction_id: str
    dependency_ready: bool = True
    overlap_risk: int = 0


@dataclass(frozen=True, slots=True)
class QueuedIssue:
    """The durable queue representation of an admitted issue."""

    issue_id: str
    project_key: str
    linear_priority: int
    state: IssueState
    instruction_id: str
    dependency_ready: bool
    overlap_risk: int
    admitted_at: datetime
    updated_at: datetime
