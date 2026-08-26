"""External-system protocols and deterministic recording fakes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class LinearPort(Protocol):
    """Minimal workflow projection boundary."""

    def project(self, issue_id: str, status: str, assignee: str) -> dict[str, Any]: ...


class WorkerPort(Protocol):
    """Project-cell worker boundary."""

    def start_project_cell(self, project_key: str, issue_id: str) -> dict[str, Any]: ...


class ReviewPort(Protocol):
    """Independent review boundary."""

    def submit(self, project_key: str, issue_id: str, sha: str) -> dict[str, Any]: ...


class SourceControlPort(Protocol):
    """Source-control read and merge boundary."""

    def merge(
        self,
        repository: str,
        pull_request: int,
        expected_sha: str,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class AdapterCall:
    """One call captured by a recording fake."""

    operation: str
    arguments: dict[str, Any]


class _RecordingFake:
    def __init__(self) -> None:
        self.calls: list[AdapterCall] = []
        self._next_failure: BaseException | None = None

    def fail_next(self, error: BaseException) -> None:
        self._next_failure = error

    def _record(self, operation: str, **arguments: Any) -> None:
        self.calls.append(AdapterCall(operation, arguments))
        if self._next_failure is not None:
            failure = self._next_failure
            self._next_failure = None
            raise failure


class FakeLinear(_RecordingFake):
    """Recording Linear boundary for domain tests."""

    def project(self, issue_id: str, status: str, assignee: str) -> dict[str, Any]:
        self._record(
            "project",
            issue_id=issue_id,
            status=status,
            assignee=assignee,
        )
        return {"changed": True}


class FakeWorker(_RecordingFake):
    """Recording worker boundary for domain tests."""

    def start_project_cell(self, project_key: str, issue_id: str) -> dict[str, Any]:
        self._record(
            "start_project_cell",
            project_key=project_key,
            issue_id=issue_id,
        )
        return {"started": True, "project_key": project_key}


class FakeReview(_RecordingFake):
    """Recording review boundary for domain tests."""

    def submit(self, project_key: str, issue_id: str, sha: str) -> dict[str, Any]:
        self._record(
            "submit",
            project_key=project_key,
            issue_id=issue_id,
            sha=sha,
        )
        return {"submitted": True, "sha": sha}


class FakeSourceControl(_RecordingFake):
    """Recording source-control boundary for domain tests."""

    def merge(
        self,
        repository: str,
        pull_request: int,
        expected_sha: str,
    ) -> dict[str, Any]:
        self._record(
            "merge",
            repository=repository,
            pull_request=pull_request,
            expected_sha=expected_sha,
        )
        return {"merged": True, "sha": expected_sha}
