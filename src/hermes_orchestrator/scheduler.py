"""Pure, deterministic scheduling plans."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from hermes_orchestrator.queue import QueueService


class AdmissionSnapshot(Protocol):
    """The resource fields the scheduler needs."""

    pressure: Any
    can_admit: bool


@dataclass(frozen=True, slots=True)
class PlannedAction:
    """An explainable action that may remain observation-only."""

    kind: str
    project_key: str | None
    issue_id: str | None
    reason: str
    execute: bool
    evidence: dict[str, Any]


def _utc_now() -> datetime:
    return datetime.now(UTC)


class Scheduler:
    """Rank admitted work and produce one lead-start plan per project."""

    def __init__(
        self,
        queue: QueueService,
        mode: str,
        active_projects: Iterable[str] | Callable[[], Iterable[str]] = (),
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        if mode not in {"observe", "active"}:
            raise ValueError(f"unsupported scheduler mode: {mode}")
        self._queue = queue
        self._mode = mode
        self._active_projects = (
            active_projects
            if callable(active_projects)
            else lambda: frozenset(active_projects)
        )
        self._now = now

    def plan(self, snapshot: AdmissionSnapshot) -> list[PlannedAction]:
        """Describe the next safe project-cell starts without side effects."""

        pressure = getattr(snapshot.pressure, "value", snapshot.pressure)
        max_priority = getattr(snapshot, "admission_max_priority", None)
        if max_priority is None and snapshot.can_admit:
            max_priority = 4
        if max_priority is None:
            return [
                PlannedAction(
                    kind="admission_blocked",
                    project_key=None,
                    issue_id=None,
                    reason="resource policy does not permit new work",
                    execute=False,
                    evidence={"pressure": str(pressure)},
                )
            ]

        actions: list[PlannedAction] = []
        planned_projects: set[str] = set()
        active_projects = frozenset(self._active_projects())
        held: list[str] = []
        for issue in self._queue.list_ranked(self._now()):
            if issue.project_key in planned_projects:
                continue
            if issue.linear_priority > max_priority:
                held.append(issue.issue_id)
                continue
            planned_projects.add(issue.project_key)
            active = issue.project_key in active_projects
            actions.append(
                PlannedAction(
                    kind=(
                        "resume_project_cell" if active else "start_project_cell"
                    ),
                    project_key=issue.project_key,
                    issue_id=issue.issue_id,
                    reason=(
                        "highest-ranked ready work for the active project lead"
                        if active
                        else "highest-ranked ready work for an inactive project"
                    ),
                    execute=self._mode == "active",
                    evidence={
                        "pressure": str(pressure),
                        "priority": issue.linear_priority,
                        "dependency_ready": issue.dependency_ready,
                    },
                )
            )
        if held:
            actions.append(
                PlannedAction(
                    kind="admission_limited",
                    project_key=None,
                    issue_id=None,
                    reason=(
                        f"{pressure}: only priority <= {max_priority} work is "
                        "admitted; active work continues"
                    ),
                    execute=False,
                    evidence={"pressure": str(pressure), "held_issues": held},
                )
            )
        return actions
