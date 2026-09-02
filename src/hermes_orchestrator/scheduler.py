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
    """An explainable action that may remain observation-only.

    INFRA-219 L1: ``lane_role`` names which lane the action plans for.
    The scheduler only ever ranks ``admitted_issues`` -- which carry no
    lane of their own -- so every action it produces is for the
    development lane today; the field exists so the plan's own
    per-cycle uniqueness (see :meth:`Scheduler.plan`) is already keyed
    by (project, lane) ahead of a lane-aware harness plan landing in
    wave 2 (INFRA-219 L2), without changing any observable output while
    no harness lane is planned.
    """

    kind: str
    project_key: str | None
    issue_id: str | None
    reason: str
    execute: bool
    evidence: dict[str, Any]
    lane_role: str | None = "development"


def _utc_now() -> datetime:
    return datetime.now(UTC)


# INFRA-219 L1: the scheduler plans only against ``admitted_issues``, which
# carries no lane of its own -- every action this module produces is for the
# development lane until a lane-aware harness plan lands in wave 2
# (INFRA-219 L2). Kept local (not imported from ``cells.py``) to keep this
# module's dependency surface unchanged.
_DEVELOPMENT_LANE = "development"


class Scheduler:
    """Rank admitted work and produce one lead-start plan per project."""

    def __init__(
        self,
        queue: QueueService,
        mode: str,
        active_projects: Iterable[str] | Callable[[], Iterable[str]] = (),
        now: Callable[[], datetime] = _utc_now,
        ready_pairs: Iterable[str] | Callable[[], Iterable[str]] | None = None,
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
        # INFRA-187 wave 2: optional so every existing caller (none of
        # which know about project-team pairs yet) keeps today's
        # behavior exactly -- a live Fable cell alone is enough to
        # resume. Only once a caller supplies this does the scheduler
        # additionally require the project's pair to be ready before
        # resuming its cell.
        self._ready_pairs = (
            None
            if ready_pairs is None
            else (
                ready_pairs
                if callable(ready_pairs)
                else lambda: frozenset(ready_pairs)
            )
        )

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
                    lane_role=None,
                )
            ]

        actions: list[PlannedAction] = []
        # INFRA-219 L1: keyed by (project, lane) -- today always paired with
        # ``_DEVELOPMENT_LANE`` since issues carry no lane of their own -- so
        # a future lane-aware harness plan for the same project can never be
        # deduplicated away by this development-lane plan, or vice versa.
        planned_projects: set[tuple[str, str]] = set()
        active_projects = frozenset(self._active_projects())
        # INFRA-187 wave 2: None (the default) keeps every existing
        # caller's behavior byte-for-byte unchanged -- the pair-ready
        # gate below only ever activates once a caller supplies
        # ``ready_pairs``.
        ready_pairs = (
            None if self._ready_pairs is None else frozenset(self._ready_pairs())
        )
        held: list[str] = []
        for issue in self._queue.list_ranked(self._now()):
            plan_key = (issue.project_key, _DEVELOPMENT_LANE)
            if plan_key in planned_projects:
                continue
            active = issue.project_key in active_projects
            not_ready = ready_pairs is not None and issue.project_key not in ready_pairs
            if active and not_ready:
                # A live Fable cell alone does not make the project's
                # execution unit complete -- consult the ready pair
                # (Fable + proven Sol) rather than inferring
                # completeness from the cell alone, and hold every
                # issue for this project until the pair is ready.
                planned_projects.add(plan_key)
                actions.append(
                    PlannedAction(
                        kind="pair_not_ready",
                        project_key=issue.project_key,
                        issue_id=issue.issue_id,
                        reason=(
                            "project pair is not ready: Fable cell live "
                            "but Sol merge lead unproven or unbound"
                        ),
                        execute=False,
                        evidence={"project_key": issue.project_key},
                        lane_role=_DEVELOPMENT_LANE,
                    )
                )
                continue
            if issue.linear_priority > max_priority:
                held.append(issue.issue_id)
                continue
            planned_projects.add(plan_key)
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
                    lane_role=_DEVELOPMENT_LANE,
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
                    lane_role=None,
                )
            )
        return actions
