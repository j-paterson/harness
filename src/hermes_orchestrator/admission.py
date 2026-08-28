"""Calibrated green, yellow, and red admission (INFRA-168).

Versioned numeric thresholds in ``config/policies.yaml`` are the only
source of truth. Green admits work; yellow closes lower-priority admission
without stopping active work; red additionally requests exactly one
checkpoint at a time from the lowest-priority checkpoint-safe worker and
continues only while a fresh sample still reads red. Levels worsen
immediately and relax only after a configured number of consecutive
calmer samples so green/yellow never flap. Nothing here terminates a
process: termination requires a checkpoint id and the process registry.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from hermes_orchestrator.config import PolicyConfig
from hermes_orchestrator.domain import QueuedIssue
from hermes_orchestrator.resources import PressureLevel, ResourceSnapshot

_GIB = 1024**3
_ORDER = {
    PressureLevel.UNKNOWN: 3,
    PressureLevel.RED: 2,
    PressureLevel.YELLOW: 1,
    PressureLevel.GREEN: 0,
}
YELLOW_ADMITS_PRIORITY_AT_MOST = 1


@dataclass(frozen=True, slots=True)
class PressureDecision:
    """One explainable classification of the host."""

    level: PressureLevel
    can_admit: bool
    admission_max_priority: int | None
    reasons: tuple[str, ...]
    evidence: dict[str, Any] = field(default_factory=dict)
    raw_level: PressureLevel | None = None


@dataclass(frozen=True, slots=True)
class WorkerState:
    """The evidence the controller needs about one active worker."""

    worker_id: str
    project_key: str
    priority: int
    checkpoint_safe: bool
    dependency_critical: bool = False
    recent_progress: float = 0.0
    rss_bytes: int = 0
    evidence: Any = None


@dataclass(frozen=True, slots=True)
class ResourceAction:
    """One prioritized, explainable resource action."""

    kind: str  # stop_admission | request_checkpoint | pause_worker
    target_id: str | None
    reason: str
    evidence: dict[str, Any] = field(default_factory=dict)
    request_id: str | None = None


class PressureClassifier:
    """Threshold evaluation with worsen-immediately / relax-slowly hysteresis."""

    def __init__(
        self,
        policy: PolicyConfig,
        *,
        memory_pressure: Callable[[], str | None] | None = None,
    ) -> None:
        self._policy = policy
        self._memory_pressure = memory_pressure or (lambda: None)
        self._held: PressureLevel | None = None
        self._calmer_streak = 0

    @property
    def calibrated(self) -> bool:
        thresholds = self._policy.resource_thresholds
        return bool(thresholds.calibrated) and None not in (
            thresholds.yellow_available_memory_gib,
            thresholds.red_available_memory_gib,
            thresholds.yellow_available_disk_gib,
            thresholds.red_available_disk_gib,
        )

    def classify(
        self,
        snapshot: ResourceSnapshot,
        *,
        previous: ResourceSnapshot | None = None,
    ) -> PressureDecision:
        """Stateless classification of one sample against the thresholds."""

        thresholds = self._policy.resource_thresholds
        if not self.calibrated:
            return PressureDecision(
                PressureLevel.UNKNOWN,
                False,
                None,
                ("resource thresholds are not calibrated",),
                {},
                PressureLevel.UNKNOWN,
            )
        memory_gib = snapshot.available_memory_bytes / _GIB
        disk_gib = (
            min(snapshot.disk_free_bytes.values()) / _GIB
            if snapshot.disk_free_bytes
            else float("inf")
        )
        evidence: dict[str, Any] = {
            "available_memory_gib": round(memory_gib, 3),
            "min_disk_free_gib": (
                None if disk_gib == float("inf") else round(disk_gib, 3)
            ),
            "load_one": snapshot.load_one,
            "logical_cpus": snapshot.logical_cpus,
            "managed_rss_bytes": snapshot.managed_rss_bytes,
        }
        red: list[str] = []
        yellow: list[str] = []
        assert thresholds.red_available_memory_gib is not None
        assert thresholds.yellow_available_memory_gib is not None
        assert thresholds.red_available_disk_gib is not None
        assert thresholds.yellow_available_disk_gib is not None
        if memory_gib <= thresholds.red_available_memory_gib:
            red.append("available memory at or below the red floor")
        if disk_gib <= thresholds.red_available_disk_gib:
            red.append("free disk at or below the red floor")
        pressure = snapshot.memory_pressure or self._memory_pressure()
        evidence["memory_pressure"] = pressure
        if pressure == "critical":
            red.append("macOS memory pressure is critical")
        if memory_gib <= thresholds.yellow_available_memory_gib:
            yellow.append("available memory at or below the yellow floor")
        if disk_gib <= thresholds.yellow_available_disk_gib:
            yellow.append("free disk at or below the yellow floor")
        if pressure == "warn":
            yellow.append("macOS memory pressure is warning")
        rate = thresholds.yellow_swap_growth_gib_per_hour
        if rate is not None and previous is not None:
            hours = (snapshot.sampled_at - previous.sampled_at).total_seconds() / 3600
            if hours > 0:
                growth = (snapshot.swap_used_bytes - previous.swap_used_bytes) / _GIB
                growth_rate = growth / hours
                evidence["swap_growth_gib_per_hour"] = round(growth_rate, 3)
                if growth_rate > rate:
                    yellow.append("swap growth exceeds the configured rate")
        ratio = thresholds.yellow_load_ratio
        if ratio is not None and snapshot.logical_cpus > 0:
            load_ratio = snapshot.load_one / snapshot.logical_cpus
            evidence["load_ratio"] = round(load_ratio, 3)
            if load_ratio > ratio:
                yellow.append("sustained load exceeds the configured CPU ratio")
        if red:
            return PressureDecision(
                PressureLevel.RED, False, None, tuple(red + yellow), evidence,
                PressureLevel.RED,
            )
        if yellow:
            return PressureDecision(
                PressureLevel.YELLOW,
                False,
                YELLOW_ADMITS_PRIORITY_AT_MOST,
                tuple(yellow),
                evidence,
                PressureLevel.YELLOW,
            )
        return PressureDecision(
            PressureLevel.GREEN, True, 4, ("all thresholds clear",), evidence,
            PressureLevel.GREEN,
        )

    def observe(
        self,
        snapshot: ResourceSnapshot,
        *,
        previous: ResourceSnapshot | None = None,
    ) -> PressureDecision:
        """Stateful classification: worsen at once, relax after N calm samples."""

        raw = self.classify(snapshot, previous=previous)
        required = self._policy.resource_thresholds.hysteresis_samples
        held = self._held
        if held is None or _ORDER[raw.level] >= _ORDER[held]:
            self._held = raw.level
            self._calmer_streak = 0
            return raw
        self._calmer_streak += 1
        if self._calmer_streak >= required:
            self._held = raw.level
            self._calmer_streak = 0
            return raw
        return PressureDecision(
            level=held,
            can_admit=held is PressureLevel.GREEN,
            admission_max_priority=(
                4
                if held is PressureLevel.GREEN
                else YELLOW_ADMITS_PRIORITY_AT_MOST
                if held is PressureLevel.YELLOW
                else None
            ),
            reasons=(
                f"holding {held.value}: {self._calmer_streak}/{required} calmer "
                "samples observed",
                *raw.reasons,
            ),
            evidence=raw.evidence,
            raw_level=raw.level,
        )


class AdmissionController:
    """Turn a decision into prioritized, one-at-a-time resource actions."""

    def __init__(self, classifier: PressureClassifier) -> None:
        self._classifier = classifier

    def evaluate(
        self,
        queue: Sequence[QueuedIssue],
        workers: Sequence[WorkerState],
        snapshot: ResourceSnapshot,
        *,
        previous: ResourceSnapshot | None = None,
        decision: PressureDecision | None = None,
    ) -> list[ResourceAction]:
        decision = decision or self._classifier.observe(snapshot, previous=previous)
        if decision.level is PressureLevel.GREEN:
            return []
        actions: list[ResourceAction] = []
        held = [
            issue.issue_id
            for issue in queue
            if decision.admission_max_priority is None
            or issue.linear_priority > decision.admission_max_priority
        ]
        actions.append(
            ResourceAction(
                kind="stop_admission",
                target_id=None,
                reason=(
                    "yellow: lower-priority admission closed; active work continues"
                    if decision.level is PressureLevel.YELLOW
                    else f"{decision.level.value}: admission closed"
                ),
                evidence={
                    "level": decision.level.value,
                    "reasons": list(decision.reasons),
                    "held_issues": held,
                    "admission_max_priority": decision.admission_max_priority,
                    **decision.evidence,
                },
            )
        )
        if decision.level is not PressureLevel.RED:
            return actions
        safe = sorted(
            (worker for worker in workers if worker.checkpoint_safe),
            key=lambda worker: (
                -worker.priority,
                worker.dependency_critical,
                worker.recent_progress,
                -worker.rss_bytes,
            ),
        )
        if not safe:
            actions.append(
                ResourceAction(
                    kind="pause_worker",
                    target_id=None,
                    reason="red: no checkpoint-safe worker; nothing paused",
                    evidence={"workers": [worker.worker_id for worker in workers]},
                )
            )
            return actions
        candidate = safe[0]
        actions.append(
            ResourceAction(
                kind="request_checkpoint",
                target_id=candidate.worker_id,
                reason=(
                    "red: checkpoint the lowest-priority safe worker, one at a "
                    "time; re-sample before any further request"
                ),
                evidence={
                    "project_key": candidate.project_key,
                    "priority": candidate.priority,
                    "dependency_critical": candidate.dependency_critical,
                    "recent_progress": candidate.recent_progress,
                    "rss_bytes": candidate.rss_bytes,
                    "candidates": [worker.worker_id for worker in safe],
                },
            )
        )
        return actions
