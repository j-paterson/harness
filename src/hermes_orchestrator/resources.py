"""Read-only host resource sampling and pressure classification."""

from __future__ import annotations

import shutil
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

import psutil

from hermes_orchestrator.config import PolicyConfig

_GIB = 1024**3


class PressureLevel(StrEnum):
    """Resource admission band."""

    UNKNOWN = "unknown"
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"


@dataclass(frozen=True, slots=True)
class ResourceSnapshot:
    """One bounded observation of host capacity."""

    sampled_at: datetime
    pressure: PressureLevel
    can_admit: bool
    available_memory_bytes: int
    total_memory_bytes: int
    swap_used_bytes: int
    load_one: float
    logical_cpus: int
    disk_free_bytes: dict[str, int]
    managed_rss_bytes: int
    memory_pressure: str | None = None
    admission_max_priority: int | None = None
    pressure_reasons: tuple[str, ...] = ()


class PsutilLike(Protocol):
    def virtual_memory(self) -> Any: ...

    def swap_memory(self) -> Any: ...

    def getloadavg(self) -> tuple[float, float, float]: ...

    def cpu_count(self, logical: bool = True) -> int | None: ...


class DiskUsageLike(Protocol):
    free: int


def _utc_now() -> datetime:
    return datetime.now(UTC)


class ResourceSampler:
    """Collect host metrics without changing processes or repositories."""

    def __init__(
        self,
        psutil_module: PsutilLike = psutil,
        policy: PolicyConfig | None = None,
        repository_paths: Mapping[str, Path] | None = None,
        disk_usage: Callable[[Path], DiskUsageLike] = shutil.disk_usage,
        managed_rss: Callable[[], int] = lambda: 0,
        now: Callable[[], datetime] = _utc_now,
        memory_pressure: Callable[[], str | None] | None = None,
        classifier: Any | None = None,
    ) -> None:
        self._psutil = psutil_module
        self._policy = policy or PolicyConfig()
        self._repository_paths = dict(repository_paths or {})
        self._disk_usage = disk_usage
        self._managed_rss = managed_rss
        self._now = now
        self._memory_pressure = memory_pressure or (lambda: None)
        # The calibrated classifier (admission.PressureClassifier) owns the
        # bands and hysteresis; the sampler only measures.
        self._classifier = classifier
        self._previous: ResourceSnapshot | None = None

    def sample(self) -> ResourceSnapshot:
        """Read current resource measurements and classify admission pressure."""

        memory = self._psutil.virtual_memory()
        swap = self._psutil.swap_memory()
        disk_free = {
            alias: int(self._disk_usage(path).free)
            for alias, path in self._repository_paths.items()
        }
        measured = ResourceSnapshot(
            sampled_at=self._now().astimezone(UTC),
            pressure=PressureLevel.UNKNOWN,
            can_admit=False,
            available_memory_bytes=int(memory.available),
            total_memory_bytes=int(memory.total),
            swap_used_bytes=int(swap.used),
            load_one=float(self._psutil.getloadavg()[0]),
            logical_cpus=int(self._psutil.cpu_count(logical=True) or 1),
            disk_free_bytes=disk_free,
            managed_rss_bytes=int(self._managed_rss()),
            memory_pressure=self._memory_pressure(),
        )
        if self._classifier is not None:
            decision = self._classifier.observe(measured, previous=self._previous)
            snapshot = replace(
                measured,
                pressure=decision.level,
                can_admit=decision.can_admit,
                admission_max_priority=decision.admission_max_priority,
                pressure_reasons=tuple(decision.reasons),
            )
        else:
            pressure = self._classify(int(memory.available), disk_free)
            snapshot = replace(
                measured,
                pressure=pressure,
                can_admit=pressure is PressureLevel.GREEN,
                admission_max_priority=4 if pressure is PressureLevel.GREEN else None,
            )
        self._previous = measured
        return snapshot

    def _classify(
        self,
        available_memory_bytes: int,
        disk_free_bytes: Mapping[str, int],
    ) -> PressureLevel:
        thresholds = self._policy.resource_thresholds
        values = (
            thresholds.yellow_available_memory_gib,
            thresholds.red_available_memory_gib,
            thresholds.yellow_available_disk_gib,
            thresholds.red_available_disk_gib,
        )
        if not thresholds.calibrated or any(value is None for value in values):
            return PressureLevel.UNKNOWN

        available_memory_gib = available_memory_bytes / _GIB
        minimum_disk_gib = (
            min(disk_free_bytes.values()) / _GIB
            if disk_free_bytes
            else float("inf")
        )
        if (
            available_memory_gib <= thresholds.red_available_memory_gib
            or minimum_disk_gib <= thresholds.red_available_disk_gib
        ):
            return PressureLevel.RED
        if (
            available_memory_gib <= thresholds.yellow_available_memory_gib
            or minimum_disk_gib <= thresholds.yellow_available_disk_gib
        ):
            return PressureLevel.YELLOW
        return PressureLevel.GREEN
