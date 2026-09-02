"""Read-only host resource sampling and pressure classification."""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

import psutil

from hermes_orchestrator.config import PolicyConfig

_GIB = 1024**3

# INFRA-199 Finding 2: resource_samples persist up to
# ``PolicyConfig.resource_sample_retention_hours`` (24h by default) so an
# obsolete green row could otherwise authorize new work long after the
# host state it described has changed. The daemon's tick cadence is tens
# of seconds (``supervisor.Supervisor``'s default ``interval_seconds``),
# so a small multiple of that — minutes, not hours — is the right MVP
# bound: tight enough that a stalled sampler cannot silently keep
# admitting work, loose enough to absorb one or two missed ticks. A
# small, fixed clock-skew allowance (not policy-configurable — it is an
# implementation tolerance, not an operator knob) guards the same check
# against rejecting a genuinely fresh sample over sub-minute clock jitter
# between the sampling process and the reading process.
DEFAULT_RESOURCE_SAMPLE_CLOCK_SKEW = timedelta(seconds=60)


class SampleExecutor(Protocol):
    """The minimal read surface ``read_fresh_sample`` needs.

    Both :class:`~hermes_orchestrator.db.Database` and a raw
    ``sqlite3.Connection`` (as handed to callers inside
    ``Database.transaction()``) satisfy this, so the same freshness
    check can run outside a transaction (an initial snapshot read) and
    again, on the very same connection, inside the atomic transaction
    that consumes it.
    """

    def execute(self, sql: str, parameters: tuple[Any, ...] = ()) -> Any: ...


@dataclass(frozen=True, slots=True)
class FreshSample:
    """One resource sample proven fresh enough to authorize dispatch."""

    pressure: str
    sampled_at: datetime


def read_fresh_sample(
    executor: SampleExecutor,
    *,
    now: datetime,
    max_age: timedelta,
    clock_skew: timedelta = DEFAULT_RESOURCE_SAMPLE_CLOCK_SKEW,
) -> FreshSample | None:
    """Read the newest ``resource_samples`` row, or ``None`` when it
    cannot be trusted to authorize new work right now.

    Fails closed (returns ``None``) when the sample is absent, its
    ``sampled_at`` is unparseable, it is dated more than ``clock_skew``
    into the future, or it is older than ``max_age``. Every one of
    those is a caller signal to refuse dispatch, never to guess at
    current capacity from evidence it cannot trust.
    """

    row = executor.execute(
        "SELECT pressure, sampled_at FROM resource_samples "
        "ORDER BY sampled_at DESC LIMIT 1",
    ).fetchone()
    if row is None:
        return None
    try:
        sampled_at = datetime.fromisoformat(str(row["sampled_at"]))
    except ValueError:
        return None
    sampled_at = (
        sampled_at.replace(tzinfo=UTC)
        if sampled_at.tzinfo is None
        else sampled_at.astimezone(UTC)
    )
    if sampled_at - now > clock_skew:
        return None
    if now - sampled_at > max_age:
        return None
    return FreshSample(pressure=str(row["pressure"]), sampled_at=sampled_at)


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


def bound_process_rss(
    database: SampleExecutor,
    *,
    process_iter: Callable[[], Iterable[Any]] = psutil.process_iter,
    daemon_pid: int | None = None,
) -> int:
    """Measure the daemon and Claude trees bound to current project cells."""

    active_states = ("starting", "active", "handoff_required", "paused")
    placeholders = ",".join("?" for _ in active_states)
    rows = database.execute(
        f"SELECT session_id FROM project_cells WHERE state IN ({placeholders}) "
        "AND session_id IS NOT NULL",
        active_states,
    ).fetchall()
    sessions = {str(row["session_id"]) for row in rows}
    daemon_pid = os.getpid() if daemon_pid is None else daemon_pid
    try:
        processes = tuple(process_iter())
    except (OSError, psutil.Error):
        return 0

    roots: list[Any] = []
    for process in processes:
        try:
            command = tuple(str(part) for part in process.cmdline())
            is_bound_lead = any(
                command[index] in {"--resume", "--session-id"}
                and command[index + 1] in sessions
                for index in range(len(command) - 1)
            )
            if int(process.pid) == daemon_pid or is_bound_lead:
                roots.append(process)
        except (OSError, psutil.Error):
            continue

    total = 0
    seen: set[int] = set()
    for root in roots:
        try:
            members = (root, *root.children(recursive=True))
        except (OSError, psutil.Error):
            members = (root,)
        for member in members:
            try:
                pid = int(member.pid)
                if pid not in seen:
                    total += int(member.memory_info().rss)
                    seen.add(pid)
            except (OSError, psutil.Error):
                continue
    return total


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

    def _free_bytes(self, path: Path) -> int:
        """Free bytes at one sampling target, degrading fail-closed.

        A leased worktree that cleanup removed mid-flight must not
        crash the long-running daemon's tick; the vanished target
        reports zero free bytes, so calibrated admission closes instead
        of admitting on missing evidence.
        """

        try:
            return int(self._disk_usage(path).free)
        except OSError:
            return 0

    def sample(self) -> ResourceSnapshot:
        """Read current resource measurements and classify admission pressure."""

        memory = self._psutil.virtual_memory()
        swap = self._psutil.swap_memory()
        disk_free = {
            alias: self._free_bytes(path)
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
