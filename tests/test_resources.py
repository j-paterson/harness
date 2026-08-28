from collections import namedtuple
from pathlib import Path

from hermes_orchestrator.config import PolicyConfig
from hermes_orchestrator.resources import PressureLevel, ResourceSampler

Memory = namedtuple("Memory", "available total")
Swap = namedtuple("Swap", "used")
Disk = namedtuple("Disk", "total used free")


class FakePsutil:
    def virtual_memory(self):
        return Memory(available=8 * 1024**3, total=24 * 1024**3)

    def swap_memory(self):
        return Swap(used=3 * 1024**3)

    def getloadavg(self):
        return (2.5, 2.0, 1.5)

    def cpu_count(self, logical=True):
        assert logical is True
        return 14


def test_uncalibrated_policy_is_observe_only(tmp_path: Path) -> None:
    policy = PolicyConfig()
    sampler = ResourceSampler(
        psutil_module=FakePsutil(),
        policy=policy,
        repository_paths={"demo": tmp_path},
        disk_usage=lambda _path: Disk(100, 60, 40),
    )

    snapshot = sampler.sample()

    assert snapshot.pressure is PressureLevel.UNKNOWN
    assert snapshot.can_admit is False
    assert snapshot.available_memory_bytes == 8 * 1024**3
    assert snapshot.logical_cpus == 14
    assert snapshot.disk_free_bytes == {"demo": 40}


def test_calibrated_policy_classifies_red_memory(tmp_path: Path) -> None:
    policy = PolicyConfig.model_validate(
        {
            "resource_thresholds": {
                "calibrated": True,
                "yellow_available_memory_gib": 10,
                "red_available_memory_gib": 9,
                "yellow_available_disk_gib": 12,
                "red_available_disk_gib": 6,
            }
        }
    )
    sampler = ResourceSampler(
        psutil_module=FakePsutil(),
        policy=policy,
        repository_paths={"demo": tmp_path},
        disk_usage=lambda _path: Disk(100 * 1024**3, 80 * 1024**3, 20 * 1024**3),
    )

    snapshot = sampler.sample()

    assert snapshot.pressure is PressureLevel.RED
    assert snapshot.can_admit is False


def test_vanished_sampling_target_degrades_to_zero_free(tmp_path: Path) -> None:
    # A leased worktree removed by cleanup must not crash the
    # long-running daemon's sampling tick; the vanished target reports
    # zero free bytes, so calibrated admission fails closed instead.
    def disk_usage(path: Path) -> Disk:
        if path.name == "gone":
            raise FileNotFoundError(path)
        return Disk(100, 60, 40)

    sampler = ResourceSampler(
        psutil_module=FakePsutil(),
        policy=PolicyConfig(),
        repository_paths={"demo": tmp_path, "gone": tmp_path / "gone"},
        disk_usage=disk_usage,
    )

    snapshot = sampler.sample()

    assert snapshot.disk_free_bytes == {"demo": 40, "gone": 0}
