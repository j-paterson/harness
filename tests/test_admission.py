"""Verify calibrated green, yellow, and red admission with hysteresis."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from hermes_orchestrator.admission import (
    AdmissionController,
    PressureClassifier,
    WorkerState,
)
from hermes_orchestrator.config import PolicyConfig
from hermes_orchestrator.domain import IssueState, QueuedIssue
from hermes_orchestrator.resources import PressureLevel, ResourceSnapshot

GIB = 1024**3
NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)


@pytest.fixture
def calibrated_policy() -> PolicyConfig:
    return PolicyConfig.model_validate(
        {
            "resource_thresholds": {
                "calibrated": True,
                "yellow_available_memory_gib": 6.0,
                "red_available_memory_gib": 4.0,
                "yellow_available_disk_gib": 12.0,
                "red_available_disk_gib": 8.0,
                "yellow_swap_growth_gib_per_hour": 1.0,
                "yellow_load_ratio": 1.5,
                "hysteresis_samples": 2,
            }
        }
    )


@pytest.fixture
def classifier(calibrated_policy: PolicyConfig) -> PressureClassifier:
    return PressureClassifier(calibrated_policy)


@pytest.fixture
def controller(classifier: PressureClassifier) -> AdmissionController:
    return AdmissionController(classifier)


def snapshot(
    memory_gib: float = 10.0,
    disk_gib: float = 20.0,
    *,
    swap_gib: float = 0.0,
    load: float = 1.0,
    cpus: int = 8,
    at: datetime = NOW,
    memory_pressure: str | None = None,
) -> ResourceSnapshot:
    return ResourceSnapshot(
        sampled_at=at,
        pressure=PressureLevel.UNKNOWN,
        can_admit=False,
        available_memory_bytes=int(memory_gib * GIB),
        total_memory_bytes=32 * GIB,
        swap_used_bytes=int(swap_gib * GIB),
        load_one=load,
        logical_cpus=cpus,
        disk_free_bytes={"demo": int(disk_gib * GIB)},
        managed_rss_bytes=0,
        memory_pressure=memory_pressure,
    )


def red_snapshot() -> ResourceSnapshot:
    return snapshot(memory_gib=3.0)


def worker(
    worker_id: str,
    *,
    priority: int,
    checkpoint_safe: bool,
    dependency_critical: bool = False,
    recent_progress: float = 0.0,
    rss_bytes: int = 0,
) -> WorkerState:
    return WorkerState(
        worker_id=worker_id,
        project_key=worker_id,
        priority=priority,
        checkpoint_safe=checkpoint_safe,
        dependency_critical=dependency_critical,
        recent_progress=recent_progress,
        rss_bytes=rss_bytes,
    )


def queued(issue_id: str, priority: int) -> QueuedIssue:
    return QueuedIssue(
        issue_id=issue_id,
        project_key="demo",
        linear_priority=priority,
        state=IssueState.QUEUED,
        instruction_id=f"i-{issue_id}",
        dependency_ready=True,
        overlap_risk=0,
        admitted_at=NOW,
        updated_at=NOW,
    )


def test_green_admits_when_all_metrics_clear(classifier: PressureClassifier) -> None:
    decision = classifier.classify(snapshot(memory_gib=10, disk_gib=20))
    assert decision.level is PressureLevel.GREEN
    assert decision.can_admit is True
    assert decision.admission_max_priority == 4


def test_yellow_stops_low_priority_admission(
    classifier: PressureClassifier, calibrated_policy: PolicyConfig
) -> None:
    yellow = calibrated_policy.resource_thresholds.yellow_available_memory_gib
    assert yellow is not None
    decision = classifier.classify(snapshot(memory_gib=yellow - 0.1, disk_gib=20))
    assert decision.level is PressureLevel.YELLOW
    assert decision.can_admit is False
    assert decision.admission_max_priority == 1
    assert "yellow floor" in decision.reasons[0]


@pytest.mark.parametrize(
    ("sample", "expected", "reason"),
    [
        (snapshot(memory_gib=3.9), PressureLevel.RED, "red floor"),
        (snapshot(disk_gib=7.5), PressureLevel.RED, "red floor"),
        (snapshot(memory_pressure="critical"), PressureLevel.RED, "critical"),
        (snapshot(disk_gib=11.0), PressureLevel.YELLOW, "yellow floor"),
        (snapshot(load=13.0, cpus=8), PressureLevel.YELLOW, "CPU ratio"),
        (snapshot(memory_pressure="warn"), PressureLevel.YELLOW, "warning"),
    ],
)
def test_thresholds_classify_each_signal(
    classifier: PressureClassifier,
    sample: ResourceSnapshot,
    expected: PressureLevel,
    reason: str,
) -> None:
    decision = classifier.classify(sample)
    assert decision.level is expected
    assert any(reason in item for item in decision.reasons)


def test_swap_growth_rate_needs_a_previous_sample(
    classifier: PressureClassifier,
) -> None:
    earlier = snapshot(swap_gib=1.0, at=NOW - timedelta(minutes=30))
    later = snapshot(swap_gib=2.0, at=NOW)
    assert classifier.classify(later).level is PressureLevel.GREEN
    decision = classifier.classify(later, previous=earlier)
    assert decision.level is PressureLevel.YELLOW
    assert decision.evidence["swap_growth_gib_per_hour"] == 2.0
    slow = snapshot(swap_gib=1.2, at=NOW)
    assert classifier.classify(slow, previous=earlier).level is PressureLevel.GREEN


def test_uncalibrated_policy_is_unknown_and_closed() -> None:
    decision = PressureClassifier(PolicyConfig()).classify(snapshot())
    assert decision.level is PressureLevel.UNKNOWN
    assert decision.can_admit is False
    assert decision.admission_max_priority is None


def test_hysteresis_prevents_green_yellow_flapping(
    classifier: PressureClassifier,
) -> None:
    assert classifier.observe(snapshot(memory_gib=5.5)).level is PressureLevel.YELLOW
    held = classifier.observe(snapshot(memory_gib=10))
    assert held.level is PressureLevel.YELLOW
    assert held.raw_level is PressureLevel.GREEN
    assert held.can_admit is False
    assert held.reasons[0].startswith("holding yellow: 1/2")
    relaxed = classifier.observe(snapshot(memory_gib=10))
    assert relaxed.level is PressureLevel.GREEN
    assert relaxed.can_admit is True
    # Worsening is immediate; relaxing steps down only after two calmer
    # samples, and a worse sample restarts the calm streak.
    assert classifier.observe(snapshot(memory_gib=3)).level is PressureLevel.RED
    assert classifier.observe(snapshot(memory_gib=10)).level is PressureLevel.RED
    assert classifier.observe(snapshot(memory_gib=5)).level is PressureLevel.YELLOW
    assert classifier.observe(snapshot(memory_gib=10)).level is PressureLevel.YELLOW
    assert classifier.observe(snapshot(memory_gib=5)).level is PressureLevel.YELLOW
    assert classifier.observe(snapshot(memory_gib=10)).level is PressureLevel.YELLOW
    assert classifier.observe(snapshot(memory_gib=10)).level is PressureLevel.GREEN


def test_green_yields_no_actions(controller: AdmissionController) -> None:
    assert controller.evaluate([], [], snapshot()) == []


def test_yellow_holds_lower_priority_without_touching_workers(
    controller: AdmissionController,
) -> None:
    actions = controller.evaluate(
        queue=[queued("ENG-1", 1), queued("ENG-2", 3)],
        workers=[worker("w", priority=2, checkpoint_safe=True)],
        snapshot=snapshot(memory_gib=5.0),
    )
    assert [action.kind for action in actions] == ["stop_admission"]
    assert actions[0].evidence["held_issues"] == ["ENG-2"]
    assert actions[0].evidence["admission_max_priority"] == 1


def test_red_pauses_lowest_priority_safe_worker(
    controller: AdmissionController,
) -> None:
    actions = controller.evaluate(
        queue=[],
        workers=[
            worker("high", priority=1, checkpoint_safe=False),
            worker("low", priority=4, checkpoint_safe=True),
        ],
        snapshot=red_snapshot(),
    )
    assert actions[0].kind == "stop_admission"
    assert actions[1].target_id == "low"
    assert actions[1].kind == "request_checkpoint"
    assert len(actions) == 2


def test_red_orders_candidates_and_requests_one_at_a_time(
    controller: AdmissionController,
) -> None:
    actions = controller.evaluate(
        queue=[],
        workers=[
            worker(
                "p4-critical",
                priority=4,
                checkpoint_safe=True,
                dependency_critical=True,
            ),
            worker(
                "p4-progressed", priority=4, checkpoint_safe=True, recent_progress=0.9
            ),
            worker("p4-idle-big", priority=4, checkpoint_safe=True, rss_bytes=9),
            worker("p4-idle-small", priority=4, checkpoint_safe=True, rss_bytes=1),
            worker("p2", priority=2, checkpoint_safe=True),
            worker("unsafe", priority=4, checkpoint_safe=False),
        ],
        snapshot=red_snapshot(),
    )
    checkpoints = [action for action in actions if action.kind == "request_checkpoint"]
    assert len(checkpoints) == 1
    assert checkpoints[0].target_id == "p4-idle-big"
    assert checkpoints[0].evidence["candidates"] == [
        "p4-idle-big",
        "p4-idle-small",
        "p4-progressed",
        "p4-critical",
        "p2",
    ]


def test_red_without_safe_workers_pauses_nothing(
    controller: AdmissionController,
) -> None:
    actions = controller.evaluate(
        queue=[queued("ENG-1", 1)],
        workers=[worker("busy", priority=4, checkpoint_safe=False)],
        snapshot=red_snapshot(),
    )
    assert [action.kind for action in actions] == ["stop_admission", "pause_worker"]
    assert actions[1].target_id is None
    assert actions[0].evidence["held_issues"] == ["ENG-1"]
