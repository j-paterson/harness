"""Verify corroborated stall detection and reason normalization."""

from __future__ import annotations

import pytest

from hermes_orchestrator.stalls import REASONS, StallDetector, StallEvidence


@pytest.fixture
def detector() -> StallDetector:
    return StallDetector(inactivity_seconds=900)


def test_elapsed_time_alone_is_not_a_stall(detector: StallDetector) -> None:
    evidence = StallEvidence(no_material_change_seconds=1800, process_alive=True)
    assert detector.evaluate(evidence) is None


def test_inactivity_needs_corroboration(detector: StallDetector) -> None:
    dead = detector.evaluate(
        StallEvidence(no_material_change_seconds=1800, process_alive=False)
    )
    assert dead is not None and dead.reason == "agent_uncertainty"
    assert dead.predicate["signal"] == "inactive"
    silent = detector.evaluate(
        StallEvidence(
            no_material_change_seconds=1800,
            output_activity=False,
            resource_activity=False,
        )
    )
    assert silent is not None and silent.predicate_key != dead.predicate_key
    short = detector.evaluate(
        StallEvidence(no_material_change_seconds=60, process_alive=False)
    )
    assert short is None


@pytest.mark.parametrize(
    ("evidence", "reason"),
    [
        (StallEvidence(provider_error="limit"), "provider_limit"),
        (StallEvidence(provider_error="auth"), "authentication"),
        (StallEvidence(blocked_request="tool approval"), "approval_wait"),
        (StallEvidence(external_failure="dependency"), "dependency"),
        (StallEvidence(external_failure="merge_conflict"), "merge_conflict"),
        (StallEvidence(external_failure="ci"), "ci_failure"),
        (StallEvidence(external_failure="environment"), "environment_failure"),
        (StallEvidence(external_failure="resource"), "resource_pressure"),
        (StallEvidence(repeated_failure="uv run pytest"), "repeated_command_failure"),
        (StallEvidence(agent_report="cannot continue"), "agent_uncertainty"),
        (StallEvidence(resource_pressure=True), "resource_pressure"),
    ],
)
def test_reasons_are_normalized_to_the_closed_set(
    detector: StallDetector, evidence: StallEvidence, reason: str
) -> None:
    diagnosis = detector.evaluate(evidence)
    assert diagnosis is not None
    assert diagnosis.reason == reason
    assert diagnosis.reason in REASONS
    assert len(diagnosis.predicate_key) == 24


def test_unknown_external_failure_is_not_guessed(detector: StallDetector) -> None:
    assert detector.evaluate(StallEvidence(external_failure="gremlins")) is None


def test_predicates_are_exact_and_stable(detector: StallDetector) -> None:
    first = detector.evaluate(StallEvidence(repeated_failure="uv run pytest"))
    again = detector.evaluate(StallEvidence(repeated_failure="uv run pytest"))
    other = detector.evaluate(StallEvidence(repeated_failure="uv run ruff"))
    assert first is not None and again is not None and other is not None
    assert first.predicate_key == again.predicate_key
    assert first.predicate_key != other.predicate_key
    with pytest.raises(ValueError):
        StallDetector(inactivity_seconds=0)


def test_active_quiet_process_is_a_long_running_command_not_a_stall(
    detector: StallDetector,
) -> None:
    """Elapsed threshold exceeded, live, quiet output, but resource-active."""

    assert (
        detector.evaluate(
            StallEvidence(
                no_material_change_seconds=7200,
                process_alive=True,
                output_activity=False,
                resource_activity=True,
            )
        )
        is None
    )


def test_dead_process_and_fully_inactive_live_process_are_stalls(
    detector: StallDetector,
) -> None:
    dead = detector.evaluate(
        StallEvidence(no_material_change_seconds=1800, process_alive=False)
    )
    assert dead is not None
    assert dead.predicate == {
        "signal": "inactive",
        "process_alive": False,
        "output_activity": True,
        "resource_activity": True,
    }
    inactive = detector.evaluate(
        StallEvidence(
            no_material_change_seconds=1800,
            process_alive=True,
            output_activity=False,
            resource_activity=False,
        )
    )
    assert inactive is not None and inactive.reason == "agent_uncertainty"
    assert inactive.predicate["process_alive"] is True
    # Resource inactivity alone (output still flowing) is not a stall either.
    assert (
        detector.evaluate(
            StallEvidence(
                no_material_change_seconds=1800,
                output_activity=True,
                resource_activity=False,
            )
        )
        is None
    )
