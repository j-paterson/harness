import pytest

from hermes_orchestrator.adapters import FakeLinear, FakeSourceControl, FakeWorker


def test_recording_fake_captures_boundary_call() -> None:
    linear = FakeLinear()

    result = linear.project("ENG-7", status="In Development", assignee="operator")

    assert result == {"changed": True}
    assert linear.calls[0].operation == "project"
    assert linear.calls[0].arguments == {
        "issue_id": "ENG-7",
        "status": "In Development",
        "assignee": "operator",
    }


def test_recording_fake_can_inject_one_failure() -> None:
    source_control = FakeSourceControl()
    source_control.fail_next(RuntimeError("unavailable"))

    with pytest.raises(RuntimeError, match="unavailable"):
        source_control.merge("owner/demo", pull_request=7, expected_sha="abc123")

    assert source_control.merge(
        "owner/demo", pull_request=7, expected_sha="abc123"
    ) == {"merged": True, "sha": "abc123"}


def test_worker_fake_records_project_cell_start() -> None:
    worker = FakeWorker()

    worker.start_project_cell("demo", "ENG-7")

    assert worker.calls[0].operation == "start_project_cell"
