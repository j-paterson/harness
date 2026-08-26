from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from uuid import UUID

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.handoffs import (
    HandoffDocument,
    HandoffRejected,
    HandoffService,
    HandoffTest,
)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def handoffs(database: Database) -> HandoffService:
    return HandoffService(database, handoff_ids=lambda: "handoff-1")


def valid_handoff() -> HandoffDocument:
    return HandoffDocument(
        cell_id="cell-demo",
        objective="Complete ENG-9 without expanding its scope.",
        status="Implementation is ready for the remaining unit-test correction.",
        decisions=["Keep the existing public interface."],
        branch="feature/eng-9",
        commits=["abc123 feat: implement ENG-9"],
        pull_request="https://github.example/pull/9",
        modified_files=["src/example.py", "tests/test_example.py"],
        tests=[HandoffTest(command="uv run pytest", outcome="1 failed, 12 passed")],
        blockers=[],
        remaining_steps=["Correct the failing boundary case."],
        commands=["uv run pytest tests/test_example.py -q"],
        environment_notes=["Python 3.13 through uv."],
        risks=[],
        next_action="Run the failing test and correct ENG-9.",
    )


def test_handoff_requires_every_contract_field(handoffs: HandoffService) -> None:
    incomplete = valid_handoff().model_copy(update={"tests": []})

    with pytest.raises(HandoffRejected, match="tests"):
        handoffs.submit(incomplete)


def test_handoff_persists_rendered_snapshot(handoffs: HandoffService) -> None:
    record = handoffs.submit(valid_handoff())

    restored = handoffs.get(record.handoff_id)

    assert restored.document == valid_handoff()
    assert "# Project lead handoff" in restored.markdown
    assert "uv run pytest" in restored.markdown
    assert restored.state == "submitted"


def test_acknowledgement_requires_restarted_next_action(
    handoffs: HandoffService,
) -> None:
    record = handoffs.submit(valid_handoff())

    with pytest.raises(HandoffRejected, match="next action"):
        handoffs.acknowledge(
            record.handoff_id,
            UUID("22222222-2222-4222-8222-222222222222"),
            "   ",
        )
