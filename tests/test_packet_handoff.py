"""Ledger-only lead handoff reconstruction (INFRA-186)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.subagent_packets import SubagentPackets

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)
SESSION = "11111111-2222-4333-8444-555555555555"


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


def test_accepted_packets_reconstruct_a_handoff_without_conversation(
    database: Database,
) -> None:
    """A replacement lead resumes orchestration from durable rows
    alone: boundaries, dependency order, verification, states, and
    accepted-evidence keys — with no prompt or response text anywhere
    in the reconstruction."""

    packets = SubagentPackets(
        database, events=EventStore(database), now=lambda: NOW
    )
    first = packets.create(
        issue_id="INFRA-186",
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        generation=1,
        model_tier="sonnet",
        effort="high",
        allowed_files=("src/a.py", "tests/test_a.py"),
        worktree="/repos/demo",
        depends_on=(),
        red_test="test_a red first",
        verification=("uv run pytest tests/test_a.py -q",),
        invariants="touch nothing else",
        resource_note="small",
    )
    packets.reserve(
        first.packet_id, session_id=SESSION, tool_use_id="toolu_1"
    )
    packets.settle(
        first.packet_id, outcome="completed", tool_use_id="toolu_1"
    )
    packets.accept(
        first.packet_id,
        evidence={"diff_files": "2", "tests_green": "12", "red_proof": "yes"},
    )
    second = packets.create(
        issue_id="INFRA-186",
        project_key="demo",
        cell_id="cell-demo",
        session_id=SESSION,
        generation=1,
        model_tier="haiku",
        effort="medium",
        allowed_files=("config/x.yaml",),
        worktree="/repos/demo",
        depends_on=(first.packet_id,),
        red_test="loads config red first",
        verification=("uv run pytest tests/test_x.py -q",),
        invariants="config only",
        resource_note="tiny",
    )

    summary = packets.handoff_summary("INFRA-186")

    assert summary["issue_id"] == "INFRA-186"
    accepted, planned = summary["packets"]
    assert accepted["state"] == "accepted"
    assert accepted["evidence_keys"] == [
        "diff_files",
        "red_proof",
        "tests_green",
    ]
    assert accepted["allowed_files"] == ["src/a.py", "tests/test_a.py"]
    assert planned["state"] == "planned"
    assert planned["depends_on"] == [first.packet_id]
    assert summary["outstanding"] == [second.packet_id]
    # No conversation text is present anywhere: only bounded ledger
    # fields and evidence KEYS, never values, prompts, or output.
    import json

    rendered = json.dumps(summary)
    assert "tests_green" in rendered
    assert "12" not in rendered.replace('"generation": 1', "")
