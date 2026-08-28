"""Verify the durable correction outbox that returns packets to the lead."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.lead_outbox import LeadCorrectionOutbox
from hermes_orchestrator.verdicts import CorrectionPacket

SHA = "a" * 40


def packet(**overrides: object) -> CorrectionPacket:
    values: dict[str, object] = {
        "severity": "Critical",
        "repository": "j-paterson/demo",
        "branch": "feature/eng-9",
        "pr_number": 14,
        "reviewed_sha": SHA,
        "evidence": "tests fail",
        "acceptance_criterion": "tests pass",
        "required_correction": "fix",
        "required_tests": ("test_fix",),
    }
    values.update(overrides)
    return CorrectionPacket(**values)  # type: ignore[arg-type]


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def outbox(database: Database) -> LeadCorrectionOutbox:
    ids = iter(f"corr-{n}" for n in range(1, 10))
    return LeadCorrectionOutbox(
        database=database,
        events=EventStore(database),
        project_for_issue=lambda issue_id: "demo",
        ids=lambda: next(ids),
    )


def test_delivery_is_durable_and_pending(outbox: LeadCorrectionOutbox) -> None:
    delivered = outbox.deliver("ENG-9", (packet(),))
    assert delivered.correction_id == "corr-1"
    assert delivered.state == "pending"
    assert delivered.reviewed_sha == SHA
    assert outbox.pending("demo") == (delivered,)
    assert outbox.pending() == (delivered,)
    assert delivered.as_dict()["packets"][0]["severity"] == "Critical"


def test_acknowledgement_is_explicit_and_idempotent(
    outbox: LeadCorrectionOutbox, database: Database
) -> None:
    delivered = outbox.deliver("ENG-9", (packet(),), source="qa_rejection")
    acknowledged = outbox.acknowledge(delivered.correction_id)
    assert acknowledged.state == "acknowledged"
    assert outbox.pending() == ()
    assert outbox.acknowledge(delivered.correction_id).state == "acknowledged"
    count = database.scalar(
        "SELECT count(*) FROM events "
        "WHERE event_type = 'lead_correction.acknowledged'"
    )
    assert count == 1
    with pytest.raises(KeyError):
        outbox.acknowledge("missing")


def test_delivery_requires_one_bound_candidate(outbox: LeadCorrectionOutbox) -> None:
    with pytest.raises(ValueError, match="at least one"):
        outbox.deliver("ENG-9", ())
    with pytest.raises(ValueError, match="same candidate"):
        outbox.deliver("ENG-9", (packet(), packet(reviewed_sha="b" * 40)))
