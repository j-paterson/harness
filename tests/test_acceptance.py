"""Durable acceptance gates: require, satisfy, and the replay contract.

INFRA-198 J1: the gate is the durable truth that a merged issue still
awaits operator acceptance. These tests pin the one-way lifecycle
(pending -> satisfied), full predicate coverage, the byte-identical
replay contract, and the journaled events.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from hermes_orchestrator.acceptance import (
    AcceptanceGateConflict,
    AcceptanceGates,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def gates(database: Database) -> AcceptanceGates:
    return AcceptanceGates(database, events=EventStore(database))


def event_count(database: Database, event_type: str) -> int:
    return int(
        database.scalar(
            "SELECT COUNT(*) FROM events WHERE event_type = ?", (event_type,)
        )
    )


# -- require -------------------------------------------------------------


def test_require_creates_a_pending_gate(
    gates: AcceptanceGates, database: Database
) -> None:
    gate = gates.require(
        "ENG-9",
        instruction_id="chat-accept-1",
        predicates=("live_smoke", "operator_signoff"),
    )

    assert gate.issue_id == "ENG-9"
    assert gate.instruction_id == "chat-accept-1"
    assert gate.predicates == ("live_smoke", "operator_signoff")
    assert gate.state == "pending"
    assert gate.evidence is None
    assert gates.pending("ENG-9") is True
    assert gate.as_dict()["predicates"] == ["live_smoke", "operator_signoff"]
    assert gate.as_dict()["evidence"] is None
    assert event_count(database, "acceptance.required") == 1


def test_require_identical_replay_journals_nothing(
    gates: AcceptanceGates, database: Database
) -> None:
    first = gates.require(
        "ENG-9", instruction_id="chat-accept-1", predicates=("live_smoke",)
    )

    replay = gates.require(
        "ENG-9", instruction_id="chat-accept-1", predicates=("live_smoke",)
    )

    assert replay == first
    assert event_count(database, "acceptance.required") == 1


def test_require_refreshes_a_pending_gate(
    gates: AcceptanceGates, database: Database
) -> None:
    gates.require(
        "ENG-9", instruction_id="chat-accept-1", predicates=("live_smoke",)
    )

    refreshed = gates.require(
        "ENG-9",
        instruction_id="chat-accept-2",
        predicates=("live_smoke", "operator_signoff"),
    )

    assert refreshed.instruction_id == "chat-accept-2"
    assert refreshed.predicates == ("live_smoke", "operator_signoff")
    assert refreshed.state == "pending"
    assert event_count(database, "acceptance.required") == 2


def test_require_refuses_a_satisfied_gate(gates: AcceptanceGates) -> None:
    gates.require(
        "ENG-9", instruction_id="chat-accept-1", predicates=("live_smoke",)
    )
    gates.satisfy("ENG-9", evidence={"live_smoke": "receipt-1"})

    with pytest.raises(AcceptanceGateConflict, match="already satisfied"):
        gates.require(
            "ENG-9", instruction_id="chat-accept-2", predicates=("rerun",)
        )


@pytest.mark.parametrize(
    ("issue_id", "instruction_id", "predicates"),
    [
        ("", "chat-accept-1", ("live_smoke",)),
        ("ENG-9", " ", ("live_smoke",)),
        ("ENG-9", "chat-accept-1", ()),
        ("ENG-9", "chat-accept-1", (" ",)),
    ],
)
def test_require_validates_inputs(
    gates: AcceptanceGates,
    issue_id: str,
    instruction_id: str,
    predicates: tuple[str, ...],
) -> None:
    with pytest.raises(ValueError):
        gates.require(
            issue_id, instruction_id=instruction_id, predicates=predicates
        )


# -- satisfy -------------------------------------------------------------


def test_satisfy_requires_full_predicate_coverage(
    gates: AcceptanceGates, database: Database
) -> None:
    gates.require(
        "ENG-9",
        instruction_id="chat-accept-1",
        predicates=("live_smoke", "operator_signoff"),
    )

    with pytest.raises(AcceptanceGateConflict, match="operator_signoff"):
        gates.satisfy("ENG-9", evidence={"live_smoke": "receipt-1"})

    assert gates.pending("ENG-9") is True
    assert event_count(database, "acceptance.satisfied") == 0


def test_satisfy_marks_the_gate_satisfied_and_journals(
    gates: AcceptanceGates, database: Database
) -> None:
    gates.require(
        "ENG-9", instruction_id="chat-accept-1", predicates=("live_smoke",)
    )

    gate = gates.satisfy("ENG-9", evidence={"live_smoke": "receipt-1"})

    assert gate.state == "satisfied"
    assert gate.evidence == (("live_smoke", "receipt-1"),)
    assert gate.as_dict()["evidence"] == {"live_smoke": "receipt-1"}
    assert gates.pending("ENG-9") is False
    assert event_count(database, "acceptance.satisfied") == 1


def test_satisfy_allows_evidence_beyond_the_predicates(
    gates: AcceptanceGates,
) -> None:
    gates.require(
        "ENG-9", instruction_id="chat-accept-1", predicates=("live_smoke",)
    )

    gate = gates.satisfy(
        "ENG-9",
        evidence={"live_smoke": "receipt-1", "extra_note": "also verified"},
    )

    assert gate.state == "satisfied"
    assert gate.evidence == (
        ("extra_note", "also verified"),
        ("live_smoke", "receipt-1"),
    )


def test_satisfy_identical_replay_journals_nothing(
    gates: AcceptanceGates, database: Database
) -> None:
    gates.require(
        "ENG-9", instruction_id="chat-accept-1", predicates=("live_smoke",)
    )
    first = gates.satisfy("ENG-9", evidence={"live_smoke": "receipt-1"})

    replay = gates.satisfy("ENG-9", evidence={"live_smoke": "receipt-1"})

    assert replay == first
    assert event_count(database, "acceptance.satisfied") == 1


def test_satisfy_refuses_rewriting_satisfied_evidence(
    gates: AcceptanceGates,
) -> None:
    gates.require(
        "ENG-9", instruction_id="chat-accept-1", predicates=("live_smoke",)
    )
    gates.satisfy("ENG-9", evidence={"live_smoke": "receipt-1"})

    with pytest.raises(AcceptanceGateConflict, match="different evidence"):
        gates.satisfy("ENG-9", evidence={"live_smoke": "receipt-2"})


def test_satisfy_unknown_issue_fails_closed(gates: AcceptanceGates) -> None:
    with pytest.raises(KeyError):
        gates.satisfy("ENG-404", evidence={"live_smoke": "receipt-1"})


def test_satisfy_refuses_blank_evidence_values(gates: AcceptanceGates) -> None:
    gates.require(
        "ENG-9", instruction_id="chat-accept-1", predicates=("live_smoke",)
    )
    with pytest.raises(ValueError, match="non-blank"):
        gates.satisfy("ENG-9", evidence={"live_smoke": " "})


# -- reads and durability -------------------------------------------------


def test_get_returns_none_for_an_ungated_issue(gates: AcceptanceGates) -> None:
    assert gates.get("ENG-9") is None
    assert gates.pending("ENG-9") is False


def test_gate_is_durable_across_reopen(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    database = Database.open(path)
    try:
        AcceptanceGates(database, events=EventStore(database)).require(
            "ENG-9", instruction_id="chat-accept-1", predicates=("live_smoke",)
        )
    finally:
        database.close()
    reopened = Database.open(path)
    try:
        gates = AcceptanceGates(reopened, events=EventStore(reopened))
        gate = gates.get("ENG-9")
        assert gate is not None
        assert gate.state == "pending"
        assert gate.predicates == ("live_smoke",)
    finally:
        reopened.close()
