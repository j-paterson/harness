"""Verify durable QA origin persistence and post-merge/rejection routing."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.linear import LinearProjection
from hermes_orchestrator.qa import QaOrigin, QaRouter


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def qa_router(database: Database) -> QaRouter:
    return QaRouter(database=database, events=EventStore(database))


def test_origin_kinds_are_closed() -> None:
    assert QaOrigin("ordinary").routes_to_qa is False
    assert QaOrigin("ryan_assigned").routes_to_qa is True
    assert QaOrigin("operator_designated").routes_to_qa is True
    with pytest.raises(ValueError, match="origin"):
        QaOrigin("inferred_from_assignee")


def test_unrecorded_issue_is_ordinary(qa_router: QaRouter) -> None:
    assert qa_router.origin_of("ENG-9") == QaOrigin("ordinary")


def test_ordinary_merge_routes_done(qa_router: QaRouter) -> None:
    assert qa_router.after_merge("ENG-9") == LinearProjection(
        status="Done", assignee_alias="operator"
    )


def test_ryan_origin_routes_back_to_ryan_in_qa(qa_router: QaRouter) -> None:
    qa_router.record_origin("ENG-10", QaOrigin("ryan_assigned"))
    assert qa_router.after_merge("ENG-10") == LinearProjection(
        status="QA", assignee_alias="ryan"
    )


def test_operator_designated_routes_to_qa_for_operator(
    qa_router: QaRouter,
) -> None:
    qa_router.record_origin("ENG-11", QaOrigin("operator_designated"))
    assert qa_router.after_merge("ENG-11") == LinearProjection(
        status="QA", assignee_alias="operator"
    )


def test_qa_rejection_returns_to_operator(qa_router: QaRouter) -> None:
    qa_router.record_origin("ENG-10", QaOrigin("ryan_assigned"))
    assert qa_router.after_rejection("ENG-10") == LinearProjection(
        status="In Development", assignee_alias="operator"
    )
    # A rejection never rewrites the durable origin: the corrected work
    # still returns to Ryan in QA after the next merge.
    assert qa_router.after_merge("ENG-10") == LinearProjection(
        status="QA", assignee_alias="ryan"
    )


def test_origin_is_durable_across_reopen(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    database = Database.open(path)
    try:
        QaRouter(database=database, events=EventStore(database)).record_origin(
            "ENG-10", QaOrigin("ryan_assigned")
        )
    finally:
        database.close()
    reopened = Database.open(path)
    try:
        router = QaRouter(database=reopened, events=EventStore(reopened))
        assert router.origin_of("ENG-10") == QaOrigin("ryan_assigned")
    finally:
        reopened.close()


def test_recording_the_same_origin_is_idempotent(
    qa_router: QaRouter, database: Database
) -> None:
    qa_router.record_origin("ENG-10", QaOrigin("ryan_assigned"))
    qa_router.record_origin("ENG-10", QaOrigin("ryan_assigned"))
    rows = database.execute(
        "SELECT COUNT(*) AS n FROM events WHERE event_type = 'qa_origin.recorded'"
    ).fetchone()
    assert rows["n"] == 1


def test_origin_changes_only_by_explicit_designation(
    qa_router: QaRouter,
) -> None:
    qa_router.record_origin("ENG-10", QaOrigin("ordinary"))
    qa_router.record_origin("ENG-10", QaOrigin("operator_designated"))
    assert qa_router.origin_of("ENG-10") == QaOrigin("operator_designated")
    with pytest.raises(ValueError, match="issue"):
        qa_router.record_origin("", QaOrigin("ordinary"))
