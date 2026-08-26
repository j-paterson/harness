from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore


@pytest.fixture
def database(tmp_path: Path):
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


def test_domain_write_and_event_are_atomic(database: Database) -> None:
    events = EventStore(database)

    with (
        pytest.raises(RuntimeError, match="rollback"),
        database.transaction() as connection,
    ):
        connection.execute(
            "INSERT INTO admitted_issues"
            "(issue_id, project_key, priority, state) "
            "VALUES ('ENG-1', 'demo', 1, 'queued')"
        )
        events.append(
            connection,
            EventInput(
                event_type="issue.admitted",
                aggregate_type="issue",
                aggregate_id="ENG-1",
                payload={"priority": 1},
            ),
        )
        raise RuntimeError("rollback")

    assert database.scalar("SELECT count(*) FROM admitted_issues") == 0
    assert events.list_after(0) == []


def test_event_payload_is_canonical_and_ordered(database: Database) -> None:
    events = EventStore(database)

    with database.transaction() as connection:
        first = events.append(
            connection,
            EventInput(
                event_type="issue.admitted",
                aggregate_type="issue",
                aggregate_id="ENG-1",
                payload={"z": 1, "a": 2},
                correlation_id="chat-123",
                actor="operator",
            ),
        )

    stored = events.list_after(0)
    assert stored == [first]
    assert stored[0].sequence == 1
    assert stored[0].payload == {"a": 2, "z": 1}
    assert stored[0].correlation_id == "chat-123"
