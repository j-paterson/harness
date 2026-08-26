"""Append-only domain event storage."""

from __future__ import annotations

import json
import secrets
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from hermes_orchestrator.db import Database


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _uuid7() -> str:
    """Create a sortable UUID with RFC 9562 version and variant bits."""

    timestamp_ms = time.time_ns() // 1_000_000
    value = (timestamp_ms & ((1 << 48) - 1)) << 80
    value |= 0x7 << 76
    value |= secrets.randbits(12) << 64
    value |= 0b10 << 62
    value |= secrets.randbits(62)
    return str(uuid.UUID(int=value))


@dataclass(frozen=True, slots=True)
class EventInput:
    """The caller-supplied fields for one event."""

    event_type: str
    aggregate_type: str
    aggregate_id: str
    payload: dict[str, Any]
    correlation_id: str | None = None
    actor: str = "orchestrator"
    event_id: str = field(default_factory=_uuid7)
    occurred_at: str = field(default_factory=_now_iso)


@dataclass(frozen=True, slots=True)
class EventRecord:
    """A stored event with its monotonic database sequence."""

    sequence: int
    event_id: str
    occurred_at: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    correlation_id: str | None
    actor: str
    payload: dict[str, Any]


class EventStore:
    """Append and read events without owning transaction boundaries."""

    def __init__(self, database: Database) -> None:
        self._database = database

    def append(
        self,
        connection: sqlite3.Connection,
        event: EventInput,
    ) -> EventRecord:
        """Append an event through the caller's active transaction."""

        payload_json = json.dumps(
            event.payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        cursor = connection.execute(
            "INSERT INTO events("
            "event_id, occurred_at, event_type, aggregate_type, aggregate_id, "
            "correlation_id, actor, payload_json"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event.event_id,
                event.occurred_at,
                event.event_type,
                event.aggregate_type,
                event.aggregate_id,
                event.correlation_id,
                event.actor,
                payload_json,
            ),
        )
        return EventRecord(
            sequence=int(cursor.lastrowid),
            event_id=event.event_id,
            occurred_at=event.occurred_at,
            event_type=event.event_type,
            aggregate_type=event.aggregate_type,
            aggregate_id=event.aggregate_id,
            correlation_id=event.correlation_id,
            actor=event.actor,
            payload=json.loads(payload_json),
        )

    def list_after(self, sequence: int) -> list[EventRecord]:
        """Return events in durable sequence order."""

        rows = self._database.execute(
            "SELECT sequence, event_id, occurred_at, event_type, aggregate_type, "
            "aggregate_id, correlation_id, actor, payload_json "
            "FROM events WHERE sequence > ? ORDER BY sequence",
            (sequence,),
        ).fetchall()
        return [
            EventRecord(
                sequence=int(row["sequence"]),
                event_id=str(row["event_id"]),
                occurred_at=str(row["occurred_at"]),
                event_type=str(row["event_type"]),
                aggregate_type=str(row["aggregate_type"]),
                aggregate_id=str(row["aggregate_id"]),
                correlation_id=row["correlation_id"],
                actor=str(row["actor"]),
                payload=json.loads(row["payload_json"]),
            )
            for row in rows
        ]
