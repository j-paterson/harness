"""Durable subagent-packet ledger (INFRA-186).

Delegating implementation to disjoint Claude subagents needs a durable
contract, not a plan carried only in a prompt. Each packet binds one
bounded unit of work (allowed files, a red test, verification
commands, invariants) to an issue, cell, and lead session before any
subagent is spawned. Reservation is an exactly-once compare-and-swap
keyed on the PreToolUse ``tool_use_id``, so a crashed or duplicated
Agent invocation can never claim the same packet twice; settlement
must match that same ``tool_use_id``, so only the reserving invocation
can report its own outcome. Acceptance is a later, separate step gated
on reviewed evidence (diff scope, red/green proof references) — the
prompt and model output text are never stored here. A reservation
whose tool-use evidence is proven absent (the invocation is no longer
live) expires to ``failed`` so a stuck packet cannot block an issue
forever.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

PACKET_MODEL_TIERS = ("haiku", "sonnet", "fable")

_SETTLEMENT_OUTCOMES = ("completed", "failed", "capped")


class PacketRefused(ValueError):
    """The requested packet creation or transition violates the ledger's
    contract (unknown tier, foreign session, stale CAS, and similar)."""


@dataclass(frozen=True, slots=True)
class SubagentPacket:
    """One durable, exactly-bound unit of delegated implementation work."""

    packet_id: str
    issue_id: str
    project_key: str
    cell_id: str
    session_id: str
    generation: int
    model_tier: str
    effort: str
    allowed_files: tuple[str, ...]
    worktree: str
    depends_on: tuple[str, ...]
    red_test: str
    verification: tuple[str, ...]
    invariants: str
    resource_note: str
    state: str
    evidence: dict[str, Any] | None
    created_at: str
    updated_at: str


class SubagentPackets:
    """Create, reserve, settle, accept, reject, and expire packets."""

    def __init__(
        self,
        database: Database,
        *,
        events: EventStore,
        ids: Callable[[], str] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._now = now or (lambda: datetime.now(UTC))

    def create(
        self,
        *,
        issue_id: str,
        project_key: str,
        cell_id: str,
        session_id: str,
        generation: int,
        model_tier: str,
        effort: str,
        allowed_files: Sequence[str],
        worktree: str,
        red_test: str,
        verification: Sequence[str],
        invariants: str,
        resource_note: str,
        depends_on: Sequence[str] = (),
    ) -> SubagentPacket:
        """Plan one packet; refuses an unknown tier, empty ``allowed_files``,
        or a ``depends_on`` naming a packet that does not exist."""

        if model_tier not in PACKET_MODEL_TIERS:
            raise PacketRefused(f"unknown model tier {model_tier!r}")
        allowed = tuple(allowed_files)
        if not allowed:
            raise PacketRefused("allowed_files must be non-empty")
        depends = tuple(depends_on)
        stamp = self._now().isoformat()
        packet_id = self._ids()
        with self._database.transaction() as connection:
            for dependency_id in depends:
                exists = connection.execute(
                    "SELECT 1 FROM subagent_packets WHERE packet_id = ?",
                    (dependency_id,),
                ).fetchone()
                if exists is None:
                    raise PacketRefused(
                        "depends_on references unknown packet "
                        f"{dependency_id!r}"
                    )
            connection.execute(
                "INSERT INTO subagent_packets("
                "packet_id, issue_id, project_key, cell_id, session_id, "
                "generation, model_tier, effort, allowed_files_json, "
                "worktree, depends_on_json, red_test, verification_json, "
                "invariants, resource_note, state, evidence_json, "
                "tool_use_id, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "'planned', NULL, NULL, ?, ?)",
                (
                    packet_id,
                    issue_id,
                    project_key,
                    cell_id,
                    session_id,
                    generation,
                    model_tier,
                    effort,
                    json.dumps(list(allowed)),
                    worktree,
                    json.dumps(list(depends)),
                    red_test,
                    json.dumps(list(verification)),
                    invariants,
                    resource_note,
                    stamp,
                    stamp,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="subagent_packet.planned",
                    aggregate_type="subagent_packet",
                    aggregate_id=packet_id,
                    payload={
                        "issue_id": issue_id,
                        "project_key": project_key,
                        "cell_id": cell_id,
                        "session_id": session_id,
                        "generation": generation,
                        "model_tier": model_tier,
                        "allowed_files": list(allowed),
                        "depends_on": list(depends),
                    },
                ),
            )
        return self.get(packet_id)

    def reserve(
        self, packet_id: str, *, session_id: str, tool_use_id: str
    ) -> SubagentPacket:
        """CAS ``planned`` -> ``reserved``, exactly once.

        Refuses an unknown packet, a foreign ``session_id`` (one other
        than the packet's bound session), a packet already reserved
        (or otherwise not ``planned``), or a ``depends_on`` entry that
        is not yet ``accepted``.
        """

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT session_id, depends_on_json FROM subagent_packets "
                "WHERE packet_id = ?",
                (packet_id,),
            ).fetchone()
            if row is None:
                raise PacketRefused(f"unknown packet {packet_id!r}")
            if str(row["session_id"]) != session_id:
                raise PacketRefused(
                    f"packet {packet_id!r} is bound to a different session"
                )
            for dependency_id in json.loads(str(row["depends_on_json"])):
                dependency = connection.execute(
                    "SELECT state FROM subagent_packets WHERE packet_id = ?",
                    (dependency_id,),
                ).fetchone()
                if dependency is None or str(dependency["state"]) != "accepted":
                    raise PacketRefused(
                        f"dependency {dependency_id!r} is not accepted"
                    )
            cursor = connection.execute(
                "UPDATE subagent_packets SET state = 'reserved', "
                "tool_use_id = ?, updated_at = ? "
                "WHERE packet_id = ? AND state = 'planned'",
                (tool_use_id, stamp, packet_id),
            )
            if cursor.rowcount != 1:
                raise PacketRefused(f"packet {packet_id!r} is not reservable")
            self._events.append(
                connection,
                EventInput(
                    event_type="subagent_packet.reserved",
                    aggregate_type="subagent_packet",
                    aggregate_id=packet_id,
                    payload={
                        "session_id": session_id,
                        "tool_use_id": tool_use_id,
                    },
                ),
            )
        return self.get(packet_id)

    def settle(
        self, packet_id: str, *, outcome: str, tool_use_id: str
    ) -> SubagentPacket:
        """CAS ``reserved`` -> ``returned`` (completed) or ``failed``
        (failed/capped); the settling ``tool_use_id`` must match the
        one recorded at reservation. Exactly once."""

        if outcome not in _SETTLEMENT_OUTCOMES:
            raise PacketRefused(f"unknown settlement outcome {outcome!r}")
        target_state = "returned" if outcome == "completed" else "failed"
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE subagent_packets SET state = ?, updated_at = ? "
                "WHERE packet_id = ? AND state = 'reserved' "
                "AND tool_use_id = ?",
                (target_state, stamp, packet_id, tool_use_id),
            )
            if cursor.rowcount != 1:
                raise PacketRefused(
                    f"packet {packet_id!r} cannot be settled by "
                    f"tool_use_id {tool_use_id!r}"
                )
            self._events.append(
                connection,
                EventInput(
                    event_type=f"subagent_packet.{target_state}",
                    aggregate_type="subagent_packet",
                    aggregate_id=packet_id,
                    payload={"outcome": outcome, "tool_use_id": tool_use_id},
                ),
            )
        return self.get(packet_id)

    def accept(
        self, packet_id: str, *, evidence: dict[str, Any]
    ) -> SubagentPacket:
        """CAS ``returned`` -> ``accepted``, storing reviewed evidence
        (diff scope, red/green proof references — never prompt or model
        output text) as JSON. Terminal."""

        stamp = self._now().isoformat()
        evidence_json = json.dumps(evidence, sort_keys=True)
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE subagent_packets SET state = 'accepted', "
                "evidence_json = ?, updated_at = ? "
                "WHERE packet_id = ? AND state = 'returned'",
                (evidence_json, stamp, packet_id),
            )
            if cursor.rowcount != 1:
                raise PacketRefused(f"packet {packet_id!r} is not acceptable")
            self._events.append(
                connection,
                EventInput(
                    event_type="subagent_packet.accepted",
                    aggregate_type="subagent_packet",
                    aggregate_id=packet_id,
                    payload={"evidence_keys": sorted(evidence.keys())},
                ),
            )
        return self.get(packet_id)

    def reject(self, packet_id: str, *, reason: str) -> SubagentPacket:
        """CAS ``returned``|``reserved`` -> ``rejected``. Terminal."""

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE subagent_packets SET state = 'rejected', "
                "updated_at = ? "
                "WHERE packet_id = ? AND state IN ('returned', 'reserved')",
                (stamp, packet_id),
            )
            if cursor.rowcount != 1:
                raise PacketRefused(f"packet {packet_id!r} is not rejectable")
            self._events.append(
                connection,
                EventInput(
                    event_type="subagent_packet.rejected",
                    aggregate_type="subagent_packet",
                    aggregate_id=packet_id,
                    payload={"reason": reason},
                ),
            )
        return self.get(packet_id)

    def expire_orphans(
        self, *, live_tool_use_ids: frozenset[str]
    ) -> tuple[SubagentPacket, ...]:
        """Fail every ``reserved`` packet whose ``tool_use_id`` is not
        in ``live_tool_use_ids``; a live reservation is preserved
        untouched. Returns the expired packets."""

        stamp = self._now().isoformat()
        expired_ids: list[str] = []
        with self._database.transaction() as connection:
            rows = connection.execute(
                "SELECT packet_id, tool_use_id FROM subagent_packets "
                "WHERE state = 'reserved'"
            ).fetchall()
            for row in rows:
                tool_use_id = row["tool_use_id"]
                if tool_use_id is not None and str(tool_use_id) in live_tool_use_ids:
                    continue
                packet_id = str(row["packet_id"])
                cursor = connection.execute(
                    "UPDATE subagent_packets SET state = 'failed', "
                    "updated_at = ? WHERE packet_id = ? AND state = 'reserved'",
                    (stamp, packet_id),
                )
                if cursor.rowcount != 1:
                    continue
                self._events.append(
                    connection,
                    EventInput(
                        event_type="subagent_packet.failed",
                        aggregate_type="subagent_packet",
                        aggregate_id=packet_id,
                        payload={"reason": "orphaned reservation expired"},
                    ),
                )
                expired_ids.append(packet_id)
        return tuple(self.get(packet_id) for packet_id in expired_ids)

    def handoff_summary(self, issue_id: str) -> dict[str, Any]:
        """Reconstruct a lead handoff view from the ledger alone.

        Everything a replacement lead needs to resume orchestration —
        each packet's objective boundary (allowed files, worktree),
        dependency order, verification commands, state, and the KEYS of
        its accepted evidence — derived purely from durable rows, never
        from conversation context, prompts, or model output.
        """

        packets = self.for_issue(issue_id)
        return {
            "issue_id": issue_id,
            "packets": [
                {
                    "packet_id": packet.packet_id,
                    "model_tier": packet.model_tier,
                    "effort": packet.effort,
                    "allowed_files": list(packet.allowed_files),
                    "worktree": packet.worktree,
                    "depends_on": list(packet.depends_on),
                    "red_test": packet.red_test,
                    "verification": list(packet.verification),
                    "invariants": packet.invariants,
                    "state": packet.state,
                    "evidence_keys": (
                        sorted(packet.evidence)
                        if packet.evidence is not None
                        else None
                    ),
                }
                for packet in packets
            ],
            "outstanding": [
                packet.packet_id
                for packet in packets
                if packet.state not in ("accepted", "rejected")
            ],
        }

    def for_issue(self, issue_id: str) -> tuple[SubagentPacket, ...]:
        rows = self._database.execute(
            "SELECT * FROM subagent_packets WHERE issue_id = ? "
            "ORDER BY created_at ASC, rowid ASC",
            (issue_id,),
        ).fetchall()
        return tuple(self._record(row) for row in rows)

    def get(self, packet_id: str) -> SubagentPacket:
        row = self._database.execute(
            "SELECT * FROM subagent_packets WHERE packet_id = ?",
            (packet_id,),
        ).fetchone()
        if row is None:
            raise KeyError(packet_id)
        return self._record(row)

    @staticmethod
    def _record(row: sqlite3.Row) -> SubagentPacket:
        evidence_json = row["evidence_json"]
        return SubagentPacket(
            packet_id=str(row["packet_id"]),
            issue_id=str(row["issue_id"]),
            project_key=str(row["project_key"]),
            cell_id=str(row["cell_id"]),
            session_id=str(row["session_id"]),
            generation=int(row["generation"]),
            model_tier=str(row["model_tier"]),
            effort=str(row["effort"]),
            allowed_files=tuple(json.loads(str(row["allowed_files_json"]))),
            worktree=str(row["worktree"]),
            depends_on=tuple(json.loads(str(row["depends_on_json"]))),
            red_test=str(row["red_test"]),
            verification=tuple(json.loads(str(row["verification_json"]))),
            invariants=str(row["invariants"]),
            resource_note=str(row["resource_note"]),
            state=str(row["state"]),
            evidence=(
                None
                if evidence_json is None
                else dict(json.loads(str(evidence_json)))
            ),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )
