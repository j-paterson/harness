"""Durable outbox of correction packets returned to the Claude lead.

INFRA-166: the orchestrator never addresses a Claude lead session directly.
Every Critical or Important packet — from a Codex verdict, a CircleCI
failure at an intake boundary, or a QA rejection — is journaled here, and
Hermes reads pending packets to resume the exact lead with the exact
reviewed SHA and branch. Acknowledgement is explicit and idempotent.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.manifests import CandidateManifest
from hermes_orchestrator.verdicts import CorrectionPacket


@dataclass(frozen=True, slots=True)
class LeadCorrection:
    """One pending or acknowledged correction delivery."""

    correction_id: str
    project_key: str
    issue_id: str
    source: str
    repository: str
    branch: str
    pr_number: int
    reviewed_sha: str
    packets: tuple[CorrectionPacket, ...]
    state: str
    created_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "correction_id": self.correction_id,
            "project_key": self.project_key,
            "issue_id": self.issue_id,
            "source": self.source,
            "repository": self.repository,
            "branch": self.branch,
            "pr_number": self.pr_number,
            "reviewed_sha": self.reviewed_sha,
            "packets": [packet_to_dict(packet) for packet in self.packets],
            "state": self.state,
            "created_at": self.created_at,
        }


class LeadCorrectionOutbox:
    """Journal correction packets for one issue; implements the lead port."""

    def __init__(
        self,
        *,
        database: Database,
        events: EventStore,
        project_for_issue: Callable[[str], str],
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._project_for_issue = project_for_issue
        self._now = now or (lambda: datetime.now(UTC))
        self._ids = ids or (lambda: uuid.uuid4().hex)

    def deliver(
        self,
        issue_id: str,
        packets: tuple[CorrectionPacket, ...],
        *,
        source: str = "codex_review",
    ) -> LeadCorrection:
        if not packets:
            raise ValueError("a correction delivery requires at least one packet")
        first = packets[0]
        for packet in packets[1:]:
            if (
                packet.repository != first.repository
                or packet.branch != first.branch
                or packet.pr_number != first.pr_number
                or packet.reviewed_sha != first.reviewed_sha
            ):
                raise ValueError("all packets must bind the same candidate")
        project_key = self._project_for_issue(issue_id)
        existing = self._database.execute(
            "SELECT correction_id FROM lead_corrections WHERE project_key = ? "
            "AND issue_id = ? AND source = ? AND reviewed_sha = ? "
            "AND state = 'pending' ORDER BY created_at ASC, rowid ASC LIMIT 1",
            (project_key, issue_id, source, first.reviewed_sha),
        ).fetchone()
        if existing is not None:
            return self.get(str(existing["correction_id"]))
        correction_id = self._ids()
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO lead_corrections("
                "correction_id, project_key, issue_id, source, repository, "
                "branch, pr_number, reviewed_sha, packets_json, state, "
                "created_at, acknowledged_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL)",
                (
                    correction_id,
                    project_key,
                    issue_id,
                    source,
                    first.repository,
                    first.branch,
                    first.pr_number,
                    first.reviewed_sha,
                    json.dumps(
                        [packet_to_dict(packet) for packet in packets],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    stamp,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="lead_correction.queued",
                    aggregate_type="issue",
                    aggregate_id=issue_id,
                    correlation_id=correction_id,
                    payload={
                        "source": source,
                        "reviewed_sha": first.reviewed_sha,
                        "branch": first.branch,
                        "packet_count": len(packets),
                        "severities": [packet.severity for packet in packets],
                    },
                ),
            )
        return self.get(correction_id)

    def pending(self, project_key: str | None = None) -> tuple[LeadCorrection, ...]:
        if project_key is None:
            rows = self._database.execute(
                "SELECT * FROM lead_corrections WHERE state = 'pending' "
                "ORDER BY created_at ASC, rowid ASC"
            ).fetchall()
        else:
            rows = self._database.execute(
                "SELECT * FROM lead_corrections WHERE state = 'pending' "
                "AND project_key = ? ORDER BY created_at ASC, rowid ASC",
                (project_key,),
            ).fetchall()
        return tuple(_row_to_correction(row) for row in rows)

    def acknowledge(self, correction_id: str) -> LeadCorrection:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE lead_corrections SET state = 'acknowledged', "
                "acknowledged_at = ? WHERE correction_id = ? "
                "AND state = 'pending'",
                (stamp, correction_id),
            )
            if cursor.rowcount == 1:
                self._events.append(
                    connection,
                    EventInput(
                        event_type="lead_correction.acknowledged",
                        aggregate_type="lead_correction",
                        aggregate_id=correction_id,
                        actor="operator",
                        payload={},
                    ),
                )
        return self.get(correction_id)

    def authorized_rework(
        self, project_key: str, candidate: CandidateManifest, packet: CorrectionPacket
    ) -> str | None:
        """Return the correction id that explicitly authorizes ``candidate``.

        Only a ``FABLE_REWORK_READY`` candidate on the packet's exact branch
        with a different SHA, whose issue received a CircleCI-failure
        correction for the packet's exact reviewed SHA, is an authorized
        rework. Ordinary, stale, foreign, or unbound candidates return
        ``None`` and change nothing.
        """

        if (
            candidate.status != "FABLE_REWORK_READY"
            or candidate.branch != packet.branch
            or candidate.candidate_sha == packet.reviewed_sha
        ):
            return None
        placeholders = ",".join("?" for _ in candidate.linear_issues)
        row = self._database.execute(
            "SELECT correction_id FROM lead_corrections WHERE project_key = ? "
            "AND source = 'ci_failure' AND reviewed_sha = ? AND branch = ? "
            f"AND issue_id IN ({placeholders}) "
            "AND state IN ('pending', 'acknowledged') "
            "ORDER BY created_at ASC, rowid ASC LIMIT 1",
            (project_key, packet.reviewed_sha, packet.branch, *candidate.linear_issues),
        ).fetchone()
        return None if row is None else str(row["correction_id"])

    def get(self, correction_id: str) -> LeadCorrection:
        row = self._database.execute(
            "SELECT * FROM lead_corrections WHERE correction_id = ?",
            (correction_id,),
        ).fetchone()
        if row is None:
            raise KeyError(correction_id)
        return _row_to_correction(row)


def packet_to_dict(packet: CorrectionPacket) -> dict[str, Any]:
    return {
        "severity": packet.severity,
        "repository": packet.repository,
        "branch": packet.branch,
        "pr_number": packet.pr_number,
        "reviewed_sha": packet.reviewed_sha,
        "evidence": packet.evidence,
        "acceptance_criterion": packet.acceptance_criterion,
        "required_correction": packet.required_correction,
        "required_tests": list(packet.required_tests),
    }


def packet_from_dict(value: dict[str, Any]) -> CorrectionPacket:
    return CorrectionPacket(
        severity=str(value["severity"]),
        repository=str(value["repository"]),
        branch=str(value["branch"]),
        pr_number=int(value["pr_number"]),
        reviewed_sha=str(value["reviewed_sha"]),
        evidence=str(value["evidence"]),
        acceptance_criterion=str(value["acceptance_criterion"]),
        required_correction=str(value["required_correction"]),
        required_tests=tuple(str(item) for item in value["required_tests"]),
    )


def _row_to_correction(row: Any) -> LeadCorrection:
    return LeadCorrection(
        correction_id=str(row["correction_id"]),
        project_key=str(row["project_key"]),
        issue_id=str(row["issue_id"]),
        source=str(row["source"]),
        repository=str(row["repository"]),
        branch=str(row["branch"]),
        pr_number=int(row["pr_number"]),
        reviewed_sha=str(row["reviewed_sha"]),
        packets=tuple(
            packet_from_dict(item) for item in json.loads(str(row["packets_json"]))
        ),
        state=str(row["state"]),
        created_at=str(row["created_at"]),
    )
