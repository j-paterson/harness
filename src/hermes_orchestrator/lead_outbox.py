"""Durable outbox of correction packets returned to the Claude lead.

INFRA-166: the orchestrator never addresses a Claude lead session directly.
Every Critical or Important packet — from a Codex verdict, a CircleCI
failure at an intake boundary, or a QA rejection — is journaled here, and
Hermes reads pending packets to resume the exact lead with the exact
reviewed SHA and branch. Acknowledgement is explicit and idempotent.

INFRA-193: a correction is only confirmable on proof of a complete read.
``fetch`` is the one supported intake — the whole durable record plus its
``declared_count`` and ``payload_sha256`` — and ``acknowledge`` refuses
unless the confirming caller reproduces both exactly. A lead who read
three of four findings cannot produce the right count, and one who read a
truncated or altered payload cannot produce the right digest.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.manifests import CandidateManifest
from hermes_orchestrator.verdicts import CorrectionPacket


class CorrectionPayloadError(ValueError):
    """The stored packets column is not a complete, parseable record."""


class CorrectionConfirmationRefused(ValueError):
    """A confirmation did not prove a complete read of the record."""


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

    @property
    def declared_count(self) -> int:
        """How many packets are durably stored under this correction."""

        return len(self.packets)

    @property
    def payload_sha256(self) -> str:
        """The digest of the canonically serialized stored packets."""

        return payload_digest(self.packets)

    def as_fetch_document(self) -> dict[str, Any]:
        """The complete record, untruncated, with its integrity metadata."""

        document = self.as_dict()
        document["declared_count"] = self.declared_count
        document["payload_sha256"] = self.payload_sha256
        return document


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
        self._listeners: list[Callable[[LeadCorrection], None]] = []

    def subscribe(self, listener: Callable[[LeadCorrection], None]) -> None:
        """Signal each newly journalled correction after its commit."""

        self._listeners.append(listener)

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
                    canonical_packets_json(packets),
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
        correction = self.get(correction_id)
        for listener in self._listeners:
            try:
                listener(correction)
            except Exception:
                # The durable row is the truth; a failed in-process
                # signal leaves it pending for the repair sweep.
                continue
        return correction

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

    def fetch(self, correction_id: str) -> dict[str, Any]:
        """Return the COMPLETE durable record and its integrity metadata.

        This is the supported intake. Display may shorten what it shows,
        but confirmation and delegation must rest on this document: it
        carries every packet with every field, plus the
        ``declared_count`` and ``payload_sha256`` a confirmation has to
        reproduce. A malformed stored payload fails closed here rather
        than returning a partial document.
        """

        return self.get(correction_id).as_fetch_document()

    def acknowledge(
        self,
        correction_id: str,
        *,
        observed_count: int,
        payload_sha256: str,
    ) -> LeadCorrection:
        """Confirm a correction on proof that the whole record was read.

        Both the observed packet count and the payload digest must match
        the durable record exactly. A mismatch refuses fail-closed with
        zero durable writes: the correction stays ``pending`` and
        retryable after a complete ``fetch``.
        """

        correction = self.get(correction_id)
        declared = correction.declared_count
        if observed_count != declared:
            raise CorrectionConfirmationRefused(
                f"correction {correction_id} declares {declared} packets but the "
                f"confirmation observed {observed_count}; fetch the complete "
                "record with fetch_correction before confirming"
            )
        expected = correction.payload_sha256
        if payload_sha256 != expected:
            raise CorrectionConfirmationRefused(
                f"correction {correction_id} payload_sha256 mismatch: durable "
                f"{expected}, confirmed {payload_sha256}; fetch the complete "
                "record with fetch_correction before confirming"
            )
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


def canonical_packets_json(packets: Sequence[CorrectionPacket]) -> str:
    """The ONE canonical serialization of a correction's packets.

    Durable storage, the fetched ``payload_sha256``, and the digest
    checked at confirmation all go through this function, so a fetch and
    a verification can never disagree about what the payload is.
    """

    return json.dumps(
        [packet_to_dict(packet) for packet in packets],
        sort_keys=True,
        separators=(",", ":"),
    )


def payload_digest(packets: Sequence[CorrectionPacket]) -> str:
    """SHA-256 over the canonical serialization of ``packets``."""

    return hashlib.sha256(canonical_packets_json(packets).encode("utf-8")).hexdigest()


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


#: The exact durable ``CorrectionPacket`` schema written by
#: :func:`packet_to_dict`. A stored packet must carry these keys, with
#: these types, and no others — see :func:`packet_schema_violation`.
_PACKET_TEXT_FIELDS = (
    "severity",
    "repository",
    "branch",
    "reviewed_sha",
    "evidence",
    "acceptance_criterion",
    "required_correction",
)
_PACKET_FIELDS = frozenset({*_PACKET_TEXT_FIELDS, "pr_number", "required_tests"})


def packet_schema_violation(value: Any) -> str | None:
    """Describe how ``value`` departs from the durable packet schema.

    ``None`` means ``value`` is exactly the object :func:`packet_to_dict`
    writes: every field present, every field of exactly the right type,
    and not one key more. Nothing is coerced or normalized here, because
    coercion is a third way to be wrong — neither a parse error nor the
    stored record, but a silent rewrite of it. A ``required_tests``
    string is rejected rather than iterated into single characters, a
    stringified ``pr_number`` is rejected rather than parsed, and an
    unknown stored key is rejected rather than dropped, so a fetched
    packet is the stored packet and never a repair of it.
    """

    if not isinstance(value, dict):
        return f"is not an object (got {type(value).__name__})"
    keys = set(value)
    missing = sorted(_PACKET_FIELDS - keys)
    if missing:
        return "is incomplete: missing " + ", ".join(missing)
    unknown = sorted(keys - _PACKET_FIELDS)
    if unknown:
        return "has unknown field(s): " + ", ".join(unknown)
    for field in _PACKET_TEXT_FIELDS:
        if not isinstance(value[field], str):
            return (
                f"field {field} must be a string, got "
                f"{type(value[field]).__name__}"
            )
    pr_number = value["pr_number"]
    if isinstance(pr_number, bool) or not isinstance(pr_number, int):
        return f"field pr_number must be an integer, got {type(pr_number).__name__}"
    required_tests = value["required_tests"]
    if not isinstance(required_tests, list | tuple) or isinstance(
        required_tests, str | bytes
    ):
        return (
            "field required_tests must be a list of strings, got "
            f"{type(required_tests).__name__}"
        )
    for position, item in enumerate(required_tests):
        if not isinstance(item, str):
            return (
                f"field required_tests[{position}] must be a string, got "
                f"{type(item).__name__}"
            )
    return None


def packet_from_dict(value: dict[str, Any]) -> CorrectionPacket:
    """Rebuild one stored packet, or refuse the whole packet.

    Validation runs before anything is constructed, and the accepted
    values are used as stored — no ``str()``/``int()`` coercion.
    """

    violation = packet_schema_violation(value)
    if violation is not None:
        raise CorrectionPayloadError(f"stored packet {violation}")
    return CorrectionPacket(
        severity=value["severity"],
        repository=value["repository"],
        branch=value["branch"],
        pr_number=value["pr_number"],
        reviewed_sha=value["reviewed_sha"],
        evidence=value["evidence"],
        acceptance_criterion=value["acceptance_criterion"],
        required_correction=value["required_correction"],
        required_tests=tuple(value["required_tests"]),
    )


def _parse_packets(correction_id: str, raw: Any) -> tuple[CorrectionPacket, ...]:
    """Parse the stored packets or fail closed — never a partial record."""

    def refuse(detail: str, error: Exception | None = None) -> CorrectionPayloadError:
        refusal = CorrectionPayloadError(
            f"correction {correction_id} has a malformed stored payload "
            f"({detail}); no partial record is returned"
        )
        if error is not None:
            refusal.__cause__ = error
        return refusal

    try:
        decoded = json.loads(str(raw))
    except ValueError as error:
        raise refuse("unparseable JSON", error) from error
    if not isinstance(decoded, list) or not decoded:
        raise refuse("the packets column is not a non-empty list")
    # Validate the whole stored payload against the exact schema BEFORE
    # any packet is constructed or any digest computed: a violation
    # anywhere yields no document at all, never a coerced one.
    for index, item in enumerate(decoded):
        violation = packet_schema_violation(item)
        if violation is not None:
            raise refuse(f"packet {index} {violation}")
    return tuple(packet_from_dict(item) for item in decoded)


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
        packets=_parse_packets(str(row["correction_id"]), row["packets_json"]),
        state=str(row["state"]),
        created_at=str(row["created_at"]),
    )
