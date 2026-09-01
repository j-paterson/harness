"""Verify the durable correction outbox that returns packets to the lead."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.lead_outbox import (
    CorrectionConfirmationRefused,
    CorrectionPayloadError,
    LeadCorrectionOutbox,
    canonical_packets_json,
    payload_digest,
)
from hermes_orchestrator.verdicts import CorrectionPacket

SHA = "a" * 40
# The width the receiving lead truncated an authoritative payload to
# (`cut -c1-2400`) before confirming three of four findings.
TRUNCATION = 2400
# Four findings, each with a distinguishing marker that must survive the
# supported fetch intact.
FINDINGS = (
    ("Critical", "F1-unbounded-retry"),
    ("Important", "F2-missing-index"),
    ("Important", "F3-silent-except"),
    ("Important", "F4-stale-cache-read"),
)


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


def four_findings() -> tuple[CorrectionPacket, ...]:
    """A genuinely large four-finding correction, as in the incident."""

    return tuple(
        packet(
            severity=severity,
            evidence=f"{marker}: " + f"reviewer evidence for {marker}. " * 40,
            acceptance_criterion=f"{marker} is covered by a regression test",
            required_correction=f"correct {marker} at its call site",
            required_tests=(f"test_{marker.replace('-', '_')}",),
        )
        for severity, marker in FINDINGS
    )


def acknowledged_events(database: Database) -> int:
    return int(
        database.scalar(  # type: ignore[arg-type]
            "SELECT count(*) FROM events "
            "WHERE event_type = 'lead_correction.acknowledged'"
        )
    )


def assert_untouched(database: Database, correction_id: str) -> None:
    """A refused confirmation leaves zero durable trace."""

    row = database.execute(
        "SELECT state, acknowledged_at FROM lead_corrections "
        "WHERE correction_id = ?",
        (correction_id,),
    ).fetchone()
    assert row["state"] == "pending"
    assert row["acknowledged_at"] is None
    assert acknowledged_events(database) == 0


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
    document = outbox.fetch(delivered.correction_id)
    acknowledged = outbox.acknowledge(
        delivered.correction_id,
        observed_count=document["declared_count"],
        payload_sha256=document["payload_sha256"],
    )
    assert acknowledged.state == "acknowledged"
    assert outbox.pending() == ()
    # Replaying the same complete, correct confirmation changes nothing.
    replayed = outbox.acknowledge(
        delivered.correction_id,
        observed_count=document["declared_count"],
        payload_sha256=document["payload_sha256"],
    )
    assert replayed.state == "acknowledged"
    assert acknowledged_events(database) == 1
    with pytest.raises(KeyError):
        outbox.acknowledge(
            "missing", observed_count=1, payload_sha256=document["payload_sha256"]
        )


def test_fetch_returns_the_whole_record_past_a_truncated_read(
    outbox: LeadCorrectionOutbox,
) -> None:
    """INFRA-193: the supported intake is complete by construction.

    The stored payload is far larger than the terminal window the
    receiving lead truncated it to, and the fetch returns every packet
    with every field — including the fourth finding that a `cut -c1-2400`
    read of `packets_json` silently dropped.
    """

    packets = four_findings()
    delivered = outbox.deliver("ENG-9", packets)

    document = outbox.fetch(delivered.correction_id)
    serialized = json.dumps(document, sort_keys=True)
    assert len(serialized) > TRUNCATION * 2

    assert document["declared_count"] == 4
    assert len(document["packets"]) == 4
    assert document["payload_sha256"] == payload_digest(packets)
    assert document["correction_id"] == delivered.correction_id
    assert document["state"] == "pending"
    for _, marker in FINDINGS:
        assert marker in serialized
    # The exact loss: the fourth finding lives beyond the truncation.
    assert "F4-stale-cache-read" not in serialized[:TRUNCATION]
    # Every field of the last packet survives, not just its marker.
    fourth = document["packets"][3]
    assert fourth["severity"] == "Important"
    assert fourth["required_tests"] == ["test_F4_stale_cache_read"]
    assert fourth["acceptance_criterion"].startswith("F4-stale-cache-read")
    assert fourth["evidence"].endswith("reviewer evidence for F4-stale-cache-read. ")


def test_confirmation_short_by_one_finding_is_refused(
    outbox: LeadCorrectionOutbox, database: Database
) -> None:
    """THE RECURRENCE: three findings read, four durably stored.

    A confirmation that reports one fewer packet than is stored is
    refused fail-closed — zero durable writes, still pending, still
    retryable once the whole record has actually been fetched.
    """

    delivered = outbox.deliver("ENG-9", four_findings())
    document = outbox.fetch(delivered.correction_id)

    with pytest.raises(CorrectionConfirmationRefused) as refusal:
        outbox.acknowledge(
            delivered.correction_id,
            observed_count=3,
            payload_sha256=document["payload_sha256"],
        )
    message = str(refusal.value)
    assert "declares 4 packets" in message
    assert "observed 3" in message
    # The refusal names the mismatch without dumping the payload.
    assert "F4-stale-cache-read" not in message

    assert_untouched(database, delivered.correction_id)
    assert [item.correction_id for item in outbox.pending("demo")] == [
        delivered.correction_id
    ]
    # Retryable: the complete, honest confirmation still lands.
    accepted = outbox.acknowledge(
        delivered.correction_id,
        observed_count=document["declared_count"],
        payload_sha256=document["payload_sha256"],
    )
    assert accepted.state == "acknowledged"
    assert acknowledged_events(database) == 1


def test_confirmation_with_a_wrong_digest_is_refused(
    outbox: LeadCorrectionOutbox, database: Database
) -> None:
    """A right count over the wrong bytes is still not a complete read."""

    delivered = outbox.deliver("ENG-9", four_findings())

    with pytest.raises(CorrectionConfirmationRefused) as refusal:
        outbox.acknowledge(
            delivered.correction_id,
            observed_count=4,
            payload_sha256="0" * 64,
        )
    message = str(refusal.value)
    assert "payload_sha256 mismatch" in message
    assert "F4-stale-cache-read" not in message

    assert_untouched(database, delivered.correction_id)


def test_fetch_fails_closed_on_a_malformed_stored_payload(
    outbox: LeadCorrectionOutbox, database: Database
) -> None:
    """A truncated or incomplete row yields no document at all."""

    delivered = outbox.deliver("ENG-9", four_findings())

    def store(payload: str) -> None:
        with database.transaction() as connection:
            connection.execute(
                "UPDATE lead_corrections SET packets_json = ? "
                "WHERE correction_id = ?",
                (payload, delivered.correction_id),
            )

    store(canonical_packets_json(four_findings())[:TRUNCATION])
    with pytest.raises(CorrectionPayloadError, match="unparseable JSON"):
        outbox.fetch(delivered.correction_id)

    store(json.dumps([{"severity": "Critical"}]))
    with pytest.raises(CorrectionPayloadError, match="packet 0 is incomplete"):
        outbox.fetch(delivered.correction_id)

    store(json.dumps([]))
    with pytest.raises(CorrectionPayloadError, match="non-empty list"):
        outbox.fetch(delivered.correction_id)

    # Nothing partial is confirmable either.
    with pytest.raises(CorrectionPayloadError):
        outbox.acknowledge(
            delivered.correction_id, observed_count=1, payload_sha256="0" * 64
        )
    assert acknowledged_events(database) == 0


def stored_packet_dicts() -> list[dict[str, object]]:
    """The exact durable form of a one-packet correction."""

    return list(json.loads(canonical_packets_json((packet(),))))


def test_fetch_fails_closed_on_a_coerced_required_tests_string(
    outbox: LeadCorrectionOutbox, database: Database
) -> None:
    """A `required_tests` STRING is refused, never split into characters.

    Coercion is the third way to be wrong: neither a parse error nor the
    stored record, but a silent rewrite of it. `"test_x"` must not become
    the six single-character "tests" `["t","e","s","t","_","x"]`.
    """

    delivered = outbox.deliver("ENG-9", (packet(),))
    corrupted = stored_packet_dicts()
    corrupted[0]["required_tests"] = "test_x"
    with database.transaction() as connection:
        connection.execute(
            "UPDATE lead_corrections SET packets_json = ? WHERE correction_id = ?",
            (json.dumps(corrupted), delivered.correction_id),
        )

    with pytest.raises(CorrectionPayloadError) as refusal:
        outbox.fetch(delivered.correction_id)
    message = str(refusal.value)
    assert "required_tests must be a list of strings" in message
    assert "no partial record is returned" in message
    # Zero partial document: nothing is returned, and the corruption the
    # coercing reconstruction produced never reaches a caller.
    assert "['t', 'e', 's', 't', '_', 'x']" not in message
    # get() and pending() fail closed identically, and no confirmation is
    # possible over the rewritten payload.
    with pytest.raises(CorrectionPayloadError):
        outbox.get(delivered.correction_id)
    with pytest.raises(CorrectionPayloadError):
        outbox.pending("demo")
    with pytest.raises(CorrectionPayloadError):
        outbox.acknowledge(
            delivered.correction_id, observed_count=1, payload_sha256="0" * 64
        )
    assert acknowledged_events(database) == 0


def test_fetch_fails_closed_on_an_unknown_stored_packet_field(
    outbox: LeadCorrectionOutbox, database: Database
) -> None:
    """An extra stored field is refused, never silently dropped.

    A field present in the durable row but absent from the fetched
    document would make the fetch a lossy summary of the record the lead
    delegates from.
    """

    delivered = outbox.deliver("ENG-9", (packet(),))
    corrupted = stored_packet_dicts()
    corrupted[0]["reviewer_note"] = "F5-unknown-field"
    with database.transaction() as connection:
        connection.execute(
            "UPDATE lead_corrections SET packets_json = ? WHERE correction_id = ?",
            (json.dumps(corrupted), delivered.correction_id),
        )

    with pytest.raises(CorrectionPayloadError) as refusal:
        outbox.fetch(delivered.correction_id)
    message = str(refusal.value)
    assert "unknown field(s): reviewer_note" in message
    assert "no partial record is returned" in message

    with pytest.raises(CorrectionPayloadError):
        outbox.get(delivered.correction_id)
    with pytest.raises(CorrectionPayloadError):
        outbox.pending("demo")
    with pytest.raises(CorrectionPayloadError):
        outbox.acknowledge(
            delivered.correction_id, observed_count=1, payload_sha256="0" * 64
        )
    assert acknowledged_events(database) == 0


def test_fetch_fails_closed_on_wrong_stored_scalar_types(
    outbox: LeadCorrectionOutbox, database: Database
) -> None:
    """Wrong scalar types are rejected, not normalized into the record."""

    delivered = outbox.deliver("ENG-9", (packet(),))

    def store_field(field: str, value: object) -> None:
        corrupted = stored_packet_dicts()
        corrupted[0][field] = value
        with database.transaction() as connection:
            connection.execute(
                "UPDATE lead_corrections SET packets_json = ? "
                "WHERE correction_id = ?",
                (json.dumps(corrupted), delivered.correction_id),
            )

    store_field("pr_number", "14")
    with pytest.raises(CorrectionPayloadError, match="pr_number must be an integer"):
        outbox.fetch(delivered.correction_id)

    store_field("severity", 3)
    with pytest.raises(CorrectionPayloadError, match="field severity must be a string"):
        outbox.fetch(delivered.correction_id)

    store_field("required_tests", ["test_ok", 7])
    with pytest.raises(
        CorrectionPayloadError, match=r"required_tests\[1\] must be a string"
    ):
        outbox.fetch(delivered.correction_id)

    store_field("required_tests", {"0": "test_ok"})
    with pytest.raises(
        CorrectionPayloadError, match="required_tests must be a list of strings"
    ):
        outbox.fetch(delivered.correction_id)


def test_delivery_requires_one_bound_candidate(outbox: LeadCorrectionOutbox) -> None:
    with pytest.raises(ValueError, match="at least one"):
        outbox.deliver("ENG-9", ())
    with pytest.raises(ValueError, match="same candidate"):
        outbox.deliver("ENG-9", (packet(), packet(reviewed_sha="b" * 40)))
