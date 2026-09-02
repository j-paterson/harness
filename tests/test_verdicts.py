"""Verify strict structured review verdict and correction packet parsing.

INFRA-217: the reviewer document carries no ``pr_number`` anywhere —
PR identity is discovered from GitHub by the submission path and
stamped onto the parsed verdict via :meth:`ReviewVerdict.with_pr_number`.
A document still carrying the retired field fails the strict key check.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.verdicts import (
    IDLE_TERMINAL_REPORT,
    VERDICT_LABELS,
    CorrectionPacket,
    ReviewVerdict,
    VerdictBinding,
    VerdictError,
    normalize_verdict,
    parse_turn_report,
    parse_verdict,
)

SCHEMAS = Path(__file__).parent.parent / "schemas"
SHA = "a" * 40


def packet(**overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "severity": "Important",
        "repository": "j-paterson/demo",
        "branch": "feature/eng-9",
        "reviewed_sha": SHA,
        "evidence": "delivery claims success on a replaced thread",
        "acceptance_criterion": "stale CAS must fail closed",
        "required_correction": "bind recording to thread id and generation",
        "required_tests": ["test_old_thread_success_is_never_claimed"],
    }
    value.update(overrides)
    return value


def verdict(**overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "verdict": "corrections_required",
        "repository": "j-paterson/demo",
        "branch": "feature/eng-9",
        "reviewed_sha": SHA,
        "packets": [packet()],
    }
    value.update(overrides)
    return value


def binding(**overrides: Any) -> VerdictBinding:
    fields: dict[str, Any] = {
        "repository": "j-paterson/demo",
        "branch": "feature/eng-9",
        "reviewed_sha": SHA,
    }
    fields.update(overrides)
    return VerdictBinding(**fields)


def test_parses_a_corrections_required_verdict() -> None:
    parsed = parse_verdict(json.dumps(verdict()), expected=binding())

    assert isinstance(parsed, ReviewVerdict)
    assert parsed.verdict == "corrections_required"
    assert parsed.packets == (
        CorrectionPacket(
            severity="Important",
            repository="j-paterson/demo",
            branch="feature/eng-9",
            pr_number=0,
            reviewed_sha=SHA,
            evidence="delivery claims success on a replaced thread",
            acceptance_criterion="stale CAS must fail closed",
            required_correction=(
                "bind recording to thread id and generation"
            ),
            required_tests=("test_old_thread_success_is_never_claimed",),
        ),
    )


def test_parses_an_approved_verdict_without_packets() -> None:
    parsed = parse_verdict(
        json.dumps(verdict(verdict="approved", packets=[])),
        expected=binding(),
    )
    assert parsed.verdict == "approved"
    assert parsed.packets == ()


def test_findings_always_force_corrections_required() -> None:
    with pytest.raises(VerdictError, match="corrections_required"):
        parse_verdict(
            json.dumps(verdict(verdict="approved")), expected=binding()
        )
    with pytest.raises(VerdictError, match="packet"):
        parse_verdict(
            json.dumps(verdict(packets=[])), expected=binding()
        )


def test_rejects_malformed_documents() -> None:
    for text in ("not json", '"a string"', "[]"):
        with pytest.raises(VerdictError):
            parse_verdict(text, expected=binding())
    with pytest.raises(VerdictError, match="verdict"):
        parse_verdict(
            json.dumps(verdict(verdict="maybe", packets=[])),
            expected=binding(),
        )
    with pytest.raises(VerdictError, match="surprise"):
        parse_verdict(json.dumps(verdict(surprise=True)), expected=binding())
    missing = verdict()
    del missing["packets"]
    with pytest.raises(VerdictError, match="packets"):
        parse_verdict(json.dumps(missing), expected=binding())


def test_documents_carrying_the_retired_pr_number_are_rejected() -> None:
    # INFRA-217: pr_number is no longer reviewer-supplied; the strict
    # key check rejects it in the envelope and in every packet.
    with pytest.raises(VerdictError, match="pr_number"):
        parse_verdict(json.dumps(verdict(pr_number=7)), expected=binding())
    document = verdict(packets=[packet(pr_number=7)])
    with pytest.raises(VerdictError, match="pr_number"):
        parse_verdict(json.dumps(document), expected=binding())


def test_with_pr_number_stamps_envelope_and_packets() -> None:
    parsed = parse_verdict(json.dumps(verdict()), expected=binding())
    assert parsed.pr_number == 0
    assert parsed.packets[0].pr_number == 0

    stamped = parsed.with_pr_number(14)
    assert stamped.pr_number == 14
    assert all(entry.pr_number == 14 for entry in stamped.packets)
    # Everything else is untouched, and the original is not mutated.
    assert stamped.reviewed_sha == parsed.reviewed_sha
    assert parsed.pr_number == 0


@pytest.mark.parametrize(
    "override",
    [
        {"severity": "Minor"},
        {"reviewed_sha": "abc"},
        {"evidence": ""},
        {"required_tests": []},
        {"required_tests": [""]},
        {"unexpected": "field"},
    ],
)
def test_rejects_invalid_correction_packets(override: dict[str, Any]) -> None:
    document = verdict(packets=[packet(**override)])
    with pytest.raises(VerdictError):
        parse_verdict(json.dumps(document), expected=binding())


def test_rejects_stale_or_foreign_verdicts() -> None:
    for override, expectation in (
        ({"repository": "j-paterson/other"}, "repository"),
        ({"branch": "feature/other"}, "branch"),
        ({"reviewed_sha": "b" * 40}, "reviewed_sha"),
    ):
        document = verdict(**override)
        with pytest.raises(VerdictError, match=expectation):
            parse_verdict(json.dumps(document), expected=binding())


def test_rejects_packets_that_disagree_with_the_envelope() -> None:
    for override in (
        {"repository": "j-paterson/other"},
        {"branch": "feature/other"},
        {"reviewed_sha": "b" * 40},
    ):
        document = verdict(packets=[packet(**override)])
        with pytest.raises(VerdictError, match="admitted candidate"):
            parse_verdict(json.dumps(document), expected=binding())


def test_verdict_carries_the_admitted_candidate_binding() -> None:
    parsed = parse_verdict(json.dumps(verdict()), expected=binding())
    assert parsed.repository == "j-paterson/demo"
    assert parsed.branch == "feature/eng-9"
    assert parsed.pr_number == 0
    assert parsed.reviewed_sha == SHA


def test_canonical_labels_normalize_to_durable_values() -> None:
    # INFRA-200: ACCEPT, ACCEPT_WITH_REVIEWER_FIX, and REWORK_REQUIRED
    # all normalize to the durable value already recorded in state.
    assert normalize_verdict("ACCEPT") == ("approved", {"label": "ACCEPT"})
    assert normalize_verdict("ACCEPT_WITH_REVIEWER_FIX") == (
        "approved",
        {"label": "ACCEPT_WITH_REVIEWER_FIX", "reviewer_fix": True},
    )
    assert normalize_verdict("REWORK_REQUIRED") == (
        "corrections_required",
        {"label": "REWORK_REQUIRED"},
    )
    assert VERDICT_LABELS == {
        "ACCEPT": "approved",
        "ACCEPT_WITH_REVIEWER_FIX": "approved",
        "REWORK_REQUIRED": "corrections_required",
    }

    # The durable values already recorded in state normalize to
    # themselves, unchanged, so every existing read path keeps working.
    assert normalize_verdict("approved") == ("approved", {"label": "approved"})
    assert normalize_verdict("corrections_required") == (
        "corrections_required",
        {"label": "corrections_required"},
    )

    with pytest.raises(VerdictError, match="unknown verdict"):
        normalize_verdict("REJECTED")


def test_parse_verdict_accepts_canonical_labels() -> None:
    # INFRA-200: parse_verdict itself — the actual inbound entry point
    # a submitted Sol verdict goes through — must accept the three
    # canonical labels, not just normalize_verdict in isolation.
    accept = parse_verdict(
        json.dumps(verdict(verdict="ACCEPT", packets=[])),
        expected=binding(),
    )
    assert accept.verdict == "approved"
    accept_payload = json.loads(accept.verdict_json)
    assert accept_payload["verdict"] == "approved"
    assert accept_payload["label"] == "ACCEPT"
    assert "reviewer_fix" not in accept_payload

    fix = parse_verdict(
        json.dumps(verdict(verdict="ACCEPT_WITH_REVIEWER_FIX", packets=[])),
        expected=binding(),
    )
    assert fix.verdict == "approved"
    fix_payload = json.loads(fix.verdict_json)
    assert fix_payload["verdict"] == "approved"
    assert fix_payload["label"] == "ACCEPT_WITH_REVIEWER_FIX"
    assert fix_payload["reviewer_fix"] is True

    rework = parse_verdict(
        json.dumps(verdict(verdict="REWORK_REQUIRED")), expected=binding()
    )
    assert rework.verdict == "corrections_required"
    rework_payload = json.loads(rework.verdict_json)
    assert rework_payload["verdict"] == "corrections_required"
    assert rework_payload["label"] == "REWORK_REQUIRED"
    assert "reviewer_fix" not in rework_payload

    # Every field the strict envelope requires survives the merge, and
    # the packets themselves parse exactly as with a durable-value
    # document.
    assert rework.packets == parse_verdict(
        json.dumps(verdict(verdict="corrections_required")),
        expected=binding(),
    ).packets
    assert rework_payload["repository"] == "j-paterson/demo"
    assert rework_payload["branch"] == "feature/eng-9"
    assert rework_payload["reviewed_sha"] == SHA

    # An unknown label still fails closed exactly as before.
    with pytest.raises(VerdictError, match="unknown verdict"):
        parse_verdict(
            json.dumps(verdict(verdict="REJECTED", packets=[])),
            expected=binding(),
        )


def test_reviewer_fix_label_is_retained_in_verdict_json() -> None:
    # No schema change: parse_verdict merges normalize_verdict's marker
    # into the SAME envelope dict it already parsed — no field is
    # dropped or renamed — and exposes it as ReviewVerdict.verdict_json,
    # the exact payload a caller persists into
    # submitted_verdicts.verdict_json to retain the original label.
    document = verdict(verdict="ACCEPT_WITH_REVIEWER_FIX", packets=[])
    parsed = parse_verdict(json.dumps(document), expected=binding())

    assert parsed.verdict == "approved"
    stored = json.loads(parsed.verdict_json)
    assert stored["verdict"] == "approved"
    assert stored["label"] == "ACCEPT_WITH_REVIEWER_FIX"
    assert stored["reviewer_fix"] is True
    assert stored["repository"] == document["repository"]
    assert stored["branch"] == document["branch"]
    assert stored["reviewed_sha"] == document["reviewed_sha"]
    assert stored["packets"] == document["packets"]

    # REWORK_REQUIRED retains its label with no reviewer_fix marker.
    rework = parse_verdict(
        json.dumps(verdict(verdict="REWORK_REQUIRED")), expected=binding()
    )
    rework_stored = json.loads(rework.verdict_json)
    assert rework_stored["label"] == "REWORK_REQUIRED"
    assert "reviewer_fix" not in rework_stored

    # Existing durable-value documents parse exactly as before: the
    # marker's label merely restates the verdict, with no reviewer_fix
    # key added — a no-op merge in substance.
    plain = parse_verdict(json.dumps(verdict()), expected=binding())
    plain_stored = json.loads(plain.verdict_json)
    assert plain_stored["verdict"] == "corrections_required"
    assert plain_stored["label"] == "corrections_required"
    assert "reviewer_fix" not in plain_stored


def test_idle_terminal_report_is_exact_and_leaves_no_continuation() -> None:
    assert IDLE_TERMINAL_REPORT == "BLOCKED_ON_EXTERNAL_INTAKE"

    idle = parse_turn_report(
        "BLOCKED_ON_EXTERNAL_INTAKE", expected=binding()
    )
    assert idle is None

    idle = parse_turn_report(
        "  BLOCKED_ON_EXTERNAL_INTAKE\n", expected=binding()
    )
    assert idle is None

    parsed = parse_turn_report(json.dumps(verdict()), expected=binding())
    assert isinstance(parsed, ReviewVerdict)


def test_forbidden_pause_and_advisory_reports_fail_closed() -> None:
    with pytest.raises(VerdictError, match="forbidden"):
        parse_turn_report("PAUSED_NO_ELIGIBLE_WORK", expected=binding())
    with pytest.raises(VerdictError):
        parse_turn_report(
            "BLOCKED_ON_EXTERNAL_INTAKE but I will keep checking",
            expected=binding(),
        )
    with pytest.raises(VerdictError):
        parse_turn_report(
            "still waiting for eligible work", expected=binding()
        )


def test_schema_files_mirror_the_typed_contract() -> None:
    verdict_schema = json.loads(
        (SCHEMAS / "review-verdict.json").read_text(encoding="utf-8")
    )
    packet_schema = json.loads(
        (SCHEMAS / "correction-packet.json").read_text(encoding="utf-8")
    )

    assert verdict_schema["properties"]["verdict"]["enum"] == [
        "approved",
        "corrections_required",
    ]
    assert verdict_schema["additionalProperties"] is False
    assert set(verdict_schema["required"]) == {
        "verdict",
        "repository",
        "branch",
        "reviewed_sha",
        "packets",
    }
    assert "pr_number" not in verdict_schema["properties"]
    assert packet_schema["properties"]["severity"]["enum"] == [
        "Critical",
        "Important",
    ]
    assert packet_schema["additionalProperties"] is False
    assert "pr_number" not in packet_schema["properties"]
    assert set(packet_schema["required"]) == set(
        packet_schema["properties"]
    )
