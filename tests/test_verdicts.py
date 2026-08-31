"""Verify strict structured review verdict and correction packet parsing."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.verdicts import (
    IDLE_TERMINAL_REPORT,
    CorrectionPacket,
    ReviewVerdict,
    VerdictBinding,
    VerdictError,
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
        "pr_number": 7,
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
        "pr_number": 7,
        "reviewed_sha": SHA,
        "packets": [packet()],
    }
    value.update(overrides)
    return value


def binding(**overrides: Any) -> VerdictBinding:
    fields: dict[str, Any] = {
        "repository": "j-paterson/demo",
        "branch": "feature/eng-9",
        "pr_number": 7,
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
            pr_number=7,
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


@pytest.mark.parametrize(
    "override",
    [
        {"severity": "Minor"},
        {"pr_number": "seven"},
        {"pr_number": 0},
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
        ({"pr_number": 9}, "pr_number"),
        ({"reviewed_sha": "b" * 40}, "reviewed_sha"),
    ):
        document = verdict(**override)
        with pytest.raises(VerdictError, match=expectation):
            parse_verdict(json.dumps(document), expected=binding())


def test_rejects_packets_that_disagree_with_the_envelope() -> None:
    for override in (
        {"repository": "j-paterson/other"},
        {"branch": "feature/other"},
        {"pr_number": 9},
        {"reviewed_sha": "b" * 40},
    ):
        document = verdict(packets=[packet(**override)])
        with pytest.raises(VerdictError, match="admitted candidate"):
            parse_verdict(json.dumps(document), expected=binding())


def test_verdict_carries_the_admitted_candidate_binding() -> None:
    parsed = parse_verdict(json.dumps(verdict()), expected=binding())
    assert parsed.repository == "j-paterson/demo"
    assert parsed.branch == "feature/eng-9"
    assert parsed.pr_number == 7
    assert parsed.reviewed_sha == SHA


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


def test_zero_pr_number_parses_for_corrections_required_against_a_zero_binding() -> (
    None
):
    # INFRA-202: Sol may submit corrections_required with no pull request
    # open yet; pr_number 0 is well-formed in both the envelope and every
    # packet when it matches the admitted binding's pr_number.
    document = verdict(pr_number=0, packets=[packet(pr_number=0)])
    parsed = parse_verdict(json.dumps(document), expected=binding(pr_number=0))

    assert parsed.pr_number == 0
    assert parsed.packets[0].pr_number == 0


def test_approved_with_zero_pr_number_is_refused() -> None:
    document = verdict(verdict="approved", pr_number=0, packets=[])
    with pytest.raises(VerdictError, match="approval requires the sole open"):
        parse_verdict(json.dumps(document), expected=binding(pr_number=0))


@pytest.mark.parametrize(
    "document_pr_number,expected_pr_number",
    [(0, 14), (14, 0)],
)
def test_zero_and_nonzero_pr_number_never_match_each_other(
    document_pr_number: int, expected_pr_number: int
) -> None:
    document = verdict(
        pr_number=document_pr_number,
        packets=[packet(pr_number=document_pr_number)],
    )
    with pytest.raises(VerdictError, match="does not match"):
        parse_verdict(
            json.dumps(document), expected=binding(pr_number=expected_pr_number)
        )


@pytest.mark.parametrize("bad", [-1, True, False, 7.0])
def test_negative_bool_and_float_pr_number_are_still_invalid(bad: object) -> None:
    document = verdict(pr_number=bad)
    with pytest.raises(VerdictError, match="pr_number"):
        parse_verdict(json.dumps(document), expected=binding(pr_number=7))

    packet_document = verdict(packets=[packet(pr_number=bad)])
    with pytest.raises(VerdictError):
        parse_verdict(json.dumps(packet_document), expected=binding())


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
        "pr_number",
        "reviewed_sha",
        "packets",
    }
    assert packet_schema["properties"]["severity"]["enum"] == [
        "Critical",
        "Important",
    ]
    assert packet_schema["additionalProperties"] is False
    assert set(packet_schema["required"]) == set(
        packet_schema["properties"]
    )
