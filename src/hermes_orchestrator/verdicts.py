"""Strict structured review verdicts and correction packets."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

VERDICTS = ("approved", "corrections_required")
SEVERITIES = ("Critical", "Important")

# The exact terminal/idle turn report. With no eligible intake the Merger
# completes its turn with this token and leaves no active goal continuation;
# only a validated explicit FABLE_* queue wake may reactivate it, and the
# formerly used pause token is forbidden because it produced repeated empty
# continuation turns.
IDLE_TERMINAL_REPORT = "BLOCKED_ON_EXTERNAL_INTAKE"
_FORBIDDEN_PAUSE_REPORT = "PAUSED_NO_ELIGIBLE_WORK"
_ENVELOPE_KEYS = frozenset(
    {
        "verdict",
        "repository",
        "branch",
        "pr_number",
        "reviewed_sha",
        "packets",
    }
)
_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_PACKET_KEYS = frozenset(
    {
        "severity",
        "repository",
        "branch",
        "pr_number",
        "reviewed_sha",
        "evidence",
        "acceptance_criterion",
        "required_correction",
        "required_tests",
    }
)


class VerdictError(ValueError):
    """Raised for any structured verdict that violates the schema."""


@dataclass(frozen=True, slots=True)
class CorrectionPacket:
    """One structured Critical or Important finding returned to the lead."""

    severity: str
    repository: str
    branch: str
    pr_number: int
    reviewed_sha: str
    evidence: str
    acceptance_criterion: str
    required_correction: str
    required_tests: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class VerdictBinding:
    """The admitted candidate identity every verdict must bind exactly."""

    repository: str
    branch: str
    pr_number: int
    reviewed_sha: str


@dataclass(frozen=True, slots=True)
class ReviewVerdict:
    """The Merger's structured outcome for one reviewed candidate."""

    verdict: str
    repository: str
    branch: str
    pr_number: int
    reviewed_sha: str
    packets: tuple[CorrectionPacket, ...]


def parse_turn_report(
    text: str, *, expected: VerdictBinding
) -> ReviewVerdict | None:
    """Parse one completed Merger turn report.

    Returns ``None`` for the exact terminal/idle ``BLOCKED_ON_EXTERNAL_INTAKE``
    report — the turn is complete with no active goal continuation — or the
    bound structured verdict. The forbidden pause token and any advisory
    free text fail closed.
    """

    stripped = text.strip()
    if stripped == IDLE_TERMINAL_REPORT:
        return None
    if _FORBIDDEN_PAUSE_REPORT in stripped:
        raise VerdictError(
            "forbidden pause report: an idle Merger must complete the turn "
            f"with {IDLE_TERMINAL_REPORT} and no goal continuation"
        )
    return parse_verdict(text, expected=expected)


def parse_verdict(text: str, *, expected: VerdictBinding) -> ReviewVerdict:
    """Parse one structured verdict bound to the admitted candidate.

    Stale or foreign verdicts — any envelope or packet whose repository,
    branch, pull request, or reviewed SHA differs from the admitted
    candidate — fail closed.
    """

    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        raise VerdictError("verdict is not valid JSON") from error
    if not isinstance(value, dict):
        raise VerdictError("verdict root must be an object")
    if set(value) != _ENVELOPE_KEYS:
        unexpected = sorted(set(value) ^ _ENVELOPE_KEYS)[0]
        raise VerdictError(f"verdict document field {unexpected} is invalid")
    verdict = value["verdict"]
    if verdict not in VERDICTS:
        raise VerdictError(f"unknown verdict {verdict!r}")
    binding = _parse_binding(value)
    for field in ("repository", "branch", "pr_number", "reviewed_sha"):
        if getattr(binding, field) != getattr(expected, field):
            raise VerdictError(
                f"verdict {field} does not match the admitted candidate"
            )
    raw_packets = value["packets"]
    if not isinstance(raw_packets, list):
        raise VerdictError("verdict packets must be an array")
    packets = tuple(_parse_packet(entry) for entry in raw_packets)
    for packet in packets:
        if (
            packet.repository != expected.repository
            or packet.branch != expected.branch
            or packet.pr_number != expected.pr_number
            or packet.reviewed_sha != expected.reviewed_sha
        ):
            raise VerdictError(
                "correction packet does not match the admitted candidate"
            )
    if verdict == "approved" and packets:
        raise VerdictError(
            "any Critical or Important finding requires "
            "corrections_required"
        )
    if verdict == "corrections_required" and not packets:
        raise VerdictError(
            "corrections_required requires at least one correction packet"
        )
    return ReviewVerdict(
        verdict=verdict,
        repository=binding.repository,
        branch=binding.branch,
        pr_number=binding.pr_number,
        reviewed_sha=binding.reviewed_sha,
        packets=packets,
    )


def _parse_binding(value: dict[str, Any]) -> VerdictBinding:
    for field in ("repository", "branch"):
        if not isinstance(value[field], str) or not value[field]:
            raise VerdictError(f"verdict {field} is invalid")
    pr_number = value["pr_number"]
    if not isinstance(pr_number, int) or isinstance(pr_number, bool) or (
        pr_number < 1
    ):
        raise VerdictError("verdict pr_number is invalid")
    reviewed_sha = value["reviewed_sha"]
    if (
        not isinstance(reviewed_sha, str)
        or _SHA_PATTERN.match(reviewed_sha) is None
    ):
        raise VerdictError("verdict reviewed_sha is invalid")
    return VerdictBinding(
        repository=value["repository"],
        branch=value["branch"],
        pr_number=pr_number,
        reviewed_sha=reviewed_sha,
    )


def _parse_packet(entry: Any) -> CorrectionPacket:
    if not isinstance(entry, dict):
        raise VerdictError("correction packet must be an object")
    if set(entry) != _PACKET_KEYS:
        unexpected = sorted(set(entry) ^ _PACKET_KEYS)[0]
        raise VerdictError(
            f"correction packet field {unexpected} is invalid"
        )
    if entry["severity"] not in SEVERITIES:
        raise VerdictError(
            f"unknown correction severity {entry['severity']!r}"
        )
    for field in (
        "repository",
        "branch",
        "evidence",
        "acceptance_criterion",
        "required_correction",
    ):
        if not isinstance(entry[field], str) or not entry[field]:
            raise VerdictError(f"correction packet {field} is invalid")
    pr_number = entry["pr_number"]
    if not isinstance(pr_number, int) or isinstance(pr_number, bool) or (
        pr_number < 1
    ):
        raise VerdictError("correction packet pr_number is invalid")
    reviewed_sha = entry["reviewed_sha"]
    if (
        not isinstance(reviewed_sha, str)
        or _SHA_PATTERN.match(reviewed_sha) is None
    ):
        raise VerdictError("correction packet reviewed_sha is invalid")
    tests = entry["required_tests"]
    if (
        not isinstance(tests, list)
        or not tests
        or not all(isinstance(item, str) and item for item in tests)
    ):
        raise VerdictError("correction packet required_tests is invalid")
    return CorrectionPacket(
        severity=entry["severity"],
        repository=entry["repository"],
        branch=entry["branch"],
        pr_number=pr_number,
        reviewed_sha=reviewed_sha,
        evidence=entry["evidence"],
        acceptance_criterion=entry["acceptance_criterion"],
        required_correction=entry["required_correction"],
        required_tests=tuple(tests),
    )
