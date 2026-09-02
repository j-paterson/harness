"""Strict structured review verdicts and correction packets.

INFRA-217: ``pr_number`` is retired as reviewer-supplied authority. The
reviewer's document asserts only ``verdict``/``repository``/``branch``/
``reviewed_sha``/``packets`` (and each packet's own
severity/repository/branch/reviewed_sha/evidence/etc); PR and merge
identity are discovered from GitHub by repository, branch, and reviewed
head SHA elsewhere (``merger_turns.MergerTurnService``), never trusted
from or vacuously matched against the document under validation.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from typing import Any

VERDICTS = ("approved", "corrections_required")
SEVERITIES = ("Critical", "Important")

#: INFRA-200: the canonical reviewer-fix labels, each mapped to the
#: durable value already recorded in state. Accepted by
#: :func:`normalize_verdict` in addition to the durable values
#: themselves (which normalize to themselves) — ``ACCEPT`` and
#: ``ACCEPT_WITH_REVIEWER_FIX`` both settle as an ``approved`` durable
#: verdict, and ``REWORK_REQUIRED`` settles as ``corrections_required``.
#: :func:`parse_verdict` calls :func:`normalize_verdict` on the
#: document's own ``verdict`` field, so a Sol verdict carrying one of
#: these three labels parses exactly as its durable value — no schema
#: change: no jsonschema (or other) validator in this codebase
#: enforces ``schemas/review-verdict.json``'s narrower two-value enum
#: against an inbound document before it reaches ``parse_verdict``;
#: that file is read only by
#: ``test_schema_files_mirror_the_typed_contract``, which asserts on
#: its own content and stays unmodified and green.
VERDICT_LABELS: dict[str, str] = {
    "ACCEPT": "approved",
    "ACCEPT_WITH_REVIEWER_FIX": "approved",
    "REWORK_REQUIRED": "corrections_required",
}

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
        "reviewed_sha",
        "packets",
    }
)
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
#: The exact reviewer-supplied key set of one correction packet. Public
#: because the durable outbox derives its own stored-packet key set from
#: this one (plus the stamped ``pr_number``), so the two cannot drift.
PACKET_KEYS = frozenset(
    {
        "severity",
        "repository",
        "branch",
        "reviewed_sha",
        "evidence",
        "acceptance_criterion",
        "required_correction",
        "required_tests",
    }
)
#: The packet fields that must each be a non-empty string.
_PACKET_TEXT_KEYS = (
    "repository",
    "branch",
    "evidence",
    "acceptance_criterion",
    "required_correction",
)


class VerdictError(ValueError):
    """Raised for any structured verdict that violates the schema."""


def normalize_verdict(value: str) -> tuple[str, dict[str, Any]]:
    """Normalize one verdict spelling to its durable value (INFRA-200).

    Accepts both the durable values already recorded in state
    (``approved``, ``corrections_required``) and the three canonical
    reviewer-facing labels in :data:`VERDICT_LABELS` — ``ACCEPT``,
    ``ACCEPT_WITH_REVIEWER_FIX``, and ``REWORK_REQUIRED``. Anything
    else raises :class:`VerdictError`.

    Returns the durable value paired with the marker fields a caller
    merges into a stored ``verdict_json`` payload. No schema change is
    needed to retain the original spelling: it always rides along
    under ``label`` in that returned dict, and
    ``ACCEPT_WITH_REVIEWER_FIX`` additionally carries ``reviewer_fix:
    True`` so a reviewer-fix-eligible approval stays distinguishable
    after normalization collapses it to ``approved``. Every read path
    that only inspects the durable value (the first tuple element)
    keeps working unchanged.
    """

    if value in VERDICTS:
        durable = value
    elif value in VERDICT_LABELS:
        durable = VERDICT_LABELS[value]
    else:
        raise VerdictError(f"unknown verdict {value!r}")
    marker: dict[str, Any] = {"label": value}
    if value == "ACCEPT_WITH_REVIEWER_FIX":
        marker["reviewer_fix"] = True
    return durable, marker


@dataclass(frozen=True, slots=True)
class CorrectionPacket:
    """One structured Critical or Important finding returned to the lead."""

    severity: str
    repository: str
    branch: str
    #: INFRA-217: not reviewer-supplied — parsed packets carry ``0``
    #: until :meth:`ReviewVerdict.with_pr_number` stamps the
    #: GitHub-derived number; internal producers (CI window) stamp
    #: their own known number directly.
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
    reviewed_sha: str


@dataclass(frozen=True, slots=True)
class ReviewVerdict:
    """The Merger's structured outcome for one reviewed candidate."""

    verdict: str
    repository: str
    branch: str
    #: INFRA-217: never read from the document — ``0`` at parse until
    #: :meth:`with_pr_number` stamps the GitHub-derived number.
    pr_number: int
    reviewed_sha: str
    packets: tuple[CorrectionPacket, ...]
    #: INFRA-200: the exact JSON text of the parsed envelope with
    #: ``verdict`` normalized to the durable value and
    #: :func:`normalize_verdict`'s marker (``label``, and
    #: ``reviewer_fix: true`` for ACCEPT_WITH_REVIEWER_FIX) merged in —
    #: the payload a caller persists into
    #: ``submitted_verdicts.verdict_json`` to retain the original
    #: reviewer-facing label alongside the durable value. Empty for a
    #: ``ReviewVerdict`` built outside :func:`parse_verdict` (no
    #: schema change: this rides inside the existing JSON blob, not a
    #: new column).
    verdict_json: str = ""

    def with_pr_number(self, pr_number: int) -> ReviewVerdict:
        """Stamp the GitHub-derived pull request number, envelope and
        packets alike, returning the bound copy (INFRA-217)."""

        return ReviewVerdict(
            verdict=self.verdict,
            repository=self.repository,
            branch=self.branch,
            pr_number=pr_number,
            reviewed_sha=self.reviewed_sha,
            packets=tuple(
                replace(packet, pr_number=pr_number)
                for packet in self.packets
            ),
            verdict_json=self.verdict_json,
        )


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
    branch, or reviewed SHA differs from the admitted candidate — fail
    closed. INFRA-217: the document carries no ``pr_number`` anywhere (a
    document still carrying one fails the strict key check); PR identity
    is discovered from GitHub by the submission path, which also owns
    the rule that an approval requires a discovered pull request at the
    exact reviewed head.
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
    # INFRA-200: normalize BEFORE the durable-value membership check, so
    # the canonical reviewer-facing labels ACCEPT, ACCEPT_WITH_REVIEWER_FIX,
    # and REWORK_REQUIRED are accepted here too, each collapsing to its
    # durable value; the original label (and the reviewer_fix marker for
    # ACCEPT_WITH_REVIEWER_FIX) is retained below in ``verdict_json``.
    verdict, label_marker = normalize_verdict(value["verdict"])
    binding = _parse_binding(value)
    for field in ("repository", "branch", "reviewed_sha"):
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
    # The parsed payload that ends up in submitted_verdicts.verdict_json:
    # the exact envelope, with ``verdict`` normalized to the durable
    # value and the original-label marker merged in. Existing durable
    # inputs round-trip unchanged in substance — ``label`` merely
    # restates ``verdict`` and no ``reviewer_fix`` key is added.
    document = {**value, "verdict": verdict, **label_marker}
    return ReviewVerdict(
        verdict=verdict,
        repository=binding.repository,
        branch=binding.branch,
        pr_number=0,
        reviewed_sha=binding.reviewed_sha,
        packets=packets,
        verdict_json=json.dumps(document, sort_keys=True),
    )


def _parse_binding(value: dict[str, Any]) -> VerdictBinding:
    for field in ("repository", "branch"):
        if not isinstance(value[field], str) or not value[field]:
            raise VerdictError(f"verdict {field} is invalid")
    reviewed_sha = value["reviewed_sha"]
    if (
        not isinstance(reviewed_sha, str)
        or SHA_PATTERN.match(reviewed_sha) is None
    ):
        raise VerdictError("verdict reviewed_sha is invalid")
    return VerdictBinding(
        repository=value["repository"],
        branch=value["branch"],
        reviewed_sha=reviewed_sha,
    )


def packet_value_violation(entry: Any) -> str | None:
    """Describe how ``entry``'s VALUES violate the packet schema.

    This is the ONE authoritative statement of the ``CorrectionPacket``
    value constraints — the severity vocabulary, the non-empty required
    text, the 40-hex reviewed SHA, and the non-empty list of non-empty
    required tests. :func:`_parse_packet` enforces it on a reviewer's
    document and ``lead_outbox.packet_schema_violation`` enforces the
    same function on a stored durable packet, so no rule can hold in one
    place and not the other.

    Keys are deliberately NOT checked here: each caller owns its own key
    set (a stored packet additionally carries the stamped ``pr_number``)
    and both derive it from :data:`PACKET_KEYS`. ``None`` means every
    value is admissible.
    """

    severity = entry.get("severity")
    if severity not in SEVERITIES:
        return f"has an unknown severity {severity!r}"
    for field in _PACKET_TEXT_KEYS:
        value = entry.get(field)
        if not isinstance(value, str) or not value:
            return f"has an invalid {field} (a non-empty string is required)"
    reviewed_sha = entry.get("reviewed_sha")
    if (
        not isinstance(reviewed_sha, str)
        or SHA_PATTERN.match(reviewed_sha) is None
    ):
        return (
            "has an invalid reviewed_sha (40 lowercase hex characters are "
            "required)"
        )
    # JSON decoding only ever yields a list here; a tuple is admitted so
    # in-process callers holding the stored form need no conversion.
    tests = entry.get("required_tests")
    if (
        not isinstance(tests, list | tuple)
        or not tests
        or not all(isinstance(item, str) and item for item in tests)
    ):
        return (
            "has an invalid required_tests (a non-empty list of non-empty "
            "strings is required)"
        )
    return None


def _parse_packet(entry: Any) -> CorrectionPacket:
    if not isinstance(entry, dict):
        raise VerdictError("correction packet must be an object")
    if set(entry) != PACKET_KEYS:
        unexpected = sorted(set(entry) ^ PACKET_KEYS)[0]
        raise VerdictError(
            f"correction packet field {unexpected} is invalid"
        )
    violation = packet_value_violation(entry)
    if violation is not None:
        raise VerdictError(f"correction packet {violation}")
    return CorrectionPacket(
        severity=entry["severity"],
        repository=entry["repository"],
        branch=entry["branch"],
        pr_number=0,
        reviewed_sha=entry["reviewed_sha"],
        evidence=entry["evidence"],
        acceptance_criterion=entry["acceptance_criterion"],
        required_correction=entry["required_correction"],
        required_tests=tuple(entry["required_tests"]),
    )
