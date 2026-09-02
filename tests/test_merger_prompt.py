"""Verify the one-paragraph Sol merger prompt (operator correction 719cd2ad).

The prompt is exactly one operational paragraph (a leading markdown title
line is allowed) carrying the operator-approved role, with the old ceremony
gone: no receipt or manifest narration mandates, no full-gate receipt
language, no polling, no heartbeat, and no control-server mechanics.
Operator correction c3f4aad5 adds the Ponytail clause to the same single
paragraph: @ponytail-review before any commit or push, safe
simplifications only, then proceed. INFRA-200 versions the paragraph as
the forward-implementation-first review contract ("fif-1", adapted from
Vuk97/forward-implementation-first, MIT licensed): semantic review first,
a bounded reviewer-fix budget, an explicit return-to-Fable list, and the
idle token BLOCKED_ON_EXTERNAL_INTAKE now stated directly in the contract
text itself (previously kept only in the durable thread goal).
"""

from __future__ import annotations

from pathlib import Path

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "codex-merger.md"


def read_prompt() -> str:
    return PROMPT_PATH.read_text(encoding="utf-8")


def normalized() -> str:
    return " ".join(read_prompt().lower().split())


def body_lines() -> list[str]:
    lines = [line for line in read_prompt().splitlines() if line.strip()]
    if lines and lines[0].lstrip().startswith("#"):
        lines = lines[1:]
    return lines


def test_prompt_is_one_operational_paragraph() -> None:
    raw = read_prompt()
    lines = body_lines()
    assert lines, "prompt has no operational content"
    # No headings, bullets, or numbered structure inside the body.
    for line in lines:
        stripped = line.lstrip()
        assert not stripped.startswith(("#", "-", "*"))
        assert not stripped[:2].rstrip(".").isdigit()
    # The body is one contiguous block: exactly one blank-line-separated
    # paragraph after the optional title line.
    chunks = [chunk for chunk in raw.split("\n\n") if chunk.strip()]
    paragraphs = [
        chunk
        for chunk in chunks
        if not chunk.lstrip().startswith("#") or "\n" in chunk.strip()
    ]
    assert len(paragraphs) == 1


def test_prompt_carries_the_operator_approved_role() -> None:
    text = normalized()
    for clause in (
        "one sol merge lead",
        "exact candidate events hermes assigns",
        "parallel bounded read-only lanes",
        "structured corrections",
        "submit the final verdict through hermes submit-review",
        "sole pull request",
        "merge only the exact approved candidate",
        "next intake boundary",
        "without polling",
        "never supervise fable",
        "select work",
        "end the turn immediately when no eligible intake exists",
    ):
        assert clause in text, clause


def test_prompt_requires_ponytail_review_before_commit_and_push() -> None:
    """Operator correction c3f4aad5 required test 6: the one-paragraph
    prompt binds @ponytail-review before commit/push, safe
    simplifications only, the explicit submit-review, and sole PR and
    merge ownership."""

    text = normalized()
    for clause in (
        "run @ponytail-review on the current diff before any git commit "
        "or git push",
        "apply only the safe simplifications it returns",
        "behavioral or judgment-bearing finding it raises back as "
        "structured corrections",
        "explicitly submit the final verdict through hermes submit-review",
        "sole pull request",
        "merge only the exact approved candidate",
    ):
        assert clause in text, clause


def test_prompt_states_the_forward_implementation_first_contract() -> None:
    """INFRA-200: the versioned contract is stated explicitly in the
    paragraph, not merely implied -- version tag, semantic-review-first
    priority, the bounded reviewer-fix budget and its consolidation into
    one commit, the explicit return-to-Fable category list,
    administrative-only drift never forcing a wide replay, sole
    integration-writer/PR-owner/merge authority, bounded read-only
    parallel lanes, the idle token, and the upstream attribution."""

    text = normalized()
    for clause in (
        "forward-implementation-first review contract fif-1",
        "prioritize semantic code and contract review",
        "roughly three files, fifty changed lines, and thirty minutes",
        "unambiguous, and mechanical",
        "focused verification",
        "one transparent reviewer-fix commit",
        "judgment-bearing, architectural, schema or migration, public "
        "api, generated-artifact, pricing or economics, or broad "
        "consumer change",
        "administrative-only drift never forces a wide semantic replay",
        "sole integration writer, merge authority, and owner of the "
        "project's sole pull request",
        "parallel bounded read-only lanes that never write or merge",
        "blocked_on_external_intake",
        "adapted from vuk97's forward-implementation-first",
        "pinned commit",
        "91fa46a0108ecfc612a55cf587a2086621a31161",
        "mit licensed",
        "skill.md",
        "sha256",
        "411a6de1df78dc0bf27572ddd1f88aeca8b1da23537152316e7a581e5e9e5b67",
        "no runtime dependency",
    ):
        assert clause in text, clause


def test_prompt_drops_the_old_ceremony() -> None:
    text = normalized()
    for clause in (
        "receipt",
        "manifest",
        "gate",
        "heartbeat",
        "control server",
        "app server",
        "codex queue",
        "merge-settle",
        "outbox",
        "cmux",
    ):
        assert clause not in text, clause
    # "poll" survives only inside the single negated phrase.
    assert "poll" not in text.replace("without polling", "")
