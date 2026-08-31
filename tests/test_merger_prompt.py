"""Verify the one-paragraph Sol merger prompt (operator correction 719cd2ad).

The prompt is exactly one operational paragraph (a leading markdown title
line is allowed) carrying the operator-approved role, with the old ceremony
gone: no BLOCKED_ON_EXTERNAL_INTAKE output, no receipt or manifest
narration mandates, no full-gate receipt language, no polling, no heartbeat,
and no control-server mechanics. Operator correction c3f4aad5 adds the
Ponytail clause to the same single paragraph: @ponytail-review before any
commit or push, safe simplifications only, then proceed.
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
        "small reviewer-fix budget only on mechanical defects",
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
        "behavioral or judgment-bearing finding back as structured "
        "corrections",
        "explicitly submit the final verdict through hermes submit-review",
        "sole pull request",
        "merge only the exact approved candidate",
    ):
        assert clause in text, clause


def test_prompt_drops_the_old_ceremony() -> None:
    text = normalized()
    assert "blocked_on_external_intake" not in text
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
