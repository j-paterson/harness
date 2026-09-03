"""Contract tests for the fast-lane Claude lead prompt.

Operator correction c84ed73a: the lead prompt must drop the ceremonial
verification mandates, keep the durable intake handshake semantics, and
split candidate submission (Fable) from review/PR/merge ownership (Sol).
"""

from pathlib import Path

import pytest

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "claude-lead.md"

# Retired mandate phrasing that must no longer appear anywhere in the prompt.
STALE_PHRASES = [
    "test-first",
    "failing test",
    "red/green",
    "record_direct_exception",
    "candidate-full-gate",
    "verify --gate",
    "attested",
    "verification receipt for the mandatory complete gate",
    "fails closed on a non-trivial candidate",
    "without accepted packets",
    "submit completed pull requests",
    "codex merger",
]


@pytest.fixture(scope="module")
def prompt() -> str:
    """Prompt text lowercased with whitespace collapsed (wrap-insensitive)."""
    raw = PROMPT_PATH.read_text(encoding="utf-8")
    return " ".join(raw.split()).lower()


@pytest.mark.parametrize("phrase", STALE_PHRASES)
def test_stale_mandates_absent(prompt: str, phrase: str) -> None:
    assert phrase not in prompt


def test_no_operator_facing_ack(prompt: str) -> None:
    # ACK/acknowledge is reserved for internal machine protocol; the operator
    # hears "confirm". No instruction may tell the lead to ACK the operator.
    for stale in ("reply ack", "ack the operator", "ack to the operator"):
        assert stale not in prompt
    assert "reserve ack/acknowledge for internal machine protocol" in prompt
    assert '"confirm"' in prompt


def test_intake_handshake_semantics_present(prompt: str) -> None:
    assert "wake signal only, never a payload" in prompt
    assert "retrieve the exact durable packet from sqlite by that id" in prompt
    assert "belongs to your project, issue, and session" in prompt
    assert "acknowledge it with the exact packet id" in prompt
    assert "existing intake and correction apis before acting on it" in prompt
    assert "already delivered or acknowledged is a no-op" in prompt
    assert "missing or mismatched packet fails closed" in prompt
    assert "only the durable sqlite record is" in prompt


def test_work_ready_names_the_existing_target_transition(prompt: str) -> None:
    assert "run `hermes-orchestrator target-issue` immediately" in prompt
    assert "do not search the database or command help" in prompt


def test_authoritative_linear_read_names_the_installed_cli(prompt: str) -> None:
    assert "`linear issue view issue --json --no-pager`" in prompt
    assert "do not search for linear credentials" in prompt


def test_fast_lane_delegation_and_evidence(prompt: str) -> None:
    assert "plans remain fable-owned" in prompt
    assert "smallest capable model" in prompt
    assert (
        "durable packet ownership exists for active concurrency and recovery, "
        "not candidate admission" in prompt
    )
    assert "exact reproducible evidence may be used directly" in prompt
    assert "no mandatory red-first execution" in prompt
    assert "focused regressions and green focused checks are sufficient" in prompt
    assert "diagnostic red run" in prompt


def test_submission_ownership_split(prompt: str) -> None:
    # Fable commits, pushes, and invokes candidate-submit without a PR.
    assert "fable commits and pushes" in prompt
    assert "candidate-submit" in prompt
    assert "do not create a pull request" in prompt
    # Sol invokes submit-review and owns the PR lifecycle end to end.
    assert (
        "sol explicitly invokes submit-review and owns pr creation, "
        "ci reconciliation, integration, and merge" in prompt
    )
    assert "never merge yourself" in prompt
