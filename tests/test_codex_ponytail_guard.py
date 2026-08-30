"""Verify the session-scoped Ponytail review gate (correction c3f4aad5).

The guard blocks exactly ``git commit`` and ``git push`` in a managed Sol
session until the current diff (SHA-256 of ``git diff HEAD``) has a
recorded @ponytail-review; a changed diff invalidates the prior review by
hash comparison; the state is one marker file overwritten in place — no
receipt or manifest subsystem; and sessions without the hook binding are
untouched (no global git hook is ever installed).
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from hermes_orchestrator.codex_ponytail_guard import (
    BLOCK_EXIT_CODE,
    MARKER_NAME,
    evaluate_command,
    guarded_subcommand,
    handle_hook_payload,
    main,
    marker_path,
    record_command,
    record_review,
    reviewed_hash,
)


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    path = tmp_path / "workspace"
    path.mkdir()
    _git(path, "init", "-b", "main")
    _git(path, "config", "user.email", "sol@example.test")
    _git(path, "config", "user.name", "Sol")
    _git(path, "config", "commit.gpgsign", "false")
    (path / "module.py").write_text("print('hello')\n", encoding="utf-8")
    _git(path, "add", ".")
    _git(path, "commit", "-m", "initial")
    return path


def dirty(repo: Path) -> None:
    (repo / "module.py").write_text("print('changed')\n", encoding="utf-8")


def sol_hook_exit(repo: Path, command: object) -> tuple[int, str]:
    """Run the guard exactly as the managed Sol session's hook does."""

    payload = {
        "session_id": "thr_sol",
        "cwd": str(repo),
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {"command": command},
    }
    import io
    import sys

    stderr = io.StringIO()
    original = sys.stderr
    sys.stderr = stderr
    try:
        code = main(["hook"], stdin=json.dumps(payload))
    finally:
        sys.stderr = original
    return code, stderr.getvalue()


# Required test 1: no commit before @ponytail-review of the current diff.
def test_managed_sol_session_cannot_commit_before_ponytail_review(
    repo: Path,
) -> None:
    dirty(repo)

    code, reason = sol_hook_exit(repo, ["bash", "-lc", "git commit -am 'x'"])

    assert code == BLOCK_EXIT_CODE
    assert "@ponytail-review" in reason
    assert record_command() in reason  # the block is self-recovering
    assert evaluate_command("git commit -m 'x'", repo).allowed is False


# Required test 2: no push before @ponytail-review of the current diff.
def test_managed_sol_session_cannot_push_before_ponytail_review(
    repo: Path,
) -> None:
    dirty(repo)

    code, reason = sol_hook_exit(repo, "git push origin main")

    assert code == BLOCK_EXIT_CODE
    assert "git push is blocked" in reason


# Required test 3: after review of the current diff the same session
# commits and pushes through its authorized workflow.
def test_reviewed_diff_commits_and_pushes_through_the_workflow(
    repo: Path, tmp_path: Path
) -> None:
    origin = tmp_path / "origin.git"
    subprocess.run(
        ["git", "init", "--bare", str(origin)], check=True, capture_output=True
    )
    _git(repo, "remote", "add", "origin", str(origin))
    dirty(repo)

    record_review(repo)  # what Sol's explicit record step runs

    code, _ = sol_hook_exit(repo, ["bash", "-lc", "git commit -am 'reviewed'"])
    assert code == 0
    _git(repo, "commit", "-am", "reviewed")
    # After the guarded commit the tree matches HEAD: nothing unreviewed
    # remains, so the push of the just-gated commit proceeds.
    code, _ = sol_hook_exit(repo, "git push origin main")
    assert code == 0
    _git(repo, "push", "origin", "main")


# Required test 4: a changed diff invalidates the prior review — via
# hash comparison over one marker file, not a receipt subsystem.
def test_changed_diff_invalidates_the_prior_review_without_receipts(
    repo: Path,
) -> None:
    dirty(repo)
    first = record_review(repo)
    (repo / "module.py").write_text("print('changed again')\n", encoding="utf-8")

    assert evaluate_command("git commit -am 'x'", repo).allowed is False

    second = record_review(repo)
    assert second != first
    assert evaluate_command("git commit -am 'x'", repo).allowed is True
    # One marker, overwritten in place: nothing accumulates.
    marker = marker_path(repo)
    assert reviewed_hash(repo) == second
    assert list(repo.rglob(f"*{MARKER_NAME}*")) == [marker]


# Required test 5: non-Sol and unmanaged Codex sessions are untouched.
def test_unmanaged_sessions_are_not_subject_to_the_guard(repo: Path) -> None:
    dirty(repo)

    # The guard never installs a git hook, so any session without the
    # managed-Sol thread binding commits freely with no review recorded.
    hooks_dir = repo / ".git" / "hooks"
    installed = {
        entry.name
        for entry in hooks_dir.iterdir()
        if not entry.name.endswith(".sample")
    }
    assert evaluate_command("git commit -am 'x'", repo).allowed is False
    _git(repo, "commit", "-am", "unguarded session")  # succeeds unreviewed
    assert {
        entry.name
        for entry in hooks_dir.iterdir()
        if not entry.name.endswith(".sample")
    } == installed


def test_non_shell_and_non_git_payloads_pass_untouched(repo: Path) -> None:
    dirty(repo)
    assert handle_hook_payload({"tool_name": "Read", "cwd": str(repo)}).allowed
    assert handle_hook_payload(
        {"hook_event_name": "PostToolUse", "tool_input": {"command": "git push"}}
    ).allowed
    for command in (
        "git status",
        "git diff HEAD",
        "git stash push",  # push is a stash verb here, not the remote push
        "echo git commit",
        ["python", "-m", "pytest"],
        "uv run pytest -q && git log --oneline",
    ):
        assert evaluate_command(command, repo).allowed, command
    assert main(["hook"], stdin="not json") == 0
    assert main(["hook"], stdin='"just a string"') == 0


def test_guarded_command_shapes_are_recognized(repo: Path) -> None:
    for command in (
        "git commit -m 'x'",
        "git push origin main",
        ["git", "push", "--force-with-lease"],
        ["bash", "-lc", "cd /work && git commit -am 'y'"],
        "git -C /work -c user.email=x commit",
        "VAR=1 env git push",
        "git fetch && git commit -m 'chained'",
        "/usr/bin/git push",
    ):
        assert guarded_subcommand(command) in {"commit", "push"}, command
    # Unparseable shell text naming a guarded operation fails closed.
    assert guarded_subcommand("git commit -m 'unterminated") == "commit"
    dirty(repo)
    assert evaluate_command("git commit -m 'unterminated", repo).allowed is False


def test_empty_diff_carries_nothing_unreviewed(repo: Path) -> None:
    decision = evaluate_command("git push origin main", repo)
    assert decision.allowed is True
    assert "nothing unreviewed" in decision.reason


def test_record_cli_writes_the_marker_for_the_workspace(
    repo: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    dirty(repo)
    monkeypatch.chdir(repo)

    assert main(["record"]) == 0

    printed = capsys.readouterr().out.strip()
    assert reviewed_hash(repo) == printed
    assert evaluate_command("git commit -am 'x'", repo).allowed is True


def test_outside_a_repository_the_gate_fails_closed(tmp_path: Path) -> None:
    bare = tmp_path / "not-a-repo"
    bare.mkdir()

    decision = evaluate_command("git push origin main", bare)

    assert decision.allowed is False
    assert "cannot verify the diff" in decision.reason
