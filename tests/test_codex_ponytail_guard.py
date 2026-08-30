"""Verify the session-scoped Ponytail review gate (correction c3f4aad5,
hardened by Sol correction a9cc6d5f).

The guard blocks exactly ``git commit`` and ``git push`` in a managed Sol
session until the current candidate snapshot — a canonical hash covering
tracked modifications, index state, deletions, renames, and untracked
files — has a recorded @ponytail-review for the command's *effective*
repository (``-C``/``cd``/``--git-dir``/``--work-tree`` resolved; unprovable
contexts fail closed). A changed candidate invalidates the prior review by
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
    snapshot_hash,
)


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True
    )


def _make_repo(path: Path) -> Path:
    path.mkdir()
    _git(path, "init", "-b", "main")
    _git(path, "config", "user.email", "sol@example.test")
    _git(path, "config", "user.name", "Sol")
    _git(path, "config", "commit.gpgsign", "false")
    (path / "module.py").write_text("print('hello')\n", encoding="utf-8")
    _git(path, "add", ".")
    _git(path, "commit", "-m", "initial")
    return path


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    return _make_repo(tmp_path / "workspace")


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


# Sol correction a9cc6d5f test 1: an untracked file added after review
# invalidates the marker and blocks `git add -A && git commit`.
def test_untracked_file_added_after_review_invalidates_the_marker(
    repo: Path,
) -> None:
    dirty(repo)
    reviewed = record_review(repo)
    assert evaluate_command("git commit -am 'x'", repo).allowed is True

    (repo / "smuggled.py").write_text("print('sneaky')\n", encoding="utf-8")

    code, reason = sol_hook_exit(repo, "git add -A && git commit -m 'x'")
    assert code == BLOCK_EXIT_CODE
    assert "@ponytail-review" in reason
    # Reviewing the complete candidate — untracked file included — differs
    # from the tracked-only review and re-opens the gate.
    assert record_review(repo) != reviewed
    assert evaluate_command("git add -A && git commit -m 'x'", repo).allowed


# Sol correction a9cc6d5f test 2: a reviewed parent repository cannot
# authorize `git -C nested commit` in an unreviewed nested repository.
def test_reviewed_parent_cannot_authorize_nested_repository_commit(
    repo: Path,
) -> None:
    nested = _make_repo(repo / "nested")
    dirty(repo)
    record_review(repo)
    assert evaluate_command("git commit -am 'parent'", repo).allowed is True

    (nested / "module.py").write_text("print('nested')\n", encoding="utf-8")

    for command in (
        "git -C nested commit -am 'x'",
        ["bash", "-lc", "cd nested && git commit -am 'x'"],
    ):
        assert evaluate_command(command, repo).allowed is False, command
    # The parent's review still stands for the parent itself.
    assert evaluate_command("git commit -am 'parent'", repo).allowed is True
    # Reviewing the nested repository itself opens its own gate.
    record_review(nested)
    assert evaluate_command("git -C nested commit -am 'x'", repo).allowed


# Sol correction a9cc6d5f test 3: --git-dir/--work-tree forms validate the
# effective target they select, or fail closed when it cannot be proven.
def test_git_dir_and_work_tree_forms_validate_the_effective_target(
    repo: Path,
) -> None:
    nested = _make_repo(repo / "nested")
    (nested / "module.py").write_text("print('nested')\n", encoding="utf-8")
    git_dir = nested / ".git"

    equivalent = (
        f"git --git-dir={git_dir} --work-tree={nested} commit -am 'x'",
        f"git --git-dir {git_dir} --work-tree {nested} commit -am 'x'",
        "git -C nested commit -am 'x'",
    )
    for command in equivalent:
        assert evaluate_command(command, repo).allowed is False, command
    record_review(nested)
    for command in equivalent:
        assert evaluate_command(command, repo).allowed is True, command

    # Context the guard cannot prove fails closed, naming the reason.
    for command in (
        "GIT_DIR=nested/.git git commit -am 'x'",
        "git -c core.worktree=nested commit -am 'x'",
        "git --namespace=other push origin main",
    ):
        decision = evaluate_command(command, repo)
        assert decision.allowed is False, command
        assert "fails closed" in decision.reason, command

    # A nested git dir crossed with the parent's work tree matches
    # neither repository's review, so it stays blocked.
    dirty(repo)
    record_review(repo)
    crossed = f"git --git-dir={git_dir} commit -am 'x'"
    assert evaluate_command(crossed, repo).allowed is False


# Sol correction a9cc6d5f test 4: staged, unstaged, deleted, renamed, and
# newly added files all affect the reviewed snapshot.
def test_every_change_class_affects_the_reviewed_snapshot(repo: Path) -> None:
    seen = [snapshot_hash(repo)]

    def changed() -> None:
        current = snapshot_hash(repo)
        assert current not in seen
        seen.append(current)

    (repo / "module.py").write_text("print('edit')\n", encoding="utf-8")
    changed()  # unstaged modification
    _git(repo, "add", "module.py")
    changed()  # staged modification (index state alone)
    (repo / "brand-new.py").write_text("print('new')\n", encoding="utf-8")
    changed()  # newly added untracked file
    _git(repo, "add", "brand-new.py")
    changed()  # staged addition
    _git(repo, "mv", "brand-new.py", "renamed.py")
    changed()  # rename
    (repo / "module.py").unlink()
    changed()  # deletion


# Sol correction 743338e2 packet 1: `command` wrapper forms are consumed
# correctly — gated when unreviewed, allowed once reviewed, and unmodeled
# options fail closed even with a review on record.
def test_command_wrapper_forms_are_gated(repo: Path) -> None:
    dirty(repo)

    for command in (
        "command -- git commit -am 'x'",
        "command -- git push origin main",
        "command -p git commit -am 'x'",
        "command git push origin main",
    ):
        code, reason = sol_hook_exit(repo, command)
        assert code == BLOCK_EXIT_CODE, command
        assert "@ponytail-review" in reason, command
    # `command -v git` prints a path and executes nothing.
    assert evaluate_command("command -v git commit", repo).allowed is True

    record_review(repo)
    assert evaluate_command("command -- git commit -am 'x'", repo).allowed
    # An unmodeled `command` option is never proven safe, review or not.
    decision = evaluate_command("command -Q git commit -am 'x'", repo)
    assert decision.allowed is False
    assert "fails closed" in decision.reason


# Sol correction 743338e2 packet 1: env wrapper options are fully
# consumed; `env -i ... git commit` cannot slip past the gate.
def test_env_wrapper_options_are_gated(repo: Path) -> None:
    dirty(repo)

    for command in (
        "env -i PATH=/usr/bin:/bin git commit -am 'x'",
        "env -u SOME_VAR git push origin main",
        "env - git commit -am 'x'",
        "env -- git push origin main",
    ):
        code, reason = sol_hook_exit(repo, command)
        assert code == BLOCK_EXIT_CODE, command
        assert "@ponytail-review" in reason, command

    record_review(repo)
    assert evaluate_command(
        "env -i PATH=/usr/bin:/bin git commit -am 'x'", repo
    ).allowed
    # Context-changing env assignments and unmodeled env options fail
    # closed even with a review on record.
    for command, named in (
        ("env GIT_DIR=elsewhere/.git git commit -am 'x'", "GIT_DIR"),
        ("env -S 'git commit -am x' true", "-S"),
    ):
        decision = evaluate_command(command, repo)
        assert decision.allowed is False, command
        assert named in decision.reason, command


# Sol correction 743338e2 packet 1: --exec-path retargets which git
# programs run, so both forms are rejected outright.
def test_exec_path_forms_are_rejected(repo: Path) -> None:
    dirty(repo)
    record_review(repo)
    assert evaluate_command("git commit -am 'x'", repo).allowed is True

    for command in (
        "git --exec-path=/tmp/fake-git-libexec commit -am 'x'",
        "git --exec-path /tmp/fake-git-libexec commit -am 'x'",
        "git --exec-path=/tmp/fake-git-libexec push origin main",
    ):
        decision = evaluate_command(command, repo)
        assert decision.allowed is False, command
        assert "--exec-path" in decision.reason, command


# Sol correction 743338e2 packet 1: shell syntax that could hide a
# guarded verb is never classified unguarded. No-space separators are
# now parsed correctly (gated, not invisible); subshells, substitution,
# pipelines, and background execution fail closed even when reviewed.
def test_shell_syntax_that_could_hide_guarded_verbs_fails_closed(
    repo: Path,
) -> None:
    dirty(repo)

    # No-space separators and logical operators reach the gate.
    for command in (
        "true;git commit -am 'x'",
        "false||git push origin main",
        "git add -A;git commit -m 'x'",
        "true&&git push origin main",
    ):
        code, reason = sol_hook_exit(repo, command)
        assert code == BLOCK_EXIT_CODE, command
        assert "@ponytail-review" in reason, command

    record_review(repo)
    assert evaluate_command("true;git commit -am 'x'", repo).allowed is True

    # Syntax the recognizer cannot prove blocks despite the review.
    for command in (
        "(git commit -am 'x')",
        "echo $(git push origin main)",
        "echo `git push origin main`",
        "git commit -am 'x' | cat",
        "echo ok && git commit -am $(date)",
        "git commit -am 'x' &",
        "git status\ngit push origin main",
        "cat <<DOC\ngit commit\nDOC",
        "sudo git commit -am 'x'",
        "xargs git push < targets.txt",
        "timeout 30 git push origin main",
        "bash script-that-commits.sh",
        # An unknown subcommand could be an alias expanding to commit.
        "git commit-tree HEAD -m 'raw'",
    ):
        decision = evaluate_command(command, repo)
        assert decision.allowed is False, command
        assert "fails closed" in decision.reason, command


# Sol correction 743338e2 packet 1 (positive pin): ordinary non-guarded
# commands stay allowed on a dirty, unreviewed workspace.
def test_ordinary_commands_remain_allowed_unreviewed(repo: Path) -> None:
    dirty(repo)

    for command in (
        "git status",
        "git diff HEAD",
        "git log --oneline",
        "git add -A",
        "git stash push",
        "git fetch origin",
        "git checkout -b feature/guard-pilot",
        "git rev-parse HEAD",
        "git log --oneline | grep commit",
        "echo git commit",
        "grep -rn 'git push' .",
        "uv run pytest tests/test_commit_flow.py -q",
        "command -v git",
        ["python", "-m", "pytest", "tests/test_push_flow.py"],
        "ls -la && echo done",
    ):
        assert evaluate_command(command, repo).allowed is True, command


# Sol correction 743338e2 packet 2: inherited GIT_DIR/GIT_WORK_TREE are
# replayed identically, so another repository's marker cannot authorize
# the effective repository the command will actually operate on.
def test_inherited_git_context_cannot_borrow_another_repos_marker(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    other = _make_repo(tmp_path / "other")
    dirty(repo)
    record_review(repo)
    assert evaluate_command("git commit -am 'x'", repo).allowed is True

    (other / "module.py").write_text("print('other')\n", encoding="utf-8")
    monkeypatch.setenv("GIT_DIR", str(other / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(other))

    # The effective repository is `other`, which has no review; the
    # marker recorded for `repo` cannot authorize it.
    assert evaluate_command("git commit -am 'x'", repo).allowed is False
    assert evaluate_command("git push origin main", repo).allowed is False

    # Reviewing the resulting effective repository opens its gate.
    record_review(other)
    assert evaluate_command("git commit -am 'x'", repo).allowed is True


# Sol correction 743338e2 packet 2: exported and command-level context
# overrides in the command itself are rejected naming the variable.
def test_exported_and_inline_git_context_fails_closed(repo: Path) -> None:
    dirty(repo)
    record_review(repo)
    assert evaluate_command("git commit -am 'x'", repo).allowed is True

    for command, named in (
        ("export GIT_DIR=/elsewhere/.git; git commit -am 'x'", "GIT_DIR"),
        (
            "export GIT_WORK_TREE=/elsewhere && git push origin main",
            "GIT_WORK_TREE",
        ),
        ("GIT_WORK_TREE=/elsewhere git commit -am 'x'", "GIT_WORK_TREE"),
        ("GIT_INDEX_FILE=/tmp/alt-index git commit -am 'x'", "GIT_INDEX_FILE"),
        ("unset GIT_DIR; git push origin main", "GIT_DIR"),
    ):
        decision = evaluate_command(command, repo)
        assert decision.allowed is False, command
        assert named in decision.reason, command
    # Context-inert exports stay usable.
    assert evaluate_command(
        "export GIT_EDITOR=true; git commit -am 'x'", repo
    ).allowed


# Sol correction 743338e2 packet 2: GIT_CONFIG_* fails closed unless
# modeled identically — context-changing keys are rejected by name
# whether inherited or written into the command line.
def test_git_config_environment_family_fails_closed(
    repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dirty(repo)
    record_review(repo)

    command_level = (
        "GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=core.worktree "
        "GIT_CONFIG_VALUE_0=/elsewhere git commit -am 'x'"
    )
    decision = evaluate_command(command_level, repo)
    assert decision.allowed is False
    assert "GIT_CONFIG" in decision.reason

    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.worktree")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "/elsewhere")
    decision = evaluate_command("git commit -am 'x'", repo)
    assert decision.allowed is False
    assert "GIT_CONFIG_KEY_0" in decision.reason

    # Context-inert inherited configuration is replayed, not rejected.
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "user.name")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "Sol")
    assert evaluate_command("git commit -am 'x'", repo).allowed is True

    monkeypatch.delenv("GIT_CONFIG_COUNT")
    monkeypatch.delenv("GIT_CONFIG_KEY_0")
    monkeypatch.delenv("GIT_CONFIG_VALUE_0")
    monkeypatch.setenv("GIT_CONFIG_PARAMETERS", "'core.worktree=/elsewhere'")
    decision = evaluate_command("git push origin main", repo)
    assert decision.allowed is False
    assert "GIT_CONFIG_PARAMETERS" in decision.reason


# Sol correction 743338e2 packet 2: --config-env injects configuration
# from the environment and fails closed in both forms.
def test_config_env_option_fails_closed(repo: Path) -> None:
    dirty(repo)
    record_review(repo)

    for command in (
        "git --config-env=user.email=EMAIL_VAR commit -am 'x'",
        "git --config-env user.email=EMAIL_VAR push origin main",
    ):
        decision = evaluate_command(command, repo)
        assert decision.allowed is False, command
        assert "--config-env" in decision.reason, command


# Sol correction 743338e2 packet 2: context-changing configuration
# includes fail closed on the command line, and file-based includes are
# resolved identically so they cannot smuggle a different worktree past
# the recorded review.
def test_context_changing_config_includes_fail_closed(
    repo: Path, tmp_path: Path
) -> None:
    dirty(repo)
    record_review(repo)
    assert evaluate_command("git commit -am 'x'", repo).allowed is True

    for command in (
        f"git -c include.path={tmp_path}/ctx.cfg commit -am 'x'",
        "git -c includeIf.gitdir:/x.path=/tmp/ctx.cfg commit -am 'x'",
    ):
        decision = evaluate_command(command, repo)
        assert decision.allowed is False, command
        assert "changes the git context" in decision.reason, command

    # Context-changing configuration in the effective config file is
    # applied identically during resolution and snapshotting (the guard
    # replays the same environment and reads the same config git will):
    # the effective candidate differs from the reviewed one, so the gate
    # closes until the resulting target itself is reviewed.
    other = tmp_path / "other-worktree"
    other.mkdir()
    (other / "smuggled.py").write_text("print('sneaky')\n", encoding="utf-8")
    _git(repo, "config", "core.worktree", str(other))

    assert evaluate_command("git commit -am 'x'", repo).allowed is False
    record_review(repo)  # now reviews the effective (redirected) candidate
    assert evaluate_command("git commit -am 'x'", repo).allowed is True


# Sol 165f5ee6 packet 1 test 1: with inherited GIT_DIR/GIT_WORK_TREE
# selecting reviewed repo B and dirty repo A as cwd, `env -i git commit`
# and push validate A (the repository the cleared command actually
# operates on) — B's review cannot authorize them.
def test_env_clear_validates_the_cwd_repo_not_the_inherited_context(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reviewed_b = _make_repo(tmp_path / "reviewed-b")
    (reviewed_b / "module.py").write_text("print('b')\n", encoding="utf-8")
    record_review(reviewed_b)
    dirty(repo)  # repo A: dirty, unreviewed

    monkeypatch.setenv("GIT_DIR", str(reviewed_b / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(reviewed_b))

    # Sanity: without env clearing the effective repository is B, whose
    # review stands.
    assert evaluate_command("git commit -am 'x'", repo).allowed is True

    # env -i strips the inherited context: the executed command runs
    # against A, so the guard validates A — dirty and unreviewed.
    for command in (
        "env -i PATH=/usr/bin:/bin git commit -am 'x'",
        "env -i PATH=/usr/bin:/bin git push origin main",
    ):
        decision = evaluate_command(command, repo)
        assert decision.allowed is False, command
        assert "@ponytail-review" in decision.reason, command

    # Reviewing A itself (not B) is what opens the gate for the cleared
    # form: the guard bound the transformed environment to A.
    monkeypatch.delenv("GIT_DIR")
    monkeypatch.delenv("GIT_WORK_TREE")
    record_review(repo)
    monkeypatch.setenv("GIT_DIR", str(reviewed_b / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(reviewed_b))
    assert evaluate_command(
        "env -i PATH=/usr/bin:/bin git commit -am 'x'", repo
    ).allowed


# Sol 165f5ee6 packet 1 test 2: `env -` and `env --ignore-environment`
# cannot authorize against the inherited context either.
def test_env_dash_forms_cannot_authorize_against_inherited_context(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reviewed_b = _make_repo(tmp_path / "reviewed-b")
    (reviewed_b / "module.py").write_text("print('b')\n", encoding="utf-8")
    record_review(reviewed_b)
    dirty(repo)

    monkeypatch.setenv("GIT_DIR", str(reviewed_b / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(reviewed_b))
    assert evaluate_command("git commit -am 'x'", repo).allowed is True

    for command in (
        "env - git commit -am 'x'",
        "env --ignore-environment git commit -am 'x'",
        "env - git push origin main",
        "env --ignore-environment git push origin main",
    ):
        assert evaluate_command(command, repo).allowed is False, command


# Sol 165f5ee6 packet 1 test 3: env -u / --unset of a context-affecting
# name is rejected by name — it cannot authorize against context the
# guard would otherwise retain, review or no review.
def test_env_unset_of_git_context_fails_closed_by_name(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reviewed_b = _make_repo(tmp_path / "reviewed-b")
    (reviewed_b / "module.py").write_text("print('b')\n", encoding="utf-8")
    record_review(reviewed_b)
    dirty(repo)
    record_review(repo)  # even with both repositories reviewed
    monkeypatch.setenv("GIT_DIR", str(reviewed_b / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(reviewed_b))

    for command, named in (
        ("env -u GIT_DIR git commit -am 'x'", "GIT_DIR"),
        ("env --unset=GIT_WORK_TREE git push origin main", "GIT_WORK_TREE"),
        ("env -u GIT_WORK_TREE -u GIT_DIR git commit -am 'x'", "GIT_WORK_TREE"),
        ("env --unset=GIT_CONFIG_COUNT git commit -am 'x'", "GIT_CONFIG_COUNT"),
    ):
        decision = evaluate_command(command, repo)
        assert decision.allowed is False, command
        assert named in decision.reason, command
        assert "fails closed" in decision.reason, command


# Sol 165f5ee6 packet 1 test 4: context-inert env assignments and unsets
# stay accepted — gated before review, allowed once reviewed.
def test_context_inert_env_transformations_stay_accepted(repo: Path) -> None:
    dirty(repo)

    inert = (
        "env FOO=bar git commit -am 'x'",
        "env -u SOME_VAR git commit -am 'x'",
        "env --unset=SOME_VAR FOO=bar git push origin main",
        "env -u SOME_VAR FOO=bar env BAR=baz git commit -am 'x'",
    )
    for command in inert:
        code, reason = sol_hook_exit(repo, command)
        assert code == BLOCK_EXIT_CODE, command
        assert "@ponytail-review" in reason, command

    record_review(repo)
    for command in inert:
        assert evaluate_command(command, repo).allowed is True, command


def test_outside_a_repository_the_gate_fails_closed(tmp_path: Path) -> None:
    bare = tmp_path / "not-a-repo"
    bare.mkdir()

    decision = evaluate_command("git push origin main", bare)

    assert decision.allowed is False
    assert "cannot verify the candidate snapshot" in decision.reason
