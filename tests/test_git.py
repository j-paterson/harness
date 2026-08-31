"""Verify argv-safe local Git evidence for merge ancestry proofs."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from hermes_orchestrator.git import (
    GitError,
    GitResult,
    GitVerifier,
    SubprocessGitRunner,
)

MERGE_SHA = "a" * 40
CANDIDATE = "b" * 40
TREE = "c" * 40
REPO = Path("/repo/demo")


@dataclass
class FakeRunner:
    """Deterministic recording runner keyed by exact argv vectors."""

    results: dict[tuple[str, ...], GitResult] = field(default_factory=dict)
    calls: list[tuple[tuple[str, ...], Path, str | None]] = field(
        default_factory=list
    )

    def run(
        self, args: tuple[str, ...], cwd: Path, *, input: str | None = None
    ) -> GitResult:
        self.calls.append((args, cwd, input))
        if args not in self.results:
            raise AssertionError(f"unexpected git argv {args!r}")
        return self.results[args]


@pytest.fixture
def runner() -> FakeRunner:
    return FakeRunner()


@pytest.fixture
def verifier(runner: FakeRunner) -> GitVerifier:
    return GitVerifier(runner=runner)


def test_fetch_runs_exact_argv_in_repo(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    runner.results[("git", "fetch", "--", "origin", "main")] = GitResult(0, "", "")
    verifier.fetch(REPO, "origin", "main")
    assert runner.calls == [
        (("git", "fetch", "--", "origin", "main"), REPO, None)
    ]


def test_fetch_failure_raises_git_error(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    runner.results[("git", "fetch", "--", "origin", "main")] = GitResult(
        128, "", "fatal: could not read from remote repository"
    )
    with pytest.raises(GitError, match="git fetch failed"):
        verifier.fetch(REPO, "origin", "main")


def test_is_ancestor_maps_zero_and_one_exit_codes(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    argv = ("git", "merge-base", "--is-ancestor", MERGE_SHA, "origin/main")
    runner.results[argv] = GitResult(0, "", "")
    assert verifier.is_ancestor(REPO, MERGE_SHA, "origin/main") is True
    runner.results[argv] = GitResult(1, "", "")
    assert verifier.is_ancestor(REPO, MERGE_SHA, "origin/main") is False


def test_is_ancestor_unknown_exit_code_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    argv = ("git", "merge-base", "--is-ancestor", MERGE_SHA, "origin/main")
    runner.results[argv] = GitResult(128, "", "fatal: not a valid commit")
    with pytest.raises(GitError, match="merge-base"):
        verifier.is_ancestor(REPO, MERGE_SHA, "origin/main")


def test_tree_of_returns_validated_tree_id(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    argv = ("git", "rev-parse", "--verify", f"{CANDIDATE}^{{tree}}")
    runner.results[argv] = GitResult(0, TREE + "\n", "")
    assert verifier.tree_of(REPO, CANDIDATE) == TREE


def test_head_of_returns_validated_commit_id(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    argv = ("git", "rev-parse", "--verify", "origin/main^{commit}")
    runner.results[argv] = GitResult(0, MERGE_SHA + "\n", "")
    assert verifier.head_of(REPO, "origin/main") == MERGE_SHA


def test_head_of_failure_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    argv = ("git", "rev-parse", "--verify", "origin/main^{commit}")
    runner.results[argv] = GitResult(128, "", "fatal: needed a single revision")
    with pytest.raises(GitError, match="rev-parse"):
        verifier.head_of(REPO, "origin/main")


def test_tree_of_rejects_malformed_output(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    argv = ("git", "rev-parse", "--verify", f"{CANDIDATE}^{{tree}}")
    runner.results[argv] = GitResult(0, "not-a-tree\n", "")
    with pytest.raises(GitError, match="tree"):
        verifier.tree_of(REPO, CANDIDATE)


def test_tree_of_failure_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    argv = ("git", "rev-parse", "--verify", f"{CANDIDATE}^{{tree}}")
    runner.results[argv] = GitResult(128, "", "fatal: needed a single revision")
    with pytest.raises(GitError, match="rev-parse"):
        verifier.tree_of(REPO, CANDIDATE)


def test_first_parent_returns_validated_commit_id(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    argv = ("git", "rev-parse", "--verify", f"{MERGE_SHA}^1")
    runner.results[argv] = GitResult(0, CANDIDATE + "\n", "")
    assert verifier.first_parent(REPO, MERGE_SHA) == CANDIDATE


def test_first_parent_rejects_malformed_output(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    argv = ("git", "rev-parse", "--verify", f"{MERGE_SHA}^1")
    runner.results[argv] = GitResult(0, "not-a-sha\n", "")
    with pytest.raises(GitError, match="parent"):
        verifier.first_parent(REPO, MERGE_SHA)


def test_first_parent_failure_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    argv = ("git", "rev-parse", "--verify", f"{MERGE_SHA}^1")
    runner.results[argv] = GitResult(128, "", "fatal: needed a single revision")
    with pytest.raises(GitError, match="rev-parse"):
        verifier.first_parent(REPO, MERGE_SHA)


def test_first_parent_requires_full_lowercase_commit_sha(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    with pytest.raises(GitError):
        verifier.first_parent(REPO, "HEAD")
    assert runner.calls == []


def test_changed_paths_returns_sorted_name_status_lines(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    argv = ("git", "diff", "--name-status", "--no-renames", CANDIDATE, MERGE_SHA)
    runner.results[argv] = GitResult(
        0, "M\tb.txt\nA\ta.txt\nD\tc.txt\n", ""
    )
    assert verifier.changed_paths(REPO, CANDIDATE, MERGE_SHA) == (
        "A\ta.txt",
        "D\tc.txt",
        "M\tb.txt",
    )


def test_changed_paths_drops_trailing_blank_lines(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    argv = ("git", "diff", "--name-status", "--no-renames", CANDIDATE, MERGE_SHA)
    runner.results[argv] = GitResult(0, "M\ta.txt\n\n", "")
    assert verifier.changed_paths(REPO, CANDIDATE, MERGE_SHA) == ("M\ta.txt",)


def test_changed_paths_failure_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    argv = ("git", "diff", "--name-status", "--no-renames", CANDIDATE, MERGE_SHA)
    runner.results[argv] = GitResult(128, "", "fatal: bad revision")
    with pytest.raises(GitError, match="git diff"):
        verifier.changed_paths(REPO, CANDIDATE, MERGE_SHA)


def test_changed_paths_requires_full_lowercase_commit_shas(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    with pytest.raises(GitError):
        verifier.changed_paths(REPO, "HEAD", MERGE_SHA)
    assert runner.calls == []


PATCH_ID = "d" * 40


def test_patch_id_diffs_then_pipes_stdin_to_patch_id(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    diff_argv = ("git", "diff", "--full-index", CANDIDATE, MERGE_SHA)
    patch_argv = ("git", "patch-id", "--stable")
    runner.results[diff_argv] = GitResult(0, "diff --git a/x b/x\n", "")
    runner.results[patch_argv] = GitResult(0, f"{PATCH_ID} {MERGE_SHA}\n", "")
    assert verifier.patch_id(REPO, CANDIDATE, MERGE_SHA) == PATCH_ID
    assert runner.calls == [
        (diff_argv, REPO, None),
        (patch_argv, REPO, "diff --git a/x b/x\n"),
    ]


def test_patch_id_diff_failure_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    diff_argv = ("git", "diff", "--full-index", CANDIDATE, MERGE_SHA)
    runner.results[diff_argv] = GitResult(128, "", "fatal: bad revision")
    with pytest.raises(GitError, match="git diff"):
        verifier.patch_id(REPO, CANDIDATE, MERGE_SHA)


def test_patch_id_command_failure_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    diff_argv = ("git", "diff", "--full-index", CANDIDATE, MERGE_SHA)
    patch_argv = ("git", "patch-id", "--stable")
    runner.results[diff_argv] = GitResult(0, "diff --git a/x b/x\n", "")
    runner.results[patch_argv] = GitResult(128, "", "fatal: broken")
    with pytest.raises(GitError, match="git patch-id"):
        verifier.patch_id(REPO, CANDIDATE, MERGE_SHA)


def test_patch_id_empty_diff_raises_git_error(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """An empty diff yields no patch-id line: fail closed rather than guess."""

    diff_argv = ("git", "diff", "--full-index", CANDIDATE, MERGE_SHA)
    patch_argv = ("git", "patch-id", "--stable")
    runner.results[diff_argv] = GitResult(0, "", "")
    runner.results[patch_argv] = GitResult(0, "", "")
    with pytest.raises(GitError, match="patch id"):
        verifier.patch_id(REPO, CANDIDATE, MERGE_SHA)


def test_patch_id_rejects_malformed_output(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    diff_argv = ("git", "diff", "--full-index", CANDIDATE, MERGE_SHA)
    patch_argv = ("git", "patch-id", "--stable")
    runner.results[diff_argv] = GitResult(0, "diff --git a/x b/x\n", "")
    runner.results[patch_argv] = GitResult(0, "not-a-patch-id\n", "")
    with pytest.raises(GitError, match="malformed"):
        verifier.patch_id(REPO, CANDIDATE, MERGE_SHA)


def test_patch_id_requires_full_lowercase_commit_shas(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    with pytest.raises(GitError):
        verifier.patch_id(REPO, "HEAD", MERGE_SHA)
    assert runner.calls == []


@pytest.mark.parametrize(
    "remote,branch",
    [
        ("-origin", "main"),
        ("origin", "-main"),
        ("origin", "--upload-pack=/bin/true"),
        ("origin", "bad branch"),
        ("origin", "a..b"),
        ("", "main"),
        ("origin", ""),
        ("origin", "bad\nbranch"),
    ],
)
def test_fetch_rejects_unsafe_names_before_any_argv(
    verifier: GitVerifier, runner: FakeRunner, remote: str, branch: str
) -> None:
    with pytest.raises(GitError):
        verifier.fetch(REPO, remote, branch)
    assert runner.calls == []


@pytest.mark.parametrize("commit", ["HEAD", "-" + "a" * 39, "A" * 40, "a" * 39])
def test_ancestry_requires_full_lowercase_commit_sha(
    verifier: GitVerifier, runner: FakeRunner, commit: str
) -> None:
    with pytest.raises(GitError):
        verifier.is_ancestor(REPO, commit, "origin/main")
    assert runner.calls == []


def test_ancestry_rejects_unsafe_ref(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    with pytest.raises(GitError):
        verifier.is_ancestor(REPO, MERGE_SHA, "--all")
    assert runner.calls == []


def test_subprocess_runner_proves_real_ancestry(tmp_path: Path) -> None:
    """End-to-end: real git history satisfies the ancestry and tree proofs."""

    repo = tmp_path / "repo"
    repo.mkdir()
    env = {
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.invalid",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.invalid",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "PATH": "/usr/bin:/bin",
    }

    def git(*args: str) -> str:
        completed = subprocess.run(
            ("git", *args),
            cwd=repo,
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        return completed.stdout.strip()

    git("init", "--initial-branch=main")
    (repo / "a.txt").write_text("one\n")
    git("add", "a.txt")
    git("commit", "-m", "first")
    first = git("rev-parse", "HEAD")
    (repo / "a.txt").write_text("two\n")
    git("commit", "-am", "second")
    second = git("rev-parse", "HEAD")

    verifier = GitVerifier(runner=SubprocessGitRunner(env=env))
    assert verifier.is_ancestor(repo, first, "main") is True
    assert verifier.is_ancestor(repo, second, first) is False
    assert verifier.tree_of(repo, second) == git("rev-parse", "HEAD^{tree}")
