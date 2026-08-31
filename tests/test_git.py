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
    calls: list[
        tuple[tuple[str, ...], Path, str | None, dict[str, str] | None]
    ] = field(default_factory=list)

    def run(
        self,
        args: tuple[str, ...],
        cwd: Path,
        *,
        input: str | None = None,
        env: dict[str, str] | None = None,
    ) -> GitResult:
        self.calls.append((args, cwd, input, env))
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
        (("git", "fetch", "--", "origin", "main"), REPO, None, None)
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


BASE = "e" * 40
PARENT = "f" * 40


def _diff_argv(base: str, head: str) -> tuple[str, ...]:
    return (
        "git",
        "diff",
        "--full-index",
        "--binary",
        "--no-renames",
        "--no-textconv",
        "--no-ext-diff",
        base,
        head,
    )


_READ_TREE_ARGV = ("git", "read-tree", PARENT)
_APPLY_ARGV = ("git", "apply", "--cached", "--binary", "--whitespace=nowarn")
_WRITE_TREE_ARGV = ("git", "write-tree")
_PATCH_TEXT = "diff --git a/x b/x\n@@ -1,2 +1,2 @@\n-old\n+new\n"


def _wire_success(runner: FakeRunner, *, tree: str = TREE) -> None:
    diff_argv = _diff_argv(BASE, CANDIDATE)
    runner.results[diff_argv] = GitResult(0, _PATCH_TEXT, "")
    runner.results[_READ_TREE_ARGV] = GitResult(0, "", "")
    runner.results[_APPLY_ARGV] = GitResult(0, "", "")
    runner.results[_WRITE_TREE_ARGV] = GitResult(0, tree + "\n", "")


def test_apply_to_tree_runs_the_exact_argv_sequence(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """diff (no-textconv/no-ext-diff/no-renames/binary/full-index), then
    read-tree the parent, then apply --cached the patch as stdin, then
    write-tree -- each read-tree/apply/write-tree call sharing one
    GIT_INDEX_FILE env that points inside a temp dir removed afterward."""

    _wire_success(runner)

    tree = verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)

    assert tree == TREE
    argvs = [call[0] for call in runner.calls]
    assert argvs == [
        _diff_argv(BASE, CANDIDATE),
        _READ_TREE_ARGV,
        _APPLY_ARGV,
        _WRITE_TREE_ARGV,
    ]
    diff_call, read_call, apply_call, write_call = runner.calls
    assert diff_call[2] is None and diff_call[3] is None
    assert apply_call[2] == _PATCH_TEXT
    for call in (read_call, apply_call, write_call):
        assert call[3] is not None
        assert "GIT_INDEX_FILE" in call[3]
    assert read_call[3] == apply_call[3] == write_call[3]
    index_path = Path(read_call[3]["GIT_INDEX_FILE"])
    assert index_path.name == "index"
    # The temp directory holding the throwaway index is always removed.
    assert not index_path.parent.exists()


def test_apply_to_tree_never_passes_3way_or_reject(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    _wire_success(runner)
    verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)
    for args, _cwd, _input, _env in runner.calls:
        assert "--3way" not in args
        assert "--reject" not in args


def test_apply_to_tree_diff_failure_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    diff_argv = _diff_argv(BASE, CANDIDATE)
    runner.results[diff_argv] = GitResult(128, "", "fatal: bad revision")
    with pytest.raises(GitError, match="git diff"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)
    assert [call[0] for call in runner.calls] == [diff_argv]


def test_apply_to_tree_empty_patch_raises_git_error(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """An empty diff is never proof: fail closed before ever touching a
    throwaway index."""

    diff_argv = _diff_argv(BASE, CANDIDATE)
    runner.results[diff_argv] = GitResult(0, "", "")
    with pytest.raises(GitError, match="empty delta"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)
    assert [call[0] for call in runner.calls] == [diff_argv]


def test_apply_to_tree_read_tree_failure_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    diff_argv = _diff_argv(BASE, CANDIDATE)
    runner.results[diff_argv] = GitResult(0, _PATCH_TEXT, "")
    runner.results[_READ_TREE_ARGV] = GitResult(128, "", "fatal: not a tree")
    with pytest.raises(GitError, match="git read-tree"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)
    read_call = runner.calls[1]
    index_path = Path(read_call[3]["GIT_INDEX_FILE"])
    assert not index_path.parent.exists()


def test_apply_to_tree_apply_failure_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """Any rejected hunk, conflict, or corrupt patch is a non-zero exit
    from ``git apply`` and must fail closed."""

    diff_argv = _diff_argv(BASE, CANDIDATE)
    runner.results[diff_argv] = GitResult(0, _PATCH_TEXT, "")
    runner.results[_READ_TREE_ARGV] = GitResult(0, "", "")
    runner.results[_APPLY_ARGV] = GitResult(1, "", "error: patch does not apply")
    with pytest.raises(GitError, match="git apply"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)
    apply_call = runner.calls[2]
    index_path = Path(apply_call[3]["GIT_INDEX_FILE"])
    assert not index_path.parent.exists()


def test_apply_to_tree_write_tree_failure_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    diff_argv = _diff_argv(BASE, CANDIDATE)
    runner.results[diff_argv] = GitResult(0, _PATCH_TEXT, "")
    runner.results[_READ_TREE_ARGV] = GitResult(0, "", "")
    runner.results[_APPLY_ARGV] = GitResult(0, "", "")
    runner.results[_WRITE_TREE_ARGV] = GitResult(128, "", "fatal: broken index")
    with pytest.raises(GitError, match="git write-tree"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)


def test_apply_to_tree_rejects_malformed_tree_id(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    _wire_success(runner, tree=TREE)
    runner.results[_WRITE_TREE_ARGV] = GitResult(0, "not-a-tree\n", "")
    with pytest.raises(GitError, match="tree"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)


@pytest.mark.parametrize(
    "base,head,parent",
    [
        ("HEAD", CANDIDATE, PARENT),
        (BASE, "HEAD", PARENT),
        (BASE, CANDIDATE, "HEAD"),
    ],
    ids=["bad-base", "bad-head", "bad-parent"],
)
def test_apply_to_tree_requires_full_lowercase_commit_shas(
    verifier: GitVerifier, runner: FakeRunner, base: str, head: str, parent: str
) -> None:
    with pytest.raises(GitError):
        verifier.apply_to_tree(REPO, base, head, parent)
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
