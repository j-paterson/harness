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
    WorktreeGit,
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
        "--unified=3",
        base,
        head,
    )


def _edit_diff_argv(path: str) -> tuple[str, ...]:
    return (
        "git",
        "diff",
        "-U0",
        "--no-renames",
        "--no-textconv",
        "--no-ext-diff",
        BASE,
        PARENT,
        "--",
        path,
    )


def _show_argv(path: str) -> tuple[str, ...]:
    return ("git", "show", f"{PARENT}:{path}")


_READ_TREE_ARGV = ("git", "read-tree", PARENT)
_APPLY_ARGV = (
    "git",
    "apply",
    "--cached",
    "--binary",
    "--whitespace=nowarn",
    "--verbose",
)
_WRITE_TREE_ARGV = ("git", "write-tree")

# A single-file, single-hunk reviewed patch: context "line1", removed
# "line2" replaced by "LINE2" -- old_len=2 (one context, one removed).
_PATCH_TEXT = (
    "diff --git a/x b/x\n--- a/x\n+++ b/x\n@@ -1,2 +1,2 @@\n"
    " line1\n-line2\n+LINE2\n"
)
# The unmodified parent (no base-to-parent edits at all): the preimage
# occurs exactly once, at its unshifted base position (line 1).
_PARENT_FILE_TEXT = "line1\nline2\n"
_APPLY_STDERR_NO_OFFSET = "Checking patch x...\nApplied patch x cleanly.\n"


def _wire_success(runner: FakeRunner, *, tree: str = TREE) -> None:
    diff_argv = _diff_argv(BASE, CANDIDATE)
    runner.results[diff_argv] = GitResult(0, _PATCH_TEXT, "")
    runner.results[_edit_diff_argv("x")] = GitResult(0, "", "")
    runner.results[_show_argv("x")] = GitResult(0, _PARENT_FILE_TEXT, "")
    runner.results[_READ_TREE_ARGV] = GitResult(0, "", "")
    runner.results[_APPLY_ARGV] = GitResult(0, "", _APPLY_STDERR_NO_OFFSET)
    runner.results[_WRITE_TREE_ARGV] = GitResult(0, tree + "\n", "")


def test_apply_to_tree_runs_the_exact_argv_sequence(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """diff (no-textconv/no-ext-diff/no-renames/binary/full-index), then
    the positional proof's per-file -U0 base-to-parent edit diff and
    ``git show`` of the parent's file content, then read-tree the parent,
    then apply --cached --verbose the patch as stdin, then write-tree --
    each read-tree/apply/write-tree call sharing one GIT_INDEX_FILE env
    that points inside a temp dir removed afterward."""

    _wire_success(runner)

    tree = verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)

    assert tree == TREE
    argvs = [call[0] for call in runner.calls]
    assert argvs == [
        _diff_argv(BASE, CANDIDATE),
        _edit_diff_argv("x"),
        _show_argv("x"),
        _READ_TREE_ARGV,
        _APPLY_ARGV,
        _WRITE_TREE_ARGV,
    ]
    diff_call, edit_call, show_call, read_call, apply_call, write_call = runner.calls
    assert diff_call[2] is None and diff_call[3] is None
    assert edit_call[2] is None and edit_call[3] is None
    assert show_call[2] is None and show_call[3] is None
    assert apply_call[2] == _PATCH_TEXT
    for call in (read_call, apply_call, write_call):
        assert call[3] is not None
        assert "GIT_INDEX_FILE" in call[3]
    assert read_call[3] == apply_call[3] == write_call[3]
    index_path = Path(read_call[3]["GIT_INDEX_FILE"])
    assert index_path.name == "index"
    # The temp directory holding the throwaway index is always removed.
    assert not index_path.parent.exists()


def test_apply_to_tree_never_passes_3way_reject_or_fuzzy_context(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    _wire_success(runner)
    verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)
    for args, _cwd, _input, _env in runner.calls:
        assert "--3way" not in args
        assert "--reject" not in args
        assert "-C" not in args


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
    _wire_success(runner)
    runner.results[_READ_TREE_ARGV] = GitResult(128, "", "fatal: not a tree")
    with pytest.raises(GitError, match="git read-tree"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)
    read_call = runner.calls[3]
    index_path = Path(read_call[3]["GIT_INDEX_FILE"])
    assert not index_path.parent.exists()


def test_apply_to_tree_apply_failure_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """Any rejected hunk, conflict, or corrupt patch is a non-zero exit
    from ``git apply`` and must fail closed."""

    _wire_success(runner)
    runner.results[_APPLY_ARGV] = GitResult(1, "", "error: patch does not apply")
    with pytest.raises(GitError, match="git apply"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)
    apply_call = runner.calls[4]
    index_path = Path(apply_call[3]["GIT_INDEX_FILE"])
    assert not index_path.parent.exists()


def test_apply_to_tree_write_tree_failure_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    _wire_success(runner)
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


# -- reviewed-patch parser -----------------------------------------------


def test_apply_to_tree_skips_devnull_and_binary_files_for_positional_proof(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """Added (``--- /dev/null``), deleted (``+++ /dev/null``), and binary
    (``GIT binary patch``) files carry no positional hunks: the positional
    proof never issues a -U0 diff or ``git show`` for them. FakeRunner
    would raise on any unregistered argv, so wiring only "x" and still
    succeeding proves the other two files were skipped."""

    patch = (
        "diff --git a/new.txt b/new.txt\nnew file mode 100644\n"
        "--- /dev/null\n+++ b/new.txt\n@@ -0,0 +1 @@\n+added\n"
        "diff --git a/bin.dat b/bin.dat\nindex 111..222 100644\n"
        "GIT binary patch\nliteral 4\nLcmZQzWMT#Y01f~L\n\n"
    ) + _PATCH_TEXT
    diff_argv = _diff_argv(BASE, CANDIDATE)
    runner.results[diff_argv] = GitResult(0, patch, "")
    runner.results[_edit_diff_argv("x")] = GitResult(0, "", "")
    runner.results[_show_argv("x")] = GitResult(0, _PARENT_FILE_TEXT, "")
    runner.results[_READ_TREE_ARGV] = GitResult(0, "", "")
    runner.results[_APPLY_ARGV] = GitResult(
        0,
        "",
        "Checking patch new.txt...\nChecking patch bin.dat...\n"
        + _APPLY_STDERR_NO_OFFSET,
    )
    runner.results[_WRITE_TREE_ARGV] = GitResult(0, TREE + "\n", "")

    tree = verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)

    assert tree == TREE
    argvs = [call[0] for call in runner.calls]
    assert _edit_diff_argv("new.txt") not in argvs
    assert _edit_diff_argv("bin.dat") not in argvs
    assert _show_argv("new.txt") not in argvs
    assert _show_argv("bin.dat") not in argvs


def test_apply_to_tree_rejects_hunk_before_any_file_header(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    diff_argv = _diff_argv(BASE, CANDIDATE)
    runner.results[diff_argv] = GitResult(
        0, "@@ -1,2 +1,2 @@\n line1\n-line2\n+LINE2\n", ""
    )
    with pytest.raises(GitError, match="malformed reviewed patch"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)
    assert [call[0] for call in runner.calls] == [diff_argv]


def test_apply_to_tree_rejects_unparseable_hunk_header(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    diff_argv = _diff_argv(BASE, CANDIDATE)
    runner.results[diff_argv] = GitResult(
        0, "diff --git a/x b/x\n@@ not a header @@\n line1\n", ""
    )
    with pytest.raises(GitError, match="malformed reviewed patch"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)
    assert [call[0] for call in runner.calls] == [diff_argv]


def test_apply_to_tree_rejects_preimage_length_mismatch(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    diff_argv = _diff_argv(BASE, CANDIDATE)
    # old_len declared as 3 but only 2 preimage lines follow.
    runner.results[diff_argv] = GitResult(
        0, "diff --git a/x b/x\n@@ -1,3 +1,3 @@\n line1\n-line2\n+LINE2\n", ""
    )
    with pytest.raises(GitError, match="malformed reviewed patch"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)
    assert [call[0] for call in runner.calls] == [diff_argv]


# -- base-to-parent edit parsing / intersection / shift -------------------

# A reusable hunk targeting base lines 10-12 (old_len=3), so edits can be
# placed unambiguously above, inside, or just below its range.
_RANGE_PATCH_TEXT = (
    "diff --git a/x b/x\n@@ -10,3 +10,3 @@\n p1\n-p2\n+P2\n p3\n"
)


def _wire_range_patch(runner: FakeRunner, edit_diff_stdout: str) -> None:
    diff_argv = _diff_argv(BASE, CANDIDATE)
    runner.results[diff_argv] = GitResult(0, _RANGE_PATCH_TEXT, "")
    runner.results[_edit_diff_argv("x")] = GitResult(0, edit_diff_stdout, "")


def test_verify_hunk_positions_edit_diff_failure_fails_closed(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    diff_argv = _diff_argv(BASE, CANDIDATE)
    runner.results[diff_argv] = GitResult(0, _RANGE_PATCH_TEXT, "")
    runner.results[_edit_diff_argv("x")] = GitResult(128, "", "fatal: bad revision")
    with pytest.raises(GitError, match="git diff -U0"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)


def test_verify_hunk_positions_rejects_intersecting_content_edit(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """A base-to-parent edit whose old range overlaps the hunk's preimage
    range means the reviewed target itself changed on the parent."""

    _wire_range_patch(runner, "@@ -11,1 +11,1 @@\n-x\n+y\n")
    with pytest.raises(
        GitError,
        match=r"reviewed hunk target changed on the merge parent: x hunk #1",
    ):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)


def test_verify_hunk_positions_rejects_insertion_strictly_inside_range(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """A pure insertion (old_len=0) landing strictly inside [10,12) is
    treated as intersecting, not as a harmless nearby shift."""

    _wire_range_patch(runner, "@@ -11,0 +12,2 @@\n+a\n+b\n")
    with pytest.raises(
        GitError,
        match=r"reviewed hunk target changed on the merge parent: x hunk #1",
    ):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)


def _range_parent_text(*, mapped: int) -> str:
    filler = [f"f{i}" for i in range(1, mapped)]
    return "\n".join([*filler, "p1", "p2", "p3"]) + "\n"


def test_verify_hunk_positions_insertion_above_shifts_and_boundary_below_does_not(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """An insertion strictly above the range (l=9 < 10) contributes its
    full +2 to the shift; a second insertion exactly at the range's last
    line (l=12, the boundary just below the range) is neither "above" nor
    intersecting and contributes nothing -- net shift is +2, mapped=12."""

    _wire_range_patch(
        runner,
        "@@ -9,0 +10,2 @@\n+ins-above-1\n+ins-above-2\n"
        "@@ -12,0 +15,1 @@\n+ins-below-boundary\n",
    )
    runner.results[_show_argv("x")] = GitResult(0, _range_parent_text(mapped=12), "")
    runner.results[_READ_TREE_ARGV] = GitResult(0, "", "")
    runner.results[_APPLY_ARGV] = GitResult(
        0, "", "Checking patch x...\nHunk #1 succeeded at 12 (offset 2 lines).\n"
    )
    runner.results[_WRITE_TREE_ARGV] = GitResult(0, TREE + "\n", "")

    tree = verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)

    assert tree == TREE


def test_verify_hunk_positions_deletion_above_yields_negative_shift(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """A deletion strictly above the range contributes new_len - old_len
    (negative) to the shift; mapped = old_start + shift = 10 - 2 = 8."""

    _wire_range_patch(runner, "@@ -3,2 +3,0 @@\n-d1\n-d2\n")
    runner.results[_show_argv("x")] = GitResult(0, _range_parent_text(mapped=8), "")
    runner.results[_READ_TREE_ARGV] = GitResult(0, "", "")
    runner.results[_APPLY_ARGV] = GitResult(
        0, "", "Checking patch x...\nHunk #1 succeeded at 8 (offset -2 lines).\n"
    )
    runner.results[_WRITE_TREE_ARGV] = GitResult(0, TREE + "\n", "")

    tree = verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)

    assert tree == TREE


# -- preimage occurrence rule ----------------------------------------------


def test_verify_hunk_positions_rejects_zero_occurrences_in_parent(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    _wire_range_patch(runner, "")
    runner.results[_show_argv("x")] = GitResult(0, "nothing\nhere\nmatches\n", "")
    with pytest.raises(
        GitError,
        match=(
            r"reviewed hunk is ambiguous or relocated on the merge parent: "
            r"x hunk #1 \(occurrences=0, expected line 10\)"
        ),
    ):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)


def test_verify_hunk_positions_rejects_two_occurrences_in_parent(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    _wire_range_patch(runner, "")
    doubled = _range_parent_text(mapped=10) + _range_parent_text(mapped=1)
    runner.results[_show_argv("x")] = GitResult(0, doubled, "")
    with pytest.raises(
        GitError,
        match=(
            r"reviewed hunk is ambiguous or relocated on the merge parent: "
            r"x hunk #1 \(occurrences=2, expected line 10\)"
        ),
    ):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)


def test_verify_hunk_positions_rejects_single_occurrence_at_wrong_line(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """Zero base-to-parent edits means the expected mapped line is 10 (no
    shift), but the preimage actually sits at line 5 in the parent: a
    relocation the intersection rule alone cannot catch."""

    _wire_range_patch(runner, "")
    runner.results[_show_argv("x")] = GitResult(0, _range_parent_text(mapped=5), "")
    with pytest.raises(
        GitError,
        match=(
            r"reviewed hunk is ambiguous or relocated on the merge parent: "
            r"x hunk #1 \(occurrences=1, expected line 10\)"
        ),
    ):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)


# -- --verbose fuzz/offset cross-check --------------------------------------


def test_apply_to_tree_rejects_fuzz_reported_in_apply_stderr(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    _wire_success(runner)
    runner.results[_APPLY_ARGV] = GitResult(
        0,
        "",
        "Checking patch x...\nHunk #1 succeeded at 1 with fuzz 1 (offset 0 lines).\n",
    )
    with pytest.raises(GitError, match="fuzz"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)
    assert all(call[0] != _WRITE_TREE_ARGV for call in runner.calls)


def test_apply_to_tree_rejects_offset_that_differs_from_computed_shift(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """The positional proof computed shift=0 for this hunk (no base-to-
    parent edits at all), but ``git apply`` reports a nonzero offset --
    the two disagree and must fail closed."""

    _wire_success(runner)
    runner.results[_APPLY_ARGV] = GitResult(
        0, "", "Checking patch x...\nHunk #1 succeeded at 5 (offset 4 lines).\n"
    )
    with pytest.raises(GitError, match="reviewed hunk applied at an unexpected offset"):
        verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)
    assert all(call[0] != _WRITE_TREE_ARGV for call in runner.calls)


def test_apply_to_tree_accepts_offset_that_matches_computed_shift(
    verifier: GitVerifier, runner: FakeRunner
) -> None:
    """A nonzero offset is accepted when it exactly matches the shift the
    positional proof computed from the base-to-parent edits above the
    hunk (the live PR #39 acceptance shape: legitimate unrelated inserts
    above push the hunk down, and git apply reports that same offset)."""

    _wire_range_patch(runner, "@@ -9,0 +10,2 @@\n+ins-above-1\n+ins-above-2\n")
    runner.results[_show_argv("x")] = GitResult(0, _range_parent_text(mapped=12), "")
    runner.results[_READ_TREE_ARGV] = GitResult(0, "", "")
    runner.results[_APPLY_ARGV] = GitResult(
        0, "", "Checking patch x...\nHunk #1 succeeded at 12 (offset 2 lines).\n"
    )
    runner.results[_WRITE_TREE_ARGV] = GitResult(0, TREE + "\n", "")

    tree = verifier.apply_to_tree(REPO, BASE, CANDIDATE, PARENT)

    assert tree == TREE


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


# -- WorktreeGit.worktree_add_detached (INFRA-198 P2) ------------------


@pytest.fixture
def worktree_git(runner: FakeRunner) -> WorktreeGit:
    return WorktreeGit(runner=runner)


def test_worktree_add_detached_runs_exact_argv(
    worktree_git: WorktreeGit, runner: FakeRunner, tmp_path: Path
) -> None:
    target = tmp_path / "checkouts" / MERGE_SHA
    runner.results[("git", "worktree", "add", "--detach", str(target), MERGE_SHA)] = (
        GitResult(0, "", "")
    )

    worktree_git.worktree_add_detached(REPO, target, MERGE_SHA)

    assert runner.calls == [
        (
            ("git", "worktree", "add", "--detach", str(target), MERGE_SHA),
            REPO,
            None,
            None,
        )
    ]


def test_worktree_add_detached_requires_full_lowercase_sha(
    worktree_git: WorktreeGit, runner: FakeRunner, tmp_path: Path
) -> None:
    target = tmp_path / "checkouts" / "x"
    with pytest.raises(GitError):
        worktree_git.worktree_add_detached(REPO, target, "not-a-sha")
    assert runner.calls == []


def test_worktree_add_detached_requires_absolute_path(
    worktree_git: WorktreeGit, runner: FakeRunner
) -> None:
    with pytest.raises(GitError):
        worktree_git.worktree_add_detached(
            REPO, Path("relative/checkout"), MERGE_SHA
        )
    assert runner.calls == []


def test_worktree_add_detached_raises_on_git_failure(
    worktree_git: WorktreeGit, runner: FakeRunner, tmp_path: Path
) -> None:
    target = tmp_path / "checkouts" / MERGE_SHA
    runner.results[("git", "worktree", "add", "--detach", str(target), MERGE_SHA)] = (
        GitResult(1, "", "fatal: already exists")
    )
    with pytest.raises(GitError):
        worktree_git.worktree_add_detached(REPO, target, MERGE_SHA)
