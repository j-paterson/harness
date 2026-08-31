"""Real-git proofs for the isolated Git application proof of patch equivalence.

INFRA-210 Sol correction 85d45884: an earlier design proved
``patch_equivalent`` by comparing a canonical digest of two deltas with
hunk coordinates normalized. That is unsound two ways: (1) two identical
repeated blocks in one file -- the candidate changing the first occurrence
and the squash changing the second -- digest equal because normalized hunk
text is byte-identical at both locations; (2) ``git diff`` honors any
repository-configured ``textconv``/``ext-diff`` driver, so a lossy driver
can make two genuinely different blobs render identically. The fix
(``GitVerifier.apply_to_tree``) is never a digest: it applies the exact
reviewed patch -- generated with ``--no-textconv --no-ext-diff`` so no
driver can hide content -- to the merge's first parent inside a throwaway
index, and accepts the result only when the written tree is byte-identical
to the merge commit's own tree.

These tests run real ``git`` (2.53, via :class:`SubprocessGitRunner` and
:class:`GitVerifier`, and :class:`IntegrationMerge` over that real verifier
with a fake GitHub/MergeClient) against ``tmp_path`` repositories that carry
a genuine ``origin`` remote, mirroring how ``_prove`` fetches
``origin/<integration_branch>`` and checks ``is_ancestor``. No fake ever
stands in for git here: every proof is produced by the real
``GitVerifier.apply_to_tree``.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.git import GitError, GitVerifier, SubprocessGitRunner
from hermes_orchestrator.merge import IntegrationMerge, ReconciliationRequired
from tests.test_merge import FakeGitHub

REPOSITORY = "j-paterson/demo"

_ENV = {
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@example.invalid",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@example.invalid",
    "GIT_CONFIG_GLOBAL": "/dev/null",
    "GIT_CONFIG_SYSTEM": "/dev/null",
    "PATH": "/usr/bin:/bin",
}

# -- fixture content ---------------------------------------------------------
#
# BASE is the pre-delta content of every file the "exact delta" touches.
# EXACT is the reviewed candidate's delta, reused verbatim by the positive
# squash so their diffs against their respective (different) bases must be
# byte-identical. Each negative variant copies EXACT and changes exactly one
# byte-level dimension while keeping every path's name-status unchanged.

BASE_FILES = {
    "foo.py": "def foo():\n    x = 1\n    return x\n",
    "config.yaml": "a:\n  b: 1\n  c: 2\n",
    "Makefile": "target:\n\tcommand\n",
    "mode_file.sh": "echo hi\n",
}
BASE_BIN = bytes([0, 1, 2, 3, 4, 5, 6, 7])

EXACT_FILES = {
    "foo.py": "def foo():\n    if True:\n        x = 1\n    return x\n",
    "config.yaml": "a:\n  b: 1\n  nested:\n    c: 2\n",
    "Makefile": "target:\n\tcommand\n\tnewcommand\n",
    "mode_file.sh": "echo hi\necho bye\n",
}
EXACT_BIN = bytes([0, 1, 2, 3, 4, 5, 6, 7, 9, 9])

# (b) Python: the nested body line is indented two spaces past "if True:"
# instead of four -- same lines touched, different whitespace.
NEG_PY_FILES = dict(EXACT_FILES)
NEG_PY_FILES["foo.py"] = "def foo():\n    if True:\n      x = 1\n    return x\n"

# (c) YAML: the nested "c: 2" is indented six spaces instead of four -- same
# structural change, different indentation depth.
NEG_YAML_FILES = dict(EXACT_FILES)
NEG_YAML_FILES["config.yaml"] = "a:\n  b: 1\n  nested:\n      c: 2\n"

# (d) Makefile: the new recipe line uses four spaces instead of a tab.
NEG_MAKE_FILES = dict(EXACT_FILES)
NEG_MAKE_FILES["Makefile"] = "target:\n\tcommand\n    newcommand\n"

# (f) binary: the appended tail bytes differ from EXACT_BIN's.
NEG_BIN = bytes([0, 1, 2, 3, 4, 5, 6, 7, 10, 10])


def _git(cwd: Path, *args: str) -> str:
    completed = subprocess.run(
        ("git", *args),
        cwd=cwd,
        env=_ENV,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    return completed.stdout.strip()


def _head(cwd: Path) -> str:
    return _git(cwd, "rev-parse", "HEAD")


def _write_files(repo_path: Path, files: dict[str, str], binary: bytes) -> None:
    for name, content in files.items():
        (repo_path / name).write_text(content)
    (repo_path / "asset.bin").write_bytes(binary)


class Repo:
    """A real working checkout with a genuine bare ``origin`` remote."""

    def __init__(self, tmp_path: Path) -> None:
        self.origin_path = tmp_path / "origin.git"
        self.origin_path.mkdir()
        _git(self.origin_path, "init", "--bare", "--initial-branch=main")

        self.path = tmp_path / "work"
        self.path.mkdir()
        _git(self.path, "init", "--initial-branch=main")
        _git(self.path, "remote", "add", "origin", str(self.origin_path))

    def commit_all(self, message: str) -> str:
        _git(self.path, "add", "-A")
        _git(self.path, "commit", "-m", message)
        return _head(self.path)

    def checkout_new(self, branch: str, start: str) -> None:
        _git(self.path, "checkout", "-b", branch, start)

    def checkout(self, ref: str) -> None:
        _git(self.path, "checkout", ref)

    def chmod_executable(self, relative: str) -> None:
        target = self.path / relative
        target.chmod(target.stat().st_mode | 0o111)

    def push_main(self, sha: str) -> None:
        """Force the origin's ``main`` ref to point exactly at ``sha``."""

        _git(self.path, "push", "--force", "origin", f"{sha}:refs/heads/main")


def _build_base(tmp_path: Path) -> tuple[Repo, str, str, str]:
    """Build base (B0), candidate (C1), and an advanced main tip (A0).

    A0 is B0 plus an unrelated file, so every diff against the touched
    files (foo.py, config.yaml, Makefile, mode_file.sh, asset.bin) starts
    from identical content whether measured from B0 or from A0.
    """

    repo = Repo(tmp_path)
    _write_files(repo.path, BASE_FILES, BASE_BIN)
    base_sha = repo.commit_all("base")

    repo.checkout_new("candidate", base_sha)
    _write_files(repo.path, EXACT_FILES, EXACT_BIN)
    candidate_sha = repo.commit_all("candidate: the reviewed delta")

    repo.checkout("main")
    (repo.path / "unrelated.txt").write_text("advanced main content\n")
    advanced_sha = repo.commit_all("advance main unrelated to the candidate")

    return repo, base_sha, candidate_sha, advanced_sha


def _project(repo: Repo) -> ProjectConfig:
    return ProjectConfig(
        linear_team="infrastructure",
        repo_path=repo.path,
        integration_branch="main",
        github_repo=REPOSITORY,
    )


def _executor(repo: Repo) -> IntegrationMerge:
    verifier = GitVerifier(runner=SubprocessGitRunner(env=_ENV))
    return IntegrationMerge(
        projects={"demo": _project(repo)}, github=FakeGitHub(), git=verifier
    )


def _squash(
    repo: Repo,
    advanced_sha: str,
    name: str,
    files: dict[str, str],
    binary: bytes,
    *,
    chmod_mode_file: bool = False,
    extra_file: str | None = None,
) -> str:
    """Build a squash-merge-shaped commit: a single child of A0."""

    repo.checkout_new(name, advanced_sha)
    _write_files(repo.path, files, binary)
    if chmod_mode_file:
        repo.chmod_executable("mode_file.sh")
    if extra_file is not None:
        (repo.path / extra_file).write_text("extra\n")
    sha = repo.commit_all(name)
    repo.push_main(sha)
    return sha


# -- (b) positive: exact-occurrence squash proves by real application -------


def test_advanced_base_squash_with_identical_delta_proves_patch_equivalent(
    tmp_path: Path,
) -> None:
    """The squash reproduces the candidate's delta exactly: applying the
    reviewed patch to the merge's first parent reproduces the merge's own
    tree, verified independently against a real ``git rev-parse``."""

    repo, base_sha, candidate_sha, advanced_sha = _build_base(tmp_path)
    merge_sha = _squash(repo, advanced_sha, "squash-exact", EXACT_FILES, EXACT_BIN)
    executor = _executor(repo)

    outcome = executor.prove_landed(
        "demo",
        candidate_sha=candidate_sha,
        candidate_branch="candidate",
        pr_number=1,
        merge_sha=merge_sha,
        base_sha=base_sha,
    )

    assert outcome.relation == "patch_equivalent"
    assert outcome.base_sha == base_sha
    assert outcome.merge_parent_sha == advanced_sha
    real_merge_tree = _git(repo.path, "rev-parse", f"{merge_sha}^{{tree}}")
    assert outcome.applied_tree_sha == real_merge_tree
    assert outcome.merge_tree_sha == real_merge_tree


# -- (f) mode/binary/whitespace drift, still one changed hunk ---------------


@pytest.mark.parametrize(
    "files,binary,chmod_mode_file",
    [
        pytest.param(NEG_PY_FILES, EXACT_BIN, False, id="python-indentation"),
        pytest.param(NEG_YAML_FILES, EXACT_BIN, False, id="yaml-indentation"),
        pytest.param(NEG_MAKE_FILES, EXACT_BIN, False, id="makefile-tab-vs-spaces"),
        pytest.param(EXACT_FILES, EXACT_BIN, True, id="executable-mode-flip"),
        pytest.param(EXACT_FILES, NEG_BIN, False, id="binary-byte-drift"),
    ],
)
def test_advanced_base_squash_differing_only_by_one_byte_dimension_is_rejected(
    tmp_path: Path,
    files: dict[str, str],
    binary: bytes,
    chmod_mode_file: bool,
) -> None:
    """A squash that drifts from the candidate's delta by whitespace, an
    executable-mode flip, or binary bytes must be rejected: applying the
    reviewed patch reproduces the *correct* tree, which cannot equal the
    drifted merge tree."""

    repo, base_sha, candidate_sha, advanced_sha = _build_base(tmp_path)
    merge_sha = _squash(
        repo,
        advanced_sha,
        "squash-negative",
        files,
        binary,
        chmod_mode_file=chmod_mode_file,
    )
    executor = _executor(repo)

    with pytest.raises(
        ReconciliationRequired, match="does not reproduce the merge tree"
    ):
        executor.prove_landed(
            "demo",
            candidate_sha=candidate_sha,
            candidate_branch="candidate",
            pr_number=1,
            merge_sha=merge_sha,
            base_sha=base_sha,
        )


# -- (e) extra merge content: rejected by changed paths, before any apply ---


def test_advanced_base_squash_with_an_extra_changed_path_is_rejected_by_paths(
    tmp_path: Path,
) -> None:
    """An extra changed file is a legitimate name-status mismatch: caught
    by the cheap changed-paths equality check, never reaching application."""

    repo, base_sha, candidate_sha, advanced_sha = _build_base(tmp_path)
    merge_sha = _squash(
        repo,
        advanced_sha,
        "squash-extra-file",
        EXACT_FILES,
        EXACT_BIN,
        extra_file="extra.txt",
    )
    executor = _executor(repo)

    with pytest.raises(ReconciliationRequired, match="changed paths differ"):
        executor.prove_landed(
            "demo",
            candidate_sha=candidate_sha,
            candidate_branch="candidate",
            pr_number=1,
            merge_sha=merge_sha,
            base_sha=base_sha,
        )


def test_squash_with_no_changes_vs_parent_fails_closed(tmp_path: Path) -> None:
    """An empty squash (no changes vs its parent) can never stand as
    proof; it fails closed with ReconciliationRequired one way or another
    (here: an empty merge-side delta means the changed-paths equality
    check itself rejects it, since the candidate's delta is non-empty)."""

    repo, base_sha, candidate_sha, advanced_sha = _build_base(tmp_path)
    repo.checkout_new("squash-empty", advanced_sha)
    _git(repo.path, "commit", "--allow-empty", "-m", "empty squash")
    merge_sha = _head(repo.path)
    repo.push_main(merge_sha)
    executor = _executor(repo)

    with pytest.raises(ReconciliationRequired):
        executor.prove_landed(
            "demo",
            candidate_sha=candidate_sha,
            candidate_branch="candidate",
            pr_number=1,
            merge_sha=merge_sha,
            base_sha=base_sha,
        )


def test_apply_to_tree_itself_fails_closed_on_an_empty_diff(tmp_path: Path) -> None:
    """Direct unit-level proof of GitVerifier.apply_to_tree's own guard:
    comparing a commit to itself is an empty diff and must raise GitError
    before any temporary index is ever created."""

    repo, base_sha, _candidate_sha, _advanced_sha = _build_base(tmp_path)
    verifier = GitVerifier(runner=SubprocessGitRunner(env=_ENV))

    with pytest.raises(GitError, match="empty delta"):
        verifier.apply_to_tree(repo.path, base_sha, base_sha, base_sha)


def test_base_unreachable_from_an_unrelated_merge_root_is_rejected(
    tmp_path: Path,
) -> None:
    """The merge's first parent is an orphan root sharing no history with
    the candidate's recorded base: ancestry, not content, fails first."""

    repo, base_sha, candidate_sha, _advanced_sha = _build_base(tmp_path)

    _git(repo.path, "checkout", "--orphan", "unrelated-root")
    _git(repo.path, "rm", "-rf", "--quiet", ".")
    (repo.path / "root.txt").write_text("unrelated history\n")
    root_sha = repo.commit_all("unrelated root")
    merge_sha = _squash(
        repo, root_sha, "squash-on-unrelated-root", EXACT_FILES, EXACT_BIN
    )
    executor = _executor(repo)

    with pytest.raises(
        ReconciliationRequired, match="candidate base is not reachable"
    ):
        executor.prove_landed(
            "demo",
            candidate_sha=candidate_sha,
            candidate_branch="candidate",
            pr_number=1,
            merge_sha=merge_sha,
            base_sha=base_sha,
        )


# -- (g) advanced base touching a candidate file elsewhere (live PR #39) ----
#
# INFRA-201/202 had edited README.md and cli.py in regions far from the
# INFRA-209 hunks before PR #39 was squashed. The hunks (with their context)
# were byte-identical on both sides, but the files' pre-/post-image blob ids
# differed. A digest normalized those blob ids away; the application proof
# never looks at blob ids at all -- it applies the patch and compares the
# resulting *tree*, which is naturally insensitive to blob-id churn caused
# by unrelated edits elsewhere in the same file.

OVERLAP_BASE_PY = (
    "def foo():\n    x = 1\n    return x\n"
    "\n\n# pad 1\n# pad 2\n# pad 3\n# pad 4\n# pad 5\n\n"
    "def tail():\n    return 0\n"
)
OVERLAP_EXACT_PY = (
    "def foo():\n    if True:\n        x = 1\n    return x\n"
    "\n\n# pad 1\n# pad 2\n# pad 3\n# pad 4\n# pad 5\n\n"
    "def tail():\n    return 0\n"
)
# The advanced base changes ``tail`` — five padding lines below foo's hunk,
# beyond git's three context lines — so foo's hunk text is unchanged.
OVERLAP_ADVANCED_TAIL = "def tail():\n    return 42\n"


def _build_overlapping_base(tmp_path: Path) -> tuple[Repo, str, str, str]:
    repo = Repo(tmp_path)
    base_files = dict(BASE_FILES)
    base_files["foo.py"] = OVERLAP_BASE_PY
    _write_files(repo.path, base_files, BASE_BIN)
    base_sha = repo.commit_all("base")

    repo.checkout_new("candidate", base_sha)
    exact_files = dict(EXACT_FILES)
    exact_files["foo.py"] = OVERLAP_EXACT_PY
    _write_files(repo.path, exact_files, EXACT_BIN)
    candidate_sha = repo.commit_all("candidate: the reviewed delta")

    repo.checkout("main")
    advanced_files = dict(base_files)
    advanced_files["foo.py"] = OVERLAP_BASE_PY.replace(
        "def tail():\n    return 0\n", OVERLAP_ADVANCED_TAIL
    )
    _write_files(repo.path, advanced_files, BASE_BIN)
    advanced_sha = repo.commit_all("advance main: foo.py tail changed elsewhere")
    return repo, base_sha, candidate_sha, advanced_sha


def test_advanced_base_that_touched_the_same_file_elsewhere_still_proves(
    tmp_path: Path,
) -> None:
    repo, base_sha, candidate_sha, advanced_sha = _build_overlapping_base(tmp_path)
    squash_files = dict(EXACT_FILES)
    squash_files["foo.py"] = OVERLAP_EXACT_PY.replace(
        "def tail():\n    return 0\n", OVERLAP_ADVANCED_TAIL
    )
    merge_sha = _squash(repo, advanced_sha, "squash-overlap", squash_files, EXACT_BIN)
    executor = _executor(repo)

    outcome = executor.prove_landed(
        "demo",
        candidate_sha=candidate_sha,
        candidate_branch="candidate",
        pr_number=1,
        merge_sha=merge_sha,
        base_sha=base_sha,
    )

    assert outcome.relation == "patch_equivalent"
    assert outcome.base_sha == base_sha
    assert outcome.merge_parent_sha == advanced_sha
    # The blob ids of foo.py differ on the two sides (the advanced base
    # rewrote the file), yet the reviewed hunk applies cleanly and the
    # resulting tree matches the merge's own tree exactly.
    verifier = GitVerifier(runner=SubprocessGitRunner(env=_ENV))
    assert verifier.tree_of(repo.path, merge_sha) != verifier.tree_of(
        repo.path, candidate_sha
    )
    assert outcome.applied_tree_sha == outcome.merge_tree_sha


def test_advanced_base_that_touched_the_same_file_elsewhere_still_rejects_drift(
    tmp_path: Path,
) -> None:
    """Same shape, but the squash also drifts inside foo's reviewed hunk
    (two-space indentation): blob ids are irrelevant, the applied tree is
    not, and it no longer matches the merge's own tree."""

    repo, base_sha, candidate_sha, advanced_sha = _build_overlapping_base(tmp_path)
    squash_files = dict(EXACT_FILES)
    squash_files["foo.py"] = OVERLAP_EXACT_PY.replace(
        "def tail():\n    return 0\n", OVERLAP_ADVANCED_TAIL
    ).replace("        x = 1\n", "      x = 1\n")
    merge_sha = _squash(repo, advanced_sha, "squash-drift", squash_files, EXACT_BIN)
    executor = _executor(repo)

    with pytest.raises(
        ReconciliationRequired, match="does not reproduce the merge tree"
    ):
        executor.prove_landed(
            "demo",
            candidate_sha=candidate_sha,
            candidate_branch="candidate",
            pr_number=1,
            merge_sha=merge_sha,
            base_sha=base_sha,
        )


# -- (a) repeated identical blocks: wrong-occurrence must never prove -------
#
# The defect this correction fixes: a file carries two byte-identical
# blocks, far enough apart that neither diff hunk's context ever reaches
# the other occurrence. The candidate edits the FIRST occurrence; a squash
# on the (untouched-here) advanced base edits the SECOND occurrence
# instead. A hunk-location-normalized digest could equate the two changes
# because the normalized hunk text is byte-identical regardless of which
# occurrence it targets. Real application is not fooled: git apply places
# the reviewed patch by its recorded line offset (which lands on the first
# occurrence), so the applied tree carries the first occurrence patched --
# never equal to a merge tree that instead patched the second.

_REPEATED_BLOCK = "def foo():\n    x = 1\n    return x\n"
_REPEATED_BLOCK_CHANGED = "def foo():\n    x = 2\n    return x\n"
_REPEATED_PAD = (
    "\n\n# pad 1\n# pad 2\n# pad 3\n# pad 4\n# pad 5\n# pad 6\n# pad 7\n# pad 8\n\n\n"
)

REPEATED_BASE_TEXT = _REPEATED_BLOCK + _REPEATED_PAD + _REPEATED_BLOCK
REPEATED_FIRST_OCCURRENCE_CHANGED = (
    _REPEATED_BLOCK_CHANGED + _REPEATED_PAD + _REPEATED_BLOCK
)
REPEATED_SECOND_OCCURRENCE_CHANGED = (
    _REPEATED_BLOCK + _REPEATED_PAD + _REPEATED_BLOCK_CHANGED
)


def _build_repeated_block_base(tmp_path: Path) -> tuple[Repo, str, str, str]:
    repo = Repo(tmp_path)
    (repo.path / "repeated.py").write_text(REPEATED_BASE_TEXT)
    base_sha = repo.commit_all("base")

    repo.checkout_new("candidate", base_sha)
    (repo.path / "repeated.py").write_text(REPEATED_FIRST_OCCURRENCE_CHANGED)
    candidate_sha = repo.commit_all("candidate: change the first occurrence")

    repo.checkout("main")
    (repo.path / "unrelated.txt").write_text("advanced main content\n")
    advanced_sha = repo.commit_all("advance main unrelated to repeated.py")

    return repo, base_sha, candidate_sha, advanced_sha


def test_repeated_identical_block_wrong_occurrence_is_rejected(
    tmp_path: Path,
) -> None:
    repo, base_sha, candidate_sha, advanced_sha = _build_repeated_block_base(
        tmp_path
    )
    repo.checkout_new("squash-wrong-occurrence", advanced_sha)
    (repo.path / "repeated.py").write_text(REPEATED_SECOND_OCCURRENCE_CHANGED)
    merge_sha = repo.commit_all("squash-wrong-occurrence")
    repo.push_main(merge_sha)
    executor = _executor(repo)

    with pytest.raises(
        ReconciliationRequired, match="does not reproduce the merge tree"
    ):
        executor.prove_landed(
            "demo",
            candidate_sha=candidate_sha,
            candidate_branch="candidate",
            pr_number=1,
            merge_sha=merge_sha,
            base_sha=base_sha,
        )


def test_repeated_identical_block_correct_occurrence_still_proves(
    tmp_path: Path,
) -> None:
    """Positive control for the repeated-block fixture: the squash edits
    the same (first) occurrence the candidate reviewed, and the proof
    succeeds."""

    repo, base_sha, candidate_sha, advanced_sha = _build_repeated_block_base(
        tmp_path
    )
    repo.checkout_new("squash-right-occurrence", advanced_sha)
    (repo.path / "repeated.py").write_text(REPEATED_FIRST_OCCURRENCE_CHANGED)
    merge_sha = repo.commit_all("squash-right-occurrence")
    repo.push_main(merge_sha)
    executor = _executor(repo)

    outcome = executor.prove_landed(
        "demo",
        candidate_sha=candidate_sha,
        candidate_branch="candidate",
        pr_number=1,
        merge_sha=merge_sha,
        base_sha=base_sha,
    )

    assert outcome.relation == "patch_equivalent"
    assert outcome.applied_tree_sha == outcome.merge_tree_sha


# -- (d) apply conflict: advanced base edits the exact lines the candidate
# reviewed, so the reviewed patch's context can no longer land -----------


def test_apply_conflict_on_advanced_base_requires_reconciliation(
    tmp_path: Path,
) -> None:
    """The advanced base rewrites the very lines the candidate's patch
    targets, so applying that patch to the merge's first parent must fail
    (rejected hunk / conflict), not silently succeed somewhere else."""

    repo = Repo(tmp_path)
    _write_files(repo.path, BASE_FILES, BASE_BIN)
    base_sha = repo.commit_all("base")

    repo.checkout_new("candidate", base_sha)
    _write_files(repo.path, EXACT_FILES, EXACT_BIN)
    candidate_sha = repo.commit_all("candidate: the reviewed delta")

    repo.checkout("main")
    conflicting_files = dict(BASE_FILES)
    conflicting_files["foo.py"] = "def foo():\n    x = 999\n    return x\n"
    _write_files(repo.path, conflicting_files, BASE_BIN)
    # An unrelated file keeps the squash's eventual tree from ever being
    # byte-identical to the candidate's tree, so the third relation
    # (reviewed_tree_equivalent) cannot short-circuit past the fourth
    # relation this test targets.
    (repo.path / "unrelated.txt").write_text("advanced main content\n")
    advanced_sha = repo.commit_all(
        "advance main: rewrite the exact lines the candidate touches"
    )

    merge_sha = _squash(
        repo, advanced_sha, "squash-on-conflicting-base", EXACT_FILES, EXACT_BIN
    )
    executor = _executor(repo)

    with pytest.raises(ReconciliationRequired, match="merge ancestry proof failed"):
        executor.prove_landed(
            "demo",
            candidate_sha=candidate_sha,
            candidate_branch="candidate",
            pr_number=1,
            merge_sha=merge_sha,
            base_sha=base_sha,
        )


# -- (c) a configured lossy textconv driver must never influence the proof --


_LOSSY_SCRIPT = """#!/bin/sh
touch "{marker}"
printf 'lossy-constant\\n'
"""


def _build_lossy_base(tmp_path: Path) -> tuple[Repo, str, str, str, Path]:
    """Base/candidate/advanced-main trio for one ``*.bin`` file whose diff
    driver is configured to render every version identically."""

    marker = tmp_path / "textconv-ran.marker"
    script_path = tmp_path / "lossy-textconv.sh"
    script_path.write_text(_LOSSY_SCRIPT.format(marker=marker))
    script_path.chmod(0o755)

    repo = Repo(tmp_path)
    (repo.path / ".gitattributes").write_text("*.bin diff=lossy\n")
    (repo.path / "asset.lossy.bin").write_bytes(bytes([1, 2, 3, 4]))
    base_sha = repo.commit_all("base")
    _git(repo.path, "config", "diff.lossy.textconv", str(script_path))

    repo.checkout_new("candidate", base_sha)
    (repo.path / "asset.lossy.bin").write_bytes(bytes([10, 20, 30, 40]))
    candidate_sha = repo.commit_all("candidate: real binary content change")

    repo.checkout("main")
    (repo.path / "unrelated.txt").write_text("advanced main content\n")
    advanced_sha = repo.commit_all("advance main unrelated to asset.lossy.bin")

    return repo, base_sha, candidate_sha, advanced_sha, marker


def test_lossy_textconv_driver_is_never_consulted_by_the_proof(
    tmp_path: Path,
) -> None:
    repo, base_sha, candidate_sha, advanced_sha, marker = _build_lossy_base(tmp_path)
    assert not marker.exists()

    repo.checkout_new("squash-distinct-bytes", advanced_sha)
    (repo.path / "asset.lossy.bin").write_bytes(bytes([99, 98, 97, 96]))
    merge_sha = repo.commit_all("squash-distinct-bytes")
    repo.push_main(merge_sha)
    executor = _executor(repo)

    # The proof must reject: the real bytes genuinely differ. And it must
    # do so without ever invoking the configured textconv helper.
    with pytest.raises(
        ReconciliationRequired, match="does not reproduce the merge tree"
    ):
        executor.prove_landed(
            "demo",
            candidate_sha=candidate_sha,
            candidate_branch="candidate",
            pr_number=1,
            merge_sha=merge_sha,
            base_sha=base_sha,
        )
    assert not marker.exists(), (
        "the lossy textconv helper must never run during the application "
        "proof -- apply_to_tree always passes --no-textconv --no-ext-diff"
    )

    # Prove the flag matters: the *plain* diff (no --no-textconv), which
    # does consult the configured driver, renders both real, genuinely
    # different blobs as the identical constant -- hiding the very
    # difference the proof must catch.
    plain_diff = _git(
        repo.path, "diff", base_sha, candidate_sha, "--", "asset.lossy.bin"
    )
    assert marker.exists(), "the plain diff should invoke the configured driver"
    assert "lossy-constant" not in plain_diff
    assert plain_diff == "", (
        "with textconv enabled, two genuinely different blobs that convert "
        "to the same constant text show as no difference at all"
    )


# -- Sol correction 61690353: exact positional/preimage proof ---------------
#
# ``git apply`` relocates a hunk to wherever its full recorded context
# matches, searching outward from the recorded line. If the advanced parent
# edits a context line the reviewed hunk itself captured, the hunk's exact
# context no longer matches at its reviewed position; when an identical
# block remains elsewhere in the file, git can relocate the hunk there
# instead, and if the squash happened to change *that* block, the applied
# tree could still equal the merge tree even though the reviewed hunk never
# actually landed on the reviewed target. The positional proof in
# ``GitVerifier.apply_to_tree`` closes this: every reviewed hunk's base
# preimage range is checked against the base-to-parent edits before the
# isolated application ever runs, so a context edit *inside* that range
# (target drift) and an ambiguous or relocated preimage both fail closed,
# while a shift caused only by edits strictly *above* the range (the live
# PR #39 shape) is still accepted.

_POS_BLOCK = "def foo():\n    x = 1\n    return x\n"
_POS_BLOCK_CHANGED = "def foo():\n    x = 2\n    return x\n"
# Padding between two occurrences of the block, wide enough that neither
# hunk's default 3-line context ever reaches the other occurrence.
_POS_PAD = (
    "\n\n# pad 1\n# pad 2\n# pad 3\n# pad 4\n# pad 5\n# pad 6\n# pad 7\n# pad 8\n\n\n"
)


def test_target_drift_with_a_remaining_identical_block_requires_reconciliation(
    tmp_path: Path,
) -> None:
    """(i) REGRESSION for the defect this correction fixes.

    The file carries two identical blocks; the candidate changes the FIRST.
    The advanced parent edits ``top3``, a context line the reviewed hunk's
    own recorded context captures (within its default 3-line radius) --
    exactly the drift that let ``git apply`` relocate the old (pre-
    correction) proof's hunk onto the second, untouched-by-the-edit block.
    The squash on that advanced parent then changes the SECOND block. The
    positional proof must reject this before ever reaching the isolated
    application: the drifted context line intersects the reviewed hunk's
    own base preimage range, so the reviewed target itself is proven to
    have changed on the parent.
    """

    prefix = "top1\ntop2\ntop3\n"
    base_text = prefix + _POS_BLOCK + _POS_PAD + _POS_BLOCK
    candidate_text = prefix + _POS_BLOCK_CHANGED + _POS_PAD + _POS_BLOCK
    # The advanced parent edits "top3", inside the reviewed hunk's own
    # captured context (old_start=2, old_len=7 covers lines 2-8).
    advanced_text = "top1\ntop2\ntop3-EDITED\n" + _POS_BLOCK + _POS_PAD + _POS_BLOCK
    # The squash changes the SECOND block instead of the first.
    squash_text = (
        "top1\ntop2\ntop3-EDITED\n" + _POS_BLOCK + _POS_PAD + _POS_BLOCK_CHANGED
    )

    repo = Repo(tmp_path)
    (repo.path / "drift.py").write_text(base_text)
    base_sha = repo.commit_all("base")

    repo.checkout_new("candidate", base_sha)
    (repo.path / "drift.py").write_text(candidate_text)
    candidate_sha = repo.commit_all("candidate: change the first occurrence")

    repo.checkout("main")
    (repo.path / "drift.py").write_text(advanced_text)
    advanced_sha = repo.commit_all("advance main: edit the context beside block one")

    repo.checkout_new("squash-wrong-block", advanced_sha)
    (repo.path / "drift.py").write_text(squash_text)
    merge_sha = repo.commit_all("squash-wrong-block")
    repo.push_main(merge_sha)
    executor = _executor(repo)

    with pytest.raises(ReconciliationRequired):
        executor.prove_landed(
            "demo",
            candidate_sha=candidate_sha,
            candidate_branch="candidate",
            pr_number=1,
            merge_sha=merge_sha,
            base_sha=base_sha,
        )


def test_pure_shift_above_a_repeated_block_file_still_proves(
    tmp_path: Path,
) -> None:
    """(ii) CONTROL: the live PR #39 acceptance shape must keep working.

    Same repeated-block file shape as the regression above (so the
    reviewed hunk's preimage is unique, not ambiguous -- block one is
    followed by real content, block two sits at end of file with nothing
    after it), but this time the advanced parent only inserts unrelated
    lines ABOVE both blocks: a pure shift, with no edit anywhere near
    either block. The squash reproduces the candidate's exact change at
    the shifted position. This must still prove ``patch_equivalent``: a
    blanket "reject any offset" rule would wrongly fail this case.
    """

    # Five header lines keep the hunk's old_start comfortably above 1;
    # git's own offset search special-cases a hunk recorded at line 1.
    header = "h1\nh2\nh3\nh4\nh5\n"
    base_text = header + _POS_BLOCK + _POS_PAD + _POS_BLOCK
    candidate_text = header + _POS_BLOCK_CHANGED + _POS_PAD + _POS_BLOCK
    inserted = "# inserted 1\n# inserted 2\n# inserted 3\n"
    advanced_text = inserted + base_text
    squash_text = inserted + candidate_text

    repo = Repo(tmp_path)
    (repo.path / "shift.py").write_text(base_text)
    base_sha = repo.commit_all("base")

    repo.checkout_new("candidate", base_sha)
    (repo.path / "shift.py").write_text(candidate_text)
    candidate_sha = repo.commit_all("candidate: change the first occurrence")

    repo.checkout("main")
    (repo.path / "shift.py").write_text(advanced_text)
    advanced_sha = repo.commit_all(
        "advance main: insert unrelated lines above both blocks"
    )

    repo.checkout_new("squash-shifted", advanced_sha)
    (repo.path / "shift.py").write_text(squash_text)
    merge_sha = repo.commit_all("squash-shifted")
    repo.push_main(merge_sha)
    executor = _executor(repo)

    outcome = executor.prove_landed(
        "demo",
        candidate_sha=candidate_sha,
        candidate_branch="candidate",
        pr_number=1,
        merge_sha=merge_sha,
        base_sha=base_sha,
    )

    assert outcome.relation == "patch_equivalent"
    assert outcome.applied_tree_sha == outcome.merge_tree_sha


def test_symmetric_repeated_context_untouched_by_parent_rejects_as_ambiguous(
    tmp_path: Path,
) -> None:
    """(iii) AMBIGUITY: the advanced parent leaves both blocks (and their
    surrounding context) completely untouched, and the blocks' immediate
    surrounding context is made deliberately identical on both sides so
    the reviewed hunk's exact preimage -- context included -- occurs
    twice. Neither occurrence is closer or further per any edit; the
    positional proof must fail closed as ambiguous rather than trust
    whichever occurrence a search happens to land on first.
    """

    ctx = "# ctx1\n# ctx2\n# ctx3\n"
    filler = "".join(f"# filler {i}\n" for i in range(1, 11))
    base_text = ctx + _POS_BLOCK + ctx + filler + ctx + _POS_BLOCK + ctx
    candidate_text = (
        ctx + _POS_BLOCK_CHANGED + ctx + filler + ctx + _POS_BLOCK + ctx
    )

    repo = Repo(tmp_path)
    (repo.path / "ambiguous.py").write_text(base_text)
    base_sha = repo.commit_all("base")

    repo.checkout_new("candidate", base_sha)
    (repo.path / "ambiguous.py").write_text(candidate_text)
    candidate_sha = repo.commit_all("candidate: change the first occurrence")

    repo.checkout("main")
    (repo.path / "unrelated.txt").write_text("advanced main content\n")
    advanced_sha = repo.commit_all("advance main unrelated to ambiguous.py")

    repo.checkout_new("squash-ambiguous", advanced_sha)
    (repo.path / "ambiguous.py").write_text(candidate_text)
    merge_sha = repo.commit_all("squash-ambiguous")
    repo.push_main(merge_sha)
    executor = _executor(repo)

    with pytest.raises(ReconciliationRequired, match="ambiguous or relocated"):
        executor.prove_landed(
            "demo",
            candidate_sha=candidate_sha,
            candidate_branch="candidate",
            pr_number=1,
            merge_sha=merge_sha,
            base_sha=base_sha,
        )


def test_deletion_above_the_hunk_yields_negative_shift_and_still_proves(
    tmp_path: Path,
) -> None:
    """(iv) A deletion strictly above the reviewed hunk's range on the
    advanced parent produces a negative shift; the mapped position moves
    backward and the proof must still succeed when the squash reproduces
    the candidate's exact change at that shifted-back position."""

    base_text = "p1\np2\np3\np4\np5\ndef foo():\n    x = 1\n    return x\n"
    candidate_text = "p1\np2\np3\np4\np5\ndef foo():\n    x = 99\n    return x\n"
    # Delete p1 and p2 -- two lines strictly above the hunk's range.
    advanced_text = "p3\np4\np5\ndef foo():\n    x = 1\n    return x\n"
    squash_text = "p3\np4\np5\ndef foo():\n    x = 99\n    return x\n"

    repo = Repo(tmp_path)
    (repo.path / "negshift.py").write_text(base_text)
    base_sha = repo.commit_all("base")

    repo.checkout_new("candidate", base_sha)
    (repo.path / "negshift.py").write_text(candidate_text)
    candidate_sha = repo.commit_all("candidate: change the body")

    repo.checkout("main")
    (repo.path / "negshift.py").write_text(advanced_text)
    advanced_sha = repo.commit_all("advance main: delete two lines above the hunk")

    repo.checkout_new("squash-negshift", advanced_sha)
    (repo.path / "negshift.py").write_text(squash_text)
    merge_sha = repo.commit_all("squash-negshift")
    repo.push_main(merge_sha)
    executor = _executor(repo)

    outcome = executor.prove_landed(
        "demo",
        candidate_sha=candidate_sha,
        candidate_branch="candidate",
        pr_number=1,
        merge_sha=merge_sha,
        base_sha=base_sha,
    )

    assert outcome.relation == "patch_equivalent"
    assert outcome.applied_tree_sha == outcome.merge_tree_sha
