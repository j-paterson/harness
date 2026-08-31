"""Argv-safe local Git evidence for merge ancestry and tree proofs."""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
# Conservative allowlist for remotes, branches, and qualified refs: no
# option-like leading dash, no whitespace or control bytes, no ".." ranges.
_REF_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")

# Unified-diff hunk header: ``@@ -old_start[,old_len] +new_start[,new_len] @@``
# optionally followed by a section heading git appends after the closing
# ``@@``. A missing count defaults to 1 (unified-diff convention).
_HUNK_HEADER = re.compile(
    r"^@@ -(?P<old_start>\d+)(?:,(?P<old_len>\d+))?"
    r" \+(?P<new_start>\d+)(?:,(?P<new_len>\d+))? @@"
)
_DIFF_GIT_HEADER = re.compile(r"^diff --git a/(?P<a>.*) b/(?P<b>.*)$")
_CHECKING_PATCH = re.compile(r"^Checking patch (?P<path>.+)\.\.\.$")
_HUNK_SUCCEEDED = re.compile(
    r"^Hunk #(?P<num>\d+) succeeded at \d+"
    r"(?: \(offset (?P<offset>-?\d+) lines?\))?\.$"
)


class GitError(RuntimeError):
    """Raised when local Git evidence cannot be produced deterministically."""


@dataclass(frozen=True, slots=True)
class GitResult:
    """One completed git invocation."""

    returncode: int
    stdout: str
    stderr: str


class GitRunner(Protocol):
    """Execution boundary: one argv vector, one repository, no shell."""

    def run(
        self,
        args: tuple[str, ...],
        cwd: Path,
        *,
        input: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> GitResult: ...


class SubprocessGitRunner:
    """Run git as an argv vector with a bounded runtime and no shell."""

    def __init__(
        self,
        *,
        env: Mapping[str, str] | None = None,
        timeout: float = 60.0,
    ) -> None:
        self._env = dict(env) if env is not None else None
        self._timeout = timeout

    def run(
        self,
        args: tuple[str, ...],
        cwd: Path,
        *,
        input: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> GitResult:
        call_env = self._env
        if env is not None:
            call_env = {**(self._env or {}), **env}
        try:
            completed = subprocess.run(
                args,
                cwd=cwd,
                env=call_env,
                capture_output=True,
                text=True,
                timeout=self._timeout,
                check=False,
                input=input,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise GitError(f"git invocation failed: {type(error).__name__}") from error
        return GitResult(completed.returncode, completed.stdout, completed.stderr)


@dataclass(frozen=True, slots=True)
class _ReviewedHunk:
    """One ``@@`` hunk of the reviewed patch, in base-file coordinates."""

    old_start: int
    old_len: int
    preimage: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _ReviewedFile:
    """One text file's hunks from the reviewed patch (never binary/dev-null)."""

    path: str
    hunks: tuple[_ReviewedHunk, ...]


@dataclass(frozen=True, slots=True)
class _Edit:
    """One base-to-parent ``@@`` hunk, stripped to its position/size facts."""

    old_start: int
    old_len: int
    new_len: int


def _split_lines_no_phantom(text: str) -> list[str]:
    """Split file content on ``\\n`` without inventing a trailing empty line.

    A file ending with a newline splits into one trailing empty string that
    is not a real line and must be dropped; a file with no trailing newline
    keeps its genuine last, incomplete, line.
    """

    if text == "":
        return []
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines = lines[:-1]
    return lines


def _parse_reviewed_patch(patch: str) -> tuple[_ReviewedFile, ...]:
    """Parse a ``--full-index --binary --no-renames`` patch into hunks.

    Files whose body is ``GIT binary patch``/``Binary files ... differ`` or
    that touch ``/dev/null`` (added or deleted) carry no positional hunks --
    they are omitted entirely, since name-status equality and the final
    tree comparison already cover them. Any structural inconsistency
    (a hunk before a file header, an unparseable ``@@`` header, or a
    preimage whose collected line count does not match the header's
    declared old length) fails closed.
    """

    lines = patch.split("\n")
    total = len(lines)
    files: list[_ReviewedFile] = []

    current_path: str | None = None
    current_hunks: list[_ReviewedHunk] = []
    skip_positional = False

    def flush() -> None:
        nonlocal current_path, current_hunks, skip_positional
        if current_path is not None and not skip_positional:
            files.append(_ReviewedFile(path=current_path, hunks=tuple(current_hunks)))
        current_path = None
        current_hunks = []
        skip_positional = False

    index = 0
    while index < total:
        line = lines[index]
        if line.startswith("diff --git a/"):
            flush()
            header = _DIFF_GIT_HEADER.match(line)
            if header is None or header.group("a") != header.group("b"):
                raise GitError("malformed reviewed patch")
            current_path = header.group("a")
            index += 1
            continue
        if line.startswith("--- /dev/null") or line.startswith("+++ /dev/null"):
            skip_positional = True
            index += 1
            continue
        if line.startswith("GIT binary patch") or line.startswith("Binary files "):
            skip_positional = True
            index += 1
            continue
        if line.startswith("@@ "):
            if current_path is None:
                raise GitError("malformed reviewed patch")
            header_match = _HUNK_HEADER.match(line)
            if header_match is None:
                raise GitError("malformed reviewed patch")
            old_start = int(header_match.group("old_start"))
            old_len_text = header_match.group("old_len")
            old_len = int(old_len_text) if old_len_text is not None else 1
            index += 1
            preimage: list[str] = []
            while index < total:
                body_line = lines[index]
                if body_line.startswith("@@ ") or body_line.startswith(
                    "diff --git a/"
                ):
                    break
                if not body_line.startswith("\\") and (
                    body_line.startswith(" ") or body_line.startswith("-")
                ):
                    preimage.append(body_line[1:])
                index += 1
            if not skip_positional:
                if len(preimage) != old_len:
                    raise GitError("malformed reviewed patch")
                current_hunks.append(
                    _ReviewedHunk(
                        old_start=old_start, old_len=old_len, preimage=tuple(preimage)
                    )
                )
            continue
        index += 1
    flush()
    return tuple(files)


def _parse_edit_hunks(diff_text: str) -> tuple[_Edit, ...]:
    """Parse a ``-U0`` diff's hunk headers into base-to-parent edits."""

    edits: list[_Edit] = []
    for line in diff_text.split("\n"):
        if not line.startswith("@@ "):
            continue
        match = _HUNK_HEADER.match(line)
        if match is None:
            raise GitError("malformed base-to-parent diff")
        old_start = int(match.group("old_start"))
        old_len_text = match.group("old_len")
        new_len_text = match.group("new_len")
        old_len = int(old_len_text) if old_len_text is not None else 1
        new_len = int(new_len_text) if new_len_text is not None else 1
        edits.append(_Edit(old_start=old_start, old_len=old_len, new_len=new_len))
    return tuple(edits)


def _edit_intersects_range(edit: _Edit, range_start: int, range_end: int) -> bool:
    """Whether ``edit`` touches the base preimage range ``[start, end]``.

    A pure insertion (``old_len == 0``) occupies no base lines; it counts
    as intersecting only when it lands strictly inside the range (not at
    or below its final line).
    """

    if edit.old_len == 0:
        return range_start <= edit.old_start < range_end
    edit_end = edit.old_start + edit.old_len - 1
    return not (edit_end < range_start or edit.old_start > range_end)


def _edit_is_above_range(edit: _Edit, range_start: int) -> bool:
    """Whether ``edit`` lies entirely before the range's first line.

    A pure insertion at line ``l`` is conceptually anchored at ``l + 0.5``
    (it inserts strictly after line ``l``), so it counts as above iff
    ``l < range_start``.
    """

    if edit.old_len == 0:
        return edit.old_start < range_start
    edit_end = edit.old_start + edit.old_len - 1
    return edit_end < range_start


def _parse_apply_offsets(stderr: str) -> dict[tuple[str, int], int]:
    """Parse ``git apply --verbose`` stderr into per-hunk applied offsets.

    Keyed by ``(path, 1-based hunk number within that file)``; a hunk
    that applied with no reported offset is not present here and is
    treated by the caller as offset 0.
    """

    offsets: dict[tuple[str, int], int] = {}
    current_path: str | None = None
    for raw_line in stderr.split("\n"):
        line = raw_line.strip()
        checking = _CHECKING_PATCH.match(line)
        if checking is not None:
            current_path = checking.group("path")
            continue
        succeeded = _HUNK_SUCCEEDED.match(line)
        if succeeded is not None and current_path is not None:
            offset_text = succeeded.group("offset")
            offset = int(offset_text) if offset_text is not None else 0
            offsets[(current_path, int(succeeded.group("num")))] = offset
    return offsets


class GitVerifier:
    """Prove merge reachability and reviewed-tree identity from a local clone."""

    def __init__(self, *, runner: GitRunner | None = None) -> None:
        self._runner = runner if runner is not None else SubprocessGitRunner()

    def fetch(self, repo_path: Path, remote: str, branch: str) -> None:
        """Fetch one branch from one named remote; any failure fails closed."""

        _require_ref(remote, "remote")
        _require_ref(branch, "branch")
        result = self._runner.run(("git", "fetch", "--", remote, branch), repo_path)
        if result.returncode != 0:
            raise GitError(f"git fetch failed with exit code {result.returncode}")

    def is_ancestor(self, repo_path: Path, commit: str, ref: str) -> bool:
        """Return whether ``commit`` is reachable from ``ref``.

        Only the documented exit codes 0 and 1 are decisions; anything else
        (unknown commit, corrupt repository) fails closed.
        """

        _require_sha(commit)
        _require_ref(ref, "ref")
        result = self._runner.run(
            ("git", "merge-base", "--is-ancestor", commit, ref), repo_path
        )
        if result.returncode == 0:
            return True
        if result.returncode == 1:
            return False
        raise GitError(
            f"git merge-base --is-ancestor failed with exit code {result.returncode}"
        )

    def head_of(self, repo_path: Path, ref: str) -> str:
        """Return the commit SHA that ``ref`` currently resolves to."""

        result = self._runner.run(
            ("git", "rev-parse", "--verify", f"{ref}^{{commit}}"), repo_path
        )
        if result.returncode != 0:
            raise GitError(
                f"git rev-parse failed with exit code {result.returncode}"
            )
        sha = result.stdout.strip()
        if _SHA_PATTERN.match(sha) is None:
            raise GitError("git rev-parse returned an invalid commit identity")
        return sha

    def tree_of(self, repo_path: Path, commit: str) -> str:
        """Return the tree object identity recorded by one exact commit."""

        _require_sha(commit)
        result = self._runner.run(
            ("git", "rev-parse", "--verify", f"{commit}^{{tree}}"), repo_path
        )
        if result.returncode != 0:
            raise GitError(
                f"git rev-parse failed with exit code {result.returncode}"
            )
        tree = result.stdout.strip()
        if _SHA_PATTERN.match(tree) is None:
            raise GitError("git rev-parse returned an invalid tree identity")
        return tree

    def first_parent(self, repo_path: Path, commit: str) -> str:
        """Return the first-parent commit identity of one exact commit."""

        _require_sha(commit)
        result = self._runner.run(
            ("git", "rev-parse", "--verify", f"{commit}^1"), repo_path
        )
        if result.returncode != 0:
            raise GitError(
                f"git rev-parse failed with exit code {result.returncode}"
            )
        parent = result.stdout.strip()
        if _SHA_PATTERN.match(parent) is None:
            raise GitError("git rev-parse returned an invalid parent identity")
        return parent

    def changed_paths(self, repo_path: Path, base: str, head: str) -> tuple[str, ...]:
        """Return the sorted, renameless status/path lines between two commits."""

        _require_sha(base)
        _require_sha(head)
        result = self._runner.run(
            ("git", "diff", "--name-status", "--no-renames", base, head), repo_path
        )
        if result.returncode != 0:
            raise GitError(
                f"git diff --name-status failed with exit code {result.returncode}"
            )
        lines = [line for line in result.stdout.splitlines() if line]
        return tuple(sorted(lines))

    def apply_to_tree(self, repo_path: Path, base: str, head: str, parent: str) -> str:
        """Prove patch equivalence by isolated Git application, not a digest.

        Generates the reviewed patch with ``git diff --full-index --binary
        --no-renames --no-textconv --no-ext-diff <base> <head>`` (an empty
        diff is never proof and fails closed). Before ever touching a
        throwaway index, an exact positional/preimage proof runs per
        reviewed hunk: ``git apply`` relocates a hunk to wherever its full
        context matches, so a context that drifted on the advanced parent
        while an identical block remains elsewhere could otherwise land the
        reviewed hunk on the wrong target. Each hunk's base preimage range
        is checked against the base-to-parent edits (``git diff -U0``): any
        edit intersecting that range means the reviewed target itself
        changed on the parent and fails closed; the net line shift of edits
        strictly above the range gives the hunk's expected mapped position,
        and the exact preimage (context plus removed lines) must occur in
        the parent file exactly once, exactly at that mapped position --
        zero, several, or a mismatched occurrence all fail closed as
        ambiguous or relocated. Only then does the isolated application
        proceed: inside a fresh temporary index that is never the user's
        worktree or real index, ``parent`` is read as the starting tree,
        the exact patch is applied with ``git apply --cached --binary
        --whitespace=nowarn --verbose`` (any rejected hunk, conflict, fuzz,
        or an applied offset that differs from the computed shift fails
        closed; no ``--3way``, no ``--reject``, no ``-C``), and the
        resulting tree is written. The caller compares the returned tree id
        to the merge commit's own tree: equal trees, reached only after
        every reviewed hunk is proven to land at its mapped position, are
        the proof that the reviewed delta, applied exactly as reviewed,
        reproduces what actually landed. Because ``--no-textconv --no-ext-
        diff`` are always passed, a repository-configured lossy diff driver
        can never hide a real content difference from this proof.
        """

        _require_sha(base)
        _require_sha(head)
        _require_sha(parent)
        diff_result = self._runner.run(
            (
                "git",
                "diff",
                "--full-index",
                "--binary",
                "--no-renames",
                "--no-textconv",
                "--no-ext-diff",
                # Sol correction 2b0e51aa: never inherit diff.context — a
                # repository configured with diff.context=0 would emit
                # contextless insertions (empty preimage) that no
                # positional proof can anchor. A fixed nonzero context is
                # part of the proof's own contract.
                "--unified=3",
                base,
                head,
            ),
            repo_path,
        )
        if diff_result.returncode != 0:
            raise GitError(
                "git diff --full-index --binary failed with exit code "
                f"{diff_result.returncode}"
            )
        patch = diff_result.stdout
        if not patch:
            raise GitError("empty delta")

        reviewed_files = _parse_reviewed_patch(patch)
        expected_shifts = self._verify_hunk_positions(
            repo_path, base, parent, reviewed_files
        )

        tmpdir = tempfile.mkdtemp()
        try:
            tmp_index = str(Path(tmpdir) / "index")
            env = {"GIT_INDEX_FILE": tmp_index}
            read_tree_result = self._runner.run(
                ("git", "read-tree", parent), repo_path, env=env
            )
            if read_tree_result.returncode != 0:
                raise GitError(
                    "git read-tree failed with exit code "
                    f"{read_tree_result.returncode}"
                )
            apply_result = self._runner.run(
                (
                    "git",
                    "apply",
                    "--cached",
                    "--binary",
                    "--whitespace=nowarn",
                    "--verbose",
                ),
                repo_path,
                input=patch,
                env=env,
            )
            if apply_result.returncode != 0:
                raise GitError(
                    "git apply failed with exit code "
                    f"{apply_result.returncode}"
                )
            if "fuzz" in apply_result.stderr.lower():
                raise GitError("reviewed hunk applied with fuzz")
            applied_offsets = _parse_apply_offsets(apply_result.stderr)
            for (path, hunk_number), shift in expected_shifts.items():
                offset = applied_offsets.get((path, hunk_number), 0)
                if offset != shift:
                    raise GitError(
                        "reviewed hunk applied at an unexpected offset: "
                        f"{path} hunk #{hunk_number}"
                    )
            write_tree_result = self._runner.run(
                ("git", "write-tree"), repo_path, env=env
            )
            if write_tree_result.returncode != 0:
                raise GitError(
                    "git write-tree failed with exit code "
                    f"{write_tree_result.returncode}"
                )
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

        tree = write_tree_result.stdout.strip()
        if _SHA_PATTERN.match(tree) is None:
            raise GitError("git write-tree returned an invalid tree identity")
        return tree

    def _verify_hunk_positions(
        self,
        repo_path: Path,
        base: str,
        parent: str,
        reviewed_files: tuple[_ReviewedFile, ...],
    ) -> dict[tuple[str, int], int]:
        """Prove every reviewed hunk lands at its mapped target on ``parent``.

        Returns each hunk's expected applied-offset shift (keyed by path
        and 1-based hunk number within that file) for the caller to cross-
        check against ``git apply --verbose``'s own reported offsets.
        """

        expected_shifts: dict[tuple[str, int], int] = {}
        for reviewed_file in reviewed_files:
            if not reviewed_file.hunks:
                continue
            edit_result = self._runner.run(
                (
                    "git",
                    "diff",
                    "-U0",
                    "--no-renames",
                    "--no-textconv",
                    "--no-ext-diff",
                    base,
                    parent,
                    "--",
                    reviewed_file.path,
                ),
                repo_path,
            )
            if edit_result.returncode != 0:
                raise GitError(
                    "git diff -U0 failed with exit code "
                    f"{edit_result.returncode} for {reviewed_file.path}"
                )
            edits = _parse_edit_hunks(edit_result.stdout)

            mapped_positions: dict[int, int] = {}
            for hunk_number, hunk in enumerate(reviewed_file.hunks, start=1):
                if not hunk.preimage:
                    # A text-file hunk against an existing file with no
                    # preimage cannot be anchored to a target or an
                    # offset (added/deleted files never reach here, and
                    # --unified=3 rules out configured contextless
                    # diffs), so it can never participate in the proof.
                    raise GitError(
                        "reviewed hunk has no positional preimage: "
                        f"{reviewed_file.path} hunk #{hunk_number}"
                    )
                range_start = hunk.old_start
                range_end = hunk.old_start + hunk.old_len - 1
                for edit in edits:
                    if _edit_intersects_range(edit, range_start, range_end):
                        raise GitError(
                            "reviewed hunk target changed on the merge "
                            f"parent: {reviewed_file.path} hunk #{hunk_number}"
                        )
                shift = sum(
                    edit.new_len - edit.old_len
                    for edit in edits
                    if _edit_is_above_range(edit, range_start)
                )
                mapped_positions[hunk_number] = hunk.old_start + shift
                expected_shifts[(reviewed_file.path, hunk_number)] = shift

            if not mapped_positions:
                continue
            show_result = self._runner.run(
                ("git", "show", f"{parent}:{reviewed_file.path}"), repo_path
            )
            if show_result.returncode != 0:
                raise GitError(
                    "git show failed to read "
                    f"{reviewed_file.path} at the merge parent"
                )
            parent_lines = _split_lines_no_phantom(show_result.stdout)
            for hunk_number, hunk in enumerate(reviewed_file.hunks, start=1):
                if hunk_number not in mapped_positions:
                    continue
                preimage = hunk.preimage
                width = len(preimage)
                occurrences = [
                    position + 1
                    for position in range(len(parent_lines) - width + 1)
                    if tuple(parent_lines[position : position + width]) == preimage
                ]
                mapped = mapped_positions[hunk_number]
                if len(occurrences) != 1 or occurrences[0] != mapped:
                    raise GitError(
                        "reviewed hunk is ambiguous or relocated on the "
                        f"merge parent: {reviewed_file.path} hunk "
                        f"#{hunk_number} (occurrences={len(occurrences)}, "
                        f"expected line {mapped})"
                    )
        return expected_shifts


@dataclass(frozen=True, slots=True)
class WorktreeStatus:
    """Tracked and untracked pending changes inside one worktree."""

    modified: tuple[str, ...]
    untracked: tuple[str, ...]

    @property
    def clean(self) -> bool:
        return not self.modified and not self.untracked


class WorktreeGit:
    """Argv-safe worktree evidence and mutation for checkpointed cleanup.

    Every ref and SHA is validated before it reaches an argv vector, the
    removal path is exactly ``git worktree remove`` (never ``--force``)
    followed by ``git worktree prune``, and nothing here ever deletes a
    path outside of git.
    """

    def __init__(self, *, runner: GitRunner | None = None) -> None:
        self._runner = runner if runner is not None else SubprocessGitRunner()

    def _run(self, args: tuple[str, ...], cwd: Path) -> GitResult:
        result = self._runner.run(args, cwd)
        if result.returncode != 0:
            raise GitError(
                f"git {args[1]} failed with exit code {result.returncode}"
            )
        return result

    def status_of(self, path: Path) -> WorktreeStatus:
        result = self._run(
            ("git", "status", "--porcelain=v2", "--branch"), path
        )
        modified: list[str] = []
        untracked: list[str] = []
        for line in result.stdout.splitlines():
            if line.startswith("? "):
                untracked.append(line[2:])
            elif line.startswith("1 "):
                modified.append(line.split(" ", 8)[8])
            elif line.startswith("2 "):
                modified.append(line.split(" ", 9)[9].split("\t", 1)[0])
            elif line.startswith("u "):
                modified.append(line.split(" ", 10)[10])
        return WorktreeStatus(modified=tuple(modified), untracked=tuple(untracked))

    def head_sha(self, path: Path) -> str:
        result = self._run(("git", "rev-parse", "--verify", "HEAD"), path)
        sha = result.stdout.strip()
        if _SHA_PATTERN.match(sha) is None:
            raise GitError("git rev-parse returned an invalid HEAD identity")
        return sha

    def branch(self, path: Path) -> str | None:
        result = self._run(("git", "branch", "--show-current"), path)
        name = result.stdout.strip()
        return name if name else None

    def head_message(self, path: Path) -> str:
        result = self._run(("git", "log", "-1", "--format=%B"), path)
        return result.stdout.strip()

    def ahead(self, path: Path) -> int | None:
        """Commits ahead of the upstream, or None when none is configured."""

        result = self._runner.run(
            ("git", "rev-list", "--count", "@{upstream}..HEAD"), path
        )
        if result.returncode != 0:
            return None
        try:
            return int(result.stdout.strip())
        except ValueError as error:
            raise GitError("git rev-list returned an invalid count") from error

    def add_all(self, path: Path) -> None:
        self._run(("git", "add", "-A"), path)

    def commit(self, path: Path, message: str) -> str:
        if not message.strip() or message.startswith("-"):
            raise GitError("commit message must be non-empty and not option-like")
        self._run(("git", "commit", "-m", message), path)
        return self.head_sha(path)

    def push(self, path: Path, remote: str, branch: str) -> None:
        _require_ref(remote, "remote")
        _require_ref(branch, "branch")
        self._run(("git", "push", "--", remote, branch), path)

    def fetch(self, path: Path, remote: str, branch: str) -> None:
        _require_ref(remote, "remote")
        _require_ref(branch, "branch")
        self._run(("git", "fetch", "--", remote, branch), path)

    def remote_contains(
        self, path: Path, sha: str, remote: str, branch: str
    ) -> bool:
        """Whether ``sha`` is reachable from the fetched remote branch tip."""

        _require_sha(sha)
        _require_ref(remote, "remote")
        _require_ref(branch, "branch")
        result = self._runner.run(
            ("git", "merge-base", "--is-ancestor", sha, f"{remote}/{branch}"),
            path,
        )
        if result.returncode == 0:
            return True
        if result.returncode == 1:
            return False
        raise GitError(
            f"git merge-base --is-ancestor failed with exit code "
            f"{result.returncode}"
        )

    def worktree_add_detached(self, repo_path: Path, path: Path, sha: str) -> None:
        """Materialize one bounded, detached worktree at an exact commit.

        INFRA-198 P2: the daemon's post-merge advance uses this to
        materialize a clean checkout at a merge SHA before spawning the
        activation applier. ``sha`` is a full lowercase commit hex and
        ``path`` an absolute filesystem path — both validated before
        reaching the argv vector. No ``--force``: an existing populated
        path fails the underlying git invocation rather than silently
        being overwritten.
        """

        _require_sha(sha)
        if not path.is_absolute():
            raise GitError("worktree path must be absolute")
        self._run(
            ("git", "worktree", "add", "--detach", str(path), sha), repo_path
        )

    def worktree_remove(self, repo_path: Path, path: Path) -> None:
        self._run(("git", "worktree", "remove", "--", str(path)), repo_path)

    def worktree_prune(self, repo_path: Path) -> None:
        self._run(("git", "worktree", "prune"), repo_path)

    def worktree_list(self, repo_path: Path) -> tuple[str, ...]:
        result = self._run(("git", "worktree", "list", "--porcelain"), repo_path)
        return tuple(
            line.removeprefix("worktree ")
            for line in result.stdout.splitlines()
            if line.startswith("worktree ")
        )


def _require_sha(value: str) -> None:
    if _SHA_PATTERN.match(value) is None:
        raise GitError("commit must be a full lowercase hexadecimal SHA")


def _require_ref(value: str, label: str) -> None:
    if _REF_PATTERN.match(value) is None or ".." in value:
        raise GitError(f"{label} contains unsafe characters")
