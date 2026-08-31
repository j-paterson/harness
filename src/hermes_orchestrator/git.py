"""Argv-safe local Git evidence for merge ancestry and tree proofs."""

from __future__ import annotations

import hashlib
import re
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
# Conservative allowlist for remotes, branches, and qualified refs: no
# option-like leading dash, no whitespace or control bytes, no ".." ranges.
_REF_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")
# Only hunk location metadata is normalized before digesting a delta: the
# leading "@@ -l,s +l,s @@" marker, never any trailing context text after
# it, and never any other line (whitespace, mode lines, blob ids, binary
# literals, additions/deletions, or paths all stay byte-for-byte).
_HUNK_HEADER = re.compile(r"^@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@")
# The pre-/post-image blob ids on an ``index`` line are file-identity
# metadata, not content: the same patch applied over an advanced base that
# touched the file elsewhere necessarily yields different blob ids while
# every hunk stays byte-identical (live PR #39: README.md and cli.py).
# Content is fully carried by the hunks (and by ``--binary`` literals for
# binary files) and mode by the mode lines and the index line's trailing
# mode token, which is kept verbatim.
_INDEX_LINE = re.compile(r"^index [0-9a-f]+\.\.[0-9a-f]+")


def _normalize_delta(text: str) -> str:
    """Normalize only location and identity metadata in a diff.

    Each hunk's location numbers become a bare ``@@`` marker and each
    ``index <blob>..<blob>`` prefix becomes ``index`` (its trailing mode
    token stays). Every other byte -- whitespace, old/new mode lines,
    ``GIT binary patch`` literals, ``\\ No newline at end of file``,
    additions/deletions, and paths -- is preserved verbatim.
    """

    normalized: list[str] = []
    for line in text.splitlines(keepends=True):
        line = _HUNK_HEADER.sub("@@", line, count=1)
        line = _INDEX_LINE.sub("index", line, count=1)
        normalized.append(line)
    return "".join(normalized)


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
        self, args: tuple[str, ...], cwd: Path, *, input: str | None = None
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
        self, args: tuple[str, ...], cwd: Path, *, input: str | None = None
    ) -> GitResult:
        try:
            completed = subprocess.run(
                args,
                cwd=cwd,
                env=self._env,
                capture_output=True,
                text=True,
                timeout=self._timeout,
                check=False,
                input=input,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise GitError(f"git invocation failed: {type(error).__name__}") from error
        return GitResult(completed.returncode, completed.stdout, completed.stderr)


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

    def delta_digest(self, repo_path: Path, base: str, head: str) -> str:
        """Return a canonical sha256 digest of the diff between two commits.

        Built from ``git diff --full-index --binary --no-renames <base>
        <head>``: every byte is kept verbatim -- whitespace, old/new mode
        lines, binary patch literals, additions/deletions, and paths --
        except each hunk's location numbers (``@@ -l,s +l,s @@``) and the
        ``index`` line's pre-/post-image blob ids, which are normalized so
        the same patch applied over an advanced base still digests equal.
        Unlike ``git patch-id --stable``, this digest
        is sensitive to whitespace-only changes (indentation), file mode
        changes, and binary content drift. An empty delta is never proof
        and fails closed.
        """

        _require_sha(base)
        _require_sha(head)
        result = self._runner.run(
            ("git", "diff", "--full-index", "--binary", "--no-renames", base, head),
            repo_path,
        )
        if result.returncode != 0:
            raise GitError(
                "git diff --full-index --binary failed with exit code "
                f"{result.returncode}"
            )
        if not result.stdout:
            raise GitError("empty delta")
        normalized = _normalize_delta(result.stdout)
        return hashlib.sha256(
            normalized.encode("utf-8", "surrogateescape")
        ).hexdigest()


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
