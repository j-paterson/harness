"""Argv-safe local Git evidence for merge ancestry and tree proofs."""

from __future__ import annotations

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

    def run(self, args: tuple[str, ...], cwd: Path) -> GitResult: ...


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

    def run(self, args: tuple[str, ...], cwd: Path) -> GitResult:
        try:
            completed = subprocess.run(
                args,
                cwd=cwd,
                env=self._env,
                capture_output=True,
                text=True,
                timeout=self._timeout,
                check=False,
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


def _require_sha(value: str) -> None:
    if _SHA_PATTERN.match(value) is None:
        raise GitError("commit must be a full lowercase hexadecimal SHA")


def _require_ref(value: str, label: str) -> None:
    if _REF_PATTERN.match(value) is None or ".." in value:
        raise GitError(f"{label} contains unsafe characters")
