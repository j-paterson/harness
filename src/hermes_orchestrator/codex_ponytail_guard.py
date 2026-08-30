"""Ponytail review gate for the Hermes-managed Sol merger session.

Operator correction c3f4aad5: block only ``git commit`` and ``git push``
in the Hermes-managed Sol session until the current diff has received
@ponytail-review, with a changed diff invalidating the prior review.

Mechanism (installed codex-cli 0.149.0-alpha.4.1; the ``hooks`` feature
is stable and enabled): the App Server accepts free-form ``config``
overrides on ``thread/start`` and ``thread/resume`` — the same
dotted-path overrides as ``codex -c`` — and the hook engine has a
first-class ``sessionFlags`` hook source scoped to the one session the
override was passed to. :func:`session_guard_config` builds the single
``hooks.PreToolUse`` override that :class:`~.codex_merger.CodexMerger`
attaches to the managed Sol thread and to nothing else: no global Git
hook, no shared ``~/.codex`` change, no effect on any other Codex
session.

The gate: a guarded command is blocked (exit code 2 with the reason on
stderr, the Claude-compatible blocking convention Codex hooks use)
unless the SHA-256 of the current ``git diff HEAD`` matches the one
recorded review marker — a single session-local file inside the
workspace ``.git`` directory, overwritten in place. Recording a review
stores the hash; any change to the diff invalidates it by hash
comparison; an empty diff means nothing unreviewed and passes. Nothing
accumulates: not a receipt, manifest, or provenance subsystem.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BLOCK_EXIT_CODE = 2
MARKER_NAME = "hermes-ponytail-review"

_GUARDED = frozenset({"commit", "push"})
_SEPARATORS = frozenset({"&&", "||", ";", "|", "&"})
_WRAPPERS = frozenset({"env", "command", "nohup"})
_ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
# git global options that consume a following value.
_GIT_VALUE_OPTIONS = frozenset(
    {"-C", "-c", "--git-dir", "--work-tree", "--exec-path", "--namespace"}
)
_LOOSE_GIT = re.compile(r"\bgit\b")
_LOOSE_GUARDED = re.compile(r"\b(commit|push)\b")


class GuardError(RuntimeError):
    """Raised when the review state of the workspace cannot be read."""


@dataclass(frozen=True, slots=True)
class GuardDecision:
    """One gate decision: allow the command, or block it with a reason."""

    allowed: bool
    reason: str


def guarded_subcommand(command: object) -> str | None:
    """Return ``commit`` or ``push`` when the command invokes it, else None.

    Accepts the shell string or argv list found in a PreToolUse
    ``tool_input.command``. Tokens that are themselves shell scripts
    (``bash -lc "…"``) are parsed recursively; shell text that cannot be
    tokenized fails closed when it plainly names a guarded git operation.
    """

    tokens = _tokens(command)
    if tokens is None:
        text = str(command)
        loose = _LOOSE_GUARDED.search(text)
        if _LOOSE_GIT.search(text) and loose:
            return loose.group(1)
        return None
    found = _scan(tokens)
    if found is not None:
        return found
    for token in tokens:
        if token != command and " " in token and _LOOSE_GIT.search(token):
            found = guarded_subcommand(token)
            if found is not None:
                return found
    return None


def _tokens(command: object) -> list[str] | None:
    if isinstance(command, list | tuple):
        return [str(token) for token in command]
    try:
        return shlex.split(str(command), posix=True)
    except ValueError:
        return None


def _scan(tokens: list[str]) -> str | None:
    """Find a guarded git subcommand at a shell command position."""

    command_position = True
    for index, token in enumerate(tokens):
        if token in _SEPARATORS:
            command_position = True
            continue
        if not command_position:
            continue
        if _ASSIGNMENT.match(token) or token in _WRAPPERS:
            continue
        if token == "git" or token.endswith("/git"):
            subcommand = _git_subcommand(tokens[index + 1 :])
            if subcommand in _GUARDED:
                return subcommand
        command_position = False
    return None


def _git_subcommand(rest: list[str]) -> str | None:
    index = 0
    while index < len(rest):
        token = rest[index]
        if token in _SEPARATORS:
            return None
        if token in _GIT_VALUE_OPTIONS:
            index += 2
            continue
        if token.startswith("-"):
            index += 1
            continue
        return token
    return None


def _run_git(repo: Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, check=False
    )
    if result.returncode != 0:
        detail = result.stderr.decode(errors="replace").strip()
        raise GuardError(f"git {args[0]} failed in {repo}: {detail}")
    return result.stdout


def diff_hash(repo: Path) -> str:
    """SHA-256 of the current ``git diff HEAD`` (staged and unstaged)."""

    return hashlib.sha256(_run_git(repo, "diff", "HEAD")).hexdigest()


def marker_path(repo: Path) -> Path:
    """The one review marker file, inside this workspace's git directory."""

    git_dir = Path(_run_git(repo, "rev-parse", "--git-dir").decode().strip())
    if not git_dir.is_absolute():
        git_dir = repo / git_dir
    return git_dir / MARKER_NAME


def record_review(repo: Path) -> str:
    """Mark the current diff as Ponytail-reviewed; return its hash.

    Overwrites the single marker in place — no history accumulates.
    """

    current = diff_hash(repo)
    marker_path(repo).write_text(current + "\n", encoding="utf-8")
    return current


def reviewed_hash(repo: Path) -> str | None:
    """The recorded reviewed-diff hash, or None when nothing is recorded."""

    try:
        return marker_path(repo).read_text(encoding="utf-8").strip() or None
    except OSError:
        return None


def evaluate_command(command: object, repo: Path) -> GuardDecision:
    """Gate one shell command against the recorded Ponytail review."""

    subcommand = guarded_subcommand(command)
    if subcommand is None:
        return GuardDecision(allowed=True, reason="not a guarded git command")
    try:
        raw = _run_git(repo, "diff", "HEAD")
    except GuardError as error:
        return GuardDecision(
            allowed=False,
            reason=f"ponytail-review gate: cannot verify the diff: {error}",
        )
    if not raw:
        return GuardDecision(
            allowed=True,
            reason="working tree matches HEAD; nothing unreviewed",
        )
    if hashlib.sha256(raw).hexdigest() == reviewed_hash(repo):
        return GuardDecision(
            allowed=True, reason="current diff has a matching ponytail review"
        )
    return GuardDecision(allowed=False, reason=_block_reason(subcommand))


def _block_reason(subcommand: str) -> str:
    return (
        f"ponytail-review gate: git {subcommand} is blocked because the "
        "current diff has not received @ponytail-review. Run "
        "@ponytail-review on the current diff, apply only the safe "
        "simplifications it returns (behavioral or judgment-bearing "
        f"findings go back as structured corrections), then run "
        f"`{record_command()}` from the workspace and retry."
    )


def handle_hook_payload(payload: dict[str, Any]) -> GuardDecision:
    """Decide one PreToolUse payload; anything without a command passes."""

    event = payload.get("hook_event_name")
    if event is not None and event != "PreToolUse":
        return GuardDecision(allowed=True, reason=f"not a PreToolUse event: {event}")
    tool_input = payload.get("tool_input")
    command = tool_input.get("command") if isinstance(tool_input, dict) else None
    if command is None:
        return GuardDecision(allowed=True, reason="no command in tool input")
    cwd = None
    if isinstance(tool_input, dict):
        cwd = tool_input.get("workdir") or tool_input.get("cwd")
    cwd = cwd or payload.get("cwd") or os.getcwd()
    return evaluate_command(command, Path(str(cwd)))


def hook_command(python: str | None = None) -> str:
    """The guard hook's shell command line, pinned to this interpreter."""

    interpreter = python if python is not None else sys.executable
    return (
        f"{shlex.quote(interpreter)} -m "
        "hermes_orchestrator.codex_ponytail_guard hook"
    )


def record_command(python: str | None = None) -> str:
    """The explicit command Sol runs to mark the current diff reviewed."""

    interpreter = python if python is not None else sys.executable
    return (
        f"{shlex.quote(interpreter)} -m "
        "hermes_orchestrator.codex_ponytail_guard record"
    )


def session_guard_config(python: str | None = None) -> dict[str, Any]:
    """The thread-scoped Codex config override binding the guard.

    Passed as the ``config`` parameter of ``thread/start`` and
    ``thread/resume`` for the managed Sol thread only: a dotted-path
    override (the ``codex -c`` convention the App Server documents for
    session config) registering one PreToolUse command hook from the
    session-scoped ``sessionFlags`` hook source. No matcher is set so
    the hook cannot silently miss a differently named shell tool; the
    guard script itself ignores everything but guarded git commands.
    """

    return {
        "hooks.PreToolUse": [
            {
                "hooks": [
                    {
                        "type": "command",
                        "command": hook_command(python),
                        "async": False,
                        "timeoutSec": 30,
                        "statusMessage": "Ponytail review gate",
                    }
                ]
            }
        ]
    }


def main(argv: list[str] | None = None, *, stdin: str | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if args == ["hook"]:
        text = sys.stdin.read() if stdin is None else stdin
        try:
            payload = json.loads(text)
        except ValueError:
            return 0
        if not isinstance(payload, dict):
            return 0
        decision = handle_hook_payload(payload)
        if decision.allowed:
            return 0
        print(decision.reason, file=sys.stderr)
        return BLOCK_EXIT_CODE
    if args == ["record"]:
        print(record_review(Path.cwd()))
        return 0
    print("usage: codex_ponytail_guard {hook|record}", file=sys.stderr)
    return 64


if __name__ == "__main__":
    raise SystemExit(main())
