"""Ponytail review gate for the Hermes-managed Sol merger session.

Operator correction c3f4aad5, hardened by Sol correction a9cc6d5f: block
only ``git commit`` and ``git push`` in the Hermes-managed Sol session
until the current candidate snapshot has received @ponytail-review, with
any change to the candidate invalidating the prior review.

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
unless the canonical candidate snapshot of the command's *effective*
repository matches the one recorded review marker — a single
session-local file inside that repository's git directory, overwritten
in place. Recording a review stores the hash; any change to the
candidate invalidates it by hash comparison; a candidate identical to
HEAD means nothing unreviewed and passes. Nothing accumulates: not a
receipt, manifest, or provenance subsystem.

Canonical snapshot (correction a9cc6d5f — ``git diff HEAD`` alone
omitted untracked files and the index): the SHA-256 over two tree ids,
each produced by ``git write-tree`` against a TEMPORARY index file
(``GIT_INDEX_FILE``), so the real index is never touched:

* the worktree tree — ``git read-tree HEAD`` into a scratch index, then
  ``git add -A``, then ``git write-tree`` — one tree covering tracked
  modifications, deletions, renames, and untracked files exactly as a
  later ``git add -A && git commit`` would capture them (ignored files
  excluded, as that commit would exclude them);
* the index tree — ``git write-tree`` over a copy of the real index —
  so staged state is captured independently of the worktree.

Effective target (same correction): the guard resolves the repository a
guarded command actually operates on — applying ``-C`` values in order,
``cd`` steps earlier in the shell line, and ``--git-dir``/``--work-tree``
in both ``=`` and split forms — then validates the marker inside THAT
repository's git directory against THAT repository's snapshot. Any
context the guard cannot prove fails closed naming the reason:
``GIT_DIR``-style environment overrides, ``-c core.worktree`` or
``-c core.bare``, ``--namespace``, ``--bare``, untrackable ``cd``
forms, and shell text that cannot be tokenized.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BLOCK_EXIT_CODE = 2
MARKER_NAME = "hermes-ponytail-review"

_GUARDED = frozenset({"commit", "push"})
_SEPARATORS = frozenset({"&&", "||", ";", "|", "&"})
_WRAPPERS = frozenset({"env", "command", "nohup"})
_CHDIR_BUILTINS = frozenset({"cd", "pushd", "popd"})
_ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
# Environment overrides that relocate the repository a git command acts
# on; the guard cannot prove the reviewed target under them.
_CONTEXT_ENV = frozenset(
    {
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_NAMESPACE",
        "GIT_CEILING_DIRECTORIES",
    }
)
# -c config keys that redirect the effective repository.
_CONTEXT_CONFIG_KEYS = frozenset({"core.worktree", "core.bare"})
_LOOSE_GIT = re.compile(r"\bgit\b")
_LOOSE_GUARDED = re.compile(r"\b(commit|push)\b")


class GuardError(RuntimeError):
    """Raised when the review state of the workspace cannot be read."""


@dataclass(frozen=True, slots=True)
class GuardDecision:
    """One gate decision: allow the command, or block it with a reason."""

    allowed: bool
    reason: str


@dataclass(frozen=True, slots=True)
class GitInvocation:
    """One guarded git invocation and its effective-context selectors.

    ``context`` holds the ordered ``(option, value)`` pairs — simulated
    ``-C`` steps from ``cd`` builtins plus the invocation's own ``-C``,
    ``--git-dir``, and ``--work-tree`` — exactly as git would apply
    them. ``unresolvable`` names the reason the effective target cannot
    be proven, which fails the gate closed.
    """

    subcommand: str
    context: tuple[tuple[str, str], ...] = ()
    unresolvable: str | None = None


def guarded_subcommand(command: object) -> str | None:
    """Return ``commit`` or ``push`` when the command invokes it, else None.

    Accepts the shell string or argv list found in a PreToolUse
    ``tool_input.command``. Tokens that are themselves shell scripts
    (``bash -lc "…"``) are parsed recursively; shell text that cannot be
    tokenized fails closed when it plainly names a guarded git operation.
    """

    invocations = guarded_invocations(command)
    return invocations[0].subcommand if invocations else None


def guarded_invocations(command: object) -> tuple[GitInvocation, ...]:
    """Every guarded git invocation in the command, with its context."""

    tokens = _tokens(command)
    if tokens is None:
        text = str(command)
        loose = _LOOSE_GUARDED.search(text)
        if _LOOSE_GIT.search(text) and loose:
            return (
                GitInvocation(
                    loose.group(1),
                    unresolvable="the shell text could not be tokenized",
                ),
            )
        return ()
    found = _scan(tokens)
    for token in tokens:
        if token != command and " " in token and _LOOSE_GIT.search(token):
            found.extend(guarded_invocations(token))
    return tuple(found)


def _tokens(command: object) -> list[str] | None:
    if isinstance(command, list | tuple):
        return [str(token) for token in command]
    try:
        return shlex.split(str(command), posix=True)
    except ValueError:
        return None


def _scan(tokens: list[str]) -> list[GitInvocation]:
    """Find guarded git subcommands at shell command positions."""

    invocations: list[GitInvocation] = []
    chdirs: list[tuple[str, str]] = []  # simulated -C steps from `cd`
    chdir_problem: str | None = None
    env_problem: str | None = None
    command_position = True
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in _SEPARATORS:
            command_position = True
            env_problem = None
            index += 1
            continue
        if not command_position:
            index += 1
            continue
        if _ASSIGNMENT.match(token):
            name = token.split("=", 1)[0]
            if name in _CONTEXT_ENV:
                env_problem = (
                    f"the {name} environment override changes the git context"
                )
            index += 1
            continue
        if token in _WRAPPERS:
            index += 1
            continue
        if token in _CHDIR_BUILTINS:
            arguments: list[str] = []
            index += 1
            while index < len(tokens) and tokens[index] not in _SEPARATORS:
                arguments.append(tokens[index])
                index += 1
            if (
                token != "popd"
                and len(arguments) == 1
                and not arguments[0].startswith("-")
            ):
                chdirs.append(("-C", arguments[0]))
            else:
                chdir_problem = (
                    f"the working directory cannot be tracked across `{token}`"
                )
            command_position = False
            continue
        if token == "git" or token.endswith("/git"):
            invocation = _parse_git(
                tokens[index + 1 :], tuple(chdirs), env_problem or chdir_problem
            )
            if invocation is not None:
                invocations.append(invocation)
        command_position = False
        index += 1
    return invocations


def _parse_git(
    rest: list[str],
    context: tuple[tuple[str, str], ...],
    problem: str | None,
) -> GitInvocation | None:
    selectors = list(context)
    subcommand: str | None = None
    index = 0
    while index < len(rest):
        token = rest[index]
        if token in _SEPARATORS:
            return None
        if token in {"-C", "--git-dir", "--work-tree", "-c", "--namespace"}:
            if index + 1 >= len(rest) or rest[index + 1] in _SEPARATORS:
                return None  # git dies on the missing value; nothing runs
            value = rest[index + 1]
            if token == "-c":
                problem = problem or _config_problem(value)
            elif token == "--namespace":
                problem = problem or "--namespace changes the git context"
            else:
                selectors.append((token, value))
            index += 2
            continue
        if token.startswith("--git-dir="):
            selectors.append(("--git-dir", token.removeprefix("--git-dir=")))
            index += 1
            continue
        if token.startswith("--work-tree="):
            selectors.append(("--work-tree", token.removeprefix("--work-tree=")))
            index += 1
            continue
        if token.startswith("--namespace="):
            problem = problem or "--namespace changes the git context"
            index += 1
            continue
        if token == "--bare":
            problem = problem or "--bare changes the git context"
            index += 1
            continue
        if token.startswith("-"):
            index += 1
            continue
        subcommand = token
        break
    if subcommand not in _GUARDED:
        return None
    return GitInvocation(subcommand, tuple(selectors), problem)


def _config_problem(value: str) -> str | None:
    key = value.split("=", 1)[0].strip().lower()
    if key in _CONTEXT_CONFIG_KEYS:
        return f"-c {key} changes the git context"
    return None


def _base_env() -> dict[str, str]:
    env = dict(os.environ)
    for name in _CONTEXT_ENV:
        env.pop(name, None)
    return env


def _git(args: list[str], env: dict[str, str] | None = None) -> bytes:
    result = subprocess.run(
        ["git", *args],
        capture_output=True,
        check=False,
        env=_base_env() if env is None else env,
    )
    if result.returncode != 0:
        detail = result.stderr.decode(errors="replace").strip()
        raise GuardError(f"git {' '.join(args)} failed: {detail}")
    return result.stdout


def _resolve_target(context_args: list[str]) -> tuple[Path, Path]:
    """The (top level, absolute git dir) a git context operates on."""

    toplevel = (
        _git([*context_args, "rev-parse", "--show-toplevel"]).decode().strip()
    )
    git_dir = (
        _git([*context_args, "rev-parse", "--absolute-git-dir"]).decode().strip()
    )
    return Path(toplevel), Path(git_dir)


def _invocation_context(base: Path, invocation: GitInvocation) -> list[str]:
    args = ["-C", str(base)]
    for option, value in invocation.context:
        args.extend([option, value])
    return args


def _snapshot(toplevel: Path, git_dir: Path) -> tuple[str, bool]:
    """(canonical snapshot hash, whether the candidate matches HEAD).

    Both tree ids come from ``git write-tree`` against temporary index
    files, so the real index is never modified; ``git add -A`` into the
    scratch index writes only loose objects, which git prunes.
    """

    ctx = ["-C", str(toplevel), "--git-dir", str(git_dir)]
    head_tree = _git([*ctx, "rev-parse", "HEAD^{tree}"]).decode().strip()
    index_path = Path(
        _git([*ctx, "rev-parse", "--git-path", "index"]).decode().strip()
    )
    if not index_path.is_absolute():
        index_path = toplevel / index_path
    with tempfile.TemporaryDirectory() as scratch:
        worktree_env = _base_env()
        worktree_env["GIT_INDEX_FILE"] = str(Path(scratch) / "worktree-index")
        _git([*ctx, "read-tree", "HEAD"], env=worktree_env)
        _git([*ctx, "add", "-A"], env=worktree_env)
        worktree_tree = (
            _git([*ctx, "write-tree"], env=worktree_env).decode().strip()
        )
        index_copy = Path(scratch) / "index-copy"
        if index_path.exists():
            shutil.copy2(index_path, index_copy)
        index_env = _base_env()
        index_env["GIT_INDEX_FILE"] = str(index_copy)
        index_tree = _git([*ctx, "write-tree"], env=index_env).decode().strip()
    digest = hashlib.sha256(
        f"worktree-tree {worktree_tree}\nindex-tree {index_tree}\n".encode()
    ).hexdigest()
    return digest, worktree_tree == index_tree == head_tree


def snapshot_hash(repo: Path) -> str:
    """Canonical candidate-snapshot hash of the repository at ``repo``."""

    toplevel, git_dir = _resolve_target(["-C", str(repo)])
    digest, _ = _snapshot(toplevel, git_dir)
    return digest


def marker_path(repo: Path) -> Path:
    """The one review marker file, inside this workspace's git directory."""

    _, git_dir = _resolve_target(["-C", str(repo)])
    return git_dir / MARKER_NAME


def record_review(repo: Path) -> str:
    """Mark the current candidate snapshot Ponytail-reviewed; return its hash.

    Computes the same canonical snapshot the gate compares, for the same
    effective repository. Overwrites the single marker in place — no
    history accumulates.
    """

    toplevel, git_dir = _resolve_target(["-C", str(repo)])
    digest, _ = _snapshot(toplevel, git_dir)
    (git_dir / MARKER_NAME).write_text(digest + "\n", encoding="utf-8")
    return digest


def reviewed_hash(repo: Path) -> str | None:
    """The recorded reviewed-snapshot hash, or None when none is recorded."""

    try:
        return marker_path(repo).read_text(encoding="utf-8").strip() or None
    except OSError:
        return None


def _marker_value(git_dir: Path) -> str | None:
    try:
        text = (git_dir / MARKER_NAME).read_text(encoding="utf-8")
    except OSError:
        return None
    return text.strip() or None


def evaluate_command(command: object, repo: Path) -> GuardDecision:
    """Gate one shell command against the recorded Ponytail review.

    Every guarded invocation in the command is resolved to its effective
    repository and compared against that repository's own marker.
    """

    invocations = guarded_invocations(command)
    if not invocations:
        return GuardDecision(allowed=True, reason="not a guarded git command")
    all_clean = True
    for invocation in invocations:
        if invocation.unresolvable is not None:
            return GuardDecision(
                allowed=False,
                reason=(
                    f"ponytail-review gate: git {invocation.subcommand} is "
                    f"blocked: {invocation.unresolvable}; the reviewed target "
                    "cannot be proven, so the gate fails closed."
                ),
            )
        try:
            toplevel, git_dir = _resolve_target(
                _invocation_context(Path(repo), invocation)
            )
            digest, clean = _snapshot(toplevel, git_dir)
        except GuardError as error:
            return GuardDecision(
                allowed=False,
                reason=(
                    "ponytail-review gate: cannot verify the candidate "
                    f"snapshot: {error}"
                ),
            )
        if clean:
            continue
        all_clean = False
        if digest != _marker_value(git_dir):
            return GuardDecision(
                allowed=False, reason=_block_reason(invocation.subcommand)
            )
    if all_clean:
        return GuardDecision(
            allowed=True,
            reason="working tree matches HEAD; nothing unreviewed",
        )
    return GuardDecision(
        allowed=True,
        reason="the candidate snapshot has a matching ponytail review",
    )


def _block_reason(subcommand: str) -> str:
    return (
        f"ponytail-review gate: git {subcommand} is blocked because the "
        "current candidate snapshot has not received @ponytail-review. Run "
        "@ponytail-review on the current changes, apply only the safe "
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
    """The explicit command Sol runs to mark the current candidate reviewed."""

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
