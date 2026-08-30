"""Ponytail review gate for the Hermes-managed Sol merger session.

Operator correction c3f4aad5, hardened by Sol corrections a9cc6d5f and
743338e2: block only ``git commit`` and ``git push`` in the
Hermes-managed Sol session until the current candidate snapshot has
received @ponytail-review, with any change to the candidate invalidating
the prior review.

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

Command analysis (correction 743338e2 — the earlier parser classified
anything it could not match as unguarded and ALLOWED it; the default is
now inverted):

1. a cheap conservative TRIGGER fires whenever the input so much as
   mentions ``commit`` or ``push`` (case-insensitive substring);
2. a triggered input must be POSITIVELY PROVEN safe by a strict
   recognizer: simple commands joined by ``&&``/``||``/``;`` (plain
   pipes are accepted only between segments proven free of guarded git
   invocations), the fully modeled wrappers ``command``/``env``/
   ``nohup``, ``export``/``unset`` of context-inert names, trackable
   ``cd``/``pushd``, ``sh``/``bash``-style ``-c`` scripts (recursed),
   and git invocations whose global options all come from an explicit
   modeled allowlist and whose subcommand is either guarded or on the
   safe-builtin allowlist. Everything else — subshells, command
   substitution, backquotes, heredocs, background ``&``, multi-line
   input, unmodeled wrapper or git options, ``--exec-path``,
   ``--config-env``, context-changing ``-c`` keys, argument-executing
   wrappers such as ``sudo``/``xargs``/``timeout``, unknown git
   subcommands (possible aliases), and untokenizable text — BLOCKS
   with the reason named;
3. proven guarded invocations then go through effective-target
   resolution and snapshot comparison exactly as before.

Environment context (same correction — the guard used to scrub the
``GIT_*`` family for its own subprocesses while the executed command
still inherited it): every guard subprocess now REPLAYS the environment
the guarded command will actually run under, so resolution and
snapshotting describe the same effective repository the guarded command
will operate on, and the marker is required from THAT repository. When
the command line itself transforms the environment through the ``env``
wrapper, the guard models the transformation exactly as env applies it
— ``-i``/``-``/``--ignore-environment`` clears the inherited
environment down to the command-line assignments, ``-u``/``--unset``
of a context-inert name removes it, and assignments overlay — and every
guard subprocess for that invocation receives the transformed
environment, never ``os.environ``. Under a modeled clear no inherited
``GIT_DIR``/``GIT_WORK_TREE`` survives, so resolution binds the
repository the cleared command will actually operate on (typically the
working directory). Removal of a context-affecting ``GIT_*`` or
``GIT_CONFIG*`` name via ``env -u``/``--unset`` is rejected by name —
never accepted — matching the ``unset`` builtin rule. Inherited context-changing
``GIT_CONFIG_*`` entries that command-line configuration could not
model (``core.worktree``, ``core.bare``, includes, or an opaque
``GIT_CONFIG_PARAMETERS``) fail closed naming the variable. Inline
``export``/assignments of the ``GIT_*`` context family and
``GIT_CONFIG*`` names in the command itself are rejected rather than
modeled, as are ``--config-env`` and context-changing ``-c`` keys.
File-based configuration includes need no enumeration: replaying the
environment identically makes ``rev-parse`` resolve the same effective
target the command will see.
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
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BLOCK_EXIT_CODE = 2
MARKER_NAME = "hermes-ponytail-review"

_GUARDED = frozenset({"commit", "push"})
_TRIGGER = re.compile(r"commit|push", re.IGNORECASE)
_ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Shell operator tokens the recognizer accepts; every other punctuation
# run (subshells, background &, heredocs, >&, |&, ...) blocks.
_SAFE_SEPARATORS = frozenset({"&&", "||", ";"})
_REDIRECTS = frozenset({">", ">>", "<"})
_PUNCTUATION = frozenset("();<>|&")

_CHDIR_BUILTINS = frozenset({"cd", "pushd", "popd"})
_SHELLS = frozenset({"sh", "bash", "zsh", "dash", "ksh"})
_SHELL_FLAG_LETTERS = frozenset("cileux")
# Programs that execute their arguments as a command; a guarded verb
# behind them cannot be attributed, so they block when triggered.
_EXEC_WRAPPERS = frozenset(
    {
        "sudo",
        "doas",
        "xargs",
        "time",
        "timeout",
        "nice",
        "ionice",
        "setsid",
        "stdbuf",
        "watch",
        "script",
        "ssh",
        "strace",
        "caffeinate",
    }
)

# Environment overrides that relocate the repository a git command acts
# on. Inherited values are replayed identically; values introduced by
# the command line itself are rejected.
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
# -c / GIT_CONFIG_* keys that redirect the effective repository or pull
# in unmodeled configuration.
_CONTEXT_CONFIG_KEYS = frozenset({"core.worktree", "core.bare", "include.path"})

# git global options proven inert for target resolution.
_INERT_GIT_OPTIONS = frozenset(
    {
        "-p",
        "--paginate",
        "-P",
        "--no-pager",
        "--no-optional-locks",
        "--no-replace-objects",
        "--literal-pathspecs",
        "--glob-pathspecs",
        "--noglob-pathspecs",
        "--icase-pathspecs",
    }
)
_VALUE_SELECTORS = frozenset({"-C", "--git-dir", "--work-tree"})

# git subcommands positively known not to be commit or push (builtins
# cannot be shadowed by aliases). Anything not listed blocks when the
# input is triggered, because an alias could expand to a guarded verb.
_SAFE_GIT_SUBCOMMANDS = frozenset(
    {
        "add",
        "apply",
        "blame",
        "branch",
        "cat-file",
        "checkout",
        "cherry-pick",
        "clean",
        "clone",
        "describe",
        "diff",
        "fetch",
        "grep",
        "init",
        "log",
        "ls-files",
        "ls-remote",
        "ls-tree",
        "merge",
        "mv",
        "pull",
        "rebase",
        "reflog",
        "remote",
        "reset",
        "restore",
        "rev-list",
        "rev-parse",
        "revert",
        "rm",
        "shortlog",
        "show",
        "stash",
        "status",
        "switch",
        "tag",
    }
)

_SAFE = ("safe", None)


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
    be proven, which fails the gate closed. ``env_ops`` holds the
    ordered environment operations the command line applies before the
    invocation runs — ``("clear",)``, ``("unset", name)``,
    ``("set", name, value)`` — exactly as ``env`` would; the gate
    replays them onto the inherited environment and resolves and
    snapshots under the result.
    """

    subcommand: str
    context: tuple[tuple[str, str], ...] = ()
    unresolvable: str | None = None
    env_ops: tuple[tuple[str, ...], ...] = ()


def guarded_subcommand(command: object) -> str | None:
    """Return ``commit`` or ``push`` when the command may invoke it.

    Accepts the shell string or argv list found in a PreToolUse
    ``tool_input.command``. Input the recognizer cannot positively
    prove safe is reported as a guarded invocation whose
    ``unresolvable`` reason fails the gate closed.
    """

    invocations = guarded_invocations(command)
    return invocations[0].subcommand if invocations else None


def guarded_invocations(command: object) -> tuple[GitInvocation, ...]:
    """Every guarded git invocation in the command, with its context.

    The default is inverted from the original parser (correction
    743338e2): once the conservative trigger fires, anything that is
    not positively proven safe is returned as a guarded invocation
    carrying an ``unresolvable`` reason, so the gate blocks it.
    """

    if isinstance(command, list | tuple):
        return _analyze_argv([str(token) for token in command])
    return _analyze_shell(str(command))


def _loose_verb(text: str) -> str:
    match = _TRIGGER.search(text)
    return match.group(0).lower() if match else "commit"


def _context_env_name(name: str) -> bool:
    return name in _CONTEXT_ENV or name.startswith("GIT_CONFIG")


def _context_config_key(key: str) -> bool:
    return key in _CONTEXT_CONFIG_KEYS or key.startswith("includeif.")


def _shell_tokens(text: str) -> list[str] | None:
    lexer = shlex.shlex(text, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    # '#' must not hide trailing shell text from the recognizer.
    lexer.commenters = ""
    try:
        return list(lexer)
    except ValueError:
        return None


def _analyze_shell(
    text: str,
    context: tuple[tuple[str, str], ...] = (),
    problem: str | None = None,
    env_ops: tuple[tuple[str, ...], ...] = (),
) -> tuple[GitInvocation, ...]:
    if not _TRIGGER.search(text):
        return ()
    verb = _loose_verb(text)

    def block(reason: str) -> tuple[GitInvocation, ...]:
        return (GitInvocation(verb, unresolvable=reason),)

    if "\n" in text or "\r" in text:
        return block(
            "multi-line shell input cannot be proven safe; "
            "run each command on its own line"
        )
    if "`" in text:
        return block("backquote command substitution cannot be proven safe")
    if "$(" in text:
        return block("command substitution cannot be proven safe")
    tokens = _shell_tokens(text)
    if tokens is None:
        return block("the shell text could not be tokenized")

    segments: list[list[str]] = [[]]
    has_pipe = False
    for token in tokens:
        if token and all(char in _PUNCTUATION for char in token):
            if token in _SAFE_SEPARATORS:
                segments.append([])
            elif token == "|":
                has_pipe = True
                segments.append([])
            elif token in _REDIRECTS:
                segments[-1].append(token)
            else:
                return block(
                    f"the shell operator `{token}` cannot be proven safe"
                )
        else:
            segments[-1].append(token)

    chdirs = list(context)
    state = {"problem": problem}
    found: list[GitInvocation] = []
    for segment in segments:
        kind, value = _classify_segment(segment, chdirs, state, env_ops)
        if kind == "blocked":
            return block(str(value))
        if kind == "guarded":
            found.extend(value)  # type: ignore[arg-type]
    if has_pipe and found:
        return block(
            "a pipeline cannot carry git commit or push; "
            "run the guarded git command on its own"
        )
    return tuple(found)


def _analyze_argv(argv: list[str]) -> tuple[GitInvocation, ...]:
    text = " ".join(argv)
    if not argv or not _TRIGGER.search(text):
        return ()
    verb = _loose_verb(text)
    if argv[0].rsplit("/", 1)[-1] in _SHELLS:
        kind, value = _shell_runner(argv[1:], (), None)
    else:
        kind, value = _classify_segment(list(argv), [], {"problem": None})
    if kind == "blocked":
        return (GitInvocation(verb, unresolvable=str(value)),)
    if kind == "guarded":
        return tuple(value)  # type: ignore[arg-type]
    return ()


def _classify_segment(
    tokens: list[str],
    chdirs: list[tuple[str, str]],
    state: dict[str, str | None],
    env_ops: tuple[tuple[str, ...], ...] = (),
) -> tuple[str, object]:
    """Prove one simple command safe, guarded, or blocked.

    ``chdirs`` and ``state`` persist across segments: ``cd`` steps in
    earlier segments select the directory later git invocations run in.
    ``env_ops`` seeds the environment transformation an enclosing
    wrapper already applies (e.g. ``env -i bash -c ...``); prefix
    assignments and ``env`` wrappers in this segment extend it, and the
    resulting operations travel with any guarded invocation found.
    """

    ops: list[tuple[str, ...]] = list(env_ops)
    core: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in _REDIRECTS:
            if index + 1 >= len(tokens):
                return "blocked", "a redirection is missing its target"
            index += 2  # redirection target is data, not a command
            continue
        core.append(token)
        index += 1

    index = 0
    while index < len(core) and _ASSIGNMENT.match(core[index]):
        name, value = core[index].split("=", 1)
        if _context_env_name(name):
            return (
                "blocked",
                f"the {name} environment override changes the git context",
            )
        ops.append(("set", name, value))
        index += 1

    while index < len(core):
        program = core[index]
        if "$" in program:
            return (
                "blocked",
                "the command name contains an unexpanded substitution "
                "or variable",
            )
        base = program.rsplit("/", 1)[-1]
        if base == "command":
            outcome = _consume_command_wrapper(core, index)
            if isinstance(outcome, tuple):
                return outcome
            if outcome < 0:
                return _SAFE  # `command -v/-V`: prints, executes nothing
            index = outcome
            continue
        if base == "env":
            outcome = _consume_env_wrapper(core, index, ops)
            if isinstance(outcome, tuple):
                return outcome
            index = outcome
            continue
        if base == "nohup":
            index += 1
            if index < len(core) and core[index].startswith("-"):
                return (
                    "blocked",
                    f"the nohup option {core[index]} is not modeled",
                )
            continue
        if program == "export":
            return _classify_export(core[index + 1 :])
        if program == "unset":
            for token in core[index + 1 :]:
                if _context_env_name(token):
                    return (
                        "blocked",
                        f"unsetting {token} desynchronizes the git context "
                        "the gate verifies",
                    )
            return _SAFE
        if program in _CHDIR_BUILTINS:
            arguments = core[index + 1 :]
            if (
                program != "popd"
                and len(arguments) == 1
                and not arguments[0].startswith("-")
                and "$" not in arguments[0]
            ):
                chdirs.append(("-C", arguments[0]))
            else:
                state["problem"] = (
                    f"the working directory cannot be tracked across "
                    f"`{program}`"
                )
            return _SAFE
        if program == "git" or program.endswith("/git"):
            return _prove_git(
                core[index + 1 :], tuple(chdirs), state["problem"], tuple(ops)
            )
        if base in _SHELLS:
            return _shell_runner(
                core[index + 1 :], tuple(chdirs), state["problem"], tuple(ops)
            )
        if base in _EXEC_WRAPPERS:
            return (
                "blocked",
                f"`{base}` executes its arguments and is not modeled by the "
                "gate; run the git command directly",
            )
        # An ordinary program: its remaining tokens are arguments (data),
        # not an executable path to git.
        return _SAFE
    return _SAFE


def _consume_command_wrapper(
    core: list[str], index: int
) -> int | tuple[str, str]:
    """Consume ``command`` and its options; -1 means a safe query form."""

    index += 1
    query = False
    while index < len(core):
        token = core[index]
        if token == "--":
            index += 1
            break
        if (
            token.startswith("-")
            and len(token) > 1
            and all(char in "pvV" for char in token[1:])
        ):
            if "v" in token or "V" in token:
                query = True
            index += 1
            continue
        if token.startswith("-"):
            return "blocked", f"the `command` option {token} is not modeled"
        break
    return -1 if query else index


def _consume_env_wrapper(
    core: list[str], index: int, ops: list[tuple[str, ...]]
) -> int | tuple[str, str]:
    """Consume ``env``, recording its transformation into ``ops``.

    Every accepted form is appended to ``ops`` exactly as env applies
    it, so the gate later replays the identical child environment.
    Removal of a context-affecting name is rejected by name — guard
    subprocesses must never retain context the executed command loses
    (nor the reverse); exotic env options stay blocked unmodeled.
    """

    index += 1
    while index < len(core):
        token = core[index]
        if token == "--":
            index += 1
            break
        if token in {"-i", "-", "--ignore-environment"}:
            ops.append(("clear",))
            index += 1
            continue
        if token == "-u" or token.startswith("--unset="):
            if token == "-u":
                if index + 1 >= len(core):
                    return "blocked", "env -u is missing its argument"
                name = core[index + 1]
                index += 2
            else:
                name = token.split("=", 1)[1]
                index += 1
            if _context_env_name(name):
                return (
                    "blocked",
                    f"env-unsetting {name} desynchronizes the git context "
                    "the gate verifies",
                )
            ops.append(("unset", name))
            continue
        if token.startswith("-"):
            return "blocked", f"the env option {token} is not modeled"
        if _ASSIGNMENT.match(token):
            name, value = token.split("=", 1)
            if _context_env_name(name):
                return (
                    "blocked",
                    f"the {name} environment override changes the git "
                    "context",
                )
            ops.append(("set", name, value))
            index += 1
            continue
        break
    return index


def _classify_export(arguments: list[str]) -> tuple[str, object]:
    for token in arguments:
        name = token.split("=", 1)[0]
        if not _NAME.match(name):
            return "blocked", "the export statement cannot be proven safe"
        if _context_env_name(name):
            return (
                "blocked",
                f"exporting {name} changes the git context for later "
                "commands",
            )
    return _SAFE


def _shell_runner(
    arguments: list[str],
    context: tuple[tuple[str, str], ...],
    problem: str | None,
    env_ops: tuple[tuple[str, ...], ...] = (),
) -> tuple[str, object]:
    """Prove a ``bash -c``-style invocation by recursing into the script."""

    script: str | None = None
    saw_script_flag = False
    for token in arguments:
        if token.startswith("-") and len(token) > 1:
            if not all(char in _SHELL_FLAG_LETTERS for char in token[1:]):
                return "blocked", f"the shell option {token} is not modeled"
            if "c" in token:
                saw_script_flag = True
            continue
        script = token
        break
    if not saw_script_flag or script is None:
        return (
            "blocked",
            "a shell invocation without a provable -c script cannot be "
            "proven safe",
        )
    inner = _analyze_shell(script, context, problem, env_ops)
    if not inner:
        return _SAFE
    return "guarded", inner


def _prove_git(
    rest: list[str],
    context: tuple[tuple[str, str], ...],
    problem: str | None,
    env_ops: tuple[tuple[str, ...], ...] = (),
) -> tuple[str, object]:
    """Prove one git invocation: every global option must be modeled."""

    selectors = list(context)
    subcommand: str | None = None
    index = 0
    while index < len(rest):
        token = rest[index]
        if token in _VALUE_SELECTORS:
            if index + 1 >= len(rest):
                return _SAFE  # git dies on the missing value; nothing runs
            value = rest[index + 1]
            if "$" in value:
                return (
                    "blocked",
                    f"the {token} value contains an unexpanded substitution "
                    "or variable",
                )
            selectors.append((token, value))
            index += 2
            continue
        if token.startswith(("--git-dir=", "--work-tree=")):
            option, value = token.split("=", 1)
            if "$" in value:
                return (
                    "blocked",
                    f"the {option} value contains an unexpanded substitution "
                    "or variable",
                )
            selectors.append((option, value))
            index += 1
            continue
        if token == "-c":
            if index + 1 >= len(rest):
                return _SAFE  # git dies on the missing value; nothing runs
            key = rest[index + 1].split("=", 1)[0].strip().lower()
            if "$" in key:
                return (
                    "blocked",
                    "the -c configuration key contains an unexpanded "
                    "substitution or variable",
                )
            if _context_config_key(key):
                return "blocked", f"-c {key} changes the git context"
            index += 2
            continue
        if token == "--config-env" or token.startswith("--config-env="):
            return (
                "blocked",
                "--config-env supplies configuration from the environment "
                "that the gate does not model",
            )
        if token == "--namespace" or token.startswith("--namespace="):
            return "blocked", "--namespace changes the git context"
        if token == "--bare":
            return "blocked", "--bare changes the git context"
        if token == "--exec-path" or token.startswith("--exec-path="):
            return "blocked", "--exec-path changes which git programs run"
        if token in _INERT_GIT_OPTIONS:
            index += 1
            continue
        if token.startswith("-"):
            return (
                "blocked",
                f"the git option {token} is not modeled; only proven-safe "
                "forms pass",
            )
        subcommand = token
        break
    if subcommand is None:
        return _SAFE  # bare `git` (or options only) prints usage
    if "$" in subcommand:
        return (
            "blocked",
            "the git subcommand contains an unexpanded substitution or "
            "variable",
        )
    if subcommand in _GUARDED:
        return "guarded", (
            GitInvocation(subcommand, tuple(selectors), problem, env_ops),
        )
    if subcommand in _SAFE_GIT_SUBCOMMANDS:
        return _SAFE
    return (
        "blocked",
        f"`git {subcommand}` cannot be proven free of commit or push "
        "(aliases and unknown subcommands are not modeled)",
    )


def _transformed_env(
    ops: tuple[tuple[str, ...], ...],
) -> dict[str, str] | None:
    """The exact child environment ``env`` would hand the command.

    ``None`` when no transformation applies — subprocesses then inherit
    the process environment unchanged, as before. Otherwise the ops are
    replayed in order onto a copy of the inherited environment: a clear
    keeps only later assignments, an unset removes a name, a set
    overlays one — exactly env's own semantics.
    """

    if not ops:
        return None
    env = dict(os.environ)
    for op in ops:
        if op[0] == "clear":
            env = {}
        elif op[0] == "unset":
            env.pop(op[1], None)
        else:
            env[op[1]] = op[2]
    return env


def _inherited_config_problem(env: Mapping[str, str]) -> str | None:
    """Context-changing GIT_CONFIG_* the invocation's environment carries.

    ``env`` is the environment the guarded invocation will actually run
    under (the transformed child environment when the command line
    transforms it). The rest of the ``GIT_*`` family is replayed
    identically into every guard subprocess, so resolution and
    snapshotting already describe the repository the command will
    operate on. Command-line-equivalent configuration entries that
    would change the context are rejected by name instead, matching the
    ``-c`` rules.
    """

    if env.get("GIT_CONFIG_PARAMETERS"):
        return (
            "the inherited GIT_CONFIG_PARAMETERS configuration cannot be "
            "modeled"
        )
    count = env.get("GIT_CONFIG_COUNT")
    if count:
        try:
            entries = int(count)
        except ValueError:
            return "the inherited GIT_CONFIG_COUNT value is not an integer"
        for position in range(entries):
            key = env.get(f"GIT_CONFIG_KEY_{position}", "").strip().lower()
            if _context_config_key(key):
                return (
                    f"the inherited GIT_CONFIG_KEY_{position}={key} "
                    "configuration changes the git context"
                )
    return None


def _git(args: list[str], env: dict[str, str] | None = None) -> bytes:
    # env=None inherits the process environment unchanged; otherwise
    # the guard replays the exact (possibly transformed) environment
    # the guarded command will run under.
    try:
        result = subprocess.run(
            ["git", *args], capture_output=True, check=False, env=env
        )
    except OSError as error:
        raise GuardError(f"git {' '.join(args)} failed: {error}") from error
    if result.returncode != 0:
        detail = result.stderr.decode(errors="replace").strip()
        raise GuardError(f"git {' '.join(args)} failed: {detail}")
    return result.stdout


def _resolve_target(
    context_args: list[str], env: dict[str, str] | None = None
) -> tuple[Path, Path]:
    """The (top level, absolute git dir) a git context operates on."""

    toplevel = (
        _git([*context_args, "rev-parse", "--show-toplevel"], env=env)
        .decode()
        .strip()
    )
    git_dir = (
        _git([*context_args, "rev-parse", "--absolute-git-dir"], env=env)
        .decode()
        .strip()
    )
    return Path(toplevel), Path(git_dir)


def _invocation_context(base: Path, invocation: GitInvocation) -> list[str]:
    args = ["-C", str(base)]
    for option, value in invocation.context:
        args.extend([option, value])
    return args


def _snapshot(
    toplevel: Path, git_dir: Path, env: dict[str, str] | None = None
) -> tuple[str, bool]:
    """(canonical snapshot hash, whether the candidate matches HEAD).

    Both tree ids come from ``git write-tree`` against temporary index
    files, so the real index is never modified; ``git add -A`` into the
    scratch index writes only loose objects, which git prunes. The
    invocation's environment — inherited, or the modeled child
    environment when the command line transforms it — is replayed
    identically, with only ``GIT_INDEX_FILE`` overridden to the scratch
    index.
    """

    base_env = dict(os.environ) if env is None else env
    ctx = ["-C", str(toplevel), "--git-dir", str(git_dir)]
    head_tree = _git([*ctx, "rev-parse", "HEAD^{tree}"], env=env).decode().strip()
    index_path = Path(
        _git([*ctx, "rev-parse", "--git-path", "index"], env=env)
        .decode()
        .strip()
    )
    if not index_path.is_absolute():
        index_path = toplevel / index_path
    with tempfile.TemporaryDirectory() as scratch:
        worktree_env = dict(base_env)
        worktree_env["GIT_INDEX_FILE"] = str(Path(scratch) / "worktree-index")
        _git([*ctx, "read-tree", "HEAD"], env=worktree_env)
        _git([*ctx, "add", "-A"], env=worktree_env)
        worktree_tree = (
            _git([*ctx, "write-tree"], env=worktree_env).decode().strip()
        )
        index_copy = Path(scratch) / "index-copy"
        if index_path.exists():
            shutil.copy2(index_path, index_copy)
        index_env = dict(base_env)
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
        env = _transformed_env(invocation.env_ops)
        config_problem = _inherited_config_problem(
            os.environ if env is None else env
        )
        if config_problem is not None:
            return GuardDecision(
                allowed=False,
                reason=(
                    f"ponytail-review gate: git {invocation.subcommand} is "
                    f"blocked: {config_problem}; the reviewed target cannot "
                    "be proven, so the gate fails closed."
                ),
            )
        try:
            toplevel, git_dir = _resolve_target(
                _invocation_context(Path(repo), invocation), env
            )
            digest, clean = _snapshot(toplevel, git_dir, env)
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
