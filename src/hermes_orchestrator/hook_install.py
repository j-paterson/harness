"""Idempotent, atomic Hermes hook installation for classic profiles.

INFRA-195 (hardened by Sol correction 032cd4a5): every eligible
classic profile must run the Hermes Stop and SubagentStop hooks (plus
the PreToolUse Agent hook that records child starts) in addition to
whatever presentation hooks it already carries. Each required hook
exists exactly once under its exact event *and matcher* — a Hermes
command found under any other matcher is misplaced and is repaired
into the one canonical binding, duplicates are collapsed, and foreign
hooks are preserved. INFRA-204 extends that same single sweep to
retire superseded Hermes shims (``.hermes/intake-poll-hook.sh``): the
one legacy hook object is removed wherever it sits, its siblings and
every unrelated entry survive, and the canonical binding below still
leaves exactly one hook per event. The settings file is replaced atomically: the
merged document is validated, written to a temporary file in the same
directory, fsynced, moved over settings.json with its permissions
preserved, and the directory is fsynced — an interruption at any point
leaves the original file complete and parseable. Running the installer
twice is a no-op.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class HookCommandSet:
    """The exact hook command per lifecycle event."""

    stop: str
    subagent_stop: str
    child_start: str


# (settings event, exact matcher, HookCommandSet attribute).
# Child starts install on SubagentStart — the lifecycle event that
# shares its agent_id namespace with SubagentStop — never on
# PreToolUse, whose tool_use_id identifies the invocation, not the
# spawned agent lifecycle.
_HOOK_SPECS: tuple[tuple[str, str, str], ...] = (
    ("Stop", "*", "stop"),
    ("SubagentStop", "*", "subagent_stop"),
    ("SubagentStart", "*", "child_start"),
)

# INFRA-204: superseded Hermes hook shells. A profile that still
# references one of these is carrying a hook this installer replaced;
# the entry is retired wherever it sits, under any event and any
# matcher. Matching is by path fragment, not by exact command, because
# the legacy entry was written with several different prefixes
# (bare path, ``bash <path>``, ``$HOME``-expanded) — but it stays a
# fragment of OUR retired shim's path, so a foreign hook can never
# match it. Retirement removes the one hook object; every sibling hook
# in the same entry and every unrelated entry survive untouched.
LEGACY_HOOK_FRAGMENTS: tuple[str, ...] = (".hermes/intake-poll-hook.sh",)


@dataclass(frozen=True, slots=True)
class ProfileHookReport:
    """What one profile's settings.json needed."""

    alias: str
    path: str
    installed: tuple[str, ...]
    repaired: tuple[str, ...]
    retired: tuple[str, ...] = ()

    @property
    def changed(self) -> bool:
        return bool(self.installed or self.repaired or self.retired)

    def as_dict(self) -> dict[str, object]:
        return {
            "alias": self.alias,
            "path": self.path,
            "installed": list(self.installed),
            "repaired": list(self.repaired),
            "retired": list(self.retired),
            "changed": self.changed,
        }


def _command_of(hook: object) -> str | None:
    """The command string of a ``{"type": "command"}`` hook, else None."""

    if isinstance(hook, dict) and hook.get("type") == "command":
        command = hook.get("command")
        if isinstance(command, str):
            return command
    return None


def _is_our_hook(hook: object, command: str) -> bool:
    return _command_of(hook) == command


def _is_legacy_hook(hook: object) -> bool:
    """A superseded Hermes shim this installer must retire."""

    command = _command_of(hook)
    if command is None:
        return False
    return any(
        fragment in command for fragment in LEGACY_HOOK_FRAGMENTS
    )


class HookInstaller:
    """Merge the Hermes hooks into each profile's settings.json."""

    def __init__(
        self,
        *,
        profiles: Mapping[str, Path],
        commands: HookCommandSet,
    ) -> None:
        self._profiles = dict(profiles)
        self._commands = commands

    def install(self) -> tuple[ProfileHookReport, ...]:
        reports = []
        for alias, config_dir in sorted(self._profiles.items()):
            reports.append(self._install_one(alias, config_dir))
        return tuple(reports)

    def _install_one(self, alias: str, config_dir: Path) -> ProfileHookReport:
        path = config_dir / "settings.json"
        settings: dict[str, object] = {}
        if path.exists():
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(loaded, dict):
                raise ValueError(f"{path} does not contain a settings object")
            settings = loaded
        hooks = settings.setdefault("hooks", {})
        if not isinstance(hooks, dict):
            raise ValueError(f"{path} carries a non-object hooks section")
        installed: list[str] = []
        repaired: list[str] = []
        # Cross-event sweep first: a Hermes command parked under any
        # event other than its home (e.g. a stale child-start left on
        # PreToolUse) is removed there and repaired into the canonical
        # binding below. The same single pass retires superseded Hermes
        # shims (INFRA-204) wherever they sit — one hook object at a
        # time, so unrelated hooks sharing the entry, and unrelated
        # entries under the same event, are never disturbed.
        home_events = {
            getattr(self._commands, attribute): event
            for event, _matcher, attribute in _HOOK_SPECS
        }
        crossed: set[str] = set()
        retired: list[str] = []
        for event_name, entries in list(hooks.items()):
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                entry_hooks = entry.get("hooks")
                if not isinstance(entry_hooks, list):
                    continue
                kept = []
                for hook in entry_hooks:
                    if _is_legacy_hook(hook):
                        if event_name not in retired:
                            retired.append(event_name)
                        continue
                    command = _command_of(hook)
                    home = home_events.get(command)
                    if home is not None and home != event_name:
                        crossed.add(home)
                        continue
                    kept.append(hook)
                entry["hooks"] = kept
            hooks[event_name] = [
                entry
                for entry in entries
                if not isinstance(entry, dict) or entry.get("hooks")
            ]
        # An event section left empty purely because its only content
        # was a retired shim is dropped, rather than published as a
        # bare `"Notification": []`. Only sections we emptied are
        # considered, and never one of our own home events.
        home_names = {event for event, _matcher, _attr in _HOOK_SPECS}
        for event_name in retired:
            if event_name in home_names:
                continue
            if hooks.get(event_name) == []:
                hooks.pop(event_name, None)
        for event, matcher, attribute in _HOOK_SPECS:
            command = getattr(self._commands, attribute)
            entries = hooks.setdefault(event, [])
            if not isinstance(entries, list):
                raise ValueError(
                    f"{path} carries a non-list {event} hook section"
                )
            # Canonicalize: strip every occurrence of our command from
            # this event — wherever and under whatever matcher it sits
            # — while counting exact-matcher hits, then bind exactly
            # one occurrence under the required matcher.
            exact = 0
            misplaced = 0
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                entry_hooks = entry.get("hooks")
                if not isinstance(entry_hooks, list):
                    continue
                kept = []
                for hook in entry_hooks:
                    if _is_our_hook(hook, command):
                        if entry.get("matcher") == matcher:
                            exact += 1
                        else:
                            misplaced += 1
                        continue
                    kept.append(hook)
                entry["hooks"] = kept
            hooks[event] = [
                entry
                for entry in entries
                if not isinstance(entry, dict) or entry.get("hooks")
            ]
            hooks[event].append(
                {
                    "matcher": matcher,
                    "hooks": [{"type": "command", "command": command}],
                }
            )
            if event in crossed:
                repaired.append(event)
            elif exact == 0 and misplaced == 0:
                installed.append(event)
            elif exact != 1 or misplaced > 0:
                repaired.append(event)
        report = ProfileHookReport(
            alias=alias,
            path=str(path),
            installed=tuple(installed),
            repaired=tuple(repaired),
            retired=tuple(retired),
        )
        if report.changed:
            config_dir.mkdir(parents=True, exist_ok=True)
            self._write_atomic(path, settings)
        return report

    @staticmethod
    def _write_atomic(path: Path, settings: dict[str, object]) -> None:
        """Validated temp file, fsync, atomic replace, directory fsync.

        The original settings.json stays complete and parseable through
        any interruption; its permissions survive the replacement.
        """

        payload = json.dumps(settings, indent=2, sort_keys=True) + "\n"
        json.loads(payload)  # the document must round-trip before it lands
        mode = path.stat().st_mode & 0o777 if path.exists() else 0o600
        descriptor, temporary = tempfile.mkstemp(
            prefix=".settings-", suffix=".tmp", dir=path.parent
        )
        try:
            os.write(descriptor, payload.encode("utf-8"))
            os.fsync(descriptor)
            os.fchmod(descriptor, mode)
        finally:
            os.close(descriptor)
        try:
            os.replace(temporary, path)
        except BaseException:
            os.unlink(temporary)
            raise
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
