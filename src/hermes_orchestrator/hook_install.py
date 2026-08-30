"""Idempotent, atomic Hermes hook installation for classic profiles.

INFRA-195 (hardened by Sol correction 032cd4a5): every eligible
classic profile must run the Hermes Stop and SubagentStop hooks (plus
the PreToolUse Agent hook that records child starts) in addition to
whatever presentation hooks it already carries. Each required hook
exists exactly once under its exact event *and matcher* — a Hermes
command found under any other matcher is misplaced and is repaired
into the one canonical binding, duplicates are collapsed, and foreign
hooks are preserved. The settings file is replaced atomically: the
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


# (settings event, exact matcher, HookCommandSet attribute)
_HOOK_SPECS: tuple[tuple[str, str, str], ...] = (
    ("Stop", "*", "stop"),
    ("SubagentStop", "*", "subagent_stop"),
    ("PreToolUse", "Agent", "child_start"),
)


@dataclass(frozen=True, slots=True)
class ProfileHookReport:
    """What one profile's settings.json needed."""

    alias: str
    path: str
    installed: tuple[str, ...]
    repaired: tuple[str, ...]

    @property
    def changed(self) -> bool:
        return bool(self.installed or self.repaired)

    def as_dict(self) -> dict[str, object]:
        return {
            "alias": self.alias,
            "path": self.path,
            "installed": list(self.installed),
            "repaired": list(self.repaired),
            "changed": self.changed,
        }


def _is_our_hook(hook: object, command: str) -> bool:
    return (
        isinstance(hook, dict)
        and hook.get("type") == "command"
        and hook.get("command") == command
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
            if exact == 0 and misplaced == 0:
                installed.append(event)
            elif exact != 1 or misplaced > 0:
                repaired.append(event)
        report = ProfileHookReport(
            alias=alias,
            path=str(path),
            installed=tuple(installed),
            repaired=tuple(repaired),
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
