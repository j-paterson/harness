"""Idempotent Hermes hook installation for classic Claude profiles.

INFRA-195: every eligible classic profile must run the Hermes Stop and
SubagentStop hooks (plus the PreToolUse Agent hook that counts child
starts) in addition to whatever presentation hooks it already carries.
Installation is a pure settings.json merge: foreign hooks are
preserved byte-for-byte in meaning, a missing Hermes hook is added
under its exact event and matcher, and a duplicated Hermes hook is
repaired down to one occurrence. Running the installer twice is a
no-op.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class HookCommandSet:
    """The exact hook command per lifecycle event."""

    stop: str
    subagent_stop: str
    child_start: str


# (settings event, matcher, HookCommandSet attribute)
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
            occurrences = 0
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                entry_hooks = entry.get("hooks")
                if not isinstance(entry_hooks, list):
                    continue
                kept = []
                for hook in entry_hooks:
                    if (
                        isinstance(hook, dict)
                        and hook.get("type") == "command"
                        and hook.get("command") == command
                    ):
                        occurrences += 1
                        if occurrences > 1:
                            # A duplicated Hermes hook would fire twice
                            # per boundary; repair down to one.
                            continue
                    kept.append(hook)
                entry["hooks"] = kept
            hooks[event] = [
                entry
                for entry in entries
                if not isinstance(entry, dict) or entry.get("hooks")
            ]
            if occurrences == 0:
                hooks[event].append(
                    {
                        "matcher": matcher,
                        "hooks": [{"type": "command", "command": command}],
                    }
                )
                installed.append(event)
            elif occurrences > 1:
                repaired.append(event)
        report = ProfileHookReport(
            alias=alias,
            path=str(path),
            installed=tuple(installed),
            repaired=tuple(repaired),
        )
        if report.changed:
            config_dir.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(settings, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        return report
