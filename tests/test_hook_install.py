"""Idempotent Hermes hook installation (INFRA-195)."""

from __future__ import annotations

import json
from pathlib import Path

from hermes_orchestrator.hook_install import (
    HookCommandSet,
    HookInstaller,
)

COMMANDS = HookCommandSet(
    stop="uv run hermes-orchestrator intake-poll",
    subagent_stop="uv run hermes-orchestrator child-stop",
    child_start="uv run hermes-orchestrator child-start",
)


def installer(profiles: dict[str, Path]) -> HookInstaller:
    return HookInstaller(profiles=profiles, commands=COMMANDS)


def read_settings(config_dir: Path) -> dict[str, object]:
    return json.loads(
        (config_dir / "settings.json").read_text(encoding="utf-8")
    )


def commands_for(settings: dict[str, object], event: str) -> list[str]:
    return [
        hook["command"]
        for entry in settings.get("hooks", {}).get(event, [])
        for hook in entry.get("hooks", [])
        if hook.get("type") == "command"
    ]


def test_a_fresh_profile_gets_all_three_hooks(tmp_path: Path) -> None:
    config_dir = tmp_path / "max-a"
    config_dir.mkdir()

    [report] = installer({"max-a": config_dir}).install()

    assert report.installed == ("Stop", "SubagentStop", "PreToolUse")
    assert report.repaired == ()
    settings = read_settings(config_dir)
    assert commands_for(settings, "Stop") == [COMMANDS.stop]
    assert commands_for(settings, "SubagentStop") == [COMMANDS.subagent_stop]
    assert commands_for(settings, "PreToolUse") == [COMMANDS.child_start]
    [agent_entry] = settings["hooks"]["PreToolUse"]
    assert agent_entry["matcher"] == "Agent"


def test_installation_is_idempotent_and_preserves_foreign_hooks(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "max-b"
    config_dir.mkdir()
    (config_dir / "settings.json").write_text(
        json.dumps(
            {
                "theme": "auto",
                "hooks": {
                    "Stop": [
                        {
                            "matcher": "*",
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": "/usr/local/bin/presentation.sh",
                                }
                            ],
                        }
                    ]
                },
            }
        )
    )

    first = installer({"max-b": config_dir}).install()
    second = installer({"max-b": config_dir}).install()

    assert first[0].changed
    assert not second[0].changed
    settings = read_settings(config_dir)
    assert settings["theme"] == "auto"
    assert commands_for(settings, "Stop") == [
        "/usr/local/bin/presentation.sh",
        COMMANDS.stop,
    ]


def test_a_duplicated_hermes_hook_is_repaired_to_one(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "max-c"
    config_dir.mkdir()
    duplicated = {
        "hooks": {
            "SubagentStop": [
                {
                    "matcher": "*",
                    "hooks": [
                        {"type": "command", "command": COMMANDS.subagent_stop},
                        {"type": "command", "command": COMMANDS.subagent_stop},
                    ],
                }
            ]
        }
    }
    (config_dir / "settings.json").write_text(json.dumps(duplicated))

    [report] = installer({"max-c": config_dir}).install()

    assert "SubagentStop" in report.repaired
    settings = read_settings(config_dir)
    assert commands_for(settings, "SubagentStop") == [
        COMMANDS.subagent_stop
    ]


def test_a_missing_settings_file_is_created(tmp_path: Path) -> None:
    config_dir = tmp_path / "max-d"

    [report] = installer({"max-d": config_dir}).install()

    assert report.changed
    assert (config_dir / "settings.json").exists()
