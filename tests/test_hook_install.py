"""Atomic, matcher-exact Hermes hook installation (INFRA-195,
Sol correction 032cd4a5)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

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


def bindings_for(
    settings: dict[str, object], event: str
) -> list[tuple[str, str]]:
    return [
        (entry.get("matcher"), hook["command"])
        for entry in settings.get("hooks", {}).get(event, [])
        for hook in entry.get("hooks", [])
        if hook.get("type") == "command"
    ]


def test_a_fresh_profile_gets_all_three_exact_bindings(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "max-a"
    config_dir.mkdir()

    [report] = installer({"max-a": config_dir}).install()

    assert report.installed == ("Stop", "SubagentStop", "SubagentStart")
    assert report.repaired == ()
    settings = read_settings(config_dir)
    assert bindings_for(settings, "Stop") == [("*", COMMANDS.stop)]
    assert bindings_for(settings, "SubagentStop") == [
        ("*", COMMANDS.subagent_stop)
    ]
    assert bindings_for(settings, "SubagentStart") == [
        ("*", COMMANDS.child_start)
    ]


def test_a_command_under_the_wrong_event_or_matcher_is_repaired(
    tmp_path: Path,
) -> None:
    """A Hermes command parked under another event (a stale child-start
    on PreToolUse) or another matcher does not satisfy the
    requirement: it is moved into the one exact home binding."""

    config_dir = tmp_path / "max-b"
    config_dir.mkdir()
    (config_dir / "settings.json").write_text(
        json.dumps(
            {
                "hooks": {
                    "PreToolUse": [
                        {
                            "matcher": "Agent",
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": COMMANDS.child_start,
                                },
                                {
                                    "type": "command",
                                    "command": "/usr/local/bin/audit.sh",
                                },
                            ],
                        }
                    ],
                    "Stop": [
                        {
                            "matcher": "Bash",
                            "hooks": [
                                {"type": "command", "command": COMMANDS.stop}
                            ],
                        }
                    ],
                    "SubagentStop": [
                        {
                            "matcher": "Agent",
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": COMMANDS.subagent_stop,
                                }
                            ],
                        }
                    ],
                }
            }
        )
    )

    [report] = installer({"max-b": config_dir}).install()

    assert sorted(report.repaired) == [
        "Stop",
        "SubagentStart",
        "SubagentStop",
    ]
    settings = read_settings(config_dir)
    # The foreign PreToolUse hook survives; the stale Hermes one is gone.
    assert bindings_for(settings, "PreToolUse") == [
        ("Agent", "/usr/local/bin/audit.sh"),
    ]
    assert bindings_for(settings, "SubagentStart") == [
        ("*", COMMANDS.child_start)
    ]
    assert bindings_for(settings, "Stop") == [("*", COMMANDS.stop)]
    assert bindings_for(settings, "SubagentStop") == [
        ("*", COMMANDS.subagent_stop)
    ]


def test_duplicates_and_misplacements_collapse_to_one_binding(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "max-c"
    config_dir.mkdir()
    (config_dir / "settings.json").write_text(
        json.dumps(
            {
                "hooks": {
                    "SubagentStop": [
                        {
                            "matcher": "*",
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": COMMANDS.subagent_stop,
                                },
                                {
                                    "type": "command",
                                    "command": COMMANDS.subagent_stop,
                                },
                            ],
                        },
                        {
                            "matcher": "Agent",
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": COMMANDS.subagent_stop,
                                }
                            ],
                        },
                    ]
                }
            }
        )
    )

    [report] = installer({"max-c": config_dir}).install()

    assert "SubagentStop" in report.repaired
    settings = read_settings(config_dir)
    assert bindings_for(settings, "SubagentStop") == [
        ("*", COMMANDS.subagent_stop)
    ]


def test_interrupted_replacement_leaves_the_original_parseable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "max-d"
    config_dir.mkdir()
    original = {"theme": "auto", "hooks": {}}
    (config_dir / "settings.json").write_text(json.dumps(original))

    import os as os_module

    def refuse_replace(*_args: object, **_kwargs: object) -> None:
        raise OSError("interrupted during replacement")

    monkeypatch.setattr(
        "hermes_orchestrator.hook_install.os.replace", refuse_replace
    )
    with pytest.raises(OSError, match="interrupted"):
        installer({"max-d": config_dir}).install()
    monkeypatch.undo()

    # The original file is byte-complete and parseable, and no
    # temporary residue was published in its place.
    assert read_settings(config_dir) == original
    assert os_module.listdir(config_dir) == ["settings.json"]

    def refuse_write(*_args: object, **_kwargs: object) -> int:
        raise OSError("interrupted before replacement")

    monkeypatch.setattr(
        "hermes_orchestrator.hook_install.os.write", refuse_write
    )
    with pytest.raises(OSError, match="before replacement"):
        installer({"max-d": config_dir}).install()
    monkeypatch.undo()
    assert read_settings(config_dir) == original


def test_atomic_replacement_preserves_permissions_and_foreign_hooks(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "max-e"
    config_dir.mkdir()
    path = config_dir / "settings.json"
    path.write_text(
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
    path.chmod(0o600)

    first = installer({"max-e": config_dir}).install()
    second = installer({"max-e": config_dir}).install()

    assert first[0].changed
    assert not second[0].changed
    assert (path.stat().st_mode & 0o777) == 0o600
    settings = read_settings(config_dir)
    assert settings["theme"] == "auto"
    assert bindings_for(settings, "Stop") == [
        ("*", "/usr/local/bin/presentation.sh"),
        ("*", COMMANDS.stop),
    ]


def test_a_missing_settings_file_is_created_privately(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "max-f"

    [report] = installer({"max-f": config_dir}).install()

    assert report.changed
    path = config_dir / "settings.json"
    assert path.exists()
    assert (path.stat().st_mode & 0o777) == 0o600
