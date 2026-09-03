"""Atomic, matcher-exact Hermes hook installation (INFRA-195,
Sol correction 032cd4a5)."""

from __future__ import annotations

import dataclasses
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

# INFRA-215: the fourth, optional hook -- PreToolUse on the exact
# "Agent" matcher, running subagent-gate -- that gates every subagent
# launch through the durable packet ledger. Kept as a separate constant
# (rather than folded into COMMANDS) so every test that does not care
# about it keeps exercising the plain three-hook default unchanged.
COMMANDS_WITH_GATE = dataclasses.replace(
    COMMANDS,
    subagent_gate=(
        "uv run hermes-orchestrator subagent-gate --freeze-dir /var/hermes/freezes"
    ),
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


def test_a_fresh_profile_with_no_gate_command_gets_only_three_bindings(
    tmp_path: Path,
) -> None:
    """``subagent_gate`` defaults to ``None``: a caller that has not
    wired a real gate command (it needs a ``--freeze-dir``, unlike the
    other three) keeps installing exactly the prior three hooks."""

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
    assert "PreToolUse" not in settings.get("hooks", {})


def test_a_fresh_profile_gets_all_four_exact_bindings(
    tmp_path: Path,
) -> None:
    """INFRA-215: with a real gate command wired, a fresh classic
    profile installs all four hooks -- including the PreToolUse hook,
    matched exactly on ``Agent``, that runs ``subagent-gate``. Without
    it, classic seats never reach ``PacketAdmission.admit`` and packets
    never leave ``planned`` -- the observed live six-child cap
    failure."""

    config_dir = tmp_path / "max-a4"
    config_dir.mkdir()

    [report] = HookInstaller(
        profiles={"max-a4": config_dir}, commands=COMMANDS_WITH_GATE
    ).install()

    assert report.installed == (
        "Stop",
        "SubagentStop",
        "SubagentStart",
        "PreToolUse",
    )
    assert report.repaired == ()
    settings = read_settings(config_dir)
    assert bindings_for(settings, "Stop") == [("*", COMMANDS_WITH_GATE.stop)]
    assert bindings_for(settings, "SubagentStop") == [
        ("*", COMMANDS_WITH_GATE.subagent_stop)
    ]
    assert bindings_for(settings, "SubagentStart") == [
        ("*", COMMANDS_WITH_GATE.child_start)
    ]
    assert bindings_for(settings, "PreToolUse") == [
        ("Agent", COMMANDS_WITH_GATE.subagent_gate)
    ]


def test_the_subagent_gate_hook_reinstalls_idempotently(
    tmp_path: Path,
) -> None:
    """A second install with the same gate command changes nothing and
    never duplicates the PreToolUse binding."""

    config_dir = tmp_path / "max-a5"
    config_dir.mkdir()
    gated_installer = HookInstaller(
        profiles={"max-a5": config_dir}, commands=COMMANDS_WITH_GATE
    )

    first = gated_installer.install()[0]
    second = gated_installer.install()[0]

    assert first.changed
    assert not second.changed
    settings = read_settings(config_dir)
    for event in ("Stop", "SubagentStop", "SubagentStart", "PreToolUse"):
        assert len(bindings_for(settings, event)) == 1
    assert bindings_for(settings, "PreToolUse") == [
        ("Agent", COMMANDS_WITH_GATE.subagent_gate)
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


def test_a_legacy_shim_entry_is_retired_without_touching_neighbours(
    tmp_path: Path,
) -> None:
    """INFRA-204: the superseded ``.hermes/intake-poll-hook.sh`` entry
    is removed wherever a profile still references it — under our own
    events and under foreign ones — while every unrelated hook in the
    same profile (including one sharing the very same entry) survives,
    and exactly one hook per event remains."""

    config_dir = tmp_path / "max-g"
    config_dir.mkdir()
    legacy = "/Users/someone/.hermes/intake-poll-hook.sh"
    (config_dir / "settings.json").write_text(
        json.dumps(
            {
                "hooks": {
                    "Stop": [
                        {
                            "matcher": "*",
                            "hooks": [
                                {"type": "command", "command": legacy}
                            ],
                        }
                    ],
                    "PreToolUse": [
                        {
                            "matcher": "Bash",
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": f"bash {legacy}",
                                },
                                {
                                    "type": "command",
                                    "command": "/usr/local/bin/audit.sh",
                                },
                            ],
                        }
                    ],
                    "Notification": [
                        {
                            "matcher": "*",
                            "hooks": [
                                {"type": "command", "command": legacy}
                            ],
                        }
                    ],
                }
            }
        )
    )

    [report] = installer({"max-g": config_dir}).install()

    assert sorted(report.retired) == ["Notification", "PreToolUse", "Stop"]
    assert report.changed
    settings = read_settings(config_dir)
    # The unrelated hook that shared the legacy entry survives intact.
    assert bindings_for(settings, "PreToolUse") == [
        ("Bash", "/usr/local/bin/audit.sh"),
    ]
    # Nothing anywhere still references the retired shim.
    everything = [
        command
        for event in settings["hooks"]
        for _matcher, command in bindings_for(settings, event)
    ]
    assert not [c for c in everything if "intake-poll-hook.sh" in c]
    # Exactly one canonical hook per event, and the event whose only
    # content was the shim is gone rather than left empty.
    assert bindings_for(settings, "Stop") == [("*", COMMANDS.stop)]
    assert bindings_for(settings, "SubagentStop") == [
        ("*", COMMANDS.subagent_stop)
    ]
    assert bindings_for(settings, "SubagentStart") == [
        ("*", COMMANDS.child_start)
    ]
    assert "Notification" not in settings["hooks"]
    for event in settings["hooks"]:
        assert len(bindings_for(settings, event)) == 1

    # A second run finds nothing left to retire.
    [again] = installer({"max-g": config_dir}).install()
    assert not again.changed


def test_a_missing_settings_file_is_created_privately(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "max-f"

    [report] = installer({"max-f": config_dir}).install()

    assert report.changed
    path = config_dir / "settings.json"
    assert path.exists()
    assert (path.stat().st_mode & 0o777) == 0o600
