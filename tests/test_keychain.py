from __future__ import annotations

import subprocess

import pytest

from hermes_orchestrator.keychain import Keychain


class RecordingRunner:
    def __init__(self, stdout: str) -> None:
        self.stdout = stdout
        self.calls: list[dict[str, object]] = []

    def __call__(
        self,
        command: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append({"command": command, **kwargs})
        return subprocess.CompletedProcess(command, 0, stdout=self.stdout, stderr="")


def test_keychain_reads_secret_without_shell_or_logging() -> None:
    runner = RecordingRunner("linear-secret\n")
    keychain = Keychain(runner=runner)

    value = keychain.read("hermes-orchestrator-linear", "default")

    assert value == "linear-secret"
    assert runner.calls == [
        {
            "command": [
                "security",
                "find-generic-password",
                "-w",
                "-s",
                "hermes-orchestrator-linear",
                "-a",
                "default",
            ],
            "capture_output": True,
            "check": True,
            "text": True,
        }
    ]


def test_keychain_rejects_empty_secret() -> None:
    keychain = Keychain(runner=RecordingRunner("\n"))

    with pytest.raises(ValueError, match="empty"):
        keychain.read("hermes-orchestrator-linear", "default")
