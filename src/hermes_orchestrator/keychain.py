"""Read runtime secrets from macOS Keychain without exposing command output."""

from __future__ import annotations

import subprocess
from collections.abc import Callable


class Keychain:
    """Minimal generic-password reader for runtime-only credentials."""

    def __init__(
        self,
        *,
        runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self._runner = runner

    def read(self, service: str, account: str) -> str:
        """Return one secret without invoking a shell or emitting its value."""

        completed = self._runner(
            [
                "security",
                "find-generic-password",
                "-w",
                "-s",
                service,
                "-a",
                account,
            ],
            capture_output=True,
            check=True,
            text=True,
        )
        secret = completed.stdout.strip()
        if not secret:
            raise ValueError("keychain secret is empty")
        return secret
