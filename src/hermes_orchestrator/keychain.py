"""Read runtime secrets from macOS Keychain without exposing command output."""

from __future__ import annotations

import subprocess
from collections.abc import Callable


class KeychainItemExists(Exception):
    """An item already exists; writes never overwrite existing credentials."""


class KeychainItemMissing(Exception):
    """A read proved the item absent; the only failure treated as absence."""


class KeychainReadError(Exception):
    """A read failed for any reason other than definite absence."""


class KeychainWriteError(Exception):
    """A write failed; the message never carries command output or secrets."""


# macOS ``security`` exits 44 (errSecItemNotFound) only when the item is
# definitely absent; every other failure (permission, interaction, ...) must
# fail closed rather than read as absence.
_ERR_SEC_ITEM_NOT_FOUND = 44


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

    def read_classified(self, service: str, account: str) -> str:
        """Read one secret, distinguishing definite absence from other failures."""

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
            check=False,
            text=True,
        )
        if completed.returncode == _ERR_SEC_ITEM_NOT_FOUND:
            raise KeychainItemMissing("keychain item not found")
        if completed.returncode != 0:
            # Messages never carry argv, command output, or secret material.
            raise KeychainReadError("keychain read failed")
        secret = completed.stdout.strip()
        if not secret:
            raise KeychainReadError("keychain secret is empty")
        return secret

    def create(self, service: str, account: str, secret: str) -> None:
        """Create one item, failing closed if it exists; never update in place."""

        probe = self._runner(
            [
                "security",
                "find-generic-password",
                "-s",
                service,
                "-a",
                account,
            ],
            capture_output=True,
            check=False,
            text=True,
        )
        if probe.returncode == 0:
            raise KeychainItemExists("keychain item already exists")
        added = self._runner(
            [
                "security",
                "add-generic-password",
                "-s",
                service,
                "-a",
                account,
                "-w",
                secret,
            ],
            capture_output=True,
            check=False,
            text=True,
        )
        if added.returncode != 0:
            # Without -U, add refuses duplicates even if one appeared after the
            # probe; surface a message that carries no argv, output, or secret.
            raise KeychainWriteError("keychain add failed")
