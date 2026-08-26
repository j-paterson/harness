"""Opaque Claude subscription profiles, probes, cooldowns, and leases."""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import yaml

_PROFILE_COUNT = 4
_ALIAS_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_SCRUBBED_ENVIRONMENT_KEYS = frozenset(
    {
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "CLAUDE_CODE_USE_BEDROCK",
        "CLAUDE_CODE_USE_VERTEX",
        "CLAUDE_CODE_USE_FOUNDRY",
        "AWS_PROFILE",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "AZURE_CLIENT_SECRET",
    }
)


@dataclass(frozen=True, slots=True)
class ProfileConfig:
    """Non-secret configuration for one opaque Claude profile slot."""

    alias: str
    config_dir: Path


class ProfileRegistry:
    """Validated registry of exactly four Claude Max profile slots."""

    def __init__(self, profiles: tuple[ProfileConfig, ...]) -> None:
        self._profiles = profiles
        self._by_alias = {profile.alias: profile for profile in profiles}

    @classmethod
    def load(cls, path: Path) -> ProfileRegistry:
        """Load an identity-free YAML profile registry."""

        with path.open(encoding="utf-8") as stream:
            document = yaml.safe_load(stream)
        if not isinstance(document, dict) or set(document) != {"profiles"}:
            raise ValueError("profile configuration must contain only profiles")
        raw_profiles = document["profiles"]
        if not isinstance(raw_profiles, list) or len(raw_profiles) != _PROFILE_COUNT:
            raise ValueError("profile configuration must define exactly four profiles")

        profiles: list[ProfileConfig] = []
        for raw in raw_profiles:
            if not isinstance(raw, dict) or set(raw) != {"alias", "config_dir"}:
                raise ValueError("each profile requires only alias and config_dir")
            alias = raw["alias"]
            config_dir = raw["config_dir"]
            if not isinstance(alias, str) or not _ALIAS_PATTERN.fullmatch(alias):
                raise ValueError("profile alias must be opaque lowercase text")
            if not isinstance(config_dir, str) or not config_dir:
                raise ValueError("profile config_dir must be a non-empty path")
            resolved_dir = Path(config_dir).expanduser()
            if not resolved_dir.is_absolute():
                resolved_dir = path.parent / resolved_dir
            profiles.append(ProfileConfig(alias, resolved_dir.resolve(strict=False)))

        aliases = {profile.alias for profile in profiles}
        directories = {profile.config_dir for profile in profiles}
        if len(aliases) != _PROFILE_COUNT or len(directories) != _PROFILE_COUNT:
            raise ValueError("profile aliases and config directories must be unique")
        return cls(tuple(profiles))

    @property
    def profiles(self) -> tuple[ProfileConfig, ...]:
        """Return profiles in stable scheduling order."""

        return self._profiles

    def get(self, alias: str) -> ProfileConfig:
        """Return one profile or raise for an unknown opaque alias."""

        try:
            return self._by_alias[alias]
        except KeyError as error:
            raise ValueError(f"unknown profile alias: {alias}") from error

    def launch_env(self, alias: str, base: Mapping[str, str]) -> dict[str, str]:
        """Build a first-party-only child environment for a profile."""

        environment = {
            key: value
            for key, value in base.items()
            if key not in _SCRUBBED_ENVIRONMENT_KEYS
        }
        environment["CLAUDE_CONFIG_DIR"] = str(self.get(alias).config_dir)
        return environment


class JsonCommand(Protocol):
    """A subprocess boundary that returns a decoded JSON object."""

    def run_json(
        self,
        command: list[str],
        env: dict[str, str],
    ) -> dict[str, object]: ...


class SubprocessJsonCommand:
    """Run a command without exposing its captured output."""

    def run_json(
        self,
        command: list[str],
        env: dict[str, str],
    ) -> dict[str, object]:
        completed = subprocess.run(
            command,
            env=env,
            capture_output=True,
            check=True,
            text=True,
        )
        value = json.loads(completed.stdout)
        if not isinstance(value, dict):
            raise ValueError("profile probe returned a non-object JSON value")
        return value


@dataclass(frozen=True, slots=True)
class ProfileHealth:
    """Redacted eligibility state safe for persistence and display."""

    profile_alias: str
    eligible: bool
    reason: str
    last_checked_at: datetime
    cooldown_until: datetime | None = None
    active_project_count: int = 0

    def as_record(self) -> dict[str, Any]:
        """Return the intentionally identity-free persistence record."""

        return {
            "profile_alias": self.profile_alias,
            "eligible": self.eligible,
            "reason": self.reason,
            "last_checked_at": self.last_checked_at.isoformat(),
            "cooldown_until": (
                self.cooldown_until.isoformat()
                if self.cooldown_until is not None
                else None
            ),
            "active_project_count": self.active_project_count,
        }


class ClaudeProfileProbe:
    """Verify that Claude CLI resolves to first-party subscription auth."""

    def __init__(
        self,
        registry: ProfileRegistry,
        command: JsonCommand | None = None,
        *,
        base_env: Mapping[str, str],
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._registry = registry
        self._command = command or SubprocessJsonCommand()
        self._base_env = base_env
        self._now = now or (lambda: datetime.now(UTC))

    def check(self, alias: str) -> ProfileHealth:
        """Probe one profile and discard all account identity fields."""

        result = self._command.run_json(
            ["claude", "auth", "status", "--json"],
            self._registry.launch_env(alias, self._base_env),
        )
        eligible = (
            result.get("loggedIn") is True
            and result.get("authMethod") == "claude.ai"
            and result.get("apiProvider") == "firstParty"
        )
        return ProfileHealth(
            profile_alias=alias,
            eligible=eligible,
            reason="eligible" if eligible else "not_first_party_subscription",
            last_checked_at=self._now(),
        )


@dataclass(frozen=True, slots=True)
class ProfileLease:
    """One project-to-profile affinity lease."""

    project_key: str
    profile_alias: str
    acquired_at: datetime


@dataclass(slots=True)
class _PoolState:
    eligible: bool = False
    cooldown_until: datetime | None = None
    active_project_count: int = 0


class ProfilePool:
    """Lease healthy profile slots while preserving project affinity."""

    def __init__(
        self,
        registry: ProfileRegistry,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._registry = registry
        self._now = now or (lambda: datetime.now(UTC))
        self._states = {profile.alias: _PoolState() for profile in registry.profiles}
        self._leases: dict[str, ProfileLease] = {}

    def acquire(self, project_key: str) -> ProfileLease | None:
        """Return the existing affinity or lease the first available slot."""

        existing = self._leases.get(project_key)
        if existing is not None:
            return existing

        now = self._now()
        for profile in self._registry.profiles:
            state = self._states[profile.alias]
            cooling_down = (
                state.cooldown_until is not None and state.cooldown_until > now
            )
            if state.eligible and not cooling_down and state.active_project_count == 0:
                lease = ProfileLease(project_key, profile.alias, now)
                self._leases[project_key] = lease
                state.active_project_count += 1
                return lease
        return None

    def release(self, project_key: str, reason: str) -> None:
        """Release a project affinity with a reason for the caller's journal."""

        if not reason:
            raise ValueError("profile release reason is required")
        lease = self._leases.pop(project_key, None)
        if lease is not None:
            self._states[lease.profile_alias].active_project_count -= 1

    def set_cooldown(self, alias: str, cooldown_until: datetime | None) -> None:
        """Prevent new leases on a capped profile until the supplied time."""

        self._registry.get(alias)
        self._states[alias].cooldown_until = cooldown_until

    def record_health(self, health: ProfileHealth) -> None:
        """Update scheduling eligibility from a redacted probe result."""

        self._registry.get(health.profile_alias)
        state = self._states[health.profile_alias]
        state.eligible = health.eligible
        state.cooldown_until = health.cooldown_until
