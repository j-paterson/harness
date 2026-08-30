"""Opaque Claude subscription profiles, probes, cooldowns, and leases."""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
            and result.get("subscriptionType") == "max"
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


# Fable budgets cycle weekly. A non-capped attestation older than one
# full cycle predates the budget window running now, so it says nothing
# about remaining capacity and no longer counts as current evidence.
CAPACITY_EVIDENCE_FRESHNESS = timedelta(days=7)


@dataclass(frozen=True, slots=True)
class CapacityObservation:
    """One durable, model-specific capacity fact about a profile.

    ``state`` is ``available`` (an operator attested remaining budget) or
    ``capped`` (the provider reported the budget exhausted). A capped
    observation with a ``resets_at`` horizon evidences availability again
    once that horizon passes; one without a horizon fails closed until a
    newer observation supersedes it.
    """

    profile_alias: str
    model: str
    state: str
    source: str
    observed_at: datetime
    resets_at: datetime | None = None
    detail: str | None = None


class CapacityEvidencePort(Protocol):
    """Read-side boundary for durable capacity observations."""

    def latest(
        self,
        profile_alias: str,
        model: str,
    ) -> CapacityObservation | None: ...


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
        capacity_evidence: CapacityEvidencePort | None = None,
    ) -> None:
        self._registry = registry
        self._now = now or (lambda: datetime.now(UTC))
        # Without a capacity-evidence port the pool keeps its historical
        # auth-health-only selection, so construction sites that predate
        # the INFRA-197-C2 wiring keep working unchanged.
        self._capacity_evidence = capacity_evidence
        self._states = {profile.alias: _PoolState() for profile in registry.profiles}
        self._leases: dict[str, ProfileLease] = {}
        self._replacement_reservations: dict[str, ProfileLease] = {}
        self._last_refusal: str | None = None

    @property
    def last_refusal(self) -> str | None:
        """Why the latest replacement reservation refused its candidates."""

        return self._last_refusal

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

    def restore(
        self,
        project_key: str,
        profile_alias: str,
        acquired_at: datetime,
        *,
        cooldown_until: datetime | None = None,
    ) -> ProfileLease:
        """Rehydrate one durable affinity before new admission begins."""

        self._registry.get(profile_alias)
        existing = self._leases.get(project_key)
        if existing is not None:
            if existing.profile_alias == profile_alias:
                return existing
            raise ValueError("project has conflicting durable profile leases")
        state = self._states[profile_alias]
        if state.active_project_count != 0:
            raise ValueError("profile has conflicting durable project leases")
        lease = ProfileLease(project_key, profile_alias, acquired_at)
        self._leases[project_key] = lease
        state.active_project_count = 1
        state.cooldown_until = cooldown_until
        return lease

    def release(self, project_key: str, reason: str) -> None:
        """Release a project affinity with a reason for the caller's journal."""

        if not reason:
            raise ValueError("profile release reason is required")
        lease = self._leases.pop(project_key, None)
        if lease is not None:
            self._states[lease.profile_alias].active_project_count -= 1

    def reserve_replacement(
        self,
        project_key: str,
        exclude_alias: str,
    ) -> ProfileLease | None:
        """Reserve another healthy slot while preserving the current lease.

        With a capacity-evidence port injected, a candidate is refused
        unless it holds current, non-capped fable-capacity evidence: auth
        health alone once seated a profile whose weekly Fable budget was
        already exhausted. Each refusal is recorded on ``last_refusal``
        so the caller's failure message can name the evidence gap.
        """

        existing = self._leases.get(project_key)
        if existing is None or existing.profile_alias != exclude_alias:
            raise ValueError("project does not hold the excluded profile lease")
        reserved = self._replacement_reservations.get(project_key)
        if reserved is not None:
            return reserved
        now = self._now()
        self._last_refusal = None
        refusals: list[str] = []
        replacement_alias = None
        for profile in self._registry.profiles:
            state = self._states[profile.alias]
            cooling_down = (
                state.cooldown_until is not None and state.cooldown_until > now
            )
            if (
                profile.alias != exclude_alias
                and state.eligible
                and not cooling_down
                and state.active_project_count == 0
            ):
                refusal = self._capacity_refusal(profile.alias, now)
                if refusal is not None:
                    refusals.append(refusal)
                    continue
                replacement_alias = profile.alias
                break
        if replacement_alias is None:
            if refusals:
                self._last_refusal = "; ".join(refusals)
            return None

        self._states[replacement_alias].active_project_count += 1
        replacement = ProfileLease(project_key, replacement_alias, now)
        self._replacement_reservations[project_key] = replacement
        return replacement

    def _capacity_refusal(self, alias: str, now: datetime) -> str | None:
        """Fail closed unless current fable-capacity evidence admits alias."""

        if self._capacity_evidence is None:
            return None
        observation = self._capacity_evidence.latest(alias, "fable")
        if observation is None:
            return f"{alias}: no current fable capacity evidence"
        if observation.state == "capped":
            if observation.resets_at is None:
                return (
                    f"{alias}: fable-capped with no reset horizon; only a "
                    "newer capacity observation (operator attestation) "
                    "clears it"
                )
            if observation.resets_at > now:
                return (
                    f"{alias}: fable-capped until "
                    f"{observation.resets_at.isoformat()}"
                )
            # The horizon passed: the budget cycled, so the observation
            # now evidences availability as of its reset time.
            effective_at = observation.resets_at
        else:
            effective_at = observation.observed_at
        if now - effective_at > CAPACITY_EVIDENCE_FRESHNESS:
            return (
                f"{alias}: fable capacity evidence is stale "
                f"(observed {effective_at.isoformat()})"
            )
        return None

    def reserve_context_only(self, project_key: str) -> ProfileLease:
        """Permit a context-only (session-only) rotation on the same profile.

        Retaining the incumbent consumes no other profile's occupancy and
        needs no fresh capacity evidence: the seat is already paid for,
        only the session's context is being renewed.
        """

        existing = self._leases.get(project_key)
        if existing is None:
            raise ValueError("project holds no profile lease to retain")
        return existing

    def cancel_replacement(self, project_key: str) -> None:
        """Release an uncommitted replacement reservation."""

        reserved = self._replacement_reservations.pop(project_key, None)
        if reserved is not None:
            self._states[reserved.profile_alias].active_project_count -= 1

    def commit_rotation(
        self,
        project_key: str,
        exclude_alias: str,
    ) -> ProfileLease:
        """Transfer affinity to the acknowledged reserved replacement."""

        existing = self._leases.get(project_key)
        reserved = self._replacement_reservations.get(project_key)
        if existing is None or existing.profile_alias != exclude_alias:
            raise ValueError("project does not hold the excluded profile lease")
        if reserved is None:
            raise ValueError("project has no replacement reservation")
        self._states[exclude_alias].active_project_count -= 1
        self._leases[project_key] = reserved
        del self._replacement_reservations[project_key]
        return reserved

    def rotate(
        self,
        project_key: str,
        exclude_alias: str,
    ) -> ProfileLease | None:
        """Reserve and immediately commit a replacement lease."""

        replacement = self.reserve_replacement(project_key, exclude_alias)
        if replacement is None:
            return None
        return self.commit_rotation(project_key, exclude_alias)

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
