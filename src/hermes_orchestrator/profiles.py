"""Opaque Claude subscription profiles, probes, cooldowns, and leases."""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from collections.abc import Callable, Iterable, Mapping
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


class ProfileBootstrap:
    """Deterministically establish pre-flight state before a profile seats.

    Sol correction a06cbce0: Hermes selected an authenticated profile
    (max-a) and Claude stopped first on the theme chooser and then on
    repository trust, because authentication alone said nothing about
    onboarding/theme/trust state. A profile admitted for managed
    rotation must reach only the hermes-control trust gate (when
    required) and then the restored session — never a first-run dialog.

    ``ensure`` reads ``<config_dir>/.claude.json`` (treating a missing
    file as ``{}``) and establishes exactly four non-secret keys when
    absent or not already satisfied: ``hasCompletedOnboarding`` (true),
    ``theme`` (left untouched when already set, otherwise ``"dark"``),
    ``bypassPermissionsModeAccepted`` (true — every managed launch uses
    ``--dangerously-skip-permissions``, so its one-time warning dialog
    must never appear either), and a trust entry for exactly the
    managed repository path(s) this instance was given. Every other key
    — including all credential/identity fields — is preserved
    untouched. Any read, parse, or write failure fails closed: the file
    is left exactly as found and the profile is reported not
    bootstrapped, never a crash.
    """

    def __init__(
        self,
        registry: ProfileRegistry,
        *,
        repo_paths: Iterable[Path],
    ) -> None:
        self._registry = registry
        self._repo_paths = tuple(str(path) for path in repo_paths)

    def ensure(self, alias: str) -> bool:
        """Idempotently establish onboarding/theme/trust state for alias.

        Returns whether ``.claude.json`` now satisfies every managed
        key. A missing file starts from ``{}``; unparseable JSON or any
        I/O failure fails closed without modifying the file.
        """

        config_dir = self._registry.get(alias).config_dir
        path = config_dir / ".claude.json"
        try:
            raw = path.read_text(encoding="utf-8") if path.exists() else ""
        except OSError:
            return False
        try:
            original: Any = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError:
            return False
        if not isinstance(original, dict):
            return False

        document = dict(original)
        if document.get("hasCompletedOnboarding") is not True:
            document["hasCompletedOnboarding"] = True
        if "theme" not in document:
            document["theme"] = "dark"
        if document.get("bypassPermissionsModeAccepted") is not True:
            document["bypassPermissionsModeAccepted"] = True

        existing_projects = original.get("projects")
        projects = (
            dict(existing_projects) if isinstance(existing_projects, dict) else {}
        )
        for repo_path in self._repo_paths:
            existing_entry = projects.get(repo_path)
            entry = dict(existing_entry) if isinstance(existing_entry, dict) else {}
            entry["hasTrustDialogAccepted"] = True
            projects[repo_path] = entry
        document["projects"] = projects

        if document == original:
            return True
        try:
            config_dir.mkdir(parents=True, exist_ok=True)
            self._write_atomic(path, document)
        except OSError:
            return False
        return True

    @staticmethod
    def _write_atomic(path: Path, document: dict[str, Any]) -> None:
        """Validated temp file, fsync, atomic replace, directory fsync.

        Mirrors ``hook_install.ProfileHookInstaller._write_atomic``: the
        original ``.claude.json`` stays complete and parseable through
        any interruption, and its permissions survive the replacement.
        """

        payload = json.dumps(document, indent=2, sort_keys=True) + "\n"
        json.loads(payload)  # the document must round-trip before it lands
        mode = path.stat().st_mode & 0o777 if path.exists() else 0o600
        descriptor, temporary = tempfile.mkstemp(
            prefix=".claude-", suffix=".tmp", dir=str(path.parent)
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


class ClaudeProfileProbe:
    """Verify that Claude CLI resolves to first-party subscription auth."""

    def __init__(
        self,
        registry: ProfileRegistry,
        command: JsonCommand | None = None,
        *,
        base_env: Mapping[str, str],
        now: Callable[[], datetime] | None = None,
        bootstrap: ProfileBootstrap | None = None,
    ) -> None:
        self._registry = registry
        self._command = command or SubprocessJsonCommand()
        self._base_env = base_env
        self._now = now or (lambda: datetime.now(UTC))
        self._bootstrap = bootstrap

    def check(self, alias: str) -> ProfileHealth:
        """Probe one profile and discard all account identity fields.

        Authentication alone is not enough to admit a profile for
        managed rotation (Sol correction a06cbce0): when this probe
        carries a :class:`ProfileBootstrap`, an authenticated profile is
        eligible only once its onboarding/theme/trust state is
        deterministically established, so no first-run dialog can ever
        surface in a restored session.
        """

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
        reason = "eligible" if eligible else "not_first_party_subscription"
        if eligible and self._bootstrap is not None:
            if self._bootstrap.ensure(alias):
                reason = "eligible"
            else:
                eligible = False
                reason = "bootstrap_incomplete"
        return ProfileHealth(
            profile_alias=alias,
            eligible=eligible,
            reason=reason,
            last_checked_at=self._now(),
        )


_DEVELOPMENT_LANE = "development"


@dataclass(frozen=True, slots=True)
class ProfileLease:
    """One (project, lane)-to-profile affinity lease.

    INFRA-219 L4: ``lane_role`` defaults to ``"development"`` so every
    call site that predates the dual-lane model keeps constructing (and
    reading) exactly the lease it always did.
    """

    project_key: str
    profile_alias: str
    acquired_at: datetime
    lane_role: str = _DEVELOPMENT_LANE


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
    """Lease healthy profile slots while preserving (project, lane) affinity.

    INFRA-219 L4: leases were keyed by ``project_key`` alone, so a
    harness cell dispatched alongside an active development cell for
    the same project could never hold its own lease -- ``acquire``
    returned the development lane's existing affinity verbatim, and the
    harness cell's durable insert then collided with it on
    ``profile_alias``. Every lease operation below is now keyed by
    ``(project_key, lane_role)``, with ``lane_role`` defaulting to
    ``"development"`` so every zero-argument call site that predates the
    dual-lane model keeps today's exact behavior. The profile-slot
    states (``_states``, keyed by alias) stay untouched: they are the
    shared, global resource limit -- one profile serves one lease at a
    time, across every project and lane -- while only the lease KEY
    gained a lane dimension.
    """

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
        self._leases: dict[tuple[str, str], ProfileLease] = {}
        self._replacement_reservations: dict[tuple[str, str], ProfileLease] = {}
        self._last_refusal: str | None = None

    @property
    def last_refusal(self) -> str | None:
        """Why the latest seating attempt refused its candidates.

        INFRA-205: written by first seating (:meth:`acquire`) as well as
        by :meth:`reserve_replacement`, since both now apply the same
        capacity rule and both owe the caller the evidence gap by name.
        """

        return self._last_refusal

    def acquire(
        self, project_key: str, lane_role: str = _DEVELOPMENT_LANE
    ) -> ProfileLease | None:
        """Return the existing affinity or lease the first available slot.

        INFRA-219 L4: keyed by ``(project_key, lane_role)`` -- a harness
        lane never inherits, and never blocks on, the development lane's
        existing affinity for the same project; it competes for a slot
        exactly as a distinct project would.

        INFRA-205: first seating applies the SAME ``_capacity_refusal``
        that ``reserve_replacement`` already applies, so one rule governs
        both paths. The live defect: rotation honoured a durable
        fable cap that first seating ignored, so a profile capped for
        another week was seated for a brand-new cell on auth health
        alone. Without a capacity-evidence port the helper returns None
        for every candidate, so an un-ported pool keeps its exact
        historical auth-health-only behavior.
        """

        key = (project_key, lane_role)
        existing = self._leases.get(key)
        if existing is not None:
            return existing

        now = self._now()
        refusals: list[str] = []
        for profile in self._registry.profiles:
            state = self._states[profile.alias]
            cooling_down = (
                state.cooldown_until is not None and state.cooldown_until > now
            )
            if state.eligible and not cooling_down and state.active_project_count == 0:
                refusal = self._durable_cap_refusal(profile.alias, now)
                if refusal is not None:
                    refusals.append(refusal)
                    continue
                lease = ProfileLease(project_key, profile.alias, now, lane_role)
                self._leases[key] = lease
                state.active_project_count += 1
                return lease
        if refusals:
            self._last_refusal = "; ".join(refusals)
        return None

    def restore(
        self,
        project_key: str,
        profile_alias: str,
        acquired_at: datetime,
        *,
        lane_role: str = _DEVELOPMENT_LANE,
        cooldown_until: datetime | None = None,
    ) -> ProfileLease:
        """Rehydrate one durable affinity before new admission begins."""

        self._registry.get(profile_alias)
        key = (project_key, lane_role)
        existing = self._leases.get(key)
        if existing is not None:
            if existing.profile_alias == profile_alias:
                return existing
            raise ValueError("project lane has conflicting durable profile leases")
        state = self._states[profile_alias]
        if state.active_project_count != 0:
            raise ValueError("profile has conflicting durable project leases")
        lease = ProfileLease(project_key, profile_alias, acquired_at, lane_role)
        self._leases[key] = lease
        state.active_project_count = 1
        state.cooldown_until = cooldown_until
        return lease

    def release(
        self, project_key: str, reason: str, *, lane_role: str = _DEVELOPMENT_LANE
    ) -> None:
        """Release a project lane's affinity with a reason for the journal."""

        if not reason:
            raise ValueError("profile release reason is required")
        lease = self._leases.pop((project_key, lane_role), None)
        if lease is not None:
            self._states[lease.profile_alias].active_project_count -= 1

    def reserve_replacement(
        self,
        project_key: str,
        exclude_alias: str,
        *,
        lane_role: str = _DEVELOPMENT_LANE,
    ) -> ProfileLease | None:
        """Reserve another healthy slot while preserving the current lease.

        With a capacity-evidence port injected, a candidate is refused
        unless it holds current, non-capped fable-capacity evidence: auth
        health alone once seated a profile whose weekly Fable budget was
        already exhausted. Each refusal is recorded on ``last_refusal``
        so the caller's failure message can name the evidence gap.
        """

        key = (project_key, lane_role)
        existing = self._leases.get(key)
        if existing is None or existing.profile_alias != exclude_alias:
            raise ValueError("project lane does not hold the excluded profile lease")
        reserved = self._replacement_reservations.get(key)
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
        replacement = ProfileLease(project_key, replacement_alias, now, lane_role)
        self._replacement_reservations[key] = replacement
        return replacement

    def _durable_cap_refusal(self, alias: str, now: datetime) -> str | None:
        """Refuse first seating ONLY on a live durable fable cap.

        INFRA-205: deliberately narrower than
        :meth:`_capacity_refusal`, which additionally refuses missing
        and stale evidence. That stricter bar is right when
        DELIBERATELY choosing a replacement, but wrong for first
        seating: a profile with no recent observation is
        eligible-to-probe, not known-bad, and refusing it would make an
        un-observed pool unseatable rather than merely cap-aware. A
        passed ``resets_at`` likewise means the budget cycled -- unknown
        again, so probe it.

        What it does refuse is the exact live defect: a profile whose
        newest fable observation says ``capped`` and whose horizon has
        NOT passed (or which carries no horizon at all, clearable only
        by a newer attestation) is never seated, however healthy its
        authentication looks.
        """

        if self._capacity_evidence is None:
            return None
        observation = self._capacity_evidence.latest(alias, "fable")
        if observation is None or observation.state != "capped":
            return None
        if observation.resets_at is None:
            return (
                f"{alias}: fable-capped with no reset horizon; only a "
                "newer capacity observation (operator attestation) clears it"
            )
        if observation.resets_at > now:
            return (
                f"{alias}: fable-capped until "
                f"{observation.resets_at.isoformat()}"
            )
        return None

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

    def reserve_context_only(
        self, project_key: str, *, lane_role: str = _DEVELOPMENT_LANE
    ) -> ProfileLease:
        """Permit a context-only (session-only) rotation on the same profile.

        Retaining the incumbent consumes no other profile's occupancy and
        needs no fresh capacity evidence: the seat is already paid for,
        only the session's context is being renewed.
        """

        existing = self._leases.get((project_key, lane_role))
        if existing is None:
            raise ValueError("project lane holds no profile lease to retain")
        return existing

    def cancel_replacement(
        self, project_key: str, *, lane_role: str = _DEVELOPMENT_LANE
    ) -> None:
        """Release an uncommitted replacement reservation."""

        reserved = self._replacement_reservations.pop((project_key, lane_role), None)
        if reserved is not None:
            self._states[reserved.profile_alias].active_project_count -= 1

    def commit_rotation(
        self,
        project_key: str,
        exclude_alias: str,
        *,
        lane_role: str = _DEVELOPMENT_LANE,
    ) -> ProfileLease:
        """Transfer affinity to the acknowledged reserved replacement."""

        key = (project_key, lane_role)
        existing = self._leases.get(key)
        reserved = self._replacement_reservations.get(key)
        if existing is None or existing.profile_alias != exclude_alias:
            raise ValueError("project lane does not hold the excluded profile lease")
        if reserved is None:
            raise ValueError("project lane has no replacement reservation")
        self._states[exclude_alias].active_project_count -= 1
        self._leases[key] = reserved
        del self._replacement_reservations[key]
        return reserved

    def rotate(
        self,
        project_key: str,
        exclude_alias: str,
        *,
        lane_role: str = _DEVELOPMENT_LANE,
    ) -> ProfileLease | None:
        """Reserve and immediately commit a replacement lease."""

        replacement = self.reserve_replacement(
            project_key, exclude_alias, lane_role=lane_role
        )
        if replacement is None:
            return None
        return self.commit_rotation(project_key, exclude_alias, lane_role=lane_role)

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
