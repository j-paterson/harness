"""Provider routes: first-party Claude Max by default, Bedrock by opt-in.

INFRA-192: an ordinary ``claude`` launch inherited Bedrock provider
selection from the shell environment outside the personal project
tree, so which provider answered depended on the current working
directory. This module makes the route an explicit, named input and
never a property of ``$PWD``:

- ``default`` and ``max-a`` .. ``max-d`` are first-party Claude Max
  routes. Their child environment is the caller's environment with
  every provider-selection and provider-credential variable removed
  and exactly one ``CLAUDE_CONFIG_DIR`` set.
- ``bedrock`` is the only route that enables Bedrock. It is optional
  metadata in ``config/providers.yaml``; when absent the route does
  not exist and every request for it fails closed. It is never one of
  the four Max subscription slots and never counts as one.

Nothing here reads, stores, or prints a credential. The Bedrock route
carries only an AWS profile name and region — selection, not secrets —
and the AWS credential chain (SSO, ``~/.aws``) supplies the rest. The
probe report keeps provider/auth-method facts and discards every
identity field, matching :class:`~hermes_orchestrator.profiles.ProfileHealth`.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml

if TYPE_CHECKING:
    from hermes_orchestrator.profiles import ProfileRegistry

FIRST_PARTY_MAX = "first_party_max"
BEDROCK = "bedrock"

DEFAULT_ROUTE = "default"
BEDROCK_ROUTE = "bedrock"

# Variables that select Amazon Bedrock as the provider or configure it.
# ``AWS_BEARER_TOKEN_BEDROCK`` is a credential AND a selector: Claude
# Code treats its presence as Bedrock intent, so it is scrubbed here.
BEDROCK_SELECTION_KEYS = frozenset(
    {
        "CLAUDE_CODE_USE_BEDROCK",
        "CLAUDE_CODE_SKIP_BEDROCK_AUTH",
        "AWS_PROFILE",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_BEARER_TOKEN_BEDROCK",
        "ANTHROPIC_BEDROCK_BASE_URL",
    }
)

# Static AWS credentials outrank a named profile in the SDK credential
# chain; a route that names a profile must not let inherited statics
# silently decide which account answers.
AWS_CREDENTIAL_KEYS = frozenset(
    {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}
)

OTHER_PROVIDER_KEYS = frozenset(
    {
        "CLAUDE_CODE_USE_VERTEX",
        "CLAUDE_CODE_SKIP_VERTEX_AUTH",
        "CLAUDE_CODE_USE_FOUNDRY",
        "CLAUDE_CODE_SKIP_FOUNDRY_AUTH",
        "ANTHROPIC_VERTEX_BASE_URL",
        "ANTHROPIC_FOUNDRY_BASE_URL",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "AZURE_CLIENT_SECRET",
    }
)

# Anything that would override a subscription login with a token or
# redirect first-party traffic through a proxy.
FIRST_PARTY_OVERRIDE_KEYS = frozenset(
    {"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL"}
)

# A first-party Max child environment carries none of these.
MAX_ROUTE_SCRUBBED_KEYS = (
    BEDROCK_SELECTION_KEYS
    | AWS_CREDENTIAL_KEYS
    | OTHER_PROVIDER_KEYS
    | FIRST_PARTY_OVERRIDE_KEYS
)

# The Bedrock child environment starts from the same clean slate and
# then re-adds exactly the configured selection.
BEDROCK_ROUTE_SCRUBBED_KEYS = MAX_ROUTE_SCRUBBED_KEYS

_ROUTE_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_AWS_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_AWS_REGION_PATTERN = re.compile(r"^[a-z]{2}(-[a-z]+)+-\d$")
# A value shaped like an AWS access key id (or any long shouting token)
# is a credential, not a profile name, and is refused as such.
_CREDENTIAL_SHAPED_PATTERN = re.compile(
    r"^(?:AKIA|ASIA|A3T|AROA|AIDA)[A-Z0-9]{12,}$|^[A-Z0-9]{20,}$"
)
_PROVIDERS_KEYS = frozenset({"default_max_alias", "bedrock", "claude_executable"})
_BEDROCK_KEYS = frozenset({"config_dir", "aws_profile", "aws_region"})
_AUTH_STATUS_COMMAND = ("auth", "status", "--json")
_MAX_EXPECTED = {
    "authMethod": "claude.ai",
    "apiProvider": "firstParty",
    "subscriptionType": "max",
}
_CAPACITY_RESET_PATTERN = re.compile(
    r"resets?\s+(?P<clock>\d{1,2}:\d{2}\s*(?:am|pm))\s*"
    r"\((?P<zone>[^)]+)\)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ProviderRoute:
    """One named, identity-free launch route."""

    name: str
    kind: str
    config_dir: Path
    aws_profile: str | None = None
    aws_region: str | None = None

    @property
    def subscription(self) -> bool:
        """Whether the route is a Claude Max subscription slot."""

        return self.kind == FIRST_PARTY_MAX

    def as_metadata(self) -> dict[str, Any]:
        """Return the non-secret provider metadata for this route."""

        return {
            "route": self.name,
            "kind": self.kind,
            "subscription": self.subscription,
            "config_dir": str(self.config_dir),
            "aws_profile": self.aws_profile,
            "aws_region": self.aws_region,
        }


class ProviderRoutes:
    """The closed set of launch routes for one operator machine."""

    def __init__(
        self,
        registry: ProfileRegistry,
        *,
        default_max_alias: str,
        bedrock: ProviderRoute | None = None,
        claude_executable: str | None = None,
    ) -> None:
        registry.get(default_max_alias)
        self._registry = registry
        self._default_max_alias = default_max_alias
        self._claude_executable = claude_executable
        if bedrock is not None:
            if bedrock.kind != BEDROCK or bedrock.name != BEDROCK_ROUTE:
                raise ValueError("the bedrock route must be the bedrock route")
            max_dirs = {profile.config_dir for profile in registry.profiles}
            if bedrock.config_dir in max_dirs:
                raise ValueError(
                    "the bedrock config_dir must not be a Claude Max profile config_dir"
                )
        self._bedrock = bedrock

    @classmethod
    def load(cls, path: Path, registry: ProfileRegistry) -> ProviderRoutes:
        """Load ``config/providers.yaml`` — selection metadata, no secrets."""

        with path.open(encoding="utf-8") as stream:
            document = yaml.safe_load(stream)
        if not isinstance(document, dict) or not document:
            raise ValueError("provider configuration must be a mapping")
        unknown = set(document) - _PROVIDERS_KEYS
        if unknown:
            raise ValueError(
                "provider configuration contains unsupported keys: "
                + ", ".join(sorted(unknown))
            )
        default_alias = document.get("default_max_alias")
        if not isinstance(default_alias, str) or not default_alias:
            raise ValueError("provider configuration requires default_max_alias")
        executable = document.get("claude_executable")
        if executable is not None and (
            not isinstance(executable, str) or not executable
        ):
            raise ValueError("claude_executable must be a non-empty path")

        bedrock: ProviderRoute | None = None
        raw_bedrock = document.get("bedrock")
        if raw_bedrock is not None:
            if not isinstance(raw_bedrock, dict) or "config_dir" not in raw_bedrock:
                raise ValueError("bedrock route requires config_dir")
            unknown = set(raw_bedrock) - _BEDROCK_KEYS
            if unknown:
                raise ValueError(
                    "bedrock route contains unsupported keys: "
                    + ", ".join(sorted(unknown))
                )
            config_dir = raw_bedrock["config_dir"]
            if not isinstance(config_dir, str) or not config_dir:
                raise ValueError("bedrock config_dir must be a non-empty path")
            resolved = Path(config_dir).expanduser()
            if not resolved.is_absolute():
                resolved = path.parent / resolved
            aws_profile = _optional_name(raw_bedrock, "aws_profile", _AWS_NAME_PATTERN)
            aws_region = _optional_name(raw_bedrock, "aws_region", _AWS_REGION_PATTERN)
            bedrock = ProviderRoute(
                name=BEDROCK_ROUTE,
                kind=BEDROCK,
                config_dir=resolved.resolve(strict=False),
                aws_profile=aws_profile,
                aws_region=aws_region,
            )
        return cls(
            registry,
            default_max_alias=default_alias,
            bedrock=bedrock,
            claude_executable=executable,
        )

    @property
    def default_max_alias(self) -> str:
        return self._default_max_alias

    @property
    def claude_executable(self) -> str | None:
        return self._claude_executable

    @property
    def names(self) -> tuple[str, ...]:
        """Every launchable route name, in stable order."""

        names = [DEFAULT_ROUTE, *(p.alias for p in self._registry.profiles)]
        if self._bedrock is not None:
            names.append(BEDROCK_ROUTE)
        return tuple(names)

    @property
    def bedrock_configured(self) -> bool:
        return self._bedrock is not None

    def route(self, name: str) -> ProviderRoute:
        """Resolve one route by name; unknown or unconfigured fails closed."""

        if name == BEDROCK_ROUTE:
            if self._bedrock is None:
                raise ValueError(
                    "the bedrock route is not configured; add a bedrock "
                    "section to config/providers.yaml to opt in"
                )
            return self._bedrock
        alias = self._default_max_alias if name == DEFAULT_ROUTE else name
        if name != DEFAULT_ROUTE and not _ROUTE_NAME_PATTERN.fullmatch(name):
            raise ValueError(f"unknown provider route: {name}")
        profile = self._registry.get(alias)
        return ProviderRoute(
            name=name, kind=FIRST_PARTY_MAX, config_dir=profile.config_dir
        )

    def metadata(self) -> tuple[dict[str, Any], ...]:
        """Non-secret metadata for every route, Bedrock marked optional."""

        return tuple(self.route(name).as_metadata() for name in self.names)

    def launch_env(self, name: str, base: Mapping[str, str]) -> dict[str, str]:
        """Build the child environment for a route.

        The result is a pure function of ``name`` and ``base``: the
        current working directory is never consulted, so the same
        route yields the same provider from every directory.
        """

        route = self.route(name)
        if route.kind == FIRST_PARTY_MAX:
            environment = {
                key: value
                for key, value in base.items()
                if key not in MAX_ROUTE_SCRUBBED_KEYS
            }
            environment["CLAUDE_CONFIG_DIR"] = str(route.config_dir)
            return environment
        environment = {
            key: value
            for key, value in base.items()
            if key not in BEDROCK_ROUTE_SCRUBBED_KEYS
        }
        environment["CLAUDE_CONFIG_DIR"] = str(route.config_dir)
        environment["CLAUDE_CODE_USE_BEDROCK"] = "1"
        if route.aws_profile is not None:
            environment["AWS_PROFILE"] = route.aws_profile
        if route.aws_region is not None:
            environment["AWS_REGION"] = route.aws_region
            environment["AWS_DEFAULT_REGION"] = route.aws_region
        return environment


def _optional_name(
    document: Mapping[str, Any], key: str, pattern: re.Pattern[str]
) -> str | None:
    value = document.get(key)
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not pattern.fullmatch(value)
        or _CREDENTIAL_SHAPED_PATTERN.fullmatch(value)
    ):
        raise ValueError(f"bedrock {key} must be a plain AWS name")
    return value


@dataclass(frozen=True, slots=True)
class RouteVerdict:
    """The fail-closed classification of one ``auth status`` report."""

    ok: bool
    reason: str
    reported_provider: str | None
    reported_auth_method: str | None


def classify(route: ProviderRoute, status: Mapping[str, object]) -> RouteVerdict:
    """Decide whether a route's live report matches its declared kind.

    A Max route that reports Bedrock (or any non-first-party provider)
    and a Bedrock route that reports a subscription login are both
    refused: the launcher never proceeds on a route whose provider
    disagrees with its name.
    """

    provider = status.get("apiProvider")
    auth_method = status.get("authMethod")
    provider_text = provider if isinstance(provider, str) else None
    auth_text = auth_method if isinstance(auth_method, str) else None
    logged_in = status.get("loggedIn") is True
    subscription = status.get("subscriptionType")

    if route.kind == FIRST_PARTY_MAX:
        if provider_text == BEDROCK:
            reason = "max_route_reports_bedrock"
        elif provider_text != _MAX_EXPECTED["apiProvider"]:
            reason = f"max_route_reports_{provider_text or 'unknown'}"
        elif not logged_in:
            reason = "not_logged_in"
        elif auth_text != _MAX_EXPECTED["authMethod"]:
            reason = "not_subscription_auth"
        elif subscription != _MAX_EXPECTED["subscriptionType"]:
            reason = "not_max_subscription"
        else:
            reason = "first_party_max"
        return RouteVerdict(
            ok=reason == "first_party_max",
            reason=reason,
            reported_provider=provider_text,
            reported_auth_method=auth_text,
        )

    if (
        provider_text == _MAX_EXPECTED["apiProvider"]
        or auth_text == _MAX_EXPECTED["authMethod"]
        or subscription is not None
    ):
        reason = "bedrock_route_reports_subscription"
    elif provider_text != BEDROCK:
        reason = f"bedrock_route_reports_{provider_text or 'unknown'}"
    elif not logged_in:
        reason = "not_logged_in"
    else:
        reason = "bedrock"
    return RouteVerdict(
        ok=reason == "bedrock",
        reason=reason,
        reported_provider=provider_text,
        reported_auth_method=auth_text,
    )


class JsonCommand(Protocol):
    def run_json(
        self, command: list[str], env: dict[str, str]
    ) -> dict[str, object]: ...


class SubprocessJsonCommand:
    """Run ``claude auth status --json`` without exposing its output."""

    def run_json(self, command: list[str], env: dict[str, str]) -> dict[str, object]:
        completed = subprocess.run(
            command, env=env, capture_output=True, check=True, text=True
        )
        value = json.loads(completed.stdout)
        if not isinstance(value, dict):
            raise ValueError("auth status returned a non-object JSON value")
        return value


@dataclass(frozen=True, slots=True)
class RouteHealth:
    """Identity-free probe outcome for one route."""

    route: str
    kind: str
    ok: bool
    reason: str
    reported_provider: str | None
    reported_auth_method: str | None

    def as_record(self) -> dict[str, Any]:
        return {
            "route": self.route,
            "kind": self.kind,
            "ok": self.ok,
            "reason": self.reason,
            "reported_provider": self.reported_provider,
            "reported_auth_method": self.reported_auth_method,
        }


@dataclass(frozen=True, slots=True)
class FableCapacity:
    """Identity-free result of one bounded Fable inference probe."""

    state: str
    resets_at: datetime | None
    detail: str


def parse_fable_capacity(output: str, *, now: datetime) -> FableCapacity:
    """Classify a bounded probe without inventing a reset horizon."""

    text = output.strip()
    if text == "CAPACITY_OK":
        return FableCapacity(
            state="available",
            resets_at=None,
            detail="bounded provider Fable probe returned CAPACITY_OK",
        )
    if "limit" not in text.lower():
        raise ValueError("Fable capacity probe returned an unrecognized response")
    match = _CAPACITY_RESET_PATTERN.search(text)
    if match is None:
        raise ValueError("Fable limit response did not include an exact reset time")
    try:
        zone = ZoneInfo(match.group("zone"))
    except ZoneInfoNotFoundError as error:
        raise ValueError("Fable limit response named an unknown timezone") from error
    clock = datetime.strptime(
        match.group("clock").replace(" ", "").lower(), "%I:%M%p"
    ).time()
    local_now = now.astimezone(zone)
    reset = datetime.combine(local_now.date(), clock, tzinfo=zone)
    if reset <= local_now:
        reset += timedelta(days=1)
    return FableCapacity(
        state="capped",
        resets_at=reset.astimezone(UTC),
        detail="bounded provider Fable probe reported a limit with exact reset",
    )


def resolve_executable(
    routes: ProviderRoutes, *, which: Callable[[str], str | None] = shutil.which
) -> str:
    """The Claude binary a route launches: configured, else on PATH."""

    configured = routes.claude_executable
    if configured is not None:
        return str(Path(configured).expanduser())
    found = which("claude")
    if found is None:
        raise ValueError("claude executable not found on PATH")
    return found


class ProviderProbe:
    """Live, non-inference ``auth status`` probe per route."""

    def __init__(
        self,
        routes: ProviderRoutes,
        command: JsonCommand | None = None,
        *,
        base_env: Mapping[str, str],
        executable: str = "claude",
    ) -> None:
        self._routes = routes
        self._command = command or SubprocessJsonCommand()
        self._base_env = base_env
        self._executable = executable

    def check(self, name: str) -> RouteHealth:
        route = self._routes.route(name)
        environment = self._routes.launch_env(name, self._base_env)
        try:
            status = self._command.run_json(
                [self._executable, *_AUTH_STATUS_COMMAND], environment
            )
        except (OSError, subprocess.CalledProcessError, ValueError):
            return RouteHealth(
                route=name,
                kind=route.kind,
                ok=False,
                reason="probe_failed",
                reported_provider=None,
                reported_auth_method=None,
            )
        verdict = classify(route, status)
        return RouteHealth(
            route=name,
            kind=route.kind,
            ok=verdict.ok,
            reason=verdict.reason,
            reported_provider=verdict.reported_provider,
            reported_auth_method=verdict.reported_auth_method,
        )

    def check_all(self) -> tuple[RouteHealth, ...]:
        return tuple(self.check(name) for name in self._routes.names)


def launch(
    routes: ProviderRoutes,
    name: str,
    arguments: Sequence[str],
    *,
    base_env: Mapping[str, str],
    executable: str,
    probe: ProviderProbe,
    execve: Callable[[str, list[str], dict[str, str]], Any] | None = None,
) -> tuple[int, str]:
    """Probe the route, then replace this process with Claude.

    Returns ``(exit_code, message)`` only when the launch is refused;
    a successful ``execve`` never returns. The message names the route
    and the reason and nothing else — never an environment fragment.
    """

    try:
        health = probe.check(name)
    except ValueError as error:
        return 2, f"claude-launch refused: {error}"
    if not health.ok:
        return 3, (
            f"claude-launch refused: route {name!r} ({health.kind}) "
            f"reported {health.reported_provider or 'no provider'} "
            f"({health.reason}); fix the route before launching"
        )
    environment = routes.launch_env(name, base_env)
    # Resolved per call, so a test may substitute ``os.execve`` and a
    # real launch never binds a stale reference at import time.
    replace = os.execve if execve is None else execve
    replace(executable, [executable, *arguments], environment)
    return 0, ""


def shell_init(routes: ProviderRoutes, launcher_command: Sequence[str]) -> str:
    """Render the zsh/bash functions that route every launch here.

    Every function forwards to ``claude-launch`` with a fixed route, so
    ``$PWD`` never participates. The snippet carries the launcher path
    and route names only; there is nothing secret to leak.
    """

    prefix = shlex.join(list(launcher_command))
    lines = [
        "# Generated by `hermes-orchestrator claude-shell-init` (INFRA-192).",
        "# First-party Claude Max by default; Bedrock only via claude-bedrock.",
        "# Contains no credentials. Regenerate rather than edit.",
    ]
    for name in routes.names:
        function = "claude" if name == DEFAULT_ROUTE else f"claude-{name}"
        lines.append(
            f"{function}() {{ {prefix} claude-launch --route "
            f'{shlex.quote(name)} -- "$@"; }}'
        )
    return "\n".join(lines) + "\n"
