from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hermes_orchestrator.profiles import (
    ClaudeProfileProbe,
    ProfileBootstrap,
    ProfileHealth,
    ProfilePool,
    ProfileRegistry,
)


@pytest.fixture
def profile_config(tmp_path: Path) -> Path:
    path = tmp_path / "profiles.yaml"
    path.write_text(
        "profiles:\n"
        "  - alias: max-a\n"
        f"    config_dir: {tmp_path / '.claude-max-a'}\n"
        "  - alias: max-b\n"
        f"    config_dir: {tmp_path / '.claude-max-b'}\n"
        "  - alias: max-c\n"
        f"    config_dir: {tmp_path / '.claude-max-c'}\n"
        "  - alias: max-d\n"
        f"    config_dir: {tmp_path / '.claude-max-d'}\n",
        encoding="utf-8",
    )
    return path


@pytest.fixture
def registry(profile_config: Path) -> ProfileRegistry:
    return ProfileRegistry.load(profile_config)


class RecordingCommand:
    def __init__(self, result: dict[str, object]) -> None:
        self.result = result
        self.calls: list[tuple[list[str], dict[str, str]]] = []

    def run_json(self, command: list[str], env: dict[str, str]) -> dict[str, object]:
        self.calls.append((command, env))
        return self.result


def eligible_pool(
    registry: ProfileRegistry,
    *,
    now: datetime | None = None,
) -> ProfilePool:
    clock = (lambda: now) if now is not None else None
    pool = ProfilePool(registry, now=clock)
    checked_at = now or datetime(2026, 8, 26, tzinfo=UTC)
    for profile in registry.profiles:
        pool.record_health(
            ProfileHealth(
                profile_alias=profile.alias,
                eligible=True,
                reason="eligible",
                last_checked_at=checked_at,
            )
        )
    return pool


def test_launch_environment_scrubs_non_subscription_providers(
    registry: ProfileRegistry,
) -> None:
    env = registry.launch_env(
        "max-a",
        {
            "PATH": "/usr/bin",
            "CLAUDE_CODE_USE_BEDROCK": "1",
            "CLAUDE_CODE_USE_VERTEX": "1",
            "CLAUDE_CODE_USE_FOUNDRY": "1",
            "AWS_PROFILE": "work",
            "AWS_REGION": "us-east-1",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "key",
            "AWS_SECRET_ACCESS_KEY": "secret",
            "AWS_SESSION_TOKEN": "session",
            "ANTHROPIC_API_KEY": "secret",
            "ANTHROPIC_AUTH_TOKEN": "secret",
            "GOOGLE_APPLICATION_CREDENTIALS": "/tmp/google.json",
            "AZURE_CLIENT_SECRET": "secret",
        },
    )

    assert env["CLAUDE_CONFIG_DIR"].endswith(".claude-max-a")
    assert env["PATH"] == "/usr/bin"
    for key in (
        "CLAUDE_CODE_USE_BEDROCK",
        "CLAUDE_CODE_USE_VERTEX",
        "CLAUDE_CODE_USE_FOUNDRY",
        "AWS_PROFILE",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "AZURE_CLIENT_SECRET",
    ):
        assert key not in env


def test_registry_requires_four_opaque_unique_profiles(tmp_path: Path) -> None:
    path = tmp_path / "profiles.yaml"
    path.write_text(
        "profiles:\n  - alias: max-a\n    config_dir: /tmp/max-a\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="exactly four"):
        ProfileRegistry.load(path)


def test_probe_rejects_bedrock_even_when_logged_in(registry: ProfileRegistry) -> None:
    command = RecordingCommand(
        {"loggedIn": True, "authMethod": "third_party", "apiProvider": "bedrock"}
    )
    probe = ClaudeProfileProbe(registry, command, base_env={"PATH": "/usr/bin"})

    health = probe.check("max-a")

    assert health.eligible is False
    assert health.reason == "not_first_party_subscription"
    assert command.calls[0][0] == ["claude", "auth", "status", "--json"]
    assert "CLAUDE_CODE_USE_BEDROCK" not in command.calls[0][1]


def test_a_non_max_subscription_is_not_a_preserved_identity(
    registry: ProfileRegistry,
) -> None:
    """INFRA-195: the profile identity Hermes preserves is exactly a
    first-party Claude Max login; anything else is ineligible."""

    command = RecordingCommand(
        {
            "loggedIn": True,
            "authMethod": "claude.ai",
            "apiProvider": "firstParty",
            "subscriptionType": "pro",
        }
    )

    health = ClaudeProfileProbe(registry, command, base_env={}).check("max-a")

    assert health.eligible is False


def test_probe_keeps_only_redacted_health_state(registry: ProfileRegistry) -> None:
    command = RecordingCommand(
        {
            "loggedIn": True,
            "authMethod": "claude.ai",
            "apiProvider": "firstParty",
            "subscriptionType": "max",
            "account": {"email": "must-not-be-stored@example.com"},
        }
    )
    probe = ClaudeProfileProbe(registry, command, base_env={})

    health = probe.check("max-a")

    assert health.eligible is True
    assert health.reason == "eligible"
    assert "account" not in json.dumps(health.as_record())
    assert "email" not in json.dumps(health.as_record())


def test_project_affinity_survives_repeated_acquire(
    registry: ProfileRegistry,
) -> None:
    pool = eligible_pool(registry)

    first = pool.acquire("demo")
    second = pool.acquire("demo")

    assert first is not None
    assert second is not None
    assert second.profile_alias == first.profile_alias
    assert second.project_key == "demo"


def test_cooldown_profile_is_skipped(registry: ProfileRegistry) -> None:
    now = datetime(2026, 8, 26, tzinfo=UTC)
    pool = eligible_pool(registry, now=now)
    pool.set_cooldown("max-a", now + timedelta(hours=1))

    lease = pool.acquire("demo")

    assert lease is not None
    assert lease.profile_alias == "max-b"


def test_unprobed_profiles_are_not_eligible(registry: ProfileRegistry) -> None:
    pool = ProfilePool(registry)

    assert pool.acquire("demo") is None


def _authenticated_command() -> RecordingCommand:
    return RecordingCommand(
        {
            "loggedIn": True,
            "authMethod": "claude.ai",
            "apiProvider": "firstParty",
            "subscriptionType": "max",
        }
    )


def test_bootstrap_establishes_exactly_the_four_managed_keys(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    """INFRA-198 (Sol correction a06cbce0): an authenticated profile with
    no prior ``.claude.json`` must never surface the theme chooser, the
    onboarding flow, the bypass-permissions warning, or repository
    trust — bootstrap establishes all four deterministically."""

    config_dir = registry.get("max-a").config_dir
    repo = tmp_path / "repo"
    bootstrap = ProfileBootstrap(registry, repo_paths=[repo])

    assert bootstrap.ensure("max-a") is True

    document = json.loads((config_dir / ".claude.json").read_text(encoding="utf-8"))
    assert document == {
        "hasCompletedOnboarding": True,
        "theme": "dark",
        "bypassPermissionsModeAccepted": True,
        "projects": {str(repo): {"hasTrustDialogAccepted": True}},
    }


def test_bootstrap_preserves_an_existing_theme_choice(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    config_dir = registry.get("max-a").config_dir
    config_dir.mkdir(parents=True)
    (config_dir / ".claude.json").write_text(
        json.dumps({"theme": "light"}), encoding="utf-8"
    )
    bootstrap = ProfileBootstrap(registry, repo_paths=[tmp_path / "repo"])

    assert bootstrap.ensure("max-a") is True

    document = json.loads((config_dir / ".claude.json").read_text(encoding="utf-8"))
    assert document["theme"] == "light"


def test_bootstrap_preserves_unrelated_keys(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    config_dir = registry.get("max-a").config_dir
    config_dir.mkdir(parents=True)
    original = {
        "numStartups": 12,
        "oauthAccount": {"emailAddress": "must-not-be-touched@example.com"},
        "projects": {
            "/some/other/repo": {
                "hasTrustDialogAccepted": True,
                "allowedTools": ["Bash"],
            }
        },
    }
    (config_dir / ".claude.json").write_text(json.dumps(original), encoding="utf-8")
    repo = tmp_path / "repo"
    bootstrap = ProfileBootstrap(registry, repo_paths=[repo])

    assert bootstrap.ensure("max-a") is True

    document = json.loads((config_dir / ".claude.json").read_text(encoding="utf-8"))
    assert document["numStartups"] == 12
    assert document["oauthAccount"] == {
        "emailAddress": "must-not-be-touched@example.com"
    }
    assert document["projects"]["/some/other/repo"] == {
        "hasTrustDialogAccepted": True,
        "allowedTools": ["Bash"],
    }
    assert document["projects"][str(repo)] == {"hasTrustDialogAccepted": True}


def test_bootstrap_fails_closed_on_unparseable_json(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    config_dir = registry.get("max-a").config_dir
    config_dir.mkdir(parents=True)
    claude_json = config_dir / ".claude.json"
    claude_json.write_text("{not valid json", encoding="utf-8")
    bootstrap = ProfileBootstrap(registry, repo_paths=[tmp_path / "repo"])

    assert bootstrap.ensure("max-a") is False
    assert claude_json.read_text(encoding="utf-8") == "{not valid json"


def test_bootstrap_trusts_exactly_the_managed_repository_paths(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    config_dir = registry.get("max-a").config_dir
    repo_one = tmp_path / "repo-one"
    repo_two = tmp_path / "repo-two"
    bootstrap = ProfileBootstrap(registry, repo_paths=[repo_one, repo_two])

    assert bootstrap.ensure("max-a") is True

    document = json.loads((config_dir / ".claude.json").read_text(encoding="utf-8"))
    assert set(document["projects"]) == {str(repo_one), str(repo_two)}
    for repo in (repo_one, repo_two):
        assert document["projects"][str(repo)] == {"hasTrustDialogAccepted": True}


def test_bootstrap_is_idempotent(tmp_path: Path, registry: ProfileRegistry) -> None:
    repo = tmp_path / "repo"
    bootstrap = ProfileBootstrap(registry, repo_paths=[repo])

    assert bootstrap.ensure("max-a") is True
    claude_json = registry.get("max-a").config_dir / ".claude.json"
    first_write = claude_json.stat().st_mtime_ns
    first_document = claude_json.read_text(encoding="utf-8")

    assert bootstrap.ensure("max-a") is True
    assert claude_json.read_text(encoding="utf-8") == first_document
    # Nothing was left to establish, so the file is not rewritten.
    assert claude_json.stat().st_mtime_ns == first_write


def test_probe_is_not_eligible_until_bootstrap_completes(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    """An authenticated profile whose onboarding/theme/trust state is
    not yet established must not be reported eligible."""

    config_dir = registry.get("max-a").config_dir
    config_dir.mkdir(parents=True)
    (config_dir / ".claude.json").write_text("{not valid json", encoding="utf-8")
    bootstrap = ProfileBootstrap(registry, repo_paths=[tmp_path / "repo"])
    probe = ClaudeProfileProbe(
        registry, _authenticated_command(), base_env={}, bootstrap=bootstrap
    )

    health = probe.check("max-a")

    assert health.eligible is False
    assert health.reason == "bootstrap_incomplete"


def test_probe_is_eligible_once_bootstrap_completes(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    bootstrap = ProfileBootstrap(registry, repo_paths=[tmp_path / "repo"])
    probe = ClaudeProfileProbe(
        registry, _authenticated_command(), base_env={}, bootstrap=bootstrap
    )

    health = probe.check("max-a")

    assert health.eligible is True
    assert health.reason == "eligible"


def test_probe_without_bootstrap_keeps_prior_auth_only_behavior(
    registry: ProfileRegistry,
) -> None:
    """Construction sites that predate this defect fix (no ``bootstrap``
    supplied) must keep their historical auth-only eligibility."""

    probe = ClaudeProfileProbe(registry, _authenticated_command(), base_env={})

    health = probe.check("max-a")

    assert health.eligible is True
    assert health.reason == "eligible"


def test_two_lanes_of_one_project_hold_distinct_profiles(
    registry: ProfileRegistry,
) -> None:
    """INFRA-219 L4: leases are keyed by (project, lane).

    The contract requires each cell to hold a DISTINCT profile lease.
    Before L4 the pool was keyed by project alone, so a harness
    acquisition returned the development lane's lease verbatim and its
    durable insert collided on the profile-alias primary key.
    """

    pool = eligible_pool(registry)

    development = pool.acquire("demo")
    harness = pool.acquire("demo", "harness")

    assert development is not None and harness is not None
    assert development.profile_alias != harness.profile_alias
    # Each lane keeps its own affinity across repeated acquisitions.
    assert pool.acquire("demo").profile_alias == development.profile_alias
    assert (
        pool.acquire("demo", "harness").profile_alias == harness.profile_alias
    )


def test_a_harness_lane_never_steals_the_development_lease(
    registry: ProfileRegistry,
) -> None:
    """The alias limit stays global: no second profile, no harness lease.

    INFRA-219 L4: lane scoping changes only the lease KEY. One profile
    still serves one lease at a time, so when every other profile is
    ineligible the harness lane fails cleanly rather than sharing or
    stealing the development lane's seat.
    """

    pool = eligible_pool(registry)
    development = pool.acquire("demo")
    assert development is not None
    for profile in registry.profiles:
        if profile.alias != development.profile_alias:
            pool.record_health(
                ProfileHealth(
                    profile_alias=profile.alias,
                    eligible=False,
                    reason="auth expired",
                    last_checked_at=datetime(2026, 8, 26, tzinfo=UTC),
                )
            )

    harness = pool.acquire("demo", "harness")

    assert harness is None
    # The development lane is untouched by the refused acquisition.
    assert pool.acquire("demo").profile_alias == development.profile_alias
