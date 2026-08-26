from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hermes_orchestrator.profiles import (
    ClaudeProfileProbe,
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


def test_probe_keeps_only_redacted_health_state(registry: ProfileRegistry) -> None:
    command = RecordingCommand(
        {
            "loggedIn": True,
            "authMethod": "claude.ai",
            "apiProvider": "firstParty",
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
