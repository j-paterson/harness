"""Tests for hermes_orchestrator.provider_routes (INFRA-192).

Mirrors tests/test_profiles.py: a four-profile YAML fixture in tmp_path
plus a RecordingCommand fake for run_json. No network, no real claude
subprocess, no operator ~/.claude* directories or real config/*.yaml.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from hermes_orchestrator.profiles import ProfileRegistry
from hermes_orchestrator.provider_routes import (
    BEDROCK,
    FIRST_PARTY_MAX,
    MAX_ROUTE_SCRUBBED_KEYS,
    ProviderProbe,
    ProviderRoute,
    ProviderRoutes,
    classify,
    launch,
    resolve_executable,
    shell_init,
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


class FailingCommand:
    def __init__(self, error: Exception) -> None:
        self.error = error

    def run_json(self, command: list[str], env: dict[str, str]) -> dict[str, object]:
        raise self.error


def _write_providers_yaml(
    tmp_path: Path,
    *,
    filename: str = "providers.yaml",
    default_max_alias: str = "max-a",
    with_bedrock: bool = True,
    bedrock_config_dir: Path | None = None,
    aws_profile: str | None = "work",
    aws_region: str | None = "us-east-1",
    claude_executable: str | None = None,
) -> Path:
    path = tmp_path / filename
    lines = [f"default_max_alias: {default_max_alias}\n"]
    if claude_executable is not None:
        lines.append(f"claude_executable: {claude_executable}\n")
    if with_bedrock:
        config_dir = bedrock_config_dir or (tmp_path / ".claude-bedrock")
        lines.append("bedrock:\n")
        lines.append(f"  config_dir: {config_dir}\n")
        if aws_profile is not None:
            lines.append(f"  aws_profile: {aws_profile}\n")
        if aws_region is not None:
            lines.append(f"  aws_region: {aws_region}\n")
    path.write_text("".join(lines), encoding="utf-8")
    return path


@pytest.fixture
def routes(tmp_path: Path, registry: ProfileRegistry) -> ProviderRoutes:
    return ProviderRoutes.load(_write_providers_yaml(tmp_path), registry)


@pytest.fixture
def routes_no_bedrock(tmp_path: Path, registry: ProfileRegistry) -> ProviderRoutes:
    path = _write_providers_yaml(
        tmp_path, with_bedrock=False, filename="no-bedrock.yaml"
    )
    return ProviderRoutes.load(path, registry)


# --- 1. ProviderRoutes.load --------------------------------------------------


def test_load_names_order_with_and_without_bedrock(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    no_bedrock_path = _write_providers_yaml(
        tmp_path, with_bedrock=False, filename="no-bedrock.yaml"
    )
    no_bedrock_routes = ProviderRoutes.load(no_bedrock_path, registry)
    with_bedrock_routes = ProviderRoutes.load(_write_providers_yaml(tmp_path), registry)

    assert no_bedrock_routes.bedrock_configured is False
    assert no_bedrock_routes.names == ("default", "max-a", "max-b", "max-c", "max-d")
    assert with_bedrock_routes.bedrock_configured is True
    assert with_bedrock_routes.names == (
        "default",
        "max-a",
        "max-b",
        "max-c",
        "max-d",
        "bedrock",
    )


def test_relative_bedrock_config_dir_resolves_against_yaml_parent(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    path = tmp_path / "providers-relative.yaml"
    path.write_text(
        "default_max_alias: max-a\nbedrock:\n  config_dir: relative-bedrock-dir\n",
        encoding="utf-8",
    )

    routes = ProviderRoutes.load(path, registry)

    expected = (tmp_path / "relative-bedrock-dir").resolve(strict=False)
    assert routes.route("bedrock").config_dir == expected


def test_load_refuses_malformed_configurations(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    bedrock_dir = tmp_path / ".claude-bedrock"
    collide_dir = tmp_path / ".claude-max-a"
    cases = [
        (
            "bad-top-key.yaml",
            "default_max_alias: max-a\nextra_key: 1\n",
            "unsupported keys",
        ),
        (
            "bad-bedrock-key.yaml",
            f"default_max_alias: max-a\nbedrock:\n  config_dir: {bedrock_dir}\n"
            "  extra: 1\n",
            "unsupported keys",
        ),
        (
            "missing-alias.yaml",
            "claude_executable: /usr/local/bin/claude\n",
            "default_max_alias",
        ),
        ("unknown-alias.yaml", "default_max_alias: max-z\n", "unknown profile alias"),
        (
            "colliding-config-dir.yaml",
            f"default_max_alias: max-a\nbedrock:\n  config_dir: {collide_dir}\n",
            "must not be a Claude Max profile",
        ),
        (
            "spaced-profile.yaml",
            f"default_max_alias: max-a\nbedrock:\n  config_dir: {bedrock_dir}\n"
            f'  aws_profile: "has space"\n',
            "plain AWS name",
        ),
        (
            "bad-region.yaml",
            f"default_max_alias: max-a\nbedrock:\n  config_dir: {bedrock_dir}\n"
            f'  aws_region: "not a region"\n',
            "plain AWS name",
        ),
    ]
    for filename, content, match in cases:
        path = tmp_path / filename
        path.write_text(content, encoding="utf-8")
        with pytest.raises(ValueError, match=match):
            ProviderRoutes.load(path, registry)


def test_bedrock_aws_profile_rejects_credential_shaped_value(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    path = _write_providers_yaml(tmp_path, aws_profile="AKIAFAKEPLACEHOLDER1")

    with pytest.raises(ValueError, match="plain AWS name"):
        ProviderRoutes.load(path, registry)


# --- 2. route() and metadata() -----------------------------------------------


def test_route_unknown_and_unconfigured_bedrock_raise(
    routes: ProviderRoutes, routes_no_bedrock: ProviderRoutes
) -> None:
    with pytest.raises(ValueError, match="unknown"):
        routes.route("Not A Route")
    with pytest.raises(ValueError, match="not configured"):
        routes_no_bedrock.route("bedrock")


def test_route_kind_subscription_and_metadata(
    routes: ProviderRoutes, registry: ProfileRegistry
) -> None:
    default_route = routes.route("default")
    bedrock_route = routes.route("bedrock")

    assert default_route.kind == FIRST_PARTY_MAX
    assert default_route.subscription is True
    assert default_route.config_dir == registry.get(routes.default_max_alias).config_dir
    assert bedrock_route.kind == BEDROCK
    assert bedrock_route.subscription is False

    metadata = {entry["route"]: entry for entry in routes.metadata()}
    assert metadata["bedrock"]["subscription"] is False
    assert metadata["default"]["subscription"] is True
    for entry in metadata.values():
        assert set(entry) == {
            "route",
            "kind",
            "subscription",
            "config_dir",
            "aws_profile",
            "aws_region",
        }


# --- 3. launch_env for the Max routes -----------------------------------------


def test_launch_env_max_routes(
    routes: ProviderRoutes, registry: ProfileRegistry
) -> None:
    base = {key: "placeholder" for key in MAX_ROUTE_SCRUBBED_KEYS}
    base["PATH"] = "/usr/bin"
    base["HOME"] = "/home/placeholder"
    base["CLAUDE_CONFIG_DIR"] = "/somewhere/else"

    for name in ("default", "max-a", "max-b", "max-c", "max-d"):
        env = routes.launch_env(name, base)
        alias = routes.default_max_alias if name == "default" else name
        assert env["CLAUDE_CONFIG_DIR"] == str(registry.get(alias).config_dir)
        assert env["CLAUDE_CONFIG_DIR"] != "/somewhere/else"
        assert env["PATH"] == "/usr/bin"
        assert env["HOME"] == "/home/placeholder"
        for key in MAX_ROUTE_SCRUBBED_KEYS:
            assert key not in env


# --- 4. cwd independence -------------------------------------------------------


def test_launch_env_is_independent_of_cwd(
    routes: ProviderRoutes, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    subdir = tmp_path / "sub"
    subdir.mkdir()
    base_root = {"PATH": "/usr/bin", "PWD": str(tmp_path)}
    base_sub = {"PATH": "/usr/bin", "PWD": str(subdir)}

    monkeypatch.chdir(tmp_path)
    env_from_root = routes.launch_env("max-a", base_root)
    monkeypatch.chdir(subdir)
    env_from_sub = routes.launch_env("max-a", base_sub)

    # PWD is an unrelated key passed through as-is (it legitimately differs
    # here); the route itself must never differ by directory.
    assert env_from_root["CLAUDE_CONFIG_DIR"] == env_from_sub["CLAUDE_CONFIG_DIR"]
    assert env_from_root["PWD"] == str(tmp_path)
    assert env_from_sub["PWD"] == str(subdir)


def test_launch_env_never_consults_getcwd(
    routes: ProviderRoutes, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _raise() -> str:
        raise AssertionError("launch_env must not call os.getcwd")

    monkeypatch.setattr("os.getcwd", _raise)

    assert routes.launch_env("max-a", {"PATH": "/usr/bin"})["CLAUDE_CONFIG_DIR"]


# --- 5. Bedrock route env ------------------------------------------------------


def test_bedrock_launch_env_strips_overrides_and_static_credentials(
    routes: ProviderRoutes,
) -> None:
    base = {
        "PATH": "/usr/bin",
        "ANTHROPIC_API_KEY": "placeholder",
        "ANTHROPIC_AUTH_TOKEN": "placeholder",
        "ANTHROPIC_BASE_URL": "https://placeholder.example",
        "AWS_ACCESS_KEY_ID": "placeholder",
        "AWS_SECRET_ACCESS_KEY": "placeholder",
        "AWS_SESSION_TOKEN": "placeholder",
        "AWS_BEARER_TOKEN_BEDROCK": "placeholder",
    }
    bedrock_route = routes.route("bedrock")

    env = routes.launch_env("bedrock", base)

    assert env["CLAUDE_CONFIG_DIR"] == str(bedrock_route.config_dir)
    assert env["CLAUDE_CODE_USE_BEDROCK"] == "1"
    assert env["AWS_PROFILE"] == bedrock_route.aws_profile
    assert env["AWS_REGION"] == bedrock_route.aws_region
    assert env["AWS_DEFAULT_REGION"] == bedrock_route.aws_region
    assert env["PATH"] == "/usr/bin"
    for key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_BEARER_TOKEN_BEDROCK",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_BASE_URL",
    ):
        assert key not in env


def test_bedrock_launch_env_omits_aws_selection_keys_when_unconfigured(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    path = _write_providers_yaml(tmp_path, aws_profile=None, aws_region=None)
    routes = ProviderRoutes.load(path, registry)

    env = routes.launch_env("bedrock", {"PATH": "/usr/bin"})

    assert env["CLAUDE_CODE_USE_BEDROCK"] == "1"
    for key in ("AWS_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION"):
        assert key not in env


# --- 6. classify ----------------------------------------------------------------


_MAX_ROUTE = ProviderRoute(name="max-a", kind=FIRST_PARTY_MAX, config_dir=Path("/x"))
_BEDROCK_ROUTE = ProviderRoute(name="bedrock", kind=BEDROCK, config_dir=Path("/y"))
_MAX_OK = {
    "loggedIn": True,
    "authMethod": "claude.ai",
    "apiProvider": "firstParty",
    "subscriptionType": "max",
}
_BEDROCK_OK = {"loggedIn": True, "authMethod": "third_party", "apiProvider": "bedrock"}


@pytest.mark.parametrize(
    ("route", "status", "reason", "ok"),
    [
        (_MAX_ROUTE, _MAX_OK, "first_party_max", True),
        (
            _MAX_ROUTE,
            {**_MAX_OK, "apiProvider": "bedrock", "authMethod": "third_party"},
            "max_route_reports_bedrock",
            False,
        ),
        (
            _MAX_ROUTE,
            {**_MAX_OK, "apiProvider": "vertex", "authMethod": "third_party"},
            "max_route_reports_vertex",
            False,
        ),
        (
            _MAX_ROUTE,
            {**_MAX_OK, "subscriptionType": "pro"},
            "not_max_subscription",
            False,
        ),
        (_MAX_ROUTE, {**_MAX_OK, "loggedIn": False}, "not_logged_in", False),
        (_BEDROCK_ROUTE, _BEDROCK_OK, "bedrock", True),
        (
            _BEDROCK_ROUTE,
            {**_BEDROCK_OK, "apiProvider": "firstParty", "authMethod": "claude.ai"},
            "bedrock_route_reports_subscription",
            False,
        ),
        (
            _BEDROCK_ROUTE,
            {**_BEDROCK_OK, "subscriptionType": "max"},
            "bedrock_route_reports_subscription",
            False,
        ),
        (
            _BEDROCK_ROUTE,
            {**_BEDROCK_OK, "apiProvider": "vertex"},
            "bedrock_route_reports_vertex",
            False,
        ),
    ],
)
def test_classify(
    route: ProviderRoute, status: dict[str, object], reason: str, ok: bool
) -> None:
    verdict = classify(route, status)

    assert verdict.ok is ok
    assert verdict.reason == reason


# --- 7. ProviderProbe.check ------------------------------------------------------


def test_probe_check_env_matches_route(routes: ProviderRoutes) -> None:
    base_env = {"PATH": "/usr/bin", "CLAUDE_CODE_USE_BEDROCK": "1"}
    max_command = RecordingCommand(_MAX_OK)
    max_health = ProviderProbe(routes, max_command, base_env=base_env).check("max-a")
    assert max_health.ok is True
    assert max_command.calls[0][0] == ["claude", "auth", "status", "--json"]
    assert max_command.calls[0][1] == routes.launch_env("max-a", base_env)
    assert "CLAUDE_CODE_USE_BEDROCK" not in max_command.calls[0][1]

    bedrock_command = RecordingCommand(_BEDROCK_OK)
    bedrock_base = {"PATH": "/usr/bin"}
    bedrock_probe = ProviderProbe(routes, bedrock_command, base_env=bedrock_base)
    bedrock_health = bedrock_probe.check("bedrock")
    assert bedrock_health.ok is True
    assert bedrock_command.calls[0][1]["CLAUDE_CODE_USE_BEDROCK"] == "1"
    assert bedrock_command.calls[0][1] == routes.launch_env("bedrock", bedrock_base)


def test_check_all_covers_every_route_name(routes: ProviderRoutes) -> None:
    probe = ProviderProbe(
        routes, RecordingCommand(_MAX_OK), base_env={"PATH": "/usr/bin"}
    )

    results = probe.check_all()

    assert tuple(result.route for result in results) == routes.names


def test_probe_check_reports_probe_failed_on_command_errors(
    routes: ProviderRoutes,
) -> None:
    for error in (subprocess.CalledProcessError(1, ["claude"]), ValueError("bad")):
        probe = ProviderProbe(
            routes, FailingCommand(error), base_env={"PATH": "/usr/bin"}
        )
        health = probe.check("max-a")
        assert health.ok is False
        assert health.reason == "probe_failed"


def test_route_health_as_record_has_only_documented_keys_and_no_identity(
    routes: ProviderRoutes,
) -> None:
    status = {
        **_MAX_OK,
        "email": "placeholder@example.com",
        "orgName": "placeholder-org",
    }
    probe = ProviderProbe(routes, RecordingCommand(status), base_env={})

    record = probe.check("max-a").as_record()

    assert set(record) == {
        "route",
        "kind",
        "ok",
        "reason",
        "reported_provider",
        "reported_auth_method",
    }
    assert "email" not in json.dumps(record)
    assert "placeholder@example.com" not in json.dumps(record)


# --- 8. launch ---------------------------------------------------------------------


def test_launch_refuses_on_mismatch_without_leaking_env(routes: ProviderRoutes) -> None:
    probe = ProviderProbe(
        routes, RecordingCommand(_BEDROCK_OK), base_env={"PATH": "/usr/bin"}
    )
    base_env = {"PATH": "/usr/bin", "ANTHROPIC_API_KEY": "placeholder-secret"}
    calls: list[tuple[str, list[str], dict[str, str]]] = []

    code, message = launch(
        routes,
        "max-a",
        ["--flag"],
        base_env=base_env,
        executable="claude",
        probe=probe,
        execve=lambda *args: calls.append(args),
    )

    assert code == 3
    assert "max-a" in message
    assert "max_route_reports_bedrock" in message
    assert "placeholder-secret" not in message
    assert calls == []


def test_launch_unknown_route_returns_2(routes: ProviderRoutes) -> None:
    probe = ProviderProbe(routes, RecordingCommand({}), base_env={})

    code, message = launch(
        routes, "not-a-route", [], base_env={}, executable="claude", probe=probe
    )

    assert code == 2
    assert "not-a-route" in message


def test_launch_success_execs_exactly_once_with_route_launch_env(
    routes: ProviderRoutes,
) -> None:
    base_env = {"PATH": "/usr/bin"}
    probe = ProviderProbe(routes, RecordingCommand(_MAX_OK), base_env=base_env)
    calls: list[tuple[str, list[str], dict[str, str]]] = []

    code, message = launch(
        routes,
        "max-a",
        ["--flag", "value"],
        base_env=base_env,
        executable="claude",
        probe=probe,
        execve=lambda *args: calls.append(args),
    )

    assert code == 0
    assert message == ""
    expected_env = routes.launch_env("max-a", base_env)
    assert calls == [("claude", ["claude", "--flag", "value"], expected_env)]


# --- 9. resolve_executable -----------------------------------------------------------


def test_resolve_executable_prefers_configured_and_expands_tilde(
    tmp_path: Path, registry: ProfileRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    path = _write_providers_yaml(tmp_path, claude_executable="~/bin/claude")
    routes = ProviderRoutes.load(path, registry)

    resolved = resolve_executable(routes, which=lambda _name: None)

    assert resolved == str(tmp_path / "bin" / "claude")


def test_resolve_executable_via_which(routes: ProviderRoutes) -> None:
    resolved = resolve_executable(routes, which=lambda name: f"/usr/local/bin/{name}")
    assert resolved == "/usr/local/bin/claude"

    with pytest.raises(ValueError, match="not found on PATH"):
        resolve_executable(routes, which=lambda _name: None)


# --- 10. shell_init -------------------------------------------------------------------


def test_shell_init_defines_forwarding_functions_for_every_route(
    routes: ProviderRoutes, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SENTINEL_SECRET", "placeholder-secret-value")

    snippet = shell_init(routes, ["/opt/hermes tools/launch", "--flag"])

    for name in ("default", "max-a", "max-b", "max-c", "max-d", "bedrock"):
        function = "claude" if name == "default" else f"claude-{name}"
        assert f"{function}() {{" in snippet
        assert f"claude-launch --route {name}" in snippet
    assert "'/opt/hermes tools/launch' --flag" in snippet
    assert "$PWD" not in snippet
    assert "case " not in snippet
    assert "placeholder-secret-value" not in snippet


def test_shell_init_omits_bedrock_function_when_not_configured(
    routes_no_bedrock: ProviderRoutes,
) -> None:
    snippet = shell_init(routes_no_bedrock, ["hermes-orchestrator"])

    assert "claude-bedrock() {" not in snippet
    assert "claude() {" in snippet
