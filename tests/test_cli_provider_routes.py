"""INFRA-192: CLI coverage for claude-launch, provider-probe and
claude-shell-init.

Every live boundary is stubbed: ``SubprocessJsonCommand.run_json`` (the
real ``claude auth status --json`` runner) is replaced with a fake keyed
by ``CLAUDE_CONFIG_DIR``, and ``os.execve`` is replaced with a recorder
so a launch never actually replaces the test process.
``launch()`` resolves ``os.execve`` per call (never as an import-time
default), so patching ``provider_routes.os.execve`` is sufficient.
"""

from __future__ import annotations

import json
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path

import pytest

from hermes_orchestrator import provider_routes
from hermes_orchestrator.cli import main

# Placeholder values standing in for real operator secrets. They are
# set on the inherited environment so tests can prove the Max routes
# scrub them and that no refusal message ever echoes one back.
PLACEHOLDER_ENV: dict[str, str] = {
    "CLAUDE_CODE_USE_BEDROCK": "1",
    "AWS_PROFILE": "operator-shell-aws-profile-placeholder",
    "AWS_REGION": "us-west-2",
    "ANTHROPIC_API_KEY": "sk-ant-placeholder-TEST-VALUE-0001",
    "ANTHROPIC_AUTH_TOKEN": "placeholder-auth-token-TEST-0002",
}

ROUTE_ALIASES = ("max-a", "max-b", "max-c", "max-d")


@dataclass(frozen=True)
class CliResult:
    exit_code: int
    stdout: str
    stderr: str


def invoke(arguments: list[str]) -> CliResult:
    stdout = StringIO()
    stderr = StringIO()
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = main(arguments)
    except SystemExit as error:
        exit_code = int(error.code)
    return CliResult(exit_code, stdout.getvalue(), stderr.getvalue())


@dataclass(frozen=True)
class ProviderRepo:
    repo_root: Path
    state_dir: Path
    profile_dirs: dict[str, Path]
    bedrock_dir: Path
    executable: Path

    def arguments(self) -> list[str]:
        return [
            "--repo-root",
            str(self.repo_root),
            "--state-dir",
            str(self.state_dir),
        ]


@pytest.fixture
def provider_repo(configured_repo: tuple[Path, Path], tmp_path: Path) -> ProviderRepo:
    """A configured repo plus a 4-slot profiles.yaml and providers.yaml.

    ``default_max_alias`` is ``max-b`` (deliberately not the first
    profile) so a test that asserts on the default route also proves
    the alias indirection, not just profile-list order.
    """

    repo_root, state_dir = configured_repo
    config = repo_root / "config"

    profile_dirs: dict[str, Path] = {}
    document = "profiles:\n"
    for alias in ROUTE_ALIASES:
        directory = tmp_path / "max-profiles" / alias
        directory.mkdir(parents=True)
        profile_dirs[alias] = directory
        document += f"  - alias: {alias}\n    config_dir: {directory}\n"
    (config / "profiles.yaml").write_text(document, encoding="utf-8")

    bedrock_dir = tmp_path / "bedrock-config"
    bedrock_dir.mkdir()
    executable = tmp_path / "fake-claude"
    executable.write_text("", encoding="utf-8")
    (config / "providers.yaml").write_text(
        "default_max_alias: max-b\n"
        f"claude_executable: {executable}\n"
        "bedrock:\n"
        f"  config_dir: {bedrock_dir}\n"
        "  aws_profile: dev\n"
        "  aws_region: us-east-1\n",
        encoding="utf-8",
    )
    return ProviderRepo(repo_root, state_dir, profile_dirs, bedrock_dir, executable)


def _max_status(**overrides: object) -> dict[str, object]:
    status: dict[str, object] = {
        "loggedIn": True,
        "authMethod": "claude.ai",
        "apiProvider": "firstParty",
        "subscriptionType": "max",
        "email": "placeholder@example.invalid",
    }
    status.update(overrides)
    return status


def _bedrock_status(**overrides: object) -> dict[str, object]:
    status: dict[str, object] = {
        "loggedIn": True,
        "authMethod": "third_party",
        "apiProvider": "bedrock",
    }
    status.update(overrides)
    return status


def _default_statuses(repo: ProviderRepo) -> dict[str, dict[str, object]]:
    statuses = {
        str(directory): _max_status() for directory in repo.profile_dirs.values()
    }
    statuses[str(repo.bedrock_dir)] = _bedrock_status()
    return statuses


@dataclass
class ProbeCall:
    command: list[str]
    env: dict[str, str]


@dataclass
class ExecveCall:
    path: str
    argv: list[str]
    env: dict[str, str]


def _install_probe(
    monkeypatch: pytest.MonkeyPatch, statuses: dict[str, dict[str, object]]
) -> list[ProbeCall]:
    """Stand in for the live ``claude auth status --json`` subprocess."""

    calls: list[ProbeCall] = []

    def fake_run_json(
        self: object, command: list[str], env: dict[str, str]
    ) -> dict[str, object]:
        calls.append(ProbeCall(list(command), dict(env)))
        return dict(statuses[env["CLAUDE_CONFIG_DIR"]])

    monkeypatch.setattr(
        provider_routes.SubprocessJsonCommand, "run_json", fake_run_json
    )
    return calls


def _install_execve(monkeypatch: pytest.MonkeyPatch) -> list[ExecveCall]:
    """Stand in for ``os.execve`` so a launch never replaces this process."""

    calls: list[ExecveCall] = []

    def fake_execve(path: str, argv: list[str], env: dict[str, str]) -> None:
        calls.append(ExecveCall(path, list(argv), dict(env)))

    monkeypatch.setattr(provider_routes.os, "execve", fake_execve)
    return calls


def _set_placeholder_env(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    for key, value in PLACEHOLDER_ENV.items():
        monkeypatch.setenv(key, value)
    # CLAUDE_CODE_USE_BEDROCK's real value is the single digit "1", which
    # coincidentally occurs inside every tmp_path (pytest numbers its
    # directories), so it is not a usable "did this leak" fingerprint.
    # The remaining values are distinctive placeholder strings that never
    # occur by coincidence.
    return {k: v for k, v in PLACEHOLDER_ENV.items() if k != "CLAUDE_CODE_USE_BEDROCK"}


@dataclass
class Wired:
    repo: ProviderRepo
    probe_calls: list[ProbeCall] = field(default_factory=list)
    execve_calls: list[ExecveCall] = field(default_factory=list)


def _wire(
    provider_repo: ProviderRepo,
    monkeypatch: pytest.MonkeyPatch,
    *,
    statuses: dict[str, dict[str, object]] | None = None,
) -> Wired:
    resolved = statuses if statuses is not None else _default_statuses(provider_repo)
    probe_calls = _install_probe(monkeypatch, resolved)
    execve_calls = _install_execve(monkeypatch)
    return Wired(provider_repo, probe_calls, execve_calls)


# --------------------------------------------------------------------
# claude-launch
# --------------------------------------------------------------------


def test_claude_launch_default_route_execs_the_alias_the_route_names(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    placeholders = _set_placeholder_env(monkeypatch)
    wired = _wire(provider_repo, monkeypatch)

    result = invoke(
        [
            *provider_repo.arguments(),
            "claude-launch",
            "--route",
            "default",
            "--",
            "-p",
            "hello",
        ]
    )

    assert result.exit_code == 0
    assert len(wired.execve_calls) == 1
    call = wired.execve_calls[0]
    assert call.path == str(provider_repo.executable)
    assert call.argv == [str(provider_repo.executable), "-p", "hello"]
    assert call.env["CLAUDE_CONFIG_DIR"] == str(provider_repo.profile_dirs["max-b"])
    for key in ("CLAUDE_CODE_USE_BEDROCK", "AWS_PROFILE", "ANTHROPIC_API_KEY"):
        assert key not in call.env
    assert placeholders  # sanity: the placeholders were actually set

    assert len(wired.probe_calls) == 1
    probe = wired.probe_calls[0]
    assert probe.command == [
        str(provider_repo.executable),
        "auth",
        "status",
        "--json",
    ]


def test_claude_launch_named_max_route_selects_its_own_config_dir(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    wired = _wire(provider_repo, monkeypatch)

    result = invoke([*provider_repo.arguments(), "claude-launch", "--route", "max-a"])

    assert result.exit_code == 0
    assert len(wired.execve_calls) == 1
    env = wired.execve_calls[0].env
    assert env["CLAUDE_CONFIG_DIR"] == str(provider_repo.profile_dirs["max-a"])
    assert "CLAUDE_CODE_USE_BEDROCK" not in env


def test_claude_launch_bedrock_route_sets_aws_selection_only(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_placeholder_env(monkeypatch)
    wired = _wire(provider_repo, monkeypatch)

    result = invoke([*provider_repo.arguments(), "claude-launch", "--route", "bedrock"])

    assert result.exit_code == 0
    assert len(wired.execve_calls) == 1
    env = wired.execve_calls[0].env
    assert env["CLAUDE_CONFIG_DIR"] == str(provider_repo.bedrock_dir)
    assert env["CLAUDE_CODE_USE_BEDROCK"] == "1"
    assert env["AWS_PROFILE"] == "dev"
    assert env["AWS_REGION"] == "us-east-1"
    assert "ANTHROPIC_API_KEY" not in env


def test_claude_launch_is_independent_of_the_working_directory(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    wired = _wire(provider_repo, monkeypatch)
    first_dir = tmp_path / "cwd-one"
    second_dir = tmp_path / "cwd-two"
    first_dir.mkdir()
    second_dir.mkdir()

    monkeypatch.chdir(first_dir)
    monkeypatch.setenv("PWD", str(first_dir))
    result_one = invoke(
        [*provider_repo.arguments(), "claude-launch", "--route", "default"]
    )

    monkeypatch.chdir(second_dir)
    monkeypatch.setenv("PWD", str(second_dir))
    result_two = invoke(
        [*provider_repo.arguments(), "claude-launch", "--route", "default"]
    )

    assert result_one.exit_code == 0
    assert result_two.exit_code == 0
    assert len(wired.execve_calls) == 2
    call_one, call_two = wired.execve_calls
    assert call_one.path == call_two.path
    assert call_one.argv == call_two.argv
    # PWD is the one variable that trivially differs by shell bookkeeping;
    # everything else -- the selected config dir chief among it -- must
    # be identical across the two working directories.
    assert call_one.env["PWD"] != call_two.env["PWD"]
    env_one = {k: v for k, v in call_one.env.items() if k != "PWD"}
    env_two = {k: v for k, v in call_two.env.items() if k != "PWD"}
    assert env_one == env_two
    assert env_one["CLAUDE_CONFIG_DIR"] == str(provider_repo.profile_dirs["max-b"])


def test_claude_launch_refuses_a_max_route_reporting_bedrock(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    placeholders = _set_placeholder_env(monkeypatch)
    statuses = _default_statuses(provider_repo)
    statuses[str(provider_repo.profile_dirs["max-a"])] = _bedrock_status()
    wired = _wire(provider_repo, monkeypatch, statuses=statuses)

    result = invoke([*provider_repo.arguments(), "claude-launch", "--route", "max-a"])

    assert result.exit_code == 3
    assert wired.execve_calls == []
    assert "max-a" in result.stderr
    assert "max_route_reports_bedrock" in result.stderr
    for value in placeholders.values():
        assert value not in result.stderr


def test_claude_launch_refuses_a_bedrock_route_reporting_a_subscription(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    placeholders = _set_placeholder_env(monkeypatch)
    statuses = _default_statuses(provider_repo)
    statuses[str(provider_repo.bedrock_dir)] = _max_status()
    wired = _wire(provider_repo, monkeypatch, statuses=statuses)

    result = invoke([*provider_repo.arguments(), "claude-launch", "--route", "bedrock"])

    assert result.exit_code == 3
    assert wired.execve_calls == []
    assert "bedrock" in result.stderr
    assert "bedrock_route_reports_subscription" in result.stderr
    for value in placeholders.values():
        assert value not in result.stderr


def test_claude_launch_refuses_an_unknown_route(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    wired = _wire(provider_repo, monkeypatch)

    result = invoke(
        [*provider_repo.arguments(), "claude-launch", "--route", "unknown-route"]
    )

    assert result.exit_code == 2
    assert wired.execve_calls == []


def test_claude_launch_refuses_bedrock_route_when_not_configured(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    (provider_repo.repo_root / "config" / "providers.yaml").write_text(
        f"default_max_alias: max-b\nclaude_executable: {provider_repo.executable}\n",
        encoding="utf-8",
    )
    wired = _wire(provider_repo, monkeypatch)

    result = invoke([*provider_repo.arguments(), "claude-launch", "--route", "bedrock"])

    assert result.exit_code == 2
    assert wired.execve_calls == []


def test_claude_launch_refuses_when_providers_yaml_is_missing(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    (provider_repo.repo_root / "config" / "providers.yaml").unlink()
    wired = _wire(provider_repo, monkeypatch)

    result = invoke([*provider_repo.arguments(), "claude-launch", "--route", "default"])

    assert result.exit_code == 2
    assert wired.execve_calls == []
    assert "config/providers.example.yaml" in result.stderr


# --------------------------------------------------------------------
# provider-probe
# --------------------------------------------------------------------


def test_provider_probe_reports_every_route_in_order_and_succeeds(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    _wire(provider_repo, monkeypatch)

    result = invoke([*provider_repo.arguments(), "provider-probe"])

    assert result.exit_code == 0
    lines = result.stdout.strip("\n").split("\n")
    routes = [line.split("\t")[0] for line in lines]
    assert routes == ["default", "max-a", "max-b", "max-c", "max-d", "bedrock"]
    for line in lines:
        assert "REFUSED" not in line


def test_provider_probe_json_shape_never_carries_identity_fields(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    _wire(provider_repo, monkeypatch)

    result = invoke([*provider_repo.arguments(), "provider-probe", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert "placeholder@example.invalid" not in result.stdout

    expected_keys = {
        "route",
        "kind",
        "ok",
        "reason",
        "reported_provider",
        "reported_auth_method",
    }
    routes_by_name = {record["route"]: record for record in payload["routes"]}
    assert set(routes_by_name) == {
        "default",
        "max-a",
        "max-b",
        "max-c",
        "max-d",
        "bedrock",
    }
    for record in payload["routes"]:
        assert set(record) == expected_keys
        assert "email" not in record

    metadata_by_route = {entry["route"]: entry for entry in payload["metadata"]}
    assert metadata_by_route["bedrock"]["subscription"] is False
    for alias in ROUTE_ALIASES:
        assert metadata_by_route[alias]["subscription"] is True


def test_provider_probe_exits_nonzero_when_one_route_is_refused(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    statuses = _default_statuses(provider_repo)
    statuses[str(provider_repo.profile_dirs["max-c"])] = _bedrock_status()
    _wire(provider_repo, monkeypatch, statuses=statuses)

    result = invoke([*provider_repo.arguments(), "provider-probe"])

    assert result.exit_code == 1
    lines = result.stdout.strip("\n").split("\n")
    by_route = {line.split("\t")[0]: line for line in lines}
    assert "REFUSED" in by_route["max-c"]
    for route, line in by_route.items():
        if route != "max-c":
            assert "REFUSED" not in line


def test_provider_probe_with_route_checks_only_that_route(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    wired = _wire(provider_repo, monkeypatch)

    result = invoke([*provider_repo.arguments(), "provider-probe", "--route", "max-c"])

    assert result.exit_code == 0
    lines = result.stdout.strip("\n").split("\n")
    assert len(lines) == 1
    assert lines[0].startswith("max-c\t")
    assert len(wired.probe_calls) == 1
    assert wired.probe_calls[0].env["CLAUDE_CONFIG_DIR"] == str(
        provider_repo.profile_dirs["max-c"]
    )


# --------------------------------------------------------------------
# claude-shell-init
# --------------------------------------------------------------------


def _write_launcher(state_dir: Path) -> Path:
    launcher = state_dir / "bin" / "hermes-orchestrator"
    launcher.parent.mkdir(parents=True, exist_ok=True)
    launcher.write_text(
        '#!/bin/sh\nexec uv run --project "$ACTIVE" hermes-orchestrator "$@"\n',
        encoding="utf-8",
    )
    launcher.chmod(0o755)
    return launcher


def test_claude_shell_init_defines_a_function_per_route(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    placeholders = _set_placeholder_env(monkeypatch)
    launcher = _write_launcher(provider_repo.state_dir)

    result = invoke([*provider_repo.arguments(), "claude-shell-init"])

    assert result.exit_code == 0
    prefix = (
        f"{launcher} --repo-root {provider_repo.repo_root} "
        f"--state-dir {provider_repo.state_dir}"
    )
    expected_functions = {
        "claude": "default",
        "claude-max-a": "max-a",
        "claude-max-b": "max-b",
        "claude-max-c": "max-c",
        "claude-max-d": "max-d",
        "claude-bedrock": "bedrock",
    }
    for function, route in expected_functions.items():
        expected = f'{function}() {{ {prefix} claude-launch --route {route} -- "$@"; }}'
        assert expected in result.stdout
    assert "$PWD" not in result.stdout
    for value in placeholders.values():
        assert value not in result.stdout


def test_claude_shell_init_refuses_without_the_stable_launcher(
    provider_repo: ProviderRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    launcher = provider_repo.state_dir / "bin" / "hermes-orchestrator"
    assert not launcher.exists()

    result = invoke([*provider_repo.arguments(), "claude-shell-init"])

    assert result.exit_code == 1
    assert "deploy-install" in result.stderr
