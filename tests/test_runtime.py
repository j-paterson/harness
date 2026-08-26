from __future__ import annotations

from pathlib import Path

import pytest

from hermes_orchestrator.config import load_settings
from hermes_orchestrator.profiles import JsonCommand
from hermes_orchestrator.runtime import open_runtime


class EligibleProfileCommand(JsonCommand):
    def __init__(self) -> None:
        self.config_dirs: list[str] = []

    def run_json(
        self,
        command: list[str],
        env: dict[str, str],
    ) -> dict[str, object]:
        assert command == ["claude", "auth", "status", "--json"]
        self.config_dirs.append(env["CLAUDE_CONFIG_DIR"])
        return {
            "loggedIn": True,
            "authMethod": "claude.ai",
            "apiProvider": "firstParty",
            "email": "must-not-be-persisted@example.test",
        }


class FakeKeychain:
    def __init__(self) -> None:
        self.reads: list[tuple[str, str]] = []

    def read(self, service: str, account: str) -> str:
        self.reads.append((service, account))
        return "linear-token"


def active_repo(tmp_path: Path) -> tuple[Path, Path]:
    config = tmp_path / "config"
    config.mkdir()
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts/claude-lead.md").write_text(
        "Work only on explicitly queued work.\n",
        encoding="utf-8",
    )
    (config / "projects.yaml").write_text(
        "projects:\n"
        "  demo:\n"
        "    linear_team: engineering\n"
        f"    repo_path: {tmp_path}\n"
        "    integration_branch: main\n"
        "    github_repo: owner/demo\n",
        encoding="utf-8",
    )
    (config / "policies.yaml").write_text("mode: observe\n", encoding="utf-8")
    (config / "policies.local.yaml").write_text(
        "mode: active\n",
        encoding="utf-8",
    )
    (config / "linear.yaml").write_text(
        "assignee_ids: {operator: user-operator, ryan: user-ryan}\n"
        "teams:\n"
        "  engineering:\n"
        "    team_id: team-engineering\n"
        "    status_ids:\n"
        "      Todo: state-todo\n"
        "      In Development: state-development\n"
        "      Review: state-review\n"
        "      QA: state-qa\n"
        "      Done: state-done\n",
        encoding="utf-8",
    )
    (config / "profiles.yaml").write_text(
        "profiles:\n"
        f"  - {{alias: max-a, config_dir: {tmp_path / 'max-a'}}}\n"
        f"  - {{alias: max-b, config_dir: {tmp_path / 'max-b'}}}\n"
        f"  - {{alias: max-c, config_dir: {tmp_path / 'max-c'}}}\n"
        f"  - {{alias: max-d, config_dir: {tmp_path / 'max-d'}}}\n",
        encoding="utf-8",
    )
    return tmp_path, tmp_path / "state"


def test_active_runtime_assembles_live_dispatch_without_identity_persistence(
    tmp_path: Path,
) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    profiles = EligibleProfileCommand()
    keychain = FakeKeychain()

    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=profiles,
        keychain=keychain,
        base_env={},
    )
    try:
        assert runtime.dispatch is not None
        assert runtime.cells is not None
        assert [health.profile_alias for health in runtime.profile_health] == [
            "max-a",
            "max-b",
            "max-c",
            "max-d",
        ]
        assert all(health.eligible for health in runtime.profile_health)
        assert len(profiles.config_dirs) == 4
        assert keychain.reads == [("hermes-orchestrator-linear", "default")]
        assert runtime.database.scalar("SELECT count(*) FROM profile_leases") == 0
    finally:
        runtime.close()


def test_observation_runtime_never_loads_live_credentials(tmp_path: Path) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    keychain = FakeKeychain()

    runtime = open_runtime(settings, enable_live=False, keychain=keychain)
    try:
        assert runtime.dispatch is None
        assert runtime.cells is None
        assert runtime.profile_health == ()
        assert keychain.reads == []
    finally:
        runtime.close()


def test_active_runtime_fails_closed_without_lead_contract(tmp_path: Path) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    (repo_root / "prompts/claude-lead.md").unlink()
    settings = load_settings(repo_root, state_dir)

    with pytest.raises(ValueError, match="claude-lead"):
        open_runtime(
            settings,
            enable_live=True,
            profile_command=EligibleProfileCommand(),
            keychain=FakeKeychain(),
            base_env={},
        )
