from pathlib import Path

import pytest

from hermes_orchestrator.config import load_settings


def test_loads_registered_project(tmp_path: Path) -> None:
    (tmp_path / "config").mkdir()
    (tmp_path / "config/projects.yaml").write_text(
        "projects:\n"
        "  polysizer:\n"
        "    linear_team: ENG\n"
        "    repo_path: /tmp/polysizer\n"
        "    integration_branch: polysizer-refactor-2\n"
        "    github_repo: owner/polysizer\n",
        encoding="utf-8",
    )
    (tmp_path / "config/policies.yaml").write_text(
        "mode: observe\nmax_unresolved_ci_merges: 2\n",
        encoding="utf-8",
    )

    settings = load_settings(tmp_path, tmp_path / "state")

    assert settings.projects["polysizer"].integration_branch == (
        "polysizer-refactor-2"
    )
    assert settings.policy.max_unresolved_ci_merges == 2
    assert settings.state_dir == tmp_path / "state"


def test_resolves_relative_project_path_from_repo_root(tmp_path: Path) -> None:
    (tmp_path / "config").mkdir()
    (tmp_path / "config/projects.yaml").write_text(
        "projects:\n"
        "  orchestrator:\n"
        "    linear_team: infrastructure\n"
        "    repo_path: .\n"
        "    integration_branch: main\n"
        "    github_repo: owner/orchestrator\n",
        encoding="utf-8",
    )
    (tmp_path / "config/policies.yaml").write_text(
        "mode: observe\n",
        encoding="utf-8",
    )

    settings = load_settings(tmp_path, tmp_path / "state")

    assert settings.projects["orchestrator"].repo_path == tmp_path


def test_rejects_secret_like_project_keys(tmp_path: Path) -> None:
    (tmp_path / "config").mkdir()
    (tmp_path / "config/projects.yaml").write_text(
        "projects:\n  demo:\n    token: secret\n",
        encoding="utf-8",
    )
    (tmp_path / "config/policies.yaml").write_text(
        "mode: observe\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="secret-like"):
        load_settings(tmp_path, tmp_path / "state")


def test_active_local_policy_requires_complete_linear_routing(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
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
        "assignee_ids:\n"
        "  operator: user-operator\n"
        "  ryan: user-ryan\n"
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

    settings = load_settings(tmp_path, tmp_path / "state")

    assert settings.policy.mode == "active"
    assert settings.linear is not None
    assert settings.linear.teams["engineering"].status_ids.review == "state-review"


def test_active_mode_fails_closed_without_linear_routing(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    (config / "projects.yaml").write_text("projects: {}\n", encoding="utf-8")
    (config / "policies.yaml").write_text("mode: observe\n", encoding="utf-8")
    (config / "policies.local.yaml").write_text(
        "mode: active\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"linear\.yaml"):
        load_settings(tmp_path, tmp_path / "state")


def test_active_linear_routing_requires_both_assignees(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    (config / "projects.yaml").write_text("projects: {}\n", encoding="utf-8")
    (config / "policies.yaml").write_text("mode: active\n", encoding="utf-8")
    (config / "linear.yaml").write_text(
        "assignee_ids: {operator: user-operator}\n"
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

    with pytest.raises(ValueError, match="at least 2 items"):
        load_settings(tmp_path, tmp_path / "state")


def test_linear_team_without_qa_state_loads_non_qa_routing(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    (config / "projects.yaml").write_text(
        "projects:\n"
        "  orchestrator:\n"
        "    linear_team: infrastructure\n"
        f"    repo_path: {tmp_path}\n"
        "    integration_branch: main\n"
        "    github_repo: owner/orchestrator\n",
        encoding="utf-8",
    )
    (config / "policies.yaml").write_text("mode: observe\n", encoding="utf-8")
    (config / "linear.yaml").write_text(
        "assignee_ids:\n"
        "  operator: user-operator\n"
        "  ryan: user-ryan\n"
        "teams:\n"
        "  infrastructure:\n"
        "    team_id: team-infrastructure\n"
        "    status_ids:\n"
        "      Todo: state-todo\n"
        "      In Development: state-progress\n"
        "      Review: state-review\n"
        "      Done: state-done\n",
        encoding="utf-8",
    )

    settings = load_settings(tmp_path, tmp_path / "state")

    assert settings.linear is not None
    statuses = settings.linear.teams["infrastructure"].status_ids.as_mapping()
    assert statuses == {
        "Todo": "state-todo",
        "In Development": "state-progress",
        "Review": "state-review",
        "Done": "state-done",
    }


def test_project_ci_policy_is_explicit_and_closed(tmp_path: Path) -> None:
    (tmp_path / "config").mkdir()
    (tmp_path / "config/policies.yaml").write_text(
        "mode: observe\nmax_unresolved_ci_merges: 2\n", encoding="utf-8"
    )
    base = (
        "    linear_team: ENG\n"
        "    repo_path: /tmp/x\n"
        "    integration_branch: main\n"
        "    github_repo: owner/x\n"
    )
    (tmp_path / "config/projects.yaml").write_text(
        f"projects:\n  quiet:\n{base}    ci: none\n  strict:\n{base}",
        encoding="utf-8",
    )
    settings = load_settings(tmp_path, tmp_path / "state")
    assert settings.projects["quiet"].ci == "none"
    assert settings.projects["strict"].ci == "circleci"

    (tmp_path / "config/projects.yaml").write_text(
        f"projects:\n  odd:\n{base}    ci: github\n", encoding="utf-8"
    )
    with pytest.raises(ValueError):
        load_settings(tmp_path, tmp_path / "state")


def _write_minimal_config(tmp_path: Path) -> None:
    (tmp_path / "config").mkdir()
    (tmp_path / "config/projects.yaml").write_text(
        "projects:\n"
        "  demo:\n"
        "    linear_team: ENG\n"
        "    repo_path: /tmp/demo\n"
        "    integration_branch: main\n"
        "    github_repo: owner/demo\n",
        encoding="utf-8",
    )
    (tmp_path / "config/policies.yaml").write_text(
        "mode: observe\n", encoding="utf-8"
    )


def test_hermes_wake_command_loads_from_config(tmp_path: Path) -> None:
    _write_minimal_config(tmp_path)
    (tmp_path / "config/hermes.yaml").write_text(
        "wake_command:\n  - /usr/local/bin/hermes-notify\n  - --wake\n",
        encoding="utf-8",
    )

    settings = load_settings(tmp_path, tmp_path / "state")

    assert settings.hermes is not None
    assert settings.hermes.wake_command == [
        "/usr/local/bin/hermes-notify",
        "--wake",
    ]


def test_absent_hermes_config_leaves_direct_delivery_off(
    tmp_path: Path,
) -> None:
    _write_minimal_config(tmp_path)

    settings = load_settings(tmp_path, tmp_path / "state")

    assert settings.hermes is None


def test_hermes_wake_command_must_not_be_empty(tmp_path: Path) -> None:
    _write_minimal_config(tmp_path)
    (tmp_path / "config/hermes.yaml").write_text(
        "wake_command: []\n", encoding="utf-8"
    )

    with pytest.raises(ValueError):
        load_settings(tmp_path, tmp_path / "state")


def test_cmux_cli_loads_from_optional_config(tmp_path: Path) -> None:
    _write_minimal_config(tmp_path)
    (tmp_path / "config/cmux.yaml").write_text(
        "cli:\n  - /Applications/cmux.app/Contents/Resources/bin/cmux\n",
        encoding="utf-8",
    )

    settings = load_settings(tmp_path, tmp_path / "state")

    assert settings.cmux is not None
    assert settings.cmux.cli == [
        "/Applications/cmux.app/Contents/Resources/bin/cmux"
    ]


def test_absent_cmux_config_disables_terminal_visibility(
    tmp_path: Path,
) -> None:
    _write_minimal_config(tmp_path)

    settings = load_settings(tmp_path, tmp_path / "state")

    assert settings.cmux is None


def test_cmux_config_rejects_unknown_or_secret_like_keys(
    tmp_path: Path,
) -> None:
    _write_minimal_config(tmp_path)
    (tmp_path / "config/cmux.yaml").write_text(
        "cli:\n  - cmux\nsocket_password: nope\n", encoding="utf-8"
    )

    with pytest.raises(ValueError):
        load_settings(tmp_path, tmp_path / "state")


def test_project_path_resolving_to_an_issue_worktree_is_refused(
    tmp_path: Path,
) -> None:
    # A linked git worktree is exactly the checkout that issue cleanup
    # later removes; pinning the durable project path (lead cwd, cmux
    # workspace cwd, resource sampling target) to it must fail closed
    # at settings load, keeping the stable checkout the only anchor.
    from tests.test_migration_env import make_linked_worktree

    _, linked = make_linked_worktree(tmp_path)
    (tmp_path / "config").mkdir()
    (tmp_path / "config/projects.yaml").write_text(
        "projects:\n"
        "  demo:\n"
        "    linear_team: ENG\n"
        f"    repo_path: {linked}\n"
        "    integration_branch: main\n"
        "    github_repo: owner/demo\n",
        encoding="utf-8",
    )
    (tmp_path / "config/policies.yaml").write_text(
        "mode: observe\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="worktree"):
        load_settings(tmp_path, tmp_path / "state")


def test_the_stable_primary_checkout_is_accepted(tmp_path: Path) -> None:
    from tests.test_migration_env import make_linked_worktree

    primary, _ = make_linked_worktree(tmp_path)
    (tmp_path / "config").mkdir()
    (tmp_path / "config/projects.yaml").write_text(
        "projects:\n"
        "  demo:\n"
        "    linear_team: ENG\n"
        f"    repo_path: {primary}\n"
        "    integration_branch: main\n"
        "    github_repo: owner/demo\n",
        encoding="utf-8",
    )
    (tmp_path / "config/policies.yaml").write_text(
        "mode: observe\n", encoding="utf-8"
    )

    settings = load_settings(tmp_path, tmp_path / "state")

    assert settings.projects["demo"].repo_path == primary
