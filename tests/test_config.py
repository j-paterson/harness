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
