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
