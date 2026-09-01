from pathlib import Path

import pytest


@pytest.fixture
def configured_repo(tmp_path: Path) -> tuple[Path, Path]:
    config = tmp_path / "config"
    config.mkdir()
    (config / "projects.yaml").write_text(
        "projects:\n"
        "  demo:\n"
        "    linear_team: ENG\n"
        f"    repo_path: {tmp_path}\n"
        "    integration_branch: main\n"
        "    github_repo: owner/demo\n",
        encoding="utf-8",
    )
    (config / "policies.yaml").write_text(
        "mode: observe\nmax_unresolved_ci_merges: 2\n",
        encoding="utf-8",
    )
    # INFRA-214: the lead launch path now fails closed on an
    # unresolvable prompt asset (a missing prompt must never reach
    # Claude as a bad argument), so a configured repo carries the
    # prompts a real checkout carries.
    prompts = tmp_path / "prompts"
    prompts.mkdir(exist_ok=True)
    for name in ("claude-lead.md", "claude-harness.md", "codex-merger.md"):
        (prompts / name).write_text(f"# {name}\n", encoding="utf-8")
    return tmp_path, tmp_path / "state"
