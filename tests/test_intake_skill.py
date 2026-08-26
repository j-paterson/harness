from pathlib import Path

import yaml


def test_repo_local_intake_skill_preserves_explicit_admission_boundary() -> None:
    skill_path = (
        Path(__file__).parents[1]
        / ".hermes"
        / "skills"
        / "hermes-orchestrator-intake"
        / "SKILL.md"
    )

    content = skill_path.read_text(encoding="utf-8")
    _, frontmatter, body = content.split("---", maxsplit=2)
    metadata = yaml.safe_load(frontmatter)

    assert metadata["name"] == "hermes-orchestrator-intake"
    assert len(metadata["description"]) <= 60
    assert "official Linear MCP" in body
    assert "Never scan Linear" in body
    assert '"intent":"queue_issue"' in body
    assert '"project_key":"agent-orchestration"' in body
    assert "queue-list --json" in body
    assert "daemon" not in body