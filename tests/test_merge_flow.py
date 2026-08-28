"""Verify live merge-flow composition helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from hermes_orchestrator.merge_flow import merger_contract_path


def test_contract_prefers_the_configuration_root(tmp_path: Path) -> None:
    (tmp_path / "prompts").mkdir()
    contract = tmp_path / "prompts" / "codex-merger.md"
    contract.write_text("# contract\n", encoding="utf-8")
    assert merger_contract_path(tmp_path, package_prompts=tmp_path / "none") == (
        contract
    )


def test_contract_falls_back_to_the_running_package_tree(tmp_path: Path) -> None:
    package = tmp_path / "pkg-prompts"
    package.mkdir()
    (package / "codex-merger.md").write_text("# contract\n", encoding="utf-8")
    resolved = merger_contract_path(tmp_path / "stale-config", package_prompts=package)
    assert resolved == package / "codex-merger.md"


def test_missing_contract_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"codex-merger\.md"):
        merger_contract_path(tmp_path, package_prompts=tmp_path / "none")


def test_running_package_ships_the_contract() -> None:
    resolved = merger_contract_path(Path("/nonexistent-config-root"))
    assert resolved.name == "codex-merger.md"
    assert "BLOCKED_ON_EXTERNAL_INTAKE" in resolved.read_text(encoding="utf-8")
