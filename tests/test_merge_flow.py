"""Verify live merge-flow composition helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from hermes_orchestrator.config import load_settings
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.merge_flow import build_merge_flow, merger_contract_path
from hermes_orchestrator.queue import QueueService


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


class _FakeKeychain:
    """A keychain stub returning a fixed token for any (service, account)."""

    def read(self, service: str, account: str) -> str:
        return "fake-token"


class _NullLinear:
    """A no-op ``LinearProjector`` collaborator; never invoked in this test."""

    async def project(self, issue_id: str, target: object, effect_id: str) -> object:
        raise AssertionError("not exercised by this test")


def _minimal_repo(tmp_path: Path) -> tuple[Path, Path]:
    """A live-flow-eligible repo/config tree with no CircleCI dependency."""

    config = tmp_path / "config"
    config.mkdir()
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts/codex-merger.md").write_text("# merger\n", encoding="utf-8")
    (config / "projects.yaml").write_text(
        "projects:\n"
        "  demo:\n"
        "    linear_team: engineering\n"
        f"    repo_path: {tmp_path}\n"
        "    integration_branch: main\n"
        "    github_repo: owner/demo\n"
        "    ci: none\n",
        encoding="utf-8",
    )
    (config / "policies.yaml").write_text("mode: observe\n", encoding="utf-8")
    return tmp_path, tmp_path / "state"


def test_build_merge_flow_wires_the_rotation_chain_lookup(tmp_path: Path) -> None:
    repo_root, state_dir = _minimal_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    settings.state_dir.mkdir(parents=True, exist_ok=True)
    database = Database.open(settings.state_dir / "state.db")
    events = EventStore(database)
    queue = QueueService(database, events, settings.projects)

    flow = build_merge_flow(
        settings,
        database=database,
        events=events,
        queue=queue,
        linear=_NullLinear(),
        keychain=_FakeKeychain(),
        base_env={},
    )

    assert flow.emitter._session_chain is not None
    assert callable(flow.emitter._session_chain)
