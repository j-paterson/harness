"""Verify live merge-flow composition helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from hermes_orchestrator.config import load_settings
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.git import GitError
from hermes_orchestrator.merge_flow import (
    _branch_head,
    _merged_candidate_proof,
    build_merge_flow,
    merger_contract_path,
)
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.review_intake import BranchHeadUnknown


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
    assert "Sol merge lead" in resolved.read_text(encoding="utf-8")


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


@pytest.mark.asyncio
async def test_built_flow_admits_zero_coverage_candidate_with_no_receipt(
    tmp_path: Path,
) -> None:
    """Operator-directed admission contract (2026-08-30 directive): a
    candidate with a clean pushed feature-branch head is admitted by the
    PRODUCTION-wired emitter even when zero accepted packets cover its
    diff and no verifier receipt exists — the manifest is written and
    delivery is attempted. Test results are advisory; Sol and CI own
    verification."""

    from tests.test_emission import FakeDeliverer, _non_trivial_git

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
    # Swap ONLY the process/network seams for fakes — the admission
    # wiring under test is exactly what build_merge_flow constructed.
    git = _non_trivial_git()
    deliverer = FakeDeliverer()
    flow.emitter._git = git
    flow.emitter._delivery = deliverer
    flow.emitter._intake_gate = None

    result = await flow.emitter.emit(
        "demo", "ENG-9", verification=(("pytest", "ok"),)
    )

    assert result.delivery.delivered is True
    assert result.manifest_path.exists()
    assert deliverer.events == [("demo", result.event)]


def test_build_merge_flow_wires_no_admission_enforcement_seams(
    tmp_path: Path,
) -> None:
    # Operator directive (2026-08-30): the production emitter carries no
    # delegation-packet ledger, no verifier, and no rotation-chain
    # lookup — those constructor seams no longer exist at all.
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

    for seam in ("_packets", "_verifier", "_session_chain", "_grandfather_binding"):
        assert not hasattr(flow.emitter, seam)


@dataclass
class FakeBranchHeadGit:
    """Recording fake standing in for GitVerifier's fetch/head_of surface."""

    heads: dict[str, str] = field(default_factory=dict)
    fetch_error: GitError | None = None
    head_of_error: GitError | None = None
    calls: list[tuple[str, ...]] = field(default_factory=list)

    def fetch(self, repo_path: Path, remote: str, branch: str) -> None:
        self.calls.append(("fetch", str(repo_path), remote, branch))
        if self.fetch_error is not None:
            raise self.fetch_error

    def head_of(self, repo_path: Path, ref: str) -> str:
        self.calls.append(("head_of", str(repo_path), ref))
        if self.head_of_error is not None:
            raise self.head_of_error
        return self.heads[ref]


def test_branch_head_resolves_the_fetched_origin_branch_sha(
    tmp_path: Path,
) -> None:
    """INFRA-202: admission's branch head comes from git, never from the
    open-PR list — no GitHub call is made to resolve it."""

    repo_root, _ = _minimal_repo(tmp_path)
    settings = load_settings(repo_root)
    git = FakeBranchHeadGit(heads={"origin/feature/eng-9": "1" * 40})

    head = _branch_head(settings, git)

    assert head("demo", "feature/eng-9") == "1" * 40
    assert git.calls == [
        ("fetch", str(repo_root), "origin", "feature/eng-9"),
        ("head_of", str(repo_root), "origin/feature/eng-9"),
    ]


def test_branch_head_raises_unknown_on_fetch_failure(
    tmp_path: Path,
) -> None:
    """INFRA-217, Sol correction c02dc0fe: a fetch failure (network,
    authentication, an unreachable remote) is NEVER authoritative branch
    absence -- it must raise :class:`BranchHeadUnknown` rather than
    collapsing to the same "" that a genuinely absent branch returns, so
    ``review_intake`` cannot mistake a transient failure for proof the
    branch is gone.
    """

    repo_root, _ = _minimal_repo(tmp_path)
    settings = load_settings(repo_root)
    git = FakeBranchHeadGit(fetch_error=GitError("git fetch failed"))

    head = _branch_head(settings, git)

    with pytest.raises(BranchHeadUnknown):
        head("demo", "feature/eng-9")


def test_branch_head_raises_unknown_on_local_resolution_error_type(
    tmp_path: Path,
) -> None:
    """A local resolution failure that is not the documented ``GitError``
    outcome (an unexpected exception) also fails closed as unknown, never
    as absence.
    """

    repo_root, _ = _minimal_repo(tmp_path)
    settings = load_settings(repo_root)
    git = FakeBranchHeadGit(head_of_error=OSError("local git corrupt"))

    head = _branch_head(settings, git)

    with pytest.raises(BranchHeadUnknown):
        head("demo", "feature/eng-9")


def test_branch_head_returns_empty_string_only_after_a_successful_fetch(
    tmp_path: Path,
) -> None:
    """AUTHORITATIVE ABSENCE: the fetch itself succeeds -- the remote was
    reachable -- and only the subsequent local resolution of
    ``origin/<branch>`` fails with the documented ``GitError``. This is
    the normal post-merge state (GitHub deletes the branch) and is still
    represented by returning "" exactly as before INFRA-217.
    """

    repo_root, _ = _minimal_repo(tmp_path)
    settings = load_settings(repo_root)
    git = FakeBranchHeadGit(head_of_error=GitError("git rev-parse failed"))

    head = _branch_head(settings, git)

    assert head("demo", "feature/eng-9") == ""
    assert git.calls == [
        ("fetch", str(repo_root), "origin", "feature/eng-9"),
        ("head_of", str(repo_root), "origin/feature/eng-9"),
    ]


@dataclass
class FakeDiscoveryGitHub:
    """Records discovery calls and answers with a canned pull."""

    pull: object | None = None
    error: Exception | None = None
    calls: list[tuple[str, str, str]] = field(default_factory=list)

    def discover_pull_request(
        self, repository: str, *, branch: str, head_sha: str
    ) -> object | None:
        self.calls.append((repository, branch, head_sha))
        if self.error is not None:
            raise self.error
        return self.pull


def _discovered(**overrides: object) -> object:
    from hermes_orchestrator.github import DiscoveredPull

    arguments: dict[str, object] = {
        "number": 14,
        "state": "closed",
        "merged": True,
        "head_sha": "1" * 40,
        "merge_sha": "2" * 40,
        "repository": "owner/demo",
        "head_repository": "owner/demo",
        "base_ref": "main",
    }
    arguments.update(overrides)
    return DiscoveredPull(**arguments)  # type: ignore[arg-type]


def test_merged_candidate_proof_accepts_only_an_exact_merged_same_repo_pull(
    tmp_path: Path,
) -> None:
    """INFRA-217: the only thing that may excuse a deleted branch.

    A merged, same-repository pull at the exact reviewed head targeting
    the integration branch proves the candidate landed; every other
    shape answers False so admission keeps failing closed.
    """

    repo_root, _ = _minimal_repo(tmp_path)
    settings = load_settings(repo_root)
    github = FakeDiscoveryGitHub(pull=_discovered())

    proof = _merged_candidate_proof(settings, github)

    assert proof("demo", "feature/eng-9", "1" * 40) is True
    assert github.calls == [("owner/demo", "feature/eng-9", "1" * 40)]


@pytest.mark.parametrize(
    "override",
    [
        {"merged": False},
        {"repository": "someone-else/demo"},
        {"head_repository": "someone-else/demo"},
        {"base_ref": "release/next"},
    ],
)
def test_merged_candidate_proof_refuses_every_other_shape(
    tmp_path: Path, override: dict[str, object]
) -> None:
    repo_root, _ = _minimal_repo(tmp_path)
    settings = load_settings(repo_root)
    github = FakeDiscoveryGitHub(pull=_discovered(**override))

    proof = _merged_candidate_proof(settings, github)

    assert proof("demo", "feature/eng-9", "1" * 40) is False


def test_merged_candidate_proof_fails_closed_on_no_match_or_github_error(
    tmp_path: Path,
) -> None:
    repo_root, _ = _minimal_repo(tmp_path)
    settings = load_settings(repo_root)

    assert _merged_candidate_proof(settings, FakeDiscoveryGitHub(pull=None))(
        "demo", "feature/eng-9", "1" * 40
    ) is False
    assert _merged_candidate_proof(
        settings, FakeDiscoveryGitHub(error=RuntimeError("github down"))
    )("demo", "feature/eng-9", "1" * 40) is False
    # An unknown project never reaches GitHub at all.
    unknown = FakeDiscoveryGitHub(pull=_discovered())
    assert _merged_candidate_proof(settings, unknown)(
        "nope", "feature/eng-9", "1" * 40
    ) is False
    assert unknown.calls == []
