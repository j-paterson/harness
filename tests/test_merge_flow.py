"""Verify live merge-flow composition helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from hermes_orchestrator.config import load_settings
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.git import GitError, GitResult
from hermes_orchestrator.merge_flow import (
    DatabaseDurableWakeReader,
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


class _RefusingKeychain:
    """A credential reader that RECORDS then REFUSES every read."""

    def __init__(self) -> None:
        self.reads: list[tuple[str, str]] = []

    def read(self, service: str, account: str) -> str:
        self.reads.append((service, account))
        raise RuntimeError(f"security exited 44 reading {service}")


def _circleci_repo(tmp_path: Path) -> tuple[Path, Path]:
    """The same tree as ``_minimal_repo`` but with a CircleCI project."""

    repo_root, state_dir = _minimal_repo(tmp_path)
    projects = repo_root / "config" / "projects.yaml"
    projects.write_text(
        projects.read_text(encoding="utf-8").replace("ci: none", "ci: circleci"),
        encoding="utf-8",
    )
    return repo_root, state_dir


def _flow_over(
    tmp_path: Path, keychain: object, *, circleci: bool = False
) -> object:
    repo_root, state_dir = (
        _circleci_repo(tmp_path) if circleci else _minimal_repo(tmp_path)
    )
    settings = load_settings(repo_root, state_dir)
    settings.state_dir.mkdir(parents=True, exist_ok=True)
    database = Database.open(settings.state_dir / "state.db")
    events = EventStore(database)
    queue = QueueService(database, events, settings.projects)
    return build_merge_flow(
        settings,
        database=database,
        events=events,
        queue=queue,
        linear=_NullLinear(),
        keychain=keychain,
        base_env={},
    )


@pytest.mark.parametrize("circleci", [False, True])
def test_build_merge_flow_reads_no_credential(
    tmp_path: Path, circleci: bool
) -> None:
    """INFRA-212: composing the graph must read nothing.

    The observed live failure was exactly this: ``submit-review`` inside
    the Codex workspace composed the full merge graph, that composition
    read the macOS Keychain, ``security`` exited 44, and a valid verdict
    could not be submitted at all. Building the flow over a keychain
    that refuses every read must now succeed, with zero reads --
    including for a deployment that DOES use CircleCI, whose token was
    the second eager read.
    """

    keychain = _RefusingKeychain()

    flow = _flow_over(tmp_path, keychain, circleci=circleci)

    assert flow is not None
    assert keychain.reads == []


def test_a_credentialed_call_still_fails_closed_at_its_point_of_use(
    tmp_path: Path,
) -> None:
    """INFRA-212: deferring is not skipping.

    A path that genuinely needs GitHub still reads the credential and
    still fails closed with the same error -- merely at the point of use
    instead of at composition. Nothing silently proceeds without the
    external effect.
    """

    keychain = _RefusingKeychain()
    flow = _flow_over(tmp_path, keychain)

    with pytest.raises(RuntimeError, match="security exited 44"):
        flow.turns._github.discover_pull_request(
            "owner/demo", branch="feature/x", head_sha="a" * 40
        )

    assert keychain.reads == [("hermes-orchestrator-github", "default")]


def test_build_merge_flow_wires_a_durable_wake_reader_into_the_emitter(
    tmp_path: Path,
) -> None:
    """Sol correction 110ed759 (INFRA-219 R3): packet L5 added the
    ``durable_wake`` port to ``CandidateEmitter`` so a manifest file
    surviving on disk is never adopted for reuse without a matching
    durable wake row for the exact event -- but the port was left
    optional and ``build_merge_flow`` composed the production emitter
    without it, so the gate never bound. This proves the emitter built by
    ``build_merge_flow`` carries a real (non-None) durable-wake reader,
    and that the reader answers correctly for a known-present and a
    known-absent event id against the SAME database the flow was built
    with -- not a fake."""

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

    reader = flow.emitter._durable_wake
    assert reader is not None
    assert isinstance(reader, DatabaseDurableWakeReader)

    assert reader.exists("demo", "unknown-event") is False

    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO wake_deliveries("
            "project_key, event_id, status, issue_id, candidate_sha, "
            "base_sha, branch, manifest_path, manifest_digest, "
            "manifest_device, manifest_inode, manifest_size, "
            "manifest_mtime_ns, manifest_mode, state, created_at, "
            "updated_at) VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "demo",
                "known-event",
                "FABLE_READY",
                "ENG-9",
                "a" * 40,
                "b" * 40,
                "feature/eng-9",
                "/tmp/manifest.json",
                "digest",
                1,
                1,
                1,
                1,
                0o100644,
                "pending",
                "2026-08-30T00:00:00+00:00",
                "2026-08-30T00:00:00+00:00",
            ),
        )

    assert reader.exists("demo", "known-event") is True
    assert reader.exists("demo", "unknown-event") is False


@dataclass
class FakeBranchHeadGit:
    """Recording fake standing in for GitVerifier's typed ``remote_head``.

    INFRA-217, Sol correction 43152bf8: ``_branch_head`` now calls the
    single typed remote-ref query rather than fetch-plus-local-rev-parse,
    so this fake exposes exactly that one seam.
    """

    heads: dict[str, str] = field(default_factory=dict)
    error: Exception | None = None
    calls: list[tuple[str, ...]] = field(default_factory=list)

    def remote_head(self, repo_path: Path, remote: str, branch: str) -> str | None:
        self.calls.append(("remote_head", str(repo_path), remote, branch))
        if self.error is not None:
            raise self.error
        return self.heads.get(f"{remote}/{branch}")


def test_branch_head_resolves_the_queried_remote_ref_sha(
    tmp_path: Path,
) -> None:
    """INFRA-202/INFRA-217: admission's branch head comes from a typed
    remote-ref query, never from the open-PR list and never from a
    fetch-plus-local-resolve pair — no GitHub call is made to resolve
    it."""

    repo_root, _ = _minimal_repo(tmp_path)
    settings = load_settings(repo_root)
    git = FakeBranchHeadGit(heads={"origin/feature/eng-9": "1" * 40})

    head = _branch_head(settings, git)

    assert head("demo", "feature/eng-9") == "1" * 40
    assert git.calls == [
        ("remote_head", str(repo_root), "origin", "feature/eng-9"),
    ]


def test_branch_head_raises_unknown_on_every_non_authoritative_failure(
    tmp_path: Path,
) -> None:
    """INFRA-217, Sol correction 43152bf8: any failure out of the typed
    remote-ref query -- transport, authentication, invocation, malformed
    output, or a local-repository error -- is NEVER authoritative branch
    absence. It must raise :class:`BranchHeadUnknown` rather than
    collapsing to the same "" that a genuinely absent branch returns, so
    ``review_intake`` cannot mistake a transient failure for proof the
    branch is gone.
    """

    repo_root, _ = _minimal_repo(tmp_path)
    settings = load_settings(repo_root)

    for error in (
        GitError("git ls-remote failed with exit code 128"),
        GitError("git ls-remote returned malformed output"),
        OSError("local git corrupt"),
    ):
        git = FakeBranchHeadGit(error=error)
        head = _branch_head(settings, git)
        with pytest.raises(BranchHeadUnknown):
            head("demo", "feature/eng-9")


def test_branch_head_returns_empty_string_on_authoritative_no_match(
    tmp_path: Path,
) -> None:
    """AUTHORITATIVE ABSENCE: the typed remote query itself succeeds with
    zero matching refs -- the remote was reachable and its ref namespace
    is authoritative. This is the normal post-merge state (GitHub deletes
    the branch) and is represented by returning "" exactly as before
    INFRA-217, but now sourced from one typed query instead of a fetch
    whose 128 exit is indistinguishable from a transport failure.
    """

    repo_root, _ = _minimal_repo(tmp_path)
    settings = load_settings(repo_root)
    git = FakeBranchHeadGit(heads={})

    head = _branch_head(settings, git)

    assert head("demo", "feature/eng-9") == ""
    assert git.calls == [
        ("remote_head", str(repo_root), "origin", "feature/eng-9"),
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


# --- INFRA-200 follow-up B: describe_candidate wired from git --------------


@dataclass
class FakeDescribeGitRunner:
    """Argv-keyed git fake for the tree/diff lookups behind
    ``describe_candidate`` (INFRA-200); unknown invocations fail like a
    broken clone."""

    responses: dict[tuple[str, ...], str] = field(default_factory=dict)
    calls: list[tuple[str, ...]] = field(default_factory=list)

    def run(
        self,
        args: tuple[str, ...],
        cwd: Path,
        *,
        input: str | None = None,
        env: dict[str, str] | None = None,
    ) -> GitResult:
        self.calls.append(args)
        if args not in self.responses:
            return GitResult(128, "", "fatal: not found")
        return GitResult(0, self.responses[args], "")


def test_describe_candidate_is_wired_from_git(tmp_path: Path) -> None:
    """INFRA-200: ``build_merge_flow`` wires a real ``describe_candidate``
    into ``CandidateAdmission``, built from read-only git evidence --
    ``git rev-parse <sha>^{tree}`` for the tree identity and ``git diff
    --name-only <sha>^ <sha>`` for the changed paths -- via the same
    argv-safe ``GitRunner`` idiom ``_branch_head``/``_base_policy`` use,
    never a shell and never a new ``git.py`` helper."""

    repo_root, state_dir = _minimal_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    settings.state_dir.mkdir(parents=True, exist_ok=True)
    database = Database.open(settings.state_dir / "state.db")
    events = EventStore(database)
    queue = QueueService(database, events, settings.projects)

    sha = "1" * 40
    tree = "2" * 40
    git = FakeDescribeGitRunner(
        responses={
            ("git", "rev-parse", "--verify", f"{sha}^{{tree}}"): tree + "\n",
            (
                "git",
                "diff",
                "--name-only",
                f"{sha}^",
                sha,
            ): "docs/readme.md\nsrc/app.py\n",
        }
    )

    flow = build_merge_flow(
        settings,
        database=database,
        events=events,
        queue=queue,
        linear=_NullLinear(),
        keychain=_FakeKeychain(),
        base_env={},
        git_runner=git,
    )

    describe = flow.admission._describe_candidate
    assert describe is not None

    tree_sha, changed = describe(sha)

    assert tree_sha == tree
    assert changed == ("docs/readme.md", "src/app.py")
    assert ("git", "rev-parse", "--verify", f"{sha}^{{tree}}") in git.calls
    assert ("git", "diff", "--name-only", f"{sha}^", sha) in git.calls


def test_describe_candidate_never_raises_on_a_local_git_failure(
    tmp_path: Path,
) -> None:
    """INFRA-200: ``CandidateAdmission._drift_verdict`` calls
    ``describe_candidate`` unconditionally, for every admission, and
    never defends against it raising -- the hint must stay purely
    advisory and never break a real admission. A local git failure (an
    unresolvable SHA here) must therefore degrade to a sentinel result,
    never propagate."""

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
        git_runner=FakeDescribeGitRunner(),
    )

    describe = flow.admission._describe_candidate
    assert describe is not None

    tree_sha, changed = describe("1" * 40)

    assert changed == ("<drift-hint-lookup-failed>",)
    assert tree_sha != ""
