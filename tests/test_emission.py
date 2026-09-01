"""Verify freeze-boundary candidate emission: manifest plus queue wake."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.codex_queue import QueueDeliveryResult
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.emission import CandidateEmitter, EmissionBlocked
from hermes_orchestrator.git import GitResult
from hermes_orchestrator.manifests import WakeEvent, read_manifest

HEAD = "1" * 40
BASE = "4" * 40
NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)


@dataclass
class FakeGitRunner:
    """Argv-keyed git fake; unknown invocations fail like a broken clone."""

    responses: dict[tuple[str, ...], str] = field(default_factory=dict)
    calls: list[tuple[str, ...]] = field(default_factory=list)
    cwds: list[Path] = field(default_factory=list)
    common_dirs: dict[Path, str] = field(default_factory=dict)

    def run(self, args: tuple[str, ...], cwd: Path) -> GitResult:
        self.calls.append(args)
        self.cwds.append(cwd)
        key = args[1:]
        if key == (
            "rev-parse",
            "--path-format=absolute",
            "--git-common-dir",
        ) and cwd in self.common_dirs:
            return GitResult(0, self.common_dirs[cwd] + "\n", "")
        if key not in self.responses:
            return GitResult(128, "", "fatal")
        return GitResult(0, self.responses[key], "")


@dataclass
class FakeDeliverer:
    events: list[tuple[str, WakeEvent]] = field(default_factory=list)

    async def deliver(self, project_key: str, event: WakeEvent) -> QueueDeliveryResult:
        self.events.append((project_key, event))
        return QueueDeliveryResult(
            delivered=True,
            attempts=1,
            thread_id="thr_demo",
            generation=1,
            reason="delivered",
        )


def clean_git(head: str = HEAD, branch: str = "feature/eng-9") -> FakeGitRunner:
    return FakeGitRunner(
        responses={
            ("status", "--porcelain"): "",
            ("rev-parse", "HEAD"): head + "\n",
            ("rev-parse", "--abbrev-ref", "HEAD"): branch + "\n",
            ("fetch", "--", "origin", "main"): "",
            ("fetch", "--", "origin", branch): "",
            ("rev-parse", f"origin/{branch}"): head + "\n",
            ("merge-base", "HEAD", "origin/main"): BASE + "\n",
            ("diff", "--name-only", BASE, head): "src/app.py\ntests/test_app.py\n",
        }
    )


@pytest.fixture
def emitter_parts(
    tmp_path: Path,
) -> tuple[CandidateEmitter, FakeGitRunner, FakeDeliverer]:
    git = clean_git()
    deliverer = FakeDeliverer()
    root = tmp_path / "manifests"
    root.mkdir()
    emitter = CandidateEmitter(
        projects={
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=tmp_path,
                integration_branch="main",
                github_repo="j-paterson/demo",
            )
        },
        git=git,
        manifest_root=root,
        delivery=deliverer,
        now=lambda: NOW,
    )
    return emitter, git, deliverer


@pytest.mark.asyncio
async def test_emits_immutable_manifest_and_delivers_typed_wake(
    emitter_parts: tuple[CandidateEmitter, FakeGitRunner, FakeDeliverer],
    tmp_path: Path,
) -> None:
    emitter, git, deliverer = emitter_parts
    result = await emitter.emit(
        "demo", "ENG-9", verification=(("uv run pytest -q", "564 passed"),)
    )
    assert result.delivery.delivered is True
    assert result.reused_manifest is False
    assert result.event.status == "FABLE_READY"
    assert result.event.issue_id == "ENG-9"
    assert result.event.candidate_sha == HEAD
    assert result.event.base_sha == BASE
    assert result.event.event_id == f"fable_ready-eng-9-{HEAD[:12]}-20260828T120000"
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert manifest.branch == "feature/eng-9"
    assert manifest.changed_files == ("src/app.py", "tests/test_app.py")
    assert manifest.verification == (("uv run pytest -q", "564 passed"),)
    assert deliverer.events == [("demo", result.event)]
    assert ("git", "fetch", "--", "origin", "feature/eng-9") in git.calls


@pytest.mark.asyncio
async def test_re_emission_reuses_the_immutable_manifest(
    emitter_parts: tuple[CandidateEmitter, FakeGitRunner, FakeDeliverer],
) -> None:
    emitter, _, deliverer = emitter_parts
    first = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))
    second = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))
    assert second.reused_manifest is True
    assert second.event == first.event
    assert len(deliverer.events) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({("status", "--porcelain"): " M src/app.py\n"}, "not clean"),
        ({("rev-parse", "origin/feature/eng-9"): "2" * 40 + "\n"}, "pushed branch"),
        ({("rev-parse", "--abbrev-ref", "HEAD"): "main\n"}, "feature branch"),
        ({("diff", "--name-only", BASE, HEAD): "\n"}, "no changes"),
    ],
)
async def test_invalid_freeze_boundaries_fail_closed(
    emitter_parts: tuple[CandidateEmitter, FakeGitRunner, FakeDeliverer],
    override: dict[tuple[str, ...], str],
    message: str,
) -> None:
    emitter, git, deliverer = emitter_parts
    git.responses.update(override)
    with pytest.raises(EmissionBlocked, match=message):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))
    assert deliverer.events == []


@pytest.mark.asyncio
async def test_git_failure_and_missing_verification_fail_closed(
    emitter_parts: tuple[CandidateEmitter, FakeGitRunner, FakeDeliverer],
) -> None:
    emitter, git, deliverer = emitter_parts
    with pytest.raises(EmissionBlocked, match="verification"):
        await emitter.emit("demo", "ENG-9", verification=())
    del git.responses[("fetch", "--", "origin", "main")]
    with pytest.raises(EmissionBlocked, match="git fetch failed"):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))
    with pytest.raises(EmissionBlocked, match="unknown project"):
        await emitter.emit("nope", "ENG-9", verification=(("pytest", "ok"),))
    assert deliverer.events == []


# --- pre-delivery intake boundary -------------------------------------------


class GateRaising:
    def __init__(self, error: Exception | None) -> None:
        self.error = error
        self.calls: list[str] = []

    def validate(self, project_key: str, candidate: object) -> None:
        self.calls.append(getattr(candidate, "event_id", ""))
        if self.error is not None:
            raise self.error


class RecordingLead:
    def __init__(self) -> None:
        self.deliveries: list[tuple[str, str]] = []

    def deliver(self, issue_id: str, packets: tuple, *, source: str = "x") -> None:
        self.deliveries.append((issue_id, source))


def gated_emitter(
    tmp_path: Path, gate: GateRaising, lead: RecordingLead | None = None
) -> tuple[CandidateEmitter, FakeDeliverer]:
    deliverer = FakeDeliverer()
    root = tmp_path / "manifests"
    root.mkdir(exist_ok=True)
    emitter = CandidateEmitter(
        projects={
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=tmp_path,
                integration_branch="main",
                github_repo="j-paterson/demo",
            )
        },
        git=clean_git(),
        manifest_root=root,
        delivery=deliverer,
        now=lambda: NOW,
        intake_gate=gate,
        lead=lead,
        issue_for_failure=lambda project_key, sha: "ENG-1",
    )
    return emitter, deliverer


@pytest.mark.asyncio
async def test_intake_gate_runs_before_the_merger_is_woken(tmp_path: Path) -> None:
    from hermes_orchestrator.ci_window import MergeWindowExhausted

    gate = GateRaising(MergeWindowExhausted("full", unresolved=("m1", "m2")))
    emitter, deliverer = gated_emitter(tmp_path, gate)
    result = await emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    assert result.intake == "deferred"
    assert result.intake_reason == "full"
    assert (result.delivery.delivered, result.delivery.reason) == (
        False,
        "intake_deferred",
    )
    assert deliverer.events == []
    assert gate.calls == [result.event.event_id]


@pytest.mark.asyncio
async def test_prior_failure_at_emission_returns_packet_without_waking(
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.ci_window import PriorMergeFailed
    from hermes_orchestrator.verdicts import CorrectionPacket

    packet = CorrectionPacket(
        severity="Critical",
        repository="j-paterson/demo",
        branch="feature/eng-1",
        pr_number=3,
        reviewed_sha="9" * 40,
        evidence="ci failed",
        acceptance_criterion="ci green",
        required_correction="fix",
        required_tests=(),
    )
    lead = RecordingLead()
    gate = GateRaising(PriorMergeFailed("blocked", packet=packet))
    emitter, deliverer = gated_emitter(tmp_path, gate, lead)
    result = await emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    assert result.intake == "blocked_prior_failure"
    assert deliverer.events == []
    assert lead.deliveries == [("ENG-1", "ci_failure")]


@pytest.mark.asyncio
async def test_clear_intake_delivers(tmp_path: Path) -> None:
    gate = GateRaising(None)
    emitter, deliverer = gated_emitter(tmp_path, gate)
    result = await emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    assert result.intake == "clear"
    assert result.delivery.delivered is True
    assert len(deliverer.events) == 1


@pytest.mark.asyncio
async def test_rejected_intake_reports_the_bounded_reason(tmp_path: Path) -> None:
    from hermes_orchestrator.review_intake import CandidateRejected

    gate = GateRaising(
        CandidateRejected("the open pull request is not proven cleanly mergeable")
    )
    emitter, deliverer = gated_emitter(tmp_path, gate)
    result = await emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    assert result.intake == "rejected"
    assert result.intake_reason == (
        "the open pull request is not proven cleanly mergeable"
    )
    assert result.delivery.reason == "intake_rejected"
    assert deliverer.events == []


@pytest.mark.asyncio
async def test_lead_worktree_checkout_is_validated_and_used(
    emitter_parts: tuple[CandidateEmitter, FakeGitRunner, FakeDeliverer],
    tmp_path: Path,
) -> None:
    emitter, git, deliverer = emitter_parts
    worktree = tmp_path / "issue-worktree"
    worktree.mkdir()
    # Both checkouts share one git common dir: the same repository.
    git.common_dirs[worktree] = str(tmp_path / ".git")
    git.common_dirs[tmp_path] = str(tmp_path / ".git")

    result = await emitter.emit(
        "demo",
        "ENG-9",
        verification=(("uv run pytest -q", "12 passed"),),
        lead_checkout=worktree,
    )

    assert result.event.candidate_sha == HEAD
    assert deliverer.events
    # Every freeze-boundary git check after the repository-identity
    # probes ran against the lead's own checkout, where the candidate
    # branch actually lives — never the stable anchor's working tree.
    assert set(git.cwds[2:]) == {worktree}


@pytest.mark.asyncio
async def test_foreign_checkout_is_refused_before_any_freeze_check(
    emitter_parts: tuple[CandidateEmitter, FakeGitRunner, FakeDeliverer],
    tmp_path: Path,
) -> None:
    emitter, git, deliverer = emitter_parts
    foreign = tmp_path / "somewhere-else"
    foreign.mkdir()
    git.common_dirs[foreign] = "/other/repository/.git"
    git.common_dirs[tmp_path] = str(tmp_path / ".git")

    with pytest.raises(
        EmissionBlocked, match="does not belong to the project repository"
    ):
        await emitter.emit(
            "demo",
            "ENG-9",
            verification=(("uv run pytest -q", "12 passed"),),
            lead_checkout=foreign,
        )

    assert deliverer.events == []
    # Nothing beyond the two identity probes ran against the foreign path.
    assert git.calls[-1][1:] == (
        "rev-parse",
        "--path-format=absolute",
        "--git-common-dir",
    )


# --- operator-directed admission contract (2026-08-30) ----------------------
#
# Candidate admission requires ONLY a clean pushed sha (the freeze gate),
# an immutable manifest, and Sol intake. There is no delegation-evidence
# accounting and no mandatory verifier receipt: verification entries are
# pure advisory pass-through, and Sol and CI own verification.


def _non_trivial_git() -> FakeGitRunner:
    """A clean freeze boundary whose diff is large enough that the
    retired delegation-evidence gate would have demanded accepted
    packet coverage — kept as the canonical 'no special treatment'
    fixture for the relaxed admission contract."""

    git = clean_git()
    git.responses[("diff", "--numstat", BASE, HEAD)] = (
        "20\t15\tsrc/app.py\n5\t4\ttests/test_app.py\n"
    )
    return git


@pytest.mark.asyncio
async def test_non_trivial_candidate_admits_with_no_packet_or_verifier_wiring(
    tmp_path: Path,
) -> None:
    # The emitter is constructed with only its base seams — the
    # constructor accepts no packets/verifier/session-chain wiring at
    # all — and a non-trivial diff with zero delegation evidence and no
    # receipt is admitted: manifest written, delivery attempted, and
    # the supplied verification recorded verbatim as advisory data.
    git = _non_trivial_git()
    deliverer = FakeDeliverer()
    root = tmp_path / "manifests"
    root.mkdir()
    emitter = CandidateEmitter(
        projects={
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=tmp_path,
                integration_branch="main",
                github_repo="j-paterson/demo",
            )
        },
        git=git,
        manifest_root=root,
        delivery=deliverer,
        now=lambda: NOW,
    )

    result = await emitter.emit(
        "demo", "ENG-9", verification=(("pytest", "advisory only"),)
    )

    assert result.delivery.delivered is True
    assert result.manifest_path.exists()
    assert deliverer.events == [("demo", result.event)]
    manifest = read_manifest(result.manifest_path, root=root)
    # Pure advisory pass-through: exactly what was supplied, nothing
    # appended by admission.
    assert manifest.verification == (("pytest", "advisory only"),)


@dataclass
class FakeLease:
    """One ``worktree_leases`` row, shaped like ``WorktreeLease``."""

    lease_id: str
    project_key: str
    issue_id: str
    path: str
    branch: str
    checkpoint_sha: str | None = None


@dataclass
class FakeLeases:
    """``WorktreeLeases.active``-shaped read port for INFRA-219 L5."""

    rows: tuple[FakeLease, ...] = ()

    def active(self, project_key: str | None = None) -> tuple[FakeLease, ...]:
        if project_key is None:
            return self.rows
        return tuple(row for row in self.rows if row.project_key == project_key)


def _lane_emitter(
    tmp_path: Path, git: FakeGitRunner, lane_path: Path
) -> tuple[CandidateEmitter, FakeDeliverer]:
    """An emitter whose project anchor is NOT the lane's own worktree."""

    deliverer = FakeDeliverer()
    root = tmp_path / "manifests"
    root.mkdir(exist_ok=True)
    git.common_dirs[lane_path] = str(tmp_path / ".git")
    git.common_dirs[tmp_path] = str(tmp_path / ".git")
    git.responses[("rev-parse", "--path-format=absolute", "--show-toplevel")] = (
        str(lane_path) + "\n"
    )
    emitter = CandidateEmitter(
        projects={
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=tmp_path,
                integration_branch="main",
                github_repo="j-paterson/demo",
            )
        },
        git=git,
        manifest_root=root,
        delivery=deliverer,
        now=lambda: NOW,
    )
    return emitter, deliverer


def test_resolve_lane_refuses_an_unbound_or_ambiguous_issue(
    tmp_path: Path,
) -> None:
    # INFRA-219 L5: an unresolvable lane is the hazard this closes, so
    # it fails closed before anything is written — never a guess.
    from hermes_orchestrator.emission import resolve_lane

    empty = FakeLeases()
    with pytest.raises(EmissionBlocked, match="no live worktree lease"):
        resolve_lane(empty, "demo", "ENG-9")

    duplicated = FakeLeases(
        rows=(
            FakeLease("l1", "demo", "ENG-9", str(tmp_path / "a"), "feature/eng-9"),
            FakeLease("l2", "demo", "ENG-9", str(tmp_path / "b"), "feature/eng-9"),
        )
    )
    with pytest.raises(EmissionBlocked, match="live worktree leases"):
        resolve_lane(duplicated, "demo", "ENG-9")


@pytest.mark.asyncio
async def test_publication_freezes_the_requested_issue_lane_not_the_current_checkout(
    tmp_path: Path,
) -> None:
    # THE reproduced defect (INFRA-219 L5): candidate-ready for one issue
    # froze whichever checkout the lead occupied — live, an INFRA-218
    # manifest was bound to feature/infra-217's head. A resolved lane now
    # wins outright and the lane's own worktree is what gets frozen.
    from hermes_orchestrator.emission import resolve_lane

    lane_path = tmp_path / "lane-eng-9"
    lane_path.mkdir()
    git = clean_git()
    emitter, _deliverer = _lane_emitter(tmp_path, git, lane_path)
    leases = FakeLeases(
        rows=(
            FakeLease("l1", "demo", "ENG-9", str(lane_path), "feature/eng-9"),
        )
    )

    lane = resolve_lane(leases, "demo", "ENG-9")
    result = await emitter.emit(
        "demo",
        "ENG-9",
        verification=(("uv run pytest -q", "ok"),),
        resolved_lane=lane,
    )

    assert result.event.issue_id == "ENG-9"
    # Every FREEZE read — working-tree state, HEAD, base, changed files —
    # ran in the lane's own worktree. The project anchor is still read
    # once, but only for the git-common-dir foreign-checkout check.
    freeze_cwds = {
        cwd
        for args, cwd in zip(git.calls, git.cwds, strict=True)
        if args[1:]
        in (
            ("status", "--porcelain"),
            ("rev-parse", "HEAD"),
            ("merge-base", "HEAD", "origin/main"),
        )
    }
    assert freeze_cwds == {lane_path}


@pytest.mark.asyncio
async def test_publication_refuses_a_lane_identity_that_is_not_the_request(
    tmp_path: Path,
) -> None:
    # A lane resolved for a DIFFERENT issue can never publish this
    # candidate, and nothing durable is written.
    lane_path = tmp_path / "lane-other"
    lane_path.mkdir()
    git = clean_git()
    emitter, deliverer = _lane_emitter(tmp_path, git, lane_path)
    from hermes_orchestrator.emission import ResolvedLane

    foreign = ResolvedLane(
        lease_id="l9",
        project_key="demo",
        issue_id="ENG-11",
        path=lane_path,
        branch="feature/eng-11",
        expected_head=None,
    )

    with pytest.raises(EmissionBlocked, match="resolved lane identity"):
        await emitter.emit(
            "demo",
            "ENG-9",
            verification=(("t", "ok"),),
            resolved_lane=foreign,
        )

    assert deliverer.events == []


@pytest.mark.asyncio
async def test_publication_refuses_a_lease_branch_or_head_that_disagrees(
    tmp_path: Path,
) -> None:
    # The lease row is a durable claim, not proof: a branch or HEAD that
    # disagrees with what was actually frozen fails closed.
    lane_path = tmp_path / "lane-eng-9"
    lane_path.mkdir()
    git = clean_git()
    emitter, deliverer = _lane_emitter(tmp_path, git, lane_path)
    from hermes_orchestrator.emission import ResolvedLane

    wrong_branch = ResolvedLane(
        lease_id="l1",
        project_key="demo",
        issue_id="ENG-9",
        path=lane_path,
        branch="feature/somewhere-else",
        expected_head=None,
    )
    with pytest.raises(EmissionBlocked, match="disagrees with the frozen branch"):
        await emitter.emit(
            "demo", "ENG-9", verification=(("t", "ok"),),
            resolved_lane=wrong_branch,
        )

    wrong_head = ResolvedLane(
        lease_id="l1",
        project_key="demo",
        issue_id="ENG-9",
        path=lane_path,
        branch="feature/eng-9",
        expected_head="f" * 40,
    )
    with pytest.raises(EmissionBlocked, match="expected head"):
        await emitter.emit(
            "demo", "ENG-9", verification=(("t", "ok"),),
            resolved_lane=wrong_head,
        )

    assert deliverer.events == []
