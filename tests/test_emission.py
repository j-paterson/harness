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


# --- delegation-evidence gate (INFRA-186) -----------------------------------


@dataclass
class FakePacket:
    state: str


@dataclass
class FakePackets:
    by_issue: dict[str, list[FakePacket]] = field(default_factory=dict)

    def for_issue(self, issue_id: str) -> tuple[FakePacket, ...]:
        return tuple(self.by_issue.get(issue_id, ()))


def _non_trivial_git() -> FakeGitRunner:
    git = clean_git()
    git.responses[("diff", "--numstat", BASE, HEAD)] = (
        "20\t15\tsrc/app.py\n5\t4\ttests/test_app.py\n"
    )
    return git


def _trivial_git() -> FakeGitRunner:
    git = clean_git()
    git.responses[("diff", "--numstat", BASE, HEAD)] = (
        "3\t2\tsrc/app.py\n1\t1\ttests/test_app.py\n"
    )
    return git


def _emitter_with_packets(
    tmp_path: Path, git: FakeGitRunner, packets: FakePackets | None
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
        git=git,
        manifest_root=root,
        delivery=deliverer,
        now=lambda: NOW,
        packets=packets,
    )
    return emitter, deliverer


@pytest.mark.asyncio
async def test_non_trivial_candidate_without_accepted_packets_is_blocked(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    packets = FakePackets(by_issue={"ENG-9": [FakePacket(state="rejected")]})
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match="delegation evidence missing"):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []
    assert list((tmp_path / "manifests").iterdir()) == []


@pytest.mark.asyncio
async def test_non_trivial_candidate_with_one_accepted_packet_publishes(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(state="reserved"),
                FakePacket(state="accepted"),
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events == [("demo", result.event)]


@pytest.mark.asyncio
async def test_trivial_candidate_with_empty_ledger_publishes(tmp_path: Path) -> None:
    git = _trivial_git()
    packets = FakePackets()
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events == [("demo", result.event)]


@pytest.mark.asyncio
async def test_no_packets_wired_skips_the_delegation_gate(tmp_path: Path) -> None:
    git = _non_trivial_git()
    emitter, deliverer = _emitter_with_packets(tmp_path, git, None)

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events == [("demo", result.event)]


# --- final candidate-gate receipt requirement (INFRA-186 P9) ---------------
#
# A real ``Verifier`` measures the ACTUAL git tree at ``repo`` (rev-parse
# HEAD^{tree}, git diff HEAD, ...); the emission tests' ``FakeGitRunner``
# fakes the CandidateEmitter's own git seam with a synthetic 40-char HEAD
# sha that is never a real commit, and ``tmp_path`` (the fake's
# ``repo_path``) is never git-initialized. Wiring a real ``Verifier``
# through that seam would be incoherent (it would shell out to git
# against a directory that both isn't a repo and doesn't have the fake's
# claimed HEAD). So — per the packet's documented discretion — a stub
# exposing exactly ``Verifier``'s two consumed methods
# (``receipt_ids_for_gate``, ``validate_for_tree``) stands in, and the
# tests assert the emitter calls it correctly rather than re-deriving
# git's own tree hashing.


@dataclass
class StubVerifier:
    ids_by_gate: dict[str, tuple[str, ...]] = field(default_factory=dict)
    outcomes: dict[str, tuple[bool, str]] = field(default_factory=dict)
    calls: list[tuple[str, ...]] = field(default_factory=list)

    def receipt_ids_for_gate(self, gate_id: str) -> tuple[str, ...]:
        self.calls.append(("receipt_ids_for_gate", gate_id))
        return self.ids_by_gate.get(gate_id, ())

    def validate_for_tree(
        self, receipt_id: str, *, cwd: Path, gate_id: str
    ) -> tuple[bool, str]:
        self.calls.append(("validate_for_tree", receipt_id, gate_id))
        return self.outcomes.get(receipt_id, (False, "unknown receipt"))


def _emitter_with_verifier(
    tmp_path: Path, git: FakeGitRunner, verifier: StubVerifier | None
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
        git=git,
        manifest_root=root,
        delivery=deliverer,
        now=lambda: NOW,
        verifier=verifier,
    )
    return emitter, deliverer


@pytest.mark.asyncio
async def test_missing_final_gate_receipt_blocks_with_no_side_effects(
    tmp_path: Path,
) -> None:
    verifier = StubVerifier()
    emitter, deliverer = _emitter_with_verifier(tmp_path, clean_git(), verifier)

    with pytest.raises(EmissionBlocked, match="final candidate-gate receipt"):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []
    assert list((tmp_path / "manifests").iterdir()) == []
    assert verifier.calls == [("receipt_ids_for_gate", "candidate-full-gate")]


@pytest.mark.asyncio
async def test_stale_final_gate_receipt_blocks_with_no_side_effects(
    tmp_path: Path,
) -> None:
    verifier = StubVerifier(
        ids_by_gate={"candidate-full-gate": ("abc123",)},
        outcomes={"abc123": (False, "stale: tree changed")},
    )
    emitter, deliverer = _emitter_with_verifier(tmp_path, clean_git(), verifier)

    with pytest.raises(EmissionBlocked, match="final candidate-gate receipt"):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []
    assert list((tmp_path / "manifests").iterdir()) == []


@pytest.mark.asyncio
async def test_fresh_final_gate_receipt_publishes_and_is_threaded_into_the_manifest(
    tmp_path: Path,
) -> None:
    verifier = StubVerifier(
        ids_by_gate={"candidate-full-gate": ("abc123",)},
        outcomes={"abc123": (True, "fresh")},
    )
    emitter, deliverer = _emitter_with_verifier(tmp_path, clean_git(), verifier)

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events == [("demo", result.event)]
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert ("gate:candidate-full-gate", "receipt:abc123") in manifest.verification
    assert manifest.verification == (
        ("pytest", "ok"),
        ("gate:candidate-full-gate", "receipt:abc123"),
    )
    assert ("validate_for_tree", "abc123", "candidate-full-gate") in verifier.calls


@pytest.mark.asyncio
async def test_no_verifier_wired_skips_the_final_gate_receipt_check(
    tmp_path: Path,
) -> None:
    emitter, deliverer = _emitter_with_verifier(tmp_path, clean_git(), None)

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events == [("demo", result.event)]
