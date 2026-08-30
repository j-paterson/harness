"""Verify freeze-boundary candidate emission: manifest plus queue wake."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator import emission as emission_module
from hermes_orchestrator.codex_queue import QueueDeliveryResult
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.emission import (
    CandidateEmitter,
    EmissionBlocked,
    build_grandfather_binding_lookup,
)
from hermes_orchestrator.git import GitResult
from hermes_orchestrator.manifests import WakeEvent, read_manifest
from hermes_orchestrator.operator_decisions import OperatorDecisions

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
    # The INFRA-194 head-blob seam (a raw ``(repo, head, path) -> bytes |
    # None`` callable, deliberately separate from the text-mode ``run``
    # seam above): ``head_blobs`` wires a present path's exact bytes or
    # an ``None`` for a positively-proven absence, ``head_blob_errors``
    # wires a failure that must raise instead (corruption, timeout, any
    # other unproven outcome). A path present in neither dict is an
    # unwired stub and also fails closed, the same as an unwired ``run``
    # invocation above.
    head_blobs: dict[tuple[str, str], bytes | None] = field(default_factory=dict)
    head_blob_errors: dict[tuple[str, str], Exception] = field(default_factory=dict)

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

    def head_blob(self, repo: Path, head: str, path: str) -> bytes | None:
        key = (head, path)
        if key in self.head_blob_errors:
            raise self.head_blob_errors[key]
        if key not in self.head_blobs:
            raise EmissionBlocked(
                f"no head-blob stub wired for {path!r} at {head}"
            )
        return self.head_blobs[key]


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
    packet_id: str = "pkt-default"
    allowed_files: tuple[str, ...] = ()
    evidence: dict[str, str] | None = None
    cell_id: str = "cell-1"
    session_id: str = "sess-1"
    worktree: str = "/repo"
    generation: int = 1
    updated_at: str = "2026-08-28T00:00:00+00:00"
    # Measured provenance (D1): ``None`` reproduces a legacy row with no
    # measurement at all. A real, credit-worthy packet must set both
    # (regular) or at least ``returned_blobs`` (exception) via the
    # ``_blob_hash``/``_wire_head_content`` helpers below so the
    # provenance it carries is coherent with the fake git tree.
    reserved_blobs: dict[str, str] | None = None
    returned_blobs: dict[str, str] | None = None


@dataclass
class FakePackets:
    by_issue: dict[str, list[FakePacket]] = field(default_factory=dict)

    def for_issue(self, issue_id: str) -> tuple[FakePacket, ...]:
        return tuple(self.by_issue.get(issue_id, ()))


def _blob_hash(content: str) -> str:
    """Mirror ``CandidateEmitter._head_blob_hash``'s hashing invariant
    (raw bytes, no text round-trip) so test fixtures can compute a
    returned blob that matches a given head-blob stub's content
    exactly."""

    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _wire_head_content(
    git: FakeGitRunner, head: str, contents: dict[str, str]
) -> dict[str, str]:
    """Stub the ``head_blob`` seam (``git.head_blob``) for each path in
    ``contents`` and return the matching ``{path: sha256-hexdigest}``
    map — the correct ``returned_blobs`` for a packet credited on
    exactly those paths."""

    returned: dict[str, str] = {}
    for path, content in contents.items():
        raw = content.encode("utf-8")
        git.head_blobs[(head, path)] = raw
        returned[path] = hashlib.sha256(raw).hexdigest()
    return returned


def _stale_reserved(returned: dict[str, str]) -> dict[str, str]:
    """A ``reserved_blobs`` map guaranteed to differ from ``returned`` at
    every path — i.e. real content actually changed during the window."""

    return {path: f"before:{blob}" for path, blob in returned.items()}


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
    tmp_path: Path,
    git: FakeGitRunner,
    packets: FakePackets | None,
    *,
    session_chain: Callable[[str], frozenset[str]] | None = None,
    grandfather_binding: Callable[[], dict[str, object] | None] | None = None,
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
        head_blob=git.head_blob,
        session_chain=session_chain,
        grandfather_binding=grandfather_binding,
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
    # Updated for the reconciliation contract: an accepted packet no
    # longer authorizes an arbitrary non-trivial diff merely by existing
    # — its ``allowed_files`` must actually cover every changed path.
    # Here the single accepted packet's scope covers both changed files,
    # so the candidate is admitted and the manifest records its credit.
    git = _non_trivial_git()
    returned = _wire_head_content(
        git,
        HEAD,
        {"src/app.py": "new src content", "tests/test_app.py": "new test content"},
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(state="reserved"),
                FakePacket(
                    state="accepted",
                    packet_id="pkt-1",
                    allowed_files=("src/app.py", "tests/test_app.py"),
                    evidence={},
                    reserved_blobs=_stale_reserved(returned),
                    returned_blobs=returned,
                ),
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events == [("demo", result.event)]
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert ("packet:pkt-1", "files=2;lines=44") in manifest.verification


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


# --- delegation-evidence reconciliation (Sol reviewer Critical finding 2) ---


def _custom_git(files: tuple[tuple[str, int, int], ...]) -> FakeGitRunner:
    """A clean freeze boundary whose diff touches exactly ``files``,
    each an ``(path, added, deleted)`` triple reflected in both
    ``diff --name-only`` and ``diff --numstat``."""

    git = clean_git()
    names = "".join(f"{name}\n" for name, _, _ in files)
    numstat = "".join(f"{added}\t{deleted}\t{name}\n" for name, added, deleted in files)
    git.responses[("diff", "--name-only", BASE, HEAD)] = names
    git.responses[("diff", "--numstat", BASE, HEAD)] = numstat
    return git


@pytest.mark.asyncio
async def test_unrelated_accepted_packet_does_not_authorize_the_diff(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-unrelated",
                    allowed_files=("some/other/file.py",),
                    evidence={},
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match=re.escape("src/app.py")):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []
    assert list((tmp_path / "manifests").iterdir()) == []


@pytest.mark.asyncio
async def test_a_changed_path_outside_every_accepted_scope_is_named(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-partial",
                    allowed_files=("src/app.py",),
                    evidence={},
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match=re.escape("tests/test_app.py")):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []


@pytest.mark.asyncio
async def test_unparsable_numstat_line_for_a_changed_path_fails_closed(
    tmp_path: Path,
) -> None:
    # A binary-style numstat line ("-\t-\t<path>") for a changed path
    # cannot be measured at all, so it must fail closed naming the path
    # even though an accepted packet fully covers every changed path.
    git = clean_git()
    changed_files = ("bin.dat", "src/app.py")
    git.responses[("diff", "--name-only", BASE, HEAD)] = "".join(
        f"{name}\n" for name in changed_files
    )
    git.responses[("diff", "--numstat", BASE, HEAD)] = (
        "-\t-\tbin.dat\n20\t15\tsrc/app.py\n"
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-reg",
                    allowed_files=("bin.dat", "src/app.py"),
                    evidence={},
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match=re.escape("bin.dat")):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []
    assert list((tmp_path / "manifests").iterdir()) == []


@pytest.mark.asyncio
async def test_direct_exception_exceeding_the_credited_file_bound_is_blocked(
    tmp_path: Path,
) -> None:
    git = _custom_git(
        (
            ("a.py", 1, 1),
            ("b.py", 1, 1),
            ("c.py", 1, 1),
        )
    )
    exc_returned = _wire_head_content(
        git, HEAD, {"a.py": "a new", "b.py": "b new", "c.py": "c new"}
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-exc",
                    allowed_files=("a.py", "b.py", "c.py"),
                    evidence={"exception_reason": "typo sweep", "expected_lines": "30"},
                    returned_blobs=exc_returned,
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match="pkt-exc"):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []


@pytest.mark.asyncio
async def test_direct_exception_exceeding_the_credited_line_bound_is_blocked(
    tmp_path: Path,
) -> None:
    # Two files, credited lines total 44 > the fixed thirty-line cap even
    # though the packet declared a generous expected_lines.
    git = _non_trivial_git()
    exc_returned = _wire_head_content(
        git,
        HEAD,
        {"src/app.py": "new src content", "tests/test_app.py": "new test content"},
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-exc",
                    allowed_files=("src/app.py", "tests/test_app.py"),
                    evidence={
                        "exception_reason": "large exception",
                        "expected_lines": "100",
                    },
                    returned_blobs=exc_returned,
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match="pkt-exc"):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []


@pytest.mark.asyncio
async def test_direct_exception_exceeding_its_own_declared_expected_lines_is_blocked(
    tmp_path: Path,
) -> None:
    # Three changed files force non-triviality by file count; the
    # exception's two credited files sum to 25 lines — under the fixed
    # cap of thirty, but over its own declared expected_lines of 20.
    git = _custom_git(
        (
            ("exc1.py", 10, 5),
            ("exc2.py", 5, 5),
            ("reg.py", 1, 1),
        )
    )
    reg_returned = _wire_head_content(git, HEAD, {"reg.py": "reg new content"})
    exc_returned = _wire_head_content(
        git, HEAD, {"exc1.py": "exc1 new content", "exc2.py": "exc2 new content"}
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-reg",
                    allowed_files=("reg.py",),
                    evidence={},
                    reserved_blobs=_stale_reserved(reg_returned),
                    returned_blobs=reg_returned,
                ),
                FakePacket(
                    state="accepted",
                    packet_id="pkt-exc",
                    allowed_files=("exc1.py", "exc2.py"),
                    evidence={
                        "exception_reason": "tight exception",
                        "expected_lines": "20",
                    },
                    returned_blobs=exc_returned,
                ),
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match="pkt-exc"):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []


@pytest.mark.asyncio
async def test_two_exceptions_claiming_the_same_path_fail_closed(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-exc-1",
                    allowed_files=("src/app.py",),
                    evidence={"exception_reason": "first", "expected_lines": "30"},
                ),
                FakePacket(
                    state="accepted",
                    packet_id="pkt-exc-2",
                    allowed_files=("src/app.py",),
                    evidence={"exception_reason": "second", "expected_lines": "30"},
                ),
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match=re.escape("src/app.py")):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []


@pytest.mark.asyncio
async def test_regular_packet_wins_over_an_exception_on_the_same_path(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    returned = _wire_head_content(
        git,
        HEAD,
        {"src/app.py": "new src content", "tests/test_app.py": "new test content"},
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-reg",
                    allowed_files=("src/app.py", "tests/test_app.py"),
                    evidence={},
                    reserved_blobs=_stale_reserved(returned),
                    returned_blobs=returned,
                ),
                FakePacket(
                    state="accepted",
                    packet_id="pkt-exc",
                    allowed_files=("src/app.py",),
                    evidence={"exception_reason": "also claims it here"},
                    # Never credited (the regular packet wins the shared
                    # path), so its own provenance is irrelevant here —
                    # left at the legacy None default on purpose.
                ),
            ]
        }
    )
    emitter, _deliverer = _emitter_with_packets(tmp_path, git, packets)

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert ("packet:pkt-reg", "files=2;lines=44") in manifest.verification
    assert not any(
        entry[0] == "packet:pkt-exc" for entry in manifest.verification
    )


@pytest.mark.asyncio
async def test_fully_covered_candidate_with_regulars_and_a_bounded_exception(
    tmp_path: Path,
) -> None:
    git = _custom_git(
        (
            ("reg1.py", 5, 5),
            ("reg2.py", 4, 4),
            ("exc.py", 3, 2),
        )
    )
    reg1_returned = _wire_head_content(git, HEAD, {"reg1.py": "reg1 new content"})
    reg2_returned = _wire_head_content(git, HEAD, {"reg2.py": "reg2 new content"})
    exc_returned = _wire_head_content(git, HEAD, {"exc.py": "exc new content"})
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-reg1",
                    allowed_files=("reg1.py",),
                    evidence={},
                    session_id="sess-shared",
                    worktree="/wt-shared",
                    generation=3,
                    reserved_blobs=_stale_reserved(reg1_returned),
                    returned_blobs=reg1_returned,
                ),
                FakePacket(
                    state="accepted",
                    packet_id="pkt-reg2",
                    allowed_files=("reg2.py",),
                    evidence={},
                    session_id="sess-shared",
                    worktree="/wt-shared",
                    generation=3,
                    reserved_blobs=_stale_reserved(reg2_returned),
                    returned_blobs=reg2_returned,
                ),
                FakePacket(
                    state="accepted",
                    packet_id="pkt-exc",
                    allowed_files=("exc.py",),
                    evidence={
                        "exception_reason": "small bounded exception",
                        "expected_lines": "10",
                    },
                    session_id="sess-shared",
                    worktree="/wt-shared",
                    generation=3,
                    returned_blobs=exc_returned,
                ),
            ]
        }
    )
    emitter, _deliverer = _emitter_with_packets(tmp_path, git, packets)

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert manifest.verification == (
        ("pytest", "ok"),
        ("packet:pkt-exc", "files=1;lines=5"),
        ("packet:pkt-reg1", "files=1;lines=10"),
        ("packet:pkt-reg2", "files=1;lines=8"),
    )


@pytest.mark.asyncio
async def test_credited_packet_identity_mismatch_is_blocked(tmp_path: Path) -> None:
    git = _non_trivial_git()
    a_returned = _wire_head_content(git, HEAD, {"src/app.py": "new src content"})
    b_returned = _wire_head_content(
        git, HEAD, {"tests/test_app.py": "new test content"}
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-a",
                    allowed_files=("src/app.py",),
                    evidence={},
                    session_id="sess-a",
                    worktree="/wt-a",
                    generation=1,
                    reserved_blobs=_stale_reserved(a_returned),
                    returned_blobs=a_returned,
                ),
                FakePacket(
                    state="accepted",
                    packet_id="pkt-b",
                    allowed_files=("tests/test_app.py",),
                    evidence={},
                    session_id="sess-b",
                    worktree="/wt-a",
                    generation=1,
                    reserved_blobs=_stale_reserved(b_returned),
                    returned_blobs=b_returned,
                ),
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match="identity mismatch"):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []


# --- rotation-chain delegation evidence (INFRA-197 packet G1) --------------
#
# Operator rotation policy deliberately rotates lead sessions at safe
# boundaries with durable, acknowledged handoffs, so a legitimate candidate
# branch may legitimately be built by a CHAIN of sessions for one cell —
# not just one. Credited packets must still share exactly one worktree and
# one cell_id, but the session_id of each credited packet is now checked
# against the cell's durably recorded rotation chain (injected via
# ``session_chain``) rather than against every other credited packet's own
# session_id. With no ``session_chain`` supplied, the OLD single-identity
# rule (one shared session, worktree, and generation) still applies —
# fail-closed by default.


def _two_regular_packets(
    git: FakeGitRunner,
    *,
    cell_a: str,
    session_a: str,
    worktree_a: str,
    cell_b: str,
    session_b: str,
    worktree_b: str,
    generation_a: int = 1,
    generation_b: int = 2,
) -> FakePackets:
    a_returned = _wire_head_content(git, HEAD, {"src/app.py": "new src content"})
    b_returned = _wire_head_content(
        git, HEAD, {"tests/test_app.py": "new test content"}
    )
    return FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-a",
                    allowed_files=("src/app.py",),
                    evidence={},
                    cell_id=cell_a,
                    session_id=session_a,
                    worktree=worktree_a,
                    generation=generation_a,
                    reserved_blobs=_stale_reserved(a_returned),
                    returned_blobs=a_returned,
                ),
                FakePacket(
                    state="accepted",
                    packet_id="pkt-b",
                    allowed_files=("tests/test_app.py",),
                    evidence={},
                    cell_id=cell_b,
                    session_id=session_b,
                    worktree=worktree_b,
                    generation=generation_b,
                    reserved_blobs=_stale_reserved(b_returned),
                    returned_blobs=b_returned,
                ),
            ]
        }
    )


@pytest.mark.asyncio
async def test_credited_packets_from_chained_rotation_sessions_pass(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    packets = _two_regular_packets(
        git,
        cell_a="cell-rot",
        session_a="sess-old",
        worktree_a="/wt-a",
        cell_b="cell-rot",
        session_b="sess-new",
        worktree_b="/wt-a",
    )
    chain = frozenset({"sess-old", "sess-new"})
    emitter, deliverer = _emitter_with_packets(
        tmp_path,
        git,
        packets,
        session_chain=lambda cell_id: chain if cell_id == "cell-rot" else frozenset(),
    )

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events != []


@pytest.mark.asyncio
async def test_credited_packet_session_outside_rotation_chain_is_blocked(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    packets = _two_regular_packets(
        git,
        cell_a="cell-rot",
        session_a="sess-old",
        worktree_a="/wt-a",
        cell_b="cell-rot",
        session_b="sess-rogue",
        worktree_b="/wt-a",
    )
    # Only "sess-old" is durably recorded for "cell-rot" — "sess-rogue"
    # never rotated in, so its packet earns no credit.
    chain = frozenset({"sess-old"})
    emitter, deliverer = _emitter_with_packets(
        tmp_path,
        git,
        packets,
        session_chain=lambda cell_id: chain if cell_id == "cell-rot" else frozenset(),
    )

    with pytest.raises(EmissionBlocked, match="identity mismatch") as excinfo:
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert "sess-rogue" in str(excinfo.value)
    assert deliverer.events == []


@pytest.mark.asyncio
async def test_credited_packets_with_mixed_cell_ids_are_blocked(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    packets = _two_regular_packets(
        git,
        cell_a="cell-a",
        session_a="sess-a",
        worktree_a="/wt-a",
        cell_b="cell-b",
        session_b="sess-b",
        worktree_b="/wt-a",
    )
    # A permissive chain lookup must not rescue a mixed-cell credit set —
    # cell_id uniformity is checked before any chain lookup happens.
    emitter, deliverer = _emitter_with_packets(
        tmp_path,
        git,
        packets,
        session_chain=lambda cell_id: frozenset({"sess-a", "sess-b"}),
    )

    with pytest.raises(EmissionBlocked, match="identity mismatch") as excinfo:
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert "cell" in str(excinfo.value)
    assert deliverer.events == []


@pytest.mark.asyncio
async def test_credited_packets_with_mixed_worktrees_are_blocked(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    packets = _two_regular_packets(
        git,
        cell_a="cell-rot",
        session_a="sess-a",
        worktree_a="/wt-a",
        cell_b="cell-rot",
        session_b="sess-b",
        worktree_b="/wt-b",
    )
    emitter, deliverer = _emitter_with_packets(
        tmp_path,
        git,
        packets,
        session_chain=lambda cell_id: frozenset({"sess-a", "sess-b"}),
    )

    with pytest.raises(EmissionBlocked, match="identity mismatch") as excinfo:
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert "worktree" in str(excinfo.value)
    assert deliverer.events == []


@pytest.mark.asyncio
async def test_credited_packets_same_session_still_pass_without_a_chain(
    tmp_path: Path,
) -> None:
    # No session_chain supplied (defaults to None): the OLD single-identity
    # rule still governs, and two packets sharing one session, worktree,
    # and generation still pass exactly as before.
    git = _non_trivial_git()
    packets = _two_regular_packets(
        git,
        cell_a="cell-rot",
        session_a="sess-shared",
        worktree_a="/wt-shared",
        cell_b="cell-rot",
        session_b="sess-shared",
        worktree_b="/wt-shared",
        generation_a=3,
        generation_b=3,
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events != []


# --- measured execution provenance (Sol reviewer Critical: PR #30) ---------
#
# Path coverage and identity agreement are not enough: a credited packet
# must PROVE — via the ledger's own reserve/settle blob snapshots — that
# it actually produced the credited fragment. Agent-authored evidence
# JSON is never consulted for credit decisions.


@pytest.mark.asyncio
async def test_packet_created_after_the_change_earns_no_credit_and_blocks_as_uncovered(
    tmp_path: Path,
) -> None:
    # The packet's reserved and returned blobs are IDENTICAL for both of
    # its credited paths: nothing changed during its reservation window,
    # so it was created after the change already existed on disk. It
    # earns no credit for either path, and since it is the sole
    # claimant, the emission fails closed as an uncovered path — the
    # same way a path with no claimant at all does.
    git = _non_trivial_git()
    same_blobs = {
        "src/app.py": _blob_hash("unchanged content"),
        "tests/test_app.py": _blob_hash("unchanged content 2"),
    }
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-1",
                    allowed_files=("src/app.py", "tests/test_app.py"),
                    evidence={},
                    reserved_blobs=dict(same_blobs),
                    returned_blobs=dict(same_blobs),
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match=re.escape("src/app.py")):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []
    assert list((tmp_path / "manifests").iterdir()) == []


@pytest.mark.asyncio
async def test_returned_blob_mismatching_head_content_is_rejected(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    git.head_blobs[(HEAD, "src/app.py")] = b"actual head content"
    git.head_blobs[(HEAD, "tests/test_app.py")] = b"actual test content"
    returned = {
        "src/app.py": _blob_hash("a fragment that is not actually at head"),
        "tests/test_app.py": _blob_hash("actual test content"),
    }
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-1",
                    allowed_files=("src/app.py", "tests/test_app.py"),
                    evidence={},
                    reserved_blobs=_stale_reserved(returned),
                    returned_blobs=returned,
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match=re.escape("pkt-1")) as excinfo:
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))
    assert "src/app.py" in str(excinfo.value)

    assert deliverer.events == []
    assert list((tmp_path / "manifests").iterdir()) == []


@pytest.mark.asyncio
async def test_matching_provenance_credits_only_the_actually_changed_paths(
    tmp_path: Path,
) -> None:
    # pkt-1's allowed_files also names a third path never touched by
    # this candidate's diff; only the two ACTUALLY changed paths are
    # credited, and only those need measured provenance to line up.
    git = _non_trivial_git()
    returned = _wire_head_content(
        git,
        HEAD,
        {"src/app.py": "new src content", "tests/test_app.py": "new test content"},
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-1",
                    allowed_files=(
                        "src/app.py",
                        "tests/test_app.py",
                        "unrelated/other.py",
                    ),
                    evidence={},
                    reserved_blobs=_stale_reserved(returned),
                    returned_blobs=returned,
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events == [("demo", result.event)]
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert ("packet:pkt-1", "files=2;lines=44") in manifest.verification


@pytest.mark.asyncio
async def test_rich_agent_authored_evidence_without_provenance_cannot_earn_credit(
    tmp_path: Path,
) -> None:
    # The evidence dict looks entirely legitimate (diff summary, proof
    # references) but the packet was never actually measured at reserve
    # or settle time — the JSON is never a substitute for the ledger's
    # own measured blobs.
    git = _non_trivial_git()
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-1",
                    allowed_files=("src/app.py", "tests/test_app.py"),
                    evidence={
                        "diff": "clean, matches red test",
                        "red_proof": "pytest -k red passed",
                        "green_proof": "pytest -q passed",
                    },
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match=re.escape("pkt-1")) as excinfo:
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))
    assert "provenance" in str(excinfo.value)

    assert deliverer.events == []
    assert list((tmp_path / "manifests").iterdir()) == []


@pytest.mark.asyncio
async def test_credited_legacy_packet_blocks_emission_naming_the_packet(
    tmp_path: Path,
) -> None:
    # A legacy row (created before D1's measured provenance existed):
    # ``reserved_blobs``/``returned_blobs`` are both ``None``.
    git = _non_trivial_git()
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-legacy",
                    allowed_files=("src/app.py", "tests/test_app.py"),
                    evidence={},
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match=re.escape("pkt-legacy")):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []
    assert list((tmp_path / "manifests").iterdir()) == []


@pytest.mark.asyncio
async def test_exception_packet_with_head_matching_returned_blob_is_admitted(
    tmp_path: Path,
) -> None:
    git = _custom_git((("reg.py", 5, 5), ("exc.py", 15, 15)))
    reg_returned = _wire_head_content(git, HEAD, {"reg.py": "reg new content"})
    exc_returned = _wire_head_content(git, HEAD, {"exc.py": "exc new content"})
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-reg",
                    allowed_files=("reg.py",),
                    evidence={},
                    reserved_blobs=_stale_reserved(reg_returned),
                    returned_blobs=reg_returned,
                ),
                FakePacket(
                    state="accepted",
                    packet_id="pkt-exc",
                    allowed_files=("exc.py",),
                    evidence={
                        "exception_reason": "bounded exception",
                        "expected_lines": "40",
                    },
                    returned_blobs=exc_returned,
                ),
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events == [("demo", result.event)]
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert ("packet:pkt-exc", "files=1;lines=30") in manifest.verification


@pytest.mark.asyncio
async def test_exception_packet_with_mismatching_returned_blob_is_rejected(
    tmp_path: Path,
) -> None:
    git = _custom_git((("reg.py", 5, 5), ("exc.py", 15, 15)))
    reg_returned = _wire_head_content(git, HEAD, {"reg.py": "reg new content"})
    git.head_blobs[(HEAD, "exc.py")] = b"actual exc head content"
    exc_returned = {"exc.py": _blob_hash("a totally different fragment")}
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-reg",
                    allowed_files=("reg.py",),
                    evidence={},
                    reserved_blobs=_stale_reserved(reg_returned),
                    returned_blobs=reg_returned,
                ),
                FakePacket(
                    state="accepted",
                    packet_id="pkt-exc",
                    allowed_files=("exc.py",),
                    evidence={
                        "exception_reason": "bounded exception",
                        "expected_lines": "40",
                    },
                    returned_blobs=exc_returned,
                ),
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match=re.escape("pkt-exc")) as excinfo:
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))
    assert "exc.py" in str(excinfo.value)

    assert deliverer.events == []
    assert list((tmp_path / "manifests").iterdir()) == []


@pytest.mark.asyncio
async def test_emission_side_effect_free_when_every_verifier_receipt_is_invalid(
    tmp_path: Path,
) -> None:
    verifier = StubVerifier(
        ids_by_gate={"candidate-full-gate": ("r-1", "r-2", "r-3")},
        outcomes={
            "r-1": (False, "stale: command not authorized"),
            "r-2": (False, "stale: command not authorized"),
            "r-3": (False, "stale: command not authorized"),
        },
    )
    emitter, deliverer = _emitter_with_verifier(tmp_path, clean_git(), verifier)

    with pytest.raises(EmissionBlocked, match="final candidate-gate receipt"):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []
    assert list((tmp_path / "manifests").iterdir()) == []


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


# --- head-blob verification must fail closed (Sol reviewer finding, PR #30) -
#
# ``_head_blob_hash`` must never conflate a genuinely deleted candidate
# path with any other git failure (corruption, an invalid object, a
# timeout, or anything else): only a git-proven absence may match the
# ledger's own ``"absent"`` sentinel, and it must hash raw bytes, never
# text-decoded content.


@pytest.mark.asyncio
async def test_deleted_candidate_path_matches_absent_sentinel_and_is_admitted(
    tmp_path: Path,
) -> None:
    # "gone.py" was deleted entirely (40 lines removed, none added) —
    # non-trivial by line count alone. The packet's returned blob for
    # it is the ledger's own "absent" sentinel, and the head-blob seam
    # positively proves the path is gone (returns ``None``), so the two
    # measurements agree and the candidate is admitted.
    git = _custom_git((("gone.py", 0, 40),))
    git.head_blobs[(HEAD, "gone.py")] = None
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-1",
                    allowed_files=("gone.py",),
                    evidence={},
                    reserved_blobs={"gone.py": _blob_hash("content before deletion")},
                    returned_blobs={"gone.py": "absent"},
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events == [("demo", result.event)]
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert ("packet:pkt-1", "files=1;lines=40") in manifest.verification


@pytest.mark.asyncio
async def test_timeout_reading_claimed_deleted_path_blocks_emission(
    tmp_path: Path,
) -> None:
    # The packet claims "gone.py" was deleted (returned blob is the
    # "absent" sentinel), but the head-blob seam cannot actually prove
    # that — it raises, as the real seam would on a timeout. A raised
    # failure must never be silently treated as proof of absence.
    git = _custom_git((("gone.py", 0, 40),))
    git.head_blob_errors[(HEAD, "gone.py")] = EmissionBlocked(
        "could not read 'gone.py' at " + HEAD + ": TimeoutExpired"
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-1",
                    allowed_files=("gone.py",),
                    evidence={},
                    reserved_blobs={"gone.py": _blob_hash("content before deletion")},
                    returned_blobs={"gone.py": "absent"},
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match="TimeoutExpired"):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []
    assert list((tmp_path / "manifests").iterdir()) == []


@pytest.mark.asyncio
async def test_generic_head_blob_failure_on_a_present_path_blocks_with_no_side_effects(
    tmp_path: Path,
) -> None:
    # A generic git failure distinct from proven absence — e.g. the
    # candidate object itself is unreadable — while verifying a path
    # the packet claims it actually changed (a real hash, not
    # "absent"). This must block before any manifest write or delivery.
    git = _custom_git((("src/app.py", 20, 15),))
    git.head_blob_errors[(HEAD, "src/app.py")] = EmissionBlocked(
        "git cat-file failed for 'src/app.py' at " + HEAD + " with exit "
        "code 128: fatal: unable to read blob object"
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-1",
                    allowed_files=("src/app.py",),
                    evidence={},
                    reserved_blobs={"src/app.py": "before-hash"},
                    returned_blobs={"src/app.py": _blob_hash("new content")},
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    with pytest.raises(EmissionBlocked, match="unable to read blob object"):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []
    assert list((tmp_path / "manifests").iterdir()) == []


@pytest.mark.asyncio
async def test_binary_head_content_is_hashed_byte_for_byte_without_text_decoding(
    tmp_path: Path,
) -> None:
    # The head-blob seam returns raw, non-UTF8 bytes (never text-mode
    # git output). Credit is earned exactly when the packet's returned
    # blob is the sha256 of those exact raw bytes — no decode/re-encode
    # round-trip involved.
    git = _custom_git((("bin.dat", 20, 15),))
    raw = b"\x00\xff\xfe"
    git.head_blobs[(HEAD, "bin.dat")] = raw
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-1",
                    allowed_files=("bin.dat",),
                    evidence={},
                    reserved_blobs={"bin.dat": "before-hash"},
                    returned_blobs={"bin.dat": hashlib.sha256(raw).hexdigest()},
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(tmp_path, git, packets)

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events == [("demo", result.event)]
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert ("packet:pkt-1", "files=1;lines=35") in manifest.verification


def test_default_head_blob_ls_tree_nonzero_exit_blocks(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Repository corruption / an unknown candidate object: ``git
    # ls-tree`` itself fails (nonzero exit). This must never be
    # confused with "path absent" — absence is proven ONLY by exit 0
    # with empty stdout.
    def fake_run(
        args: tuple[str, ...], **kwargs: object
    ) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(
            args, 128, stdout=b"", stderr=b"fatal: not a valid object name"
        )

    monkeypatch.setattr(emission_module.subprocess, "run", fake_run)

    with pytest.raises(EmissionBlocked, match="ls-tree failed"):
        emission_module._default_head_blob(tmp_path, "d" * 40, "some/path.py")


def test_default_head_blob_cat_file_nonzero_exit_blocks(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # The path is proven present by ``ls-tree``, but the follow-up
    # ``git cat-file`` read fails — a generic git failure distinct
    # from path absence, which must still fail closed.
    def fake_run(
        args: tuple[str, ...], **kwargs: object
    ) -> subprocess.CompletedProcess[bytes]:
        if args[3] == "ls-tree":
            return subprocess.CompletedProcess(
                args,
                0,
                stdout=b"100644 blob " + b"a" * 40 + b"\tsome/path.py\n",
                stderr=b"",
            )
        return subprocess.CompletedProcess(
            args, 1, stdout=b"", stderr=b"fatal: unable to read blob object"
        )

    monkeypatch.setattr(emission_module.subprocess, "run", fake_run)

    with pytest.raises(EmissionBlocked, match="cat-file failed"):
        emission_module._default_head_blob(tmp_path, "d" * 40, "some/path.py")


def test_default_head_blob_against_a_real_tmp_git_repo(tmp_path: Path) -> None:
    # Integration-style: a REAL git repository (init'd inside tmp_path,
    # which is permitted — this mutates only the disposable tmp repo,
    # never the actual clone), a committed binary file read back
    # exactly byte-for-byte through the default seam, and a
    # nonexistent path proven absent.
    repo = tmp_path / "repo"
    repo.mkdir()
    run = lambda *args: subprocess.run(  # noqa: E731
        ("git", *args), cwd=repo, check=True, capture_output=True
    )
    run("init", "-q")
    run("config", "user.email", "a@example.com")
    run("config", "user.name", "Test")
    binary_path = repo / "assets" / "img.bin"
    binary_path.parent.mkdir(parents=True)
    raw = b"\x00\xff\xfeBINARY\x01\x02\xfd"
    binary_path.write_bytes(raw)
    run("add", "-A")
    run("commit", "-q", "-m", "add binary asset")
    head = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    content = emission_module._default_head_blob(repo, head, "assets/img.bin")
    assert content == raw

    absent = emission_module._default_head_blob(
        repo, head, "assets/does-not-exist.bin"
    )
    assert absent is None


# --- INFRA-197 packet G2: the one-time provenance grandfather ---------------
#
# A durable operator decision (infra-197-provenance-grandfather-20260830-v1)
# authorizes a ONE-TIME transition grandfather for enumerated residual paths
# that predate the delegation-evidence gate: a binding receipt ties the
# exact final candidate SHA to the exact SHA-256 blobs of only those paths.
# It is single-use (it can only ever match one candidate SHA), grants no
# general bypass, and is invalid on any path/blob/candidate drift. These
# tests prove the two narrow rescue points and their fail-closed guards.


def _grandfather_lookup(
    candidate_sha: str, blobs: dict[str, str]
) -> Callable[[], dict[str, object] | None]:
    return lambda: {"candidate_sha": candidate_sha, "blobs": blobs}


@pytest.mark.asyncio
async def test_no_binding_uncovered_path_fails_closed_unchanged(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-partial",
                    allowed_files=("src/app.py",),
                    evidence={},
                )
            ]
        }
    )
    emitter, deliverer = _emitter_with_packets(
        tmp_path, git, packets, grandfather_binding=None
    )

    with pytest.raises(EmissionBlocked, match=re.escape("tests/test_app.py")):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []


@pytest.mark.asyncio
async def test_wrong_candidate_sha_never_rescues_uncovered_path(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    # The recorded blob is the ACTUAL matching content for the uncovered
    # path, but the binding is bound to a different candidate SHA — the
    # rescue must never be consulted at all.
    returned = _wire_head_content(
        git, HEAD, {"tests/test_app.py": "new test content"}
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-partial",
                    allowed_files=("src/app.py",),
                    evidence={},
                )
            ]
        }
    )
    binding = _grandfather_lookup(
        "9" * 40, {"tests/test_app.py": returned["tests/test_app.py"]}
    )
    emitter, deliverer = _emitter_with_packets(
        tmp_path, git, packets, grandfather_binding=binding
    )

    with pytest.raises(EmissionBlocked, match=re.escape("tests/test_app.py")):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []


@pytest.mark.asyncio
async def test_path_missing_from_binding_blobs_fails_closed(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-partial",
                    allowed_files=("src/app.py",),
                    evidence={},
                )
            ]
        }
    )
    # The binding matches this exact candidate, but names a wholly
    # different path — the uncovered path is not in it at all.
    binding = _grandfather_lookup(HEAD, {"some/other/path.py": "a" * 64})
    emitter, deliverer = _emitter_with_packets(
        tmp_path, git, packets, grandfather_binding=binding
    )

    with pytest.raises(EmissionBlocked, match=re.escape("tests/test_app.py")):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []


@pytest.mark.asyncio
async def test_blob_mismatch_drifted_content_fails_closed(tmp_path: Path) -> None:
    git = _non_trivial_git()
    # Wire real head content for the uncovered path, but record a
    # DIFFERENT sha256 in the binding — the actual content has drifted
    # away from what the binding was made against.
    _wire_head_content(git, HEAD, {"tests/test_app.py": "new test content"})
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-partial",
                    allowed_files=("src/app.py",),
                    evidence={},
                )
            ]
        }
    )
    binding = _grandfather_lookup(HEAD, {"tests/test_app.py": "0" * 64})
    emitter, deliverer = _emitter_with_packets(
        tmp_path, git, packets, grandfather_binding=binding
    )

    with pytest.raises(EmissionBlocked, match=re.escape("tests/test_app.py")):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert deliverer.events == []


@pytest.mark.asyncio
async def test_uncovered_path_with_matching_binding_is_grandfathered(
    tmp_path: Path,
) -> None:
    # ``src/app.py`` is properly credited to an accepted packet;
    # ``tests/test_app.py`` is claimed by no packet at all but is
    # rescued by the grandfather binding.
    git = _non_trivial_git()
    src_returned = _wire_head_content(git, HEAD, {"src/app.py": "new src content"})
    test_returned = _wire_head_content(
        git, HEAD, {"tests/test_app.py": "new test content"}
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-partial",
                    allowed_files=("src/app.py",),
                    evidence={},
                    reserved_blobs=_stale_reserved(src_returned),
                    returned_blobs=src_returned,
                )
            ]
        }
    )
    binding = _grandfather_lookup(
        HEAD, {"tests/test_app.py": test_returned["tests/test_app.py"]}
    )
    emitter, deliverer = _emitter_with_packets(
        tmp_path, git, packets, grandfather_binding=binding
    )

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events == [("demo", result.event)]
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert ("packet:pkt-partial", "files=1;lines=35") in manifest.verification
    assert (
        "grandfathered:tests/test_app.py",
        f"blob={test_returned['tests/test_app.py'][:12]}",
    ) in manifest.verification


@pytest.mark.asyncio
async def test_fully_uncovered_diff_rescued_entirely_by_the_binding(
    tmp_path: Path,
) -> None:
    # Every changed path is covered by ONE accepted packet (required —
    # ``_enforce_delegation_evidence`` still refuses when there are zero
    # accepted packets at all, regardless of the binding), but that
    # packet's scope names neither changed path, so both paths must be
    # rescued purely by the grandfather binding.
    git = _non_trivial_git()
    returned = _wire_head_content(
        git,
        HEAD,
        {"src/app.py": "new src content", "tests/test_app.py": "new test content"},
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-unrelated",
                    allowed_files=("some/other/file.py",),
                    evidence={},
                )
            ]
        }
    )
    binding = _grandfather_lookup(HEAD, dict(returned))
    emitter, deliverer = _emitter_with_packets(
        tmp_path, git, packets, grandfather_binding=binding
    )

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events == [("demo", result.event)]
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert (
        "grandfathered:src/app.py",
        f"blob={returned['src/app.py'][:12]}",
    ) in manifest.verification
    assert (
        "grandfathered:tests/test_app.py",
        f"blob={returned['tests/test_app.py'][:12]}",
    ) in manifest.verification
    assert not any(
        entry[0].startswith("packet:") for entry in manifest.verification
    )


@pytest.mark.asyncio
async def test_packet_credited_path_with_blob_drift_is_rescued_by_binding(
    tmp_path: Path,
) -> None:
    # ``src/app.py`` is properly credited (packet's returned blob matches
    # the real head content). ``tests/test_app.py`` is claimed by the
    # SAME packet, but the packet's own returned-blob measurement does
    # not match the real head content (drift) — the grandfather binding
    # rescues exactly that one path, and the packet is credited only for
    # the path it actually proved.
    git = _non_trivial_git()
    returned = _wire_head_content(
        git,
        HEAD,
        {"src/app.py": "new src content", "tests/test_app.py": "new test content"},
    )
    drifted_returned = {
        "src/app.py": returned["src/app.py"],
        "tests/test_app.py": "0" * 64,
    }
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-1",
                    allowed_files=("src/app.py", "tests/test_app.py"),
                    evidence={},
                    reserved_blobs=_stale_reserved(drifted_returned),
                    returned_blobs=drifted_returned,
                ),
            ]
        }
    )
    binding = _grandfather_lookup(
        HEAD, {"tests/test_app.py": returned["tests/test_app.py"]}
    )
    emitter, deliverer = _emitter_with_packets(
        tmp_path, git, packets, grandfather_binding=binding
    )

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    assert deliverer.events == [("demo", result.event)]
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert ("packet:pkt-1", "files=1;lines=35") in manifest.verification
    assert (
        "grandfathered:tests/test_app.py",
        f"blob={returned['tests/test_app.py'][:12]}",
    ) in manifest.verification


@pytest.mark.asyncio
async def test_fully_packet_covered_candidate_ignores_the_binding_entirely(
    tmp_path: Path,
) -> None:
    git = _non_trivial_git()
    returned = _wire_head_content(
        git,
        HEAD,
        {"src/app.py": "new src content", "tests/test_app.py": "new test content"},
    )
    packets = FakePackets(
        by_issue={
            "ENG-9": [
                FakePacket(
                    state="accepted",
                    packet_id="pkt-1",
                    allowed_files=("src/app.py", "tests/test_app.py"),
                    evidence={},
                    reserved_blobs=_stale_reserved(returned),
                    returned_blobs=returned,
                ),
            ]
        }
    )
    # A perfectly valid, matching-candidate binding is wired but never
    # needed — nothing in the diff requires rescue.
    binding = _grandfather_lookup(HEAD, dict(returned))
    emitter, _deliverer = _emitter_with_packets(
        tmp_path, git, packets, grandfather_binding=binding
    )

    result = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))

    assert result.delivery.delivered is True
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert ("packet:pkt-1", "files=2;lines=44") in manifest.verification
    assert not any(
        entry[0].startswith("grandfathered:") for entry in manifest.verification
    )


# --- build_grandfather_binding_lookup: real durable operator_decisions -----


@pytest.fixture
def gf_database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


def _record_binding_decision(
    database: Database, *, source_message: str, decision_id: str = "gf-binding-dec"
) -> None:
    decisions = OperatorDecisions(database)
    decisions.record_pending(
        decision_id=decision_id,
        issue_id="INFRA-197",
        project_key="demo",
        cell_id="cell-1",
        session_id="session-1",
        choice="infra-197-provenance-grandfather-binding-v1",
    )
    decisions.apply(
        decision_id=decision_id,
        status="approved",
        source_message=source_message,
    )


def test_build_grandfather_binding_lookup_reads_the_approved_binding(
    gf_database: Database,
) -> None:
    payload = json.dumps(
        {
            "candidate_sha": HEAD,
            "blobs": {"legacy/module.py": "a" * 64, "legacy/other.py": "b" * 64},
        }
    )
    with gf_database.transaction() as connection:
        connection.execute(
            "INSERT INTO operator_decisions("
            "decision_id, issue_id, project_key, cell_id, session_id, "
            "actor, choice, status, source_message, recorded_at"
            ") VALUES (?, 'INFRA-197', 'demo', 'cell-1', 'session-1', "
            "'operator', 'infra-197-provenance-grandfather-binding-v1', "
            "'approved', ?, '2026-08-30T00:00:00+00:00')",
            (emission_module._GRANDFATHER_BINDING_DECISION_ID, payload),
        )

    lookup = build_grandfather_binding_lookup(gf_database)

    assert lookup() == {
        "candidate_sha": HEAD,
        "blobs": {"legacy/module.py": "a" * 64, "legacy/other.py": "b" * 64},
    }


def test_build_grandfather_binding_lookup_no_row_returns_none(
    gf_database: Database,
) -> None:
    lookup = build_grandfather_binding_lookup(gf_database)

    assert lookup() is None


def test_build_grandfather_binding_lookup_wrong_decision_id_returns_none(
    gf_database: Database,
) -> None:
    payload = json.dumps({"candidate_sha": HEAD, "blobs": {"a.py": "a" * 64}})
    with gf_database.transaction() as connection:
        connection.execute(
            "INSERT INTO operator_decisions("
            "decision_id, issue_id, project_key, cell_id, session_id, "
            "actor, choice, status, source_message, recorded_at"
            ") VALUES ('some-other-decision', 'INFRA-197', 'demo', "
            "'cell-1', 'session-1', 'operator', 'not-the-binding', "
            "'approved', ?, '2026-08-30T00:00:00+00:00')",
            (payload,),
        )

    lookup = build_grandfather_binding_lookup(gf_database)

    assert lookup() is None


def test_build_grandfather_binding_lookup_pending_status_returns_none(
    gf_database: Database,
) -> None:
    decisions = OperatorDecisions(gf_database)
    decisions.record_pending(
        decision_id=emission_module._GRANDFATHER_BINDING_DECISION_ID,
        issue_id="INFRA-197",
        project_key="demo",
        cell_id="cell-1",
        session_id="session-1",
        choice="infra-197-provenance-grandfather-binding-v1",
    )

    lookup = build_grandfather_binding_lookup(gf_database)

    assert lookup() is None


@pytest.mark.parametrize(
    "payload",
    [
        # candidate_sha too short.
        json.dumps({"candidate_sha": "a" * 39, "blobs": {"a.py": "a" * 64}}),
        # candidate_sha uppercase (not lowercase-hex).
        json.dumps({"candidate_sha": "A" * 40, "blobs": {"a.py": "a" * 64}}),
        # a blob sha256 with the wrong length.
        json.dumps({"candidate_sha": "a" * 40, "blobs": {"a.py": "a" * 63}}),
        # a blob sha256 that is not hex.
        json.dumps({"candidate_sha": "a" * 40, "blobs": {"a.py": "g" * 64}}),
        # an unknown extra top-level key.
        json.dumps(
            {
                "candidate_sha": "a" * 40,
                "blobs": {"a.py": "a" * 64},
                "extra": "field",
            }
        ),
        # blobs is not a dict.
        json.dumps({"candidate_sha": "a" * 40, "blobs": ["a.py"]}),
        # blobs is empty.
        json.dumps({"candidate_sha": "a" * 40, "blobs": {}}),
        # not even valid JSON.
        "{not json",
        # a JSON list instead of an object.
        json.dumps(["candidate_sha", "blobs"]),
    ],
)
def test_build_grandfather_binding_lookup_malformed_payload_returns_none(
    gf_database: Database, payload: str
) -> None:
    with gf_database.transaction() as connection:
        connection.execute(
            "INSERT INTO operator_decisions("
            "decision_id, issue_id, project_key, cell_id, session_id, "
            "actor, choice, status, source_message, recorded_at"
            ") VALUES (?, 'INFRA-197', 'demo', 'cell-1', 'session-1', "
            "'operator', 'infra-197-provenance-grandfather-binding-v1', "
            "'approved', ?, '2026-08-30T00:00:00+00:00')",
            (emission_module._GRANDFATHER_BINDING_DECISION_ID, payload),
        )

    lookup = build_grandfather_binding_lookup(gf_database)

    assert lookup() is None
