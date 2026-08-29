"""End-to-end Codex merge acceptance: intake, review, merge, CI window, Linear.

Every external boundary is a local fake and every durable state lives in a
temporary SQLite database. No network, no CircleCI polling, no sleeping.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.ci_window import CircleCiIntakeGate, CiWindow
from hermes_orchestrator.circleci import CiCheck
from hermes_orchestrator.codex_merger import CodexMerger
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest, IssueState
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.github import (
    MergeBlocked,
    MergeEffectJournal,
    MergeResult,
)
from hermes_orchestrator.linear import LinearProjection
from hermes_orchestrator.manifests import (
    MANIFEST_VERSION,
    CandidateManifest,
    read_manifest_snapshot,
    wake_event_for,
    write_manifest,
)
from hermes_orchestrator.merge import GitHubIntakeGate, IntegrationMerge
from hermes_orchestrator.qa import QaOrigin, QaRouter
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.review_intake import (
    CandidateAdmission,
    CandidateRejected,
    CompositeIntakeGate,
)
from hermes_orchestrator.reviews import ReviewService
from hermes_orchestrator.settlement import MergeSettlements
from hermes_orchestrator.verdicts import CorrectionPacket, VerdictBinding, parse_verdict
from tests.test_merge import FakeGit, FakeGitHub, open_pull, open_summary
from tests.test_review_intake import NullRpc, stored_channel

REPOSITORY = "j-paterson/demo"
BASE = "4" * 40
BAD = "bad111".ljust(40, "a")
GOOD = "00d222".ljust(40, "b")
THIRD = "333".ljust(40, "c")
FOURTH = "444".ljust(40, "d")
NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)
PROJECTS = {
    "demo": ProjectConfig(
        linear_team="infrastructure",
        repo_path=Path("/repo/demo"),
        integration_branch="main",
        github_repo=REPOSITORY,
    )
}


def merge_sha_for(candidate: str) -> str:
    return ("m" + candidate[1:]).replace("m", "e", 1)


@dataclass
class RecordingLinear:
    targets: list[tuple[str, str | None, str]] = field(default_factory=list)
    effect_ids: list[str] = field(default_factory=list)

    @property
    def last_target(self) -> tuple[str, str | None, str] | None:
        return self.targets[-1] if self.targets else None

    async def project(
        self, issue_id: str, target: LinearProjection, effect_id: str
    ) -> object:
        if effect_id in self.effect_ids:
            return object()
        self.effect_ids.append(effect_id)
        self.targets.append((issue_id, target.status, target.assignee_alias))
        return object()


@dataclass
class RecordingLead:
    deliveries: list[tuple[str, tuple[CorrectionPacket, ...]]] = field(
        default_factory=list
    )
    sources: list[str] = field(default_factory=list)

    @property
    def last_packet(self) -> CorrectionPacket:
        return self.deliveries[-1][1][0]

    def deliver(
        self,
        issue_id: str,
        packets: tuple[CorrectionPacket, ...],
        *,
        source: str = "codex_review",
    ) -> None:
        self.deliveries.append((issue_id, packets))
        self.sources.append(source)


@dataclass
class FakeStatusPort:
    results: dict[str, CiCheck] = field(default_factory=dict)
    calls: list[str] = field(default_factory=list)

    def check(self, project_slug: str, branch: str, merge_sha: str) -> CiCheck:
        self.calls.append(merge_sha)
        return self.results.get(
            merge_sha, CiCheck(outcome="nonterminal", reason="no pipeline yet")
        )


class JournalledGitHub:
    """The fake transport fronted by the real merge-effect journal.

    Mirrors ``GitHubClient.merge``'s fence exactly — claim, attempted,
    mutate, complete — so a replayed effect id returns the completed
    response without a second transport call, just like production.
    Reads delegate untouched.
    """

    def __init__(self, inner: FakeGitHub, database: Database) -> None:
        self.inner = inner
        self.journal = MergeEffectJournal(database)

    def merge(
        self,
        repository: str,
        number: int,
        *,
        expected_head_sha: str,
        expected_head_ref: str,
        expected_base: str,
        effect_id: str,
        merge_method: str = "squash",
    ) -> MergeResult:
        claim = self.journal.claim(
            effect_id,
            request={
                "repository": repository,
                "number": number,
                "sha": expected_head_sha,
                "head_ref": expected_head_ref,
                "base": expected_base,
                "merge_method": merge_method,
            },
        )
        if claim.completed_response is not None:
            return MergeResult(
                str(claim.completed_response["sha"]), already_merged=True
            )
        assert claim.token is not None
        self.journal.mark_attempted(effect_id, token=claim.token)
        result = self.inner.merge(
            repository,
            number,
            expected_head_sha=expected_head_sha,
            expected_head_ref=expected_head_ref,
            expected_base=expected_base,
            effect_id=effect_id,
            merge_method=merge_method,
        )
        self.journal.complete(
            effect_id,
            {"merged": True, "sha": result.merge_sha},
            token=claim.token,
        )
        return result

    def get_pull_request(self, repository: str, number: int) -> Any:
        return self.inner.get_pull_request(repository, number)

    def list_open_pulls(self, repository: str, *, base: str) -> Any:
        return self.inner.list_open_pulls(repository, base=base)


class Acceptance:
    """Local composition of the full merge flow with recording fakes."""

    def __init__(self, tmp_path: Path) -> None:
        self.root = tmp_path
        self.database = Database.open(tmp_path / "state.db")
        self.events = EventStore(self.database)
        self.queue = QueueService(
            self.database, self.events, {"demo"}, now=lambda: NOW
        )
        self.merger = CodexMerger(
            rpc=NullRpc(),
            database=self.database,
            projects=PROJECTS,
            prompt_file=Path(__file__).parent.parent.parent
            / "prompts"
            / "codex-merger.md",
            now=lambda: NOW,
        )
        stored_channel(self.database)
        self.github = FakeGitHub()
        self.git = FakeGit()
        self.ci = FakeStatusPort()
        self.window = CiWindow(
            database=self.database,
            status=self.ci,
            max_unresolved=2,
            now=lambda: NOW,
        )
        self.linear = RecordingLinear()
        self.claude = RecordingLead()
        self.qa = QaRouter(database=self.database, events=self.events)
        self.head = ""
        self.admission = CandidateAdmission(
            channels=self.merger,
            manifest_root=tmp_path,
            branch_head=lambda project_key, branch: self.head,
            base_policy=lambda project_key, base_sha: True,
            intake_gate=CompositeIntakeGate(
                (
                    CircleCiIntakeGate(self.window),
                    GitHubIntakeGate(projects=PROJECTS, github=self.github),
                )
            ),
        )
        self.settlements = MergeSettlements(
            self.database, self.events, now=lambda: NOW
        )
        self.guarded_github = JournalledGitHub(self.github, self.database)
        self.service = ReviewService(
            database=self.database,
            events=self.events,
            projects=PROJECTS,
            queue=self.queue,
            github=self.guarded_github,
            merge=IntegrationMerge(
                projects=PROJECTS, github=self.guarded_github, git=self.git
            ),
            window=self.window,
            linear=self.linear,
            qa=self.qa,
            lead=self.claude,
            settlements=self.settlements,
            merge_journal=self.guarded_github.journal,
            now=lambda: NOW,
        )
        self._events = 0

    def close(self) -> None:
        self.database.close()

    def prepare(
        self,
        issue_id: str,
        sha: str,
        *,
        qa_origin: str | None = None,
        pr_number: int = 14,
    ) -> Any:
        branch = f"feature/{issue_id.lower()}"
        try:
            self.queue.get(issue_id)
        except KeyError:
            self.queue.admit(
                AdmissionRequest(
                    issue_id=issue_id,
                    project_key="demo",
                    linear_priority=1,
                    admitted_by="operator",
                    instruction_id=f"chat-{issue_id}",
                )
            )
        if qa_origin is not None:
            self.qa.record_origin(issue_id, QaOrigin(qa_origin))
        self.github.open_pulls = (
            open_summary(number=pr_number, head_sha=sha, head_ref=branch),
        )
        self.github.full_pulls = {
            pr_number: open_pull(number=pr_number, head_sha=sha, head_ref=branch)
        }
        merge_sha = merge_sha_for(sha)
        self.github.merge_result = MergeResult(merge_sha, already_merged=False)
        self.git.ancestor[(merge_sha, "origin/main")] = True
        self.git.ancestor[(sha, merge_sha)] = False
        self.git.trees[merge_sha] = "tree-" + sha
        self.git.trees[sha] = "tree-" + sha
        self.head = sha
        self._events += 1
        manifest = CandidateManifest(
            manifest_version=MANIFEST_VERSION,
            event_id=f"evt-{self._events}",
            status="FABLE_READY",
            candidate_sha=sha,
            base_sha=BASE,
            branch=branch,
            linear_issues=(issue_id,),
            changed_files=("src/app.py",),
            verification=(("uv run pytest -q", "passed"),),
            blockers=(),
            created_at=NOW.isoformat(),
        )
        path = write_manifest(self.root, manifest, head_sha=sha)
        snapshot = read_manifest_snapshot(path, root=self.root)
        event = wake_event_for(manifest, path)
        registration = self.merger.register_wake("demo", event, manifest=snapshot)
        assert registration.state == "pending"
        assert self.merger.record_wake_delivery_success(
            "demo",
            thread_id=registration.thread_id,
            generation=registration.generation,
            event_id=event.event_id,
            claim_token=registration.claim_token,
            candidate_sha=sha,
        )
        return event, branch, pr_number

    async def submit(
        self,
        issue_id: str,
        sha: str,
        *,
        qa_origin: str | None = None,
        defect: bool = False,
        pr_number: int = 14,
    ) -> Any:
        event, branch, number = self.prepare(
            issue_id, sha, qa_origin=qa_origin, pr_number=pr_number
        )
        admitted = self.admission.admit("demo", event, received_generation=1)
        binding = VerdictBinding(
            repository=REPOSITORY, branch=branch, pr_number=number, reviewed_sha=sha
        )
        document: dict[str, Any] = {
            "verdict": "corrections_required" if defect else "approved",
            "repository": REPOSITORY,
            "branch": branch,
            "pr_number": number,
            "reviewed_sha": sha,
            "packets": [],
        }
        if defect:
            document["packets"] = [
                {
                    "severity": "Critical",
                    "repository": REPOSITORY,
                    "branch": branch,
                    "pr_number": number,
                    "reviewed_sha": sha,
                    "evidence": "merge proof accepts an unrelated tree",
                    "acceptance_criterion": "proof binds the reviewed tree",
                    "required_correction": "compare trees before proving",
                    "required_tests": ["test_unrelated_tree_is_rejected"],
                }
            ]
        verdict = parse_verdict(json.dumps(document), expected=binding)
        try:
            return await self.service.complete_review(admitted, issue_id, verdict)
        finally:
            assert self.merger.complete_admitted_wake("demo", event.event_id)


@pytest.fixture
def acceptance(tmp_path: Path) -> Any:
    harness = Acceptance(tmp_path)
    try:
        yield harness
    finally:
        harness.close()


@pytest.mark.asyncio
async def test_codex_defect_then_corrected_merge(acceptance: Acceptance) -> None:
    first = await acceptance.submit(
        "ENG-10", sha=BAD, qa_origin="ryan_assigned", defect=True
    )
    assert first.state == "corrections_required"
    assert acceptance.claude.last_packet.reviewed_sha == BAD
    assert acceptance.claude.last_packet.severity == "Critical"
    assert acceptance.github.merge_calls == []
    assert acceptance.linear.last_target == ("ENG-10", "In Development", "operator")
    assert acceptance.queue.get("ENG-10").state is IssueState.IN_DEVELOPMENT

    second = await acceptance.submit("ENG-10", sha=GOOD)
    assert second.state == "merged"
    assert second.merge_sha == merge_sha_for(GOOD)
    assert acceptance.linear.last_target == ("ENG-10", "QA", "ryan")
    assert acceptance.queue.get("ENG-10").state is IssueState.QA
    assert ("is_ancestor", "/repo/demo", merge_sha_for(GOOD), "origin/main") in (
        acceptance.git.calls
    )
    assert acceptance.github.merge_calls[-1]["expected_head_sha"] == GOOD
    assert [item.merge_sha for item in acceptance.window.unresolved_items("demo")] == [
        merge_sha_for(GOOD)
    ]
    assert acceptance.ci.calls == []


@pytest.mark.asyncio
async def test_ordinary_merge_completes_done(acceptance: Acceptance) -> None:
    outcome = await acceptance.submit("ENG-9", sha=GOOD)
    assert outcome.state == "merged"
    assert acceptance.linear.targets == [
        ("ENG-9", "Review", "operator"),
        ("ENG-9", "Done", "operator"),
    ]
    assert acceptance.queue.get("ENG-9").state is IssueState.DONE


@pytest.mark.asyncio
async def test_next_intake_reconciles_prior_merge_exactly_once(
    acceptance: Acceptance,
) -> None:
    await acceptance.submit("ENG-9", sha=GOOD)
    acceptance.ci.results[merge_sha_for(GOOD)] = CiCheck(
        outcome="success", reason="all workflows succeeded"
    )
    outcome = await acceptance.submit("ENG-11", sha=THIRD, pr_number=15)
    assert outcome.state == "merged"
    assert acceptance.ci.calls == [merge_sha_for(GOOD)]
    assert [item.merge_sha for item in acceptance.window.unresolved_items("demo")] == [
        merge_sha_for(THIRD)
    ]


@pytest.mark.asyncio
async def test_prior_ci_failure_blocks_intake_with_critical_packet(
    acceptance: Acceptance,
) -> None:
    await acceptance.submit("ENG-9", sha=GOOD)
    acceptance.ci.results[merge_sha_for(GOOD)] = CiCheck(
        outcome="failure", reason="workflow build failed", evidence=("build: failed",)
    )
    with pytest.raises(CandidateRejected) as rejected:
        await acceptance.submit("ENG-11", sha=THIRD, pr_number=15)
    packet = rejected.value.packet
    assert packet.severity == "Critical"
    assert packet.reviewed_sha == GOOD
    assert packet.branch == "feature/eng-9"
    assert acceptance.github.merge_calls[-1]["expected_head_sha"] == GOOD
    assert len(acceptance.github.merge_calls) == 1


@pytest.mark.asyncio
async def test_two_unresolved_merges_defer_a_third(acceptance: Acceptance) -> None:
    await acceptance.submit("ENG-9", sha=GOOD)
    await acceptance.submit("ENG-11", sha=THIRD, pr_number=15)
    assert len(acceptance.window.unresolved_items("demo")) == 2
    with pytest.raises(CandidateRejected, match="deferred"):
        await acceptance.submit("ENG-12", sha=FOURTH, pr_number=16)
    assert len(acceptance.github.merge_calls) == 2
    assert len(acceptance.window.unresolved_items("demo")) == 2


@pytest.mark.asyncio
async def test_stale_head_after_approval_never_merges(
    acceptance: Acceptance,
) -> None:
    event, branch, number = acceptance.prepare("ENG-9", GOOD)
    admitted = acceptance.admission.admit("demo", event, received_generation=1)
    verdict = parse_verdict(
        json.dumps(
            {
                "verdict": "approved",
                "repository": REPOSITORY,
                "branch": branch,
                "pr_number": number,
                "reviewed_sha": GOOD,
                "packets": [],
            }
        ),
        expected=VerdictBinding(REPOSITORY, branch, number, GOOD),
    )
    review = await acceptance.service.record_verdict(admitted, "ENG-9", verdict)
    # The lead pushes again before the merge runs.
    acceptance.github.full_pulls[number] = open_pull(
        number=number, head_sha=THIRD, head_ref=branch
    )
    outcome = await acceptance.service.merge_approved(review.review_id)
    assert outcome.state == "stale"
    assert acceptance.github.merge_calls == []
    assert acceptance.linear.targets == [("ENG-9", "Review", "operator")]
    assert acceptance.queue.get("ENG-9").state is IssueState.REVIEW


@pytest.mark.asyncio
async def test_merge_block_leaves_review_open(acceptance: Acceptance) -> None:
    acceptance.github.merge_error = MergeBlocked("base moved")
    outcome = await acceptance.submit("ENG-9", sha=GOOD)
    assert outcome.state == "blocked"
    assert acceptance.linear.targets == [("ENG-9", "Review", "operator")]
    assert acceptance.window.unresolved_items("demo") == ()
    assert acceptance.queue.get("ENG-9").state is IssueState.REVIEW


@pytest.mark.asyncio
async def test_proof_failure_requires_reconciliation_not_done(
    acceptance: Acceptance,
) -> None:
    event, branch, number = acceptance.prepare("ENG-9", GOOD)
    acceptance.git.ancestor[(merge_sha_for(GOOD), "origin/main")] = False
    admitted = acceptance.admission.admit("demo", event, received_generation=1)
    verdict = parse_verdict(
        json.dumps(
            {
                "verdict": "approved",
                "repository": REPOSITORY,
                "branch": branch,
                "pr_number": number,
                "reviewed_sha": GOOD,
                "packets": [],
            }
        ),
        expected=VerdictBinding(REPOSITORY, branch, number, GOOD),
    )
    outcome = await acceptance.service.complete_review(admitted, "ENG-9", verdict)
    assert outcome.state == "reconciliation_required"
    assert acceptance.linear.targets == [("ENG-9", "Review", "operator")]
    assert acceptance.window.unresolved_items("demo") == ()


@pytest.mark.asyncio
async def test_merge_approved_is_idempotent_after_restart(
    acceptance: Acceptance,
) -> None:
    outcome = await acceptance.submit("ENG-9", sha=GOOD)
    replay = await acceptance.service.merge_approved(outcome.review_id)
    assert replay == outcome
    assert len(acceptance.github.merge_calls) == 1
    assert len(acceptance.linear.targets) == 2


@pytest.mark.asyncio
async def test_qa_rejection_returns_work_to_the_lead(acceptance: Acceptance) -> None:
    outcome = await acceptance.submit("ENG-10", sha=GOOD, qa_origin="ryan_assigned")
    assert acceptance.linear.last_target == ("ENG-10", "QA", "ryan")

    rejection = await acceptance.service.reject_from_qa(
        "ENG-10", reason="settings page still 500s", evidence="QA-run-42"
    )
    assert rejection.state == "qa_rejected"
    assert rejection.review_id == outcome.review_id
    assert acceptance.linear.last_target == ("ENG-10", "In Development", "operator")
    assert acceptance.claude.last_packet.reviewed_sha == GOOD
    assert acceptance.claude.last_packet.evidence == "settings page still 500s"
    assert acceptance.queue.get("ENG-10").state is IssueState.IN_DEVELOPMENT
    assert acceptance.service.current_review("ENG-10") is None

    corrected = await acceptance.submit("ENG-10", sha=THIRD, pr_number=15)
    assert corrected.state == "merged"
    assert acceptance.linear.last_target == ("ENG-10", "QA", "ryan")


@pytest.mark.asyncio
async def test_rejecting_unmerged_work_fails_closed(acceptance: Acceptance) -> None:
    with pytest.raises(ValueError, match="merged"):
        await acceptance.service.reject_from_qa(
            "ENG-9", reason="nothing merged", evidence="none"
        )
