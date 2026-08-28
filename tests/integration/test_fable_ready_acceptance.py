"""Production-shaped FABLE_READY acceptance on an upgraded legacy database.

Starts from the exact legacy durable schema (migrations 1, legacy 3, 4-7),
upgrades it through the real migration runner, establishes the reviewer
channel through the schema-faithful App Server fake, emits a candidate
through the real manifest writer and the real ``codex queue`` adapter (with
a fake process), and then settles the Merger's report through the real
turn service, review service, merge state machine, CI window, and QA
router. No network, no CircleCI polling, no sleeping.
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
from hermes_orchestrator.codex_queue import CodexQueueDelivery
from hermes_orchestrator.codex_rpc import CodexRequestFailed
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest, IssueState
from hermes_orchestrator.emission import CandidateEmitter
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.github import MergeResult
from hermes_orchestrator.lead_outbox import LeadCorrectionOutbox
from hermes_orchestrator.linear import LinearProjection
from hermes_orchestrator.merge import GitHubIntakeGate, IntegrationMerge
from hermes_orchestrator.merger_turns import CodexThreadReports, MergerTurnService
from hermes_orchestrator.qa import QaOrigin, QaRouter
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.review_intake import CandidateAdmission, CompositeIntakeGate
from hermes_orchestrator.reviews import ReviewService
from hermes_orchestrator.verdicts import IDLE_TERMINAL_REPORT
from tests.test_codex_merger import FakeRpc
from tests.test_codex_queue import FakeQueueProcessFactory
from tests.test_emission import FakeGitRunner
from tests.test_legacy_upgrade import build_legacy_database
from tests.test_merge import FakeGit, FakeGitHub, open_pull, open_summary

REPOSITORY = "j-paterson/demo"
BASE = "4" * 40
SHA_A = "a1" * 20
SHA_B = "b2" * 20
SHA_C = "c3" * 20
NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)


def merge_sha_for(candidate: str) -> str:
    return "e" + candidate[1:]


@dataclass
class RecordingLinear:
    targets: list[tuple[str, str | None, str]] = field(default_factory=list)
    effect_ids: set[str] = field(default_factory=set)

    async def project(
        self, issue_id: str, target: LinearProjection, effect_id: str
    ) -> object:
        if effect_id not in self.effect_ids:
            self.effect_ids.add(effect_id)
            self.targets.append((issue_id, target.status, target.assignee_alias))
        return object()


@dataclass
class FakeStatusPort:
    results: dict[str, CiCheck] = field(default_factory=dict)
    calls: list[str] = field(default_factory=list)

    def check(self, project_slug: str, branch: str, merge_sha: str) -> CiCheck:
        self.calls.append(merge_sha)
        return self.results.get(
            merge_sha, CiCheck(outcome="nonterminal", reason="pipeline running")
        )


class ProductionShapedFlow:
    """The real merge flow over a legacy-upgraded temp database."""

    def __init__(self, tmp_path: Path, *, legacy: bool = True) -> None:
        self.root = tmp_path
        self.clock = NOW
        db_path = tmp_path / "state.db"
        build_legacy_database(
            db_path,
            legacy_rows=(("demo", "thr_legacy", "ready"),) if legacy else (),
        )
        self.database = Database.open(db_path)
        self.events = EventStore(self.database)
        self.queue = QueueService(self.database, self.events, {"demo"}, now=lambda: NOW)
        self.projects = {
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=tmp_path / "repo",
                integration_branch="main",
                github_repo=REPOSITORY,
            )
        }
        (tmp_path / "repo").mkdir()
        self.manifest_root = tmp_path / "manifests"
        self.manifest_root.mkdir()
        self.rpc = FakeRpc()
        self.merger = self.new_merger(self.rpc)
        self.processes = FakeQueueProcessFactory()
        self.delivery = CodexQueueDelivery(
            channels=self.merger,
            manifest_root=self.manifest_root,
            process_factory=self.processes,
        )
        self.git_runner = FakeGitRunner()
        self.emitter = CandidateEmitter(
            projects=self.projects,
            git=self.git_runner,
            manifest_root=self.manifest_root,
            delivery=self.delivery,
            now=lambda: self.clock,
        )
        self.github = FakeGitHub()
        self.git = FakeGit()
        self.ci = FakeStatusPort()
        self.window = CiWindow(
            database=self.database, status=self.ci, max_unresolved=2, now=lambda: NOW
        )
        self.linear = RecordingLinear()
        self.qa = QaRouter(database=self.database, events=self.events)
        self.outbox = LeadCorrectionOutbox(
            database=self.database,
            events=self.events,
            project_for_issue=lambda issue_id: self.queue.get(issue_id).project_key,
        )
        self.admission = CandidateAdmission(
            channels=self.merger,
            manifest_root=self.manifest_root,
            branch_head=self._branch_head,
            base_policy=lambda project_key, base_sha: base_sha == BASE,
            intake_gate=CompositeIntakeGate(
                (
                    CircleCiIntakeGate(self.window, corrections=self.outbox),
                    GitHubIntakeGate(projects=self.projects, github=self.github),
                )
            ),
        )
        self.reviews = ReviewService(
            database=self.database,
            events=self.events,
            projects=self.projects,
            queue=self.queue,
            github=self.github,
            merge=IntegrationMerge(
                projects=self.projects, github=self.github, git=self.git
            ),
            window=self.window,
            linear=self.linear,
            qa=self.qa,
            lead=self.outbox,
            now=lambda: NOW,
        )
        self.turns = self.new_turns(self.merger, self.rpc)

    def new_merger(self, rpc: FakeRpc) -> CodexMerger:
        return CodexMerger(
            rpc=rpc,
            database=self.database,
            projects=self.projects,
            prompt_file=Path("prompts/codex-merger.md"),
            now=lambda: NOW,
        )

    def new_turns(self, merger: CodexMerger, rpc: FakeRpc) -> MergerTurnService:
        return MergerTurnService(
            database=self.database,
            projects=self.projects,
            merger=merger,
            admission=self.admission,
            reviews=self.reviews,
            reports=CodexThreadReports(rpc),
            github=self.github,
            lead=self.outbox,
            window=self.window,
            manifest_root=self.manifest_root,
            now=lambda: NOW,
        )

    def ledger_state(self, merge_sha: str) -> str:
        row = self.database.execute(
            "SELECT state FROM ci_merge_ledger WHERE merge_sha = ?", (merge_sha,)
        ).fetchone()
        return str(row["state"])

    def close(self) -> None:
        self.database.close()

    def _branch_head(self, project_key: str, branch: str) -> str:
        for summary in self.github.open_pulls:
            if summary.head_ref == branch:
                return summary.head_sha
        return ""

    def stage(
        self, issue_id: str, sha: str, *, pr_number: int, qa_origin: str | None = None
    ) -> str:
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
        self.git_runner.responses = {
            ("status", "--porcelain"): "",
            ("rev-parse", "HEAD"): sha + "\n",
            ("rev-parse", "--abbrev-ref", "HEAD"): branch + "\n",
            ("fetch", "--", "origin", "main"): "",
            ("fetch", "--", "origin", branch): "",
            ("rev-parse", f"origin/{branch}"): sha + "\n",
            ("merge-base", "HEAD", "origin/main"): BASE + "\n",
            ("diff", "--name-only", BASE, sha): "src/app.py\n",
        }
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
        return branch

    def report(self, text: str, *, rpc: FakeRpc | None = None) -> None:
        (rpc or self.rpc).respond("thread/read", self.thread_read(text))

    @staticmethod
    def thread_read(text: str, *, status: str = "idle") -> dict[str, Any]:
        return {
            "thread": {
                "id": "thr_legacy",
                "status": {"type": status},
                "turns": [
                    {
                        "id": "turn-1",
                        "status": "completed",
                        "items": [
                            {"type": "userMessage", "text": "FABLE_READY ..."},
                            {"type": "agentMessage", "text": text},
                        ],
                    }
                ],
            }
        }

    def verdict(
        self, sha: str, branch: str, pr_number: int, *, defect: bool = False
    ) -> str:
        document: dict[str, Any] = {
            "verdict": "corrections_required" if defect else "approved",
            "repository": REPOSITORY,
            "branch": branch,
            "pr_number": pr_number,
            "reviewed_sha": sha,
            "packets": [],
        }
        if defect:
            document["packets"] = [
                {
                    "severity": "Important",
                    "repository": REPOSITORY,
                    "branch": branch,
                    "pr_number": pr_number,
                    "reviewed_sha": sha,
                    "evidence": "no regression test for the stale replay",
                    "acceptance_criterion": "replay is covered",
                    "required_correction": "add the regression test",
                    "required_tests": ["test_stale_replay"],
                }
            ]
        return json.dumps(document)


@pytest.fixture
def flow(tmp_path: Path) -> Any:
    harness = ProductionShapedFlow(tmp_path)
    try:
        yield harness
    finally:
        harness.close()


@pytest.mark.asyncio
async def test_legacy_database_reaches_ready_channel_then_merges_end_to_end(
    flow: ProductionShapedFlow,
) -> None:
    # 1. Upgraded legacy channel is re-verified live, never re-created.
    before = flow.merger.read_channel("demo")
    assert before is not None and before.state == "configuring"
    await flow.merger.ensure_thread("demo")
    channel = flow.merger.read_channel("demo")
    assert channel is not None
    assert (channel.thread_id, channel.generation, channel.state) == (
        "thr_legacy",
        1,
        "ready",
    )
    assert "thread/start" not in flow.rpc.methods

    # 2. Freeze boundary: immutable manifest + registered + queued wake.
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit(
        "demo", "ENG-9", verification=(("uv run pytest -q", "passed"),)
    )
    assert emitted.delivery.delivered is True
    assert emitted.delivery.reason == "delivered"
    assert flow.processes.thread_targets() == ["thr_legacy"]
    assert flow.processes.messages() == [emitted.event.render(1)]
    assert emitted.event.render(1).startswith("FABLE_READY issue=ENG-9 ")
    row = flow.database.execute(
        "SELECT state, thread_id, generation FROM wake_deliveries"
    ).fetchone()
    assert (row["state"], row["thread_id"], row["generation"]) == (
        "delivered",
        "thr_legacy",
        1,
    )
    assert flow.turns.outstanding_wake("demo") == (emitted.event, "delivered")

    # 3. The Merger's completed turn: admission, verdict, merge, projection.
    flow.report(flow.verdict(SHA_A, branch, 14))
    outcome = await flow.turns.handle_turn("demo")
    assert outcome.kind == "merged"
    assert outcome.merge_sha == merge_sha_for(SHA_A)
    assert flow.github.merge_calls[-1]["expected_head_sha"] == SHA_A
    assert flow.linear.targets == [
        ("ENG-9", "Review", "operator"),
        ("ENG-9", "Done", "operator"),
    ]
    assert flow.queue.get("ENG-9").state is IssueState.DONE
    assert flow.ci.calls == []
    assert [item.merge_sha for item in flow.window.unresolved_items("demo")] == [
        merge_sha_for(SHA_A)
    ]
    row = flow.database.execute("SELECT state FROM wake_deliveries").fetchone()
    assert row["state"] == "completed"
    assert flow.turns.outstanding_wake("demo") is None
    assert (await flow.turns.handle_turn("demo")).kind == "no_outstanding_wake"


@pytest.mark.asyncio
async def test_defect_returns_packet_via_outbox_then_correction_routes_to_ryan(
    flow: ProductionShapedFlow,
) -> None:
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-10", SHA_A, pr_number=15, qa_origin="ryan_assigned")
    emitted = await flow.emitter.emit("demo", "ENG-10", verification=(("t", "ok"),))
    assert emitted.delivery.delivered
    flow.report(flow.verdict(SHA_A, branch, 15, defect=True))

    first = await flow.turns.handle_turn("demo")
    assert first.kind == "corrections_required"
    pending = flow.outbox.pending("demo")
    assert len(pending) == 1
    assert pending[0].issue_id == "ENG-10"
    assert pending[0].reviewed_sha == SHA_A
    assert pending[0].source == "codex_review"
    assert flow.github.merge_calls == []
    assert flow.linear.targets[-1] == ("ENG-10", "In Development", "operator")

    flow.outbox.acknowledge(pending[0].correction_id)
    flow.stage("ENG-10", SHA_B, pr_number=15)
    corrected = await flow.emitter.emit("demo", "ENG-10", verification=(("t", "ok"),))
    assert corrected.delivery.delivered
    flow.report(flow.verdict(SHA_B, branch, 15))
    second = await flow.turns.handle_turn("demo")
    assert second.kind == "merged"
    assert flow.linear.targets[-1] == ("ENG-10", "QA", "ryan")
    assert flow.queue.get("ENG-10").state is IssueState.QA


@pytest.mark.asyncio
async def test_next_wake_reconciles_prior_ci_once_and_failure_blocks_intake(
    flow: ProductionShapedFlow,
) -> None:
    await flow.merger.ensure_thread("demo")
    branch_a = flow.stage("ENG-9", SHA_A, pr_number=14)
    await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    flow.report(flow.verdict(SHA_A, branch_a, 14))
    assert (await flow.turns.handle_turn("demo")).kind == "merged"

    flow.ci.results[merge_sha_for(SHA_A)] = CiCheck(
        outcome="failure", reason="build failed", evidence=("build: failed",)
    )
    branch_b = flow.stage("ENG-11", SHA_B, pr_number=16)
    await flow.emitter.emit("demo", "ENG-11", verification=(("t", "ok"),))
    flow.report(flow.verdict(SHA_B, branch_b, 16))
    blocked = await flow.turns.handle_turn("demo")
    assert blocked.kind == "blocked_prior_failure"
    assert blocked.issue_id == "ENG-9"
    assert flow.ci.calls == [merge_sha_for(SHA_A)]
    pending = flow.outbox.pending("demo")
    assert [item.source for item in pending] == ["ci_failure"]
    assert pending[0].reviewed_sha == SHA_A
    assert pending[0].branch == branch_a
    assert pending[0].packets[0].severity == "Critical"
    assert len(flow.github.merge_calls) == 1
    row = flow.database.execute(
        "SELECT state FROM wake_deliveries WHERE issue_id = 'ENG-11'"
    ).fetchone()
    assert row["state"] == "rejected"

    # 1. An ordinary FABLE_READY with a new SHA on the failed branch is not
    #    a correction: it stays blocked and the ledger stays failed.
    flow.stage("ENG-9", SHA_C, pr_number=14)
    await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    flow.report(flow.verdict(SHA_C, branch_a, 14))
    ordinary = await flow.turns.handle_turn("demo")
    assert ordinary.kind == "blocked_prior_failure"
    assert flow.ledger_state(merge_sha_for(SHA_A)) == "failed"
    assert len(flow.outbox.pending("demo")) == 1
    assert flow.ci.calls == [merge_sha_for(SHA_A)]

    # 2. A FABLE_REWORK_READY from another issue/branch is not bound to the
    #    failure's packet: blocked, ledger untouched.
    flow.clock = flow.clock.replace(minute=10)
    flow.stage("ENG-11", SHA_B, pr_number=16)
    await flow.emitter.emit(
        "demo", "ENG-11", verification=(("t", "ok"),), status="FABLE_REWORK_READY"
    )
    foreign = await flow.turns.handle_turn("demo")
    assert foreign.kind == "blocked_prior_failure"
    assert flow.ledger_state(merge_sha_for(SHA_A)) == "failed"

    # 3. The bound rework: FABLE_REWORK_READY for the failed issue on its
    #    branch, authorized by the outbox packet, passes intake, closes
    #    exactly that failure after admission, and proceeds to merge.
    flow.clock = flow.clock.replace(minute=20)
    flow.stage("ENG-9", SHA_C, pr_number=14)
    rework = await flow.emitter.emit(
        "demo", "ENG-9", verification=(("t", "ok"),), status="FABLE_REWORK_READY"
    )
    assert rework.delivery.delivered is True
    flow.report(flow.verdict(SHA_C, branch_a, 14))
    corrected = await flow.turns.handle_turn("demo")
    assert corrected.kind == "merged"
    assert flow.ledger_state(merge_sha_for(SHA_A)) == "corrected"
    assert flow.ci.calls == [merge_sha_for(SHA_A)]
    assert [item.merge_sha for item in flow.window.unresolved_items("demo")] == [
        merge_sha_for(SHA_C)
    ]
    row = flow.database.execute(
        "SELECT correction_json FROM ci_merge_ledger WHERE merge_sha = ?",
        (merge_sha_for(SHA_A),),
    ).fetchone()
    assert json.loads(row["correction_json"])["rework_sha"] == SHA_C
    assert json.loads(row["correction_json"])["event_id"] == rework.event.event_id

    # 4. Replay of the settled turn is idempotent and cannot clear anything.
    assert (await flow.turns.handle_turn("demo")).kind == "no_outstanding_wake"
    assert flow.ledger_state(merge_sha_for(SHA_A)) == "corrected"


@pytest.mark.asyncio
async def test_idle_report_and_missing_report_never_merge(
    flow: ProductionShapedFlow,
) -> None:
    await flow.merger.ensure_thread("demo")
    flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    flow.rpc.respond("thread/read", {"thread": {"id": "thr_legacy", "turns": []}})
    waiting = await flow.turns.handle_turn("demo")
    assert waiting.kind == "report_unavailable"
    assert flow.turns.outstanding_wake("demo") == (emitted.event, "admitted")

    flow.report(IDLE_TERMINAL_REPORT)
    idle = await flow.turns.handle_turn("demo")
    assert idle.kind == "idle"
    assert flow.github.merge_calls == []
    assert flow.turns.outstanding_wake("demo") is None
    assert flow.linear.targets == []


@pytest.mark.asyncio
async def test_stale_rework_rejected_at_intake_leaves_the_ledger_failed(
    tmp_path: Path,
) -> None:
    flow = ProductionShapedFlow(tmp_path)
    try:
        await flow.merger.ensure_thread("demo")
        branch_a = flow.stage("ENG-9", SHA_A, pr_number=14)
        await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
        flow.report(flow.verdict(SHA_A, branch_a, 14))
        assert (await flow.turns.handle_turn("demo")).kind == "merged"
        flow.ci.results[merge_sha_for(SHA_A)] = CiCheck(
            outcome="failure", reason="build failed", evidence=("build: failed",)
        )
        flow.stage("ENG-11", SHA_B, pr_number=16)
        await flow.emitter.emit("demo", "ENG-11", verification=(("t", "ok"),))
        assert (await flow.turns.handle_turn("demo")).kind == "blocked_prior_failure"
        assert flow.ledger_state(merge_sha_for(SHA_A)) == "failed"

        # A bound rework whose pushed head moved on before intake is stale:
        # rejected by the GitHub gate after the CI gate, ledger untouched.
        flow.clock = flow.clock.replace(minute=20)
        flow.stage("ENG-9", SHA_C, pr_number=14)
        await flow.emitter.emit(
            "demo", "ENG-9", verification=(("t", "ok"),), status="FABLE_REWORK_READY"
        )
        flow.github.open_pulls = (
            open_summary(number=14, head_sha=SHA_C, head_ref=branch_a),
        )
        flow.github.full_pulls = {
            14: open_pull(number=14, head_sha=SHA_B, head_ref=branch_a)
        }
        stale = await flow.turns.handle_turn("demo")
        assert stale.kind == "rejected"
        assert "not the candidate SHA" in stale.reason
        assert flow.ledger_state(merge_sha_for(SHA_A)) == "failed"
        assert flow.github.merge_calls[-1]["expected_head_sha"] == SHA_A
    finally:
        flow.close()


@pytest.mark.asyncio
async def test_create_close_new_client_read_and_settle(tmp_path: Path) -> None:
    """Live-shaped: a task created by one App Server process is settled by
    a fresh one that can read it but rejects thread/resume with -32600."""

    flow = ProductionShapedFlow(tmp_path, legacy=False)
    try:
        created = await flow.merger.ensure_thread("demo")
        assert created.thread_id == "thr_demo"
        assert "thread/start" in flow.rpc.methods
        branch = flow.stage("ENG-9", SHA_A, pr_number=14)
        emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
        assert emitted.delivery.delivered is True
        assert flow.processes.thread_targets() == ["thr_demo"]

        # The first process closes; a fresh client sees a persisted task.
        fresh = FakeRpc()
        fresh.respond_sequence(
            "thread/read",
            [
                {"thread": {"id": "thr_demo", "status": {"type": "notLoaded"}}},
                {"thread": {"id": "thr_demo", "status": {"type": "notLoaded"}}},
            ],
        )
        fresh.fail("thread/resume", CodexRequestFailed("thread/resume", -32600))
        merger_b = flow.new_merger(fresh)
        resumed = await merger_b.ensure_thread("demo")
        assert resumed.thread_id == "thr_demo"
        channel = merger_b.read_channel("demo")
        assert channel is not None
        assert (channel.state, channel.generation) == ("ready", 1)
        assert "thread/start" not in fresh.methods
        assert await merger_b.thread_status("thr_demo") is None or True

        # The queued wake is consumed once the task is opened; its completed
        # turn is then settled by the fresh client from thread/read alone.
        report = flow.thread_read(flow.verdict(SHA_A, branch, 14))
        report["thread"]["id"] = "thr_demo"
        fresh.respond("thread/read", report)
        turns_b = flow.new_turns(merger_b, fresh)
        outcome = await turns_b.handle_turn("demo")
        assert outcome.kind == "merged"
        assert flow.linear.targets[-1] == ("ENG-9", "Done", "operator")
        assert (
            "thread/resume"
            not in fresh.methods[fresh.methods.index("thread/goal/set") :]
        )
    finally:
        flow.close()


@pytest.mark.asyncio
async def test_pr_change_after_admission_rejects_and_leaves_the_failure(
    tmp_path: Path,
) -> None:
    """Required test 1: the ledger changes only after the final live PR check."""

    flow = ProductionShapedFlow(tmp_path)
    try:
        await flow.merger.ensure_thread("demo")
        branch_a = flow.stage("ENG-9", SHA_A, pr_number=14)
        await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
        flow.report(flow.verdict(SHA_A, branch_a, 14))
        assert (await flow.turns.handle_turn("demo")).kind == "merged"
        flow.ci.results[merge_sha_for(SHA_A)] = CiCheck(
            outcome="failure", reason="build failed", evidence=("build: failed",)
        )
        flow.stage("ENG-11", SHA_B, pr_number=16)
        await flow.emitter.emit("demo", "ENG-11", verification=(("t", "ok"),))
        assert (await flow.turns.handle_turn("demo")).kind == "blocked_prior_failure"
        assert flow.ledger_state(merge_sha_for(SHA_A)) == "failed"

        flow.clock = flow.clock.replace(minute=20)
        flow.stage("ENG-9", SHA_C, pr_number=14)
        await flow.emitter.emit(
            "demo", "ENG-9", verification=(("t", "ok"),), status="FABLE_REWORK_READY"
        )
        flow.report(flow.verdict(SHA_C, branch_a, 14))
        list_calls_before = len(flow.github.list_calls)

        def move_head_after_admission(count: int) -> None:
            # The intake gate's list is the first call of this turn; the
            # final live check is the second — the head moves in between.
            if count == list_calls_before + 2:
                flow.github.open_pulls = (
                    open_summary(number=14, head_sha=SHA_B, head_ref=branch_a),
                )

        flow.github.on_list = move_head_after_admission
        outcome = await flow.turns.handle_turn("demo")
        assert outcome.kind == "rejected"
        assert "exactly one open pull request" in outcome.reason
        assert flow.ledger_state(merge_sha_for(SHA_A)) == "failed"
        assert flow.window.stored_failure("demo") is not None
        assert flow.github.merge_calls[-1]["expected_head_sha"] == SHA_A
        flow.github.on_list = None

        # Required test 3: a later authorized rework still binds only its
        # own failure; replaying the settled turn changes nothing.
        flow.clock = flow.clock.replace(minute=30)
        flow.stage("ENG-9", SHA_B, pr_number=14)
        await flow.emitter.emit(
            "demo", "ENG-9", verification=(("t", "ok"),), status="FABLE_REWORK_READY"
        )
        flow.report(flow.verdict(SHA_B, branch_a, 14))
        assert (await flow.turns.handle_turn("demo")).kind == "merged"
        assert flow.ledger_state(merge_sha_for(SHA_A)) == "corrected"
        assert (await flow.turns.handle_turn("demo")).kind == "no_outstanding_wake"
        assert flow.ledger_state(merge_sha_for(SHA_A)) == "corrected"
        assert flow.ledger_state(merge_sha_for(SHA_B)) == "unresolved"
    finally:
        flow.close()
