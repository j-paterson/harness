"""Verify Merger turn handling outside the full acceptance flow."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.circleci import CiCheck
from hermes_orchestrator.codex_rpc import RpcNotification
from hermes_orchestrator.merger_turns import CodexThreadReports, SubmissionRejected
from hermes_orchestrator.verdicts import IDLE_TERMINAL_REPORT
from tests.integration.test_fable_ready_acceptance import (
    SHA_A,
    SHA_B,
    SHA_C,
    ProductionShapedFlow,
    merge_sha_for,
)


class RecordingRpc:
    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    async def request(
        self, method: str, params: dict[str, Any] | None, timeout: float
    ) -> dict[str, Any]:
        self.calls.append((method, params))
        return self.result


@pytest.fixture
def flow(tmp_path: Path) -> Any:
    harness = ProductionShapedFlow(tmp_path)
    try:
        yield harness
    finally:
        harness.close()


@pytest.mark.asyncio
async def test_thread_reports_read_the_last_agent_message_only() -> None:
    rpc = RecordingRpc(
        {
            "thread": {
                "turns": [
                    {
                        "status": "completed",
                        "items": [{"type": "agentMessage", "text": "old"}],
                    },
                    {
                        "status": "completed",
                        "items": [
                            {"type": "agentMessage", "text": "  final  "},
                            {"type": "commandExecution", "text": "ignored"},
                        ],
                    },
                ]
            }
        }
    )
    assert await CodexThreadReports(rpc).latest_report("thr") == "  final  "
    assert rpc.calls == [("thread/read", {"threadId": "thr", "includeTurns": True})]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [
        {},
        {"thread": {"turns": []}},
        {
            "thread": {
                "turns": [
                    {
                        "status": "inProgress",
                        "items": [{"type": "agentMessage", "text": "x"}],
                    }
                ]
            }
        },
        {
            "thread": {
                "turns": [
                    {
                        "status": "completed",
                        "items": [{"type": "userMessage", "text": "x"}],
                    }
                ]
            }
        },
        {
            "thread": {
                "turns": [
                    {
                        "status": "completed",
                        "items": [{"type": "agentMessage", "text": "   "}],
                    }
                ]
            }
        },
    ],
)
async def test_thread_reports_without_a_completed_agent_message_are_none(
    result: dict[str, Any],
) -> None:
    assert await CodexThreadReports(RecordingRpc(result)).latest_report("thr") is None


@pytest.mark.asyncio
async def test_turn_requires_a_ready_channel(flow: ProductionShapedFlow) -> None:
    outcome = await flow.turns.handle_turn("demo")
    assert outcome.kind == "channel_unavailable"
    with pytest.raises(ValueError, match="unknown project"):
        await flow.turns.handle_turn("nope")


@pytest.mark.asyncio
async def test_notifications_route_only_known_thread_turns(
    flow: ProductionShapedFlow,
) -> None:
    await flow.merger.ensure_thread("demo")
    ignored = await flow.turns.on_notification(RpcNotification("item/completed", {}))
    assert ignored is None
    assert (
        await flow.turns.on_notification(
            RpcNotification("turn/completed", {"threadId": "thr_other"})
        )
        is None
    )
    routed = await flow.turns.on_notification(
        RpcNotification("turn/completed", {"threadId": "thr_legacy"})
    )
    assert routed is not None and routed.kind == "no_outstanding_wake"


@pytest.mark.asyncio
async def test_full_window_defers_and_returns_the_wake_to_pending(
    flow: ProductionShapedFlow,
) -> None:
    await flow.merger.ensure_thread("demo")
    for issue, sha, number in (("ENG-9", SHA_A, 14), ("ENG-11", SHA_B, 16)):
        branch = flow.stage(issue, sha, pr_number=number)
        await flow.emitter.emit("demo", issue, verification=(("t", "ok"),))
        flow.report(flow.verdict(sha, branch, number))
        assert (await flow.turns.handle_turn("demo")).kind == "merged"
    assert len(flow.window.unresolved_items("demo")) == 2

    branch = flow.stage("ENG-12", SHA_C, pr_number=17)
    emitted = await flow.emitter.emit("demo", "ENG-12", verification=(("t", "ok"),))
    flow.report(flow.verdict(SHA_C, branch, 17))
    deferred = await flow.turns.handle_turn("demo")
    assert deferred.kind == "deferred"
    assert len(flow.github.merge_calls) == 2
    row = flow.database.execute(
        "SELECT state, claim_token FROM wake_deliveries WHERE event_id = ?",
        (emitted.event.event_id,),
    ).fetchone()
    assert (row["state"], row["claim_token"]) == ("deferred", None)
    assert flow.turns.outstanding_wake("demo") is None

    # The spent event can never be re-delivered; the window is reconciled
    # only at a fresh boundary, so the candidate is re-emitted as a new event.
    spent = await flow.delivery.deliver("demo", emitted.event)
    assert (spent.delivered, spent.reason) == (False, "candidate_deferred")
    # ENG-11's intake reconciled A once; ENG-12's intake reconciled A and B.
    assert flow.ci.calls == [
        merge_sha_for(SHA_A),
        merge_sha_for(SHA_A),
        merge_sha_for(SHA_B),
    ]

    flow.clock = flow.clock.replace(minute=30)
    flow.ci.results[merge_sha_for(SHA_A)] = CiCheck(outcome="success", reason="ok")
    reemitted = await flow.emitter.emit("demo", "ENG-12", verification=(("t", "ok"),))
    assert reemitted.event.event_id != emitted.event.event_id
    assert reemitted.delivery.delivered is True
    rows = flow.database.execute(
        "SELECT event_id, state FROM wake_deliveries WHERE candidate_sha = ?",
        (SHA_C,),
    ).fetchall()
    assert [(r["event_id"], r["state"]) for r in rows] == [
        (reemitted.event.event_id, "delivered")
    ]
    assert (await flow.turns.handle_turn("demo")).kind == "merged"
    assert flow.ci.calls[-2:] == [merge_sha_for(SHA_A), merge_sha_for(SHA_B)]
    assert [item.merge_sha for item in flow.window.unresolved_items("demo")] == [
        merge_sha_for(SHA_B),
        merge_sha_for(SHA_C),
    ]


@pytest.mark.asyncio
async def test_stale_or_malformed_reports_never_merge(
    flow: ProductionShapedFlow,
) -> None:
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    flow.report(flow.verdict(SHA_B, branch, 14))
    stale = await flow.turns.handle_turn("demo")
    assert stale.kind == "verdict_invalid"
    flow.report("PAUSED_NO_ELIGIBLE_WORK")
    forbidden = await flow.turns.handle_turn("demo")
    assert forbidden.kind == "verdict_invalid"
    assert "forbidden pause" in forbidden.reason
    assert flow.github.merge_calls == []
    assert flow.turns.outstanding_wake("demo") is not None


def _submission(
    event_id: str, issue_id: str, sha: str, document: str
) -> dict[str, Any]:
    return {
        "issue_id": issue_id,
        "event_id": event_id,
        "candidate_sha": sha,
        "reviewed_thread_id": "thr_legacy",
        "reviewed_generation": 1,
        "verdict_json": document,
    }


def _submitted_rows(flow: ProductionShapedFlow) -> list[tuple[str, str, str]]:
    rows = flow.database.execute(
        "SELECT event_id, state, verdict_json FROM submitted_verdicts "
        "ORDER BY created_at ASC, rowid ASC"
    ).fetchall()
    return [(r["event_id"], r["state"], r["verdict_json"]) for r in rows]


def _review_count(flow: ProductionShapedFlow) -> int:
    return int(flow.database.scalar("SELECT count(*) FROM reviews"))


@pytest.mark.asyncio
async def test_bound_submission_persists_once_and_settles_corrections(
    flow: ProductionShapedFlow,
) -> None:
    # Required test 1: a bound Sol submission with exact project, issue,
    # event, candidate SHA, thread, and generation persists exactly once
    # and immediately settles corrections_required routing.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-10", SHA_A, pr_number=15)
    emitted = await flow.emitter.emit("demo", "ENG-10", verification=(("t", "ok"),))
    document = flow.verdict(SHA_A, branch, 15, defect=True)

    outcome = await flow.turns.submit_review(
        "demo", **_submission(emitted.event.event_id, "ENG-10", SHA_A, document)
    )

    assert outcome.kind == "corrections_required"
    assert outcome.event_id == emitted.event.event_id
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "settled", document)
    ]
    pending = flow.outbox.pending("demo")
    assert len(pending) == 1
    assert pending[0].issue_id == "ENG-10"
    assert pending[0].reviewed_sha == SHA_A
    assert pending[0].source == "codex_review"
    assert flow.github.merge_calls == []
    # The verdict came from the explicit submission, never a thread pull.
    assert "thread/read" not in flow.rpc.methods


@pytest.mark.asyncio
async def test_identical_duplicate_submission_is_idempotent(
    flow: ProductionShapedFlow,
) -> None:
    # Required test 2: the same exact submission returns the same result
    # and produces no duplicate review, correction, projection, or merge.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-10", SHA_A, pr_number=15)
    emitted = await flow.emitter.emit("demo", "ENG-10", verification=(("t", "ok"),))
    document = flow.verdict(SHA_A, branch, 15, defect=True)
    submission = _submission(emitted.event.event_id, "ENG-10", SHA_A, document)

    first = await flow.turns.submit_review("demo", **submission)
    reviews = _review_count(flow)
    corrections = len(flow.outbox.pending("demo"))
    projections = list(flow.linear.targets)

    second = await flow.turns.submit_review("demo", **submission)

    assert second == first
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "settled", document)
    ]
    assert _review_count(flow) == reviews
    assert len(flow.outbox.pending("demo")) == corrections
    assert flow.linear.targets == projections
    assert flow.github.merge_calls == []


@pytest.mark.asyncio
async def test_unbound_stale_or_conflicting_submissions_fail_closed(
    flow: ProductionShapedFlow,
) -> None:
    # Required test 3: wrong project, issue, event, SHA, thread,
    # generation, stale wake, malformed verdict, or conflicting duplicate
    # fails closed before settlement, with no side effects.
    await flow.merger.ensure_thread("demo")

    # Stale: no outstanding wake exists at all yet.
    early = _submission("evt-none", "ENG-9", SHA_A, "{}")
    with pytest.raises(SubmissionRejected):
        await flow.turns.submit_review("demo", **early)

    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    document = flow.verdict(SHA_A, branch, 14)
    good = _submission(emitted.event.event_id, "ENG-9", SHA_A, document)

    rejected: list[dict[str, Any]] = [
        {**good, "issue_id": "ENG-999"},
        {**good, "event_id": "evt-other"},
        {**good, "candidate_sha": SHA_B},
        {**good, "reviewed_thread_id": "thr_other"},
        {**good, "reviewed_generation": 2},
        {**good, "verdict_json": "not json"},
        {**good, "verdict_json": IDLE_TERMINAL_REPORT},
        {**good, "verdict_json": flow.verdict(SHA_B, branch, 14)},
    ]
    for submission in rejected:
        with pytest.raises(SubmissionRejected):
            await flow.turns.submit_review("demo", **submission)
    with pytest.raises(SubmissionRejected):
        await flow.turns.submit_review("nope", **good)

    assert _submitted_rows(flow) == []
    assert _review_count(flow) == 0
    assert flow.outbox.pending("demo") == ()
    assert flow.github.merge_calls == []
    assert flow.turns.outstanding_wake("demo") == (emitted.event, "delivered")

    # Conflicting duplicate: same event, different verdict bytes.
    assert (await flow.turns.submit_review("demo", **good)).kind == "merged"
    conflicting = {**good, "verdict_json": flow.verdict(SHA_A, branch, 14, defect=True)}
    with pytest.raises(SubmissionRejected):
        await flow.turns.submit_review("demo", **conflicting)
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "settled", document)
    ]
    assert len(flow.github.merge_calls) == 1
    assert flow.outbox.pending("demo") == ()


@pytest.mark.asyncio
async def test_approved_submission_enters_the_guarded_merge_transition(
    flow: ProductionShapedFlow,
) -> None:
    # Required test 4: an approved verdict immediately enters the existing
    # guarded merge transition (corrections_required routing of the durable
    # correction path is proven in the corrections settlement test above).
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))

    outcome = await flow.turns.submit_review(
        "demo",
        **_submission(
            emitted.event.event_id, "ENG-9", SHA_A, flow.verdict(SHA_A, branch, 14)
        ),
    )

    assert outcome.kind == "merged"
    assert outcome.merge_sha == merge_sha_for(SHA_A)
    assert flow.github.merge_calls[-1]["expected_head_sha"] == SHA_A
    assert [item.merge_sha for item in flow.window.unresolved_items("demo")] == [
        merge_sha_for(SHA_A)
    ]
    row = flow.database.execute("SELECT state FROM wake_deliveries").fetchone()
    assert row["state"] == "completed"
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "settled", flow.verdict(SHA_A, branch, 14))
    ]


@pytest.mark.asyncio
async def test_observation_never_infers_a_verdict_and_resumes_durable_ones(
    flow: ProductionShapedFlow,
) -> None:
    # Required test 5: idle/turn observation does not create or infer a
    # new verdict; it only resumes settlement when a matching verdict was
    # already durably written before submission completion.
    await flow.merger.ensure_thread("demo")
    flow.stage("ENG-9", SHA_A, pr_number=14)
    await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    flow.report(IDLE_TERMINAL_REPORT)
    idle = await flow.turns.handle_turn("demo")
    assert idle.kind == "idle"
    assert _submitted_rows(flow) == []
    assert flow.github.merge_calls == []

    # A durably written but unsettled submission (crash between the
    # exactly-once persist and settlement completion) is resumed by the
    # observation fallback, from the stored verdict, never the thread.
    branch = flow.stage("ENG-11", SHA_B, pr_number=16)
    emitted = await flow.emitter.emit("demo", "ENG-11", verification=(("t", "ok"),))
    document = flow.verdict(SHA_B, branch, 16)
    with flow.database.transaction() as connection:
        connection.execute(
            "INSERT INTO submitted_verdicts("
            "event_id, project_key, issue_id, candidate_sha, "
            "reviewed_thread_id, reviewed_generation, verdict_json, state, "
            "created_at, updated_at) "
            "VALUES (?, 'demo', 'ENG-11', ?, 'thr_legacy', 1, ?, 'submitted', "
            "'2026-08-28T12:00:00+00:00', '2026-08-28T12:00:00+00:00')",
            (emitted.event.event_id, SHA_B, document),
        )
    flow.report(IDLE_TERMINAL_REPORT)

    resumed = await flow.turns.handle_turn("demo")

    assert resumed.kind == "merged"
    assert resumed.merge_sha == merge_sha_for(SHA_B)
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "settled", document)
    ]
    assert len(flow.github.merge_calls) == 1
