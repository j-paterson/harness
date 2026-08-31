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
from tests.test_merge import open_summary


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
        emitted = await flow.emitter.emit(
            "demo", issue, verification=(("t", "ok"),)
        )
        settled = await flow.turns.submit_review(
            "demo",
            **_submission(
                emitted.event.event_id, issue, sha,
                flow.verdict(sha, branch, number),
            ),
        )
        assert settled.kind == "merged"
    assert len(flow.window.unresolved_items("demo")) == 2

    branch = flow.stage("ENG-12", SHA_C, pr_number=17)
    emitted = await flow.emitter.emit("demo", "ENG-12", verification=(("t", "ok"),))
    deferred = await flow.turns.submit_review(
        "demo",
        **_submission(
            emitted.event.event_id, "ENG-12", SHA_C, flow.verdict(SHA_C, branch, 17)
        ),
    )
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
    resubmitted = await flow.turns.submit_review(
        "demo",
        **_submission(
            reemitted.event.event_id, "ENG-12", SHA_C,
            flow.verdict(SHA_C, branch, 17),
        ),
    )
    assert resubmitted.kind == "merged"
    assert flow.ci.calls[-2:] == [merge_sha_for(SHA_A), merge_sha_for(SHA_B)]
    assert [item.merge_sha for item in flow.window.unresolved_items("demo")] == [
        merge_sha_for(SHA_B),
        merge_sha_for(SHA_C),
    ]
    # Every settlement above came from a persisted submission; the thread
    # was never pulled as a verdict source.
    assert "thread/read" not in flow.rpc.methods


@pytest.mark.asyncio
async def test_completed_report_without_submission_is_non_settling(
    flow: ProductionShapedFlow,
) -> None:
    # Sol a9cc6d5f required test 1: a completed thread report with no
    # submitted_verdicts row does not settle and does not consume the
    # wake — even a fully mergeable report is never pulled as a verdict.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    flow.report(flow.verdict(SHA_A, branch, 14))

    outcome = await flow.turns.handle_turn("demo")

    assert outcome.kind == "awaiting_submission"
    assert outcome.event_id == emitted.event.event_id
    assert flow.turns.outstanding_wake("demo") == (emitted.event, "delivered")
    assert _submitted_rows(flow) == []
    assert flow.github.merge_calls == []
    assert "thread/read" not in flow.rpc.methods

    # The turn-completed notification path is the same observation and
    # stays non-settling and stable across repeats.
    routed = await flow.turns.on_notification(
        RpcNotification("turn/completed", {"threadId": "thr_legacy"})
    )
    assert routed is not None and routed.kind == "awaiting_submission"
    assert flow.turns.outstanding_wake("demo") == (emitted.event, "delivered")
    assert _submitted_rows(flow) == []
    assert flow.github.merge_calls == []
    assert "thread/read" not in flow.rpc.methods


@pytest.mark.asyncio
async def test_startup_recovery_without_submission_leaves_wake_outstanding(
    flow: ProductionShapedFlow,
) -> None:
    # Sol a9cc6d5f required test 2: startup recovery with no submitted
    # verdict leaves the wake outstanding; only a later explicit
    # submission settles it.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-10", SHA_A, pr_number=15)
    emitted = await flow.emitter.emit("demo", "ENG-10", verification=(("t", "ok"),))
    flow.report(flow.verdict(SHA_A, branch, 15))

    outcomes = await flow.turns.recover_outstanding()

    assert [outcome.kind for outcome in outcomes] == ["awaiting_submission"]
    assert outcomes[0].event_id == emitted.event.event_id
    assert flow.turns.outstanding_wake("demo") == (emitted.event, "delivered")
    assert _submitted_rows(flow) == []
    assert flow.github.merge_calls == []
    assert "thread/read" not in flow.rpc.methods

    settled = await flow.turns.submit_review(
        "demo",
        **_submission(
            emitted.event.event_id, "ENG-10", SHA_A, flow.verdict(SHA_A, branch, 15)
        ),
    )
    assert settled.kind == "merged"
    assert "thread/read" not in flow.rpc.methods


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
        {**good, "verdict_json": "PAUSED_NO_ELIGIBLE_WORK"},
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
async def test_interleaved_opposing_submission_settles_exactly_once(
    flow: ProductionShapedFlow,
) -> None:
    # Sol a9cc6d5f required test 3: interleave turn-completed handling
    # with an opposing valid submit-review and prove exactly one
    # settlement, from the persisted document. The old race window —
    # observation reading no pending submission, then awaiting the
    # thread report — is armed as a trap: any thread/read fires the
    # conflicting submission mid-await, so if observation ever reopens
    # that window the stale approved report would settle after the
    # opposing verdict and the assertions below would catch the double
    # settlement.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    opposing = flow.verdict(SHA_A, branch, 14, defect=True)
    flow.report(flow.verdict(SHA_A, branch, 14))
    fired: list[Any] = []

    async def fire(method: str) -> None:
        if method == "thread/read" and not fired:
            fired.append(
                await flow.turns.submit_review(
                    "demo",
                    **_submission(
                        emitted.event.event_id, "ENG-9", SHA_A, opposing
                    ),
                )
            )

    flow.rpc.on_request = fire

    observed = await flow.turns.on_notification(
        RpcNotification("turn/completed", {"threadId": "thr_legacy"})
    )

    # Observation never opened the window: no report await, no trap.
    assert observed is not None and observed.kind == "awaiting_submission"
    assert fired == [] and "thread/read" not in flow.rpc.methods
    assert flow.turns.outstanding_wake("demo") == (emitted.event, "delivered")

    outcome = await flow.turns.submit_review(
        "demo",
        **_submission(emitted.event.event_id, "ENG-9", SHA_A, opposing),
    )

    assert outcome.kind == "corrections_required"
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "settled", opposing)
    ]
    pending = flow.outbox.pending("demo")
    assert len(pending) == 1 and pending[0].reviewed_sha == SHA_A
    # Exactly one settlement: the opposing persisted document routed
    # corrections; the thread's approved report never caused a merge.
    assert flow.github.merge_calls == []
    assert "thread/read" not in flow.rpc.methods


@pytest.mark.asyncio
async def test_crash_after_submission_resumes_the_exact_document(
    flow: ProductionShapedFlow,
) -> None:
    # Sol a9cc6d5f required test 4: a crash between the exactly-once
    # persist and settlement completion resumes that exact document on
    # the next observation, without pulling the thread.
    await flow.merger.ensure_thread("demo")
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
    # No thread/read response is armed: the stored document alone settles.

    resumed = await flow.turns.handle_turn("demo")

    assert resumed.kind == "merged"
    assert resumed.merge_sha == merge_sha_for(SHA_B)
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "settled", document)
    ]
    assert len(flow.github.merge_calls) == 1
    assert "thread/read" not in flow.rpc.methods


def _persist_submitted_row(
    flow: ProductionShapedFlow,
    event_id: str,
    issue_id: str,
    sha: str,
    document: str,
) -> None:
    """A submission durably persisted, then the process died pre-settlement."""

    with flow.database.transaction() as connection:
        connection.execute(
            "INSERT INTO submitted_verdicts("
            "event_id, project_key, issue_id, candidate_sha, "
            "reviewed_thread_id, reviewed_generation, verdict_json, state, "
            "created_at, updated_at) "
            "VALUES (?, 'demo', ?, ?, 'thr_legacy', 1, ?, 'submitted', "
            "'2026-08-28T12:00:00+00:00', '2026-08-28T12:00:00+00:00')",
            (event_id, issue_id, sha, document),
        )


def _replace_channel(flow: ProductionShapedFlow) -> None:
    """Replace the reviewer channel: new thread, incremented generation."""

    flow.merger.begin_replacement(
        "demo",
        expected_thread_id="thr_legacy",
        expected_generation=1,
        reason="reviewer channel rotated",
    )
    flow.merger.complete_replacement(
        "demo",
        expected_thread_id="thr_legacy",
        expected_generation=1,
        new_thread_id="thr_new",
    )


@pytest.mark.asyncio
async def test_channel_replacement_leaves_persisted_submission_non_settling(
    flow: ProductionShapedFlow,
) -> None:
    # Sol 743338e2 required test 1: persist a valid submission, replace
    # the reviewer channel with a new generation, run recovery, and
    # assert zero review, merge, correction, or wake-completion effects.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    document = flow.verdict(SHA_A, branch, 14)
    _persist_submitted_row(flow, emitted.event.event_id, "ENG-9", SHA_A, document)
    _replace_channel(flow)

    outcomes = await flow.turns.recover_outstanding()

    assert [outcome.kind for outcome in outcomes] == ["stale_submission"]
    assert outcomes[0].event_id == emitted.event.event_id
    # The row stays 'submitted': non-settling, awaiting the new binding.
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "submitted", document)
    ]
    assert _review_count(flow) == 0
    assert flow.github.merge_calls == []
    assert flow.outbox.pending("demo") == ()
    assert flow.linear.targets == []
    assert flow.turns.outstanding_wake("demo") == (emitted.event, "delivered")
    # The refusal is stable across repeated recovery: still no effects.
    again = await flow.turns.recover_outstanding()
    assert [outcome.kind for outcome in again] == ["stale_submission"]
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "submitted", document)
    ]
    assert flow.turns.outstanding_wake("demo") == (emitted.event, "delivered")


@pytest.mark.asyncio
async def test_replacement_binding_submission_supersedes_and_settles_once(
    flow: ProductionShapedFlow,
) -> None:
    # Sol 743338e2 required test 2: after the stale refusal, a submission
    # from the replacement thread/generation supersedes the stale row via
    # the UPDATE-if-stale CAS and settles exactly once — settling its own
    # document, never the stale approved evidence.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    stale_document = flow.verdict(SHA_A, branch, 14)
    _persist_submitted_row(
        flow, emitted.event.event_id, "ENG-9", SHA_A, stale_document
    )
    _replace_channel(flow)
    stale = await flow.turns.recover_outstanding()
    assert [outcome.kind for outcome in stale] == ["stale_submission"]
    # The wake is re-delivered to the replacement channel: register_wake
    # reopens the stale delivery atomically and delivery re-binds it.
    redelivered = await flow.delivery.deliver("demo", emitted.event)
    assert redelivered.delivered is True
    assert (redelivered.thread_id, redelivered.generation) == ("thr_new", 2)

    fresh_document = flow.verdict(SHA_A, branch, 14, defect=True)
    fresh = {
        **_submission(emitted.event.event_id, "ENG-9", SHA_A, fresh_document),
        "reviewed_thread_id": "thr_new",
        "reviewed_generation": 2,
    }
    outcome = await flow.turns.submit_review("demo", **fresh)

    assert outcome.kind == "corrections_required"
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "settled", fresh_document)
    ]
    pending = flow.outbox.pending("demo")
    assert len(pending) == 1
    assert pending[0].reviewed_sha == SHA_A
    # Exactly once: the stale approved document never merged, and the
    # identical duplicate from the current binding replays the result.
    assert flow.github.merge_calls == []
    duplicate = await flow.turns.submit_review("demo", **fresh)
    assert duplicate == outcome
    assert len(flow.outbox.pending("demo")) == 1
    assert flow.github.merge_calls == []


@pytest.mark.asyncio
async def test_replacement_racing_persistence_persists_no_stale_identity(
    flow: ProductionShapedFlow,
) -> None:
    # Sol 743338e2 required test 3 (first interleaving): the channel is
    # replaced between submission validation and persistence, via an
    # injected hook on the persistence seam. The binding-conditioned
    # persist stores nothing, so no stale identity exists to settle.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    document = flow.verdict(SHA_A, branch, 14)
    original = flow.turns._persist_submission

    def replace_then_persist(**kwargs: Any) -> Any:
        _replace_channel(flow)
        return original(**kwargs)

    flow.turns._persist_submission = replace_then_persist  # type: ignore[method-assign]
    with pytest.raises(SubmissionRejected, match="replaced while persisting"):
        await flow.turns.submit_review(
            "demo",
            **_submission(emitted.event.event_id, "ENG-9", SHA_A, document),
        )

    assert _submitted_rows(flow) == []
    assert _review_count(flow) == 0
    assert flow.github.merge_calls == []
    assert flow.outbox.pending("demo") == ()
    assert flow.turns.outstanding_wake("demo") == (emitted.event, "delivered")
    # Recovery finds nothing submitted: the stale identity cannot settle.
    flow.turns._persist_submission = original  # type: ignore[method-assign]
    outcomes = await flow.turns.recover_outstanding()
    assert [outcome.kind for outcome in outcomes] == ["awaiting_submission"]


@pytest.mark.asyncio
async def test_replacement_after_persistence_refuses_direct_settlement(
    flow: ProductionShapedFlow,
) -> None:
    # Sol 743338e2 required test 3 (second interleaving): the channel is
    # replaced after the exactly-once persist but before the direct
    # settlement. The settlement-entry revalidation refuses; the row
    # stays 'submitted' and nothing settles from the stale identity.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    document = flow.verdict(SHA_A, branch, 14)
    original = flow.turns._persist_submission

    def persist_then_replace(**kwargs: Any) -> Any:
        persisted = original(**kwargs)
        _replace_channel(flow)
        return persisted

    flow.turns._persist_submission = persist_then_replace  # type: ignore[method-assign]
    outcome = await flow.turns.submit_review(
        "demo",
        **_submission(emitted.event.event_id, "ENG-9", SHA_A, document),
    )

    assert outcome.kind == "stale_submission"
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "submitted", document)
    ]
    assert _review_count(flow) == 0
    assert flow.github.merge_calls == []
    assert flow.outbox.pending("demo") == ()
    assert flow.turns.outstanding_wake("demo") == (emitted.event, "delivered")


async def _approved_settlement(
    flow: ProductionShapedFlow, issue_id: str, sha: str, branch: str, number: int
) -> Any:
    """An approved review + settlement durably bound to generation 1."""

    from hermes_orchestrator.verdicts import VerdictBinding, parse_verdict

    emitted = await flow.emitter.emit("demo", issue_id, verification=(("t", "ok"),))
    admitted = flow.admission.admit("demo", emitted.event, received_generation=1)
    verdict = parse_verdict(
        flow.verdict(sha, branch, number),
        expected=VerdictBinding(
            repository="j-paterson/demo",
            branch=branch,
            pr_number=number,
            reviewed_sha=sha,
        ),
    )
    record = await flow.reviews.record_verdict(admitted, issue_id, verdict)
    return emitted, record


@pytest.mark.asyncio
async def test_handle_turn_boundary_refuses_the_stale_settlement(
    flow: ProductionShapedFlow,
) -> None:
    # Sol 165f5ee6 packet 3 required test 2 (intake recovery): an
    # approved generation-1 settlement whose wake already completed is
    # re-driven by handle_turn's boundary resume after the channel is
    # replaced — the settlement-claim fence refuses, the row stays
    # recorded, and nothing crosses the GitHub boundary.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-3", SHA_C, pr_number=16)
    emitted, record = await _approved_settlement(flow, "ENG-3", SHA_C, branch, 16)
    assert flow.merger.complete_admitted_wake("demo", emitted.event.event_id)
    _replace_channel(flow)

    boundary = await flow.turns.handle_turn("demo")

    assert boundary.kind == "no_outstanding_wake"
    settlement = flow.settlements.get(record.review_id)
    assert settlement.state == "recorded"
    assert (settlement.thread_id, settlement.thread_generation) == ("thr_legacy", 1)
    assert flow.github.merge_calls == []
    assert _review_count(flow) == 1
    review_state = flow.database.scalar(
        "SELECT state FROM reviews WHERE review_id = ?", (record.review_id,)
    )
    assert str(review_state) == "approved"
    # The refusal is stable across repeated boundaries.
    again = await flow.turns.handle_turn("demo")
    assert again.kind == "no_outstanding_wake"
    assert flow.settlements.get(record.review_id).state == "recorded"
    assert flow.github.merge_calls == []


@pytest.mark.asyncio
async def test_fresh_generation_submission_supersedes_the_stale_settlement(
    flow: ProductionShapedFlow,
) -> None:
    # Sol 165f5ee6 packet 3 required test 4: generation-1 approval and
    # settlement survive a crash; the channel is replaced; every
    # recovery refuses; then a fresh generation-2 submission supersedes
    # both stale rows (submitted verdict and settlement binding) and
    # settles exactly once.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    document = flow.verdict(SHA_A, branch, 14)
    emitted, record = await _approved_settlement(flow, "ENG-9", SHA_A, branch, 14)
    _persist_submitted_row(flow, emitted.event.event_id, "ENG-9", SHA_A, document)
    _replace_channel(flow)

    outcomes = await flow.turns.recover_outstanding()
    assert [outcome.kind for outcome in outcomes] == ["stale_submission"]
    assert flow.settlements.get(record.review_id).state == "recorded"
    assert flow.github.merge_calls == []

    fresh = {
        **_submission(emitted.event.event_id, "ENG-9", SHA_A, document),
        "reviewed_thread_id": "thr_new",
        "reviewed_generation": 2,
    }
    outcome = await flow.turns.submit_review("demo", **fresh)

    assert outcome.kind == "merged"
    assert outcome.merge_sha == merge_sha_for(SHA_A)
    assert len(flow.github.merge_calls) == 1
    settlement = flow.settlements.get(record.review_id)
    assert settlement.state == "settled"
    assert (settlement.thread_id, settlement.thread_generation) == ("thr_new", 2)
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "settled", document)
    ]
    # Exactly once: an identical duplicate replays the settled result.
    duplicate = await flow.turns.submit_review("demo", **fresh)
    assert duplicate == outcome
    assert len(flow.github.merge_calls) == 1


@pytest.mark.asyncio
async def test_duplicates_idempotent_only_for_the_current_binding(
    flow: ProductionShapedFlow,
) -> None:
    # Sol 743338e2 required test 4: identical duplicate submissions are
    # idempotent only while their thread/generation is still the current
    # binding; after replacement the exact same bytes fail closed.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    document = flow.verdict(SHA_A, branch, 14)
    submission = _submission(emitted.event.event_id, "ENG-9", SHA_A, document)

    first = await flow.turns.submit_review("demo", **submission)
    assert first.kind == "merged"
    second = await flow.turns.submit_review("demo", **submission)
    assert second == first
    assert len(flow.github.merge_calls) == 1

    _replace_channel(flow)
    with pytest.raises(SubmissionRejected, match="reviewer-channel binding"):
        await flow.turns.submit_review("demo", **submission)
    assert len(flow.github.merge_calls) == 1
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "settled", document)
    ]


def _clear_open_pulls_after_admission(flow: ProductionShapedFlow) -> None:
    """Zero the open pull requests only for the settlement's own list call.

    Admission's own ``GitHubIntakeGate.validate`` list call (the first of
    the turn) still sees the staged matching pull request, so admission
    passes unchanged; ``_pull_number``'s live check (the second list call)
    then sees zero open pull requests, exactly as if Sol has not opened
    the sole pull request yet.
    """

    list_calls_before = len(flow.github.list_calls)

    def clear(count: int) -> None:
        if count == list_calls_before + 2:
            flow.github.open_pulls = ()

    flow.github.on_list = clear


@pytest.mark.asyncio
async def test_zero_open_prs_settles_corrections_required_with_pr_number_zero(
    flow: ProductionShapedFlow,
) -> None:
    # INFRA-202 (a): Fable pushes a clean candidate with no pull request;
    # Sol may still return corrections_required without having opened one
    # yet. Settlement binds pr_number 0, delivers the packet to the lead
    # outbox with pr_number 0, and completes the wake.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    document = flow.verdict(SHA_A, branch, 0, defect=True)
    _clear_open_pulls_after_admission(flow)

    outcome = await flow.turns.submit_review(
        "demo", **_submission(emitted.event.event_id, "ENG-9", SHA_A, document)
    )
    flow.github.on_list = None

    assert outcome.kind == "corrections_required"
    assert _submitted_rows(flow) == [
        (emitted.event.event_id, "settled", document)
    ]
    pending = flow.outbox.pending("demo")
    assert len(pending) == 1
    assert pending[0].pr_number == 0
    row = flow.database.execute("SELECT state FROM wake_deliveries").fetchone()
    assert row["state"] == "completed"
    assert flow.github.merge_calls == []


@pytest.mark.asyncio
async def test_zero_open_prs_refuses_an_approved_verdict(
    flow: ProductionShapedFlow,
) -> None:
    # INFRA-202 (b): an approved verdict still requires the sole open
    # pull request at the candidate head; Sol must create it before
    # approving. The document's own declared pr_number 0 is caught by
    # parse_verdict's approval rule at submission, before anything
    # settles or merges — the wake is left outstanding, not consumed.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    document = flow.verdict(SHA_A, branch, 0)
    _clear_open_pulls_after_admission(flow)

    with pytest.raises(SubmissionRejected, match="approval requires the sole open"):
        await flow.turns.submit_review(
            "demo", **_submission(emitted.event.event_id, "ENG-9", SHA_A, document)
        )
    flow.github.on_list = None

    assert _submitted_rows(flow) == []
    assert flow.github.merge_calls == []
    assert flow.turns.outstanding_wake("demo") == (emitted.event, "delivered")


@pytest.mark.asyncio
async def test_exactly_one_matching_pr_still_binds_its_number(
    flow: ProductionShapedFlow,
) -> None:
    # INFRA-202 (c): exactly one open pull request whose head matches the
    # candidate still binds that number, unchanged from prior behavior.
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


@pytest.mark.asyncio
async def test_mismatched_or_multiple_open_prs_still_reject_settlement(
    flow: ProductionShapedFlow,
) -> None:
    # INFRA-202 (d): a single open pull request whose head does not match
    # the candidate, or two or more open pull requests, remain invariant
    # breaches that reject settlement with the updated wording.
    await flow.merger.ensure_thread("demo")
    branch = flow.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await flow.emitter.emit("demo", "ENG-9", verification=(("t", "ok"),))
    list_calls_before = len(flow.github.list_calls)

    def mismatch(count: int) -> None:
        if count == list_calls_before + 2:
            flow.github.open_pulls = (
                open_summary(number=14, head_sha=SHA_B, head_ref=branch),
            )

    flow.github.on_list = mismatch
    outcome = await flow.turns.submit_review(
        "demo",
        **_submission(
            emitted.event.event_id, "ENG-9", SHA_A, flow.verdict(SHA_A, branch, 14)
        ),
    )
    flow.github.on_list = None

    assert outcome.kind == "rejected"
    assert (
        "the candidate is not the head of the sole open pull request"
        in outcome.reason
    )
    assert flow.github.merge_calls == []

    branch_c = flow.stage("ENG-11", SHA_C, pr_number=16)
    emitted_c = await flow.emitter.emit(
        "demo", "ENG-11", verification=(("t", "ok"),)
    )
    list_calls_before = len(flow.github.list_calls)

    def two_open(count: int) -> None:
        if count == list_calls_before + 2:
            flow.github.open_pulls = (
                open_summary(number=16, head_sha=SHA_C, head_ref=branch_c),
                open_summary(number=17, head_sha=SHA_A, head_ref=branch),
            )

    flow.github.on_list = two_open
    outcome_c = await flow.turns.submit_review(
        "demo",
        **_submission(
            emitted_c.event.event_id,
            "ENG-11",
            SHA_C,
            flow.verdict(SHA_C, branch_c, 16),
        ),
    )
    flow.github.on_list = None

    assert outcome_c.kind == "rejected"
    assert (
        "the candidate is not the head of the sole open pull request"
        in outcome_c.reason
    )


@pytest.mark.asyncio
async def test_has_pending_submission_true_only_while_a_submitted_row_exists(
    flow: ProductionShapedFlow,
) -> None:
    """INFRA-198 P1: the read-only helper MergerSession.review_active uses.

    True only for a project with a durable submitted_verdicts row still in
    state 'submitted'; false with no row, for a different project, and
    once the row is settled.
    """

    assert flow.turns.has_pending_submission("demo") is False

    now = flow.clock.isoformat()
    flow.database.execute(
        "INSERT INTO submitted_verdicts(event_id, project_key, issue_id, "
        "candidate_sha, reviewed_thread_id, reviewed_generation, "
        "verdict_json, state, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'submitted', ?, ?)",
        ("evt-pending-1", "demo", "ENG-1", SHA_A, "thr", 1, "{}", now, now),
    )

    assert flow.turns.has_pending_submission("demo") is True
    assert flow.turns.has_pending_submission("other-project") is False

    flow.database.execute(
        "UPDATE submitted_verdicts SET state = 'settled' WHERE event_id = ?",
        ("evt-pending-1",),
    )

    assert flow.turns.has_pending_submission("demo") is False
