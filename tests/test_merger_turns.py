"""Verify Merger turn handling outside the full acceptance flow."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.circleci import CiCheck
from hermes_orchestrator.codex_rpc import RpcNotification
from hermes_orchestrator.merger_turns import CodexThreadReports
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
