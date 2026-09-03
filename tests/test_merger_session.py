"""Verify MergerSession's event-scoped Codex App Server ownership.

INFRA-198 P1 (Sol correction 9a512ed7, Critical, live): before
``MergerSession`` existed the daemon started the merge flow's App Server
at process startup and held its process-group lease for the daemon's
whole lifetime, so the bound Sol thread showed "open in another app"
while idle. These tests pin the fix: the session opens only while
durable SQLite state shows review work active, and releases (freeing the
lease) the instant nothing is outstanding — at startup recovery, on a
maintenance-style ``reconcile``, and from its own listener's
terminal-idle hook without a self-cancellation deadlock.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Sequence
from pathlib import Path
from types import SimpleNamespace

import pytest

from hermes_orchestrator.codex_rpc import CodexRpcClient, RpcNotification
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.merger_session import MergerSession
from hermes_orchestrator.processes import ProcessRegistry
from tests.integration.test_fable_ready_acceptance import (
    SHA_A,
    ProductionShapedFlow,
)

_CLOSED = object()


class FakeRpc:
    """Records start/close counts; ``notifications()`` ends on close."""

    def __init__(self, *, fail_start: bool = False) -> None:
        self.fail_start = fail_start
        self.start_calls = 0
        self.close_calls = 0
        self._queue: asyncio.Queue[object] = asyncio.Queue()

    async def start(self) -> None:
        if self.fail_start:
            raise RuntimeError("codex app-server did not start")
        self.start_calls += 1
        self._queue = asyncio.Queue()

    async def close(self) -> None:
        self.close_calls += 1
        await self._queue.put(_CLOSED)

    async def notifications(self):
        while True:
            item = await self._queue.get()
            if item is _CLOSED:
                return
            yield item

    async def push(self, notification: RpcNotification) -> None:
        await self._queue.put(notification)


class FakeMerger:
    def __init__(self, *, fail_projects: frozenset[str] = frozenset()) -> None:
        self.ensure_calls: list[str] = []
        self._fail = fail_projects

    async def ensure_thread(self, project_key: str) -> None:
        self.ensure_calls.append(project_key)
        if project_key in self._fail:
            raise RuntimeError("merger channel unavailable")


class FakeTurns:
    def __init__(self) -> None:
        self.outstanding: dict[str, object | None] = {}
        self.pending: dict[str, bool] = {}
        self.stalled: dict[str, tuple[object, ...]] = {}
        self.recover_calls: list[tuple[str, ...]] = []
        self.settle_calls: list[tuple[tuple[str, ...], str]] = []
        self.on_settle_idle: Callable[[str], None] | None = None
        self.notification_calls: list[RpcNotification] = []
        self.notification_result: object | None = None
        self.on_notification_hook: Callable[[RpcNotification], None] | None = None

    def outstanding_wake(self, project_key: str) -> object | None:
        return self.outstanding.get(project_key)

    def has_pending_submission(self, project_key: str) -> bool:
        return self.pending.get(project_key, False)

    def stalled_wakes(self, project_key: str) -> tuple[object, ...]:
        return self.stalled.get(project_key, ())

    async def recover_outstanding(
        self, projects: Sequence[str]
    ) -> tuple[object, ...]:
        self.recover_calls.append(tuple(projects))
        return ()

    async def settle_idle_thread(
        self, projects: Sequence[str], thread_id: str
    ) -> None:
        self.settle_calls.append((tuple(projects), thread_id))
        if self.on_settle_idle is not None:
            self.on_settle_idle(thread_id)

    async def on_notification(self, notification: RpcNotification) -> object | None:
        self.notification_calls.append(notification)
        if self.on_notification_hook is not None:
            self.on_notification_hook(notification)
        return self.notification_result


class FakeReviews:
    def __init__(self) -> None:
        self.resume_calls = 0

    async def resume_settlements(self) -> tuple[object, ...]:
        self.resume_calls += 1
        return ()


def _flow(
    *, rpc: FakeRpc, merger: FakeMerger, turns: FakeTurns, reviews: FakeReviews
) -> SimpleNamespace:
    return SimpleNamespace(rpc=rpc, merger=merger, turns=turns, reviews=reviews)


def _event_kinds(events: EventStore) -> list[str]:
    return [record.event_type for record in events.list_after(0)]


async def _wait_until(
    predicate: Callable[[], bool], *, timeout: float = 2.0
) -> None:
    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout=timeout)


@pytest.fixture
def database(tmp_path: Path):
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.mark.asyncio
async def test_startup_with_no_work_opens_recovers_and_releases_once(
    database: Database,
) -> None:
    events = EventStore(database)
    rpc = FakeRpc()
    merger = FakeMerger()
    turns = FakeTurns()
    reviews = FakeReviews()
    session = MergerSession(
        _flow(rpc=rpc, merger=merger, turns=turns, reviews=reviews),
        ("demo",),
        events=events,
        database=database,
    )

    await session.startup()

    assert rpc.start_calls == 1
    assert reviews.resume_calls == 1
    assert turns.recover_calls == [("demo",)]
    assert rpc.close_calls == 1
    assert session.is_open is False
    assert merger.ensure_calls == ["demo"]
    assert _event_kinds(events) == [
        "merger.session_opened",
        "merger.session_released",
    ]


@pytest.mark.asyncio
async def test_startup_surfaces_stalled_wakes_without_retrying_them(
    database: Database,
) -> None:
    # Sol correction 538f1b4e: the startup boundary must never infer a
    # verdict or blindly requeue a stalled wake -- it only journals that
    # one is outstanding, durable, and waiting on the operator's
    # explicit retry command.
    events = EventStore(database)
    rpc = FakeRpc()
    merger = FakeMerger()
    turns = FakeTurns()
    turns.stalled["demo"] = (SimpleNamespace(event_id="evt-stalled-1"),)
    reviews = FakeReviews()
    session = MergerSession(
        _flow(rpc=rpc, merger=merger, turns=turns, reviews=reviews),
        ("demo",),
        events=events,
        database=database,
    )

    await session.startup()

    # Never touched: no state read/written on the fake beyond the listing.
    assert turns.outstanding.get("demo") is None
    assert turns.recover_calls == [("demo",)]
    surfaced = [
        record
        for record in events.list_after(0)
        if record.event_type == "merger.stalled_wakes_outstanding"
    ]
    assert len(surfaced) == 1
    assert surfaced[0].payload == {
        "project_key": "demo",
        "event_ids": ["evt-stalled-1"],
    }
    assert _event_kinds(events) == [
        "merger.session_opened",
        "merger.stalled_wakes_outstanding",
        "merger.session_released",
    ]


@pytest.mark.asyncio
async def test_reconcile_opens_on_a_delivered_wake_and_is_idempotent(
    database: Database,
) -> None:
    events = EventStore(database)
    rpc = FakeRpc()
    turns = FakeTurns()
    session = MergerSession(
        _flow(rpc=rpc, merger=FakeMerger(), turns=turns, reviews=FakeReviews()),
        ("demo",),
        events=events,
        database=database,
    )
    await session.startup()
    assert (rpc.start_calls, rpc.close_calls) == (1, 1)

    turns.outstanding["demo"] = ("evt-1", "delivered")
    await session.reconcile("wake_delivered")

    assert rpc.start_calls == 2
    assert session.is_open is True

    # Idempotent: the same reconcile call again opens nothing new.
    await session.reconcile("wake_delivered_again")
    assert rpc.start_calls == 2
    assert session.is_open is True

    await session.release("test_teardown")


@pytest.mark.asyncio
async def test_reconcile_releases_once_work_settles(database: Database) -> None:
    events = EventStore(database)
    rpc = FakeRpc()
    turns = FakeTurns()
    session = MergerSession(
        _flow(rpc=rpc, merger=FakeMerger(), turns=turns, reviews=FakeReviews()),
        ("demo",),
        events=events,
        database=database,
    )
    turns.outstanding["demo"] = ("evt-1", "delivered")
    await session.reconcile("wake_delivered")
    assert (rpc.start_calls, session.is_open) == (1, True)

    turns.outstanding["demo"] = None
    await session.reconcile("settled")

    assert rpc.close_calls == 1
    assert session.is_open is False
    assert _event_kinds(events) == [
        "merger.session_opened",
        "merger.session_released",
    ]


@pytest.mark.asyncio
async def test_terminal_idle_notification_releases_without_deadlock(
    database: Database,
) -> None:
    events = EventStore(database)
    rpc = FakeRpc()
    turns = FakeTurns()
    turns.outstanding["demo"] = ("evt-1", "delivered")

    def settle(thread_id: str) -> None:
        # The durable submission settled: nothing is outstanding anymore.
        turns.outstanding["demo"] = None

    turns.on_settle_idle = settle
    session = MergerSession(
        _flow(rpc=rpc, merger=FakeMerger(), turns=turns, reviews=FakeReviews()),
        ("demo",),
        events=events,
        database=database,
    )

    opened = await session.open("wake_delivered")
    assert opened is True
    assert session.is_open is True

    await rpc.push(
        RpcNotification(
            method="thread/status/changed",
            params={"status": {"type": "idle"}, "threadId": "thr-1"},
        )
    )

    # If reconcile()/release() deadlocked trying to cancel+await its own
    # listener task, this would hang until the test framework's timeout;
    # instead the listener task detects it is running the release itself
    # and skips self-cancellation, so this resolves promptly.
    await _wait_until(lambda: not session.is_open)

    assert rpc.close_calls == 1
    assert turns.settle_calls == [(("demo",), "thr-1")]
    assert _event_kinds(events) == [
        "merger.session_opened",
        "merger.session_released",
    ]


@pytest.mark.asyncio
async def test_stalled_turn_notification_reconciles_and_releases(
    database: Database,
) -> None:
    # INFRA-223 recurrence: MergerTurnService.on_notification itself
    # marks an unverdicted completed/interrupted turn's wake 'stalled'
    # and returns a TurnOutcome naming it. The default listener must ACT
    # on that outcome instead of discarding it, by reconciling
    # immediately — the same shape as the terminal-idle branch above —
    # so review_active() re-evaluates to false and release() runs right
    # away instead of leaving the bounded helper attached to the now
    # idle task until some later, unrelated boundary happens to
    # reconcile.
    events = EventStore(database)
    rpc = FakeRpc()
    turns = FakeTurns()
    turns.outstanding["demo"] = ("evt-1", "delivered")

    def stall(notification: RpcNotification) -> None:
        # The wake is durably stalled: nothing is outstanding anymore.
        turns.outstanding["demo"] = None

    turns.on_notification_hook = stall
    turns.notification_result = SimpleNamespace(kind="stalled")
    session = MergerSession(
        _flow(rpc=rpc, merger=FakeMerger(), turns=turns, reviews=FakeReviews()),
        ("demo",),
        events=events,
        database=database,
    )

    opened = await session.open("wake_delivered")
    assert opened is True
    assert session.is_open is True

    await rpc.push(
        RpcNotification(method="turn/completed", params={"threadId": "thr-1"})
    )

    await _wait_until(lambda: not session.is_open)

    assert rpc.close_calls == 1
    assert len(turns.notification_calls) == 1
    assert turns.notification_calls[0].method == "turn/completed"
    assert _event_kinds(events) == [
        "merger.session_opened",
        "merger.session_released",
    ]


@pytest.mark.asyncio
async def test_non_stalled_notification_outcome_does_not_force_a_reconcile(
    database: Database,
) -> None:
    # A non-'stalled' outcome (e.g. a genuine settlement, or the
    # non-settling 'awaiting_submission' observation) must not itself
    # force a reconcile beyond what review_active() already reports —
    # only 'stalled' gets that special treatment.
    events = EventStore(database)
    rpc = FakeRpc()
    turns = FakeTurns()
    turns.outstanding["demo"] = ("evt-1", "delivered")
    turns.notification_result = SimpleNamespace(kind="awaiting_submission")
    session = MergerSession(
        _flow(rpc=rpc, merger=FakeMerger(), turns=turns, reviews=FakeReviews()),
        ("demo",),
        events=events,
        database=database,
    )

    opened = await session.open("wake_delivered")
    assert opened is True

    await rpc.push(
        RpcNotification(method="turn/completed", params={"threadId": "thr-1"})
    )

    await _wait_until(lambda: len(turns.notification_calls) == 1)
    # Give the listener a moment to (not) act further; work is still
    # outstanding per FakeTurns, so the session must stay open.
    await asyncio.sleep(0.02)

    assert session.is_open is True
    assert rpc.close_calls == 0

    await session.release("test_teardown")


@pytest.mark.asyncio
async def test_restart_over_the_same_rows_repeats_startup_with_no_duplicate_open(
    database: Database,
) -> None:
    events = EventStore(database)

    first_rpc = FakeRpc()
    first_session = MergerSession(
        _flow(
            rpc=first_rpc,
            merger=FakeMerger(),
            turns=FakeTurns(),
            reviews=FakeReviews(),
        ),
        ("demo",),
        events=events,
        database=database,
    )
    await first_session.startup()
    assert (first_rpc.start_calls, first_rpc.close_calls) == (1, 1)

    # A fresh process, a fresh MergerSession — but the same durable rows
    # (still nothing outstanding), so restart repeats exactly one
    # open->recover->release cycle, never a duplicate open.
    second_rpc = FakeRpc()
    second_session = MergerSession(
        _flow(
            rpc=second_rpc,
            merger=FakeMerger(),
            turns=FakeTurns(),
            reviews=FakeReviews(),
        ),
        ("demo",),
        events=events,
        database=database,
    )
    await second_session.startup()

    assert (second_rpc.start_calls, second_rpc.close_calls) == (1, 1)
    assert second_session.is_open is False
    assert _event_kinds(events) == [
        "merger.session_opened",
        "merger.session_released",
        "merger.session_opened",
        "merger.session_released",
    ]


@pytest.mark.asyncio
async def test_open_returns_false_and_journals_nothing_on_rpc_start_failure(
    database: Database,
) -> None:
    events = EventStore(database)
    rpc = FakeRpc(fail_start=True)
    session = MergerSession(
        _flow(
            rpc=rpc, merger=FakeMerger(), turns=FakeTurns(), reviews=FakeReviews()
        ),
        ("demo",),
        events=events,
        database=database,
    )

    opened = await session.open("wake_delivered")

    assert opened is False
    assert session.is_open is False
    assert rpc.close_calls == 0
    assert _event_kinds(events) == []


@pytest.mark.asyncio
async def test_review_active_is_true_for_a_pending_submission_alone(
    database: Database,
) -> None:
    turns = FakeTurns()
    turns.pending["demo"] = True
    session = MergerSession(
        _flow(
            rpc=FakeRpc(), merger=FakeMerger(), turns=turns, reviews=FakeReviews()
        ),
        ("demo",),
    )

    assert session.review_active() is True


@pytest.mark.asyncio
async def test_is_review_active_override_replaces_the_default_check(
    database: Database,
) -> None:
    session = MergerSession(
        _flow(
            rpc=FakeRpc(),
            merger=FakeMerger(),
            turns=FakeTurns(),
            reviews=FakeReviews(),
        ),
        ("demo",),
        is_review_active=lambda: True,
    )

    # No project has any outstanding wake or pending submission, yet the
    # override wins.
    assert session.review_active() is True


@pytest.mark.asyncio
async def test_release_frees_the_real_codex_app_server_process_lease(
    tmp_path: Path, database: Database
) -> None:
    """(g): a real CodexRpcClient's process lease clears on release.

    Reuses the scripted App Server child from tests/test_codex_rpc.py so
    the handshake completes over real stdio pipes, and a real
    ProcessRegistry so the lease lifecycle is genuine, not simulated.
    """

    from tests.test_codex_rpc import FakeCodexServer

    processes = ProcessRegistry(database, EventStore(database), grace_seconds=1.0)
    server = FakeCodexServer(tmp_path)
    rpc = CodexRpcClient(
        server.command,
        base_env={},
        handshake_timeout=5.0,
        termination_timeout=2.0,
        processes=processes,
        project_key="merger",
    )
    session = MergerSession(
        _flow(
            rpc=rpc, merger=FakeMerger(), turns=FakeTurns(), reviews=FakeReviews()
        ),
        ("demo",),
    )

    opened = await session.open("live_wake")

    assert opened is True
    leases = processes.active("merger")
    assert len(leases) == 1
    assert leases[0].kind == "codex_app_server"

    await session.release("live_release")

    assert processes.active("merger") == ()


@pytest.mark.asyncio
async def test_reconcile_reopens_after_the_connection_ended_on_its_own() -> None:
    """A dead App Server connection must not leave the session stuck open:
    the listener has finished on its own, so reconcile releases (freeing
    the lease) and, with review work still active, reopens a fresh
    connection instead of waiting on a dead one forever."""

    rpc = FakeRpc()
    flow = _flow(rpc=rpc, merger=FakeMerger(), turns=FakeTurns(), reviews=FakeReviews())
    session = MergerSession(flow, ("demo",), is_review_active=lambda: True)
    assert await session.open("test")
    assert session.is_open
    # The child dies: the notification stream ends without any close()
    # call, so the listener task completes while _open stays True.
    await rpc._queue.put(_CLOSED)
    await _wait_until(
        lambda: session._listener is not None and session._listener.done()
    )

    await session.reconcile("maintenance")

    assert session.is_open
    assert rpc.close_calls == 1
    assert rpc.start_calls == 2


@pytest.fixture
def merge_flow(tmp_path: Path):
    """The real merge flow (real MergerTurnService over real SQLite)."""

    harness = ProductionShapedFlow(tmp_path)
    try:
        yield harness
    finally:
        harness.close()


async def _orphan_merged_submission(harness: ProductionShapedFlow) -> str:
    """Drive a real approval to merged, then re-open its submitted row.

    INFRA-223's observed shape: the wake is durably 'completed' and the
    review durably 'merged', but the submission's own terminal write was
    lost, leaving the row 'submitted'.
    """

    await harness.merger.ensure_thread("demo")
    branch = harness.stage("ENG-9", SHA_A, pr_number=14)
    emitted = await harness.emitter.emit(
        "demo", "ENG-9", verification=(("t", "ok"),)
    )
    settled = await harness.turns.submit_review(
        "demo",
        issue_id="ENG-9",
        event_id=emitted.event.event_id,
        candidate_sha=SHA_A,
        reviewed_thread_id="thr_legacy",
        reviewed_generation=1,
        verdict_json=harness.verdict(SHA_A, branch, 14),
    )
    assert settled.kind == "merged"
    harness.database.execute(
        "UPDATE submitted_verdicts SET state = 'submitted', result_json = NULL "
        "WHERE event_id = ?",
        (emitted.event.event_id,),
    )
    return str(emitted.event.event_id)


@pytest.mark.asyncio
async def test_startup_reconciles_an_orphaned_submission_and_holds_nothing(
    merge_flow: ProductionShapedFlow,
) -> None:
    """INFRA-223: a historical submission never holds the Sol thread.

    Before the fix ``has_pending_submission`` reported ANY 'submitted'
    row as live review work, so this startup reopened — and kept — an App
    Server over an idle thread ("open in another app"). The startup
    boundary now repairs the row durably and the session releases.
    """

    event_id = await _orphan_merged_submission(merge_flow)
    rpc = FakeRpc()
    session = MergerSession(
        _flow(
            rpc=rpc,
            merger=FakeMerger(),
            turns=merge_flow.turns,
            reviews=merge_flow.reviews,
        ),
        ("demo",),
    )

    await session.startup()

    # The row was repaired durably, not merely filtered out of a query.
    row = merge_flow.database.execute(
        "SELECT state, result_json FROM submitted_verdicts WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    assert str(row["state"]) == "settled"
    assert json.loads(str(row["result_json"]))["kind"] == "merged"
    # No review work is active, so the App Server opened for the recovery
    # pass is released again and no later tick reopens one.
    assert session.review_active() is False
    assert session.is_open is False
    assert rpc.close_calls == 1
    await session.reconcile("maintenance")
    assert session.is_open is False
    assert rpc.start_calls == 1
