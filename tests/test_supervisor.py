from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from hermes_orchestrator.scheduler import PlannedAction
from hermes_orchestrator.supervisor import Supervisor


@dataclass(frozen=True)
class FakeTick:
    planned_actions: tuple[PlannedAction, ...] = ()


@dataclass(frozen=True)
class FakeReconciliation:
    admission_open: bool


class FakeService:
    def __init__(
        self,
        actions: tuple[PlannedAction, ...] = (),
        *,
        admission_open: bool = True,
    ) -> None:
        self.actions = actions
        self.admission_open = admission_open
        self.started = 0
        self.ticks = 0

    def start(self) -> object:
        self.started += 1
        return FakeReconciliation(self.admission_open)

    def tick(self) -> FakeTick:
        self.ticks += 1
        return FakeTick(self.actions)


@pytest.mark.asyncio
async def test_shutdown_stops_admission_before_children() -> None:
    checkpoint_events: list[str] = []

    async def checkpoint() -> None:
        checkpoint_events.append("requested")

    supervisor = Supervisor(
        FakeService(),
        checkpoint_workers=checkpoint,
        interval_seconds=60,
    )

    await supervisor.start()
    await supervisor.shutdown()

    assert checkpoint_events == ["requested"]
    assert supervisor.events[-3:] == [
        "admission.closed",
        "workers.checkpoint_requested",
        "supervisor.stopped",
    ]


@pytest.mark.asyncio
async def test_run_once_dispatches_only_enabled_actions() -> None:
    actions = (
        PlannedAction("start_project_cell", "demo", "ENG-9", "ready", True, {}),
        PlannedAction("start_project_cell", "other", "ENG-10", "dry", False, {}),
    )
    dispatched: list[str] = []

    async def dispatch(issue_id: str) -> None:
        dispatched.append(issue_id)

    supervisor = Supervisor(FakeService(actions), dispatch=dispatch)

    await supervisor.run_once()

    assert dispatched == ["ENG-9"]


@pytest.mark.asyncio
async def test_run_once_does_not_dispatch_when_reconciliation_closes_admission(
) -> None:
    action = PlannedAction(
        "start_project_cell",
        "demo",
        "ENG-9",
        "ready",
        True,
        {},
    )
    dispatched: list[str] = []

    async def dispatch(issue_id: str) -> None:
        dispatched.append(issue_id)

    supervisor = Supervisor(
        FakeService((action,), admission_open=False),
        dispatch=dispatch,
    )

    await supervisor.run_once()

    assert dispatched == []


@pytest.mark.asyncio
async def test_checkpoint_timeout_does_not_abort_shutdown() -> None:
    never = asyncio.Event()

    async def checkpoint() -> None:
        await never.wait()

    supervisor = Supervisor(
        FakeService(),
        checkpoint_workers=checkpoint,
        interval_seconds=60,
        checkpoint_timeout=0.01,
    )
    await supervisor.start()

    await supervisor.shutdown()

    assert supervisor.events[-3:] == [
        "admission.closed",
        "workers.checkpoint_requested",
        "supervisor.stopped",
    ]


@pytest.mark.asyncio
async def test_run_once_requests_exactly_one_checkpoint_per_tick() -> None:
    from hermes_orchestrator.admission import ResourceAction

    class RedService:
        def start(self) -> object:
            return type("R", (), {"admission_open": True})()

        def tick(self) -> object:
            return type(
                "T",
                (),
                {
                    "planned_actions": (),
                    "resource_actions": (
                        ResourceAction("stop_admission", None, "red"),
                        ResourceAction("request_checkpoint", "cell-a", "red"),
                        ResourceAction("request_checkpoint", "cell-b", "red"),
                    ),
                },
            )()

    requested: list[tuple[str, str]] = []

    async def request(cell_id: str, reason: str) -> None:
        requested.append((cell_id, reason))

    supervisor = Supervisor(RedService(), request_checkpoint=request)
    await supervisor.run_once()
    assert requested == [("cell-a", "red")]
    await supervisor.run_once()
    assert requested == [("cell-a", "red"), ("cell-a", "red")]


@pytest.mark.asyncio
async def test_supervisor_delivers_reserved_requests_through_the_dispatcher() -> None:
    from hermes_orchestrator.admission import ResourceAction

    class FakeDispatcher:
        def __init__(self) -> None:
            self.delivered: list[str] = []
            self.pending_before: str | None = "ckpt:old"

        def undelivered(self) -> str | None:
            value, self.pending_before = self.pending_before, None
            return value

        async def deliver(self, request_id: str) -> str:
            self.delivered.append(request_id)
            return "delivered"

    class RedService:
        def start(self) -> object:
            return type("R", (), {"admission_open": True})()

        def tick(self) -> object:
            return type(
                "T",
                (),
                {
                    "planned_actions": (),
                    "resource_actions": (
                        ResourceAction("stop_admission", None, "red"),
                        ResourceAction(
                            "request_checkpoint", "cell-a", "red", request_id="ckpt:a"
                        ),
                    ),
                },
            )()

    dispatcher = FakeDispatcher()
    supervisor = Supervisor(RedService(), checkpoint_dispatcher=dispatcher)
    await supervisor.run_once()
    # The reserved-before-restart request is delivered first, then the
    # tick's own reservation; the legacy callback path is not used.
    assert dispatcher.delivered == ["ckpt:old", "ckpt:a"]
    assert supervisor.checkpoint_deliveries == [
        ("ckpt:old", "delivered"),
        ("ckpt:a", "delivered"),
    ]


class FakeWakeDelivery:
    """Signal-bearing wake driver double mirroring LeadWakeDelivery."""

    def __init__(self) -> None:
        self.signal = asyncio.Event()
        self.drains = 0
        self.deliverable: list[str] = []

    async def drain(self) -> tuple[str, ...]:
        self.signal.clear()
        self.drains += 1
        delivered, self.deliverable = tuple(self.deliverable), []
        return delivered


@pytest.mark.asyncio
async def test_wake_signal_wakes_the_loop_before_the_interval() -> None:
    service = FakeService()
    delivery = FakeWakeDelivery()
    supervisor = Supervisor(
        service,
        wake_delivery=delivery,
        interval_seconds=60,
    )

    await supervisor.start()
    try:
        await asyncio.sleep(0.05)
        assert service.ticks == 1
        delivery.signal.set()
        await asyncio.sleep(0.1)
        # The committed wake woke the loop immediately instead of waiting
        # out the 60-second interval.
        assert service.ticks == 2
    finally:
        await supervisor.shutdown()


@pytest.mark.asyncio
async def test_run_once_drains_wake_deliveries() -> None:
    delivery = FakeWakeDelivery()
    delivery.deliverable = ["wake-1"]
    supervisor = Supervisor(FakeService(), wake_delivery=delivery)

    await supervisor.run_once()

    assert delivery.drains == 1
    assert supervisor.wake_deliveries == ["wake-1"]


@pytest.mark.asyncio
async def test_wake_drain_runs_every_tick() -> None:
    delivery = FakeWakeDelivery()
    supervisor = Supervisor(FakeService(), wake_delivery=delivery)

    await supervisor.run_once()
    await supervisor.run_once()

    # A row the transport left pending retries on the next tick: the
    # interval itself is the retry backoff.
    assert delivery.drains == 2


@pytest.mark.asyncio
async def test_failed_transport_retries_on_interval_without_polling(
    tmp_path: object,
) -> None:
    from datetime import UTC, datetime
    from pathlib import Path
    from uuid import UUID

    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.lead_wakes import (
        LeadTerminalWakes,
        LeadWakeDelivery,
        TerminalWakeInput,
    )

    database = Database.open(Path(str(tmp_path)) / "state.db")
    try:
        wakes = LeadTerminalWakes(
            database=database,
            events=EventStore(database),
            now=lambda: datetime(2026, 8, 28, tzinfo=UTC),
        )
        outcomes = [False, True]
        delivered: list[str] = []

        async def flaky_transport(wake: object) -> bool:
            accepted = outcomes.pop(0)
            if accepted:
                delivered.append(wake.wake_id)  # type: ignore[attr-defined]
            return accepted

        delivery = LeadWakeDelivery(wakes, transport=flaky_transport)
        supervisor = Supervisor(FakeService(), wake_delivery=delivery)
        wake = wakes.commit(
            TerminalWakeInput(
                project_key="demo",
                issue_id="INFRA-181",
                cell_id="cell-demo",
                session_id=UUID("11111111-1111-4111-8111-111111111111"),
                profile_alias="max-a",
                turn_key="evt-1",
                kind="completed",
                reason="turn_completed",
            )
        )

        # First tick: the consumer rejects, the row stays pending, and no
        # Hermes polling is required to keep it alive.
        await supervisor.run_once()
        assert supervisor.wake_deliveries == []
        assert [row.wake_id for row in wakes.pending()] == [wake.wake_id]

        # Next interval tick: the same row is pushed again and acknowledged
        # only after acceptance.
        await supervisor.run_once()
        assert supervisor.wake_deliveries == [wake.wake_id]
        assert delivered == [wake.wake_id]
        assert wakes.pending() == ()
        assert wakes.get(wake.wake_id).state == "delivered"
    finally:
        database.close()


@pytest.mark.asyncio
async def test_maintenance_runs_each_tick_and_never_breaks_the_loop() -> None:
    ran: list[int] = []

    async def maintenance() -> None:
        ran.append(1)
        raise RuntimeError("cmux went away")

    supervisor = Supervisor(FakeService(), maintenance=maintenance)

    await supervisor.run_once()
    await supervisor.run_once()

    assert len(ran) == 2
    assert supervisor.ticks == 2
