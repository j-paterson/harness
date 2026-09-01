from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.chat_resume import (
    CHAT_RESUME_EVENT_TYPE,
    CHAT_RESUME_LITERAL,
    ChatResumeService,
    ChatResumeWakes,
    HermesChatResumeTransport,
    classify_recovery,
)
from hermes_orchestrator.cmux import CmuxSurfaceRef
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.orchestrator_workspace import OrchestratorWorkspaceState

WORKSPACE_UUID = "11111111-1111-4111-8111-111111111111"


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


def build_service(
    database: Database,
    *,
    transport: object,
    ids: Iterator[str] | None = None,
) -> ChatResumeService:
    generator = ids or iter(f"wake-{index}" for index in range(1, 100))
    events = EventStore(database)
    wakes = ChatResumeWakes(
        database=database,
        now=lambda: datetime(2026, 8, 31, 12, 0, tzinfo=UTC),
        ids=lambda: next(generator),
    )
    return ChatResumeService(
        database=database,
        events=events,
        session_name="orchestrator",
        wakes=wakes,
        transport=transport,  # type: ignore[arg-type]
    )


def recovered_state(
    *, generation: int = 1, respawned: tuple[str, ...] = ("hermes",)
) -> OrchestratorWorkspaceState:
    return OrchestratorWorkspaceState(
        workspace_uuid=WORKSPACE_UUID,
        upper=CmuxSurfaceRef(workspace_uuid=WORKSPACE_UUID, surface_uuid="up"),
        lower=CmuxSurfaceRef(workspace_uuid=WORKSPACE_UUID, surface_uuid="lo"),
        binding_id="binding-1",
        generation=generation,
        outcome="recovered",
        respawned=respawned,
    )


def created_state(
    *, generation: int, binding_id: str = "binding-1"
) -> OrchestratorWorkspaceState:
    return OrchestratorWorkspaceState(
        workspace_uuid=WORKSPACE_UUID,
        upper=CmuxSurfaceRef(workspace_uuid=WORKSPACE_UUID, surface_uuid="up"),
        lower=CmuxSurfaceRef(workspace_uuid=WORKSPACE_UUID, surface_uuid="lo"),
        binding_id=binding_id,
        generation=generation,
        outcome="created",
        respawned=(),
    )


def adopted_state() -> OrchestratorWorkspaceState:
    return OrchestratorWorkspaceState(
        workspace_uuid=WORKSPACE_UUID,
        upper=CmuxSurfaceRef(workspace_uuid=WORKSPACE_UUID, surface_uuid="up"),
        lower=CmuxSurfaceRef(workspace_uuid=WORKSPACE_UUID, surface_uuid="lo"),
        binding_id="binding-1",
        generation=1,
        outcome="adopted",
        respawned=(),
    )


class FakeTransport:
    """A canned transport that never touches the real hermes binary."""

    def __init__(self, results: list[bool | Exception]) -> None:
        self._results = list(results)
        self.calls: list[str] = []

    async def deliver(self, *, session_name: str) -> bool:
        self.calls.append(session_name)
        outcome = self._results.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def event_rows(database: Database) -> list[dict[str, object]]:
    rows = database.execute(
        "SELECT event_type, payload_json FROM events "
        "WHERE event_type = ? ORDER BY sequence",
        (CHAT_RESUME_EVENT_TYPE,),
    ).fetchall()
    import json

    return [json.loads(row["payload_json"]) for row in rows]


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def test_classify_recovery_recognizes_lower_respawn() -> None:
    assert classify_recovery(recovered_state()) == "recovered_lower_respawn"


def test_classify_recovery_recognizes_replacement() -> None:
    assert classify_recovery(created_state(generation=2)) == "created_replacement"


def test_classify_recovery_is_none_for_first_ever_creation() -> None:
    assert classify_recovery(created_state(generation=1)) is None


def test_classify_recovery_is_none_for_upper_only_respawn() -> None:
    state = recovered_state(respawned=("dashboard",))
    assert classify_recovery(state) is None


def test_classify_recovery_is_none_for_adoption() -> None:
    assert classify_recovery(adopted_state()) is None


# ---------------------------------------------------------------------------
# (1) recovery with lower respawn claims + delivers exactly once
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lower_respawn_delivers_exactly_once(database: Database) -> None:
    transport = FakeTransport([True, True])
    service = build_service(database, transport=transport)
    state = recovered_state()

    await service.observe(state)
    await service.observe(state)

    assert transport.calls == ["orchestrator"]
    rows = database.execute("SELECT * FROM chat_resume_wakes").fetchall()
    assert len(rows) == 1
    assert rows[0]["state"] == "delivered"
    assert rows[0]["delivered_at"] is not None


# ---------------------------------------------------------------------------
# (2) adopted state delivers nothing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_adopted_state_delivers_nothing(database: Database) -> None:
    transport = FakeTransport([])
    service = build_service(database, transport=transport)

    await service.observe(adopted_state())

    assert transport.calls == []
    rows = database.execute("SELECT * FROM chat_resume_wakes").fetchall()
    assert rows == []
    assert event_rows(database) == []


# ---------------------------------------------------------------------------
# (3) rebuild/replacement outcome delivers once
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replacement_delivers_once(database: Database) -> None:
    transport = FakeTransport([True])
    service = build_service(database, transport=transport)

    await service.observe(created_state(generation=2))
    await service.observe(created_state(generation=2))

    assert transport.calls == ["orchestrator"]
    rows = database.execute("SELECT * FROM chat_resume_wakes").fetchall()
    assert len(rows) == 1
    assert rows[0]["state"] == "delivered"


# ---------------------------------------------------------------------------
# (4) failed delivery leaves the claim unsettled; later pass retries once
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failed_delivery_retries_and_settles_exactly_once(
    database: Database,
) -> None:
    transport = FakeTransport([False, True, True])
    service = build_service(database, transport=transport)
    state = recovered_state()

    await service.observe(state)  # fails; stays pending
    rows = database.execute("SELECT * FROM chat_resume_wakes").fetchall()
    assert len(rows) == 1
    assert rows[0]["state"] == "pending"
    assert rows[0]["last_error"] is not None

    await service.observe(state)  # succeeds; settles
    await service.observe(state)  # already delivered; no further attempt

    assert transport.calls == ["orchestrator", "orchestrator"]
    rows = database.execute("SELECT * FROM chat_resume_wakes").fetchall()
    assert len(rows) == 1
    assert rows[0]["state"] == "delivered"


@pytest.mark.asyncio
async def test_raising_transport_is_treated_as_failure(database: Database) -> None:
    transport = FakeTransport([RuntimeError("boom"), True])
    service = build_service(database, transport=transport)
    state = recovered_state()

    await service.observe(state)
    rows = database.execute("SELECT * FROM chat_resume_wakes").fetchall()
    assert rows[0]["state"] == "pending"
    assert "boom" in str(rows[0]["last_error"])

    await service.observe(state)
    rows = database.execute("SELECT * FROM chat_resume_wakes").fetchall()
    assert rows[0]["state"] == "delivered"


# ---------------------------------------------------------------------------
# (5) the composed argv refuses a malformed session name
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transport_refuses_malformed_session_name() -> None:
    calls: list[tuple[str, ...]] = []

    async def process_factory(*argv: str, **kwargs: object) -> None:
        calls.append(argv)
        raise AssertionError("must not spawn for a malformed session name")

    transport = HermesChatResumeTransport(process_factory=process_factory)

    with pytest.raises(ValueError):
        await transport.deliver(session_name="not a valid name!")

    assert calls == []


@pytest.mark.asyncio
async def test_transport_composes_the_pinned_grammar_bound_argv() -> None:
    captured: dict[str, object] = {}

    class FakeProcess:
        returncode = 0

        async def wait(self) -> int:
            return 0

    async def process_factory(*argv: str, **kwargs: object) -> FakeProcess:
        captured["argv"] = argv
        return FakeProcess()

    transport = HermesChatResumeTransport(process_factory=process_factory)

    delivered = await transport.deliver(session_name="orchestrator")

    assert delivered is True
    assert captured["argv"] == (
        "hermes",
        "chat",
        "--continue",
        "orchestrator",
        "--create-if-missing",
        "-q",
        CHAT_RESUME_LITERAL,
        "-Q",
    )


@pytest.mark.asyncio
async def test_transport_does_not_kill_a_still_running_turn() -> None:
    """A still-running process is treated as delivered, never killed.

    Killing it would reproduce the exact SIGHUP-an-active-turn bug
    INFRA-216 exists to fix.
    """

    killed = False

    class NeverExitsProcess:
        returncode = None

        async def wait(self) -> int:
            import asyncio

            await asyncio.sleep(10)
            return 0

        def kill(self) -> None:
            nonlocal killed
            killed = True

    async def process_factory(*argv: str, **kwargs: object) -> NeverExitsProcess:
        return NeverExitsProcess()

    transport = HermesChatResumeTransport(
        process_factory=process_factory, timeout_seconds=0.05
    )

    delivered = await transport.deliver(session_name="orchestrator")

    assert delivered is True
    assert killed is False


# ---------------------------------------------------------------------------
# (6) one event journaled per recovery, carrying the delivery status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_event_journaled_per_recovery_with_delivery_status(
    database: Database,
) -> None:
    transport = FakeTransport([True])
    service = build_service(database, transport=transport)
    state = recovered_state()

    await service.observe(state)
    await service.observe(state)

    events = event_rows(database)
    assert len(events) == 1
    payload = events[0]
    assert payload["delivery_status"] == "delivered"
    assert payload["workspace_uuid"] == WORKSPACE_UUID
    assert payload["binding_id"] == "binding-1"
    assert payload["generation"] == 1
    assert payload["outcome"] == "recovered"
    assert payload["respawned"] == ["hermes"]
    assert payload["trigger"] == "recovered_lower_respawn"


@pytest.mark.asyncio
async def test_event_journaled_once_even_across_a_failed_then_retried_delivery(
    database: Database,
) -> None:
    transport = FakeTransport([False, True])
    service = build_service(database, transport=transport)
    state = recovered_state()

    await service.observe(state)
    events = event_rows(database)
    assert len(events) == 1
    assert events[0]["delivery_status"] == "failed"

    await service.observe(state)
    events = event_rows(database)
    assert len(events) == 1  # no second event on the settling retry
