from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from hermes_orchestrator.cells import ProjectCellService, RotationBlocked
from hermes_orchestrator.claude import ClaudeEvent, LeadTurnRequest
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.handoffs import HandoffService
from hermes_orchestrator.profiles import (
    CapacityObservation,
    ProfileHealth,
    ProfilePool,
    ProfileRegistry,
)
from hermes_orchestrator.queue import QueueService
from tests.test_cells import SESSION_ID, RecordingLinear, RecordingRunner, admit
from tests.test_handoffs import valid_handoff


@pytest.mark.asyncio
async def test_rotation_closes_replacement_stream_when_acknowledgement_fails(
    tmp_path: Path,
) -> None:
    config = tmp_path / "profiles.yaml"
    config.write_text(
        "profiles:\n"
        "  - {alias: max-a, config_dir: /tmp/max-a}\n"
        "  - {alias: max-b, config_dir: /tmp/max-b}\n"
        "  - {alias: max-c, config_dir: /tmp/max-c}\n"
        "  - {alias: max-d, config_dir: /tmp/max-d}\n",
        encoding="utf-8",
    )
    registry = ProfileRegistry.load(config)
    profiles = ProfilePool(registry)
    for profile in registry.profiles:
        profiles.record_health(
            ProfileHealth(
                profile_alias=profile.alias,
                eligible=True,
                reason="eligible",
                last_checked_at=datetime(2026, 8, 26, tzinfo=UTC),
            )
        )
    database = Database.open(tmp_path / "state.db")
    replacement_session = UUID("22222222-2222-4222-8222-222222222222")

    class ReplacementStream:
        def __init__(self) -> None:
            self.events = iter(
                [
                    ClaudeEvent(
                        "session.started",
                        "system",
                        replacement_session,
                        None,
                        None,
                        {},
                    ),
                    ClaudeEvent(
                        "handoff.acknowledged",
                        "result",
                        replacement_session,
                        None,
                        None,
                        {},
                        restated_next_action="Run the failing test.",
                    ),
                ]
            )
            self.closed = False

        def __aiter__(self):  # type: ignore[no-untyped-def]
            return self

        async def __anext__(self) -> ClaudeEvent:
            try:
                return next(self.events)
            except StopIteration as error:
                raise StopAsyncIteration from error

        async def aclose(self) -> None:
            self.closed = True

    stream = ReplacementStream()

    class RotationRunner(RecordingRunner):
        def start_lead(self, request: LeadTurnRequest):  # type: ignore[no-untyped-def]
            return stream

    try:
        now = datetime(2026, 8, 26, tzinfo=UTC).isoformat()
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO project_cells("
                "cell_id, project_key, state, profile_alias, session_id, "
                "created_at, updated_at) "
                "VALUES (?, 'demo', 'active', 'max-a', ?, ?, ?)",
                ("cell-demo", str(SESSION_ID), now, now),
            )
            connection.execute(
                "INSERT INTO profile_leases("
                "profile_alias, project_key, state, acquired_at) "
                "VALUES ('max-a', 'demo', 'active', ?)",
                (now,),
            )
        handoffs = HandoffService(database, handoff_ids=lambda: "handoff-1")
        record = handoffs.submit(valid_handoff())

        class FailingAcknowledgement:
            def get(self, handoff_id: str):  # type: ignore[no-untyped-def]
                return handoffs.get(handoff_id)

            def acknowledge(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                raise RuntimeError("acknowledgement failed")

        cells = ProjectCellService(
            database=database,
            events=EventStore(database),
            queue=QueueService(database, EventStore(database), {"demo"}),
            profiles=profiles,
            runner=RotationRunner(),
            linear=RecordingLinear(),
            project_paths={"demo": tmp_path},
            handoffs=FailingAcknowledgement(),  # type: ignore[arg-type]
            replacement_session_ids=lambda: replacement_session,
        )

        with pytest.raises(RuntimeError, match="acknowledgement failed"):
            await cells.rotate("cell-demo", record.handoff_id)

        assert stream.closed is True
    finally:
        database.close()


@pytest.mark.asyncio
async def test_old_lead_retires_only_after_acknowledgement(tmp_path: Path) -> None:
    config = tmp_path / "profiles.yaml"
    config.write_text(
        "profiles:\n"
        "  - {alias: max-a, config_dir: /tmp/max-a}\n"
        "  - {alias: max-b, config_dir: /tmp/max-b}\n"
        "  - {alias: max-c, config_dir: /tmp/max-c}\n"
        "  - {alias: max-d, config_dir: /tmp/max-d}\n",
        encoding="utf-8",
    )
    registry = ProfileRegistry.load(config)
    # This scenario seeds no capacity evidence on purpose: a pool built
    # WITHOUT a capacity-evidence port runs in the pre-INFRA-197-C2
    # as-today mode, where auth health alone admits a replacement. The
    # fail-closed capacity gate applies only once the port is injected.
    profiles = ProfilePool(registry)
    for profile in registry.profiles:
        profiles.record_health(
            ProfileHealth(
                profile_alias=profile.alias,
                eligible=True,
                reason="eligible",
                last_checked_at=datetime(2026, 8, 26, tzinfo=UTC),
            )
        )
    database = Database.open(tmp_path / "state.db")
    try:
        events = EventStore(database)
        queue = QueueService(database, events, {"demo"})
        runner = RecordingRunner()
        handoffs = HandoffService(database, handoff_ids=lambda: "handoff-1")
        cells = ProjectCellService(
            database=database,
            events=events,
            queue=queue,
            profiles=profiles,
            runner=runner,
            linear=RecordingLinear(),
            project_paths={"demo": tmp_path},
            session_ids=lambda: SESSION_ID,
            cell_ids=lambda: "cell-demo",
            handoffs=handoffs,
            replacement_session_ids=lambda: UUID(
                "22222222-2222-4222-8222-222222222222"
            ),
        )
        admit(queue, "ENG-9")
        await cells.dispatch("ENG-9")
        record = handoffs.submit(valid_handoff())

        with pytest.raises(RotationBlocked, match="acknowledged"):
            await cells.rotate("cell-demo", record.handoff_id)
        assert runner.retired_sessions == []
        assert database.scalar(
            "SELECT profile_alias FROM project_cells WHERE cell_id = 'cell-demo'"
        ) == "max-a"

        runner.raise_on_start = True
        with pytest.raises(RuntimeError, match="replacement start failed"):
            await cells.rotate("cell-demo", record.handoff_id)
        assert runner.retired_sessions == []
        assert database.scalar(
            "SELECT profile_alias FROM project_cells WHERE cell_id = 'cell-demo'"
        ) == "max-a"

        runner.raise_on_start = False
        runner.emit_handoff_ack = True
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO profile_leases("
                "profile_alias, project_key, state, acquired_at"
                ") VALUES ('max-b', 'conflict', 'active', 'now')"
            )
        with pytest.raises(sqlite3.IntegrityError):
            await cells.rotate("cell-demo", record.handoff_id)
        assert profiles.acquire("demo").profile_alias == "max-a"
        with database.transaction() as connection:
            connection.execute(
                "DELETE FROM profile_leases WHERE profile_alias = 'max-b'"
            )

        rotated = await cells.rotate("cell-demo", record.handoff_id)

        replacement_session = UUID("22222222-2222-4222-8222-222222222222")
        assert rotated.profile_alias == "max-b"
        assert rotated.session_id == replacement_session
        assert runner.retired_sessions == [SESSION_ID]
        acknowledged = handoffs.get(record.handoff_id)
        assert acknowledged.state == "acknowledged"
        assert acknowledged.replacement_session_id == replacement_session
        replacement_request = runner.start_requests[-1]
        assert replacement_request.profile_alias == "max-b"
        assert replacement_request.output_schema is not None
        assert "# Project lead handoff" in replacement_request.prompt
        assert database.scalar(
            "SELECT profile_alias FROM project_cells WHERE cell_id = 'cell-demo'"
        ) == "max-b"
    finally:
        database.close()


@pytest.mark.asyncio
async def test_recovery_restores_pool_affinity_to_the_persisted_profile(
    tmp_path: Path,
) -> None:
    """Sol b4b545f3 P2: an acknowledged-but-untransferred rotation recovers
    on the persisted profile — never a fresh selection (which would order
    max-b first) — and leaves the pool's project affinity on that exact
    profile. A repeat call after the transfer is durable stays a no-op."""

    config = tmp_path / "profiles.yaml"
    config.write_text(
        "profiles:\n"
        "  - {alias: max-a, config_dir: /tmp/max-a}\n"
        "  - {alias: max-b, config_dir: /tmp/max-b}\n"
        "  - {alias: max-c, config_dir: /tmp/max-c}\n"
        "  - {alias: max-d, config_dir: /tmp/max-d}\n",
        encoding="utf-8",
    )
    registry = ProfileRegistry.load(config)
    profiles = ProfilePool(registry)
    for profile in registry.profiles:
        profiles.record_health(
            ProfileHealth(
                profile_alias=profile.alias,
                eligible=True,
                reason="eligible",
                last_checked_at=datetime(2026, 8, 26, tzinfo=UTC),
            )
        )
    database = Database.open(tmp_path / "state.db")
    try:
        events = EventStore(database)
        queue = QueueService(database, events, {"demo"})
        runner = RecordingRunner()
        handoffs = HandoffService(database, handoff_ids=lambda: "handoff-1")
        replacement_session = UUID("22222222-2222-4222-8222-222222222222")
        cells = ProjectCellService(
            database=database,
            events=events,
            queue=queue,
            profiles=profiles,
            runner=runner,
            linear=RecordingLinear(),
            project_paths={"demo": tmp_path},
            session_ids=lambda: SESSION_ID,
            cell_ids=lambda: "cell-demo",
            handoffs=handoffs,
            replacement_session_ids=lambda: replacement_session,
        )
        admit(queue, "ENG-9")
        await cells.dispatch("ENG-9")
        record = handoffs.submit(valid_handoff())
        handoffs.acknowledge(
            record.handoff_id,
            replacement_session,
            "Run the failing unit test and correct ENG-9.",
            profile_alias="max-c",
        )
        starts_after_dispatch = runner.start_count

        rotated = await cells.rotate("cell-demo", record.handoff_id)

        assert rotated.profile_alias == "max-c"
        assert rotated.session_id == replacement_session
        assert runner.start_count == starts_after_dispatch  # no ack turn re-run
        assert runner.retired_sessions == [SESSION_ID]
        assert profiles.acquire("demo").profile_alias == "max-c"
        assert database.scalar(
            "SELECT profile_alias FROM profile_leases WHERE project_key = 'demo'"
        ) == "max-c"

        again = await cells.rotate("cell-demo", record.handoff_id)

        assert again.session_id == replacement_session
        assert again.profile_alias == "max-c"
        assert database.scalar(
            "SELECT count(*) FROM events "
            "WHERE event_type = 'project_cell.rotated'"
        ) == 1
        assert profiles.acquire("demo").profile_alias == "max-c"
    finally:
        database.close()


# --- INFRA-197 correction C2: Fable-capacity evidence gates replacement ---
#
# Motivating incident: max-a was auth-healthy (first-party Max) while its
# weekly Fable usage sat at 100%. Auth health alone is not capacity
# evidence, so reserve_replacement must fail closed unless the candidate
# has a current, non-capped fable-capacity observation.

NOW = datetime(2026, 8, 26, 12, tzinfo=UTC)


class FakeCapacityEvidence:
    """In-memory capacity-evidence port for pool-level tests."""

    def __init__(self) -> None:
        self.observations: dict[tuple[str, str], CapacityObservation] = {}

    def record(self, observation: CapacityObservation) -> None:
        self.observations[(observation.profile_alias, observation.model)] = (
            observation
        )

    def latest(
        self, profile_alias: str, model: str
    ) -> CapacityObservation | None:
        return self.observations.get((profile_alias, model))


def _registry(tmp_path: Path) -> ProfileRegistry:
    config = tmp_path / "profiles.yaml"
    config.write_text(
        "profiles:\n"
        "  - {alias: max-a, config_dir: /tmp/max-a}\n"
        "  - {alias: max-b, config_dir: /tmp/max-b}\n"
        "  - {alias: max-c, config_dir: /tmp/max-c}\n"
        "  - {alias: max-d, config_dir: /tmp/max-d}\n",
        encoding="utf-8",
    )
    return ProfileRegistry.load(config)


def _gated_pool(
    tmp_path: Path,
    clock: list[datetime],
    evidence: FakeCapacityEvidence | None,
) -> ProfilePool:
    registry = _registry(tmp_path)
    pool = ProfilePool(
        registry,
        now=lambda: clock[0],
        capacity_evidence=evidence,
    )
    for profile in registry.profiles:
        pool.record_health(
            ProfileHealth(
                profile_alias=profile.alias,
                eligible=True,
                reason="eligible",
                last_checked_at=clock[0],
            )
        )
    pool.restore("demo", "max-a", clock[0])
    return pool


def test_reserve_replacement_refuses_without_fable_capacity_evidence(
    tmp_path: Path,
) -> None:
    clock = [NOW]
    pool = _gated_pool(tmp_path, clock, FakeCapacityEvidence())

    reservation = pool.reserve_replacement("demo", "max-a")

    assert reservation is None
    assert pool.last_refusal is not None
    assert "no current fable capacity evidence" in pool.last_refusal
    assert "max-b" in pool.last_refusal


def test_fresh_available_observation_permits_replacement(tmp_path: Path) -> None:
    clock = [NOW]
    evidence = FakeCapacityEvidence()
    evidence.record(
        CapacityObservation(
            profile_alias="max-b",
            model="fable",
            state="available",
            source="operator_attestation",
            observed_at=NOW - timedelta(hours=1),
        )
    )
    pool = _gated_pool(tmp_path, clock, evidence)

    reservation = pool.reserve_replacement("demo", "max-a")

    assert reservation is not None
    assert reservation.profile_alias == "max-b"
    assert pool.last_refusal is None


def test_capped_observation_refuses_until_its_reset_horizon(
    tmp_path: Path,
) -> None:
    clock = [NOW]
    evidence = FakeCapacityEvidence()
    evidence.record(
        CapacityObservation(
            profile_alias="max-b",
            model="fable",
            state="capped",
            source="provider_limit",
            observed_at=NOW - timedelta(hours=1),
            resets_at=NOW + timedelta(hours=2),
        )
    )
    pool = _gated_pool(tmp_path, clock, evidence)

    assert pool.reserve_replacement("demo", "max-a") is None
    assert pool.last_refusal is not None
    assert "fable-capped until" in pool.last_refusal

    # Once the recorded horizon passes, the cap is stale: the budget has
    # cycled and the same observation now evidences availability.
    clock[0] = NOW + timedelta(hours=3)
    reservation = pool.reserve_replacement("demo", "max-a")
    assert reservation is not None
    assert reservation.profile_alias == "max-b"


def test_capped_observation_without_horizon_fails_closed(tmp_path: Path) -> None:
    clock = [NOW]
    evidence = FakeCapacityEvidence()
    evidence.record(
        CapacityObservation(
            profile_alias="max-b",
            model="fable",
            state="capped",
            source="operator_attestation",
            observed_at=NOW - timedelta(hours=1),
            resets_at=None,
        )
    )
    pool = _gated_pool(tmp_path, clock, evidence)

    # Even far beyond any weekly cycle, a horizonless cap only clears via
    # a newer observation: fail closed, and say what is needed.
    clock[0] = NOW + timedelta(days=30)
    assert pool.reserve_replacement("demo", "max-a") is None
    assert pool.last_refusal is not None
    assert "no reset horizon" in pool.last_refusal


def test_stale_available_observation_is_not_current(tmp_path: Path) -> None:
    clock = [NOW]
    evidence = FakeCapacityEvidence()
    evidence.record(
        CapacityObservation(
            profile_alias="max-b",
            model="fable",
            state="available",
            source="operator_attestation",
            # Older than one full weekly Fable budget cycle: it says
            # nothing about the budget window running now.
            observed_at=NOW - timedelta(days=8),
        )
    )
    pool = _gated_pool(tmp_path, clock, evidence)

    assert pool.reserve_replacement("demo", "max-a") is None
    assert pool.last_refusal is not None
    assert "stale" in pool.last_refusal


def test_pool_without_capacity_port_behaves_as_today(tmp_path: Path) -> None:
    # Degrade-safe pin: un-migrated construction sites (runtime/CLI) keep
    # today's auth-health-only behavior until the later wiring packet.
    clock = [NOW]
    pool = _gated_pool(tmp_path, clock, None)

    reservation = pool.reserve_replacement("demo", "max-a")

    assert reservation is not None
    assert reservation.profile_alias == "max-b"


def test_context_only_rotation_retains_the_incumbent_profile(
    tmp_path: Path,
) -> None:
    # A context-only (session-only) rotation keeps the same profile, so
    # it needs no new-capacity evidence and must not consume another
    # profile's occupancy.
    clock = [NOW]
    pool = _gated_pool(tmp_path, clock, FakeCapacityEvidence())

    retained = pool.reserve_context_only("demo")

    assert retained.profile_alias == "max-a"
    # No other profile's occupancy was consumed: the next project still
    # gets the first free slot.
    other = pool.acquire("other")
    assert other is not None
    assert other.profile_alias == "max-b"


@pytest.mark.asyncio
async def test_rotation_blocked_message_names_the_evidence_gap(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    clock = [NOW]
    profiles = ProfilePool(
        registry,
        now=lambda: clock[0],
        capacity_evidence=FakeCapacityEvidence(),
    )
    for profile in registry.profiles:
        profiles.record_health(
            ProfileHealth(
                profile_alias=profile.alias,
                eligible=True,
                reason="eligible",
                last_checked_at=clock[0],
            )
        )
    database = Database.open(tmp_path / "state.db")
    try:
        events = EventStore(database)
        queue = QueueService(database, events, {"demo"})
        runner = RecordingRunner()
        handoffs = HandoffService(database, handoff_ids=lambda: "handoff-1")
        cells = ProjectCellService(
            database=database,
            events=events,
            queue=queue,
            profiles=profiles,
            runner=runner,
            linear=RecordingLinear(),
            project_paths={"demo": tmp_path},
            session_ids=lambda: SESSION_ID,
            cell_ids=lambda: "cell-demo",
            now=lambda: clock[0],
            handoffs=handoffs,
        )
        admit(queue, "ENG-9")
        await cells.dispatch("ENG-9")
        record = handoffs.submit(valid_handoff())

        with pytest.raises(
            RotationBlocked, match="no current fable capacity evidence"
        ):
            await cells.rotate("cell-demo", record.handoff_id)
    finally:
        database.close()
