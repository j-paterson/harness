from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from hermes_orchestrator.cells import ProjectCellService, RotationBlocked
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.handoffs import HandoffService
from hermes_orchestrator.profiles import ProfileHealth, ProfilePool, ProfileRegistry
from hermes_orchestrator.queue import QueueService
from tests.test_cells import SESSION_ID, RecordingLinear, RecordingRunner, admit
from tests.test_handoffs import valid_handoff


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
