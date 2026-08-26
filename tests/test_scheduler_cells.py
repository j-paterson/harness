from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.cells import ProjectCellService
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.profiles import ProfileHealth, ProfilePool, ProfileRegistry
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.scheduler import Scheduler
from tests.test_cells import SESSION_ID, RecordingLinear, RecordingRunner, admit


@dataclass(frozen=True)
class GreenSnapshot:
    pressure: str = "green"
    can_admit: bool = True


@pytest.mark.asyncio
async def test_scheduler_reads_active_projects_from_cells(
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
    try:
        events = EventStore(database)
        queue = QueueService(database, events, {"demo"})
        cell_service = ProjectCellService(
            database=database,
            events=events,
            queue=queue,
            profiles=profiles,
            runner=RecordingRunner(),
            linear=RecordingLinear(),
            project_paths={"demo": tmp_path},
            session_ids=lambda: SESSION_ID,
            cell_ids=lambda: "cell-demo",
        )
        admit(queue, "ENG-9")
        await cell_service.dispatch("ENG-9")
        admit(queue, "ENG-10")
        scheduler = Scheduler(
            queue,
            mode="active",
            active_projects=cell_service.active_projects,
        )

        actions = scheduler.plan(GreenSnapshot())

        assert not any(action.kind == "start_project_cell" for action in actions)
    finally:
        database.close()
