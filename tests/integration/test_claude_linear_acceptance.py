from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from hermes_orchestrator.cells import ProjectCellService
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.hermes_tools import HermesCommandService
from hermes_orchestrator.profiles import ProfileHealth, ProfilePool, ProfileRegistry
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.scheduler import Scheduler
from tests.test_cells import RecordingLinear, RecordingRunner
from tests.test_scheduler_cells import GreenSnapshot


@pytest.mark.asyncio
async def test_explicit_queue_to_one_lead_and_linear_projection(tmp_path: Path) -> None:
    profile_config = tmp_path / "profiles.yaml"
    profile_config.write_text(
        "profiles:\n"
        "  - {alias: max-a, config_dir: /tmp/max-a}\n"
        "  - {alias: max-b, config_dir: /tmp/max-b}\n"
        "  - {alias: max-c, config_dir: /tmp/max-c}\n"
        "  - {alias: max-d, config_dir: /tmp/max-d}\n",
        encoding="utf-8",
    )
    registry = ProfileRegistry.load(profile_config)
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
        command = HermesCommandService(queue)
        admitted = command.execute(
            {
                "intent": "queue_issue",
                "issue_id": "ENG-9",
                "project_key": "demo",
                "priority": 1,
                "operator_instruction_id": "chat-9",
            }
        )
        runner = RecordingRunner()
        linear = RecordingLinear()
        cells = ProjectCellService(
            database=database,
            events=events,
            queue=queue,
            profiles=profiles,
            runner=runner,
            linear=linear,
            project_paths={"demo": tmp_path},
            session_ids=lambda: UUID("11111111-1111-4111-8111-111111111111"),
            cell_ids=lambda: "cell-demo",
        )
        scheduler = Scheduler(
            queue,
            mode="active",
            active_projects=cells.active_projects,
        )

        actions = scheduler.plan(GreenSnapshot())
        result = await cells.dispatch(actions[0].issue_id)

        assert admitted.code == "queued"
        assert result.status == "working"
        assert runner.start_count == 1
        assert linear.targets == [("ENG-9", "In Development", "operator")]

        second = command.execute(
            {
                "intent": "queue_issue",
                "issue_id": "ENG-10",
                "project_key": "demo",
                "priority": 1,
                "operator_instruction_id": "chat-10",
            }
        )
        restarted_runner = RecordingRunner()
        restarted_profiles = ProfilePool(registry)
        restarted_cells = ProjectCellService(
            database=database,
            events=events,
            queue=queue,
            profiles=restarted_profiles,
            runner=restarted_runner,
            linear=linear,
            project_paths={"demo": tmp_path},
        )
        restarted_scheduler = Scheduler(
            queue,
            mode="active",
            active_projects=restarted_cells.active_projects,
        )

        assert restarted_profiles.acquire("demo").profile_alias == "max-a"
        resumed_actions = restarted_scheduler.plan(GreenSnapshot())
        # INFRA-199: a distinct queued issue never starts while another
        # issue of the project is still in development.
        busy = await restarted_cells.dispatch(resumed_actions[0].issue_id)
        assert busy.status == "project_busy"
        queue.complete("ENG-9", reason="merged", evidence="integration test")
        resumed = await restarted_cells.dispatch(resumed_actions[0].issue_id)

        assert second.code == "queued"
        assert resumed_actions[0].kind == "resume_project_cell"
        assert resumed.status == "working"
        assert restarted_runner.start_count == 0
        assert restarted_runner.resume_count == 1
        assert database.scalar("SELECT count(*) FROM project_cells") == 1
        assert linear.targets[-1] == ("ENG-10", "In Development", "operator")
    finally:
        database.close()
