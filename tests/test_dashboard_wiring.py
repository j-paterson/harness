"""Runtime composition and daemon wiring for the Orchestrator dashboard.

The dashboard MVP modules are composed in runtime.py and ticked from the
daemon's exception-suppressed maintenance slot in cli.py; these tests pin
that wiring end to end over a tmp database. Nothing here calls a model.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.cli import _run_daemon
from hermes_orchestrator.config import load_settings
from hermes_orchestrator.dashboard_pane import DashboardPane
from hermes_orchestrator.dashboard_refresh import DashboardRefreshAction
from hermes_orchestrator.dashboard_sources import (
    CodexFact,
    DashboardSnapshot,
    ProfileUsage,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.profiles import ProfileConfig, ProfileRegistry
from hermes_orchestrator.runtime import open_runtime
from tests.test_runtime import EligibleProfileCommand, FakeKeychain, active_repo
from tests.test_supervisor import FakeService

_ALIASES = ("max-a", "max-b", "max-c", "max-d")


class _TtyStream(io.StringIO):
    """StringIO that reports a TTY so the pane writes for real."""

    def isatty(self) -> bool:
        return True


class _ScriptedSources:
    """Fail on scripted ticks, then serve a fixed snapshot."""

    def __init__(self, failures: int) -> None:
        self.failures = failures

    async def collect(self, now: datetime) -> DashboardSnapshot:
        if self.failures > 0:
            self.failures -= 1
            raise TimeoutError("sources unavailable")
        return DashboardSnapshot(
            generated_at=now.isoformat(),
            usage=tuple(
                ProfileUsage(alias, fable_tokens=0, overall_tokens=0)
                for alias in _ALIASES
            ),
            leases=(),
            codex=CodexFact(
                available=False,
                unavailable_since=now.isoformat(),
            ),
        )


def _clock() -> datetime:
    return datetime(2026, 8, 30, 12, 0, tzinfo=UTC)


def _registry(tmp_path: Path) -> ProfileRegistry:
    return ProfileRegistry(
        tuple(ProfileConfig(alias, tmp_path / alias) for alias in _ALIASES)
    )


def _seeded_database(tmp_path: Path) -> Database:
    database = Database.open(tmp_path / "state.db")
    events = EventStore(database)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO profile_leases("
            "profile_alias, project_key, state, acquired_at, cooldown_until"
            ") VALUES (?, ?, ?, ?, ?)",
            ("max-b", "demo", "active", "2026-08-30T08:00:00+00:00", None),
        )
        events.append(
            connection,
            EventInput(
                event_type="stream.assistant",
                aggregate_type="project_cell",
                aggregate_id="cell-1",
                payload={
                    "profile_alias": "max-b",
                    "parent_tool_use_id": None,
                    "usage": {"input_tokens": 12, "output_tokens": 3},
                },
            ),
        )
    return database


def _composed_action(
    database: Database, tmp_path: Path, stream: _TtyStream
) -> DashboardRefreshAction:
    """Compose the daemon's pieces exactly as runtime.py does, on a
    fake TTY with a fixed size and clock for determinism."""

    return DashboardRefreshAction(
        database=database,
        registry=_registry(tmp_path),
        pane=DashboardPane(stream, rows=lambda: 50),
        now=_clock,
    )


def test_open_runtime_with_registry_composes_dashboard_refresh(
    tmp_path: Path,
) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        assert isinstance(runtime.dashboard_refresh, DashboardRefreshAction)
    finally:
        runtime.close()


def test_open_runtime_without_registry_composes_no_dashboard(
    tmp_path: Path,
) -> None:
    # Observation-only assembly never loads profiles.yaml, so there is
    # no registry to read from: the field stays None and the daemon
    # fails open to no dashboard instead of crashing.
    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(settings, enable_live=False)
    try:
        assert runtime.dashboard_refresh is None
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_daemon_maintenance_tick_drives_read_render_draw(
    tmp_path: Path,
) -> None:
    database = _seeded_database(tmp_path)
    try:
        stream = _TtyStream()
        action = _composed_action(database, tmp_path, stream)
        service = FakeService()

        await _run_daemon(
            service, once=True, interval=60, dashboard_refresh=action
        )

        output = stream.getvalue()
        # The scroll region was established exactly once (DECSTBM to the
        # fake 50-row terminal) even though both the startup hook and the
        # maintenance tick drew.
        assert output.count(";50r") == 1
        # Seeded durable rows made it through read -> render -> draw.
        assert "max-b" in output
        assert "demo/active" in output
        assert "last tick ok" in output
        assert service.ticks == 1
    finally:
        database.close()


@pytest.mark.asyncio
async def test_restart_recomposes_pane_and_renders_identical_content(
    tmp_path: Path,
) -> None:
    database = _seeded_database(tmp_path)
    try:
        first_stream = _TtyStream()
        await _run_daemon(
            FakeService(),
            once=True,
            interval=60,
            dashboard_refresh=_composed_action(database, tmp_path, first_stream),
        )

        # A daemon restart composes fresh pieces over the same durable
        # state; the fresh writer's idempotent establishment recovers the
        # region without any dedicated recovery path, and the full-journal
        # re-read reproduces the exact same frame.
        second_stream = _TtyStream()
        await _run_daemon(
            FakeService(),
            once=True,
            interval=60,
            dashboard_refresh=_composed_action(
                database, tmp_path, second_stream
            ),
        )

        assert second_stream.getvalue().count(";50r") == 1
        assert second_stream.getvalue() == first_stream.getvalue()
    finally:
        database.close()


@pytest.mark.asyncio
async def test_tick_exception_is_contained_and_surfaces_next_render(
    tmp_path: Path,
) -> None:
    stream = _TtyStream()
    action = DashboardRefreshAction(
        sources=_ScriptedSources(failures=1),
        pane=DashboardPane(stream, rows=lambda: 50),
        now=_clock,
    )
    service = FakeService()

    # The startup hook's tick fails: nothing propagates out of the
    # daemon and nothing is drawn for that tick. The maintenance tick
    # then renders the recorded failure as an explicit fact.
    supervisor = await _run_daemon(
        service, once=True, interval=60, dashboard_refresh=action
    )

    assert supervisor.ticks == 1
    output = stream.getvalue()
    assert "failed at 2026-08-30T12:00:00+00:00" in output
    assert "TimeoutError" in output
