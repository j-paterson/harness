"""Tick-driven dashboard refresh in the daemon's maintenance slot."""

from __future__ import annotations

import io
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

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

_ALIASES = ("max-a", "max-b", "max-c", "max-d")


class _TtyStream(io.StringIO):
    def isatty(self) -> bool:  # pragma: no cover - trivial
        return True


class _RecordingPane:
    def __init__(self) -> None:
        self.frames: list[tuple[str, ...]] = []

    def draw(self, lines: Sequence[str]) -> None:
        self.frames.append(tuple(lines))

    def restore(self) -> None:  # pragma: no cover - protocol completeness
        return None


class _ScriptedSources:
    """Fail on scripted ticks, then serve a fixed snapshot."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.collected = 0

    async def collect(self, now: datetime) -> DashboardSnapshot:
        self.collected += 1
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


@pytest.mark.asyncio
async def test_one_tick_reads_sources_renders_and_draws() -> None:
    pane = _RecordingPane()
    action = DashboardRefreshAction(
        sources=_ScriptedSources(failures=0),
        pane=pane,
        now=_clock,
        width=80,
    )
    await action.tick()
    assert len(pane.frames) == 1
    frame = pane.frames[0]
    assert all(len(line) == 80 for line in frame)
    assert any("last tick ok" in line for line in frame)


@pytest.mark.asyncio
async def test_tick_failure_is_recorded_on_the_next_success() -> None:
    pane = _RecordingPane()
    action = DashboardRefreshAction(
        sources=_ScriptedSources(failures=1),
        pane=pane,
        now=_clock,
        width=100,
    )
    # The failing tick never raises through the maintenance slot and
    # draws nothing.
    await action.tick()
    assert pane.frames == []

    # The next success renders the failure as an explicit recorded fact.
    await action.tick()
    status = next(line for line in pane.frames[0] if "tick" in line)
    assert "failed at 2026-08-30T12:00:00+00:00" in status
    assert "TimeoutError" in status

    # Once shown, a later healthy tick reports ok again.
    await action.tick()
    assert any("last tick ok" in line for line in pane.frames[1])


@pytest.mark.asyncio
async def test_repeated_ticks_reestablish_the_pane_after_interruption() -> None:
    stream = _TtyStream()
    pane = DashboardPane(stream, rows=lambda: 50)
    action = DashboardRefreshAction(
        sources=_ScriptedSources(failures=0),
        pane=pane,
        now=_clock,
        width=80,
    )
    await action.tick()
    establishments = stream.getvalue().count(";50r")
    assert establishments == 1

    # An interruption tears the region down; the next tick re-establishes
    # it without any dedicated recovery path.
    pane.restore()
    await action.tick()
    assert stream.getvalue().count(";50r") == 2


@pytest.mark.asyncio
async def test_default_wiring_is_one_constructor_call(tmp_path: Path) -> None:
    # The deferred runtime.py wiring is exactly this constructor shape:
    # DashboardRefreshAction(database=..., registry=...) with every other
    # collaborator defaulted per the Optional=None convention.
    database = Database.open(tmp_path / "state.db")
    events = EventStore(database)
    with database.transaction() as connection:
        events.append(
            connection,
            EventInput(
                event_type="stream.assistant",
                aggregate_type="project_cell",
                aggregate_id="cell-1",
                payload={
                    "profile_alias": "max-a",
                    "parent_tool_use_id": None,
                    "usage": {"input_tokens": 10},
                },
            ),
        )
    action = DashboardRefreshAction(
        database=database,
        registry=_registry(tmp_path),
    )
    # The default pane targets stdout, which is not a TTY under pytest,
    # so the tick is a safe no-op surface-wise while sources still run.
    await action.tick()
    database.close()


def test_construction_requires_sources_or_database() -> None:
    with pytest.raises(ValueError):
        DashboardRefreshAction()


# ---------------------------------------------------------------------------
# INFRA-209: frame=True renders via render_frame with live callable
# width/height, resolved fresh every tick (a live terminal-size read).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_frame_true_renders_via_render_frame_with_exact_size() -> None:
    pane = _RecordingPane()
    action = DashboardRefreshAction(
        sources=_ScriptedSources(failures=0),
        pane=pane,
        now=_clock,
        frame=True,
        width=lambda: 44,
        height=lambda: 12,
    )
    await action.tick()
    assert len(pane.frames) == 1
    frame = pane.frames[0]
    assert len(frame) == 12
    assert all(len(line) == 44 for line in frame)
    assert any("HERMES" in line for line in frame)
    assert any("Attention" in line for line in frame)


@pytest.mark.asyncio
async def test_frame_true_resolves_width_and_height_callables_every_tick() -> None:
    pane = _RecordingPane()
    sizes = [(40, 10), (60, 16)]
    index = {"value": 0}
    action = DashboardRefreshAction(
        sources=_ScriptedSources(failures=0),
        pane=pane,
        now=_clock,
        frame=True,
        width=lambda: sizes[index["value"]][0],
        height=lambda: sizes[index["value"]][1],
    )
    await action.tick()
    first = pane.frames[0]
    assert len(first) == 10
    assert all(len(line) == 40 for line in first)

    index["value"] = 1
    await action.tick()
    second = pane.frames[1]
    assert len(second) == 16
    assert all(len(line) == 60 for line in second)


@pytest.mark.asyncio
async def test_frame_true_color_flag_reaches_render_frame() -> None:
    pane = _RecordingPane()
    action = DashboardRefreshAction(
        sources=_ScriptedSources(failures=0),
        pane=pane,
        now=_clock,
        frame=True,
        width=44,
        height=14,
        color=True,
    )
    await action.tick()
    frame = pane.frames[0]
    assert any("\x1b[" in line for line in frame)


@pytest.mark.asyncio
async def test_frame_true_detail_flag_adds_the_detail_section() -> None:
    pane = _RecordingPane()
    without_detail = DashboardRefreshAction(
        sources=_ScriptedSources(failures=0),
        pane=pane,
        now=_clock,
        frame=True,
        width=90,
        height=24,
        detail=False,
    )
    await without_detail.tick()
    assert not any("Detail" in line for line in pane.frames[0])

    with_detail = DashboardRefreshAction(
        sources=_ScriptedSources(failures=0),
        pane=pane,
        now=_clock,
        frame=True,
        width=90,
        height=24,
        detail=True,
    )
    await with_detail.tick()
    assert any("Detail" in line for line in pane.frames[1])
