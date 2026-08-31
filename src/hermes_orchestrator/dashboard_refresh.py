"""Tick-driven dashboard refresh for the daemon's maintenance slot.

The action fits the supervisor's exception-suppressed 30-second
maintenance pattern (supervisor.py) and runs on the event-loop thread,
which owns the single SQLite connection (db.py). The deferred one-line
runtime.py wiring is exactly::

    dashboard_refresh = DashboardRefreshAction(
        database=database, registry=registry
    )

with every other collaborator defaulted per the Optional=None
convention, and cli.py's ``_maintenance`` then awaiting
``dashboard_refresh.tick()``.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Protocol

from hermes_orchestrator.dashboard_pane import DashboardPane
from hermes_orchestrator.dashboard_render import (
    TickFailure,
    render_dashboard,
    render_frame,
)
from hermes_orchestrator.dashboard_sources import (
    CodexRateLimitReader,
    DashboardSnapshot,
    DashboardSources,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.profiles import ProfileRegistry


class SnapshotSources(Protocol):
    """The provider port one refresh tick reads from."""

    async def collect(self, now: datetime) -> DashboardSnapshot: ...


class PaneWriter(Protocol):
    """The surface port one refresh tick writes to."""

    def draw(self, lines: Sequence[str]) -> None: ...

    def restore(self) -> None: ...


def _utc_now() -> datetime:
    return datetime.now(UTC)


class DashboardRefreshAction:
    """One tick: read sources, render, write the pane; never crash."""

    def __init__(
        self,
        *,
        database: Database | None = None,
        registry: ProfileRegistry | None = None,
        codex_rate_limits: CodexRateLimitReader | None = None,
        sources: SnapshotSources | None = None,
        pane: PaneWriter | None = None,
        now: Callable[[], datetime] | None = None,
        width: int | Callable[[], int] = 80,
        height: int | Callable[[], int] | None = None,
        frame: bool = False,
        color: bool = False,
        detail: bool = False,
    ) -> None:
        if sources is None:
            if database is None or registry is None:
                raise ValueError(
                    "dashboard refresh requires sources or database+registry"
                )
            sources = DashboardSources(
                database=database,
                registry=registry,
                codex_rate_limits=codex_rate_limits,
            )
        self._sources = sources
        self._pane = pane if pane is not None else DashboardPane()
        self._now = now or _utc_now
        self._width = width
        self._height = height
        self._frame = frame
        self._color = color
        self._detail = detail
        self._failure: TickFailure | None = None

    async def tick(self) -> None:
        """Refresh the dashboard once; record any failure as a fact.

        A failing tick draws nothing and raises nothing; the recorded
        failure appears in the block rendered by the next successful
        tick. The pane re-establishes itself on demand, so repeated
        ticks after any interruption recover the surface.
        """

        try:
            now = self._now()
            snapshot = await self._sources.collect(now)
            width = _resolve(self._width)
            if self._frame:
                height = _resolve(self._height) if self._height is not None else 24
                lines = render_frame(
                    snapshot,
                    width=width,
                    height=height,
                    now=now,
                    failure=self._failure,
                    color=self._color,
                    detail=self._detail,
                )
            else:
                lines = render_dashboard(
                    snapshot,
                    width=width,
                    failure=self._failure,
                )
            self._pane.draw(lines)
        except Exception as error:
            self._failure = TickFailure(
                at=self._now().isoformat(),
                reason=type(error).__name__,
            )
            return
        self._failure = None


def _resolve(value: int | Callable[[], int]) -> int:
    return value() if callable(value) else value
