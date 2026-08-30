"""Deterministic dashboard providers over durable state and static config.

Every fact on the supervisor dashboard comes from durable SQLite rows or
the static profile registry; the one external read (codex rate limits)
is an injected callable whose unavailability becomes an explicit
recorded fact instead of an exception. Nothing here calls a model.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime

from hermes_orchestrator.codex_rpc import CodexRateLimits
from hermes_orchestrator.db import Database
from hermes_orchestrator.profiles import ProfileRegistry

# A stream.result record's usage is the cumulative run total (all
# invocations plus children — see the INFRA-188 note in cells.py), so
# adding it to the per-invocation records it summarizes would double
# count the whole run.
_CUMULATIVE_EVENT_TYPES = frozenset({"stream.result"})

CodexRateLimitReader = Callable[[], Awaitable[CodexRateLimits]]


@dataclass(frozen=True, slots=True)
class ProfileUsage:
    """Token totals for one profile: lead (fable) versus everything."""

    profile_alias: str
    fable_tokens: int
    overall_tokens: int


@dataclass(frozen=True, slots=True)
class ProfileLeaseFact:
    """One durable profile_leases row, verbatim."""

    profile_alias: str
    project_key: str
    state: str
    acquired_at: str
    cooldown_until: str | None


@dataclass(frozen=True, slots=True)
class CodexFact:
    """The codex rate-limit surface, or its recorded unavailability."""

    available: bool
    primary_used_percent: int | None = None
    secondary_used_percent: int | None = None
    reached: bool = False
    unavailable_since: str | None = None


@dataclass(frozen=True, slots=True)
class DashboardSnapshot:
    """One frozen tick's worth of dashboard facts."""

    generated_at: str
    usage: tuple[ProfileUsage, ...]
    leases: tuple[ProfileLeaseFact, ...]
    codex: CodexFact


class UsageAggregator:
    """Accumulate per-profile usage from events rows seen exactly once.

    The last-seen sequence watermark lives in memory only: a restart
    re-reads the full journal, which is deterministic, while within one
    daemon lifetime each tick reads only rows appended since the last.
    No new tables or migrations are involved.
    """

    def __init__(self, database: Database) -> None:
        self._database = database
        self._watermark = 0
        self._fable: dict[str, int] = {}
        self._overall: dict[str, int] = {}

    @property
    def watermark(self) -> int:
        """Return the highest events sequence already aggregated."""

        return self._watermark

    def advance(self) -> None:
        """Fold every not-yet-seen usage-bearing event into the totals."""

        rows = self._database.execute(
            "SELECT sequence, event_type, payload_json FROM events "
            "WHERE sequence > ? AND (event_type LIKE 'stream.%' "
            "OR event_type LIKE 'subagent.%') ORDER BY sequence",
            (self._watermark,),
        ).fetchall()
        for row in rows:
            self._watermark = max(self._watermark, int(row["sequence"]))
            if str(row["event_type"]) in _CUMULATIVE_EVENT_TYPES:
                continue
            try:
                payload = json.loads(str(row["payload_json"]))
            except ValueError:
                continue
            if not isinstance(payload, dict):
                continue
            alias = payload.get("profile_alias")
            usage = payload.get("usage")
            if not isinstance(alias, str) or not isinstance(usage, dict):
                continue
            total = sum(
                count
                for count in usage.values()
                if isinstance(count, int) and not isinstance(count, bool)
            )
            self._overall[alias] = self._overall.get(alias, 0) + total
            # The lead turn (no parent tool use) always runs the fable
            # tier; children carry a parent_tool_use_id. This mirrors
            # the fable_share convention in service.py.
            if payload.get("parent_tool_use_id") is None:
                self._fable[alias] = self._fable.get(alias, 0) + total

    def usage_for(self, aliases: Sequence[str]) -> tuple[ProfileUsage, ...]:
        """Return one usage fact per alias, absent profiles at zero."""

        return tuple(
            ProfileUsage(
                profile_alias=alias,
                fable_tokens=self._fable.get(alias, 0),
                overall_tokens=self._overall.get(alias, 0),
            )
            for alias in aliases
        )


class CodexStatusProvider:
    """Read codex rate limits; unavailability is a fact, never a raise."""

    def __init__(self, read: CodexRateLimitReader | None) -> None:
        self._read = read
        self._unavailable_since: str | None = None

    async def read(self, now_iso: str) -> CodexFact:
        """Return the current snapshot or a sticky unavailability fact."""

        if self._read is None:
            return self._record_unavailable(now_iso)
        try:
            limits = await self._read()
        except Exception:
            return self._record_unavailable(now_iso)
        self._unavailable_since = None
        return CodexFact(
            available=True,
            primary_used_percent=limits.primary_used_percent,
            secondary_used_percent=limits.secondary_used_percent,
            reached=limits.reached,
        )

    def _record_unavailable(self, now_iso: str) -> CodexFact:
        if self._unavailable_since is None:
            self._unavailable_since = now_iso
        return CodexFact(
            available=False,
            unavailable_since=self._unavailable_since,
        )


class DashboardSources:
    """Compose every provider into one frozen snapshot per tick."""

    def __init__(
        self,
        *,
        database: Database,
        registry: ProfileRegistry,
        codex_rate_limits: CodexRateLimitReader | None = None,
    ) -> None:
        self._database = database
        self._registry = registry
        self._usage = UsageAggregator(database)
        self._codex = CodexStatusProvider(codex_rate_limits)

    async def collect(self, now: datetime) -> DashboardSnapshot:
        """Read durable state and return this tick's frozen facts."""

        generated_at = now.isoformat()
        self._usage.advance()
        aliases = tuple(profile.alias for profile in self._registry.profiles)
        rows = self._database.execute(
            "SELECT profile_alias, project_key, state, acquired_at, "
            "cooldown_until FROM profile_leases ORDER BY profile_alias",
        ).fetchall()
        leases = tuple(
            ProfileLeaseFact(
                profile_alias=str(row["profile_alias"]),
                project_key=str(row["project_key"]),
                state=str(row["state"]),
                acquired_at=str(row["acquired_at"]),
                cooldown_until=(
                    str(row["cooldown_until"])
                    if row["cooldown_until"] is not None
                    else None
                ),
            )
            for row in rows
        )
        codex = await self._codex.read(generated_at)
        return DashboardSnapshot(
            generated_at=generated_at,
            usage=self._usage.usage_for(aliases),
            leases=leases,
            codex=codex,
        )
