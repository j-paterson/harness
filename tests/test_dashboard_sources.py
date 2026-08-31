"""Deterministic dashboard providers over durable state only."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.codex_rpc import CodexRateLimits
from hermes_orchestrator.dashboard_sources import (
    CodexStatusProvider,
    DashboardSources,
    ProfileUsage,
    UsageAggregator,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.profiles import ProfileConfig, ProfileRegistry

_ALIASES = ("max-a", "max-b", "max-c", "max-d")


def _registry(tmp_path: Path) -> ProfileRegistry:
    return ProfileRegistry(
        tuple(
            ProfileConfig(alias, tmp_path / alias) for alias in _ALIASES
        )
    )


def _database(tmp_path: Path) -> Database:
    return Database.open(tmp_path / "state.db")


def _append_usage(
    database: Database,
    *,
    event_type: str = "stream.assistant",
    profile_alias: str = "max-a",
    parent_tool_use_id: str | None = None,
    usage: dict[str, int],
) -> None:
    events = EventStore(database)
    with database.transaction() as connection:
        events.append(
            connection,
            EventInput(
                event_type=event_type,
                aggregate_type="project_cell",
                aggregate_id="cell-1",
                payload={
                    "profile_alias": profile_alias,
                    "parent_tool_use_id": parent_tool_use_id,
                    "usage": usage,
                },
            ),
        )


def test_watermark_never_double_counts_across_ticks(tmp_path: Path) -> None:
    database = _database(tmp_path)
    aggregator = UsageAggregator(database)

    _append_usage(database, usage={"input_tokens": 100})
    aggregator.advance()
    first = aggregator.usage_for(_ALIASES)
    assert first[0] == ProfileUsage("max-a", fable_tokens=100, overall_tokens=100)

    # A tick with no new rows must not change any total.
    aggregator.advance()
    assert aggregator.usage_for(_ALIASES) == first

    _append_usage(database, usage={"input_tokens": 25})
    aggregator.advance()
    assert aggregator.usage_for(_ALIASES)[0] == ProfileUsage(
        "max-a", fable_tokens=125, overall_tokens=125
    )
    database.close()


def test_fable_versus_overall_split_by_profile_alias(tmp_path: Path) -> None:
    database = _database(tmp_path)
    aggregator = UsageAggregator(database)

    _append_usage(database, profile_alias="max-a", usage={"input_tokens": 100})
    _append_usage(
        database,
        profile_alias="max-a",
        parent_tool_use_id="tool-use-1",
        usage={"input_tokens": 40},
    )
    _append_usage(
        database,
        profile_alias="max-b",
        parent_tool_use_id="tool-use-2",
        usage={"output_tokens": 7},
    )
    aggregator.advance()

    usage = aggregator.usage_for(_ALIASES)
    assert usage[0] == ProfileUsage("max-a", fable_tokens=100, overall_tokens=140)
    assert usage[1] == ProfileUsage("max-b", fable_tokens=0, overall_tokens=7)
    assert usage[2] == ProfileUsage("max-c", fable_tokens=0, overall_tokens=0)
    database.close()


def test_cumulative_result_records_are_excluded(tmp_path: Path) -> None:
    # A stream.result usage is the cumulative run total (see cells.py's
    # INFRA-188 note); adding it to per-invocation records double-counts.
    database = _database(tmp_path)
    aggregator = UsageAggregator(database)

    _append_usage(database, usage={"input_tokens": 100})
    _append_usage(database, event_type="stream.result", usage={"input_tokens": 999})
    _append_usage(
        database,
        event_type="project_cell.issue_already_completed",
        usage={"input_tokens": 555},
    )
    aggregator.advance()

    assert aggregator.usage_for(_ALIASES)[0] == ProfileUsage(
        "max-a", fable_tokens=100, overall_tokens=100
    )
    database.close()


@pytest.mark.asyncio
async def test_codex_unavailability_is_a_sticky_recorded_fact() -> None:
    calls = {"count": 0}

    async def failing() -> CodexRateLimits:
        calls["count"] += 1
        raise TimeoutError("codex did not answer")

    provider = CodexStatusProvider(failing)
    first = await provider.read("2026-08-30T10:00:00+00:00")
    assert first.available is False
    assert first.unavailable_since == "2026-08-30T10:00:00+00:00"

    # The recorded fact keeps the first failure time across later failures.
    second = await provider.read("2026-08-30T10:05:00+00:00")
    assert second.unavailable_since == "2026-08-30T10:00:00+00:00"
    assert calls["count"] == 2


@pytest.mark.asyncio
async def test_codex_recovery_clears_and_refailure_restamps() -> None:
    answers: list[object] = [
        TimeoutError("down"),
        CodexRateLimits(
            primary_used_percent=41,
            secondary_used_percent=12,
            primary_resets_at=None,
            reached=False,
        ),
        RuntimeError("down again"),
    ]

    async def scripted() -> CodexRateLimits:
        answer = answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    provider = CodexStatusProvider(scripted)
    down = await provider.read("2026-08-30T10:00:00+00:00")
    assert down.unavailable_since == "2026-08-30T10:00:00+00:00"

    up = await provider.read("2026-08-30T10:01:00+00:00")
    assert up.available is True
    assert up.primary_used_percent == 41
    assert up.secondary_used_percent == 12
    assert up.unavailable_since is None

    refailed = await provider.read("2026-08-30T10:02:00+00:00")
    assert refailed.unavailable_since == "2026-08-30T10:02:00+00:00"


@pytest.mark.asyncio
async def test_missing_codex_reader_is_an_explicit_fact() -> None:
    provider = CodexStatusProvider(None)
    fact = await provider.read("2026-08-30T09:00:00+00:00")
    assert fact.available is False
    assert fact.unavailable_since == "2026-08-30T09:00:00+00:00"


@pytest.mark.asyncio
async def test_collect_reports_all_profiles_leases_and_codex(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO profile_leases("
            "profile_alias, project_key, state, acquired_at, cooldown_until"
            ") VALUES (?, ?, ?, ?, ?)",
            ("max-b", "demo", "active", "2026-08-30T08:00:00+00:00", None),
        )
    _append_usage(database, profile_alias="max-b", usage={"input_tokens": 9})

    sources = DashboardSources(
        database=database,
        registry=_registry(tmp_path),
        codex_rate_limits=None,
    )
    now = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    snapshot = await sources.collect(now)

    assert snapshot.generated_at == now.isoformat()
    assert tuple(usage.profile_alias for usage in snapshot.usage) == _ALIASES
    assert snapshot.usage[1].overall_tokens == 9
    assert len(snapshot.leases) == 1
    lease = snapshot.leases[0]
    assert (lease.profile_alias, lease.project_key, lease.state) == (
        "max-b",
        "demo",
        "active",
    )
    assert snapshot.codex.available is False
    assert snapshot.codex.unavailable_since == now.isoformat()
    database.close()
