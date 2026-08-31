"""Pure fixed-width rendering from frozen dashboard snapshots."""

from __future__ import annotations

import re
from datetime import UTC, datetime

from hermes_orchestrator.dashboard_render import (
    TickFailure,
    render_dashboard,
    render_frame,
    visible_length,
)
from hermes_orchestrator.dashboard_sources import (
    CapacityFact,
    CodexFact,
    ControlAttentionFact,
    DashboardSnapshot,
    ProfileLeaseFact,
    ProfileUsage,
    ResourceFact,
    TaskFact,
    TransitionFact,
    UsageWindows,
    WorkerFact,
)

_ALIASES = ("max-a", "max-b", "max-c", "max-d")


def _snapshot(
    *,
    codex: CodexFact | None = None,
    leases: tuple[ProfileLeaseFact, ...] = (),
) -> DashboardSnapshot:
    return DashboardSnapshot(
        generated_at="2026-08-30T12:00:00+00:00",
        usage=tuple(
            ProfileUsage(alias, fable_tokens=index * 100, overall_tokens=index * 250)
            for index, alias in enumerate(_ALIASES)
        ),
        leases=leases,
        codex=codex
        or CodexFact(
            available=False,
            unavailable_since="2026-08-30T11:00:00+00:00",
        ),
    )


def test_every_line_has_the_exact_fixed_width() -> None:
    lines = render_dashboard(_snapshot(), width=60)
    assert lines
    assert all(len(line) == 60 for line in lines)


def test_line_count_is_stable_across_content_shapes() -> None:
    bare = render_dashboard(_snapshot(), width=80)
    lease = ProfileLeaseFact(
        profile_alias="max-a",
        project_key="demo",
        state="active",
        acquired_at="2026-08-30T08:00:00+00:00",
        cooldown_until=None,
    )
    full = render_dashboard(
        _snapshot(leases=(lease,)),
        width=80,
        failure=TickFailure(
            at="2026-08-30T11:59:30+00:00", reason="TimeoutError"
        ),
    )
    assert len(bare) == len(full)


def test_rendering_is_deterministic_for_equal_input() -> None:
    assert render_dashboard(_snapshot(), width=72) == render_dashboard(
        _snapshot(), width=72
    )


def test_unavailable_codex_renders_the_recorded_fact() -> None:
    lines = render_dashboard(_snapshot(), width=80)
    codex_line = next(line for line in lines if "codex" in line)
    assert "unavailable since 2026-08-30T11:00:00+00:00" in codex_line


def test_available_codex_renders_percentages() -> None:
    codex = CodexFact(
        available=True,
        primary_used_percent=41,
        secondary_used_percent=12,
        reached=True,
    )
    lines = render_dashboard(_snapshot(codex=codex), width=80)
    codex_line = next(line for line in lines if "codex" in line)
    assert "41%" in codex_line
    assert "12%" in codex_line
    assert "reached" in codex_line


def test_usage_split_and_lease_absence_are_explicit() -> None:
    lease = ProfileLeaseFact(
        profile_alias="max-b",
        project_key="demo",
        state="active",
        acquired_at="2026-08-30T08:00:00+00:00",
        cooldown_until=None,
    )
    lines = render_dashboard(_snapshot(leases=(lease,)), width=100)
    profile_b = next(line for line in lines if line.startswith("max-b"))
    tokens = profile_b.split()
    assert tokens[tokens.index("fable") + 1] == "100"
    assert tokens[tokens.index("overall") + 1] == "250"
    assert "demo/active" in profile_b
    profile_a = next(line for line in lines if line.startswith("max-a"))
    assert "no lease" in profile_a


def test_tick_failure_fact_appears_and_ok_otherwise() -> None:
    failed = render_dashboard(
        _snapshot(),
        width=90,
        failure=TickFailure(
            at="2026-08-30T11:59:30+00:00", reason="TimeoutError"
        ),
    )
    status = next(line for line in failed if "tick" in line)
    assert "failed at 2026-08-30T11:59:30+00:00" in status
    assert "TimeoutError" in status

    healthy = render_dashboard(_snapshot(), width=90)
    assert any("last tick ok" in line for line in healthy)


def test_overlong_content_is_truncated_to_width() -> None:
    codex = CodexFact(
        available=False,
        unavailable_since="2026-08-30T11:00:00+00:00",
    )
    lines = render_dashboard(_snapshot(codex=codex), width=24)
    assert all(len(line) == 24 for line in lines)


# ---------------------------------------------------------------------------
# INFRA-209 (requirements reread): render_frame — header, Work, Capacity,
# (Detail), System, Attention.
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 8, 31, 12, 0, 0, tzinfo=UTC)
_EMPTY_WINDOWS = UsageWindows(
    fetched_at=None,
    five_hour_used=None,
    five_hour_resets_at=None,
    seven_day_used=None,
    seven_day_resets_at=None,
    fable_used=None,
    fable_resets_at=None,
    fable_severity=None,
    fable_active=None,
)


def _frame_usage() -> tuple[ProfileUsage, ...]:
    return tuple(
        ProfileUsage(alias, fable_tokens=index * 100_000, overall_tokens=index * 2)
        for index, alias in enumerate(_ALIASES, start=1)
    )


def _task(
    issue_id: str = "INFRA-1",
    project_key: str = "proj",
    priority: int = 1,
    operator_state: str = "Working",
    lead_profile: str | None = "max-a",
    children_completed: int = 1,
    children_total: int = 2,
    pr_number: int | None = 12,
    review_state: str | None = "approved",
    settlement_state: str | None = None,
    updated_at: str = "2026-08-31T11:55:00+00:00",
) -> TaskFact:
    return TaskFact(
        issue_id=issue_id,
        project_key=project_key,
        priority=priority,
        operator_state=operator_state,
        lead_profile=lead_profile,
        children_completed=children_completed,
        children_total=children_total,
        pr_number=pr_number,
        review_state=review_state,
        settlement_state=settlement_state,
        updated_at=updated_at,
    )


def _capacity_fact(
    alias: str,
    *,
    state: str | None = "available",
    source: str | None = "operator_attestation",
    observed_at: str | None = "2026-08-31T11:00:00+00:00",
    resets_at: str | None = None,
    windows: UsageWindows = _EMPTY_WINDOWS,
) -> CapacityFact:
    return CapacityFact(
        profile_alias=alias,
        state=state,
        source=source,
        observed_at=observed_at,
        resets_at=resets_at,
        windows=windows,
    )


def _resource_fact(
    *,
    pressure: str = "green",
    sampled_at: str = "2026-08-31T11:59:30+00:00",
    min_disk_free_bytes: int | None = 137 * 1024**3,
) -> ResourceFact:
    return ResourceFact(
        sampled_at=sampled_at,
        pressure=pressure,
        available_memory_bytes=int(8.3 * 1024**3),
        total_memory_bytes=int(25.8 * 1024**3),
        swap_used_bytes=int(1.2 * 1024**3),
        load_one=2.6,
        logical_cpus=14,
        managed_rss_bytes=67 * 1024**2,
        min_disk_free_bytes=min_disk_free_bytes,
    )


def _frame_snapshot(
    *,
    tasks: tuple[TaskFact, ...] = (),
    capacity: tuple[CapacityFact, ...] | None = None,
    resource: ResourceFact | None = None,
    leases: tuple[ProfileLeaseFact, ...] = (),
    codex: CodexFact | None = None,
    workers: WorkerFact | None = None,
    transitions: tuple[TransitionFact, ...] = (),
    attention_control: ControlAttentionFact | None = None,
    tasks_observed_at: str | None = "2026-08-31T11:55:00+00:00",
) -> DashboardSnapshot:
    if capacity is None:
        capacity = tuple(_capacity_fact(alias) for alias in _ALIASES)
    if resource is None:
        resource = _resource_fact()
    if codex is None:
        codex = CodexFact(
            available=True, primary_used_percent=20, secondary_used_percent=10
        )
    if workers is None:
        workers = WorkerFact(active_total=0, active_by_kind=())
    return DashboardSnapshot(
        generated_at=_NOW.isoformat(),
        usage=_frame_usage(),
        leases=leases,
        codex=codex,
        tasks=tasks,
        capacity=capacity,
        resource=resource,
        tasks_observed_at=tasks_observed_at,
        workers=workers,
        transitions=transitions,
        attention_control=attention_control,
    )


def test_frame_is_exactly_height_lines_and_width_visible_length() -> None:
    snapshot = _frame_snapshot(tasks=(_task(),))
    for width, height in ((40, 12), (60, 20), (24, 6), (100, 30)):
        lines = render_frame(snapshot, width=width, height=height, now=_NOW)
        assert len(lines) == height
        assert all(visible_length(line) == width for line in lines)


def test_frame_rendering_is_deterministic() -> None:
    snapshot = _frame_snapshot(tasks=(_task(),))
    first = render_frame(snapshot, width=48, height=16, now=_NOW)
    second = render_frame(snapshot, width=48, height=16, now=_NOW)
    assert first == second


def test_frame_shows_header_and_all_sections() -> None:
    snapshot = _frame_snapshot(tasks=(_task(),))
    lines = render_frame(snapshot, width=60, height=24, now=_NOW)
    joined = "\n".join(lines)
    assert "HERMES 12:00:00Z" in joined
    assert "Work" in joined
    assert "Capacity" in joined
    assert "System" in joined
    assert "Attention" in joined
    assert any("last tick ok" in line for line in lines)


def test_work_rows_are_trimmed_with_a_more_row_when_short_on_height() -> None:
    tasks = tuple(
        _task(
            issue_id=f"INFRA-{i}",
            operator_state="Queued",
            pr_number=None,
            review_state=None,
        )
        for i in range(20)
    )
    snapshot = _frame_snapshot(tasks=tasks)
    # A generous width but a height too small to fit all 20 rows.
    lines = render_frame(snapshot, width=60, height=14, now=_NOW)
    assert any("more" in line for line in lines)


def test_no_in_flight_work_is_shown_explicitly() -> None:
    snapshot = _frame_snapshot(tasks=())
    lines = render_frame(snapshot, width=60, height=20, now=_NOW)
    assert any("no in-flight work" in line for line in lines)


def test_capacity_row_shows_unknown_for_no_observation_and_no_cache() -> None:
    capacity = tuple(
        _capacity_fact(alias, state=None, source=None, observed_at=None)
        for alias in _ALIASES
    )
    snapshot = _frame_snapshot(capacity=capacity)
    lines = render_frame(snapshot, width=60, height=20, now=_NOW)
    max_a = next(line for line in lines if line.strip().startswith("max-a"))
    assert "unknown" in max_a


def test_capacity_row_is_stale_after_six_hours_with_no_fresher_cache() -> None:
    capacity = (
        _capacity_fact(
            "max-a",
            state="available",
            observed_at="2026-08-31T04:00:00+00:00",  # 8h before _NOW
        ),
        *(_capacity_fact(alias) for alias in _ALIASES[1:]),
    )
    snapshot = _frame_snapshot(capacity=capacity)
    lines = render_frame(snapshot, width=60, height=20, now=_NOW)
    max_a = next(line for line in lines if line.strip().startswith("max-a"))
    assert "stale" in max_a


def test_capacity_row_capped_from_cache_used_at_100_percent() -> None:
    windows = UsageWindows(
        fetched_at="2026-08-31T11:55:00+00:00",
        five_hour_used=10,
        five_hour_resets_at=None,
        seven_day_used=100,
        seven_day_resets_at="2026-09-01T00:00:00+00:00",
        fable_used=5,
        fable_resets_at="2026-09-01T00:00:00+00:00",
        fable_severity="normal",
        fable_active=True,
    )
    capacity = (
        _capacity_fact("max-a", windows=windows),
        *(_capacity_fact(alias) for alias in _ALIASES[1:]),
    )
    snapshot = _frame_snapshot(capacity=capacity)
    lines = render_frame(snapshot, width=60, height=20, now=_NOW)
    max_a = next(line for line in lines if line.strip().startswith("max-a"))
    assert "capped" in max_a


def test_codex_row_shows_remaining_percent_or_unavailable() -> None:
    codex = CodexFact(
        available=True, primary_used_percent=12, secondary_used_percent=40
    )
    snapshot = _frame_snapshot(codex=codex)
    lines = render_frame(snapshot, width=60, height=20, now=_NOW)
    codex_line = next(line for line in lines if line.strip().startswith("codex"))
    assert "88%" in codex_line
    assert "60%" in codex_line

    unavailable = CodexFact(
        available=False, unavailable_since="2026-08-31T10:00:00+00:00"
    )
    snapshot2 = _frame_snapshot(codex=unavailable)
    lines2 = render_frame(snapshot2, width=60, height=20, now=_NOW)
    codex_line2 = next(line for line in lines2 if line.strip().startswith("codex"))
    assert "unavailable" in codex_line2


def test_system_shows_pressure_meter_mem_disk_swap_load_and_workers() -> None:
    workers = WorkerFact(active_total=4, active_by_kind=(("claude_subagent", 4),))
    snapshot = _frame_snapshot(workers=workers)
    lines = render_frame(snapshot, width=90, height=20, now=_NOW)
    system_line = next(line for line in lines if "mem" in line and "workers" in line)
    assert "[" in system_line and "]" in system_line
    assert "disk" in system_line
    assert "swap" in system_line
    assert "load" in system_line
    assert "workers 4" in system_line


def test_attention_prioritizes_red_pressure_over_everything() -> None:
    resource = _resource_fact(pressure="red")
    snapshot = _frame_snapshot(
        resource=resource,
        tasks=(_task(operator_state="Blocked"),),
    )
    lines = render_frame(snapshot, width=60, height=20, now=_NOW)
    attention_line = next(
        line
        for index, line in enumerate(lines)
        if "Attention" in lines[index - 1]
    )
    assert "pressure" in attention_line


def test_attention_falls_back_to_the_most_recent_transition() -> None:
    transitions = (
        TransitionFact("proj", "2026-08-31T11:00:00+00:00", "rotated max-b→max-c"),
    )
    snapshot = _frame_snapshot(transitions=transitions)
    lines = render_frame(snapshot, width=60, height=20, now=_NOW)
    joined = "\n".join(lines)
    assert "rotated max-b→max-c" in joined


def test_attention_reports_nothing_needs_attention_when_all_clear() -> None:
    snapshot = _frame_snapshot()
    lines = render_frame(snapshot, width=60, height=20, now=_NOW)
    joined = "\n".join(lines)
    assert "nothing needs attention" in joined


def test_last_tick_failure_and_ok_are_shown() -> None:
    snapshot = _frame_snapshot()
    failed = render_frame(
        snapshot,
        width=60,
        height=20,
        now=_NOW,
        failure=TickFailure(at="2026-08-31T11:59:30+00:00", reason="TimeoutError"),
    )
    assert any("failed at 2026-08-31T11:59:30+00:00" in line for line in failed)
    assert any("TimeoutError" in line for line in failed)

    healthy = render_frame(snapshot, width=60, height=20, now=_NOW)
    assert any("last tick ok" in line for line in healthy)


def test_color_and_no_color_have_equal_visible_widths_and_only_color_adds_sgr() -> None:
    snapshot = _frame_snapshot(
        tasks=(_task(),),
        resource=_resource_fact(pressure="red"),
    )
    plain = render_frame(snapshot, width=48, height=18, now=_NOW, color=False)
    colored = render_frame(snapshot, width=48, height=18, now=_NOW, color=True)

    assert len(plain) == len(colored)
    for plain_line, colored_line in zip(plain, colored, strict=True):
        assert visible_length(colored_line) == 48
        assert "\x1b[" not in plain_line
    assert any("\x1b[" in line for line in colored)
    # No SGR state ever bleeds past the end of a line.
    for line in colored:
        assert line.endswith("\x1b[0m") or "\x1b[" not in line


def test_detail_false_hides_raw_token_totals_by_default() -> None:
    snapshot = _frame_snapshot(tasks=(_task(),))
    lines = render_frame(snapshot, width=60, height=30, now=_NOW, detail=False)
    joined = "\n".join(lines)
    assert "Detail" not in joined
    assert re.search(r"overall \d", joined) is None


def test_detail_true_shows_the_detail_section_with_token_totals() -> None:
    snapshot = _frame_snapshot(tasks=(_task(),))
    lines = render_frame(snapshot, width=60, height=30, now=_NOW, detail=True)
    joined = "\n".join(lines)
    assert "Detail" in joined
    assert re.search(r"overall \d", joined) is not None


def test_narrow_and_wide_frames_fit_exactly() -> None:
    snapshot = _frame_snapshot(tasks=(_task(),))
    narrow = render_frame(snapshot, width=40, height=14, now=_NOW)
    wide = render_frame(snapshot, width=100, height=30, now=_NOW)
    assert len(narrow) == 14 and all(visible_length(line) == 40 for line in narrow)
    assert len(wide) == 30 and all(visible_length(line) == 100 for line in wide)


def test_below_minimum_size_still_returns_exactly_height_lines() -> None:
    snapshot = _frame_snapshot(tasks=(_task(),))
    lines = render_frame(snapshot, width=10, height=3, now=_NOW)
    assert len(lines) == 3
    assert all(visible_length(line) == 10 for line in lines)


# ---------------------------------------------------------------------------
# INFRA-209 narrow-width follow-up: five defects fixed against live-state
# feedback at 46x24/76x30.
# ---------------------------------------------------------------------------


def test_no_mid_token_truncation_at_narrow_width() -> None:
    windows = UsageWindows(
        fetched_at="2026-08-31T11:00:00+00:00",
        five_hour_used=41,
        five_hour_resets_at="2026-08-31T14:00:00+00:00",
        seven_day_used=63,
        seven_day_resets_at="2026-09-01T00:00:00+00:00",
        fable_used=12,
        fable_resets_at="2026-09-01T00:00:00+00:00",
        fable_severity="normal",
        fable_active=True,
    )
    capacity = (
        _capacity_fact("max-a", windows=windows),
        *(_capacity_fact(alias) for alias in _ALIASES[1:]),
    )
    snapshot = _frame_snapshot(
        tasks=(_task(),),
        capacity=capacity,
        leases=(
            ProfileLeaseFact(
                "max-a", "hermes-orchestrator", "active",
                "2026-08-31T08:00:00+00:00", None,
            ),
        ),
    )
    lines = render_frame(snapshot, width=46, height=24, now=_NOW)
    assert len(lines) == 24
    assert all(visible_length(line) == 46 for line in lines)
    for line in lines:
        stripped = line.rstrip()
        # A truncated line ends in the ellipsis and nothing follows it
        # (word-boundary backoff, never a mid-word cut).
        if "…" in stripped:
            assert stripped.endswith("…")
            assert stripped.count("…") == 1


def test_capacity_two_line_layout_at_46_cols_matches_the_narrow_format() -> None:
    snapshot = _frame_snapshot(
        capacity=tuple(_capacity_fact(alias) for alias in _ALIASES),
    )
    lines = render_frame(snapshot, width=46, height=24, now=_NOW)
    max_a_index = next(
        i for i, line in enumerate(lines) if line.strip().startswith("max-a")
    )
    line1 = lines[max_a_index]
    line2 = lines[max_a_index + 1]
    assert "available" in line1
    assert line2.startswith("       ")  # indented under "max-a  "
    assert "all" in line2 and "fable" in line2


def test_expired_window_is_excluded_from_capped_and_renders_reset() -> None:
    # A 5h window at 0% remaining whose reset horizon has already
    # passed is stale evidence, not a live cap — it must not flip
    # availability to capped/near-cap, and its own token reads "reset".
    windows = UsageWindows(
        fetched_at="2026-08-31T11:00:00+00:00",
        five_hour_used=100,
        five_hour_resets_at="2026-08-31T10:00:00+00:00",  # in the past
        seven_day_used=20,
        seven_day_resets_at="2026-09-01T00:00:00+00:00",
        fable_used=10,
        fable_resets_at="2026-09-01T00:00:00+00:00",
        fable_severity="normal",
        fable_active=True,
    )
    capacity = (
        _capacity_fact("max-a", windows=windows),
        *(_capacity_fact(alias) for alias in _ALIASES[1:]),
    )
    snapshot = _frame_snapshot(capacity=capacity)
    lines = render_frame(snapshot, width=100, height=20, now=_NOW)
    max_a = next(line for line in lines if line.strip().startswith("max-a"))
    assert "available" in max_a
    assert "capped" not in max_a
    assert "near cap" not in max_a
    assert "5h reset" in max_a


def test_numbers_staleness_is_independent_of_a_fresh_availability_word() -> None:
    # The numbers (windows) line carries its own cache-age marker; an
    # available word from a fresh operator observation must never hide
    # 11h-old cached percentages without the numbers' own stale flag.
    windows = UsageWindows(
        fetched_at="2026-08-31T01:00:00+00:00",  # 11h before _NOW
        five_hour_used=10,
        five_hour_resets_at=None,
        seven_day_used=20,
        seven_day_resets_at=None,
        fable_used=5,
        fable_resets_at=None,
        fable_severity="normal",
        fable_active=True,
    )
    capacity = (
        _capacity_fact(
            "max-a",
            state="available",
            source="operator_attestation",
            observed_at="2026-08-31T11:55:00+00:00",  # fresh operator observation
            windows=windows,
        ),
        *(_capacity_fact(alias) for alias in _ALIASES[1:]),
    )
    snapshot = _frame_snapshot(capacity=capacity)
    lines = render_frame(snapshot, width=100, height=20, now=_NOW)
    max_a = next(line for line in lines if line.strip().startswith("max-a"))
    assert "available" in max_a
    assert "(cache 11h stale)" in max_a


def test_kids_shown_only_on_the_working_row() -> None:
    working = _task(
        issue_id="INFRA-1",
        operator_state="Working",
        children_completed=1,
        children_total=2,
    )
    queued = _task(
        issue_id="INFRA-2",
        operator_state="Queued",
        children_completed=1,
        children_total=2,
        pr_number=None,
        review_state=None,
    )
    snapshot = _frame_snapshot(tasks=(working, queued))
    lines = render_frame(snapshot, width=70, height=20, now=_NOW)
    working_line = next(line for line in lines if "INFRA-1" in line)
    queued_line = next(line for line in lines if "INFRA-2" in line)
    assert "kids" in working_line
    assert "kids" not in queued_line


def test_paused_state_renders_as_paused_and_is_never_attention() -> None:
    paused = _task(
        issue_id="INFRA-3", operator_state="Paused", pr_number=None, review_state=None
    )
    snapshot = _frame_snapshot(tasks=(paused,))
    lines = render_frame(snapshot, width=70, height=20, now=_NOW)
    joined = "\n".join(lines)
    assert "Paused" in joined
    assert "Blocked" not in joined
    attention_line = next(
        line for index, line in enumerate(lines) if "Attention" in lines[index - 1]
    )
    assert "INFRA-3" not in attention_line
    assert "nothing needs attention" in attention_line


def test_height_budgeting_with_two_line_capacity_rows_keeps_exact_height() -> None:
    snapshot = _frame_snapshot(tasks=(_task(),))
    for height in (6, 14, 24, 40):
        lines = render_frame(snapshot, width=46, height=height, now=_NOW)
        assert len(lines) == height
        assert all(visible_length(line) == 46 for line in lines)
