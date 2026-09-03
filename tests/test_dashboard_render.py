"""Pure fixed-width rendering from frozen dashboard snapshots."""

from __future__ import annotations

import dataclasses
import re
from datetime import UTC, datetime

from hermes_orchestrator.dashboard_render import (
    TickFailure,
    render_dashboard,
    render_frame,
    visible_length,
)
from hermes_orchestrator.dashboard_sources import (
    BatchFact,
    CapacityFact,
    CodexFact,
    ControlAttentionFact,
    DashboardSnapshot,
    DecisionInboxFact,
    IdleFact,
    LaneCellFact,
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
    batches: tuple[BatchFact, ...] = (),
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
        batches=batches,
    )


def _batch_fact(
    *,
    project_key: str = "proj-a",
    objective: str = "chat-INFRA-1, chat-INFRA-2",
    admitted_total: int = 2,
    terminal_total: int = 0,
    active: tuple[tuple[str, str], ...] = (("INFRA-2", "in_development"),),
    runnable: tuple[str, ...] = ("INFRA-1",),
    pending: str = "candidates=1 corrections=0 merges=0 leases=1",
    next_action: str = "replenish:INFRA-1",
    blocker: str | None = None,
    complete: bool = False,
) -> BatchFact:
    return BatchFact(
        project_key=project_key,
        objective=objective,
        admitted_total=admitted_total,
        terminal_total=terminal_total,
        active=active,
        runnable=runnable,
        pending=pending,
        next_action=next_action,
        blocker=blocker,
        complete=complete,
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
    assert "usage unknown since 2026-08-30T11:00:00+00:00" in codex_line


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
# INFRA-215 (reopened): the per-project batch-completion section.
# ---------------------------------------------------------------------------


def test_batch_section_shows_objective_runnable_next_action_and_blocker() -> None:
    fact = _batch_fact(
        project_key="proj-a",
        objective="chat-INFRA-1, chat-INFRA-2",
        admitted_total=2,
        terminal_total=0,
        active=(("INFRA-2", "in_development"),),
        runnable=("INFRA-1",),
        pending="candidates=1 corrections=0 merges=0 leases=1",
        next_action="await_operator_decision:INFRA-2",
        blocker="operator_decision:INFRA-2",
        complete=False,
    )
    lines = render_dashboard(_snapshot(batches=(fact,)), width=100)

    objective_line = next(line for line in lines if line.startswith("batch proj-a"))
    assert objective_line.rstrip() == (
        "batch proj-a objective: chat-INFRA-1, chat-INFRA-2"
    )
    issues_line = next(line for line in lines if line.startswith("issues "))
    assert "0/2 terminal" in issues_line
    assert "active: INFRA-2:in_development" in issues_line
    assert "runnable: INFRA-1" in issues_line
    pending_line = next(line for line in lines if line.startswith("pending "))
    assert (
        pending_line.rstrip()
        == "pending candidates=1 corrections=0 merges=0 leases=1"
    )
    next_line = next(line for line in lines if line.startswith("next "))
    assert next_line.rstrip() == "next await_operator_decision:INFRA-2"
    blocker_line = next(line for line in lines if line.startswith("blocker "))
    assert blocker_line.rstrip() == "blocker operator_decision:INFRA-2"


def test_batch_section_omits_blocker_line_when_absent() -> None:
    fact = _batch_fact(blocker=None)
    lines = render_dashboard(_snapshot(batches=(fact,)), width=100)

    assert not any(line.startswith("blocker") for line in lines)
    issues_line = next(line for line in lines if line.startswith("issues "))
    assert "runnable: INFRA-1" in issues_line


def test_batch_section_renders_complete() -> None:
    fact = _batch_fact(
        project_key="proj-b",
        active=(),
        runnable=(),
        pending="candidates=0 corrections=0 merges=0 leases=0",
        next_action="complete",
        blocker=None,
        complete=True,
    )
    lines = render_dashboard(_snapshot(batches=(fact,)), width=100)

    complete_line = next(line for line in lines if line.startswith("batch proj-b"))
    assert complete_line.rstrip() == "batch proj-b complete"
    assert not any(line.startswith("objective") for line in lines)
    assert not any(line.startswith("issues ") for line in lines)
    assert not any(line.startswith("pending ") for line in lines)
    assert not any(line.startswith("next ") for line in lines)


def test_batch_section_renders_one_block_per_project_in_order() -> None:
    fact_a = _batch_fact(project_key="proj-a")
    fact_b = _batch_fact(
        project_key="proj-b",
        active=(),
        runnable=(),
        next_action="complete",
        complete=True,
    )
    lines = render_dashboard(_snapshot(batches=(fact_a, fact_b)), width=100)

    index_a = next(
        index
        for index, line in enumerate(lines)
        if line.startswith("batch proj-a objective:")
    )
    index_b = next(
        index for index, line in enumerate(lines) if line.startswith("batch proj-b")
    )
    assert index_a < index_b


def test_no_batches_leaves_existing_render_dashboard_output_unchanged() -> None:
    """The wiring/render tests above assert exact strings from `_snapshot()`
    with no `batches` argument -- confirm a default (empty) batches tuple
    renders no batch lines at all, so every pre-existing assertion in this
    module still holds unchanged."""

    lines = render_dashboard(_snapshot(), width=80)
    assert not any(line.strip().startswith("batch") for line in lines)
    assert not any(line.strip().startswith("issues ") for line in lines)
    assert not any(line.strip().startswith("pending ") for line in lines)


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
    idle: tuple[IdleFact, ...] = (),
    lanes: tuple[LaneCellFact, ...] = (),
    decisions: DecisionInboxFact | None = None,
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
        idle=idle,
        lanes=lanes,
        decisions=decisions,
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


def test_idle_project_shows_no_queued_work_line() -> None:
    idle = (IdleFact(project_key="proj-a", hold="no queued work"),)
    snapshot = _frame_snapshot(tasks=(), idle=idle)
    lines = render_frame(snapshot, width=60, height=20, now=_NOW)
    assert any("proj-a: no queued work" in line for line in lines)


def test_idle_project_capacity_hold_shows_pressure_and_sample_age() -> None:
    idle = (IdleFact(project_key="proj-a", hold="capacity limited"),)
    resource = _resource_fact(pressure="yellow", sampled_at=_NOW.isoformat())
    snapshot = _frame_snapshot(tasks=(_task(),), idle=idle, resource=resource)
    lines = render_frame(snapshot, width=90, height=20, now=_NOW)
    row = next(line for line in lines if "proj-a: capacity limited" in line)
    assert "yellow" in row
    assert "sample" in row


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


def test_capped_provider_observation_owns_its_reset_horizon() -> None:
    windows = UsageWindows(
        fetched_at="2026-08-31T11:59:00+00:00",
        five_hour_used=100,
        five_hour_resets_at="2026-08-31T20:00:00+00:00",
        seven_day_used=50,
        seven_day_resets_at="2026-09-01T20:00:00+00:00",
        fable_used=100,
        fable_resets_at="2026-09-01T20:00:00+00:00",
        fable_severity="critical",
        fable_active=True,
    )
    capacity = (
        _capacity_fact(
            "max-a",
            state="capped",
            source="provider_limit",
            observed_at="2026-08-31T11:47:00+00:00",
            resets_at="2026-08-31T12:47:00+00:00",
            windows=windows,
        ),
        *(_capacity_fact(alias) for alias in _ALIASES[1:]),
    )

    lines = render_frame(
        _frame_snapshot(capacity=capacity), width=100, height=20, now=_NOW
    )
    max_a = next(line for line in lines if line.strip().startswith("max-a"))

    assert "resets in 47m" in max_a
    assert "20:00Z" not in max_a


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
    assert "usage unknown" in codex_line2


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


def test_attention_does_not_repeat_a_nonactionable_transition() -> None:
    transitions = (
        TransitionFact("proj", "2026-08-31T11:00:00+00:00", "rotated max-b→max-c"),
    )
    snapshot = _frame_snapshot(transitions=transitions)
    lines = render_frame(snapshot, width=60, height=20, now=_NOW)
    attention_line = next(
        line for index, line in enumerate(lines) if "Attention" in lines[index - 1]
    )
    assert attention_line.strip() == "nothing needs attention"


def test_attention_reports_nothing_needs_attention_when_all_clear() -> None:
    snapshot = _frame_snapshot()
    lines = render_frame(snapshot, width=60, height=20, now=_NOW)
    joined = "\n".join(lines)
    assert "nothing needs attention" in joined


def test_channel_blocked_attention_does_not_claim_there_is_a_dialog() -> None:
    snapshot = _frame_snapshot(
        tasks=(_task(issue_id="INFRA-192", project_key="agent-orchestration"),),
        attention_control=ControlAttentionFact(
            kind="channel.blocked",
            project_key="agent-orchestration",
            created_at="2026-08-31T11:59:00+00:00",
        ),
    )

    lines = render_frame(snapshot, width=80, height=20, now=_NOW)
    joined = "\n".join(lines)

    assert "channel blocked: agent-orchestration" in joined
    assert "confirm channel dialog" not in joined


def _decision_fact(
    *,
    pending: int = 2,
    next_decision_id: str | None = "dec-1",
    next_project_key: str | None = "proj",
    next_issue_id: str | None = "INFRA-224",
    next_urgency: int | None = 1,
    next_question: str | None = "Approve external repo deletion?",
    next_recorded_at: str | None = "2026-08-31T11:00:00+00:00",
) -> DecisionInboxFact:
    return DecisionInboxFact(
        pending=pending,
        next_decision_id=next_decision_id,
        next_project_key=next_project_key,
        next_issue_id=next_issue_id,
        next_urgency=next_urgency,
        next_question=next_question,
        next_recorded_at=next_recorded_at,
    )


def test_pending_decisions_show_count_and_next_item_in_attention() -> None:
    snapshot = _frame_snapshot(decisions=_decision_fact())

    lines = render_frame(snapshot, width=80, height=20, now=_NOW)
    attention_line = next(
        line for index, line in enumerate(lines) if "Attention" in lines[index - 1]
    )

    assert attention_line.strip() == (
        "decisions 2 pending · next INFRA-224 [high] "
        "Approve external repo deletion?"
    )


def test_zero_pending_decisions_leaves_attention_output_unchanged() -> None:
    with_none = render_frame(
        _frame_snapshot(decisions=None), width=80, height=20, now=_NOW
    )
    with_zero = render_frame(
        _frame_snapshot(decisions=_decision_fact(pending=0)),
        width=80,
        height=20,
        now=_NOW,
    )
    without_field = render_frame(_frame_snapshot(), width=80, height=20, now=_NOW)

    assert with_none == without_field
    assert with_zero == without_field


def test_decisions_attention_line_ranks_below_red_pressure_and_capacity_alerts() -> (
    None
):
    resource = _resource_fact(pressure="red")
    snapshot = _frame_snapshot(resource=resource, decisions=_decision_fact())
    lines = render_frame(snapshot, width=80, height=20, now=_NOW)
    attention_line = next(
        line for index, line in enumerate(lines) if "Attention" in lines[index - 1]
    )
    assert "pressure" in attention_line
    assert "decisions" not in attention_line


def test_decisions_attention_line_ranks_above_corrections_required() -> None:
    snapshot = _frame_snapshot(
        tasks=(_task(review_state="corrections_required", pr_number=7),),
        decisions=_decision_fact(),
    )
    lines = render_frame(snapshot, width=80, height=20, now=_NOW)
    attention_line = next(
        line for index, line in enumerate(lines) if "Attention" in lines[index - 1]
    )
    assert attention_line.strip().startswith("decisions 2 pending")
    assert "corrections" not in attention_line


def test_long_decision_question_truncates_within_width() -> None:
    long_question = "Approve " + "a very long external repo deletion " * 5 + "now?"
    snapshot = _frame_snapshot(
        decisions=_decision_fact(next_question=long_question)
    )

    lines = render_frame(snapshot, width=60, height=20, now=_NOW)

    for line in lines:
        assert len(line) == 60
    attention_line = next(
        line for index, line in enumerate(lines) if "Attention" in lines[index - 1]
    )
    assert attention_line.startswith("decisions 2 pending · next INFRA-224 [high]")
    assert attention_line.rstrip().endswith("…")
    assert long_question not in attention_line


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


# Sol correction deacc190 (INFRA-209 / INFRA-202): pr_number 0 is the
# explicit no-PR state of a corrections_required verdict returned before
# Sol opens the sole pull request. It must never render as PR#0.
def test_zero_pr_correction_renders_as_pre_pr_in_the_work_row() -> None:
    task = _task(
        issue_id="INFRA-209", pr_number=0, review_state="corrections_required"
    )
    lines = render_frame(
        _frame_snapshot(tasks=(task,)), width=76, height=20, now=_NOW
    )
    joined = "\n".join(lines)
    work_row = next(line for line in lines if line.startswith("INFRA-209"))
    assert "pre-PR corrections" in work_row
    assert "PR#0" not in joined
    assert "PR#" not in joined


def test_zero_pr_correction_in_attention_never_references_a_pull_request() -> None:
    task = _task(
        issue_id="INFRA-209", pr_number=0, review_state="corrections_required"
    )
    lines = render_frame(
        _frame_snapshot(tasks=(task,)), width=76, height=20, now=_NOW
    )
    attention_line = next(
        line for index, line in enumerate(lines) if "Attention" in lines[index - 1]
    )
    assert attention_line.strip() == "corrections requested before PR (INFRA-209)"
    assert "PR#" not in attention_line


def test_positive_pr_numbers_still_render_as_pr_n_in_work_and_attention() -> None:
    task = _task(
        issue_id="INFRA-208", pr_number=35, review_state="corrections_required"
    )
    lines = render_frame(
        _frame_snapshot(tasks=(task,)), width=76, height=20, now=_NOW
    )
    work_row = next(line for line in lines if line.startswith("INFRA-208"))
    assert "PR#35 corrections" in work_row
    attention_line = next(
        line for index, line in enumerate(lines) if "Attention" in lines[index - 1]
    )
    assert attention_line.strip() == "corrections requested on PR#35 (INFRA-208)"


# ---------------------------------------------------------------------------
# INFRA-219 R4 (Sol correction 110ed759): render_frame's Lanes section --
# the actual pane dashboard_refresh.py draws every tick was silently
# ignoring snapshot.lanes entirely. These tests exercise render_frame
# directly (not render_dashboard), which is the substitution the defect
# was about.
# ---------------------------------------------------------------------------


def _lane(
    *,
    project_key: str = "proj",
    lane_role: str = "development",
    cell_id: str = "cell-dev",
    session_id: str = "sess-dev",
    state: str = "active",
    issue_ids: tuple[str, ...] = (),
    blocked_issue_ids: tuple[str, ...] = (),
    subagents_total: int = 0,
    subagents_completed: int = 0,
) -> LaneCellFact:
    return LaneCellFact(
        project_key=project_key,
        lane_role=lane_role,
        cell_id=cell_id,
        session_id=session_id,
        state=state,
        issue_ids=issue_ids,
        blocked_issue_ids=blocked_issue_ids,
        subagents_total=subagents_total,
        subagents_completed=subagents_completed,
    )


def test_frame_shows_both_leads_lane_specific_issues_without_cross_attribution() -> (
    None
):
    dev = _lane(
        lane_role="development",
        cell_id="cell-dev",
        session_id="sess-dev",
        issue_ids=("INFRA-1", "INFRA-2"),
    )
    harness = _lane(
        lane_role="harness",
        cell_id="cell-harness",
        session_id="sess-harness",
        issue_ids=(),
    )
    snapshot = _frame_snapshot(lanes=(dev, harness))
    lines = render_frame(snapshot, width=100, height=30, now=_NOW)
    joined = "\n".join(lines)
    assert "Lanes" in joined

    dev_row = next(line for line in lines if "proj/development" in line)
    harness_row = next(line for line in lines if "proj/harness" in line)
    assert "INFRA-1" in dev_row and "INFRA-2" in dev_row
    # The harness row must never carry the development lane's issues --
    # this is exactly the cross-lane attribution Sol's correction bars.
    assert "INFRA-1" not in harness_row and "INFRA-2" not in harness_row
    assert "no product issue" in harness_row


def test_frame_lane_row_shows_subagents_head_event_pressure_and_blockers() -> None:
    dev = _lane(
        issue_ids=("INFRA-5",),
        blocked_issue_ids=("INFRA-5",),
        subagents_total=3,
        subagents_completed=1,
    )
    transitions = (
        TransitionFact(
            project_key="proj",
            occurred_at="2026-08-31T11:58:00+00:00",
            phrase="issue INFRA-5 → blocked",
        ),
    )
    resource = _resource_fact(pressure="yellow")
    snapshot = _frame_snapshot(lanes=(dev,), transitions=transitions, resource=resource)
    lines = render_frame(snapshot, width=200, height=30, now=_NOW)
    dev_row = next(line for line in lines if "proj/development" in line)
    assert "kids 1/3" in dev_row
    assert "issue INFRA-5" in dev_row  # head/event phrase
    assert "pressure yellow" in dev_row
    assert "blockers INFRA-5" in dev_row


def test_frame_lane_row_placeholders_when_event_or_resource_are_unknown() -> None:
    dev = _lane()
    # ``_frame_snapshot`` substitutes a default resource fact when given
    # None, so absence is expressed on the built snapshot itself.
    snapshot = dataclasses.replace(
        _frame_snapshot(lanes=(dev,), transitions=()), resource=None
    )
    lines = render_frame(snapshot, width=200, height=30, now=_NOW)
    dev_row = next(line for line in lines if "proj/development" in line)
    assert "no recorded event" in dev_row
    assert "no resource sample" in dev_row
    assert "no active issue" in dev_row
    assert "blockers none" in dev_row


def test_frame_with_lanes_still_honors_exact_height_and_width_contract() -> None:
    dev = _lane(issue_ids=("INFRA-1",))
    harness = _lane(lane_role="harness", cell_id="cell-h", session_id="sess-h")
    snapshot = _frame_snapshot(tasks=(_task(),), lanes=(dev, harness))
    for width, height in ((40, 12), (60, 20), (24, 6), (100, 30)):
        lines = render_frame(snapshot, width=width, height=height, now=_NOW)
        assert len(lines) == height
        assert all(visible_length(line) == width for line in lines)
