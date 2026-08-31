"""Pure fixed-width rendering from frozen dashboard snapshots."""

from __future__ import annotations

from hermes_orchestrator.dashboard_render import TickFailure, render_dashboard
from hermes_orchestrator.dashboard_sources import (
    CodexFact,
    DashboardSnapshot,
    ProfileLeaseFact,
    ProfileUsage,
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
