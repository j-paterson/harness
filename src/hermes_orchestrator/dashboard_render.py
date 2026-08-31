"""Pure fixed-width rendering of dashboard snapshots.

Functions here perform no I/O and read no clocks: every timestamp on
the dashboard was injected into the snapshot or the failure fact by the
caller, so rendering the same input always yields the same lines and
always the same number of them (a stable pane height).
"""

from __future__ import annotations

from dataclasses import dataclass

from hermes_orchestrator.dashboard_sources import (
    CodexFact,
    DashboardSnapshot,
    ProfileLeaseFact,
    ProfileUsage,
)


@dataclass(frozen=True, slots=True)
class TickFailure:
    """A recorded refresh-tick failure shown on the next success."""

    at: str
    reason: str


def render_dashboard(
    snapshot: DashboardSnapshot,
    *,
    width: int = 80,
    failure: TickFailure | None = None,
) -> tuple[str, ...]:
    """Render one snapshot to a fixed-width, fixed-height block."""

    leases = {lease.profile_alias: lease for lease in snapshot.leases}
    lines = [
        f"HERMES ORCHESTRATOR {snapshot.generated_at}",
        *(
            _profile_line(usage, leases.get(usage.profile_alias))
            for usage in snapshot.usage
        ),
        _codex_line(snapshot.codex),
        _status_line(failure),
    ]
    return tuple(_fit(line, width) for line in lines)


def _profile_line(usage: ProfileUsage, lease: ProfileLeaseFact | None) -> str:
    if lease is None:
        lease_text = "no lease"
    else:
        lease_text = f"{lease.project_key}/{lease.state}"
        if lease.cooldown_until is not None:
            lease_text += f" cooldown until {lease.cooldown_until}"
    return (
        f"{usage.profile_alias:<8} "
        f"fable {usage.fable_tokens:>12} "
        f"overall {usage.overall_tokens:>12}  {lease_text}"
    )


def _codex_line(codex: CodexFact) -> str:
    if not codex.available:
        return f"codex    unavailable since {codex.unavailable_since}"
    primary = _percent(codex.primary_used_percent)
    secondary = _percent(codex.secondary_used_percent)
    reached = " limit reached" if codex.reached else ""
    return f"codex    primary {primary} secondary {secondary}{reached}"


def _status_line(failure: TickFailure | None) -> str:
    if failure is None:
        return "last tick ok"
    return f"last tick failed at {failure.at} ({failure.reason})"


def _percent(value: int | None) -> str:
    return "?" if value is None else f"{value}%"


def _fit(line: str, width: int) -> str:
    return line[:width].ljust(width)
