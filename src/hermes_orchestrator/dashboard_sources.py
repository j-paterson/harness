"""Deterministic dashboard providers over durable state and static config.

Every fact on the supervisor dashboard comes from durable SQLite rows or
the static profile registry; the one external read (codex rate limits)
is an injected callable whose unavailability becomes an explicit
recorded fact instead of an exception. Nothing here calls a model.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from hermes_orchestrator.codex_rpc import CodexRateLimits
from hermes_orchestrator.db import Database
from hermes_orchestrator.profiles import ProfileRegistry

# The project_cells states that mean "this row is the project's current
# lead" (mirrors the one_active_cell_per_project unique index in
# migrations/0001_initial.sql).
_ACTIVE_CELL_STATES = ("starting", "active", "handoff_required", "paused")

# INFRA-209 (requirements reread; narrow-width follow-up point 4):
# admitted_issues.state maps onto the operator-facing Work states
# Queued/Working/Review/Paused/Blocked/Done. Paused is a deliberate
# hold (dimmed, never Attention-worthy) and is distinct from Blocked
# (failed/blocked/stalled — an actual problem). Any raw state outside
# these buckets is shown title-cased rather than hidden, so an
# unexpected state is still visible instead of silently dropped.
_QUEUED_STATES = ("queued", "admitted")
_PAUSED_STATES = ("paused",)
_BLOCKED_STATES = ("blocked", "failed", "stalled")

_OPERATOR_STATE_RANK = {
    "Working": 0,
    "Review": 1,
    "Queued": 2,
    "Paused": 3,
    "Blocked": 4,
    "Done": 5,
}

# A stream.result record's usage is the cumulative run total (all
# invocations plus children — see the INFRA-188 note in cells.py), so
# adding it to the per-invocation records it summarizes would double
# count the whole run.
_CUMULATIVE_EVENT_TYPES = frozenset({"stream.result"})

# INFRA-209 (requirements reread): the whitelist of event types the
# Work section's per-project "last:" line is built from — a bounded,
# named set of meaningful transitions, never a raw event dump.
_TRANSITION_EVENT_TYPES = frozenset(
    {
        "issue.transitioned",
        "issue.started",
        "assignment.published",
        "lead_correction.queued",
        "review.recorded",
        "review.corrections",
        "project_cell.rotated",
        "cmux_binding.bound",
        "control_operation.published",
    }
)

# Only these control_operation.published kinds are meaningful
# transitions (Work's "last:" line) or Attention-worthy on their own.
_CONTROL_OPERATION_ATTENTION_KINDS = frozenset(
    {
        "channel.approval_required",
        "channel.rebind_refused",
        "channel.adopt_refused",
        "lead.launch_failed",
        "signal.failed",
        "channel.blocked",
        # INFRA-198: a mid-run dead lead was retired and its cell
        # released; the operator has to seat the replacement.
        "lead.dead_worker_retired",
    }
)

CodexRateLimitReader = Callable[[], Awaitable[CodexRateLimits]]


@dataclass(frozen=True, slots=True)
class ProfileUsage:
    """Token totals for one profile: lead (fable) versus everything.

    Kept for the `--detail` view only — the default Capacity section
    never shows raw cumulative token counts (operator correction).
    """

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
class TaskFact:
    """One in-flight (or, if it is the project's only issue, done)
    admitted_issues row, joined to its lead, children, and Sol state.

    Session ids are never carried here — the Work section names issues
    and profiles, never raw session identity.
    """

    issue_id: str
    project_key: str
    priority: int
    operator_state: str
    lead_profile: str | None
    children_completed: int
    children_total: int
    pr_number: int | None
    review_state: str | None
    settlement_state: str | None
    updated_at: str


@dataclass(frozen=True, slots=True)
class UsageWindows:
    """The locally cached Claude usage percentages for one profile.

    Sourced from ``<config_dir>/.claude.json``'s ``cachedUsageUtilization``
    — read-only, never refreshed or authenticated by the dashboard. Every
    field is None when the cache is absent, unreadable, or malformed.
    """

    fetched_at: str | None
    five_hour_used: int | None
    five_hour_resets_at: str | None
    seven_day_used: int | None
    seven_day_resets_at: str | None
    fable_used: int | None
    fable_resets_at: str | None
    fable_severity: str | None
    fable_active: bool | None


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


@dataclass(frozen=True, slots=True)
class CapacityFact:
    """The latest fable capacity observation for one profile, or none.

    ``windows`` carries the separate locally cached live percentages
    (see `UsageWindows`) alongside the durable observation fields —
    the two sources are merged for display in dashboard_render.py, not
    here, so each stays independently testable.
    """

    profile_alias: str
    state: str | None
    source: str | None
    observed_at: str | None
    resets_at: str | None
    detail: str | None = None
    windows: UsageWindows = _EMPTY_WINDOWS


@dataclass(frozen=True, slots=True)
class ResourceFact:
    """The latest bounded resource_samples row."""

    sampled_at: str
    pressure: str
    available_memory_bytes: int
    total_memory_bytes: int
    swap_used_bytes: int
    load_one: float
    logical_cpus: int
    managed_rss_bytes: int
    min_disk_free_bytes: int | None


@dataclass(frozen=True, slots=True)
class IdleFact:
    """One live-lead project with nothing in development right now
    (INFRA-199): either its queue truly holds no ready work, or the
    concrete hold on its top-ranked lane.
    """

    project_key: str
    hold: str


@dataclass(frozen=True, slots=True)
class WorkerFact:
    """Active worker_leases, total and broken down by kind."""

    active_total: int
    active_by_kind: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class TransitionFact:
    """The most recent whitelisted transition for one project."""

    project_key: str
    occurred_at: str
    phrase: str


@dataclass(frozen=True, slots=True)
class ControlAttentionFact:
    """The newest still-published, Attention-worthy control operation."""

    kind: str
    project_key: str
    created_at: str


@dataclass(frozen=True, slots=True)
class LaneCellFact:
    """One lead cell's lane row (INFRA-219 L2, corrected under R4).

    Sol correction 110ed759: the prior shape resolved a single
    "current issue" per PROJECT (``admitted_issues`` carries no lane
    dimension) and reported it on every lane row for that project --
    the harness lane claimed a product issue it never held, and a
    development lane running MULTIPLE concurrent issues (bounded by
    ``cells.MAX_DEVELOPMENT_ISSUE_LANES``) only ever showed one.

    ``issue_ids``/``blocked_issue_ids`` are instead resolved through
    ``lead_assignments``, which IS durably lane-scoped: dispatch
    (``cells._dispatch_locked``) only ever publishes an assignment
    packet for a non-harness dispatch, bound to the exact ``cell_id``
    of the lane doing the work. Joining ``admitted_issues`` to the
    live (non-superseded) assignment for THIS cell_id therefore
    attributes every issue to the one lane that actually holds it: a
    harness row's cell_id never has a live assignment, so it resolves
    to no issues at all -- never the project's development issue(s).

    ``subagents_total``/``subagents_completed`` come from
    ``lead_children`` keyed by this lane's own ``session_id`` -- each
    lane has its own session, so this is naturally lane-scoped with no
    cross-lane risk.

    Head/event and resource pressure are deliberately NOT carried
    here: their durable sources (``TransitionProvider``,
    ``ResourceProvider``) are project-wide and host-wide respectively
    -- neither has a lane dimension to resolve, so
    ``dashboard_render`` reads them straight off the snapshot
    (matched by ``project_key``, or global) rather than inventing a
    lane-exclusive fact that the durable state does not actually
    contain.
    """

    project_key: str
    lane_role: str
    cell_id: str
    session_id: str
    state: str
    issue_ids: tuple[str, ...]
    blocked_issue_ids: tuple[str, ...]
    subagents_total: int
    subagents_completed: int


@dataclass(frozen=True, slots=True)
class DashboardSnapshot:
    """One frozen tick's worth of dashboard facts."""

    generated_at: str
    usage: tuple[ProfileUsage, ...]
    leases: tuple[ProfileLeaseFact, ...]
    codex: CodexFact
    tasks: tuple[TaskFact, ...] = ()
    capacity: tuple[CapacityFact, ...] = ()
    resource: ResourceFact | None = None
    tasks_observed_at: str | None = None
    workers: WorkerFact = WorkerFact(active_total=0, active_by_kind=())
    transitions: tuple[TransitionFact, ...] = ()
    attention_control: ControlAttentionFact | None = None
    idle: tuple[IdleFact, ...] = ()
    lanes: tuple[LaneCellFact, ...] = ()


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


def _operator_state(raw: str) -> str:
    if raw in _QUEUED_STATES:
        return "Queued"
    if raw == "in_development":
        return "Working"
    if raw == "review":
        return "Review"
    if raw in _PAUSED_STATES:
        return "Paused"
    if raw in _BLOCKED_STATES:
        return "Blocked"
    if raw == "done":
        return "Done"
    return raw.capitalize()


class TaskProvider:
    """Read admitted issues, joined to their lead, children, and Sol state.

    Every project shows its in-flight (non-done) issues; a project
    whose issues are all done shows only its single most recently
    updated one, so a finished project never vanishes from Work
    entirely, but its issues no longer flood the frame once complete.
    """

    def __init__(self, database: Database) -> None:
        self._database = database

    def tasks(self) -> tuple[TaskFact, ...]:
        """Return every selected admitted issue, ranked for the frame."""

        issue_rows = self._database.execute(
            "SELECT issue_id, project_key, priority, state, updated_at "
            "FROM admitted_issues",
        ).fetchall()
        by_project: dict[str, list] = {}
        for row in issue_rows:
            by_project.setdefault(str(row["project_key"]), []).append(row)

        selected_rows = []
        for rows in by_project.values():
            non_done = [row for row in rows if str(row["state"]) != "done"]
            if non_done:
                selected_rows.extend(non_done)
            else:
                selected_rows.append(
                    max(rows, key=lambda row: str(row["updated_at"]))
                )

        issue_leads = self._issue_leads()
        project_leads = self._leads()
        latest_review = self._latest_reviews()
        latest_settlement = self._latest_settlements()
        children = self._children_by_session()

        facts = []
        for row in selected_rows:
            issue_id = str(row["issue_id"])
            project_key = str(row["project_key"])
            lead_profile, lead_session = issue_leads.get(
                issue_id, project_leads.get(project_key, (None, None))
            )
            total, completed = (
                children.get(lead_session, (0, 0)) if lead_session else (0, 0)
            )
            pr = latest_review.get(issue_id)
            pr_number, review_state = pr if pr is not None else (None, None)
            facts.append(
                TaskFact(
                    issue_id=issue_id,
                    project_key=project_key,
                    priority=int(row["priority"]),
                    operator_state=_operator_state(str(row["state"])),
                    lead_profile=lead_profile,
                    children_completed=completed,
                    children_total=total,
                    pr_number=pr_number,
                    review_state=review_state,
                    settlement_state=latest_settlement.get(issue_id),
                    updated_at=str(row["updated_at"]),
                )
            )
        facts.sort(
            key=lambda fact: (
                _OPERATOR_STATE_RANK.get(
                    fact.operator_state, len(_OPERATOR_STATE_RANK)
                ),
                fact.priority,
                fact.issue_id,
            )
        )
        return tuple(facts)

    def observed_at(self) -> str | None:
        """Return the durable freshness bound for the tasks source."""

        row = self._database.execute(
            "SELECT max(updated_at) FROM admitted_issues",
        ).fetchone()
        value = row[0] if row is not None else None
        return str(value) if value is not None else None

    def _leads(self) -> dict[str, tuple[str | None, str | None]]:
        rows = self._database.execute(
            "SELECT project_key, profile_alias, session_id FROM "
            "project_cells WHERE state IN (?, ?, ?, ?) "
            "ORDER BY CASE lane_role WHEN 'development' THEN 0 ELSE 1 END, "
            "updated_at DESC",
            _ACTIVE_CELL_STATES,
        ).fetchall()
        leads: dict[str, tuple[str | None, str | None]] = {}
        for row in rows:
            project_key = str(row["project_key"])
            if project_key in leads:
                continue
            leads[project_key] = (
                str(row["profile_alias"])
                if row["profile_alias"] is not None
                else None,
                str(row["session_id"])
                if row["session_id"] is not None
                else None,
            )
        return leads

    def _issue_leads(self) -> dict[str, tuple[str | None, str | None]]:
        """Resolve ownership from each issue's current lane assignment.

        A project may have development and harness cells concurrently, so
        project-level ownership is only a fallback for pre-assignment rows.
        """

        placeholders = ",".join("?" for _ in _ACTIVE_CELL_STATES)
        rows = self._database.execute(
            "SELECT la.issue_id, pc.profile_alias, pc.session_id "
            "FROM lead_assignments la JOIN project_cells pc "
            "ON pc.cell_id = la.cell_id AND pc.session_id = la.session_id "
            f"WHERE la.state != 'superseded' AND pc.state IN ({placeholders}) "
            "ORDER BY la.updated_at DESC",
            _ACTIVE_CELL_STATES,
        ).fetchall()
        leads: dict[str, tuple[str | None, str | None]] = {}
        for row in rows:
            issue_id = str(row["issue_id"])
            if issue_id in leads:
                continue
            leads[issue_id] = (
                str(row["profile_alias"])
                if row["profile_alias"] is not None
                else None,
                str(row["session_id"])
                if row["session_id"] is not None
                else None,
            )
        return leads

    def lane_cells(self) -> tuple[LaneCellFact, ...]:
        """One row per live lead cell, development and harness alike
        (INFRA-219 L2; issue attribution corrected under R4, Sol
        correction 110ed759 -- see ``LaneCellFact``).

        ``issue_ids`` is every currently in-flight or blocked
        admitted issue whose live (non-superseded) ``lead_assignments``
        row is bound to THIS cell's ``cell_id`` -- never a
        project-wide guess. Harness cells never publish an assignment
        (``cells._dispatch_locked`` skips it whenever the dispatch is
        a harness one), so a harness row's ``issue_ids`` is always
        empty by construction, not by a name/state filter that could
        rot out of sync. ``blocked_issue_ids`` is the subset of those
        issues sitting in a blocked/failed/stalled state -- this
        lane's own blockers, never another lane's.
        """

        rows = self._database.execute(
            "SELECT cell_id, project_key, lane_role, session_id, state "
            "FROM project_cells WHERE state IN (?, ?, ?, ?) "
            "ORDER BY project_key, lane_role",
            _ACTIVE_CELL_STATES,
        ).fetchall()
        issue_rows = self._database.execute(
            "SELECT ai.issue_id AS issue_id, ai.state AS issue_state, "
            "la.cell_id AS cell_id "
            "FROM admitted_issues ai "
            "JOIN lead_assignments la ON la.issue_id = ai.issue_id "
            "WHERE la.state != 'superseded' AND ai.state IN "
            "('in_development', 'review', 'blocked', 'failed', 'stalled') "
            "ORDER BY ai.issue_id",
        ).fetchall()
        issues_by_cell: dict[str, list[str]] = {}
        blocked_by_cell: dict[str, list[str]] = {}
        for issue_row in issue_rows:
            cell_id = str(issue_row["cell_id"])
            issue_id = str(issue_row["issue_id"])
            issues_by_cell.setdefault(cell_id, []).append(issue_id)
            if str(issue_row["issue_state"]) in _BLOCKED_STATES:
                blocked_by_cell.setdefault(cell_id, []).append(issue_id)

        subagents_by_session = self._children_by_session()

        return tuple(
            LaneCellFact(
                project_key=str(row["project_key"]),
                lane_role=str(row["lane_role"]),
                cell_id=str(row["cell_id"]),
                session_id=str(row["session_id"]),
                state=str(row["state"]),
                issue_ids=tuple(issues_by_cell.get(str(row["cell_id"]), ())),
                blocked_issue_ids=tuple(
                    blocked_by_cell.get(str(row["cell_id"]), ())
                ),
                subagents_total=subagents_by_session.get(
                    str(row["session_id"]), (0, 0)
                )[0],
                subagents_completed=subagents_by_session.get(
                    str(row["session_id"]), (0, 0)
                )[1],
            )
            for row in rows
        )

    def _latest_reviews(self) -> dict[str, tuple[int, str]]:
        rows = self._database.execute(
            "SELECT issue_id, pr_number, state FROM reviews "
            "ORDER BY issue_id, created_at DESC",
        ).fetchall()
        latest: dict[str, tuple[int, str]] = {}
        for row in rows:
            issue_id = str(row["issue_id"])
            if issue_id in latest:
                continue
            latest[issue_id] = (int(row["pr_number"]), str(row["state"]))
        return latest

    def _latest_settlements(self) -> dict[str, str]:
        rows = self._database.execute(
            "SELECT issue_id, state FROM merge_settlements "
            "ORDER BY issue_id, created_at DESC",
        ).fetchall()
        latest: dict[str, str] = {}
        for row in rows:
            issue_id = str(row["issue_id"])
            if issue_id in latest:
                continue
            latest[issue_id] = str(row["state"])
        return latest

    def _children_by_session(self) -> dict[str, tuple[int, int]]:
        rows = self._database.execute(
            "SELECT session_id, state FROM lead_children",
        ).fetchall()
        counts: dict[str, tuple[int, int]] = {}
        for row in rows:
            session_id = str(row["session_id"])
            total, completed = counts.get(session_id, (0, 0))
            total += 1
            if str(row["state"]) == "completed":
                completed += 1
            counts[session_id] = (total, completed)
        return counts

    def idle_notes(self, resource: ResourceFact | None) -> tuple[IdleFact, ...]:
        """Name why each live-lead, non-working project is idle
        (INFRA-199): the top-ranked lane's concrete hold, or that the
        queue simply holds no ready work.
        """

        from hermes_orchestrator.admission import YELLOW_ADMITS_PRIORITY_AT_MOST
        from hermes_orchestrator.operator_decisions import OperatorDecisions

        working = {
            str(row["project_key"])
            for row in self._database.execute(
                "SELECT DISTINCT project_key FROM admitted_issues "
                "WHERE state = 'in_development'",
            ).fetchall()
        }
        live = {
            str(row["project_key"])
            for row in self._database.execute(
                "SELECT DISTINCT project_key FROM project_cells "
                "WHERE state = 'active'",
            ).fetchall()
        }
        if not (live - working):
            return ()
        max_priority = (
            {"green": 4, "yellow": YELLOW_ADMITS_PRIORITY_AT_MOST}.get(
                resource.pressure
            )
            if resource is not None
            else None
        )
        decisions = OperatorDecisions(self._database)
        notes = []
        for project_key in sorted(live - working):
            hold = "no queued work"
            top = self._database.execute(
                "SELECT issue_id, priority, dependency_ready FROM "
                "admitted_issues WHERE project_key = ? AND state IN "
                "('queued', 'blocked') ORDER BY priority ASC, "
                "dependency_ready DESC, admitted_at ASC, overlap_risk ASC, "
                "issue_id ASC LIMIT 1",
                (project_key,),
            ).fetchone()
            if top is not None:
                if not top["dependency_ready"]:
                    hold = "dependency-blocked"
                elif decisions.pending_for_issue(str(top["issue_id"])):
                    hold = "operator decision pending"
                elif max_priority is None or int(top["priority"]) > max_priority:
                    hold = "capacity limited"
            notes.append(IdleFact(project_key=project_key, hold=hold))
        return tuple(notes)


class CapacityProvider:
    """Read the latest fable capacity observation per registry alias."""

    def __init__(self, database: Database, registry: ProfileRegistry) -> None:
        self._database = database
        self._registry = registry

    def capacity(
        self,
        windows_by_alias: Mapping[str, UsageWindows] | None = None,
    ) -> tuple[CapacityFact, ...]:
        """Return one capacity fact per registry alias, in registry order.

        `windows_by_alias` merges in the separately read live usage-cache
        percentages (see `ClaudeUsageCacheProvider`); when omitted every
        fact's `windows` is the all-None placeholder.
        """

        windows_by_alias = windows_by_alias or {}
        facts = []
        for profile in self._registry.profiles:
            windows = windows_by_alias.get(profile.alias, _EMPTY_WINDOWS)
            row = self._database.execute(
                "SELECT state, source, observed_at, resets_at, detail FROM "
                "profile_capacity_observations WHERE profile_alias = ? "
                "AND model = 'fable' "
                "ORDER BY observed_at DESC, observation_id DESC LIMIT 1",
                (profile.alias,),
            ).fetchone()
            if row is None:
                facts.append(
                    CapacityFact(
                        profile_alias=profile.alias,
                        state=None,
                        source=None,
                        observed_at=None,
                        resets_at=None,
                        detail=None,
                        windows=windows,
                    )
                )
                continue
            facts.append(
                CapacityFact(
                    profile_alias=profile.alias,
                    state=str(row["state"]),
                    source=str(row["source"]),
                    observed_at=str(row["observed_at"]),
                    resets_at=(
                        str(row["resets_at"])
                        if row["resets_at"] is not None
                        else None
                    ),
                    detail=(
                        str(row["detail"]) if row["detail"] is not None else None
                    ),
                    windows=windows,
                )
            )
        return tuple(facts)


def _default_read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _ms_to_iso(value: object) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        return datetime.fromtimestamp(value / 1000, tz=UTC).isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def _window_fields(window: object) -> tuple[int | None, str | None]:
    if not isinstance(window, dict):
        return None, None
    used = window.get("utilization")
    if isinstance(used, bool) or not isinstance(used, int):
        used = None
    resets_at = window.get("resets_at")
    if not isinstance(resets_at, str):
        resets_at = None
    return used, resets_at


class ClaudeUsageCacheProvider:
    """Read each profile's local `~/.claude.json` usage-percentage cache.

    Read-only: a missing file, an unreadable one, malformed JSON, or a
    missing key all resolve to the all-None `UsageWindows` placeholder
    rather than raising — this provider never writes, refreshes, or
    authenticates anything.
    """

    def __init__(
        self,
        registry: ProfileRegistry,
        read_text: Callable[[Path], str] = _default_read_text,
    ) -> None:
        self._registry = registry
        self._read_text = read_text

    def windows(self) -> tuple[tuple[str, UsageWindows], ...]:
        """Return (alias, UsageWindows) pairs in registry order."""

        return tuple(
            (profile.alias, self._read_one(profile))
            for profile in self._registry.profiles
        )

    def _read_one(self, profile) -> UsageWindows:
        path = profile.config_dir / ".claude.json"
        try:
            text = self._read_text(path)
        except OSError:
            return _EMPTY_WINDOWS
        try:
            document = json.loads(text)
        except ValueError:
            return _EMPTY_WINDOWS
        if not isinstance(document, dict):
            return _EMPTY_WINDOWS
        cache = document.get("cachedUsageUtilization")
        if not isinstance(cache, dict):
            return _EMPTY_WINDOWS
        fetched_at = _ms_to_iso(cache.get("fetchedAtMs"))
        utilization = cache.get("utilization")
        if not isinstance(utilization, dict):
            return UsageWindows(
                fetched_at=fetched_at,
                five_hour_used=None,
                five_hour_resets_at=None,
                seven_day_used=None,
                seven_day_resets_at=None,
                fable_used=None,
                fable_resets_at=None,
                fable_severity=None,
                fable_active=None,
            )
        five_hour_used, five_hour_resets_at = _window_fields(
            utilization.get("five_hour")
        )
        seven_day_used, seven_day_resets_at = _window_fields(
            utilization.get("seven_day")
        )
        fable_used = fable_resets_at = fable_severity = fable_active = None
        limits = utilization.get("limits")
        if isinstance(limits, list):
            for limit in limits:
                if not isinstance(limit, dict) or limit.get("kind") != "weekly_scoped":
                    continue
                scope = limit.get("scope")
                model = scope.get("model") if isinstance(scope, dict) else None
                display_name = (
                    model.get("display_name") if isinstance(model, dict) else None
                )
                if display_name != "Fable":
                    continue
                percent = limit.get("percent")
                fable_used = (
                    percent
                    if isinstance(percent, int) and not isinstance(percent, bool)
                    else None
                )
                resets = limit.get("resets_at")
                fable_resets_at = resets if isinstance(resets, str) else None
                severity = limit.get("severity")
                fable_severity = severity if isinstance(severity, str) else None
                active = limit.get("is_active")
                fable_active = active if isinstance(active, bool) else None
                break
        return UsageWindows(
            fetched_at=fetched_at,
            five_hour_used=five_hour_used,
            five_hour_resets_at=five_hour_resets_at,
            seven_day_used=seven_day_used,
            seven_day_resets_at=seven_day_resets_at,
            fable_used=fable_used,
            fable_resets_at=fable_resets_at,
            fable_severity=fable_severity,
            fable_active=fable_active,
        )


class ResourceProvider:
    """Read the latest bounded resource sample, if any has been taken."""

    def __init__(self, database: Database) -> None:
        self._database = database

    def resource(self) -> ResourceFact | None:
        """Return the newest resource_samples row, or None if absent."""

        row = self._database.execute(
            "SELECT sampled_at, pressure, available_memory_bytes, "
            "total_memory_bytes, swap_used_bytes, load_one, logical_cpus, "
            "managed_rss_bytes, disk_json FROM resource_samples "
            "ORDER BY sampled_at DESC LIMIT 1",
        ).fetchone()
        if row is None:
            return None
        min_disk_free_bytes = None
        try:
            disk = json.loads(str(row["disk_json"]))
        except ValueError:
            disk = None
        if isinstance(disk, dict) and disk:
            values = [v for v in disk.values() if isinstance(v, int)]
            if values:
                min_disk_free_bytes = min(values)
        return ResourceFact(
            sampled_at=str(row["sampled_at"]),
            pressure=str(row["pressure"]),
            available_memory_bytes=int(row["available_memory_bytes"]),
            total_memory_bytes=int(row["total_memory_bytes"]),
            swap_used_bytes=int(row["swap_used_bytes"]),
            load_one=float(row["load_one"]),
            logical_cpus=int(row["logical_cpus"]),
            managed_rss_bytes=int(row["managed_rss_bytes"]),
            min_disk_free_bytes=min_disk_free_bytes,
        )


class WorkerProvider:
    """Count active worker_leases, overall and by kind."""

    def __init__(self, database: Database) -> None:
        self._database = database

    def workers(self) -> WorkerFact:
        rows = self._database.execute(
            "SELECT kind, COUNT(*) AS n FROM worker_leases "
            "WHERE state = 'active' GROUP BY kind ORDER BY kind",
        ).fetchall()
        by_kind = tuple((str(row["kind"]), int(row["n"])) for row in rows)
        return WorkerFact(
            active_total=sum(n for _, n in by_kind),
            active_by_kind=by_kind,
        )


def _transition_phrase(event_type: str, payload: dict) -> str:
    if event_type == "review.recorded":
        return f"review recorded {payload.get('state', '?')}"
    if event_type in ("review.corrections", "lead_correction.queued"):
        reason = payload.get("reason")
        return "correction queued" + (f" ({reason})" if reason else "")
    if event_type == "project_cell.rotated":
        previous = payload.get("previous_profile_alias", "?")
        current = payload.get("profile_alias", "?")
        return f"rotated {previous}→{current}"
    if event_type == "assignment.published":
        return f"assignment published {payload.get('issue_id', '')}".strip()
    if event_type == "issue.started":
        return f"issue started {payload.get('issue_id', '')}".strip()
    if event_type == "issue.transitioned":
        issue_id = payload.get("issue_id", "")
        state = payload.get("state", "")
        return f"issue {issue_id} → {state}".strip()
    if event_type == "cmux_binding.bound":
        return "cmux binding bound"
    if event_type == "control_operation.published":
        return f"control: {payload.get('kind', '?')}"
    return event_type


class TransitionProvider:
    """Track the newest whitelisted transition per project.

    Mirrors UsageAggregator's watermark shape: a restart re-reads the
    full journal (deterministic), and within one process lifetime each
    tick reads only rows appended since the last.
    """

    def __init__(self, database: Database) -> None:
        self._database = database
        self._watermark = 0
        self._latest: dict[str, TransitionFact] = {}

    def advance(self) -> None:
        """Fold every not-yet-seen whitelisted event into the latest map."""

        rows = self._database.execute(
            "SELECT sequence, occurred_at, event_type, payload_json "
            "FROM events WHERE sequence > ? ORDER BY sequence",
            (self._watermark,),
        ).fetchall()
        for row in rows:
            self._watermark = max(self._watermark, int(row["sequence"]))
            event_type = str(row["event_type"])
            if event_type not in _TRANSITION_EVENT_TYPES:
                continue
            try:
                payload = json.loads(str(row["payload_json"]))
            except ValueError:
                continue
            if not isinstance(payload, dict):
                continue
            if (
                event_type == "control_operation.published"
                and payload.get("kind") not in _CONTROL_OPERATION_ATTENTION_KINDS
            ):
                continue
            project_key = payload.get("project_key")
            if not isinstance(project_key, str):
                continue
            self._latest[project_key] = TransitionFact(
                project_key=project_key,
                occurred_at=str(row["occurred_at"]),
                phrase=_transition_phrase(event_type, payload),
            )

    def transitions(self) -> tuple[TransitionFact, ...]:
        """Return the latest transition per project, project-key ordered."""

        return tuple(self._latest[key] for key in sorted(self._latest))


class ControlAttentionProvider:
    """Read the newest still-published, Attention-worthy control op."""

    def __init__(self, database: Database) -> None:
        self._database = database

    def latest(self) -> ControlAttentionFact | None:
        placeholders = ",".join("?" for _ in _CONTROL_OPERATION_ATTENTION_KINDS)
        cell_states = ",".join("?" for _ in _ACTIVE_CELL_STATES)
        row = self._database.execute(
            "SELECT co.kind, co.project_key, co.created_at "
            "FROM control_operations co "
            f"WHERE co.state = 'published' AND co.kind IN ({placeholders}) "
            "AND NOT (co.kind = 'channel.blocked' AND EXISTS ("
            "SELECT 1 FROM cmux_surface_bindings binding "
            "WHERE binding.cell_id = co.cell_id "
            "AND binding.session_id = co.session_id "
            "AND binding.state = 'active'"
            ")) "
            "AND (co.session_id IS NULL OR EXISTS ("
            "SELECT 1 FROM project_cells exact WHERE exact.cell_id = co.cell_id "
            "AND exact.session_id = co.session_id "
            f"AND exact.state IN ({cell_states})"
            ") OR NOT EXISTS ("
            "SELECT 1 FROM project_cells current "
            "WHERE current.project_key = co.project_key "
            f"AND current.state IN ({cell_states})"
            ")) "
            "ORDER BY co.created_at DESC LIMIT 1",
            (
                *_CONTROL_OPERATION_ATTENTION_KINDS,
                *_ACTIVE_CELL_STATES,
                *_ACTIVE_CELL_STATES,
            ),
        ).fetchone()
        if row is None:
            return None
        return ControlAttentionFact(
            kind=str(row["kind"]),
            project_key=str(row["project_key"]),
            created_at=str(row["created_at"]),
        )


class DashboardSources:
    """Compose every provider into one frozen snapshot per tick."""

    def __init__(
        self,
        *,
        database: Database,
        registry: ProfileRegistry,
        codex_rate_limits: CodexRateLimitReader | None = None,
        claude_usage_read_text: Callable[[Path], str] = _default_read_text,
    ) -> None:
        self._database = database
        self._registry = registry
        self._usage = UsageAggregator(database)
        self._codex = CodexStatusProvider(codex_rate_limits)
        self._tasks = TaskProvider(database)
        self._capacity = CapacityProvider(database, registry)
        self._claude_usage_cache = ClaudeUsageCacheProvider(
            registry, claude_usage_read_text
        )
        self._resource = ResourceProvider(database)
        self._workers = WorkerProvider(database)
        self._transitions = TransitionProvider(database)
        self._control_attention = ControlAttentionProvider(database)

    async def collect(self, now: datetime) -> DashboardSnapshot:
        """Read durable state and return this tick's frozen facts."""

        generated_at = now.isoformat()
        self._usage.advance()
        self._transitions.advance()
        windows_by_alias = dict(self._claude_usage_cache.windows())
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
        resource = self._resource.resource()
        return DashboardSnapshot(
            generated_at=generated_at,
            usage=self._usage.usage_for(aliases),
            leases=leases,
            codex=codex,
            tasks=self._tasks.tasks(),
            capacity=self._capacity.capacity(windows_by_alias),
            resource=resource,
            tasks_observed_at=self._tasks.observed_at(),
            workers=self._workers.workers(),
            transitions=self._transitions.transitions(),
            attention_control=self._control_attention.latest(),
            idle=self._tasks.idle_notes(resource),
            lanes=self._tasks.lane_cells(),
        )
