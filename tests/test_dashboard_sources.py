"""Deterministic dashboard providers over durable state only."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.codex_rpc import CodexRateLimits
from hermes_orchestrator.control_operations import ControlOperations
from hermes_orchestrator.dashboard_sources import (
    CapacityProvider,
    ClaudeUsageCacheProvider,
    CodexStatusProvider,
    ControlAttentionProvider,
    DashboardSources,
    IdleFact,
    ProfileUsage,
    ResourceFact,
    ResourceProvider,
    TaskProvider,
    TransitionProvider,
    UsageAggregator,
    WorkerProvider,
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


# ---------------------------------------------------------------------------
# INFRA-209 (requirements reread): Work/Capacity/System/Attention providers.
# ---------------------------------------------------------------------------


def _insert_issue(
    connection,
    *,
    issue_id: str,
    project_key: str,
    priority: int,
    state: str,
    updated_at: str,
    dependency_ready: int = 1,
) -> None:
    connection.execute(
        "INSERT INTO admitted_issues("
        "issue_id, project_key, priority, state, admitted_at, updated_at, "
        "dependency_ready"
        ") VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            issue_id,
            project_key,
            priority,
            state,
            updated_at,
            updated_at,
            dependency_ready,
        ),
    )


def _insert_cell(
    connection,
    *,
    cell_id: str,
    project_key: str,
    state: str,
    profile_alias: str | None,
    session_id: str | None,
    lane_role: str = "development",
) -> None:
    connection.execute(
        "INSERT INTO project_cells("
        "cell_id, project_key, state, profile_alias, session_id, "
        "lane_role, created_at, updated_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            cell_id,
            project_key,
            state,
            profile_alias,
            session_id,
            lane_role,
            "2026-08-30T00:00:00+00:00",
            "2026-08-30T00:00:00+00:00",
        ),
    )


def _insert_assignment(
    connection,
    *,
    assignment_id: str,
    project_key: str,
    issue_id: str,
    cell_id: str,
    session_id: str,
    state: str = "published",
) -> None:
    connection.execute(
        "INSERT INTO lead_assignments("
        "assignment_id, schema_version, project_key, issue_id, cell_id, "
        "session_id, profile_alias, instruction_id, queue_transition, "
        "state, created_at, updated_at"
        ") VALUES (?, 1, ?, ?, ?, ?, 'max-a', ?, 'queued->in_development', "
        "?, ?, ?)",
        (
            assignment_id,
            project_key,
            issue_id,
            cell_id,
            session_id,
            f"instr-{assignment_id}",
            state,
            "2026-08-30T00:00:00+00:00",
            "2026-08-30T00:00:00+00:00",
        ),
    )


def _insert_review(
    connection,
    *,
    review_id: str,
    issue_id: str,
    pr_number: int,
    state: str,
    created_at: str,
) -> None:
    connection.execute(
        "INSERT INTO reviews("
        "review_id, project_key, issue_id, event_id, repository, branch, "
        "pr_number, reviewed_sha, state, created_at, updated_at"
        ") VALUES (?, 'proj', ?, ?, 'org/repo', 'feature', ?, 'sha', "
        "?, ?, ?)",
        (review_id, issue_id, review_id, pr_number, state, created_at, created_at),
    )


def _insert_settlement(
    connection,
    *,
    settlement_id: str,
    issue_id: str,
    state: str,
    created_at: str,
) -> None:
    connection.execute(
        "INSERT INTO merge_settlements("
        "settlement_id, project_key, issue_id, event_id, repository, "
        "branch, pr_number, base_sha, candidate_sha, thread_id, "
        "thread_generation, manifest_version, path, state, created_at, "
        "updated_at"
        ") VALUES (?, 'proj', ?, ?, 'org/repo', 'feature', 1, 'base', "
        "'candidate', 'thread', 1, 1, 'guarded', ?, ?, ?)",
        (settlement_id, issue_id, settlement_id, state, created_at, created_at),
    )


def _insert_child(
    connection, *, session_id: str, child_id: str, state: str
) -> None:
    connection.execute(
        "INSERT INTO lead_children(session_id, child_id, state, started_at) "
        "VALUES (?, ?, ?, '2026-08-30T00:00:00+00:00')",
        (session_id, child_id, state),
    )


def test_work_states_are_mapped_and_ranked(tmp_path: Path) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _insert_issue(
            connection,
            issue_id="INFRA-1",
            project_key="proj-a",
            priority=2,
            state="queued",
            updated_at="2026-08-30T10:00:00+00:00",
        )
        _insert_issue(
            connection,
            issue_id="INFRA-2",
            project_key="proj-b",
            priority=1,
            state="in_development",
            updated_at="2026-08-30T11:00:00+00:00",
        )
        _insert_issue(
            connection,
            issue_id="INFRA-3",
            project_key="proj-c",
            priority=3,
            state="review",
            updated_at="2026-08-30T09:00:00+00:00",
        )
        _insert_issue(
            connection,
            issue_id="INFRA-4",
            project_key="proj-d",
            priority=1,
            state="paused",
            updated_at="2026-08-30T08:00:00+00:00",
        )
        _insert_issue(
            connection,
            issue_id="INFRA-5",
            project_key="proj-e",
            priority=1,
            state="failed",
            updated_at="2026-08-30T08:00:00+00:00",
        )

    tasks = TaskProvider(database).tasks()

    assert [(task.issue_id, task.operator_state) for task in tasks] == [
        ("INFRA-2", "Working"),
        ("INFRA-3", "Review"),
        ("INFRA-1", "Queued"),
        ("INFRA-4", "Paused"),
        ("INFRA-5", "Blocked"),
    ]
    database.close()


def test_done_issue_is_omitted_unless_it_is_the_projects_only_issue(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        # proj-a has an in-flight issue: its done issue must be omitted.
        _insert_issue(
            connection,
            issue_id="INFRA-10",
            project_key="proj-a",
            priority=1,
            state="in_development",
            updated_at="2026-08-30T11:00:00+00:00",
        )
        _insert_issue(
            connection,
            issue_id="INFRA-9",
            project_key="proj-a",
            priority=1,
            state="done",
            updated_at="2026-08-30T09:00:00+00:00",
        )
        # proj-b has only done issues: the most recently updated survives.
        _insert_issue(
            connection,
            issue_id="INFRA-20",
            project_key="proj-b",
            priority=1,
            state="done",
            updated_at="2026-08-30T08:00:00+00:00",
        )
        _insert_issue(
            connection,
            issue_id="INFRA-21",
            project_key="proj-b",
            priority=1,
            state="done",
            updated_at="2026-08-30T12:00:00+00:00",
        )

    tasks = TaskProvider(database).tasks()

    assert {task.issue_id for task in tasks} == {"INFRA-10", "INFRA-21"}
    survivor = next(task for task in tasks if task.issue_id == "INFRA-21")
    assert survivor.operator_state == "Done"
    database.close()


def test_tasks_join_lead_children_pr_and_settlement(tmp_path: Path) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _insert_issue(
            connection,
            issue_id="INFRA-30",
            project_key="proj",
            priority=1,
            state="in_development",
            updated_at="2026-08-30T10:00:00+00:00",
        )
        _insert_cell(
            connection,
            cell_id="cell-old",
            project_key="proj",
            state="completed",
            profile_alias="max-z",
            session_id="stale-session",
        )
        _insert_cell(
            connection,
            cell_id="cell-active",
            project_key="proj",
            state="active",
            profile_alias="max-c",
            session_id="session-live",
        )
        _insert_child(
            connection,
            session_id="session-live",
            child_id="child-1",
            state="completed",
        )
        _insert_child(
            connection,
            session_id="session-live",
            child_id="child-2",
            state="started",
        )
        _insert_child(
            connection,
            session_id="stale-session",
            child_id="child-x",
            state="completed",
        )
        _insert_review(
            connection,
            review_id="rev-1",
            issue_id="INFRA-30",
            pr_number=35,
            state="corrections_required",
            created_at="2026-08-30T09:00:00+00:00",
        )
        _insert_review(
            connection,
            review_id="rev-2",
            issue_id="INFRA-30",
            pr_number=37,
            state="merged",
            created_at="2026-08-30T11:00:00+00:00",
        )
        _insert_settlement(
            connection,
            settlement_id="settle-1",
            issue_id="INFRA-30",
            state="settled",
            created_at="2026-08-30T11:30:00+00:00",
        )

    [task] = TaskProvider(database).tasks()

    assert task.lead_profile == "max-c"
    assert task.children_completed == 1
    assert task.children_total == 2
    # The latest review (by created_at) wins, not insertion order.
    assert task.pr_number == 37
    assert task.review_state == "merged"
    assert task.settlement_state == "settled"
    database.close()


def test_tasks_use_the_issue_assignment_instead_of_an_arbitrary_project_lane(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _insert_issue(
            connection,
            issue_id="INFRA-198",
            project_key="agent-orchestration",
            priority=1,
            state="post_merge_acceptance",
            updated_at="2026-09-02T02:00:00+00:00",
        )
        _insert_cell(
            connection,
            cell_id="cell-development",
            project_key="agent-orchestration",
            state="active",
            profile_alias="max-a",
            session_id="session-development",
            lane_role="development",
        )
        _insert_cell(
            connection,
            cell_id="cell-harness",
            project_key="agent-orchestration",
            state="active",
            profile_alias="max-d",
            session_id="session-harness",
            lane_role="harness",
        )
        _insert_assignment(
            connection,
            assignment_id="assignment-harness",
            project_key="agent-orchestration",
            issue_id="INFRA-198",
            cell_id="cell-harness",
            session_id="session-harness",
            state="acknowledged",
        )

    [task] = TaskProvider(database).tasks()

    assert task.lead_profile == "max-d"
    database.close()


def test_tasks_observed_at_is_the_latest_updated_at_or_none(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    assert TaskProvider(database).observed_at() is None

    with database.transaction() as connection:
        _insert_issue(
            connection,
            issue_id="INFRA-11",
            project_key="proj",
            priority=1,
            state="queued",
            updated_at="2026-08-30T10:00:00+00:00",
        )
        _insert_issue(
            connection,
            issue_id="INFRA-12",
            project_key="proj",
            priority=1,
            state="done",
            updated_at="2026-08-30T12:00:00+00:00",
        )

    assert TaskProvider(database).observed_at() == "2026-08-30T12:00:00+00:00"
    database.close()


def test_idle_notes_shows_no_queued_work_for_an_idle_live_project(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _insert_cell(
            connection,
            cell_id="cell-a",
            project_key="proj-a",
            state="active",
            profile_alias="max-a",
            session_id="s-1",
        )

    notes = TaskProvider(database).idle_notes(None)

    assert notes == (IdleFact(project_key="proj-a", hold="no queued work"),)
    database.close()


def test_idle_notes_omits_a_project_already_working(tmp_path: Path) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _insert_cell(
            connection,
            cell_id="cell-a",
            project_key="proj-a",
            state="active",
            profile_alias="max-a",
            session_id="s-1",
        )
        _insert_issue(
            connection,
            issue_id="INFRA-1",
            project_key="proj-a",
            priority=1,
            state="in_development",
            updated_at="2026-08-30T10:00:00+00:00",
        )

    assert TaskProvider(database).idle_notes(None) == ()
    database.close()


@pytest.mark.parametrize(
    "priority, dependency_ready, decision_pending, resource, expected_hold",
    [
        (1, 0, False, None, "dependency-blocked"),
        (1, 1, True, None, "operator decision pending"),
        (
            2,
            1,
            False,
            ResourceFact(
                sampled_at="2026-08-30T12:00:00+00:00",
                pressure="yellow",
                available_memory_bytes=1,
                total_memory_bytes=2,
                swap_used_bytes=0,
                load_one=0.1,
                logical_cpus=1,
                managed_rss_bytes=1,
                min_disk_free_bytes=None,
            ),
            "capacity limited",
        ),
    ],
    ids=["dependency-blocked", "operator-decision", "capacity-limited"],
)
def test_idle_notes_reports_the_top_lanes_concrete_hold(
    tmp_path: Path,
    priority: int,
    dependency_ready: int,
    decision_pending: bool,
    resource: ResourceFact | None,
    expected_hold: str,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _insert_cell(
            connection,
            cell_id="cell-a",
            project_key="proj-a",
            state="active",
            profile_alias="max-a",
            session_id="s-1",
        )
        _insert_issue(
            connection,
            issue_id="INFRA-1",
            project_key="proj-a",
            priority=priority,
            state="queued",
            updated_at="2026-08-30T10:00:00+00:00",
            dependency_ready=dependency_ready,
        )
        if decision_pending:
            connection.execute(
                "INSERT INTO operator_decisions("
                "decision_id, issue_id, project_key, cell_id, session_id, "
                "actor, choice, status, recorded_at"
                ") VALUES ('dec-1', 'INFRA-1', 'proj-a', 'cell-a', 's-1', "
                "'operator', 'hold', 'pending', '2026-08-30T10:00:00+00:00')"
            )

    [note] = TaskProvider(database).idle_notes(resource)

    assert note == IdleFact(project_key="proj-a", hold=expected_hold)
    database.close()


# ---------------------------------------------------------------------------
# INFRA-219 R4 (Sol correction 110ed759): TaskProvider.lane_cells attribution.
#
# The prior implementation resolved one "current issue" per PROJECT and
# copied it onto every lane row -- these tests pin the corrected,
# cell-scoped (via lead_assignments) behavior: a harness row never
# inherits a development issue, and a development row can carry more
# than one concurrently assigned issue.
# ---------------------------------------------------------------------------


def _lane_setup(connection) -> None:
    _insert_cell(
        connection,
        cell_id="cell-dev",
        project_key="proj",
        state="active",
        profile_alias="max-a",
        session_id="sess-dev",
        lane_role="development",
    )
    _insert_cell(
        connection,
        cell_id="cell-harness",
        project_key="proj",
        state="active",
        profile_alias="max-b",
        session_id="sess-harness",
        lane_role="harness",
    )


def test_lane_cells_never_attributes_a_development_issue_to_the_harness_row(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _lane_setup(connection)
        _insert_issue(
            connection,
            issue_id="INFRA-1",
            project_key="proj",
            priority=1,
            state="in_development",
            updated_at="2026-08-30T10:00:00+00:00",
        )
        _insert_assignment(
            connection,
            assignment_id="a-1",
            project_key="proj",
            issue_id="INFRA-1",
            cell_id="cell-dev",
            session_id="sess-dev",
        )

    lanes = {lane.lane_role: lane for lane in TaskProvider(database).lane_cells()}

    assert lanes["development"].issue_ids == ("INFRA-1",)
    assert lanes["harness"].issue_ids == ()
    database.close()


def test_lane_cells_reports_multiple_concurrent_development_issues(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _lane_setup(connection)
        for issue_id in ("INFRA-1", "INFRA-2", "INFRA-3"):
            _insert_issue(
                connection,
                issue_id=issue_id,
                project_key="proj",
                priority=1,
                state="in_development",
                updated_at="2026-08-30T10:00:00+00:00",
            )
            _insert_assignment(
                connection,
                assignment_id=f"a-{issue_id}",
                project_key="proj",
                issue_id=issue_id,
                cell_id="cell-dev",
                session_id="sess-dev",
            )

    lanes = {lane.lane_role: lane for lane in TaskProvider(database).lane_cells()}

    assert set(lanes["development"].issue_ids) == {"INFRA-1", "INFRA-2", "INFRA-3"}
    assert lanes["harness"].issue_ids == ()
    database.close()


def test_lane_cells_reports_this_lanes_own_blockers_only(tmp_path: Path) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _lane_setup(connection)
        _insert_issue(
            connection,
            issue_id="INFRA-4",
            project_key="proj",
            priority=1,
            state="blocked",
            updated_at="2026-08-30T10:00:00+00:00",
        )
        _insert_assignment(
            connection,
            assignment_id="a-4",
            project_key="proj",
            issue_id="INFRA-4",
            cell_id="cell-dev",
            session_id="sess-dev",
        )

    lanes = {lane.lane_role: lane for lane in TaskProvider(database).lane_cells()}

    assert lanes["development"].blocked_issue_ids == ("INFRA-4",)
    assert lanes["harness"].blocked_issue_ids == ()


def test_lane_cells_ignores_superseded_assignments(tmp_path: Path) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _lane_setup(connection)
        _insert_issue(
            connection,
            issue_id="INFRA-5",
            project_key="proj",
            priority=1,
            state="in_development",
            updated_at="2026-08-30T10:00:00+00:00",
        )
        _insert_assignment(
            connection,
            assignment_id="a-5",
            project_key="proj",
            issue_id="INFRA-5",
            cell_id="cell-dev",
            session_id="sess-dev",
            state="superseded",
        )

    lanes = {lane.lane_role: lane for lane in TaskProvider(database).lane_cells()}

    assert lanes["development"].issue_ids == ()
    database.close()


def test_lane_cells_reports_subagents_from_its_own_session_only(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _lane_setup(connection)
        _insert_child(
            connection, session_id="sess-dev", child_id="c-1",
            state="completed",
        )
        _insert_child(
            connection, session_id="sess-dev", child_id="c-2",
            state="started",
        )

    lanes = {lane.lane_role: lane for lane in TaskProvider(database).lane_cells()}

    development = lanes["development"]
    assert (development.subagents_completed, development.subagents_total) == (
        1,
        2,
    )
    harness = lanes["harness"]
    assert (harness.subagents_completed, harness.subagents_total) == (0, 0)
    database.close()


def test_capacity_provider_returns_latest_per_alias_and_none_when_absent(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO profile_capacity_observations("
            "profile_alias, model, state, source, observed_at, resets_at"
            ") VALUES (?, 'fable', 'capped', 'provider_limit', ?, ?)",
            ("max-a", "2026-08-30T06:00:00+00:00", "2026-08-31T00:00:00+00:00"),
        )
        connection.execute(
            "INSERT INTO profile_capacity_observations("
            "profile_alias, model, state, source, observed_at, resets_at"
            ") VALUES (?, 'fable', 'available', 'operator_attestation', ?, NULL)",
            ("max-a", "2026-08-30T09:00:00+00:00"),
        )
        # A non-fable observation must never shadow the fable row.
        connection.execute(
            "INSERT INTO profile_capacity_observations("
            "profile_alias, model, state, source, observed_at, resets_at"
            ") VALUES (?, 'other-model', 'capped', 'provider_limit', ?, NULL)",
            ("max-a", "2026-08-30T10:00:00+00:00"),
        )

    capacity = CapacityProvider(database, _registry(tmp_path)).capacity()

    assert tuple(fact.profile_alias for fact in capacity) == _ALIASES
    max_a = capacity[0]
    assert max_a.state == "available"
    assert max_a.source == "operator_attestation"
    assert max_a.observed_at == "2026-08-30T09:00:00+00:00"
    assert max_a.resets_at is None

    max_b = capacity[1]
    assert max_b.state is None
    assert max_b.source is None
    assert max_b.observed_at is None
    assert max_b.resets_at is None
    database.close()


def test_capacity_provider_uses_observation_time_not_insertion_order(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO profile_capacity_observations("
            "profile_alias, model, state, source, observed_at, resets_at"
            ") VALUES ('max-a', 'fable', 'capped', 'provider_limit', "
            "'2026-09-02T02:47:00+00:00', '2026-09-02T03:47:00+00:00')"
        )
        # A delayed write carrying older evidence must not replace the
        # fresher provider observation merely because its row id is newer.
        connection.execute(
            "INSERT INTO profile_capacity_observations("
            "profile_alias, model, state, source, observed_at, resets_at"
            ") VALUES ('max-a', 'fable', 'available', 'operator_attestation', "
            "'2026-09-01T03:44:00+00:00', NULL)"
        )

    [max_a, *_] = CapacityProvider(database, _registry(tmp_path)).capacity()

    assert max_a.state == "capped"
    assert max_a.resets_at == "2026-09-02T03:47:00+00:00"
    database.close()


def test_resource_provider_returns_the_latest_sample_and_min_disk_free(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    assert ResourceProvider(database).resource() is None

    with database.transaction() as connection:
        for sample_id, sampled_at, disk_json in (
            ("sample-1", "2026-08-30T10:00:00+00:00", "{}"),
            (
                "sample-2",
                "2026-08-30T11:00:00+00:00",
                '{"root": 500, "data": 137}',
            ),
        ):
            connection.execute(
                "INSERT INTO resource_samples("
                "sample_id, sampled_at, pressure, available_memory_bytes, "
                "total_memory_bytes, swap_used_bytes, load_one, "
                "logical_cpus, disk_json, managed_rss_bytes"
                ") VALUES (?, ?, 'green', 1000, 2000, 0, 0.5, 8, ?, 500)",
                (sample_id, sampled_at, disk_json),
            )

    resource = ResourceProvider(database).resource()

    assert resource is not None
    assert resource.sampled_at == "2026-08-30T11:00:00+00:00"
    assert resource.pressure == "green"
    assert resource.available_memory_bytes == 1000
    assert resource.total_memory_bytes == 2000
    assert resource.logical_cpus == 8
    assert resource.managed_rss_bytes == 500
    assert resource.min_disk_free_bytes == 137
    database.close()


def test_resource_provider_none_disk_free_when_disk_json_is_empty(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO resource_samples("
            "sample_id, sampled_at, pressure, available_memory_bytes, "
            "total_memory_bytes, swap_used_bytes, load_one, "
            "logical_cpus, disk_json, managed_rss_bytes"
            ") VALUES ('s', '2026-08-30T10:00:00+00:00', 'green', 1, 2, "
            "0, 0.1, 1, '{}', 1)",
        )

    resource = ResourceProvider(database).resource()

    assert resource is not None
    assert resource.min_disk_free_bytes is None
    database.close()


def test_worker_provider_counts_active_leases_by_kind(tmp_path: Path) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        for lease_id, kind, state in (
            ("w-1", "claude_subagent", "active"),
            ("w-2", "claude_subagent", "active"),
            ("w-3", "resource_sampler", "active"),
            ("w-4", "claude_subagent", "expired"),
        ):
            connection.execute(
                "INSERT INTO worker_leases("
                "lease_id, worker_id, project_key, kind, state, acquired_at"
                ") VALUES (?, ?, 'proj', ?, ?, '2026-08-30T00:00:00+00:00')",
                (lease_id, lease_id, kind, state),
            )

    workers = WorkerProvider(database).workers()

    assert workers.active_total == 3
    assert dict(workers.active_by_kind) == {
        "claude_subagent": 2,
        "resource_sampler": 1,
    }
    database.close()


def test_transition_provider_tracks_the_latest_whitelisted_event_per_project(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    events = EventStore(database)
    provider = TransitionProvider(database)

    def _append(event_type: str, payload: dict, aggregate_id: str = "x") -> None:
        with database.transaction() as connection:
            events.append(
                connection,
                EventInput(
                    event_type=event_type,
                    aggregate_type="project_cell",
                    aggregate_id=aggregate_id,
                    payload=payload,
                ),
            )

    _append(
        "review.recorded",
        {"project_key": "proj-a", "state": "approved"},
    )
    _append(
        "project_cell.rotated",
        {
            "project_key": "proj-b",
            "previous_profile_alias": "max-b",
            "profile_alias": "max-c",
        },
    )
    _append(
        "lead_correction.queued",
        {"project_key": "proj-a", "reason": "Important"},
    )
    # Not on the whitelist: must never surface as a transition.
    _append("stream.assistant", {"project_key": "proj-a"})
    # A control op event of an unlisted kind must be ignored too.
    _append(
        "control_operation.published",
        {"project_key": "proj-c", "kind": "housekeeping.noop"},
    )

    provider.advance()
    transitions = {t.project_key: t.phrase for t in provider.transitions()}

    assert transitions["proj-a"] == "correction queued (Important)"
    assert transitions["proj-b"] == "rotated max-b→max-c"
    assert "proj-c" not in transitions

    # A second advance with no new rows must not change anything.
    provider.advance()
    assert {t.project_key: t.phrase for t in provider.transitions()} == transitions
    database.close()


def test_control_prompts_are_attention_only_while_unconfirmed(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    events = EventStore(database)
    provider = TransitionProvider(database)
    operations = ControlOperations(database, events=events, ids=lambda: "op-1")
    operation = operations.record(
        kind="channel.approval_required",
        project_key="proj-a",
        cell_id="cell-1",
        session_id="session-1",
        result={},
    )
    assert operation is not None

    provider.advance()
    assert provider.transitions() == ()
    assert ControlAttentionProvider(database).latest() is not None

    assert operations.acknowledge("op-1", session_id="session-1") is True

    provider.advance()
    assert provider.transitions() == ()
    assert ControlAttentionProvider(database).latest() is None
    database.close()


def test_control_attention_provider_finds_the_latest_whitelisted_published_op(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO control_operations("
            "operation_id, schema_version, kind, project_key, cell_id, "
            "session_id, dedup_key, result_json, state, created_at, "
            "updated_at"
            ") VALUES ('op-1', 1, 'lead.launch_failed', 'proj-a', 'cell-1', "
            "'sess-1', 'dedup-1', '{}', 'published', "
            "'2026-08-30T09:00:00+00:00', '2026-08-30T09:00:00+00:00')",
        )
        connection.execute(
            "INSERT INTO control_operations("
            "operation_id, schema_version, kind, project_key, cell_id, "
            "session_id, dedup_key, result_json, state, created_at, "
            "updated_at"
            ") VALUES ('op-2', 1, 'channel.approval_required', 'proj-b', "
            "'cell-2', 'sess-2', 'dedup-2', '{}', 'published', "
            "'2026-08-30T10:00:00+00:00', '2026-08-30T10:00:00+00:00')",
        )
        # A superseded row and an unlisted kind must never win.
        connection.execute(
            "INSERT INTO control_operations("
            "operation_id, schema_version, kind, project_key, cell_id, "
            "session_id, dedup_key, result_json, state, created_at, "
            "updated_at"
            ") VALUES ('op-3', 1, 'channel.approval_required', 'proj-c', "
            "'cell-3', 'sess-3', 'dedup-3', '{}', 'superseded', "
            "'2026-08-30T11:00:00+00:00', '2026-08-30T11:00:00+00:00')",
        )

    fact = ControlAttentionProvider(database).latest()

    assert fact is not None
    assert fact.project_key == "proj-b"
    assert fact.kind == "channel.approval_required"
    database.close()


def test_control_attention_ignores_a_replaced_sessions_unresolved_prompt(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _insert_cell(
            connection,
            cell_id="cell-current",
            project_key="proj-a",
            state="active",
            profile_alias="max-b",
            session_id="session-current",
        )
        connection.execute(
            "INSERT INTO control_operations("
            "operation_id, schema_version, kind, project_key, cell_id, "
            "session_id, dedup_key, result_json, state, created_at, updated_at"
            ") VALUES ('op-stale', 1, 'channel.approval_required', 'proj-a', "
            "'cell-old', 'session-old', 'dedup-old', '{}', 'published', "
            "'2026-09-02T02:00:00+00:00', '2026-09-02T02:00:00+00:00')"
        )

    assert ControlAttentionProvider(database).latest() is None
    database.close()


def test_control_attention_ignores_channel_block_resolved_by_active_binding(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _insert_cell(
            connection,
            cell_id="cell-current",
            project_key="proj-a",
            state="active",
            profile_alias="max-b",
            session_id="session-current",
        )
        connection.execute(
            "INSERT INTO cmux_surface_bindings("
            "binding_id, role, project_key, cell_id, session_id, "
            "profile_alias, workspace_uuid, surface_uuid, generation, "
            "state, created_at, updated_at, lane_role) VALUES ("
            "'binding-1', 'lead', 'proj-a', 'cell-current', "
            "'session-current', 'max-b', 'workspace-1', 'surface-1', 1, "
            "'active', '2026-09-02T03:00:00+00:00', "
            "'2026-09-02T03:00:00+00:00', 'development')"
        )
        connection.execute(
            "INSERT INTO control_operations("
            "operation_id, schema_version, kind, project_key, cell_id, "
            "session_id, dedup_key, result_json, state, created_at, updated_at"
            ") VALUES ('op-blocked', 1, 'channel.blocked', 'proj-a', "
            "'cell-current', 'session-current', 'dedup-blocked', "
            "'{\"refusal\":\"no active seat binding for this cell\"}', "
            "'published', '2026-09-02T02:00:00+00:00', "
            "'2026-09-02T02:00:00+00:00')"
        )

    assert ControlAttentionProvider(database).latest() is None
    database.close()


@pytest.mark.asyncio
async def test_collect_fills_work_capacity_system_and_attention_facts(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    with database.transaction() as connection:
        _insert_issue(
            connection,
            issue_id="INFRA-20",
            project_key="proj",
            priority=1,
            state="in_development",
            updated_at="2026-08-30T10:00:00+00:00",
        )
        connection.execute(
            "INSERT INTO profile_capacity_observations("
            "profile_alias, model, state, source, observed_at, resets_at"
            ") VALUES ('max-a', 'fable', 'available', "
            "'operator_attestation', ?, NULL)",
            ("2026-08-30T09:00:00+00:00",),
        )
        connection.execute(
            "INSERT INTO resource_samples("
            "sample_id, sampled_at, pressure, available_memory_bytes, "
            "total_memory_bytes, swap_used_bytes, load_one, "
            "logical_cpus, disk_json, managed_rss_bytes"
            ") VALUES ('sample-1', ?, 'green', 1000, 2000, 0, 0.5, 8, "
            "'{}', 500)",
            ("2026-08-30T11:30:00+00:00",),
        )
        connection.execute(
            "INSERT INTO worker_leases("
            "lease_id, worker_id, project_key, kind, state, acquired_at"
            ") VALUES ('w-1', 'w-1', 'proj', 'claude_subagent', 'active', "
            "'2026-08-30T00:00:00+00:00')",
        )

    sources = DashboardSources(database=database, registry=_registry(tmp_path))
    now = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    snapshot = await sources.collect(now)

    assert len(snapshot.tasks) == 1
    assert snapshot.tasks[0].issue_id == "INFRA-20"
    assert snapshot.tasks_observed_at == "2026-08-30T10:00:00+00:00"
    assert len(snapshot.capacity) == len(_ALIASES)
    assert snapshot.capacity[0].state == "available"
    assert snapshot.resource is not None
    assert snapshot.resource.sampled_at == "2026-08-30T11:30:00+00:00"
    assert snapshot.workers.active_total == 1
    assert snapshot.transitions == ()
    assert snapshot.attention_control is None
    database.close()


# ---------------------------------------------------------------------------
# INFRA-209 addendum: the local Claude usage-percentage cache.
# ---------------------------------------------------------------------------

_FABLE_LIMIT = {
    "kind": "weekly_scoped",
    "scope": {"model": {"display_name": "Fable"}},
    "percent": 12,
    "resets_at": "2026-09-01T00:00:00+00:00",
    "severity": "normal",
    "is_active": True,
}

_CACHE_DOCUMENT = {
    "cachedUsageUtilization": {
        "fetchedAtMs": 1798700400000,  # 2026-12-31T05:00:00Z-ish; any int works
        "utilization": {
            "five_hour": {
                "utilization": 41,
                "resets_at": "2026-08-30T14:00:00+00:00",
            },
            "seven_day": {
                "utilization": 63,
                "resets_at": "2026-09-01T00:00:00+00:00",
            },
            "limits": [
                {
                    "kind": "weekly_scoped",
                    "scope": {"model": {"display_name": "Codex"}},
                    "percent": 90,
                },
                _FABLE_LIMIT,
            ],
        },
    }
}


def _fake_reader(documents: dict):
    import json as _json

    def _read(path) -> str:
        key = str(path)
        if key not in documents:
            raise FileNotFoundError(key)
        return _json.dumps(documents[key])

    return _read


def test_claude_usage_cache_parses_five_hour_seven_day_and_fable_window(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    path = str(registry.get("max-a").config_dir / ".claude.json")
    provider = ClaudeUsageCacheProvider(
        registry, read_text=_fake_reader({path: _CACHE_DOCUMENT})
    )

    windows = dict(provider.windows())

    max_a = windows["max-a"]
    assert max_a.five_hour_used == 41
    assert max_a.five_hour_resets_at == "2026-08-30T14:00:00+00:00"
    assert max_a.seven_day_used == 63
    assert max_a.fable_used == 12
    assert max_a.fable_resets_at == "2026-09-01T00:00:00+00:00"
    assert max_a.fable_severity == "normal"
    assert max_a.fable_active is True
    assert max_a.fetched_at is not None

    # No file at all for the other aliases.
    assert windows["max-b"].fetched_at is None
    assert windows["max-b"].fable_used is None


def test_claude_usage_cache_absent_or_malformed_is_never_a_raise(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)

    def _raising(_path) -> str:
        raise PermissionError("no access")

    windows = dict(ClaudeUsageCacheProvider(registry, read_text=_raising).windows())
    assert windows["max-a"].fetched_at is None

    def _malformed(_path) -> str:
        return "{not valid json"

    windows = dict(ClaudeUsageCacheProvider(registry, read_text=_malformed).windows())
    assert windows["max-a"].fetched_at is None

    def _missing_key(_path) -> str:
        return "{}"

    windows = dict(
        ClaudeUsageCacheProvider(registry, read_text=_missing_key).windows()
    )
    assert windows["max-a"].fetched_at is None


def test_capacity_provider_merges_windows_by_alias(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    from hermes_orchestrator.dashboard_sources import _EMPTY_WINDOWS, UsageWindows

    windows_by_alias = {
        "max-a": UsageWindows(
            fetched_at="2026-08-30T11:00:00+00:00",
            five_hour_used=10,
            five_hour_resets_at=None,
            seven_day_used=20,
            seven_day_resets_at=None,
            fable_used=30,
            fable_resets_at=None,
            fable_severity="normal",
            fable_active=True,
        ),
    }
    facts = CapacityProvider(_database(tmp_path), registry).capacity(
        windows_by_alias
    )
    assert facts[0].windows.fable_used == 30
    assert facts[1].windows == _EMPTY_WINDOWS


@pytest.mark.asyncio
async def test_cache_change_between_ticks_updates_only_that_profile(
    tmp_path: Path,
) -> None:
    # Acceptance (b): the same action/sources instance re-reads the cache
    # every tick, and a change to one profile's file must never disturb
    # any other profile's row.
    registry = _registry(tmp_path)
    documents: dict[str, dict] = {}
    path_a = str(registry.get("max-a").config_dir / ".claude.json")
    path_b = str(registry.get("max-b").config_dir / ".claude.json")
    documents[path_a] = _CACHE_DOCUMENT

    def _read(path) -> str:
        import json as _json

        key = str(path)
        if key not in documents:
            raise FileNotFoundError(key)
        return _json.dumps(documents[key])

    database = _database(tmp_path)
    sources = DashboardSources(
        database=database, registry=registry, claude_usage_read_text=_read
    )
    now = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)

    first = await sources.collect(now)
    first_by_alias = {fact.profile_alias: fact for fact in first.capacity}
    assert first_by_alias["max-a"].windows.fable_used == 12
    assert first_by_alias["max-b"].windows.fetched_at is None

    documents[path_b] = {
        "cachedUsageUtilization": {
            "fetchedAtMs": 1798700400000,
            "utilization": {
                "five_hour": {"utilization": 5, "resets_at": None},
                "seven_day": {"utilization": 9, "resets_at": None},
                "limits": [
                    {
                        "kind": "weekly_scoped",
                        "scope": {"model": {"display_name": "Fable"}},
                        "percent": 3,
                        "resets_at": None,
                        "severity": "normal",
                        "is_active": True,
                    }
                ],
            },
        }
    }
    second = await sources.collect(now)
    second_by_alias = {fact.profile_alias: fact for fact in second.capacity}

    # max-b now has fresh windows; max-a's are unchanged.
    assert second_by_alias["max-b"].windows.fable_used == 3
    assert second_by_alias["max-a"].windows.fable_used == 12
    database.close()
