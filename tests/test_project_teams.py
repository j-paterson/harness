"""Durable Fable+Sol pair coordinator tests (INFRA-187)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.project_teams import (
    SOL_MODEL,
    SOL_PROVIDER,
    ProjectTeam,
    ProjectTeamService,
    RetirementEvidenceRequired,
    SolModelMismatch,
    StaleTeamMember,
    TeamAdmissionRefused,
    TeamMemberMismatch,
    TeamUncertain,
)

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)
NOW_ISO = NOW.isoformat()


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def teams(database: Database) -> ProjectTeamService:
    return ProjectTeamService(database, now=lambda: NOW)


def seed_cell(
    database: Database,
    *,
    cell_id: str,
    project_key: str,
    state: str = "active",
    lane_role: str = "development",
    session_id: str = "sess-fable-1",
    profile_alias: str = "max-c",
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at, lane_role) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                cell_id,
                project_key,
                state,
                profile_alias,
                session_id,
                NOW_ISO,
                NOW_ISO,
                lane_role,
            ),
        )


def seed_channel(
    database: Database,
    *,
    project_key: str,
    thread_id: str,
    generation: int,
    state: str = "ready",
    integration_branch: str = "main",
    model: str | None = None,
    provider: str | None = None,
    model_verified_at: str | None = None,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO reviewer_channels("
            "project_key, thread_id, generation, state, integration_branch, "
            "model, provider, model_verified_at, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(project_key) DO UPDATE SET "
            "thread_id = excluded.thread_id, "
            "generation = excluded.generation, "
            "state = excluded.state, "
            "integration_branch = excluded.integration_branch, "
            "model = excluded.model, "
            "provider = excluded.provider, "
            "model_verified_at = excluded.model_verified_at, "
            "updated_at = excluded.updated_at",
            (
                project_key,
                thread_id,
                generation,
                state,
                integration_branch,
                model,
                provider,
                model_verified_at,
                NOW_ISO,
                NOW_ISO,
            ),
        )


def bind_ready(
    teams: ProjectTeamService,
    database: Database,
    project_key: str = "demo",
    *,
    cell_id: str = "cell-1",
    thread_id: str = "thread-1",
    sol_generation: int = 1,
) -> ProjectTeam:
    seed_cell(database, cell_id=cell_id, project_key=project_key)
    seed_channel(
        database,
        project_key=project_key,
        thread_id=thread_id,
        generation=sol_generation,
        model=SOL_MODEL,
        provider=SOL_PROVIDER,
        model_verified_at=NOW_ISO,
    )
    team = teams.reserve(project_key, repo_path="/repo/demo", integration_branch="main")
    team = teams.bind_fable(
        project_key,
        expected_generation=team.generation,
        cell_id=cell_id,
        session_id="sess-fable-1",
        profile_alias="max-c",
    )
    return teams.bind_sol(
        project_key,
        expected_generation=team.generation,
        thread_id=thread_id,
        sol_generation=sol_generation,
        model=SOL_MODEL,
        provider=SOL_PROVIDER,
    )


class _Uncertain(RuntimeError):
    """A Sol-creation outcome ambiguous enough to require reconciliation."""


def test_duplicate_activation_converges(
    teams: ProjectTeamService, database: Database
) -> None:
    first = teams.reserve("demo", repo_path="/repo/demo", integration_branch="main")
    again = teams.reserve("demo", repo_path="/repo/demo", integration_branch="main")

    assert first == again
    assert database.scalar("SELECT COUNT(*) FROM project_teams") == 1


@pytest.mark.asyncio
async def test_failure_after_fable_creation_resumes_sol_without_second_ensure_fable(
    teams: ProjectTeamService, database: Database
) -> None:
    seed_cell(database, cell_id="cell-1", project_key="demo")
    seed_channel(
        database,
        project_key="demo",
        thread_id="thread-1",
        generation=1,
        model=SOL_MODEL,
        provider=SOL_PROVIDER,
        model_verified_at=NOW_ISO,
    )
    fable_calls = 0

    def ensure_fable() -> tuple[str, str, str]:
        nonlocal fable_calls
        fable_calls += 1
        return "cell-1", "sess-fable-1", "max-c"

    async def ensure_sol_fails() -> tuple[str, int, str, str]:
        raise RuntimeError("transient rpc timeout")

    with pytest.raises(RuntimeError, match="transient rpc timeout"):
        await teams.activate(
            "demo",
            repo_path="/repo/demo",
            integration_branch="main",
            ensure_fable=ensure_fable,
            ensure_sol=ensure_sol_fails,
            can_admit=lambda: True,
        )

    assert fable_calls == 1
    mid = teams.live_team("demo")
    assert mid is not None
    assert mid.state == "fable_bound"
    assert mid.fable_cell_id == "cell-1"

    async def ensure_sol_succeeds() -> tuple[str, int, str, str]:
        return "thread-1", 1, SOL_MODEL, SOL_PROVIDER

    ready = await teams.activate(
        "demo",
        repo_path="/repo/demo",
        integration_branch="main",
        ensure_fable=ensure_fable,
        ensure_sol=ensure_sol_succeeds,
        can_admit=lambda: True,
    )

    assert fable_calls == 1
    assert ready.state == "ready"
    assert ready.sol_thread_id == "thread-1"


@pytest.mark.asyncio
async def test_ambiguous_sol_creation_marks_uncertain_and_closes_admission(
    teams: ProjectTeamService, database: Database
) -> None:
    seed_cell(database, cell_id="cell-1", project_key="demo")

    async def ensure_sol_ambiguous() -> tuple[str, int, str, str]:
        raise _Uncertain("codex rpc outcome unknown")

    with pytest.raises(_Uncertain):
        await teams.activate(
            "demo",
            repo_path="/repo/demo",
            integration_branch="main",
            ensure_fable=lambda: ("cell-1", "sess-fable-1", "max-c"),
            ensure_sol=ensure_sol_ambiguous,
            can_admit=lambda: True,
        )

    team = teams.live_team("demo")
    assert team is not None
    assert team.state == "uncertain"
    assert "demo" not in teams.ready_projects()


def test_non_sol_model_refuses_with_no_write(
    teams: ProjectTeamService, database: Database
) -> None:
    seed_cell(database, cell_id="cell-1", project_key="demo")
    team = teams.reserve("demo", repo_path="/repo/demo", integration_branch="main")
    team = teams.bind_fable(
        "demo",
        expected_generation=team.generation,
        cell_id="cell-1",
        session_id="sess-fable-1",
        profile_alias="max-c",
    )
    seed_channel(database, project_key="demo", thread_id="thread-1", generation=1)

    with pytest.raises(SolModelMismatch):
        teams.bind_sol(
            "demo",
            expected_generation=team.generation,
            thread_id="thread-1",
            sol_generation=1,
            model="gpt-4-turbo",
            provider=SOL_PROVIDER,
        )
    with pytest.raises(SolModelMismatch):
        teams.bind_sol(
            "demo",
            expected_generation=team.generation,
            thread_id="thread-1",
            sol_generation=1,
            model=SOL_MODEL,
            provider="anthropic",
        )

    unchanged = teams.live_team("demo")
    assert unchanged is not None
    assert unchanged.state == "fable_bound"
    assert unchanged.sol_thread_id is None
    assert unchanged.updated_at == team.updated_at


def test_resolve_with_stale_fable_generation_refuses(
    teams: ProjectTeamService, database: Database
) -> None:
    ready = bind_ready(teams, database)

    with pytest.raises(StaleTeamMember):
        teams.resolve(
            "demo",
            fable_cell_id=ready.fable_cell_id,
            fable_generation=ready.fable_generation + 1,
        )

    assert teams.resolve("demo", fable_generation=ready.fable_generation) == ready


def test_resolve_with_stale_sol_generation_refuses(
    teams: ProjectTeamService, database: Database
) -> None:
    ready = bind_ready(teams, database)

    with pytest.raises(StaleTeamMember):
        teams.resolve(
            "demo",
            sol_thread_id=ready.sol_thread_id,
            sol_generation=(ready.sol_generation or 0) + 1,
        )

    assert teams.resolve("demo", sol_generation=ready.sol_generation) == ready


def test_rotate_fable_preserves_sol_member(
    teams: ProjectTeamService, database: Database
) -> None:
    ready = bind_ready(teams, database)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE project_cells SET state = 'released' WHERE cell_id = ?",
            ("cell-1",),
        )
    seed_cell(
        database,
        cell_id="cell-2",
        project_key="demo",
        session_id="sess-fable-2",
    )

    rotated = teams.rotate_fable(
        "demo",
        expected_generation=ready.generation,
        cell_id="cell-2",
        session_id="sess-fable-2",
        profile_alias="max-c",
    )

    assert rotated.generation == ready.generation + 1
    assert rotated.state == "ready"
    assert rotated.fable_cell_id == "cell-2"
    assert rotated.fable_session_id == "sess-fable-2"
    assert rotated.fable_generation == ready.fable_generation + 1
    assert rotated.sol_thread_id == ready.sol_thread_id
    assert rotated.sol_generation == ready.sol_generation
    assert rotated.sol_model == ready.sol_model
    assert rotated.sol_provider == ready.sol_provider

    old_row = database.execute(
        "SELECT state FROM project_teams WHERE project_key = ? AND generation = ?",
        ("demo", ready.generation),
    ).fetchone()
    assert str(old_row["state"]) == "superseded"


def test_replace_sol_preserves_fable_member(
    teams: ProjectTeamService, database: Database
) -> None:
    ready = bind_ready(teams, database)
    seed_channel(
        database,
        project_key="demo",
        thread_id="thread-2",
        generation=2,
        model=SOL_MODEL,
        provider=SOL_PROVIDER,
        model_verified_at=NOW_ISO,
    )

    replaced = teams.replace_sol(
        "demo",
        expected_generation=ready.generation,
        thread_id="thread-2",
        sol_generation=2,
        model=SOL_MODEL,
        provider=SOL_PROVIDER,
    )

    assert replaced.generation == ready.generation + 1
    assert replaced.state == "ready"
    assert replaced.sol_thread_id == "thread-2"
    assert replaced.sol_generation == 2
    assert replaced.fable_cell_id == ready.fable_cell_id
    assert replaced.fable_session_id == ready.fable_session_id
    assert replaced.fable_generation == ready.fable_generation

    old_row = database.execute(
        "SELECT state FROM project_teams WHERE project_key = ? AND generation = ?",
        ("demo", ready.generation),
    ).fetchone()
    assert str(old_row["state"]) == "superseded"


def test_cross_project_cell_and_thread_ids_refuse(
    teams: ProjectTeamService, database: Database
) -> None:
    seed_cell(database, cell_id="cell-other", project_key="other")
    seed_channel(database, project_key="other", thread_id="thread-other", generation=1)
    team = teams.reserve("demo", repo_path="/repo/demo", integration_branch="main")

    with pytest.raises(TeamMemberMismatch):
        teams.bind_fable(
            "demo",
            expected_generation=team.generation,
            cell_id="cell-other",
            session_id="sess-fable-1",
            profile_alias="max-c",
        )
    with pytest.raises(TeamMemberMismatch):
        teams.bind_sol(
            "demo",
            expected_generation=team.generation,
            thread_id="thread-other",
            sol_generation=1,
            model=SOL_MODEL,
            provider=SOL_PROVIDER,
        )

    unchanged = teams.live_team("demo")
    assert unchanged is not None
    assert unchanged.state == "reserved"
    assert unchanged.fable_cell_id is None
    assert unchanged.sol_thread_id is None


def test_retirement_without_evidence_refuses_and_with_evidence_retires(
    teams: ProjectTeamService, database: Database
) -> None:
    ready = bind_ready(teams, database)

    with pytest.raises(RetirementEvidenceRequired):
        teams.retire(
            "demo",
            expected_generation=ready.generation,
            merge_checkpoint_sha="",
            cleanup_evidence="worktree removed",
        )
    with pytest.raises(RetirementEvidenceRequired):
        teams.retire(
            "demo",
            expected_generation=ready.generation,
            merge_checkpoint_sha="abc123",
            cleanup_evidence="   ",
        )

    still_ready = teams.live_team("demo")
    assert still_ready is not None
    assert still_ready.state == "ready"

    retired = teams.retire(
        "demo",
        expected_generation=ready.generation,
        merge_checkpoint_sha="abc123",
        cleanup_evidence="worktree removed",
    )

    assert retired.state == "retired"
    assert retired.retired_at == NOW_ISO
    assert teams.live_team("demo") is None


@pytest.mark.asyncio
async def test_can_admit_false_refuses_activation(
    teams: ProjectTeamService, database: Database
) -> None:
    with pytest.raises(TeamAdmissionRefused):
        await teams.activate(
            "demo",
            repo_path="/repo/demo",
            integration_branch="main",
            ensure_fable=lambda: ("cell-1", "sess-fable-1", "max-c"),
            ensure_sol=_unreachable_ensure_sol,
            can_admit=lambda: False,
        )

    assert teams.live_team("demo") is None
    assert database.scalar("SELECT COUNT(*) FROM project_teams") == 0


async def _unreachable_ensure_sol() -> tuple[str, int, str, str]:
    raise AssertionError("ensure_sol must not run when admission is refused")


def test_ready_projects_reports_only_ready_teams(
    teams: ProjectTeamService, database: Database
) -> None:
    bind_ready(
        teams,
        database,
        project_key="alpha",
        cell_id="cell-alpha",
        thread_id="thread-alpha",
    )

    seed_cell(
        database,
        cell_id="cell-beta",
        project_key="beta",
        session_id="sess-fable-beta",
    )
    beta = teams.reserve("beta", repo_path="/repo/beta", integration_branch="main")
    teams.bind_fable(
        "beta",
        expected_generation=beta.generation,
        cell_id="cell-beta",
        session_id="sess-fable-beta",
        profile_alias="max-c",
    )

    teams.reserve("gamma", repo_path="/repo/gamma", integration_branch="main")

    assert teams.ready_projects() == frozenset({"alpha"})


@pytest.mark.asyncio
async def test_activate_on_uncertain_team_refuses_without_calling_ensure_callables(
    teams: ProjectTeamService, database: Database
) -> None:
    seed_cell(database, cell_id="cell-1", project_key="demo")
    team = teams.reserve("demo", repo_path="/repo/demo", integration_branch="main")
    team = teams.bind_fable(
        "demo",
        expected_generation=team.generation,
        cell_id="cell-1",
        session_id="sess-fable-1",
        profile_alias="max-c",
    )
    teams.mark_uncertain(
        "demo",
        expected_generation=team.generation,
        reason="ambiguous sol thread/start outcome",
    )
    fable_calls = 0
    sol_calls = 0

    def ensure_fable_spy() -> tuple[str, str, str]:
        nonlocal fable_calls
        fable_calls += 1
        return "cell-1", "sess-fable-1", "max-c"

    async def ensure_sol_spy() -> tuple[str, int, str, str]:
        nonlocal sol_calls
        sol_calls += 1
        return "thread-1", 1, SOL_MODEL, SOL_PROVIDER

    with pytest.raises(TeamUncertain, match="ambiguous sol thread/start outcome"):
        await teams.activate(
            "demo",
            repo_path="/repo/demo",
            integration_branch="main",
            ensure_fable=ensure_fable_spy,
            ensure_sol=ensure_sol_spy,
            can_admit=lambda: True,
        )

    assert fable_calls == 0
    assert sol_calls == 0
    still_uncertain = teams.live_team("demo")
    assert still_uncertain is not None
    assert still_uncertain.state == "uncertain"

    for member_write in (
        lambda: teams.bind_fable(
            "demo",
            expected_generation=still_uncertain.generation,
            cell_id="cell-1",
            session_id="sess-fable-1",
            profile_alias="max-c",
        ),
        lambda: teams.rotate_fable(
            "demo",
            expected_generation=still_uncertain.generation,
            cell_id="cell-1",
            session_id="sess-fable-1",
            profile_alias="max-c",
        ),
    ):
        with pytest.raises(TeamUncertain):
            member_write()


def test_reconcile_existing_derives_ready_team_from_proven_members(
    teams: ProjectTeamService, database: Database
) -> None:
    seed_cell(database, cell_id="cell-1", project_key="demo")
    seed_channel(
        database,
        project_key="demo",
        thread_id="thread-1",
        generation=1,
        model=SOL_MODEL,
        provider=SOL_PROVIDER,
        model_verified_at=NOW_ISO,
    )

    team = teams.reconcile_existing(
        "demo",
        repo_path="/repo/demo",
        integration_branch="main",
        cell=("cell-1", "sess-fable-1", "max-c"),
        channel=("thread-1", 1, SOL_MODEL, SOL_PROVIDER),
        channel_proven=True,
    )

    assert team is not None
    assert team.state == "ready"
    assert team.fable_cell_id == "cell-1"
    assert team.sol_thread_id == "thread-1"
    assert team.sol_model == SOL_MODEL
    assert team.sol_provider == SOL_PROVIDER
    assert "demo" in teams.ready_projects()


def test_reconcile_existing_leaves_sol_unbound_until_proven(
    teams: ProjectTeamService, database: Database
) -> None:
    seed_cell(database, cell_id="cell-1", project_key="demo")
    seed_channel(database, project_key="demo", thread_id="thread-1", generation=1)

    team = teams.reconcile_existing(
        "demo",
        repo_path="/repo/demo",
        integration_branch="main",
        cell=("cell-1", "sess-fable-1", "max-c"),
        channel=("thread-1", 1, None, None),
        channel_proven=False,
    )

    assert team is not None
    assert team.state == "fable_bound"
    assert team.fable_cell_id == "cell-1"
    assert team.sol_thread_id is None
    assert "demo" not in teams.ready_projects()

    # A proven channel presented afterward with an unproven flag still
    # must not bind -- only channel_proven=True with the Sol identity
    # may bind Sol.
    still_unbound = teams.reconcile_existing(
        "demo",
        repo_path="/repo/demo",
        integration_branch="main",
        cell=("cell-1", "sess-fable-1", "max-c"),
        channel=("thread-1", 1, SOL_MODEL, SOL_PROVIDER),
        channel_proven=False,
    )
    assert still_unbound is not None
    assert still_unbound.sol_thread_id is None
    assert still_unbound.state == "fable_bound"


def test_reconcile_existing_is_idempotent_and_never_creates_second_row(
    teams: ProjectTeamService, database: Database
) -> None:
    seed_cell(database, cell_id="cell-1", project_key="demo")
    seed_channel(
        database,
        project_key="demo",
        thread_id="thread-1",
        generation=1,
        model=SOL_MODEL,
        provider=SOL_PROVIDER,
        model_verified_at=NOW_ISO,
    )

    first = teams.reconcile_existing(
        "demo",
        repo_path="/repo/demo",
        integration_branch="main",
        cell=("cell-1", "sess-fable-1", "max-c"),
        channel=("thread-1", 1, SOL_MODEL, SOL_PROVIDER),
        channel_proven=True,
    )
    second = teams.reconcile_existing(
        "demo",
        repo_path="/repo/demo",
        integration_branch="main",
        cell=("cell-1", "sess-fable-1", "max-c"),
        channel=("thread-1", 1, SOL_MODEL, SOL_PROVIDER),
        channel_proven=True,
    )

    assert first == second
    assert database.scalar("SELECT COUNT(*) FROM project_teams") == 1


def test_reconcile_existing_rotates_terminal_bound_member_to_live_cell(
    teams: ProjectTeamService, database: Database
) -> None:
    ready = bind_ready(teams, database)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE project_cells SET state = 'released' WHERE cell_id = ?",
            ("cell-1",),
        )
    seed_cell(
        database,
        cell_id="cell-2",
        project_key="demo",
        session_id="sess-fable-2",
    )

    team = teams.reconcile_existing(
        "demo",
        repo_path="/repo/demo",
        integration_branch="main",
        cell=("cell-2", "sess-fable-2", "max-c"),
        channel=("thread-1", 1, SOL_MODEL, SOL_PROVIDER),
        channel_proven=True,
    )

    assert team is not None
    assert team.state == "ready"
    assert team.generation == ready.generation + 1
    assert team.fable_cell_id == "cell-2"
    assert team.sol_thread_id == "thread-1"

    live = teams.live_team("demo")
    assert live is not None
    assert live == team


def test_reconcile_existing_recovers_failed_bound_fable_to_sole_live_replacement(
    teams: ProjectTeamService, database: Database
) -> None:
    ready = bind_ready(teams, database)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE project_cells SET state = 'failed' WHERE cell_id = ?",
            ("cell-1",),
        )
    seed_cell(
        database,
        cell_id="cell-2",
        project_key="demo",
        session_id="sess-fable-2",
    )
    teams.mark_uncertain(
        "demo",
        expected_generation=ready.generation,
        reason=(
            "reconciliation observed live fable cell 'cell-2' but the "
            "bound member is 'cell-1'"
        ),
    )

    recovered = teams.reconcile_existing(
        "demo",
        repo_path="/repo/demo",
        integration_branch="main",
        cell=("cell-2", "sess-fable-2", "max-c"),
        channel=("thread-1", 1, SOL_MODEL, SOL_PROVIDER),
        channel_proven=True,
    )

    assert recovered is not None
    assert recovered.generation == ready.generation + 1
    assert recovered.state == "ready"
    assert recovered.fable_cell_id == "cell-2"
    assert recovered.fable_session_id == "sess-fable-2"
    assert recovered.fable_generation == ready.fable_generation + 1
    assert recovered.sol_thread_id == ready.sol_thread_id
    assert recovered.sol_generation == ready.sol_generation
    old = database.execute(
        "SELECT state FROM project_teams WHERE project_key = ? AND generation = ?",
        ("demo", ready.generation),
    ).fetchone()
    assert str(old["state"]) == "superseded"


def test_reconcile_existing_recovers_when_recorded_replacement_also_failed(
    teams: ProjectTeamService, database: Database
) -> None:
    ready = bind_ready(teams, database)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE project_cells SET state = 'failed' WHERE cell_id = ?",
            ("cell-1",),
        )
    seed_cell(
        database,
        cell_id="cell-2",
        project_key="demo",
        session_id="sess-fable-2",
    )
    teams.mark_uncertain(
        "demo",
        expected_generation=ready.generation,
        reason=(
            "reconciliation observed live fable cell 'cell-2' but the "
            "bound member is 'cell-1'"
        ),
    )
    with database.transaction() as connection:
        connection.execute(
            "UPDATE project_cells SET state = 'failed' WHERE cell_id = ?",
            ("cell-2",),
        )
    seed_cell(
        database,
        cell_id="cell-3",
        project_key="demo",
        session_id="sess-fable-3",
    )

    recovered = teams.reconcile_existing(
        "demo",
        repo_path="/repo/demo",
        integration_branch="main",
        cell=("cell-3", "sess-fable-3", "max-c"),
        channel=("thread-1", 1, SOL_MODEL, SOL_PROVIDER),
        channel_proven=True,
    )

    assert recovered is not None
    assert recovered.generation == ready.generation + 1
    assert recovered.state == "ready"
    assert recovered.fable_cell_id == "cell-3"
    assert recovered.fable_session_id == "sess-fable-3"
    assert recovered.sol_thread_id == ready.sol_thread_id


def test_reconcile_existing_does_not_clear_unrelated_uncertainty(
    teams: ProjectTeamService, database: Database
) -> None:
    ready = bind_ready(teams, database)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE project_cells SET state = 'failed' WHERE cell_id = ?",
            ("cell-1",),
        )
    seed_cell(
        database,
        cell_id="cell-2",
        project_key="demo",
        session_id="sess-fable-2",
    )
    uncertain = teams.mark_uncertain(
        "demo",
        expected_generation=ready.generation,
        reason="codex rpc outcome unknown",
    )

    unchanged = teams.reconcile_existing(
        "demo",
        repo_path="/repo/demo",
        integration_branch="main",
        cell=("cell-2", "sess-fable-2", "max-c"),
        channel=("thread-1", 1, SOL_MODEL, SOL_PROVIDER),
        channel_proven=True,
    )

    assert unchanged == uncertain


def test_reconcile_existing_returns_none_when_no_members(
    teams: ProjectTeamService, database: Database
) -> None:
    team = teams.reconcile_existing(
        "demo",
        repo_path="/repo/demo",
        integration_branch="main",
        cell=None,
        channel=None,
        channel_proven=False,
    )

    assert team is None
    assert teams.live_team("demo") is None
    assert database.scalar("SELECT COUNT(*) FROM project_teams") == 0


@pytest.mark.asyncio
async def test_resolve_uncertain_reopens_activation(
    teams: ProjectTeamService, database: Database
) -> None:
    seed_cell(database, cell_id="cell-1", project_key="demo")
    seed_channel(
        database,
        project_key="demo",
        thread_id="thread-1",
        generation=1,
        model=SOL_MODEL,
        provider=SOL_PROVIDER,
        model_verified_at=NOW_ISO,
    )
    team = teams.reserve("demo", repo_path="/repo/demo", integration_branch="main")
    team = teams.bind_fable(
        "demo",
        expected_generation=team.generation,
        cell_id="cell-1",
        session_id="sess-fable-1",
        profile_alias="max-c",
    )
    uncertain = teams.mark_uncertain(
        "demo",
        expected_generation=team.generation,
        reason="ambiguous sol thread/start outcome",
    )

    reopened = teams.resolve_uncertain("demo", expected_generation=uncertain.generation)
    assert reopened.state == "reserved"
    assert reopened.fable_cell_id == "cell-1"

    fable_calls = 0
    sol_calls = 0

    def ensure_fable_spy() -> tuple[str, str, str]:
        nonlocal fable_calls
        fable_calls += 1
        return "cell-1", "sess-fable-1", "max-c"

    async def ensure_sol_spy() -> tuple[str, int, str, str]:
        nonlocal sol_calls
        sol_calls += 1
        return "thread-1", 1, SOL_MODEL, SOL_PROVIDER

    ready = await teams.activate(
        "demo",
        repo_path="/repo/demo",
        integration_branch="main",
        ensure_fable=ensure_fable_spy,
        ensure_sol=ensure_sol_spy,
        can_admit=lambda: True,
    )

    assert fable_calls == 0
    assert sol_calls == 1
    assert ready.state == "ready"
    assert ready.fable_cell_id == "cell-1"
    assert ready.sol_thread_id == "thread-1"


def test_bind_fable_refuses_wrong_session_or_profile(
    teams: ProjectTeamService, database: Database
) -> None:
    """Sol reviewer correction a63d6197: an otherwise-live development
    cell must still refuse to bind when the caller-supplied
    session_id or profile_alias disagrees with the owning
    ``project_cells`` row -- the cell key existing is not enough."""

    seed_cell(
        database,
        cell_id="cell-1",
        project_key="demo",
        session_id="sess-fable-1",
        profile_alias="max-c",
    )
    team = teams.reserve("demo", repo_path="/repo/demo", integration_branch="main")

    with pytest.raises(TeamMemberMismatch):
        teams.bind_fable(
            "demo",
            expected_generation=team.generation,
            cell_id="cell-1",
            session_id="sess-fable-WRONG",
            profile_alias="max-c",
        )
    with pytest.raises(TeamMemberMismatch):
        teams.bind_fable(
            "demo",
            expected_generation=team.generation,
            cell_id="cell-1",
            session_id="sess-fable-1",
            profile_alias="max-WRONG",
        )

    unchanged = teams.live_team("demo")
    assert unchanged is not None
    assert unchanged.state == "reserved"
    assert unchanged.fable_cell_id is None
    assert unchanged.fable_session_id is None
    assert unchanged.fable_profile_alias is None
    assert unchanged.updated_at == team.updated_at


def test_bind_sol_refuses_null_or_mismatched_persisted_model(
    teams: ProjectTeamService, database: Database
) -> None:
    """Sol reviewer correction a63d6197: a reviewer channel whose
    persisted model/provider are NULL (unproven) or disagree with the
    supplied identity must refuse even though the caller supplies the
    one authenticated gpt-5.6-sol/chatgpt identity -- the thread/
    generation key existing is not enough."""

    seed_cell(database, cell_id="cell-1", project_key="demo")
    team = teams.reserve("demo", repo_path="/repo/demo", integration_branch="main")
    team = teams.bind_fable(
        "demo",
        expected_generation=team.generation,
        cell_id="cell-1",
        session_id="sess-fable-1",
        profile_alias="max-c",
    )

    # Persisted model/provider are NULL -- an unproven, legacy channel.
    seed_channel(database, project_key="demo", thread_id="thread-1", generation=1)
    with pytest.raises(SolModelMismatch):
        teams.bind_sol(
            "demo",
            expected_generation=team.generation,
            thread_id="thread-1",
            sol_generation=1,
            model=SOL_MODEL,
            provider=SOL_PROVIDER,
        )

    # Persisted model/provider are set, but disagree with what is
    # supplied (a different authenticated boundary than the row proves).
    seed_channel(
        database,
        project_key="demo",
        thread_id="thread-1",
        generation=1,
        model="gpt-4-turbo",
        provider="anthropic",
        model_verified_at=NOW_ISO,
    )
    with pytest.raises(SolModelMismatch):
        teams.bind_sol(
            "demo",
            expected_generation=team.generation,
            thread_id="thread-1",
            sol_generation=1,
            model=SOL_MODEL,
            provider=SOL_PROVIDER,
        )

    unchanged = teams.live_team("demo")
    assert unchanged is not None
    assert unchanged.state == "fable_bound"
    assert unchanged.sol_thread_id is None
    assert unchanged.sol_model is None
    assert unchanged.sol_provider is None
    assert unchanged.updated_at == team.updated_at


def test_exact_owning_row_identities_bind_and_reach_ready(
    teams: ProjectTeamService, database: Database
) -> None:
    """Sol reviewer correction a63d6197: when every supplied identity
    exactly matches its source-of-truth durable row -- Fable cell/
    project/lane/live state/session/profile and Sol project/thread/
    generation/persisted authenticated model/provider -- both members
    bind and the pair reaches ``ready``."""

    seed_cell(
        database,
        cell_id="cell-1",
        project_key="demo",
        state="active",
        lane_role="development",
        session_id="sess-fable-1",
        profile_alias="max-c",
    )
    seed_channel(
        database,
        project_key="demo",
        thread_id="thread-1",
        generation=1,
        model=SOL_MODEL,
        provider=SOL_PROVIDER,
        model_verified_at=NOW_ISO,
    )
    team = teams.reserve("demo", repo_path="/repo/demo", integration_branch="main")

    team = teams.bind_fable(
        "demo",
        expected_generation=team.generation,
        cell_id="cell-1",
        session_id="sess-fable-1",
        profile_alias="max-c",
    )
    assert team.state == "fable_bound"
    assert team.fable_cell_id == "cell-1"
    assert team.fable_session_id == "sess-fable-1"
    assert team.fable_profile_alias == "max-c"

    team = teams.bind_sol(
        "demo",
        expected_generation=team.generation,
        thread_id="thread-1",
        sol_generation=1,
        model=SOL_MODEL,
        provider=SOL_PROVIDER,
    )

    assert team.state == "ready"
    assert team.sol_thread_id == "thread-1"
    assert team.sol_model == SOL_MODEL
    assert team.sol_provider == SOL_PROVIDER
    assert "demo" in teams.ready_projects()
