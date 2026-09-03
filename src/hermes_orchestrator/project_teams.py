"""Durable Fable+Sol pair coordinator, one execution unit per project.

INFRA-187: a project's execution unit is exactly one Fable work lead
plus one Sol merge lead. Team identity is ``(project_key, generation)``.
Members are never duplicated state -- ``fable_*`` columns reference the
exact ``project_cells`` row (``cells.py``) that IS the Fable lead, and
``sol_*`` columns reference the exact ``reviewer_channels`` thread
generation (``codex_merger.py``) that IS the Sol reviewer channel. This
module only binds and validates those identities; it never writes to
``project_cells`` or ``reviewer_channels``.

Every mutation is a compare-and-swap on the live row at an exact
``expected_generation``: a mismatch (the row moved, or a caller is
racing against a rotation/replacement) raises :class:`StaleTeamGeneration`
and writes nothing. Binding a member additionally cross-checks its
identity against the owning table -- a cross-project or non-live cell,
a stale or foreign reviewer-channel thread, or a Sol model/provider
other than the one authenticated boundary all refuse fail-closed with
no write.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hermes_orchestrator.db import Database

# The exact ``project_cells`` states a Fable cell may be bound from
# (mirrors ``cells.py``'s ``_ACTIVE_CELL_STATES``, restated here rather
# than imported so this module never depends on ``cells.py`` internals
# beyond the documented row shape).
_LIVE_CELL_STATES = ("starting", "active", "handoff_required", "paused")

# The single authenticated Sol boundary this coordinator ever binds.
# Restated here (rather than imported from ``codex_merger.py``) so this
# module has no import-time dependency on that concurrently-edited
# module; see ``codex_merger.MERGER_MODEL`` for the source of truth.
SOL_MODEL = "gpt-5.6-sol"
SOL_PROVIDER = "chatgpt"

_LIVE_TEAM_STATES = (
    "reserved",
    "fable_bound",
    "sol_bound",
    "ready",
    "uncertain",
)


class TeamMemberMismatch(ValueError):
    """A bound member's identity disagrees with its owning table."""


class SolModelMismatch(ValueError):
    """The Sol member is not the one authenticated model/provider."""


class StaleTeamGeneration(ValueError):
    """A write's ``expected_generation`` no longer names the live row."""


class StaleTeamMember(ValueError):
    """A supplied member identity disagrees with the ready team's own."""


class TeamUncertain(ValueError):
    """The live team is ``uncertain``: only ``resolve_uncertain`` may act.

    Ambiguous remote thread/session creation must leave the pair
    uncertain and admission closed rather than ever risking a
    duplicate -- so every write that could invoke a creation callable
    or move the pair past ``uncertain`` refuses immediately, before
    touching anything, once the live row is uncertain. Carries the
    stored ``reason`` from the ``mark_uncertain`` call that produced it.
    """


class RetirementEvidenceRequired(ValueError):
    """Retirement was requested without proof of merge and cleanup."""


class TeamAdmissionRefused(ValueError):
    """A brand-new team may not be admitted right now."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class ProjectTeam:
    """One durable (project_key, generation) Fable+Sol execution unit."""

    project_key: str
    generation: int
    state: str
    repo_path: str
    integration_branch: str
    fable_cell_id: str | None
    fable_session_id: str | None
    fable_profile_alias: str | None
    fable_generation: int
    sol_thread_id: str | None
    sol_generation: int | None
    sol_model: str | None
    sol_provider: str | None
    reason: str | None
    created_at: str
    updated_at: str
    retired_at: str | None


class ProjectTeamService:
    """Reserve, bind, rotate, and retire the per-project pair team."""

    def __init__(
        self,
        database: Database,
        *,
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._database = database
        self._now = now

    # -- reads ----------------------------------------------------------

    def live_team(self, project_key: str) -> ProjectTeam | None:
        """The one row whose state is not superseded/retired, if any."""

        row = self._database.execute(
            "SELECT * FROM project_teams WHERE project_key = ? "
            "AND state NOT IN ('superseded', 'retired') "
            "ORDER BY generation DESC LIMIT 1",
            (project_key,),
        ).fetchone()
        return None if row is None else self._record(row)

    def ready_team(self, project_key: str) -> ProjectTeam | None:
        """The live team, only if it has reached ``ready``."""

        team = self.live_team(project_key)
        return team if team is not None and team.state == "ready" else None

    def ready_projects(self) -> frozenset[str]:
        """Every project whose live team is currently ``ready``."""

        rows = self._database.execute(
            "SELECT DISTINCT project_key FROM project_teams WHERE state = 'ready'"
        ).fetchall()
        return frozenset(str(row["project_key"]) for row in rows)

    def resolve(
        self,
        project_key: str,
        *,
        fable_cell_id: str | None = None,
        fable_generation: int | None = None,
        sol_thread_id: str | None = None,
        sol_generation: int | None = None,
    ) -> ProjectTeam:
        """The ready team, iff every supplied identity matches exactly."""

        team = self.ready_team(project_key)
        if team is None:
            raise StaleTeamMember(f"no ready team for project {project_key!r}")
        if fable_cell_id is not None and fable_cell_id != team.fable_cell_id:
            raise StaleTeamMember(
                f"fable cell {fable_cell_id!r} is not the ready team's "
                f"{team.fable_cell_id!r} for project {project_key!r}"
            )
        if fable_generation is not None and fable_generation != team.fable_generation:
            raise StaleTeamMember(
                f"fable generation {fable_generation} is not the ready "
                f"team's {team.fable_generation} for project {project_key!r}"
            )
        if sol_thread_id is not None and sol_thread_id != team.sol_thread_id:
            raise StaleTeamMember(
                f"sol thread {sol_thread_id!r} is not the ready team's "
                f"{team.sol_thread_id!r} for project {project_key!r}"
            )
        if sol_generation is not None and sol_generation != team.sol_generation:
            raise StaleTeamMember(
                f"sol generation {sol_generation} is not the ready team's "
                f"{team.sol_generation} for project {project_key!r}"
            )
        return team

    # -- reservation ------------------------------------------------------

    def reserve(
        self,
        project_key: str,
        *,
        repo_path: str | Path,
        integration_branch: str,
    ) -> ProjectTeam:
        """Create the project's first live team, or converge on it.

        Duplicate activation must never race two live rows into
        existence: if a live team already exists it is returned
        unchanged, and a concurrent insert racing this one is resolved
        by re-reading rather than by erroring.
        """

        existing = self.live_team(project_key)
        if existing is not None:
            return existing
        next_generation = int(
            self._database.scalar(
                "SELECT coalesce(max(generation), 0) + 1 "
                "FROM project_teams WHERE project_key = ?",
                (project_key,),
            )
            or 1
        )
        stamp = self._now().isoformat()
        try:
            with self._database.transaction() as connection:
                connection.execute(
                    "INSERT INTO project_teams("
                    "project_key, generation, state, repo_path, "
                    "integration_branch, fable_generation, created_at, "
                    "updated_at) "
                    "VALUES (?, ?, 'reserved', ?, ?, 0, ?, ?)",
                    (
                        project_key,
                        next_generation,
                        str(repo_path),
                        integration_branch,
                        stamp,
                        stamp,
                    ),
                )
        except sqlite3.IntegrityError:
            pass
        existing = self.live_team(project_key)
        if existing is None:
            raise RuntimeError(f"failed to reserve a project team for {project_key!r}")
        return existing

    # -- member binding ---------------------------------------------------

    def bind_fable(
        self,
        project_key: str,
        *,
        expected_generation: int,
        cell_id: str,
        session_id: str,
        profile_alias: str,
    ) -> ProjectTeam:
        """CAS-bind the exact Fable cell as this generation's work lead."""

        team = self._require_live(project_key, expected_generation)
        self._refuse_if_uncertain(team)
        self._verify_fable_cell(
            project_key, cell_id, session_id=session_id, profile_alias=profile_alias
        )
        if team.fable_cell_id is None:
            new_fable_generation = 1
        elif cell_id != team.fable_cell_id:
            new_fable_generation = team.fable_generation + 1
        else:
            new_fable_generation = team.fable_generation
        new_state = "ready" if team.sol_thread_id is not None else "fable_bound"
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE project_teams SET fable_cell_id = ?, "
                "fable_session_id = ?, fable_profile_alias = ?, "
                "fable_generation = ?, state = ?, updated_at = ? "
                "WHERE project_key = ? AND generation = ? AND state = ?",
                (
                    cell_id,
                    session_id,
                    profile_alias,
                    new_fable_generation,
                    new_state,
                    stamp,
                    project_key,
                    expected_generation,
                    team.state,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleTeamGeneration(
                    f"project team {project_key!r} generation "
                    f"{expected_generation} changed underneath bind_fable"
                )
        return self._team_at(project_key, expected_generation)

    def bind_sol(
        self,
        project_key: str,
        *,
        expected_generation: int,
        thread_id: str,
        sol_generation: int,
        model: str,
        provider: str,
    ) -> ProjectTeam:
        """CAS-bind the exact Sol reviewer channel as this generation's
        merge lead. Refuses with no write unless it is the one
        authenticated model/provider."""

        self._verify_sol_identity(model, provider)
        team = self._require_live(project_key, expected_generation)
        self._refuse_if_uncertain(team)
        self._verify_sol_channel(
            project_key, thread_id, sol_generation, model=model, provider=provider
        )
        new_state = "ready" if team.fable_cell_id is not None else "sol_bound"
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE project_teams SET sol_thread_id = ?, "
                "sol_generation = ?, sol_model = ?, sol_provider = ?, "
                "state = ?, updated_at = ? "
                "WHERE project_key = ? AND generation = ? AND state = ?",
                (
                    thread_id,
                    sol_generation,
                    model,
                    provider,
                    new_state,
                    stamp,
                    project_key,
                    expected_generation,
                    team.state,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleTeamGeneration(
                    f"project team {project_key!r} generation "
                    f"{expected_generation} changed underneath bind_sol"
                )
        return self._team_at(project_key, expected_generation)

    # -- reconciliation from existing members ------------------------------

    def reconcile_existing(
        self,
        project_key: str,
        *,
        repo_path: str | Path,
        integration_branch: str,
        cell: tuple[str, str, str] | None,
        channel: tuple[str, int, str | None, str | None] | None,
        channel_proven: bool,
    ) -> ProjectTeam | None:
        """Idempotently derive a team from members that already exist.

        INFRA-187 wave 2: a scheduler active-project check must consult
        a *ready pair*, not infer completeness from a live Fable cell
        alone -- and a pre-existing reviewer channel needs an explicit
        migration/reconciliation proof before its pair becomes ready.
        This converges the durable team onto whatever members are
        already live, without ever inventing a new remote cell or
        channel itself (see :meth:`activate` for that saga).

        ``cell`` is the observed live development cell as
        ``(cell_id, session_id, profile_alias)``; ``channel`` is the
        observed reviewer channel as
        ``(thread_id, generation, model, provider)``. Either may be
        ``None`` when that member does not currently exist.

        - Neither member exists: writes nothing and returns ``None``.
        - The live team is ``uncertain``: returned unchanged, no
          binding is attempted (only :meth:`resolve_uncertain` may
          reopen it).
        - A member that is not yet bound is bound if observed. The Sol
          member is only ever bound here when ``channel_proven`` is
          True, the observed model/provider are the one authenticated
          Sol identity, AND the persisted ``reviewer_channels`` row
          itself already carries that same proven identity --
          ``channel_proven`` is a caller hint, never trusted on its
          own, since the actual CAS bind re-derives proof from the
          owning row (see ``_channel_is_proven``). Otherwise the
          member is left unbound and the pair stays not-ready until
          the merger's recovery-time reconciliation
          (``CodexMerger.model_proven``) proves the model.
        - A member that IS already bound but disagrees with what was
          observed (e.g. the live cell has a different ``cell_id``, or
          the channel has a different thread/generation) is never
          silently rebound: the team is marked ``uncertain`` naming the
          mismatch and returned. Rotation must go through
          :meth:`rotate_fable`/:meth:`replace_sol` explicitly.

        Reserves (converging on the live row, never creating a second
        one) before binding, so repeated calls with the same observed
        members are idempotent no-ops once the pair is settled.
        """

        if cell is None and channel is None:
            return None
        team = self.reserve(
            project_key,
            repo_path=repo_path,
            integration_branch=integration_branch,
        )
        if team.state == "uncertain":
            if (
                cell is None
                or team.fable_cell_id == cell[0]
                or team.reason
                != self._fable_mismatch_reason(team.fable_cell_id, cell[0])
                or not self._fable_replacement_is_unambiguous(team, cell)
            ):
                return team
            team = self._rotate_fable_member(team, *cell)
        if cell is not None:
            cell_id, session_id, profile_alias = cell
            if team.fable_cell_id is not None and team.fable_cell_id != cell_id:
                if self._fable_replacement_is_unambiguous(team, cell):
                    team = self._rotate_fable_member(team, *cell)
                else:
                    return self.mark_uncertain(
                        project_key,
                        expected_generation=team.generation,
                        reason=self._fable_mismatch_reason(
                            team.fable_cell_id, cell_id
                        ),
                    )
            if team.fable_cell_id is None:
                team = self.bind_fable(
                    project_key,
                    expected_generation=team.generation,
                    cell_id=cell_id,
                    session_id=session_id,
                    profile_alias=profile_alias,
                )
        if channel is not None:
            thread_id, sol_generation, model, provider = channel
            if team.sol_thread_id is not None and (
                team.sol_thread_id != thread_id
                or team.sol_generation != sol_generation
            ):
                return self.mark_uncertain(
                    project_key,
                    expected_generation=team.generation,
                    reason=(
                        "reconciliation observed reviewer channel "
                        f"{thread_id!r} generation {sol_generation} but "
                        f"the bound member is {team.sol_thread_id!r} "
                        f"generation {team.sol_generation}"
                    ),
                )
            if (
                team.sol_thread_id is None
                and channel_proven
                and model == SOL_MODEL
                and provider == SOL_PROVIDER
                and self._channel_is_proven(project_key)
            ):
                team = self.bind_sol(
                    project_key,
                    expected_generation=team.generation,
                    thread_id=thread_id,
                    sol_generation=sol_generation,
                    model=model,
                    provider=provider,
                )
        return team

    # -- uncertainty --------------------------------------------------------

    def mark_uncertain(
        self,
        project_key: str,
        *,
        expected_generation: int,
        reason: str,
    ) -> ProjectTeam:
        """CAS the live row to ``uncertain``; admission stays closed."""

        team = self._require_live(project_key, expected_generation)
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE project_teams SET state = 'uncertain', reason = ?, "
                "updated_at = ? "
                "WHERE project_key = ? AND generation = ? AND state = ?",
                (reason, stamp, project_key, expected_generation, team.state),
            )
            if cursor.rowcount != 1:
                raise StaleTeamGeneration(
                    f"project team {project_key!r} generation "
                    f"{expected_generation} changed underneath mark_uncertain"
                )
        return self._team_at(project_key, expected_generation)

    def resolve_uncertain(
        self,
        project_key: str,
        *,
        expected_generation: int,
    ) -> ProjectTeam:
        """Operator reconciliation entry point: uncertain -> reserved,
        keeping whichever members were already bound."""

        team = self._require_live(project_key, expected_generation)
        if team.state != "uncertain":
            raise StaleTeamGeneration(
                f"project team {project_key!r} generation "
                f"{expected_generation} is {team.state!r}, not 'uncertain'"
            )
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE project_teams SET state = 'reserved', reason = NULL, "
                "updated_at = ? "
                "WHERE project_key = ? AND generation = ? AND state = "
                "'uncertain'",
                (stamp, project_key, expected_generation),
            )
            if cursor.rowcount != 1:
                raise StaleTeamGeneration(
                    f"project team {project_key!r} generation "
                    f"{expected_generation} changed underneath "
                    "resolve_uncertain"
                )
        return self._team_at(project_key, expected_generation)

    # -- rotation / replacement ---------------------------------------------

    def rotate_fable(
        self,
        project_key: str,
        *,
        expected_generation: int,
        cell_id: str,
        session_id: str,
        profile_alias: str,
    ) -> ProjectTeam:
        """Supersede the live row and insert generation+1 with a new
        Fable member, preserving the Sol counterpart untouched."""

        team = self._require_live(project_key, expected_generation)
        self._refuse_if_uncertain(team)
        self._verify_fable_cell(
            project_key, cell_id, session_id=session_id, profile_alias=profile_alias
        )
        return self._rotate_fable_member(
            team, cell_id, session_id, profile_alias
        )

    def _rotate_fable_member(
        self,
        team: ProjectTeam,
        cell_id: str,
        session_id: str,
        profile_alias: str,
    ) -> ProjectTeam:
        new_generation = team.generation + 1
        new_fable_generation = team.fable_generation + 1
        new_state = "ready" if team.sol_thread_id is not None else "fable_bound"
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            self._supersede(connection, team, stamp)
            connection.execute(
                "INSERT INTO project_teams("
                "project_key, generation, state, repo_path, "
                "integration_branch, fable_cell_id, fable_session_id, "
                "fable_profile_alias, fable_generation, sol_thread_id, "
                "sol_generation, sol_model, sol_provider, created_at, "
                "updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    team.project_key,
                    new_generation,
                    new_state,
                    team.repo_path,
                    team.integration_branch,
                    cell_id,
                    session_id,
                    profile_alias,
                    new_fable_generation,
                    team.sol_thread_id,
                    team.sol_generation,
                    team.sol_model,
                    team.sol_provider,
                    stamp,
                    stamp,
                ),
            )
        return self._team_at(team.project_key, new_generation)

    def _fable_replacement_is_unambiguous(
        self,
        team: ProjectTeam,
        cell: tuple[str, str, str],
    ) -> bool:
        """True when a failed bound cell has exactly one live successor."""

        cell_id, session_id, profile_alias = cell
        self._verify_fable_cell(
            team.project_key,
            cell_id,
            session_id=session_id,
            profile_alias=profile_alias,
        )
        bound = self._database.execute(
            "SELECT state FROM project_cells WHERE cell_id = ?",
            (team.fable_cell_id,),
        ).fetchone()
        if bound is None or str(bound["state"]) in _LIVE_CELL_STATES:
            return False
        placeholders = ",".join("?" for _ in _LIVE_CELL_STATES)
        rows = self._database.execute(
            f"SELECT cell_id FROM project_cells WHERE project_key = ? "
            f"AND lane_role = 'development' AND state IN ({placeholders})",
            (team.project_key, *_LIVE_CELL_STATES),
        ).fetchall()
        return len(rows) == 1 and str(rows[0]["cell_id"]) == cell_id

    @staticmethod
    def _fable_mismatch_reason(bound_cell_id: str | None, live_cell_id: str) -> str:
        return (
            "reconciliation observed live fable cell "
            f"{live_cell_id!r} but the bound member is {bound_cell_id!r}"
        )

    def replace_sol(
        self,
        project_key: str,
        *,
        expected_generation: int,
        thread_id: str,
        sol_generation: int,
        model: str,
        provider: str,
    ) -> ProjectTeam:
        """Supersede the live row and insert generation+1 with a new
        Sol member, preserving the Fable counterpart untouched."""

        self._verify_sol_identity(model, provider)
        team = self._require_live(project_key, expected_generation)
        self._refuse_if_uncertain(team)
        self._verify_sol_channel(
            project_key, thread_id, sol_generation, model=model, provider=provider
        )
        new_generation = expected_generation + 1
        new_state = "ready" if team.fable_cell_id is not None else "sol_bound"
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            self._supersede(connection, team, stamp)
            connection.execute(
                "INSERT INTO project_teams("
                "project_key, generation, state, repo_path, "
                "integration_branch, fable_cell_id, fable_session_id, "
                "fable_profile_alias, fable_generation, sol_thread_id, "
                "sol_generation, sol_model, sol_provider, created_at, "
                "updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    project_key,
                    new_generation,
                    new_state,
                    team.repo_path,
                    team.integration_branch,
                    team.fable_cell_id,
                    team.fable_session_id,
                    team.fable_profile_alias,
                    team.fable_generation,
                    thread_id,
                    sol_generation,
                    model,
                    provider,
                    stamp,
                    stamp,
                ),
            )
        return self._team_at(project_key, new_generation)

    def _supersede(
        self,
        connection: sqlite3.Connection,
        team: ProjectTeam,
        stamp: str,
    ) -> None:
        cursor = connection.execute(
            "UPDATE project_teams SET state = 'superseded', updated_at = ? "
            "WHERE project_key = ? AND generation = ? AND state = ?",
            (stamp, team.project_key, team.generation, team.state),
        )
        if cursor.rowcount != 1:
            raise StaleTeamGeneration(
                f"project team {team.project_key!r} generation "
                f"{team.generation} changed underneath rotation"
            )

    # -- retirement -----------------------------------------------------

    def retire(
        self,
        project_key: str,
        *,
        expected_generation: int,
        merge_checkpoint_sha: str,
        cleanup_evidence: str,
    ) -> ProjectTeam:
        """Retire the live row, refusing with no write unless proof of
        the merge checkpoint and cleanup was supplied."""

        if not merge_checkpoint_sha.strip() or not cleanup_evidence.strip():
            raise RetirementEvidenceRequired(
                f"retiring project team {project_key!r} requires both a "
                "merge checkpoint sha and cleanup evidence"
            )
        team = self._require_live(project_key, expected_generation)
        stamp = self._now().isoformat()
        reason = (
            f"merge_checkpoint_sha={merge_checkpoint_sha} "
            f"cleanup_evidence={cleanup_evidence}"
        )
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE project_teams SET state = 'retired', reason = ?, "
                "retired_at = ?, updated_at = ? "
                "WHERE project_key = ? AND generation = ? AND state = ?",
                (
                    reason,
                    stamp,
                    stamp,
                    project_key,
                    expected_generation,
                    team.state,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleTeamGeneration(
                    f"project team {project_key!r} generation "
                    f"{expected_generation} changed underneath retire"
                )
        return self._team_at(project_key, expected_generation)

    # -- the recoverable activation saga ------------------------------------

    async def activate(
        self,
        project_key: str,
        *,
        repo_path: str | Path,
        integration_branch: str,
        ensure_fable: Callable[[], tuple[str, str, str]],
        ensure_sol: Callable[[], Awaitable[tuple[str, int, str, str]]],
        can_admit: Callable[[], bool],
    ) -> ProjectTeam:
        """Reserve, then bind each member exactly once, resuming cleanly
        after a failure between the two binds.

        A brand-new team is refused outright when ``can_admit()`` is
        False; an already-live team (mid-saga, or already ready) is
        always resumed regardless of ``can_admit`` -- admission gates
        starting a new unit of work, never finishing one already
        underway. Each member is only ever created once: if it is
        already bound on the live row, the matching ``ensure_*``
        callable is never invoked again.

        A live team that is ``uncertain`` refuses immediately with
        :class:`TeamUncertain`, WITHOUT calling ``ensure_fable`` or
        ``ensure_sol`` -- an earlier ambiguous remote creation already
        left the pair uncertain and admission closed, so retrying here
        would risk exactly the duplicate remote thread/session creation
        that state exists to prevent. Only an explicit
        :meth:`resolve_uncertain` reopens the saga.
        """

        team = self.live_team(project_key)
        if team is not None and team.state == "uncertain":
            raise TeamUncertain(
                team.reason
                or f"project team {project_key!r} is uncertain and "
                "requires resolve_uncertain before activation may resume"
            )
        if team is None and not can_admit():
            raise TeamAdmissionRefused(
                f"admission refused for a new project team on {project_key!r}"
            )
        team = self.reserve(
            project_key,
            repo_path=repo_path,
            integration_branch=integration_branch,
        )
        if team.fable_cell_id is None:
            cell_id, session_id, profile_alias = ensure_fable()
            team = self.bind_fable(
                project_key,
                expected_generation=team.generation,
                cell_id=cell_id,
                session_id=session_id,
                profile_alias=profile_alias,
            )
        if team.sol_thread_id is None:
            try:
                thread_id, sol_generation, model, provider = await ensure_sol()
                team = self.bind_sol(
                    project_key,
                    expected_generation=team.generation,
                    thread_id=thread_id,
                    sol_generation=sol_generation,
                    model=model,
                    provider=provider,
                )
            except SolModelMismatch:
                self.mark_uncertain(
                    project_key,
                    expected_generation=team.generation,
                    reason="sol model/provider mismatch",
                )
                raise
            except Exception as error:
                if "Uncertain" in type(error).__name__:
                    self.mark_uncertain(
                        project_key,
                        expected_generation=team.generation,
                        reason=str(error) or type(error).__name__,
                    )
                raise
        ready = self.ready_team(project_key)
        if ready is None:
            raise TeamAdmissionRefused(
                f"project team {project_key!r} did not reach 'ready'"
            )
        return ready

    # -- internals --------------------------------------------------------

    def _require_live(self, project_key: str, expected_generation: int) -> ProjectTeam:
        team = self.live_team(project_key)
        if team is None or team.generation != expected_generation:
            raise StaleTeamGeneration(
                f"no live project team for {project_key!r} at generation "
                f"{expected_generation}"
            )
        return team

    @staticmethod
    def _refuse_if_uncertain(team: ProjectTeam) -> None:
        if team.state == "uncertain":
            raise TeamUncertain(
                team.reason
                or f"project team {team.project_key!r} generation "
                f"{team.generation} is uncertain and requires "
                "resolve_uncertain before it can be written to"
            )

    def _team_at(self, project_key: str, generation: int) -> ProjectTeam:
        row = self._database.execute(
            "SELECT * FROM project_teams WHERE project_key = ? AND generation = ?",
            (project_key, generation),
        ).fetchone()
        if row is None:
            raise StaleTeamGeneration(
                f"no project team row for {project_key!r} generation {generation}"
            )
        return self._record(row)

    def _verify_fable_cell(
        self,
        project_key: str,
        cell_id: str,
        *,
        session_id: str,
        profile_alias: str,
    ) -> None:
        """Refuse (no write) unless the FULL supplied Fable identity --
        project, lane, live state, session, and profile -- matches the
        owning ``project_cells`` row exactly. A caller-supplied
        ``session_id``/``profile_alias`` is never trusted merely
        because ``cell_id`` names a live development cell."""

        row = self._database.execute(
            "SELECT project_key, lane_role, state, session_id, profile_alias "
            "FROM project_cells WHERE cell_id = ?",
            (cell_id,),
        ).fetchone()
        if (
            row is None
            or str(row["project_key"]) != project_key
            or str(row["lane_role"]) != "development"
            or str(row["state"]) not in _LIVE_CELL_STATES
            or _opt_str(row["session_id"]) != session_id
            or _opt_str(row["profile_alias"]) != profile_alias
        ):
            raise TeamMemberMismatch(
                f"fable cell {cell_id!r} is not a live development cell "
                f"for project {project_key!r} with session {session_id!r} "
                f"and profile {profile_alias!r}"
            )

    def _verify_sol_channel(
        self,
        project_key: str,
        thread_id: str,
        sol_generation: int,
        *,
        model: str,
        provider: str,
    ) -> None:
        """Refuse (no write) unless the owning ``reviewer_channels`` row
        matches the FULL supplied Sol identity -- project, thread,
        generation, AND the persisted, previously-verified model/
        provider. A caller-supplied ``model``/``provider`` is never
        trusted merely because ``thread_id``/``generation`` name the
        live channel: a NULL or non-Sol persisted model/provider, or
        one that disagrees with what was supplied, both refuse."""

        row = self._database.execute(
            "SELECT project_key, thread_id, generation, model, provider, "
            "model_verified_at FROM reviewer_channels WHERE project_key = ?",
            (project_key,),
        ).fetchone()
        if (
            row is None
            or str(row["project_key"]) != project_key
            or str(row["thread_id"]) != thread_id
            or int(row["generation"]) != sol_generation
        ):
            raise TeamMemberMismatch(
                f"sol thread {thread_id!r} generation {sol_generation} is "
                f"not the live reviewer channel for project {project_key!r}"
            )
        persisted_model = _opt_str(row["model"])
        persisted_provider = _opt_str(row["provider"])
        if (
            row["model_verified_at"] is None
            or persisted_model is None
            or persisted_provider is None
            or persisted_model != SOL_MODEL
            or persisted_provider != SOL_PROVIDER
            or persisted_model != model
            or persisted_provider != provider
        ):
            raise SolModelMismatch(
                f"sol reviewer channel for project {project_key!r} has "
                f"persisted model {persisted_model!r} on provider "
                f"{persisted_provider!r}, which is not the proven "
                f"{SOL_MODEL!r} on {SOL_PROVIDER!r} matching the "
                f"supplied model {model!r} on provider {provider!r}"
            )

    def _channel_is_proven(self, project_key: str) -> bool:
        """True only when the owning ``reviewer_channels`` row for
        ``project_key`` itself already carries a verified Sol model/
        provider identity -- read directly from the row rather than
        trusted from any caller-supplied value."""

        row = self._database.execute(
            "SELECT model, provider, model_verified_at FROM reviewer_channels "
            "WHERE project_key = ?",
            (project_key,),
        ).fetchone()
        return (
            row is not None
            and row["model_verified_at"] is not None
            and _opt_str(row["model"]) == SOL_MODEL
            and _opt_str(row["provider"]) == SOL_PROVIDER
        )

    @staticmethod
    def _verify_sol_identity(model: str, provider: str) -> None:
        if model != SOL_MODEL or provider != SOL_PROVIDER:
            raise SolModelMismatch(
                f"sol member must be model {SOL_MODEL!r} on provider "
                f"{SOL_PROVIDER!r}, got model {model!r} on provider "
                f"{provider!r}"
            )

    @staticmethod
    def _record(row: sqlite3.Row) -> ProjectTeam:
        return ProjectTeam(
            project_key=str(row["project_key"]),
            generation=int(row["generation"]),
            state=str(row["state"]),
            repo_path=str(row["repo_path"]),
            integration_branch=str(row["integration_branch"]),
            fable_cell_id=_opt_str(row["fable_cell_id"]),
            fable_session_id=_opt_str(row["fable_session_id"]),
            fable_profile_alias=_opt_str(row["fable_profile_alias"]),
            fable_generation=int(row["fable_generation"]),
            sol_thread_id=_opt_str(row["sol_thread_id"]),
            sol_generation=(
                None if row["sol_generation"] is None else int(row["sol_generation"])
            ),
            sol_model=_opt_str(row["sol_model"]),
            sol_provider=_opt_str(row["sol_provider"]),
            reason=_opt_str(row["reason"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            retired_at=_opt_str(row["retired_at"]),
        )


def _opt_str(value: Any) -> str | None:
    return None if value is None else str(value)


__all__ = [
    "SOL_MODEL",
    "SOL_PROVIDER",
    "ProjectTeam",
    "ProjectTeamService",
    "RetirementEvidenceRequired",
    "SolModelMismatch",
    "StaleTeamGeneration",
    "StaleTeamMember",
    "TeamAdmissionRefused",
    "TeamMemberMismatch",
    "TeamUncertain",
]
