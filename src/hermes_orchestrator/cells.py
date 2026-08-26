"""Durable one-lead-per-project cell lifecycle."""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from uuid import UUID

from hermes_orchestrator.claude import ClaudeEvent, LeadTurnRequest
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import IssueState
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.linear import LinearProjection
from hermes_orchestrator.profiles import ProfilePool
from hermes_orchestrator.queue import QueueService

_ACTIVE_CELL_STATES = ("starting", "active", "handoff_required", "paused")


class LeadRunner(Protocol):
    """Claude lead execution boundary used by project cells."""

    def start_lead(self, request: LeadTurnRequest) -> AsyncIterator[ClaudeEvent]: ...

    def resume_lead(
        self,
        session_id: UUID,
        request: LeadTurnRequest,
    ) -> AsyncIterator[ClaudeEvent]: ...

    async def retire_session(self, session_id: UUID) -> None: ...


class LinearProjector(Protocol):
    """Approved Linear projection boundary used by project cells."""

    async def project(
        self,
        issue_id: str,
        target: LinearProjection,
        effect_id: str,
    ) -> object: ...


class HandoffPort(Protocol):
    """Acknowledged handoff lookup used by rotation."""

    def get(self, handoff_id: str) -> object: ...

    def acknowledge(
        self,
        handoff_id: str,
        session_id: UUID,
        restated_next_action: str,
    ) -> object: ...


class RotationBlocked(RuntimeError):
    """The old lead must remain active because rotation is not yet safe."""


@dataclass(frozen=True, slots=True)
class ProjectCell:
    """One durable persistent lead for a project."""

    cell_id: str
    project_key: str
    state: str
    profile_alias: str
    session_id: UUID


@dataclass(frozen=True, slots=True)
class DispatchResult:
    """Outcome of dispatching an explicitly queued issue."""

    status: str
    issue_id: str
    cell_id: str | None = None
    session_id: UUID | None = None
    profile_alias: str | None = None


def _utc_now() -> datetime:
    return datetime.now(UTC)


class ProjectCellService:
    """Start or resume exactly one profile-pinned Claude lead per project."""

    def __init__(
        self,
        *,
        database: Database,
        events: EventStore,
        queue: QueueService,
        profiles: ProfilePool,
        runner: LeadRunner,
        linear: LinearProjector,
        project_paths: Mapping[str, Path],
        session_ids: Callable[[], UUID] = uuid.uuid4,
        cell_ids: Callable[[], str] | None = None,
        now: Callable[[], datetime] = _utc_now,
        handoffs: HandoffPort | None = None,
        replacement_session_ids: Callable[[], UUID] = uuid.uuid4,
    ) -> None:
        self._database = database
        self._events = events
        self._queue = queue
        self._profiles = profiles
        self._runner = runner
        self._linear = linear
        self._project_paths = dict(project_paths)
        self._session_ids = session_ids
        self._cell_ids = cell_ids or (lambda: str(uuid.uuid4()))
        self._now = now
        self._handoffs = handoffs
        self._replacement_session_ids = replacement_session_ids
        self._restore_profile_leases()

    def active_projects(self) -> frozenset[str]:
        """Return projects that already own a live logical lead cell."""

        placeholders = ",".join("?" for _ in _ACTIVE_CELL_STATES)
        rows = self._database.execute(
            f"SELECT DISTINCT project_key FROM project_cells "
            f"WHERE state IN ({placeholders})",
            _ACTIVE_CELL_STATES,
        ).fetchall()
        return frozenset(str(row["project_key"]) for row in rows)

    async def dispatch(self, issue_id: str) -> DispatchResult:
        """Start or resume the issue's project lead after explicit admission."""

        issue = self._queue.get(issue_id)
        cell = self._find_active_cell(issue.project_key)
        created = cell is None
        if cell is None:
            cell = self._create_cell(issue.project_key)
            if cell is None:
                return DispatchResult(status="waiting_for_profile", issue_id=issue_id)
        elif cell.state == "handoff_required":
            return DispatchResult(
                status="handoff_required",
                issue_id=issue_id,
                cell_id=cell.cell_id,
                session_id=cell.session_id,
                profile_alias=cell.profile_alias,
            )

        request = LeadTurnRequest(
            session_id=cell.session_id,
            cwd=self._project_paths[issue.project_key],
            prompt=(
                f"Work only on Hermes-queued issue {issue_id}. "
                "Plan it, coordinate safe parallel work, and report checkpoints."
            ),
            profile_alias=cell.profile_alias,
            resume=not created,
        )
        stream = (
            self._runner.start_lead(request)
            if created
            else self._runner.resume_lead(cell.session_id, request)
        )

        session_confirmed = False
        handoff_required = False
        async for event in stream:
            self.record(cell, event)
            if event.kind == "session.started":
                if event.session_id != cell.session_id:
                    raise RuntimeError(
                        "Claude session id did not match its project cell"
                    )
                session_confirmed = True
                self._activate_issue(cell, issue_id)
                await self._linear.project(
                    issue_id,
                    LinearProjection(
                        status="In Development",
                        assignee_alias="operator",
                    ),
                    effect_id=f"linear:{issue_id}:in-development",
                )
            elif event.kind == "provider.limit":
                handoff_required = True
                self._require_handoff(cell, "subscription_limit")

        if handoff_required:
            status = "handoff_required"
        elif session_confirmed:
            status = "working"
        else:
            status = "start_unconfirmed"
        return DispatchResult(
            status=status,
            issue_id=issue_id,
            cell_id=cell.cell_id,
            session_id=cell.session_id,
            profile_alias=cell.profile_alias,
        )

    def record(self, cell: ProjectCell, event: ClaudeEvent) -> None:
        """Journal a sanitized Claude event and any child worker observation."""

        with self._database.transaction() as connection:
            self._events.append(
                connection,
                EventInput(
                    event_type=event.kind,
                    aggregate_type="project_cell",
                    aggregate_id=cell.cell_id,
                    payload={
                        "profile_alias": cell.profile_alias,
                        "session_id": (
                            str(event.session_id)
                            if event.session_id is not None
                            else None
                        ),
                        "parent_tool_use_id": event.parent_tool_use_id,
                        "usage": event.usage,
                        "error_code": event.error_code,
                    },
                ),
            )
            if event.kind == "subagent.started":
                lease_id = event.parent_tool_use_id or str(uuid.uuid4())
                connection.execute(
                    "INSERT OR IGNORE INTO worker_leases("
                    "lease_id, worker_id, project_key, kind, state, acquired_at"
                    ") VALUES (?, ?, ?, 'claude_subagent', 'active', ?)",
                    (
                        lease_id,
                        lease_id,
                        cell.project_key,
                        self._aware_now().isoformat(),
                    ),
                )

    async def rotate(self, cell_id: str, handoff_id: str) -> ProjectCell:
        """Transfer a cell only after its replacement acknowledged the handoff."""

        if self._handoffs is None:
            raise RotationBlocked("handoff service is not configured")
        handoff = self._handoffs.get(handoff_id)
        if getattr(handoff, "cell_id", None) != cell_id:
            raise RotationBlocked("handoff belongs to another project cell")
        current = self._get_cell(cell_id)
        reservation = self._profiles.reserve_replacement(
            current.project_key,
            current.profile_alias,
        )
        if reservation is None:
            raise RotationBlocked("no different healthy profile is available")
        replacement_session = getattr(handoff, "replacement_session_id", None)
        already_acknowledged = getattr(handoff, "state", None) == "acknowledged"
        if not isinstance(replacement_session, UUID):
            replacement_session = self._replacement_session_ids()

        request = LeadTurnRequest(
            session_id=replacement_session,
            cwd=self._project_paths[current.project_key],
            prompt=(
                f"{getattr(handoff, 'markdown', '')}\n\n"
                "Acknowledge this handoff and restate the exact next action."
            ),
            profile_alias=reservation.profile_alias,
            output_schema={
                "type": "object",
                "properties": {
                    "acknowledged": {"const": True},
                    "restated_next_action": {"type": "string", "minLength": 1},
                },
                "required": ["acknowledged", "restated_next_action"],
                "additionalProperties": False,
            },
        )
        session_started = False
        acknowledged = already_acknowledged
        try:
            stream = self._runner.start_lead(request)
            async for event in stream:
                if event.kind == "session.started":
                    session_started = event.session_id == replacement_session
                elif (
                    event.kind == "handoff.acknowledged"
                    and event.session_id == replacement_session
                    and event.restated_next_action
                ):
                    self._handoffs.acknowledge(
                        handoff_id,
                        replacement_session,
                        event.restated_next_action,
                    )
                    acknowledged = True
        except BaseException:
            self._profiles.cancel_replacement(current.project_key)
            raise
        if not session_started:
            self._profiles.cancel_replacement(current.project_key)
            raise RotationBlocked("replacement session start was not confirmed")
        if not acknowledged:
            self._profiles.cancel_replacement(current.project_key)
            raise RotationBlocked("handoff was not acknowledged by the replacement")

        now = self._aware_now().isoformat()
        rotated = ProjectCell(
            cell_id=current.cell_id,
            project_key=current.project_key,
            state="active",
            profile_alias=reservation.profile_alias,
            session_id=replacement_session,
        )
        try:
            with self._database.transaction() as connection:
                connection.execute(
                    "DELETE FROM profile_leases WHERE profile_alias = ?",
                    (current.profile_alias,),
                )
                connection.execute(
                    "INSERT INTO profile_leases("
                    "profile_alias, project_key, state, acquired_at"
                    ") VALUES (?, ?, 'active', ?)",
                    (rotated.profile_alias, rotated.project_key, now),
                )
                connection.execute(
                    "UPDATE project_cells SET state = 'active', "
                    "profile_alias = ?, session_id = ?, updated_at = ? "
                    "WHERE cell_id = ?",
                    (
                        rotated.profile_alias,
                        str(rotated.session_id),
                        now,
                        rotated.cell_id,
                    ),
                )
                self._events.append(
                    connection,
                    EventInput(
                        event_type="project_cell.rotated",
                        aggregate_type="project_cell",
                        aggregate_id=rotated.cell_id,
                        payload={
                            "from_profile": current.profile_alias,
                            "to_profile": rotated.profile_alias,
                            "handoff_id": handoff_id,
                            "session_id": str(rotated.session_id),
                        },
                    ),
                )
        except BaseException:
            self._profiles.cancel_replacement(current.project_key)
            raise
        self._profiles.commit_rotation(
            current.project_key,
            current.profile_alias,
        )
        await self._runner.retire_session(current.session_id)
        return rotated

    def _find_active_cell(self, project_key: str) -> ProjectCell | None:
        placeholders = ",".join("?" for _ in _ACTIVE_CELL_STATES)
        row = self._database.execute(
            f"SELECT * FROM project_cells WHERE project_key = ? "
            f"AND state IN ({placeholders})",
            (project_key, *_ACTIVE_CELL_STATES),
        ).fetchone()
        return self._row_to_cell(row) if row is not None else None

    def _restore_profile_leases(self) -> None:
        rows = self._database.execute(
            "SELECT profile_alias, project_key, acquired_at, cooldown_until "
            "FROM profile_leases WHERE state IN ('active', 'capped')"
        ).fetchall()
        for row in rows:
            cooldown = row["cooldown_until"]
            self._profiles.restore(
                project_key=str(row["project_key"]),
                profile_alias=str(row["profile_alias"]),
                acquired_at=datetime.fromisoformat(str(row["acquired_at"])),
                cooldown_until=(
                    datetime.fromisoformat(str(cooldown)) if cooldown else None
                ),
            )

    def _get_cell(self, cell_id: str) -> ProjectCell:
        row = self._database.execute(
            "SELECT * FROM project_cells WHERE cell_id = ?",
            (cell_id,),
        ).fetchone()
        if row is None:
            raise KeyError(cell_id)
        return self._row_to_cell(row)

    def _create_cell(self, project_key: str) -> ProjectCell | None:
        lease = self._profiles.acquire(project_key)
        if lease is None:
            return None
        now = self._aware_now().isoformat()
        cell = ProjectCell(
            cell_id=self._cell_ids(),
            project_key=project_key,
            state="starting",
            profile_alias=lease.profile_alias,
            session_id=self._session_ids(),
        )
        try:
            with self._database.transaction() as connection:
                connection.execute(
                    "INSERT INTO project_cells("
                    "cell_id, project_key, state, profile_alias, session_id, "
                    "created_at, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        cell.cell_id,
                        cell.project_key,
                        cell.state,
                        cell.profile_alias,
                        str(cell.session_id),
                        now,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO profile_leases("
                    "profile_alias, project_key, state, acquired_at"
                    ") VALUES (?, ?, 'active', ?)",
                    (cell.profile_alias, cell.project_key, now),
                )
                self._events.append(
                    connection,
                    EventInput(
                        event_type="project_cell.started",
                        aggregate_type="project_cell",
                        aggregate_id=cell.cell_id,
                        payload={
                            "project_key": project_key,
                            "profile_alias": cell.profile_alias,
                            "session_id": str(cell.session_id),
                        },
                    ),
                )
        except sqlite3.IntegrityError:
            self._profiles.release(project_key, "cell_creation_conflict")
            existing = self._find_active_cell(project_key)
            if existing is not None:
                return existing
            raise
        return cell

    def _activate_issue(self, cell: ProjectCell, issue_id: str) -> None:
        now = self._aware_now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE project_cells SET state = 'active', updated_at = ? "
                "WHERE cell_id = ?",
                (now, cell.cell_id),
            )
            connection.execute(
                "UPDATE admitted_issues SET state = ?, updated_at = ? "
                "WHERE issue_id = ?",
                (IssueState.IN_DEVELOPMENT.value, now, issue_id),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="issue.started",
                    aggregate_type="issue",
                    aggregate_id=issue_id,
                    payload={"cell_id": cell.cell_id},
                ),
            )

    def _require_handoff(self, cell: ProjectCell, reason: str) -> None:
        now = self._aware_now()
        cooldown_until = now + timedelta(hours=1)
        self._profiles.set_cooldown(cell.profile_alias, cooldown_until)
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE project_cells SET state = 'handoff_required', updated_at = ? "
                "WHERE cell_id = ?",
                (now.isoformat(), cell.cell_id),
            )
            connection.execute(
                "UPDATE profile_leases SET state = 'capped', cooldown_until = ? "
                "WHERE profile_alias = ?",
                (cooldown_until.isoformat(), cell.profile_alias),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="project_cell.handoff_required",
                    aggregate_type="project_cell",
                    aggregate_id=cell.cell_id,
                    payload={"reason": reason},
                ),
            )

    def _aware_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return value.astimezone(UTC)

    @staticmethod
    def _row_to_cell(row: sqlite3.Row) -> ProjectCell:
        return ProjectCell(
            cell_id=str(row["cell_id"]),
            project_key=str(row["project_key"]),
            state=str(row["state"]),
            profile_alias=str(row["profile_alias"]),
            session_id=UUID(str(row["session_id"])),
        )
