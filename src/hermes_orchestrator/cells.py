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


class LinearProjector(Protocol):
    """Approved Linear projection boundary used by project cells."""

    async def project(
        self,
        issue_id: str,
        target: LinearProjection,
        effect_id: str,
    ) -> object: ...


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

    def _find_active_cell(self, project_key: str) -> ProjectCell | None:
        placeholders = ",".join("?" for _ in _ACTIVE_CELL_STATES)
        row = self._database.execute(
            f"SELECT * FROM project_cells WHERE project_key = ? "
            f"AND state IN ({placeholders})",
            (project_key, *_ACTIVE_CELL_STATES),
        ).fetchone()
        return self._row_to_cell(row) if row is not None else None

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
