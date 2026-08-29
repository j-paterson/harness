"""Durable one-lead-per-project cell lifecycle."""

from __future__ import annotations

import asyncio
import sqlite3
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from uuid import UUID

from hermes_orchestrator.checkpoints import CheckpointRequests, CheckpointSafetyStore
from hermes_orchestrator.claude import ClaudeEvent, LeadTurnRequest
from hermes_orchestrator.cmux_surfaces import classic_resume_command
from hermes_orchestrator.context import ActiveTimeTracker, ContextMonitor, ContextSignal
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import IssueState
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.lead_wakes import TerminalWakeInput
from hermes_orchestrator.linear import LinearProjection
from hermes_orchestrator.operator_decisions import OperatorDecisions
from hermes_orchestrator.profiles import ProfilePool
from hermes_orchestrator.queue import QueueService

_ACTIVE_CELL_STATES = ("starting", "active", "handoff_required", "paused")


class LeadStream(Protocol):
    """A lead event stream with deterministic process cleanup."""

    def __aiter__(self) -> AsyncIterator[ClaudeEvent]: ...

    async def __anext__(self) -> ClaudeEvent: ...

    async def aclose(self) -> None: ...


class LeadRunner(Protocol):
    """Claude lead execution boundary used by project cells."""

    def start_lead(self, request: LeadTurnRequest) -> LeadStream: ...

    def resume_lead(
        self,
        session_id: UUID,
        request: LeadTurnRequest,
    ) -> LeadStream: ...

    async def retire_session(self, session_id: UUID) -> None: ...


class LeadSeatEnsurer(Protocol):
    """The visible terminal seat a lead session runs in (or beside)."""

    async def ensure(
        self,
        *,
        project_key: str,
        cell_id: str,
        session_id: str,
        profile_alias: str,
        issue_id: str,
        classic_command: str | None = None,
    ) -> object | None: ...


class LinearProjector(Protocol):
    """Approved Linear projection boundary used by project cells."""

    async def validate(self, project_key: str, issue_id: str) -> object: ...

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


class LeadCompletionSink(Protocol):
    """Durable terminal-wake publication boundary used by project cells.

    Transport-agnostic by design: the sink owns persistence, deduplication
    by turn identity, and any in-process signalling. Cells only report
    that one lead turn reached a terminal boundary.
    """

    def commit(self, wake: TerminalWakeInput) -> object: ...


class RotationBlocked(RuntimeError):
    """The old lead must remain active because rotation is not yet safe."""


class _IssueAlreadyCompleted(RuntimeError):
    """Dispatch lost the race to an explicit terminal reconciliation."""


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
        safety: CheckpointSafetyStore | None = None,
        checkpoints: CheckpointRequests | None = None,
        context: ContextMonitor | None = None,
        active_time: ActiveTimeTracker | None = None,
        context_window_tokens: int = 200_000,
        replacement_session_ids: Callable[[], UUID] = uuid.uuid4,
        completion_sink: LeadCompletionSink | None = None,
        surfaces: LeadSeatEnsurer | None = None,
        classic_seats: bool = False,
        decisions: OperatorDecisions | None = None,
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
        self._safety = safety
        self._checkpoints = checkpoints
        self._context = context
        self._active_time = active_time
        self._context_window_tokens = context_window_tokens
        self._replacement_session_ids = replacement_session_ids
        self._completion_sink = completion_sink
        self._surfaces = surfaces
        self._classic_seats = classic_seats
        self._decisions = decisions
        self._dispatch_locks: dict[str, asyncio.Lock] = {}
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
        if self._decisions is not None:
            pending = self._decisions.pending_for_issue(issue_id)
            if pending:
                return DispatchResult(
                    status="awaiting_operator_decision", issue_id=issue_id
                )
        lock = self._dispatch_locks.setdefault(issue.project_key, asyncio.Lock())
        async with lock:
            return await self._dispatch_locked(issue_id)

    async def _dispatch_locked(self, issue_id: str) -> DispatchResult:
        """Dispatch one issue while holding its project-wide in-process lock."""

        issue = self._queue.get(issue_id)
        await self._linear.validate(issue.project_key, issue_id)
        cell = self._find_active_cell(issue.project_key)
        created = False
        if cell is None:
            try:
                cell, created = self._create_cell(issue.project_key, issue_id)
            except _IssueAlreadyCompleted:
                return DispatchResult(status="already_completed", issue_id=issue_id)
            if cell is None:
                return DispatchResult(status="waiting_for_profile", issue_id=issue_id)
        elif cell.state == "starting":
            return DispatchResult(
                status="start_reconciliation_required",
                issue_id=issue_id,
                cell_id=cell.cell_id,
                session_id=cell.session_id,
                profile_alias=cell.profile_alias,
            )
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
            project_key=issue.project_key,
        )
        session_confirmed = False
        handoff_required = False
        issue_completed = False
        start_capped = False
        limit_kind: str | None = None
        # New work starts: any prior safe-boundary proof is no longer current.
        if self._safety is not None:
            self._safety.invalidate(cell.cell_id, reason=f"dispatch:{issue_id}")
        last_event_id: str | None = None
        # Durable context state is the truth: a session already at
        # rotation_pending/rotate_now must be operationally frozen before the
        # lead can run and admit any Agent assignment, even if the process
        # stopped between the durable transition and the marker write.
        self.reconcile_freeze(cell)
        # The ordering/compensation boundary between seat activation and
        # lead launch: with terminal visibility configured, the visible
        # seat activates BEFORE any lead process exists, and a failed
        # activation refuses the launch entirely — a lead without its
        # seat would be a hidden process no operator can see. There is
        # nothing to compensate on this side of the boundary because
        # nothing has been launched yet.
        if self._surfaces is not None:
            seated = await self._ensure_lead_seat(
                cell, issue_id, resume=not created
            )
            if not seated:
                if created:
                    self._fail_unconfirmed_start(cell, issue_id=issue_id)
                return DispatchResult(
                    status="seat_failed",
                    issue_id=issue_id,
                    cell_id=cell.cell_id,
                    session_id=cell.session_id,
                    profile_alias=cell.profile_alias,
                )
            if self._classic_seats:
                # The pane itself runs the classic interactive lead; the
                # daemon never launches a -p/stream-JSON shadow of it.
                await self._linear.project(
                    issue_id,
                    LinearProjection(
                        status="In Development",
                        assignee_alias="operator",
                    ),
                    effect_id=f"linear:{issue_id}:in-development:v2",
                )
                if self._activate_issue(cell, issue_id):
                    return DispatchResult(
                        status="seated",
                        issue_id=issue_id,
                        cell_id=cell.cell_id,
                        session_id=cell.session_id,
                        profile_alias=cell.profile_alias,
                    )
                issue_completed = (
                    self._queue.get(issue_id).state == IssueState.DONE
                )
                if issue_completed:
                    self._record_issue_already_completed(cell, issue_id)
                elif created:
                    self._fail_unconfirmed_start(cell, issue_id=issue_id)
                return DispatchResult(
                    status=(
                        "already_completed"
                        if issue_completed
                        else "start_failed"
                    ),
                    issue_id=issue_id,
                    cell_id=cell.cell_id,
                    session_id=cell.session_id,
                    profile_alias=cell.profile_alias,
                )
        try:
            stream = (
                self._runner.start_lead(request)
                if created
                else self._runner.resume_lead(cell.session_id, request)
            )
            try:
                async for event in stream:
                    # Context is observed before any worker lease can be
                    # created, so a threshold crossed by this very event
                    # already freezes new assignments.
                    if session_confirmed or event.kind == "session.started":
                        self._observe_context(cell, event, safe_boundary=False)
                    last_event_id = self.record(cell, event)
                    if event.kind == "session.started":
                        if event.session_id != cell.session_id:
                            raise RuntimeError(
                                "Claude session id did not match its project cell"
                            )
                        await self._linear.project(
                            issue_id,
                            LinearProjection(
                                status="In Development",
                                assignee_alias="operator",
                            ),
                            effect_id=f"linear:{issue_id}:in-development:v2",
                        )
                        if self._activate_issue(cell, issue_id):
                            session_confirmed = True
                            if self._active_time is not None:
                                self._active_time.open(
                                    self._worker_key(cell), self._aware_now()
                                )
                        else:
                            issue_completed = (
                                self._queue.get(issue_id).state == IssueState.DONE
                            )
                            if issue_completed:
                                self._record_issue_already_completed(
                                    cell, issue_id
                                )
                    elif event.kind == "provider.limit":
                        limit_kind = event.limit_kind
                        if session_confirmed:
                            handoff_required = True
                            self._require_handoff(cell, "subscription_limit")
                        else:
                            start_capped = True
                            break
            finally:
                await stream.aclose()
                if self._active_time is not None and session_confirmed:
                    self._active_time.idle(self._worker_key(cell), self._aware_now())
        except BaseException:
            if created and not session_confirmed:
                self._fail_unconfirmed_start(
                    cell, issue_id=issue_id, already_completed=issue_completed
                )
            raise

        if created and not session_confirmed and not handoff_required:
            self._fail_unconfirmed_start(
                cell,
                issue_id=issue_id,
                capped=start_capped,
                already_completed=issue_completed,
            )

        if issue_completed:
            status = "already_completed"
        elif handoff_required:
            status = "handoff_required"
        elif session_confirmed:
            status = "working"
            # The turn ended normally: a durable, session-bound safe boundary.
            if self._safety is not None and last_event_id is not None:
                self._safety.mark_safe(
                    cell.cell_id,
                    str(cell.session_id),
                    boundary_kind="turn_completed",
                    evidence_id=last_event_id,
                )
            if self._observe_context(cell, None, safe_boundary=True):
                status = "handoff_required"
        else:
            status = "start_unconfirmed"
        if self._completion_sink is not None:
            try:
                self._publish_terminal_wake(
                    cell,
                    issue_id,
                    status=status,
                    provider_capped=handoff_required or start_capped,
                    limit_kind=limit_kind,
                    # Terminal transitions rebind this to their own durable
                    # evidence event; the fresh fallback key covers only a
                    # turn that left no evidence at all, so repeated failures
                    # still each wake Hermes instead of deduplicating into
                    # the first one.
                    turn_key=last_event_id or f"start:{uuid.uuid4().hex}",
                )
            except Exception:
                # Publication is a side effect of an already-durable terminal
                # state; a sink failure must not destroy the turn's result.
                # The row is reconstructible from the journal and the Hermes
                # pending_wakes surface tolerates its absence.
                with suppress(Exception), (
                    self._database.transaction()
                ) as connection:
                    self._events.append(
                        connection,
                        EventInput(
                            event_type="lead_wake.publish_failed",
                            aggregate_type="project_cell",
                            aggregate_id=cell.cell_id,
                            payload={"issue_id": issue_id, "status": status},
                        ),
                    )
        return DispatchResult(
            status=status,
            issue_id=issue_id,
            cell_id=cell.cell_id,
            session_id=cell.session_id,
            profile_alias=cell.profile_alias,
        )

    def _publish_terminal_wake(
        self,
        cell: ProjectCell,
        issue_id: str,
        *,
        status: str,
        provider_capped: bool,
        limit_kind: str | None,
        turn_key: str,
    ) -> None:
        """Publish exactly one durable wake for this turn's terminal boundary.

        The sink deduplicates by project/cell/session/turn/kind, so a
        repeated publication of the same terminal state is a no-op. A
        provider cap's reset metadata is read back from the durable profile
        lease rather than recomputed, so the wake and the cooldown can
        never disagree.
        """

        assert self._completion_sink is not None
        if status == "working":
            kind, reason = "completed", "turn_completed"
        elif status == "already_completed":
            kind, reason = "completed", "issue_already_completed"
        elif provider_capped:
            kind = "provider_capped"
            reason = (
                "subscription_limit"
                if limit_kind is None
                else f"subscription_limit:{limit_kind}"
            )
        elif status == "handoff_required":
            kind, reason = "handoff_required", "context_rotation"
        else:
            kind, reason = "blocked", "start_unconfirmed"
        # A terminal transition binds the wake to its own durable evidence
        # event: the same identity a repair pass derives, so a wake lost
        # between the transition and this insert reconstructs into exactly
        # one deduplicated row.
        if status == "handoff_required":
            evidence = self._latest_terminal_evidence_id(
                cell.cell_id, "project_cell.handoff_required"
            )
            if evidence is not None:
                turn_key = f"handoff:{evidence}"
        elif status == "already_completed":
            evidence = self._latest_terminal_evidence_id(
                cell.cell_id, "project_cell.issue_already_completed"
            )
            if evidence is not None:
                turn_key = f"already_completed:{evidence}"
        elif status == "start_unconfirmed":
            evidence = self._latest_terminal_evidence_id(
                cell.cell_id, "project_cell.start_failed"
            )
            if evidence is not None:
                turn_key = f"start_failed:{evidence}"
        reset_at: str | None = None
        if kind == "provider_capped":
            row = self._database.execute(
                "SELECT cooldown_until FROM profile_leases "
                "WHERE profile_alias = ?",
                (cell.profile_alias,),
            ).fetchone()
            if row is not None and row["cooldown_until"]:
                reset_at = str(row["cooldown_until"])
        self._completion_sink.commit(
            TerminalWakeInput(
                project_key=cell.project_key,
                issue_id=issue_id,
                cell_id=cell.cell_id,
                session_id=cell.session_id,
                profile_alias=cell.profile_alias,
                turn_key=turn_key,
                kind=kind,
                reason=reason,
                reset_at=reset_at,
            )
        )

    async def _ensure_lead_seat(
        self, cell: ProjectCell, issue_id: str, *, resume: bool
    ) -> bool:
        """Activate the lead's visible seat before any process launch.

        Returns False — journaling one ``cmux.seat_failed`` event — when
        the seat cannot be activated. The caller must then refuse the
        launch: with visibility configured, a lead without its seat
        would be a hidden process. In classic mode the seat's workspace
        runs the sanitized native TUI command for this exact session.
        """

        if self._surfaces is None:
            return True
        classic_command = (
            classic_resume_command(str(cell.session_id), resume=resume)
            if self._classic_seats
            else None
        )
        try:
            seat = await self._surfaces.ensure(
                project_key=cell.project_key,
                cell_id=cell.cell_id,
                session_id=str(cell.session_id),
                profile_alias=cell.profile_alias,
                issue_id=issue_id,
                classic_command=classic_command,
            )
        except Exception as error:
            self._journal_seat_failure(
                cell, issue_id, reason=type(error).__name__
            )
            return False
        if seat is None:
            self._journal_seat_failure(
                cell, issue_id, reason="unresolvable_identity"
            )
            return False
        return True

    def _journal_seat_failure(
        self, cell: ProjectCell, issue_id: str, *, reason: str
    ) -> None:
        with suppress(Exception), (
            self._database.transaction()
        ) as connection:
            self._events.append(
                connection,
                EventInput(
                    event_type="cmux.seat_failed",
                    aggregate_type="project_cell",
                    aggregate_id=cell.cell_id,
                    payload={"issue_id": issue_id, "reason": reason},
                ),
            )

    def _record_issue_already_completed(
        self, cell: ProjectCell, issue_id: str
    ) -> None:
        """Journal identity-complete already_completed terminal evidence.

        The direct wake binds its turn key to this event, and startup
        repair derives the identical project/issue/cell/session/turn/kind/
        reason from it, so a wake lost before its outbox insert is
        deterministically reconstructible without a redispatch.
        """

        with self._database.transaction() as connection:
            self._events.append(
                connection,
                EventInput(
                    event_type="project_cell.issue_already_completed",
                    aggregate_type="project_cell",
                    aggregate_id=cell.cell_id,
                    payload={
                        "project_key": cell.project_key,
                        "profile_alias": cell.profile_alias,
                        "issue_id": issue_id,
                        "session_id": str(cell.session_id),
                    },
                ),
            )

    def _latest_terminal_evidence_id(
        self, cell_id: str, event_type: str
    ) -> str | None:
        row = self._database.execute(
            "SELECT event_id FROM events "
            "WHERE aggregate_type = 'project_cell' AND aggregate_id = ? "
            "AND event_type = ? ORDER BY sequence DESC LIMIT 1",
            (cell_id, event_type),
        ).fetchone()
        return None if row is None else str(row["event_id"])

    def record(self, cell: ProjectCell, event: ClaudeEvent) -> str:
        """Journal a sanitized Claude event and any child worker observation.

        Returns the durable event id so callers can bind evidence to it.
        """

        with self._database.transaction() as connection:
            recorded = self._events.append(
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
                        "limit_kind": event.limit_kind,
                    },
                ),
            )
            if event.kind == "subagent.started" and self._assignments_frozen(cell):
                self._events.append(
                    connection,
                    EventInput(
                        event_type="subagent.rejected",
                        aggregate_type="project_cell",
                        aggregate_id=cell.cell_id,
                        payload={
                            "parent_tool_use_id": event.parent_tool_use_id,
                            "reason": "rotation_pending: new subwork is frozen",
                        },
                    ),
                )
            elif event.kind == "subagent.started":
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
        return recorded.event_id

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
            try:
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
            finally:
                await stream.aclose()
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
        if self._safety is not None:
            self._safety.invalidate(rotated.cell_id, reason="session_rotated")
        if self._context is not None:
            self._context.reset(self._worker_key(current), reason="rotated")
        thaw = getattr(self._runner, "thaw_assignments", None)
        if thaw is not None:
            thaw(current.session_id)
        if self._checkpoints is not None:
            self._checkpoints.resolve_for_cell(
                rotated.cell_id, outcome="completed", detail=f"handoff:{handoff_id}"
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
            "SELECT profile_alias, project_key, state, acquired_at, cooldown_until "
            "FROM profile_leases WHERE state IN ('active', 'capped')"
        ).fetchall()
        for row in rows:
            cooldown = row["cooldown_until"]
            if str(row["state"]) == "capped":
                if not cooldown:
                    raise ValueError("capped profile lease is missing cooldown_until")
                cooldown_until = datetime.fromisoformat(str(cooldown))
                active_cell = self._database.execute(
                    "SELECT 1 FROM project_cells WHERE project_key = ? "
                    "AND profile_alias = ? "
                    "AND state IN ('starting', 'active', 'handoff_required', 'paused')",
                    (str(row["project_key"]), str(row["profile_alias"])),
                ).fetchone()
                if active_cell is not None:
                    self._profiles.restore(
                        project_key=str(row["project_key"]),
                        profile_alias=str(row["profile_alias"]),
                        acquired_at=datetime.fromisoformat(str(row["acquired_at"])),
                        cooldown_until=cooldown_until,
                    )
                    continue
                if cooldown_until <= self._aware_now():
                    with self._database.transaction() as connection:
                        connection.execute(
                            "DELETE FROM profile_leases "
                            "WHERE profile_alias = ? AND state = 'capped'",
                            (str(row["profile_alias"]),),
                        )
                    continue
                self._profiles.set_cooldown(
                    str(row["profile_alias"]),
                    cooldown_until,
                )
                continue
            self._profiles.restore(
                project_key=str(row["project_key"]),
                profile_alias=str(row["profile_alias"]),
                acquired_at=datetime.fromisoformat(str(row["acquired_at"])),
                cooldown_until=(
                    datetime.fromisoformat(str(cooldown)) if cooldown else None
                ),
            )

    def _fail_unconfirmed_start(
        self,
        cell: ProjectCell,
        *,
        issue_id: str,
        capped: bool = False,
        already_completed: bool = False,
    ) -> bool:
        if self._safety is not None:
            self._safety.invalidate(cell.cell_id, reason="start_failed")
        if self._checkpoints is not None:
            self._checkpoints.resolve_for_cell(
                cell.cell_id, outcome="failed", detail="cell_start_failed"
            )
        now_value = self._aware_now()
        now = now_value.isoformat()
        cooldown_until = now_value + timedelta(hours=1) if capped else None
        with self._database.transaction() as connection:
            updated = connection.execute(
                "UPDATE project_cells SET state = 'failed', updated_at = ? "
                "WHERE cell_id = ? AND state = 'starting'",
                (now, cell.cell_id),
            )
            if updated.rowcount == 0:
                return False
            if capped:
                connection.execute(
                    "UPDATE profile_leases SET state = 'capped', cooldown_until = ? "
                    "WHERE project_key = ? AND profile_alias = ?",
                    (
                        cooldown_until.isoformat(),
                        cell.project_key,
                        cell.profile_alias,
                    ),
                )
            else:
                connection.execute(
                    "DELETE FROM profile_leases WHERE project_key = ? "
                    "AND profile_alias = ?",
                    (cell.project_key, cell.profile_alias),
                )
            self._events.append(
                connection,
                EventInput(
                    event_type="project_cell.start_failed",
                    aggregate_type="project_cell",
                    aggregate_id=cell.cell_id,
                    # The full wake identity rides on the terminal evidence,
                    # so a wake lost after this commit is reconstructible.
                    payload={
                        "project_key": cell.project_key,
                        "profile_alias": cell.profile_alias,
                        "issue_id": issue_id,
                        "session_id": str(cell.session_id),
                        # An already-completed issue owns this turn's wake
                        # through its dedicated terminal evidence; tagging
                        # the start failure keeps repair from also deriving
                        # a blocked or capped wake the direct path never
                        # published.
                        "reason": (
                            "issue_already_completed"
                            if already_completed
                            else "subscription_limit"
                            if capped
                            else "unconfirmed"
                        ),
                    },
                ),
            )
        if cooldown_until is not None:
            self._profiles.set_cooldown(cell.profile_alias, cooldown_until)
        self._profiles.release(cell.project_key, "lead_start_failed")
        return True

    def _get_cell(self, cell_id: str) -> ProjectCell:
        row = self._database.execute(
            "SELECT * FROM project_cells WHERE cell_id = ?",
            (cell_id,),
        ).fetchone()
        if row is None:
            raise KeyError(cell_id)
        return self._row_to_cell(row)

    def _create_cell(
        self,
        project_key: str,
        issue_id: str,
    ) -> tuple[ProjectCell | None, bool]:
        lease = self._profiles.acquire(project_key)
        if lease is None:
            return None, False
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
                issue_row = connection.execute(
                    "SELECT state FROM admitted_issues WHERE issue_id = ?",
                    (issue_id,),
                ).fetchone()
                if issue_row is None:
                    raise KeyError(issue_id)
                if str(issue_row["state"]) == IssueState.DONE.value:
                    raise _IssueAlreadyCompleted(issue_id)
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
                    "DELETE FROM profile_leases WHERE profile_alias = ? "
                    "AND state = 'capped' AND cooldown_until IS NOT NULL "
                    "AND cooldown_until <= ?",
                    (cell.profile_alias, now),
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
        except _IssueAlreadyCompleted:
            self._profiles.release(project_key, "issue_already_completed")
            raise
        except sqlite3.IntegrityError:
            self._profiles.release(project_key, "cell_creation_conflict")
            existing = self._find_active_cell(project_key)
            if existing is not None:
                return existing, False
            raise
        return cell, True

    def _activate_issue(self, cell: ProjectCell, issue_id: str) -> bool:
        now = self._aware_now().isoformat()
        with self._database.transaction() as connection:
            issue_row = connection.execute(
                "SELECT state FROM admitted_issues WHERE issue_id = ?",
                (issue_id,),
            ).fetchone()
            if issue_row is None:
                raise KeyError(issue_id)
            if str(issue_row["state"]) == IssueState.DONE.value:
                return False
            activated = connection.execute(
                "UPDATE project_cells SET state = 'active', updated_at = ? "
                "WHERE cell_id = ? AND state IN ('starting', 'active') "
                "AND EXISTS ("
                "SELECT 1 FROM profile_leases "
                "WHERE project_key = ? AND profile_alias = ? AND state = 'active'"
                ")",
                (now, cell.cell_id, cell.project_key, cell.profile_alias),
            )
            if activated.rowcount == 0:
                return False
            updated = connection.execute(
                "UPDATE admitted_issues SET state = ?, updated_at = ? "
                "WHERE issue_id = ? AND state != ?",
                (
                    IssueState.IN_DEVELOPMENT.value,
                    now,
                    issue_id,
                    IssueState.DONE.value,
                ),
            )
            if updated.rowcount == 0:
                return False
            self._events.append(
                connection,
                EventInput(
                    event_type="issue.started",
                    aggregate_type="issue",
                    aggregate_id=issue_id,
                    payload={"cell_id": cell.cell_id},
                ),
            )
        return True

    def _worker_key(self, cell: ProjectCell) -> str:
        return f"{cell.cell_id}:{cell.session_id}"

    def _observe_context(
        self, cell: ProjectCell, event: ClaudeEvent | None, *, safe_boundary: bool
    ) -> bool:
        """Feed one durable context signal; True when rotation is now required.

        Mid-turn signals never claim a safe boundary; a completed turn does.
        ``prepare`` requests a handoff draft without stopping assigned work,
        ``rotation_pending`` stops new subwork and waits, ``rotate_now``
        invokes the acknowledged handoff flow. A context error is an
        emergency: the handoff is required immediately.
        """

        if self._context is None:
            return False
        worker = self._worker_key(cell)
        now = self._aware_now()
        percent = None
        compaction = False
        context_error = False
        if event is not None:
            if event.kind == "context.compacted":
                compaction = True
            elif event.kind == "context.error":
                context_error = True
            elif (
                event.original_type == "assistant"
                and event.kind != "provider.limit"
                and event.parent_tool_use_id is None
                and event.usage
            ):
                # Only an individual top-level assistant invocation reports
                # this session's live context occupancy. A result record's
                # usage is the cumulative run total (all invocations plus
                # children), a user/tool-result record can forward a child
                # worker's usage, and a synthetic limit notice is not an
                # invocation at all: any of them would poison the sticky
                # monitor into a false rotation.
                occupied = (
                    event.usage.get("input_tokens", 0)
                    + event.usage.get("cache_read_input_tokens", 0)
                    + event.usage.get("cache_creation_input_tokens", 0)
                )
                if occupied > 0:
                    percent = 100.0 * occupied / self._context_window_tokens
        previous = self._context.state(worker)
        rapid_refill = False
        if percent is not None and previous in ("prepare", "rotation_pending"):
            row = self._database.execute(
                "SELECT compactions, last_percent FROM context_evidence "
                "WHERE worker_id = ?",
                (worker,),
            ).fetchone()
            if (
                row is not None
                and int(row["compactions"]) > 0
                and row["last_percent"] is not None
                and percent >= float(row["last_percent"]) + 20.0
            ):
                rapid_refill = True
        active_hours = None
        if self._active_time is not None:
            active_hours = (
                self._active_time.total(worker, now).total_seconds() / 3600.0
            )
        decision = self._context.record(
            ContextSignal(
                worker_id=worker,
                at=now,
                percent=percent,
                compaction=compaction,
                rapid_refill=rapid_refill,
                context_error=context_error,
                active_hours=active_hours,
                safe_boundary=safe_boundary,
            )
        )
        if decision.state == previous:
            return decision.state == "rotate_now"
        if decision.state == "prepare" and self._handoffs is not None and hasattr(
            self._handoffs, "request"
        ):
            self._handoffs.request(cell.cell_id, "context_prepare")
        if decision.state in ("rotation_pending", "rotate_now"):
            # Operational freeze reconciled from durable state: idempotent,
            # so the transition and any later replay converge on one marker.
            self.reconcile_freeze(cell, reasons=decision.reasons)
        if decision.state == "rotate_now":
            reason = "context_error" if context_error else "context_rotation"
            self._require_handoff(cell, reason)
            if self._handoffs is not None and hasattr(self._handoffs, "request"):
                self._handoffs.request(cell.cell_id, reason)
            return True
        return False

    def reconcile_freeze(
        self, cell: ProjectCell, *, reasons: tuple[str, ...] = ()
    ) -> bool:
        """Make the runner freeze marker match durable context state.

        Idempotent: when the session is durably at rotation_pending or
        rotate_now and the runner reports no marker, the marker is written
        and one ``assignments.frozen`` event is journaled; an existing
        marker is left alone and nothing is journaled. Returns True when
        the session is frozen after reconciliation.
        """

        if not self._assignments_frozen(cell):
            return False
        freeze = getattr(self._runner, "freeze_assignments", None)
        frozen_check = getattr(self._runner, "assignments_frozen", None)
        if freeze is None:
            return True
        if frozen_check is not None and frozen_check(cell.session_id):
            return True
        state = self._context.state(self._worker_key(cell))  # type: ignore[union-attr]
        reason = reasons[0] if reasons else "reconstructed from durable context state"
        freeze(cell.session_id, f"{state}: {reason}")
        with self._database.transaction() as connection:
            self._events.append(
                connection,
                EventInput(
                    event_type="assignments.frozen",
                    aggregate_type="project_cell",
                    aggregate_id=cell.cell_id,
                    payload={
                        "session_id": str(cell.session_id),
                        "state": state,
                        "reasons": list(reasons) or [reason],
                    },
                ),
            )
        return True

    def _assignments_frozen(self, cell: ProjectCell) -> bool:
        """Durable truth: the session's context decision has reached pending."""

        if self._context is None:
            return False
        return self._context.state(self._worker_key(cell)) in (
            "rotation_pending",
            "rotate_now",
        )

    def current_session(self, cell_id: str) -> str | None:
        """The session id of an active cell, else None (durable read)."""

        row = self._database.execute(
            "SELECT session_id FROM project_cells WHERE cell_id = ? "
            "AND state IN ('starting', 'active')",
            (cell_id,),
        ).fetchone()
        return None if row is None else str(row["session_id"])

    def request_checkpoint(self, cell_id: str, reason: str) -> bool:
        """Ask one active lead to checkpoint and hand off at a safe boundary.

        Used by resource governance at red pressure: the cell is marked
        handoff-required and a durable handoff request is journaled when a
        handoff service is configured. Returns False when the cell is not
        active. Never terminates a process.
        """

        cell = self._get_cell(cell_id)
        if cell.state not in ("starting", "active"):
            return False
        self._require_handoff(cell, reason)
        if self._handoffs is not None and hasattr(self._handoffs, "request"):
            self._handoffs.request(cell_id, reason)
        return True

    def _require_handoff(self, cell: ProjectCell, reason: str) -> None:
        now = self._aware_now()
        cooldown_until = now + timedelta(hours=1)
        with self._database.transaction() as connection:
            updated = connection.execute(
                "UPDATE project_cells SET state = 'handoff_required', updated_at = ? "
                "WHERE cell_id = ? AND state != 'handoff_required'",
                (now.isoformat(), cell.cell_id),
            )
            if updated.rowcount == 0:
                return
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
                    # The session binding makes this terminal evidence
                    # reconstructible into exactly this epoch's wake.
                    payload={
                        "reason": reason,
                        "session_id": str(cell.session_id),
                    },
                ),
            )
        self._profiles.set_cooldown(cell.profile_alias, cooldown_until)

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
