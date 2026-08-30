"""One first-class idempotent lead-rotation transition (INFRA-197).

:class:`LeadRotation` is a thin orchestration over EXISTING durable
primitives — it reimplements none of their logic:

- :class:`~hermes_orchestrator.cells.ProjectCellService` owns the
  replacement session's acknowledgement turn and the transactional
  lease/cell transfer (``rotate``).
- :class:`~hermes_orchestrator.handoffs.HandoffService` owns the
  handoff document and its acknowledgement state.
- :class:`~hermes_orchestrator.cmux_surfaces.CmuxSurfaceBindings` and
  :class:`~hermes_orchestrator.cmux_surfaces.CmuxLeadSeater` own the
  visible managed classic seat.

Every invocation derives its phase from durable rows alone — never from
in-memory phase flags — so a crash between any two of these primitives'
own transactions resumes correctly on the next call rather than
repeating already-durable work.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from hermes_orchestrator.cells import ProjectCellService, RotationBlocked
from hermes_orchestrator.cmux_surfaces import (
    CmuxSurfaceBindings,
    classic_resume_command,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.handoffs import HandoffService

# Mirrors cells._ACTIVE_CELL_STATES: the lifecycle states in which a
# project cell is understood to hold a live, addressable lead — never
# reimplemented here, just read back the same way dispatch() does.
_LIVE_CELL_STATES = ("starting", "active", "handoff_required", "paused")

_RESUMABLE_HANDOFF_STATES = ("submitted", "acknowledged")


class SeatEnsurer(Protocol):
    """The standard managed classic seat path used by lead rotation."""

    async def ensure(
        self,
        *,
        project_key: str,
        cell_id: str,
        session_id: str,
        profile_alias: str,
        classic_command: str | None = None,
    ) -> object | None: ...


@dataclass(frozen=True, slots=True)
class WorktreeState:
    """The project worktree facts the rotation precondition gate needs."""

    branch: str
    head: str
    origin_head: str
    dirty: bool


@dataclass(frozen=True, slots=True)
class RotationReport:
    """The outcome of one :meth:`LeadRotation.rotate` invocation."""

    ok: bool
    phase: str
    cell_id: str
    handoff_id: str | None = None
    replacement_session: str | None = None
    profile: str | None = None
    binding_id: str | None = None
    failure: str | None = None


class LeadRotation:
    """Compose the existing rotation primitives into one durable transition."""

    def __init__(
        self,
        *,
        database: Database,
        handoffs: HandoffService,
        cells: ProjectCellService,
        bindings: CmuxSurfaceBindings,
        seater: SeatEnsurer,
        worktree_state: Callable[[str], WorktreeState],
    ) -> None:
        self._database = database
        self._handoffs = handoffs
        self._cells = cells
        self._bindings = bindings
        self._seater = seater
        self._worktree_state = worktree_state

    async def rotate(self, cell_id: str) -> RotationReport:
        """Advance one cell's lead rotation exactly as far as durable
        state allows, resuming idempotently from wherever a prior run
        (or this same run, replayed) left off."""

        row = self._cell_row(cell_id)
        if row is None:
            return self._blocked(
                cell_id, None, phase="precondition",
                failure=f"cell {cell_id!r} does not exist",
            )
        state = str(row["state"])
        if state not in _LIVE_CELL_STATES:
            return self._blocked(
                cell_id, None, phase="precondition",
                failure=f"cell {cell_id!r} is not active (state={state!r})",
            )
        session_raw = row["session_id"]
        if session_raw is None:
            return self._blocked(
                cell_id, None, phase="precondition",
                failure=f"cell {cell_id!r} has no incumbent session",
            )
        project_key = str(row["project_key"])
        incumbent_session = str(session_raw)
        incumbent_profile = (
            str(row["profile_alias"]) if row["profile_alias"] is not None else None
        )

        handoff_id = self._newest_handoff_id(cell_id)
        if handoff_id is None:
            return self._blocked(
                cell_id, None, phase="precondition",
                failure="no handoff has been submitted for this cell",
            )
        handoff = self._handoffs.get(handoff_id)
        if handoff.state not in _RESUMABLE_HANDOFF_STATES:
            return self._blocked(
                cell_id, handoff_id, phase="precondition",
                failure=(
                    f"the newest handoff for this cell is {handoff.state!r}, "
                    "not submitted"
                ),
            )

        worktree = self._worktree_state(project_key)
        if worktree.dirty:
            return self._blocked(
                cell_id, handoff_id, phase="precondition",
                failure="project worktree has uncommitted changes; rotation refused",
            )
        if worktree.head != worktree.origin_head:
            return self._blocked(
                cell_id, handoff_id, phase="precondition",
                failure=(
                    "project HEAD does not match the pushed origin branch "
                    "head; push before rotating"
                ),
            )

        replacement_from_handoff = (
            str(handoff.replacement_session_id)
            if handoff.replacement_session_id is not None
            else None
        )
        already_rotated = (
            handoff.state == "acknowledged"
            and replacement_from_handoff is not None
            and replacement_from_handoff == incumbent_session
        )
        if already_rotated:
            # cells.rotate() already committed its transactional transfer
            # on a prior run: the durable session/profile ARE the
            # replacement's. Calling rotate() again would try to reserve
            # a replacement to exclude the profile the cell already
            # holds, which is not the transition rotate() performs.
            replacement_session = incumbent_session
            profile_alias = incumbent_profile
        else:
            try:
                rotated = await self._cells.rotate(cell_id, handoff_id)
            except RotationBlocked as error:
                return self._blocked(
                    cell_id, handoff_id, phase="transfer", failure=str(error)
                )
            replacement_session = str(rotated.session_id)
            profile_alias = rotated.profile_alias

        return await self._seat(
            cell_id=cell_id,
            handoff_id=handoff_id,
            project_key=project_key,
            replacement_session=replacement_session,
            profile_alias=profile_alias,
        )

    async def _seat(
        self,
        *,
        cell_id: str,
        handoff_id: str,
        project_key: str,
        replacement_session: str,
        profile_alias: str | None,
    ) -> RotationReport:
        # A durable read, not an in-memory flag: an active binding
        # already owned by the replacement session means a prior run (or
        # this one, replayed) already completed the seat phase, and no
        # further activation is attempted.
        existing_binding = self._bindings.active_lead(cell_id)
        if (
            existing_binding is not None
            and existing_binding.session_id == replacement_session
        ):
            return RotationReport(
                ok=True,
                phase="complete",
                cell_id=cell_id,
                handoff_id=handoff_id,
                replacement_session=replacement_session,
                profile=profile_alias,
                binding_id=existing_binding.binding_id,
            )
        classic_command = classic_resume_command(replacement_session, resume=True)
        try:
            binding = await self._seater.ensure(
                project_key=project_key,
                cell_id=cell_id,
                session_id=replacement_session,
                profile_alias=str(profile_alias),
                classic_command=classic_command,
            )
        except Exception as error:  # refused seat is a report, not a crash
            return RotationReport(
                ok=False,
                phase="seat",
                cell_id=cell_id,
                handoff_id=handoff_id,
                replacement_session=replacement_session,
                profile=profile_alias,
                failure=f"seat activation failed: {error}",
            )
        if binding is None:
            return RotationReport(
                ok=False,
                phase="seat",
                cell_id=cell_id,
                handoff_id=handoff_id,
                replacement_session=replacement_session,
                profile=profile_alias,
                failure="lead seat could not be activated for the replacement session",
            )
        return RotationReport(
            ok=True,
            phase="complete",
            cell_id=cell_id,
            handoff_id=handoff_id,
            replacement_session=replacement_session,
            profile=profile_alias,
            binding_id=binding.binding_id,
        )

    def _cell_row(self, cell_id: str) -> object | None:
        return self._database.execute(
            "SELECT * FROM project_cells WHERE cell_id = ?",
            (cell_id,),
        ).fetchone()

    def _newest_handoff_id(self, cell_id: str) -> str | None:
        row = self._database.execute(
            "SELECT handoff_id FROM handoffs WHERE cell_id = ? "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (cell_id,),
        ).fetchone()
        return None if row is None else str(row["handoff_id"])

    @staticmethod
    def _blocked(
        cell_id: str,
        handoff_id: str | None,
        *,
        phase: str,
        failure: str,
    ) -> RotationReport:
        return RotationReport(
            ok=False,
            phase=phase,
            cell_id=cell_id,
            handoff_id=handoff_id,
            failure=failure,
        )
