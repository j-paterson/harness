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
repeating already-durable work. A seat is not reported ``complete``
until the replacement session's active ``channel_registrations`` row
is durably observed (a bounded, injectable-sleep wait) — a rotation
whose seat came up without its hermes-control channel is reported
blocked, never a false success, and rerunning once the registration
lands completes idempotently.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from hermes_orchestrator.cells import ProjectCellService, RotationBlocked
from hermes_orchestrator.cmux_surfaces import (
    CmuxSurfaceBindings,
    classic_resume_command,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.handoffs import HandoffService

# Mirrors cells._ACTIVE_CELL_STATES: the lifecycle states in which a
# project cell is understood to hold a live, addressable lead — never
# reimplemented here, just read back the same way dispatch() does.
_LIVE_CELL_STATES = ("starting", "active", "handoff_required", "paused")

_RESUMABLE_HANDOFF_STATES = ("submitted", "acknowledged")

# Margin added to ``registration_wait_seconds`` to form the rotation
# claim-age bound (see ``LeadRotation._claim_attempt``). Between the
# claim and the transactional transfer a live rotation runs at most the
# replacement acknowledgement turn plus seat launch plus the bounded
# registration wait; five minutes of slack over the registration wait
# is conservative for all of that, so a younger open attempt is treated
# as a LIVE claimant and a concurrent request fails closed.
_CLAIM_AGE_MARGIN_SECONDS = 300.0


def live_cell_project_key(database: Database, cell_id: str) -> str | None:
    """Read the durable project key for ``cell_id`` without requiring an
    active cmux lead binding.

    ``rotate-lead`` used to demand ``active_lead(cell_id)`` before it
    would even construct :class:`LeadRotation`, so a lost or
    never-registered classic seat refused the command outright — even
    when the durable cell and its acknowledged handoff already named
    the exact replacement to reseat (Sol a06cbce0). Project identity
    only needs to survive long enough to look up ``settings.projects``;
    :meth:`LeadRotation.rotate` re-derives every other fact itself from
    ``project_cells`` and ``handoffs``. Returns ``None`` when the cell
    does not exist or is not in a live state, so the caller can fail
    closed exactly as it already does for a missing binding.
    """

    row = database.execute(
        "SELECT project_key, state FROM project_cells WHERE cell_id = ?",
        (cell_id,),
    ).fetchone()
    if row is None or str(row["state"]) not in _LIVE_CELL_STATES:
        return None
    return str(row["project_key"])


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
        registration_wait_seconds: float = 60.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._database = database
        self._events = EventStore(database)
        self._handoffs = handoffs
        self._cells = cells
        self._bindings = bindings
        self._seater = seater
        self._worktree_state = worktree_state
        self._registration_wait_seconds = registration_wait_seconds
        self._sleep = sleep

    async def rotate(self, cell_id: str) -> RotationReport:
        """Advance one cell's lead rotation exactly as far as durable
        state allows, resuming idempotently from wherever a prior run
        (or this same run, replayed) left off — up to, but not through,
        a completed rotation's own terminal report (Sol correction
        3c1651df; see the ``transfer_already_committed`` branch below)."""

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
        transfer_already_committed = (
            handoff.state == "acknowledged"
            and replacement_from_handoff is not None
            and replacement_from_handoff == incumbent_session
        )
        if transfer_already_committed:
            # cells.rotate()'s transactional transfer already committed
            # on a prior pass: the durable cell session/profile ARE this
            # handoff's replacement. The cell/handoff rows alone cannot
            # say WHICH pass produced that shape — an in-flight rotation
            # resuming after a crash or a registration wait looks
            # byte-identical to an old, fully reported rotation whose
            # handoff merely remains the newest row (Sol correction
            # 3c1651df: the live defect — a stale acknowledged handoff
            # returned ok=true for a brand-new rotate-lead request while
            # nothing rotated). The attempt journal is the durable
            # discriminator: every rotation journals
            # ``lead_rotation.attempt`` (write-ahead, before the
            # transfer) and closes it with ``lead_rotation.completed``
            # alongside the one ``complete`` report. An OPEN attempt for
            # this handoff means the in-flight rotation may resume
            # through the seat/registration phases below; no open
            # attempt means the handoff was already consumed (or predates
            # the attempt journal) and can never satisfy a new request —
            # fail closed and demand a fresh handoff.
            if not self._attempt_open(handoff_id):
                return self._blocked(
                    cell_id,
                    handoff_id,
                    phase="precondition",
                    failure=(
                        f"handoff {handoff_id!r} already completed a "
                        "rotation onto the current incumbent "
                        f"({incumbent_profile!r} / {incumbent_session}); a "
                        "stale acknowledged handoff cannot satisfy a new "
                        "rotation request. Submit a fresh handoff naming "
                        "a new replacement for this cell, then retry "
                        "rotate-lead"
                    ),
                )
            replacement_session = incumbent_session
            profile_alias = incumbent_profile
        else:
            # Sol 524a38ed finding 3: the claim is acquired atomically,
            # BEFORE cells.rotate or any runner/seat launch, so two
            # concurrent rotate-lead processes can never both transfer.
            refusal = self._claim_attempt(cell_id, handoff_id)
            if refusal is not None:
                return self._blocked(
                    cell_id, handoff_id, phase="claim", failure=refusal
                )
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
            return await self._completion_report(
                cell_id=cell_id,
                handoff_id=handoff_id,
                replacement_session=replacement_session,
                profile_alias=profile_alias,
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
        return await self._completion_report(
            cell_id=cell_id,
            handoff_id=handoff_id,
            replacement_session=replacement_session,
            profile_alias=profile_alias,
            binding_id=binding.binding_id,
        )

    async def _completion_report(
        self,
        *,
        cell_id: str,
        handoff_id: str,
        replacement_session: str,
        profile_alias: str | None,
        binding_id: str,
    ) -> RotationReport:
        """Report ``complete`` only once the replacement session's active
        hermes-control registration is durably observed — a bound seat
        without its channel is reported blocked instead of a false
        success. The seat itself is never retired, closed, or otherwise
        modified here: a rerun re-enters the idempotent short-circuit
        above and re-checks the registration."""

        if await self._replacement_is_registered(replacement_session):
            self._journal_completed(cell_id, handoff_id, replacement_session)
            return RotationReport(
                ok=True,
                phase="complete",
                cell_id=cell_id,
                handoff_id=handoff_id,
                replacement_session=replacement_session,
                profile=profile_alias,
                binding_id=binding_id,
            )
        return RotationReport(
            ok=False,
            phase="channel_registration",
            cell_id=cell_id,
            handoff_id=handoff_id,
            replacement_session=replacement_session,
            profile=profile_alias,
            binding_id=binding_id,
            failure=(
                f"replacement session {replacement_session} has no active "
                "hermes-control registration after "
                f"{self._registration_wait_seconds:g}s; the seat stays "
                "bound and drains on the Stop-hook poll; rerun rotate-lead "
                "once the channel launch is repaired"
            ),
        )

    async def _replacement_is_registered(self, session_id: str) -> bool:
        """Poll immediately, then every 1.0s (via the injected sleep)
        until an active registration for ``session_id`` is observed or
        ``registration_wait_seconds`` elapses (0 checks exactly once)."""

        elapsed = 0.0
        while True:
            if self._database.execute(
                "SELECT 1 FROM channel_registrations "
                "WHERE session_id = ? AND state = 'active' LIMIT 1",
                (session_id,),
            ).fetchone() is not None:
                return True
            if elapsed >= self._registration_wait_seconds:
                return False
            await self._sleep(1.0)
            elapsed += 1.0

    def _attempt_open(self, handoff_id: str) -> bool:
        """True while a journaled ``lead_rotation.attempt`` for
        ``handoff_id`` has no matching ``lead_rotation.completed``. Only
        such an open attempt may resume through the seat and
        registration phases; a handoff without one — including every
        handoff consumed before the attempt journal existed — never
        satisfies a new rotation request (Sol correction 3c1651df)."""

        return self._database.execute(
            "SELECT 1 FROM events WHERE event_type = 'lead_rotation.attempt' "
            "AND aggregate_id = ? AND NOT EXISTS ("
            "SELECT 1 FROM events AS closed "
            "WHERE closed.event_type = 'lead_rotation.completed' "
            "AND closed.aggregate_id = ?) LIMIT 1",
            (handoff_id, handoff_id),
        ).fetchone() is not None

    def _claim_attempt(self, cell_id: str, handoff_id: str) -> str | None:
        """Atomically claim the exclusive rotation attempt for this handoff.

        Sol 524a38ed finding 3: the open-attempt check used to run
        OUTSIDE the append transaction, so two concurrent rotate-lead
        processes could both observe "no open attempt", both journal one,
        and both enter ``cells.rotate`` — potentially launching two
        replacements. Here the check AND the write-ahead
        ``lead_rotation.attempt`` append run inside ONE
        ``database.transaction()``: ``BEGIN IMMEDIATE`` serializes
        writers, so exactly one caller finds no open attempt and appends
        it (the winner); every concurrent caller's own atomic
        check-then-append observes an open attempt it did not create.
        The claim is acquired before ``cells.rotate`` or any runner/seat
        launch, so the loser fails closed with zero side effects.

        Claim-age rule: an open attempt does not by itself prove a LIVE
        claimant — the winner may have crashed before closing it. The
        attempt event's ``occurred_at`` is the discriminator, bounded by
        ``registration_wait_seconds + 300s``: between claiming and the
        transactional transfer (the only window this claim guards — a
        committed transfer resumes through the ``transfer_already_
        committed`` branch instead), a live rotation runs at most the
        replacement acknowledgement turn, the seat launch, and the
        bounded registration wait, so five minutes of margin over the
        registration wait is conservative. An attempt YOUNGER than the
        bound presumes a live claimant and this call fails closed
        (returns the refusal reason); an OLDER one presumes the claimant
        dead and the retry adopts the open attempt (no second append —
        the journal keeps exactly one open attempt per handoff) and
        resumes. Adoption is safe because nothing pre-transfer is
        externally visible and every later phase derives from durable
        rows. An unparsable timestamp fails closed. Returns ``None``
        when the claim is acquired, fresh or adopted.
        """

        now = datetime.now(UTC)
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT occurred_at FROM events "
                "WHERE event_type = 'lead_rotation.attempt' "
                "AND aggregate_id = ? AND NOT EXISTS ("
                "SELECT 1 FROM events AS closed "
                "WHERE closed.event_type = 'lead_rotation.completed' "
                "AND closed.aggregate_id = ?) "
                "ORDER BY sequence DESC LIMIT 1",
                (handoff_id, handoff_id),
            ).fetchone()
            if row is not None:
                bound = (
                    self._registration_wait_seconds + _CLAIM_AGE_MARGIN_SECONDS
                )
                try:
                    occurred = datetime.fromisoformat(str(row["occurred_at"]))
                    if occurred.tzinfo is None:
                        occurred = occurred.replace(tzinfo=UTC)
                    age = (now - occurred).total_seconds()
                except ValueError:
                    age = None
                if age is None or age <= bound:
                    return (
                        "rotation attempt already in progress for handoff "
                        f"{handoff_id!r}: another rotate-lead holds the "
                        "journaled claim (within the "
                        f"{bound:g}s liveness bound); nothing was "
                        "transferred or launched. Retry once it completes "
                        "or its claim ages out"
                    )
                # Aged claim: the claimant is presumed dead; adopt its
                # open attempt and resume without a second append.
                return None
            self._events.append(
                connection,
                EventInput(
                    event_type="lead_rotation.attempt",
                    aggregate_type="handoff",
                    aggregate_id=handoff_id,
                    payload={"cell_id": cell_id},
                ),
            )
        return None

    def _journal_completed(
        self, cell_id: str, handoff_id: str, replacement_session: str
    ) -> None:
        """Close the attempt with the one ``complete`` report, once."""

        if not self._attempt_open(handoff_id):
            return
        with self._database.transaction() as connection:
            self._events.append(
                connection,
                EventInput(
                    event_type="lead_rotation.completed",
                    aggregate_type="handoff",
                    aggregate_id=handoff_id,
                    payload={
                        "cell_id": cell_id,
                        "replacement_session": replacement_session,
                    },
                ),
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
