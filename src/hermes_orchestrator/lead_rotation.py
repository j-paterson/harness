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
import json
import sqlite3
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
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
# claim-EXPIRY bound (see ``LeadRotation._claim_attempt``). Sol 04d013b0
# finding 4: expiry is measured from the NEWEST renewal appended by the
# claim's current owner (or from the ownership event itself while no
# renewal exists), never from the original claim's wall-clock age alone
# — a live owner heartbeats through every long-running stretch (the
# in-transfer heartbeat task brackets the unbounded acknowledgement
# runner stream at ``_RENEWAL_INTERVAL_SECONDS``), so the bound only
# has to outlast one renewal gap plus scheduling slack, and five
# minutes over the registration wait is conservative for that.
_CLAIM_AGE_MARGIN_SECONDS = 300.0

# How often the in-transfer heartbeat task renews the journaled claim
# while ``cells.rotate`` runs. The replacement acknowledgement runner
# stream inside ``cells.rotate`` has no upper bound and offers
# LeadRotation no seam to renew from within a turn, so the heartbeat
# runs CONCURRENTLY with the awaited transfer — appends only ever
# interleave between transactions (every ``database.transaction()``
# block is synchronous), and the cadence keeps a live slow owner's
# newest renewal far inside the expiry bound.
_RENEWAL_INTERVAL_SECONDS = 60.0

# The events that carry ownership of one handoff's open rotation
# attempt: the write-ahead claim itself and any takeover of an expired
# claim. The NEWEST of these is the current owner.
_OWNERSHIP_EVENT_TYPES = ("lead_rotation.attempt", "lead_rotation.takeover")


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
        renewal_interval_seconds: float = _RENEWAL_INTERVAL_SECONDS,
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
        self._renewal_interval_seconds = renewal_interval_seconds

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
            # Post-transfer recovery holds no claim token: the claim
            # guards only the pre-transfer window, every phase from here
            # on is idempotent, and duplicate completion is excluded by
            # the atomic ``_journal_completed`` CAS instead.
            replacement_session = incumbent_session
            profile_alias = incumbent_profile
            claim_token: str | None = None
        else:
            # Sol 524a38ed finding 3: the claim is acquired atomically,
            # BEFORE cells.rotate or any runner/seat launch, so two
            # concurrent rotate-lead processes can never both transfer.
            refusal, claim_token = self._claim_attempt(cell_id, handoff_id)
            if refusal is not None:
                return self._blocked(
                    cell_id, handoff_id, phase="claim", failure=refusal
                )
            assert claim_token is not None
            # Sol 04d013b0 finding 4: the acknowledgement runner stream
            # inside cells.rotate has no upper bound and no seam for
            # LeadRotation to renew from within, so a concurrent
            # heartbeat task brackets the whole awaited transfer —
            # renewals interleave only between transactions, and a live
            # slow owner is never mistaken for a dead one.
            heartbeat = asyncio.ensure_future(
                self._renew_periodically(cell_id, handoff_id, claim_token)
            )
            try:
                rotated = await self._cells.rotate(cell_id, handoff_id)
            except RotationBlocked as error:
                return self._blocked(
                    cell_id, handoff_id, phase="transfer", failure=str(error)
                )
            finally:
                heartbeat.cancel()
                with suppress(asyncio.CancelledError):
                    await heartbeat
            # Renewal at the phase transition: the transfer committed.
            self._renew(cell_id, handoff_id, claim_token, phase="transferred")
            replacement_session = str(rotated.session_id)
            profile_alias = rotated.profile_alias

        return await self._seat(
            cell_id=cell_id,
            handoff_id=handoff_id,
            project_key=project_key,
            replacement_session=replacement_session,
            profile_alias=profile_alias,
            claim_token=claim_token,
        )

    async def _seat(
        self,
        *,
        cell_id: str,
        handoff_id: str,
        project_key: str,
        replacement_session: str,
        profile_alias: str | None,
        claim_token: str | None = None,
    ) -> RotationReport:
        if claim_token is not None:
            # Renewal at the phase transition: entering the seat phase.
            self._renew(cell_id, handoff_id, claim_token, phase="seat")
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
                claim_token=claim_token,
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
            claim_token=claim_token,
        )

    async def _completion_report(
        self,
        *,
        cell_id: str,
        handoff_id: str,
        replacement_session: str,
        profile_alias: str | None,
        binding_id: str,
        claim_token: str | None = None,
    ) -> RotationReport:
        """Report ``complete`` only once the replacement session's active
        hermes-control registration is durably observed — a bound seat
        without its channel is reported blocked instead of a false
        success. The seat itself is never retired, closed, or otherwise
        modified here: a rerun re-enters the idempotent short-circuit
        above and re-checks the registration. Exactly one caller ever
        reports ``complete``: the atomic ``_journal_completed`` CAS
        closes the attempt once, and a concurrent recovery call whose
        completion lost that race reports the loss instead of a second
        success (Sol 04d013b0 finding 5)."""

        if await self._replacement_is_registered(
            replacement_session,
            cell_id=cell_id,
            handoff_id=handoff_id,
            claim_token=claim_token,
        ):
            if not self._journal_completed(
                cell_id, handoff_id, replacement_session
            ):
                return RotationReport(
                    ok=False,
                    phase="completed_elsewhere",
                    cell_id=cell_id,
                    handoff_id=handoff_id,
                    replacement_session=replacement_session,
                    profile=profile_alias,
                    binding_id=binding_id,
                    failure=(
                        "a concurrent recovery call already journaled this "
                        "rotation's completion; the one complete report was "
                        "issued there and nothing further remains to do"
                    ),
                )
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

    async def _replacement_is_registered(
        self,
        session_id: str,
        *,
        cell_id: str | None = None,
        handoff_id: str | None = None,
        claim_token: str | None = None,
    ) -> bool:
        """Poll immediately, then every 1.0s (via the injected sleep)
        until an active registration for ``session_id`` is observed or
        ``registration_wait_seconds`` elapses (0 checks exactly once).
        A caller that still owns the journaled claim renews it on every
        wait iteration, so this bounded stretch never ages the claim
        toward a false expiry."""

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
            if claim_token is not None and cell_id and handoff_id:
                self._renew(
                    cell_id, handoff_id, claim_token, phase="registration_wait"
                )

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

    def _claim_attempt(
        self, cell_id: str, handoff_id: str
    ) -> tuple[str | None, str | None]:
        """Atomically claim exclusive rotation ownership for this handoff.

        Sol 524a38ed finding 3 established the write-ahead claim; Sol
        04d013b0 finding 4 makes it a durable exclusive OWNERSHIP
        protocol over the events journal. Ownership events —
        ``lead_rotation.attempt`` (the initial claim) and
        ``lead_rotation.takeover`` (adoption of an expired claim) —
        each carry a unique ``claim_token``; the NEWEST ownership event
        for an open attempt names the current owner, and the owner
        proves liveness by appending ``lead_rotation.renewed`` events
        carrying its token at every phase transition (plus the
        in-transfer heartbeat that brackets the unbounded
        acknowledgement stream).

        Everything here — the newest-ownership read, the expiry
        decision, and the append — runs inside ONE
        ``database.transaction()`` (``BEGIN IMMEDIATE`` takes the write
        lock at BEGIN), so writers are fully serialized:

        - No open attempt: append the claim; this caller owns it.
        - Open attempt whose owner is LIVE (its newest renewal — or the
          ownership event itself when none exists — is within
          ``registration_wait_seconds + 300s``): fail closed. A slow
          but renewing owner can therefore never be stolen on the
          original claim's wall-clock age alone.
        - Open attempt whose owner EXPIRED: an atomic takeover CAS. The
          same transaction re-verifies the target is still the newest
          ownership event AND that no takeover already references it
          (the NOT-EXISTS guard) before appending the takeover — so of
          any number of concurrent adopters exactly one appends, and
          every loser's own serialized transaction then observes a
          fresh takeover it did not create and fails closed.

        The claim guards only the pre-transfer window (a committed
        transfer resumes through the ``transfer_already_committed``
        branch, whose phases are idempotent and whose completion is a
        CAS). An unparsable timestamp fails closed. Returns
        ``(refusal_reason, None)`` on refusal or ``(None, claim_token)``
        when ownership is acquired, fresh or taken over.
        """

        now = datetime.now(UTC)
        token = uuid.uuid4().hex
        bound = self._registration_wait_seconds + _CLAIM_AGE_MARGIN_SECONDS
        refusal = (
            "rotation attempt already in progress for handoff "
            f"{handoff_id!r}: another rotate-lead holds the journaled "
            f"claim (renewed within the {bound:g}s liveness bound); "
            "nothing was transferred or launched. Retry once it "
            "completes or its claim expires without renewal"
        )
        placeholders = ",".join("?" for _ in _OWNERSHIP_EVENT_TYPES)
        with self._database.transaction() as connection:
            owner = connection.execute(
                "SELECT sequence, occurred_at, payload_json FROM events "
                f"WHERE event_type IN ({placeholders}) "
                "AND aggregate_id = ? AND NOT EXISTS ("
                "SELECT 1 FROM events AS closed "
                "WHERE closed.event_type = 'lead_rotation.completed' "
                "AND closed.aggregate_id = ?) "
                "ORDER BY sequence DESC LIMIT 1",
                (*_OWNERSHIP_EVENT_TYPES, handoff_id, handoff_id),
            ).fetchone()
            if owner is None:
                self._events.append(
                    connection,
                    EventInput(
                        event_type="lead_rotation.attempt",
                        aggregate_type="handoff",
                        aggregate_id=handoff_id,
                        payload={"cell_id": cell_id, "claim_token": token},
                    ),
                )
                return None, token
            owner_sequence = int(owner["sequence"])
            owner_token = _payload_token(owner["payload_json"])
            liveness = self._newest_liveness_stamp(
                connection,
                handoff_id,
                owner_sequence=owner_sequence,
                owner_token=owner_token,
                fallback=str(owner["occurred_at"]),
            )
            age = _seconds_since(liveness, now)
            if age is None or age <= bound:
                return refusal, None
            # Expired owner: the takeover CAS. The NOT-EXISTS guard runs
            # in this same serialized transaction, so exactly one
            # concurrent adopter appends the takeover.
            taken = connection.execute(
                "SELECT 1 FROM events "
                "WHERE event_type = 'lead_rotation.takeover' "
                "AND aggregate_id = ? AND sequence > ? LIMIT 1",
                (handoff_id, owner_sequence),
            ).fetchone()
            if taken is not None:
                return refusal, None
            self._events.append(
                connection,
                EventInput(
                    event_type="lead_rotation.takeover",
                    aggregate_type="handoff",
                    aggregate_id=handoff_id,
                    payload={
                        "cell_id": cell_id,
                        "claim_token": token,
                        "supersedes_sequence": owner_sequence,
                        "supersedes_token": owner_token,
                    },
                ),
            )
        return None, token

    def _newest_liveness_stamp(
        self,
        connection: sqlite3.Connection,
        handoff_id: str,
        *,
        owner_sequence: int,
        owner_token: str | None,
        fallback: str,
    ) -> str:
        """The current owner's newest renewal timestamp, else its claim's.

        Only renewals appended AFTER the ownership event and carrying
        the owner's exact ``claim_token`` count — a legacy ownership
        event without a token (pre-protocol rows) can never be renewed
        and expires from its own ``occurred_at``.
        """

        if owner_token is None:
            return fallback
        rows = connection.execute(
            "SELECT occurred_at, payload_json FROM events "
            "WHERE event_type = 'lead_rotation.renewed' "
            "AND aggregate_id = ? AND sequence > ? "
            "ORDER BY sequence DESC",
            (handoff_id, owner_sequence),
        ).fetchall()
        for row in rows:
            if _payload_token(row["payload_json"]) == owner_token:
                return str(row["occurred_at"])
        return fallback

    def _renew(
        self, cell_id: str, handoff_id: str, claim_token: str, *, phase: str
    ) -> None:
        """Append one durable claim renewal proving the owner is alive."""

        with self._database.transaction() as connection:
            self._events.append(
                connection,
                EventInput(
                    event_type="lead_rotation.renewed",
                    aggregate_type="handoff",
                    aggregate_id=handoff_id,
                    payload={
                        "cell_id": cell_id,
                        "claim_token": claim_token,
                        "phase": phase,
                    },
                ),
            )

    async def _renew_periodically(
        self, cell_id: str, handoff_id: str, claim_token: str
    ) -> None:
        """Heartbeat the claim while the transfer (and its unbounded
        acknowledgement runner stream) runs; cancelled when it returns."""

        while True:
            await self._sleep(self._renewal_interval_seconds)
            self._renew(
                cell_id, handoff_id, claim_token, phase="acknowledgement_turn"
            )

    def _journal_completed(
        self, cell_id: str, handoff_id: str, replacement_session: str
    ) -> bool:
        """Atomically close the open attempt with its one completion.

        Sol 04d013b0 finding 5: the openness check used to run OUTSIDE
        the append transaction, so concurrent post-transfer recovery
        could journal duplicate completion events. Check and append now
        run inside ONE ``BEGIN IMMEDIATE`` transaction — the same
        serialized check-then-append pattern as the claim — so exactly
        one caller appends ``lead_rotation.completed`` and returns True
        (earning the one ``complete`` report); every other concurrent
        caller observes the closed attempt and returns False.
        """

        with self._database.transaction() as connection:
            open_attempt = connection.execute(
                "SELECT 1 FROM events "
                "WHERE event_type = 'lead_rotation.attempt' "
                "AND aggregate_id = ? AND NOT EXISTS ("
                "SELECT 1 FROM events AS closed "
                "WHERE closed.event_type = 'lead_rotation.completed' "
                "AND closed.aggregate_id = ?) LIMIT 1",
                (handoff_id, handoff_id),
            ).fetchone()
            if open_attempt is None:
                return False
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
        return True

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


def _payload_token(payload_json: object) -> str | None:
    """The ``claim_token`` carried by one journaled event, if any."""

    try:
        payload = json.loads(str(payload_json))
    except (TypeError, ValueError):
        return None
    token = payload.get("claim_token") if isinstance(payload, dict) else None
    return None if token is None else str(token)


def _seconds_since(stamp: str, now: datetime) -> float | None:
    """Age of one ISO timestamp, or ``None`` when it cannot be parsed
    (the caller fails closed on ``None``)."""

    try:
        occurred = datetime.fromisoformat(stamp)
    except ValueError:
        return None
    if occurred.tzinfo is None:
        occurred = occurred.replace(tzinfo=UTC)
    return (now - occurred).total_seconds()
