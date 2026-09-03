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
import re
import sqlite3
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from hermes_orchestrator.cells import ProjectCell, ProjectCellService, RotationBlocked
from hermes_orchestrator.cmux_surfaces import (
    CmuxSurfaceBindings,
    classic_resume_command,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.handoffs import (
    HandoffRecord,
    HandoffService,
    HandoffStale,
    refresh_handoff_head,
    require_current_head,
)
from hermes_orchestrator.lead_wakes import (
    LeadTerminalWakes,
    TerminalWake,
    TerminalWakeInput,
)

# Mirrors cells._ACTIVE_CELL_STATES: the lifecycle states in which a
# project cell is understood to hold a live, addressable lead — never
# reimplemented here, just read back the same way dispatch() does.
_LIVE_CELL_STATES = ("starting", "active", "handoff_required", "paused")

_RESUMABLE_HANDOFF_STATES = ("submitted", "acknowledged")

# admitted_issues.state values understood as "implementation still
# ongoing" — mirrors the exact filter ``_inflight_issue`` already
# applies below. The staleness gate's issue criterion (see
# ``_stale_submitted_handoff_evidence``) applies ONLY when the
# handoff's own recorded status names an issue that WAS in one of
# these states at submission time — a handoff correctly recording a
# completed issue (a cell can carry several issue lanes; "issue X is
# done, next action: dispatch the next admitted issue" is a legitimate
# fresh handoff) or one with no parseable status is never penalized for
# that (INFRA-184 correction: the prior "most recently updated
# admitted_issues row" heuristic falsely staled exactly this shape, and
# also a newly admitted sibling issue could poison the lookup).
_IN_PROGRESS_ISSUE_STATES = ("in_development", "review")

# ``handoffs.derived_handoff_document`` writes ``status`` as
# ``"issue {issue_id} is {issue_state}; branch ... "`` — the only place
# the issue identity/state recorded AT SUBMISSION survives on the
# document. Matched, never reimplemented, by the staleness gate below.
_STATUS_ISSUE_PATTERN = re.compile(r"^issue (\S+) is (\S+);")

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

# Sol 52d15493 (INFRA-198): a consumed newest handoff no longer ends in
# a terminal operator instruction. The rotation itself files the fresh
# durable handoff request through the EXISTING request channel
# (``cells.request_checkpoint`` -> ``handoff_requests`` row +
# ``project_cell.handoff_required`` evidence + the deduplicated
# ``handoff_required`` terminal wake) and journals this cell-scoped
# awaiting marker. The marker is the durable discriminator between a
# rotation-requested handoff and every ordinary handoff request: only a
# submission that CAS-closes an open awaiting marker (with
# ``_AWAITING_RESUMED_EVENT``) may re-drive the rotation, so ordinary
# stall/red-pressure handoffs never trigger a rotation by accident.
_AWAITING_EVENT = "lead_rotation.awaiting_handoff"
_AWAITING_RESUMED_EVENT = "lead_rotation.awaiting_resumed"

# The only handoff-document content the incumbent is asked to provide
# when the rotation requests a fresh handoff; every other field is
# mechanically derived from durable rows and the worktree probe (see
# ``handoffs.derived_handoff_document``).
NON_DERIVABLE_HANDOFF_CONTENT = (
    "decisions, caveats/blockers, risks, and the exact next action"
)


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
    # Whether ``head`` is a PROVEN ancestor of the fetched integration
    # head (``origin/HEAD``, as last fetched into local
    # remote-tracking refs). Only ever consulted by the zero-work
    # exception below — never part of the strict
    # ``head != origin_head`` comparison itself. Fails closed to
    # False: absence of proof, a missing ``origin/HEAD``, or a broken
    # git probe must never read as an ancestor relationship.
    head_is_integration_ancestor: bool = False


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
    # Set only on the non-terminal ``awaiting_handoff`` report: the
    # durable ``handoff_requests`` row asking the incumbent for the
    # fresh handoff this rotation resumes on (Sol 52d15493).
    request_id: str | None = None
    # INFRA-222: the exact worktree the replacement launched (or
    # resumed) into -- ``cells.rotation_cwd``'s answer for this cell,
    # not necessarily the lane's shared ``project.lead_cwd`` -- and the
    # cell's CURRENT ``worker_bindings`` generation once seating is
    # attempted. Both ``None`` on a report issued before seating (a
    # precondition/claim/transfer refusal never reaches a cwd or a
    # binding).
    cwd: str | None = None
    binding_generation: int | None = None


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
        handoff_command: str = "hermes-orchestrator submit-handoff",
    ) -> None:
        self._database = database
        self._events = EventStore(database)
        self._wakes = LeadTerminalWakes(database=database, events=self._events)
        self._handoffs = handoffs
        self._cells = cells
        self._bindings = bindings
        self._seater = seater
        self._worktree_state = worktree_state
        self._registration_wait_seconds = registration_wait_seconds
        self._sleep = sleep
        self._renewal_interval_seconds = renewal_interval_seconds
        self._handoff_command = handoff_command

    async def rotate(
        self, cell_id: str, *, rearm_delivered_handoff: bool = False
    ) -> RotationReport:
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
        # INFRA-222: the exact worktree this cell's replacement belongs
        # in -- its own leased issue lane when one is open, the lane
        # cwd otherwise (``cells.rotation_cwd``) -- derived ONCE here
        # from the durable row already in hand, then threaded through
        # every path below (recovery re-seat, the fresh-transfer probe,
        # and the completion report) so all three agree on one answer.
        current_cell = ProjectCell(
            cell_id=cell_id,
            project_key=project_key,
            state=state,
            profile_alias=incumbent_profile or "",
            session_id=uuid.UUID(incumbent_session),
            lane_role=str(row["lane_role"]),
        )
        replacement_cwd = str(self._cells.rotation_cwd(current_cell))

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
        if transfer_already_committed and self._attempt_open(handoff_id):
            # The writer transfer and its clean-worktree precondition already
            # committed. Recovery now owns only seating the recorded session;
            # re-deriving an issue worktree can neither make that safer nor
            # undo it, and coordinator cells legitimately have several lanes.
            return await self._seat(
                cell_id=cell_id,
                handoff_id=handoff_id,
                project_key=project_key,
                replacement_session=incumbent_session,
                profile_alias=incumbent_profile,
                claim_token=None,
                cwd=replacement_cwd,
            )

        worktree = self._worktree_state(project_key)
        if worktree.dirty:
            return self._blocked(
                cell_id, handoff_id, phase="precondition",
                failure="project worktree has uncommitted changes; rotation refused",
            )
        # ``origin_head == ""`` means the leased issue branch has no
        # remote counterpart (rev-parse of the remote-tracking ref
        # failed) — a clean zero-work seat can never push one into
        # existence just to satisfy this gate. When that is so AND HEAD
        # is a proven ancestor of the fetched integration head, zero
        # implementation delta is proven and the handoff needs no
        # remote checkpoint: local implementation commits are never
        # ancestors of ``origin/HEAD``, so an ancestor HEAD can only be
        # the (possibly older, still-reachable) integration head itself
        # — equality is just the trivial ancestor case. Any remote
        # issue branch (``origin_head != ""``) or any unproven/absent
        # ancestor relationship keeps today's strict refusal verbatim —
        # and a broken git probe still refuses because ``head`` itself
        # resolves to "".
        zero_work = (
            worktree.origin_head == ""
            and worktree.head != ""
            and worktree.head_is_integration_ancestor
        )
        if worktree.head != worktree.origin_head and not zero_work:
            return self._blocked(
                cell_id, handoff_id, phase="precondition",
                failure=(
                    "project HEAD does not match the pushed origin branch "
                    "head; push before rotating"
                ),
            )

        # Sol INFRA-192: a submitted-but-never-consumed handoff can still
        # be STALE — its mechanically derived facts (branch/HEAD) or the
        # issue it presumes is still in flight can predate reality (the
        # issue already merged, or the worktree moved on). Never
        # acknowledged merely for having gone unconsumed: route into the
        # SAME fresh-handoff request path the already-consumed handoff
        # uses, before any claim/transfer/ack work is attempted.
        if handoff.state == "submitted":
            stale_evidence = self._stale_submitted_handoff_evidence(
                handoff, project_key, worktree
            )
            if stale_evidence is not None:
                return self._request_fresh_handoff(
                    cell_id=cell_id,
                    consumed_handoff_id=handoff_id,
                    project_key=project_key,
                    incumbent_session=incumbent_session,
                    incumbent_profile=incumbent_profile,
                    worktree=worktree,
                    stale_evidence=stale_evidence,
                    rearm_delivered=rearm_delivered_handoff,
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
            # the attempt journal) and can never satisfy a new request.
            # It still never satisfies THIS request — but instead of a
            # terminal operator instruction, the rotation itself files
            # the fresh durable handoff request with the incumbent and
            # journals its awaiting marker, so the one Hermes-owned
            # rotate invocation resumes automatically on the submission
            # (Sol 52d15493).
            return self._request_fresh_handoff(
                cell_id=cell_id,
                consumed_handoff_id=handoff_id,
                project_key=project_key,
                incumbent_session=incumbent_session,
                incumbent_profile=incumbent_profile,
                worktree=worktree,
                rearm_delivered=rearm_delivered_handoff,
            )
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
            # INFRA-222 (the live 2026-09-01 rotation defect): the
            # acknowledgement turn below can run arbitrarily long, and
            # this handoff's recorded branch/HEAD were derived whenever
            # it was submitted -- possibly well before now. Re-probe the
            # LEASED worktree (the same path the replacement is about to
            # launch into) immediately before spending that turn on it.
            # Every handoff's branch/head are themselves mechanically
            # derived in the first place (``derived_handoff_document``
            # never takes them from the incumbent's own account), so a
            # HEAD that merely advanced is refreshed and persisted here
            # rather than refused; only a branch mismatch or a failed
            # probe (fail-closed to "" by the worktree probe) can never
            # be repaired this way and refuses outright.
            lease_probe = self._worktree_state(replacement_cwd)
            try:
                require_current_head(
                    handoff.document, branch=lease_probe.branch, head=lease_probe.head
                )
            except HandoffStale as stale:
                if (
                    not lease_probe.branch
                    or not lease_probe.head
                    or lease_probe.branch != handoff.document.branch
                ):
                    return self._blocked(
                        cell_id,
                        handoff_id,
                        phase="transfer",
                        failure=(
                            f"leased worktree {replacement_cwd} failed "
                            f"currency verification for handoff "
                            f"{handoff_id!r}: {stale}; submit a fresh "
                            "handoff for this cell and retry rotate-lead"
                        ),
                    )
                refreshed_document = refresh_handoff_head(
                    handoff.document,
                    branch=lease_probe.branch,
                    head=lease_probe.head,
                )
                handoff = self._handoffs.refresh(handoff_id, refreshed_document)
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
            cwd=replacement_cwd,
        )

    async def resume_on_submission(
        self, handoff: HandoffRecord
    ) -> RotationReport | None:
        """Continue an awaiting rotation the moment its fresh handoff is
        durably submitted (Sol 52d15493).

        The event-driven continuation the handoff submission path drives
        through :meth:`HandoffService.subscribe`'s post-commit signal:
        when the submitted handoff's cell holds an OPEN awaiting marker
        (journaled by the consumed-handoff branch of :meth:`rotate`),
        exactly one caller CAS-closes it and re-drives the SAME durable
        rotation — the fresh submitted handoff is now the cell's newest,
        so the ordinary claim/transfer/seat phases run and change both
        the profile and the session. Returns ``None`` for every ordinary
        submission (no awaiting rotation, or a raced resumer already
        claimed it): the continuation machinery never fires outside a
        rotation-requested handoff.
        """

        if handoff.state != "submitted":
            return None
        if not self._claim_awaiting_resumption(
            handoff.cell_id, handoff.handoff_id
        ):
            return None
        return await self.rotate(handoff.cell_id)

    def _request_fresh_handoff(
        self,
        *,
        cell_id: str,
        consumed_handoff_id: str,
        project_key: str,
        incumbent_session: str,
        incumbent_profile: str | None,
        worktree: WorktreeState,
        stale_evidence: dict[str, str] | None = None,
        rearm_delivered: bool = False,
    ) -> RotationReport:
        """File the fresh durable handoff request and report awaiting.

        Reuses the EXISTING request channel end to end — nothing new is
        invented: ``cells.request_checkpoint`` marks the cell
        ``handoff_required``, journals the ``project_cell.
        handoff_required`` evidence, and persists the ``handoff_requests``
        row; the deduplicated ``handoff_required`` terminal wake (bound
        to that same evidence identity, exactly as the cells direct path
        binds it) rides the existing outbox/intake delivery to wake the
        incumbent. The request reason carries every mechanically derived
        handoff fact, so the incumbent is asked ONLY for the
        non-derivable content. Idempotent: a repeat call while the cell
        already awaits re-reports the same request without duplicating
        the row, the wake, or the awaiting marker.

        ``stale_evidence`` is set only by the staleness gate in
        :meth:`rotate` (INFRA-192): a submitted-but-never-consumed
        handoff whose mechanical facts disagree with durable/worktree
        reality. Its compact dict is folded into the SAME durably
        journaled reason (rather than a new mechanism), so the evidence
        that justified refusing the handoff survives on the
        ``handoff_requests`` row and the terminal wake.
        """

        issue_id, issue_state = self._inflight_issue(project_key)
        kind = (
            "lead_rotation:stale_handoff_detected"
            if stale_evidence is not None
            else "lead_rotation:fresh_handoff_required"
        )
        reason = (
            f"{kind} "
            f"consumed={consumed_handoff_id} cell={cell_id} "
            f"project={project_key} session={incumbent_session} "
            f"profile={incumbent_profile} issue={issue_id} "
            f"issue_state={issue_state} branch={worktree.branch} "
            f"head={worktree.head}; "
        )
        if stale_evidence is not None:
            evidence = " ".join(f"{k}={v}" for k, v in stale_evidence.items())
            reason += f"stale_evidence: {evidence}; "
        reason += (
            "every mechanical handoff field is "
            "derived from durable state — provide only "
            f"{NON_DERIVABLE_HANDOFF_CONTENT} "
            f"({self._handoff_command}); the rotation resumes "
            "automatically on the durable submission"
        )
        filed = self._cells.request_checkpoint(cell_id, reason)
        request_id = self._newest_handoff_request_id(cell_id)
        if request_id is None:
            # Nothing was filed and nothing is pending (e.g. a paused
            # cell the request channel refuses): fail closed exactly as
            # before rather than reporting an awaiting state no durable
            # row backs.
            if stale_evidence is not None:
                context = (
                    f"handoff {consumed_handoff_id!r} is stale relative to "
                    "durable state and cannot satisfy this rotation"
                )
            else:
                context = (
                    f"handoff {consumed_handoff_id!r} already completed a "
                    "rotation onto the current incumbent "
                    f"({incumbent_profile!r} / {incumbent_session})"
                )
            return self._blocked(
                cell_id,
                consumed_handoff_id,
                phase="precondition",
                failure=(
                    f"{context} and a "
                    "fresh handoff request could not be filed "
                    f"(request_checkpoint refused, filed={filed}); submit "
                    "a fresh handoff for this cell, then retry rotate-lead"
                ),
            )
        wake = self._commit_handoff_request_wake(
            cell_id=cell_id,
            project_key=project_key,
            issue_id=issue_id,
            incumbent_session=incumbent_session,
            incumbent_profile=incumbent_profile,
            reason=reason,
        )
        if rearm_delivered and wake is not None:
            latest = self._database.execute(
                "SELECT wake_id FROM lead_terminal_wakes WHERE cell_id = ? "
                "AND session_id = ? AND kind = 'handoff_required' "
                "AND (wake_id = ? OR turn_key LIKE ?) "
                "ORDER BY rowid DESC LIMIT 1",
                (
                    cell_id,
                    incumbent_session,
                    wake.wake_id,
                    f"handoff-retry:{request_id}:%",
                ),
            ).fetchone()
            source = self._wakes.get(str(latest["wake_id"])) if latest else wake
            if source.state == "delivered":
                self._wakes.commit(
                    TerminalWakeInput(
                        project_key=project_key,
                        issue_id=issue_id,
                        cell_id=cell_id,
                        session_id=uuid.UUID(incumbent_session),
                        profile_alias=incumbent_profile or "unknown",
                        turn_key=(
                            f"handoff-retry:{request_id}:{source.wake_id}"
                        ),
                        kind="handoff_required",
                        reason=reason,
                    )
                )
        self._journal_awaiting(cell_id, consumed_handoff_id, request_id)
        return RotationReport(
            ok=False,
            phase="awaiting_handoff",
            cell_id=cell_id,
            handoff_id=consumed_handoff_id,
            request_id=request_id,
        )

    def _stale_submitted_handoff_evidence(
        self,
        handoff: HandoffRecord,
        project_key: str,
        worktree: WorktreeState,
    ) -> dict[str, str] | None:
        """Compare a SUBMITTED handoff's mechanically derived facts
        against current durable/worktree reality (INFRA-192: a stale
        submitted handoff was consumed for rotation whose derived
        branch/HEAD and next action predated the issue's completed
        merge).

        - the document's ``branch`` no longer matching the current
          worktree branch is enough alone;
        - the document's first ``commits`` entry (HEAD at submission)
          matching neither the current worktree HEAD nor the fetched
          origin HEAD is enough alone;
        - the issue criterion is narrower, and deliberately does NOT use
          "the project's current admitted issue" (a cell can carry
          several issue lanes, and a legitimate handoff can correctly
          say "issue X is done; next action: dispatch the next admitted
          issue" — INFRA-184 correction). Instead it parses the ONE
          issue identity/state the document itself recorded at
          submission from ``status`` (written by
          ``handoffs.derived_handoff_document`` as
          ``"issue {id} is {state}; ..."``, via
          ``_STATUS_ISSUE_PATTERN``). The criterion applies ONLY when
          that parse succeeds AND the recorded state was itself
          in-progress (``_IN_PROGRESS_ISSUE_STATES``) — i.e. the
          handoff itself presumed ongoing implementation on that issue.
          When it applies, THAT issue's CURRENT ``admitted_issues.state``
          is looked up by issue id (never "most recently updated", which
          a newly admitted sibling issue could poison); stale iff it no
          longer equals the recorded state (e.g. in_development -> done).
          A status line that does not parse, or records a
          non-in-progress state (``done``/unknown/no issue), leaves the
          issue criterion inapplicable — branch/HEAD still apply.

        Returns a compact evidence dict (``handoff_head``,
        ``current_head``, ``handoff_branch``, ``current_branch``,
        ``issue_state``, the last being the recorded issue's CURRENT
        state or ``"n/a"`` when the criterion did not apply) when stale,
        ``None`` when the handoff is fresh.
        """

        document = handoff.document
        handoff_head = document.commits[0]
        branch_stale = document.branch != worktree.branch
        head_stale = handoff_head not in (worktree.head, worktree.origin_head)

        issue_state = "n/a"
        issue_stale = False
        match = _STATUS_ISSUE_PATTERN.match(document.status)
        if match is not None:
            recorded_issue_id, recorded_state = match.group(1), match.group(2)
            if recorded_state in _IN_PROGRESS_ISSUE_STATES:
                issue_state = self._issue_state_by_id(recorded_issue_id, project_key)
                issue_stale = issue_state != recorded_state

        if not (branch_stale or head_stale or issue_stale):
            return None
        return {
            "handoff_head": handoff_head,
            "current_head": worktree.head,
            "handoff_branch": document.branch,
            "current_branch": worktree.branch,
            "issue_state": issue_state,
        }

    def _issue_state_by_id(self, issue_id: str, project_key: str) -> str:
        """The CURRENT durable state of one specific admitted issue —
        looked up by identity, never by recency, so an unrelated sibling
        issue (a newly admitted one, or another lane's) can never be
        mistaken for the issue a handoff's status line recorded.
        ``"unknown"`` when that issue no longer has an admitted_issues
        row for this project."""

        row = self._database.execute(
            "SELECT state FROM admitted_issues "
            "WHERE issue_id = ? AND project_key = ?",
            (issue_id, project_key),
        ).fetchone()
        return "unknown" if row is None else str(row["state"])

    def _inflight_issue(self, project_key: str) -> tuple[str, str]:
        """The single in-flight issue for the project, from durable rows
        alone; ``("none", "unknown")`` when zero or several exist."""

        rows = self._database.execute(
            "SELECT issue_id, state FROM admitted_issues "
            "WHERE project_key = ? AND state IN ('in_development', 'review') "
            "ORDER BY issue_id",
            (project_key,),
        ).fetchall()
        if len(rows) == 1:
            return str(rows[0]["issue_id"]), str(rows[0]["state"])
        return "none", "unknown"

    def _newest_handoff_request_id(self, cell_id: str) -> str | None:
        row = self._database.execute(
            "SELECT request_id FROM handoff_requests WHERE cell_id = ? "
            "ORDER BY requested_at DESC, rowid DESC LIMIT 1",
            (cell_id,),
        ).fetchone()
        return None if row is None else str(row["request_id"])

    def _commit_handoff_request_wake(
        self,
        *,
        cell_id: str,
        project_key: str,
        issue_id: str,
        incumbent_session: str,
        incumbent_profile: str | None,
        reason: str,
    ) -> TerminalWake | None:
        """Wake the incumbent through the existing terminal-wake outbox.

        The turn key binds to the newest ``project_cell.handoff_required``
        evidence — the same identity the cells direct path and the wake
        reconciler derive — so direct commit, repair, and replay all
        converge on one deduplicated row.
        """

        evidence = self._database.execute(
            "SELECT event_id FROM events "
            "WHERE event_type = 'project_cell.handoff_required' "
            "AND aggregate_id = ? ORDER BY sequence DESC LIMIT 1",
            (cell_id,),
        ).fetchone()
        if evidence is None:
            return None
        return self._wakes.commit(
            TerminalWakeInput(
                project_key=project_key,
                issue_id=issue_id,
                cell_id=cell_id,
                session_id=uuid.UUID(incumbent_session),
                profile_alias=incumbent_profile or "unknown",
                turn_key=f"handoff:{evidence['event_id']}",
                kind="handoff_required",
                reason=reason,
            )
        )

    def _journal_awaiting(
        self, cell_id: str, consumed_handoff_id: str, request_id: str
    ) -> None:
        """Journal the awaiting marker once per open request (serialized
        check-then-append, the same idiom as the claim)."""

        with self._database.transaction() as connection:
            if self._open_awaiting_sequence(connection, cell_id) is not None:
                return
            self._events.append(
                connection,
                EventInput(
                    event_type=_AWAITING_EVENT,
                    aggregate_type="project_cell",
                    aggregate_id=cell_id,
                    payload={
                        "consumed_handoff_id": consumed_handoff_id,
                        "request_id": request_id,
                    },
                ),
            )

    def _claim_awaiting_resumption(
        self, cell_id: str, fresh_handoff_id: str
    ) -> bool:
        """CAS-close the open awaiting marker; exactly one resumer wins.

        Check and append run inside ONE ``BEGIN IMMEDIATE`` transaction
        (the ``_journal_completed`` idiom), so of any number of
        concurrent submissions exactly one observes the open marker,
        appends the resumed event, and re-drives the rotation.
        """

        with self._database.transaction() as connection:
            awaiting = self._open_awaiting_sequence(connection, cell_id)
            if awaiting is None:
                return False
            self._events.append(
                connection,
                EventInput(
                    event_type=_AWAITING_RESUMED_EVENT,
                    aggregate_type="project_cell",
                    aggregate_id=cell_id,
                    payload={
                        "handoff_id": fresh_handoff_id,
                        "awaiting_sequence": awaiting,
                    },
                ),
            )
        return True

    def _open_awaiting_sequence(
        self, connection: sqlite3.Connection, cell_id: str
    ) -> int | None:
        """The newest awaiting marker not yet closed by a resumption."""

        row = connection.execute(
            f"SELECT sequence FROM events "
            f"WHERE event_type = '{_AWAITING_EVENT}' AND aggregate_id = ? "
            f"AND sequence > COALESCE((SELECT MAX(sequence) FROM events "
            f"WHERE event_type = '{_AWAITING_RESUMED_EVENT}' "
            f"AND aggregate_id = ?), 0) "
            "ORDER BY sequence DESC LIMIT 1",
            (cell_id, cell_id),
        ).fetchone()
        return None if row is None else int(row["sequence"])

    async def _seat(
        self,
        *,
        cell_id: str,
        handoff_id: str,
        project_key: str,
        replacement_session: str,
        profile_alias: str | None,
        claim_token: str | None = None,
        cwd: str | None = None,
    ) -> RotationReport:
        if claim_token is not None:
            # Renewal at the phase transition: entering the seat phase.
            self._renew(cell_id, handoff_id, claim_token, phase="seat")
        # CmuxLeadSeater is the idempotent seat owner. Always pass recovery
        # through it: it reuses an existing binding and, while the channel is
        # still unregistered, retries the bounded trust confirmation.
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
                cwd=cwd,
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
                cwd=cwd,
            )
        return await self._completion_report(
            cell_id=cell_id,
            handoff_id=handoff_id,
            replacement_session=replacement_session,
            profile_alias=profile_alias,
            binding_id=binding.binding_id,
            claim_token=claim_token,
            cwd=cwd,
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
        cwd: str | None = None,
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

        # INFRA-222: the cell's CURRENT worker_bindings generation, read
        # straight off the durable row -- the same identity
        # ``cells._finalize_transfer`` just swapped (or bound for a
        # legacy cell with no prior binding). ``None`` only for a cell
        # with no binding at all, which recovery/an unwired composition
        # can still reach.
        generation_row = self._database.execute(
            "SELECT generation FROM worker_bindings "
            "WHERE cell_id = ? AND state = 'active'",
            (cell_id,),
        ).fetchone()
        binding_generation = (
            int(generation_row["generation"]) if generation_row is not None else None
        )

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
                    cwd=cwd,
                    binding_generation=binding_generation,
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
                cwd=cwd,
                binding_generation=binding_generation,
            )
        return RotationReport(
            ok=False,
            phase="channel_registration",
            cell_id=cell_id,
            handoff_id=handoff_id,
            replacement_session=replacement_session,
            profile=profile_alias,
            binding_id=binding_id,
            cwd=cwd,
            binding_generation=binding_generation,
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
