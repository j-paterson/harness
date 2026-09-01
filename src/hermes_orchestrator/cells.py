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
from hermes_orchestrator.lead_assignments import LeadAssignment, LeadAssignments
from hermes_orchestrator.lead_wakes import TerminalWakeInput
from hermes_orchestrator.linear import (
    ExternalEffectStore,
    LinearProjection,
    projection_request,
)
from hermes_orchestrator.operator_decisions import OperatorDecisions
from hermes_orchestrator.profiles import CapacityObservation, ProfilePool
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.subagent_packets import PacketRefused, SubagentPackets

_ACTIVE_CELL_STATES = ("starting", "active", "handoff_required", "paused")

# A fable cap is a weekly-budget exhaustion, but the sanitized Claude
# stream exposes no reset horizon. The conservative fallback is one full
# weekly cycle — the documented worst case — never the flat hour that
# session and monthly-spend caps keep.
_FABLE_CAP_FALLBACK = timedelta(days=7)
_FABLE_CAP_FALLBACK_DETAIL = (
    "provider fable limit with no reset horizon in the stream; capped for "
    "the worst-case weekly cycle unless a newer observation clears it"
)


class ProfileCapacityEvidence:
    """Durable capacity-evidence port backed by the orchestrator database.

    Reads satisfy the ``CapacityEvidencePort`` protocol consumed by
    ``ProfilePool``; the newest observation per (profile_alias, model)
    is the current evidence.
    """

    def __init__(self, database: Database) -> None:
        self._database = database

    def latest(
        self,
        profile_alias: str,
        model: str,
    ) -> CapacityObservation | None:
        row = self._database.execute(
            "SELECT profile_alias, model, state, source, observed_at, "
            "resets_at, detail FROM profile_capacity_observations "
            "WHERE profile_alias = ? AND model = ? "
            "ORDER BY observation_id DESC LIMIT 1",
            (profile_alias, model),
        ).fetchone()
        if row is None:
            return None
        resets_at = row["resets_at"]
        return CapacityObservation(
            profile_alias=str(row["profile_alias"]),
            model=str(row["model"]),
            state=str(row["state"]),
            source=str(row["source"]),
            observed_at=datetime.fromisoformat(str(row["observed_at"])),
            resets_at=(
                datetime.fromisoformat(str(resets_at)) if resets_at else None
            ),
            detail=str(row["detail"]) if row["detail"] else None,
        )


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
    """Approved Linear projection boundary used by project cells.

    INFRA-199 v2: Linear never authorizes activation, so this contract
    is projection-only. There is deliberately no ``validate`` method
    here any more — the removed pre-commit team/routing check that
    used to gate whether a candidate could even start is gone (see
    ``ProjectCellService._dispatch_locked`` and
    :func:`activate_admitted_issue`); team/routing correctness is
    proven, post-commit, by ``project``'s own internal
    ``validate_issue`` call (``LinearClient.project`` in ``linear.py``)
    the one time Linear is ever consulted in this path.
    """

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


# The only source states an activation may CAS out of: the exact
# runnable states ranked dispatch selects from. Anything else — done,
# paused, review, qa, in_development — refuses at commit time.
_RUNNABLE_ISSUE_STATES = (
    IssueState.QUEUED.value,
    IssueState.BLOCKED.value,
)

# Project occupancy (INFRA-199 v2 / INFRA-211): a project is "busy" —
# refusing a DIFFERENT issue's activation — while it has an issue in
# EITHER of these states. ``in_development`` is the obvious case; the
# INFRA-211 failure reproduction showed a second issue starting while
# the first had already moved to ``review`` (the lead had handed off,
# but the project is not yet free — the reviewer/settlement flow still
# owns it), so review occupies the project exactly like in_development
# does.
_OCCUPYING_ISSUE_STATES = (
    IssueState.IN_DEVELOPMENT.value,
    IssueState.REVIEW.value,
)


def admission_priority_ceiling(
    executor: object, *, now: datetime, freshness_minutes: int
) -> int | None:
    """The highest issue priority the freshest resource sample
    authorizes right now, or ``None`` when nothing may be admitted.

    THE one shared freshness/priority guard body (Sol ec0ed7fe gap 3):
    the Stop-hook idle dispatcher (``cli._idle_admission_priority``)
    and the daemon dispatch path
    (:meth:`ProjectCellService._activate_issue`'s transaction-local
    guard) both run exactly this function — first on a plain database
    snapshot for candidate selection where applicable, then again on
    the activation transaction's own connection — so the two paths can
    never diverge on what capacity evidence admits.

    A missing, malformed, implausibly future-dated, or stale sample
    (INFRA-199 Finding 2) all fail closed here exactly like an explicit
    red/unknown pressure reading does — refuse admission, never guess.
    """

    from hermes_orchestrator.admission import YELLOW_ADMITS_PRIORITY_AT_MOST
    from hermes_orchestrator.resources import read_fresh_sample

    reading = read_fresh_sample(
        executor,  # type: ignore[arg-type]
        now=now,
        max_age=timedelta(minutes=freshness_minutes),
    )
    if reading is None:
        return None
    return {"green": 4, "yellow": YELLOW_ADMITS_PRIORITY_AT_MOST}.get(
        reading.pressure
    )


def _in_development_effect_id(issue_id: str) -> str:
    """The one stable Linear ``In Development`` projection effect id.

    Hermes' durable local lifecycle is authoritative (INFRA-199 v2):
    local activation commits before this projection is ever attempted,
    and nothing here ever compensates or re-projects under a bumped
    epoch, so — unlike the removed intent machinery's per-compensation
    suffix — a single, permanently stable id is correct: at most one
    legitimate ``In Development`` projection exists per issue's
    activation, and ``ExternalEffectStore``/``LinearClient`` (see
    ``linear.py``) make any replay of it idempotent for free.
    """

    return f"linear:{issue_id}:in-development:v2"


async def _project_in_development(linear: LinearProjector, issue_id: str) -> None:
    """Best-effort, idempotent ``In Development`` projection attempted
    ONLY after the local activation transaction already committed
    (INFRA-199 v2: local-first activation).

    Linear is an eventually consistent workflow projection, never an
    authority over Hermes' durable local lifecycle. Any failure here —
    composing the live client itself (the idle path composes its
    Keychain/config-backed router lazily, inside this very call), a
    network error, an unreachable or deleted issue, a disallowed
    status transition — is swallowed: it must never roll back, retry-
    loop, or duplicate the local work that already committed.

    The durable trace is not lost, though: the activation transaction
    that this projection follows already journaled the stable
    TARGET-ONLY ``pending`` row in ``external_effects``
    (``ExternalEffectStore.begin_in`` inside
    :func:`_activate_issue_transaction`, no live client required), and
    the real ``LinearClient.project`` (``linear.py``) ADOPTS that
    exact row — same effect id, byte-identical request — before its
    first network operation, so ANY failure class here (router
    composition, the initial issue read, validation, the update)
    leaves exactly that one row behind — never nothing. Startup/periodic
    reconciliation (``ReconcileService._stage_linear`` /
    ``_project_pending_linear_effect`` in ``reconcile.py``) already
    reads every ``pending`` Linear effect and surfaces it as a durable
    finding; that existing, read-only mechanism — not a retry loop
    here — is what an operator or a later reconciliation pass resolves
    it through. A later legitimate replay of this same stable effect
    id (the issue's project cell resuming after a restart, say) is
    naturally idempotent through that same store, so it converges the
    pending row forward on its own without any bespoke convergence
    protocol.
    """

    with suppress(Exception):
        await linear.project(
            issue_id,
            LinearProjection(status="In Development", assignee_alias="operator"),
            effect_id=_in_development_effect_id(issue_id),
        )


def _activate_issue_transaction(
    *,
    database: Database,
    events: EventStore,
    cell_id: str,
    project_key: str,
    profile_alias: str,
    issue_id: str,
    session_id: str,
    assignments: LeadAssignments | None,
    now: Callable[[], datetime],
    guard: Callable[[sqlite3.Connection], bool] | None = None,
) -> tuple[bool, LeadAssignment | None]:
    """The one durable, LOCAL-ONLY activation transaction every dispatch
    path shares: the full commit-time eligibility recheck, the
    ``project_cells`` active-lease guard, the ``admitted_issues`` CAS
    into ``in_development``, the journaled ``issue.started`` event, and
    (when ``assignments`` is supplied) the durable assignment packet —
    all in one exclusive write transaction (``Database.transaction``
    opens ``BEGIN IMMEDIATE``), so a queue transition can never again
    outrun its assignment and no predicate can change between its
    recheck and the commit.

    INFRA-199 v2: this transaction never consults Linear. Hermes'
    durable local lifecycle is authoritative; Linear is an eventually
    consistent workflow projection attempted only AFTER this commits
    (see :func:`_project_in_development`, called by both callers below
    once this returns ``True``).

    Both :meth:`ProjectCellService._activate_issue` (explicit dispatch)
    and :func:`activate_admitted_issue` (the Stop-hook idle dispatcher,
    ``cli.py``'s ``_dispatch_idle_lead``) call this exact function;
    neither reimplements the state machine.

    Commit-time eligibility (INFRA-199 Finding 2) — every mutable
    predicate observed before dispatch selected this candidate is
    re-proven inside this transaction, and any failure aborts with
    ZERO writes:

    - ``guard``, when supplied, runs first on the same connection for
      any FRESH activation (resource-sample freshness plus the
      priority ceiling — the idle path and the armed daemon path both
      supply the identical :func:`admission_priority_ceiling` check);
      a replay of an already-``in_development`` issue is exempt: it
      resumes work this same guard already authorized, it never
      admits new work;
    - the issue's source state must be one of the exact runnable
      states (queued/blocked) and the CAS is ``WHERE state = ?`` with
      that observed value — never merely ``!= done``;
    - ``dependency_ready`` must still hold;
    - no OTHER issue of the project may occupy it — ``in_development``
      OR ``review`` (INFRA-211: a lead that already handed off to
      review still owns the project until settlement clears it);
    - no pending operator decision may exist for the issue;
    - the cell must still be live and its profile lease active
      (the ``project_cells`` CAS with the ``profile_leases`` guard).

    An issue that is ALREADY ``in_development`` is the replay/resume
    case: the cell lease is confirmed and the assignment packet is
    (idempotently) ensured, but no second ``issue.started`` is ever
    journaled and no queue transition happens — full replay
    idempotence across effect, transition, event, and assignment.
    """

    stamp = now().isoformat()
    assignment: LeadAssignment | None = None
    with database.transaction() as connection:
        issue_row = connection.execute(
            "SELECT state, instruction_id, project_key, dependency_ready "
            "FROM admitted_issues WHERE issue_id = ?",
            (issue_id,),
        ).fetchone()
        if issue_row is None:
            raise KeyError(issue_id)
        prior_state = str(issue_row["state"])
        replaying = prior_state == IssueState.IN_DEVELOPMENT.value
        if not replaying:
            if guard is not None and not guard(connection):
                return False, None
            if prior_state not in _RUNNABLE_ISSUE_STATES:
                return False, None
            if not int(issue_row["dependency_ready"]):
                return False, None
            busy = connection.execute(
                "SELECT 1 FROM admitted_issues WHERE project_key = ? "
                "AND state IN (?, ?) AND issue_id != ? LIMIT 1",
                (
                    str(issue_row["project_key"]),
                    *_OCCUPYING_ISSUE_STATES,
                    issue_id,
                ),
            ).fetchone()
            if busy is not None:
                return False, None
            pending_decision = connection.execute(
                "SELECT 1 FROM operator_decisions WHERE issue_id = ? "
                "AND status = 'pending' LIMIT 1",
                (issue_id,),
            ).fetchone()
            if pending_decision is not None:
                return False, None
        activated = connection.execute(
            "UPDATE project_cells SET state = 'active', updated_at = ? "
            "WHERE cell_id = ? AND state IN ('starting', 'active') "
            "AND EXISTS ("
            "SELECT 1 FROM profile_leases "
            "WHERE project_key = ? AND profile_alias = ? AND state = 'active'"
            ")",
            (stamp, cell_id, project_key, profile_alias),
        )
        if activated.rowcount == 0:
            return False, None
        if not replaying:
            updated = connection.execute(
                "UPDATE admitted_issues SET state = ?, updated_at = ? "
                "WHERE issue_id = ? AND state = ?",
                (
                    IssueState.IN_DEVELOPMENT.value,
                    stamp,
                    issue_id,
                    prior_state,
                ),
            )
            if updated.rowcount == 0:
                return False, None
            events.append(
                connection,
                EventInput(
                    event_type="issue.started",
                    aggregate_type="issue",
                    aggregate_id=issue_id,
                    payload={"cell_id": cell_id},
                ),
            )
        if assignments is not None:
            assignment = assignments.publish_in(
                connection,
                project_key=project_key,
                issue_id=issue_id,
                cell_id=cell_id,
                session_id=session_id,
                profile_alias=profile_alias,
                instruction_id=str(issue_row["instruction_id"]),
                queue_transition=(
                    f"{prior_state}->{IssueState.IN_DEVELOPMENT.value}"
                ),
            )
        # Sol ec0ed7fe gap 2: the stable TARGET-ONLY ``In Development``
        # projection record becomes durable in this very commit, BEFORE
        # any fallible Linear operation can run — no live client is
        # required to write it. ``LinearClient.project`` later ADOPTS
        # this exact row (same effect id, byte-identical
        # ``projection_request`` payload) instead of double-beginning,
        # so every post-commit failure class — router composition, the
        # initial issue read, team/mapping/transition validation, the
        # update itself — leaves exactly ONE pending row for
        # reconciliation (``reconcile._project_pending_linear_effect``).
        # On a replay the row already exists (pending or completed) and
        # is adopted untouched, keeping the journal at exactly one row.
        ExternalEffectStore.begin_in(
            connection,
            _in_development_effect_id(issue_id),
            target=issue_id,
            request=projection_request(
                issue_id,
                LinearProjection(
                    status="In Development", assignee_alias="operator"
                ),
            ),
        )
    return True, assignment


async def activate_admitted_issue(
    *,
    database: Database,
    events: EventStore,
    linear: LinearProjector,
    assignments: LeadAssignments | None,
    cell_id: str,
    project_key: str,
    profile_alias: str,
    session_id: str,
    issue_id: str,
    now: Callable[[], datetime] = _utc_now,
    guard: Callable[[sqlite3.Connection], bool] | None = None,
) -> tuple[bool, LeadAssignment | None]:
    """Dispatch one already-admitted issue onto a live cell durably.

    INFRA-199 v2 (local-first activation): the Stop-hook idle
    dispatcher (``cli.py``'s ``_dispatch_idle_lead``) calls this
    instead of reimplementing any part of dispatch's activation
    sequence. It delegates to the very same
    :func:`_activate_issue_transaction` that backs
    :meth:`ProjectCellService._activate_issue`, so the idle path can
    never diverge from the activation state machine.

    Ordering: the shared LOCAL activation transaction commits FIRST —
    it never consults Linear at all (see
    :func:`_activate_issue_transaction`) — and only once it reports
    success is the idempotent ``In Development`` projection attempted
    through :func:`_project_in_development`. Linear never authorizes
    activation: a deleted/unreachable issue, a disallowed transition,
    or any other Linear failure cannot block or undo the local work
    that already committed, and is swallowed rather than retried here
    (see :func:`_project_in_development` for where that failure's
    durable trace lives and how it gets resolved).

    A commit-time refusal (occupancy, exact-state CAS, dependency
    flip, pending decision, stale guard, lease identity) leaves ZERO
    local writes and Linear is never even attempted — there is nothing
    to project for a candidate that did not activate.
    """

    activated, assignment = _activate_issue_transaction(
        database=database,
        events=events,
        cell_id=cell_id,
        project_key=project_key,
        profile_alias=profile_alias,
        issue_id=issue_id,
        session_id=session_id,
        assignments=assignments,
        now=now,
        guard=guard,
    )
    if not activated:
        return False, None
    await _project_in_development(linear, issue_id)
    return True, assignment


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
        assignments: LeadAssignments | None = None,
        packets: SubagentPackets | None = None,
        dispatch_freshness_minutes: int | None = None,
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
        self._assignments = assignments
        self._packets = packets
        self._dispatch_freshness_minutes = dispatch_freshness_minutes
        self._dispatch_locks: dict[str, asyncio.Lock] = {}
        self._restore_profile_leases()

    def require_dispatch_capacity_guard(self, freshness_minutes: int) -> None:
        """Arm the daemon dispatch path's transaction-local capacity guard.

        Sol ec0ed7fe gap 3: the live daemon (``cli``'s ``daemon``
        command) arms this with the same
        ``policy.resource_sample_freshness_minutes`` bound the idle
        dispatcher reads, so every FRESH activation through
        :meth:`dispatch` re-proves — inside the activation transaction,
        on its own connection — that the newest resource sample is
        fresh enough and its pressure admits the issue's priority
        (:func:`admission_priority_ceiling`, the identical check the
        idle path runs). Unarmed (unit composition, one-shot rotation)
        the dispatch path keeps its historical behavior.
        """

        self._dispatch_freshness_minutes = int(freshness_minutes)

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
        lock = self._dispatch_locks.setdefault(issue.project_key, asyncio.Lock())
        async with lock:
            if self._decisions is not None:
                pending = self._decisions.pending_for_issue(issue_id)
                if pending:
                    return DispatchResult(
                        status="awaiting_operator_decision", issue_id=issue_id
                    )
            return await self._dispatch_locked(issue_id)

    async def _dispatch_locked(self, issue_id: str) -> DispatchResult:
        """Dispatch one issue while holding its project-wide in-process lock."""

        issue = self._queue.get(issue_id)
        # Project occupancy (INFRA-199 v2 / INFRA-211): a DIFFERENT queued
        # issue never starts while this project already has one mid-flight
        # in EITHER in_development or review. Dispatching that same
        # in_development issue itself (resume after restart, reconciliation,
        # handoff) is untouched — only a distinct issue_id is refused here,
        # and the refusal is a plain read with zero writes. This is a coarse
        # pre-check only, purely to avoid launching a Claude lead process for
        # a candidate that cannot activate; the shared activation
        # transaction (`_activate_issue_transaction`) re-proves the same
        # occupancy predicate, transactionally, at commit time.
        busy = self._database.execute(
            "SELECT 1 FROM admitted_issues WHERE project_key = ? "
            "AND state IN (?, ?) AND issue_id != ? LIMIT 1",
            (issue.project_key, *_OCCUPYING_ISSUE_STATES, issue_id),
        ).fetchone()
        if busy is not None:
            return DispatchResult(status="project_busy", issue_id=issue_id)
        # INFRA-199 v2: Linear is never consulted before the local commit.
        # Hermes' durable local lifecycle is authoritative; a pre-commit
        # `linear.validate` here used to gate whether this candidate could
        # even start a Claude lead, which meant a deleted/unreachable/
        # misrouted Linear issue could block a locally-eligible, fully
        # runnable issue from ever activating. That is exactly the
        # authority Linear must not hold. Team/project routing is instead
        # proven, post-commit, by the one Linear call this path still makes
        # — `_project_and_activate`'s idempotent projection, whose
        # `LinearClient.project` internally re-validates the team/issue
        # before mutating anything — so a routing problem surfaces as a
        # durable pending `external_effects` row for reconciliation, never
        # as a block on local activation.
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
                activated, assignment = await self._project_and_activate(
                    cell, issue_id
                )
                if activated:
                    if assignment is not None and self._assignments is not None:
                        # The packet is already durable; this only routes
                        # it to the channel immediately instead of the
                        # next repair tick.
                        self._assignments.notify_committed(assignment)
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
                        activated, _ = await self._project_and_activate(
                            cell, issue_id
                        )
                        if activated:
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
                            self._require_handoff(
                                cell,
                                "subscription_limit",
                                limit_kind=limit_kind,
                            )
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
                limit_kind=limit_kind,
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
            elif (
                event.kind in ("subagent.completed", "subagent.failed")
                and event.parent_tool_use_id is not None
            ):
                # Exactly-once lease release: a duplicate terminal event
                # finds rowcount 0 against the 'active' guard and does
                # nothing further.
                connection.execute(
                    "UPDATE worker_leases SET state = 'released' "
                    "WHERE lease_id = ? AND state = 'active'",
                    (event.parent_tool_use_id,),
                )
        if (
            event.kind in ("subagent.completed", "subagent.failed")
            and event.parent_tool_use_id is not None
            and self._packets is not None
            and event.packet_id is not None
        ):
            outcome = "completed" if event.kind == "subagent.completed" else "failed"
            with suppress(PacketRefused):
                self._packets.settle(
                    event.packet_id,
                    outcome=outcome,
                    tool_use_id=event.parent_tool_use_id,
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
        persisted_session = getattr(handoff, "replacement_session_id", None)
        persisted_profile = getattr(handoff, "replacement_profile_alias", None)
        already_acknowledged = getattr(handoff, "state", None) == "acknowledged"
        if already_acknowledged:
            if persisted_session is not None and persisted_profile is not None:
                # Sol correction b4b545f3 P2: the acknowledgement already
                # persisted the exact replacement identities, so recovery
                # reconstructs them — no lead-runner call, no capacity
                # reselection.
                return await self._recover_acknowledged_rotation(
                    current,
                    handoff_id,
                    replacement_session=UUID(str(persisted_session)),
                    replacement_profile=str(persisted_profile),
                )
            # Sol correction f0a5a403 P1: a migration-era acknowledged row
            # can legitimately lack the durable replacement profile.
            # Falling through would reserve fresh capacity and relaunch a
            # lead for a handoff that is already acknowledged, so fail
            # closed before any reservation or runner call.
            missing = (
                "replacement_profile_alias"
                if persisted_session is not None
                else "replacement_session_id and replacement_profile_alias"
            )
            raise RotationBlocked(
                f"handoff {handoff_id} is acknowledged but its durable "
                f"{missing} is missing (migration-era row): backfill it "
                "with an identical re-acknowledgement naming the selected "
                "profile_alias (or run operator recovery), then retry — "
                "no capacity was reserved and no lead was launched"
            )

        capped_attempts: list[tuple[str, datetime]] = []
        attempted_profiles: set[str] = set()
        while True:
            reservation = self._profiles.reserve_replacement(
                current.project_key,
                current.profile_alias,
            )
            if reservation is None:
                message = "no different healthy profile is available"
                details = [
                    str(value)
                    for value in (getattr(self._profiles, "last_refusal", None),)
                    if value
                ]
                details.extend(
                    f"{alias}: fable-capped until {resets_at.isoformat()}"
                    for alias, resets_at in capped_attempts
                )
                if details:
                    message = f"{message}: {'; '.join(details)}"
                raise RotationBlocked(message)
            if reservation.profile_alias in attempted_profiles:
                self._profiles.cancel_replacement(current.project_key)
                raise RotationBlocked(
                    "replacement selection repeated an already attempted profile: "
                    f"{reservation.profile_alias}"
                )
            attempted_profiles.add(reservation.profile_alias)
            if persisted_session is not None:
                # The durable column is TEXT; parse rather than minting a new
                # session id for an acknowledgement that already named one.
                replacement_session = UUID(str(persisted_session))
            else:
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
            acknowledged = False
            replacement_capped = False
            try:
                stream = self._runner.start_lead(request)
                try:
                    async for event in stream:
                        if event.kind == "session.started":
                            session_started = event.session_id == replacement_session
                        elif event.kind == "provider.limit":
                            resets_at = self._record_replacement_cap(
                                current,
                                handoff_id=handoff_id,
                                replacement_session=replacement_session,
                                replacement_profile=reservation.profile_alias,
                                limit_kind=event.limit_kind,
                            )
                            capped_attempts.append(
                                (reservation.profile_alias, resets_at)
                            )
                            replacement_capped = True
                            self._profiles.cancel_replacement(current.project_key)
                            break
                        elif (
                            event.kind == "handoff.acknowledged"
                            and event.session_id == replacement_session
                            and event.restated_next_action
                        ):
                            # One durable transition persists the acknowledged
                            # state together with BOTH selected identities.
                            self._handoffs.acknowledge(
                                handoff_id,
                                replacement_session,
                                event.restated_next_action,
                                profile_alias=reservation.profile_alias,
                            )
                            acknowledged = True
                            break
                finally:
                    await stream.aclose()
            except BaseException:
                if not replacement_capped:
                    self._profiles.cancel_replacement(current.project_key)
                raise
            if replacement_capped:
                persisted_session = None
                continue
            if not session_started:
                self._profiles.cancel_replacement(current.project_key)
                raise RotationBlocked("replacement session start was not confirmed")
            if not acknowledged:
                self._profiles.cancel_replacement(current.project_key)
                raise RotationBlocked(
                    "handoff was not acknowledged by the replacement"
                )
            break

        return await self._finalize_transfer(
            current,
            handoff_id,
            replacement_session=replacement_session,
            replacement_profile=reservation.profile_alias,
            recovering=False,
        )

    async def _recover_acknowledged_rotation(
        self,
        current: ProjectCell,
        handoff_id: str,
        *,
        replacement_session: UUID,
        replacement_profile: str,
    ) -> ProjectCell:
        """Complete an acknowledged-but-untransferred rotation from the
        identities persisted at acknowledgement (Sol b4b545f3 P2): never
        call the lead runner again and never reselect capacity."""

        if (
            current.session_id == replacement_session
            and current.profile_alias == replacement_profile
        ):
            # The transfer transaction already committed on a prior pass;
            # only the pool's in-memory affinity may still need resyncing
            # to the durable rows.
            self._profiles.release(current.project_key, reason="rotation_recovered")
            self._profiles.restore(
                current.project_key, replacement_profile, self._aware_now()
            )
            return current
        conflict = self._database.execute(
            "SELECT project_key FROM profile_leases WHERE profile_alias = ?",
            (replacement_profile,),
        ).fetchone()
        if conflict is not None:
            raise RotationBlocked(
                f"recovered replacement profile {replacement_profile!r} "
                "already holds a profile lease "
                f"(project {str(conflict['project_key'])!r})"
            )
        return await self._finalize_transfer(
            current,
            handoff_id,
            replacement_session=replacement_session,
            replacement_profile=replacement_profile,
            recovering=True,
        )

    async def _finalize_transfer(
        self,
        current: ProjectCell,
        handoff_id: str,
        *,
        replacement_session: UUID,
        replacement_profile: str,
        recovering: bool,
    ) -> ProjectCell:
        """Run the one transactional lease/cell transfer and its follow-ups."""

        now = self._aware_now().isoformat()
        rotated = ProjectCell(
            cell_id=current.cell_id,
            project_key=current.project_key,
            state="active",
            profile_alias=replacement_profile,
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
            if not recovering:
                self._profiles.cancel_replacement(current.project_key)
            raise
        if recovering:
            # No in-memory reservation exists on recovery: rebuild the
            # pool's affinity from the durable identities instead of
            # committing a reservation that was never re-made.
            self._profiles.release(current.project_key, reason="rotation_recovered")
            self._profiles.restore(
                current.project_key, replacement_profile, self._aware_now()
            )
        else:
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
        limit_kind: str | None = None,
    ) -> bool:
        if self._safety is not None:
            self._safety.invalidate(cell.cell_id, reason="start_failed")
        if self._checkpoints is not None:
            self._checkpoints.resolve_for_cell(
                cell.cell_id, outcome="failed", detail="cell_start_failed"
            )
        now_value = self._aware_now()
        now = now_value.isoformat()
        cooldown_until = (
            self._cap_cooldown(now_value, limit_kind) if capped else None
        )
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
                if limit_kind == "fable":
                    self._record_fable_cap(
                        connection,
                        cell.profile_alias,
                        observed_at=now_value,
                        resets_at=cooldown_until,
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

    async def _project_and_activate(
        self, cell: ProjectCell, issue_id: str
    ) -> tuple[bool, LeadAssignment | None]:
        """Local commit first, then the Linear projection (INFRA-199 v2).

        The daemon-side twin of :func:`activate_admitted_issue`'s
        local-first ordering: :meth:`_activate_issue` — the shared
        activation transaction — commits first and never consults
        Linear; only once it reports success is the idempotent ``In
        Development`` projection attempted, through
        :func:`_project_in_development`, which swallows any Linear
        failure rather than rolling back or retrying the already-
        durable local activation (see its docstring for where that
        failure's durable trace lives).
        """

        activated, assignment = self._activate_issue(cell, issue_id)
        if not activated:
            return False, None
        await _project_in_development(self._linear, issue_id)
        return True, assignment

    def _activate_issue(
        self,
        cell: ProjectCell,
        issue_id: str,
    ) -> tuple[bool, LeadAssignment | None]:
        """Activate the cell and issue; on the classic path, commit the
        durable assignment packet in the very same transaction, so a
        queue transition can never again outrun its assignment.

        Delegates to the shared :func:`_activate_issue_transaction` — the
        same activation state machine the idle dispatcher's
        :func:`activate_admitted_issue` runs. Never consults Linear.

        When the dispatch capacity guard is armed
        (:meth:`require_dispatch_capacity_guard` — the live daemon), the
        transaction runs the IDENTICAL transaction-local guard the idle
        path supplies: :func:`admission_priority_ceiling` on the
        activation transaction's own connection, so a sample that went
        stale — or was superseded by a red one — between scheduler
        planning and this commit fails the activation closed instead of
        authorizing it (Sol ec0ed7fe gap 3).
        """

        return _activate_issue_transaction(
            database=self._database,
            events=self._events,
            cell_id=cell.cell_id,
            project_key=cell.project_key,
            profile_alias=cell.profile_alias,
            issue_id=issue_id,
            session_id=str(cell.session_id),
            assignments=(
                self._assignments if self._classic_seats else None
            ),
            now=self._aware_now,
            guard=self._capacity_guard(issue_id),
        )

    def _capacity_guard(
        self, issue_id: str
    ) -> Callable[[sqlite3.Connection], bool] | None:
        """The armed daemon path's transaction-local freshness/priority
        guard — the same predicate ``cli._dispatch_idle_lead`` builds
        for the idle path, over the same shared
        :func:`admission_priority_ceiling`. ``None`` while unarmed."""

        if self._dispatch_freshness_minutes is None:
            return None
        freshness_minutes = self._dispatch_freshness_minutes

        def guard(connection: sqlite3.Connection) -> bool:
            ceiling = admission_priority_ceiling(
                connection,
                now=self._aware_now(),
                freshness_minutes=freshness_minutes,
            )
            row = connection.execute(
                "SELECT priority FROM admitted_issues WHERE issue_id = ?",
                (issue_id,),
            ).fetchone()
            return (
                row is not None
                and ceiling is not None
                and int(row["priority"]) <= ceiling
            )

        return guard

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

    def _require_handoff(
        self,
        cell: ProjectCell,
        reason: str,
        *,
        limit_kind: str | None = None,
    ) -> None:
        now = self._aware_now()
        cooldown_until = self._cap_cooldown(now, limit_kind)
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
            if limit_kind == "fable":
                self._record_fable_cap(
                    connection,
                    cell.profile_alias,
                    observed_at=now,
                    resets_at=cooldown_until,
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

    @staticmethod
    def _cap_cooldown(now: datetime, limit_kind: str | None) -> datetime:
        """Cooldown horizon for a cap: weekly worst case for fable caps.

        A fable cap exhausts a weekly budget; the stream exposes no reset
        time, so the cooldown conservatively covers one full cycle and the
        recorded observation carries the same horizon — the two can never
        disagree. Session and monthly-spend caps (and non-limit handoffs)
        keep the historical flat hour.
        """

        if limit_kind == "fable":
            return now + _FABLE_CAP_FALLBACK
        return now + timedelta(hours=1)

    def _record_fable_cap(
        self,
        connection: sqlite3.Connection,
        profile_alias: str,
        *,
        observed_at: datetime,
        resets_at: datetime,
    ) -> None:
        """Append the durable fable-capacity observation for a provider cap."""

        connection.execute(
            "INSERT INTO profile_capacity_observations("
            "profile_alias, model, state, source, observed_at, resets_at, "
            "detail) VALUES (?, 'fable', 'capped', 'provider_limit', ?, ?, ?)",
            (
                profile_alias,
                observed_at.isoformat(),
                resets_at.isoformat(),
                _FABLE_CAP_FALLBACK_DETAIL,
            ),
        )

    def _record_replacement_cap(
        self,
        current: ProjectCell,
        *,
        handoff_id: str,
        replacement_session: UUID,
        replacement_profile: str,
        limit_kind: str | None,
    ) -> datetime:
        """Persist replacement cap evidence before its reservation is released."""

        observed_at = self._aware_now()
        resets_at = self._cap_cooldown(observed_at, limit_kind)
        with self._database.transaction() as connection:
            self._record_fable_cap(
                connection,
                replacement_profile,
                observed_at=observed_at,
                resets_at=resets_at,
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="provider.limit",
                    aggregate_type="project_cell",
                    aggregate_id=current.cell_id,
                    payload={
                        "phase": "replacement_acknowledgement",
                        "handoff_id": handoff_id,
                        "profile_alias": replacement_profile,
                        "session_id": str(replacement_session),
                        "limit_kind": limit_kind,
                        "resets_at": resets_at.isoformat(),
                    },
                ),
            )
        self._profiles.set_cooldown(replacement_profile, resets_at)
        return resets_at

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
