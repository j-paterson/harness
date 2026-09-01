"""Durable one-lead-per-project cell lifecycle."""

from __future__ import annotations

import asyncio
import sqlite3
import sys
import uuid
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
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
from hermes_orchestrator.worktrees import bind_issue_worktree

_ACTIVE_CELL_STATES = ("starting", "active", "handoff_required", "paused")

# INFRA-219 L1: the durable lane-role model. A project cell now belongs to
# exactly one lane; ``DEVELOPMENT_LANE`` is the historical, sole lane every
# existing row and caller defaults to, so a project with no harness cell is
# byte-compatible with pre-INFRA-219 behavior. The harness lane's launch/
# rotate/recover surface is wave 2 (INFRA-219 L2) -- this packet only makes
# the durable identity lane-aware.
DEVELOPMENT_LANE = "development"
HARNESS_LANE = "harness"


class HarnessCheckoutPort(Protocol):
    """The exact worktree-git surface the harness checkout needs.

    INFRA-219 R7 / Sol correction 110ed759: a structural subset of
    ``hermes_orchestrator.git.WorktreeGit`` (already used for
    checkpointed worktree reclamation) so :func:`ensure_harness_checkout`
    reuses the existing worktree machinery -- never a bespoke
    ``subprocess`` call -- and so tests exercise it with a fake port
    instead of spawning real git.
    """

    def worktree_list(self, repo_path: Path) -> tuple[str, ...]: ...

    def worktree_add_detached(self, repo_path: Path, path: Path, sha: str) -> None: ...

    def head_sha(self, path: Path) -> str: ...

    def branch(self, path: Path) -> str | None: ...


class HarnessCheckoutRefused(ValueError):
    """A harness checkout's identity disagrees with what Hermes expects.

    A ``ValueError`` subclass on purpose: ``cli.py``'s
    ``_open_rotation_collaborators`` caller already turns a ``ValueError``
    into a clean, nonzero CLI refusal (see its docstring), so this needs
    no new exception handling at the call site to fail closed.
    """


@dataclass(frozen=True, slots=True)
class HarnessCheckoutStatus:
    """The resolved, verified state of one project's harness checkout."""

    path: Path
    provisioned: bool
    head_sha: str
    branch: str | None


def ensure_harness_checkout(
    git: HarnessCheckoutPort,
    *,
    repo_path: Path,
    harness_path: Path,
    expected_branch: str | None = None,
) -> HarnessCheckoutStatus:
    """VALIDATE-then-provision the harness lane's dedicated checkout.

    INFRA-219 R7 / Sol correction 110ed759 (verbatim): ``cli.py``'s
    ``_harness_lead_cwd`` "derives a sibling path but explicitly leaves
    worktree provisioning out of scope" -- nothing ever created or
    validated that checkout, so a harness lane start silently launched
    (or resumed) into a directory that might not exist, or might exist
    as something other than a worktree of THIS repository.

    Strategy, in order:

    1. If ``harness_path`` is already registered in ``git worktree
       list`` for ``repo_path`` (the same idiom ``worktrees.py`` uses at
       its own reconciliation/removal checkpoints -- membership in the
       repo's own worktree list, not a bespoke path comparison) it is a
       genuine worktree of this repository; validated, never re-created.
    2. Otherwise, if nothing exists at ``harness_path`` yet, it is
       PROVISIONED via ``git.worktree_add_detached`` -- the existing
       worktree-materialization primitive INFRA-198 P2 already uses for
       exactly this "bounded, detached worktree at an exact commit"
       shape -- pinned to ``repo_path``'s current HEAD as the stable,
       operational-only harness head.
    3. If ``harness_path`` exists but is NOT registered as a worktree of
       ``repo_path``, this refuses fail-closed with
       :class:`HarnessCheckoutRefused` -- a foreign directory (a stray
       clone, an unrelated project, a leftover from another repo) is
       never silently adopted.

    After validating or provisioning, the resolved checkout's branch
    (when ``expected_branch`` is supplied) and HEAD (always, against the
    commit it was provisioned from) are re-proven to agree with what
    Hermes expects; any disagreement refuses fail-closed rather than
    launching a harness lead into an unexpected identity.
    """

    registered = harness_path in {Path(entry) for entry in git.worktree_list(repo_path)}
    provisioned = False
    if not registered:
        if harness_path.exists():
            raise HarnessCheckoutRefused(
                f"harness checkout {harness_path} exists but is not a git "
                f"worktree of {repo_path} -- refusing to adopt a foreign "
                "directory (INFRA-219 R7 / Sol correction 110ed759)"
            )
        expected_head = git.head_sha(repo_path)
        git.worktree_add_detached(repo_path, harness_path, expected_head)
        provisioned = True
        post = {Path(entry) for entry in git.worktree_list(repo_path)}
        if harness_path not in post:
            raise HarnessCheckoutRefused(
                f"provisioning {harness_path} did not register it as a "
                f"worktree of {repo_path}"
            )
        actual_head = git.head_sha(harness_path)
        if actual_head != expected_head:
            raise HarnessCheckoutRefused(
                f"provisioned harness checkout {harness_path} HEAD "
                f"{actual_head!r} disagrees with the repository HEAD "
                f"{expected_head!r} it was provisioned from"
            )

    actual_branch = git.branch(harness_path)
    if expected_branch is not None and actual_branch != expected_branch:
        raise HarnessCheckoutRefused(
            f"harness checkout {harness_path} is on branch "
            f"{actual_branch!r}, expected {expected_branch!r}"
        )
    return HarnessCheckoutStatus(
        path=harness_path,
        provisioned=provisioned,
        head_sha=git.head_sha(harness_path),
        branch=actual_branch,
    )

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
    """One durable persistent lead for a project's lane.

    INFRA-219 L1: ``lane_role`` is the durable lane identity
    (``DEVELOPMENT_LANE`` or ``HARNESS_LANE``) -- every lookup and
    mutation below is scoped to it, so a harness cell can never
    interrupt or be confused with its project's development cell.
    """

    cell_id: str
    project_key: str
    state: str
    profile_alias: str
    session_id: UUID
    lane_role: str = DEVELOPMENT_LANE


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

# Project occupancy (INFRA-199 v2 / INFRA-211): an issue is "occupying"
# its project — counted against the project's development-lane
# concurrency bound below — while it is in EITHER of these states.
# ``in_development`` is the obvious case; the INFRA-211 failure
# reproduction showed a second issue starting while the first had
# already moved to ``review`` (the lead had handed off, but the
# project is not yet free — the reviewer/settlement flow still owns
# it), so review occupies the project exactly like in_development
# does.
_OCCUPYING_ISSUE_STATES = (
    IssueState.IN_DEVELOPMENT.value,
    IssueState.REVIEW.value,
)

#: The states in which an issue may hold a bound issue worktree lease.
#: DELIBERATELY wider than ``_OCCUPYING_ISSUE_STATES`` and used only by
#: :func:`bind_admitted_issue_worktree` -- never by the lane-occupancy
#: accounting, which must keep counting exactly what it counted before.
#:
#: INFRA-198: a merged, acceptance-gated issue is held in
#: ``post_merge_acceptance`` while its lead keeps working it and keeps
#: publishing candidates -- the observed harness-blocker assignment is
#: exactly that shape. Excluding it made ``candidate-ready`` refuse for
#: an issue actively under assignment, with no supported way to bind.
#: Admitting it here does NOT make the issue dispatchable: it stays
#: outside every dispatch filter, as its own state contract requires.
_LANE_BINDABLE_ISSUE_STATES = (
    *_OCCUPYING_ISSUE_STATES,
    IssueState.POST_MERGE_ACCEPTANCE.value,
)

# INFRA-219 R6 (Sol correction 110ed759): the operator ruling behind
# INFRA-219 is that "one Fable project lead owns a single project ...
# and continuously coordinates up to six explicitly admitted Linear
# issue lanes when resources and dependency boundaries permit" —
# concurrency of ADMITTED ISSUES within one project's development
# lane, not a single-issue-per-project rule. Packet L3 scoped the
# pre-existing "any other occupying issue refuses" rule to the
# development lane (correctly excluding the harness lane, which never
# claims a product issue at all), but Sol's verification of R1-R5
# found that scoping left the ORIGINAL one-issue-per-project refusal
# fully intact *within* that lane — a second admitted issue could
# never activate for the same project even though nothing in the
# contract limits development to one concurrent issue. It limits it
# to a bounded COUNT, six per the contract's own words. This constant
# is that bound: both the coarse pre-check in
# :meth:`ProjectCellService._dispatch_locked` and the transactional,
# commit-time re-proof in :func:`_activate_issue_transaction` count
# distinct OTHER occupying issues (``_OCCUPYING_ISSUE_STATES``,
# excluding the issue being (re-)dispatched itself) and refuse only
# once that count has already reached this many — never merely
# because the count is nonzero. A deliberately plain module constant,
# not new config surface, exactly as R6 requires.
MAX_DEVELOPMENT_ISSUE_LANES = 6


def development_lane_saturated(
    reader: Database | sqlite3.Connection,
    *,
    project_key: str,
    issue_id: str,
) -> bool:
    """The ONE development-lane capacity predicate, side-effect free.

    True exactly when ``project_key``'s development lane already holds
    :data:`MAX_DEVELOPMENT_ISSUE_LANES` OTHER occupying issues —
    distinct ``admitted_issues`` rows in ``in_development`` OR
    ``review`` (INFRA-211: a lead that handed off to review still owns
    its lane until settlement clears it), with ``issue_id`` itself
    always excluded so a resume/replay of an already-occupying issue is
    never refused by its own lane.

    Factored out of :func:`_activate_issue_transaction` (INFRA-220, Sol
    correction 9944530c packet 2) so that every caller — the coarse
    pre-check in :meth:`ProjectCellService._dispatch_locked`, the
    transactional commit-time re-proof in the activation transaction,
    and the publish-time re-proof in
    ``issue_targeting.target_issue`` — shares this single bounded-COUNT
    rule instead of re-implementing it. A duplicated copy in
    ``issue_targeting`` had drifted into the pre-INFRA-219 "any other
    occupying issue refuses" shape and rejected targeting whenever the
    project held even one active issue.

    ``reader`` is whatever connection the caller is already using: pass
    the open transaction's ``sqlite3.Connection`` where the count must
    be a transactional re-proof (the exclusive write lock is what makes
    two racing dispatches unable to together exceed the bound), or the
    :class:`Database` for a read-only pre-check.
    """

    row = reader.execute(
        "SELECT COUNT(*) AS occupying_count FROM admitted_issues "
        "WHERE project_key = ? AND state IN (?, ?) AND issue_id != ?",
        (project_key, *_OCCUPYING_ISSUE_STATES, issue_id),
    ).fetchone()
    return (
        row is not None
        and int(row["occupying_count"]) >= MAX_DEVELOPMENT_ISSUE_LANES
    )


def issue_lane_branch(issue_id: str) -> str:
    """The lane branch this project uses for an admitted issue."""

    return f"feature/{issue_id.lower()}"


def bind_admitted_issue_worktree(
    database: Database,
    leases: object,
    git: object,
    *,
    project_key: str,
    issue_id: str,
    repo_path: Path,
    branch: str | None = None,
    forbidden: Sequence[Path] = (),
    integration_branch: str = "main",
) -> tuple[str, ...]:
    """Materialize and bind ONE admitted issue's dedicated worktree.

    Module level, not a cell-service method, because the INFRA-214
    migration runs from ``reconcile`` — a NON-live runtime that builds
    no ``ProjectCellService`` at all (that needs the profile pool,
    runner and Linear client, none of which this binding touches).
    Gating the catch-up on a live cell service made it silently skip and
    still exit zero, which is the same "looks bound, cannot publish"
    failure this whole path exists to remove.

    The occupancy proof stays attached to the binding wherever it runs:
    an issue that does not currently occupy a development lane of this
    project is REFUSED, so the migration can never invent a lease for a
    completed, unknown, or unadmitted issue.
    """

    placeholders = ",".join("?" for _ in _LANE_BINDABLE_ISSUE_STATES)
    row = database.execute(
        "SELECT issue_id FROM admitted_issues "
        f"WHERE project_key = ? AND issue_id = ? AND state IN ({placeholders})",
        (project_key, issue_id, *_LANE_BINDABLE_ISSUE_STATES),
    ).fetchone()
    if row is None:
        raise ValueError(
            f"{issue_id!r} is not an admitted issue of {project_key!r} in a "
            "lane-bindable state; the catch-up targets one exact issue"
        )
    bind_issue_worktree(
        leases,
        git,
        project_key=project_key,
        issue_id=issue_id,
        repo_path=repo_path,
        branch=branch or issue_lane_branch(issue_id),
        forbidden=tuple(forbidden),
        integration_branch=integration_branch,
    )
    return (issue_id,)


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


class _ActivationRefused(Exception):
    """Internal: unwind the activation transaction with ZERO writes.

    ``Database.transaction`` commits on any normal exit from its body,
    so a plain ``return`` from a point where writes are already staged
    would make those writes durable. Raising this instead takes the
    context manager's ``ROLLBACK`` path; :func:`_activate_issue_transaction`
    catches it at the boundary and reports an ordinary refusal.
    """


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
    on_eligible: Callable[[sqlite3.Connection], bool] | None = None,
    lane_role: str = DEVELOPMENT_LANE,
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
    - the project's development-lane issue-lane bound must not already
      be saturated by OTHER occupying issues — ``in_development`` OR
      ``review`` (INFRA-211: a lead that already handed off to review
      still owns its lane until settlement clears it) — counted
      against :data:`MAX_DEVELOPMENT_ISSUE_LANES` (INFRA-219 R6, Sol
      correction 110ed759: up to six admitted issue lanes may occupy
      one project's development lane concurrently; refusal is a
      bounded-count check, not a "any other occupying issue" check,
      and re-proving THIS count here — not merely re-reading it — is
      exactly what keeps two racing dispatches from ever together
      exceeding the bound);
    - no pending operator decision may exist for the issue;
    - the cell must still BE the cell the caller named, and its profile
      lease must still be active. INFRA-220 (Sol correction 25689ebd
      packet 1): the ``project_cells`` CAS re-proves the cell's CURRENT
      durable identity — ``project_key``, ``lane_role``,
      ``profile_alias`` and ``session_id`` all have to equal the ones
      this activation was asked for, on top of the live-state and
      active-lease predicates. Matching on ``cell_id`` alone let a cell
      that had ROTATED to a different session or profile (or been moved
      to another lane) still be activated on behalf of the stale
      identity that named it: the row was checked for existence, never
      for still being what the caller meant.

    Post-eligibility hook (INFRA-220, Sol correction 25689ebd packet
    2): ``on_eligible``, when supplied, runs LAST — after every
    eligibility predicate above has passed and every activation write
    is staged, but still INSIDE this transaction, on this very
    connection — and a ``False`` return rolls the whole transaction
    back. It is the seam a caller uses to make some other durable
    write commit atomically WITH the activation (``issue_targeting``
    consumes the exact ACK packet there) without reimplementing any of
    this state machine and without the pre-eligibility ``guard``'s
    fatal property: ``guard`` runs BEFORE the predicates, so a write
    made there would commit even when eligibility later refuses.

    An issue that is ALREADY ``in_development`` is the replay/resume
    case: the cell lease is confirmed and the assignment packet is
    (idempotently) ensured, but no second ``issue.started`` is ever
    journaled and no queue transition happens — full replay
    idempotence across effect, transition, event, and assignment.

    INFRA-219 L3: ``lane_role`` scopes PRODUCT-ISSUE occupancy to the
    development lane, per the operator's ruling that "the harness lead
    does not select or implement unrelated product issues." When
    ``lane_role`` is ``HARNESS_LANE`` this transaction skips only the
    occupancy predicates (runnable-state/dependency/busy/pending-
    decision) AND never CASes ``admitted_issues`` or journals
    ``issue.started`` for it — the harness lane activates its own cell
    lease only, and the issue row it names (whatever operational-test
    issue backs its dispatch) is left byte-for-byte untouched. The
    shared ``guard`` (capacity/freshness ceiling, when the daemon path
    arms it) is a RESOURCE limit, not product-issue occupancy, so it
    still runs for the harness lane exactly as the ruling requires
    ("shared profile/resource and one-heavy-test limits continue to
    apply to both lanes") — harness dispatches get no exemption from
    it, including on a resumed/already-active harness lease, since
    ``admitted_issues`` never carries a harness "already in flight"
    signal to replay-exempt against the way development's own issue
    transition does.
    """

    stamp = now().isoformat()
    harness = lane_role == HARNESS_LANE
    try:
        return _activate_issue_body(
            database=database,
            events=events,
            cell_id=cell_id,
            project_key=project_key,
            profile_alias=profile_alias,
            issue_id=issue_id,
            session_id=session_id,
            assignments=assignments,
            stamp=stamp,
            guard=guard,
            on_eligible=on_eligible,
            harness=harness,
            lane_role=lane_role,
        )
    except _ActivationRefused:
        # The transaction rolled back: neither the activation nor
        # anything ``on_eligible`` wrote alongside it is durable.
        return False, None


def _activate_issue_body(
    *,
    database: Database,
    events: EventStore,
    cell_id: str,
    project_key: str,
    profile_alias: str,
    issue_id: str,
    session_id: str,
    assignments: LeadAssignments | None,
    stamp: str,
    guard: Callable[[sqlite3.Connection], bool] | None,
    on_eligible: Callable[[sqlite3.Connection], bool] | None,
    harness: bool,
    lane_role: str,
) -> tuple[bool, LeadAssignment | None]:
    """The transaction body of :func:`_activate_issue_transaction`.

    NEVER call this directly: it can raise :class:`_ActivationRefused`
    (the rollback path a refusing ``on_eligible`` takes), which only
    :func:`_activate_issue_transaction` translates back into an
    ordinary ``(False, None)`` refusal. It exists as its own function
    solely so that ``with database.transaction()`` can sit inside that
    translator's ``try`` without re-indenting the state machine.
    """

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
        replaying = (not harness) and prior_state == IssueState.IN_DEVELOPMENT.value
        if harness:
            if guard is not None and not guard(connection):
                return False, None
        elif not replaying:
            if guard is not None and not guard(connection):
                return False, None
            if prior_state not in _RUNNABLE_ISSUE_STATES:
                return False, None
            if not int(issue_row["dependency_ready"]):
                return False, None
            # INFRA-219 R6 (Sol correction 110ed759): a bounded COUNT of
            # other occupying issues, re-proven transactionally, on the
            # very same connection as the CAS below — never a plain
            # "is any other issue occupying" existence check. This is
            # what makes two dispatches racing on the same project's
            # development lane commit-time-safe: each holds this
            # transaction's exclusive write lock while it counts, so
            # the count either one observes can never be stale by the
            # time it CASes ``admitted_issues`` below, and the bound
            # can never be exceeded no matter how many candidates were
            # concurrently pre-checked in ``_dispatch_locked``.
            if development_lane_saturated(
                connection,
                project_key=str(issue_row["project_key"]),
                issue_id=issue_id,
            ):
                return False, None
            pending_decision = connection.execute(
                "SELECT 1 FROM operator_decisions WHERE issue_id = ? "
                "AND status = 'pending' LIMIT 1",
                (issue_id,),
            ).fetchone()
            if pending_decision is not None:
                return False, None
        # INFRA-220 (Sol correction 25689ebd packet 1): the cell must
        # still BE the cell this activation names. Every identity
        # column the caller bound itself to -- the project, the lane
        # role, the profile alias, and the exact current session -- is
        # a predicate of this one CAS, evaluated on the transaction's
        # own connection at commit time. The previous form matched
        # ``cell_id`` and liveness only, so a cell that had rotated to
        # a new session/profile (or been moved to the harness lane)
        # between the caller's read and this commit was still activated
        # under the STALE identity: the row existed and held a lease,
        # which is not the same thing as it still being what the caller
        # meant. Nothing has been written yet at this point, so a
        # refusal here leaves zero durable writes.
        activated = connection.execute(
            "UPDATE project_cells SET state = 'active', updated_at = ? "
            "WHERE cell_id = ? AND project_key = ? AND lane_role = ? "
            "AND profile_alias = ? AND session_id = ? "
            "AND state IN ('starting', 'active') "
            "AND EXISTS ("
            "SELECT 1 FROM profile_leases "
            "WHERE project_key = ? AND profile_alias = ? AND state = 'active'"
            ")",
            (
                stamp,
                cell_id,
                project_key,
                lane_role,
                profile_alias,
                session_id,
                project_key,
                profile_alias,
            ),
        )
        if activated.rowcount == 0:
            return False, None
        if not harness and not replaying:
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
        if not harness and assignments is not None:
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
        if not harness:
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
            # INFRA-219 L3: the harness lane never claims a product issue,
            # so it never opens this Linear "In Development" projection
            # either -- there is no product-issue transition to project.
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
        if on_eligible is not None and not on_eligible(connection):
            # INFRA-220 (Sol correction 25689ebd packet 2): every
            # eligibility predicate has passed and every activation
            # write above is staged in THIS transaction. The hook ran
            # last, on this same connection, and refused -- so the
            # transaction unwinds and neither the activation nor the
            # hook's own write survives. There is no window in which
            # one is durable without the other.
            raise _ActivationRefused
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
    on_eligible: Callable[[sqlite3.Connection], bool] | None = None,
    lane_role: str = DEVELOPMENT_LANE,
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
    flip, pending decision, stale guard, cell identity, lease
    identity, or a refusing ``on_eligible`` hook) leaves ZERO local
    writes and Linear is never even attempted — there is nothing to
    project for a candidate that did not activate.

    ``on_eligible`` is the post-eligibility transactional callback
    documented on :func:`_activate_issue_transaction`: it runs inside
    the activation transaction once every predicate has passed and
    every activation write is staged, so a caller's own durable write
    commits with the activation or not at all.

    INFRA-219 L3: ``lane_role`` defaults to ``DEVELOPMENT_LANE`` — the
    Stop-hook idle dispatcher never touches the harness lane — so
    every existing zero-argument caller keeps today's occupancy
    behavior exactly.
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
        on_eligible=on_eligible,
        lane_role=lane_role,
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
        lane_project_paths: Mapping[tuple[str, str], Path] | None = None,
        worktree_leases: object | None = None,
        issue_git: object | None = None,
        issue_repo_paths: Mapping[str, Path] | None = None,
        issue_integration_branches: Mapping[str, str] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._queue = queue
        self._profiles = profiles
        self._runner = runner
        self._linear = linear
        self._project_paths = dict(project_paths)
        # INFRA-219 L2: a lane-specific worktree override, keyed by
        # (project_key, lane_role). The harness lane's whole point is
        # that it never shares the development lead's worktree/session,
        # so a caller wiring a harness cell hands its own dedicated
        # checkout here; unset lanes (development, always) fall back to
        # ``self._project_paths`` -- byte-compatible with pre-L2 wiring.
        self._worktree_leases = worktree_leases
        self._issue_git = issue_git
        # The STABLE repository roots, never lead_cwd: an issue lane's
        # dedicated path is derived from the repo, not from the lead's
        # own worktree (INFRA-214).
        self._issue_repo_paths: dict[str, Path] = dict(issue_repo_paths or {})
        # INFRA-214: a first assignment bases its lane on the project's
        # FETCHED integration head, so the branch name comes from config.
        self._issue_integration_branches: dict[str, str] = dict(
            issue_integration_branches or {}
        )
        self._lane_project_paths: dict[tuple[str, str], Path] = dict(
            lane_project_paths or {}
        )
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

    def active_projects(
        self, lane_role: str = DEVELOPMENT_LANE
    ) -> frozenset[str]:
        """Return projects that already own a live logical lead cell in
        ``lane_role``.

        INFRA-219 L1: scoped to one lane (development by default, so
        every zero-argument caller -- the scheduler wiring in
        ``runtime.py`` included -- keeps today's behavior unchanged).
        A harness cell alone never makes this report its project as
        having an active development lead.
        """

        placeholders = ",".join("?" for _ in _ACTIVE_CELL_STATES)
        rows = self._database.execute(
            f"SELECT DISTINCT project_key FROM project_cells "
            f"WHERE state IN ({placeholders}) AND lane_role = ?",
            (*_ACTIVE_CELL_STATES, lane_role),
        ).fetchall()
        return frozenset(str(row["project_key"]) for row in rows)

    def active_cell(
        self, project_key: str, lane_role: str = DEVELOPMENT_LANE
    ) -> ProjectCell | None:
        """Public read of the project's live cell in ``lane_role``.

        INFRA-219 L2: the launch/recover surface (``cli.py``'s
        ``start-lane`` and the dashboard) needs the same lane-scoped
        lookup :meth:`dispatch` uses internally, without reaching into
        the private :meth:`_find_active_cell`.
        """

        return self._find_active_cell(project_key, lane_role)

    def _cell_cwd(self, project_key: str, lane_role: str) -> Path:
        """The worktree a cell in ``lane_role`` launches/resumes into.

        INFRA-219 L2: a harness cell never shares the development
        lead's worktree -- ``lane_project_paths`` (constructor) is
        consulted first for an explicit override, keyed by
        ``(project_key, lane_role)``, and only development (or an
        unconfigured lane) falls back to the historical
        ``self._project_paths`` mapping.
        """

        override = self._lane_project_paths.get((project_key, lane_role))
        if override is not None:
            return override
        return self._project_paths[project_key]

    async def dispatch(
        self,
        issue_id: str,
        *,
        lane_role: str = DEVELOPMENT_LANE,
        harness_run: str | None = None,
    ) -> DispatchResult:
        """Start or resume the issue's project lead after explicit admission.

        INFRA-219 L2: ``lane_role`` (development by default) scopes
        every cell lookup/creation this dispatch performs, so a
        harness-lane dispatch can never find, resume, or collide with
        the project's development cell, and vice versa.

        INFRA-219 L3: ``harness_run`` is the explicit harness-run
        request the operator ruling requires — "the harness lane
        requires an explicit harness-run request bound to
        ``lane_role='harness'``." It must be a non-empty identifier for
        a harness-lane dispatch and MUST be absent for a development-
        lane dispatch (development occupancy is proven by explicit
        issue admission alone, exactly as today; it must never also
        gain a harness-shaped request). Either mismatch refuses
        fail-closed BEFORE the per-lane lock, the queue read, or any
        other touch of durable state — zero writes, a plain status.
        This is deliberately a required call-time argument rather than
        new durable storage: it is enough to prove the caller issued
        this harness dispatch on purpose, and the L3 packet boundary
        prefers it explicitly over adding a migration for it.
        """

        if lane_role == HARNESS_LANE and not harness_run:
            return DispatchResult(status="harness_run_required", issue_id=issue_id)
        if lane_role != HARNESS_LANE and harness_run:
            return DispatchResult(
                status="harness_run_not_permitted", issue_id=issue_id
            )
        issue = self._queue.get(issue_id)
        lock_key = f"{issue.project_key}:{lane_role}"
        lock = self._dispatch_locks.setdefault(lock_key, asyncio.Lock())
        async with lock:
            if self._decisions is not None:
                pending = self._decisions.pending_for_issue(issue_id)
                if pending:
                    return DispatchResult(
                        status="awaiting_operator_decision", issue_id=issue_id
                    )
            return await self._dispatch_locked(issue_id, lane_role=lane_role)

    async def _dispatch_locked(
        self, issue_id: str, lane_role: str = DEVELOPMENT_LANE
    ) -> DispatchResult:
        """Dispatch one issue while holding its project-wide in-process lock."""

        issue = self._queue.get(issue_id)
        # Project occupancy (INFRA-199 v2 / INFRA-211; bounded per
        # INFRA-219 R6, Sol correction 110ed759): a DIFFERENT queued
        # issue is refused here only once this project's development
        # lane already has ``MAX_DEVELOPMENT_ISSUE_LANES`` OTHER issues
        # mid-flight in EITHER in_development or review — the contract
        # is up to six CONCURRENT admitted issue lanes per project, not
        # one. Dispatching an issue that is already one of those
        # occupying lanes (resume after restart, reconciliation,
        # handoff) is untouched — it is excluded from its own count, so
        # it is never refused by the bound — and this is a coarse
        # pre-check only, purely to avoid launching a Claude lead
        # process/turn for a candidate that cannot activate; the shared
        # activation transaction (`_activate_issue_transaction`)
        # re-proves the identical bounded-count predicate,
        # transactionally, at commit time, so this read-only estimate
        # can never itself let the bound be exceeded.
        #
        # INFRA-219 L3: this is a DEVELOPMENT-LANE fact only — the operator
        # ruling is explicit that "the harness lane never claims a product
        # issue," so a harness dispatch is never gated by, and this
        # pre-check never even reads for, product-issue occupancy.
        if lane_role == DEVELOPMENT_LANE and development_lane_saturated(
            self._database, project_key=issue.project_key, issue_id=issue_id
        ):
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
        cell = self._find_active_cell(issue.project_key, lane_role)
        created = False
        if cell is None:
            try:
                cell, created = self._create_cell(
                    issue.project_key, issue_id, lane_role=lane_role
                )
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
            cwd=self._cell_cwd(issue.project_key, lane_role),
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
        # INFRA-205 B: the horizon of a cap persisted the moment the
        # pre-confirmation limit arrived, so the release path reuses the
        # recorded horizon instead of computing a second one that could
        # disagree with the durable observation.
        start_cap_resets_at: datetime | None = None
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
                    await self._fail_unconfirmed_start(
                        cell, issue_id=issue_id
                    )
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
                    await self._fail_unconfirmed_start(
                        cell, issue_id=issue_id
                    )
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
                            # INFRA-205 C: the capped seat can no longer
                            # author its own handoff, and rotate-lead
                            # refuses without one. Hermes forms the
                            # minimal, unmistakably machine-formed
                            # document from durable state so the
                            # EXISTING rotation path is reachable.
                            self._form_capped_seat_handoff(
                                cell, issue_id, limit_kind=limit_kind
                            )
                        else:
                            start_capped = True
                            # INFRA-205 B: persist the cap BEFORE the
                            # reservation is released -- and before the
                            # ``aclose()`` await below, which is exactly
                            # the window in which a concurrent seating
                            # could otherwise re-pick the profile that
                            # just died.
                            start_cap_resets_at = self._record_start_cap(
                                cell,
                                limit_kind=limit_kind,
                                resets_at=getattr(event, "resets_at", None),
                            )
                            break
            finally:
                await stream.aclose()
                if self._active_time is not None and session_confirmed:
                    self._active_time.idle(self._worker_key(cell), self._aware_now())
        except BaseException:
            if created and not session_confirmed:
                await self._fail_unconfirmed_start(
                    cell, issue_id=issue_id, already_completed=issue_completed
                )
            raise

        if created and not session_confirmed and not handoff_required:
            await self._fail_unconfirmed_start(
                cell,
                issue_id=issue_id,
                capped=start_capped,
                already_completed=issue_completed,
                limit_kind=limit_kind,
                capped_resets_at=start_cap_resets_at,
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
                # INFRA-214: the seat persists its TRUE lane, so harness
                # residue is never recorded as the development lane's.
                lane_role=cell.lane_role,
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
                lane_role=current.lane_role,
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
                self._profiles.cancel_replacement(
                    current.project_key, lane_role=current.lane_role
                )
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
                            self._profiles.cancel_replacement(
                                current.project_key, lane_role=current.lane_role
                            )
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
                    self._profiles.cancel_replacement(
                        current.project_key, lane_role=current.lane_role
                    )
                raise
            if replacement_capped:
                persisted_session = None
                continue
            if not session_started:
                self._profiles.cancel_replacement(
                    current.project_key, lane_role=current.lane_role
                )
                raise RotationBlocked("replacement session start was not confirmed")
            if not acknowledged:
                self._profiles.cancel_replacement(
                    current.project_key, lane_role=current.lane_role
                )
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
            self._profiles.release(
                current.project_key,
                reason="rotation_recovered",
                lane_role=current.lane_role,
            )
            self._profiles.restore(
                current.project_key,
                replacement_profile,
                self._aware_now(),
                lane_role=current.lane_role,
            )
            return current
        # No lane scoping needed here either: profile_alias is the
        # global PRIMARY KEY on profile_leases, so this conflict check
        # already asks "does ANY lease anywhere hold this profile" --
        # exactly the global shared-resource limit migration 0055 left
        # untouched ("one profile serves one lease at a time").
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
            lane_role=current.lane_role,
        )
        try:
            with self._database.transaction() as connection:
                # No lane scoping needed here: profile_alias is the
                # table's own PRIMARY KEY (migration 0055's note),
                # globally unique across every lane, so it already names
                # exactly one row -- the rotating cell's own prior lease.
                connection.execute(
                    "DELETE FROM profile_leases WHERE profile_alias = ?",
                    (current.profile_alias,),
                )
                # Sol correction 110ed759 (INFRA-219 R2): migration 0055
                # (packet L4) gave profile_leases a lane_role column with a
                # unique index on (project_key, lane_role), but this insert
                # predates that and always wrote the implicit default
                # 'development' -- rotating a HARNESS cell silently wrote
                # its replacement lease into the development lane,
                # colliding with (or displacing) the real development
                # lease. The replacement lease must carry the rotating
                # cell's OWN lane, never an assumed default.
                connection.execute(
                    "INSERT INTO profile_leases("
                    "profile_alias, project_key, state, acquired_at, "
                    "lane_role"
                    ") VALUES (?, ?, 'active', ?, ?)",
                    (
                        rotated.profile_alias,
                        rotated.project_key,
                        now,
                        rotated.lane_role,
                    ),
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
                self._profiles.cancel_replacement(
                    current.project_key, lane_role=current.lane_role
                )
            raise
        if recovering:
            # No in-memory reservation exists on recovery: rebuild the
            # pool's affinity from the durable identities instead of
            # committing a reservation that was never re-made.
            self._profiles.release(
                current.project_key,
                reason="rotation_recovered",
                lane_role=current.lane_role,
            )
            self._profiles.restore(
                current.project_key,
                replacement_profile,
                self._aware_now(),
                lane_role=current.lane_role,
            )
        else:
            self._profiles.commit_rotation(
                current.project_key,
                current.profile_alias,
                lane_role=current.lane_role,
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

    def _find_active_cell(
        self, project_key: str, lane_role: str = DEVELOPMENT_LANE
    ) -> ProjectCell | None:
        """The project's live cell in ``lane_role`` (development by
        default), or ``None``. INFRA-219 L1: lane-scoped so a harness
        cell is never returned to a development-lane lookup, or vice
        versa -- the two lanes' uniqueness and activation never
        collide.
        """

        placeholders = ",".join("?" for _ in _ACTIVE_CELL_STATES)
        row = self._database.execute(
            f"SELECT * FROM project_cells WHERE project_key = ? "
            f"AND lane_role = ? AND state IN ({placeholders})",
            (project_key, lane_role, *_ACTIVE_CELL_STATES),
        ).fetchone()
        return self._row_to_cell(row) if row is not None else None

    def _restore_profile_leases(self) -> None:
        """Rehydrate durable leases into the pool -- INFRA-219 L4: each
        row now also names the lane the lease belongs to, so a
        project's development and harness leases restore into their
        own distinct ``ProfilePool`` slots rather than colliding on a
        project-only key.
        """

        rows = self._database.execute(
            "SELECT profile_alias, project_key, lane_role, state, "
            "acquired_at, cooldown_until "
            "FROM profile_leases WHERE state IN ('active', 'capped')"
        ).fetchall()
        for row in rows:
            cooldown = row["cooldown_until"]
            lane_role = str(row["lane_role"])
            if str(row["state"]) == "capped":
                if not cooldown:
                    raise ValueError("capped profile lease is missing cooldown_until")
                cooldown_until = datetime.fromisoformat(str(cooldown))
                active_cell = self._database.execute(
                    "SELECT 1 FROM project_cells WHERE project_key = ? "
                    "AND lane_role = ? AND profile_alias = ? "
                    "AND state IN ('starting', 'active', 'handoff_required', 'paused')",
                    (str(row["project_key"]), lane_role, str(row["profile_alias"])),
                ).fetchone()
                if active_cell is not None:
                    self._profiles.restore(
                        project_key=str(row["project_key"]),
                        profile_alias=str(row["profile_alias"]),
                        acquired_at=datetime.fromisoformat(str(row["acquired_at"])),
                        lane_role=lane_role,
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
                lane_role=lane_role,
                cooldown_until=(
                    datetime.fromisoformat(str(cooldown)) if cooldown else None
                ),
            )

    async def _fail_unconfirmed_start(
        self,
        cell: ProjectCell,
        *,
        issue_id: str,
        capped: bool = False,
        already_completed: bool = False,
        limit_kind: str | None = None,
        capped_resets_at: datetime | None = None,
    ) -> bool:
        """Fail one unconfirmed start and release its reservation.

        INFRA-205 B: ``capped_resets_at`` is the horizon of a capacity
        observation this turn ALREADY persisted (at the moment the
        pre-confirmation limit arrived, before this release). When it is
        supplied the lease cooldown reuses that exact horizon and no
        second observation is written; unset, the historical behavior is
        unchanged — the horizon is computed here and the observation is
        recorded in this same transaction.
        """

        if self._safety is not None:
            self._safety.invalidate(cell.cell_id, reason="start_failed")
        if self._checkpoints is not None:
            self._checkpoints.resolve_for_cell(
                cell.cell_id, outcome="failed", detail="cell_start_failed"
            )
        now_value = self._aware_now()
        now = now_value.isoformat()
        cooldown_until = (
            (capped_resets_at or self._cap_cooldown(now_value, limit_kind))
            if capped
            else None
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
                if limit_kind == "fable" and capped_resets_at is None:
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
        self._profiles.release(
            cell.project_key, "lead_start_failed", lane_role=cell.lane_role
        )
        # INFRA-214 (observed live 2026-09-01): the cell and its profile
        # lease were released, but the cmux BINDING was not — the two
        # failed harness starts left two dead visible workspaces and
        # active binding residue behind, so an immediate retry could not
        # start clean. Retire the exact binding for this cell so the
        # seat, its workspace and its channel configuration are durably
        # released alongside the lease.
        await self._retire_failed_seat(cell)
        return True

    async def _retire_failed_seat(self, cell: ProjectCell) -> None:
        """Durably retire the failed start's seat binding, if any.

        Best-effort and never raising: the launch has already failed and
        the caller is on its cleanup path, so a retirement problem is
        reported rather than allowed to mask the original failure. The
        binding lookup is exact-cell, so a sibling lane's live seat is
        never touched (INFRA-214).
        """

        if self._surfaces is None:
            return
        retire = getattr(self._surfaces, "retire_failed_seat", None)
        if retire is None:
            return
        try:
            await retire(
                cell_id=cell.cell_id,
                session_id=str(cell.session_id),
                reason="lead_start_failed",
            )
        except Exception as error:  # pragma: no cover - cleanup guard
            print(
                f"failed-seat retirement for {cell.cell_id!r} did not "
                f"complete: {type(error).__name__}: {error}",
                file=sys.stderr,
            )

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
        lane_role: str = DEVELOPMENT_LANE,
    ) -> tuple[ProjectCell | None, bool]:
        lease = self._profiles.acquire(project_key, lane_role)
        if lease is None:
            return None, False
        now = self._aware_now().isoformat()
        cell = ProjectCell(
            cell_id=self._cell_ids(),
            project_key=project_key,
            state="starting",
            profile_alias=lease.profile_alias,
            session_id=self._session_ids(),
            lane_role=lane_role,
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
                    "lane_role, created_at, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        cell.cell_id,
                        cell.project_key,
                        cell.state,
                        cell.profile_alias,
                        str(cell.session_id),
                        cell.lane_role,
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
                    "profile_alias, project_key, lane_role, state, acquired_at"
                    ") VALUES (?, ?, ?, 'active', ?)",
                    (cell.profile_alias, cell.project_key, cell.lane_role, now),
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
            self._profiles.release(
                project_key, "issue_already_completed", lane_role=lane_role
            )
            raise
        except sqlite3.IntegrityError:
            self._profiles.release(
                project_key, "cell_creation_conflict", lane_role=lane_role
            )
            existing = self._find_active_cell(project_key, lane_role)
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

        INFRA-219 L3: the harness lane never claims a product issue, so
        it never projects one "In Development" either — activation
        alone (the cell lease) is all a harness dispatch commits.
        """

        activated, assignment = self._activate_issue(cell, issue_id)
        if not activated:
            return False, None
        if cell.lane_role != HARNESS_LANE:
            await _project_in_development(self._linear, issue_id)
        return True, assignment

    def bind_missing_issue_lanes(
        self,
        project_key: str,
        *,
        issue_id: str,
        branch: str | None = None,
    ) -> tuple[str, ...]:
        """Materialize and bind lanes for already-active admitted issues.

        INFRA-214 migration path, deliberately TARGETED and OBSERVABLE.
        The dispatch hook binds an issue's dedicated worktree at
        ASSIGNMENT, but issues admitted before that hook existed are
        already ``in_development``, so the hook never runs for them.

        It takes ONE exact ``issue_id`` rather than sweeping every
        occupying issue: a blanket sweep would try to bind legacy issues
        whose feature branches are already checked out in other
        worktrees, and each of those failures would be noise obscuring
        the one binding actually being repaired. It RAISES on failure
        rather than reporting and continuing, because a silent failure
        here reproduces the exact defect this repairs — an issue that
        looks bound but cannot publish.

        ``branch`` overrides the derived lane branch when the issue's
        work already lives on a differently named branch; it is
        validated by ``bind_issue_worktree`` like any other, so it can
        never bind a mismatched or unregistered checkout. Idempotent:
        an issue with a live lease returns it unchanged.
        """

        if self._worktree_leases is None or self._issue_git is None:
            return ()
        cell = self._find_active_cell(project_key, DEVELOPMENT_LANE)
        if cell is None:
            return ()
        repo_path = self._issue_repo_paths.get(project_key)
        if repo_path is None:
            return ()
        return bind_admitted_issue_worktree(
            self._database,
            self._worktree_leases,
            self._issue_git,
            project_key=project_key,
            issue_id=issue_id,
            repo_path=repo_path,
            branch=branch,
            forbidden=self._forbidden_lane_paths(cell),
            integration_branch=self._issue_integration_branches.get(
                project_key, "main"
            ),
        )

    def _bind_issue_lane(
        self, cell: ProjectCell, issue_id: str, *, branch: str | None = None
    ) -> bool:
        """Register the issue lane's dedicated worktree lease, if wired.

        INFRA-214: ``resolve_lane`` refuses publication without exactly
        one live lease for the issue, and nothing in production created
        one — ``worktree_leases`` was empty. The binding belongs here,
        at the assignment boundary, beside the occupancy proof.
        Optional collaborator: with no lease store wired, behavior is
        exactly as before.
        """

        if cell.lane_role != DEVELOPMENT_LANE:
            # INFRA-214: only the DEVELOPMENT lane owns product issue
            # lanes. The harness runs on its own dedicated harness
            # checkout and never publishes product candidates, so it
            # must neither provision an issue worktree nor be blocked by
            # one — binding here would make `start-lane --lane harness`
            # depend on, and fail closed on, a development artifact it
            # will never use.
            return True
        if self._worktree_leases is None or self._issue_git is None:
            return True
        repo_path = self._issue_repo_paths.get(cell.project_key)
        if repo_path is None:
            return True
        lane_branch = branch or self._issue_branch(issue_id)
        try:
            bind_issue_worktree(
                self._worktree_leases,
                self._issue_git,
                project_key=cell.project_key,
                issue_id=issue_id,
                repo_path=repo_path,
                branch=lane_branch,
                forbidden=self._forbidden_lane_paths(cell),
                integration_branch=self._issue_integration_branches.get(
                    cell.project_key, "main"
                ),
            )
        except Exception as error:
            # FAIL CLOSED. Swallowing this and activating anyway is what
            # produced the original defect: an issue that looks assigned
            # but can never publish, discovered only when candidate-ready
            # refuses. The issue stays durably queued instead, so the
            # failure is visible and retryable.
            print(
                f"issue-lane worktree binding for {issue_id!r} failed; "
                f"leaving the issue queued: {type(error).__name__}: {error}",
                file=sys.stderr,
            )
            return False
        return True

    @staticmethod
    def _issue_branch(issue_id: str) -> str:
        return issue_lane_branch(issue_id)

    def _forbidden_lane_paths(self, cell: ProjectCell) -> tuple[Path, ...]:
        """Checkouts that may never carry an issue lane's lease."""

        denied: list[Path] = [Path.cwd()]
        for lane in (DEVELOPMENT_LANE, HARNESS_LANE):
            path = self._lane_project_paths.get((cell.project_key, lane))
            if path is not None:
                denied.append(Path(path))
        shared = self._project_paths.get(cell.project_key)
        if shared is not None:
            denied.append(Path(shared))
        return tuple(denied)

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

        INFRA-219 L3: ``cell.lane_role`` (the durable lane identity the
        cell was created/found in) is threaded straight through, so a
        harness cell's activation is scoped to that lane by construction
        — never a second inference of which lane this is.
        """

        # INFRA-214 / reopened INFRA-219: bind the issue's own dedicated
        # worktree lease at the ASSIGNMENT boundary, so candidate
        # publication can resolve this exact lane's checkout. Never
        # allowed to break dispatch — a lead works fine without the
        # lease; only publication needs it — so a refusal or git failure
        # is reported and activation continues.
        if not self._bind_issue_lane(cell, issue_id):
            return False, None
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
            lane_role=cell.lane_role,
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

    def _record_start_cap(
        self,
        cell: ProjectCell,
        *,
        limit_kind: str | None,
        resets_at: datetime | None = None,
    ) -> datetime | None:
        """Persist a pre-confirmation cap BEFORE its reservation frees.

        INFRA-205 B (observed live): a seat that hit its provider limit
        before confirming released its reservation leaving NO durable
        trace, so the very next seating knew nothing and could re-pick
        the profile that had just died -- which is how ``max-b`` was
        seated while already capped until 2026-09-08.

        Mirrors :meth:`_record_replacement_cap`, the equivalent writer on
        the rotation path, and reuses the same
        :meth:`_record_fable_cap` observation and the same horizon rule,
        so a cap recorded here is indistinguishable from one recorded
        there. The stream's own reset horizon wins when it carries one;
        otherwise the worst-case weekly cycle applies, exactly as
        :meth:`_cap_cooldown` decides for the lease cooldown, so the
        observation and the cooldown can never disagree.

        Only a FABLE cap is recorded: session and monthly-spend limits
        say nothing about weekly Fable capacity, and inventing an
        observation for them would make an unrelated profile ineligible.
        """

        if limit_kind != "fable" or not cell.profile_alias:
            return None
        observed_at = self._aware_now()
        horizon = resets_at or self._cap_cooldown(observed_at, limit_kind)
        if horizon.tzinfo is None:
            horizon = horizon.replace(tzinfo=UTC)
        with self._database.transaction() as connection:
            self._record_fable_cap(
                connection,
                cell.profile_alias,
                observed_at=observed_at,
                resets_at=horizon,
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="provider.limit",
                    aggregate_type="project_cell",
                    aggregate_id=cell.cell_id,
                    payload={
                        "phase": "start_unconfirmed",
                        "profile_alias": cell.profile_alias,
                        "session_id": str(cell.session_id),
                        "limit_kind": limit_kind,
                        "resets_at": horizon.isoformat(),
                    },
                ),
            )
        self._profiles.set_cooldown(cell.profile_alias, horizon)
        return horizon

    def _form_capped_seat_handoff(
        self, cell: ProjectCell, issue_id: str, *, limit_kind: str | None
    ) -> None:
        """Compose the minimal handoff a capped seat can no longer write.

        INFRA-205 C (observed live): ``rotate-lead`` refuses without a
        submitted handoff, and a worker that hits its provider limit
        dies before authoring one -- so the lane was stuck between a
        capped incumbent and the one command that would replace it
        ("no handoff has been submitted for this cell").

        Every field comes from DURABLE state -- the cell, its issue, its
        lane branch, its pending assignment -- and nothing describes work
        that may never have happened: this seat may have produced no
        commits, no tests and no pull request at all. The document says
        so in its own text, so it can never be mistaken for the lead's
        own account of its progress.

        Best effort by construction: the cap itself is already durable
        and the incumbent is already held, so a failure here leaves the
        existing state untouched and merely leaves the operator to open
        the handoff, exactly as before this method existed.
        """

        if self._handoffs is None or not hasattr(self._handoffs, "submit"):
            return
        row = self._database.execute(
            "SELECT assignment_id FROM lead_assignments "
            "WHERE cell_id = ? AND session_id = ? "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (cell.cell_id, str(cell.session_id)),
        ).fetchone()
        assignment = str(row["assignment_id"]) if row is not None else "none"
        from hermes_orchestrator.handoffs import HandoffDocument, HandoffTest

        machine_formed = (
            "machine-formed by Hermes for a provider-capped seat; the "
            "worker reported its "
            f"{limit_kind or 'provider'} limit before it could hand off"
        )
        unknown = "unknown: the capped seat reported none before it exited"
        branch = self._issue_branch(issue_id)
        # Sol correction 41aa9af5: the document is constructed OUTSIDE any
        # exception guard. It is built entirely from our own durable
        # facts, so a schema error here is a programming defect and must
        # surface loudly -- the previous version wrapped construction in
        # a bare ``except`` and turned a validation failure into a silent
        # no-op, leaving rotate-lead unreachable while reporting nothing.
        document = HandoffDocument(
            cell_id=cell.cell_id,
            objective=f"Continue {issue_id} on the {cell.lane_role} lane.",
            status=machine_formed,
            decisions=[machine_formed],
            branch=branch,
            commits=[unknown],
            pull_request="none",
            modified_files=[unknown],
            tests=[
                HandoffTest(
                    command="none",
                    outcome="not run: the seat was capped before working",
                )
            ],
            blockers=[
                f"profile {cell.profile_alias} is fable-capped; "
                f"pending assignment {assignment} is unconfirmed"
            ],
            remaining_steps=[
                f"Re-seat {issue_id} on a fable-capable profile and "
                f"confirm assignment {assignment}."
            ],
            commands=[
                f"hermes-orchestrator rotate-lead --cell {cell.cell_id}"
            ],
            environment_notes=[
                f"lane {cell.lane_role}; branch {branch}; "
                f"session {cell.session_id}"
            ],
            risks=[
                "this document is machine-formed: it describes no work, "
                "because the seat was capped before performing any"
            ],
            next_action=(
                f"Rotate {cell.cell_id} onto a fable-capable profile and "
                f"resume {issue_id}."
            ),
        )
        try:
            self._handoffs.submit(document)
        except Exception as error:
            # Only the durable WRITE is guarded, and never silently: the
            # cap is already persisted and the incumbent already held, so
            # a failed submit leaves the operator exactly where they were
            # -- but it says so rather than reporting a handoff that does
            # not exist.
            print(
                "could not store the capped seat's machine-formed handoff "
                f"for {cell.cell_id!r}: {type(error).__name__}: {error}",
                file=sys.stderr,
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
            lane_role=str(row["lane_role"]),
        )
