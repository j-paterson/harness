"""Turn the Merger's completed review turn into durable review outcomes.

INFRA-166: after a delivered ``FABLE_*`` wake, the Codex Merger thread
reviews the candidate on its own turn and reports either the exact idle
``BLOCKED_ON_EXTERNAL_INTAKE`` line or a structured verdict. This service
is the orchestrator side of that boundary. Sol correction a9cc6d5f: the
only verdict source is an explicit ``submit_review`` submission, durably
claimed under the wake's event_id before settlement. Observation —
turn-completed notifications and startup recovery — is non-settling: it
resumes the settlement of an already-submitted document and otherwise
leaves the wake outstanding, never pulling the thread's report as a
verdict. Nothing here polls: every call is caused by a delivered wake, a
completed-turn notification, or an explicit submission.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from hermes_orchestrator.ci_window import (
    CiWindow,
    MergeWindowExhausted,
    PriorMergeFailed,
)
from hermes_orchestrator.codex_merger import CodexMerger, ReviewerChannel
from hermes_orchestrator.codex_rpc import RpcNotification
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.emission import wake_event_with_drift_hint
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.github import DiscoveredPull, GitHubError
from hermes_orchestrator.manifests import (
    CandidateManifest,
    ManifestError,
    WakeEvent,
    read_manifest_snapshot,
)
from hermes_orchestrator.merge import MergeClient
from hermes_orchestrator.review_intake import (
    AdmittedCandidate,
    CandidateAdmission,
    CandidateRejected,
)
from hermes_orchestrator.reviews import ReviewService
from hermes_orchestrator.verdicts import (
    CorrectionPacket,
    VerdictBinding,
    VerdictError,
    normalize_verdict,
    parse_turn_report,
    parse_verdict,
)

TURN_COMPLETED_METHODS = frozenset({"turn/completed", "thread/turn/completed"})

# INFRA-223 recurrence: the notifications that prove -- from the
# notification itself, never from transcript text -- that a turn is no
# longer active. A completed turn ended normally; an interrupted turn
# was cut short (e.g. an operator- or system-issued ``turn/interrupt``
# request). Both are equally definitive that nothing further will ever
# arrive for that exact turn, so both are eligible to mark an
# unverdicted outstanding wake ``stalled`` in ``handle_turn``.
TURN_INTERRUPTED_METHODS = frozenset(
    {"turn/interrupted", "thread/turn/interrupted"}
)
_TURN_ENDED_METHODS = TURN_COMPLETED_METHODS | TURN_INTERRUPTED_METHODS

#: INFRA-200: the two additive keys :func:`~hermes_orchestrator.verdicts.
#: normalize_verdict` merges into a parsed verdict's ``verdict_json`` --
#: never part of the base envelope :func:`~hermes_orchestrator.verdicts.
#: parse_verdict` itself accepts on input.
_VERDICT_MARKER_KEYS = ("label", "reviewer_fix")


def _settlement_ready_verdict_json(verdict_json: str) -> str:
    """Strip INFRA-200's persisted ``label``/``reviewer_fix`` markers so a
    durably stored, already-normalized verdict can be re-parsed.

    ``submitted_verdicts.verdict_json`` now stores ``ReviewVerdict.
    verdict_json`` -- ``parse_verdict``'s own envelope, normalized to its
    durable value with the original reviewer-facing label (and, for
    ``ACCEPT_WITH_REVIEWER_FIX``, ``reviewer_fix: true``) merged in (see
    ``submit_review``) -- but ``parse_verdict``'s own envelope-key check
    is exact against the five base fields and rejects those two
    additive keys outright. Every settlement re-parse of a durable row
    (direct, recovered, or duplicate-resumed) must therefore strip them
    back out first, here, before calling ``parse_turn_report``. Anything
    that is not a JSON object -- the idle terminal report, or malformed
    text ``parse_verdict`` itself must reject -- passes through
    completely unchanged.
    """

    try:
        value = json.loads(verdict_json)
    except json.JSONDecodeError:
        return verdict_json
    if not isinstance(value, dict):
        return verdict_json
    if not any(key in value for key in _VERDICT_MARKER_KEYS):
        return verdict_json
    filtered = {k: v for k, v in value.items() if k not in _VERDICT_MARKER_KEYS}
    return json.dumps(filtered, sort_keys=True)


def _normalize_verdict_json_best_effort(verdict_json: str) -> str:
    """Best-effort INFRA-200 normalization of a RAW incoming verdict
    document, for duplicate-identity comparison only.

    Mirrors exactly what ``parse_verdict`` computes for its own
    ``verdict_json`` output -- ``{**value, "verdict": durable, **marker}``
    -- so for any genuinely valid envelope this produces the
    byte-identical string ``parse_verdict`` (and therefore
    ``submit_review``'s persistence) will. Unlike ``parse_verdict`` it
    never raises: anything that is not a JSON object with a recognized
    string ``verdict`` field is returned unchanged, and the authoritative
    parse still owns rejecting it. Used only to compare a fresh,
    not-yet-parsed resubmission against an already-persisted
    (already-normalized) row, before the outstanding wake and immutable
    manifest are even read.
    """

    try:
        value = json.loads(verdict_json)
    except json.JSONDecodeError:
        return verdict_json
    if not isinstance(value, dict) or not isinstance(value.get("verdict"), str):
        return verdict_json
    try:
        durable, marker = normalize_verdict(value["verdict"])
    except VerdictError:
        return verdict_json
    document = {**value, "verdict": durable, **marker}
    return json.dumps(document, sort_keys=True)

# INFRA-223: the exact terminal vocabularies the orphaned-submission
# reconciliation joins against. Neither is invented here.
#
# ``wake_deliveries``: the states ``codex_merger._reviewer_is_held``
# documents as "the terminal ``completed``/``deferred``/``rejected``
# written when a verdict settles" -- the same three
# ``release_next_candidate`` names as the terminal wake write, produced by
# ``CodexMerger.complete_admitted_wake`` (``completed``) and
# ``CodexMerger.release_delivered_wake`` (``deferred``/``rejected``).
# Every other state (``pending``, ``claimed``, ``delivered``,
# ``admitted``) still holds the reviewer, so a submission bound to it is
# live by definition.
_TERMINAL_WAKE_STATES = ("completed", "deferred", "rejected")

# ``reviews``: the only two states the codebase already calls terminal
# for a review's OWN settlement -- ``merged`` (``ReviewService.
# _settle_proven``: "this already-terminal review", and the proven-merged
# predicate ``_reconcile_settled_wake`` joins on below) and
# ``corrections_required`` (``_obsolete_wake_reason``: "this event's own
# ``reviews`` row is terminal at ``'corrections_required'``"). Everything
# else -- ``recorded``, ``merging``, ``approved``, ``blocked``,
# ``reconciliation_required``, ``stale`` -- is still in flight and is
# deliberately NOT terminal here, so the reconciliation fails closed.
# ``reviews._LIVE_STATES`` is a different question (which review owns the
# ISSUE's review slot: ``merged`` is live there while terminal here) and
# is deliberately not reused.
_TERMINAL_REVIEW_STATES = ("merged", "corrections_required")


class RpcRequester(Protocol):
    async def request(
        self, method: str, params: dict[str, Any] | None, timeout: float
    ) -> dict[str, Any]: ...


class ThreadReportSource(Protocol):
    """Read the latest completed agent report of one thread."""

    async def latest_report(self, thread_id: str) -> str | None: ...


class WakeDeliverer(Protocol):
    """The existing queue delivery boundary (``codex queue`` only).

    INFRA-221: releasing a queued candidate is not a new transport — it
    is the very same ``CodexQueueDelivery.deliver`` the emitter uses, so
    a released candidate is registered, claimed, rendered, and recorded
    exactly like a freshly emitted one.
    """

    async def deliver(self, project_key: str, event: WakeEvent) -> object: ...


class CorrectionSink(Protocol):
    def deliver(
        self,
        issue_id: str,
        packets: tuple[CorrectionPacket, ...],
        *,
        source: str = ...,
    ) -> object: ...

    def authorized_rework(
        self, project_key: str, candidate: CandidateManifest, packet: CorrectionPacket
    ) -> str | None: ...


class CodexThreadReports:
    """Read the last agent message of a thread's last turn via ``thread/read``.

    The item shape follows the App Server v2 thread payload: the thread's
    turns each carry items, and an ``agentMessage`` item carries ``text``.
    Anything else is treated as no report, never as a verdict.
    """

    def __init__(self, rpc: RpcRequester, *, timeout: float = 60.0) -> None:
        if timeout <= 0:
            raise ValueError("request timeout must be positive")
        self._rpc = rpc
        self._timeout = timeout

    async def latest_report(self, thread_id: str) -> str | None:
        result = await self._rpc.request(
            "thread/read", {"threadId": thread_id, "includeTurns": True}, self._timeout
        )
        thread = result.get("thread")
        if not isinstance(thread, dict):
            return None
        turns = thread.get("turns")
        if not isinstance(turns, list) or not turns:
            return None
        last = turns[-1]
        if not isinstance(last, dict):
            return None
        if last.get("status") not in (None, "completed"):
            return None
        items = last.get("items")
        if not isinstance(items, list):
            return None
        for item in reversed(items):
            if isinstance(item, dict) and item.get("type") == "agentMessage":
                text = item.get("text")
                return text if isinstance(text, str) and text.strip() else None
        return None


@dataclass(frozen=True, slots=True)
class TurnOutcome:
    """Bounded result of handling one project's Merger turn."""

    project_key: str
    kind: str
    event_id: str | None
    issue_id: str | None
    reason: str
    review_id: str | None = None
    merge_sha: str | None = None


@dataclass(frozen=True, slots=True)
class RetryResult:
    """Bounded result of one live retry of a single stalled wake.

    Sol correction 538f1b4e: a supported retry must actually re-deliver
    the exact wake through the existing queue path, not merely reset its
    row. ``retried`` is true iff the CAS from ``'stalled'`` to
    ``'pending'`` applied (idempotent -- a second call for the same wake
    once it is no longer ``'stalled'`` returns ``retried=False`` with no
    further write). ``delivered`` is true only when the SAME delivery
    adapter :meth:`MergerTurnService.release_next_candidate` uses
    actually rendered this exact wake into the reviewer thread; when the
    reviewer is held by another candidate the CAS still applies (the
    wake is durably queued again) but ``delivered`` is false and
    ``reason`` names why, exactly mirroring the delivery adapter's own
    ``reason`` for a queued release.
    """

    event_id: str
    issue_id: str | None
    retried: bool
    delivered: bool
    reason: str


class SubmissionRejected(ValueError):
    """An explicit Sol submission that fails closed with no side effects."""


@dataclass(frozen=True, slots=True)
class _Submission:
    """One durable exactly-once row of the submitted_verdicts table."""

    event_id: str
    project_key: str
    issue_id: str
    candidate_sha: str
    reviewed_thread_id: str
    reviewed_generation: int
    verdict_json: str
    state: str
    result_json: str | None


def _outcome_to_json(outcome: TurnOutcome) -> str:
    return json.dumps(
        {
            "project_key": outcome.project_key,
            "kind": outcome.kind,
            "event_id": outcome.event_id,
            "issue_id": outcome.issue_id,
            "reason": outcome.reason,
            "review_id": outcome.review_id,
            "merge_sha": outcome.merge_sha,
        },
        sort_keys=True,
    )


def _outcome_from_json(text: str) -> TurnOutcome:
    value = json.loads(text)
    return TurnOutcome(
        project_key=str(value["project_key"]),
        kind=str(value["kind"]),
        event_id=value["event_id"],
        issue_id=value["issue_id"],
        reason=str(value["reason"]),
        review_id=value["review_id"],
        merge_sha=value["merge_sha"],
    )


def _approval_ineligibility_reason(
    discovered: DiscoveredPull | None, project: ProjectConfig
) -> str | None:
    """``None`` when GitHub proves an approval-eligible pull; else why not.

    Sol correction 6e1bfe60 (INFRA-217, Important): a *discovered* pull is
    not automatically an *eligible* one. Before this correction, a
    closed-unmerged exact-head pull was still returned by discovery, and
    the only rejection test was ``discovered is None`` — so a closed
    pull's approval was persisted here and rejected only later by the
    merge driver, consuming the wake and forcing a corrected retry to
    conflict with the stale submission. This predicate is the single
    source of truth for approval eligibility, applied identically at both
    call sites (``submit_review`` before any persistence, and
    ``_settle_wake`` before any review/settlement/wake-terminal write):
    eligible means GitHub proved exactly one same-repository pull at the
    reviewed branch and SHA, whose head repository is also the project
    repository (no foreign fork slip-through), that targets the
    project's integration branch (no wrong-base slip-through), and that
    is either still open or already merged — closed-unmerged is
    INELIGIBLE. Head branch and head SHA are already exact by
    construction of ``discover_pull_request``'s query and need no
    re-check here.
    """

    if discovered is None:
        return (
            "approval requires a pull request at the exact reviewed head; "
            "none was discovered on GitHub"
        )
    if (
        discovered.repository != project.github_repo
        or discovered.head_repository != project.github_repo
    ):
        return (
            "approval requires a same-repository pull request; the "
            "discovered pull's repository or head repository is not the "
            "project repository"
        )
    if discovered.base_ref != project.integration_branch:
        return (
            "approval requires a pull request targeting the project's "
            "integration branch; the discovered pull targets a different "
            "base"
        )
    if discovered.state != "open" and not discovered.merged:
        return (
            "approval requires an open or already-merged pull request; "
            "the discovered pull is closed and unmerged"
        )
    return None


class MergerTurnService:
    """Admit the delivered wake and settle the submitted verdict, once."""

    def __init__(
        self,
        *,
        database: Database,
        projects: Mapping[str, ProjectConfig],
        merger: CodexMerger,
        admission: CandidateAdmission,
        reviews: ReviewService,
        reports: ThreadReportSource,
        github: MergeClient,
        lead: CorrectionSink,
        window: CiWindow,
        manifest_root: Path,
        now: Callable[[], datetime] | None = None,
        delivery: WakeDeliverer | None = None,
    ) -> None:
        self._database = database
        self._window = window
        # INFRA-221: the wake path used to release the next durably
        # queued candidate once the current verdict settles. Optional so a
        # caller that has not wired delivery keeps today's behavior: the
        # gate still holds later candidates queued, they are simply
        # released at the next boundary that does have it.
        self._delivery = delivery
        self._projects = dict(projects)
        self._merger = merger
        self._admission = admission
        self._reviews = reviews
        self._reports = reports
        self._github = github
        self._lead = lead
        self._manifest_root = manifest_root
        self._now = now or (lambda: datetime.now(UTC))
        self._events = EventStore(self._database)

    def outstanding_wake(self, project_key: str) -> tuple[WakeEvent, str] | None:
        """The project's admitted wake, else its oldest delivered wake.

        INFRA-216 rework R2: before a candidate row is returned, any
        admitted or delivered wake whose review is durably proven merged
        AND whose merge settlement is durably settled is first reconciled
        to ``'completed'`` -- exactly the terminal vocabulary
        ``complete_admitted_wake`` writes for a cleanly settled wake (see
        ``_settle_wake``/``_record_settled``). Without this, a wake whose
        review was driven to completion but never transitioned (crash
        after settlement, an externally reconciled merge, or a settlement
        resumed by ``resume_settlements`` without its wake row catching
        up) would permanently mask the genuinely outstanding wake behind
        it: ``handle_turn`` would keep selecting the dead row and report
        ``awaiting_submission`` forever. The predicate is exact and
        fail-closed, joined on the wake's own identity -- ``project_key``,
        ``event_id``, and ``candidate_sha`` -- against the ``reviews`` row
        for that event (``state = 'merged'``) and the ``merge_settlements``
        row for that event (``state = 'settled'``); anything ambiguous or
        missing leaves the row untouched and selection is unchanged.
        Reconciled rows are skipped in the same pass, so the next
        genuinely outstanding wake surfaces without a second call.

        INFRA-218 S2: a second, independent retirement reason runs in the
        same scan, mirroring the shape above exactly -- ``_reconcile_
        superseded_wake`` retires an OBSOLETE older wake for the SAME
        issue (unreviewed, or reviewed with corrections and no longer
        live) once a strictly newer wake for that issue carries a
        candidate SHA proven -- via ``ReviewService.
        is_descendant_candidate`` -- to be a git descendant of the older
        wake's ``candidate_sha``. Different issues never supersede each
        other, a wake with a live submitted/settling verdict is never
        retired, and the newest wake for an issue has no newer row to be
        superseded by, so it is never retired either. Skipped rows are
        the same terminal ``'completed'`` vocabulary written above, so
        the surviving newest wake for the issue surfaces in this same
        pass without a second call.

        INFRA-223 recurrence: the scan below is deliberately limited to
        exactly ``'admitted'``/``'delivered'`` and must stay that way --
        a wake ``_mark_wake_stalled`` moved to ``'stalled'`` is NEVER
        outstanding here, which is precisely what lets
        ``MergerSession.review_active`` see it as no longer live.
        """

        for state in ("admitted", "delivered"):
            rows = self._database.execute(
                "SELECT status, issue_id, candidate_sha, base_sha, "
                "manifest_path, event_id, manifest_digest FROM wake_deliveries "
                "WHERE project_key = ? AND state = ? "
                "ORDER BY created_at ASC, rowid ASC",
                (project_key, state),
            ).fetchall()
            for row in rows:
                if self._reconcile_settled_wake(project_key, state, row):
                    continue
                if self._reconcile_superseded_wake(project_key, state, row):
                    continue
                return _row_to_event(row), state
        return None

    def _reconcile_settled_wake(
        self, project_key: str, state: str, row: sqlite3.Row
    ) -> bool:
        """Reconcile one stale admitted/delivered row to 'completed'.

        Returns ``True`` iff the row was reconciled (so the caller should
        keep scanning past it for the genuinely outstanding wake); ``False``
        leaves the row exactly as it was, meaning it is still outstanding.
        Both durable facts -- a proven-merged review and a settled
        settlement -- must exist for this exact wake identity or nothing
        is written.
        """

        event_id = str(row["event_id"])
        candidate_sha = str(row["candidate_sha"])
        merged_review = self._database.execute(
            "SELECT 1 FROM reviews WHERE project_key = ? AND event_id = ? "
            "AND reviewed_sha = ? AND state = 'merged' LIMIT 1",
            (project_key, event_id, candidate_sha),
        ).fetchone()
        if merged_review is None:
            return False
        settled_settlement = self._database.execute(
            "SELECT 1 FROM merge_settlements WHERE project_key = ? "
            "AND event_id = ? AND candidate_sha = ? AND state = 'settled' "
            "LIMIT 1",
            (project_key, event_id, candidate_sha),
        ).fetchone()
        if settled_settlement is None:
            return False
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE wake_deliveries SET state = 'completed', "
                "updated_at = ? WHERE project_key = ? AND event_id = ? "
                "AND state = ?",
                (stamp, project_key, event_id, state),
            )
            if cursor.rowcount == 1:
                self._events.append(
                    connection,
                    EventInput(
                        event_type="wake_delivery.reconciled_complete",
                        aggregate_type="wake_delivery",
                        aggregate_id=f"wake:{project_key}:{event_id}",
                        correlation_id=event_id,
                        actor="merger_turns",
                        payload={
                            "project_key": project_key,
                            "issue_id": str(row["issue_id"]),
                            "candidate_sha": candidate_sha,
                            "prior_state": state,
                            "reason": (
                                "review already merged and merge settlement "
                                "already settled; reconciled before "
                                "outstanding-wake selection (INFRA-216 R2)"
                            ),
                        },
                    ),
                )
        return cursor.rowcount == 1

    def _obsolete_wake_reason(
        self,
        project_key: str,
        event_id: str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> str | None:
        """``'unreviewed'``/``'correction-completed'`` iff obsolete, else ``None``.

        INFRA-218 S2: the two -- and only two -- ways an older wake can be
        obsolete. ``unreviewed``: no ``submitted_verdicts`` row exists at
        all for this exact event_id -- nothing was ever submitted for it.
        ``correction-completed``: this event's own ``reviews`` row is
        terminal at ``'corrections_required'`` -- its verdict returned
        corrections and settled there; it is not merging and nothing
        further is live for it. Anything else -- a live ``'submitted'``
        row awaiting settlement, or a review that is ``'approved'``,
        ``'merged'``, or otherwise still in flight -- is NOT obsolete and
        answers ``None``, so it can never be retired here.

        INFRA-218 Sol correction 44eb2806 (c): accepts an optional
        transaction ``connection`` so ``_reconcile_superseded_wake`` can
        RE-PROVE this exact predicate on the same connection immediately
        before its retiring UPDATE -- a true compare-and-set read,
        never a stale read taken before the transaction started.
        Defaults to ``self._database`` (an out-of-transaction read) for
        every other caller, unchanged.
        """

        executor = connection if connection is not None else self._database
        submitted = executor.execute(
            "SELECT 1 FROM submitted_verdicts WHERE project_key = ? "
            "AND event_id = ? LIMIT 1",
            (project_key, event_id),
        ).fetchone()
        if submitted is None:
            return "unreviewed"
        review = executor.execute(
            "SELECT state FROM reviews WHERE project_key = ? AND event_id = ? "
            "LIMIT 1",
            (project_key, event_id),
        ).fetchone()
        if review is not None and str(review["state"]) == "corrections_required":
            return "correction-completed"
        return None

    def _reconcile_superseded_wake(
        self, project_key: str, state: str, row: sqlite3.Row
    ) -> bool:
        """Retire one OBSOLETE older wake proven superseded by a newer one.

        INFRA-218 S2 (Linear clause 1): "A newer candidate for the same
        issue whose commit is a descendant of an older candidate
        supersedes that issue's obsolete unreviewed or
        correction-completed wakes, so a stale wake can never hold
        intake." Mirrors ``_reconcile_settled_wake`` exactly -- same
        terminal ``'completed'`` state, same conditional UPDATE shape,
        same single journaled event -- with a different, independent
        retirement predicate:

        1. This row's own wake must be obsolete (``_obsolete_wake_reason``);
           a live submitted/settling verdict is never obsolete and this
           returns ``False`` immediately.
        2. Some OTHER admitted/delivered wake for the SAME
           ``project_key`` and ``issue_id`` must exist strictly newer
           than this row (later ``created_at``, tie-broken by rowid) --
           different issues never supersede each other, and a row with
           no strictly-newer sibling (including the newest wake for the
           issue) is left untouched.
        3. That newer wake's ``candidate_sha`` must be a PROVEN
           descendant of this row's ``candidate_sha`` via
           ``ReviewService.is_descendant_candidate`` -- fail-closed by
           construction, so an unprovable or unrelated ancestry never
           retires anything.

        Returns ``True`` iff the row was retired (the caller keeps
        scanning past it); ``False`` leaves it exactly as it was, still
        outstanding. Idempotent: once retired the row is no longer
        ``admitted``/``delivered`` and a later pass never reconsiders it.

        INFRA-218 Sol correction 44eb2806 (c): steps 1-3 above classify
        obsolescence and prove descendant ancestry OUTSIDE any
        transaction, and the retiring UPDATE below used to guard only on
        the wake's own ``state`` -- so a verdict submitted for this
        EXACT event, concurrently with this method's read window, could
        be orphaned: its wake would complete underneath it (state
        ``'completed'``) with nothing left outstanding to settle it,
        even though the verdict now genuinely needs settlement. Retiring
        a wake is now a transactional compare-and-set: immediately
        before the UPDATE, inside the SAME transaction, the obsolete
        predicate is RE-PROVEN from ``_obsolete_wake_reason`` against
        that transaction's own connection. Only an identical
        classification to the one this call started with (no live
        ``submitted`` row now exists for this event, and the review
        state is still the one that made it obsolete) proceeds to
        retirement; any drift -- most critically a verdict submitted in
        the interim -- refuses retirement outright and leaves the wake
        exactly as it was, still outstanding, so its now-live verdict is
        never orphaned.
        """

        event_id = str(row["event_id"])
        issue_id = str(row["issue_id"])
        candidate_sha = str(row["candidate_sha"])
        reason = self._obsolete_wake_reason(project_key, event_id)
        if reason is None:
            return False
        anchor = self._database.execute(
            "SELECT created_at, rowid AS rid FROM wake_deliveries "
            "WHERE project_key = ? AND event_id = ?",
            (project_key, event_id),
        ).fetchone()
        if anchor is None:
            return False
        newer_rows = self._database.execute(
            "SELECT candidate_sha FROM wake_deliveries WHERE project_key = ? "
            "AND issue_id = ? AND state IN ('admitted', 'delivered') "
            "AND event_id != ? "
            "AND (created_at > ? OR (created_at = ? AND rowid > ?)) "
            "ORDER BY created_at ASC, rowid ASC",
            (
                project_key,
                issue_id,
                event_id,
                anchor["created_at"],
                anchor["created_at"],
                anchor["rid"],
            ),
        ).fetchall()
        superseded_by: str | None = None
        for newer in newer_rows:
            newer_sha = str(newer["candidate_sha"])
            if self._reviews.is_descendant_candidate(
                project_key, newer_sha=newer_sha, older_sha=candidate_sha
            ):
                superseded_by = newer_sha
                break
        if superseded_by is None:
            return False
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            # INFRA-218 Sol correction 44eb2806 (c): the transactional
            # compare-and-set re-proof -- refuse retirement if a
            # concurrently submitted verdict (or any other drift) has
            # made this wake no longer obsolete for the reason it was
            # classified under, above.
            live_reason = self._obsolete_wake_reason(
                project_key, event_id, connection=connection
            )
            if live_reason != reason:
                return False
            cursor = connection.execute(
                "UPDATE wake_deliveries SET state = 'completed', "
                "updated_at = ? WHERE project_key = ? AND event_id = ? "
                "AND state = ?",
                (stamp, project_key, event_id, state),
            )
            if cursor.rowcount == 1:
                self._events.append(
                    connection,
                    EventInput(
                        event_type="wake_delivery.reconciled_complete",
                        aggregate_type="wake_delivery",
                        aggregate_id=f"wake:{project_key}:{event_id}",
                        correlation_id=event_id,
                        actor="merger_turns",
                        payload={
                            "project_key": project_key,
                            "issue_id": issue_id,
                            "candidate_sha": candidate_sha,
                            "prior_state": state,
                            "reason": (
                                f"superseded by proven descendant candidate "
                                f"{superseded_by} for the same issue "
                                f"({reason}; INFRA-218 S2)"
                            ),
                        },
                    ),
                )
        return cursor.rowcount == 1

    def reconcile_orphaned_submissions(self, project_key: str) -> tuple[str, ...]:
        """Settle every ``submitted`` verdict whose own wake AND review ended.

        INFRA-223: ``has_pending_submission`` treated ANY row still in
        state ``'submitted'`` as live review work, with no reconciliation
        against that submission's own wake or review, so
        ``MergerSession.review_active`` reported live work for a purely
        historical submission and reopened -- and held -- a Codex App
        Server over an idle Sol thread. Observed live: two submissions
        stayed ``'submitted'`` while their matching ``reviews`` rows were
        proven merged and their ``wake_deliveries`` rows were
        ``completed`` (a crash, or a lost signal, between the settlement's
        durable review/wake writes and ``_record_settled``'s terminal
        write on the submission itself).

        The repair is durable, not a filter: each ``submitted`` row is
        joined to its OWN exact ``wake_deliveries`` row and its OWN exact
        ``reviews`` row -- by this submission's ``event_id``, and (like
        ``_reconcile_settled_wake``) by its ``candidate_sha`` against the
        review's ``reviewed_sha`` -- and is settled only when BOTH are
        terminal (``_TERMINAL_WAKE_STATES`` / ``_TERMINAL_REVIEW_STATES``,
        both taken from the existing vocabularies). Both joins are INNER:
        a submission whose wake or review row is missing proves nothing
        and is left exactly as it is, as is one whose wake still holds the
        reviewer (``delivered``/``admitted``/``pending``/``claimed``) or
        whose review is still in flight.

        The settled result is DERIVED from those durable rows only -- the
        review's own terminal state as the outcome kind, its ``review_id``
        and proven ``merge_sha`` -- and is written through the very same
        ``_record_settled`` terminal path every settlement uses. The
        reviewer thread is never read, no verdict or approval is ever
        inferred, and no review, merge, or pull request is re-run: the
        review and merge this submission describes already reached their
        terminal durable state, which is the whole precondition.

        Idempotent by construction, so a restart repeats it safely: the
        scan selects only ``state = 'submitted'`` rows, and
        ``_record_settled``'s UPDATE is itself a compare-and-set on
        ``state = 'submitted'`` -- a row settled by this pass, or
        concurrently by a real settlement, is matched by neither on the
        next run, and exactly one journal event is written per repaired
        row. Returns the event ids actually repaired (empty on the second
        and every later pass).
        """

        wake_slots = ",".join("?" for _ in _TERMINAL_WAKE_STATES)
        review_slots = ",".join("?" for _ in _TERMINAL_REVIEW_STATES)
        rows = self._database.execute(
            "SELECT s.event_id AS event_id, s.issue_id AS issue_id, "
            "s.candidate_sha AS candidate_sha, w.state AS wake_state, "
            "r.review_id AS review_id, r.state AS review_state, "
            "r.merge_sha AS merge_sha FROM submitted_verdicts AS s "
            "JOIN wake_deliveries AS w ON w.project_key = s.project_key "
            "AND w.event_id = s.event_id "
            "JOIN reviews AS r ON r.project_key = s.project_key "
            "AND r.event_id = s.event_id AND r.reviewed_sha = s.candidate_sha "
            "WHERE s.project_key = ? AND s.state = 'submitted' "
            f"AND w.state IN ({wake_slots}) AND r.state IN ({review_slots}) "
            "ORDER BY s.created_at ASC, s.rowid ASC",
            (project_key, *_TERMINAL_WAKE_STATES, *_TERMINAL_REVIEW_STATES),
        ).fetchall()
        reconciled: list[str] = []
        for row in rows:
            event_id = str(row["event_id"])
            issue_id = str(row["issue_id"])
            wake_state = str(row["wake_state"])
            review_state = str(row["review_state"])
            merge_sha = row["merge_sha"]
            outcome = TurnOutcome(
                project_key=project_key,
                kind=review_state,
                event_id=event_id,
                issue_id=issue_id,
                reason=(
                    "the submission's own wake is terminal "
                    f"('{wake_state}') and its own review is terminal "
                    f"('{review_state}'); settled from those durable rows "
                    "alone, with no review or merge re-run and no verdict "
                    "inferred (INFRA-223)"
                ),
                review_id=str(row["review_id"]),
                merge_sha=None if merge_sha is None else str(merge_sha),
            )
            if not self._record_settled(event_id, outcome):
                continue
            reconciled.append(event_id)
            with self._database.transaction() as connection:
                self._events.append(
                    connection,
                    EventInput(
                        event_type="submitted_verdict.reconciled_settled",
                        aggregate_type="submitted_verdict",
                        aggregate_id=f"verdict:{project_key}:{event_id}",
                        correlation_id=event_id,
                        actor="merger_turns",
                        payload={
                            "project_key": project_key,
                            "issue_id": issue_id,
                            "candidate_sha": str(row["candidate_sha"]),
                            "wake_state": wake_state,
                            "review_state": review_state,
                            "reason": outcome.reason,
                        },
                    ),
                )
        return tuple(reconciled)

    def has_pending_submission(self, project_key: str) -> bool:
        """True iff a LIVE ``submitted_verdicts`` row for the project exists.

        Durable SQLite check (INFRA-198 P1): used by ``MergerSession`` to
        decide whether review work is active without any RPC or model
        call. Distinct from ``_pending_submission``, which is scoped to
        one exact wake's ``event_id``.

        INFRA-223: a historical submission -- one whose own wake and own
        review are both already terminal -- is REPAIRED first
        (``reconcile_orphaned_submissions``, which settles it durably)
        and the answer is then read from the repaired state, rather than
        being filtered out of this query and left orphaned in the
        database. In the steady state nothing is repairable and this
        stays a pair of reads.
        """

        self.reconcile_orphaned_submissions(project_key)
        row = self._database.execute(
            "SELECT 1 FROM submitted_verdicts "
            "WHERE project_key = ? AND state = 'submitted' LIMIT 1",
            (project_key,),
        ).fetchone()
        return row is not None

    async def settle_idle_thread(
        self, projects: Sequence[str], thread_id: str
    ) -> TurnOutcome | None:
        """Non-cli reuse of the idle-thread crash-recovery settlement.

        Same rule as ``cli._settle_idle_merger_thread`` (INFRA-198 P1):
        an idle ``thread/status/changed`` for a project's bound reviewer
        channel resumes settlement ONLY when a durable ``submitted``
        verdict already exists for the project's outstanding wake; it
        never pulls the thread's report as a verdict. Kept here, not in
        cli.py, so ``MergerSession``'s own listener can reuse it without
        importing the CLI module.
        """

        for project_key in projects:
            channel = self._merger.read_channel(project_key)
            if channel is None or channel.thread_id != thread_id:
                continue
            outstanding = self.outstanding_wake(project_key)
            if outstanding is None:
                return None
            event, _state = outstanding
            if self._pending_submission(project_key, event.event_id) is None:
                return None
            return await self.handle_turn(project_key)
        return None

    async def on_notification(
        self, notification: RpcNotification
    ) -> TurnOutcome | None:
        """Handle a completed/interrupted-turn notification for a known thread.

        INFRA-223 recurrence: this is the ONLY caller that ever passes
        ``turn_ended_thread_id`` to :meth:`handle_turn` -- the exact
        ``threadId`` this completed/interrupted notification names, never
        inferred any other way. That is what lets ``handle_turn`` decide,
        from the notification alone, that a turn with no submitted
        verdict has genuinely ended rather than merely being observed
        mid-flight by recovery or an idle-thread resume.
        """

        if notification.method not in _TURN_ENDED_METHODS:
            return None
        thread_id = notification.params.get("threadId")
        if not isinstance(thread_id, str):
            return None
        for project_key in self._projects:
            channel = self._merger.read_channel(project_key)
            if channel is not None and channel.thread_id == thread_id:
                return await self.handle_turn(
                    project_key, turn_ended_thread_id=thread_id
                )
        return None

    async def recover_outstanding(
        self, project_keys: tuple[str, ...] | None = None
    ) -> tuple[TurnOutcome, ...]:
        """Resume any submitted-but-unsettled verdict after a lost signal.

        INFRA-194: the completed-turn notification is delivery, not
        truth — a daemon restart, an rpc drop, or a crashed handler
        loses it while the delivered wake and any durably submitted
        verdict both survive. This boundary pass is non-settling
        observation: each project with an outstanding wake gets exactly
        one ``handle_turn``, which resumes settlement only when a
        ``submitted`` verdict row already exists and otherwise leaves
        the wake outstanding — it never pulls the thread's report as a
        verdict source. Never called on a timer — only at startup and
        explicit intake boundaries, so nothing polls.

        INFRA-223: the orphaned-submission repair rides this same
        existing startup/recovery boundary (``MergerSession.startup``
        calls it immediately after ``ReviewService.resume_settlements``),
        so a submission left ``'submitted'`` behind an already-terminal
        wake and review is settled durably at daemon startup instead of
        holding an App Server open over an idle Sol thread. No new hook,
        and the pass is idempotent across restarts.
        """

        outcomes: list[TurnOutcome] = []
        for project_key in project_keys or tuple(self._projects):
            self.reconcile_orphaned_submissions(project_key)
            if self.outstanding_wake(project_key) is None:
                # INFRA-221: a restart between a settled verdict and the
                # release of the candidate queued behind it leaves nothing
                # outstanding and a durably pending wake. Release it here
                # so the queue can never wedge; when a candidate IS still
                # current this releases nothing and recovery reopens that
                # same candidate below, never advancing past it.
                await self.release_next_candidate(project_key)
                continue
            try:
                outcomes.append(await self.handle_turn(project_key))
            except Exception as error:  # pragma: no cover - infra guard
                outcomes.append(
                    TurnOutcome(
                        project_key,
                        "recovery_failed",
                        None,
                        None,
                        f"{type(error).__name__}: {error}",
                    )
                )
        return tuple(outcomes)

    async def release_next_candidate(self, project_key: str) -> object | None:
        """Wake the next queued candidate, but only once nothing is current.

        INFRA-221: the release half of the one-candidate-at-a-time gate.
        :meth:`CodexMerger.next_releasable_candidate` answers ``None``
        while any candidate still holds the reviewer, which is exactly the
        fail-closed rule the issue requires: a submission that failed or
        settled ambiguously leaves its ``wake_deliveries`` row
        ``delivered``/``admitted``, so it stays current and NOTHING is
        released. Only a durably settled verdict — whose terminal wake
        write (``completed``/``deferred``/``rejected``) is made in the
        same settlement path — frees the slot. The release itself goes
        through the ordinary delivery adapter, whose claim
        compare-and-swap makes the wake exactly-once.
        """

        if self._delivery is None:
            return None
        event = self._merger.next_releasable_candidate(project_key)
        if event is None:
            return None
        # INFRA-200: a purely advisory, read-only dry run of admission's
        # LOCAL checks only (``external_gates=False`` -- no remote head,
        # base policy, or intake gate; no credential, no durable write)
        # against the reviewer channel's OWN current generation, which
        # this call can only ever match -- so this can reject only on a
        # genuine manifest/envelope mismatch, never on staleness. Its
        # sole purpose here is the administrative-drift hint
        # (``AdmittedCandidate.drift``) this release's wake carries into
        # Sol's intake message; any rejection is swallowed and the
        # candidate is still released, unhinted, exactly as before this
        # existed -- the hint never gates or delays a release, and the
        # settlement-time admission this release is eventually reviewed
        # under is completely unaffected.
        channel = self._merger.read_channel(project_key)
        if channel is not None and channel.state == "ready":
            try:
                admitted = self._admission.validate_only(
                    project_key,
                    event,
                    received_generation=channel.generation,
                    external_gates=False,
                )
            except CandidateRejected:
                pass
            else:
                event = wake_event_with_drift_hint(event, admitted.drift)
        return await self._delivery.deliver(project_key, event)

    def _wake_binding_matches(
        self,
        project_key: str,
        event_id: str,
        *,
        state: str,
        thread_id: str,
        generation: int,
    ) -> bool:
        """True iff this exact wake's OWN delivery binding is this turn's.

        Read fresh from ``wake_deliveries`` itself, never from anything
        cached: ``thread_id``/``generation`` on that row are written by
        the very delivery/admission path that put the wake into
        ``state`` -- ``record_wake_delivery_success`` for ``'delivered'``,
        carried through unchanged by ``admit_wake`` into ``'admitted'``
        -- so this proves the reviewer binding THIS wake was actually put
        in front of, not merely whatever channel happens to be live now.
        A wake bound to a different thread or an already-superseded
        generation is never matched, so a stale completed-turn
        notification for an old binding can never stall the CURRENT
        wake, and vice versa.
        """

        row = self._database.execute(
            "SELECT 1 FROM wake_deliveries WHERE project_key = ? "
            "AND event_id = ? AND state = ? AND thread_id = ? "
            "AND generation = ?",
            (project_key, event_id, state, thread_id, generation),
        ).fetchone()
        return row is not None

    def _mark_wake_stalled(
        self,
        project_key: str,
        event: WakeEvent,
        channel: ReviewerChannel,
        state: str,
    ) -> TurnOutcome:
        """CAS the exact wake from ``state`` to ``'stalled'``; journal it.

        INFRA-223 recurrence: "completion without a structured verdict
        must release the bounded helper and surface one actionable retry
        state. It must never leave the helper attached to an idle
        task." This is that release: the CAS is conditioned on the
        wake's own identity (``project_key``, ``event_id``), its prior
        ``state`` (``'delivered'`` or ``'admitted'``, whichever
        ``outstanding_wake`` read), and its own recorded
        ``thread_id``/``generation`` -- so only the EXACT wake this
        completed/interrupted turn belongs to is ever touched, never a
        different wake or a wake whose turn is still active. Once
        ``'stalled'``, ``outstanding_wake`` no longer returns this row
        (its scan is limited to ``'admitted'``/``'delivered'``), so
        ``MergerSession.review_active`` sees no outstanding work for it
        and the App Server lease is released the moment nothing else
        keeps the session open. No verdict is ever inferred: nothing
        here reads the thread, and no ``reviews``/``submitted_verdicts``
        row is created. Retryable, and only, via
        :meth:`retry_stalled_wake`.

        Fails closed to the ordinary non-settling ``'awaiting_submission'``
        outcome if the CAS misses -- the row changed underneath this
        call (a racing retry, or a settlement that reached it first) --
        so nothing here ever overwrites what a concurrent winner already
        wrote.
        """

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE wake_deliveries SET state = 'stalled', "
                "updated_at = ? WHERE project_key = ? AND event_id = ? "
                "AND state = ? AND thread_id = ? AND generation = ?",
                (
                    stamp,
                    project_key,
                    event.event_id,
                    state,
                    channel.thread_id,
                    channel.generation,
                ),
            )
            if cursor.rowcount == 1:
                self._events.append(
                    connection,
                    EventInput(
                        event_type="merger.turn_stalled",
                        aggregate_type="wake_delivery",
                        aggregate_id=f"wake:{project_key}:{event.event_id}",
                        correlation_id=event.event_id,
                        actor="merger_turns",
                        payload={
                            "project_key": project_key,
                            "issue_id": event.issue_id,
                            "thread_id": channel.thread_id,
                            "generation": channel.generation,
                            "reason": (
                                "turn completed without a structured verdict"
                            ),
                            "retry_hint": (
                                "MergerTurnService.retry_stalled_wake"
                                f"({project_key!r}, {event.event_id!r}) "
                                "requeues this wake for redelivery"
                            ),
                        },
                    ),
                )
        if cursor.rowcount != 1:
            return TurnOutcome(
                project_key, "awaiting_submission", event.event_id,
                event.issue_id,
                "no submitted verdict for the outstanding wake; "
                "observation never infers one from the thread",
            )
        return TurnOutcome(
            project_key, "stalled", event.event_id, event.issue_id,
            "turn completed without a structured verdict; the bounded "
            "helper is released and the wake is retryable via "
            "retry_stalled_wake",
        )

    def stalled_wakes(self, project_key: str) -> tuple[WakeEvent, ...]:
        """Read-only listing of every wake currently ``'stalled'``.

        For a dashboard/CLI surface (wired separately): exposes the same
        durable wake identity :meth:`outstanding_wake` reads, scoped to
        the ``'stalled'`` state it deliberately never returns. Purely a
        read: never mutates anything, never touches the reviewer thread.
        """

        rows = self._database.execute(
            "SELECT status, issue_id, candidate_sha, base_sha, "
            "manifest_path, event_id, manifest_digest FROM wake_deliveries "
            "WHERE project_key = ? AND state = 'stalled' "
            "ORDER BY created_at ASC, rowid ASC",
            (project_key,),
        ).fetchall()
        return tuple(_row_to_event(row) for row in rows)

    def _requeue_stalled_wake(
        self, project_key: str, event_id: str
    ) -> WakeEvent | None:
        """CAS the exact ``'stalled'`` wake back to ``'pending'``.

        Refuses -- returns ``None``, no side effect at all -- unless a
        row for this exact ``project_key``/``event_id`` is currently
        ``'stalled'``: a non-stalled row (already retried, already
        superseded, still active, or never stalled) is never touched.
        The reset mirrors exactly the stale-claim-to-``'pending'`` shape
        the ordinary delivery path already uses elsewhere
        (``CodexMerger.register_wake``'s ``stale_delivery`` branch):
        ``claim_token``/``claim_expires_at`` cleared, the prior
        ``thread_id``/``generation`` left in place until the next
        successful delivery overwrites them. So the retried row is
        picked up by the SAME queue delivery path
        (``CodexMerger.register_wake`` -> ``release_or_queue`` ->
        ``WakeDeliverer.deliver``) as any other queued candidate --
        no new transport, no bespoke redelivery mechanism. Appends
        ``merger.turn_retry_requested`` exactly once, only when the CAS
        actually applies. Purely the durable half of
        :meth:`retry_stalled_wake` -- callers that need the wake actually
        redelivered use that method, not this one.
        """

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT status, issue_id, candidate_sha, base_sha, "
                "manifest_path, manifest_digest FROM wake_deliveries "
                "WHERE project_key = ? AND event_id = ? AND state = 'stalled'",
                (project_key, event_id),
            ).fetchone()
            if row is None:
                return None
            cursor = connection.execute(
                "UPDATE wake_deliveries SET state = 'pending', "
                "claim_token = NULL, claim_expires_at = NULL, "
                "updated_at = ? WHERE project_key = ? AND event_id = ? "
                "AND state = 'stalled'",
                (stamp, project_key, event_id),
            )
            if cursor.rowcount != 1:
                return None
            self._events.append(
                connection,
                EventInput(
                    event_type="merger.turn_retry_requested",
                    aggregate_type="wake_delivery",
                    aggregate_id=f"wake:{project_key}:{event_id}",
                    correlation_id=event_id,
                    actor="merger_turns",
                    payload={
                        "project_key": project_key,
                        "issue_id": str(row["issue_id"]),
                        "reason": (
                            "operator-requested retry of a wake stalled "
                            "without a structured verdict"
                        ),
                    },
                ),
            )
        return WakeEvent(
            status=str(row["status"]),
            issue_id=str(row["issue_id"]),
            candidate_sha=str(row["candidate_sha"]),
            base_sha=str(row["base_sha"]),
            manifest_path=str(row["manifest_path"]),
            event_id=event_id,
            manifest_digest=str(row["manifest_digest"]),
        )

    async def retry_stalled_wake(
        self, project_key: str, event_id: str
    ) -> RetryResult:
        """The one supported live retry: CAS to ``'pending'`` then redeliver.

        Sol correction 538f1b4e: a stalled turn's only supported retry
        action must re-deliver the EXACT wake through the existing queue
        path, without inference or a new protocol. This is that action,
        and it is the production entry point both
        :meth:`retry_stalled_wakes_for_issue` and the ``retry``
        hermes-command operator handler call.

        First runs :meth:`_requeue_stalled_wake` -- the unchanged,
        already-tested CAS. When it refuses (the row is not currently
        ``'stalled'``), this is a pure no-op: ``retried=False``,
        ``delivered=False``, no second write, no second event -- calling
        retry again for an already-retried or never-stalled wake is
        idempotent by construction, not by a separate check.

        When the CAS applies, the exact same wake is handed to
        ``self._delivery`` -- the identical adapter
        :meth:`release_next_candidate` calls, the one
        :class:`WakeDeliverer` boundary in this service -- so redelivery
        goes through no new transport. That adapter re-registers the
        wake (a no-op against its own unchanged payload) and then either
        claims and renders it into the reviewer thread, or leaves it
        durably queued if another candidate currently holds the reviewer
        (the same one-candidate-at-a-time rule
        :meth:`release_next_candidate` honors) -- never a bespoke
        redelivery mechanism, never a second in-thread message for an
        already-outstanding candidate.
        """

        requeued = self._requeue_stalled_wake(project_key, event_id)
        if requeued is None:
            return RetryResult(
                event_id=event_id,
                issue_id=None,
                retried=False,
                delivered=False,
                reason="not_stalled",
            )
        if self._delivery is None:
            return RetryResult(
                event_id=event_id,
                issue_id=requeued.issue_id,
                retried=True,
                delivered=False,
                reason="no_delivery_adapter",
            )
        result = await self._delivery.deliver(project_key, requeued)
        delivered = bool(getattr(result, "delivered", False))
        reason = getattr(result, "reason", None)
        if not isinstance(reason, str) or not reason:
            reason = "delivered" if delivered else "queued"
        return RetryResult(
            event_id=event_id,
            issue_id=requeued.issue_id,
            retried=True,
            delivered=delivered,
            reason=reason,
        )

    async def retry_stalled_wakes_for_issue(
        self, project_key: str, issue_id: str
    ) -> tuple[RetryResult, ...]:
        """Retry every ``'stalled'`` wake bound to one issue; issue-scoped.

        Reads the durable ``'stalled'`` listing once, filters to
        ``issue_id``, then retries each match, oldest first, through
        :meth:`retry_stalled_wake`. A stalled wake for a DIFFERENT issue
        is never even read as a candidate, let alone touched. Usually
        exactly one row -- an issue rarely has more than one stalled
        wake at a time -- but never more than this issue's own.
        """

        targets = [
            event.event_id
            for event in self.stalled_wakes(project_key)
            if event.issue_id == issue_id
        ]
        results = []
        for target_event_id in targets:
            results.append(
                await self.retry_stalled_wake(project_key, target_event_id)
            )
        return tuple(results)

    async def handle_turn(
        self, project_key: str, *, turn_ended_thread_id: str | None = None
    ) -> TurnOutcome:
        """Reconcile the outstanding wake; settle only a submitted verdict.

        ``turn_ended_thread_id`` is set ONLY by :meth:`on_notification`,
        from a completed/interrupted turn notification's own
        ``threadId`` -- never inferred from anything else, and never set
        by :meth:`recover_outstanding`, :meth:`settle_idle_thread`, or
        the duplicate-submission resume path in :meth:`submit_review`,
        none of which can prove the turn itself has ended. When it is
        set AND no submitted verdict exists for the outstanding wake,
        :meth:`_wake_binding_matches` proves whether that wake's OWN
        recorded delivery binding (its ``wake_deliveries.thread_id``/
        ``generation``, written by the delivery/admission path that put
        it in its current state) is exactly this ended turn's thread and
        the channel's current generation; only then is it durably marked
        ``'stalled'`` (INFRA-223 recurrence) instead of the
        non-settling ``'awaiting_submission'`` observation staying
        outstanding forever.
        """

        project = self._projects.get(project_key)
        if project is None:
            raise ValueError(f"unknown project {project_key!r}")
        channel = self._merger.read_channel(project_key)
        if channel is None or channel.state != "ready":
            return TurnOutcome(
                project_key, "channel_unavailable", None, None,
                "reviewer channel is not ready",
            )
        # Reconcile any prior settlement — including a permitted direct
        # exact-head Sol merge, which the driver detects and folds into
        # the same durable receipts — before admitting another
        # candidate.
        await self._reviews.resume_settlements(project_key)
        outstanding = self.outstanding_wake(project_key)
        if outstanding is None:
            # INFRA-221: nothing holds the reviewer, so a candidate that
            # was queued behind an earlier verdict (or whose release was
            # lost to a crash between settlement and delivery) is woken
            # here, at the same boundary, with no polling turn.
            await self.release_next_candidate(project_key)
            return TurnOutcome(
                project_key, "no_outstanding_wake", None, None,
                "no delivered candidate wake; terminal idle",
            )
        event, state = outstanding
        # Sol correction a9cc6d5f: observation is non-settling. Only an
        # explicit submission durably claimed under the event_id primary
        # key is ever a verdict source; when no such row exists this path
        # neither pulls the thread's report nor consumes the wake, so a
        # racing submit_review can never be preempted by an inferred
        # verdict.
        submitted = self._pending_submission(project_key, event.event_id)
        if submitted is None:
            if turn_ended_thread_id is not None and self._wake_binding_matches(
                project_key,
                event.event_id,
                state=state,
                thread_id=turn_ended_thread_id,
                generation=channel.generation,
            ):
                return self._mark_wake_stalled(project_key, event, channel, state)
            return TurnOutcome(
                project_key, "awaiting_submission", event.event_id,
                event.issue_id,
                "no submitted verdict for the outstanding wake; "
                "observation never infers one from the thread",
            )
        outcome = await self._settle_wake(
            project, project_key, channel, event, state, submitted=submitted
        )
        self._record_settled(event.event_id, outcome)
        await self.release_next_candidate(project_key)
        return outcome

    async def _settle_wake(
        self,
        project: ProjectConfig,
        project_key: str,
        channel: ReviewerChannel,
        event: WakeEvent,
        state: str,
        *,
        submitted: _Submission,
    ) -> TurnOutcome:
        """Admit the wake and settle the submitted document, exactly once.

        The single verdict source is the immutable ``submitted_verdicts``
        row that ``submit_review`` durably claimed; the thread is never
        pulled here. Both the direct submission path and every recovery
        path (notification, startup, duplicate resume) converge on this
        settlement: admission gates, the live pull-request check, parsing
        against the admitted binding, and the idempotent review drive.

        Sol correction 743338e2: a persisted verdict may settle only while
        its exact reviewed thread and generation remain bound to the ready
        reviewer channel. The live channel is re-read here, at the single
        settlement entry every direct and recovered path converges on; a
        replaced channel refuses with the non-settling ``stale_submission``
        outcome, the row stays ``submitted``, and only a fresh submission
        from the new binding (which supersedes the stale identity) can
        settle the event.

        INFRA-212: the credentialed steps -- the admission gates' remote
        head, base policy and intake gate, and pull-request discovery --
        are the ones that authorize crossing an external boundary, so
        they run for an APPROVAL. A ``corrections_required`` verdict
        (and a terminal idle report) settles through the identical
        durable path with none of them, which is what lets a correction
        be submitted from a workspace holding no credentials at all.
        """

        live = self._merger.read_channel(project_key)
        if (
            live is None
            or live.state != "ready"
            or live.thread_id != submitted.reviewed_thread_id
            or live.generation != submitted.reviewed_generation
        ):
            return TurnOutcome(
                project_key, "stale_submission", event.event_id,
                event.issue_id,
                "the persisted submission's reviewed thread and generation "
                "no longer match the ready reviewer channel; the row stays "
                "'submitted' and non-settling until the new binding submits",
            )
        channel = live
        # INFRA-212: the submitted document is parsed against its
        # immutable manifest BEFORE admission, because the verdict is
        # what decides whether this settlement may cross an external
        # boundary at all. Nothing about the parse changed -- the same
        # digest-and-identity-checked snapshot, the same binding, the
        # same failure outcome -- only the order of two pure reads.
        try:
            snapshot = read_manifest_snapshot(
                Path(event.manifest_path),
                root=self._manifest_root,
                expected_digest=event.manifest_digest,
            )
        except ManifestError as error:
            return TurnOutcome(
                project_key, "manifest_invalid", event.event_id,
                event.issue_id, str(error),
            )
        binding = VerdictBinding(
            repository=project.github_repo,
            branch=snapshot.manifest.branch,
            reviewed_sha=snapshot.manifest.candidate_sha,
        )
        try:
            verdict = parse_turn_report(
                _settlement_ready_verdict_json(submitted.verdict_json),
                expected=binding,
            )
        except VerdictError as error:
            return TurnOutcome(
                project_key, "verdict_invalid", event.event_id, event.issue_id,
                str(error),
            )
        # INFRA-212: only an approval crosses an external boundary. A
        # corrections_required verdict (and a terminal idle report) opens
        # no pull request and merges nothing, so it needs neither the
        # credentialed admission gates (see
        # ``CandidateAdmission._run_checks``) nor pull-request discovery,
        # and settles from Sol's own workspace with no credential read.
        needs_external = verdict is not None and verdict.verdict == "approved"
        if state == "delivered":
            try:
                admitted = self._admission.admit(
                    project_key,
                    event,
                    received_generation=channel.generation,
                    external_gates=needs_external,
                )
            except PriorMergeFailed as rejected:
                packet = rejected.packet
                issue_id = (
                    self._reviews.issue_for_candidate(project_key, packet.reviewed_sha)
                    or event.issue_id
                )
                self._lead.deliver(issue_id, (packet,), source="ci_failure")
                self._merger.release_delivered_wake(
                    project_key, event.event_id, outcome="rejected"
                )
                return TurnOutcome(
                    project_key, "blocked_prior_failure", event.event_id,
                    issue_id, str(rejected),
                )
            except MergeWindowExhausted as deferred:
                self._merger.release_delivered_wake(
                    project_key, event.event_id, outcome="deferred"
                )
                return TurnOutcome(
                    project_key, "deferred", event.event_id, event.issue_id,
                    str(deferred),
                )
            except CandidateRejected as rejected:
                # INFRA-217, Sol correction 43152bf8: ``submit_review``'s
                # ``validate_only`` prevalidation already proved this
                # exact wake admissible, with zero durable writes, before
                # the ``submitted_verdicts`` row below was persisted --
                # this settlement-time re-run of the identical checks
                # exists only to catch a race where the admissible
                # condition (most often the remote branch head) changed
                # AFTER that prevalidation and BEFORE this admission.
                # Treating that race as a genuine rejection is wrong: the
                # wake_deliveries row must NOT be released as 'rejected'
                # (terminal for this candidate identity -- a corrected
                # retry would need a brand-new SHA and event under that
                # vocabulary) and the outcome must NOT be terminally
                # recorded onto the submitted row (``_record_settled``
                # would settle it, poisoning a corrected same-wake
                # retry). Both durable rows are left exactly as they
                # are -- wake_deliveries stays 'delivered', the
                # submitted_verdicts row stays 'submitted' -- a
                # zero-additional-write, genuinely retryable state: the
                # identical wake is picked up again (recovery's
                # ``handle_turn``, or ``submit_review``'s identical-
                # duplicate path resuming settlement) and admission is
                # simply re-evaluated once the underlying condition is
                # corrected.
                return TurnOutcome(
                    project_key, "admission_race", event.event_id,
                    event.issue_id,
                    "settlement-time admission rejected a prevalidated "
                    f"candidate; left retryable with no terminal write: "
                    f"{rejected}",
                )
        else:
            admitted = AdmittedCandidate(
                project_key=project_key,
                manifest=snapshot.manifest,
                thread_id=channel.thread_id,
                generation=channel.generation,
            )
        discovered = None
        if needs_external:
            # INFRA-217: PR and merge identity derive from GitHub discovery
            # by exact head — repository, head branch, reviewed head SHA,
            # merged pulls included — never from reviewer input and never
            # from a sole-open-pull-request rule. A GitHubError here
            # propagates exactly as the old open-pulls listing's did: the
            # submitted row stays 'submitted' and recovery resumes it.
            discovered = self._github.discover_pull_request(
                project.github_repo,
                branch=admitted.manifest.branch,
                head_sha=admitted.manifest.candidate_sha,
            )
            # Sol correction 6e1bfe60: the same eligibility judgement used
            # before persistence in submit_review is re-applied here, so
            # an ineligible pull (closed-unmerged, wrong base, foreign
            # head repository) cannot slip through this recovery/resume
            # settlement site either. The rejection-capable gate still
            # runs BEFORE the rework failure close, exactly where the
            # retired sole-open-pull-request check rejected — a rework
            # candidate whose approval cannot bind an eligible pull
            # request must leave its bound failure stored. The no-pull
            # rejection reason is unchanged from before this correction.
            ineligibility = _approval_ineligibility_reason(discovered, project)
            if ineligibility is not None:
                self._merger.complete_admitted_wake(project_key, event.event_id)
                return TurnOutcome(
                    project_key, "rejected", event.event_id, event.issue_id,
                    ineligibility,
                )
        # Every rejection-capable validation — manifest, channel generation,
        # branch head, base, issue, intake gates, admission, and the
        # discovery gate above — has now passed; only here may an
        # explicitly authorized rework close its bound failure. Later
        # steps replay this event-bound close idempotently.
        self._close_bound_failure(project_key, admitted.manifest)
        if verdict is None:
            self._merger.complete_admitted_wake(project_key, event.event_id)
            return TurnOutcome(
                project_key, "idle", event.event_id, event.issue_id,
                "the Merger reported terminal idle for the admitted candidate",
            )
        # An already-merged exact-head pull request keeps its discovered
        # number and converges through the merge driver's existing
        # already-merged adoption; corrections may proceed with no pull
        # request at all (number 0), exactly as before.
        verdict = verdict.with_pr_number(
            discovered.number if discovered is not None else 0
        )
        outcome = await self._reviews.complete_review(
            admitted, event.issue_id, verdict
        )
        self._merger.complete_admitted_wake(project_key, event.event_id)
        return TurnOutcome(
            project_key,
            outcome.state,
            event.event_id,
            event.issue_id,
            outcome.reason,
            review_id=outcome.review_id,
            merge_sha=outcome.merge_sha,
        )

    def _advance_wake_to_reviewer_fix_successor(
        self,
        project_key: str,
        *,
        outstanding: WakeEvent,
        state: str,
        successor_event_id: str,
        issue_id: str,
        final_sha: str,
    ) -> WakeEvent | None:
        """Advance the ONE outstanding wake to its reviewer-fix successor.

        INFRA-194 (2026-09-03): Sol's bounded reviewer fix moves the
        branch head from the submitted candidate to a final SHA and the
        helper records that mapping durably in ``reviewer_fixes`` (state
        ``recorded``, carrying the successor ``fable_rework_ready`` event
        id and the manifest digest it published) -- but the outstanding
        ``wake_deliveries`` row still names the original event, so a
        submission against the successor was refused and one against
        the original could no longer find a pull request at its head.
        This proves the mapping from durable rows only: the recorded fix
        must belong to this project and issue, name exactly the
        outstanding wake's candidate as ``submitted_sha``, exactly the
        submitted event as its successor and exactly the submitted SHA
        as ``final_sha``; the successor manifest must exist under the
        confined root with the recorded digest and agree on event,
        candidate and issue. Then the exact outstanding row -- and only
        it, keyed by its original event, candidate and state -- is
        rewritten in place to the successor identity, keeping its
        delivered/admitted state, thread, generation and claim, and one
        ``wake_delivery.advanced`` event is journaled. Anything else
        returns ``None`` and the caller refuses exactly as before; a
        retry after the advance finds the successor outstanding
        directly, so the operation is idempotent.
        """

        row = self._database.execute(
            "SELECT fix_id, manifest_digest FROM reviewer_fixes "
            "WHERE project_key = ? AND issue_id = ? AND event_id = ? "
            "AND submitted_sha = ? AND final_sha = ? AND state = 'recorded'",
            (
                project_key,
                issue_id,
                successor_event_id,
                outstanding.candidate_sha,
                final_sha,
            ),
        ).fetchone()
        if row is None or outstanding.issue_id != issue_id:
            return None
        path = self._manifest_root / f"{successor_event_id}.json"
        try:
            snapshot = read_manifest_snapshot(
                path,
                root=self._manifest_root,
                expected_digest=str(row["manifest_digest"]),
            )
        except ManifestError as error:
            raise SubmissionRejected(
                f"reviewer-fix successor manifest is invalid: {error}"
            ) from error
        manifest = snapshot.manifest
        if (
            manifest.event_id != successor_event_id
            or manifest.candidate_sha != final_sha
            or manifest.base_sha != outstanding.base_sha
            or issue_id not in manifest.linear_issues
        ):
            return None
        identity = snapshot.identity
        stamp = datetime.now(UTC).isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE wake_deliveries SET event_id = ?, status = ?, "
                "candidate_sha = ?, base_sha = ?, branch = ?, "
                "manifest_path = ?, manifest_digest = ?, manifest_device = ?, "
                "manifest_inode = ?, manifest_size = ?, manifest_mtime_ns = ?, "
                "manifest_mode = ?, updated_at = ? "
                "WHERE project_key = ? AND event_id = ? AND candidate_sha = ? "
                "AND issue_id = ? AND state = ?",
                (
                    successor_event_id,
                    manifest.status,
                    final_sha,
                    manifest.base_sha,
                    manifest.branch,
                    str(path),
                    snapshot.digest,
                    identity.device,
                    identity.inode,
                    identity.size,
                    identity.mtime_ns,
                    identity.mode,
                    stamp,
                    project_key,
                    outstanding.event_id,
                    outstanding.candidate_sha,
                    issue_id,
                    state,
                ),
            )
            if cursor.rowcount != 1:
                return None
            self._events.append(
                connection,
                EventInput(
                    event_type="wake_delivery.advanced",
                    aggregate_type="wake_delivery",
                    aggregate_id=successor_event_id,
                    payload={
                        "project_key": project_key,
                        "issue_id": issue_id,
                        "original_event_id": outstanding.event_id,
                        "original_candidate_sha": outstanding.candidate_sha,
                        "candidate_sha": final_sha,
                        "fix_id": str(row["fix_id"]),
                        "reason": "reviewer-fix successor",
                    },
                ),
            )
        return WakeEvent(
            status=manifest.status,
            issue_id=outstanding.issue_id,
            candidate_sha=final_sha,
            base_sha=manifest.base_sha,
            manifest_path=str(path),
            event_id=successor_event_id,
            manifest_digest=snapshot.digest,
        )

    async def submit_review(
        self,
        project_key: str,
        *,
        issue_id: str,
        event_id: str,
        candidate_sha: str,
        reviewed_thread_id: str,
        reviewed_generation: int,
        verdict_json: str,
    ) -> TurnOutcome:
        """Accept Sol's explicit final verdict and settle it exactly once.

        Operator correction ec1f6bdf: the verdict is submitted, never
        inferred. Every field is validated against the outstanding
        admitted/delivered wake, the immutable manifest, and the ready
        reviewer-channel binding before anything durable happens; the
        verdict is then persisted exactly once (event_id is the
        compare-and-set key) and the shared idempotent settlement path
        runs immediately. An identical duplicate returns the recorded
        result; a conflicting or stale submission raises
        :class:`SubmissionRejected` with no side effects.

        Sol correction 743338e2: the ready reviewer-channel binding is
        validated before duplicate resolution, so a duplicate is
        idempotent only while its thread and generation are still the
        current binding. A ``submitted`` row left stale by a channel
        replacement never resolves as a duplicate: this fresh submission
        from the new binding supersedes it with an UPDATE-if-stale CAS
        that re-binds the event's single row (the event_id primary key)
        to the new identity inside one transaction, and persistence
        itself is conditioned on the channel still holding that exact
        binding.

        INFRA-212: every validation named above -- project, reviewer
        channel, thread, generation, outstanding wake, event, issue,
        candidate SHA, immutable manifest, and the verdict document
        itself -- is proven from durable local state, so a
        ``corrections_required`` submission validates, persists and
        settles with no Linear, GitHub, CircleCI, App Server, Keychain,
        network or cmux access at all. Only an approval, which must
        cross the merge boundary, reaches the credentialed steps, and
        those still fail closed exactly as before when a credential
        cannot be read.
        """

        project = self._projects.get(project_key)
        if project is None:
            raise SubmissionRejected(f"unknown project {project_key!r}")
        channel = self._merger.read_channel(project_key)
        if channel is None or channel.state != "ready":
            raise SubmissionRejected(
                "the project's reviewer channel is not ready"
            )
        if (
            channel.thread_id != reviewed_thread_id
            or channel.generation != reviewed_generation
        ):
            raise SubmissionRejected(
                "submission does not match the ready reviewer-channel "
                "binding (thread or generation)"
            )
        existing = self._read_submission(event_id)
        stale_existing = existing is not None and (
            existing.state == "submitted"
            and (
                existing.reviewed_thread_id != channel.thread_id
                or existing.reviewed_generation != channel.generation
            )
        )
        if existing is not None and not stale_existing:
            # INFRA-200: ``existing.verdict_json`` is already durably
            # stored in its normalized form (see ``_persist_submission``/
            # ``_supersede_submission`` below), so this RAW, not-yet-
            # parsed resubmission is normalized the same way, purely for
            # this identity comparison, before any wake or manifest is
            # even read.
            return await self._resolve_duplicate(
                existing,
                project_key=project_key,
                issue_id=issue_id,
                event_id=event_id,
                candidate_sha=candidate_sha,
                reviewed_thread_id=reviewed_thread_id,
                reviewed_generation=reviewed_generation,
                verdict_json=_normalize_verdict_json_best_effort(verdict_json),
            )
        await self._reviews.resume_settlements(project_key)
        outstanding = self.outstanding_wake(project_key)
        if outstanding is None:
            raise SubmissionRejected(
                "no outstanding admitted or delivered wake for the project"
            )
        event, state = outstanding
        if event.event_id != event_id:
            # INFRA-194 (2026-09-03): a reviewer-fix successor is proven
            # lazily at this boundary -- the recorded ``reviewer_fixes``
            # row must map the outstanding wake's exact candidate to the
            # submitted event/final SHA -- and only then is that one wake
            # advanced to the successor identity so the ordinary
            # validation below continues against the final head.
            advanced = self._advance_wake_to_reviewer_fix_successor(
                project_key,
                outstanding=event,
                state=state,
                successor_event_id=event_id,
                issue_id=issue_id,
                final_sha=candidate_sha,
            )
            if advanced is None:
                raise SubmissionRejected(
                    "submission event does not match the outstanding wake"
                )
            event = advanced
        if event.issue_id != issue_id or event.candidate_sha != candidate_sha:
            raise SubmissionRejected(
                "submission does not match the outstanding wake's issue "
                "or candidate SHA"
            )
        try:
            snapshot = read_manifest_snapshot(
                Path(event.manifest_path),
                root=self._manifest_root,
                expected_digest=event.manifest_digest,
            )
        except ManifestError as error:
            raise SubmissionRejected(
                f"immutable manifest is invalid: {error}"
            ) from error
        manifest = snapshot.manifest
        if (
            manifest.event_id != event_id
            or manifest.candidate_sha != candidate_sha
            or issue_id not in manifest.linear_issues
        ):
            raise SubmissionRejected(
                "submission does not match the immutable manifest"
            )
        binding = VerdictBinding(
            repository=project.github_repo,
            branch=manifest.branch,
            reviewed_sha=candidate_sha,
        )
        try:
            document = parse_verdict(verdict_json, expected=binding)
        except VerdictError as error:
            raise SubmissionRejected(
                f"verdict document is invalid: {error}"
            ) from error
        # INFRA-200: persist the NORMALIZED envelope -- durable value plus
        # the ``label``/``reviewer_fix`` markers ``parse_verdict`` merged
        # in -- rather than the raw pre-parse string, so a stored
        # ``submitted_verdicts.verdict_json`` row retains the original
        # reviewer-facing label. ``document.verdict_json`` is only empty
        # for a ``ReviewVerdict`` built outside ``parse_verdict`` (never
        # the case here, since ``document`` came from ``parse_verdict``
        # above), so the raw fallback is defensive, not a normal path.
        persisted_verdict_json = document.verdict_json or verdict_json
        # INFRA-212: everything above is proven from durable local state
        # alone -- the outstanding wake, the immutable manifest, the
        # ready reviewer-channel binding, and the submitted document.
        # Everything below is needed only by a verdict that will cross an
        # external boundary, so a corrections_required submission reaches
        # persistence and settlement without a single credential read.
        needs_external = document.verdict == "approved"
        if needs_external:
            # INFRA-217: PR identity is derived from GitHub — by
            # repository, head branch, and reviewed head SHA, merged
            # pulls included — never read from the document. The
            # discovery gate runs BEFORE any persistence, so an approval
            # with no pull request at the exact reviewed head (and any
            # discovery failure) rejects with zero durable writes and a
            # corrected retry starts clean.
            try:
                discovered = self._github.discover_pull_request(
                    project.github_repo,
                    branch=manifest.branch,
                    head_sha=candidate_sha,
                )
            except GitHubError as error:
                raise SubmissionRejected(
                    f"pull-request discovery failed: {error}"
                ) from error
            # Sol correction 6e1bfe60: discovery alone is not eligibility.
            # This full identity-and-state judgement runs before ANY
            # persistence (submitted_verdict, review, settlement,
            # correction, or wake-terminal write) below, so a closed-
            # unmerged, wrong-base, or foreign-head-repository pull
            # rejects with zero durable writes and a corrected retry for
            # the same wake starts clean.
            ineligibility = _approval_ineligibility_reason(discovered, project)
            if ineligibility is not None:
                raise SubmissionRejected(ineligibility)

        # INFRA-217, Sol correction c02dc0fe: the complete read-only
        # candidate-admission checks -- exact remote-head (or
        # proven-deleted-branch) validation, base policy, and the intake
        # gate -- previously ran for the FIRST time inside
        # ``_settle_wake``'s call to ``CandidateAdmission.admit``, AFTER
        # this method had already persisted the submitted_verdicts row
        # below. A stale or mismatched remote branch could therefore
        # create a durable submission before the exact-head gate rejected
        # it, and ``_record_settled`` would then terminally record the
        # rejection, poisoning a corrected retry. ``validate_only`` runs
        # the identical checks here -- read-only, zero durable writes --
        # BEFORE ``_persist_submission``/``_supersede_submission``, so a
        # rejection here leaves every durable table and wake state
        # untouched and a corrected retry starts clean. This only applies
        # while the wake is still ``delivered``: an already-``admitted``
        # wake ran these same checks durably when it was admitted, and
        # ``_settle_wake`` does not re-run them for that state either, so
        # prevalidation stays consistent with the settlement-time gate it
        # mirrors. The settlement-time call to ``CandidateAdmission.admit``
        # inside ``_settle_wake`` is UNCHANGED and still re-runs the same
        # checks immediately before the actual admission, to catch races
        # between this prevalidation and settlement.
        #
        # INFRA-212: ``external_gates`` mirrors exactly what
        # ``_settle_wake`` will run for this same verdict, so
        # prevalidation stays a faithful dry run of the settlement gate
        # rather than a stricter or looser one.
        if state == "delivered":
            try:
                self._admission.validate_only(
                    project_key,
                    event,
                    received_generation=channel.generation,
                    external_gates=needs_external,
                )
            except (MergeWindowExhausted, PriorMergeFailed):
                # NOT prevalidation refusals. A full merge window defers
                # the wake back to pending, and a bound prior failure
                # routes a correction packet to the lead -- both are
                # recoverable outcomes the settlement path already
                # produces ('deferred' / 'blocked_prior_failure'), not
                # zero-write rejections of the submission. Converting
                # them here would turn a retryable deferral into a hard
                # refusal, so they deliberately fall through to
                # persistence and let ``_settle_wake`` handle them
                # exactly as before this correction. Both subclass
                # CandidateRejected, so this handler MUST precede it.
                pass
            except CandidateRejected as rejected:
                raise SubmissionRejected(str(rejected)) from rejected
        if stale_existing:
            assert existing is not None
            submission = self._supersede_submission(
                existing,
                reviewed_thread_id=reviewed_thread_id,
                reviewed_generation=reviewed_generation,
                verdict_json=persisted_verdict_json,
            )
        else:
            submission = self._persist_submission(
                event_id=event_id,
                project_key=project_key,
                issue_id=issue_id,
                candidate_sha=candidate_sha,
                reviewed_thread_id=reviewed_thread_id,
                reviewed_generation=reviewed_generation,
                verdict_json=persisted_verdict_json,
            )
        if submission is None:
            # Lost the exactly-once CAS to a concurrent submission, or the
            # reviewer channel was replaced between validation and the
            # binding-conditioned persist.
            raced = self._read_submission(event_id)
            if raced is None:
                raise SubmissionRejected(
                    "the reviewer channel was replaced while persisting the "
                    "submission; submit again from the new binding"
                )
            # INFRA-200: ``persisted_verdict_json`` is already the exact
            # normalized form a winning racer's row would hold, so this
            # comparison needs no further normalization (unlike the
            # early, pre-parse duplicate check above).
            return await self._resolve_duplicate(
                raced,
                project_key=project_key,
                issue_id=issue_id,
                event_id=event_id,
                candidate_sha=candidate_sha,
                reviewed_thread_id=reviewed_thread_id,
                reviewed_generation=reviewed_generation,
                verdict_json=persisted_verdict_json,
            )
        outcome = await self._settle_wake(
            project, project_key, channel, event, state, submitted=submission
        )
        self._record_settled(event_id, outcome)
        # INFRA-221: the next queued candidate is released ONLY after this
        # verdict's settlement durably succeeded — outputting the document
        # is not completion, and a non-settling outcome keeps this
        # candidate current so nothing is released here.
        await self.release_next_candidate(project_key)
        return outcome

    async def _resolve_duplicate(
        self,
        existing: _Submission,
        *,
        project_key: str,
        issue_id: str,
        event_id: str,
        candidate_sha: str,
        reviewed_thread_id: str,
        reviewed_generation: int,
        verdict_json: str,
    ) -> TurnOutcome:
        """Idempotent identical duplicate; conflicting one fails closed."""

        identical = (
            existing.project_key == project_key
            and existing.issue_id == issue_id
            and existing.candidate_sha == candidate_sha
            and existing.reviewed_thread_id == reviewed_thread_id
            and existing.reviewed_generation == reviewed_generation
            and existing.verdict_json == verdict_json
        )
        if not identical:
            raise SubmissionRejected(
                "a conflicting verdict submission already exists for "
                "this wake event"
            )
        if existing.state == "settled" and existing.result_json is not None:
            return _outcome_from_json(existing.result_json)
        # Durably submitted, settlement incomplete: resume its settlement.
        outstanding = self.outstanding_wake(project_key)
        if outstanding is not None and outstanding[0].event_id == event_id:
            return await self.handle_turn(project_key)
        await self._reviews.resume_settlements(project_key)
        outcome = TurnOutcome(
            project_key, "no_outstanding_wake", event_id, issue_id,
            "the submitted verdict's wake is no longer outstanding; "
            "prior settlement was reconciled",
        )
        self._record_settled(event_id, outcome)
        return outcome

    def _read_submission(self, event_id: str) -> _Submission | None:
        row = self._database.execute(
            "SELECT event_id, project_key, issue_id, candidate_sha, "
            "reviewed_thread_id, reviewed_generation, verdict_json, state, "
            "result_json FROM submitted_verdicts WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        return None if row is None else _row_to_submission(row)

    def _pending_submission(
        self, project_key: str, event_id: str
    ) -> _Submission | None:
        row = self._database.execute(
            "SELECT event_id, project_key, issue_id, candidate_sha, "
            "reviewed_thread_id, reviewed_generation, verdict_json, state, "
            "result_json FROM submitted_verdicts "
            "WHERE event_id = ? AND project_key = ? AND state = 'submitted'",
            (event_id, project_key),
        ).fetchone()
        return None if row is None else _row_to_submission(row)

    def _persist_submission(
        self,
        *,
        event_id: str,
        project_key: str,
        issue_id: str,
        candidate_sha: str,
        reviewed_thread_id: str,
        reviewed_generation: int,
        verdict_json: str,
    ) -> _Submission | None:
        now = self._now().isoformat()
        try:
            with self._database.transaction() as connection:
                cursor = connection.execute(
                    "INSERT INTO submitted_verdicts(event_id, project_key, "
                    "issue_id, candidate_sha, reviewed_thread_id, "
                    "reviewed_generation, verdict_json, state, created_at, "
                    "updated_at) "
                    "SELECT ?, ?, ?, ?, ?, ?, ?, 'submitted', ?, ? "
                    "WHERE EXISTS (SELECT 1 FROM reviewer_channels "
                    "WHERE project_key = ? AND thread_id = ? "
                    "AND generation = ? AND state = 'ready')",
                    (
                        event_id,
                        project_key,
                        issue_id,
                        candidate_sha,
                        reviewed_thread_id,
                        reviewed_generation,
                        verdict_json,
                        now,
                        now,
                        project_key,
                        reviewed_thread_id,
                        reviewed_generation,
                    ),
                )
        except sqlite3.IntegrityError:
            return None
        if cursor.rowcount != 1:
            # Sol correction 743338e2: persistence is bound to the channel —
            # a replacement that lands before this transaction leaves
            # nothing persisted, so no stale identity can ever settle.
            return None
        return _Submission(
            event_id=event_id,
            project_key=project_key,
            issue_id=issue_id,
            candidate_sha=candidate_sha,
            reviewed_thread_id=reviewed_thread_id,
            reviewed_generation=reviewed_generation,
            verdict_json=verdict_json,
            state="submitted",
            result_json=None,
        )

    def _supersede_submission(
        self,
        existing: _Submission,
        *,
        reviewed_thread_id: str,
        reviewed_generation: int,
        verdict_json: str,
    ) -> _Submission | None:
        """Re-bind the event's stale ``submitted`` row to the new identity.

        Sol correction 743338e2: the event_id primary key allows exactly
        one row per event, so a fresh submission from the replacement
        binding supersedes the stale one with an UPDATE-if-stale CAS —
        conditioned on the exact stale identity still being present, the
        row still being ``submitted``, and the reviewer channel still
        holding the new binding — all inside one transaction. Exactly-once
        holds: the row never duplicates, and only the identity that
        matches the ready channel at settlement can settle.
        """

        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE submitted_verdicts SET reviewed_thread_id = ?, "
                "reviewed_generation = ?, verdict_json = ?, updated_at = ? "
                "WHERE event_id = ? AND state = 'submitted' "
                "AND reviewed_thread_id = ? AND reviewed_generation = ? "
                "AND EXISTS (SELECT 1 FROM reviewer_channels "
                "WHERE project_key = ? AND thread_id = ? "
                "AND generation = ? AND state = 'ready')",
                (
                    reviewed_thread_id,
                    reviewed_generation,
                    verdict_json,
                    self._now().isoformat(),
                    existing.event_id,
                    existing.reviewed_thread_id,
                    existing.reviewed_generation,
                    existing.project_key,
                    reviewed_thread_id,
                    reviewed_generation,
                ),
            )
        if cursor.rowcount != 1:
            return None
        return _Submission(
            event_id=existing.event_id,
            project_key=existing.project_key,
            issue_id=existing.issue_id,
            candidate_sha=existing.candidate_sha,
            reviewed_thread_id=reviewed_thread_id,
            reviewed_generation=reviewed_generation,
            verdict_json=verdict_json,
            state="submitted",
            result_json=None,
        )

    def _record_settled(self, event_id: str, outcome: TurnOutcome) -> bool:
        """Terminally record one outcome onto its ``submitted`` row.

        Returns ``True`` iff this call is the one that settled the row --
        the UPDATE is a compare-and-set on ``state = 'submitted'``, so a
        row already settled (by an earlier call, or concurrently) answers
        ``False`` and nothing is written twice. INFRA-223 uses that
        return to journal its reconciliation exactly once; the two
        non-settling refusals below still write nothing at all.
        """

        if outcome.kind == "stale_submission":
            # The refusal is non-settling: the row stays 'submitted' so
            # only a fresh submission from the new binding can settle it.
            return False
        if outcome.kind == "admission_race":
            # INFRA-217, Sol correction 43152bf8: a settlement-time
            # admission rejection of a prevalidated candidate is a race,
            # not a terminal verdict -- recording it here would settle
            # (terminally) a row that must stay retryable for the same
            # wake. Leave the submitted_verdicts row exactly as
            # ``_settle_wake`` left it ('submitted', no result_json).
            return False
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE submitted_verdicts SET state = 'settled', "
                "result_json = ?, updated_at = ? "
                "WHERE event_id = ? AND state = 'submitted'",
                (
                    _outcome_to_json(outcome),
                    self._now().isoformat(),
                    event_id,
                ),
            )
        return cursor.rowcount == 1

    def _close_bound_failure(
        self, project_key: str, manifest: CandidateManifest
    ) -> None:
        if manifest.status != "FABLE_REWORK_READY":
            return
        packet = self._window.stored_failure(project_key)
        if packet is None:
            return
        correction_id = self._lead.authorized_rework(project_key, manifest, packet)
        if correction_id is None:
            return
        self._window.close_failure_for_rework(
            project_key,
            failed_candidate_sha=packet.reviewed_sha,
            rework=manifest,
            correction_id=correction_id,
        )


def _row_to_submission(row: sqlite3.Row) -> _Submission:
    result_json = row["result_json"]
    return _Submission(
        event_id=str(row["event_id"]),
        project_key=str(row["project_key"]),
        issue_id=str(row["issue_id"]),
        candidate_sha=str(row["candidate_sha"]),
        reviewed_thread_id=str(row["reviewed_thread_id"]),
        reviewed_generation=int(row["reviewed_generation"]),
        verdict_json=str(row["verdict_json"]),
        state=str(row["state"]),
        result_json=None if result_json is None else str(result_json),
    )


def _row_to_event(row: sqlite3.Row) -> WakeEvent:
    return WakeEvent(
        status=str(row["status"]),
        issue_id=str(row["issue_id"]),
        candidate_sha=str(row["candidate_sha"]),
        base_sha=str(row["base_sha"]),
        manifest_path=str(row["manifest_path"]),
        event_id=str(row["event_id"]),
        manifest_digest=str(row["manifest_digest"]),
    )
