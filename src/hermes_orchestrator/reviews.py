"""Independent Codex merge workflow: verdict, merge, CI window, Linear.

INFRA-166 composes the pieces delivered by INFRA-162 through INFRA-165 into
one durable, restart-safe flow. A recorded verdict either returns structured
correction packets to the Claude lead or becomes an approved review; an
approved review is merged only through :class:`IntegrationMerge`, journaled
in the CircleCI merge window without any CI query, and only then projected
to Done or QA by the :class:`QaRouter` — unless a pending acceptance gate
(INFRA-198 J1) holds the issue in ``post_merge_acceptance`` with the next
acceptance action dispatched durably. Any failure before ancestry proof
leaves Linear in Review. A QA rejection supersedes the merged review,
projects In Development to the operator, and returns a Critical packet to
the lead. CircleCI is never polled here.
"""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from hermes_orchestrator.acceptance import (
    ACCEPTANCE_PENDING,
    ACCEPTANCE_SATISFIED,
    AcceptanceGate,
    AcceptanceGates,
)
from hermes_orchestrator.ci_window import CiWindow
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import IssueState
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.git import GitError
from hermes_orchestrator.github import MergeBlocked, MergeEffectJournal
from hermes_orchestrator.lead_assignments import LeadAssignments
from hermes_orchestrator.linear import (
    _ALLOWED_STATUS_TRANSITIONS as _LINEAR_STATUS_TRANSITIONS,
)
from hermes_orchestrator.linear import (
    ExternalEffectStore,
    LinearProjection,
    projection_request,
)
from hermes_orchestrator.manifests import CandidateManifest
from hermes_orchestrator.merge import (
    IntegrationMerge,
    MergeClient,
    ReconciliationRequired,
)
from hermes_orchestrator.qa import QaRouter
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.review_intake import AdmittedCandidate
from hermes_orchestrator.settlement import (
    EXTERNALLY_MERGED,
    MergeSettlements,
    Settlement,
    SettlementBinding,
    SettlementConflict,
)
from hermes_orchestrator.verdicts import CorrectionPacket, ReviewVerdict

# Non-settling refusal: the settlement's stored reviewer thread and
# generation no longer match the live ready reviewer channel. The row
# stays resumable (``recorded``, or released back to it) but can never
# settle until a fresh generation-bound explicit submission re-approves
# the candidate, which re-binds the settlement to the live identity.
STALE_SETTLEMENT = "stale_settlement"

# Review states that still own the issue's review slot.
_LIVE_STATES = (
    "approved",
    "corrections_required",
    "merged",
    "blocked",
    "reconciliation_required",
)

# INFRA-218 Sol correction d65491fe (defect 2): ``LinearClient.project``
# raises exactly this message (see linear.py's transition-legality check)
# when the live current status and the requested target status are not a
# legal single-hop pair. Parsed here so a replay can recover the actual
# live current status without a second read of its own -- the message is
# the only channel the ``LinearProjector`` protocol exposes for it.
_TRANSITION_NOT_ALLOWED = re.compile(
    r"^Linear status transition (?P<current>.+) -> (?P<target>.+) is not allowed$"
)


def _legal_status_path(current: str, target: str) -> tuple[str, ...] | None:
    """The shortest legal hop sequence from ``current`` to ``target``.

    Walks ``linear._ALLOWED_STATUS_TRANSITIONS`` breadth-first so a stored
    target unreachable in a single hop (e.g. "In Development" -> "Done")
    is instead reached through its legal intermediate states ("In
    Development" -> "Review" -> "Done"). Returns the hop statuses AFTER
    ``current``, ending with ``target`` -- empty when they already match.
    ``None`` when no legal walk exists at all.
    """

    if current == target:
        return ()
    frontier: list[tuple[str, tuple[str, ...]]] = [(current, ())]
    seen = {current}
    while frontier:
        next_frontier: list[tuple[str, tuple[str, ...]]] = []
        for status, path in frontier:
            for from_status, to_status in _LINEAR_STATUS_TRANSITIONS:
                if from_status != status or to_status in seen:
                    continue
                hop_path = (*path, to_status)
                if to_status == target:
                    return hop_path
                seen.add(to_status)
                next_frontier.append((to_status, hop_path))
        frontier = next_frontier
    return None


class LinearProjector(Protocol):
    """The idempotent Linear projection boundary."""

    async def project(
        self, issue_id: str, target: LinearProjection, effect_id: str
    ) -> object: ...


class LeadCorrectionPort(Protocol):
    """Where structured correction packets return to the Claude lead."""

    def deliver(
        self,
        issue_id: str,
        packets: tuple[CorrectionPacket, ...],
        *,
        source: str = ...,
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class ReviewRecord:
    """One durable review of an admitted candidate."""

    review_id: str
    project_key: str
    issue_id: str
    event_id: str
    repository: str
    branch: str
    pr_number: int
    reviewed_sha: str
    state: str
    merge_sha: str | None
    reason: str | None
    projection: LinearProjection | None

    def as_verdict(self) -> ReviewVerdict:
        return ReviewVerdict(
            verdict="approved",
            repository=self.repository,
            branch=self.branch,
            pr_number=self.pr_number,
            reviewed_sha=self.reviewed_sha,
            packets=(),
        )


@dataclass(frozen=True, slots=True)
class AcceptanceRepair:
    """One acceptance-gate repair applied by the reconciliation pass."""

    issue_id: str
    gate_state: str
    action: str


@dataclass(frozen=True, slots=True)
class MergeOutcome:
    """The result of driving one review as far as it can safely go."""

    review_id: str
    issue_id: str
    state: str
    merge_sha: str | None
    projection: LinearProjection | None
    reason: str


class ReviewService:
    """Drive one admitted candidate from verdict to its exact Linear state."""

    def __init__(
        self,
        *,
        database: Database,
        events: EventStore,
        projects: Mapping[str, ProjectConfig],
        queue: QueueService,
        github: MergeClient,
        merge: IntegrationMerge,
        window: CiWindow,
        linear: LinearProjector,
        qa: QaRouter,
        lead: LeadCorrectionPort,
        settlements: MergeSettlements,
        merge_journal: MergeEffectJournal | None = None,
        acceptance: AcceptanceGates | None = None,
        assignments: LeadAssignments | None = None,
        now: Callable[[], datetime] | None = None,
        on_merged: Callable[..., None] | None = None,
    ) -> None:
        for name, port in (
            ("github", github),
            ("merge", merge),
            ("window", window),
            ("linear", linear),
            ("qa", qa),
            ("lead", lead),
            ("settlements", settlements),
        ):
            if port is None:
                raise ValueError(f"review service requires {name}")
        self._database = database
        self._events = events
        self._projects = dict(projects)
        self._queue = queue
        self._github = github
        self._merge = merge
        self._window = window
        self._linear = linear
        self._qa = qa
        self._lead = lead
        self._settlements = settlements
        self._merge_journal = merge_journal
        # INFRA-218 Sol correction deb5ec49: the same target-only durable
        # journal activation already relies on (``ExternalEffectStore``,
        # adopted via ``begin``/``begin_in`` in ``cells.py``), built over
        # this same ``database`` so it reads and writes the identical
        # ``external_effects`` rows a real ``LinearClient`` would use.
        # ``_project_after_merge`` journals the exact ordinary post-merge
        # Linear target here BEFORE its first live read, and
        # ``reconcile_post_merge_projections`` replays whatever is left
        # pending from the same recovery boundary as
        # ``reconcile_acceptance``.
        self._effects = ExternalEffectStore(database)
        # INFRA-198 J1: optional post-merge acceptance policy. When wired
        # and a pending gate exists, a proven merge holds the issue in
        # ``post_merge_acceptance`` instead of completing it; ``None``
        # keeps the pre-gate behavior byte-for-byte.
        self._acceptance = acceptance
        self._assignments = assignments
        self._now = now or (lambda: datetime.now(UTC))
        # INFRA-198 P2: an optional accelerator over durable rows, wired
        # by the caller (never by this constructor's own defaults) as a
        # public attribute so it can be attached after construction —
        # ``ReviewService`` is built inside ``merge_flow.build_merge_flow``,
        # outside the post-merge-advance composition boundary. When unset,
        # the daemon's post-merge tick discovers every terminal merge from
        # durable rows instead; either path is idempotent and safe to run
        # more than once for the same merge.
        self.on_merged = on_merged

    # -- verdicts ---------------------------------------------------------

    async def record_verdict(
        self,
        admitted: AdmittedCandidate,
        issue_id: str,
        verdict: ReviewVerdict,
    ) -> ReviewRecord:
        """Durably bind one verdict to its admitted candidate and route it."""

        project = self._projects.get(admitted.project_key)
        if project is None:
            raise ValueError(f"unknown project {admitted.project_key!r}")
        manifest = admitted.manifest
        if issue_id not in manifest.linear_issues:
            raise ValueError("issue is not part of the admitted candidate")
        try:
            issue = self._queue.get(issue_id)
        except KeyError as error:
            raise ValueError(f"issue {issue_id} was never queued") from error
        if issue.project_key != admitted.project_key:
            raise ValueError("issue belongs to a different project")
        if (
            verdict.repository != project.github_repo
            or verdict.branch != manifest.branch
            or verdict.reviewed_sha != manifest.candidate_sha
        ):
            raise ValueError("verdict is not bound to the admitted candidate")
        if verdict.verdict not in ("approved", "corrections_required"):
            raise ValueError(f"unknown verdict {verdict.verdict!r}")

        review_id = f"review:{admitted.project_key}:{manifest.event_id}"
        state = "corrections_required" if verdict.packets else "approved"
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM reviews WHERE review_id = ?", (review_id,)
            ).fetchone()
            if existing is not None:
                record = _row_to_record(existing)
                if (
                    record.issue_id != issue_id
                    or record.reviewed_sha != verdict.reviewed_sha
                    or record.pr_number != verdict.pr_number
                    or record.branch != verdict.branch
                ):
                    raise ValueError(
                        "review id is already bound to a different candidate"
                    )
            else:
                stuck = connection.execute(
                    "SELECT review_id FROM reviews WHERE issue_id = ? "
                    "AND state = 'reconciliation_required'",
                    (issue_id,),
                ).fetchone()
                if stuck is not None:
                    raise ValueError(
                        f"issue {issue_id} has a review awaiting operator "
                        "reconciliation"
                    )
                connection.execute(
                    "UPDATE reviews SET state = 'superseded', updated_at = ? "
                    "WHERE issue_id = ? AND state IN "
                    "('approved', 'corrections_required', 'blocked')",
                    (stamp, issue_id),
                )
                connection.execute(
                    "INSERT INTO reviews("
                    "review_id, project_key, issue_id, event_id, repository, "
                    "branch, pr_number, reviewed_sha, state, merge_sha, reason, "
                    "projection_json, created_at, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)",
                    (
                        review_id,
                        admitted.project_key,
                        issue_id,
                        manifest.event_id,
                        verdict.repository,
                        verdict.branch,
                        verdict.pr_number,
                        verdict.reviewed_sha,
                        state,
                        stamp,
                        stamp,
                    ),
                )
                self._events.append(
                    connection,
                    EventInput(
                        event_type="review.recorded",
                        aggregate_type="review",
                        aggregate_id=review_id,
                        correlation_id=manifest.event_id,
                        actor="codex_merger",
                        payload={
                            "issue_id": issue_id,
                            "reviewed_sha": verdict.reviewed_sha,
                            "state": state,
                            "packet_count": len(verdict.packets),
                        },
                    ),
                )
            if state == "approved":
                # The completion operation is atomic: the approved
                # review and its settlement — binding project, issue,
                # repository, branch, PR, base, candidate SHA, wake
                # event, reviewer thread + generation, and manifest —
                # commit together, before anything can cross the
                # GitHub mutation boundary.
                self._settlements.record_in(
                    connection,
                    SettlementBinding(
                        settlement_id=review_id,
                        project_key=admitted.project_key,
                        issue_id=issue_id,
                        event_id=manifest.event_id,
                        repository=verdict.repository,
                        branch=verdict.branch,
                        pr_number=verdict.pr_number,
                        base_sha=manifest.base_sha,
                        candidate_sha=verdict.reviewed_sha,
                        thread_id=admitted.thread_id,
                        thread_generation=admitted.generation,
                        manifest_version=manifest.manifest_version,
                    ),
                )
                # Sol 165f5ee6 packet 3: the settlement insert is
                # replay-ignored, so a fresh generation-bound approval
                # of the same wake event must supersede a stale stored
                # reviewer identity. The UPDATE-if-stale CAS (the
                # submitted_verdicts supersession idiom) re-binds the
                # event's single settlement row to the approving
                # thread/generation — only while it is unclaimed or its
                # claim lease has expired, in the same transaction as
                # the verdict — so exactly one settlement per event
                # survives and only the live binding can settle it.
                connection.execute(
                    "UPDATE merge_settlements SET thread_id = ?, "
                    "thread_generation = ?, updated_at = ? "
                    "WHERE settlement_id = ? "
                    "AND (state = 'recorded' OR (state = 'merging' "
                    "AND lease_expires_at IS NOT NULL "
                    "AND lease_expires_at < ?)) "
                    "AND (thread_id != ? OR thread_generation != ?)",
                    (
                        admitted.thread_id,
                        admitted.generation,
                        stamp,
                        review_id,
                        stamp,
                        admitted.thread_id,
                        admitted.generation,
                    ),
                )
        record = self._get(review_id)
        if record.state == "corrections_required":
            self._dispatch_packets(
                record, verdict.packets, "review.corrections", source="codex_review"
            )
            await self._return_to_lead(
                record, effect_id=f"linear:{issue_id}:corrections:{review_id}"
            )
        elif record.state == "approved":
            self._queue.transition(
                issue_id,
                IssueState.REVIEW,
                actor="codex_merger",
                reason=f"approved {verdict.reviewed_sha}",
            )
            await self._linear.project(
                issue_id,
                LinearProjection(status="Review", assignee_alias="operator"),
                effect_id=f"linear:{issue_id}:review:{review_id}",
            )
        return record

    async def complete_review(
        self,
        admitted: AdmittedCandidate,
        issue_id: str,
        verdict: ReviewVerdict,
    ) -> MergeOutcome:
        """Record the verdict, then merge immediately when it approved."""

        record = await self.record_verdict(admitted, issue_id, verdict)
        if record.state == "corrections_required":
            return _outcome(record, reason="correction packets returned to lead")
        return await self.merge_approved(record.review_id)

    # -- merge ------------------------------------------------------------

    async def merge_approved(self, review_id: str) -> MergeOutcome:
        """Settle one approved review under its exclusive merge claim.

        The GitHub mutation boundary is crossed only while holding the
        settlement's owner-token lease — the exclusive journaled merge
        claim recorded with the verdict. A settlement that is already
        terminal replays its outcome; a claim held elsewhere defers
        without any external effect; and any interrupted attempt keeps
        its ``merging`` lease so exactly one resumer re-drives it after
        expiry. Every stage of the drive itself is idempotent under
        deterministic effect ids, so replays are exactly-once
        effective.
        """

        settlement = self._settlements.find(review_id)
        if settlement is None:
            raise SettlementConflict(
                "no recorded settlement authorizes this merge; the "
                "verdict was never durably bound"
            )
        record = self._get(review_id)
        if settlement.state == "settled":
            return _outcome(record, reason=record.reason or "merged")
        if settlement.state == "failed":
            return _outcome(record, reason=settlement.reason or record.state)
        if record.state == "approved" and settlement.state == "recorded":
            # Settlement-claim fence (Sol 165f5ee6 packet 3, scope
            # refined by Sol eadf249b packet 2): every recovery entry —
            # startup resume, intake-boundary resume, the merge-settle
            # CLI, and the direct submission drive — converges here, and
            # a PRE-MUTATION ``recorded`` settlement whose stored
            # reviewer thread/generation is no longer the live ready
            # channel is never claimed: nothing external can have
            # happened, so the stale refusal is immediate. An in-flight
            # ``merging`` row is NOT fenced at this boundary — its
            # previous owner may have crossed the GitHub boundary before
            # crashing, so once its lease expires it is claimed and
            # driven: the drive inspects authoritative PR/effect state,
            # reconciles an already-completed exact-head merge into its
            # receipts, and still applies the final pre-mutation
            # revalidation before any NEW merge call. A ``merged``
            # settlement's receipt tail replays for a mutation that
            # already exists.
            stale = self._stale_settlement_reason(settlement)
            if stale is not None:
                return _outcome(record, state=STALE_SETTLEMENT, reason=stale)
        token = self._settlements.claim(review_id)
        if token is None:
            return _outcome(
                record,
                state="deferred",
                reason="the exclusive merge claim is held elsewhere",
            )
        # An unexpected exception leaves the settlement 'merging' under
        # its lease: nothing is released (the mutation state is
        # unknown) and exactly one resumer re-drives after expiry.
        #
        # INFRA-218 Sol correction 44eb2806 (a): captured BEFORE the
        # drive, so it reflects whether this call is the fresh merge
        # (the review entered ``approved``) or a resumed convergence of
        # an already-``merged`` review — the exact ``reconciling``
        # distinction ``_project_after_merge`` requires, now threaded
        # from here instead of from inside ``_drive_merge``.
        was_already_merged = record.state == "merged"
        outcome = await self._drive_merge(review_id)
        if outcome.state == "merged":
            self._settlements.mark_merged(
                review_id,
                token=token,
                merge_sha=str(outcome.merge_sha),
                path=(
                    EXTERNALLY_MERGED
                    if outcome.reason.startswith("externally merged")
                    else None
                ),
            )
            self._settlements.mark_settled(review_id, token=token)
            # INFRA-218 Sol correction 44eb2806 (a): the durable
            # settlement is now TERMINAL — ``merged`` and ``settled``,
            # carrying the real ``merge_sha`` — before the post-merge
            # Linear projection is attempted at all. No projection call
            # is ever awaited while the ``merging`` lease is open, so a
            # cancellation, a killed process, or an indefinitely hung
            # Linear call can no longer strand a proven merge behind a
            # non-terminal settlement: the worst it can do now is leave
            # the ALREADY-terminal settlement's projection pending,
            # which ``reconcile_post_merge_projections`` (riding the
            # same ``resume_settlements`` boundary) replays from the
            # intent ``_project_after_merge`` journals before its first
            # live Linear read (Sol correction deb5ec49).
            await self._project_fail_soft(
                self._get(review_id), reconciling=was_already_merged
            )
        elif outcome.state in ("deferred", STALE_SETTLEMENT):
            # Nothing external happened under the claim: a stale
            # pre-mutation revalidation releases back to ``recorded`` so
            # the settlement stays resumable but non-settling.
            self._settlements.release(review_id, token=token)
        else:
            self._settlements.mark_failed(review_id, token=token, reason=outcome.reason)
        return outcome

    async def resume_settlements(
        self, project_key: str | None = None
    ) -> tuple[MergeOutcome, ...]:
        """Re-drive every resumable settlement, oldest first.

        Called at startup and at intake boundaries: an approved review
        separated from its merge by a crash, a full window, or a lost
        completion event can never strand — the durable settlement row
        is re-derived and driven to a terminal state.
        """

        outcomes: list[MergeOutcome] = []
        for settlement in self._settlements.resumable(project_key):
            outcomes.append(await self.merge_approved(settlement.settlement_id))
        # INFRA-198 packet K: the acceptance reconciliation pass rides
        # every resume boundary — daemon startup (MergerSession.startup),
        # the intake-boundary resumes in merger_turns, and the
        # `merge-settle --project` CLI all converge on this method, so
        # composing the pass here reaches every recovery entry without
        # touching those callers.
        await self.reconcile_acceptance(project_key)
        # INFRA-218 Sol correction deb5ec49: rides the identical recovery
        # boundary as ``reconcile_acceptance`` above to replay any
        # ordinary (non-acceptance-gated) post-merge Linear projection
        # left durably pending by a first-read outage.
        await self.reconcile_post_merge_projections(project_key)
        return tuple(outcomes)

    async def reconcile_acceptance(
        self, project_key: str | None = None
    ) -> tuple[AcceptanceRepair, ...]:
        """Repair every acceptance gate whose downstream state drifted.

        Sol 524a38ed finding 2: ``acceptance_gates`` was durable but
        never read by any startup or maintenance path, so a crash (or a
        lifecycle-blind advance) anywhere between the gate row and the
        queue/assignment/Linear state it governs stranded the issue
        forever. This pass re-derives the exact downstream state each
        gate demands, idempotently and exactly-once:

        - A PENDING gate whose issue has a proven merged review (a
          ``reviews`` row in state ``merged`` with a non-null
          ``merge_sha``) but sits in a premature ``done`` — or holds
          correctly in ``post_merge_acceptance`` with its assignment or
          Linear hold effect missing — replays the acceptance hold:
          queue back to ``post_merge_acceptance``, the idempotent
          ``acceptance-hold`` Linear effect, and exactly one durable
          acceptance assignment once a live project cell exists (the
          same ``_dispatch_acceptance_assignment`` identity binding the
          settlement path uses, so replays are durable no-ops).
        - Sol 04d013b0 finding 2 (option a — reconcile QA into the
          hold): a PENDING gate on a proven merged review whose issue
          was routed to ``qa`` BEFORE the gate existed (a late gate; a
          gate pending at merge time always wins over QA routing in
          ``_project_after_merge``) transitions into the same hold.
          Option (a) is chosen over refusing late gate creation because
          the ``("QA", "In Development")`` Linear pair is already
          allowed (``linear._ALLOWED_STATUS_TRANSITIONS`` — the
          ``qa_reject`` return path projects exactly it), a merged
          QA-routed issue is a legitimate target for a late acceptance
          requirement, and a creation-time refusal could not bind the
          lifecycle anyway: the issue's state keeps moving after the
          gate row persists, so drift must be repaired at this
          reconciliation boundary regardless.
        - Sol 04d013b0 finding 1: the reconciliation replay of the hold
          is gate-scoped-deduplicated — an existing non-superseded
          assignment for the issue and instruction (``published`` OR
          ``acknowledged``) means the action is already dispatched, so
          recovery never supersedes an acknowledged packet into a
          duplicate. Only the genuine re-queue path (the merge-time
          dispatch) keeps ``publish_in``'s consumed-epoch supersession.
        - A SATISFIED gate whose queue or Linear completion never landed
          (a crash after ``AcceptanceGates.satisfy`` persisted but
          before the queue transition / Linear projection) completes the
          issue: queue to ``done`` and the ``acceptance-satisfied``
          Linear effect under the satisfy path's exact effect-id
          convention, so a replay of either entry projects nothing
          twice.
        - A gate on an issue with NO proven merged review never advances
          anything — acceptance can only ever complete merged work.

        Linear availability: every boundary that reaches this pass
        composes a real Linear projector (``build_merge_flow`` requires
        ``linear``, and the ``ReviewService`` constructor fails closed
        without one), so composition is never impossible here; a
        projector that FAILS at runtime (network, credentials) is
        contained per gate — the failure is reported to stderr, the
        durable gate row is left untouched, and the next recovery
        boundary retries the same repair.
        """

        if self._acceptance is None:
            return ()
        repairs: list[AcceptanceRepair] = []
        for gate in self._acceptance.all_gates():
            record = self._merged_review(gate.issue_id)
            if record is None:
                # Unmerged (or QA-rejected/superseded) work: the gate
                # holds no authority to advance anything.
                continue
            if project_key is not None and record.project_key != project_key:
                continue
            try:
                issue_state = self._queue.get(gate.issue_id).state
            except KeyError:
                continue
            try:
                if gate.state == ACCEPTANCE_PENDING and issue_state in (
                    IssueState.DONE,
                    IssueState.POST_MERGE_ACCEPTANCE,
                    IssueState.QA,
                ):
                    await self._hold_for_acceptance(record, reconciling=True)
                    repairs.append(
                        AcceptanceRepair(gate.issue_id, gate.state, "held")
                    )
                elif gate.state == ACCEPTANCE_SATISFIED:
                    self._queue.transition(
                        gate.issue_id,
                        IssueState.DONE,
                        actor="operator_acceptance",
                        reason=(
                            "acceptance satisfied for instruction "
                            f"{gate.instruction_id}"
                        ),
                    )
                    await self._linear.project(
                        gate.issue_id,
                        LinearProjection(
                            status="Done", assignee_alias="operator"
                        ),
                        effect_id=(
                            f"linear:{gate.issue_id}:acceptance-satisfied:"
                            f"{gate.instruction_id}"
                        ),
                    )
                    repairs.append(
                        AcceptanceRepair(gate.issue_id, gate.state, "completed")
                    )
            except Exception as error:  # fail soft: durable rows retry later
                print(
                    "acceptance reconciliation for "
                    f"{gate.issue_id!r} failed and will retry at the next "
                    f"recovery boundary: {type(error).__name__}: {error}",
                    file=sys.stderr,
                )
        return tuple(repairs)


    def _reconstruct_missing_after_merge_intents(
        self, project_key: str | None
    ) -> None:
        """Re-journal an after-merge intent lost to a post-settle crash.

        INFRA-218 Sol correction d65491fe. Selects every settled
        settlement whose review is proven merged and which has NO
        after-merge effect row (neither pending nor completed), and
        re-creates the exact intent the fresh path would have written.
        Deterministic by construction: the effect id and the target both
        come from the merged review row, so this is idempotent against
        any real intent and against itself. A review with no stored
        projection is skipped — there is nothing to reconstruct and
        inventing a target would be a guess.
        """

        rows = self._database.execute(
            "SELECT r.issue_id AS issue_id, r.review_id AS review_id "
            "FROM reviews r JOIN merge_settlements s "
            "  ON s.settlement_id = r.review_id "
            "WHERE r.state = 'merged' AND r.merge_sha IS NOT NULL "
            "  AND s.state = 'settled' "
            "  AND NOT EXISTS ("
            "    SELECT 1 FROM external_effects e "
            "    WHERE e.effect_id = 'linear:' || r.issue_id "
            "      || ':after-merge:' || r.review_id"
            "  )"
        ).fetchall()
        for row in rows:
            record = self._merged_review(str(row["issue_id"]))
            if record is None or record.projection is None:
                continue
            if project_key is not None and record.project_key != project_key:
                continue
            if (
                self._acceptance is not None
                and self._acceptance.get(record.issue_id) is not None
            ):
                # Acceptance-GATED issues are not ours to reconstruct.
                # Their post-merge projection is owned end to end by the
                # gate path (hold, then its own satisfaction convergence)
                # under DIFFERENT effect identities, so a missing
                # after-merge row there means the ordinary branch never
                # ran -- not that an intent was lost. Reconstructing one
                # would re-apply a Done the gate path already converged,
                # which is exactly the duplicate this guard prevents.
                continue
            effect_id = (
                f"linear:{record.issue_id}:after-merge:{record.review_id}"
            )
            try:
                self._effects.begin(
                    effect_id,
                    target=record.issue_id,
                    request=projection_request(
                        record.issue_id, record.projection
                    ),
                )
            except Exception as error:  # pragma: no cover - infra guard
                print(
                    "post-merge intent reconstruction failed for "
                    f"{effect_id!r}: {type(error).__name__}: {error}",
                    file=sys.stderr,
                )

    async def reconcile_post_merge_projections(
        self, project_key: str | None = None
    ) -> tuple[str, ...]:
        """Replay every ordinary post-merge Linear projection left pending.

        INFRA-218 Sol correction deb5ec49: S1(a)/S3 made every post-merge
        projection fail soft (``_project_fail_soft``) so a durably-proven
        merge or settlement always reaches its terminal state even when
        Linear raises. But Sol's finding is that a successfully settled
        ORDINARY (non-acceptance-gated) merge is never returned by
        ``resume_settlements`` again once ``settled``, and
        ``reconcile_acceptance`` only repairs acceptance-GATED issues —
        so if the very FIRST Linear read for that projection failed
        (``LinearClient.project`` only journals its own pending row
        AFTER that read succeeds), nothing durable survived to retry and
        Linear could stay permanently stale despite a fully converged
        Git/GitHub settlement.

        The fix lives in two halves. ``_project_after_merge``'s ordinary
        branch now journals the exact target — issue, status, assignee
        alias, and this effect's id — through ``self._effects`` (the
        same target-only ``ExternalEffectStore`` primitive activation
        already uses via ``ExternalEffectStore.begin_in`` in
        ``cells._activate_issue_transaction``) BEFORE calling
        ``self._linear.project``, so a pending row always survives even
        a first-read outage. This method is the other half: it rides the
        same ``resume_settlements`` recovery boundary as
        ``reconcile_acceptance`` and re-drives every such pending row
        against the journaled target — no new GitHub merge, no new
        protocol, just an idempotent replay of the same
        ``LinearClient.project`` call. A row already completed (by the
        live projector or by :meth:`_complete_post_merge_effect`) is
        never selected again, so a repeated recovery pass is a stable
        no-op, and each failure here is reported and left for the next
        boundary exactly like ``reconcile_acceptance`` already does per
        gate.

        INFRA-218 Sol correction 44eb2806 (b): a stored pending target
        can predate a NEW acceptance gate created after the ordinary
        (non-gated) projection was journaled — the gate pending at
        merge time always wins in ``_project_after_merge``, but a LATE
        gate arriving after journaling has no way to intercept a
        replay of the already-stored target, which would otherwise
        project the stale Done/QA target and override the required
        acceptance hold. Before replaying any stored target, this loop
        re-evaluates current acceptance policy with the exact same
        read ``_project_after_merge`` uses for a fresh merge
        (``self._acceptance.pending(issue_id)``) — the same predicate
        ``reconcile_acceptance`` polices from the gate's side. When a
        gate now requires a hold, the stale stored target is suppressed
        entirely (never projected) and replaced with the required
        ``_hold_for_acceptance`` projection (post_merge_acceptance /
        In Development) instead, then the original pending effect is
        marked completed so it is never reconsidered — the hold is the
        durable terminal projection for this merge now, exactly as if
        the gate had existed at merge time.
        """

        # INFRA-218 Sol correction d65491fe (defect 1): the previous
        # correction moved the projection AFTER the terminal
        # ``mark_settled`` write so nothing is ever awaited under the
        # merge lease — but that opened a narrow window of its own. A
        # crash between that terminal write and
        # ``_project_after_merge``'s ``self._effects.begin`` leaves a
        # SETTLED settlement whose review is merged and NO after-merge
        # effect row at all, so the pending scan below finds nothing and
        # the projection is lost permanently. Reverting to projecting
        # under the lease is not the fix (it restores the stranding
        # defect), so the missing intent is instead reconstructed here,
        # deterministically, from the merged review itself: the same
        # ``linear:<issue>:after-merge:<review>`` effect id and the same
        # stored ``record.projection`` target the fresh path would have
        # journaled. Because the id is identical, a reconstruction can
        # never double-apply against a real one, and a later pass finds
        # the row already present.
        self._reconstruct_missing_after_merge_intents(project_key)
        rows = self._database.execute(
            "SELECT effect_id, request_json FROM external_effects "
            "WHERE adapter = 'linear' AND operation = 'project' "
            "AND state = 'pending' AND effect_id LIKE 'linear:%:after-merge:%'"
        ).fetchall()
        replayed: list[str] = []
        for row in rows:
            effect_id = str(row["effect_id"])
            try:
                request = json.loads(row["request_json"])
                issue_id = str(request["issue_id"])
                target = LinearProjection.model_validate(request["target"])
            except (KeyError, TypeError, ValueError) as error:
                print(
                    "post-merge projection recovery could not parse "
                    f"{effect_id!r}: {type(error).__name__}: {error}",
                    file=sys.stderr,
                )
                continue
            record = self._merged_review(issue_id)
            if record is None:
                continue
            if project_key is not None and record.project_key != project_key:
                continue
            if self._acceptance is not None and self._acceptance.pending(issue_id):
                # INFRA-218 Sol correction 44eb2806 (b): a gate now
                # requires a hold — suppress the stale stored Done/QA
                # target and replace it with the required hold instead
                # of replaying stale completion over it.
                try:
                    await self._hold_for_acceptance(record, reconciling=True)
                    self._complete_post_merge_effect(effect_id)
                    replayed.append(effect_id)
                except Exception as error:  # fail soft: retries later
                    print(
                        "post-merge acceptance-hold recovery for "
                        f"{effect_id!r} failed and will retry at the "
                        f"next recovery boundary: {type(error).__name__}: "
                        f"{error}",
                        file=sys.stderr,
                    )
                continue
            try:
                await self._linear.project(issue_id, target, effect_id=effect_id)
                self._complete_post_merge_effect(effect_id)
                replayed.append(effect_id)
            except Exception as error:  # fail soft: the durable row retries later
                print(
                    "post-merge projection recovery for "
                    f"{effect_id!r} failed and will retry at the next "
                    f"recovery boundary: {type(error).__name__}: {error}",
                    file=sys.stderr,
                )
        return tuple(replayed)

    def _complete_post_merge_effect(self, effect_id: str) -> None:
        """Complete a post-merge Linear effect a live client left pending.

        A real ``LinearClient.project`` already completes its own
        journal row internally before returning successfully, so this
        is a no-op against production. It exists only for a
        :class:`LinearProjector` that never touches the journal (a bare
        test double), so a successful call still leaves the durable row
        completed and ``reconcile_post_merge_projections`` never
        re-selects it.
        """

        effect = self._effects.get(effect_id)
        if effect is not None and effect.state != "completed":
            self._effects.complete(effect_id, {"completed": True})

    def _merged_review(self, issue_id: str) -> ReviewRecord | None:
        """The newest proven merged review for ``issue_id``, if any."""

        row = self._database.execute(
            "SELECT * FROM reviews WHERE issue_id = ? AND state = 'merged' "
            "AND merge_sha IS NOT NULL "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (issue_id,),
        ).fetchone()
        return None if row is None else _row_to_record(row)

    async def _drive_merge(self, review_id: str) -> MergeOutcome:
        """The idempotent merge body; call only under a settlement claim.

        Re-reads the pull request, refuses to overfill the CI window,
        merges through the proven state machine, journals the unresolved
        merge without any CI query, then projects Done or QA. Every path
        before ancestry proof leaves Linear in Review. Safe to replay.
        """

        record = self._get(review_id)
        if record.state == "merged":
            # Replay, not a fresh merge (Sol Critical a626cf1f): the
            # review already recorded "merged" on a prior pass — this
            # call is a resumed settlement (crashed between
            # ``mark_merged`` and ``mark_settled``) or a settled replay
            # — never the merge that just happened. The acceptance hold
            # this re-derives must dedup against a live (published or
            # acknowledged) assignment instead of opening a fresh
            # dispatch epoch that supersedes one the lead already
            # acknowledged.
            #
            # INFRA-218 (a): the merge itself was already proven on the
            # pass that first recorded ``merged`` — this branch's own
            # job is convergence of the settlement row, not projection.
            # A downstream projection failure (Linear, the acceptance
            # hold, or an assignment dispatch) must never re-strand an
            # already-proven merge under a fresh lease forever: that is
            # exactly the live INFRA-216 blocker, where a settlement
            # already ``merging`` with an ``externally_merged`` path
            # re-raised on every resume and never reached ``settled``.
            # INFRA-218 Sol correction 44eb2806 (a): no projection call
            # here either — this branch performs no mutation at all,
            # and ``merge_approved`` now performs the projection, as
            # separately recoverable work, only after ITS OWN
            # ``mark_merged``/``mark_settled`` tail (which runs right
            # after this method returns) has converged the settlement
            # to ``settled``. Leave the durable projection inputs (the
            # acceptance gate row, the queue state, the stored review
            # projection) exactly as they are — no new merge is ever
            # performed on this path.
            return _outcome(record, reason=record.reason or "merged")
        if record.state != "approved":
            return _outcome(record, reason=record.reason or record.state)
        project = self._projects[record.project_key]
        settlement = self._settlements.get(review_id)

        pull = self._github.get_pull_request(record.repository, record.pr_number)
        effect = (
            self._merge_journal.get(f"merge:{review_id}")
            if self._merge_journal is not None
            else None
        )
        effect_binding: tuple[str, str, str] | None = None
        if (
            effect is not None
            and effect.state == "completed"
            and effect.response is not None
            and effect.request.get("repository") == record.repository
            and effect.request.get("number") == record.pr_number
            and effect.request.get("base") == project.integration_branch
            and effect.request.get("merge_method") == "squash"
            and pull.head_repository == record.repository
            and pull.base_repository == record.repository
            and pull.head_sha == effect.request.get("sha")
            and pull.head_ref == effect.request.get("head_ref")
            and pull.merge_commit_sha == effect.response.get("sha")
        ):
            effect_binding = (
                str(effect.request["sha"]),
                str(effect.request["head_ref"]),
                str(effect.response["sha"]),
            )
        exact_review_binding = (
            pull.head_sha == record.reviewed_sha and pull.head_ref == record.branch
        )
        if (
            pull.merged
            and (exact_review_binding or effect_binding is not None)
            and pull.repository == record.repository
            and pull.head_repository == record.repository
            and pull.base_repository == record.repository
            and pull.base_ref == project.integration_branch
            and pull.merge_commit_sha is not None
        ):
            # A permitted direct exact-head merge (Sol merged the exact
            # approved candidate itself): reconcile it into the same
            # durable receipts instead of going stale — proofs first,
            # then the external merge-effect receipt, then the ledger,
            # review, and Linear tail, all exactly once.
            candidate_sha, candidate_branch, merge_sha = effect_binding or (
                record.reviewed_sha,
                record.branch,
                pull.merge_commit_sha,
            )
            try:
                proven = self._merge.prove_landed(
                    record.project_key,
                    candidate_sha=candidate_sha,
                    candidate_branch=candidate_branch,
                    pr_number=record.pr_number,
                    merge_sha=merge_sha,
                    base_sha=settlement.base_sha,
                )
            except ReconciliationRequired as error:
                record = self._transition(
                    record, "reconciliation_required", reason=str(error)
                )
                return _outcome(record, reason=str(error))
            if self._merge_journal is not None and effect is None:
                self._merge_journal.record_external(
                    f"merge:{record.review_id}",
                    request={
                        "repository": record.repository,
                        "number": record.pr_number,
                        "sha": candidate_sha,
                        "head_ref": candidate_branch,
                        "base": project.integration_branch,
                        "merge_method": "squash",
                    },
                    response={
                        "merged": True,
                        "sha": proven.merge_sha,
                        "external": True,
                    },
                )
            return await self._settle_proven(
                record,
                proven,
                reason=f"externally merged; reconciled {proven.relation}",
            )
        if (
            pull.state != "open"
            or pull.merged
            or pull.head_sha != record.reviewed_sha
            or pull.head_ref != record.branch
            or pull.base_ref != project.integration_branch
        ):
            record = self._transition(
                record,
                "stale",
                reason="pull request no longer matches the approved candidate",
            )
            return _outcome(record, reason=record.reason or "stale")
        if not self._window.has_capacity(record.project_key):
            return _outcome(
                record,
                state="deferred",
                reason="unresolved merge window is full",
            )

        # Final pre-mutation revalidation (Sol 165f5ee6 packet 3): the
        # channel can be replaced between the claim fence and this exact
        # point, so the live ready reviewer channel is re-read one last
        # time immediately before the GitHub merge call. A stale binding
        # returns without any mutation and the caller releases the claim
        # back to ``recorded``.
        settlement = self._settlements.get(review_id)
        stale = self._stale_settlement_reason(settlement)
        if stale is not None:
            return _outcome(record, state=STALE_SETTLEMENT, reason=stale)
        try:
            proven = self._merge.merge_approved(
                record.project_key,
                record.as_verdict(),
                effect_id=f"merge:{review_id}",
                base_sha=settlement.base_sha,
            )
        except MergeBlocked as error:
            record = self._transition(record, "blocked", reason=str(error))
            return _outcome(record, reason=str(error))
        except ReconciliationRequired as error:
            record = self._transition(
                record, "reconciliation_required", reason=str(error)
            )
            return _outcome(record, reason=str(error))

        return await self._settle_proven(
            record, proven, reason=f"proven {proven.relation}"
        )

    async def _settle_proven(
        self, record: ReviewRecord, proven: Any, *, reason: str
    ) -> MergeOutcome:
        """Journal and transition one proven merge to ``merged``; idempotent.

        INFRA-218 Sol correction 44eb2806 (a): this method used to also
        AWAIT the post-merge Linear projection here, before either of
        its callers (``_drive_merge`` and ``reconcile_external_merge``)
        had run the settlement's own ``mark_merged``/``mark_settled``
        tail -- so the durable settlement row sat in its ``merging``
        lease for the full duration of a Linear call. ``_project_fail_
        soft`` only catches ``Exception``, so ``asyncio.CancelledError``
        (a ``BaseException``), a killed process, or an indefinitely
        hung Linear call left a genuinely proven merge (``reviews.state
        == 'merged'``, real ``merge_sha``) stuck behind a settlement
        that never reached ``settled``. A targeted cancellation probe
        reproduced exactly this split.

        The fix is ordering, not exception handling: this method now
        performs ONLY the durable, already-local writes -- the CI
        window record, the review's ``merged`` transition with its
        proven ``merge_sha``, the fast-path callback, and the
        ``merge.proven`` event -- and returns. It never calls
        ``self._linear`` (directly or via ``_project_fail_soft``) at
        all. Both callers now run the settlement's ``mark_merged``/
        ``mark_settled`` tail against this already-terminal review
        BEFORE performing the post-merge projection as separate,
        independently recoverable work -- so no projection is ever
        awaited while the merge lease is open, and the projection
        intent journaled by ``_project_after_merge`` (Sol correction
        deb5ec49) together with ``reconcile_post_merge_projections``
        still replays it if this process never gets that far.
        """

        self._window.record_merge(proven)
        projection = self._qa.after_merge(record.issue_id)
        record = self._transition(
            record,
            "merged",
            reason=reason,
            merge_sha=proven.merge_sha,
            projection=projection,
        )
        if self.on_merged is not None:
            # Fast path only: the daemon's 30s post-merge tick discovers
            # and idempotently repairs this exact merge from durable rows
            # regardless, so a failure here must never interrupt the
            # settlement it rides on.
            try:
                self.on_merged(
                    project_key=record.project_key,
                    issue_id=record.issue_id,
                    review_id=record.review_id,
                    merge_sha=record.merge_sha,
                )
            except Exception as error:
                print(
                    "post-merge advance fast path failed for "
                    f"{record.review_id!r}: {type(error).__name__}: {error}",
                    file=sys.stderr,
                )
        self._record_merge_proven(record, proven, reason=reason)
        # INFRA-218 Sol correction 44eb2806 (a): no projection call here
        # any more — see the docstring above. The caller's settlement
        # tail (``mark_merged``/``mark_settled``) runs against this
        # already-``merged`` review immediately after this returns, and
        # ONLY THEN does the caller perform the post-merge projection as
        # separately recoverable work.
        return _outcome(record, reason=record.reason or "merged")

    def _record_merge_proven(
        self, record: ReviewRecord, proven: Any, *, reason: str
    ) -> None:
        """Append the one durable ``merge.proven`` event for this review.

        ``_transition`` owns its own transaction for the ``merged`` state
        change, so this appends in a separate small transaction right
        after it, skipping the append when an identical event already
        exists so a replay is a no-op. ``getattr`` with ``None`` defaults
        keeps this safe against older :class:`ProvenMerge` shapes.
        """

        with self._database.transaction() as connection:
            existing = connection.execute(
                "SELECT 1 FROM events WHERE event_type = 'merge.proven' "
                "AND aggregate_id = ? LIMIT 1",
                (record.review_id,),
            ).fetchone()
            if existing is not None:
                return
            self._events.append(
                connection,
                EventInput(
                    event_type="merge.proven",
                    aggregate_type="review",
                    aggregate_id=record.review_id,
                    correlation_id=record.event_id,
                    payload={
                        "repository": getattr(proven, "repository", None),
                        "pr_number": getattr(proven, "pr_number", None),
                        "submitted_sha": record.reviewed_sha,
                        "final_integration_sha": getattr(
                            proven, "candidate_sha", None
                        ),
                        "candidate_sha": getattr(proven, "candidate_sha", None),
                        "candidate_branch": getattr(proven, "candidate_branch", None),
                        "base_sha": getattr(proven, "base_sha", None),
                        "merge_sha": getattr(proven, "merge_sha", None),
                        "merge_parent_sha": getattr(
                            proven, "merge_parent_sha", None
                        ),
                        "integration_branch": getattr(
                            proven, "integration_branch", None
                        ),
                        "relation": getattr(proven, "relation", None),
                        "applied_tree_sha": getattr(
                            proven, "applied_tree_sha", None
                        ),
                        "merge_tree_sha": getattr(
                            proven, "merge_tree_sha", None
                        ),
                        "changed_paths": list(
                            getattr(proven, "changed_paths", ()) or ()
                        ),
                        "reason": reason,
                    },
                ),
            )

    # -- external reconciliation ------------------------------------------

    async def reconcile_external_merge(
        self,
        *,
        project_key: str,
        issue_id: str,
        manifest: CandidateManifest,
        pr_number: int,
        submitted_sha: str | None = None,
        final_integration_sha: str | None = None,
        merge_sha: str | None = None,
    ) -> MergeOutcome:
        """Reconstruct receipts for a PR merged before verdict settlement.

        INFRA-194 recovery path: the mutation already exists on GitHub,
        so this path performs NO merge — ever. It first validates the
        repository, pull request, exact reviewed head, base, merge
        commit reachability, and candidate-tree equivalence; only a
        full proof reconstructs the auditable review, settlement
        (``path='externally_merged'``), merge-effect, CI-ledger, and
        Linear receipts, each idempotent under its deterministic id.
        Any validation failure raises with zero receipts written.
        """

        if self._merge_journal is None:
            raise ValueError(
                "external reconciliation requires the merge-effect journal"
            )
        project = self._projects.get(project_key)
        if project is None:
            raise ValueError(f"unknown project {project_key!r}")
        if issue_id not in manifest.linear_issues:
            raise ValueError("issue is not part of the emitted candidate")
        review_id = f"review:{project_key}:{manifest.event_id}"

        identity_chain = (submitted_sha, final_integration_sha, merge_sha)
        has_identity_chain = any(value is not None for value in identity_chain)
        if has_identity_chain and not all(
            value is not None for value in identity_chain
        ):
            raise ValueError(
                "submitted, final integration, and merge shas must be provided together"
            )
        if has_identity_chain and submitted_sha != manifest.candidate_sha:
            raise ValueError("submitted sha is not the immutable reviewed candidate")

        pull = self._github.get_pull_request(project.github_repo, pr_number)
        if not pull.merged:
            raise ValueError("the pull request is not merged; nothing to reconcile")
        if (
            pull.repository != project.github_repo
            or pull.head_repository != project.github_repo
            or pull.base_repository != project.github_repo
        ):
            raise ValueError("the pull request repository does not match the project")
        if has_identity_chain:
            if pull.head_sha != final_integration_sha:
                raise ValueError(
                    "the pull request head is not the final integration sha"
                )
            if pull.merge_commit_sha != merge_sha:
                raise ValueError("the pull request merge sha does not match")
            proven_candidate = str(final_integration_sha)
            proven_branch = pull.head_ref
            proven_merge = str(merge_sha)
        elif (
            pull.head_ref != manifest.branch
            or pull.head_sha != manifest.candidate_sha
        ):
            raise ValueError(
                "the merged pull request head is not the reviewed candidate"
            )
        else:
            proven_candidate = manifest.candidate_sha
            proven_branch = manifest.branch
            proven_merge = str(pull.merge_commit_sha)
        if pull.base_ref != project.integration_branch:
            raise ValueError(
                "the merged pull request base is not the integration branch"
            )
        if pull.merge_commit_sha is None:
            raise ReconciliationRequired(
                "the merged pull request reports no merge commit"
            )
        if has_identity_chain:
            self._merge.prove_landed(
                project_key,
                candidate_sha=manifest.candidate_sha,
                candidate_branch=manifest.branch,
                pr_number=pr_number,
                merge_sha=proven_merge,
                base_sha=manifest.base_sha,
                exact_pr_binding=False,
            )
        proven = self._merge.prove_landed(
            project_key,
            candidate_sha=proven_candidate,
            candidate_branch=proven_branch,
            pr_number=pr_number,
            merge_sha=proven_merge,
            base_sha=manifest.base_sha,
        )
        request = {
            "repository": project.github_repo,
            "number": pr_number,
            "sha": proven_candidate,
            "head_ref": proven_branch,
            "base": project.integration_branch,
            "merge_method": "squash",
        }
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM reviews WHERE review_id = ?", (review_id,)
            ).fetchone()
            if existing is not None:
                record = _row_to_record(existing)
                if (
                    record.issue_id != issue_id
                    or record.reviewed_sha != manifest.candidate_sha
                    or record.pr_number != pr_number
                ):
                    raise ValueError(
                        "review id is already bound to a different candidate"
                    )
                if record.state not in (
                    "approved",
                    "merged",
                    "reconciliation_required",
                ):
                    raise ValueError(
                        f"a review in state {record.state!r} cannot be "
                        "reconciled as merged"
                    )
            else:
                connection.execute(
                    "INSERT INTO reviews("
                    "review_id, project_key, issue_id, event_id, repository, "
                    "branch, pr_number, reviewed_sha, state, merge_sha, "
                    "reason, projection_json, created_at, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'approved', NULL, "
                    "NULL, NULL, ?, ?)",
                    (
                        review_id,
                        project_key,
                        issue_id,
                        manifest.event_id,
                        project.github_repo,
                        manifest.branch,
                        pr_number,
                        manifest.candidate_sha,
                        stamp,
                        stamp,
                    ),
                )
                self._events.append(
                    connection,
                    EventInput(
                        event_type="review.recorded",
                        aggregate_type="review",
                        aggregate_id=review_id,
                        correlation_id=manifest.event_id,
                        actor="external_reconciliation",
                        payload={
                            "issue_id": issue_id,
                            "reviewed_sha": manifest.candidate_sha,
                            "state": "approved",
                            "external": True,
                        },
                    ),
                )
            self._settlements.record_in(
                connection,
                SettlementBinding(
                    settlement_id=review_id,
                    project_key=project_key,
                    issue_id=issue_id,
                    event_id=manifest.event_id,
                    repository=project.github_repo,
                    branch=manifest.branch,
                    pr_number=pr_number,
                    base_sha=manifest.base_sha,
                    candidate_sha=manifest.candidate_sha,
                    thread_id="external_reconciliation",
                    thread_generation=0,
                    manifest_version=manifest.manifest_version,
                ),
                path=EXTERNALLY_MERGED,
            )
        settlement = self._settlements.get(review_id)
        record = self._get(review_id)
        if settlement.state != "failed":
            self._merge_journal.record_external(
                f"merge:{review_id}",
                request=request,
                response={
                    "merged": True,
                    "sha": proven.merge_sha,
                    "external": True,
                },
            )
        if settlement.state == "settled":
            return _outcome(record, reason=record.reason or "merged")
        if settlement.state == "failed":
            # This path exists exactly to repair a post-merge proof
            # failure: reopen only when the failed settlement's own
            # merge effect actually completed and its response sha is
            # the exact merge commit this fresh proof just bound —
            # never invent or repeat a mutation.
            effect = (
                self._merge_journal.get(f"merge:{review_id}")
                if self._merge_journal is not None
                else None
            )
            reopenable = (
                effect is not None
                and effect.state == "completed"
                and effect.response is not None
                and str(effect.response.get("sha")) == proven.merge_sha
            )
            if not reopenable:
                return _outcome(record, reason=settlement.reason or record.state)
            self._settlements.reopen_failed(
                review_id,
                reason=(
                    f"reconciled {proven.relation} at base {manifest.base_sha}"
                ),
            )
            settlement = self._settlements.get(review_id)
        token = self._settlements.claim(review_id)
        if token is None:
            return _outcome(
                record,
                state="deferred",
                reason="the exclusive merge claim is held elsewhere",
            )
        # INFRA-218 Sol correction 44eb2806 (a): captured before either
        # branch runs, mirroring ``merge_approved`` — reflects whether
        # this call is the fresh reconciliation (the review enters
        # anything other than ``merged``) or a resumed convergence of
        # an already-``merged`` review, the exact ``reconciling``
        # distinction ``_project_after_merge`` requires.
        was_already_merged = record.state == "merged"
        if was_already_merged:
            # Same replay character as ``_drive_merge``'s already-merged
            # branch (Sol Critical a626cf1f): reaching here with the
            # review already ``merged`` means a prior pass completed
            # ``_settle_proven`` and crashed before this method's own
            # ``mark_merged``/``mark_settled`` tail below, so this is a
            # resumed reconciliation, never the merge that just
            # happened. No projection call here — see below.
            outcome = _outcome(record, reason=record.reason or "merged")
        else:
            outcome = await self._settle_proven(
                record,
                proven,
                reason=f"externally merged; reconciled {proven.relation}",
            )
        self._settlements.mark_merged(
            review_id, token=token, merge_sha=proven.merge_sha
        )
        self._settlements.mark_settled(review_id, token=token)
        # INFRA-218 Sol correction 44eb2806 (a): the settlement is now
        # TERMINAL before the post-merge Linear projection is ever
        # attempted — see ``merge_approved`` for the full rationale.
        # Dedup against a live assignment on a replay via
        # ``reconciling``, exactly as before.
        await self._project_fail_soft(
            self._get(review_id), reconciling=was_already_merged
        )
        return outcome

    # -- QA ---------------------------------------------------------------

    async def reject_from_qa(
        self, issue_id: str, *, reason: str, evidence: str
    ) -> MergeOutcome:
        """Return merged work to the Claude lead after a QA rejection."""

        if not reason.strip():
            raise ValueError("rejection reason is required")
        if not evidence.strip():
            raise ValueError("rejection evidence is required")
        record = self.current_review(issue_id)
        if record is None or record.state != "merged":
            raise ValueError(f"issue {issue_id} has no merged review to reject")
        packet = CorrectionPacket(
            severity="Critical",
            repository=record.repository,
            branch=record.branch,
            pr_number=record.pr_number,
            reviewed_sha=record.reviewed_sha,
            evidence=reason,
            acceptance_criterion="QA acceptance of the merged change",
            required_correction=(
                "Correct the QA-rejected behaviour and submit a new candidate; "
                f"QA evidence: {evidence}"
            ),
            required_tests=(),
        )
        record = self._transition(
            record,
            "qa_rejected",
            reason=reason,
            projection=self._qa.after_rejection(issue_id),
        )
        self._dispatch_packets(
            record, (packet,), "review.qa_rejected", source="qa_rejection"
        )
        await self._return_to_lead(
            record, effect_id=f"linear:{issue_id}:qa-rejected:{record.review_id}"
        )
        return _outcome(record, reason=reason)

    # -- reads ------------------------------------------------------------

    def current_review(self, issue_id: str) -> ReviewRecord | None:
        placeholders = ",".join("?" for _ in _LIVE_STATES)
        row = self._database.execute(
            f"SELECT * FROM reviews WHERE issue_id = ? AND state IN ({placeholders}) "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (issue_id, *_LIVE_STATES),
        ).fetchone()
        return None if row is None else _row_to_record(row)

    def is_descendant_candidate(
        self, project_key: str, *, newer_sha: str, older_sha: str
    ) -> bool:
        """True iff ``newer_sha`` is a proven descendant of ``older_sha``.

        INFRA-218 (S2 plumbing): candidate supersession needs the exact
        ancestry predicate the merge machinery already owns and proves
        with — ``AncestryVerifier.is_ancestor`` over the project's own
        checkout, the same port and repository path
        :class:`~hermes_orchestrator.merge.IntegrationMerge` uses when
        it proves a candidate reachable from its merge commit. That
        port is private to the merge state machine, so this narrow
        read-only accessor exposes exactly the one question wake
        supersession must answer, and nothing else: no merge, no
        mutation, no new git surface.

        Fail closed: an unknown project, an identical pair (a commit is
        never its own supersessor), or any git error answers ``False``,
        so an unprovable relationship can never retire a wake.
        """

        project = self._projects.get(project_key)
        if project is None or newer_sha == older_sha:
            return False
        try:
            return self._merge.is_ancestor_commit(
                project_key, ancestor=older_sha, descendant=newer_sha
            )
        except GitError:
            return False

    def issue_for_candidate(self, project_key: str, reviewed_sha: str) -> str | None:
        """The issue whose review bound this exact candidate SHA, if any."""

        row = self._database.execute(
            "SELECT issue_id FROM reviews WHERE project_key = ? "
            "AND reviewed_sha = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (project_key, reviewed_sha),
        ).fetchone()
        return None if row is None else str(row["issue_id"])

    # -- internals --------------------------------------------------------

    def _stale_settlement_reason(self, settlement: Settlement) -> str | None:
        """The refusal reason when the stored reviewer binding went stale.

        Sol 165f5ee6 packet 3: a durable settlement carries the reviewer
        thread and generation whose submission approved it, and it may
        settle only while that exact identity is still the live ready
        reviewer channel. The check reads the durable
        ``reviewer_channels`` row directly, so every recovery entry point
        fences against the same truth the submission fence uses; a
        missing or not-ready channel fails closed. Externally-merged
        reconciliation settlements carry no live-channel binding — the
        mutation already exists on GitHub — and are exempt.
        """

        if settlement.path == EXTERNALLY_MERGED:
            return None
        row = self._database.execute(
            "SELECT thread_id, generation FROM reviewer_channels "
            "WHERE project_key = ? AND state = 'ready'",
            (settlement.project_key,),
        ).fetchone()
        if (
            row is not None
            and str(row["thread_id"]) == settlement.thread_id
            and int(row["generation"]) == settlement.thread_generation
        ):
            return None
        return (
            "the settlement's reviewed thread and generation no longer "
            "match the ready reviewer channel; the settlement stays "
            "resumable and non-settling until a fresh generation-bound "
            "submission re-approves the candidate"
        )

    def _get(self, review_id: str) -> ReviewRecord:
        row = self._database.execute(
            "SELECT * FROM reviews WHERE review_id = ?", (review_id,)
        ).fetchone()
        if row is None:
            raise KeyError(review_id)
        return _row_to_record(row)

    def _transition(
        self,
        record: ReviewRecord,
        state: str,
        *,
        reason: str,
        merge_sha: str | None = None,
        projection: LinearProjection | None = None,
    ) -> ReviewRecord:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviews SET state = ?, reason = ?, merge_sha = "
                "COALESCE(?, merge_sha), projection_json = COALESCE(?, "
                "projection_json), updated_at = ? WHERE review_id = ? "
                "AND state = ?",
                (
                    state,
                    reason,
                    merge_sha,
                    None if projection is None else projection.model_dump_json(),
                    stamp,
                    record.review_id,
                    record.state,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"review {record.review_id} changed state concurrently"
                )
            self._events.append(
                connection,
                EventInput(
                    event_type=f"review.{state}",
                    aggregate_type="review",
                    aggregate_id=record.review_id,
                    correlation_id=record.event_id,
                    payload={
                        "issue_id": record.issue_id,
                        "from": record.state,
                        "reason": reason,
                        "merge_sha": merge_sha,
                    },
                ),
            )
        return self._get(record.review_id)

    def _dispatch_packets(
        self,
        record: ReviewRecord,
        packets: tuple[CorrectionPacket, ...],
        event_type: str,
        *,
        source: str,
    ) -> None:
        with self._database.transaction() as connection:
            self._events.append(
                connection,
                EventInput(
                    event_type=event_type,
                    aggregate_type="review",
                    aggregate_id=record.review_id,
                    correlation_id=record.event_id,
                    payload={
                        "issue_id": record.issue_id,
                        "reviewed_sha": record.reviewed_sha,
                        "packets": [
                            {
                                "severity": packet.severity,
                                "acceptance_criterion": packet.acceptance_criterion,
                                "required_correction": packet.required_correction,
                                "required_tests": list(packet.required_tests),
                            }
                            for packet in packets
                        ],
                    },
                ),
            )
        self._lead.deliver(record.issue_id, packets, source=source)

    async def _return_to_lead(self, record: ReviewRecord, *, effect_id: str) -> None:
        self._queue.transition(
            record.issue_id,
            IssueState.IN_DEVELOPMENT,
            actor="codex_merger",
            reason=f"{record.state} {record.reviewed_sha}",
        )
        await self._linear.project(
            record.issue_id,
            self._qa.after_rejection(record.issue_id),
            effect_id=effect_id,
        )

    async def _project_fail_soft(
        self, record: ReviewRecord, *, reconciling: bool
    ) -> None:
        """Run :meth:`_project_after_merge`, but never let it block settlement.

        INFRA-218 (S3), generalizing S1(a): a durably-proven merge or
        settlement must reach its terminal durable state regardless of
        any downstream Linear/queue/assignment projection failure — a
        missing stored projection, a Linear API error, an
        acceptance-dispatch failure. Every caller of
        ``_project_after_merge`` reaches it only after its own durable
        write has already committed (the review row's ``merged``
        transition and ``merge.proven`` event for the fresh caller in
        ``_settle_proven``; nothing new at all for the replay callers
        in ``_drive_merge`` and ``reconcile_external_merge``, which
        perform no mutation), so a projection failure here is reported
        and left for the next recovery boundary — ``reconcile_acceptance``,
        riding the same ``resume_settlements`` pass — to repair,
        exactly as that pass already fails soft per gate.
        """

        try:
            await self._project_after_merge(record, reconciling=reconciling)
        except Exception as error:
            print(
                "post-merge projection for "
                f"{record.review_id!r} failed and will retry at the "
                f"next recovery boundary: {type(error).__name__}: "
                f"{error}",
                file=sys.stderr,
            )

    async def _project_after_merge(
        self, record: ReviewRecord, *, reconciling: bool = False
    ) -> None:
        """Project a merged review onward; ``reconciling`` marks a replay.

        ``reconciling`` is threaded to :meth:`_hold_for_acceptance` (and
        from there to ``_dispatch_acceptance_assignment``) so that every
        caller re-deriving this projection for a review that was
        already ``merged`` on a prior pass — not the fresh merge that
        just happened — dedups against a live acceptance assignment
        instead of opening a fresh dispatch epoch over one the lead may
        have already acknowledged. The one FRESH caller is
        ``_settle_proven``, right after ``_transition`` first marks the
        review ``merged``; every REPLAY caller (``_drive_merge``'s
        already-merged branch, and ``reconcile_external_merge``'s) must
        pass ``reconciling=True``.
        """

        if self._acceptance is not None and self._acceptance.pending(
            record.issue_id
        ):
            await self._hold_for_acceptance(record, reconciling=reconciling)
            return
        projection = record.projection
        if projection is None:
            raise RuntimeError("merged review has no stored projection")
        self._queue.transition(
            record.issue_id,
            IssueState.QA if projection.status == "QA" else IssueState.DONE,
            actor="codex_merger",
            reason=f"merged {record.merge_sha}",
        )
        effect_id = f"linear:{record.issue_id}:after-merge:{record.review_id}"
        # INFRA-218 Sol correction deb5ec49: journal the exact target
        # BEFORE the first live Linear read (the identical target-only
        # primitive activation already uses — see
        # ``ExternalEffectStore.begin_in`` in
        # ``cells._activate_issue_transaction``), so an outage on that
        # very first ``validate_issue`` read still leaves a durable
        # pending row for ``reconcile_post_merge_projections`` to
        # replay. ``begin`` adopts an already-pending or already-
        # completed row untouched, so a replay of this method (a
        # resumed settlement whose review was already ``merged`` on a
        # prior pass) never double-journals.
        self._effects.begin(
            effect_id,
            target=record.issue_id,
            request=projection_request(record.issue_id, projection),
        )
        # INFRA-218 Sol correction d65491fe (defect 2): route through the
        # legal-walk applier below rather than a bare ``self._linear.project``
        # call, so even a fresh merge whose issue drifted status between the
        # review being opened and the merge landing converges instead of
        # raising.
        await self._apply_after_merge_target(record.issue_id, projection, effect_id)

    async def _apply_after_merge_target(
        self, issue_id: str, target: LinearProjection, effect_id: str
    ) -> None:
        """Apply an after-merge Linear target, walking illegal hops.

        INFRA-218 Sol correction d65491fe (defect 2): a stored Done/QA
        target can be unreachable in a single hop from the issue's CURRENT
        Linear status -- a review whose stored target is "Done" replayed
        while the issue is still "In Development" is not a legal direct
        pair in ``linear._ALLOWED_STATUS_TRANSITIONS``, and
        ``LinearClient.project`` raises rather than mutating. That raise
        names the live current status in its message; :data:`_legal_status_path`
        walks the legal intermediate states from that status to
        ``target.status`` (e.g. In Development -> Review -> Done), applying
        one hop at a time. Each hop is projected under its own deterministic
        effect id (the base id suffixed with the hop's status), so a replay
        of any hop is idempotent and a partial walk (crash mid-walk) resumes
        exactly where it stopped -- the final hop reuses the ORIGINAL
        ``effect_id`` and the exact ``target``, so the durable pending row
        this effect id names completes normally, exactly as a direct
        single-hop projection would have. When no legal walk exists at all,
        the intent is left pending (not raised) for the next recovery
        boundary -- the caller's own fail-soft wrapper never sees an
        exception for this case.
        """

        try:
            await self._linear.project(issue_id, target, effect_id=effect_id)
            self._complete_post_merge_effect(effect_id)
            return
        except ValueError as error:
            match = _TRANSITION_NOT_ALLOWED.match(str(error))
            if match is None or target.status is None:
                raise
            current_status = match.group("current")

        path = _legal_status_path(current_status, target.status)
        if path is None:
            print(
                "post-merge projection for "
                f"{effect_id!r} has no legal Linear status walk from "
                f"{current_status!r} to {target.status!r}; left pending "
                "for the next recovery boundary",
                file=sys.stderr,
            )
            return

        for hop_status in path[:-1]:
            hop_effect_id = f"{effect_id}:via:{hop_status.replace(' ', '-')}"
            await self._linear.project(
                issue_id,
                LinearProjection(
                    status=hop_status, assignee_alias=target.assignee_alias
                ),
                effect_id=hop_effect_id,
            )
            self._complete_post_merge_effect(hop_effect_id)

        await self._linear.project(issue_id, target, effect_id=effect_id)
        self._complete_post_merge_effect(effect_id)

    async def _hold_for_acceptance(
        self, record: ReviewRecord, *, reconciling: bool = False
    ) -> None:
        """Hold a merged, acceptance-gated issue short of completion.

        INFRA-198 J1: the merge recorded implementation completion, not
        operator acceptance. The queue holds the issue in
        ``post_merge_acceptance``, exactly one durable acceptance
        assignment dispatches the next post-merge action to the live
        project cell, and Linear returns to In Development for the
        operator. Every step is idempotent, so a settlement replay
        produces one queue transition, one durable assignment, and one
        Linear effect. Once the gate is satisfied (or absent), replays
        of this projection complete the issue exactly as before.
        ``reconciling`` marks a recovery replay of an already-projected
        merge (Sol 04d013b0 finding 1; scope widened by Sol Critical
        a626cf1f to every replay caller of ``_project_after_merge``, not
        only the ``reconcile_acceptance`` pass): dispatch then dedups
        against any live assignment instead of opening a fresh dispatch
        epoch over an acknowledged one.
        """

        assert self._acceptance is not None
        gate = self._acceptance.get(record.issue_id)
        assert gate is not None
        prior_state = self._queue.get(record.issue_id).state
        self._queue.transition(
            record.issue_id,
            IssueState.POST_MERGE_ACCEPTANCE,
            actor="codex_merger",
            reason=f"merged {record.merge_sha}; acceptance pending",
        )
        self._dispatch_acceptance_assignment(
            record, gate, prior_state=prior_state.value, reconciling=reconciling
        )
        await self._linear.project(
            record.issue_id,
            LinearProjection(status="In Development", assignee_alias="operator"),
            effect_id=(
                f"linear:{record.issue_id}:acceptance-hold:{record.review_id}"
            ),
        )

    def _dispatch_acceptance_assignment(
        self,
        record: ReviewRecord,
        gate: AcceptanceGate,
        *,
        prior_state: str,
        reconciling: bool = False,
    ) -> None:
        """Publish the durable acceptance assignment packet, exactly once.

        Follows the classic-dispatch contract: the packet commits inside
        one transaction with the live project cell's exact identity, and
        ``publish_in``'s own supersede/no-op logic makes replays durable
        no-ops (an unconsumed ``published`` packet is never duplicated).
        Without the wired :class:`LeadAssignments` collaborator, or when
        the project has no live cell to return the acceptance action to,
        the assignment is skipped — the issue still holds in
        ``post_merge_acceptance``, and the reconciliation repair (packet
        K) re-derives the missing dispatch from the durable gate.

        Sol 04d013b0 finding 1: ``publish_in`` treats an acknowledged
        row as a consumed dispatch epoch and supersedes it with a fresh
        packet — right for a genuine re-queue, wrong for recovery, which
        must repair a MISSING dispatch, never replace a delivered one.
        On any REPLAY path — the ``reconcile_acceptance`` pass, or a
        resumed settlement/external-merge reconciliation whose review
        was already ``merged`` on a prior pass (Sol Critical a626cf1f)
        — a gate-scoped durable dedup runs in the same transaction
        first: any non-superseded assignment for this issue and
        instruction (``published`` OR ``acknowledged``) proves the
        action already dispatched, and the replay does nothing. Only
        the genuine merge-time dispatch (a review transitioning to
        ``merged`` for the first time) keeps its epoch semantics.
        """

        if self._assignments is None:
            return
        assignment = None
        with self._database.transaction() as connection:
            if reconciling:
                dispatched = connection.execute(
                    "SELECT 1 FROM lead_assignments WHERE issue_id = ? "
                    "AND instruction_id = ? AND state != 'superseded' "
                    "LIMIT 1",
                    (record.issue_id, gate.instruction_id),
                ).fetchone()
                if dispatched is not None:
                    return
            cell = connection.execute(
                "SELECT cell_id, session_id, profile_alias FROM project_cells "
                "WHERE project_key = ? AND state IN "
                "('starting', 'active', 'handoff_required', 'paused')",
                (record.project_key,),
            ).fetchone()
            if cell is None or cell["session_id"] is None:
                return
            assignment = self._assignments.publish_in(
                connection,
                project_key=record.project_key,
                issue_id=record.issue_id,
                cell_id=str(cell["cell_id"]),
                session_id=str(cell["session_id"]),
                profile_alias=str(cell["profile_alias"] or ""),
                instruction_id=gate.instruction_id,
                queue_transition=(
                    f"{prior_state}->{IssueState.POST_MERGE_ACCEPTANCE.value}"
                ),
            )
        if assignment is not None:
            # The durable row is already the truth; this only routes it
            # to the channel immediately instead of the next repair tick.
            self._assignments.notify_committed(assignment)


def _outcome(
    record: ReviewRecord, *, reason: str, state: str | None = None
) -> MergeOutcome:
    return MergeOutcome(
        review_id=record.review_id,
        issue_id=record.issue_id,
        state=state or record.state,
        merge_sha=record.merge_sha,
        projection=record.projection,
        reason=reason,
    )


def _row_to_record(row: Any) -> ReviewRecord:
    projection_json = row["projection_json"]
    return ReviewRecord(
        review_id=str(row["review_id"]),
        project_key=str(row["project_key"]),
        issue_id=str(row["issue_id"]),
        event_id=str(row["event_id"]),
        repository=str(row["repository"]),
        branch=str(row["branch"]),
        pr_number=int(row["pr_number"]),
        reviewed_sha=str(row["reviewed_sha"]),
        state=str(row["state"]),
        merge_sha=None if row["merge_sha"] is None else str(row["merge_sha"]),
        reason=None if row["reason"] is None else str(row["reason"]),
        projection=(
            None
            if projection_json is None
            else LinearProjection.model_validate(json.loads(projection_json))
        ),
    )
