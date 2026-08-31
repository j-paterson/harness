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
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from hermes_orchestrator.acceptance import AcceptanceGate, AcceptanceGates
from hermes_orchestrator.ci_window import CiWindow
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import IssueState
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.github import MergeBlocked, MergeEffectJournal
from hermes_orchestrator.lead_assignments import LeadAssignments
from hermes_orchestrator.linear import LinearProjection
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
        return tuple(outcomes)

    async def _drive_merge(self, review_id: str) -> MergeOutcome:
        """The idempotent merge body; call only under a settlement claim.

        Re-reads the pull request, refuses to overfill the CI window,
        merges through the proven state machine, journals the unresolved
        merge without any CI query, then projects Done or QA. Every path
        before ancestry proof leaves Linear in Review. Safe to replay.
        """

        record = self._get(review_id)
        if record.state == "merged":
            await self._project_after_merge(record)
            return _outcome(record, reason=record.reason or "merged")
        if record.state != "approved":
            return _outcome(record, reason=record.reason or record.state)
        project = self._projects[record.project_key]
        settlement = self._settlements.get(review_id)

        pull = self._github.get_pull_request(record.repository, record.pr_number)
        if (
            pull.merged
            and pull.head_sha == record.reviewed_sha
            and pull.head_ref == record.branch
            and pull.base_ref == project.integration_branch
            and pull.merge_commit_sha is not None
        ):
            # A permitted direct exact-head merge (Sol merged the exact
            # approved candidate itself): reconcile it into the same
            # durable receipts instead of going stale — proofs first,
            # then the external merge-effect receipt, then the ledger,
            # review, and Linear tail, all exactly once.
            try:
                proven = self._merge.prove_landed(
                    record.project_key,
                    candidate_sha=record.reviewed_sha,
                    candidate_branch=record.branch,
                    pr_number=record.pr_number,
                    merge_sha=pull.merge_commit_sha,
                    base_sha=settlement.base_sha,
                )
            except ReconciliationRequired as error:
                record = self._transition(
                    record, "reconciliation_required", reason=str(error)
                )
                return _outcome(record, reason=str(error))
            if self._merge_journal is not None:
                self._merge_journal.record_external(
                    f"merge:{record.review_id}",
                    request={
                        "repository": record.repository,
                        "number": record.pr_number,
                        "sha": record.reviewed_sha,
                        "head_ref": record.branch,
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
        """Journal, transition, and project one proven merge; idempotent."""

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
        await self._project_after_merge(record)
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

        pull = self._github.get_pull_request(project.github_repo, pr_number)
        if not pull.merged:
            raise ValueError("the pull request is not merged; nothing to reconcile")
        if pull.head_ref != manifest.branch or pull.head_sha != manifest.candidate_sha:
            raise ValueError(
                "the merged pull request head is not the reviewed candidate"
            )
        if pull.base_ref != project.integration_branch:
            raise ValueError(
                "the merged pull request base is not the integration branch"
            )
        if pull.merge_commit_sha is None:
            raise ReconciliationRequired(
                "the merged pull request reports no merge commit"
            )
        proven = self._merge.prove_landed(
            project_key,
            candidate_sha=manifest.candidate_sha,
            candidate_branch=manifest.branch,
            pr_number=pr_number,
            merge_sha=pull.merge_commit_sha,
            base_sha=manifest.base_sha,
        )

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
        self._merge_journal.record_external(
            f"merge:{review_id}",
            request={
                "repository": project.github_repo,
                "number": pr_number,
                "sha": manifest.candidate_sha,
                "head_ref": manifest.branch,
                "base": project.integration_branch,
                "merge_method": "squash",
            },
            response={
                "merged": True,
                "sha": proven.merge_sha,
                "external": True,
            },
        )
        if record.state == "merged":
            await self._project_after_merge(record)
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

    async def _project_after_merge(self, record: ReviewRecord) -> None:
        if self._acceptance is not None and self._acceptance.pending(
            record.issue_id
        ):
            await self._hold_for_acceptance(record)
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
        await self._linear.project(
            record.issue_id,
            projection,
            effect_id=f"linear:{record.issue_id}:after-merge:{record.review_id}",
        )

    async def _hold_for_acceptance(self, record: ReviewRecord) -> None:
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
            record, gate, prior_state=prior_state.value
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
        """

        if self._assignments is None:
            return
        assignment = None
        with self._database.transaction() as connection:
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
