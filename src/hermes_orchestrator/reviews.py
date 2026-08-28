"""Independent Codex merge workflow: verdict, merge, CI window, Linear.

INFRA-166 composes the pieces delivered by INFRA-162 through INFRA-165 into
one durable, restart-safe flow. A recorded verdict either returns structured
correction packets to the Claude lead or becomes an approved review; an
approved review is merged only through :class:`IntegrationMerge`, journaled
in the CircleCI merge window without any CI query, and only then projected
to Done or QA by the :class:`QaRouter`. Any failure before ancestry proof
leaves Linear in Review. A QA rejection supersedes the merged review,
projects In Development to the operator, and returns a Critical packet to
the lead. CircleCI is never polled here.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from hermes_orchestrator.ci_window import CiWindow
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import IssueState
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.github import MergeBlocked
from hermes_orchestrator.linear import LinearProjection
from hermes_orchestrator.merge import (
    IntegrationMerge,
    MergeClient,
    ReconciliationRequired,
)
from hermes_orchestrator.qa import QaRouter
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.review_intake import AdmittedCandidate
from hermes_orchestrator.verdicts import CorrectionPacket, ReviewVerdict

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
        now: Callable[[], datetime] | None = None,
    ) -> None:
        for name, port in (
            ("github", github),
            ("merge", merge),
            ("window", window),
            ("linear", linear),
            ("qa", qa),
            ("lead", lead),
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
        self._now = now or (lambda: datetime.now(UTC))

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
        state = (
            "corrections_required" if verdict.packets else "approved"
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
        """Merge one approved review and project its exact Linear state.

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

        pull = self._github.get_pull_request(record.repository, record.pr_number)
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

        try:
            proven = self._merge.merge_approved(
                record.project_key,
                record.as_verdict(),
                effect_id=f"merge:{review_id}",
            )
        except MergeBlocked as error:
            record = self._transition(record, "blocked", reason=str(error))
            return _outcome(record, reason=str(error))
        except ReconciliationRequired as error:
            record = self._transition(
                record, "reconciliation_required", reason=str(error)
            )
            return _outcome(record, reason=str(error))

        self._window.record_merge(proven)
        projection = self._qa.after_merge(record.issue_id)
        record = self._transition(
            record,
            "merged",
            reason=f"proven {proven.relation}",
            merge_sha=proven.merge_sha,
            projection=projection,
        )
        await self._project_after_merge(record)
        return _outcome(record, reason=record.reason or "merged")

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

    def issue_for_candidate(
        self, project_key: str, reviewed_sha: str
    ) -> str | None:
        """The issue whose review bound this exact candidate SHA, if any."""

        row = self._database.execute(
            "SELECT issue_id FROM reviews WHERE project_key = ? "
            "AND reviewed_sha = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (project_key, reviewed_sha),
        ).fetchone()
        return None if row is None else str(row["issue_id"])

    # -- internals --------------------------------------------------------

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
