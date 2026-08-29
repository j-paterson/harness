"""Turn the Merger's completed review turn into durable review outcomes.

INFRA-166: after a delivered ``FABLE_*`` wake, the Codex Merger thread
reviews the candidate on its own turn and reports either the exact idle
``BLOCKED_ON_EXTERNAL_INTAKE`` line or a structured verdict. This service
is the orchestrator side of that boundary: on a turn-completed signal it
admits the delivered wake through every intake gate (CI reconciliation,
then the one-pull-request GitHub gate), reads the thread's latest report,
parses it against the admitted candidate binding, and drives
:class:`ReviewService`. Nothing here polls: every call is caused by a
delivered wake or a completed-turn notification.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from hermes_orchestrator.ci_window import (
    CiWindow,
    MergeWindowExhausted,
    PriorMergeFailed,
)
from hermes_orchestrator.codex_merger import CodexMerger
from hermes_orchestrator.codex_rpc import RpcNotification
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
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
    parse_turn_report,
)

TURN_COMPLETED_METHODS = frozenset({"turn/completed", "thread/turn/completed"})


class RpcRequester(Protocol):
    async def request(
        self, method: str, params: dict[str, Any] | None, timeout: float
    ) -> dict[str, Any]: ...


class ThreadReportSource(Protocol):
    """Read the latest completed agent report of one thread."""

    async def latest_report(self, thread_id: str) -> str | None: ...


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


class MergerTurnService:
    """Admit the delivered wake and settle the Merger's report, exactly once."""

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
    ) -> None:
        self._database = database
        self._window = window
        self._projects = dict(projects)
        self._merger = merger
        self._admission = admission
        self._reviews = reviews
        self._reports = reports
        self._github = github
        self._lead = lead
        self._manifest_root = manifest_root
        self._now = now or (lambda: datetime.now(UTC))

    def outstanding_wake(self, project_key: str) -> tuple[WakeEvent, str] | None:
        """The project's admitted wake, else its oldest delivered wake."""

        for state in ("admitted", "delivered"):
            row = self._database.execute(
                "SELECT status, issue_id, candidate_sha, base_sha, "
                "manifest_path, event_id, manifest_digest FROM wake_deliveries "
                "WHERE project_key = ? AND state = ? "
                "ORDER BY created_at ASC, rowid ASC LIMIT 1",
                (project_key, state),
            ).fetchone()
            if row is not None:
                return _row_to_event(row), state
        return None

    async def on_notification(
        self, notification: RpcNotification
    ) -> TurnOutcome | None:
        """Handle a completed-turn notification for a known Merger thread."""

        if notification.method not in TURN_COMPLETED_METHODS:
            return None
        thread_id = notification.params.get("threadId")
        if not isinstance(thread_id, str):
            return None
        for project_key in self._projects:
            channel = self._merger.read_channel(project_key)
            if channel is not None and channel.thread_id == thread_id:
                return await self.handle_turn(project_key)
        return None

    async def recover_outstanding(
        self, project_keys: tuple[str, ...] | None = None
    ) -> tuple[TurnOutcome, ...]:
        """Settle any wake whose completed-turn notification was lost.

        INFRA-194: the completed-turn notification is delivery, not
        truth — a daemon restart, an rpc drop, or a crashed handler
        loses it while the delivered wake and the thread's completed
        report both survive durably. This boundary pass re-derives
        them: each project with an outstanding wake gets exactly one
        ``handle_turn``, which no-ops when the report is not yet a
        verdict and settles exactly once when it is (the settlement
        claim and idempotent review make a racing notification
        harmless). Never called on a timer — only at startup and
        explicit intake boundaries, so nothing polls.
        """

        outcomes: list[TurnOutcome] = []
        for project_key in project_keys or tuple(self._projects):
            if self.outstanding_wake(project_key) is None:
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

    async def handle_turn(self, project_key: str) -> TurnOutcome:
        """Admit, read, parse, and settle the project's outstanding wake."""

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
            return TurnOutcome(
                project_key, "no_outstanding_wake", None, None,
                "no delivered candidate wake; terminal idle",
            )
        event, state = outstanding
        if state == "delivered":
            try:
                admitted = self._admission.admit(
                    project_key, event, received_generation=channel.generation
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
                self._merger.release_delivered_wake(
                    project_key, event.event_id, outcome="rejected"
                )
                return TurnOutcome(
                    project_key, "rejected", event.event_id, event.issue_id,
                    str(rejected),
                )
        else:
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
            admitted = AdmittedCandidate(
                project_key=project_key,
                manifest=snapshot.manifest,
                thread_id=channel.thread_id,
                generation=channel.generation,
            )
        pr_number = self._pull_number(project, admitted)
        if pr_number is None:
            self._merger.complete_admitted_wake(project_key, event.event_id)
            return TurnOutcome(
                project_key, "rejected", event.event_id, event.issue_id,
                "the candidate is no longer the head of exactly one open pull "
                "request toward the integration branch",
            )
        # Every rejection-capable validation — manifest, channel generation,
        # branch head, base, issue, intake gates, admission, and the final
        # live pull-request check — has now passed; only here may an
        # explicitly authorized rework close its bound failure. Later steps
        # (report missing or malformed) are not rejections and replay this
        # event-bound close idempotently.
        self._close_bound_failure(project_key, admitted.manifest)
        text = await self._reports.latest_report(channel.thread_id)
        if text is None:
            return TurnOutcome(
                project_key, "report_unavailable", event.event_id,
                event.issue_id, "the Merger thread has no completed report yet",
            )
        binding = VerdictBinding(
            repository=project.github_repo,
            branch=admitted.manifest.branch,
            pr_number=pr_number,
            reviewed_sha=admitted.manifest.candidate_sha,
        )
        try:
            verdict = parse_turn_report(text, expected=binding)
        except VerdictError as error:
            return TurnOutcome(
                project_key, "verdict_invalid", event.event_id, event.issue_id,
                str(error),
            )
        if verdict is None:
            self._merger.complete_admitted_wake(project_key, event.event_id)
            return TurnOutcome(
                project_key, "idle", event.event_id, event.issue_id,
                "the Merger reported terminal idle for the admitted candidate",
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

    def _pull_number(
        self, project: ProjectConfig, admitted: AdmittedCandidate
    ) -> int | None:
        summaries = self._github.list_open_pulls(
            project.github_repo, base=project.integration_branch
        )
        matching = [
            summary
            for summary in summaries
            if summary.head_ref == admitted.manifest.branch
            and summary.head_sha == admitted.manifest.candidate_sha
        ]
        if len(summaries) != 1 or len(matching) != 1:
            return None
        return matching[0].number


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
