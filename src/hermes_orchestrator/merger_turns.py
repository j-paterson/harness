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

import json
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
from hermes_orchestrator.codex_merger import CodexMerger, ReviewerChannel
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
    parse_verdict,
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
        # Crash-recovery fallback only: a verdict already durably written
        # by an explicit submission whose settlement did not complete is
        # resumed here. Observation never writes a submitted_verdicts row
        # and never infers a fresh verdict from idle.
        submitted = self._pending_submission(project_key, event.event_id)
        outcome = await self._settle_wake(
            project, project_key, channel, event, state, submitted=submitted
        )
        if submitted is not None:
            self._record_settled(event.event_id, outcome)
        return outcome

    async def _settle_wake(
        self,
        project: ProjectConfig,
        project_key: str,
        channel: ReviewerChannel,
        event: WakeEvent,
        state: str,
        *,
        submitted: _Submission | None,
    ) -> TurnOutcome:
        """Admit the wake and settle one verdict source, exactly once.

        The shared downstream settlement for both verdict sources: the
        explicit Sol submission (``submitted`` set — its durable verdict
        document is settled) and the completed-turn observation fallback
        (``submitted`` is ``None`` — the thread's latest report is
        pulled). Everything after the verdict source — admission gates,
        the live pull-request check, parsing against the admitted
        binding, and the idempotent review drive — is identical.
        """

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
        if submitted is not None:
            text: str | None = submitted.verdict_json
        else:
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
        """

        project = self._projects.get(project_key)
        if project is None:
            raise SubmissionRejected(f"unknown project {project_key!r}")
        existing = self._read_submission(event_id)
        if existing is not None:
            return await self._resolve_duplicate(
                existing,
                project_key=project_key,
                issue_id=issue_id,
                event_id=event_id,
                candidate_sha=candidate_sha,
                reviewed_thread_id=reviewed_thread_id,
                reviewed_generation=reviewed_generation,
                verdict_json=verdict_json,
            )
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
        await self._reviews.resume_settlements(project_key)
        outstanding = self.outstanding_wake(project_key)
        if outstanding is None:
            raise SubmissionRejected(
                "no outstanding admitted or delivered wake for the project"
            )
        event, state = outstanding
        if event.event_id != event_id:
            raise SubmissionRejected(
                "submission event does not match the outstanding wake"
            )
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
            pr_number=self._document_pr_number(verdict_json),
            reviewed_sha=candidate_sha,
        )
        try:
            parse_verdict(verdict_json, expected=binding)
        except VerdictError as error:
            raise SubmissionRejected(
                f"verdict document is invalid: {error}"
            ) from error
        submission = self._persist_submission(
            event_id=event_id,
            project_key=project_key,
            issue_id=issue_id,
            candidate_sha=candidate_sha,
            reviewed_thread_id=reviewed_thread_id,
            reviewed_generation=reviewed_generation,
            verdict_json=verdict_json,
        )
        if submission is None:
            # Lost the exactly-once CAS to a concurrent submission.
            raced = self._read_submission(event_id)
            if raced is None:  # pragma: no cover - CAS row cannot vanish
                raise SubmissionRejected("submission row disappeared")
            return await self._resolve_duplicate(
                raced,
                project_key=project_key,
                issue_id=issue_id,
                event_id=event_id,
                candidate_sha=candidate_sha,
                reviewed_thread_id=reviewed_thread_id,
                reviewed_generation=reviewed_generation,
                verdict_json=verdict_json,
            )
        outcome = await self._settle_wake(
            project, project_key, channel, event, state, submitted=submission
        )
        self._record_settled(event_id, outcome)
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

    @staticmethod
    def _document_pr_number(verdict_json: str) -> int:
        """Extract the document's own pull-request number for parsing.

        Only the shape is read here; :func:`parse_verdict` re-validates
        it, and the live single-open-pull-request check inside the shared
        settlement path binds it to reality.
        """

        try:
            value = json.loads(verdict_json)
        except json.JSONDecodeError as error:
            raise SubmissionRejected(
                "verdict document is invalid: not valid JSON"
            ) from error
        pr_number = value.get("pr_number") if isinstance(value, dict) else None
        if (
            not isinstance(pr_number, int)
            or isinstance(pr_number, bool)
            or pr_number < 1
        ):
            raise SubmissionRejected(
                "verdict document is invalid: pr_number is invalid"
            )
        return pr_number

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
                connection.execute(
                    "INSERT INTO submitted_verdicts(event_id, project_key, "
                    "issue_id, candidate_sha, reviewed_thread_id, "
                    "reviewed_generation, verdict_json, state, created_at, "
                    "updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 'submitted', ?, ?)",
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
                    ),
                )
        except sqlite3.IntegrityError:
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

    def _record_settled(self, event_id: str, outcome: TurnOutcome) -> None:
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE submitted_verdicts SET state = 'settled', "
                "result_json = ?, updated_at = ? "
                "WHERE event_id = ? AND state = 'submitted'",
                (
                    _outcome_to_json(outcome),
                    self._now().isoformat(),
                    event_id,
                ),
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
