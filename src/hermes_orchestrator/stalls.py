"""Stall diagnosis, operator consultation, and bounded playbook automation.

INFRA-170: a worker is stalled only when corroborating evidence says so —
elapsed time alone never is. Reasons are normalized to a closed set and
paired with an exact evidence predicate. The operator is consulted on the
first two occurrences of a predicate; the approved remedy is written
atomically to the versioned ``config/playbooks.yaml`` and its content hash
recorded durably. Only the third exact recurrence — same predicate, same
playbook hash, two approved consultations, no failed automatic execution
since — runs automatically. Credentials, spending, external messages,
destructive exceptions, and unregistered commands always ask the operator.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

REASONS = (
    "provider_limit",
    "authentication",
    "approval_wait",
    "dependency",
    "merge_conflict",
    "ci_failure",
    "environment_failure",
    "resource_pressure",
    "repeated_command_failure",
    "agent_uncertainty",
)
SENSITIVE_ACTIONS = frozenset(
    {
        "change_credentials",
        "spend",
        "send_external_message",
        "destructive_exception",
    }
)
REGISTERED_ACTIONS = frozenset(
    {
        "retry_command",
        "rerun_tests",
        "rebase_branch",
        "resume_lead",
        "request_handoff",
        "rotate_profile",
        "wait_for_reset",
        "pause_issue",
        "request_operator",
        "reconcile_ci",
    }
)
_EXTERNAL_FAILURES = {
    "dependency": "dependency",
    "merge_conflict": "merge_conflict",
    "ci": "ci_failure",
    "ci_failure": "ci_failure",
    "environment": "environment_failure",
    "environment_failure": "environment_failure",
    "resource": "resource_pressure",
    "resource_pressure": "resource_pressure",
}
REQUIRED_APPROVALS = 2


@dataclass(frozen=True, slots=True)
class StallEvidence:
    """Raw observations about one worker; every field is optional evidence."""

    no_material_change_seconds: int = 0
    process_alive: bool = True
    output_activity: bool = True
    resource_activity: bool = True
    repeated_failure: str | None = None
    blocked_request: str | None = None
    provider_error: str | None = None
    external_failure: str | None = None
    agent_report: str | None = None
    resource_pressure: bool = False
    project_key: str = "global"


@dataclass(frozen=True, slots=True)
class StallDiagnosis:
    reason: str
    predicate: dict[str, Any]
    predicate_key: str
    project_key: str
    summary: str


@dataclass(frozen=True, slots=True)
class Remedy:
    actions: tuple[str, ...]
    verification: str
    timeout_seconds: int
    rollback: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "actions": list(self.actions),
            "verification": self.verification,
            "timeout_seconds": self.timeout_seconds,
            "rollback": self.rollback,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Remedy:
        return cls(
            actions=tuple(str(item) for item in value["actions"]),
            verification=str(value["verification"]),
            timeout_seconds=int(value["timeout_seconds"]),
            rollback=str(value["rollback"]),
        )

    @property
    def requires_operator(self) -> bool:
        return any(
            action in SENSITIVE_ACTIONS or action not in REGISTERED_ACTIONS
            for action in self.actions
        )


@dataclass(frozen=True, slots=True)
class Playbook:
    reason: str
    predicate: dict[str, Any]
    predicate_key: str
    project_key: str | None
    remedy: Remedy
    approvals: int
    version: int
    entry_hash: str = ""


@dataclass(frozen=True, slots=True)
class Consultation:
    consultation_id: str
    project_key: str
    reason: str
    predicate_key: str
    state: str
    proposed: Remedy | None
    remedy: Remedy | None
    playbook_hash: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "consultation_id": self.consultation_id,
            "project_key": self.project_key,
            "reason": self.reason,
            "predicate_key": self.predicate_key,
            "state": self.state,
            "proposed": None if self.proposed is None else self.proposed.as_dict(),
            "remedy": None if self.remedy is None else self.remedy.as_dict(),
            "playbook_hash": self.playbook_hash,
        }


@dataclass(frozen=True, slots=True)
class ResolutionPlan:
    mode: str  # ask_operator | automatic
    reason: str
    predicate_key: str
    consultation_id: str | None
    actions: tuple[str, ...]
    playbook_hash: str | None
    explanation: str
    remedy: Remedy | None = None
    evidence: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "reason": self.reason,
            "predicate_key": self.predicate_key,
            "consultation_id": self.consultation_id,
            "actions": list(self.actions),
            "playbook_hash": self.playbook_hash,
            "explanation": self.explanation,
            "remedy": None if self.remedy is None else self.remedy.as_dict(),
        }


def predicate_key_for(reason: str, predicate: dict[str, Any]) -> str:
    canonical = json.dumps(
        {"reason": reason, **predicate}, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]


class StallDetector:
    """Classify corroborated stall evidence into a normalized diagnosis."""

    def __init__(self, *, inactivity_seconds: int = 900) -> None:
        if inactivity_seconds <= 0:
            raise ValueError("inactivity_seconds must be positive")
        self._inactivity = inactivity_seconds

    def evaluate(self, evidence: StallEvidence) -> StallDiagnosis | None:
        reason: str | None = None
        predicate: dict[str, Any] = {}
        summary = ""
        error = (evidence.provider_error or "").strip().lower()
        if error in ("limit", "provider_limit", "usage_limit"):
            reason, predicate = "provider_limit", {"signal": "provider_limit"}
            summary = "provider reported a usage limit"
        elif error in ("auth", "authentication", "unauthorized"):
            reason, predicate = "authentication", {"signal": "authentication"}
            summary = "provider reported an authentication failure"
        elif evidence.blocked_request:
            reason = "approval_wait"
            predicate = {
                "signal": "blocked_request",
                "request": evidence.blocked_request,
            }
            summary = f"blocked on {evidence.blocked_request}"
        elif evidence.external_failure:
            mapped = _EXTERNAL_FAILURES.get(evidence.external_failure.strip().lower())
            if mapped is None:
                return None
            reason, predicate = mapped, {"signal": mapped}
            summary = f"{mapped.replace('_', ' ')} reported"
        elif evidence.repeated_failure:
            reason = "repeated_command_failure"
            predicate = {
                "signal": "repeated_failure",
                "command": evidence.repeated_failure,
            }
            summary = f"repeated failure of {evidence.repeated_failure}"
        elif evidence.agent_report:
            reason = "agent_uncertainty"
            predicate = {"signal": "agent_report", "report": evidence.agent_report}
            summary = "worker reported it cannot continue"
        elif evidence.resource_pressure:
            reason, predicate = "resource_pressure", {"signal": "resource_pressure"}
            summary = "host resource pressure"
        elif evidence.no_material_change_seconds >= self._inactivity and (
            not evidence.process_alive
            or (not evidence.output_activity and not evidence.resource_activity)
        ):
            # Inactivity counts only with corroboration: a dead process, or a
            # live one with neither meaningful output nor resource use. A
            # quiet but resource-active process is a long-running command,
            # never a stall on elapsed time alone.
            reason = "agent_uncertainty"
            predicate = {
                "signal": "inactive",
                "process_alive": evidence.process_alive,
                "output_activity": evidence.output_activity,
                "resource_activity": evidence.resource_activity,
            }
            summary = "no material change with corroborating inactivity"
        if reason is None:
            return None
        return StallDiagnosis(
            reason=reason,
            predicate=predicate,
            predicate_key=predicate_key_for(reason, predicate),
            project_key=evidence.project_key,
            summary=summary,
        )


class PlaybookService:
    """Consult twice, persist the approved playbook, automate the third time."""

    def __init__(
        self,
        database: Database,
        events: EventStore,
        *,
        playbook_path: Path,
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._path = playbook_path
        self._now = now or (lambda: datetime.now(UTC))
        self._ids = ids or (lambda: uuid.uuid4().hex)

    # -- playbook file ---------------------------------------------------

    def load(self) -> tuple[dict[str, Any], str]:
        """Return the playbook document and its exact content hash."""

        if not self._path.exists():
            return {"version": 0, "playbooks": []}, _hash(b"")
        raw = self._path.read_bytes()
        document = yaml.safe_load(raw) or {}
        if not isinstance(document, dict) or not isinstance(
            document.get("playbooks", []), list
        ):
            raise ValueError("playbooks.yaml must be a mapping with a playbooks list")
        document.setdefault("version", 0)
        document.setdefault("playbooks", [])
        return document, _hash(raw)

    def current_hash(self) -> str:
        return self.load()[1]

    def playbook_for(self, diagnosis: StallDiagnosis) -> Playbook | None:
        document, _ = self.load()
        chosen: dict[str, Any] | None = None
        for entry in document["playbooks"]:
            if not isinstance(entry, dict):
                continue
            if entry.get("predicate_key") != diagnosis.predicate_key:
                continue
            scope = entry.get("project_key")
            if scope == diagnosis.project_key:
                chosen = entry
                break
            if scope is None and chosen is None:
                chosen = entry
        if chosen is None:
            return None
        return Playbook(
            reason=str(chosen["reason"]),
            predicate=dict(chosen["predicate"]),
            predicate_key=str(chosen["predicate_key"]),
            project_key=chosen.get("project_key"),
            remedy=Remedy.from_dict(chosen["remedy"]),
            approvals=int(chosen.get("approvals", 0)),
            version=int(chosen.get("version", 1)),
            entry_hash=entry_hash(chosen),
        )

    def file_is_approved_version(self, content_hash: str) -> bool:
        """The exact current file content was written by an operator approval."""

        return (
            self._database.scalar(
                "SELECT count(*) FROM playbook_versions WHERE content_hash = ?",
                (content_hash,),
            )
            == 1
        )

    # -- consultation cycle ----------------------------------------------

    def resolve(self, diagnosis: StallDiagnosis) -> ResolutionPlan:
        """Ask on the first two occurrences; automate only the exact third."""

        if diagnosis.reason not in REASONS:
            raise ValueError(f"unknown stall reason {diagnosis.reason!r}")
        playbook = self.playbook_for(diagnosis)
        file_hash = self.current_hash()
        # Approvals bind the exact playbook entry content; the file itself
        # must be an operator-written version, so any out-of-band edit
        # revokes automation until the operator approves again.
        approvals = (
            self._approved_count(diagnosis.predicate_key, playbook.entry_hash)
            if playbook is not None and self.file_is_approved_version(file_hash)
            else 0
        )
        failed_since = playbook is not None and self._failed_since_last_approval(
            diagnosis.predicate_key, playbook.entry_hash
        )
        automatic = (
            playbook is not None
            and approvals >= REQUIRED_APPROVALS
            and not playbook.remedy.requires_operator
            and not failed_since
        )
        if automatic:
            assert playbook is not None
            self._record_event(
                "stall.automatic",
                diagnosis,
                {"playbook_hash": file_hash, "actions": list(playbook.remedy.actions)},
            )
            return ResolutionPlan(
                mode="automatic",
                reason=diagnosis.reason,
                predicate_key=diagnosis.predicate_key,
                consultation_id=None,
                actions=playbook.remedy.actions,
                playbook_hash=file_hash,
                explanation=(
                    f"approved playbook v{playbook.version} matched the exact "
                    f"predicate after {approvals} consultations"
                ),
                remedy=playbook.remedy,
                evidence={"approvals": approvals},
            )
        consultation = self.consult(
            diagnosis, proposed=playbook.remedy if playbook else None
        )
        if playbook is not None and playbook.remedy.requires_operator:
            explanation = (
                "the approved remedy includes a sensitive or unregistered action; "
                "the operator must approve every occurrence"
            )
        elif failed_since:
            explanation = (
                "the last automatic remedy failed or timed out; the operator "
                "must be consulted again"
            )
        elif playbook is None:
            explanation = "no approved playbook matches this exact predicate"
        else:
            explanation = (
                f"{approvals} of {REQUIRED_APPROVALS} approved consultations for "
                "this predicate and playbook hash"
            )
        return ResolutionPlan(
            mode="ask_operator",
            reason=diagnosis.reason,
            predicate_key=diagnosis.predicate_key,
            consultation_id=consultation.consultation_id,
            actions=() if playbook is None else playbook.remedy.actions,
            playbook_hash=file_hash if playbook is not None else None,
            explanation=explanation,
            remedy=None if playbook is None else playbook.remedy,
            evidence={"approvals": approvals, "failed_since": failed_since},
        )

    async def resolve_and_execute(
        self, diagnosis: StallDiagnosis, executor: RemedyExecutor
    ) -> tuple[ResolutionPlan, ExecutionOutcome | None]:
        """Resolve; on the exact third recurrence apply the remedy fail-closed.

        The outcome is recorded durably before returning, so a failure,
        timeout, or failed verification revokes later automation until
        the operator is consulted again.
        """

        plan = self.resolve(diagnosis)
        if plan.mode != "automatic" or plan.remedy is None:
            return plan, None
        try:
            outcome = await executor.execute(diagnosis, plan.remedy)
        except RemedyRefused as refused:
            # A wiring defect (synchronous handler, sensitive rollback) is a
            # failed execution: nothing ran, and automation is revoked.
            outcome = ExecutionOutcome(
                success=False,
                detail=f"refused: {refused}",
                actions_run=(),
                verified=False,
                rolled_back=False,
                elapsed_seconds=0.0,
            )
        self.record_execution(
            diagnosis,
            playbook_hash=plan.playbook_hash,
            mode="automatic",
            success=outcome.success,
            detail=outcome.detail,
        )
        return plan, outcome

    def consult(
        self, diagnosis: StallDiagnosis, *, proposed: Remedy | None = None
    ) -> Consultation:
        consultation_id = self._ids()
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO stall_consultations("
                "consultation_id, project_key, reason, predicate_key, "
                "predicate_json, state, proposed_json, remedy_json, playbook_hash, "
                "asked_at, resolved_at) VALUES (?, ?, ?, ?, ?, 'asked', ?, NULL, "
                "NULL, ?, NULL)",
                (
                    consultation_id,
                    diagnosis.project_key,
                    diagnosis.reason,
                    diagnosis.predicate_key,
                    json.dumps(diagnosis.predicate, sort_keys=True),
                    None if proposed is None else json.dumps(proposed.as_dict()),
                    stamp,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="stall.consultation_requested",
                    aggregate_type="stall",
                    aggregate_id=consultation_id,
                    payload={
                        "reason": diagnosis.reason,
                        "predicate_key": diagnosis.predicate_key,
                        "summary": diagnosis.summary,
                        "project_key": diagnosis.project_key,
                    },
                ),
            )
        return self.get(consultation_id)

    def record_operator_result(
        self, consultation_id: str, remedy: Remedy, *, approved: bool = True
    ) -> Playbook | None:
        """Persist the operator's answer; an approval writes the playbook."""

        consultation = self.get(consultation_id)
        if consultation.state != "asked":
            raise ValueError(f"consultation {consultation_id} is already resolved")
        if not approved:
            self._resolve_consultation(consultation_id, "rejected", remedy, None)
            return None
        return self.approve(consultation_id, remedy)

    def approve(self, consultation_id: str, remedy: Remedy) -> Playbook:
        """Write the approved playbook atomically and bind the consultation."""

        consultation = self.get(consultation_id)
        if consultation.state != "asked":
            raise ValueError(f"consultation {consultation_id} is already resolved")
        if not remedy.actions:
            raise ValueError("an approved remedy requires at least one action")
        if remedy.timeout_seconds <= 0:
            raise ValueError("an approved remedy requires a positive timeout")
        row = self._database.execute(
            "SELECT predicate_json FROM stall_consultations WHERE consultation_id = ?",
            (consultation_id,),
        ).fetchone()
        predicate = json.loads(str(row["predicate_json"]))
        document, _ = self.load()
        entries = [e for e in document["playbooks"] if isinstance(e, dict)]
        scope: str | None = (
            None if consultation.project_key == "global" else consultation.project_key
        )
        existing = next(
            (
                e
                for e in entries
                if e.get("predicate_key") == consultation.predicate_key
                and e.get("project_key") == scope
            ),
            None,
        )
        # A changed remedy is a new playbook version; approvals of the old
        # content do not carry over because the file hash changes.
        if existing is None:
            entry: dict[str, Any] = {
                "reason": consultation.reason,
                "predicate": predicate,
                "predicate_key": consultation.predicate_key,
                "project_key": scope,
                "remedy": remedy.as_dict(),
                "approvals": 1,
                "version": 1,
            }
            entries.append(entry)
        else:
            if existing.get("remedy") != remedy.as_dict():
                existing["version"] = int(existing.get("version", 1)) + 1
                existing["remedy"] = remedy.as_dict()
            existing["approvals"] = int(existing.get("approvals", 0)) + 1
            entry = existing
        document["playbooks"] = entries
        document["version"] = int(document.get("version", 0)) + 1
        content_hash = self._write_atomically(document)
        self._record_version(content_hash, int(document["version"]))
        self._resolve_consultation(
            consultation_id, "approved", remedy, entry_hash(entry)
        )
        return Playbook(
            reason=consultation.reason,
            predicate=predicate,
            predicate_key=consultation.predicate_key,
            project_key=scope,
            remedy=remedy,
            approvals=int(entry["approvals"]),
            version=int(entry["version"]),
            entry_hash=entry_hash(entry),
        )

    def record_execution(
        self,
        diagnosis: StallDiagnosis,
        *,
        playbook_hash: str | None,
        mode: str,
        success: bool,
        detail: str = "",
    ) -> None:
        """Journal a remedy outcome; failures revoke automation until re-consulted."""

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO stall_executions(execution_id, project_key, "
                "predicate_key, playbook_hash, mode, success, detail, executed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    self._ids(),
                    diagnosis.project_key,
                    diagnosis.predicate_key,
                    playbook_hash,
                    mode,
                    int(success),
                    detail,
                    stamp,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="stall.remedy_" + ("succeeded" if success else "failed"),
                    aggregate_type="stall",
                    aggregate_id=diagnosis.predicate_key,
                    payload={
                        "mode": mode,
                        "detail": detail,
                        "playbook_hash": playbook_hash,
                    },
                ),
            )

    def pending(self, project_key: str | None = None) -> tuple[Consultation, ...]:
        if project_key is None:
            rows = self._database.execute(
                "SELECT consultation_id FROM stall_consultations WHERE state = 'asked' "
                "ORDER BY asked_at, rowid"
            ).fetchall()
        else:
            rows = self._database.execute(
                "SELECT consultation_id FROM stall_consultations WHERE state = 'asked' "
                "AND project_key = ? ORDER BY asked_at, rowid",
                (project_key,),
            ).fetchall()
        return tuple(self.get(str(row["consultation_id"])) for row in rows)

    def get(self, consultation_id: str) -> Consultation:
        row = self._database.execute(
            "SELECT * FROM stall_consultations WHERE consultation_id = ?",
            (consultation_id,),
        ).fetchone()
        if row is None:
            raise KeyError(consultation_id)
        return Consultation(
            consultation_id=str(row["consultation_id"]),
            project_key=str(row["project_key"]),
            reason=str(row["reason"]),
            predicate_key=str(row["predicate_key"]),
            state=str(row["state"]),
            proposed=(
                None
                if row["proposed_json"] is None
                else Remedy.from_dict(json.loads(str(row["proposed_json"])))
            ),
            remedy=(
                None
                if row["remedy_json"] is None
                else Remedy.from_dict(json.loads(str(row["remedy_json"])))
            ),
            playbook_hash=row["playbook_hash"],
        )

    # -- internals -------------------------------------------------------

    def _approved_count(self, predicate_key: str, content_hash: str) -> int:
        return int(
            self._database.scalar(
                "SELECT count(*) FROM stall_consultations WHERE predicate_key = ? "
                "AND state = 'approved' AND playbook_hash = ?",
                (predicate_key, content_hash),
            )  # type: ignore[arg-type]
        )

    def _failed_since_last_approval(
        self, predicate_key: str, content_hash: str
    ) -> bool:
        last_approval = self._database.scalar(
            "SELECT max(resolved_at) FROM stall_consultations WHERE predicate_key = ? "
            "AND state = 'approved' AND playbook_hash = ?",
            (predicate_key, content_hash),
        )
        if last_approval is None:
            return False
        failed = self._database.scalar(
            "SELECT count(*) FROM stall_executions WHERE predicate_key = ? "
            "AND success = 0 AND executed_at >= ?",
            (predicate_key, str(last_approval)),
        )
        return int(failed) > 0  # type: ignore[arg-type]

    def _resolve_consultation(
        self,
        consultation_id: str,
        state: str,
        remedy: Remedy,
        content_hash: str | None,
    ) -> None:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE stall_consultations SET state = ?, remedy_json = ?, "
                "playbook_hash = ?, resolved_at = ? WHERE consultation_id = ? "
                "AND state = 'asked'",
                (
                    state,
                    json.dumps(remedy.as_dict()),
                    content_hash,
                    stamp,
                    consultation_id,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type=f"stall.consultation_{state}",
                    aggregate_type="stall",
                    aggregate_id=consultation_id,
                    actor="operator",
                    payload={
                        "playbook_hash": content_hash,
                        "actions": list(remedy.actions),
                    },
                ),
            )

    def _record_version(self, content_hash: str, version: int) -> None:
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO playbook_versions(content_hash, version, path, "
                "recorded_at) VALUES (?, ?, ?, ?)",
                (content_hash, version, str(self._path), self._now().isoformat()),
            )

    def _write_atomically(self, document: dict[str, Any]) -> str:
        payload = yaml.safe_dump(document, sort_keys=True).encode("utf-8")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(
            prefix=".playbooks-", suffix=".tmp", dir=self._path.parent
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._path)
        except BaseException:
            with _suppress_missing():
                os.unlink(temporary)
            raise
        return _hash(payload)

    def _record_event(
        self, event_type: str, diagnosis: StallDiagnosis, payload: dict[str, Any]
    ) -> None:
        with self._database.transaction() as connection:
            self._events.append(
                connection,
                EventInput(
                    event_type=event_type,
                    aggregate_type="stall",
                    aggregate_id=diagnosis.predicate_key,
                    payload={"reason": diagnosis.reason, **payload},
                ),
            )


class RemedyRefused(RuntimeError):
    """A remedy that must never execute automatically reached the executor."""


@dataclass(frozen=True, slots=True)
class ExecutionOutcome:
    success: bool
    detail: str
    actions_run: tuple[str, ...]
    verified: bool
    rolled_back: bool
    elapsed_seconds: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "detail": self.detail,
            "actions_run": list(self.actions_run),
            "verified": self.verified,
            "rolled_back": self.rolled_back,
            "elapsed_seconds": round(self.elapsed_seconds, 3),
        }


ActionHandler = Callable[[StallDiagnosis, Remedy], Any]
Verifier = Callable[[StallDiagnosis, Remedy], Any]


class RemedyExecutor:
    """Fail-closed, async-only execution of registered, approved remedies.

    Every action must be a registered, non-sensitive name with a wired
    handler, and every handler, verifier, and rollback must be a
    cooperatively cancellable coroutine function — a synchronous callable
    is refused before anything runs, because a detached thread cannot be
    stopped at the deadline. The actions and the named verification share
    one deadline; on timeout the running coroutine is cancelled and the
    cancellation is awaited before the approved rollback runs, so a
    timed-out action can never apply a late mutation over the rollback.
    Rollback is bounded by the same approved timeout and its own timeout or
    failure is recorded. Everything runs on the event-loop thread that owns
    the SQLite connection. The outcome is always returned for durable
    recording; nothing here is silently skipped.
    """

    def __init__(
        self,
        *,
        handlers: dict[str, ActionHandler] | None = None,
        verifiers: dict[str, Verifier] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._handlers = dict(handlers or {})
        self._verifiers = dict(verifiers or {})
        self._clock = clock or __import__("time").monotonic

    def refuse_if_operator_only(self, remedy: Remedy) -> None:
        for action in remedy.actions:
            if action in SENSITIVE_ACTIONS:
                raise RemedyRefused(f"sensitive action {action!r} is operator-only")
            if action not in REGISTERED_ACTIONS:
                raise RemedyRefused(f"unregistered action {action!r} is operator-only")
        if remedy.rollback in SENSITIVE_ACTIONS:
            raise RemedyRefused("sensitive rollback is operator-only")

    def refuse_if_not_async(self, remedy: Remedy) -> None:
        """Reject any synchronous handler before a single action starts."""

        for action in remedy.actions:
            handler = self._handlers.get(action)
            if handler is not None and not asyncio.iscoroutinefunction(handler):
                raise RemedyRefused(
                    f"handler for {action!r} is synchronous; remedies must be "
                    "cooperatively cancellable coroutines"
                )
        verifier = self._verifiers.get(remedy.verification)
        if verifier is not None and not asyncio.iscoroutinefunction(verifier):
            raise RemedyRefused(
                f"verifier {remedy.verification!r} is synchronous; remedies must "
                "be cooperatively cancellable coroutines"
            )
        rollback = self._handlers.get(remedy.rollback)
        if rollback is not None and not asyncio.iscoroutinefunction(rollback):
            raise RemedyRefused(
                f"rollback {remedy.rollback!r} is synchronous; remedies must be "
                "cooperatively cancellable coroutines"
            )

    async def execute(
        self, diagnosis: StallDiagnosis, remedy: Remedy
    ) -> ExecutionOutcome:
        self.refuse_if_operator_only(remedy)
        self.refuse_if_not_async(remedy)
        started = self._clock()
        deadline = started + remedy.timeout_seconds
        actions_run: list[str] = []

        async def run() -> str | None:
            for action in remedy.actions:
                handler = self._handlers.get(action)
                if handler is None:
                    return f"no handler wired for registered action {action!r}"
                await asyncio.wait_for(
                    handler(diagnosis, remedy), timeout=deadline - self._clock()
                )
                actions_run.append(action)
            verifier = self._verifiers.get(remedy.verification)
            if verifier is None:
                return f"verification {remedy.verification!r} is not registered"
            passed = await asyncio.wait_for(
                verifier(diagnosis, remedy), timeout=deadline - self._clock()
            )
            if not passed:
                return f"verification {remedy.verification!r} failed"
            return None

        failure: str | None
        verified = False
        try:
            # On timeout wait_for cancels the running coroutine and awaits
            # its cancellation before raising: nothing is still executing
            # when the rollback below starts.
            failure = await asyncio.wait_for(run(), timeout=remedy.timeout_seconds)
            verified = failure is None
        except TimeoutError:
            failure = f"timed out after {remedy.timeout_seconds}s"
        except asyncio.CancelledError:
            raise
        except Exception as error:
            failure = f"execution failed: {type(error).__name__}"
        rolled_back = False
        if failure is not None:
            rolled_back, rollback_detail = await self._rollback(diagnosis, remedy)
            failure += f"; {rollback_detail}"
        elapsed = self._clock() - started
        return ExecutionOutcome(
            success=failure is None,
            detail="verified" if failure is None else failure,
            actions_run=tuple(actions_run),
            verified=verified,
            rolled_back=rolled_back,
            elapsed_seconds=elapsed,
        )

    async def _rollback(
        self, diagnosis: StallDiagnosis, remedy: Remedy
    ) -> tuple[bool, str]:
        """Bounded rollback under the approved timeout; never unbounded."""

        handler = self._handlers.get(remedy.rollback)
        if handler is None:
            return False, (
                "no rollback"
                if remedy.rollback in ("", "none")
                else f"rollback {remedy.rollback!r} has no wired handler"
            )
        try:
            await asyncio.wait_for(
                handler(diagnosis, remedy), timeout=remedy.timeout_seconds
            )
        except TimeoutError:
            return False, f"rollback timed out after {remedy.timeout_seconds}s"
        except asyncio.CancelledError:
            raise
        except Exception as error:
            return False, f"rollback failed: {type(error).__name__}"
        return True, "rollback applied"


class ScheduledResets:
    """Durable, restart-safe scheduled resets consumed once due."""

    def __init__(
        self,
        database: Database,
        events: EventStore,
        *,
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._now = now or (lambda: datetime.now(UTC))
        self._ids = ids or (lambda: uuid.uuid4().hex)

    def schedule(
        self,
        project_key: str,
        predicate_key: str,
        *,
        reason: str,
        delay_seconds: int,
    ) -> str:
        if delay_seconds <= 0:
            raise ValueError("a scheduled reset requires a positive delay")
        moment = self._now()
        reset_id = self._ids()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO scheduled_resets(reset_id, project_key, predicate_key, "
                "reason, scheduled_at, due_at, state, consumed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 'scheduled', NULL)",
                (
                    reset_id,
                    project_key,
                    predicate_key,
                    reason,
                    moment.isoformat(),
                    (moment + timedelta(seconds=delay_seconds)).isoformat(),
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="stall.reset_scheduled",
                    aggregate_type="stall",
                    aggregate_id=predicate_key,
                    payload={
                        "reset_id": reset_id,
                        "project_key": project_key,
                        "delay_seconds": delay_seconds,
                    },
                ),
            )
        return reset_id

    def pending(self, project_key: str, predicate_key: str) -> tuple[str, ...]:
        rows = self._database.execute(
            "SELECT reset_id FROM scheduled_resets WHERE project_key = ? "
            "AND predicate_key = ? AND state = 'scheduled' ORDER BY due_at",
            (project_key, predicate_key),
        ).fetchall()
        return tuple(str(row["reset_id"]) for row in rows)

    def consume_due(self, now: datetime | None = None) -> list[str]:
        moment = (now or self._now()).isoformat()
        consumed: list[str] = []
        with self._database.transaction() as connection:
            rows = connection.execute(
                "SELECT reset_id, project_key, predicate_key FROM scheduled_resets "
                "WHERE state = 'scheduled' AND due_at <= ? ORDER BY due_at, rowid",
                (moment,),
            ).fetchall()
            for row in rows:
                connection.execute(
                    "UPDATE scheduled_resets SET state = 'consumed', consumed_at = ? "
                    "WHERE reset_id = ? AND state = 'scheduled'",
                    (moment, str(row["reset_id"])),
                )
                self._events.append(
                    connection,
                    EventInput(
                        event_type="stall.reset_consumed",
                        aggregate_type="stall",
                        aggregate_id=str(row["predicate_key"]),
                        payload={
                            "reset_id": str(row["reset_id"]),
                            "project_key": str(row["project_key"]),
                        },
                    ),
                )
                consumed.append(str(row["reset_id"]))
        return consumed


class _suppress_missing:
    def __enter__(self) -> None:
        return None

    def __exit__(self, kind: object, value: object, traceback: object) -> bool:
        return isinstance(value, FileNotFoundError)


def _hash(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def entry_hash(entry: dict[str, Any]) -> str:
    """Exact content identity of one playbook entry (scope, predicate, remedy)."""

    canonical = json.dumps(
        {
            "reason": entry.get("reason"),
            "predicate": entry.get("predicate"),
            "predicate_key": entry.get("predicate_key"),
            "project_key": entry.get("project_key"),
            "remedy": entry.get("remedy"),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
