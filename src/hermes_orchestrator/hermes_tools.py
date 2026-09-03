"""Strict, identity-free command boundary exposed to Hermes."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, Protocol

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    TypeAdapter,
    ValidationError,
)

from hermes_orchestrator.domain import AdmissionRequest
from hermes_orchestrator.qa import QaOrigin, QaRouter
from hermes_orchestrator.queue import AdmissionDenied, IdempotencyConflict, QueueService


class LinearReadPort(Protocol):
    """Read-only access to one Linear issue's current workflow status.

    INFRA-227: kept minimal and local rather than importing
    ``reconcile.LinearReadPort`` -- this boundary needs only ``.status``
    off whatever ``get_issue`` returns, never the reconciler's own
    machinery.
    """

    def get_issue(self, issue_id: str) -> Any: ...

NonEmptyText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


class _Command(BaseModel):
    model_config = ConfigDict(extra="forbid")


class QueueIssueCommand(_Command):
    intent: Literal["queue_issue"]
    issue_id: NonEmptyText
    project_key: NonEmptyText
    priority: int = Field(ge=1, le=4)
    operator_instruction_id: NonEmptyText
    qa_origin: Literal["ordinary", "ryan_assigned", "operator_designated"] | None = (
        None
    )


class StatusCommand(_Command):
    intent: Literal["status"]


class PauseCommand(_Command):
    intent: Literal["pause"]
    project_key: NonEmptyText
    reason: NonEmptyText


class ResumeCommand(_Command):
    intent: Literal["resume"]
    project_key: NonEmptyText


class RetryCommand(_Command):
    intent: Literal["retry"]
    issue_id: NonEmptyText


class ReprioritizeCommand(_Command):
    intent: Literal["reprioritize"]
    issue_id: NonEmptyText
    priority: int = Field(ge=1, le=4)


class ApproveHandoffCommand(_Command):
    intent: Literal["approve_handoff"]
    handoff_id: NonEmptyText


class ApproveStallCommand(_Command):
    intent: Literal["approve_stall"]
    project_key: NonEmptyText


class RequestCheckpointCommand(_Command):
    intent: Literal["request_checkpoint"]
    project_key: NonEmptyText


class RequestCleanupCommand(_Command):
    intent: Literal["request_cleanup"]
    project_key: NonEmptyText


class QaRejectCommand(_Command):
    intent: Literal["qa_reject"]
    issue_id: NonEmptyText
    reason: NonEmptyText
    evidence: NonEmptyText


class ReportStallCommand(_Command):
    intent: Literal["report_stall"]
    project_key: NonEmptyText
    no_material_change_seconds: int = Field(default=0, ge=0)
    process_alive: bool = True
    output_activity: bool = True
    resource_activity: bool = True
    repeated_failure: NonEmptyText | None = None
    blocked_request: NonEmptyText | None = None
    provider_error: NonEmptyText | None = None
    external_failure: NonEmptyText | None = None
    agent_report: NonEmptyText | None = None
    resource_pressure: bool = False


class ApprovePlaybookCommand(_Command):
    intent: Literal["approve_playbook"]
    consultation_id: NonEmptyText
    actions: list[NonEmptyText] = Field(min_length=1)
    verification: NonEmptyText
    timeout_seconds: int = Field(gt=0)
    rollback: NonEmptyText
    approved: bool = True


class PendingConsultationsCommand(_Command):
    intent: Literal["pending_consultations"]
    project_key: NonEmptyText | None = None


class RecordRemedyResultCommand(_Command):
    intent: Literal["record_remedy_result"]
    project_key: NonEmptyText
    predicate_key: NonEmptyText
    reason: NonEmptyText
    mode: Literal["automatic", "consulted"]
    success: bool
    playbook_hash: NonEmptyText | None = None
    detail: str = ""


class PendingCorrectionsCommand(_Command):
    intent: Literal["pending_corrections"]
    project_key: NonEmptyText | None = None


class FetchCorrectionCommand(_Command):
    """The one supported intake of a correction's complete record."""

    intent: Literal["fetch_correction"]
    correction_id: NonEmptyText


class AckCorrectionCommand(_Command):
    """Confirmation as an assertion of what was actually read.

    ``observed_count`` and ``payload_sha256`` are required and strict: a
    missing or blank field cannot parse into this command, so a
    confirmation informed by a truncated glance at the payload never
    reaches the outbox at all.
    """

    intent: Literal["ack_correction"]
    correction_id: NonEmptyText
    observed_count: int = Field(ge=0)
    payload_sha256: NonEmptyText


class PendingWakesCommand(_Command):
    intent: Literal["pending_wakes"]
    project_key: NonEmptyText | None = None


class AckWakeCommand(_Command):
    intent: Literal["ack_wake"]
    wake_id: NonEmptyText


class CreatePacketCommand(_Command):
    intent: Literal["create_packet"]
    issue_id: NonEmptyText
    model_tier: NonEmptyText
    effort: NonEmptyText
    allowed_files: list[NonEmptyText] = Field(min_length=1)
    worktree: NonEmptyText
    depends_on: list[NonEmptyText] = Field(default_factory=list)
    red_test: NonEmptyText
    verification: list[NonEmptyText] = Field(min_length=1)
    invariants: NonEmptyText
    resource_note: NonEmptyText


class AcceptPacketCommand(_Command):
    intent: Literal["accept_packet"]
    packet_id: NonEmptyText
    evidence: dict[str, str]


class RejectPacketCommand(_Command):
    intent: Literal["reject_packet"]
    packet_id: NonEmptyText
    reason: NonEmptyText


class RecordDirectExceptionCommand(_Command):
    intent: Literal["record_direct_exception"]
    issue_id: NonEmptyText
    reason: NonEmptyText
    expected_files: list[NonEmptyText] = Field(min_length=1)
    expected_lines: int = Field(gt=0)
    verification: NonEmptyText
    worktree: str | None = None


class RequireAcceptanceCommand(_Command):
    intent: Literal["require_acceptance"]
    issue_id: NonEmptyText
    instruction_id: NonEmptyText
    predicates: list[NonEmptyText] = Field(min_length=1)


class SatisfyAcceptanceCommand(_Command):
    intent: Literal["satisfy_acceptance"]
    issue_id: NonEmptyText
    evidence: dict[NonEmptyText, NonEmptyText] = Field(min_length=1)


class ContinueWorkCommand(_Command):
    """Continue a bound seat's already-assigned issue, never assign one.

    ``request_id`` scopes idempotency to THIS request: retrying the same
    ``request_id`` for the same issue deduplicates through the wake
    store's existing turn-identity index, while a later, distinct
    ``request_id`` for the same issue is a new, legitimate continuation
    and may commit another wake.
    """

    intent: Literal["continue_work"]
    issue_id: NonEmptyText
    request_id: NonEmptyText


class ApplyOperatorDecisionCommand(_Command):
    """The only event shape that may resolve a pending decision.

    Every field is schema-enforced nonempty: a blank message, hook
    firing, or replayed transcript cannot parse into this command and
    therefore cannot mutate decision state. INFRA-224: an optional
    ``answer`` routes resolution through the global inbox (recording
    the operator's answer and waking the bound lane exactly once);
    when ``answer`` is absent this keeps the original
    ``OperatorDecisions.apply`` path unchanged.
    """

    intent: Literal["apply_operator_decision"]
    decision_id: NonEmptyText
    status: Literal["approved", "rejected"]
    source_message: NonEmptyText
    answer: NonEmptyText | None = None
    next_action: NonEmptyText | None = None


class DecisionOptionPayload(BaseModel):
    """One choice offered to the operator, with its tradeoffs."""

    model_config = ConfigDict(extra="forbid")

    label: NonEmptyText
    tradeoffs: NonEmptyText


class RaiseOperatorDecisionCommand(_Command):
    """Raise one authority-worthy request into the global operator inbox.

    INFRA-224: ``cell_id``, ``session_id``, and ``project_key`` are
    never caller-supplied -- they are resolved server-side from the
    issue's own active cell, so a request can never be bound to a
    foreign or stale lane.
    """

    intent: Literal["raise_operator_decision"]
    issue_id: NonEmptyText
    requesting_role: NonEmptyText
    question: NonEmptyText
    authority_reason: NonEmptyText
    facts: list[NonEmptyText] = Field(default_factory=list)
    options: list[DecisionOptionPayload] = Field(min_length=1)
    recommendation: NonEmptyText
    delay_impact: NonEmptyText
    paused_scope: NonEmptyText
    urgency: int = Field(default=2, ge=0, le=3)
    category: NonEmptyText = "authority"
    request_key: str | None = None


class PendingOperatorDecisionsCommand(_Command):
    intent: Literal["pending_operator_decisions"]
    project_key: NonEmptyText | None = None


class NextOperatorDecisionCommand(_Command):
    intent: Literal["next_operator_decision"]
    project_key: NonEmptyText | None = None


HermesCommand = Annotated[
    QueueIssueCommand
    | StatusCommand
    | PauseCommand
    | ResumeCommand
    | RetryCommand
    | ReprioritizeCommand
    | ApproveHandoffCommand
    | ApproveStallCommand
    | RequestCheckpointCommand
    | RequestCleanupCommand
    | QaRejectCommand
    | PendingCorrectionsCommand
    | FetchCorrectionCommand
    | AckCorrectionCommand
    | ReportStallCommand
    | ApprovePlaybookCommand
    | PendingConsultationsCommand
    | RecordRemedyResultCommand
    | PendingWakesCommand
    | AckWakeCommand
    | ApplyOperatorDecisionCommand
    | RaiseOperatorDecisionCommand
    | PendingOperatorDecisionsCommand
    | NextOperatorDecisionCommand
    | CreatePacketCommand
    | AcceptPacketCommand
    | RejectPacketCommand
    | RecordDirectExceptionCommand
    | RequireAcceptanceCommand
    | SatisfyAcceptanceCommand
    | ContinueWorkCommand,
    Field(discriminator="intent"),
]

_COMMAND_ADAPTER = TypeAdapter(HermesCommand)
# Intents execute() serves without a registered handler.
_INLINE_INTENTS = frozenset({"queue_issue", "status", "reprioritize"})
_ALLOWED_INTENTS = frozenset(
    {
        "queue_issue",
        "status",
        "pause",
        "resume",
        "retry",
        "reprioritize",
        "approve_handoff",
        "approve_stall",
        "request_checkpoint",
        "request_cleanup",
        "qa_reject",
        "pending_corrections",
        "fetch_correction",
        "ack_correction",
        "report_stall",
        "approve_playbook",
        "pending_consultations",
        "record_remedy_result",
        "pending_wakes",
        "ack_wake",
        "apply_operator_decision",
        "raise_operator_decision",
        "pending_operator_decisions",
        "next_operator_decision",
        "create_packet",
        "accept_packet",
        "reject_packet",
        "record_direct_exception",
        "require_acceptance",
        "satisfy_acceptance",
        "continue_work",
    }
)


@dataclass(frozen=True, slots=True)
class HermesCommandResult:
    """Sanitized result safe to return through chat or a phone console."""

    correlation_id: str
    code: str
    state: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "correlation_id": self.correlation_id,
            "code": self.code,
            "state": self.state,
        }


CommandHandler = Callable[[BaseModel], dict[str, Any]]


class HermesCommandService:
    """Admit only explicit work and route a fixed set of local intents."""

    def __init__(
        self,
        queue: QueueService,
        *,
        handlers: Mapping[str, CommandHandler] | None = None,
        correlation_ids: Callable[[], str] | None = None,
        qa: QaRouter | None = None,
        linear_reads: LinearReadPort | None = None,
    ) -> None:
        self._queue = queue
        self._qa = qa
        self._handlers = dict(handlers or {})
        self._correlation_ids = correlation_ids or (lambda: str(uuid.uuid4()))
        self._linear_reads = linear_reads

    def supports(self, intent: str) -> bool:
        """True when the intent reaches real code instead of failing.

        A remote surface must not prepare a confirmation for an intent
        this service would only answer with ``intent_unavailable``.
        """

        if intent not in _ALLOWED_INTENTS:
            return False
        return intent in _INLINE_INTENTS or intent in self._handlers

    def execute(self, raw: object) -> HermesCommandResult:
        """Validate and execute one strict JSON command."""

        correlation_id = self._correlation_ids()
        if not isinstance(raw, dict):
            return HermesCommandResult(correlation_id, "invalid_command", {})
        intent = raw.get("intent")
        if intent not in _ALLOWED_INTENTS:
            return HermesCommandResult(correlation_id, "intent_not_allowed", {})
        if intent == "queue_issue" and not raw.get("operator_instruction_id"):
            return HermesCommandResult(
                correlation_id,
                "operator_instruction_required",
                {},
            )
        try:
            command = _COMMAND_ADAPTER.validate_python(raw)
        except ValidationError:
            return HermesCommandResult(correlation_id, "invalid_command", {})

        if isinstance(command, QueueIssueCommand):
            return self._queue_issue(correlation_id, command)
        if isinstance(command, StatusCommand):
            issues = self._queue.list_ranked(datetime.now(UTC))
            return HermesCommandResult(
                correlation_id,
                "ok",
                {"queued_issue_count": len(issues)},
            )
        if isinstance(command, ReprioritizeCommand):
            try:
                issue = self._queue.reprioritize(command.issue_id, command.priority)
            except (AdmissionDenied, KeyError):
                return HermesCommandResult(correlation_id, "issue_not_found", {})
            return HermesCommandResult(
                correlation_id,
                "reprioritized",
                {"issue_id": issue.issue_id, "priority": issue.linear_priority},
            )

        handler = self._handlers.get(command.intent)
        if handler is None:
            return HermesCommandResult(correlation_id, "intent_unavailable", {})
        try:
            state = handler(command)
        except (ValueError, KeyError) as error:
            return HermesCommandResult(
                correlation_id, "rejected", {"reason": str(error)}
            )
        return HermesCommandResult(correlation_id, "accepted", state)

    def _queue_issue(
        self,
        correlation_id: str,
        command: QueueIssueCommand,
    ) -> HermesCommandResult:
        if command.qa_origin is not None and self._qa is None:
            return HermesCommandResult(correlation_id, "qa_routing_unavailable", {})
        request = AdmissionRequest(
            issue_id=command.issue_id,
            project_key=command.project_key,
            linear_priority=command.priority,
            admitted_by="operator",
            instruction_id=command.operator_instruction_id,
        )
        reactivated = False
        try:
            issue = self._queue.admit(request)
        except IdempotencyConflict:
            return HermesCommandResult(correlation_id, "admission_denied", {})
        except AdmissionDenied as error:
            if not str(error).endswith("is already admitted"):
                return HermesCommandResult(correlation_id, "admission_denied", {})
            # INFRA-227: the only admission_denied shape that might be a
            # reopened issue, not a genuine duplicate admission -- try
            # reactivation, which itself fails closed on every other
            # local or Linear-side condition.
            if self._linear_reads is None:
                return HermesCommandResult(
                    correlation_id,
                    "admission_denied",
                    {"reason": "reactivation requires a Linear read"},
                )
            try:
                linear_issue = self._linear_reads.get_issue(command.issue_id)
            except Exception:
                return HermesCommandResult(
                    correlation_id,
                    "admission_denied",
                    {"reason": "linear read failed"},
                )
            try:
                issue = self._queue.reactivate(
                    request, linear_status=linear_issue.status
                )
            except (AdmissionDenied, IdempotencyConflict) as reactivate_error:
                return HermesCommandResult(
                    correlation_id,
                    "admission_denied",
                    {"reason": str(reactivate_error)},
                )
            reactivated = True
        if command.qa_origin is not None and self._qa is not None:
            self._qa.record_origin(issue.issue_id, QaOrigin(command.qa_origin))
        state = {
            "issue_id": issue.issue_id,
            "project_key": issue.project_key,
            "priority": issue.linear_priority,
            "state": issue.state.value,
        }
        if self._qa is not None:
            state["qa_origin"] = self._qa.origin_of(issue.issue_id).kind
        if reactivated:
            state["reactivated"] = True
        return HermesCommandResult(correlation_id, "queued", state)
