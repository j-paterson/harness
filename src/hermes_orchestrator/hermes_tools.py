"""Strict, identity-free command boundary exposed to Hermes."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any, Literal

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


class QaRejectCommand(_Command):
    intent: Literal["qa_reject"]
    issue_id: NonEmptyText
    reason: NonEmptyText
    evidence: NonEmptyText


class PendingCorrectionsCommand(_Command):
    intent: Literal["pending_corrections"]
    project_key: NonEmptyText | None = None


class AckCorrectionCommand(_Command):
    intent: Literal["ack_correction"]
    correction_id: NonEmptyText


HermesCommand = Annotated[
    QueueIssueCommand
    | StatusCommand
    | PauseCommand
    | ResumeCommand
    | RetryCommand
    | ReprioritizeCommand
    | ApproveHandoffCommand
    | QaRejectCommand
    | PendingCorrectionsCommand
    | AckCorrectionCommand,
    Field(discriminator="intent"),
]

_COMMAND_ADAPTER = TypeAdapter(HermesCommand)
_ALLOWED_INTENTS = frozenset(
    {
        "queue_issue",
        "status",
        "pause",
        "resume",
        "retry",
        "reprioritize",
        "approve_handoff",
        "qa_reject",
        "pending_corrections",
        "ack_correction",
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
    ) -> None:
        self._queue = queue
        self._qa = qa
        self._handlers = dict(handlers or {})
        self._correlation_ids = correlation_ids or (lambda: str(uuid.uuid4()))

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
        try:
            issue = self._queue.admit(
                AdmissionRequest(
                    issue_id=command.issue_id,
                    project_key=command.project_key,
                    linear_priority=command.priority,
                    admitted_by="operator",
                    instruction_id=command.operator_instruction_id,
                )
            )
        except (AdmissionDenied, IdempotencyConflict):
            return HermesCommandResult(correlation_id, "admission_denied", {})
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
        return HermesCommandResult(correlation_id, "queued", state)
