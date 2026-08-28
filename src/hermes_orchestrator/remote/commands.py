"""Confirmed remote command execution with durable state (INFRA-175).

Every mutation the remote surface can perform passes through a two-step
confirmation: ``prepare`` authorizes the intent against the deny-by-default
policy, validates the target kind and the intent's closed parameter model,
and persists a pending record with a random confirmation id, an exact
uppercase phrase, and a 120-second expiry; ``confirm`` re-validates every
gate — session, state, expiry, phrase, policy, executor support — then
durably claims the pending row in its own committed transaction BEFORE
crossing the mutation boundary, executes exactly once through the strict
Hermes command service, and persists the sanitized result together with
the ``executed`` state. A claimed row without a stored result means the
outcome of a crossing is unknown; such confirmations fail closed forever
and the executor is never re-invoked without proof no mutation occurred.
Results persist under (idempotency key, session) bound to their
confirmation, so an honest retry of the same confirmation returns the
stored result verbatim while a key reused against a different
confirmation is rejected. All audit events append through the established
:class:`EventStore` path inside the same transaction as the state change,
and no error or event ever carries a caller-supplied value.
"""

from __future__ import annotations

import hmac
import json
import secrets
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Protocol

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
)

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.remote.policy import RemoteIntent, RemotePolicy
from hermes_orchestrator.remote.views import FORBIDDEN_VALUE_TERMS

CONFIRMATION_LIFETIME = timedelta(seconds=120)

# How each target kind names itself in a strict Hermes command payload.
_KIND_FIELDS: dict[str, str] = {
    "project": "project_key",
    "issue": "issue_id",
    "handoff": "handoff_id",
}

_NonEmptyText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


class _NoParameters(BaseModel):
    """The closed empty parameter set; extra operator fields are rejected."""

    model_config = ConfigDict(extra="forbid")


class QueueIssueParameters(_NoParameters):
    """Operator-controlled fields of a remote queue_issue; never synthesized."""

    issue_id: _NonEmptyText
    priority: int = Field(ge=1, le=4)
    operator_instruction_id: _NonEmptyText


class ReprioritizeParameters(_NoParameters):
    priority: int = Field(ge=1, le=4)


@dataclass(frozen=True, slots=True)
class _IntentSpec:
    """One remotely allowed mutation: its target kind, parameters, extras."""

    target_kind: str
    parameters_model: type[_NoParameters]
    extras: tuple[tuple[str, str], ...] = ()


# The closed table of preparable mutations. An allowlisted intent absent
# here (status is a read, not a mutation) can never be prepared, so no
# confirmation can exist that the strict executor would refuse as
# invalid_command. Extras are fixed values the strict schema requires; no
# caller-supplied value ever flows into them.
_INTENT_SPECS: dict[RemoteIntent, _IntentSpec] = {
    RemoteIntent.PAUSE: _IntentSpec(
        "project", _NoParameters, (("reason", "remote operator confirmation"),)
    ),
    RemoteIntent.RESUME: _IntentSpec("project", _NoParameters),
    RemoteIntent.RETRY: _IntentSpec("issue", _NoParameters),
    RemoteIntent.REPRIORITIZE: _IntentSpec("issue", ReprioritizeParameters),
    RemoteIntent.QUEUE_ISSUE: _IntentSpec("project", QueueIssueParameters),
    RemoteIntent.APPROVE_HANDOFF: _IntentSpec("handoff", _NoParameters),
    RemoteIntent.APPROVE_STALL: _IntentSpec("project", _NoParameters),
    RemoteIntent.REQUEST_CHECKPOINT: _IntentSpec("project", _NoParameters),
    RemoteIntent.REQUEST_CLEANUP: _IntentSpec("project", _NoParameters),
}


class RemoteCommandError(Exception):
    """Base for confirmation failures; messages are static reason codes."""


class UnknownTarget(RemoteCommandError):
    """The target is malformed or not present in the catalog."""


class UnknownConfirmation(RemoteCommandError):
    """No pending record is visible to this session under that id."""


class PhraseMismatch(RemoteCommandError):
    """The presented phrase is not the exact expected uppercase phrase."""


class ConfirmationExpired(RemoteCommandError):
    """The pending record's 120-second window has closed."""


class ConfirmationReplayed(RemoteCommandError):
    """The confirmation id was already consumed by a successful confirm."""


class IntentDenied(RemoteCommandError):
    """The deny-by-default policy refused the intent."""


class UnsupportedIntent(RemoteCommandError):
    """The intent has no strict command mapping or no wired handler."""


class IncompatibleTarget(RemoteCommandError):
    """The target kind is not the one the intent operates on."""


class InvalidParameters(RemoteCommandError):
    """The intent's closed parameter model rejected the request."""


class ExecutionUnresolved(RemoteCommandError):
    """A claimed execution has no stored result; the outcome is unknown."""


class IdempotencyKeyConflict(RemoteCommandError):
    """The key is bound to a different confirmation's stored result."""


@dataclass(frozen=True, slots=True)
class RemoteCommand:
    """One operator-requested mutation: intent, target, and parameters."""

    intent: RemoteIntent
    target: str
    parameters: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PendingConfirmation:
    """The prepared-command surface returned to the operator."""

    confirmation_id: str
    confirmation_phrase: str
    impact_summary: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class CommandResult:
    """A sanitized execution result safe to render and to store."""

    code: str
    correlation_id: str
    state: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _StoredResult:
    """A persisted result plus the confirmation identity it is bound to."""

    confirmation_id: str
    result: CommandResult


class TargetCatalog(Protocol):
    """Existence checks for operation targets; the wiring adapts stores."""

    def exists(self, kind: str, name: str) -> bool: ...


class ExecutedCommand(Protocol):
    """The result shape the strict Hermes command service returns."""

    @property
    def code(self) -> str: ...

    @property
    def correlation_id(self) -> str: ...

    @property
    def state(self) -> dict[str, Any]: ...


class CommandExecutor(Protocol):
    """The strict Hermes command boundary; never a shell."""

    def supports(self, intent: str) -> bool: ...

    def execute(self, raw: object) -> ExecutedCommand: ...


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _matches(value: str) -> bool:
    lowered = value.lower()
    return any(term in lowered for term in FORBIDDEN_VALUE_TERMS)


def _state_is_clean(state: dict[str, Any]) -> bool:
    """True when no nested string value carries a known secret field name."""

    stack: list[Any] = [state]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            stack.extend(current.keys())
            stack.extend(current.values())
        elif isinstance(current, list | tuple):
            stack.extend(current)
        elif isinstance(current, str) and _matches(current):
            return False
    return True


class ConfirmationService:
    """Prepare and confirm remote commands with fail-closed gates."""

    def __init__(
        self,
        *,
        database: Database,
        events: EventStore,
        policy: RemotePolicy,
        targets: TargetCatalog,
        executor: CommandExecutor,
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._database = database
        self._events = events
        self._policy = policy
        self._targets = targets
        self._executor = executor
        self._now = now

    def available(self, intent: RemoteIntent) -> bool:
        """True when the intent is specced AND its executor path is wired.

        Exactly the gate ``prepare`` applies after policy: a surface may use
        this to avoid rendering a control that could never be prepared.
        """

        return intent in _INTENT_SPECS and self._executor.supports(intent.value)

    def prepare(
        self, command: RemoteCommand, *, session_id: str
    ) -> PendingConfirmation:
        decision = self._policy.authorize(command.intent)
        if not decision.allowed:
            # The target is still unvalidated caller input here; the audit
            # payload records only the parseable intent member.
            with self._database.transaction() as connection:
                self._append_event(
                    connection,
                    "remote_command_denied",
                    confirmation_id="",
                    intent=command.intent,
                    target="",
                    reason="intent_not_allowlisted",
                )
            raise IntentDenied("intent_not_allowlisted")
        spec = _INTENT_SPECS.get(command.intent)
        if spec is None or not self._executor.supports(command.intent.value):
            raise UnsupportedIntent("unsupported_intent")
        kind, name = self._parse_target(command.target)
        if kind != spec.target_kind:
            raise IncompatibleTarget("incompatible_target")
        try:
            parameters = spec.parameters_model.model_validate(
                dict(command.parameters)
            )
        except ValidationError:
            raise InvalidParameters("invalid_parameters") from None
        confirmation_id = secrets.token_urlsafe(16)
        prepared_at = self._now()
        expires_at = prepared_at + CONFIRMATION_LIFETIME
        phrase = f"{command.intent.value.upper()} {name.upper()}"
        impact = f"{command.intent.value} will be applied to {kind} {name}"
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO remote_pending_commands("
                "confirmation_id, session_id, intent, target, "
                "confirmation_phrase, impact_summary, prepared_at, expires_at, "
                "state, parameters_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)",
                (
                    confirmation_id,
                    session_id,
                    command.intent.value,
                    command.target,
                    phrase,
                    impact,
                    prepared_at.isoformat(),
                    expires_at.isoformat(),
                    json.dumps(parameters.model_dump(), sort_keys=True),
                ),
            )
            self._append_event(
                connection,
                "remote_command_prepared",
                confirmation_id=confirmation_id,
                intent=command.intent,
                target=command.target,
            )
        return PendingConfirmation(
            confirmation_id=confirmation_id,
            confirmation_phrase=phrase,
            impact_summary=impact,
            expires_at=expires_at,
        )

    def confirm(
        self,
        confirmation_id: str,
        phrase: str,
        idempotency_key: str,
        *,
        session_id: str,
    ) -> CommandResult:
        stored = self._stored_result(idempotency_key, session_id)
        if stored is not None:
            if not hmac.compare_digest(
                stored.confirmation_id.encode(), confirmation_id.encode()
            ):
                raise IdempotencyKeyConflict("idempotency_key_conflict")
            return stored.result
        row = self._database.execute(
            "SELECT confirmation_id, session_id, intent, target, "
            "confirmation_phrase, expires_at, state, parameters_json "
            "FROM remote_pending_commands WHERE confirmation_id = ?",
            (confirmation_id,),
        ).fetchone()
        if row is None:
            raise UnknownConfirmation("unknown_confirmation")
        if not hmac.compare_digest(
            str(row["session_id"]).encode(), session_id.encode()
        ):
            # Existence is never revealed across sessions.
            raise UnknownConfirmation("unknown_confirmation")
        try:
            intent = RemoteIntent(str(row["intent"]))
        except ValueError:
            # A stored intent outside the closed enum is corrupt state;
            # nothing with it may ever execute.
            raise IntentDenied("intent_not_allowlisted") from None
        target = str(row["target"])
        state = str(row["state"])
        if state == "executed":
            self._reject(row, "replayed")
            raise ConfirmationReplayed("confirmation_replayed")
        if state == "expired":
            raise ConfirmationExpired("confirmation_expired")
        if state == "claimed":
            # The mutation boundary was crossed but no result was stored:
            # the outcome is unknown, so this confirmation fails closed
            # forever rather than risking a second execution.
            self._unresolved(row)
        if self._now() >= datetime.fromisoformat(str(row["expires_at"])):
            with self._database.transaction() as connection:
                connection.execute(
                    "UPDATE remote_pending_commands SET state = 'expired' "
                    "WHERE confirmation_id = ?",
                    (confirmation_id,),
                )
                self._append_event(
                    connection,
                    "remote_command_rejected",
                    confirmation_id=confirmation_id,
                    intent=intent,
                    target=target,
                    reason="expired",
                )
            raise ConfirmationExpired("confirmation_expired")
        if not hmac.compare_digest(
            str(row["confirmation_phrase"]).encode(), phrase.encode()
        ):
            self._reject(row, "phrase_mismatch")
            raise PhraseMismatch("phrase_mismatch")
        decision = self._policy.authorize(intent)
        if not decision.allowed:
            self._reject(row, "intent_not_allowlisted")
            raise IntentDenied("intent_not_allowlisted")
        spec = _INTENT_SPECS.get(intent)
        if spec is None or not self._executor.supports(intent.value):
            self._reject(row, "unsupported_intent")
            raise UnsupportedIntent("unsupported_intent")
        payload = self._payload(intent, target, str(row["parameters_json"]))
        # The durable claim commits in its own transaction BEFORE the
        # executor runs: a crash on either side of the mutation boundary
        # leaves a claimed row, never a re-executable pending one. The
        # unique (session_id, idempotency_key) index makes this UPDATE the
        # atomic reservation of the key: a competing claim by a DIFFERENT
        # confirmation in the same session fails here, before any mutation.
        try:
            with self._database.transaction() as connection:
                claimed = connection.execute(
                    "UPDATE remote_pending_commands "
                    "SET state = 'claimed', claimed_at = ?, idempotency_key = ? "
                    "WHERE confirmation_id = ? AND state = 'pending'",
                    (self._now().isoformat(), idempotency_key, confirmation_id),
                )
                won_claim = claimed.rowcount == 1
                if won_claim:
                    self._append_event(
                        connection,
                        "remote_command_claimed",
                        confirmation_id=confirmation_id,
                        intent=intent,
                        target=target,
                    )
        except sqlite3.IntegrityError:
            # The reservation is held by another row; the transaction rolled
            # back, so no claimed event was appended and nothing executes.
            return self._lost_reservation(row, idempotency_key, session_id)
        if not won_claim:
            # A concurrent confirm won the compare-and-swap between our
            # read and this write. Return its stored result if it has
            # already persisted one; otherwise the execution is in flight
            # or its outcome is unknown — fail closed.
            settled = self._stored_result(idempotency_key, session_id)
            if settled is not None and hmac.compare_digest(
                settled.confirmation_id.encode(), confirmation_id.encode()
            ):
                return settled.result
            self._unresolved(row)
        executed = self._executor.execute(payload)
        redacted = not _state_is_clean(dict(executed.state))
        result = CommandResult(
            code="result_redacted" if redacted else executed.code,
            correlation_id=executed.correlation_id,
            state={} if redacted else dict(executed.state),
        )
        with self._database.transaction() as connection:
            if redacted:
                self._events.append(
                    connection,
                    EventInput(
                        event_type="redaction_failure",
                        aggregate_type="remote_command",
                        aggregate_id=confirmation_id,
                        payload={
                            "confirmation_id": confirmation_id,
                            "field": "result_state",
                        },
                        actor="remote",
                    ),
                )
            connection.execute(
                "UPDATE remote_pending_commands SET state = 'executed' "
                "WHERE confirmation_id = ? AND state = 'claimed'",
                (confirmation_id,),
            )
            connection.execute(
                "INSERT INTO remote_command_results("
                "idempotency_key, session_id, confirmation_id, code, "
                "correlation_id, state_json, executed_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    idempotency_key,
                    session_id,
                    confirmation_id,
                    result.code,
                    result.correlation_id,
                    json.dumps(result.state, sort_keys=True),
                    self._now().isoformat(),
                ),
            )
            self._append_event(
                connection,
                "remote_command_executed",
                confirmation_id=confirmation_id,
                intent=intent,
                target=target,
                reason=result.code,
            )
        return result

    def _parse_target(self, target: str) -> tuple[str, str]:
        kind, separator, name = target.partition(":")
        if not separator or not kind or not name:
            raise UnknownTarget("unknown_target")
        if kind not in _KIND_FIELDS:
            raise UnknownTarget("unknown_target")
        if not self._targets.exists(kind, name):
            raise UnknownTarget("unknown_target")
        return kind, name

    def _payload(
        self, intent: RemoteIntent, target: str, parameters_json: str
    ) -> dict[str, Any]:
        spec = _INTENT_SPECS[intent]
        kind, name = self._parse_target(target)
        try:
            parameters = spec.parameters_model.model_validate(
                json.loads(parameters_json)
            )
        except (ValidationError, ValueError):
            # Stored parameters that no longer validate are corrupt state;
            # nothing built from them may ever execute.
            raise InvalidParameters("invalid_parameters") from None
        payload: dict[str, Any] = {
            "intent": intent.value,
            _KIND_FIELDS[kind]: name,
        }
        payload.update(parameters.model_dump())
        payload.update(dict(spec.extras))
        return payload

    def _stored_result(
        self, idempotency_key: str, session_id: str
    ) -> _StoredResult | None:
        row = self._database.execute(
            "SELECT confirmation_id, code, correlation_id, state_json "
            "FROM remote_command_results "
            "WHERE idempotency_key = ? AND session_id = ?",
            (idempotency_key, session_id),
        ).fetchone()
        if row is None:
            return None
        return _StoredResult(
            confirmation_id=str(row["confirmation_id"]),
            result=CommandResult(
                code=str(row["code"]),
                correlation_id=str(row["correlation_id"]),
                state=json.loads(str(row["state_json"])),
            ),
        )

    def _lost_reservation(
        self, row: sqlite3.Row, idempotency_key: str, session_id: str
    ) -> CommandResult:
        """Resolve a claim that lost the (session, key) reservation.

        The key is bound to exactly one confirmation and the loser never
        executes: a different bound confirmation is a conflict; the same
        confirmation (retry interleavings) resolves to its settled result,
        or fails closed while its execution is unsettled.
        """

        holder = self._database.scalar(
            "SELECT confirmation_id FROM remote_pending_commands "
            "WHERE session_id = ? AND idempotency_key = ?",
            (session_id, idempotency_key),
        )
        confirmation_id = str(row["confirmation_id"])
        if holder is None or not hmac.compare_digest(
            str(holder).encode(), confirmation_id.encode()
        ):
            raise IdempotencyKeyConflict("idempotency_key_conflict")
        settled = self._stored_result(idempotency_key, session_id)
        if settled is not None and hmac.compare_digest(
            settled.confirmation_id.encode(), confirmation_id.encode()
        ):
            return settled.result
        self._unresolved(row)
        raise ExecutionUnresolved("execution_unresolved")  # pragma: no cover

    def _unresolved(self, row: sqlite3.Row) -> None:
        """Audit and refuse a claimed execution with an unknown outcome.

        The claim is never rolled back to pending (that would silently
        re-authorize execution) and never advanced (there is no proof any
        mutation occurred); resolution requires operator investigation.
        """

        with self._database.transaction() as connection:
            self._append_event(
                connection,
                "remote_command_unresolved",
                confirmation_id=str(row["confirmation_id"]),
                intent=RemoteIntent(str(row["intent"])),
                target=str(row["target"]),
                reason="execution_unresolved",
            )
        raise ExecutionUnresolved("execution_unresolved")

    def _reject(self, row: sqlite3.Row, reason: str) -> None:
        with self._database.transaction() as connection:
            self._append_event(
                connection,
                "remote_command_rejected",
                confirmation_id=str(row["confirmation_id"]),
                intent=RemoteIntent(str(row["intent"])),
                target=str(row["target"]),
                reason=reason,
            )

    def _append_event(
        self,
        connection: sqlite3.Connection,
        event_type: str,
        *,
        confirmation_id: str,
        intent: RemoteIntent,
        target: str,
        reason: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "confirmation_id": confirmation_id,
            "intent": intent.value,
            "target": target,
        }
        if reason is not None:
            payload["reason"] = reason
        self._events.append(
            connection,
            EventInput(
                event_type=event_type,
                aggregate_type="remote_command",
                aggregate_id=confirmation_id or intent.value,
                payload=payload,
                actor="remote",
            ),
        )
