"""ConfirmationService state machine, durability, and audit tests (INFRA-175).

Everything runs against a temp sqlite database, an injected clock, and a
recording fake executor — no real processes, sockets, or Keychain.
"""

from datetime import timedelta
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.hermes_tools import (
    ApproveHandoffCommand,
    ApproveStallCommand,
    HermesCommandResult,
    HermesCommandService,
    PauseCommand,
    RequestCheckpointCommand,
    RequestCleanupCommand,
    ResumeCommand,
    RetryCommand,
)
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.remote.commands import (
    CONFIRMATION_LIFETIME,
    ConfirmationExpired,
    ConfirmationReplayed,
    ConfirmationService,
    ExecutionUnresolved,
    IdempotencyKeyConflict,
    IncompatibleTarget,
    IntentDenied,
    InvalidParameters,
    PhraseMismatch,
    RemoteCommand,
    UnknownConfirmation,
    UnknownTarget,
    UnsupportedIntent,
)
from hermes_orchestrator.remote.policy import RemoteIntent, RemotePolicy
from tests.remote.test_auth import FakeClock


class FakeTargets:
    """Closed catalog of known operation targets."""

    def __init__(self) -> None:
        self.known = {
            ("project", "demo"),
            ("issue", "infra-1"),
            ("handoff", "h-1"),
        }

    def exists(self, kind: str, name: str) -> bool:
        return (kind, name) in self.known


class FakeExecutor:
    """Records every payload and returns a distinct sanitized result."""

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.state: dict = {"outcome": "applied"}
        self.unsupported: set[str] = set()

    def supports(self, intent: str) -> bool:
        return intent not in self.unsupported

    def execute(self, raw: object) -> HermesCommandResult:
        self.calls.append(dict(raw))
        return HermesCommandResult(
            f"corr-{len(self.calls)}", "accepted", dict(self.state)
        )


class SimulatedCrash(RuntimeError):
    """Stands in for a process death at the mutation boundary."""


class CrashingExecutor(FakeExecutor):
    """Applies (records) its effect and then dies before returning."""

    def execute(self, raw: object) -> HermesCommandResult:
        self.calls.append(dict(raw))
        raise SimulatedCrash("process died after the mutation was applied")


class CrashingBeforeEffectExecutor(FakeExecutor):
    """Dies before any effect is applied — crash-after-claim injection."""

    def execute(self, raw: object) -> HermesCommandResult:
        raise SimulatedCrash("process died before the executor ran")


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def database(tmp_path: Path):
    opened = Database.open(tmp_path / "state.db")
    yield opened
    opened.close()


def make_service(
    database: Database,
    clock: FakeClock,
    *,
    executor: FakeExecutor | None = None,
    targets: FakeTargets | None = None,
) -> tuple[ConfirmationService, FakeExecutor]:
    executor = executor if executor is not None else FakeExecutor()
    service = ConfirmationService(
        database=database,
        events=EventStore(database),
        policy=RemotePolicy(),
        targets=targets if targets is not None else FakeTargets(),
        executor=executor,
        now=clock.now,
    )
    return service, executor


def pause_command() -> RemoteCommand:
    return RemoteCommand(intent=RemoteIntent.PAUSE, target="project:demo")


# --- prepare ----------------------------------------------------------------


def test_prepare_returns_phrase_impact_and_expiry(database, clock) -> None:
    service, _ = make_service(database, clock)

    pending = service.prepare(pause_command(), session_id="session-a")

    assert pending.confirmation_phrase == "PAUSE DEMO"
    assert pending.impact_summary
    assert pending.expires_at == clock.now() + CONFIRMATION_LIFETIME
    assert timedelta(seconds=120) == CONFIRMATION_LIFETIME


def test_prepare_ids_are_random_and_unique(database, clock) -> None:
    service, _ = make_service(database, clock)

    first = service.prepare(pause_command(), session_id="session-a")
    second = service.prepare(pause_command(), session_id="session-a")

    assert first.confirmation_id != second.confirmation_id
    assert len(first.confirmation_id) >= 16


@pytest.mark.parametrize("target", ["project:ghost", "demo", "spaceship:x", ""])
def test_prepare_rejects_unknown_or_malformed_targets(
    database, clock, target: str
) -> None:
    service, executor = make_service(database, clock)

    with pytest.raises(UnknownTarget):
        service.prepare(
            RemoteCommand(intent=RemoteIntent.PAUSE, target=target),
            session_id="session-a",
        )

    assert executor.calls == []
    count = database.scalar("SELECT count(*) FROM remote_pending_commands")
    assert count == 0


def test_prepare_denies_forbidden_intent_without_persisting(database, clock) -> None:
    service, executor = make_service(database, clock)

    with pytest.raises(IntentDenied):
        service.prepare(
            RemoteCommand(intent=RemoteIntent.SHELL, target="project:demo"),
            session_id="session-a",
        )

    assert executor.calls == []
    assert database.scalar("SELECT count(*) FROM remote_pending_commands") == 0
    events = EventStore(database).list_after(0)
    assert [e.event_type for e in events] == ["remote_command_denied"]


def test_prepare_persists_pending_across_restart(tmp_path: Path, clock) -> None:
    path = tmp_path / "state.db"
    first_db = Database.open(path)
    service, _ = make_service(first_db, clock)
    pending = service.prepare(pause_command(), session_id="session-a")
    first_db.close()

    reopened = Database.open(path)
    try:
        service, executor = make_service(reopened, clock)
        result = service.confirm(
            pending.confirmation_id,
            "PAUSE DEMO",
            "key-restart",
            session_id="session-a",
        )
        assert result.code == "accepted"
        assert len(executor.calls) == 1
    finally:
        reopened.close()


# --- confirm gates ----------------------------------------------------------


def test_confirm_executes_via_the_strict_command_payload(database, clock) -> None:
    service, executor = make_service(database, clock)
    pending = service.prepare(pause_command(), session_id="session-a")

    result = service.confirm(
        pending.confirmation_id, "PAUSE DEMO", "key-1", session_id="session-a"
    )

    assert result.code == "accepted"
    assert result.correlation_id == "corr-1"
    assert executor.calls == [
        {
            "intent": "pause",
            "project_key": "demo",
            "reason": "remote operator confirmation",
        }
    ]


def test_confirm_maps_issue_targets_to_issue_commands(database, clock) -> None:
    service, executor = make_service(database, clock)
    pending = service.prepare(
        RemoteCommand(intent=RemoteIntent.RETRY, target="issue:infra-1"),
        session_id="session-a",
    )

    service.confirm(
        pending.confirmation_id, "RETRY INFRA-1", "key-1", session_id="session-a"
    )

    assert executor.calls == [{"intent": "retry", "issue_id": "infra-1"}]


@pytest.mark.parametrize("phrase", ["pause demo", "PAUSE  DEMO", "PAUSE", ""])
def test_confirm_requires_the_exact_uppercase_phrase(
    database, clock, phrase: str
) -> None:
    service, executor = make_service(database, clock)
    pending = service.prepare(pause_command(), session_id="session-a")

    with pytest.raises(PhraseMismatch):
        service.confirm(
            pending.confirmation_id, phrase, "key-1", session_id="session-a"
        )

    assert executor.calls == []


def test_wrong_phrase_leaves_the_pending_record_confirmable(database, clock) -> None:
    service, executor = make_service(database, clock)
    pending = service.prepare(pause_command(), session_id="session-a")

    with pytest.raises(PhraseMismatch):
        service.confirm(
            pending.confirmation_id, "wrong", "key-1", session_id="session-a"
        )
    result = service.confirm(
        pending.confirmation_id, "PAUSE DEMO", "key-2", session_id="session-a"
    )

    assert result.code == "accepted"
    assert len(executor.calls) == 1


def test_confirm_rejects_at_the_120_second_boundary(database, clock) -> None:
    service, executor = make_service(database, clock)
    pending = service.prepare(pause_command(), session_id="session-a")

    clock.advance(seconds=120)
    with pytest.raises(ConfirmationExpired):
        service.confirm(
            pending.confirmation_id, "PAUSE DEMO", "key-1", session_id="session-a"
        )

    assert executor.calls == []


def test_confirm_succeeds_just_before_expiry(database, clock) -> None:
    service, executor = make_service(database, clock)
    pending = service.prepare(pause_command(), session_id="session-a")

    clock.advance(seconds=119)
    result = service.confirm(
        pending.confirmation_id, "PAUSE DEMO", "key-1", session_id="session-a"
    )

    assert result.code == "accepted"
    assert len(executor.calls) == 1


def test_confirm_unknown_id_rejects(database, clock) -> None:
    service, executor = make_service(database, clock)

    with pytest.raises(UnknownConfirmation):
        service.confirm("missing", "PAUSE DEMO", "key-1", session_id="session-a")

    assert executor.calls == []


def test_confirm_from_another_session_reads_as_unknown(database, clock) -> None:
    service, executor = make_service(database, clock)
    pending = service.prepare(pause_command(), session_id="session-a")

    with pytest.raises(UnknownConfirmation):
        service.confirm(
            pending.confirmation_id, "PAUSE DEMO", "key-1", session_id="session-b"
        )

    assert executor.calls == []


def test_replayed_confirmation_id_is_rejected_after_success(database, clock) -> None:
    service, executor = make_service(database, clock)
    pending = service.prepare(pause_command(), session_id="session-a")
    service.confirm(
        pending.confirmation_id, "PAUSE DEMO", "key-1", session_id="session-a"
    )

    with pytest.raises(ConfirmationReplayed):
        service.confirm(
            pending.confirmation_id, "PAUSE DEMO", "key-2", session_id="session-a"
        )

    assert len(executor.calls) == 1


# --- idempotency ------------------------------------------------------------


def test_retried_confirmation_with_same_key_executes_exactly_once(
    database, clock
) -> None:
    service, executor = make_service(database, clock)
    pending = service.prepare(pause_command(), session_id="session-a")
    first = service.confirm(
        pending.confirmation_id, "PAUSE DEMO", "same-key", session_id="session-a"
    )
    second = service.confirm(
        pending.confirmation_id, "PAUSE DEMO", "same-key", session_id="session-a"
    )

    assert first == second
    assert len(executor.calls) == 1


def test_key_reuse_against_a_different_confirmation_is_rejected(
    database, clock
) -> None:
    """Packet 1d: a key never aliases an unrelated stored result."""

    service, executor = make_service(database, clock)
    first_pending = service.prepare(pause_command(), session_id="session-a")
    first = service.confirm(
        first_pending.confirmation_id, "PAUSE DEMO", "same-key", session_id="session-a"
    )
    second_pending = service.prepare(pause_command(), session_id="session-a")

    with pytest.raises(IdempotencyKeyConflict):
        service.confirm(
            second_pending.confirmation_id,
            "PAUSE DEMO",
            "same-key",
            session_id="session-a",
        )

    assert len(executor.calls) == 1
    assert first.code == "accepted"


def test_idempotent_result_survives_restart_without_reexecution(
    tmp_path: Path, clock
) -> None:
    path = tmp_path / "state.db"
    first_db = Database.open(path)
    service, _ = make_service(first_db, clock)
    pending = service.prepare(pause_command(), session_id="session-a")
    first = service.confirm(
        pending.confirmation_id, "PAUSE DEMO", "same-key", session_id="session-a"
    )
    first_db.close()

    reopened = Database.open(path)
    try:
        service, fresh_executor = make_service(reopened, clock)
        second = service.confirm(
            pending.confirmation_id,
            "PAUSE DEMO",
            "same-key",
            session_id="session-a",
        )
        assert second == first
        assert fresh_executor.calls == []
    finally:
        reopened.close()


def test_idempotency_keys_are_scoped_to_the_session(database, clock) -> None:
    service, executor = make_service(database, clock)
    pending = service.prepare(pause_command(), session_id="session-a")
    service.confirm(
        pending.confirmation_id, "PAUSE DEMO", "same-key", session_id="session-a"
    )
    other = service.prepare(pause_command(), session_id="session-b")

    service.confirm(
        other.confirmation_id, "PAUSE DEMO", "same-key", session_id="session-b"
    )

    assert len(executor.calls) == 2


# --- policy is re-checked at confirm ----------------------------------------


def test_an_injected_forbidden_pending_row_never_reaches_the_executor(
    database, clock
) -> None:
    service, executor = make_service(database, clock)
    now = clock.now()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO remote_pending_commands("
            "confirmation_id, session_id, intent, target, confirmation_phrase, "
            "impact_summary, prepared_at, expires_at, state"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')",
            (
                "injected",
                "session-a",
                "shell",
                "project:demo",
                "SHELL DEMO",
                "injected row",
                now.isoformat(),
                (now + CONFIRMATION_LIFETIME).isoformat(),
            ),
        )

    with pytest.raises(IntentDenied):
        service.confirm("injected", "SHELL DEMO", "key-1", session_id="session-a")

    assert executor.calls == []


# --- result screening -------------------------------------------------------


def test_result_values_matching_forbidden_terms_are_redacted(database, clock) -> None:
    executor = FakeExecutor()
    executor.state = {"note": "api_key=abc123"}
    service, _ = make_service(database, clock, executor=executor)
    pending = service.prepare(pause_command(), session_id="session-a")

    result = service.confirm(
        pending.confirmation_id, "PAUSE DEMO", "key-1", session_id="session-a"
    )

    assert result.code == "result_redacted"
    assert result.state == {}
    events = EventStore(database).list_after(0)
    assert "redaction_failure" in [e.event_type for e in events]
    assert all("abc123" not in str(e.payload) for e in events)


# --- audit trail ------------------------------------------------------------


def test_prepare_and_confirm_append_durable_audit_events(database, clock) -> None:
    service, _ = make_service(database, clock)
    pending = service.prepare(pause_command(), session_id="session-a")
    service.confirm(
        pending.confirmation_id, "PAUSE DEMO", "key-1", session_id="session-a"
    )

    events = EventStore(database).list_after(0)
    types = [e.event_type for e in events]
    assert types == [
        "remote_command_prepared",
        "remote_command_claimed",
        "remote_command_executed",
    ]
    for event in events:
        assert event.aggregate_type == "remote_command"
        assert event.actor == "remote"
        assert event.payload["confirmation_id"] == pending.confirmation_id
        assert event.payload["intent"] == "pause"
        assert event.payload["target"] == "project:demo"


def test_rejections_append_a_reasoned_audit_event(database, clock) -> None:
    service, _ = make_service(database, clock)
    pending = service.prepare(pause_command(), session_id="session-a")

    with pytest.raises(PhraseMismatch):
        service.confirm(
            pending.confirmation_id, "wrong", "key-1", session_id="session-a"
        )

    events = EventStore(database).list_after(0)
    rejected = [e for e in events if e.event_type == "remote_command_rejected"]
    assert len(rejected) == 1
    assert rejected[0].payload["reason"] == "phrase_mismatch"
    assert "wrong" not in str(rejected[0].payload.values())


# --- packet 1: exactly-once execution fence ---------------------------------


class ReentrantExecutor(FakeExecutor):
    """Forces the concurrent-confirm interleaving on one thread.

    While the first confirm is between its durable claim and its result
    persistence, this executor issues the second confirm reentrantly —
    exactly the window in which two OS-level requests could race.
    """

    def __init__(self) -> None:
        super().__init__()
        self.reentry: tuple[ConfirmationService, str, str, str, str] | None = None
        self.inner_error: Exception | None = None
        self.inner_result: object | None = None

    def execute(self, raw: object) -> HermesCommandResult:
        result = super().execute(raw)
        if self.reentry is not None:
            service, confirmation_id, phrase, key, session_id = self.reentry
            self.reentry = None
            try:
                self.inner_result = service.confirm(
                    confirmation_id, phrase, key, session_id=session_id
                )
            except Exception as error:  # recorded for the test's asserts
                self.inner_error = error
        return result


def pending_state(database: Database, confirmation_id: str) -> str:
    return str(
        database.scalar(
            "SELECT state FROM remote_pending_commands WHERE confirmation_id = ?",
            (confirmation_id,),
        )
    )


def test_concurrent_confirms_execute_exactly_once(database, clock) -> None:
    executor = ReentrantExecutor()
    service, _ = make_service(database, clock, executor=executor)
    pending = service.prepare(pause_command(), session_id="session-a")
    executor.reentry = (
        service,
        pending.confirmation_id,
        "PAUSE DEMO",
        "race-key",
        "session-a",
    )

    outer = service.confirm(
        pending.confirmation_id, "PAUSE DEMO", "race-key", session_id="session-a"
    )

    assert len(executor.calls) == 1
    assert outer.code == "accepted"
    assert isinstance(executor.inner_error, ExecutionUnresolved)
    retried = service.confirm(
        pending.confirmation_id, "PAUSE DEMO", "race-key", session_id="session-a"
    )
    assert retried == outer
    assert len(executor.calls) == 1


def test_crash_after_effect_before_persist_never_reexecutes(
    tmp_path: Path, clock
) -> None:
    path = tmp_path / "state.db"
    first_db = Database.open(path)
    crashing = CrashingExecutor()
    service, _ = make_service(first_db, clock, executor=crashing)
    pending = service.prepare(pause_command(), session_id="session-a")
    with pytest.raises(SimulatedCrash):
        service.confirm(
            pending.confirmation_id, "PAUSE DEMO", "key-1", session_id="session-a"
        )
    assert len(crashing.calls) == 1
    first_db.close()

    reopened = Database.open(path)
    try:
        service, fresh_executor = make_service(reopened, clock)
        with pytest.raises(ExecutionUnresolved):
            service.confirm(
                pending.confirmation_id,
                "PAUSE DEMO",
                "key-1",
                session_id="session-a",
            )
        with pytest.raises(ExecutionUnresolved):
            service.confirm(
                pending.confirmation_id,
                "PAUSE DEMO",
                "key-2",
                session_id="session-a",
            )
        assert fresh_executor.calls == []
        assert pending_state(reopened, pending.confirmation_id) == "claimed"
    finally:
        reopened.close()


def test_crash_after_claim_before_execute_recovers_explicitly(
    tmp_path: Path, clock
) -> None:
    """The claim survives, is auditable, and never silently re-authorizes."""

    path = tmp_path / "state.db"
    first_db = Database.open(path)
    crashing = CrashingBeforeEffectExecutor()
    service, _ = make_service(first_db, clock, executor=crashing)
    pending = service.prepare(pause_command(), session_id="session-a")
    with pytest.raises(SimulatedCrash):
        service.confirm(
            pending.confirmation_id, "PAUSE DEMO", "key-1", session_id="session-a"
        )
    assert pending_state(first_db, pending.confirmation_id) == "claimed"
    first_db.close()

    reopened = Database.open(path)
    try:
        service, fresh_executor = make_service(reopened, clock)
        with pytest.raises(ExecutionUnresolved):
            service.confirm(
                pending.confirmation_id,
                "PAUSE DEMO",
                "key-1",
                session_id="session-a",
            )
        # The explicit safe transition: the claim is preserved (no silent
        # loss of the authorization record), never rolled back to pending
        # (no silent re-authorization), and the unresolved outcome is
        # durably audited.
        assert fresh_executor.calls == []
        assert pending_state(reopened, pending.confirmation_id) == "claimed"
        events = EventStore(reopened).list_after(0)
        assert "remote_command_unresolved" in [e.event_type for e in events]
    finally:
        reopened.close()


def test_claim_is_durable_before_the_executor_runs(database, clock) -> None:
    """The CAS commits in its own transaction before the mutation boundary."""

    class StateObservingExecutor(FakeExecutor):
        def __init__(self, observed_db: Database) -> None:
            super().__init__()
            self.observed_db = observed_db
            self.state_at_execute: str | None = None
            self.confirmation_id = ""

        def execute(self, raw: object) -> HermesCommandResult:
            self.state_at_execute = pending_state(
                self.observed_db, self.confirmation_id
            )
            return super().execute(raw)

    executor = StateObservingExecutor(database)
    service, _ = make_service(database, clock, executor=executor)
    pending = service.prepare(pause_command(), session_id="session-a")
    executor.confirmation_id = pending.confirmation_id

    service.confirm(
        pending.confirmation_id, "PAUSE DEMO", "key-1", session_id="session-a"
    )

    assert executor.state_at_execute == "claimed"
    assert pending_state(database, pending.confirmation_id) == "executed"


# --- packet 2: closed intent-specific commands ------------------------------


class RecordingHandlers:
    """Captures the exact validated strict command each handler receives."""

    def __init__(self) -> None:
        self.received: list[object] = []

    def make(self, intents: list[str]) -> dict[str, object]:
        def handler(command: object) -> dict[str, object]:
            self.received.append(command)
            return {"handled": type(command).__name__}

        return {intent: handler for intent in intents}


def make_e2e_service(
    database: Database, clock: FakeClock
) -> tuple[ConfirmationService, QueueService, RecordingHandlers]:
    """ConfirmationService over the REAL strict HermesCommandService."""

    queue = QueueService(database, EventStore(database), {"demo"})
    recorder = RecordingHandlers()
    executor = HermesCommandService(
        queue,
        handlers=recorder.make(
            [
                "pause",
                "resume",
                "retry",
                "approve_handoff",
                "approve_stall",
                "request_checkpoint",
                "request_cleanup",
            ]
        ),
    )
    service = ConfirmationService(
        database=database,
        events=EventStore(database),
        policy=RemotePolicy(),
        targets=FakeTargets(),
        executor=executor,
        now=clock.now,
    )
    return service, queue, recorder


def confirm_prepared(
    service: ConfirmationService, pending, key: str = "e2e-key"
) -> object:
    return service.confirm(
        pending.confirmation_id,
        pending.confirmation_phrase,
        key,
        session_id="session-a",
    )


def test_queue_issue_end_to_end_queues_the_exact_operator_request(
    database, clock
) -> None:
    service, queue, _ = make_e2e_service(database, clock)
    pending = service.prepare(
        RemoteCommand(
            intent=RemoteIntent.QUEUE_ISSUE,
            target="project:demo",
            parameters={
                "issue_id": "ENG-7",
                "priority": 2,
                "operator_instruction_id": "chat-7",
            },
        ),
        session_id="session-a",
    )

    result = confirm_prepared(service, pending)

    assert result.code == "queued"
    assert result.state == {
        "issue_id": "ENG-7",
        "project_key": "demo",
        "priority": 2,
        "state": "queued",
    }
    queued = queue.get("ENG-7")
    assert queued.project_key == "demo"
    assert queued.linear_priority == 2


def test_reprioritize_end_to_end_applies_the_bounded_priority(
    database, clock
) -> None:
    service, queue, _ = make_e2e_service(database, clock)
    queue.admit(
        AdmissionRequest(
            issue_id="infra-1",
            project_key="demo",
            linear_priority=1,
            admitted_by="operator",
            instruction_id="chat-1",
        )
    )
    pending = service.prepare(
        RemoteCommand(
            intent=RemoteIntent.REPRIORITIZE,
            target="issue:infra-1",
            parameters={"priority": 3},
        ),
        session_id="session-a",
    )

    result = confirm_prepared(service, pending)

    assert result.code == "reprioritized"
    assert queue.get("infra-1").linear_priority == 3


@pytest.mark.parametrize(
    ("intent", "target", "expected_type", "expected_fields"),
    [
        (
            RemoteIntent.PAUSE,
            "project:demo",
            PauseCommand,
            {"project_key": "demo", "reason": "remote operator confirmation"},
        ),
        (RemoteIntent.RESUME, "project:demo", ResumeCommand, {"project_key": "demo"}),
        (RemoteIntent.RETRY, "issue:infra-1", RetryCommand, {"issue_id": "infra-1"}),
        (
            RemoteIntent.APPROVE_HANDOFF,
            "handoff:h-1",
            ApproveHandoffCommand,
            {"handoff_id": "h-1"},
        ),
        (
            RemoteIntent.APPROVE_STALL,
            "project:demo",
            ApproveStallCommand,
            {"project_key": "demo"},
        ),
        (
            RemoteIntent.REQUEST_CHECKPOINT,
            "project:demo",
            RequestCheckpointCommand,
            {"project_key": "demo"},
        ),
        (
            RemoteIntent.REQUEST_CLEANUP,
            "project:demo",
            RequestCleanupCommand,
            {"project_key": "demo"},
        ),
    ],
)
def test_handler_intents_end_to_end_deliver_exact_strict_commands(
    database, clock, intent, target, expected_type, expected_fields
) -> None:
    service, _, recorder = make_e2e_service(database, clock)
    pending = service.prepare(
        RemoteCommand(intent=intent, target=target), session_id="session-a"
    )

    result = confirm_prepared(service, pending)

    assert result.code == "accepted"
    assert len(recorder.received) == 1
    command = recorder.received[0]
    assert isinstance(command, expected_type)
    for field_name, expected in expected_fields.items():
        assert getattr(command, field_name) == expected


@pytest.mark.parametrize(
    ("intent", "target"),
    [
        (RemoteIntent.PAUSE, "issue:infra-1"),
        (RemoteIntent.PAUSE, "handoff:h-1"),
        (RemoteIntent.RESUME, "issue:infra-1"),
        (RemoteIntent.RETRY, "project:demo"),
        (RemoteIntent.REPRIORITIZE, "project:demo"),
        (RemoteIntent.QUEUE_ISSUE, "issue:infra-1"),
        (RemoteIntent.APPROVE_HANDOFF, "project:demo"),
        (RemoteIntent.APPROVE_STALL, "issue:infra-1"),
        (RemoteIntent.REQUEST_CHECKPOINT, "handoff:h-1"),
        (RemoteIntent.REQUEST_CLEANUP, "issue:infra-1"),
    ],
)
def test_incompatible_intent_target_pairs_are_rejected_at_prepare(
    database, clock, intent, target
) -> None:
    service, executor = make_service(database, clock)

    with pytest.raises(IncompatibleTarget):
        service.prepare(
            RemoteCommand(intent=intent, target=target), session_id="session-a"
        )

    assert executor.calls == []
    assert database.scalar("SELECT count(*) FROM remote_pending_commands") == 0


@pytest.mark.parametrize(
    "parameters",
    [
        {},
        {"issue_id": "ENG-7", "priority": 2},
        {"issue_id": "ENG-7", "operator_instruction_id": "chat-7"},
        {"priority": 2, "operator_instruction_id": "chat-7"},
        {"issue_id": "", "priority": 2, "operator_instruction_id": "chat-7"},
        {"issue_id": "ENG-7", "priority": 0, "operator_instruction_id": "chat-7"},
        {"issue_id": "ENG-7", "priority": 5, "operator_instruction_id": "chat-7"},
        {
            "issue_id": "ENG-7",
            "priority": 2,
            "operator_instruction_id": "chat-7",
            "extra": "field",
        },
    ],
)
def test_queue_issue_enforces_bounded_required_parameters(
    database, clock, parameters
) -> None:
    service, executor = make_service(database, clock)

    with pytest.raises(InvalidParameters):
        service.prepare(
            RemoteCommand(
                intent=RemoteIntent.QUEUE_ISSUE,
                target="project:demo",
                parameters=parameters,
            ),
            session_id="session-a",
        )

    assert executor.calls == []
    assert database.scalar("SELECT count(*) FROM remote_pending_commands") == 0


@pytest.mark.parametrize(
    "parameters", [{}, {"priority": 0}, {"priority": 5}, {"priority": "high"}]
)
def test_reprioritize_enforces_the_bounded_priority(
    database, clock, parameters
) -> None:
    service, executor = make_service(database, clock)

    with pytest.raises(InvalidParameters):
        service.prepare(
            RemoteCommand(
                intent=RemoteIntent.REPRIORITIZE,
                target="issue:infra-1",
                parameters=parameters,
            ),
            session_id="session-a",
        )

    assert executor.calls == []
    assert database.scalar("SELECT count(*) FROM remote_pending_commands") == 0


def test_parameters_are_rejected_for_intents_that_take_none(
    database, clock
) -> None:
    service, executor = make_service(database, clock)

    with pytest.raises(InvalidParameters):
        service.prepare(
            RemoteCommand(
                intent=RemoteIntent.PAUSE,
                target="project:demo",
                parameters={"reason": "operator supplied"},
            ),
            session_id="session-a",
        )

    assert executor.calls == []
    assert database.scalar("SELECT count(*) FROM remote_pending_commands") == 0


def test_status_is_not_preparable_as_a_mutation(database, clock) -> None:
    service, executor = make_service(database, clock)

    with pytest.raises(UnsupportedIntent):
        service.prepare(
            RemoteCommand(intent=RemoteIntent.STATUS, target="project:demo"),
            session_id="session-a",
        )

    assert executor.calls == []
    assert database.scalar("SELECT count(*) FROM remote_pending_commands") == 0


def test_unwired_intents_cannot_be_prepared(database, clock) -> None:
    executor = FakeExecutor()
    executor.unsupported = {"approve_stall"}
    service, _ = make_service(database, clock, executor=executor)

    with pytest.raises(UnsupportedIntent):
        service.prepare(
            RemoteCommand(
                intent=RemoteIntent.APPROVE_STALL, target="project:demo"
            ),
            session_id="session-a",
        )

    assert executor.calls == []
    assert database.scalar("SELECT count(*) FROM remote_pending_commands") == 0


def test_unwired_intents_cannot_be_marked_executed(database, clock) -> None:
    """Even an injected pending row for an unwired intent never executes."""

    executor = FakeExecutor()
    executor.unsupported = {"request_cleanup"}
    service, _ = make_service(database, clock, executor=executor)
    now = clock.now()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO remote_pending_commands("
            "confirmation_id, session_id, intent, target, confirmation_phrase, "
            "impact_summary, prepared_at, expires_at, state"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')",
            (
                "injected-unwired",
                "session-a",
                "request_cleanup",
                "project:demo",
                "REQUEST_CLEANUP DEMO",
                "injected row",
                now.isoformat(),
                (now + CONFIRMATION_LIFETIME).isoformat(),
            ),
        )

    with pytest.raises(UnsupportedIntent):
        service.confirm(
            "injected-unwired",
            "REQUEST_CLEANUP DEMO",
            "key-1",
            session_id="session-a",
        )

    assert executor.calls == []
    assert pending_state(database, "injected-unwired") != "executed"


# --- correction 86c93b7d: (session_id, idempotency_key) reservation ---------


def test_same_key_across_two_confirmations_executes_exactly_once(
    database, clock
) -> None:
    """Two DISTINCT confirmations racing one key must not both execute.

    The executor of confirmation A reentrantly confirms confirmation B with
    the same idempotency key — the window in which two OS-level requests
    could race distinct pending rows onto one key.
    """

    executor = ReentrantExecutor()
    service, _ = make_service(database, clock, executor=executor)
    first = service.prepare(pause_command(), session_id="session-a")
    second = service.prepare(pause_command(), session_id="session-a")
    executor.reentry = (
        service,
        second.confirmation_id,
        "PAUSE DEMO",
        "shared-key",
        "session-a",
    )

    outer = service.confirm(
        first.confirmation_id, "PAUSE DEMO", "shared-key", session_id="session-a"
    )

    assert len(executor.calls) == 1
    assert outer.code == "accepted"
    assert isinstance(executor.inner_error, IdempotencyKeyConflict)
    assert pending_state(database, second.confirmation_id) == "pending"
    # The reservation stays bound to the winner: the loser's confirmation
    # can never adopt that key later either.
    with pytest.raises(IdempotencyKeyConflict):
        service.confirm(
            second.confirmation_id,
            "PAUSE DEMO",
            "shared-key",
            session_id="session-a",
        )
    assert len(executor.calls) == 1


def test_key_reservation_is_enforced_across_service_instances(
    tmp_path: Path, clock
) -> None:
    """The reservation lives in the database, not in process memory."""

    path = tmp_path / "state.db"
    first_db = Database.open(path)
    second_db = Database.open(path)
    try:
        executor = ReentrantExecutor()
        first_service, _ = make_service(first_db, clock, executor=executor)
        second_service, second_executor = make_service(second_db, clock)
        first = first_service.prepare(pause_command(), session_id="session-a")
        second = first_service.prepare(pause_command(), session_id="session-a")
        executor.reentry = (
            second_service,
            second.confirmation_id,
            "PAUSE DEMO",
            "shared-key",
            "session-a",
        )

        outer = first_service.confirm(
            first.confirmation_id,
            "PAUSE DEMO",
            "shared-key",
            session_id="session-a",
        )

        assert outer.code == "accepted"
        assert len(executor.calls) == 1
        assert second_executor.calls == []
        assert isinstance(executor.inner_error, IdempotencyKeyConflict)
        assert pending_state(second_db, second.confirmation_id) == "pending"
    finally:
        second_db.close()
        first_db.close()


def test_unresolved_winner_keeps_its_key_reserved(tmp_path: Path, clock) -> None:
    """A crashed winner's key stays bound; no other confirmation may take it."""

    path = tmp_path / "state.db"
    first_db = Database.open(path)
    crashing = CrashingExecutor()
    service, _ = make_service(first_db, clock, executor=crashing)
    first = service.prepare(pause_command(), session_id="session-a")
    second = service.prepare(pause_command(), session_id="session-a")
    with pytest.raises(SimulatedCrash):
        service.confirm(
            first.confirmation_id, "PAUSE DEMO", "key-1", session_id="session-a"
        )
    assert len(crashing.calls) == 1
    first_db.close()

    reopened = Database.open(path)
    try:
        service, fresh_executor = make_service(reopened, clock)
        with pytest.raises(IdempotencyKeyConflict):
            service.confirm(
                second.confirmation_id,
                "PAUSE DEMO",
                "key-1",
                session_id="session-a",
            )
        assert fresh_executor.calls == []
        assert pending_state(reopened, first.confirmation_id) == "claimed"
        assert pending_state(reopened, second.confirmation_id) == "pending"
        # The original outcome stays unresolved, never re-executed.
        with pytest.raises(ExecutionUnresolved):
            service.confirm(
                first.confirmation_id,
                "PAUSE DEMO",
                "key-1",
                session_id="session-a",
            )
        assert fresh_executor.calls == []
    finally:
        reopened.close()


def test_a_reserved_key_stays_usable_in_another_session(
    tmp_path: Path, clock
) -> None:
    """The reservation is scoped per session even while unresolved."""

    path = tmp_path / "state.db"
    first_db = Database.open(path)
    crashing = CrashingExecutor()
    service, _ = make_service(first_db, clock, executor=crashing)
    held = service.prepare(pause_command(), session_id="session-a")
    with pytest.raises(SimulatedCrash):
        service.confirm(
            held.confirmation_id, "PAUSE DEMO", "same-key", session_id="session-a"
        )
    first_db.close()

    reopened = Database.open(path)
    try:
        service, executor = make_service(reopened, clock)
        other = service.prepare(pause_command(), session_id="session-b")

        result = service.confirm(
            other.confirmation_id,
            "PAUSE DEMO",
            "same-key",
            session_id="session-b",
        )

        assert result.code == "accepted"
        assert len(executor.calls) == 1
        assert pending_state(reopened, held.confirmation_id) == "claimed"
    finally:
        reopened.close()
