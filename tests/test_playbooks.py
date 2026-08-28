"""Verify the consult-twice, automate-third playbook cycle and YAML durability."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.stalls import (
    PlaybookService,
    Remedy,
    StallDetector,
    StallDiagnosis,
    StallEvidence,
)

NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)


class Clock:
    def __init__(self) -> None:
        self.value = NOW

    def now(self) -> datetime:
        self.value = self.value + timedelta(seconds=1)
        return self.value


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def playbook_path(tmp_path: Path) -> Path:
    path = tmp_path / "config" / "playbooks.yaml"
    path.parent.mkdir()
    path.write_text("version: 1\nplaybooks: []\n", encoding="utf-8")
    return path


@pytest.fixture
def playbooks(database: Database, playbook_path: Path) -> PlaybookService:
    ids = iter(f"consult-{n}" for n in range(1, 50))
    return PlaybookService(
        database,
        EventStore(database),
        playbook_path=playbook_path,
        now=Clock().now,
        ids=lambda: next(ids),
    )


@pytest.fixture
def diagnosis() -> StallDiagnosis:
    found = StallDetector().evaluate(
        StallEvidence(repeated_failure="uv run pytest -q", project_key="demo")
    )
    assert found is not None
    return found


def approved_remedy(action: str = "rerun_tests") -> Remedy:
    return Remedy(
        actions=(action,),
        verification="uv run pytest -q passes",
        timeout_seconds=600,
        rollback="none; read-only retry",
    )


def approve_twice(
    playbooks: PlaybookService, diagnosis: StallDiagnosis, action: str = "rerun_tests"
) -> None:
    for _ in range(2):
        plan = playbooks.resolve(diagnosis)
        assert plan.mode == "ask_operator" and plan.consultation_id is not None
        playbooks.record_operator_result(plan.consultation_id, approved_remedy(action))


def test_first_two_matches_require_consultation(
    playbooks: PlaybookService, diagnosis: StallDiagnosis
) -> None:
    first = playbooks.resolve(diagnosis)
    assert first.consultation_id is not None
    playbooks.record_operator_result(first.consultation_id, approved_remedy())
    second = playbooks.resolve(diagnosis)
    assert first.mode == "ask_operator"
    assert second.mode == "ask_operator"
    assert second.actions == approved_remedy().actions  # refined from the result
    assert second.evidence["approvals"] == 1


def test_third_match_uses_approved_playbook(
    playbooks: PlaybookService, diagnosis: StallDiagnosis
) -> None:
    approve_twice(playbooks, diagnosis)
    third = playbooks.resolve(diagnosis)
    assert third.mode == "automatic"
    assert third.actions == approved_remedy().actions
    assert third.playbook_hash == playbooks.current_hash()
    assert third.consultation_id is None


def test_sensitive_action_always_asks(
    playbooks: PlaybookService, diagnosis: StallDiagnosis
) -> None:
    approve_twice(playbooks, diagnosis, action="change_credentials")
    result = playbooks.resolve(diagnosis)
    assert result.mode == "ask_operator"
    assert "sensitive" in result.explanation


def test_unregistered_command_always_asks(
    playbooks: PlaybookService, diagnosis: StallDiagnosis
) -> None:
    approve_twice(playbooks, diagnosis, action="rm_rf_worktree")
    assert playbooks.resolve(diagnosis).mode == "ask_operator"


def test_changed_predicate_starts_a_new_cycle(
    playbooks: PlaybookService, diagnosis: StallDiagnosis
) -> None:
    approve_twice(playbooks, diagnosis)
    other = StallDetector().evaluate(
        StallEvidence(repeated_failure="uv run ruff check .", project_key="demo")
    )
    assert other is not None
    assert playbooks.resolve(other).mode == "ask_operator"
    assert playbooks.resolve(diagnosis).mode == "automatic"


def test_changed_playbook_content_invalidates_prior_approvals(
    playbooks: PlaybookService, diagnosis: StallDiagnosis, playbook_path: Path
) -> None:
    approve_twice(playbooks, diagnosis)
    assert playbooks.resolve(diagnosis).mode == "automatic"
    # Any out-of-band edit changes the exact hash: automation is revoked.
    playbook_path.write_text(
        playbook_path.read_text(encoding="utf-8") + "# edited\n", encoding="utf-8"
    )
    plan = playbooks.resolve(diagnosis)
    assert plan.mode == "ask_operator"
    assert plan.evidence["approvals"] == 0


def test_failed_or_timed_out_remedy_requires_reconsultation(
    playbooks: PlaybookService, diagnosis: StallDiagnosis
) -> None:
    approve_twice(playbooks, diagnosis)
    automatic = playbooks.resolve(diagnosis)
    assert automatic.mode == "automatic"
    playbooks.record_execution(
        diagnosis,
        playbook_hash=automatic.playbook_hash,
        mode="automatic",
        success=False,
        detail="timeout after 600s",
    )
    again = playbooks.resolve(diagnosis)
    assert again.mode == "ask_operator"
    assert "failed or timed out" in again.explanation
    # One more approval after the failure restores automation.
    assert again.consultation_id is not None
    playbooks.record_operator_result(again.consultation_id, approved_remedy())
    assert playbooks.resolve(diagnosis).mode == "automatic"


def test_project_override_beats_the_global_playbook(
    playbooks: PlaybookService, diagnosis: StallDiagnosis
) -> None:
    global_diagnosis = StallDiagnosis(
        reason=diagnosis.reason,
        predicate=diagnosis.predicate,
        predicate_key=diagnosis.predicate_key,
        project_key="global",
        summary=diagnosis.summary,
    )
    approve_twice(playbooks, global_diagnosis, action="retry_command")
    # Global by default: the project inherits the approved global playbook.
    inherited = playbooks.resolve(diagnosis)
    assert (inherited.mode, inherited.actions) == ("automatic", ("retry_command",))
    # An explicit project override needs its own two consultations.
    for _ in range(2):
        consultation = playbooks.consult(diagnosis)
        playbooks.approve(consultation.consultation_id, approved_remedy("rerun_tests"))
    demo = playbooks.playbook_for(diagnosis)
    assert demo is not None and demo.project_key == "demo"
    shared = playbooks.playbook_for(global_diagnosis)
    assert shared is not None and shared.project_key is None
    assert playbooks.resolve(diagnosis).actions == ("rerun_tests",)
    assert playbooks.resolve(global_diagnosis).actions == ("retry_command",)


def test_rejection_and_double_resolution(
    playbooks: PlaybookService, diagnosis: StallDiagnosis
) -> None:
    plan = playbooks.resolve(diagnosis)
    assert plan.consultation_id is not None
    assert (
        playbooks.record_operator_result(
            plan.consultation_id, approved_remedy(), approved=False
        )
        is None
    )
    assert playbooks.get(plan.consultation_id).state == "rejected"
    with pytest.raises(ValueError, match="already resolved"):
        playbooks.record_operator_result(plan.consultation_id, approved_remedy())
    assert playbooks.pending("demo") == ()
    with pytest.raises(ValueError, match="at least one action"):
        playbooks.approve(
            playbooks.resolve(diagnosis).consultation_id,  # type: ignore[arg-type]
            Remedy((), "v", 10, "r"),
        )


def test_yaml_round_trip_is_atomic_versioned_and_hashed(
    playbooks: PlaybookService,
    diagnosis: StallDiagnosis,
    playbook_path: Path,
    database: Database,
) -> None:
    plan = playbooks.resolve(diagnosis)
    assert plan.consultation_id is not None
    playbook = playbooks.approve(plan.consultation_id, approved_remedy())
    document = yaml.safe_load(playbook_path.read_text(encoding="utf-8"))
    assert document["version"] == 2
    entry = document["playbooks"][0]
    assert entry["reason"] == "repeated_command_failure"
    assert entry["predicate"] == diagnosis.predicate
    assert entry["predicate_key"] == diagnosis.predicate_key
    assert entry["project_key"] == "demo"
    assert entry["remedy"] == approved_remedy().as_dict()
    assert (entry["approvals"], entry["version"]) == (1, 1)
    assert playbook.approvals == 1
    assert [p.name for p in playbook_path.parent.iterdir()] == ["playbooks.yaml"]
    recorded = database.execute(
        "SELECT content_hash, version FROM playbook_versions"
    ).fetchall()
    assert [(row["content_hash"], row["version"]) for row in recorded] == [
        (playbooks.current_hash(), 2)
    ]
    # A refined remedy on the second consultation bumps the entry version.
    second = playbooks.resolve(diagnosis)
    assert second.consultation_id is not None
    refined = Remedy(("rerun_tests", "rebase_branch"), "green", 900, "revert rebase")
    playbook = playbooks.approve(second.consultation_id, refined)
    assert (playbook.approvals, playbook.version) == (2, 2)
    assert database.scalar("SELECT count(*) FROM playbook_versions") == 2
    # Loading a corrupt file fails closed.
    playbook_path.write_text("- not\n- a mapping\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"playbooks\.yaml"):
        playbooks.resolve(diagnosis)


# --- fail-closed remedy execution on the exact third recurrence ------------


import asyncio as _asyncio  # noqa: E402

from hermes_orchestrator.stalls import RemedyExecutor, RemedyRefused  # noqa: E402


class Handlers:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fail_action = False
        self.verify_result = True
        self.sleep = 0.0

    async def rerun_tests(self, diagnosis: StallDiagnosis, remedy: Remedy) -> None:
        self.calls.append("rerun_tests")
        if self.sleep:
            await _asyncio.sleep(self.sleep)
        if self.fail_action:
            raise RuntimeError("pytest crashed")

    async def rebase_branch(self, diagnosis: StallDiagnosis, remedy: Remedy) -> None:
        self.calls.append("rollback:rebase_branch")

    async def green(self, diagnosis: StallDiagnosis, remedy: Remedy) -> bool:
        self.calls.append("verify:green")
        return self.verify_result


def executor_with(handlers: Handlers) -> RemedyExecutor:
    return RemedyExecutor(
        handlers={
            "rerun_tests": handlers.rerun_tests,
            "rebase_branch": handlers.rebase_branch,
        },
        verifiers={"green": handlers.green},
    )


def remedy_with_rollback(timeout: int = 5) -> Remedy:
    return Remedy(("rerun_tests",), "green", timeout, "rebase_branch")


def approve_twice_with(
    playbooks: PlaybookService, diagnosis: StallDiagnosis, remedy: Remedy
) -> None:
    for _ in range(2):
        plan = playbooks.resolve(diagnosis)
        assert plan.mode == "ask_operator" and plan.consultation_id is not None
        playbooks.record_operator_result(plan.consultation_id, remedy)


def executions(database: Database) -> list[tuple[str, int, str]]:
    return [
        (str(r["mode"]), int(r["success"]), str(r["detail"]))
        for r in database.execute(
            "SELECT mode, success, detail FROM stall_executions ORDER BY rowid"
        ).fetchall()
    ]


@pytest.mark.asyncio
async def test_third_recurrence_executes_verifies_and_records_success(
    playbooks: PlaybookService, diagnosis: StallDiagnosis, database: Database
) -> None:
    handlers = Handlers()
    approve_twice_with(playbooks, diagnosis, remedy_with_rollback())
    assert handlers.calls == []
    plan, outcome = await playbooks.resolve_and_execute(
        diagnosis, executor_with(handlers)
    )
    assert plan.mode == "automatic"
    assert outcome is not None and outcome.success is True
    assert (outcome.verified, outcome.rolled_back, outcome.detail) == (
        True,
        False,
        "verified",
    )
    assert handlers.calls == ["rerun_tests", "verify:green"]
    assert executions(database) == [("automatic", 1, "verified")]
    # Automation persists after a verified success.
    again, _ = await playbooks.resolve_and_execute(diagnosis, executor_with(handlers))
    assert again.mode == "automatic"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("setup", "expected_detail", "rolled_back"),
    [
        ("fail_action", "execution failed: RuntimeError; rollback applied", True),
        ("verify_fail", "verification 'green' failed; rollback applied", True),
        ("timeout", "timed out after 1s; rollback applied", True),
    ],
)
async def test_failures_roll_back_record_and_revoke_automation(
    playbooks: PlaybookService,
    diagnosis: StallDiagnosis,
    database: Database,
    setup: str,
    expected_detail: str,
    rolled_back: bool,
) -> None:
    handlers = Handlers()
    if setup == "fail_action":
        handlers.fail_action = True
    elif setup == "verify_fail":
        handlers.verify_result = False
    else:
        handlers.sleep = 2.0
    approve_twice_with(playbooks, diagnosis, remedy_with_rollback(timeout=1))
    plan, outcome = await playbooks.resolve_and_execute(
        diagnosis, executor_with(handlers)
    )
    assert plan.mode == "automatic"
    assert outcome is not None and outcome.success is False
    assert outcome.detail == expected_detail
    assert outcome.rolled_back is rolled_back
    assert "rollback:rebase_branch" in handlers.calls
    assert executions(database) == [("automatic", 0, expected_detail)]
    # The next recurrence must ask the operator again.
    nxt, nothing = await playbooks.resolve_and_execute(
        diagnosis, executor_with(handlers)
    )
    assert (nxt.mode, nothing) == ("ask_operator", None)
    assert "failed or timed out" in nxt.explanation


@pytest.mark.asyncio
async def test_missing_handler_or_verifier_fails_closed(
    playbooks: PlaybookService, diagnosis: StallDiagnosis, database: Database
) -> None:
    approve_twice_with(
        playbooks, diagnosis, Remedy(("retry_command",), "green", 5, "none")
    )
    plan, outcome = await playbooks.resolve_and_execute(diagnosis, RemedyExecutor())
    assert plan.mode == "automatic"
    assert outcome is not None and outcome.success is False
    assert outcome.detail == (
        "no handler wired for registered action 'retry_command'; no rollback"
    )
    assert outcome.rolled_back is False
    assert executions(database)[0][1] == 0
    assert playbooks.resolve(diagnosis).mode == "ask_operator"


@pytest.mark.asyncio
async def test_sensitive_and_unregistered_actions_never_reach_the_executor(
    playbooks: PlaybookService, diagnosis: StallDiagnosis, database: Database
) -> None:
    handlers = Handlers()
    executor = executor_with(handlers)
    for action in ("change_credentials", "rm_rf_worktree"):
        with pytest.raises(RemedyRefused):
            await executor.execute(diagnosis, Remedy((action,), "green", 5, "none"))
    with pytest.raises(RemedyRefused, match="sensitive rollback"):
        await executor.execute(diagnosis, Remedy(("rerun_tests",), "green", 5, "spend"))
    assert handlers.calls == []
    # Through the service: sensitive approvals resolve to ask_operator and
    # nothing executes or is recorded.
    approve_twice_with(
        playbooks, diagnosis, Remedy(("change_credentials",), "green", 5, "none")
    )
    plan, outcome = await playbooks.resolve_and_execute(diagnosis, executor)
    assert (plan.mode, outcome) == ("ask_operator", None)
    assert handlers.calls == []
    assert executions(database) == []


# --- async-only, cooperatively cancellable execution -------------------------


import sqlite3 as _sqlite3  # noqa: E402
import threading as _threading  # noqa: E402


class Mutations:
    """Records whether a remedy touched state, and on which thread."""

    def __init__(self) -> None:
        self.mutated: list[str] = []
        self.calls: list[str] = []
        self.threads: set[int] = set()

    def sync_mutating_action(self, diagnosis: StallDiagnosis, remedy: Remedy) -> None:
        self.calls.append("sync_action")
        self.mutated.append("sync_action")

    async def late_mutating_action(
        self, diagnosis: StallDiagnosis, remedy: Remedy
    ) -> None:
        self.calls.append("action_started")
        self.threads.add(_threading.get_ident())
        await _asyncio.sleep(2)  # cancelled here at the deadline
        self.mutated.append("late_action")

    async def rollback(self, diagnosis: StallDiagnosis, remedy: Remedy) -> None:
        self.calls.append("rollback")
        self.threads.add(_threading.get_ident())
        self.mutated.append("rollback")

    async def verify(self, diagnosis: StallDiagnosis, remedy: Remedy) -> bool:
        self.calls.append("verify")
        self.threads.add(_threading.get_ident())
        return True

    def sync_verify(self, diagnosis: StallDiagnosis, remedy: Remedy) -> bool:
        self.calls.append("sync_verify")
        return True


@pytest.mark.asyncio
async def test_synchronous_mutation_handler_is_refused_before_it_starts(
    playbooks: PlaybookService, diagnosis: StallDiagnosis, database: Database
) -> None:
    state = Mutations()
    executor = RemedyExecutor(
        handlers={
            "rerun_tests": state.sync_mutating_action,
            "rebase_branch": state.rollback,
        },
        verifiers={"green": state.verify},
    )
    with pytest.raises(RemedyRefused, match="synchronous"):
        await executor.execute(diagnosis, remedy_with_rollback())
    assert state.calls == []
    assert state.mutated == []
    # Through the service the refusal is a recorded failure: nothing ran and
    # the next recurrence returns to the operator.
    approve_twice_with(playbooks, diagnosis, remedy_with_rollback())
    plan, outcome = await playbooks.resolve_and_execute(diagnosis, executor)
    assert plan.mode == "automatic"
    assert outcome is not None and outcome.success is False
    assert outcome.detail.startswith(
        "refused: handler for 'rerun_tests' is synchronous"
    )
    assert outcome.actions_run == ()
    assert state.calls == [] and state.mutated == []
    assert executions(database)[0][1] == 0
    assert playbooks.resolve(diagnosis).mode == "ask_operator"


@pytest.mark.asyncio
async def test_synchronous_verifier_and_rollback_are_refused_before_execution(
    diagnosis: StallDiagnosis,
) -> None:
    state = Mutations()
    sync_verifier = RemedyExecutor(
        handlers={"rerun_tests": state.late_mutating_action},
        verifiers={"green": state.sync_verify},
    )
    with pytest.raises(RemedyRefused, match="verifier 'green' is synchronous"):
        await sync_verifier.execute(
            diagnosis, Remedy(("rerun_tests",), "green", 5, "none")
        )
    sync_rollback = RemedyExecutor(
        handlers={
            "rerun_tests": state.late_mutating_action,
            "rebase_branch": state.sync_mutating_action,
        },
        verifiers={"green": state.verify},
    )
    with pytest.raises(RemedyRefused, match="rollback 'rebase_branch' is synchronous"):
        await sync_rollback.execute(diagnosis, remedy_with_rollback())
    assert state.calls == [] and state.mutated == []


@pytest.mark.asyncio
async def test_timeout_cancels_the_action_before_rollback_and_no_late_mutation(
    playbooks: PlaybookService, diagnosis: StallDiagnosis, database: Database
) -> None:
    state = Mutations()
    executor = RemedyExecutor(
        handlers={
            "rerun_tests": state.late_mutating_action,
            "rebase_branch": state.rollback,
        },
        verifiers={"green": state.verify},
    )
    approve_twice_with(playbooks, diagnosis, remedy_with_rollback(timeout=1))
    ticks = 0

    async def heartbeat() -> None:
        nonlocal ticks
        while True:
            await _asyncio.sleep(0.1)
            ticks += 1

    beat = _asyncio.create_task(heartbeat())
    plan, outcome = await playbooks.resolve_and_execute(diagnosis, executor)
    beat.cancel()
    assert plan.mode == "automatic"
    assert outcome is not None and outcome.success is False
    assert outcome.detail == "timed out after 1s; rollback applied"
    assert outcome.rolled_back is True
    # Ordering: the action started, was cancelled at the deadline, and only
    # then did rollback run; the late mutation never happened.
    assert state.calls == ["action_started", "rollback"]
    assert state.mutated == ["rollback"]
    assert ticks >= 5
    # Even after the action's original sleep would have elapsed, nothing
    # overwrote the rollback state: the coroutine was cancelled, not detached.
    await _asyncio.sleep(2.2)
    assert state.mutated == ["rollback"]
    assert executions(database) == [
        ("automatic", 0, "timed out after 1s; rollback applied")
    ]
    assert playbooks.resolve(diagnosis).mode == "ask_operator"


@pytest.mark.asyncio
async def test_remedies_run_on_the_connection_owning_thread(
    diagnosis: StallDiagnosis, database: Database
) -> None:
    state = Mutations()
    executor = RemedyExecutor(
        handlers={
            "rerun_tests": state.late_mutating_action,
            "rebase_branch": state.rollback,
        },
        verifiers={"green": state.verify},
    )
    await executor.execute(
        diagnosis, Remedy(("rerun_tests",), "green", 1, "rebase_branch")
    )
    assert state.threads == {_threading.get_ident()}
    # SQLite same-thread ownership is preserved: another thread may not use
    # the connection at all.
    errors: list[BaseException] = []

    def other_thread() -> None:
        try:
            database.execute("SELECT 1")
        except BaseException as error:
            errors.append(error)

    worker = _threading.Thread(target=other_thread)
    worker.start()
    worker.join()
    assert len(errors) == 1 and isinstance(errors[0], _sqlite3.ProgrammingError)


@pytest.mark.asyncio
async def test_failing_rollback_is_recorded_not_applied(
    diagnosis: StallDiagnosis,
) -> None:
    async def quick(diagnosis: StallDiagnosis, remedy: Remedy) -> None:
        return None

    async def failing_rollback(diagnosis: StallDiagnosis, remedy: Remedy) -> None:
        raise RuntimeError("cannot revert")

    async def red(diagnosis: StallDiagnosis, remedy: Remedy) -> bool:
        return False

    executor = RemedyExecutor(
        handlers={"rerun_tests": quick, "rebase_branch": failing_rollback},
        verifiers={"green": red},
    )
    outcome = await executor.execute(diagnosis, remedy_with_rollback())
    assert outcome.success is False
    assert outcome.rolled_back is False
    assert outcome.detail == (
        "verification 'green' failed; rollback failed: RuntimeError"
    )


@pytest.mark.asyncio
async def test_slow_rollback_is_bounded_and_recorded(
    diagnosis: StallDiagnosis,
) -> None:
    async def quick(diagnosis: StallDiagnosis, remedy: Remedy) -> None:
        return None

    async def slow_rollback(diagnosis: StallDiagnosis, remedy: Remedy) -> None:
        await _asyncio.sleep(3)

    async def red(diagnosis: StallDiagnosis, remedy: Remedy) -> bool:
        return False

    executor = RemedyExecutor(
        handlers={"rerun_tests": quick, "rebase_branch": slow_rollback},
        verifiers={"green": red},
    )
    outcome = await executor.execute(diagnosis, remedy_with_rollback(timeout=1))
    assert outcome.detail == "verification 'green' failed; rollback timed out after 1s"
    assert outcome.rolled_back is False


@pytest.mark.asyncio
async def test_deadline_spans_actions_and_verification(
    diagnosis: StallDiagnosis,
) -> None:
    """Two actions that each fit the timeout must still finish together."""

    async def half(diagnosis: StallDiagnosis, remedy: Remedy) -> None:
        await _asyncio.sleep(0.7)

    async def ok(diagnosis: StallDiagnosis, remedy: Remedy) -> bool:
        return True

    executor = RemedyExecutor(
        handlers={"rerun_tests": half, "rebase_branch": half},
        verifiers={"green": ok},
    )
    outcome = await executor.execute(
        diagnosis, Remedy(("rerun_tests", "rebase_branch"), "green", 1, "none")
    )
    assert outcome.success is False
    assert outcome.detail.startswith("timed out after 1s")
    assert outcome.actions_run == ("rerun_tests",)


# --- durable scheduled resets -----------------------------------------------


from hermes_orchestrator.stalls import ScheduledResets  # noqa: E402


def test_scheduled_reset_survives_restart_and_is_consumed_when_due(
    tmp_path: Path,
) -> None:
    path = tmp_path / "resets.db"
    database = Database.open(path)
    at = [NOW]
    try:
        resets = ScheduledResets(database, EventStore(database), now=lambda: at[0])
        with pytest.raises(ValueError, match="positive delay"):
            resets.schedule("demo", "pk", reason="provider_limit", delay_seconds=0)
        reset_id = resets.schedule(
            "demo", "pk", reason="provider_limit", delay_seconds=300
        )
        assert resets.pending("demo", "pk") == (reset_id,)
    finally:
        database.close()
    reopened = Database.open(path)
    try:
        restarted = ScheduledResets(reopened, EventStore(reopened), now=lambda: at[0])
        assert restarted.pending("demo", "pk") == (reset_id,)
        assert restarted.pending("other", "pk") == ()
        assert restarted.consume_due(NOW + timedelta(seconds=299)) == []
        assert restarted.consume_due(NOW + timedelta(seconds=300)) == [reset_id]
        assert restarted.pending("demo", "pk") == ()
        assert restarted.consume_due(NOW + timedelta(seconds=600)) == []
        kinds = [
            row["event_type"]
            for row in reopened.execute(
                "SELECT event_type FROM events WHERE aggregate_id = 'pk' "
                "ORDER BY sequence"
            ).fetchall()
        ]
        assert kinds == ["stall.reset_scheduled", "stall.reset_consumed"]
    finally:
        reopened.close()
