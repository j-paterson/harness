from __future__ import annotations

import asyncio
import json
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import pytest

from hermes_orchestrator.cli import _run_daemon, main
from tests.test_supervisor import FakeService


@dataclass(frozen=True)
class CliResult:
    exit_code: int
    stdout: str
    stderr: str

    @property
    def output(self) -> str:
        return self.stdout + self.stderr


def invoke(arguments: list[str]) -> CliResult:
    stdout = StringIO()
    stderr = StringIO()
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = main(arguments)
    except SystemExit as error:
        exit_code = int(error.code)
    return CliResult(exit_code, stdout.getvalue(), stderr.getvalue())


def base_arguments(configured_repo: tuple[Path, Path]) -> list[str]:
    repo_root, state_dir = configured_repo
    return ["--repo-root", str(repo_root), "--state-dir", str(state_dir)]


def test_queue_add_requires_explicit_flag(
    configured_repo: tuple[Path, Path],
) -> None:
    result = invoke(
        [
            *base_arguments(configured_repo),
            "queue-add",
            "ENG-7",
            "--project",
            "demo",
            "--priority",
            "2",
        ]
    )

    assert result.exit_code == 2
    assert "--operator-instruction" in result.output


def test_init_creates_runtime_database(configured_repo: tuple[Path, Path]) -> None:
    _, state_dir = configured_repo

    result = invoke([*base_arguments(configured_repo), "init", "--json"])

    assert result.exit_code == 0
    assert (state_dir / "state.db").exists()
    assert json.loads(result.stdout)["schema_version"] == 24


def test_observe_rejects_watch_interval_below_five_seconds(
    configured_repo: tuple[Path, Path],
) -> None:
    result = invoke(
        [*base_arguments(configured_repo), "observe", "--watch", "1", "--json"]
    )

    assert result.exit_code == 2
    assert "at least 5 seconds" in result.output


def test_observe_samples_state_volume_without_registered_projects(
    tmp_path: Path,
) -> None:
    config = tmp_path / "config"
    config.mkdir()
    (config / "projects.yaml").write_text("projects: {}\n", encoding="utf-8")
    (config / "policies.yaml").write_text("mode: observe\n", encoding="utf-8")
    state_dir = tmp_path / "state"

    result = invoke(
        [
            "--repo-root",
            str(tmp_path),
            "--state-dir",
            str(state_dir),
            "observe",
            "--json",
        ]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["snapshot"]["disk_free_bytes"]["orchestrator_state"] > 0


def test_hermes_command_accepts_strict_json_queue_intent(
    configured_repo: tuple[Path, Path],
) -> None:
    request = json.dumps(
        {
            "intent": "queue_issue",
            "issue_id": "ENG-9",
            "project_key": "demo",
            "priority": 1,
            "operator_instruction_id": "chat-9",
        }
    )

    result = invoke(
        [*base_arguments(configured_repo), "hermes-command", "--json", request]
    )

    assert result.exit_code == 0
    assert json.loads(result.stdout)["code"] == "queued"


def test_queue_complete_reconciles_externally_completed_issue(
    configured_repo: tuple[Path, Path],
) -> None:
    add = invoke(
        [
            *base_arguments(configured_repo),
            "queue-add",
            "ENG-7",
            "--project",
            "demo",
            "--priority",
            "2",
            "--operator-instruction",
            "chat-7",
            "--json",
        ]
    )
    assert add.exit_code == 0

    completed = invoke(
        [
            *base_arguments(configured_repo),
            "queue-complete",
            "ENG-7",
            "--reason",
            "linear_completed",
            "--evidence",
            "https://linear.example/ENG-7",
            "--json",
        ]
    )

    assert completed.exit_code == 0
    assert json.loads(completed.stdout)["state"] == "done"
    listed = invoke([*base_arguments(configured_repo), "queue-list", "--json"])
    assert json.loads(listed.stdout) == []
    status = invoke([*base_arguments(configured_repo), "status", "--json"])
    assert json.loads(status.stdout)["queue_count"] == 0


def test_daemon_once_reconciles_and_samples(
    configured_repo: tuple[Path, Path],
) -> None:
    result = invoke(
        [*base_arguments(configured_repo), "daemon", "--once", "--json"]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ticks"] == 1
    assert payload["mode"] == "observe"


@pytest.mark.asyncio
async def test_continuous_daemon_stops_cleanly_when_signaled() -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(
        _run_daemon(
            FakeService(),
            once=False,
            interval=60,
            shutdown_event=stop,
        )
    )
    await asyncio.sleep(0)

    stop.set()
    supervisor = await task

    assert supervisor.events[-3:] == [
        "admission.closed",
        "workers.checkpoint_requested",
        "supervisor.stopped",
    ]


def test_merge_flow_commands_are_registered() -> None:
    from hermes_orchestrator.cli import _parser

    args = _parser().parse_args(
        [
            "candidate-ready",
            "ENG-9",
            "--project",
            "demo",
            "--verified",
            "uv run pytest -q=564 passed",
            "--json",
        ]
    )
    assert args.command == "candidate-ready"
    assert args.verified == ["uv run pytest -q=564 passed"]
    assert args.status == "FABLE_READY"
    turn = _parser().parse_args(["merger-turn", "--project", "demo"])
    assert turn.command == "merger-turn"


def test_subagent_gate_blocks_only_frozen_sessions(tmp_path: Path) -> None:
    import json as _json

    from hermes_orchestrator.cli import main, subagent_gate

    freeze_dir = tmp_path / "freezes"
    freeze_dir.mkdir()
    payload = _json.dumps({"session_id": "s-1", "tool_name": "Agent"})
    assert subagent_gate(freeze_dir, payload) == (0, "")
    (freeze_dir / "s-1.frozen").write_text("rotation_pending: 85%\n")
    code, message = subagent_gate(freeze_dir, payload)
    assert code == 2 and "frozen" in message and "85%" in message
    assert subagent_gate(freeze_dir, _json.dumps({"session_id": "s-2"})) == (0, "")
    assert subagent_gate(freeze_dir, "{not json")[0] == 2
    assert subagent_gate(freeze_dir, "{}") == (0, "")
    # The subcommand needs no configuration or database.
    import io
    import sys as _sys

    stdin = _sys.stdin
    _sys.stdin = io.StringIO(payload)
    try:
        assert main(["subagent-gate", "--freeze-dir", str(freeze_dir)]) == 2
    finally:
        _sys.stdin = stdin


def test_hermes_stall_cycle_through_the_cli(tmp_path: Path) -> None:
    import json as _json

    from hermes_orchestrator.cli import main
    from tests.conftest import configured_repo as _cr  # noqa: F401 - fixture doc

    config = tmp_path / "config"
    config.mkdir()
    (config / "projects.yaml").write_text(
        "projects:\n  demo:\n    linear_team: ENG\n"
        f"    repo_path: {tmp_path}\n    integration_branch: main\n"
        "    github_repo: owner/demo\n",
        encoding="utf-8",
    )
    (config / "policies.yaml").write_text("mode: observe\n", encoding="utf-8")
    (config / "playbooks.yaml").write_text("version: 1\nplaybooks: []\n")
    state = tmp_path / "state"

    def command(payload: dict[str, object]) -> dict[str, object]:
        import io
        import sys as _sys

        out = io.StringIO()
        stdout = _sys.stdout
        _sys.stdout = out
        try:
            code = main(
                [
                    "--repo-root",
                    str(tmp_path),
                    "--state-dir",
                    str(state),
                    "hermes-command",
                    "--json",
                    _json.dumps(payload),
                ]
            )
        finally:
            _sys.stdout = stdout
        assert code == 0, out.getvalue()
        return _json.loads(out.getvalue().strip().splitlines()[-1])

    stall = {
        "intent": "report_stall",
        "project_key": "demo",
        "repeated_failure": "pytest",
    }
    first = command(stall)
    assert first["state"]["mode"] == "ask_operator"
    pending = command({"intent": "pending_consultations", "project_key": "demo"})
    assert len(pending["state"]["consultations"]) == 1
    remedy = {
        "intent": "approve_playbook",
        "consultation_id": first["state"]["consultation_id"],
        "actions": ["wait_for_reset"],
        "verification": "reset_scheduled",
        "timeout_seconds": 300,
        "rollback": "none",
    }
    approved = command(remedy)
    assert approved["state"]["approvals"] == 1
    second = command(stall)
    assert second["state"]["mode"] == "ask_operator"
    remedy["consultation_id"] = second["state"]["consultation_id"]
    assert command(remedy)["state"]["approvals"] == 2
    third = command(stall)
    assert third["state"]["mode"] == "automatic"
    assert third["state"]["actions"] == ["wait_for_reset"]
    # The registered remedy actually executed and verified in this runtime.
    assert third["state"]["execution"]["success"] is True
    assert third["state"]["execution"]["actions_run"] == ["wait_for_reset"]
    assert third["state"]["execution"]["verified"] is True
    assert "playbooks:" in (config / "playbooks.yaml").read_text(encoding="utf-8")
    recorded = command(
        {
            "intent": "record_remedy_result",
            "project_key": "demo",
            "predicate_key": third["state"]["predicate_key"],
            "reason": "repeated_command_failure",
            "mode": "automatic",
            "success": False,
            "playbook_hash": third["state"]["playbook_hash"],
            "detail": "timeout",
        }
    )
    assert recorded["state"]["recorded"] is True
    assert command(stall)["state"]["mode"] == "ask_operator"


def _runtime_like(tmp_path: Path) -> object:
    from types import SimpleNamespace

    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.queue import QueueService
    from hermes_orchestrator.stalls import ScheduledResets

    database = Database.open(tmp_path / "state.db")
    events = EventStore(database)
    queue = QueueService(database, events, {"demo"})
    return SimpleNamespace(
        database=database,
        queue=queue,
        cells=None,
        resets=ScheduledResets(database, events),
    )


def _diagnosis(project: str = "demo") -> object:
    from hermes_orchestrator.stalls import StallDetector, StallEvidence

    found = StallDetector().evaluate(
        StallEvidence(provider_error="limit", project_key=project)
    )
    assert found is not None
    return found


def _admit(runtime: object, issue_id: str, state: str) -> None:
    from hermes_orchestrator.domain import AdmissionRequest

    runtime.queue.admit(  # type: ignore[attr-defined]
        AdmissionRequest(issue_id, "demo", 2, "operator", f"i-{issue_id}")
    )
    with runtime.database.transaction() as connection:  # type: ignore[attr-defined]
        connection.execute(
            "UPDATE admitted_issues SET state = ? WHERE issue_id = ?",
            (state, issue_id),
        )


@pytest.mark.asyncio
async def test_pause_issue_pauses_exactly_the_selected_issue(tmp_path: Path) -> None:
    from hermes_orchestrator.cli import _remedy_executor
    from hermes_orchestrator.domain import IssueState
    from hermes_orchestrator.stalls import Remedy

    runtime = _runtime_like(tmp_path)
    try:
        _admit(runtime, "ENG-1", "in_development")
        _admit(runtime, "ENG-2", "queued")
        executor = _remedy_executor(runtime)  # type: ignore[arg-type]
        outcome = await executor.execute(
            _diagnosis(),  # type: ignore[arg-type]
            Remedy(("pause_issue",), "issue_paused", 5, "none"),
        )
        assert (outcome.success, outcome.verified, outcome.detail) == (
            True,
            True,
            "verified",
        )
        assert runtime.queue.get("ENG-1").state is IssueState.PAUSED  # type: ignore[attr-defined]
        assert runtime.queue.get("ENG-2").state is IssueState.QUEUED  # type: ignore[attr-defined]
    finally:
        runtime.database.close()  # type: ignore[attr-defined]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("states", "message"),
    [
        ([], "no in-flight issue"),
        (["in_development", "review"], "ambiguous pause target"),
    ],
)
async def test_pause_issue_fails_closed_for_missing_or_ambiguous_targets(
    tmp_path: Path, states: list[str], message: str
) -> None:
    from hermes_orchestrator.cli import _remedy_executor
    from hermes_orchestrator.domain import IssueState
    from hermes_orchestrator.stalls import Remedy

    runtime = _runtime_like(tmp_path)
    try:
        for index, state in enumerate(states):
            _admit(runtime, f"ENG-{index}", state)
        executor = _remedy_executor(runtime)  # type: ignore[arg-type]
        outcome = await executor.execute(
            _diagnosis(),  # type: ignore[arg-type]
            Remedy(("pause_issue",), "issue_paused", 5, "none"),
        )
        assert outcome.success is False
        assert outcome.detail == "execution failed: RuntimeError; no rollback"
        assert outcome.actions_run == ()
        for index in range(len(states)):
            assert (
                runtime.queue.get(f"ENG-{index}").state  # type: ignore[attr-defined]
                is not IssueState.PAUSED
            )
    finally:
        runtime.database.close()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_wait_for_reset_schedules_durable_work_that_is_consumed(
    tmp_path: Path,
) -> None:
    from datetime import UTC, datetime, timedelta

    from hermes_orchestrator.cli import _remedy_executor
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.stalls import Remedy, ScheduledResets

    runtime = _runtime_like(tmp_path)
    diagnosis = _diagnosis()
    try:
        executor = _remedy_executor(runtime)  # type: ignore[arg-type]
        outcome = await executor.execute(
            diagnosis,  # type: ignore[arg-type]
            Remedy(("wait_for_reset",), "reset_scheduled", 120, "none"),
        )
        assert outcome.success is True
    finally:
        runtime.database.close()  # type: ignore[attr-defined]
    # A fresh process sees the scheduled work and consumes it when due.
    reopened = Database.open(tmp_path / "state.db")
    try:
        resets = ScheduledResets(reopened, EventStore(reopened))
        pending = resets.pending("demo", diagnosis.predicate_key)  # type: ignore[attr-defined]
        assert len(pending) == 1
        assert resets.consume_due(datetime.now(UTC)) == []
        consumed = resets.consume_due(datetime.now(UTC) + timedelta(seconds=121))
        assert consumed == list(pending)
    finally:
        reopened.close()


def test_hermes_command_lists_and_acks_lead_wakes(
    configured_repo: tuple[Path, Path],
) -> None:
    from uuid import uuid4

    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.lead_wakes import LeadTerminalWakes, TerminalWakeInput

    _repo_root, state_dir = configured_repo
    database = Database.open(state_dir / "state.db")
    try:
        wake = LeadTerminalWakes(
            database=database, events=EventStore(database)
        ).commit(
            TerminalWakeInput(
                project_key="demo",
                issue_id="ENG-9",
                cell_id="cell-1",
                session_id=uuid4(),
                profile_alias="max-a",
                turn_key="evt-1",
                kind="completed",
                reason="turn_completed",
            )
        )
    finally:
        database.close()

    listed = invoke(
        [
            *base_arguments(configured_repo),
            "hermes-command",
            "--json",
            json.dumps({"intent": "pending_wakes"}),
        ]
    )
    assert listed.exit_code == 0
    payload = json.loads(listed.stdout)
    assert payload["code"] == "accepted"
    assert [item["wake_id"] for item in payload["state"]["wakes"]] == [
        wake.wake_id
    ]

    acked = invoke(
        [
            *base_arguments(configured_repo),
            "hermes-command",
            "--json",
            json.dumps({"intent": "ack_wake", "wake_id": wake.wake_id}),
        ]
    )
    assert acked.exit_code == 0
    assert json.loads(acked.stdout)["state"]["state"] == "delivered"

    relisted = invoke(
        [
            *base_arguments(configured_repo),
            "hermes-command",
            "--json",
            json.dumps({"intent": "pending_wakes"}),
        ]
    )
    assert json.loads(relisted.stdout)["state"]["wakes"] == []


def _recording_hermes_config(tmp_path: Path, *, exit_code: int = 0) -> Path:
    """Configure a real recording Hermes consumer command for the daemon."""

    record = tmp_path / "hermes-recorded.jsonl"
    script = tmp_path / "hermes-notify.sh"
    script.write_text(
        f"#!/bin/sh\ncat >> {record}\nprintf '\\n' >> {record}\n"
        f"exit {exit_code}\n",
        encoding="utf-8",
    )
    (tmp_path / "config" / "hermes.yaml").write_text(
        f"wake_command:\n  - /bin/sh\n  - {script}\n",
        encoding="utf-8",
    )
    return record


def _seed_wake(state_dir: Path) -> str:
    from uuid import uuid4

    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.lead_wakes import (
        LeadTerminalWakes,
        TerminalWakeInput,
    )

    database = Database.open(state_dir / "state.db")
    try:
        wake = LeadTerminalWakes(
            database=database, events=EventStore(database)
        ).commit(
            TerminalWakeInput(
                project_key="demo",
                issue_id="ENG-9",
                cell_id="cell-1",
                session_id=uuid4(),
                profile_alias="max-a",
                turn_key="evt-1",
                kind="completed",
                reason="turn_completed",
            )
        )
    finally:
        database.close()
    return wake.wake_id


def _wake_states(state_dir: Path) -> dict[str, str]:
    from hermes_orchestrator.db import Database

    database = Database.open(state_dir / "state.db")
    try:
        return {
            str(row["wake_id"]): str(row["state"])
            for row in database.execute(
                "SELECT wake_id, state FROM lead_terminal_wakes"
            ).fetchall()
        }
    finally:
        database.close()


def test_daemon_pushes_committed_wake_to_hermes_without_polling(
    configured_repo: tuple[Path, Path],
) -> None:
    repo_root, state_dir = configured_repo
    record = _recording_hermes_config(repo_root)
    # The wake was committed before this daemon start: startup replay must
    # push it through the configured transport, not wait for pending_wakes.
    wake_id = _seed_wake(state_dir)

    result = invoke([*base_arguments(configured_repo), "daemon", "--once"])

    assert result.exit_code == 0
    recorded = [
        json.loads(line)
        for line in record.read_text(encoding="utf-8").splitlines()
        if line
    ]
    assert [item["wake_id"] for item in recorded] == [wake_id]
    assert recorded[0]["kind"] == "completed"
    assert recorded[0]["issue_id"] == "ENG-9"
    # Acknowledged only after the consumer accepted; no pending_wakes or
    # ack_wake intent was involved.
    assert _wake_states(state_dir) == {wake_id: "delivered"}


def test_daemon_keeps_wake_pending_until_hermes_accepts(
    configured_repo: tuple[Path, Path],
) -> None:
    repo_root, state_dir = configured_repo
    record = _recording_hermes_config(repo_root, exit_code=1)
    wake_id = _seed_wake(state_dir)

    result = invoke([*base_arguments(configured_repo), "daemon", "--once"])

    assert result.exit_code == 0
    # The push was attempted, but a rejecting consumer never acknowledges
    # the row: it stays pending for the next interval retry.
    assert record.read_text(encoding="utf-8").strip() != ""
    assert _wake_states(state_dir) == {wake_id: "pending"}


def test_daemon_reconstructs_lost_wake_from_terminal_evidence(
    configured_repo: tuple[Path, Path],
) -> None:
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventInput, EventStore

    repo_root, state_dir = configured_repo
    record = _recording_hermes_config(repo_root)
    session_id = "33333333-3333-4333-8333-333333333333"

    # Seed the durable aftermath of a crash between the terminal handoff
    # transition and the wake outbox insert: evidence, but no wake row.
    database = Database.open(state_dir / "state.db")
    try:
        events = EventStore(database)
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO project_cells("
                "cell_id, project_key, state, profile_alias, session_id, "
                "created_at, updated_at) VALUES (?, 'demo', "
                "'handoff_required', 'max-a', ?, ?, ?)",
                ("cell-1", session_id, "2026-08-28T00:00:00+00:00",
                 "2026-08-28T00:00:00+00:00"),
            )
            events.append(
                connection,
                EventInput(
                    event_type="issue.started",
                    aggregate_type="issue",
                    aggregate_id="ENG-9",
                    payload={"cell_id": "cell-1"},
                ),
            )
            events.append(
                connection,
                EventInput(
                    event_type="project_cell.handoff_required",
                    aggregate_type="project_cell",
                    aggregate_id="cell-1",
                    payload={
                        "reason": "context_rotation",
                        "session_id": session_id,
                    },
                ),
            )
    finally:
        database.close()

    result = invoke([*base_arguments(configured_repo), "daemon", "--once"])

    assert result.exit_code == 0
    recorded = [
        json.loads(line)
        for line in record.read_text(encoding="utf-8").splitlines()
        if line
    ]
    assert [item["kind"] for item in recorded] == ["handoff_required"]
    assert recorded[0]["issue_id"] == "ENG-9"
    assert recorded[0]["cell_id"] == "cell-1"
    assert recorded[0]["session_id"] == session_id
    assert recorded[0]["turn_key"].startswith("handoff:")
    assert list(_wake_states(state_dir).values()) == ["delivered"]

    # A second daemon start neither re-manufactures nor re-delivers it.
    again = invoke([*base_arguments(configured_repo), "daemon", "--once"])
    assert again.exit_code == 0
    assert len(_wake_states(state_dir)) == 1
