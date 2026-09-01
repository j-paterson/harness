from __future__ import annotations

import asyncio
import inspect
import json
import subprocess
import sys
import types
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.cli import _listen_for_merger_turns, _run_daemon, main
from hermes_orchestrator.codex_rpc import RpcNotification
from hermes_orchestrator.merger_turns import TurnOutcome
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
    assert json.loads(result.stdout)["schema_version"] == 56


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


@pytest.mark.parametrize("blocked_state", ["paused", "blocked"])
def test_hermes_command_retries_a_locally_blocked_issue(
    configured_repo: tuple[Path, Path], blocked_state: str
) -> None:
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.domain import IssueState
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.queue import QueueService

    added = invoke(
        [
            *base_arguments(configured_repo),
            "queue-add",
            "ENG-9",
            "--project",
            "demo",
            "--priority",
            "1",
            "--operator-instruction",
            "chat-9",
        ]
    )
    assert added.exit_code == 0
    _, state_dir = configured_repo
    database = Database.open(state_dir / "state.db")
    try:
        queue = QueueService(database, EventStore(database), {"demo"})
        queue.transition(
            "ENG-9",
            IssueState(blocked_state),
            actor="test",
            reason="exercise local retry",
        )
    finally:
        database.close()

    result = invoke(
        [
            *base_arguments(configured_repo),
            "hermes-command",
            "--json",
            json.dumps({"intent": "retry", "issue_id": "ENG-9"}),
        ]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["code"] == "accepted"
    assert payload["state"] == {"issue_id": "ENG-9", "state": "queued"}


def test_hermes_command_retry_refuses_a_non_retryable_issue(
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
    assert invoke(
        [*base_arguments(configured_repo), "hermes-command", "--json", request]
    ).exit_code == 0

    result = invoke(
        [
            *base_arguments(configured_repo),
            "hermes-command",
            "--json",
            json.dumps({"intent": "retry", "issue_id": "ENG-9"}),
        ]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["code"] == "rejected"
    assert payload["state"] == {"reason": "retry_not_applicable"}


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


@pytest.mark.asyncio
async def test_daemon_opens_and_closes_the_channel_hub(
    tmp_path: Path,
) -> None:
    import tempfile

    from hermes_orchestrator.channel_hub import (
        ChannelCapabilities,
        ChannelHub,
    )
    from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore

    database = Database.open(tmp_path / "state.db")
    try:
        with tempfile.TemporaryDirectory() as short:
            socket_path = Path(short) / "hub.sock"
            hub = ChannelHub(
                database=database,
                bindings=CmuxSurfaceBindings(
                    database=database, events=EventStore(database)
                ),
                capabilities=ChannelCapabilities(
                    database=database, state_dir=tmp_path
                ),
                socket_path=socket_path,
            )
            service = FakeService()

            await _run_daemon(
                service, once=True, interval=60, channel_hub=hub
            )

            # The hub served for the daemon's lifetime and its socket
            # was removed at shutdown — nothing dangles for a stale
            # sidecar to connect to.
            assert service.ticks == 1
            assert not socket_path.exists()
    finally:
        database.close()


@pytest.mark.asyncio
async def test_daemon_starts_when_cmux_fails_after_ping(
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.cmux import CmuxUnavailable
    from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore
    from tests.test_cmux_surfaces import LEAD, SESSION, FakePort, reconciler

    database = Database.open(tmp_path / "state.db")
    try:
        bindings = CmuxSurfaceBindings(
            database=database, events=EventStore(database)
        )
        binding = bindings.bind_lead(
            project_key="demo",
            cell_id="cell-demo",
            session_id=SESSION,
            profile_alias="max-a",
            ref=LEAD,
        )
        port = FakePort(
            fail={"surface_alive": CmuxUnavailable("cmux command timed out")}
        )
        service = FakeService()

        supervisor = await _run_daemon(
            service,
            once=True,
            interval=60,
            cmux_reconciler=reconciler(bindings, port),
        )

        # cmux answered the ping but failed mid-reconciliation; the
        # optional boundary absorbed it, the daemon ran its tick, and the
        # binding stayed active and recoverable.
        assert service.ticks == 1
        assert supervisor is not None
        assert bindings.get(binding.binding_id).state == "active"
    finally:
        database.close()


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
    submit = _parser().parse_args(
        [
            "submit-review",
            "--project",
            "demo",
            "--issue",
            "ENG-9",
            "--event",
            "evt-1",
            "--candidate-sha",
            "a" * 40,
            "--thread",
            "thread-1",
            "--generation",
            "3",
            "--verdict",
            "-",
        ]
    )
    assert submit.command == "submit-review"
    assert submit.generation == 3
    assert submit.verdict == "-"


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


def test_subagent_gate_admits_through_the_packet_ledger_when_wired(
    tmp_path: Path,
) -> None:
    import json as _json

    from hermes_orchestrator.cli import subagent_gate
    from hermes_orchestrator.packet_admission import AdmissionDecision

    freeze_dir = tmp_path / "freezes"
    freeze_dir.mkdir()
    packet_id = "a" * 32
    payload = _json.dumps(
        {
            "session_id": "s-1",
            "tool_name": "Agent",
            "tool_use_id": "toolu_1",
            "tool_input": {
                "description": f"packet:{packet_id} implement the red test",
                "model": "sonnet",
                "effort": "high",
            },
        }
    )

    class RecordingAdmission:
        def __init__(self, allowed: bool, reason: str) -> None:
            self.allowed = allowed
            self.reason = reason
            self.calls: list[dict[str, str]] = []

        def admit(self, **kwargs: str) -> AdmissionDecision:
            self.calls.append(kwargs)
            return AdmissionDecision(
                allowed=self.allowed,
                reason=self.reason,
                packet_id=kwargs["packet_id"] if self.allowed else None,
            )

    refusing = RecordingAdmission(False, "overlapping ownership with reserved packet")
    code, message = subagent_gate(freeze_dir, payload, admission=refusing)
    assert code == 2
    assert "overlapping ownership" in message
    assert refusing.calls == [
        {
            "session_id": "s-1",
            "packet_id": packet_id,
            "model": "sonnet",
            "effort": "high",
            "tool_use_id": "toolu_1",
        }
    ]

    allowing = RecordingAdmission(True, "reserved")
    assert subagent_gate(freeze_dir, payload, admission=allowing) == (0, "")

    no_marker_payload = _json.dumps(
        {
            "session_id": "s-1",
            "tool_input": {"description": "no marker in this description"},
        }
    )
    # No marker: freeze-only behavior, unchanged whether or not admission
    # is wired, and admission is never even consulted.
    assert subagent_gate(freeze_dir, no_marker_payload, admission=refusing) == (
        0,
        "",
    )
    assert refusing.calls == [
        {
            "session_id": "s-1",
            "packet_id": packet_id,
            "model": "sonnet",
            "effort": "high",
            "tool_use_id": "toolu_1",
        }
    ]

    # A marker with no admission wired into this process allows through.
    assert subagent_gate(freeze_dir, payload, admission=None) == (0, "")

    # The freeze marker still takes priority over admission entirely.
    (freeze_dir / "s-1.frozen").write_text("rotation_pending: 90%\n")
    never_consulted = RecordingAdmission(True, "reserved")
    code, message = subagent_gate(freeze_dir, payload, admission=never_consulted)
    assert code == 2 and "frozen" in message
    assert never_consulted.calls == []


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


def _seed_active_cell(
    database_path: Path,
    *,
    project_key: str = "demo",
    session_id: str = "sess-1",
    cell_id: str = "cell-1",
) -> None:
    from hermes_orchestrator.db import Database

    database = Database.open(database_path)
    try:
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO project_cells("
                "cell_id, project_key, state, profile_alias, session_id, "
                "created_at, updated_at) VALUES (?, ?, 'active', 'max-a', ?, "
                "'2026-08-28T00:00:00+00:00', '2026-08-28T00:00:00+00:00')",
                (cell_id, project_key, session_id),
            )
    finally:
        database.close()


def test_hermes_command_create_accept_reject_packet_lifecycle(
    configured_repo: tuple[Path, Path],
) -> None:
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.subagent_packets import SubagentPackets

    repo_root, state_dir = configured_repo
    add = invoke(
        [
            *base_arguments(configured_repo),
            "queue-add",
            "ENG-9",
            "--project",
            "demo",
            "--priority",
            "1",
            "--operator-instruction",
            "chat-9",
        ]
    )
    assert add.exit_code == 0
    _seed_active_cell(state_dir / "state.db")

    def create_packet(worktree: str) -> str:
        result = invoke(
            [
                *base_arguments(configured_repo),
                "hermes-command",
                "--json",
                json.dumps(
                    {
                        "intent": "create_packet",
                        "issue_id": "ENG-9",
                        "model_tier": "sonnet",
                        "effort": "high",
                        "allowed_files": ["src/app.py"],
                        "worktree": worktree,
                        "red_test": "pytest -k red",
                        "verification": ["pytest -q"],
                        "invariants": "no other files change",
                        "resource_note": "one sonnet slot",
                    }
                ),
            ]
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["code"] == "accepted"
        assert payload["state"]["state"] == "planned"
        return str(payload["state"]["packet_id"])

    accept_packet_id = create_packet(str(repo_root / "accept-worktree"))
    reject_packet_id = create_packet(str(repo_root / "reject-worktree"))

    database = Database.open(state_dir / "state.db")
    try:
        packets = SubagentPackets(database, events=EventStore(database))
        packets.reserve(accept_packet_id, session_id="sess-1", tool_use_id="tu-1")
        packets.settle(accept_packet_id, outcome="completed", tool_use_id="tu-1")
        packets.reserve(reject_packet_id, session_id="sess-1", tool_use_id="tu-2")
        packets.settle(reject_packet_id, outcome="completed", tool_use_id="tu-2")
    finally:
        database.close()

    accept = invoke(
        [
            *base_arguments(configured_repo),
            "hermes-command",
            "--json",
            json.dumps(
                {
                    "intent": "accept_packet",
                    "packet_id": accept_packet_id,
                    "evidence": {"diff": "clean"},
                }
            ),
        ]
    )
    assert accept.exit_code == 0, accept.output
    accepted = json.loads(accept.stdout)
    assert accepted["code"] == "accepted"
    assert accepted["state"]["state"] == "accepted"
    assert accepted["state"]["evidence"] == {"diff": "clean"}

    reject = invoke(
        [
            *base_arguments(configured_repo),
            "hermes-command",
            "--json",
            json.dumps(
                {
                    "intent": "reject_packet",
                    "packet_id": reject_packet_id,
                    "reason": "scope creep",
                }
            ),
        ]
    )
    assert reject.exit_code == 0, reject.output
    rejected = json.loads(reject.stdout)
    assert rejected["code"] == "accepted"
    assert rejected["state"]["state"] == "rejected"

    no_cell = invoke(
        [
            *base_arguments(configured_repo),
            "hermes-command",
            "--json",
            json.dumps(
                {
                    "intent": "create_packet",
                    "issue_id": "ENG-UNKNOWN",
                    "model_tier": "sonnet",
                    "effort": "high",
                    "allowed_files": ["src/app.py"],
                    "worktree": str(repo_root),
                    "red_test": "pytest -k red",
                    "verification": ["pytest -q"],
                    "invariants": "no other files change",
                    "resource_note": "one sonnet slot",
                }
            ),
        ]
    )
    assert no_cell.exit_code == 0
    no_cell_payload = json.loads(no_cell.stdout)
    assert no_cell_payload["code"] == "rejected"
    assert "no active cell" in str(no_cell_payload["state"]["reason"])


def test_hermes_command_record_direct_exception_creates_accepted_packet(
    configured_repo: tuple[Path, Path], tmp_path: Path
) -> None:
    _repo_root, state_dir = configured_repo
    add = invoke(
        [
            *base_arguments(configured_repo),
            "queue-add",
            "ENG-9",
            "--project",
            "demo",
            "--priority",
            "1",
            "--operator-instruction",
            "chat-9",
        ]
    )
    assert add.exit_code == 0
    _seed_active_cell(state_dir / "state.db")

    # Measurement must SUCCEED before any packet is created — there is
    # no "unmeasured" fallback — so the exception's worktree must be a
    # real git checkout with a real, measurable diff against HEAD.
    worktree = tmp_path / "direct-exception-worktree"
    (worktree / "src").mkdir(parents=True)
    (worktree / "src" / "app.py").write_text("x = 1\n", encoding="utf-8")
    _verifier_git(worktree, "init", "-q")
    _verifier_git(worktree, "add", "-A")
    _verifier_git(worktree, "commit", "-qm", "init")
    (worktree / "src" / "app.py").write_text(
        "x = 1\ny = 2\nz = 3\n", encoding="utf-8"
    )

    within_scale = invoke(
        [
            *base_arguments(configured_repo),
            "hermes-command",
            "--json",
            json.dumps(
                {
                    "intent": "record_direct_exception",
                    "issue_id": "ENG-9",
                    "reason": "one-line typo fix",
                    "expected_files": ["src/app.py"],
                    "expected_lines": 5,
                    "verification": "pytest -q",
                    "worktree": str(worktree),
                }
            ),
        ]
    )
    assert within_scale.exit_code == 0, within_scale.output
    payload = json.loads(within_scale.stdout)
    assert payload["code"] == "accepted"
    assert payload["state"]["state"] == "accepted"
    assert payload["state"]["model_tier"] == "fable"
    assert payload["state"]["allowed_files"] == ["src/app.py"]
    # Updated for the CLI-side measurement requirement: the live diff is
    # now ALWAYS measured before any packet is created (no "unmeasured"
    # fallback), so evidence records the real added+deleted line count
    # measured against the worktree's actual git history.
    assert payload["state"]["evidence"] == {
        "exception_reason": "one-line typo fix",
        "expected_lines": "5",
        "measured_lines": "2",
    }

    over_scale = invoke(
        [
            *base_arguments(configured_repo),
            "hermes-command",
            "--json",
            json.dumps(
                {
                    "intent": "record_direct_exception",
                    "issue_id": "ENG-9",
                    "reason": "too much for a direct exception",
                    "expected_files": ["a.py", "b.py", "c.py"],
                    "expected_lines": 5,
                    "verification": "pytest -q",
                }
            ),
        ]
    )
    assert over_scale.exit_code == 0
    over_payload = json.loads(over_scale.stdout)
    assert over_payload["code"] == "rejected"
    assert "reviewer-fix scale" in str(over_payload["state"]["reason"])


def test_record_direct_exception_measures_the_live_diff_via_the_run_seam(
    tmp_path: Path,
) -> None:
    """The declared ``expected_files``/``expected_lines`` are never taken
    on faith when a worktree is given: ``record_direct_exception`` shells
    out (through an injectable ``run`` seam) to measure the actual git
    diff and refuses before creating any packet when the measured total
    exceeds either the fixed reviewer-fix scale or the caller's own
    declared bound; within bounds, the measurement is recorded as
    ``measured_lines`` evidence."""

    from types import SimpleNamespace

    from hermes_orchestrator.cli import _hermes_handlers
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.lead_outbox import LeadCorrectionOutbox
    from hermes_orchestrator.subagent_packets import SubagentPackets

    runtime = _runtime_like(tmp_path)
    _admit(runtime, "ENG-9", "in_development")
    _seed_active_cell(
        tmp_path / "state.db",
        project_key="demo",
        session_id="sess-1",
        cell_id="cell-1",
    )
    events = EventStore(runtime.database)
    outbox = LeadCorrectionOutbox(
        database=runtime.database,
        events=events,
        project_for_issue=lambda issue_id: "demo",
    )
    packets = SubagentPackets(runtime.database, events=events)
    settings = SimpleNamespace(repo_root=tmp_path)

    def command(**overrides: object) -> object:
        base: dict[str, object] = {
            "issue_id": "ENG-9",
            "reason": "measured exception",
            "expected_files": ["src/app.py"],
            "expected_lines": 20,
            "verification": "pytest -q",
            "worktree": "/some/worktree",
        }
        base.update(overrides)
        return SimpleNamespace(**base)

    over_bound_calls: list[list[str]] = []

    def over_bound_run(args: list[str]) -> str:
        over_bound_calls.append(list(args))
        return "15\t10\tsrc/app.py\n"  # 25 lines: over the declared cap of 20

    handlers = _hermes_handlers(settings, runtime, outbox, packets, run=over_bound_run)
    with pytest.raises(ValueError, match="measured diff bound"):
        handlers["record_direct_exception"](command())
    assert over_bound_calls == [
        [
            "git",
            "-C",
            "/some/worktree",
            "diff",
            "--numstat",
            "HEAD",
            "--",
            "src/app.py",
        ]
    ]
    # No packet was created by the refused attempt.
    assert packets.for_issue("ENG-9") == ()

    def within_bound_run(args: list[str]) -> str:
        return "5\t3\tsrc/app.py\n"  # 8 lines: within every bound

    handlers = _hermes_handlers(
        settings, runtime, outbox, packets, run=within_bound_run
    )
    result = handlers["record_direct_exception"](command())
    assert result["evidence"] == {
        "exception_reason": "measured exception",
        "expected_lines": "20",
        "measured_lines": "8",
    }


def test_record_direct_exception_refuses_before_any_packet_when_unmeasurable(
    tmp_path: Path,
) -> None:
    """Measurement must SUCCEED before any packet is created: an absent
    worktree, a failing git invocation, and unparsable numstat output
    each refuse with ``ValueError`` before ``packets.create`` runs —
    there is no "unmeasured" fallback."""

    from types import SimpleNamespace

    from hermes_orchestrator.cli import _hermes_handlers
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.lead_outbox import LeadCorrectionOutbox
    from hermes_orchestrator.subagent_packets import SubagentPackets

    runtime = _runtime_like(tmp_path)
    _admit(runtime, "ENG-9", "in_development")
    _seed_active_cell(
        tmp_path / "state.db",
        project_key="demo",
        session_id="sess-1",
        cell_id="cell-1",
    )
    events = EventStore(runtime.database)
    outbox = LeadCorrectionOutbox(
        database=runtime.database,
        events=events,
        project_for_issue=lambda issue_id: "demo",
    )
    packets = SubagentPackets(runtime.database, events=events)
    settings = SimpleNamespace(repo_root=tmp_path)

    def command(**overrides: object) -> object:
        base: dict[str, object] = {
            "issue_id": "ENG-9",
            "reason": "measured exception",
            "expected_files": ["src/app.py"],
            "expected_lines": 20,
            "verification": "pytest -q",
            "worktree": "/some/worktree",
        }
        base.update(overrides)
        return SimpleNamespace(**base)

    # 1. No worktree at all: git must never even be invoked.
    def unreached_run(args: list[str]) -> str:
        raise AssertionError("git must never be invoked without a worktree")

    handlers = _hermes_handlers(settings, runtime, outbox, packets, run=unreached_run)
    with pytest.raises(ValueError, match="worktree"):
        handlers["record_direct_exception"](command(worktree=None))
    assert packets.for_issue("ENG-9") == ()

    # 2. The numstat run itself raises (a broken git invocation).
    def raising_run(args: list[str]) -> str:
        raise RuntimeError("git binary not found")

    handlers = _hermes_handlers(settings, runtime, outbox, packets, run=raising_run)
    with pytest.raises(ValueError, match="git numstat failed"):
        handlers["record_direct_exception"](command())
    assert packets.for_issue("ENG-9") == ()

    # 3. The numstat output cannot be parsed for a declared file.
    def unparsable_run(args: list[str]) -> str:
        return "-\t-\tsrc/app.py\n"

    handlers = _hermes_handlers(settings, runtime, outbox, packets, run=unparsable_run)
    with pytest.raises(ValueError, match="unparsable"):
        handlers["record_direct_exception"](command())
    assert packets.for_issue("ENG-9") == ()

    # 4. A successful, fully measured path still records real measured_lines
    # (covered end-to-end above); confirm no stray packets were left behind
    # by any of the three refused attempts.
    assert packets.for_issue("ENG-9") == ()


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


def test_cmux_focus_requires_configuration(
    configured_repo: tuple[Path, Path],
) -> None:
    result = invoke(
        [*base_arguments(configured_repo), "cmux-focus", "--json"]
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "cmux is not configured"


def test_cmux_focus_fails_closed_from_binding_to_socket(
    configured_repo: tuple[Path, Path],
) -> None:
    from hermes_orchestrator.cmux import CmuxSurfaceRef
    from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore

    repo_root, state_dir = configured_repo
    (repo_root / "config" / "cmux.yaml").write_text(
        "cli:\n  - /usr/bin/false\n", encoding="utf-8"
    )

    missing = invoke(
        [
            *base_arguments(configured_repo),
            "cmux-focus",
            "--project",
            "demo",
            "--json",
        ]
    )
    assert missing.exit_code == 1
    assert json.loads(missing.stdout)["error"] == "no active binding"

    database = Database.open(state_dir / "state.db")
    try:
        CmuxSurfaceBindings(
            database=database, events=EventStore(database)
        ).bind_lead(
            project_key="demo",
            cell_id="cell-demo",
            session_id="11111111-1111-4111-8111-111111111111",
            profile_alias="max-a",
            ref=CmuxSurfaceRef(
                workspace_uuid="22222222-2222-4222-8222-222222222222",
                surface_uuid="33333333-3333-4333-8333-333333333333",
            ),
        )
    finally:
        database.close()

    denied = invoke(
        [
            *base_arguments(configured_repo),
            "cmux-focus",
            "--project",
            "demo",
            "--json",
        ]
    )
    # The CLI exits nonzero, so focusing fails closed and the durable
    # binding is untouched; only the error type is ever printed.
    assert denied.exit_code == 1
    assert json.loads(denied.stdout)["error"] == "CmuxUnavailable"


def _write_cmux_config(repo_root: Path) -> None:
    (repo_root / "config" / "cmux.yaml").write_text(
        "cli:\n  - /usr/bin/false\n", encoding="utf-8"
    )


def _write_profiles_config(repo_root: Path) -> None:
    (repo_root / "config" / "profiles.yaml").write_text(
        "profiles:\n"
        "  - alias: max-a\n"
        "    config_dir: profiles/max-a\n"
        "  - alias: max-b\n"
        "    config_dir: profiles/max-b\n"
        "  - alias: max-c\n"
        "    config_dir: profiles/max-c\n"
        "  - alias: max-d\n"
        "    config_dir: profiles/max-d\n",
        encoding="utf-8",
    )


def _bind_rotate_lead_cell(state_dir: Path, *, cell_id: str) -> None:
    from hermes_orchestrator.cmux import CmuxSurfaceRef
    from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore

    database = Database.open(state_dir / "state.db")
    try:
        CmuxSurfaceBindings(database=database, events=EventStore(database)).bind_lead(
            project_key="demo",
            cell_id=cell_id,
            session_id="11111111-1111-4111-8111-111111111111",
            profile_alias="max-a",
            ref=CmuxSurfaceRef(
                workspace_uuid="22222222-2222-4222-8222-222222222222",
                surface_uuid="33333333-3333-4333-8333-333333333333",
            ),
        )
    finally:
        database.close()


@dataclass
class _FakeRotationReport:
    """Mirrors the lead's ``RotationReport`` contract for INFRA-197 P2."""

    phase: str
    cell_id: str
    handoff_id: str | None
    replacement_session: str | None
    profile: str | None
    binding_id: str | None
    failure: str | None
    request_id: str | None = None


def _install_fake_lead_rotation(
    monkeypatch: pytest.MonkeyPatch, *, report: _FakeRotationReport
) -> list[dict[str, object]]:
    """Stub the CLI's lazily-imported rotation boundary.

    ``hermes_orchestrator.lead_rotation`` is concurrently authored by
    another packet in this same worktree, so the real module is never
    imported here: a fake module is injected into ``sys.modules`` ahead
    of the CLI's own ``from hermes_orchestrator.lead_rotation import
    LeadRotation``, which resolves it without touching the file on disk.
    Returns the list of constructor kwargs the CLI passed, for callers
    that want to assert on the collaborators it wired up.
    """

    calls: list[dict[str, object]] = []

    class _FakeLeadRotation:
        def __init__(self, **kwargs: object) -> None:
            calls.append(kwargs)

        async def rotate(self, cell_id: str) -> _FakeRotationReport:
            assert cell_id == report.cell_id
            return report

    fake_module = types.ModuleType("hermes_orchestrator.lead_rotation")
    fake_module.LeadRotation = _FakeLeadRotation  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "hermes_orchestrator.lead_rotation", fake_module)
    return calls


def test_rotate_lead_requires_cmux_configuration(
    configured_repo: tuple[Path, Path],
) -> None:
    result = invoke(
        [
            *base_arguments(configured_repo),
            "rotate-lead",
            "--cell",
            "cell-1",
            "--json",
        ]
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "cmux is not configured"


def test_rotate_lead_reports_a_successful_rotation(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _bind_rotate_lead_cell(state_dir, cell_id="cell-1")
    report = _FakeRotationReport(
        phase="seated",
        cell_id="cell-1",
        handoff_id="handoff-1",
        replacement_session="22222222-2222-4222-8222-222222222222",
        profile="max-b",
        binding_id="binding-1",
        failure=None,
    )
    _install_fake_lead_rotation(monkeypatch, report=report)

    result = invoke(
        [
            *base_arguments(configured_repo),
            "rotate-lead",
            "--cell",
            "cell-1",
            "--json",
        ]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["phase"] == "seated"
    assert payload["cell_id"] == "cell-1"
    assert payload["handoff_id"] == "handoff-1"
    assert payload["profile"] == "max-b"
    assert payload["binding_id"] == "binding-1"
    assert payload["failure"] is None


def test_rotate_lead_refuses_with_one_actionable_message(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _bind_rotate_lead_cell(state_dir, cell_id="cell-1")
    report = _FakeRotationReport(
        phase="refused",
        cell_id="cell-1",
        handoff_id=None,
        replacement_session=None,
        profile=None,
        binding_id=None,
        failure="no different healthy profile is available",
    )
    _install_fake_lead_rotation(monkeypatch, report=report)

    result = invoke(
        [*base_arguments(configured_repo), "rotate-lead", "--cell", "cell-1"]
    )

    assert result.exit_code == 1
    assert "no different healthy profile is available" in result.output


def test_worktree_state_fails_closed_on_a_non_git_directory(tmp_path: Path) -> None:
    from hermes_orchestrator.cli import _worktree_state

    state = _worktree_state(tmp_path)

    assert state.branch == ""
    assert state.head == ""
    assert state.origin_head == ""
    assert state.dirty is True


def test_rotate_lead_probes_the_dedicated_lead_worktree_when_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import hermes_orchestrator.cli as cli_module

    repo_root = tmp_path
    state_dir = tmp_path / "state"
    lead_worktree = tmp_path / "lead-worktree"
    lead_worktree.mkdir()
    (repo_root / "config").mkdir()
    # INFRA-214: the launch path fails closed on a missing prompt.
    (repo_root / "prompts").mkdir(exist_ok=True)
    for _name in ("claude-lead.md", "claude-harness.md"):
        (repo_root / "prompts" / _name).write_text("# prompt\n")
    (repo_root / "config/projects.yaml").write_text(
        "projects:\n"
        "  demo:\n"
        "    linear_team: ENG\n"
        f"    repo_path: {repo_root}\n"
        f"    lead_worktree: {lead_worktree}\n"
        "    integration_branch: main\n"
        "    github_repo: owner/demo\n",
        encoding="utf-8",
    )
    (repo_root / "config/policies.yaml").write_text(
        "mode: observe\nmax_unresolved_ci_merges: 2\n",
        encoding="utf-8",
    )
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _bind_rotate_lead_cell(state_dir, cell_id="cell-1")

    probed: list[Path] = []

    def _fake_worktree_state(path: Path) -> cli_module.WorktreeState:
        probed.append(path)
        return cli_module.WorktreeState(
            branch="main", head="a", origin_head="a", dirty=False
        )

    monkeypatch.setattr(cli_module, "_worktree_state", _fake_worktree_state)
    report = _FakeRotationReport(
        phase="seated",
        cell_id="cell-1",
        handoff_id="handoff-1",
        replacement_session="22222222-2222-4222-8222-222222222222",
        profile="max-b",
        binding_id="binding-1",
        failure=None,
    )
    calls = _install_fake_lead_rotation(monkeypatch, report=report)

    result = invoke(
        [
            "--repo-root",
            str(repo_root),
            "--state-dir",
            str(state_dir),
            "rotate-lead",
            "--cell",
            "cell-1",
            "--json",
        ]
    )

    assert result.exit_code == 0
    # The fake ``LeadRotation`` never calls ``worktree_state`` itself
    # (it ignores its constructor kwargs), so the closure the CLI wired
    # up is invoked here directly, exactly as the real gate would call
    # it: with the project key, not the worktree path.
    calls[0]["worktree_state"]("demo")
    assert probed == [lead_worktree]


def test_rotate_lead_seeds_the_profile_pool_before_reserving_a_replacement(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.profiles import ProfileHealth

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _bind_rotate_lead_cell(state_dir, cell_id="cell-1")

    probed: list[str] = []

    def _fake_check(self: object, alias: str) -> ProfileHealth:
        probed.append(alias)
        return ProfileHealth(
            profile_alias=alias,
            eligible=True,
            reason="eligible",
            last_checked_at=datetime.now(UTC),
        )

    monkeypatch.setattr(cli_module.ClaudeProfileProbe, "check", _fake_check)
    report = _FakeRotationReport(
        phase="seated",
        cell_id="cell-1",
        handoff_id="handoff-1",
        replacement_session="22222222-2222-4222-8222-222222222222",
        profile="max-b",
        binding_id="binding-1",
        failure=None,
    )
    _install_fake_lead_rotation(monkeypatch, report=report)

    result = invoke(
        [
            *base_arguments(configured_repo),
            "rotate-lead",
            "--cell",
            "cell-1",
            "--json",
        ]
    )

    assert result.exit_code == 0
    # Every registry profile must be health-seeded into the pool the
    # cell service receives — exactly once each — mirroring
    # open_runtime's own startup loop; otherwise every slot stays at
    # its default ineligible state and rotation can never reserve a
    # replacement.
    assert sorted(probed) == ["max-a", "max-b", "max-c", "max-d"]


def _seed_rotation_cell_state(state_dir: Path) -> None:
    """Durably seed the max-b incumbent cell, its lease, its cmux lead
    binding, and one submitted handoff for ``cell-demo``.

    ``ProjectCellService`` rehydrates the profile pool from the
    ``profile_leases`` table at construction, so the lease row is what
    lets ``rotate`` find the incumbent affinity in the one-shot
    rotate-lead command's freshly built pool.
    """

    from hermes_orchestrator.cmux import CmuxSurfaceRef
    from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.handoffs import HandoffService
    from tests.test_handoffs import valid_handoff

    database = Database.open(state_dir / "state.db")
    try:
        now = datetime.now(UTC).isoformat()
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO project_cells("
                "cell_id, project_key, state, profile_alias, session_id, "
                "created_at, updated_at) "
                "VALUES ('cell-demo', 'demo', 'active', 'max-b', ?, ?, ?)",
                ("11111111-1111-4111-8111-111111111111", now, now),
            )
            connection.execute(
                "INSERT INTO profile_leases("
                "profile_alias, project_key, state, acquired_at) "
                "VALUES ('max-b', 'demo', 'active', ?)",
                (now,),
            )
        CmuxSurfaceBindings(database=database, events=EventStore(database)).bind_lead(
            project_key="demo",
            cell_id="cell-demo",
            session_id="11111111-1111-4111-8111-111111111111",
            profile_alias="max-b",
            ref=CmuxSurfaceRef(
                workspace_uuid="22222222-2222-4222-8222-222222222222",
                surface_uuid="33333333-3333-4333-8333-333333333333",
            ),
        )
        HandoffService(database, handoff_ids=lambda: "handoff-1").submit(
            valid_handoff()
        )
    finally:
        database.close()


def _seed_acknowledged_replacement_with_lost_binding(
    state_dir: Path,
    *,
    cell_id: str = "cell-demo",
    incumbent_session: str = "11111111-1111-4111-8111-111111111111",
    incumbent_profile: str = "max-b",
    replacement_session: str = "44444444-4444-4444-8444-444444444444",
    replacement_profile: str = "max-c",
) -> None:
    """Durably seed the exact shape Sol correction a06cbce0 found refused:
    an incumbent cell still durably ``active``, its handoff already
    ``acknowledged`` with the replacement session/profile persisted, but
    whose classic lead seat was lost — never rebound to the replacement —
    so ``cmux_bindings.active_lead(cell_id)`` returns ``None``.
    """

    from uuid import UUID

    from hermes_orchestrator.cmux import CmuxSurfaceRef
    from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.handoffs import HandoffService
    from tests.test_handoffs import valid_handoff

    database = Database.open(state_dir / "state.db")
    try:
        now = datetime.now(UTC).isoformat()
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO project_cells("
                "cell_id, project_key, state, profile_alias, session_id, "
                "created_at, updated_at) "
                "VALUES (?, 'demo', 'active', ?, ?, ?, ?)",
                (cell_id, incumbent_profile, incumbent_session, now, now),
            )
            connection.execute(
                "INSERT INTO profile_leases("
                "profile_alias, project_key, state, acquired_at) "
                "VALUES (?, 'demo', 'active', ?)",
                (incumbent_profile, now),
            )
        bindings = CmuxSurfaceBindings(database=database, events=EventStore(database))
        lost = bindings.bind_lead(
            project_key="demo",
            cell_id=cell_id,
            session_id=incumbent_session,
            profile_alias=incumbent_profile,
            ref=CmuxSurfaceRef(
                workspace_uuid="22222222-2222-4222-8222-222222222222",
                surface_uuid="33333333-3333-4333-8333-333333333333",
            ),
        )
        bindings.mark_lost(lost.binding_id, reason="replacement seat failed to launch")
        handoffs = HandoffService(database, handoff_ids=lambda: "handoff-1")
        handoffs.submit(valid_handoff().model_copy(update={"cell_id": cell_id}))
        handoffs.acknowledge(
            "handoff-1",
            UUID(replacement_session),
            "Resume the rotation and finish ENG-9.",
            profile_alias=replacement_profile,
        )
    finally:
        database.close()


def _seed_capacity_observation(
    state_dir: Path,
    alias: str,
    state: str,
    *,
    observed_at: datetime,
    resets_at: datetime | None = None,
) -> None:
    """Append one durable fable-capacity observation to the CLI's database."""

    from hermes_orchestrator.db import Database

    database = Database.open(state_dir / "state.db")
    try:
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO profile_capacity_observations("
                "profile_alias, model, state, source, observed_at, resets_at"
                ") VALUES (?, 'fable', ?, ?, ?, ?)",
                (
                    alias,
                    state,
                    (
                        "provider_limit"
                        if state == "capped"
                        else "operator_attestation"
                    ),
                    observed_at.isoformat(),
                    resets_at.isoformat() if resets_at is not None else None,
                ),
            )
    finally:
        database.close()


def _seed_channel_registration(
    state_dir: Path,
    *,
    project_key: str,
    cell_id: str,
    session_id: str,
    profile_alias: str,
) -> None:
    """One active ``channel_registrations`` row, as the sidecar leaves
    behind once a freshly seated session registers (migration 0033)."""

    from hermes_orchestrator.db import Database

    database = Database.open(state_dir / "state.db")
    try:
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO channel_registrations("
                "registration_id, project_key, cell_id, session_id, "
                "profile_alias, generation, state, connected_at"
                ") VALUES (?, ?, ?, ?, ?, 1, 'active', ?)",
                (
                    f"registration-{session_id}",
                    project_key,
                    cell_id,
                    session_id,
                    profile_alias,
                    datetime.now(UTC).isoformat(),
                ),
            )
    finally:
        database.close()


def _install_rotation_process_and_probe_fakes(
    monkeypatch: pytest.MonkeyPatch,
    state_dir: Path,
) -> list[str]:
    """Fake ONLY the rotate-lead seams that reach outside the process.

    Everything else — ``_open_rotation_collaborators``'s real pool with
    its database-backed capacity evidence, the real ``LeadRotation``,
    ``ProjectCellService.rotate``, ``HandoffService``, and the durable
    rows — runs for real. Faked seams: the profile auth probe (every
    profile authenticated), the worktree gate (clean and pushed), the
    lead process runner (confirms the replacement session and
    acknowledges the handoff), and the cmux seat activation, which also
    inserts an active channel registration for the seated session so
    the completion gate sees a real registered replacement, exactly as
    the sidecar would leave behind. Returns the list of profile aliases
    the runner was asked to start a replacement on, in order.
    """

    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.claude import ClaudeEvent
    from hermes_orchestrator.profiles import ProfileHealth

    def _eligible_check(self: object, alias: str) -> ProfileHealth:
        return ProfileHealth(
            profile_alias=alias,
            eligible=True,
            reason="eligible",
            last_checked_at=datetime.now(UTC),
        )

    monkeypatch.setattr(cli_module.ClaudeProfileProbe, "check", _eligible_check)
    monkeypatch.setattr(
        cli_module,
        "_worktree_state",
        lambda path: cli_module.WorktreeState(
            branch="main", head="a", origin_head="a", dirty=False
        ),
    )

    started: list[str] = []

    class _FakeLeadRunner:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def start_lead(self, request: object) -> object:
            started.append(request.profile_alias)  # type: ignore[attr-defined]
            session_id = request.session_id  # type: ignore[attr-defined]

            async def _events():  # type: ignore[no-untyped-def]
                yield ClaudeEvent(
                    kind="session.started",
                    original_type="system",
                    session_id=session_id,
                    parent_tool_use_id=None,
                    timestamp="2026-08-30T00:00:00Z",
                    usage={},
                )
                yield ClaudeEvent(
                    kind="handoff.acknowledged",
                    original_type="result",
                    session_id=session_id,
                    parent_tool_use_id=None,
                    timestamp="2026-08-30T00:00:01Z",
                    usage={},
                    restated_next_action=(
                        "Run the failing test and correct ENG-9."
                    ),
                )

            return _events()

        async def retire_session(self, session_id: object) -> None:
            return None

    monkeypatch.setattr(cli_module, "ClaudeRunner", _FakeLeadRunner)

    async def _fake_ensure(self: object, **kwargs: object) -> object:
        _seed_channel_registration(
            state_dir,
            project_key=str(kwargs["project_key"]),
            cell_id=str(kwargs["cell_id"]),
            session_id=str(kwargs["session_id"]),
            profile_alias=str(kwargs["profile_alias"]),
        )
        return types.SimpleNamespace(binding_id="binding-rotated")

    monkeypatch.setattr(cli_module.CmuxLeadSeater, "ensure", _fake_ensure)
    return started


def test_rotate_lead_skips_a_fable_capped_profile_and_rotates_to_the_next(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """INFRA-205 regression 3 (and 4): max-a is authenticated but holds a
    non-expired fable cap, max-b is the incumbent, max-c holds current
    available evidence — the real rotate-lead path must select max-c,
    complete the existing confirmation/transfer/seat sequence, and never
    start a replacement process on the capped profile."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    now = datetime.now(UTC)
    _seed_capacity_observation(
        state_dir,
        "max-a",
        "capped",
        observed_at=now - timedelta(hours=1),
        resets_at=now + timedelta(hours=11),
    )
    _seed_capacity_observation(
        state_dir, "max-c", "available", observed_at=now - timedelta(hours=1)
    )
    started = _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)

    result = invoke(
        [
            *base_arguments(configured_repo),
            "rotate-lead",
            "--cell",
            "cell-demo",
            "--json",
        ]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["phase"] == "complete"
    assert payload["profile"] == "max-c"
    assert payload["binding_id"] == "binding-rotated"
    assert payload["failure"] is None
    # Regression 4: the capped profile never saw a process launch — the
    # only replacement start was on the selected healthy profile.
    assert started == ["max-c"]

    database = Database.open(state_dir / "state.db")
    try:
        cell = database.execute(
            "SELECT state, profile_alias, session_id FROM project_cells "
            "WHERE cell_id = 'cell-demo'"
        ).fetchone()
        assert str(cell["state"]) == "active"
        assert str(cell["profile_alias"]) == "max-c"
        assert str(cell["session_id"]) == payload["replacement_session"]
        handoff = database.execute(
            "SELECT state, replacement_profile_alias FROM handoffs "
            "WHERE handoff_id = 'handoff-1'"
        ).fetchone()
        assert str(handoff["state"]) == "acknowledged"
        assert str(handoff["replacement_profile_alias"]) == "max-c"
        lease = database.execute(
            "SELECT profile_alias FROM profile_leases WHERE project_key = 'demo'"
        ).fetchone()
        assert str(lease["profile_alias"]) == "max-c"
    finally:
        database.close()


def test_rotate_lead_fails_closed_when_every_replacement_is_fable_capped(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """INFRA-205 regression 5 (and 4): with every candidate fable-capped
    the rotation refuses with ONE actionable reason naming the capacity
    evidence, launches no replacement process, and leaves the incumbent
    cell, handoff, lease, and cmux binding untouched."""

    from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    now = datetime.now(UTC)
    for alias in ("max-a", "max-c", "max-d"):
        _seed_capacity_observation(
            state_dir,
            alias,
            "capped",
            observed_at=now - timedelta(hours=1),
            resets_at=now + timedelta(hours=11),
        )
    started = _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)

    result = invoke(
        [
            *base_arguments(configured_repo),
            "rotate-lead",
            "--cell",
            "cell-demo",
            "--json",
        ]
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["phase"] == "transfer"
    failure = payload["failure"]
    # One actionable reason, and the capacity evidence is named in it.
    assert failure.startswith("no different healthy profile is available: ")
    assert "max-a: fable-capped until" in failure
    assert "max-c: fable-capped until" in failure
    assert "max-d: fable-capped until" in failure
    # Regression 4: selection refusal precedes any runner start.
    assert started == []

    database = Database.open(state_dir / "state.db")
    try:
        cell = database.execute(
            "SELECT state, profile_alias, session_id FROM project_cells "
            "WHERE cell_id = 'cell-demo'"
        ).fetchone()
        assert str(cell["state"]) == "active"
        assert str(cell["profile_alias"]) == "max-b"
        assert str(cell["session_id"]) == "11111111-1111-4111-8111-111111111111"
        handoff = database.execute(
            "SELECT state, replacement_session_id, replacement_profile_alias "
            "FROM handoffs WHERE handoff_id = 'handoff-1'"
        ).fetchone()
        assert str(handoff["state"]) == "submitted"
        assert handoff["replacement_session_id"] is None
        assert handoff["replacement_profile_alias"] is None
        lease = database.execute(
            "SELECT profile_alias FROM profile_leases WHERE project_key = 'demo'"
        ).fetchone()
        assert str(lease["profile_alias"]) == "max-b"
        binding = CmuxSurfaceBindings(
            database=database, events=EventStore(database)
        ).active_lead("cell-demo")
        assert binding is not None
        assert binding.session_id == "11111111-1111-4111-8111-111111111111"
    finally:
        database.close()


def test_rotate_lead_resumes_an_acknowledged_replacement_when_the_seat_was_lost(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sol correction a06cbce0: a lost (or never re-bound) classic lead
    seat must not refuse rotate-lead when the durable cell is still live
    and its newest handoff already names the exact replacement to
    reseat. The CLI derives the project from the durable cell row
    instead of the (absent) active binding, and LeadRotation's existing
    acknowledged-handoff recovery reconstructs the replacement without
    reselecting capacity or launching a fresh lead process."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_acknowledged_replacement_with_lost_binding(state_dir)
    started = _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)

    result = invoke(
        [
            *base_arguments(configured_repo),
            "rotate-lead",
            "--cell",
            "cell-demo",
            "--json",
        ]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["phase"] == "complete"
    assert payload["profile"] == "max-c"
    assert payload["replacement_session"] == "44444444-4444-4444-8444-444444444444"
    assert payload["failure"] is None
    # Recovery reconstructs the identities persisted at acknowledgement;
    # it never calls the lead runner to start a fresh replacement.
    assert started == []

    database = Database.open(state_dir / "state.db")
    try:
        cell = database.execute(
            "SELECT state, profile_alias, session_id FROM project_cells "
            "WHERE cell_id = 'cell-demo'"
        ).fetchone()
        assert str(cell["state"]) == "active"
        assert str(cell["profile_alias"]) == "max-c"
        assert str(cell["session_id"]) == "44444444-4444-4444-8444-444444444444"
        handoff = database.execute(
            "SELECT state, replacement_session_id, replacement_profile_alias "
            "FROM handoffs WHERE handoff_id = 'handoff-1'"
        ).fetchone()
        assert str(handoff["state"]) == "acknowledged"
        assert str(handoff["replacement_session_id"]) == (
            "44444444-4444-4444-8444-444444444444"
        )
        assert str(handoff["replacement_profile_alias"]) == "max-c"
        lease = database.execute(
            "SELECT profile_alias FROM profile_leases WHERE project_key = 'demo'"
        ).fetchone()
        assert str(lease["profile_alias"]) == "max-c"
    finally:
        database.close()


def test_rotate_lead_retry_after_seating_the_recovery_reuses_the_same_binding(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recovery completes and reports once; re-running rotate-lead after
    that complete report is a NEW rotation request: the consumed handoff
    never satisfies it — no second transfer or seat activation — and the
    rotation reports awaiting the fresh handoff it just requested
    (Sol 3c1651df / 52d15493 — the in-flight dedup window before the
    complete report is pinned by the lead-rotation suite instead)."""

    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.cmux import CmuxSurfaceRef
    from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_acknowledged_replacement_with_lost_binding(state_dir)
    _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)

    ensure_calls: list[str] = []

    async def _persisting_ensure(self: object, **kwargs: object) -> object:
        # Unlike the shared fake, this seat activation durably rebinds
        # the classic lead (as the real ``CmuxLeadSeater`` does), so a
        # retry's ``active_lead`` read can find it and skip reactivating.
        ensure_calls.append(str(kwargs["session_id"]))
        database = Database.open(state_dir / "state.db")
        try:
            binding = CmuxSurfaceBindings(
                database=database, events=EventStore(database)
            ).bind_lead(
                project_key=str(kwargs["project_key"]),
                cell_id=str(kwargs["cell_id"]),
                session_id=str(kwargs["session_id"]),
                profile_alias=str(kwargs["profile_alias"]),
                ref=CmuxSurfaceRef(
                    workspace_uuid="55555555-5555-4555-8555-555555555555",
                    surface_uuid="66666666-6666-4666-8666-666666666666",
                ),
            )
        finally:
            database.close()
        _seed_channel_registration(
            state_dir,
            project_key=str(kwargs["project_key"]),
            cell_id=str(kwargs["cell_id"]),
            session_id=str(kwargs["session_id"]),
            profile_alias=str(kwargs["profile_alias"]),
        )
        return binding

    monkeypatch.setattr(cli_module.CmuxLeadSeater, "ensure", _persisting_ensure)

    first = invoke(
        [
            *base_arguments(configured_repo),
            "rotate-lead",
            "--cell",
            "cell-demo",
            "--json",
        ]
    )
    assert first.exit_code == 0
    first_payload = json.loads(first.stdout)
    assert first_payload["phase"] == "complete"
    assert first_payload["profile"] == "max-c"

    second = invoke(
        [
            *base_arguments(configured_repo),
            "rotate-lead",
            "--cell",
            "cell-demo",
            "--json",
        ]
    )

    # Sol 3c1651df: the first invocation REPORTED complete, consuming the
    # handoff. The retry is a new rotation request: the stale handoff never
    # satisfies it and nothing re-transfers — and instead of a terminal
    # refusal the rotation files the fresh durable handoff request and
    # reports awaiting (Sol 52d15493).
    assert second.exit_code == 0
    second_payload = json.loads(second.stdout)
    assert second_payload["ok"] is False
    assert second_payload["phase"] == "awaiting_handoff"
    assert second_payload["failure"] is None
    assert second_payload["request_id"] is not None
    # Seat activation still ran exactly once across both invocations.
    assert ensure_calls == [first_payload["replacement_session"]]

    database = Database.open(state_dir / "state.db")
    try:
        leases = database.execute(
            "SELECT profile_alias FROM profile_leases WHERE project_key = 'demo'"
        ).fetchall()
        assert [str(row["profile_alias"]) for row in leases] == ["max-c"]
    finally:
        database.close()


def test_rotate_lead_refuses_with_neither_an_active_binding_nor_a_live_cell(
    configured_repo: tuple[Path, Path],
) -> None:
    """Nothing to derive project identity from — no active binding, and
    no durable cell row at all — must still refuse cleanly rather than
    guess or crash."""

    repo_root, _state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)

    result = invoke(
        [
            *base_arguments(configured_repo),
            "rotate-lead",
            "--cell",
            "cell-nowhere",
            "--json",
        ]
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "no active lead binding for this cell"


def test_rotate_lead_refuses_when_the_durable_cell_is_in_a_terminal_state(
    configured_repo: tuple[Path, Path],
) -> None:
    """A durable cell row that exists but is no longer live (e.g. it
    already failed) must not be treated as a resumable rotation target
    just because it names a project — the refusal from a missing
    binding still applies."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    database = Database.open(state_dir / "state.db")
    try:
        now = datetime.now(UTC).isoformat()
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO project_cells("
                "cell_id, project_key, state, profile_alias, session_id, "
                "created_at, updated_at) "
                "VALUES ('cell-failed', 'demo', 'failed', 'max-b', ?, ?, ?)",
                ("11111111-1111-4111-8111-111111111111", now, now),
            )
    finally:
        database.close()

    result = invoke(
        [
            *base_arguments(configured_repo),
            "rotate-lead",
            "--cell",
            "cell-failed",
            "--json",
        ]
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "no active lead binding for this cell"


def _install_fake_lane_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    *,
    dispatch_result: object | None = None,
    dispatch_should_be_called: bool = True,
    active_cell: object | None = None,
) -> list[dict[str, object]]:
    """Stub ``_open_rotation_collaborators`` so ``start-lane`` tests never
    touch the real cmux/profile-pool machinery ``rotate-lead``'s own
    fixtures exercise elsewhere.

    Sol correction 110ed759 / INFRA-219 R1: these tests are about the
    CLI boundary's own harness-run validation and exit-code mapping,
    not about ``ProjectCellService``'s internals (those are L3's own
    ``tests/test_cells.py`` regressions) -- a fake ``dispatch`` that
    records its kwargs and returns a fixed ``DispatchResult`` isolates
    exactly that boundary. ``dispatch_should_be_called=False`` asserts
    the CLI's own pre-dispatch guard actually fires -- a validation
    that only happened to work because ``dispatch`` itself refused
    would not prove the CLI-side guard exists.

    ``active_cell`` supplies the durable cell row this lane already
    holds (default: none), so the adoption branch can be exercised.
    """

    import hermes_orchestrator.cli as cli_module

    calls: list[dict[str, object]] = []

    class _FakeCells:
        def active_cell(self, project_key: str, lane_role: str) -> object | None:
            return active_cell

        async def dispatch(
            self,
            issue_id: str,
            *,
            lane_role: str,
            harness_run: str | None = None,
            **extra: object,
        ) -> object:
            # ``**extra`` absorbs lane arguments later packets add (R7's
            # harness_project_key), so this CLI-boundary fake never has
            # to track ProjectCellService.dispatch's full signature.
            calls.append(
                {
                    "issue_id": issue_id,
                    "lane_role": lane_role,
                    "harness_run": harness_run,
                    **extra,
                }
            )
            if not dispatch_should_be_called:
                raise AssertionError(
                    "dispatch must not be called when the CLI boundary "
                    "guard should have already refused"
                )
            assert dispatch_result is not None
            return dispatch_result

    monkeypatch.setattr(
        cli_module,
        "_open_rotation_collaborators",
        lambda settings, runtime, *, lane_role="development", **extra: (
            _FakeCells(),
            None,
        ),
    )
    return calls


def _harness_cell(session_id: str) -> object:
    """The durable harness-lane cell ``active_cell`` hands back."""

    import uuid

    from hermes_orchestrator.cells import HARNESS_LANE, ProjectCell

    return ProjectCell(
        cell_id="cell-harness",
        project_key="demo",
        state="active",
        profile_alias="max-a",
        session_id=uuid.UUID(session_id),
        lane_role=HARNESS_LANE,
    )


def test_start_lane_refuses_an_active_cell_whose_worker_is_dead(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """INFRA-198 (observed live): cmux workspace C39EBCDC / surface
    F5B1BD55 still existed, but ``claude --resume 9b539c86`` had exited
    with "No conversation found" and the pane was a bare shell prompt.
    ``start-lane --lane harness`` returned ``already_running`` and
    exited 0 purely on ``cells.active_cell(...)``, with no liveness
    test at all -- so the operator's only signal was that the lane was
    fine while it was in fact down. A DEFINITIVELY absent worker must
    refuse instead, nonzero, naming the dead session, and must never
    reach ``dispatch``."""

    import hermes_orchestrator.cli as cli_module

    repo_root, _state_dir = configured_repo
    _write_cmux_config(repo_root)
    session_id = "9b539c86-c52f-43b2-b077-57491066ebcf"
    calls = _install_fake_lane_dispatch(
        monkeypatch,
        dispatch_should_be_called=False,
        active_cell=_harness_cell(session_id),
    )
    monkeypatch.setattr(
        cli_module, "managed_claude_worker_alive", lambda _session_id: False
    )

    result = invoke(
        [
            *base_arguments(configured_repo),
            "start-lane",
            "--project",
            "demo",
            "--lane",
            "harness",
            "--harness-run",
            "run-1",
            "--json",
        ]
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload.get("status") != "already_running"
    assert payload["error"] == "dead_worker"
    assert payload["session_id"] == session_id
    assert calls == []

    human_result = invoke(
        [
            *base_arguments(configured_repo),
            "start-lane",
            "--project",
            "demo",
            "--lane",
            "harness",
            "--harness-run",
            "run-1",
        ]
    )
    assert human_result.exit_code == 1
    assert session_id in human_result.output


@pytest.mark.parametrize("worker_alive", [True, None])
def test_start_lane_still_adopts_a_cell_whose_worker_is_not_provably_dead(
    configured_repo: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    worker_alive: bool | None,
) -> None:
    """The other half of the tri-state: a running worker (``True``) and
    an UNMEASURABLE one (``None`` -- ``ps`` failed or matched
    ambiguously) are both still adopted and reported
    ``already_running``. Refusing on an unknown would break every
    healthy re-run of ``start-lane`` on a transient ``ps`` hiccup."""

    import hermes_orchestrator.cli as cli_module

    repo_root, _state_dir = configured_repo
    _write_cmux_config(repo_root)
    session_id = "9b539c86-c52f-43b2-b077-57491066ebcf"
    _install_fake_lane_dispatch(
        monkeypatch,
        dispatch_should_be_called=False,
        active_cell=_harness_cell(session_id),
    )
    monkeypatch.setattr(
        cli_module, "managed_claude_worker_alive", lambda _session_id: worker_alive
    )

    result = invoke(
        [
            *base_arguments(configured_repo),
            "start-lane",
            "--project",
            "demo",
            "--lane",
            "harness",
            "--harness-run",
            "run-1",
            "--json",
        ]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "already_running"
    assert payload["session_id"] == session_id


def test_start_lane_harness_without_harness_run_refuses_nonzero(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sol correction 110ed759 / INFRA-219 R1: L3 made ``harness_run``
    mandatory for a harness dispatch, but ``start-lane`` never carried
    the argument and the exit line only mapped
    ``waiting_for_profile``/``start_failed`` to nonzero -- so a harness
    start refused and still exited 0. ``--lane harness`` without
    ``--harness-run`` must now refuse at the CLI boundary itself,
    before ``dispatch`` is ever called, with a clear message and a
    nonzero exit."""

    repo_root, _state_dir = configured_repo
    _write_cmux_config(repo_root)
    calls = _install_fake_lane_dispatch(monkeypatch, dispatch_should_be_called=False)

    result = invoke(
        [
            *base_arguments(configured_repo),
            "start-lane",
            "--project",
            "demo",
            "--lane",
            "harness",
            "--issue",
            "ENG-1",
            "--json",
        ]
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "harness_run_required"
    assert calls == []

    human_result = invoke(
        [
            *base_arguments(configured_repo),
            "start-lane",
            "--project",
            "demo",
            "--lane",
            "harness",
            "--issue",
            "ENG-1",
        ]
    )
    assert human_result.exit_code == 1
    assert "--harness-run" in human_result.output


def test_start_lane_development_with_harness_run_refuses_nonzero(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The mirror refusal: a development-lane start must never carry a
    harness-run identity -- development occupancy is proven by explicit
    issue admission alone (Sol correction 110ed759 / INFRA-219 L3,
    R1)."""

    repo_root, _state_dir = configured_repo
    _write_cmux_config(repo_root)
    calls = _install_fake_lane_dispatch(monkeypatch, dispatch_should_be_called=False)

    result = invoke(
        [
            *base_arguments(configured_repo),
            "start-lane",
            "--project",
            "demo",
            "--lane",
            "development",
            "--issue",
            "ENG-1",
            "--harness-run",
            "run-1",
            "--json",
        ]
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "harness_run_not_permitted"
    assert calls == []


def test_start_lane_dispatch_refusal_status_exits_nonzero(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression for the actual defect: the OLD exit line
    (``return 0 if result.status not in ("waiting_for_profile",
    "start_failed") else 1``) was a deny-list, so a refusal status it
    never anticipated -- like ``project_busy`` or L3's own
    ``harness_run_required`` -- silently exited 0. The new allow-list
    must exit nonzero for ANY status that is not a genuine start/adopt
    outcome; ``project_busy`` is used here as the easiest refusal to
    force through the fake dispatch without standing up real
    project-occupancy state."""

    from hermes_orchestrator.cells import DispatchResult

    repo_root, _state_dir = configured_repo
    _write_cmux_config(repo_root)
    _install_fake_lane_dispatch(
        monkeypatch,
        dispatch_result=DispatchResult(status="project_busy", issue_id="ENG-1"),
    )

    result = invoke(
        [
            *base_arguments(configured_repo),
            "start-lane",
            "--project",
            "demo",
            "--lane",
            "development",
            "--issue",
            "ENG-1",
            "--json",
        ]
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["status"] == "project_busy"


def test_start_lane_working_status_exits_zero(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The allow-list's positive case: a genuine ``working`` dispatch
    outcome (a lead ran a turn) still exits 0, so R1's fix is not
    merely a nonzero-everything regression."""

    from hermes_orchestrator.cells import DispatchResult

    repo_root, _state_dir = configured_repo
    _write_cmux_config(repo_root)
    calls = _install_fake_lane_dispatch(
        monkeypatch,
        dispatch_result=DispatchResult(
            status="working", issue_id="ENG-1", cell_id="cell-1"
        ),
    )

    result = invoke(
        [
            *base_arguments(configured_repo),
            "start-lane",
            "--project",
            "demo",
            "--lane",
            "harness",
            "--issue",
            "ENG-1",
            "--harness-run",
            "run-1",
            "--json",
        ]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "working"
    assert calls == [
        {"issue_id": "ENG-1", "lane_role": "harness", "harness_run": "run-1"}
    ]


def test_start_lane_json_emits_session_id_as_string(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """INFRA-198 blocker 2 (observed live 2026-09-01): ``start-lane``
    built its ``--json`` payload with ``dataclasses.asdict(result)``,
    which preserves ``DispatchResult.session_id``'s ``UUID`` object, so
    ``_print``'s ``json.dumps`` raised ``TypeError: Object of type UUID
    is not JSON serializable``. ``cells.dispatch`` had already run --
    the cell was created and the seat launched -- so a succeeded
    command reported as a traceback with a nonzero exit, and the
    obvious operator retry risked a duplicate lane. The real CLI path
    must now emit a parseable object whose ``session_id`` is the
    identifier's string form, under the same key."""

    import uuid

    from hermes_orchestrator.cells import DispatchResult

    session_id = uuid.UUID("9b539c86-c52f-43b2-b077-57491066ebcf")
    repo_root, _state_dir = configured_repo
    _write_cmux_config(repo_root)
    _install_fake_lane_dispatch(
        monkeypatch,
        dispatch_result=DispatchResult(
            status="working",
            issue_id="ENG-1",
            cell_id="8369559d-f4cd-45ca-aa43-aae412853f16",
            session_id=session_id,
        ),
    )

    result = invoke(
        [
            *base_arguments(configured_repo),
            "start-lane",
            "--project",
            "demo",
            "--lane",
            "harness",
            "--issue",
            "ENG-1",
            "--harness-run",
            "run-1",
            "--json",
        ]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["session_id"] == str(session_id)
    assert isinstance(payload["session_id"], str)


def test_start_lane_json_keeps_absent_session_id_null(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The coercion must not stringify absence: a refusal carries no
    session, and ``session_id`` has to stay JSON ``null`` rather than
    becoming the string ``"None"`` -- a consumer testing the field for
    presence would read that as a real identifier."""

    from hermes_orchestrator.cells import DispatchResult

    repo_root, _state_dir = configured_repo
    _write_cmux_config(repo_root)
    _install_fake_lane_dispatch(
        monkeypatch,
        dispatch_result=DispatchResult(status="project_busy", issue_id="ENG-1"),
    )

    result = invoke(
        [
            *base_arguments(configured_repo),
            "start-lane",
            "--project",
            "demo",
            "--lane",
            "development",
            "--issue",
            "ENG-1",
            "--json",
        ]
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["session_id"] is None


def test_print_json_falls_back_to_str_for_unencodable_values() -> None:
    """INFRA-198 blocker 2, second half: ``_print`` is the last step of
    commands whose effects have already committed, so it must never be
    the thing that raises. A payload carrying a value ``json.dumps``
    cannot natively encode -- a ``UUID`` is the one observed live --
    has to render as its string form instead of aborting the
    command."""

    import uuid

    from hermes_orchestrator.cli import _print

    session_id = uuid.UUID("9b539c86-c52f-43b2-b077-57491066ebcf")
    stdout = StringIO()
    with redirect_stdout(stdout):
        _print(
            {"session_id": session_id, "status": "working"},
            json_output=True,
            human="unused",
        )

    payload = json.loads(stdout.getvalue())
    assert payload == {"session_id": str(session_id), "status": "working"}


def test_start_lane_parses_its_existing_flags(
    configured_repo: tuple[Path, Path],
) -> None:
    """Argparse-level: ``start-lane`` still accepts its pre-R1 flags
    (``--project``, ``--lane``, ``--issue``, ``--json``) unchanged --
    the new ``--harness-run`` argument is additive, not a replacement.
    No cmux configuration is written, so this exercises argument
    parsing through to the CLI's own early cmux-configured check."""

    result = invoke(
        [
            *base_arguments(configured_repo),
            "start-lane",
            "--project",
            "demo",
            "--lane",
            "development",
            "--issue",
            "ENG-1",
            "--json",
        ]
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "cmux is not configured"


def test_open_rotation_collaborators_wires_channel_launch_and_trust_with_node(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """INFRA-207: the one-shot rotate-lead seater must carry the same
    channel launcher and trust confirmer the live daemon composes —
    otherwise a rotated-in lead is seated without the hermes-control
    channel."""

    import hermes_orchestrator.cli as cli_module
    import hermes_orchestrator.runtime as runtime_module
    from hermes_orchestrator.config import load_settings
    from hermes_orchestrator.runtime import open_runtime

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    fake_node = repo_root / "fake-node"
    fake_node.write_text("#!/bin/sh\n", encoding="utf-8")
    monkeypatch.setattr(
        runtime_module.shutil,
        "which",
        lambda name: str(fake_node) if name == "node" else None,
    )

    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(settings, enable_live=False)
    try:
        _cells, seater = cli_module._open_rotation_collaborators(settings, runtime)
        assert seater._channel_launch is not None
        assert seater._channel_trust is not None
    finally:
        runtime.close()


def test_open_rotation_collaborators_composes_bootstrap_with_managed_repo_paths(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """INFRA-198 (Sol correction a06cbce0): the one-shot rotate-lead
    seater must gate profile readiness on the same deterministic
    onboarding/theme/trust bootstrap the live daemon needs, wired with
    exactly the managed repository path a seated lead actually runs
    from — the project's ``lead_worktree`` when configured, else its
    ``repo_path`` — otherwise a restored session can still stop on a
    first-run dialog instead of Hermes's handoff."""

    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.config import load_settings
    from hermes_orchestrator.runtime import open_runtime

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)

    captured_repo_paths: list[tuple[object, ...]] = []

    class _FakeBootstrap:
        def __init__(self, registry: object, *, repo_paths: object) -> None:
            captured_repo_paths.append(tuple(repo_paths))

        def ensure(self, alias: str) -> bool:
            return True

    monkeypatch.setattr(cli_module, "ProfileBootstrap", _FakeBootstrap)

    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(settings, enable_live=False)
    try:
        cli_module._open_rotation_collaborators(settings, runtime)
    finally:
        runtime.close()

    assert len(captured_repo_paths) == 1
    project = settings.projects["demo"]
    expected = project.lead_cwd
    assert captured_repo_paths[0] == (expected,)


def test_open_rotation_collaborators_agrees_bootstrap_and_seat_paths_on_lead_worktree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sol correction c5600e31: bootstrap trust and the ``project_paths``
    handed to the seater/cell service must derive from the same
    canonical ``project.lead_cwd`` — with a dedicated ``lead_worktree``
    configured, both must agree on the worktree, never a mix of
    worktree-for-bootstrap and repo_path-for-the-seat that would still
    land an eligible profile on the repository trust prompt."""

    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.config import load_settings
    from hermes_orchestrator.runtime import open_runtime

    repo_root = tmp_path
    state_dir = tmp_path / "state"
    lead_worktree = tmp_path / "lead-worktree"
    lead_worktree.mkdir()
    (repo_root / "config").mkdir()
    # INFRA-214: the launch path fails closed on a missing prompt.
    (repo_root / "prompts").mkdir(exist_ok=True)
    for _name in ("claude-lead.md", "claude-harness.md"):
        (repo_root / "prompts" / _name).write_text("# prompt\n")
    (repo_root / "config/projects.yaml").write_text(
        "projects:\n"
        "  demo:\n"
        "    linear_team: ENG\n"
        f"    repo_path: {repo_root}\n"
        f"    lead_worktree: {lead_worktree}\n"
        "    integration_branch: main\n"
        "    github_repo: owner/demo\n",
        encoding="utf-8",
    )
    (repo_root / "config/policies.yaml").write_text(
        "mode: observe\nmax_unresolved_ci_merges: 2\n",
        encoding="utf-8",
    )
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)

    captured_repo_paths: list[tuple[object, ...]] = []

    class _FakeBootstrap:
        def __init__(self, registry: object, *, repo_paths: object) -> None:
            captured_repo_paths.append(tuple(repo_paths))

        def ensure(self, alias: str) -> bool:
            return True

    monkeypatch.setattr(cli_module, "ProfileBootstrap", _FakeBootstrap)

    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(settings, enable_live=False)
    try:
        cells, seater = cli_module._open_rotation_collaborators(settings, runtime)
    finally:
        runtime.close()

    assert captured_repo_paths == [(lead_worktree,)]
    assert seater._project_paths == {"demo": lead_worktree}
    assert cells._project_paths == {"demo": lead_worktree}


def test_migration_env_provision_plans_dry_by_default(tmp_path: Path) -> None:
    result = invoke(
        [
            "migration-env",
            "provision",
            "--target-repo",
            str(tmp_path),
            "--slug",
            "infra-189",
        ]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["executed"] is False
    run = next(s for s in payload["plan"] if s["code"] == "container_run")
    # The disposable container may only ever publish on loopback.
    assert "127.0.0.1:5439:5432" in run["argv"]


def test_migration_env_gate_preview_lists_deterministic_checks(
    tmp_path: Path,
) -> None:
    result = invoke(
        [
            "migration-env",
            "gate",
            "--target-repo",
            str(tmp_path),
            "--slug",
            "infra-189",
        ]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["executed"] is False
    assert payload["checks"][0] == "repo_head_resolved"
    assert payload["checks"][-1] == "repo_head_stable"
    assert "corpus_synthetic_seed" in payload["checks"]
    assert "historical_schema_authority" in payload["checks"]


def test_migration_env_gate_execute_requires_a_verdict_path(
    tmp_path: Path,
) -> None:
    result = invoke(
        [
            "migration-env",
            "gate",
            "--target-repo",
            str(tmp_path),
            "--slug",
            "infra-189",
            "--execute",
        ]
    )

    assert result.exit_code == 2
    assert "requires --out" in result.stderr


def test_migration_env_handoff_names_env_vars_but_never_values(
    tmp_path: Path,
) -> None:
    out = tmp_path / "handoff.md"
    result = invoke(
        [
            "migration-env",
            "handoff",
            "--target-repo",
            str(tmp_path / "worktree"),
            "--slug",
            "infra-189",
            "--out",
            str(out),
        ]
    )

    assert result.exit_code == 0
    text = out.read_text()
    assert "jo_local_fable_infra_189_source" in text
    assert "DATABASE_URL_READ_ONLY_STAGE" in text
    assert "reset-staging" in text
    # Env var names only: no connection string ever lands in the doc.
    assert "postgresql://" not in text


def test_migration_env_mark_stamps_only_a_linked_worktree(
    tmp_path: Path,
) -> None:
    from tests.test_migration_env import make_linked_worktree

    primary, worktree = make_linked_worktree(tmp_path)

    refused = invoke(
        [
            "migration-env",
            "mark",
            "--target-repo",
            str(primary),
            "--slug",
            "infra-189",
        ]
    )
    assert refused.exit_code == 1
    assert "primary checkout" in refused.stderr
    assert not (primary / ".fable-isolated-worktree").exists()

    result = invoke(
        [
            "migration-env",
            "mark",
            "--target-repo",
            str(worktree),
            "--slug",
            "infra-189",
        ]
    )
    assert result.exit_code == 0
    marker = (worktree / ".fable-isolated-worktree").read_text()
    assert "slug=infra_189" in marker
    assert "repository=" in marker


@pytest.mark.asyncio
async def test_daemon_starts_when_intake_probes_fail(
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.cmux import CmuxUnavailable
    from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.lead_intake import LeadIntakeRouter
    from tests.test_lead_intake import (
        RecordingPort,
        seat,
        seed_active_cell,
        seed_packets,
        transport,
    )

    database = Database.open(tmp_path / "state.db")
    try:
        bindings = CmuxSurfaceBindings(
            database=database, events=EventStore(database)
        )
        seed_packets(database)
        seed_active_cell(database)
        seat(bindings)
        port = RecordingPort()
        port.probe_error = CmuxUnavailable("cmux command timed out")
        router = LeadIntakeRouter(
            database=database,
            transport=transport(database, bindings, port),
        )
        service = FakeService()

        supervisor = await _run_daemon(
            service, once=True, interval=60, lead_intake=router
        )

        # The optional intake channel timing out never blocks startup
        # or the supervised tick; nothing was typed and the durable
        # packets stayed pending for the next pass.
        assert service.ticks == 1
        assert supervisor is not None
        assert port.notifications == []
        pending = database.scalar(
            "SELECT count(*) FROM lead_corrections WHERE state = 'pending'"
        )
        assert int(pending) == 1
    finally:
        database.close()


def test_intake_poll_offers_and_only_explicit_ack_delivers(
    tmp_path: Path,
) -> None:
    import re

    from hermes_orchestrator.db import Database
    from tests.test_lead_intake import (
        SESSION,
        seed_active_cell,
        seed_packets,
    )

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    database = Database.open(state_dir / "state.db")
    try:
        seed_packets(database)
        seed_active_cell(database)
    finally:
        database.close()
    base = ["--state-dir", str(state_dir)]
    poll_args = [*base, "intake-poll", "--session", SESSION]

    first = invoke(poll_args)
    assert first.exit_code == 0
    payload = json.loads(first.stdout)
    assert payload["decision"] == "block"
    match = re.search(
        r"(HERMES_\w+ [0-9a-f]{32}) — Hermes intake offer ([0-9a-f]{32})",
        payload["reason"],
    )
    assert match is not None
    envelope, token = match.group(1), match.group(2)
    packet_id = envelope.split()[1]

    # Without an acknowledgement the packet is never delivered: after
    # the second packet is also leased, further polls go quiet, but
    # nothing is marked delivered.
    second = invoke(poll_args)
    assert second.exit_code == 0 and second.stdout
    third = invoke(poll_args)
    assert third.exit_code == 0 and third.stdout == ""

    # A wrong acknowledgement changes nothing and exits nonzero.
    bad = invoke(
        [
            *base,
            "intake-ack",
            "--session",
            SESSION,
            "--packet",
            packet_id,
            "--offer",
            "0" * 32,
        ]
    )
    assert bad.exit_code == 1

    # The exact acknowledgement records the delivery once.
    good = invoke(
        [
            *base,
            "intake-ack",
            "--session",
            SESSION,
            "--packet",
            packet_id,
            "--offer",
            token,
        ]
    )
    assert good.exit_code == 0
    assert json.loads(good.stdout)["acknowledged"] == packet_id
    duplicate = invoke(
        [
            *base,
            "intake-ack",
            "--session",
            SESSION,
            "--packet",
            packet_id,
            "--offer",
            token,
        ]
    )
    assert duplicate.exit_code == 1


def test_intake_poll_settles_a_maintenance_receipt_silently(
    tmp_path: Path,
) -> None:
    """INFRA-201: a maintenance receipt is settled by the poll's own
    Stop-hook call — silently, with no printed offer — instead of
    reaching the primary view as a wake."""

    from datetime import UTC, datetime

    from hermes_orchestrator.db import Database
    from tests.test_lead_intake import SESSION, seed_active_cell

    now = datetime(2026, 8, 30, 12, tzinfo=UTC).isoformat()
    operation_id = "9" * 32
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    database = Database.open(state_dir / "state.db")
    try:
        seed_active_cell(database)
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO control_operations("
                "operation_id, schema_version, kind, project_key, "
                "cell_id, session_id, dedup_key, result_json, reason, "
                "state, created_at, updated_at, acknowledged_at) VALUES "
                "(?, 1, 'intake.dedup_repaired', 'demo', 'cell-demo', ?, "
                "'intake.dedup_repaired:' || ?, '{\"interval_seconds\": 30}', "
                "NULL, 'published', ?, ?, NULL)",
                (operation_id, SESSION, SESSION, now, now),
            )
    finally:
        database.close()

    result = invoke(
        ["--state-dir", str(state_dir), "intake-poll", "--session", SESSION]
    )

    assert result.exit_code == 0
    assert result.stdout == ""

    database = Database.open(state_dir / "state.db")
    try:
        state = database.scalar(
            "SELECT state FROM control_operations WHERE operation_id = ?",
            (operation_id,),
        )
    finally:
        database.close()
    assert str(state) == "acknowledged"


# --- intake-poll idle-boundary dispatch (INFRA-199) ------------------------


class _IdleLinear:
    """Fake ``LinearProjector`` for the idle dispatcher's shared
    ``activate_admitted_issue`` service, composed in place of
    ``cli._open_idle_linear_router`` for these hook-level tests. Models
    the real ``LinearClient``'s per-``effect_id`` idempotence so a
    replay test can prove no duplicate projection is ever sent.

    INFRA-199 v2: ``validate`` is no longer part of the shared
    ``LinearProjector`` contract (Linear never authorizes activation),
    so this fake has no ``validate`` method at all — ``project`` is the
    only Linear operation the idle path ever calls, and only AFTER its
    local activation transaction already committed.
    """

    def __init__(
        self,
        *,
        project_error: Exception | None = None,
        on_project: object = None,
    ) -> None:
        self.targets: list[tuple[str, str | None, str]] = []
        self.effect_ids: list[str] = []
        self._project_error = project_error
        self._on_project = on_project
        self._completed: dict[str, object] = {}

    async def project(self, issue_id: str, target: object, effect_id: str) -> object:
        if effect_id in self._completed:
            return self._completed[effect_id]
        if self._on_project is not None:
            self._on_project()
        if self._project_error is not None:
            raise self._project_error
        self.targets.append((issue_id, target.status, target.assignee_alias))
        self.effect_ids.append(effect_id)
        result = object()
        self._completed[effect_id] = result
        return result


def _patch_idle_linear(monkeypatch: pytest.MonkeyPatch, linear: object) -> None:
    import hermes_orchestrator.cli as cli_module

    monkeypatch.setattr(
        cli_module,
        "_open_idle_linear_router",
        lambda settings, *, database, queue: linear,
    )


def _seed_idle_dispatch(
    state_dir: Path,
    *,
    with_cell: bool = True,
    with_issue: bool = True,
    pressure: str | None = "green",
    sampled_at: str | None = None,
    issue_priority: int = 1,
    issue_state: str = "queued",
    dependency_ready: int = 1,
    operator_decision_pending: bool = False,
    outstanding_child: bool = False,
) -> None:
    """Seed one live 'demo' cell/lease bound to SESSION plus, per flag,
    one admitted issue, the latest resource sample, an operator
    decision hold, or one outstanding background child."""

    from hermes_orchestrator.db import Database
    from tests.test_lead_intake import SESSION

    state_dir.mkdir(exist_ok=True)
    database = Database.open(state_dir / "state.db")
    try:
        now = datetime.now(UTC).isoformat()
        with database.transaction() as connection:
            if with_cell:
                connection.execute(
                    "INSERT INTO project_cells("
                    "cell_id, project_key, state, profile_alias, "
                    "session_id, created_at, updated_at) VALUES "
                    "('cell-demo', 'demo', 'active', 'max-a', ?, ?, ?)",
                    (SESSION, now, now),
                )
                connection.execute(
                    "INSERT INTO profile_leases("
                    "profile_alias, project_key, state, acquired_at) "
                    "VALUES ('max-a', 'demo', 'active', ?)",
                    (now,),
                )
            if with_issue:
                connection.execute(
                    "INSERT INTO admitted_issues("
                    "issue_id, project_key, priority, state, "
                    "instruction_id, dependency_ready, overlap_risk, "
                    "admitted_at, updated_at"
                    ") VALUES ('INFRA-9', 'demo', ?, ?, 'instr-9', ?, 0, "
                    "?, ?)",
                    (issue_priority, issue_state, dependency_ready, now, now),
                )
            if pressure is not None:
                connection.execute(
                    "INSERT INTO resource_samples("
                    "sample_id, sampled_at, pressure, "
                    "available_memory_bytes, total_memory_bytes, "
                    "swap_used_bytes, load_one, logical_cpus, disk_json, "
                    "managed_rss_bytes"
                    ") VALUES ('s', ?, ?, 1, 2, 0, 0.1, 1, '{}', 1)",
                    (sampled_at if sampled_at is not None else now, pressure),
                )
            if operator_decision_pending:
                connection.execute(
                    "INSERT INTO operator_decisions("
                    "decision_id, issue_id, project_key, cell_id, "
                    "session_id, actor, choice, status, recorded_at"
                    ") VALUES ('dec-1', 'INFRA-9', 'demo', 'cell-demo', "
                    "?, 'operator', 'hold', 'pending', ?)",
                    (SESSION, now),
                )
            if outstanding_child:
                connection.execute(
                    "INSERT INTO lead_children("
                    "session_id, child_id, state, started_at"
                    ") VALUES (?, 'child-1', 'started', ?)",
                    (SESSION, now),
                )
    finally:
        database.close()


def _poll_at_idle_boundary(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> CliResult:
    from tests.test_lead_intake import SESSION

    monkeypatch.setattr(
        sys,
        "stdin",
        StringIO(json.dumps({"session_id": SESSION, "hook_event_name": "Stop"})),
    )
    return invoke([*base_arguments(configured_repo), "intake-poll"])


def _idle_dispatch_counts(state_dir: Path) -> tuple[int, int, str | None]:
    from hermes_orchestrator.db import Database

    database = Database.open(state_dir / "state.db")
    try:
        started = int(
            database.scalar(
                "SELECT count(*) FROM events WHERE event_type = 'issue.started'"
            )
        )
        assigned = int(database.scalar("SELECT count(*) FROM lead_assignments"))
        issue_state = database.scalar(
            "SELECT state FROM admitted_issues WHERE issue_id = 'INFRA-9'"
        )
        return started, assigned, (
            str(issue_state) if issue_state is not None else None
        )
    finally:
        database.close()


def _set_idle_sample_age(state_dir: Path, sampled_at: str) -> None:
    from hermes_orchestrator.db import Database

    database = Database.open(state_dir / "state.db")
    try:
        with database.transaction() as connection:
            connection.execute(
                "UPDATE resource_samples SET sampled_at = ?", (sampled_at,)
            )
    finally:
        database.close()


def test_intake_poll_dispatches_ready_work_at_the_idle_boundary(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    _repo_root, state_dir = configured_repo
    _seed_idle_dispatch(state_dir)
    linear = _IdleLinear()
    _patch_idle_linear(monkeypatch, linear)

    first = _poll_at_idle_boundary(configured_repo, monkeypatch)
    assert first.exit_code == 0
    assert "HERMES_ASSIGNMENT_READY" in json.loads(first.stdout)["reason"]
    assert _idle_dispatch_counts(state_dir) == (1, 1, "in_development")
    # INFRA-199 v2: local activation commits first; the idle path never
    # consults Linear before that commit, and only then projects the
    # normal ``In Development`` effect id exactly once (asserted below).
    assert linear.targets == [("INFRA-9", "In Development", "operator")]

    # A repeated Stop at the same (now non-runnable) idle boundary is a
    # strict no-op: the project already has an issue occupying it (this
    # very one), so the occupancy pre-check short-circuits before Linear
    # is ever composed again.
    second = _poll_at_idle_boundary(configured_repo, monkeypatch)
    assert second.exit_code == 0
    assert _idle_dispatch_counts(state_dir) == (1, 1, "in_development")
    assert linear.targets == [("INFRA-9", "In Development", "operator")]


def test_intake_poll_idle_dispatch_commits_locally_before_projecting(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The local activation transaction must land BEFORE the Linear
    projection (INFRA-199 v2): a fake that inspects durable state from
    inside ``project`` observes the issue already ``in_development``
    and its assignment already committed."""

    _repo_root, state_dir = configured_repo
    _seed_idle_dispatch(state_dir)
    from hermes_orchestrator.db import Database

    observed: dict[str, object] = {}

    def _observe() -> None:
        database = Database.open(state_dir / "state.db")
        try:
            observed["assignments"] = int(
                database.scalar("SELECT count(*) FROM lead_assignments")
            )
            observed["issue_state"] = database.scalar(
                "SELECT state FROM admitted_issues WHERE issue_id = 'INFRA-9'"
            )
        finally:
            database.close()

    linear = _IdleLinear(on_project=_observe)
    _patch_idle_linear(monkeypatch, linear)

    result = _poll_at_idle_boundary(configured_repo, monkeypatch)

    assert result.exit_code == 0
    assert observed == {"assignments": 1, "issue_state": "in_development"}
    assert _idle_dispatch_counts(state_dir) == (1, 1, "in_development")


@pytest.mark.parametrize(
    "kwargs, expect_silent",
    [
        pytest.param({"with_issue": False}, True, id="nothing-runnable"),
        pytest.param({"dependency_ready": 0}, False, id="dependency-blocked"),
        pytest.param({"issue_state": "paused"}, False, id="paused"),
        pytest.param(
            {"operator_decision_pending": True}, False, id="operator-decision"
        ),
        pytest.param(
            {"pressure": "yellow", "issue_priority": 2},
            False,
            id="yellow-priority-too-high",
        ),
        pytest.param({"pressure": "red"}, False, id="red-sample"),
        pytest.param({"pressure": None}, False, id="no-sample"),
        pytest.param({"outstanding_child": True}, False, id="outstanding-children"),
        pytest.param({"with_cell": False}, True, id="no-live-cell"),
        pytest.param(
            {"sampled_at": "not-a-timestamp"}, False, id="malformed-sampled-at"
        ),
        pytest.param(
            {
                "sampled_at": (
                    datetime.now(UTC) - timedelta(minutes=6)
                ).isoformat()
            },
            False,
            id="stale-sample",
        ),
        pytest.param(
            {
                "sampled_at": (
                    datetime.now(UTC) + timedelta(minutes=10)
                ).isoformat()
            },
            False,
            id="future-skewed-sample",
        ),
    ],
)
def test_intake_poll_never_dispatches_a_held_or_ineligible_lane(
    configured_repo: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict,
    expect_silent: bool,
) -> None:
    _repo_root, state_dir = configured_repo
    _seed_idle_dispatch(state_dir, **kwargs)
    linear = _IdleLinear()
    _patch_idle_linear(monkeypatch, linear)

    result = _poll_at_idle_boundary(configured_repo, monkeypatch)

    assert result.exit_code == 0
    if expect_silent:
        assert result.stdout == ""
    started, assigned, issue_state = _idle_dispatch_counts(state_dir)
    assert (started, assigned) == (0, 0)
    if kwargs.get("with_issue", True) and kwargs.get("with_cell", True):
        assert issue_state == kwargs.get("issue_state", "queued")
    # None of these ineligible lanes ever reach Linear: capacity/queue
    # eligibility is decided before Linear composition even happens.
    assert linear.targets == []


def test_intake_poll_idle_dispatch_rechecks_freshness_at_commit_time(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Freshness is proven at TWO points, not just the initial snapshot
    (INFRA-199 Finding 2). INFRA-199 v2: a commit-time refusal now
    happens entirely BEFORE Linear is ever consulted — the local
    activation transaction never even attempts the projection — so the
    candidate simply stays queued with zero Linear traffic, and the
    very next idle boundary (once the sample is fresh again) runs the
    whole sequence from scratch and succeeds exactly once."""

    _repo_root, state_dir = configured_repo
    _seed_idle_dispatch(state_dir)

    import hermes_orchestrator.cells as cells_module

    linear = _IdleLinear()
    _patch_idle_linear(monkeypatch, linear)
    real_activate = cells_module.activate_admitted_issue
    degrade = {"armed": True}

    async def _degrade_then_activate(**kwargs: object) -> object:
        if degrade["armed"]:
            # Degrade the sample after candidate selection but before
            # the shared activation transaction opens — well before its
            # guard re-reads the sample on the transaction's own
            # connection — to model a sampler tick landing mid-dispatch.
            degrade["armed"] = False
            _set_idle_sample_age(
                state_dir,
                (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
            )
        return await real_activate(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        cells_module, "activate_admitted_issue", _degrade_then_activate
    )

    first = _poll_at_idle_boundary(configured_repo, monkeypatch)

    assert first.exit_code == 0
    # The local activation transaction refused before Linear was ever
    # attempted: there is nothing to project for a candidate that never
    # activated.
    assert linear.targets == []
    assert _idle_dispatch_counts(state_dir) == (0, 0, "queued")

    # A fresh sample lands before the next idle boundary; the whole
    # sequence runs again from scratch and succeeds exactly once.
    _set_idle_sample_age(state_dir, datetime.now(UTC).isoformat())
    second = _poll_at_idle_boundary(configured_repo, monkeypatch)
    assert second.exit_code == 0
    assert linear.targets == [("INFRA-9", "In Development", "operator")]
    assert _idle_dispatch_counts(state_dir) == (1, 1, "in_development")

    # Any further pass is a strict no-op: the project is now occupied.
    third = _poll_at_idle_boundary(configured_repo, monkeypatch)
    assert third.exit_code == 0
    assert linear.targets == [("INFRA-9", "In Development", "operator")]
    assert _idle_dispatch_counts(state_dir) == (1, 1, "in_development")


def test_intake_poll_idle_dispatch_local_activation_survives_a_linear_failure(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """INFRA-199 v2 (flipped O-era test): a deleted/unreachable Linear
    issue, a network error, a disallowed transition — any Linear
    projection failure — never blocks or undoes the local activation
    that already committed. Linear never authorizes activation; the
    durable pending trace this leaves (exercised at the ``LinearClient``
    layer in ``tests/test_cells.py``) is reconciliation's job to
    surface, never a reason to strand the queue."""

    _repo_root, state_dir = configured_repo
    _seed_idle_dispatch(state_dir)
    linear = _IdleLinear(project_error=TimeoutError("Linear is unavailable"))
    _patch_idle_linear(monkeypatch, linear)

    result = _poll_at_idle_boundary(configured_repo, monkeypatch)

    assert result.exit_code == 0
    assert linear.targets == []  # the fake raised before recording a target
    started, assigned, issue_state = _idle_dispatch_counts(state_dir)
    assert (started, assigned) == (1, 1)
    assert issue_state == "in_development"


def _pending_projection_rows(state_dir: Path) -> list[tuple[str, dict]]:
    """Pending Linear effects, read EXACTLY the way reconciliation's
    ``_stage_linear`` reads them."""

    from hermes_orchestrator.db import Database

    database = Database.open(state_dir / "state.db")
    try:
        rows = database.execute(
            "SELECT effect_id, target, request_json FROM external_effects "
            "WHERE adapter = 'linear' AND state = 'pending'"
        ).fetchall()
        return [
            (str(row["effect_id"]), json.loads(row["request_json"]))
            for row in rows
        ]
    finally:
        database.close()


def test_intake_poll_idle_dispatch_survives_a_linear_composition_failure(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sol ec0ed7fe gap 1 (flipped router-before-activation test): a
    Keychain/settings composition failure must never crash the hook AND
    must never block the eligible LOCAL activation — the router is only
    composed lazily, after the local commit. Exactly one local start
    and assignment land, and the stable target-only pending projection
    row journaled with that same commit is the durable Linear trace
    reconciliation resolves."""

    _repo_root, state_dir = configured_repo
    _seed_idle_dispatch(state_dir)

    import hermes_orchestrator.cli as cli_module

    compositions = {"count": 0}

    def _broken_composition(
        settings: object, *, database: object, queue: object
    ) -> object:
        compositions["count"] += 1
        raise RuntimeError("Keychain credential unavailable")

    monkeypatch.setattr(cli_module, "_open_idle_linear_router", _broken_composition)

    result = _poll_at_idle_boundary(configured_repo, monkeypatch)

    assert result.exit_code == 0
    # The composition failure happened — but only AFTER the local
    # activation had already committed.
    assert compositions["count"] == 1
    started, assigned, issue_state = _idle_dispatch_counts(state_dir)
    assert (started, assigned) == (1, 1)
    assert issue_state == "in_development"
    assert _pending_projection_rows(state_dir) == [
        (
            "linear:INFRA-9:in-development:v2",
            {
                "issue_id": "INFRA-9",
                "target": {
                    "status": "In Development",
                    "assignee_alias": "operator",
                },
            },
        )
    ]


def test_idle_dispatch_rechecks_reprioritization_in_transaction(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The idle transaction must see a post-selection priority change."""

    _repo_root, state_dir = configured_repo
    _seed_idle_dispatch(state_dir, pressure="yellow", issue_priority=1)
    linear = _IdleLinear()
    _patch_idle_linear(monkeypatch, linear)
    import hermes_orchestrator.cells as cells_module

    real_activate = cells_module.activate_admitted_issue

    async def reprioritize_then_activate(**kwargs: object) -> object:
        from hermes_orchestrator.db import Database
        from hermes_orchestrator.events import EventStore
        from hermes_orchestrator.queue import QueueService

        database = Database.open(state_dir / "state.db")
        try:
            QueueService(
                database, EventStore(database), registered_projects=("demo",)
            ).reprioritize("INFRA-9", 2)
        finally:
            database.close()
        return await real_activate(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        cells_module, "activate_admitted_issue", reprioritize_then_activate
    )

    result = _poll_at_idle_boundary(configured_repo, monkeypatch)

    assert result.exit_code == 0
    assert _idle_dispatch_counts(state_dir) == (0, 0, "queued")
    assert linear.targets == []


@pytest.mark.parametrize(
    "occupying_state", ["in_development", "review"], ids=["in_development", "review"]
)
def test_intake_poll_never_starts_a_second_issue_for_a_working_project(
    configured_repo: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    occupying_state: str,
) -> None:
    """Project occupancy (INFRA-199 v2 / INFRA-211): a runnable queued
    lane is refused while another issue in the same project is already
    ``in_development`` OR ``review``."""

    from hermes_orchestrator.db import Database

    _repo_root, state_dir = configured_repo
    _seed_idle_dispatch(state_dir)
    linear = _IdleLinear()
    _patch_idle_linear(monkeypatch, linear)
    database = Database.open(state_dir / "state.db")
    try:
        now = datetime.now(UTC).isoformat()
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO admitted_issues("
                "issue_id, project_key, priority, state, instruction_id, "
                "dependency_ready, overlap_risk, admitted_at, updated_at"
                ") VALUES ('INFRA-8', 'demo', 1, ?, "
                "'instr-8', 1, 0, ?, ?)",
                (occupying_state, now, now),
            )
    finally:
        database.close()

    result = _poll_at_idle_boundary(configured_repo, monkeypatch)

    assert result.exit_code == 0
    started, assigned, issue_state = _idle_dispatch_counts(state_dir)
    assert (started, assigned) == (0, 0)
    assert issue_state == "queued"
    assert linear.targets == []


# --- verify / verify-check (INFRA-186 P9: any-agent verifier CLI) ----------


def _verifier_git(checkout: Path, *args: str) -> None:
    import subprocess

    subprocess.run(
        ("git", "-C", str(checkout), *args),
        check=True,
        capture_output=True,
        env={
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.invalid",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.invalid",
            "PATH": "/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin",
            "HOME": str(checkout),
        },
    )


def _build_verify_target(root: Path) -> Path:
    target = root / "verify-target"
    target.mkdir()
    (target / "README.md").write_text("hello\n")
    _verifier_git(target, "init", "-q")
    _verifier_git(target, "add", "-A")
    _verifier_git(target, "commit", "-qm", "init")
    return target


def test_verify_runs_a_command_and_prints_a_receipt_id(
    configured_repo: tuple[Path, Path], tmp_path: Path
) -> None:
    target = _build_verify_target(tmp_path)

    result = invoke(
        [
            *base_arguments(configured_repo),
            "verify",
            "--gate",
            "demo-gate",
            "--cwd",
            str(target),
            "--json",
            "--",
            "python3",
            "-c",
            "print('1 passed')",
        ]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["gate_id"] == "demo-gate"
    assert payload["exit_code"] == 0
    assert payload["fresh"] is True
    assert len(payload["receipt_id"]) == 64


def test_verify_refuses_an_empty_trailing_command(
    configured_repo: tuple[Path, Path], tmp_path: Path
) -> None:
    target = _build_verify_target(tmp_path)

    result = invoke(
        [
            *base_arguments(configured_repo),
            "verify",
            "--gate",
            "demo-gate",
            "--cwd",
            str(target),
        ]
    )

    assert result.exit_code == 2
    assert "command" in result.output


def test_verify_check_is_fresh_then_stale_after_a_tree_change(
    configured_repo: tuple[Path, Path], tmp_path: Path
) -> None:
    target = _build_verify_target(tmp_path)
    first = invoke(
        [
            *base_arguments(configured_repo),
            "verify",
            "--gate",
            "demo-gate",
            "--cwd",
            str(target),
            "--json",
            "--",
            "python3",
            "-c",
            "print('1 passed')",
        ]
    )
    receipt_id = json.loads(first.stdout)["receipt_id"]

    fresh = invoke(
        [
            *base_arguments(configured_repo),
            "verify-check",
            "--gate",
            "demo-gate",
            "--receipt",
            receipt_id,
            "--cwd",
            str(target),
            "--json",
        ]
    )
    assert fresh.exit_code == 0
    assert json.loads(fresh.stdout) == {
        "receipt_id": receipt_id,
        "valid": True,
        "reason": "fresh",
    }

    (target / "README.md").write_text("changed\n")
    _verifier_git(target, "add", "README.md")
    _verifier_git(target, "commit", "-qm", "change")

    stale = invoke(
        [
            *base_arguments(configured_repo),
            "verify-check",
            "--gate",
            "demo-gate",
            "--receipt",
            receipt_id,
            "--cwd",
            str(target),
            "--json",
        ]
    )
    assert stale.exit_code == 1
    payload = json.loads(stale.stdout)
    assert payload["valid"] is False
    assert payload["reason"] == "stale: tree changed"


# SR2 listener demotion (supersedes fast-lane a3aa8cd8's idle-primary
# design): an idle thread/status/changed for the project-bound Merger
# thread is crash-recovery fallback ONLY — it resumes settlement solely
# when a durable submitted_verdicts row in state 'submitted' exists for
# the project's outstanding wake, and NEVER triggers a
# thread-pull-and-infer settlement. turn/completed keeps its behavior.


def _status_changed(thread_id: str, status_type: str) -> RpcNotification:
    return RpcNotification(
        "thread/status/changed",
        {"threadId": thread_id, "status": {"type": status_type}},
    )


def _turn_completed(thread_id: str) -> RpcNotification:
    return RpcNotification("turn/completed", {"threadId": thread_id})


class _ListenerRpc:
    """Finite notification feed; the listener returns when it drains."""

    def __init__(self, notifications: list[RpcNotification]) -> None:
        self._notifications = notifications

    async def notifications(self):
        for notification in self._notifications:
            yield notification


class _ListenerMerger:
    """Read-only project-bound reviewer channels."""

    def __init__(self, channels: dict[str, str]) -> None:
        self._channels = channels

    def read_channel(self, project_key: str) -> types.SimpleNamespace | None:
        thread_id = self._channels.get(project_key)
        if thread_id is None:
            return None
        return types.SimpleNamespace(thread_id=thread_id)


class _ListenerTurns:
    """Honest stand-in for MergerTurnService's single settlement path.

    ``handle_turn`` is the one entry point: the first call for an
    outstanding wake settles the completed structured verdict (recorded
    in ``settlements``) and consumes the wake, exactly like the durable
    claim; every later call finds no outstanding wake and no-ops. A
    duplicate invocation is therefore visible as an extra entry in
    ``handle_calls`` without an extra entry in ``settlements``, and a
    parallel settlement architecture would show up as settlements not
    produced by ``handle_turn``.
    """

    def __init__(
        self,
        merger: _ListenerMerger,
        projects: tuple[str, ...],
        outstanding: dict[str, str | None],
        pending: dict[str, str | None] | None = None,
    ) -> None:
        self._merger = merger
        self._projects = projects
        self._outstanding = outstanding
        self._pending = pending or {}
        self.handle_calls: list[str] = []
        self.settlements: list[str] = []

    def outstanding_wake(
        self, project_key: str
    ) -> tuple[types.SimpleNamespace, str] | None:
        state = self._outstanding.get(project_key)
        if state is None:
            return None
        return (types.SimpleNamespace(event_id="evt-1"), state)

    def _pending_submission(
        self, project_key: str, event_id: str
    ) -> types.SimpleNamespace | None:
        if self._pending.get(project_key) != event_id:
            return None
        return types.SimpleNamespace(
            event_id=event_id, project_key=project_key, state="submitted"
        )

    async def handle_turn(self, project_key: str) -> TurnOutcome:
        self.handle_calls.append(project_key)
        if self._outstanding.get(project_key) is None:
            return TurnOutcome(
                project_key,
                "no_outstanding_wake",
                None,
                None,
                "no delivered candidate wake; terminal idle",
            )
        self._outstanding[project_key] = None
        # A resumed pending submission is durably marked settled.
        self._pending[project_key] = None
        self.settlements.append(project_key)
        return TurnOutcome(
            project_key,
            "corrections_required",
            "evt-1",
            "ENG-1",
            "structured corrections_required verdict settled",
        )

    async def on_notification(
        self, notification: RpcNotification
    ) -> TurnOutcome | None:
        if notification.method not in ("turn/completed", "thread/turn/completed"):
            return None
        thread_id = notification.params.get("threadId")
        for project_key in self._projects:
            channel = self._merger.read_channel(project_key)
            if channel is not None and channel.thread_id == thread_id:
                return await self.handle_turn(project_key)
        return None


def _listener_flow(
    *,
    channels: dict[str, str],
    outstanding: dict[str, str | None],
    notifications: list[RpcNotification],
    pending: dict[str, str | None] | None = None,
) -> types.SimpleNamespace:
    merger = _ListenerMerger(channels)
    return types.SimpleNamespace(
        rpc=_ListenerRpc(notifications),
        merger=merger,
        turns=_ListenerTurns(merger, tuple(channels), outstanding, pending),
    )


async def _run_listener(flow: types.SimpleNamespace, projects: tuple[str, ...]) -> str:
    stdout = StringIO()
    with redirect_stdout(stdout):
        await _listen_for_merger_turns(flow, projects)
    return stdout.getvalue()


def _settled_lines(output: str) -> list[dict[str, object]]:
    lines = [json.loads(line) for line in output.splitlines()]
    return [line for line in lines if line.get("merger_turn") == "corrections_required"]


@pytest.mark.asyncio
async def test_idle_with_wake_but_no_submitted_verdict_is_a_noop() -> None:
    # Demotion: idle with an outstanding wake but NO durable submitted
    # verdict must never pull the thread and infer a settlement; the
    # later turn/completed observation keeps its existing behavior.
    flow = _listener_flow(
        channels={"demo": "thread-1"},
        outstanding={"demo": "delivered"},
        notifications=[
            _status_changed("thread-1", "idle"),
            _turn_completed("thread-1"),
        ],
    )

    output = await _run_listener(flow, ("demo",))

    # Exactly one handle_turn call — from turn/completed, not idle.
    assert flow.turns.handle_calls == ["demo"]
    assert flow.turns.settlements == ["demo"]
    assert len(_settled_lines(output)) == 1


@pytest.mark.asyncio
async def test_idle_with_durable_pending_submission_settles_once() -> None:
    # Crash-recovery fallback: an explicit submission durably in state
    # 'submitted' whose settlement was cut short resumes on idle.
    flow = _listener_flow(
        channels={"demo": "thread-1"},
        outstanding={"demo": "admitted"},
        pending={"demo": "evt-1"},
        notifications=[_status_changed("thread-1", "idle")],
    )

    output = await _run_listener(flow, ("demo",))

    assert flow.turns.handle_calls == ["demo"]
    assert flow.turns.settlements == ["demo"]
    settled = _settled_lines(output)
    assert len(settled) == 1
    assert settled[0]["event_id"] == "evt-1"
    assert settled[0]["issue_id"] == "ENG-1"


@pytest.mark.asyncio
async def test_duplicate_idle_and_turn_completed_settle_exactly_once() -> None:
    flow = _listener_flow(
        channels={"demo": "thread-1"},
        outstanding={"demo": "delivered"},
        pending={"demo": "evt-1"},
        notifications=[
            _status_changed("thread-1", "idle"),
            _status_changed("thread-1", "idle"),
            _turn_completed("thread-1"),
        ],
    )

    output = await _run_listener(flow, ("demo",))

    # Exactly one settlement: no duplicate review, correction,
    # projection, or merge from the replayed notifications.
    assert flow.turns.settlements == ["demo"]
    assert len(_settled_lines(output)) == 1


@pytest.mark.asyncio
async def test_idle_without_wake_and_non_idle_statuses_no_op() -> None:
    no_wake = _listener_flow(
        channels={"demo": "thread-1"},
        outstanding={"demo": None},
        notifications=[_status_changed("thread-1", "idle")],
    )

    await _run_listener(no_wake, ("demo",))

    assert no_wake.turns.handle_calls == []
    assert no_wake.turns.settlements == []

    non_idle = _listener_flow(
        channels={"demo": "thread-1"},
        outstanding={"demo": "delivered"},
        notifications=[
            _status_changed("thread-1", "active"),
            _status_changed("thread-9", "idle"),
        ],
    )

    await _run_listener(non_idle, ("demo",))

    assert non_idle.turns.handle_calls == []
    assert non_idle.turns.settlements == []


# SR2 submit-review (operator correction ec1f6bdf): the smallest strict
# local subcommand for the bound Sol session — the verdict is submitted,
# never inferred; exit 0 on a settled outcome, exit 1 with the rejection
# message on SubmissionRejected or invalid input.


class _FakeSubmitTurns:
    def __init__(self, result: object) -> None:
        self._result = result
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def submit_review(self, project_key: str, **kwargs: object) -> object:
        self.calls.append((project_key, kwargs))
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


def _submit_arguments(configured_repo: tuple[Path, Path], verdict: str) -> list[str]:
    return [
        *base_arguments(configured_repo),
        "submit-review",
        "--project",
        "demo",
        "--issue",
        "ENG-9",
        "--event",
        "evt-1",
        "--candidate-sha",
        "a" * 40,
        "--thread",
        "thread-1",
        "--generation",
        "3",
        "--verdict",
        verdict,
    ]


def test_submit_review_settles_the_submitted_verdict(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    import hermes_orchestrator.cli as cli_module

    repo_root, _state_dir = configured_repo
    verdict_path = repo_root / "verdict.json"
    verdict_path.write_text('{"pr_number": 7}', encoding="utf-8")
    turns = _FakeSubmitTurns(
        TurnOutcome(
            "demo",
            "approved",
            "evt-1",
            "ENG-9",
            "review approved and merged",
            review_id="rev-1",
            merge_sha="b" * 40,
        )
    )
    monkeypatch.setattr(
        cli_module,
        "_open_merge_flow",
        lambda settings, runtime: types.SimpleNamespace(turns=turns),
    )

    result = invoke(_submit_arguments(configured_repo, str(verdict_path)))

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["merger_turn"] == "approved"
    assert payload["event_id"] == "evt-1"
    assert payload["issue_id"] == "ENG-9"
    project_key, kwargs = turns.calls[0]
    assert project_key == "demo"
    assert kwargs == {
        "issue_id": "ENG-9",
        "event_id": "evt-1",
        "candidate_sha": "a" * 40,
        "reviewed_thread_id": "thread-1",
        "reviewed_generation": 3,
        "verdict_json": '{"pr_number": 7}',
    }


def test_submit_review_rejection_fails_closed_from_stdin(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.merger_turns import SubmissionRejected

    turns = _FakeSubmitTurns(
        SubmissionRejected("submission event does not match the outstanding wake")
    )
    monkeypatch.setattr(
        cli_module,
        "_open_merge_flow",
        lambda settings, runtime: types.SimpleNamespace(turns=turns),
    )
    monkeypatch.setattr(sys, "stdin", StringIO('{"pr_number": 7}'))

    result = invoke(_submit_arguments(configured_repo, "-"))

    assert result.exit_code == 1
    assert "does not match the outstanding wake" in result.stderr
    # The stdin document reached the service verbatim before rejection.
    assert turns.calls[0][1]["verdict_json"] == '{"pr_number": 7}'


def test_submit_review_refuses_invalid_input_before_opening_the_flow(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    import hermes_orchestrator.cli as cli_module

    def _never_opened(settings: object, runtime: object) -> object:
        raise AssertionError("the merge flow must not open on invalid input")

    monkeypatch.setattr(cli_module, "_open_merge_flow", _never_opened)

    bad_sha = invoke(
        [
            *base_arguments(configured_repo),
            "submit-review",
            "--project",
            "demo",
            "--issue",
            "ENG-9",
            "--event",
            "evt-1",
            "--candidate-sha",
            "not-a-sha",
            "--thread",
            "thread-1",
            "--generation",
            "3",
            "--verdict",
            "-",
        ]
    )
    assert bad_sha.exit_code == 1
    assert "40-hex" in bad_sha.stderr

    repo_root, _state_dir = configured_repo
    missing = invoke(
        _submit_arguments(configured_repo, str(repo_root / "absent.json"))
    )
    assert missing.exit_code == 1
    assert "cannot read verdict document" in missing.stderr

    monkeypatch.setattr(sys, "stdin", StringIO("   \n"))
    empty = invoke(_submit_arguments(configured_repo, "-"))
    assert empty.exit_code == 1
    assert "verdict document is empty" in empty.stderr


# Sol f0a5a403 P3 (CLI half): channel-trust-confirm pins its exit codes —
# confirmed=True exits 0; a refused or ambiguous non-success verdict
# (confirmed=False, including first_failure="confirm_outcome_ambiguous")
# exits nonzero with the state visible in the JSON output.


def _invoke_channel_trust_confirm(
    configured_repo: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    verdict: object,
) -> CliResult:
    import hermes_orchestrator.channel_trust as channel_trust_module
    import hermes_orchestrator.cli as cli_module
    import hermes_orchestrator.runtime as runtime_module

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _bind_rotate_lead_cell(state_dir, cell_id="cell-demo")

    dialog = (
        "Loading development channels\n"
        "server:hermes-control\n"
        "I am using this for local development\n"
        "Enter to confirm\n"
    )

    async def _fake_read_screen(self: object, ref: object, lines: int = 200) -> str:
        return dialog

    monkeypatch.setattr(cli_module.CmuxCliAdapter, "read_screen", _fake_read_screen)
    monkeypatch.setattr(
        channel_trust_module.ChannelTrustAnchors,
        "active_for_cell",
        lambda self, cell_id: types.SimpleNamespace(
            anchor_id="anchor-1",
            prompt_pattern="pattern",
            canonical_entry_path="/entry",
        ),
    )
    entry = state_dir / "sidecar" / "dist" / "channel" / "entry.js"
    monkeypatch.setattr(
        runtime_module,
        "resolve_sidecar_entry",
        lambda *, repo_root, state_dir: entry,
    )
    monkeypatch.setattr(
        cli_module, "_live_claude_argv", lambda session_id: ["claude"]
    )
    monkeypatch.setattr(
        channel_trust_module.ChannelTrustGate,
        "evaluate",
        lambda self, **kwargs: verdict,
    )

    return invoke(
        [
            *base_arguments(configured_repo),
            "channel-trust-confirm",
            "--cell",
            "cell-demo",
            "--wait-seconds",
            "1",
            "--json",
        ]
    )


@pytest.mark.parametrize(
    ("confirmed", "first_failure", "expected_exit"),
    [
        (True, None, 0),
        (False, "entry_sha256", 1),
        (False, "confirm_outcome_ambiguous", 1),
    ],
)
def test_channel_trust_confirm_exit_codes_fail_closed(
    configured_repo: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    confirmed: bool,
    first_failure: str | None,
    expected_exit: int,
) -> None:
    from hermes_orchestrator.channel_trust import TrustVerdict

    verdict = TrustVerdict(
        confirmed=confirmed,
        anchor_id="anchor-1",
        first_failure=first_failure,
        receipt_operation_id="op-1",
    )

    result = _invoke_channel_trust_confirm(configured_repo, monkeypatch, verdict)

    assert result.exit_code == expected_exit
    payload = json.loads(result.stdout)
    assert payload["confirmed"] is confirmed
    if first_failure is not None:
        # The non-success state — ambiguous included — is visible.
        assert payload["first_failure"] == first_failure


# Sol correction a06cbce0: prompt capture validates exactly one
# DISPLAYED Channels entry inside the confirmation dialog, ignoring an
# echoed shell launch command that legitimately repeats the channel
# token above the dialog in a full-scrollback capture.

_CAPTURE_DIALOG = (
    "Loading development channels\n"
    "server:hermes-control\n"
    "I am using this for local development\n"
    "Enter to confirm\n"
)


def _invoke_channel_trust_confirm_capture(
    configured_repo: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    screen: str,
) -> tuple[CliResult, list[tuple[str, str]]]:
    import hermes_orchestrator.channel_trust as channel_trust_module
    import hermes_orchestrator.cli as cli_module
    import hermes_orchestrator.runtime as runtime_module

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _bind_rotate_lead_cell(state_dir, cell_id="cell-demo")

    entry = state_dir / "sidecar" / "dist" / "channel" / "entry.js"

    async def _fake_read_screen(self: object, ref: object, lines: int = 200) -> str:
        return screen

    monkeypatch.setattr(cli_module.CmuxCliAdapter, "read_screen", _fake_read_screen)
    monkeypatch.setattr(
        channel_trust_module.ChannelTrustAnchors,
        "active_for_cell",
        lambda self, cell_id: types.SimpleNamespace(
            anchor_id="anchor-1",
            prompt_pattern=None,
            canonical_entry_path=str(entry),
        ),
    )
    monkeypatch.setattr(
        runtime_module,
        "resolve_sidecar_entry",
        lambda *, repo_root, state_dir: entry,
    )
    monkeypatch.setattr(
        cli_module, "_live_claude_argv", lambda session_id: ["claude"]
    )
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        channel_trust_module.ChannelTrustAnchors,
        "complete_prompt",
        lambda self, anchor_id, pattern: calls.append((anchor_id, pattern)),
    )

    result = invoke(
        [
            *base_arguments(configured_repo),
            "channel-trust-confirm",
            "--cell",
            "cell-demo",
            "--wait-seconds",
            "1",
            "--capture-prompt",
            "--json",
        ]
    )
    return result, calls


def test_channel_trust_confirm_capture_ignores_echoed_launch_argv(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A full-scrollback capture legitimately shows the shell's own
    ``--dangerously-load-development-channels server:hermes-control``
    launch argument above a well-formed, single-entry dialog. That
    echoed argv must not be conjured into a second Channels entry."""

    from hermes_orchestrator.channel_trust import APPROVED_PROMPT_PATTERN

    screen = (
        "claude --resume 11111111-1111-4111-8111-111111111111 "
        "--dangerously-skip-permissions "
        "--mcp-config /tmp/11111111-1111-4111-8111-111111111111.mcp.json "
        "--dangerously-load-development-channels server:hermes-control\n"
        "\n" + _CAPTURE_DIALOG
    )

    result, calls = _invoke_channel_trust_confirm_capture(
        configured_repo, monkeypatch, screen
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["prompt_evidence"] == "bound"
    assert calls == [("anchor-1", APPROVED_PROMPT_PATTERN)]


def test_channel_trust_confirm_capture_fails_closed_on_extra_displayed_channel(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A second development-channel entry actually DISPLAYED inside the
    dialog — not merely echoed argv above it — still fails closed."""

    screen = (
        "Loading development channels\n"
        "server:hermes-control\n"
        "server:evil\n"
        "I am using this for local development\n"
        "Enter to confirm\n"
    )

    result, calls = _invoke_channel_trust_confirm_capture(
        configured_repo, monkeypatch, screen
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert (
        "operator-recorded normalized structure" in payload["error"]
    )
    assert calls == []


# INFRA-198 J2 (acceptance-completion policy root fix): require_acceptance
# and satisfy_acceptance drive the durable acceptance gate from strict
# hermes-command intents. satisfy_acceptance's Linear projection must go
# through the same idempotent external-effect machinery every merge
# settlement uses, so these tests stand in for the live Linear router
# with a fake that is itself backed by the real ExternalEffectStore —
# faithfully reproducing the read-before-write contract
# ``LinearClient.project`` uses in production, so "recorded exactly
# once" is proven against the real durable table, not just a call count.
class _RecordingAcceptanceLinear:
    def __init__(self, effects: Any) -> None:
        self._effects = effects
        self.calls: list[str] = []

    async def project(self, issue_id: str, target: Any, effect_id: str) -> object:
        effect = self._effects.get(effect_id)
        if effect is not None and effect.state == "completed":
            return effect.response
        self._effects.begin(
            effect_id,
            target=issue_id,
            request={
                "issue_id": issue_id,
                "target": target.model_dump(mode="json"),
            },
        )
        self.calls.append(effect_id)
        return self._effects.complete(
            effect_id, {"issue_id": issue_id, "changed_fields": ["status"]}
        )


def _insert_merged_review(database: Any, issue_id: str) -> None:
    """Persist the proven merged review row a settled merge leaves behind
    (state ``merged`` with a non-null ``merge_sha``), which the hardened
    ``satisfy_acceptance`` demands before completing any issue."""

    stamp = "2026-08-28T12:00:00+00:00"
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO reviews("
            "review_id, project_key, issue_id, event_id, repository, branch, "
            "pr_number, reviewed_sha, state, merge_sha, reason, "
            "projection_json, created_at, updated_at"
            ") VALUES (?, 'demo', ?, ?, 'j-paterson/demo', 'feature/eng-9', "
            "14, ?, 'merged', ?, 'proven exact', NULL, ?, ?)",
            (
                f"review:demo:evt-{issue_id}",
                issue_id,
                f"evt-{issue_id}",
                "c" * 40,
                "e" * 40,
                stamp,
                stamp,
            ),
        )


def test_hermes_command_require_then_satisfy_acceptance_completes_the_issue(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.domain import IssueState
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.linear import ExternalEffectStore
    from hermes_orchestrator.queue import QueueService

    _repo_root, state_dir = configured_repo
    add = invoke(
        [
            *base_arguments(configured_repo),
            "queue-add",
            "ENG-9",
            "--project",
            "demo",
            "--priority",
            "1",
            "--operator-instruction",
            "chat-9",
        ]
    )
    assert add.exit_code == 0

    def command(payload: dict[str, object]) -> dict[str, object]:
        result = invoke(
            [
                *base_arguments(configured_repo),
                "hermes-command",
                "--json",
                json.dumps(payload),
            ]
        )
        assert result.exit_code == 0, result.output
        return json.loads(result.stdout)

    require = command(
        {
            "intent": "require_acceptance",
            "issue_id": "ENG-9",
            "instruction_id": "chat-9",
            "predicates": ["tests pass", "docs updated"],
        }
    )
    assert require["code"] == "accepted"
    assert require["state"]["state"] == "pending"
    assert require["state"]["predicates"] == ["tests pass", "docs updated"]

    database = Database.open(state_dir / "state.db")
    effects = ExternalEffectStore(database)
    linear = _RecordingAcceptanceLinear(effects)
    monkeypatch.setattr(
        cli_module, "_open_acceptance_linear", lambda settings, runtime: linear
    )
    queue = QueueService(database, EventStore(database), {"demo"})

    full_evidence = {
        "tests pass": "pytest output attached",
        "docs updated": "README diff attached",
    }
    # Sol 524a38ed finding 1 — flipped from the J2-era pin of
    # queued -> Done: full evidence on a merely QUEUED issue is refused.
    # The gate stays pending and untouched; zero queue or Linear writes.
    premature = command(
        {
            "intent": "satisfy_acceptance",
            "issue_id": "ENG-9",
            "evidence": full_evidence,
        }
    )
    assert premature["code"] == "rejected"
    assert "post_merge_acceptance" in str(premature["state"]["reason"])
    assert linear.calls == []
    assert queue.get("ENG-9").state.value == "queued"
    assert (
        str(
            database.scalar(
                "SELECT state FROM acceptance_gates WHERE issue_id = 'ENG-9'"
            )
        )
        == "pending"
    )

    # The merge settles and the acceptance hold applies: the issue holds
    # in post_merge_acceptance with a proven merged review on record.
    queue.transition(
        "ENG-9",
        IssueState.POST_MERGE_ACCEPTANCE,
        actor="codex_merger",
        reason="merged; acceptance pending",
    )
    _insert_merged_review(database, "ENG-9")

    partial = command(
        {
            "intent": "satisfy_acceptance",
            "issue_id": "ENG-9",
            "evidence": {"tests pass": "pytest output attached"},
        }
    )
    assert partial["code"] == "rejected"
    assert "missing" in str(partial["state"]["reason"])
    assert linear.calls == []
    assert queue.get("ENG-9").state.value == "post_merge_acceptance"

    still_pending = command(
        {
            "intent": "require_acceptance",
            "issue_id": "ENG-9",
            "instruction_id": "chat-9",
            "predicates": ["tests pass", "docs updated"],
        }
    )
    assert still_pending["code"] == "accepted"
    assert still_pending["state"]["state"] == "pending"

    satisfied = command(
        {
            "intent": "satisfy_acceptance",
            "issue_id": "ENG-9",
            "evidence": full_evidence,
        }
    )
    assert satisfied["code"] == "accepted", satisfied
    assert satisfied["state"]["state"] == "satisfied"
    assert satisfied["state"]["evidence"] == full_evidence
    assert queue.get("ENG-9").state.value == "done"
    assert len(linear.calls) == 1
    effect_id = linear.calls[0]

    replay = command(
        {
            "intent": "satisfy_acceptance",
            "issue_id": "ENG-9",
            "evidence": full_evidence,
        }
    )
    assert replay["code"] == "accepted"
    assert replay["state"]["evidence"] == full_evidence
    assert queue.get("ENG-9").state.value == "done"
    # Replay-safe: no duplicate queue transition event and no duplicate
    # Linear effect for the same effect id.
    assert linear.calls == [effect_id]

    require_after_satisfied = command(
        {
            "intent": "require_acceptance",
            "issue_id": "ENG-9",
            "instruction_id": "chat-9",
            "predicates": ["tests pass", "docs updated"],
        }
    )
    assert require_after_satisfied["code"] == "rejected"
    assert "already satisfied" in str(require_after_satisfied["state"]["reason"])

    database.close()


def test_hermes_command_require_acceptance_refuses_unknown_issue(
    configured_repo: tuple[Path, Path],
) -> None:
    result = invoke(
        [
            *base_arguments(configured_repo),
            "hermes-command",
            "--json",
            json.dumps(
                {
                    "intent": "require_acceptance",
                    "issue_id": "ENG-404",
                    "instruction_id": "chat-404",
                    "predicates": ["tests pass"],
                }
            ),
        ]
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["code"] == "rejected"


def test_hermes_command_satisfy_acceptance_refuses_without_a_gate(
    configured_repo: tuple[Path, Path],
) -> None:
    add = invoke(
        [
            *base_arguments(configured_repo),
            "queue-add",
            "ENG-9",
            "--project",
            "demo",
            "--priority",
            "1",
            "--operator-instruction",
            "chat-9",
        ]
    )
    assert add.exit_code == 0

    result = invoke(
        [
            *base_arguments(configured_repo),
            "hermes-command",
            "--json",
            json.dumps(
                {
                    "intent": "satisfy_acceptance",
                    "issue_id": "ENG-9",
                    "evidence": {"tests pass": "pytest output"},
                }
            ),
        ]
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["code"] == "rejected"


@pytest.mark.parametrize(
    "lifecycle",
    ["queued", "in_development", "review", "unmerged_hold"],
)
def test_hermes_command_satisfy_acceptance_refuses_unmerged_lifecycles(
    configured_repo: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    lifecycle: str,
) -> None:
    """Sol 524a38ed finding 1, required test 1: satisfy_acceptance is
    refused for queued, in-development, in-review, and unmerged
    (``post_merge_acceptance`` without a proven merged review) work —
    zero advancement: the gate stays pending and untouched, the queue
    does not move, no Linear projector is even composed, and no event
    of any kind is journaled by the refusal."""

    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.domain import IssueState
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.queue import QueueService

    _repo_root, state_dir = configured_repo
    add = invoke(
        [
            *base_arguments(configured_repo),
            "queue-add",
            "ENG-9",
            "--project",
            "demo",
            "--priority",
            "1",
            "--operator-instruction",
            "chat-9",
        ]
    )
    assert add.exit_code == 0
    require = invoke(
        [
            *base_arguments(configured_repo),
            "hermes-command",
            "--json",
            json.dumps(
                {
                    "intent": "require_acceptance",
                    "issue_id": "ENG-9",
                    "instruction_id": "chat-9",
                    "predicates": ["tests pass"],
                }
            ),
        ]
    )
    assert require.exit_code == 0
    assert json.loads(require.stdout)["code"] == "accepted"

    database = Database.open(state_dir / "state.db")
    try:
        queue = QueueService(database, EventStore(database), {"demo"})
        target = {
            "queued": IssueState.QUEUED,
            "in_development": IssueState.IN_DEVELOPMENT,
            "review": IssueState.REVIEW,
            "unmerged_hold": IssueState.POST_MERGE_ACCEPTANCE,
        }[lifecycle]
        if target is not IssueState.QUEUED:
            queue.transition(
                "ENG-9", target, actor="test", reason=f"placed in {lifecycle}"
            )

        def never_compose(settings: Any, runtime: Any) -> Any:
            raise AssertionError(
                "a refused satisfy_acceptance must never compose Linear"
            )

        monkeypatch.setattr(
            cli_module, "_open_acceptance_linear", never_compose
        )
        events_before = int(database.scalar("SELECT COUNT(*) FROM events"))

        result = invoke(
            [
                *base_arguments(configured_repo),
                "hermes-command",
                "--json",
                json.dumps(
                    {
                        "intent": "satisfy_acceptance",
                        "issue_id": "ENG-9",
                        "evidence": {"tests pass": "pytest output attached"},
                    }
                ),
            ]
        )

        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["code"] == "rejected"
        expected_fragment = (
            "no proven merged review"
            if lifecycle == "unmerged_hold"
            else "post_merge_acceptance"
        )
        assert expected_fragment in str(payload["state"]["reason"])
        # Zero advancement: gate pending and evidence-free, queue
        # unmoved, and not a single event journaled by the refusal.
        gate = database.execute(
            "SELECT state, evidence_json FROM acceptance_gates "
            "WHERE issue_id = 'ENG-9'"
        ).fetchone()
        assert str(gate["state"]) == "pending"
        assert gate["evidence_json"] is None
        assert queue.get("ENG-9").state is target
        assert (
            int(database.scalar("SELECT COUNT(*) FROM events"))
            == events_before
        )
        assert (
            int(
                database.scalar(
                    "SELECT COUNT(*) FROM events "
                    "WHERE event_type = 'acceptance.satisfied'"
                )
            )
            == 0
        )
    finally:
        database.close()


def test_merge_settle_review_reconciles_a_stranded_satisfied_gate(
    configured_repo: tuple[Path, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sol 04d013b0 finding 3 (required test 3): the acceptance gate's
    satisfaction persisted but the process crashed before the queue
    transition and Linear completion. The per-review maintenance entry —
    ``merge-settle --review`` — must run the acceptance reconciliation
    exactly like the project-wide resume does, converging the issue to
    Done exactly once, and a replay of the same command repeats
    nothing."""

    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.domain import IssueState
    from tests.integration.test_codex_merge_acceptance import GOOD, Acceptance

    harness_root = tmp_path / "acceptance-harness"
    harness_root.mkdir()
    harness = Acceptance(harness_root)
    try:
        harness.seat_cell()
        harness.gates.require(
            "ENG-9",
            instruction_id="chat-accept-eng-9",
            predicates=("live_smoke",),
        )
        outcome = asyncio.run(harness.submit("ENG-9", GOOD))
        assert outcome.state == "merged"
        assert (
            harness.queue.get("ENG-9").state
            is IssueState.POST_MERGE_ACCEPTANCE
        )

        harness.gates.satisfy("ENG-9", evidence={"live_smoke": "receipt-1"})
        # The crash point: satisfaction is durable, completion never ran.
        assert (
            harness.queue.get("ENG-9").state
            is IssueState.POST_MERGE_ACCEPTANCE
        )
        assert not any(t[1] == "Done" for t in harness.linear.targets)

        monkeypatch.setattr(
            cli_module,
            "_open_merge_flow",
            lambda settings, runtime: types.SimpleNamespace(
                reviews=harness.service
            ),
        )
        settle = [
            *base_arguments(configured_repo),
            "merge-settle",
            "--project",
            "demo",
            "--review",
            outcome.review_id,
            "--json",
        ]

        result = invoke(settle)

        assert result.exit_code == 0
        [payload] = json.loads(result.stdout)
        assert payload["state"] == "merged"
        assert payload["review_id"] == outcome.review_id
        assert harness.queue.get("ENG-9").state is IssueState.DONE
        assert [
            t for t in harness.linear.targets if t[1] == "Done"
        ] == [("ENG-9", "Done", "operator")]

        def done_transitions() -> int:
            return int(
                harness.database.scalar(
                    "SELECT COUNT(*) FROM events WHERE event_type = "
                    "'issue.transitioned' AND aggregate_id = 'ENG-9' AND "
                    "payload_json LIKE '%\"to\":\"done\"%'"
                )
            )

        assert done_transitions() == 1

        # Exactly once: replaying the same maintenance command changes
        # nothing — no second transition, no second Linear projection.
        replay = invoke(settle)
        assert replay.exit_code == 0
        assert done_transitions() == 1
        assert [
            t for t in harness.linear.targets if t[1] == "Done"
        ] == [("ENG-9", "Done", "operator")]
    finally:
        harness.close()


# -- awaiting-handoff report and submit-handoff continuation (INFRA-198) ---


def test_rotate_lead_reports_awaiting_handoff_non_terminally(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sol 52d15493: the awaiting report is a successful non-terminal
    outcome — the fresh-handoff request was filed and the rotation
    resumes on submission — never a refusal exit."""

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _bind_rotate_lead_cell(state_dir, cell_id="cell-1")
    report = _FakeRotationReport(
        phase="awaiting_handoff",
        cell_id="cell-1",
        handoff_id="handoff-consumed",
        replacement_session=None,
        profile=None,
        binding_id=None,
        failure=None,
        request_id="request-1",
    )
    _install_fake_lead_rotation(monkeypatch, report=report)

    result = invoke(
        [
            *base_arguments(configured_repo),
            "rotate-lead",
            "--cell",
            "cell-1",
            "--json",
        ]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["phase"] == "awaiting_handoff"
    assert payload["request_id"] == "request-1"
    assert payload["failure"] is None


def _seed_consumed_handoff_cell_state(state_dir: Path) -> None:
    """The live-defect durable shape (Sol 52d15493): an active max-b
    incumbent whose newest handoff is already acknowledged onto that
    same incumbent with NO open rotation attempt — consumed."""

    from uuid import UUID

    from hermes_orchestrator.db import Database
    from hermes_orchestrator.handoffs import HandoffService

    _seed_rotation_cell_state(state_dir)
    database = Database.open(state_dir / "state.db")
    try:
        HandoffService(database).acknowledge(
            "handoff-1",
            UUID("11111111-1111-4111-8111-111111111111"),
            "stale: replacement is the incumbent",
            profile_alias="max-b",
        )
    finally:
        database.close()


def test_one_rotation_request_resumes_automatically_on_submit_handoff(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The INFRA-198 acceptance shape end to end over the real CLI: ONE
    Hermes-owned rotate-lead on the consumed newest handoff files the
    durable fresh-handoff request and wake; the incumbent's ONE
    submit-handoff (non-derivable content only) submits the derived
    document and the SAME rotation resumes automatically to completion,
    changing both profile and session — no second rotate-lead, no
    manual prompt input anywhere."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_consumed_handoff_cell_state(state_dir)
    # Current fable-capacity evidence for the replacement candidate the
    # continuation must select (max-b is the incumbent).
    _seed_capacity_observation(
        state_dir,
        "max-c",
        "available",
        observed_at=datetime.now(UTC) - timedelta(hours=1),
    )
    started = _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)

    first = invoke(
        [
            *base_arguments(configured_repo),
            "rotate-lead",
            "--cell",
            "cell-demo",
            "--json",
        ]
    )

    assert first.exit_code == 0
    awaiting = json.loads(first.stdout)
    assert awaiting["phase"] == "awaiting_handoff"
    assert awaiting["handoff_id"] == "handoff-1"
    assert awaiting["request_id"] is not None
    assert started == []  # nothing launched while awaiting

    database = Database.open(state_dir / "state.db")
    try:
        request = database.execute(
            "SELECT reason, state FROM handoff_requests "
            "WHERE cell_id = 'cell-demo'"
        ).fetchone()
        assert request is not None
        assert str(request["state"]) == "requested"
        reason = str(request["reason"])
        assert "consumed=handoff-1" in reason
        assert "profile=max-b" in reason
        assert "session=11111111-1111-4111-8111-111111111111" in reason
        assert (
            "provide only decisions, caveats/blockers, risks, "
            "and the exact next action" in reason
        )
        wake = database.execute(
            "SELECT kind, state FROM lead_terminal_wakes "
            "WHERE cell_id = 'cell-demo'"
        ).fetchone()
        assert wake is not None
        assert str(wake["kind"]) == "handoff_required"
        assert database.scalar(
            "SELECT state FROM project_cells WHERE cell_id = 'cell-demo'"
        ) == "handoff_required"
    finally:
        database.close()

    second = invoke(
        [
            *base_arguments(configured_repo),
            "submit-handoff",
            "--cell",
            "cell-demo",
            "--decision",
            "Keep the existing public interface.",
            "--caveat",
            "CI flake on the network suite.",
            "--risk",
            "None known beyond the flake.",
            "--next-action",
            "Run the failing test and correct ENG-9.",
            "--json",
        ]
    )

    assert second.exit_code == 0
    payload = json.loads(second.stdout)
    resumed = payload["resumed_rotation"]
    assert resumed is not None
    assert resumed["phase"] == "complete"
    assert resumed["profile"] == "max-c"
    assert resumed["replacement_session"] != (
        "11111111-1111-4111-8111-111111111111"
    )
    assert started == ["max-c"]  # exactly one replacement launch, post-resume

    database = Database.open(state_dir / "state.db")
    try:
        cell = database.execute(
            "SELECT state, profile_alias, session_id FROM project_cells "
            "WHERE cell_id = 'cell-demo'"
        ).fetchone()
        assert str(cell["state"]) == "active"
        assert str(cell["profile_alias"]) == "max-c"
        assert str(cell["session_id"]) == resumed["replacement_session"]
        submitted = database.execute(
            "SELECT handoff_id, state FROM handoffs "
            "WHERE handoff_id = ?",
            (payload["handoff_id"],),
        ).fetchone()
        assert str(submitted["state"]) == "acknowledged"
    finally:
        database.close()


def test_submit_handoff_without_an_awaiting_rotation_just_submits(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ordinary submission (no awaiting marker) stores the durable
    handoff and resumes nothing — the continuation never misfires."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    started = _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)

    result = invoke(
        [
            *base_arguments(configured_repo),
            "submit-handoff",
            "--cell",
            "cell-demo",
            "--decision",
            "Keep the existing public interface.",
            "--next-action",
            "Run the failing test and correct ENG-9.",
            "--json",
        ]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["state"] == "submitted"
    assert payload["resumed_rotation"] is None
    assert started == []

    database = Database.open(state_dir / "state.db")
    try:
        assert database.scalar(
            "SELECT count(*) FROM handoffs WHERE handoff_id = ?",
            (payload["handoff_id"],),
        ) == 1
        # The incumbent cell is untouched by an ordinary submission.
        assert database.scalar(
            "SELECT state FROM project_cells WHERE cell_id = 'cell-demo'"
        ) == "active"
    finally:
        database.close()


# -- seat-derived handoff issue and branch (INFRA-198) ---------------------

_SEAT_SESSION = "11111111-1111-4111-8111-111111111111"


def _seed_admitted_issues(
    state_dir: Path, *issue_ids: str, state: str = "in_development"
) -> None:
    """Durably admit each issue as active work on the demo project."""

    from hermes_orchestrator.db import Database

    database = Database.open(state_dir / "state.db")
    try:
        now = datetime.now(UTC).isoformat()
        with database.transaction() as connection:
            for issue_id in issue_ids:
                connection.execute(
                    "INSERT INTO admitted_issues("
                    "issue_id, project_key, priority, state, instruction_id, "
                    "dependency_ready, overlap_risk, admitted_at, updated_at"
                    ") VALUES (?, 'demo', 1, ?, ?, 1, 0, ?, ?)",
                    (issue_id, state, f"instr-{issue_id}", now, now),
                )
    finally:
        database.close()


def _seed_lead_assignment(
    state_dir: Path,
    *,
    issue_id: str,
    cell_id: str = "cell-demo",
    session_id: str = _SEAT_SESSION,
) -> None:
    """Publish the seat's own durable lead assignment for ``issue_id``."""

    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.lead_assignments import LeadAssignments

    database = Database.open(state_dir / "state.db")
    try:
        assignments = LeadAssignments(database, events=EventStore(database))
        with database.transaction() as connection:
            assignments.publish_in(
                connection,
                project_key="demo",
                issue_id=issue_id,
                cell_id=cell_id,
                session_id=session_id,
                profile_alias="max-b",
                instruction_id=f"instr-{issue_id}",
                queue_transition="queued->in_development",
            )
    finally:
        database.close()


def _seed_worktree_lease(
    state_dir: Path, *, issue_id: str, path: Path, branch: str
) -> None:
    """Register one ACTIVE worktree lease binding ``issue_id`` to a lane."""

    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.worktrees import WorktreeLeaseInput, WorktreeLeases

    database = Database.open(state_dir / "state.db")
    try:
        WorktreeLeases(database, EventStore(database)).register(
            WorktreeLeaseInput(
                project_key="demo",
                issue_id=issue_id,
                repo_path=str(state_dir.parent),
                path=str(path),
                branch=branch,
                remote="origin",
            )
        )
    finally:
        database.close()


def _install_detached_coordinator_probe(
    monkeypatch: pytest.MonkeyPatch, lanes: dict[Path, str]
) -> list[str]:
    """Probe git per PATH: each leased lane reports its own branch, and
    every other checkout -- the coordinator's own ``lead_cwd`` -- reports
    the DETACHED shape ``_worktree_state`` maps an unreadable branch to.

    Returns the list of paths probed, in order.
    """

    import hermes_orchestrator.cli as cli_module

    probed: list[str] = []
    branches = {str(path): branch for path, branch in lanes.items()}

    def _probe(path: Path) -> Any:
        probed.append(str(path))
        return cli_module.WorktreeState(
            branch=branches.get(str(path), ""),
            head="a",
            origin_head="a",
            dirty=False,
        )

    monkeypatch.setattr(cli_module, "_worktree_state", _probe)
    return probed


def _submit_handoff_for_demo_cell(
    configured_repo: tuple[Path, Path],
) -> CliResult:
    return invoke(
        [
            *base_arguments(configured_repo),
            "submit-handoff",
            "--cell",
            "cell-demo",
            "--decision",
            "Keep the existing public interface.",
            "--next-action",
            "Run the failing test and correct ENG-9.",
            "--json",
        ]
    )


def test_submit_handoff_derives_issue_and_branch_from_the_seat(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The live INFRA-198 defect: the project has SEVERAL active issues
    and the coordinator's own checkout is detached, so the old
    project-wide derivation yielded issue ``none`` and an empty branch
    ("handoff field is incomplete: branch"). The cell's own live lead
    assignment names its issue, and that issue's single active worktree
    lease names the tree to probe -- so the submission succeeds carrying
    both."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    _seed_admitted_issues(state_dir, "INFRA-212", "INFRA-215")
    _seed_lead_assignment(state_dir, issue_id="INFRA-215")
    lane = repo_root / "lane-215"
    _seed_worktree_lease(
        state_dir,
        issue_id="INFRA-215",
        path=lane,
        branch="feature/infra-215-lane",
    )
    _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)
    probed = _install_detached_coordinator_probe(
        monkeypatch, {lane: "feature/infra-215-lane"}
    )

    result = _submit_handoff_for_demo_cell(configured_repo)

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert str(lane) in probed
    database = Database.open(state_dir / "state.db")
    try:
        document = json.loads(
            str(
                database.scalar(
                    "SELECT document_json FROM handoffs WHERE handoff_id = ?",
                    (payload["handoff_id"],),
                )
            )
        )
    finally:
        database.close()
    assert document["branch"] == "feature/infra-215-lane"
    assert "INFRA-215" in document["objective"]
    assert "issue INFRA-215 is in_development" in document["status"]


def test_submit_handoff_derives_the_seat_whose_assignment_was_superseded(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The real dual-lane sequence: the harness cell takes its issue, and
    the SAME issue is then assigned to a different cell, which marks the
    harness row ``superseded``. Supersession is per-ISSUE, so it says
    nothing about whether the harness cell is still bound and working --
    that cell must still derive its issue and its leased branch."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    _seed_admitted_issues(state_dir, "INFRA-212", "INFRA-215")
    _seed_lead_assignment(state_dir, issue_id="INFRA-215")
    # The other lane later takes the same issue: per-issue supersession
    # rewrites the bound harness seat's row without unbinding the seat.
    _seed_lead_assignment(
        state_dir,
        issue_id="INFRA-215",
        cell_id="cell-other",
        session_id="66666666-6666-4666-8666-666666666666",
    )
    lane = repo_root / "lane-215"
    _seed_worktree_lease(
        state_dir,
        issue_id="INFRA-215",
        path=lane,
        branch="feature/infra-215-lane",
    )
    _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)
    probed = _install_detached_coordinator_probe(
        monkeypatch, {lane: "feature/infra-215-lane"}
    )
    database = Database.open(state_dir / "state.db")
    try:
        # Precondition: the sequence really did supersede the seat's row.
        assert (
            database.scalar(
                "SELECT state FROM lead_assignments WHERE cell_id = 'cell-demo'"
            )
            == "superseded"
        )
    finally:
        database.close()

    result = _submit_handoff_for_demo_cell(configured_repo)

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert str(lane) in probed
    database = Database.open(state_dir / "state.db")
    try:
        document = json.loads(
            str(
                database.scalar(
                    "SELECT document_json FROM handoffs WHERE handoff_id = ?",
                    (payload["handoff_id"],),
                )
            )
        )
    finally:
        database.close()
    assert document["branch"] == "feature/infra-215-lane"
    assert "INFRA-215" in document["objective"]
    assert "issue INFRA-215 is in_development" in document["status"]


def test_submit_handoff_prefers_the_newest_assignment_for_the_same_seat(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Freshness for the seat comes from the ordering, not from the row
    state: with two assignments for the SAME cell and session, the newer
    one names the issue and the branch."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    _seed_admitted_issues(state_dir, "INFRA-212", "INFRA-215")
    _seed_lead_assignment(state_dir, issue_id="INFRA-212")
    _seed_lead_assignment(state_dir, issue_id="INFRA-215")
    older = repo_root / "lane-212"
    newer = repo_root / "lane-215"
    _seed_worktree_lease(
        state_dir,
        issue_id="INFRA-212",
        path=older,
        branch="feature/infra-212-lane",
    )
    _seed_worktree_lease(
        state_dir,
        issue_id="INFRA-215",
        path=newer,
        branch="feature/infra-215-lane",
    )
    _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)
    _install_detached_coordinator_probe(
        monkeypatch,
        {older: "feature/infra-212-lane", newer: "feature/infra-215-lane"},
    )

    result = _submit_handoff_for_demo_cell(configured_repo)

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    database = Database.open(state_dir / "state.db")
    try:
        document = json.loads(
            str(
                database.scalar(
                    "SELECT document_json FROM handoffs WHERE handoff_id = ?",
                    (payload["handoff_id"],),
                )
            )
        )
    finally:
        database.close()
    assert document["branch"] == "feature/infra-215-lane"
    assert "issue INFRA-215 is in_development" in document["status"]


def test_submit_handoff_refuses_a_cell_and_session_no_assignment_names(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Dropping the state filter widens nothing beyond the exact bound
    identity: an assignment naming a different cell, and one naming this
    cell under a stale session, both leave the seat underivable and the
    ambiguous project still refuses with the actionable message."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    _seed_admitted_issues(state_dir, "INFRA-212", "INFRA-215")
    _seed_lead_assignment(
        state_dir,
        issue_id="INFRA-215",
        cell_id="cell-other",
        session_id="66666666-6666-4666-8666-666666666666",
    )
    _seed_lead_assignment(
        state_dir,
        issue_id="INFRA-212",
        cell_id="cell-demo",
        session_id="77777777-7777-4777-8777-777777777777",
    )
    _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)
    _install_detached_coordinator_probe(monkeypatch, {})

    result = _submit_handoff_for_demo_cell(configured_repo)

    assert result.exit_code == 1
    message = json.loads(result.stdout)["error"]
    assert "no live lead assignment" in message
    assert "2 active issues (INFRA-212, INFRA-215)" in message
    assert "assign the cell its issue" in message
    database = Database.open(state_dir / "state.db")
    try:
        assert database.scalar("SELECT count(*) FROM handoffs") == 1
    finally:
        database.close()


def test_submit_handoff_refuses_an_unassigned_cell_in_an_ambiguous_project(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """No live assignment binds the cell and the project has several
    active issues: the handoff's identity is underivable, so the command
    refuses with an actionable message and writes nothing."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    _seed_admitted_issues(state_dir, "INFRA-212", "INFRA-215")
    _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)
    _install_detached_coordinator_probe(monkeypatch, {})

    result = _submit_handoff_for_demo_cell(configured_repo)

    assert result.exit_code == 1
    message = json.loads(result.stdout)["error"]
    assert "no live lead assignment" in message
    assert "2 active issues (INFRA-212, INFRA-215)" in message
    assert "assign the cell its issue" in message
    database = Database.open(state_dir / "state.db")
    try:
        # Only the handoff seeded by the incumbent state exists.
        assert database.scalar("SELECT count(*) FROM handoffs") == 1
    finally:
        database.close()


def test_submit_handoff_refuses_when_the_assigned_issue_has_no_one_lease(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The seat's issue is unambiguous but its lane is not: zero active
    leases and several active leases each refuse with their own
    actionable message, and never fall back to the coordinator's tree."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    _seed_admitted_issues(state_dir, "INFRA-212", "INFRA-215")
    _seed_lead_assignment(state_dir, issue_id="INFRA-215")
    _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)
    _install_detached_coordinator_probe(monkeypatch, {})

    without_lease = _submit_handoff_for_demo_cell(configured_repo)

    assert without_lease.exit_code == 1
    message = json.loads(without_lease.stdout)["error"]
    assert "is assigned issue 'INFRA-215'" in message
    assert "has no active worktree lease" in message
    assert "leave exactly one active worktree lease" in message

    for suffix in ("a", "b"):
        _seed_worktree_lease(
            state_dir,
            issue_id="INFRA-215",
            path=repo_root / f"lane-215-{suffix}",
            branch=f"feature/infra-215-{suffix}",
        )

    with_two_leases = _submit_handoff_for_demo_cell(configured_repo)

    assert with_two_leases.exit_code == 1
    ambiguous = json.loads(with_two_leases.stdout)["error"]
    assert "has 2 active worktree leases" in ambiguous

    # A per-issue supersession of the seat's own row relaxes nothing: the
    # issue is still derived from the bound seat and still demands
    # exactly one active lease.
    _seed_lead_assignment(
        state_dir,
        issue_id="INFRA-215",
        cell_id="cell-other",
        session_id="66666666-6666-4666-8666-666666666666",
    )

    superseded = _submit_handoff_for_demo_cell(configured_repo)

    assert superseded.exit_code == 1
    assert "has 2 active worktree leases" in json.loads(superseded.stdout)["error"]
    database = Database.open(state_dir / "state.db")
    try:
        assert database.scalar("SELECT count(*) FROM handoffs") == 1
    finally:
        database.close()


def test_submit_handoff_keeps_the_single_active_issue_fallback(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserved: a cell with no assignment in a project with exactly one
    active issue still derives that issue and still probes the
    coordinator's own worktree, exactly as before."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    _seed_admitted_issues(state_dir, "INFRA-212")
    _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)

    result = _submit_handoff_for_demo_cell(configured_repo)

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    database = Database.open(state_dir / "state.db")
    try:
        document = json.loads(
            str(
                database.scalar(
                    "SELECT document_json FROM handoffs WHERE handoff_id = ?",
                    (payload["handoff_id"],),
                )
            )
        )
    finally:
        database.close()
    assert document["branch"] == "main"
    assert "issue INFRA-212 is in_development" in document["status"]


def _install_seat_lane_probe(
    monkeypatch: pytest.MonkeyPatch, lanes: dict[Path, Any]
) -> list[str]:
    """Probe git per PATH, with the COORDINATOR's tree left unusable.

    Every path named in ``lanes`` reports the given state; every other
    checkout -- notably ``project.lead_cwd`` -- reports the live INFRA-198
    coordinator shape: detached (no branch, no readable ``origin/<branch>``)
    AND dirty from an unrelated third party's untracked workspace. Any
    rotation that measures the coordinator therefore refuses, so a passing
    rotation proves the seat's own lane was measured. Returns the probed
    paths, in order.

    Installed AFTER ``_install_rotation_process_and_probe_fakes``, whose
    unconditional clean probe this deliberately replaces.
    """

    import hermes_orchestrator.cli as cli_module

    probed: list[str] = []
    states = {str(path): state for path, state in lanes.items()}

    def _probe(path: Path) -> Any:
        probed.append(str(path))
        return states.get(
            str(path),
            cli_module.WorktreeState(
                branch="", head="ec7ade4", origin_head="", dirty=True
            ),
        )

    monkeypatch.setattr(cli_module, "_worktree_state", _probe)
    return probed


def _lane_state(
    *, head: str = "2a85585", origin_head: str = "2a85585", dirty: bool = False
) -> Any:
    """One lane's probe result; clean, pushed, and on its own branch by
    default -- the shape the live INFRA-198 harness lease actually had."""

    import hermes_orchestrator.cli as cli_module

    return cli_module.WorktreeState(
        branch="feature/infra-198-harness-trust",
        head=head,
        origin_head=origin_head,
        dirty=dirty,
    )


def _rotate_lead_for_demo_cell(
    configured_repo: tuple[Path, Path],
) -> CliResult:
    return invoke(
        [
            *base_arguments(configured_repo),
            "rotate-lead",
            "--cell",
            "cell-demo",
            "--json",
        ]
    )


def test_rotate_lead_measures_the_bound_seat_worktree_not_the_coordinator(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The live INFRA-198 defect: the rotation precondition closed over
    ``project.lead_cwd``, so the harness cell was judged by the
    COORDINATOR's checkout -- detached and dirtied by an unrelated
    third-party workspace -- and refused with "project HEAD does not
    match the pushed origin branch head". The cell's own bound issue
    lease names a clean, pushed lane, so the rotation proceeds past the
    precondition and completes; the coordinator's tree is never probed."""


    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    _seed_admitted_issues(state_dir, "INFRA-212", "INFRA-215")
    _seed_lead_assignment(state_dir, issue_id="INFRA-215")
    lane = repo_root / "lane-215"
    _seed_worktree_lease(
        state_dir,
        issue_id="INFRA-215",
        path=lane,
        branch="feature/infra-198-harness-trust",
    )
    _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)
    probed = _install_seat_lane_probe(monkeypatch, {lane: _lane_state()})

    result = _rotate_lead_for_demo_cell(configured_repo)

    payload = json.loads(result.stdout)
    # The precondition is the subject here: rotation must get PAST it on
    # the bound seat's clean, pushed lane. Whether a replacement profile
    # happens to be available is a different precondition entirely, and
    # standing a healthy pool up would test that instead of this.
    assert payload["phase"] != "precondition", payload
    # The leased lane was measured -- never the project coordinator.
    assert probed == [str(lane)]
    assert str(repo_root) not in probed


def test_rotate_lead_refuses_a_dirty_bound_seat_worktree(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only WHICH tree is measured changed: the existing cleanliness
    assertion still refuses when the seat's OWN bound lane is dirty, and
    nothing rotates."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    _seed_admitted_issues(state_dir, "INFRA-215")
    _seed_lead_assignment(state_dir, issue_id="INFRA-215")
    lane = repo_root / "lane-215"
    _seed_worktree_lease(
        state_dir,
        issue_id="INFRA-215",
        path=lane,
        branch="feature/infra-198-harness-trust",
    )
    _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)
    _install_seat_lane_probe(monkeypatch, {lane: _lane_state(dirty=True)})

    result = _rotate_lead_for_demo_cell(configured_repo)

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["phase"] == "precondition"
    assert payload["failure"] == (
        "project worktree has uncommitted changes; rotation refused"
    )
    database = Database.open(state_dir / "state.db")
    try:
        cell = database.execute(
            "SELECT profile_alias FROM project_cells WHERE cell_id = 'cell-demo'"
        ).fetchone()
        assert str(cell["profile_alias"]) == "max-b"
    finally:
        database.close()


def test_rotate_lead_refuses_an_origin_mismatched_bound_seat_worktree(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The head-vs-origin assertion is unchanged too: an unpushed
    commit on the seat's own bound lane still refuses."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    _seed_admitted_issues(state_dir, "INFRA-215")
    _seed_lead_assignment(state_dir, issue_id="INFRA-215")
    lane = repo_root / "lane-215"
    _seed_worktree_lease(
        state_dir,
        issue_id="INFRA-215",
        path=lane,
        branch="feature/infra-198-harness-trust",
    )
    _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)
    _install_seat_lane_probe(monkeypatch, {lane: _lane_state(head="9f1c2d3")})

    result = _rotate_lead_for_demo_cell(configured_repo)

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["phase"] == "precondition"
    assert payload["failure"] == (
        "project HEAD does not match the pushed origin branch head; "
        "push before rotating"
    )
    database = Database.open(state_dir / "state.db")
    try:
        cell = database.execute(
            "SELECT profile_alias FROM project_cells WHERE cell_id = 'cell-demo'"
        ).fetchone()
        assert str(cell["profile_alias"]) == "max-b"
    finally:
        database.close()


def test_rotate_lead_never_adopts_another_cells_lease(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A clean, pushed lane belonging to some OTHER seat never stands in
    for this one. Neither an assignment naming a different cell/session,
    nor a lease bound to an issue this cell is not assigned, qualifies:
    both fail closed naming exactly what was missing, and the tempting
    clean lane is never probed."""

    from hermes_orchestrator.db import Database

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    _seed_admitted_issues(state_dir, "INFRA-212", "INFRA-215")
    other_lane = repo_root / "lane-212"
    _seed_worktree_lease(
        state_dir,
        issue_id="INFRA-212",
        path=other_lane,
        branch="feature/infra-212-lane",
    )
    _seed_lead_assignment(
        state_dir,
        issue_id="INFRA-212",
        cell_id="cell-other",
        session_id="66666666-6666-4666-8666-666666666666",
    )
    _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)
    probed = _install_seat_lane_probe(
        monkeypatch, {other_lane: _lane_state()}
    )

    unassigned = _rotate_lead_for_demo_cell(configured_repo)

    assert unassigned.exit_code == 1
    message = json.loads(unassigned.stdout)["error"]
    assert "cell 'cell-demo' has no live lead assignment" in message
    assert "2 active issues (INFRA-212, INFRA-215)" in message
    assert "assign the cell its issue before rotating" in message

    # Now the cell IS assigned -- but to the issue whose lane nobody
    # leased. The other cell's clean lease still never qualifies.
    _seed_lead_assignment(state_dir, issue_id="INFRA-215")

    mismatched = _rotate_lead_for_demo_cell(configured_repo)

    assert mismatched.exit_code == 1
    message = json.loads(mismatched.stdout)["error"]
    assert "cell 'cell-demo' is assigned issue 'INFRA-215'" in message
    assert "has no active worktree lease" in message
    assert "leave exactly one active worktree lease for INFRA-215" in message
    assert "and rotate again" in message

    assert probed == []
    database = Database.open(state_dir / "state.db")
    try:
        cell = database.execute(
            "SELECT profile_alias FROM project_cells WHERE cell_id = 'cell-demo'"
        ).fetchone()
        assert str(cell["profile_alias"]) == "max-b"
    finally:
        database.close()


def test_rotate_lead_keeps_the_unassigned_single_lane_fallback(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserved: a genuinely unassigned cell in a project with at most
    one active issue is still judged by ``project.lead_cwd``, exactly as
    before -- the seat-bound selection widens nothing for single-lane
    callers."""

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)
    _seed_rotation_cell_state(state_dir)
    _seed_admitted_issues(state_dir, "INFRA-212")
    _install_rotation_process_and_probe_fakes(monkeypatch, state_dir)
    probed = _install_seat_lane_probe(
        monkeypatch, {repo_root: _lane_state()}
    )

    result = _rotate_lead_for_demo_cell(configured_repo)

    payload = json.loads(result.stdout)
    assert payload["phase"] != "precondition", payload
    # ``project.lead_cwd`` is ``repo_path`` for this project, and it is
    # the ONLY tree the precondition measured.
    assert probed == [str(repo_root)]


def test_open_rotation_collaborators_validates_or_provisions_harness_checkout(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """INFRA-219 R7 / Sol correction 110ed759: cli.py:1152-1161 derived a
    sibling harness path but left worktree provisioning out of scope --
    nothing ever created or validated that checkout. A harness-lane call
    to ``_open_rotation_collaborators`` now runs
    ``ensure_harness_checkout`` against exactly the project's real
    repository path and its ``_harness_lead_cwd`` sibling before
    composing the seater/cell service; a development-lane call, and a
    harness-lane call with no ``harness_project_key``, never touch it."""

    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.config import load_settings
    from hermes_orchestrator.runtime import open_runtime

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)

    calls: list[dict[str, object]] = []

    def _fake_ensure_harness_checkout(
        git: object,
        *,
        repo_path: object,
        harness_path: object,
        expected_branch: object = None,
    ) -> object:
        calls.append({"repo_path": repo_path, "harness_path": harness_path})
        return object()

    monkeypatch.setattr(
        cli_module, "ensure_harness_checkout", _fake_ensure_harness_checkout
    )

    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(settings, enable_live=False)
    try:
        cli_module._open_rotation_collaborators(
            settings, runtime, lane_role=cli_module.DEVELOPMENT_LANE
        )
        assert calls == []

        cli_module._open_rotation_collaborators(
            settings, runtime, lane_role=cli_module.HARNESS_LANE
        )
        assert calls == []

        cli_module._open_rotation_collaborators(
            settings,
            runtime,
            lane_role=cli_module.HARNESS_LANE,
            harness_project_key="demo",
        )
    finally:
        runtime.close()

    project = settings.projects["demo"]
    assert calls == [
        {
            "repo_path": project.repo_path,
            "harness_path": cli_module._harness_lead_cwd(project.lead_cwd),
        }
    ]


def test_open_rotation_collaborators_propagates_harness_checkout_refusal(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A mismatched or foreign harness checkout must refuse fail-closed
    all the way out of ``_open_rotation_collaborators`` as the same
    ``ValueError`` its docstring already promises callers turn into a
    clean, nonzero CLI refusal -- never a traceback, never a silent
    launch."""

    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.cells import HarnessCheckoutRefused
    from hermes_orchestrator.config import load_settings
    from hermes_orchestrator.runtime import open_runtime

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)

    def _refusing(*args: object, **kwargs: object) -> object:
        raise HarnessCheckoutRefused("foreign harness checkout")

    monkeypatch.setattr(cli_module, "ensure_harness_checkout", _refusing)

    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(settings, enable_live=False)
    try:
        with pytest.raises(ValueError, match="foreign harness checkout"):
            cli_module._open_rotation_collaborators(
                settings,
                runtime,
                lane_role=cli_module.HARNESS_LANE,
                harness_project_key="demo",
            )
    finally:
        runtime.close()


def test_start_lane_harness_checkout_refusal_exits_nonzero(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """End to end through ``start-lane``: a refused harness checkout
    (mismatched or foreign) never reaches ``dispatch`` and exits
    nonzero with a clear message, exactly like the existing
    ``harness_run_required`` CLI-boundary refusal."""

    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.cells import HarnessCheckoutRefused

    repo_root, _state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)

    def _refusing(*args: object, **kwargs: object) -> object:
        raise HarnessCheckoutRefused(
            "harness checkout is not a worktree of this repository"
        )

    monkeypatch.setattr(cli_module, "ensure_harness_checkout", _refusing)

def test_target_issue_refuses_an_unknown_issue_without_writes(
    configured_repo: tuple[Path, Path],
) -> None:
    # INFRA-220: the CLI surface reaches the strict transition and its
    # fail-closed refusal exits non-zero without publishing anything.
    invoke([*base_arguments(configured_repo), "init"])

    result = invoke(
        [
            *base_arguments(configured_repo),
            "start-lane",
            "--project",
            "demo",
            "--lane",
            "harness",
            "--issue",
            "ENG-1",
            "--harness-run",
            "run-1",
            "--json",

            "target-issue",
            "ENG-404",
            "--project",
            "demo",
            "--cell",
            "cell-1",
            "--session",
            "session-1",
            "--instruction",
            "work this issue next",
        ]
    )

    assert result.exit_code == 1
    assert "not a worktree" in json.loads(result.stdout)["error"]


def test_open_rotation_collaborators_selects_prompt_by_lane(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """INFRA-219 R7 / Sol correction 110ed759: the harness lane must
    launch with an operational-only prompt distinct from the
    development lead's ``prompts/claude-lead.md`` -- selected by
    ``lane_role``, not shared."""

    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.config import load_settings
    from hermes_orchestrator.runtime import open_runtime

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    _write_profiles_config(repo_root)

    monkeypatch.setattr(
        cli_module,
        "ensure_harness_checkout",
        lambda *args, **kwargs: object(),
    )

    prompt_files: list[Path] = []

    class _RecordingRunner:
        def __init__(self, *args: object, prompt_file: Path, **kwargs: object) -> None:
            prompt_files.append(prompt_file)

    monkeypatch.setattr(cli_module, "ClaudeRunner", _RecordingRunner)

    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(settings, enable_live=False)
    try:
        cli_module._open_rotation_collaborators(
            settings, runtime, lane_role=cli_module.DEVELOPMENT_LANE
        )
        cli_module._open_rotation_collaborators(
            settings,
            runtime,
            lane_role=cli_module.HARNESS_LANE,
            harness_project_key="demo",
        )
    finally:
        runtime.close()

    assert [path.name for path in prompt_files] == [
        "claude-lead.md",
        "claude-harness.md",
    ]
    assert prompt_files[1].parent == prompt_files[0].parent


def test_start_lane_composes_the_visible_classic_seat_not_a_hidden_runner(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """INFRA-214 (observed live 2026-09-01): ``start-lane``'s one-shot
    composition omitted classic-seat mode, so ``_activate_seat`` passed
    ``classic_command=None`` -- Hermes created an EMPTY cmux workspace
    and separately launched a hidden ``claude -p`` shadow process
    instead of the visible channel-enabled classic session. The
    composition must agree with the daemon's own predicate."""

    import hermes_orchestrator.cli as cli_module
    from hermes_orchestrator.config import load_settings

    repo_root, state_dir = configured_repo
    _write_cmux_config(repo_root)
    settings = load_settings(repo_root, state_dir)

    assert settings.cmux is not None
    # The daemon's predicate (runtime.py) and this command's must agree:
    # with cmux configured for classic leads, the seat is classic.
    assert settings.cmux.classic_leads is True
    source = inspect.getsource(cli_module._open_rotation_collaborators)
    assert "classic_seats=" in source, (
        "start-lane's composition must set classic_seats; omitting it is "
        "what produced the empty workspace plus hidden claude -p shadow"
    )
    assert "settings.cmux.classic_leads" in source


def test_start_lane_prompt_resolution_fails_closed_before_launch(
    configured_repo: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing prompt asset must be refused BEFORE any process is
    launched -- the observed failure handed Claude a path that did not
    exist and let it exit 1 (INFRA-214)."""

    import hermes_orchestrator.cli as cli_module

    repo_root, _state_dir = configured_repo
    _write_cmux_config(repo_root)
    (repo_root / "prompts" / "claude-harness.md").unlink()

    source = inspect.getsource(cli_module._open_rotation_collaborators)
    assert "resolve_prompt_file" in source
    assert "refusing to launch a lead with an unresolvable prompt asset" in source


def test_start_lane_composition_wires_assignments_for_classic_seats(
    configured_repo: tuple[Path, Path],
) -> None:
    """INFRA-214: with ``classic_seats`` enabled,
    ``_activate_issue_transaction`` publishes the lead's durable
    assignment through ``self._assignments``. Omitting it would let the
    visible harness seat launch correctly and then sit IDLE with no
    assignment or channel wake -- the same silent-idle failure class
    this issue exists to remove, and one that would only surface as a
    lead that started and did nothing."""

    import hermes_orchestrator.cli as cli_module

    source = inspect.getsource(cli_module._open_rotation_collaborators)
    assert "classic_seats=" in source
    assert "assignments=LeadAssignments(" in source, (
        "classic seats publish the assignment through self._assignments; "
        "the composition must supply it or the seat starts and sits idle"
    )


def _git(repository: Path, *arguments: str) -> None:
    subprocess.run(
        ("git", *arguments),
        cwd=repository,
        check=True,
        capture_output=True,
    )


def _configured_git_project(tmp_path: Path) -> tuple[Path, Path]:
    """A registered project whose ``repo_path`` is a REAL checkout.

    The issue-lane binding runs actual git, so proving it from the CLI
    needs a real repository with a real ``origin`` carrying the
    integration branch -- exactly the shape the first assignment of a
    never-before-seen issue meets in production.
    """

    origin = tmp_path / "origin.git"
    repository = tmp_path / "project"
    repository.mkdir()
    subprocess.run(
        ("git", "init", "--bare", "-b", "main", str(origin)),
        check=True,
        capture_output=True,
    )
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "test@example.com")
    _git(repository, "config", "user.name", "Test")
    (repository / "README.md").write_text("demo\n", encoding="utf-8")
    _git(repository, "add", "README.md")
    _git(repository, "commit", "-m", "initial")
    _git(repository, "remote", "add", "origin", str(origin))
    _git(repository, "push", "-u", "origin", "main")

    config = tmp_path / "config"
    config.mkdir()
    (config / "projects.yaml").write_text(
        "projects:\n"
        "  demo:\n"
        "    linear_team: ENG\n"
        f"    repo_path: {repository}\n"
        "    integration_branch: main\n"
        "    github_repo: owner/demo\n",
        encoding="utf-8",
    )
    (config / "policies.yaml").write_text(
        "mode: observe\nmax_unresolved_ci_merges: 2\n",
        encoding="utf-8",
    )
    return tmp_path, tmp_path / "state"


def _admit_through_cli(configured: tuple[Path, Path], issue_id: str) -> None:
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.domain import IssueState
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.queue import AdmissionRequest, QueueService

    _repo_root, state_dir = configured
    database = Database.open(state_dir / "state.db")
    try:
        QueueService(database, EventStore(database), {"demo"}).admit(
            AdmissionRequest(
                issue_id=issue_id,
                project_key="demo",
                linear_priority=1,
                admitted_by="operator",
                instruction_id=f"chat-{issue_id}",
            )
        )
        # The migration exists FOR issues already occupying a lane from
        # before the assignment-time hook existed; that state is the
        # fixture, not something this path is expected to produce.
        database.execute(
            "UPDATE admitted_issues SET state = ? WHERE issue_id = ?",
            (IssueState.IN_DEVELOPMENT.value, issue_id),
        )
    finally:
        database.close()


def test_reconcile_binds_issue_lane_without_a_live_cell_service(
    tmp_path: Path,
) -> None:
    """INFRA-214: the catch-up must BIND from ``reconcile``.

    ``reconcile`` opens a NON-live runtime, which deliberately builds no
    ``ProjectCellService`` (that needs the profile pool, runner and
    Linear client -- none of which this binding touches). Gating the
    catch-up on ``runtime.cells`` made it silently skip and still exit
    zero: the operator saw a clean reconcile while the lease store
    stayed empty and the candidate stayed unpublishable, which is the
    very failure this path exists to remove.
    """

    from hermes_orchestrator.db import Database
    from hermes_orchestrator.emission import resolve_lane
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.worktrees import WorktreeLeases

    configured = _configured_git_project(tmp_path)
    assert invoke([*base_arguments(configured), "init", "--json"]).exit_code == 0
    _admit_through_cli(configured, "ENG-9")

    result = invoke(
        [
            *base_arguments(configured),
            "reconcile",
            "--bind-issue-lane",
            "demo:ENG-9",
            "--json",
        ]
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["bound_issue_lanes"] == ["ENG-9"]

    _repo_root, state_dir = configured
    database = Database.open(state_dir / "state.db")
    try:
        lane = resolve_lane(
            WorktreeLeases(database, EventStore(database)), "demo", "ENG-9"
        )
    finally:
        database.close()
    # The dedicated per-issue checkout, on the derived lane branch --
    # never the coordinator's own working copy.
    assert lane.path == tmp_path / "project-issue-ENG-9"
    assert lane.path.is_dir()
    assert (
        subprocess.run(
            ("git", "rev-parse", "--abbrev-ref", "HEAD"),
            cwd=lane.path,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        == "feature/eng-9"
    )


def test_reconcile_binds_a_post_merge_acceptance_issue_lane(
    tmp_path: Path,
) -> None:
    """INFRA-198: an acceptance-gated issue is held in
    ``post_merge_acceptance`` while its lead keeps working it and keeps
    publishing candidates. Excluding that state made ``candidate-ready``
    refuse for an issue actively under assignment with no supported way
    to bind its lane -- a live publication dead end."""

    from hermes_orchestrator.db import Database
    from hermes_orchestrator.domain import IssueState
    from hermes_orchestrator.emission import resolve_lane
    from hermes_orchestrator.events import EventStore
    from hermes_orchestrator.worktrees import WorktreeLeases

    configured = _configured_git_project(tmp_path)
    assert invoke([*base_arguments(configured), "init", "--json"]).exit_code == 0
    _admit_through_cli(configured, "ENG-9")
    _repo_root, state_dir = configured
    database = Database.open(state_dir / "state.db")
    try:
        database.execute(
            "UPDATE admitted_issues SET state = ? WHERE issue_id = ?",
            (IssueState.POST_MERGE_ACCEPTANCE.value, "ENG-9"),
        )
    finally:
        database.close()

    result = invoke(
        [
            *base_arguments(configured),
            "reconcile",
            "--bind-issue-lane",
            "demo:ENG-9",
            "--json",
        ]
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["bound_issue_lanes"] == ["ENG-9"]
    database = Database.open(state_dir / "state.db")
    try:
        lane = resolve_lane(
            WorktreeLeases(database, EventStore(database)), "demo", "ENG-9"
        )
    finally:
        database.close()
    assert lane.path == tmp_path / "project-issue-ENG-9"


def test_reconcile_reports_an_unbindable_issue_lane_instead_of_exiting_clean(
    tmp_path: Path,
) -> None:
    """A failed catch-up must be LOUD. Reporting success for an issue
    that was never bound is what let the empty lease store survive
    unnoticed until ``candidate-ready`` refused."""

    configured = _configured_git_project(tmp_path)
    assert invoke([*base_arguments(configured), "init", "--json"]).exit_code == 0

    result = invoke(
        [
            *base_arguments(configured),
            "reconcile",
            "--bind-issue-lane",
            "demo:ENG-404",
            "--json",
        ]
    )

    assert result.exit_code == 1
    assert "ENG-404" in json.loads(result.stdout)["error"]
    assert not (tmp_path / "project-issue-ENG-404").exists()

    assert result.output.strip()


def test_target_issue_requires_every_binding_flag(
    configured_repo: tuple[Path, Path],
) -> None:
    result = invoke(
        [*base_arguments(configured_repo), "target-issue", "ENG-7", "--project", "demo"]
    )

    assert result.exit_code == 2
    assert "--cell" in result.output
