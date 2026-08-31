from __future__ import annotations

import asyncio
import json
import sys
import types
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path

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
    assert json.loads(result.stdout)["schema_version"] == 52


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
    """Once the recovered replacement's seat is durably bound, a retry
    must resume through the very same derivation (durable cell, not the
    binding) and report the identical effective replacement without a
    second transfer or a second seat activation: ``already_rotated``
    short-circuits the transfer, and the seat phase's own durable
    binding check short-circuits activation."""

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

    assert second.exit_code == 0
    second_payload = json.loads(second.stdout)
    assert second_payload["phase"] == "complete"
    assert second_payload["replacement_session"] == first_payload["replacement_session"]
    assert second_payload["profile"] == "max-c"
    assert second_payload["binding_id"] == first_payload["binding_id"]
    assert second_payload["failure"] is None
    # Seat activation ran exactly once across both invocations.
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
                "(?, 1, 'daemon.restarted', 'demo', 'cell-demo', ?, "
                "'daemon.restarted:' || ?, '{\"interval_seconds\": 30}', "
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
