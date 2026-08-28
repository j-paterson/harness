"""Observation-only operator command line interface."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from hermes_orchestrator import migration_env as migration_env_module
from hermes_orchestrator.checkpoints import CheckpointDispatcher
from hermes_orchestrator.cmux import CmuxCliAdapter, CmuxError
from hermes_orchestrator.cmux_surfaces import (
    CmuxHibernationDriver,
    CmuxSurfaceReconciler,
)
from hermes_orchestrator.config import Settings, load_settings
from hermes_orchestrator.db import Database
from hermes_orchestrator.deploy import lifecycle
from hermes_orchestrator.deploy.launchd import standard_inventory
from hermes_orchestrator.domain import AdmissionRequest, IssueState, QueuedIssue
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.hermes_tools import HermesCommandService
from hermes_orchestrator.keychain import Keychain, KeychainWriteError
from hermes_orchestrator.lead_intake import LeadIntakeRouter
from hermes_orchestrator.lead_outbox import LeadCorrectionOutbox
from hermes_orchestrator.lead_wakes import (
    CommandWakeTransport,
    LeadWakeDelivery,
    LeadWakeReconciler,
)
from hermes_orchestrator.merge_flow import MergeFlow, build_merge_flow
from hermes_orchestrator.migration_gate import (
    MigrationGate,
    jo_loans_commands,
    load_verdict,
    render_handoff,
)
from hermes_orchestrator.qa import QaRouter
from hermes_orchestrator.queue import AdmissionDenied, IdempotencyConflict
from hermes_orchestrator.remote.auth import (
    CredentialConflict,
    CredentialExists,
    CredentialStateUnreadable,
    RemoteCredentialService,
)
from hermes_orchestrator.remote.serve import build_console_dependencies, run_console
from hermes_orchestrator.resources import ResourceSnapshot
from hermes_orchestrator.runtime import (
    Dispatch,
    Runtime,
    build_linear_router,
    cmux_password_source,
    open_runtime,
)
from hermes_orchestrator.service import OrchestratorService
from hermes_orchestrator.stalls import (
    PlaybookService,
    Remedy,
    RemedyExecutor,
    ScheduledResets,
    StallDetector,
    StallDiagnosis,
    StallEvidence,
)
from hermes_orchestrator.supervisor import Supervisor


def _watch_interval(raw: str) -> int:
    try:
        seconds = int(raw)
    except ValueError as error:
        raise argparse.ArgumentTypeError("watch interval must be an integer") from error
    if seconds < 5:
        raise argparse.ArgumentTypeError("watch interval must be at least 5 seconds")
    return seconds


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hermes-orchestrator")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--state-dir", type=Path)
    commands = parser.add_subparsers(dest="command", required=True)

    initialize = commands.add_parser("init", help="initialize local durable state")
    initialize.add_argument("--json", action="store_true")

    queue_add = commands.add_parser("queue-add", help="admit an explicit Linear issue")
    queue_add.add_argument("issue_id")
    queue_add.add_argument("--project", required=True)
    queue_add.add_argument("--priority", required=True, type=int, choices=range(1, 5))
    queue_add.add_argument("--operator-instruction", required=True)
    queue_add.add_argument("--json", action="store_true")

    queue_list = commands.add_parser("queue-list", help="list the private queue")
    queue_list.add_argument("--json", action="store_true")

    queue_complete = commands.add_parser(
        "queue-complete",
        help="reconcile an explicitly completed issue",
    )
    queue_complete.add_argument("issue_id")
    queue_complete.add_argument("--reason", required=True)
    queue_complete.add_argument("--evidence", required=True)
    queue_complete.add_argument("--json", action="store_true")

    status = commands.add_parser("status", help="show durable status")
    status.add_argument("--json", action="store_true")

    observe = commands.add_parser("observe", help="sample resources without mutation")
    observe.add_argument("--watch", type=_watch_interval)
    observe.add_argument("--json", action="store_true")

    reconcile = commands.add_parser("reconcile", help="reconcile durable local state")
    reconcile.add_argument("--json", action="store_true")

    daemon = commands.add_parser("daemon", help="run the local supervisor loop")
    daemon.add_argument("--once", action="store_true")
    daemon.add_argument("--interval", type=_watch_interval, default=30)
    daemon.add_argument("--json", action="store_true")

    cmux_focus = commands.add_parser(
        "cmux-focus",
        help="focus the cmux surface bound to a project's lead "
        "or the Orchestrator pane",
    )
    cmux_focus.add_argument("--project", default=None)
    cmux_focus.add_argument("--json", action="store_true")

    candidate = commands.add_parser(
        "candidate-ready",
        help="publish an immutable candidate manifest and wake the Merger",
    )
    candidate.add_argument("issue_id")
    candidate.add_argument("--project", required=True)
    candidate.add_argument(
        "--verified",
        action="append",
        required=True,
        metavar="COMMAND=RESULT",
        help="one recorded verification, repeatable",
    )
    candidate.add_argument("--blocker", action="append", default=[])
    candidate.add_argument("--status", default="FABLE_READY")
    candidate.add_argument("--json", action="store_true")

    merger_turn = commands.add_parser(
        "merger-turn",
        help="settle the Merger's completed turn for one project",
    )
    merger_turn.add_argument("--project", required=True)
    merger_turn.add_argument("--json", action="store_true")

    gate = commands.add_parser(
        "subagent-gate",
        help="Claude Code PreToolUse hook: block subagents while frozen",
    )
    gate.add_argument("--freeze-dir", type=Path, required=True)

    commands.add_parser(
        "remote-auth-init",
        help="create remote console credentials and display the token once",
    )

    hermes_command = commands.add_parser(
        "hermes-command",
        help="execute one strict Hermes JSON command",
    )
    hermes_command.add_argument("--json", dest="request_json", required=True)

    def _deploy_spec_arguments(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--binary", type=PurePosixPath, required=True)
        subparser.add_argument("--config-repo", type=PurePosixPath, required=True)
        subparser.add_argument(
            "--service-state-dir", type=PurePosixPath, required=True
        )
        subparser.add_argument("--log-dir", type=PurePosixPath, required=True)
        subparser.add_argument("--console-port", type=int, default=8787)

    deploy_render = commands.add_parser(
        "deploy-render",
        help="write launchd and tailnet-only serve artifacts; never activates",
    )
    _deploy_spec_arguments(deploy_render)
    deploy_render.add_argument("--output-dir", type=Path, required=True)

    for deploy_name, deploy_help in (
        ("deploy-install", "plan (or with --execute run) the guarded install"),
        ("deploy-uninstall", "plan (or with --execute run) the full reversal"),
        ("deploy-status", "plan (or with --execute run) read-only status"),
    ):
        deploy_sub = commands.add_parser(deploy_name, help=deploy_help)
        _deploy_spec_arguments(deploy_sub)
        deploy_sub.add_argument("--rendered-dir", type=PurePosixPath, required=True)
        deploy_sub.add_argument(
            "--launch-agents-dir", type=PurePosixPath, required=True
        )
        deploy_sub.add_argument("--uid", type=int, required=True)
        deploy_sub.add_argument("--execute", action="store_true")

    deploy_serve_enable = commands.add_parser(
        "deploy-serve-enable",
        help="plan (or with --execute run) the guarded tailnet-only serve enable",
    )
    deploy_serve_enable.add_argument("--console-port", type=int, default=8787)
    deploy_serve_enable.add_argument("--execute", action="store_true")

    serve_console = commands.add_parser(
        "serve-console",
        help="run the loopback-only operations console over durable state",
    )
    serve_console.add_argument("--host", default="127.0.0.1")
    serve_console.add_argument("--port", type=int, required=True)

    migration_env = commands.add_parser(
        "migration-env",
        help=(
            "plan (or with --execute run) the disposable loopback "
            "migration environment and its fail-closed gate"
        ),
    )
    migration_env.add_argument(
        "action",
        choices=("provision", "mark", "gate", "handoff", "teardown"),
    )
    migration_env.add_argument("--target-repo", type=Path, required=True)
    migration_env.add_argument("--slug", required=True)
    migration_env.add_argument(
        "--container", default=migration_env_module.DEFAULT_CONTAINER
    )
    migration_env.add_argument(
        "--port", type=int, default=migration_env_module.DEFAULT_PORT
    )
    migration_env.add_argument("--out", type=Path)
    migration_env.add_argument("--verdict", type=Path)
    migration_env.add_argument("--execute", action="store_true")
    return parser


async def _listen_for_merger_turns(flow: MergeFlow) -> None:
    """Settle Merger turns as the App Server reports them; never poll."""

    async for notification in flow.rpc.notifications():
        if notification.method == "thread/status/changed":
            status = notification.params.get("status")
            print(
                json.dumps(
                    {
                        "thread_status": (
                            status.get("type") if isinstance(status, dict) else None
                        ),
                        "thread_id": notification.params.get("threadId"),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                flush=True,
            )
            continue
        outcome = await flow.turns.on_notification(notification)
        if outcome is not None:
            print(
                json.dumps(
                    {
                        "merger_turn": outcome.kind,
                        "project_key": outcome.project_key,
                        "issue_id": outcome.issue_id,
                        "event_id": outcome.event_id,
                        "reason": outcome.reason,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                flush=True,
            )


async def _start_merge_flow(flow: MergeFlow, projects: Sequence[str]) -> bool:
    """Start the App Server and re-verify each channel; fail closed to stderr."""

    try:
        await flow.rpc.start()
    except Exception as error:
        print(f"codex app-server unavailable: {error}", file=sys.stderr)
        return False
    for project_key in projects:
        try:
            await flow.merger.ensure_thread(project_key)
        except Exception as error:
            print(
                f"merger channel for {project_key} unavailable: {error}",
                file=sys.stderr,
            )
    return True


async def _run_daemon(
    service: OrchestratorService,
    *,
    once: bool,
    interval: int,
    dispatch: Dispatch | None = None,
    shutdown_event: asyncio.Event | None = None,
    merge_flow: MergeFlow | None = None,
    projects: Sequence[str] = (),
    request_checkpoint: Callable[[str, str], Awaitable[object]] | None = None,
    checkpoint_dispatcher: CheckpointDispatcher | None = None,
    wake_delivery: LeadWakeDelivery | None = None,
    wake_reconciler: LeadWakeReconciler | None = None,
    cmux_reconciler: CmuxSurfaceReconciler | None = None,
    cmux_hibernation: CmuxHibernationDriver | None = None,
    lead_intake: LeadIntakeRouter | None = None,
) -> Supervisor:
    async def _maintenance() -> None:
        if cmux_hibernation is not None:
            await cmux_hibernation.tick()
        if lead_intake is not None:
            # Restart-safe by derivation: every tick re-routes any
            # durable pending correction or wake whose envelope has
            # not been delivered to its classic seat.
            await lead_intake.tick()

    supervisor = Supervisor(
        service,
        dispatch=dispatch,
        request_checkpoint=request_checkpoint,
        checkpoint_dispatcher=checkpoint_dispatcher,
        interval_seconds=interval,
        wake_delivery=wake_delivery,
        maintenance=(
            None
            if cmux_hibernation is None and lead_intake is None
            else _maintenance
        ),
    )
    if cmux_reconciler is not None:
        # Visible cmux seats are restored from durable identity before
        # any work is accepted; a denied or unreachable socket leaves the
        # bindings untouched and never blocks orchestration.
        await cmux_reconciler.reconcile()
    if wake_reconciler is not None:
        # Deterministic repair before replay: a wake lost between a
        # terminal commit and its outbox insert is reconstructed from
        # durable terminal evidence into the same deduplicated identity.
        wake_reconciler.reconcile()
    if wake_delivery is not None:
        # Idempotent startup replay: wakes committed before a crash or
        # restart re-arm the event-driven loop exactly once.
        wake_delivery.replay_startup()
    if lead_intake is not None:
        # Deliver any envelope a crash separated from its published
        # packet before the first supervised tick.
        await lead_intake.tick()
    if once:
        await supervisor.run_once()
        return supervisor
    listener: asyncio.Task[None] | None = None
    if merge_flow is not None and await _start_merge_flow(merge_flow, projects):
        listener = asyncio.create_task(_listen_for_merger_turns(merge_flow))
    stop = shutdown_event or asyncio.Event()
    registered_signals: list[signal.Signals] = []
    if shutdown_event is None:
        loop = asyncio.get_running_loop()
        for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(shutdown_signal, stop.set)
            except NotImplementedError:
                continue
            registered_signals.append(shutdown_signal)
    await supervisor.start()
    try:
        await stop.wait()
    finally:
        if shutdown_event is None:
            loop = asyncio.get_running_loop()
            for shutdown_signal in registered_signals:
                loop.remove_signal_handler(shutdown_signal)
        await supervisor.shutdown()
        if listener is not None:
            listener.cancel()
            with suppress(asyncio.CancelledError):
                await listener
        if merge_flow is not None:
            await merge_flow.rpc.close()
    return supervisor


def _open_merge_flow(settings: Any, runtime: Runtime) -> MergeFlow:
    """Compose the live merge flow on the CLI's already-open database."""

    if runtime.merge_flow is not None:
        return runtime.merge_flow
    keychain = Keychain()
    linear = build_linear_router(
        settings, database=runtime.database, queue=runtime.queue, keychain=keychain
    )
    return build_merge_flow(
        settings,
        database=runtime.database,
        events=EventStore(runtime.database),
        queue=runtime.queue,
        linear=linear,
        keychain=keychain,
        base_env=os.environ,
        processes=runtime.processes,
    )


async def _candidate_ready(flow: MergeFlow, args: Any) -> dict[str, Any]:
    verification: list[tuple[str, str]] = []
    for entry in args.verified:
        command, separator, result = entry.partition("=")
        if not separator or not command.strip() or not result.strip():
            raise ValueError("--verified expects COMMAND=RESULT")
        verification.append((command.strip(), result.strip()))
    if not await _start_merge_flow(flow, [args.project]):
        raise RuntimeError("codex app-server unavailable")
    try:
        emitted = await flow.emitter.emit(
            args.project,
            args.issue_id,
            verification=tuple(verification),
            blockers=tuple(args.blocker),
            status=args.status,
            # The lead runs candidate-ready from its own checkout; the
            # emitter validates it belongs to the project repository.
            lead_checkout=Path.cwd(),
        )
        thread_status: str | None = None
        if emitted.delivery.thread_id is not None:
            thread_status = await flow.merger.thread_status(emitted.delivery.thread_id)
    finally:
        await flow.rpc.close()
    return {
        # A notLoaded task holds the queued wake until it is opened once in
        # Codex; nothing re-delivers or polls, the completed turn settles it.
        "thread_status": thread_status,
        "awaiting_load": thread_status == "notLoaded",
        "intake": emitted.intake,
        "intake_reason": emitted.intake_reason,
        "event_id": emitted.event.event_id,
        "status": emitted.event.status,
        "issue_id": emitted.event.issue_id,
        "candidate_sha": emitted.event.candidate_sha,
        "base_sha": emitted.event.base_sha,
        "manifest_path": str(emitted.manifest_path),
        "reused_manifest": emitted.reused_manifest,
        "delivered": emitted.delivery.delivered,
        "delivery_reason": emitted.delivery.reason,
        "attempts": emitted.delivery.attempts,
    }


async def _merger_turn(flow: MergeFlow, args: Any) -> dict[str, Any]:
    if not await _start_merge_flow(flow, [args.project]):
        raise RuntimeError("codex app-server unavailable")
    try:
        outcome = await flow.turns.handle_turn(args.project)
    finally:
        await flow.rpc.close()
    return {
        "kind": outcome.kind,
        "project_key": outcome.project_key,
        "issue_id": outcome.issue_id,
        "event_id": outcome.event_id,
        "review_id": outcome.review_id,
        "merge_sha": outcome.merge_sha,
        "reason": outcome.reason,
    }


def _remedy_executor(runtime: Runtime) -> RemedyExecutor:
    """Wire the registered actions this runtime can actually perform.

    Every handler and verifier operates on durable, restart-safe state and
    fails closed on a missing or ambiguous target. Registered actions
    without a wired handler fail at execution (recorded as a failure that
    revokes automation); sensitive and unregistered actions are refused
    before anything runs.
    """

    cells = runtime.cells
    queue = runtime.queue
    database = runtime.database
    resets = runtime.resets or ScheduledResets(database, EventStore(database))
    selected_issue: dict[str, str] = {}

    def exact_target(diagnosis: StallDiagnosis) -> str:
        """Exactly one in-flight issue for the project, else fail closed."""

        rows = database.execute(
            "SELECT issue_id FROM admitted_issues WHERE project_key = ? "
            "AND state IN ('in_development', 'review') ORDER BY issue_id",
            (diagnosis.project_key,),
        ).fetchall()
        if not rows:
            raise RuntimeError(
                f"no in-flight issue for project {diagnosis.project_key}"
            )
        if len(rows) > 1:
            raise RuntimeError(
                f"ambiguous pause target: {len(rows)} in-flight issues for "
                f"{diagnosis.project_key}"
            )
        return str(rows[0]["issue_id"])

    async def request_handoff(diagnosis: StallDiagnosis, remedy: Remedy) -> None:
        if cells is None:
            raise RuntimeError("no project cells in this runtime")
        rows = database.execute(
            "SELECT cell_id FROM project_cells WHERE project_key = ? "
            "AND state IN ('starting', 'active')",
            (diagnosis.project_key,),
        ).fetchall()
        if len(rows) != 1:
            raise RuntimeError("no single active lead to hand off")
        if not cells.request_checkpoint(
            str(rows[0]["cell_id"]), f"stall:{diagnosis.reason}"
        ):
            raise RuntimeError("the active lead refused the checkpoint request")

    async def pause_issue(diagnosis: StallDiagnosis, remedy: Remedy) -> None:
        issue_id = exact_target(diagnosis)
        queue.transition(
            issue_id, IssueState.PAUSED, actor="orchestrator", reason=diagnosis.reason
        )
        selected_issue[diagnosis.predicate_key] = issue_id

    async def wait_for_reset(diagnosis: StallDiagnosis, remedy: Remedy) -> None:
        resets.schedule(
            diagnosis.project_key,
            diagnosis.predicate_key,
            reason=diagnosis.reason,
            delay_seconds=remedy.timeout_seconds,
        )

    async def handoff_requested(diagnosis: StallDiagnosis, remedy: Remedy) -> bool:
        return (
            database.scalar(
                "SELECT count(*) FROM project_cells WHERE project_key = ? "
                "AND state = 'handoff_required'",
                (diagnosis.project_key,),
            )
            == 1
        )

    async def issue_paused(diagnosis: StallDiagnosis, remedy: Remedy) -> bool:
        issue_id = selected_issue.get(diagnosis.predicate_key)
        if issue_id is None:
            return False
        return queue.get(issue_id).state is IssueState.PAUSED

    async def reset_scheduled(diagnosis: StallDiagnosis, remedy: Remedy) -> bool:
        return bool(resets.pending(diagnosis.project_key, diagnosis.predicate_key))

    return RemedyExecutor(
        handlers={
            "request_handoff": request_handoff,
            "pause_issue": pause_issue,
            "wait_for_reset": wait_for_reset,
        },
        verifiers={
            "handoff_requested": handoff_requested,
            "issue_paused": issue_paused,
            "reset_scheduled": reset_scheduled,
        },
    )


def _hermes_handlers(
    settings: Any, runtime: Runtime, outbox: LeadCorrectionOutbox
) -> dict[str, Any]:
    def pending_corrections(command: Any) -> dict[str, Any]:
        items = outbox.pending(command.project_key)
        return {"corrections": [item.as_dict() for item in items]}

    def ack_correction(command: Any) -> dict[str, Any]:
        return outbox.acknowledge(command.correction_id).as_dict()

    def pending_wakes(command: Any) -> dict[str, Any]:
        if runtime.lead_wakes is None:
            raise ValueError("lead terminal wakes are unavailable")
        items = runtime.lead_wakes.pending(command.project_key)
        return {"wakes": [item.as_dict() for item in items]}

    def ack_wake(command: Any) -> dict[str, Any]:
        if runtime.lead_wakes is None:
            raise ValueError("lead terminal wakes are unavailable")
        return runtime.lead_wakes.mark_delivered(command.wake_id).as_dict()

    def qa_reject(command: Any) -> dict[str, Any]:
        flow = _open_merge_flow(settings, runtime)
        outcome = asyncio.run(
            flow.reviews.reject_from_qa(
                command.issue_id, reason=command.reason, evidence=command.evidence
            )
        )
        return {
            "issue_id": outcome.issue_id,
            "review_id": outcome.review_id,
            "state": outcome.state,
            "reason": outcome.reason,
        }

    playbooks = PlaybookService(
        runtime.database,
        EventStore(runtime.database),
        playbook_path=settings.repo_root / "config" / "playbooks.yaml",
    )
    detector = StallDetector()

    executor = _remedy_executor(runtime)

    def report_stall(command: Any) -> dict[str, Any]:
        evidence = StallEvidence(
            no_material_change_seconds=command.no_material_change_seconds,
            process_alive=command.process_alive,
            output_activity=command.output_activity,
            resource_activity=command.resource_activity,
            repeated_failure=command.repeated_failure,
            blocked_request=command.blocked_request,
            provider_error=command.provider_error,
            external_failure=command.external_failure,
            agent_report=command.agent_report,
            resource_pressure=command.resource_pressure,
            project_key=command.project_key,
        )
        diagnosis = detector.evaluate(evidence)
        if diagnosis is None:
            return {"stalled": False, "reason": None}
        plan, outcome = asyncio.run(playbooks.resolve_and_execute(diagnosis, executor))
        return {
            "stalled": True,
            "summary": diagnosis.summary,
            **plan.as_dict(),
            "execution": None if outcome is None else outcome.as_dict(),
        }

    def approve_playbook(command: Any) -> dict[str, Any]:
        remedy = Remedy(
            actions=tuple(command.actions),
            verification=command.verification,
            timeout_seconds=command.timeout_seconds,
            rollback=command.rollback,
        )
        playbook = playbooks.record_operator_result(
            command.consultation_id, remedy, approved=command.approved
        )
        return {
            "consultation_id": command.consultation_id,
            "approved": command.approved,
            "playbook_hash": playbooks.current_hash() if playbook else None,
            "approvals": None if playbook is None else playbook.approvals,
            "version": None if playbook is None else playbook.version,
        }

    def pending_consultations(command: Any) -> dict[str, Any]:
        return {
            "consultations": [
                item.as_dict() for item in playbooks.pending(command.project_key)
            ]
        }

    def record_remedy_result(command: Any) -> dict[str, Any]:
        diagnosis = StallDiagnosis(
            reason=command.reason,
            predicate={},
            predicate_key=command.predicate_key,
            project_key=command.project_key,
            summary="operator-reported remedy result",
        )
        playbooks.record_execution(
            diagnosis,
            playbook_hash=command.playbook_hash,
            mode=command.mode,
            success=command.success,
            detail=command.detail,
        )
        return {"recorded": True, "success": command.success}

    return {
        "pending_corrections": pending_corrections,
        "ack_correction": ack_correction,
        "pending_wakes": pending_wakes,
        "ack_wake": ack_wake,
        "qa_reject": qa_reject,
        "report_stall": report_stall,
        "approve_playbook": approve_playbook,
        "pending_consultations": pending_consultations,
        "record_remedy_result": record_remedy_result,
    }


def _checkpoint_requester(cells: Any) -> Callable[[str, str], Awaitable[object]]:
    async def request(cell_id: str, reason: str) -> object:
        return cells.request_checkpoint(cell_id, reason)

    return request


def _issue_payload(issue: QueuedIssue) -> dict[str, Any]:
    return {
        "issue_id": issue.issue_id,
        "project_key": issue.project_key,
        "priority": issue.linear_priority,
        "state": issue.state.value,
        "dependency_ready": issue.dependency_ready,
        "overlap_risk": issue.overlap_risk,
        "admitted_at": issue.admitted_at.isoformat(),
    }


def _snapshot_payload(snapshot: ResourceSnapshot) -> dict[str, Any]:
    return {
        "sampled_at": snapshot.sampled_at.isoformat(),
        "pressure": snapshot.pressure.value,
        "can_admit": snapshot.can_admit,
        "available_memory_bytes": snapshot.available_memory_bytes,
        "total_memory_bytes": snapshot.total_memory_bytes,
        "swap_used_bytes": snapshot.swap_used_bytes,
        "load_one": snapshot.load_one,
        "logical_cpus": snapshot.logical_cpus,
        "disk_free_bytes": snapshot.disk_free_bytes,
        "managed_rss_bytes": snapshot.managed_rss_bytes,
    }


def _print(payload: Any, *, json_output: bool, human: str) -> None:
    if json_output:
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    else:
        print(human)


def subagent_gate(freeze_dir: Path, payload: str) -> tuple[int, str]:
    """PreToolUse hook: block Agent tool use while the session is frozen.

    Exit 2 blocks the tool call in Claude Code and returns the message to
    the lead; exit 0 allows it. Malformed input never blocks silently: it
    fails closed with exit 2 only when a freeze marker cannot be ruled out.
    """

    try:
        document = json.loads(payload or "{}")
    except json.JSONDecodeError:
        return 2, "subagent gate: unreadable hook input; assignment refused"
    session_id = document.get("session_id") if isinstance(document, dict) else None
    if not isinstance(session_id, str) or not session_id:
        return 0, ""
    marker = freeze_dir / f"{session_id}.frozen"
    if marker.exists():
        reason = marker.read_text(encoding="utf-8").strip() or "rotation_pending"
        return 2, (
            "subagent gate: new subwork is frozen for this session "
            f"({reason}); finish current work to a safe boundary and hand off"
        )
    return 0, ""


def _remote_auth_init() -> int:
    """Initialize remote console credentials, displaying the token exactly once."""

    credentials = RemoteCredentialService(keychain=Keychain())
    try:
        token = credentials.initialize()
    except CredentialExists:
        print(
            "remote credential already exists; refusing to overwrite",
            file=sys.stderr,
        )
        _print_remote_auth_diagnostic(credentials)
        return 1
    except CredentialConflict:
        print(
            "remote credential state is unproven; "
            "refusing to display or overwrite it",
            file=sys.stderr,
        )
        _print_remote_auth_diagnostic(credentials)
        return 1
    except CredentialStateUnreadable:
        print(
            "remote credential initialization failed: keychain state unreadable",
            file=sys.stderr,
        )
        _print_remote_auth_diagnostic(credentials)
        return 1
    except KeychainWriteError:
        print(
            "remote credential initialization failed: keychain write refused",
            file=sys.stderr,
        )
        _print_remote_auth_diagnostic(credentials)
        return 1
    print("Remote application token (displayed once; stored only in Keychain):")
    print(token)
    return 0


def _print_remote_auth_diagnostic(credentials: RemoteCredentialService) -> None:
    """Print bounded account-presence lines; account names only, no values."""

    presence = credentials.account_presence()
    for account in sorted(presence):
        print(f"keychain account '{account}': {presence[account]}", file=sys.stderr)
    print(
        "no stored values were displayed; rerun remote-auth-init to recover",
        file=sys.stderr,
    )


def _deploy_inventory(args: argparse.Namespace):
    return standard_inventory(
        binary=args.binary,
        config_repo=args.config_repo,
        state_dir=args.service_state_dir,
        log_dir=args.log_dir,
        console_port=args.console_port,
    )


def _deploy_render(args: argparse.Namespace) -> int:
    paths = lifecycle.render_artifacts(
        _deploy_inventory(args), args.output_dir, console_port=args.console_port
    )
    print(json.dumps({"artifacts": [str(path) for path in paths]}, indent=2))
    return 0


def _run_deploy_plan(
    args: argparse.Namespace,
    steps: tuple[lifecycle.CommandStep, ...],
    runner: lifecycle.CommandRunner | None,
    journal: lifecycle.MutationJournal | None = None,
) -> int:
    if not args.execute:
        print(
            json.dumps(
                {
                    "executed": False,
                    "plan": [
                        {
                            "argv": list(step.argv),
                            "kind": step.kind,
                            "code": step.code,
                        }
                        for step in steps
                    ],
                },
                indent=2,
            )
        )
        return 0
    active_runner = runner if runner is not None else lifecycle.SubprocessRunner()
    report = lifecycle.execute_plan(
        steps, active_runner, console_port=args.console_port, journal=journal
    )
    print(
        json.dumps(
            {
                "executed": True,
                "completed": report.completed,
                "refusal_code": report.refusal_code,
                "records": [list(record) for record in report.records],
                "compensations": [list(pair) for pair in report.compensations],
                "residual_codes": list(report.residual_codes),
            },
            indent=2,
        )
    )
    return 0 if report.completed else 1


def _deploy_install(
    args: argparse.Namespace, runner: lifecycle.CommandRunner | None = None
) -> int:
    steps = lifecycle.plan_install(
        _deploy_inventory(args),
        rendered_dir=args.rendered_dir,
        launch_agents_dir=args.launch_agents_dir,
        uid=args.uid,
        console_port=args.console_port,
    )
    journal: lifecycle.MutationJournal | None = None
    if args.execute:
        # Dry runs must construct nothing; the journal exists only when
        # mutations may actually happen.
        journal = lifecycle.FileMutationJournal(
            Path(args.rendered_dir) / "install-journal.json"
        )
    return _run_deploy_plan(args, steps, runner, journal=journal)


def _deploy_uninstall(
    args: argparse.Namespace, runner: lifecycle.CommandRunner | None = None
) -> int:
    steps = lifecycle.plan_uninstall(
        _deploy_inventory(args),
        launch_agents_dir=args.launch_agents_dir,
        uid=args.uid,
        console_port=args.console_port,
    )
    return _run_deploy_plan(args, steps, runner)


def _deploy_serve_enable(
    args: argparse.Namespace, runner: lifecycle.CommandRunner | None = None
) -> int:
    steps = lifecycle.plan_serve_enable(console_port=args.console_port)
    return _run_deploy_plan(args, steps, runner)


def _deploy_status(
    args: argparse.Namespace, runner: lifecycle.CommandRunner | None = None
) -> int:
    steps = lifecycle.plan_status(
        _deploy_inventory(args), uid=args.uid, console_port=args.console_port
    )
    return _run_deploy_plan(args, steps, runner)


def _migration_env(
    args: argparse.Namespace,
    runner: migration_env_module.EnvCommandRunner | None = None,
) -> int:
    """Plan or drive the disposable migration environment.

    Every action is a dry plan unless ``--execute`` is passed; the gate
    additionally refuses to execute without a verdict output path, so a
    run can never happen without leaving its durable artifact.
    """

    config = migration_env_module.MigrationEnvConfig(
        repo_path=args.target_repo,
        slug=args.slug,
        container=args.container,
        port=args.port,
    )
    if args.action == "mark":
        try:
            marker = migration_env_module.mark_isolated_worktree(
                args.target_repo, slug=args.slug
            )
        except migration_env_module.MigrationEnvRefusal as refusal:
            print(json.dumps({"error": str(refusal)}), file=sys.stderr)
            return 1
        print(json.dumps({"marked": str(marker)}))
        return 0
    if args.action == "handoff":
        if args.out is None:
            print("handoff requires --out", file=sys.stderr)
            return 2
        verdict = None
        if args.verdict is not None and args.verdict.exists():
            verdict = load_verdict(args.verdict)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(
            render_handoff(
                config=config,
                commands=jo_loans_commands(),
                verdict=verdict,
            )
        )
        print(json.dumps({"handoff": str(args.out)}))
        return 0
    if args.action == "gate":
        commands = jo_loans_commands()
        if not args.execute:
            codes = [
                "repo_head_resolved",
                "isolated_worktree",
                "container_identity",
                "loopback_bindings",
                "disposable_naming",
                "pending_before",
                "migrate_deploy",
                "pending_after",
                "deploy_idempotent",
                *(f"corpus_{code}" for code, _ in commands.corpus),
                *(f"historical_{code}" for code, _ in commands.historical),
                "generation_first",
                "generation_byte_clean",
                "container_identity_stable",
                "repo_head_stable",
            ]
            print(json.dumps({"executed": False, "checks": codes}))
            return 0
        if args.out is None:
            print("gate --execute requires --out", file=sys.stderr)
            return 2
        verdict = MigrationGate(
            config=config,
            commands=commands,
            runner=runner or migration_env_module.EnvSubprocessRunner(),
            verdict_path=args.out,
        ).run()
        print(
            json.dumps(
                {
                    "executed": True,
                    "green": verdict.green,
                    "verdict": str(args.out),
                    "failed": [
                        f.code
                        for f in verdict.findings
                        if f.status == "fail"
                    ],
                }
            )
        )
        return 0 if verdict.green else 1
    steps = (
        migration_env_module.plan_provision(config)
        if args.action == "provision"
        else migration_env_module.plan_teardown(config)
    )
    if not args.execute:
        print(
            json.dumps(
                {
                    "executed": False,
                    "plan": [
                        {
                            "argv": list(step.argv),
                            "kind": step.kind,
                            "code": step.code,
                        }
                        for step in steps
                    ],
                }
            )
        )
        return 0
    active_runner = runner or migration_env_module.EnvSubprocessRunner()
    report = (
        migration_env_module.provision_disposable(config, active_runner)
        if args.action == "provision"
        else migration_env_module.teardown_disposable(config, active_runner)
    )
    print(
        json.dumps(
            {
                "executed": True,
                "completed": report.completed,
                "refusal_code": report.refusal_code,
                "records": [list(record) for record in report.records],
                "container_id": report.container_id,
            }
        )
    )
    return 0 if report.completed else 1


def _serve_console(args: argparse.Namespace, settings: Settings) -> int:
    """Serve the operations console over the durable state database.

    The console only ever binds the loopback address; tailnet exposure is
    Tailscale Serve's job. No runtime, daemon lock, or live credential
    loading is involved — only the state database and the keychain-backed
    remote services behind the login boundary.
    """

    if args.host != "127.0.0.1":
        print("serve-console binds 127.0.0.1 only", file=sys.stderr)
        return 2
    database = Database.open(settings.state_dir / "state.db")
    try:
        dependencies = build_console_dependencies(
            database=database,
            github_repos={
                alias: project.github_repo
                for alias, project in settings.projects.items()
            },
            playbook_path=settings.repo_root / "config" / "playbooks.yaml",
        )
        run_console(dependencies, port=args.port)
    finally:
        database.close()
    return 0


def main(arguments: Sequence[str] | None = None) -> int:
    """Run one CLI command and return its process exit code."""

    args = _parser().parse_args(arguments)
    if args.command == "subagent-gate":
        code, message = subagent_gate(args.freeze_dir, sys.stdin.read())
        if message:
            print(message, file=sys.stderr)
        return code
    if args.command == "remote-auth-init":
        return _remote_auth_init()
    if args.command == "deploy-render":
        return _deploy_render(args)
    if args.command == "deploy-install":
        return _deploy_install(args)
    if args.command == "deploy-uninstall":
        return _deploy_uninstall(args)
    if args.command == "deploy-serve-enable":
        return _deploy_serve_enable(args)
    if args.command == "deploy-status":
        return _deploy_status(args)
    if args.command == "migration-env":
        return _migration_env(args)
    settings = load_settings(args.repo_root, args.state_dir)
    if args.command == "serve-console":
        return _serve_console(args, settings)
    enable_live = args.command == "daemon" and settings.policy.mode == "active"
    runtime = open_runtime(settings, enable_live=enable_live)
    database = runtime.database
    queue = runtime.queue
    service = runtime.service
    try:
        if args.command == "init":
            payload = {
                "schema_version": database.schema_version(),
                "state_database": str(database.path),
                "mode": settings.policy.mode,
            }
            _print(payload, json_output=args.json, human="Local state initialized.")
            return 0

        if args.command == "cmux-focus":
            if settings.cmux is None or runtime.cmux_bindings is None:
                _print(
                    {"error": "cmux is not configured"},
                    json_output=args.json,
                    human="cmux is not configured (config/cmux.yaml).",
                )
                return 1
            binding = (
                runtime.cmux_bindings.active_lead_for_project(args.project)
                if args.project
                else runtime.cmux_bindings.active_orchestrator()
            )
            if binding is None:
                _print(
                    {"error": "no active binding", "project": args.project},
                    json_output=args.json,
                    human="No active cmux binding for that seat.",
                )
                return 1
            port = CmuxCliAdapter(
                settings.cmux.cli,
                base_env=os.environ,
                password_source=cmux_password_source(Keychain()),
            )
            try:
                asyncio.run(port.focus_workspace(binding.workspace_uuid))
            except CmuxError as error:
                _print(
                    {"error": type(error).__name__},
                    json_output=args.json,
                    human="cmux focus failed; the binding is unchanged.",
                )
                return 1
            _print(
                {
                    "workspace_uuid": binding.workspace_uuid,
                    "surface_uuid": binding.surface_uuid,
                    "role": binding.role,
                    "project": binding.project_key,
                },
                json_output=args.json,
                human="Focused the bound cmux surface.",
            )
            return 0

        if args.command == "hermes-command":
            try:
                request = json.loads(args.request_json)
            except json.JSONDecodeError:
                print("invalid JSON command", file=sys.stderr)
                return 2
            events = EventStore(database)
            outbox = LeadCorrectionOutbox(
                database=database,
                events=events,
                project_for_issue=lambda issue_id: queue.get(issue_id).project_key,
            )
            result = HermesCommandService(
                queue,
                qa=QaRouter(database=database, events=events),
                handlers=_hermes_handlers(settings, runtime, outbox),
            ).execute(request)
            print(json.dumps(result.as_dict(), sort_keys=True, separators=(",", ":")))
            rejected = {"invalid_command", "intent_not_allowed"}
            return 0 if result.code not in rejected else 1

        if args.command == "daemon":
            supervisor = asyncio.run(
                _run_daemon(
                    service,
                    once=args.once,
                    interval=args.interval,
                    dispatch=runtime.dispatch,
                    merge_flow=runtime.merge_flow,
                    projects=tuple(settings.projects),
                    request_checkpoint=(
                        None
                        if runtime.cells is None
                        else _checkpoint_requester(runtime.cells)
                    ),
                    checkpoint_dispatcher=(
                        None
                        if runtime.cells is None or runtime.checkpoints is None
                        else CheckpointDispatcher(
                            runtime.checkpoints,
                            callback=runtime.cells.request_checkpoint,
                            current_session=runtime.cells.current_session,
                        )
                    ),
                    wake_delivery=(
                        None
                        if runtime.lead_wakes is None
                        else LeadWakeDelivery(
                            runtime.lead_wakes,
                            # Direct production delivery: each committed
                            # wake is pushed to the configured Hermes
                            # consumer with wake_id as the idempotency
                            # key; pending_wakes/ack_wake stays as the
                            # metadata-only operational fallback.
                            transport=(
                                None
                                if settings.hermes is None
                                else CommandWakeTransport(
                                    settings.hermes.wake_command
                                )
                            ),
                        )
                    ),
                    wake_reconciler=(
                        None
                        if runtime.lead_wakes is None
                        else LeadWakeReconciler(
                            database=runtime.database,
                            events=EventStore(runtime.database),
                            wakes=runtime.lead_wakes,
                        )
                    ),
                    cmux_reconciler=runtime.cmux_reconciler,
                    cmux_hibernation=runtime.cmux_hibernation,
                    lead_intake=runtime.lead_intake,
                )
            )
            payload = {
                "mode": settings.policy.mode,
                "ticks": supervisor.ticks,
                "events": supervisor.events,
                "profiles": [
                    {
                        "alias": health.profile_alias,
                        "eligible": health.eligible,
                        "reason": health.reason,
                    }
                    for health in runtime.profile_health
                ],
            }
            _print(
                payload,
                json_output=args.json,
                human=f"Supervisor completed {supervisor.ticks} tick(s).",
            )
            return 0

        if args.command == "queue-add":
            try:
                issue = queue.admit(
                    AdmissionRequest(
                        issue_id=args.issue_id,
                        project_key=args.project,
                        linear_priority=args.priority,
                        admitted_by="operator",
                        instruction_id=args.operator_instruction,
                    )
                )
            except (AdmissionDenied, IdempotencyConflict) as error:
                print(str(error), file=sys.stderr)
                return 1
            _print(
                _issue_payload(issue),
                json_output=args.json,
                human=f"Queued {issue.issue_id} for {issue.project_key}.",
            )
            return 0

        if args.command == "queue-list":
            issues = queue.list_ranked(datetime.now(UTC))
            payload = [_issue_payload(issue) for issue in issues]
            _print(
                payload,
                json_output=args.json,
                human=f"{len(payload)} explicitly admitted issue(s).",
            )
            return 0

        if args.command == "queue-complete":
            try:
                issue = queue.complete(
                    args.issue_id,
                    reason=args.reason,
                    evidence=args.evidence,
                )
            except (KeyError, ValueError) as error:
                print(str(error), file=sys.stderr)
                return 1
            _print(
                _issue_payload(issue),
                json_output=args.json,
                human=f"Completed {issue.issue_id}.",
            )
            return 0

        if args.command == "candidate-ready":
            try:
                flow = _open_merge_flow(settings, runtime)
                payload = asyncio.run(_candidate_ready(flow, args))
            except (ValueError, RuntimeError) as error:
                print(str(error), file=sys.stderr)
                return 1
            _print(
                payload,
                json_output=args.json,
                human=(
                    f"Candidate {payload['candidate_sha'][:12]} for "
                    f"{payload['issue_id']}: {payload['delivery_reason']}."
                ),
            )
            return 0 if payload["delivered"] else 1

        if args.command == "merger-turn":
            try:
                flow = _open_merge_flow(settings, runtime)
                payload = asyncio.run(_merger_turn(flow, args))
            except (ValueError, RuntimeError) as error:
                print(str(error), file=sys.stderr)
                return 1
            _print(
                payload,
                json_output=args.json,
                human=f"Merger turn for {args.project}: {payload['kind']}.",
            )
            return 0

        if args.command == "status":
            payload = {
                "mode": settings.policy.mode,
                "admission_open": False,
                "queue_count": len(queue.list_ranked(datetime.now(UTC))),
                "project_count": len(settings.projects),
                "schema_version": database.schema_version(),
            }
            _print(
                payload,
                json_output=args.json,
                human=(
                    f"Mode: {payload['mode']}; queue: {payload['queue_count']}; "
                    "admission: closed."
                ),
            )
            return 0

        if args.command == "reconcile":
            result = service.start()
            payload = {
                "completed": result.completed,
                "findings": list(result.findings),
                "admission_open": result.admission_open,
            }
            _print(
                payload,
                json_output=args.json,
                human=("Reconciliation completed; admission remains closed."),
            )
            return 0 if result.completed else 1

        if args.command == "observe":
            reconciliation = service.start()
            if not reconciliation.completed:
                print("reconciliation failed", file=sys.stderr)
                return 1
            while True:
                tick = service.tick()
                payload = {
                    "snapshot": _snapshot_payload(tick.snapshot),
                    "resource_actions": [
                        {
                            "kind": action.kind,
                            "target_id": action.target_id,
                            "reason": action.reason,
                        }
                        for action in tick.resource_actions
                    ],
                    "planned_actions": [
                        {
                            "kind": action.kind,
                            "project_key": action.project_key,
                            "issue_id": action.issue_id,
                            "reason": action.reason,
                            "execute": action.execute,
                            "evidence": action.evidence,
                        }
                        for action in tick.planned_actions
                    ],
                }
                _print(
                    payload,
                    json_output=args.json,
                    human=(
                        f"Pressure: {tick.snapshot.pressure.value}; "
                        f"planned actions: {len(tick.planned_actions)}; executed: 0."
                    ),
                )
                if args.watch is None:
                    return 0
                time.sleep(args.watch)
    finally:
        runtime.close()

    raise AssertionError(f"unhandled command: {args.command}")


def entrypoint() -> None:
    """Console-script wrapper."""

    raise SystemExit(main())
