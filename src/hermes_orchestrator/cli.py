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
from pathlib import Path
from typing import Any

from hermes_orchestrator.checkpoints import CheckpointDispatcher
from hermes_orchestrator.config import load_settings
from hermes_orchestrator.domain import AdmissionRequest, QueuedIssue
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.hermes_tools import HermesCommandService
from hermes_orchestrator.keychain import Keychain
from hermes_orchestrator.lead_outbox import LeadCorrectionOutbox
from hermes_orchestrator.merge_flow import MergeFlow, build_merge_flow
from hermes_orchestrator.qa import QaRouter
from hermes_orchestrator.queue import AdmissionDenied, IdempotencyConflict
from hermes_orchestrator.resources import ResourceSnapshot
from hermes_orchestrator.runtime import (
    Dispatch,
    Runtime,
    build_linear_router,
    open_runtime,
)
from hermes_orchestrator.service import OrchestratorService
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

    hermes_command = commands.add_parser(
        "hermes-command",
        help="execute one strict Hermes JSON command",
    )
    hermes_command.add_argument("--json", dest="request_json", required=True)
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
) -> Supervisor:
    supervisor = Supervisor(
        service,
        dispatch=dispatch,
        request_checkpoint=request_checkpoint,
        checkpoint_dispatcher=checkpoint_dispatcher,
        interval_seconds=interval,
    )
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


def _hermes_handlers(
    settings: Any, runtime: Runtime, outbox: LeadCorrectionOutbox
) -> dict[str, Any]:
    def pending_corrections(command: Any) -> dict[str, Any]:
        items = outbox.pending(command.project_key)
        return {"corrections": [item.as_dict() for item in items]}

    def ack_correction(command: Any) -> dict[str, Any]:
        return outbox.acknowledge(command.correction_id).as_dict()

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

    return {
        "pending_corrections": pending_corrections,
        "ack_correction": ack_correction,
        "qa_reject": qa_reject,
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


def main(arguments: Sequence[str] | None = None) -> int:
    """Run one CLI command and return its process exit code."""

    args = _parser().parse_args(arguments)
    if args.command == "subagent-gate":
        code, message = subagent_gate(args.freeze_dir, sys.stdin.read())
        if message:
            print(message, file=sys.stderr)
        return code
    settings = load_settings(args.repo_root, args.state_dir)
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
