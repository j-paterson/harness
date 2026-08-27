"""Observation-only operator command line interface."""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hermes_orchestrator.config import load_settings
from hermes_orchestrator.domain import AdmissionRequest, QueuedIssue
from hermes_orchestrator.hermes_tools import HermesCommandService
from hermes_orchestrator.queue import AdmissionDenied, IdempotencyConflict
from hermes_orchestrator.resources import ResourceSnapshot
from hermes_orchestrator.runtime import Dispatch, open_runtime
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

    hermes_command = commands.add_parser(
        "hermes-command",
        help="execute one strict Hermes JSON command",
    )
    hermes_command.add_argument("--json", dest="request_json", required=True)
    return parser


async def _run_daemon(
    service: OrchestratorService,
    *,
    once: bool,
    interval: int,
    dispatch: Dispatch | None = None,
    shutdown_event: asyncio.Event | None = None,
) -> Supervisor:
    supervisor = Supervisor(
        service,
        dispatch=dispatch,
        interval_seconds=interval,
    )
    if once:
        await supervisor.run_once()
        return supervisor
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
    return supervisor


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


def main(arguments: Sequence[str] | None = None) -> int:
    """Run one CLI command and return its process exit code."""

    args = _parser().parse_args(arguments)
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
            result = HermesCommandService(queue).execute(request)
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
