"""Observation-only operator command line interface."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hermes_orchestrator.config import Settings, load_settings
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest, QueuedIssue
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.queue import AdmissionDenied, IdempotencyConflict, QueueService
from hermes_orchestrator.resources import ResourceSampler, ResourceSnapshot
from hermes_orchestrator.scheduler import Scheduler
from hermes_orchestrator.service import OrchestratorService


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

    status = commands.add_parser("status", help="show durable status")
    status.add_argument("--json", action="store_true")

    observe = commands.add_parser("observe", help="sample resources without mutation")
    observe.add_argument("--watch", type=_watch_interval)
    observe.add_argument("--json", action="store_true")

    reconcile = commands.add_parser("reconcile", help="reconcile durable local state")
    reconcile.add_argument("--json", action="store_true")
    return parser


def _open(settings: Settings) -> tuple[Database, QueueService, OrchestratorService]:
    database = Database.open(settings.state_dir / "state.db")
    events = EventStore(database)
    queue = QueueService(database, events, settings.projects)
    scheduler = Scheduler(queue, mode=settings.policy.mode)
    sampler = ResourceSampler(
        policy=settings.policy,
        repository_paths={
            alias: project.repo_path for alias, project in settings.projects.items()
        },
    )
    service = OrchestratorService(
        database=database,
        events=events,
        sampler=sampler,
        scheduler=scheduler,
        policy=settings.policy,
    )
    return database, queue, service


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
    database, queue, service = _open(settings)
    try:
        if args.command == "init":
            payload = {
                "schema_version": database.schema_version(),
                "state_database": str(database.path),
                "mode": settings.policy.mode,
            }
            _print(payload, json_output=args.json, human="Local state initialized.")
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

        if args.command == "status":
            payload = {
                "mode": settings.policy.mode,
                "admission_open": False,
                "queue_count": int(
                    database.scalar("SELECT count(*) FROM admitted_issues")
                ),
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
        database.close()

    raise AssertionError(f"unhandled command: {args.command}")


def entrypoint() -> None:
    """Console-script wrapper."""

    raise SystemExit(main())
