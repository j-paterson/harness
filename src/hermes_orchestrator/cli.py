"""Observation-only operator command line interface."""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import hashlib
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from hermes_orchestrator import migration_env as migration_env_module
from hermes_orchestrator.cells import ProfileCapacityEvidence, ProjectCellService
from hermes_orchestrator.channel_hub import (
    ChannelHub,
    hub_socket_path,
)
from hermes_orchestrator.channel_hub import (
    nudge as nudge_channel,
)
from hermes_orchestrator.checkpoints import CheckpointDispatcher, CheckpointSafetyStore
from hermes_orchestrator.claude import (
    ClaudeRunner,
    control_launch_failure_recorder,
)
from hermes_orchestrator.cmux import CmuxCliAdapter, CmuxError
from hermes_orchestrator.cmux_surfaces import (
    CHANNEL_ENTRY,
    CmuxHibernationDriver,
    CmuxLeadSeater,
    CmuxSurfaceReconciler,
    RegistryProfileDirectory,
)
from hermes_orchestrator.config import Settings, load_settings
from hermes_orchestrator.context import ContextMonitor
from hermes_orchestrator.control_operations import ControlOperations
from hermes_orchestrator.dashboard_pane import FramePane
from hermes_orchestrator.dashboard_refresh import DashboardRefreshAction
from hermes_orchestrator.db import Database
from hermes_orchestrator.deploy import lifecycle
from hermes_orchestrator.deploy.launchd import standard_inventory
from hermes_orchestrator.domain import AdmissionRequest, IssueState, QueuedIssue
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.fakechat_router import FakechatWakeRouter
from hermes_orchestrator.handoffs import HandoffService
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
from hermes_orchestrator.merger_session import MergerSession
from hermes_orchestrator.merger_turns import SubmissionRejected, TurnOutcome
from hermes_orchestrator.migration_gate import (
    MigrationGate,
    jo_loans_commands,
    load_verdict,
    render_handoff,
)
from hermes_orchestrator.operator_decisions import (
    DecisionRefused,
    OperatorDecisions,
)
from hermes_orchestrator.orchestrator_workspace import (
    OrchestratorWorkspaceOwner,
)
from hermes_orchestrator.packet_admission import PacketAdmission
from hermes_orchestrator.post_merge import PostMergeAdvance
from hermes_orchestrator.profiles import (
    ClaudeProfileProbe,
    ProfileBootstrap,
    ProfileHealth,
    ProfilePool,
    ProfileRegistry,
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
    DaemonAlreadyRunning,
    Dispatch,
    Runtime,
    acquire_workspace_fence,
    build_channel_collaborators,
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
from hermes_orchestrator.subagent_packets import SubagentPackets
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

    dashboard = commands.add_parser(
        "dashboard",
        help=(
            "render the Orchestrator dashboard in this terminal: "
            "token-free, read-only, no daemon lock (the upper-pane "
            "entry; the daemon stays the sole supervisor)"
        ),
    )
    dashboard.add_argument("--once", action="store_true")
    dashboard.add_argument("--interval", type=_watch_interval, default=30)
    dashboard.add_argument("--json", action="store_true")
    dashboard.add_argument(
        "--detail",
        action="store_true",
        help=(
            "show the Detail section: local fable/overall token "
            "accumulation and raw per-window usage percentages "
            "(hidden by default per the operator capacity correction)"
        ),
    )

    cmux_focus = commands.add_parser(
        "cmux-focus",
        help="focus the cmux surface bound to a project's lead "
        "or the Orchestrator pane",
    )
    cmux_focus.add_argument("--project", default=None)
    cmux_focus.add_argument("--json", action="store_true")

    orchestrator_ws = commands.add_parser(
        "orchestrator-workspace",
        help=(
            "ensure or smoke-test the recoverable two-pane Orchestrator "
            "workspace (upper supervisor/dashboard pane, lower Nous "
            "Hermes classic durable session)"
        ),
    )
    orchestrator_ws.add_argument("action", choices=("ensure", "smoke"))
    orchestrator_ws.add_argument("--name", default="orchestrator")
    orchestrator_ws.add_argument("--title", default="Orchestrator")
    orchestrator_ws.add_argument(
        "--session",
        default=None,
        help="Hermes durable session name (default: the workspace name)",
    )
    orchestrator_ws.add_argument(
        "--interval", type=_watch_interval, default=30
    )
    orchestrator_ws.add_argument(
        "--settle-seconds",
        type=float,
        default=8.0,
        help="smoke: seconds to let pane processes start before inspection",
    )
    orchestrator_ws.add_argument(
        "--cmux-cli",
        type=Path,
        default=None,
        help="cmux CLI path when config/cmux.yaml is absent",
    )
    orchestrator_ws.add_argument(
        "--evidence-file",
        type=Path,
        default=None,
        help=(
            "smoke: write a sha256-digest-sealed evidence document "
            "(source sha, exact invocation, full evidence) to this path"
        ),
    )
    orchestrator_ws.add_argument("--json", action="store_true")

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

    submit_review = commands.add_parser(
        "submit-review",
        help=(
            "submit Sol's explicit final review verdict for the "
            "outstanding wake and settle it exactly once (operator "
            "correction ec1f6bdf); the verdict is submitted, never "
            "inferred from an idle thread"
        ),
    )
    submit_review.add_argument("--project", required=True)
    submit_review.add_argument("--issue", required=True)
    submit_review.add_argument("--event", required=True)
    submit_review.add_argument("--candidate-sha", required=True)
    submit_review.add_argument("--thread", required=True)
    submit_review.add_argument("--generation", type=int, required=True)
    submit_review.add_argument(
        "--verdict",
        required=True,
        help="path to the verdict JSON document, or - to read stdin",
    )

    reviewer_fix = commands.add_parser(
        "reviewer-fix",
        help=(
            "the bounded ACCEPT_WITH_REVIEWER_FIX helper: commit the "
            "eligible mechanical fix atop the immutable submitted SHA, "
            "verify, push, recompute the base-to-final manifest, and "
            "record the auditable receipt"
        ),
    )
    reviewer_fix.add_argument("--project", required=True)
    reviewer_fix.add_argument("--issue", required=True)
    reviewer_fix.add_argument("--submitted", required=True)
    reviewer_fix.add_argument("--message", required=True)
    reviewer_fix.add_argument(
        "--verify",
        action="append",
        required=True,
        metavar="COMMAND",
        help="one focused verification command, repeatable",
    )
    reviewer_fix.add_argument("--json", action="store_true")

    merge_settle = commands.add_parser(
        "merge-settle",
        help=(
            "drive approved reviews through the guarded settlement path "
            "— the single bounded merge capability; without --review it "
            "resumes every resumable settlement for the project"
        ),
    )
    merge_settle.add_argument("--project", required=True)
    merge_settle.add_argument("--review", default=None)
    merge_settle.add_argument("--json", action="store_true")

    merge_reconcile = commands.add_parser(
        "merge-reconcile",
        help=(
            "reconstruct auditable receipts for a pull request merged "
            "externally before verdict settlement; validates the exact "
            "reviewed head and merge proof, never merges"
        ),
    )
    merge_reconcile.add_argument("--project", required=True)
    merge_reconcile.add_argument("--issue", required=True)
    merge_reconcile.add_argument("--event", required=True)
    merge_reconcile.add_argument("--pr", type=int, required=True)
    merge_reconcile.add_argument("--json", action="store_true")

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

    child_start = commands.add_parser(
        "child-start",
        help=(
            "durably count one background child start "
            "(Claude Code PreToolUse Agent hook)"
        ),
    )
    child_start.add_argument("--session", default=None)
    child_start.add_argument("--child", default=None)

    child_stop = commands.add_parser(
        "child-stop",
        help=(
            "durably count one background child completion and "
            "reactivate a waiting lead exactly once "
            "(Claude Code SubagentStop hook)"
        ),
    )
    child_stop.add_argument("--session", default=None)
    child_stop.add_argument("--child", default=None)

    runtime_activate = commands.add_parser(
        "runtime-activate",
        help=(
            "prove this checkout (clean tree, schema match) and record "
            "it as the active runtime identity; a failed attempt is "
            "recorded and the prior activation stays active"
        ),
    )
    runtime_activate.add_argument("--json", action="store_true")
    runtime_activate.add_argument(
        "--apply",
        action="store_true",
        help=(
            "journaled apply: activate, restart the supervised launchd "
            "job, verify the new daemon reports this exact generation, "
            "and perform a proven process rollback on failure"
        ),
    )

    runtime_exec = commands.add_parser(
        "runtime-exec",
        help=(
            "resolve the durable active runtime activation and exec "
            "the daemon from that exact checkout (the supervised "
            "launchd entry point)"
        ),
    )
    runtime_exec.add_argument("--interval", type=int, default=30)

    hooks_install = commands.add_parser(
        "hooks-install",
        help=(
            "install the Hermes Stop/SubagentStop/PreToolUse hooks "
            "into every eligible classic profile's settings.json, "
            "idempotently"
        ),
    )
    hooks_install.add_argument("--json", action="store_true")

    intake_poll = commands.add_parser(
        "intake-poll",
        help=(
            "lead-owned intake handshake: lease the next pending "
            "envelope as an offer through the application layer "
            "(Claude Code hook)"
        ),
    )
    intake_poll.add_argument("--session", default=None)

    intake_signal = commands.add_parser(
        "intake-signal",
        help=(
            "deliberately wake one idle classic lead with the bounded "
            "signal; your invocation asserts no draft is staged in its "
            "prompt"
        ),
    )
    intake_signal.add_argument("--cell", required=True)
    intake_signal.add_argument("--session", required=True)
    intake_signal.add_argument("--packet", required=True)
    intake_signal.add_argument(
        "--kind", choices=("correction", "work"), required=True
    )
    intake_signal.add_argument("--json", action="store_true")

    intake_ack = commands.add_parser(
        "intake-ack",
        help=(
            "acknowledge one exact intake offer after retrieving its "
            "packet; only this records the delivery"
        ),
    )
    intake_ack.add_argument("--session", required=True)
    intake_ack.add_argument("--packet", required=True)
    intake_ack.add_argument("--offer", required=True)

    channel_trust_capture = commands.add_parser(
        "channel-trust-capture",
        help=(
            "persist the single manual channel-trust event's measured "
            "build/binding evidence as the cell's active trust anchor "
            "(INFRA-197 v5.1); re-measures from disk and refuses on "
            "any drift from the recorded evidence"
        ),
    )
    channel_trust_capture.add_argument("--evidence", type=Path, required=True)
    channel_trust_capture.add_argument("--json", action="store_true")

    channel_trust_confirm = commands.add_parser(
        "channel-trust-confirm",
        help=(
            "watch one lead seat for the hermes-control confirmation "
            "dialog, then either bind the anchor's pending prompt "
            "evidence from the live screen (no keypress) or evaluate "
            "the v5.1 trust gate, which presses Enter only on an "
            "exact-build full match; any mismatch fails closed as "
            "CHANNEL CONFIRMATION REQUIRED"
        ),
    )
    channel_trust_confirm.add_argument("--cell", required=True)
    channel_trust_confirm.add_argument("--wait-seconds", type=int, default=90)
    channel_trust_confirm.add_argument("--capture-prompt", action="store_true")
    channel_trust_confirm.add_argument("--json", action="store_true")

    rotate_lead = commands.add_parser(
        "rotate-lead",
        help=(
            "first-class idempotent lead rotation: validates the "
            "submitted handoff and clean pushed checkpoint, reserves a "
            "different healthy first-party Max profile, drives exact "
            "handoff acknowledgment, atomically transfers the cell and "
            "lease, and seats the replacement through the managed "
            "classic path; any failure is one actionable message "
            "(operator directive "
            "infra-197-first-class-profile-rotation-20260830-v1)"
        ),
    )
    rotate_lead.add_argument("--cell", required=True)
    rotate_lead.add_argument("--json", action="store_true")

    decision_import = commands.add_parser(
        "decision-import",
        help=(
            "import one recorded operator-decision receipt as durable "
            "state; refuses unless the file matches the given SHA-256"
        ),
    )
    decision_import.add_argument("--receipt", type=Path, required=True)
    decision_import.add_argument("--sha256", required=True)
    decision_import.add_argument("--json", action="store_true")

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

    verify = commands.add_parser(
        "verify",
        help=(
            "any-agent trusted verification runner: execute a gate "
            "command under --cwd and record a signed, content-addressed "
            "receipt (INFRA-186)"
        ),
    )
    verify.add_argument("--gate", required=True)
    verify.add_argument("--cwd", type=Path, default=Path("."))
    verify.add_argument("--json", action="store_true")
    # NOTE: the positional is named ``gate_command`` (not ``command``)
    # because the subparsers' own dispatch dest is ALSO "command"
    # (``commands = parser.add_subparsers(dest="command", ...)`` above);
    # a same-named positional here would silently overwrite that dest
    # with the REMAINDER list and break dispatch in ``main()``. The
    # metavar keeps the help text reading as "command".
    verify.add_argument("gate_command", nargs=argparse.REMAINDER, metavar="command")

    verify_check = commands.add_parser(
        "verify-check",
        help=(
            "any-agent transition validator: prove a fresh, signed "
            "receipt for --gate exists on the current tree at --cwd, "
            "without restating the gate's command (INFRA-186)"
        ),
    )
    verify_check.add_argument("--gate", required=True)
    verify_check.add_argument("--receipt", required=True)
    verify_check.add_argument("--cwd", type=Path, default=Path("."))
    verify_check.add_argument("--json", action="store_true")
    return parser


def _print_merger_turn(outcome: TurnOutcome) -> None:
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


async def _settle_idle_merger_thread(
    flow: MergeFlow, projects: Sequence[str], thread_id: str
) -> TurnOutcome | None:
    """Crash-recovery fallback for an explicitly submitted verdict.

    SR2 (supersedes fast-lane a3aa8cd8's idle-primary design): the
    verdict is submitted, never inferred. An idle
    ``thread/status/changed`` for the project's bound reviewer channel
    resumes settlement ONLY when a durable ``submitted_verdicts`` row in
    state ``'submitted'`` already exists for the project's outstanding
    wake — an explicit ``submit-review`` whose settlement a crash cut
    short. ``handle_turn`` then settles that durable document (it
    resumes the pending submission before ever pulling the thread), so
    idle never triggers a thread-pull-and-infer settlement. Without the
    durable pending submission, idle is a no-op; ``turn/completed``
    observation keeps its existing behavior.
    """

    for project_key in projects:
        channel = flow.merger.read_channel(project_key)
        if channel is None or channel.thread_id != thread_id:
            continue
        outstanding = flow.turns.outstanding_wake(project_key)
        if outstanding is None:
            return None
        event, _state = outstanding
        # The durable-read guard: MergerTurnService's own pending-
        # submission read (the same one its resume path settles from).
        if flow.turns._pending_submission(project_key, event.event_id) is None:
            return None
        return await flow.turns.handle_turn(project_key)
    return None


async def _listen_for_merger_turns(
    flow: MergeFlow,
    projects: Sequence[str] = (),
    *,
    session: MergerSession | None = None,
) -> None:
    """Settle Merger turns as the App Server reports them; never poll.

    INFRA-198 P1: when a ``session`` is given, an idle status also
    reconciles it — the terminal-idle hook that releases the App Server
    process lease the instant nothing is left outstanding. Reconciling
    from inside this same listener task never deadlocks:
    ``MergerSession.release`` recognizes it is running inside its own
    listener task and skips the self-cancellation, only closing the RPC
    inline, which is what lets this loop's own ``notifications()``
    iteration end and return on its own right after.
    """

    async for notification in flow.rpc.notifications():
        if notification.method == "thread/status/changed":
            status = notification.params.get("status")
            status_type = status.get("type") if isinstance(status, dict) else None
            thread_id = notification.params.get("threadId")
            print(
                json.dumps(
                    {
                        "thread_status": status_type,
                        "thread_id": thread_id,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                flush=True,
            )
            if status_type == "idle" and isinstance(thread_id, str):
                outcome = await _settle_idle_merger_thread(
                    flow, projects, thread_id
                )
                if outcome is not None:
                    _print_merger_turn(outcome)
                if session is not None:
                    await session.reconcile("terminal_idle")
            continue
        outcome = await flow.turns.on_notification(notification)
        if outcome is not None:
            _print_merger_turn(outcome)


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
    database: Database | None = None,
    request_checkpoint: Callable[[str, str], Awaitable[object]] | None = None,
    checkpoint_dispatcher: CheckpointDispatcher | None = None,
    wake_delivery: LeadWakeDelivery | None = None,
    wake_reconciler: LeadWakeReconciler | None = None,
    cmux_reconciler: CmuxSurfaceReconciler | None = None,
    cmux_hibernation: CmuxHibernationDriver | None = None,
    lead_intake: LeadIntakeRouter | None = None,
    channel_hub: ChannelHub | None = None,
    control_operations: ControlOperations | None = None,
    fakechat_router: FakechatWakeRouter | None = None,
    activation: object | None = None,
    dashboard_refresh: DashboardRefreshAction | None = None,
    orchestrator_workspace: OrchestratorWorkspaceOwner | None = None,
    post_merge: PostMergeAdvance | None = None,
) -> Supervisor:
    # INFRA-198 P1: bound to the merge flow's lifetime once merge_flow is
    # created below (only in the non-``once`` path); the maintenance
    # closure reads this by reference at tick time, never at definition
    # time, so it always observes the current session.
    session: MergerSession | None = None

    async def _maintenance() -> None:
        if cmux_hibernation is not None:
            await cmux_hibernation.tick()
        if session is not None:
            # Event-scoped App Server ownership: open when review work
            # is active and the session is closed, release when it is
            # open and nothing is outstanding. Never raises.
            await session.reconcile("maintenance")
        if orchestrator_workspace is not None:
            # The bounded reconciliation cadence for the two-pane
            # Orchestrator workspace: the owner absorbs cmux failures
            # and seated refusals itself, so the tick never raises.
            await orchestrator_workspace.tick()
        if dashboard_refresh is not None:
            # tick() contains its own failures and the pane no-ops off
            # a TTY, so the dashboard refresh rides the maintenance
            # slot unconditionally.
            await dashboard_refresh.tick()
        if lead_intake is not None:
            # Restart-safe by derivation: every tick re-routes any
            # durable pending correction or wake whose envelope has
            # not been delivered to its classic seat.
            await lead_intake.tick()
        if channel_hub is not None:
            # Low-frequency repair only: freshly committed packets
            # route directly through the in-process router and the
            # nudge op; this sweep recovers whatever those missed.
            await channel_hub.publish_pending()
        if fakechat_router is not None:
            # Same recovery shape for the fakechat wake plane: the
            # commit-time router already signaled fresh envelopes;
            # this sweep re-wakes whatever stayed announced.
            fakechat_router.signal_pending()
        if post_merge is not None:
            # INFRA-198 P2: discover any terminal merge the review
            # flow's fast path missed, and advance any pending
            # self-host activation intent or successor dependency
            # flip from durable rows. Contains its own failures.
            await post_merge.tick()

    supervisor = Supervisor(
        service,
        dispatch=dispatch,
        request_checkpoint=request_checkpoint,
        checkpoint_dispatcher=checkpoint_dispatcher,
        interval_seconds=interval,
        wake_delivery=wake_delivery,
        maintenance=(
            None
            if (
                cmux_hibernation is None
                and lead_intake is None
                and channel_hub is None
                and dashboard_refresh is None
                and orchestrator_workspace is None
                and merge_flow is None
                and post_merge is None
            )
            else _maintenance
        ),
    )
    if channel_hub is not None:
        # The hub socket exists before any tick so a sidecar spawned
        # by an already-running classic seat can register immediately.
        await channel_hub.start()
    if control_operations is not None:
        # Every active lead gets a durable, ACKable restart receipt —
        # carrying the exact activated runtime identity — so daemon
        # recovery is provable without watching any terminal.
        result: dict[str, object] = {
            "interval_seconds": interval,
            "once": once,
        }
        if activation is not None:
            result["activation"] = {
                "generation": activation.generation,  # type: ignore[attr-defined]
                "git_sha": activation.git_sha,  # type: ignore[attr-defined]
                "checkout_root": activation.checkout_root,  # type: ignore[attr-defined]
                "binary_path": activation.binary_path,  # type: ignore[attr-defined]
                "database_schema": activation.database_schema,  # type: ignore[attr-defined]
            }
        with suppress(Exception):
            control_operations.record_for_active_cells(
                kind="daemon.restarted", result=result
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
    if orchestrator_workspace is not None:
        # The daemon ensures its two-pane Orchestrator workspace
        # autonomously the moment it starts: create it, adopt a healthy
        # survivor exactly once, or recover it — no manual CLI step.
        # start() absorbs cmux failures, so visibility never blocks
        # orchestration.
        await orchestrator_workspace.start()
    if dashboard_refresh is not None:
        # Establish the dashboard pane the moment the daemon starts (and
        # after any restart): the pane's idempotent fresh-writer
        # establishment is the whole recovery story, and tick() never
        # raises.
        await dashboard_refresh.tick()
    if once:
        try:
            await supervisor.run_once()
        finally:
            if channel_hub is not None:
                await channel_hub.stop()
        return supervisor
    if merge_flow is not None:
        # INFRA-198 P1: the session owns the App Server connection's
        # whole lifecycle — startup recovery (open, resume_settlements,
        # recover_outstanding, then release unless work is still
        # outstanding), the maintenance-tick open/release above, the
        # listener's own terminal-idle release, and daemon shutdown
        # below — so the process lease is held only while review work is
        # actually active.
        session = MergerSession(
            merge_flow,
            tuple(projects),
            events=(EventStore(database) if database is not None else None),
            database=database,
            listener_factory=(
                lambda flow, session_projects, active_session: (
                    _listen_for_merger_turns(
                        flow, session_projects, session=active_session
                    )
                )
            ),
        )
        await session.startup()
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
        if session is not None:
            await session.shutdown()
        if channel_hub is not None:
            await channel_hub.stop()
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


ROTATION_REGISTRATION_WAIT_SECONDS = 60.0
"""How long ``rotate-lead`` waits for the replacement session's active
hermes-control channel registration before reporting the rotation
blocked (see ``LeadRotation``'s completion gate)."""


class _NoDispatchLinear:
    """Placeholder Linear projector for one-shot lead rotation.

    ``ProjectCellService.rotate`` never dispatches a fresh issue or
    projects a Linear status — only ``dispatch`` does that — so
    ``_open_rotation_collaborators`` builds its cell service without the
    live Linear router the daemon needs for admission, avoiding a
    network/Keychain dependency the rotation itself never exercises.
    Either method being called here would be a programming error, not
    an operator refusal, so both raise loudly rather than returning
    anything.
    """

    async def validate(self, project_key: str, issue_id: str) -> object:
        raise AssertionError("lead rotation never validates a Linear issue")

    async def project(self, issue_id: str, target: Any, effect_id: str) -> object:
        raise AssertionError("lead rotation never projects a Linear status")


def _open_rotation_collaborators(
    settings: Settings, runtime: Runtime
) -> tuple[ProjectCellService, CmuxLeadSeater]:
    """Compose the project cell service and classic seater rotation needs.

    Neither is exposed on :class:`Runtime`: ``cells`` is only assembled
    for the live daemon (``enable_live``), and the classic
    :class:`CmuxLeadSeater` was never stored on the runtime graph at
    all — ``open_runtime`` builds it as a local variable and hands it
    straight to ``ProjectCellService`` without keeping a reference. Both
    are constructed here from the same local, credential-free
    collaborators ``open_runtime`` uses for them (profile registry, the
    auth-status probe, the cmux CLI adapter), mirroring
    ``_open_merge_flow``'s reuse-else-construct idiom. Raises
    ``ValueError`` with an actionable message when a required local file
    (``config/profiles.yaml``) is missing; the caller turns that into a
    clean refusal rather than a traceback.
    """

    assert settings.cmux is not None
    assert runtime.cmux_bindings is not None
    database = runtime.database
    events = EventStore(database)
    profile_path = settings.repo_root / "config" / "profiles.yaml"
    if not profile_path.exists():
        raise ValueError("the cell service or seater is unavailable")
    registry = ProfileRegistry.load(profile_path)
    environment = dict(os.environ)
    # Sol correction a06cbce0: an authenticated profile is eligible only
    # once its onboarding/theme/trust state is deterministically
    # established for exactly the managed repositories a seated lead
    # actually runs from — the same paths the seater launches from (see
    # ``lead_worktree = project.lead_cwd`` at this command's rotation
    # call site) — otherwise a restored session can still stop on a
    # first-run dialog instead of Hermes's handoff.
    # Sol correction c5600e31: derived from ``project.lead_cwd`` — the
    # one canonical managed lead cwd — so bootstrap trust and the
    # ``project_paths`` handed to the seater/cell service below can
    # never disagree.
    managed_repo_paths = tuple(
        dict.fromkeys(
            project.lead_cwd for project in settings.projects.values()
        )
    )
    bootstrap = ProfileBootstrap(registry, repo_paths=managed_repo_paths)
    probe = ClaudeProfileProbe(registry, base_env=environment, bootstrap=bootstrap)
    # Mirrors open_runtime's own startup seeding (runtime.py:453-458): a
    # freshly built ProfilePool starts every slot ineligible, and only
    # ``record_health`` ever flips one to eligible, so the pool this
    # one-shot command hands to ``ProjectCellService`` must be seeded
    # exactly like the daemon's before ``reserve_replacement`` can ever
    # find a healthy profile. Unlike the daemon's startup assembly —
    # which lets a probe failure abort the whole live runtime — this
    # command probes four profiles for a single rotation, so one
    # profile's probe failing must not crash the others: it is recorded
    # as an ineligible, reasoned ``ProfileHealth`` instead of raising
    # past this builder.
    # INFRA-205: replacement selection fails closed on durable Fable
    # capacity evidence, not auth health alone — the live defect seated
    # max-a while profile_capacity_observations recorded it capped.
    pool = ProfilePool(
        registry, capacity_evidence=ProfileCapacityEvidence(database)
    )
    for profile in registry.profiles:
        try:
            health = probe.check(profile.alias)
        except (OSError, subprocess.SubprocessError, ValueError) as error:
            health = ProfileHealth(
                profile_alias=profile.alias,
                eligible=False,
                reason=f"probe_failed: {error}",
                last_checked_at=datetime.now(UTC),
            )
        pool.record_health(health)
    # Sol correction c5600e31: the same canonical ``lead_cwd`` used for
    # ``managed_repo_paths`` above — the seater and cell service must
    # launch into exactly the directory bootstrap already trusted.
    project_paths: Mapping[str, Path] = {
        alias: project.lead_cwd for alias, project in settings.projects.items()
    }
    cmux_port = CmuxCliAdapter(
        settings.cmux.cli,
        base_env=environment,
        password_source=cmux_password_source(Keychain()),
    )
    # INFRA-207: the one-shot seater must carry the same hermes-control
    # channel launcher and trust confirmer the live daemon composes —
    # otherwise a lead rotation seats the replacement without its
    # channel.
    channel_launcher, channel_confirmer = build_channel_collaborators(
        settings=settings,
        database=database,
        events=events,
        control_operations=runtime.control_operations,
        cmux_port=cmux_port,
    )
    seater = CmuxLeadSeater(
        bindings=runtime.cmux_bindings,
        port=cmux_port,
        project_paths=project_paths,
        profile_dirs=RegistryProfileDirectory(registry),
        auth_probe=lambda alias: probe.check(alias).eligible,
        channel_launch=channel_launcher,
        control=runtime.control_operations,
        channel_trust=channel_confirmer,
    )
    if runtime.cells is not None:
        return runtime.cells, seater
    runner = ClaudeRunner(
        registry,
        prompt_file=settings.repo_root / "prompts" / "claude-lead.md",
        base_env=environment,
        processes=runtime.processes,
        freeze_dir=settings.state_dir / "freezes",
        launch_failure_recorder=(
            control_launch_failure_recorder(runtime.control_operations)
            if runtime.control_operations is not None
            else None
        ),
    )
    cells = ProjectCellService(
        database=database,
        events=events,
        queue=runtime.queue,
        profiles=pool,
        runner=runner,
        linear=_NoDispatchLinear(),
        project_paths=project_paths,
        handoffs=HandoffService(database),
        safety=CheckpointSafetyStore(database, events),
        checkpoints=runtime.checkpoints,
        context=ContextMonitor(database, events, policy=settings.policy),
        surfaces=seater,
    )
    return cells, seater


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


def _active_cell_for_issue(database: Database, issue_id: str) -> tuple[str, str, str]:
    """Resolve the ``(cell_id, session_id, project_key)`` owning ``issue_id``.

    Looks up the issue's project from ``admitted_issues`` and then the
    single active cell for that project; refuses closed when either is
    absent so a packet can never be bound to a stale or foreign cell.
    """

    issue_row = database.execute(
        "SELECT project_key FROM admitted_issues WHERE issue_id = ?",
        (issue_id,),
    ).fetchone()
    if issue_row is None:
        raise ValueError(f"no active cell owns issue {issue_id!r}")
    project_key = str(issue_row["project_key"])
    cell_row = database.execute(
        "SELECT cell_id, session_id, project_key FROM project_cells "
        "WHERE project_key = ? AND state = 'active'",
        (project_key,),
    ).fetchone()
    if cell_row is None:
        raise ValueError(f"no active cell for project {project_key!r}")
    return (
        str(cell_row["cell_id"]),
        str(cell_row["session_id"]),
        str(cell_row["project_key"]),
    )


def _packet_payload(packet: Any) -> dict[str, Any]:
    return dict(dataclasses.asdict(packet))


def _measure_direct_exception_diff(
    run: Callable[[Sequence[str]], str],
    worktree: str | None,
    expected_files: Sequence[str],
) -> int:
    """The authoritative-on-the-cli-side measured added+deleted line total
    for ``expected_files`` in ``worktree``.

    Measurement must SUCCEED before any direct-exception packet is
    created — there is no "unmeasured" fallback. Raises ``ValueError``
    when the worktree is absent, the git measurement command fails, or
    its numstat output cannot be parsed for a declared file."""

    if not worktree:
        raise ValueError(
            "direct-work exception requires a worktree to measure the "
            "live diff: refusing before any packet is created"
        )
    try:
        output = run(
            [
                "git",
                "-C",
                str(worktree),
                "diff",
                "--numstat",
                "HEAD",
                "--",
                *expected_files,
            ]
        )
    except Exception as error:
        raise ValueError(
            "direct-work exception could not measure the live diff: "
            f"git numstat failed: {error}"
        ) from error
    total = 0
    for line in output.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        try:
            total += int(parts[0]) + int(parts[1])
        except ValueError as error:
            raise ValueError(
                "direct-work exception could not measure the live diff: "
                f"git numstat produced an unparsable line: {line!r}"
            ) from error
    return total


def _default_diff_numstat_run(args: Sequence[str]) -> str:
    """Argv-safe, no-shell subprocess seam for one bounded git invocation."""

    completed = subprocess.run(
        list(args),
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    return completed.stdout


def _hermes_handlers(
    settings: Any,
    runtime: Runtime,
    outbox: LeadCorrectionOutbox,
    packets: SubagentPackets,
    *,
    run: Callable[[Sequence[str]], str] | None = None,
) -> dict[str, Any]:
    git_diff_run = run or _default_diff_numstat_run
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

    def apply_operator_decision(command: Any) -> dict[str, Any]:
        decisions = OperatorDecisions(runtime.database)
        try:
            decision = decisions.apply(
                decision_id=command.decision_id,
                status=command.status,
                source_message=command.source_message,
            )
        except DecisionRefused as error:
            raise ValueError(str(error)) from error
        return {
            "decision_id": decision.decision_id,
            "issue_id": decision.issue_id,
            "status": decision.status,
            "applied_at": decision.applied_at,
        }

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

    def create_packet(command: Any) -> dict[str, Any]:
        cell_id, session_id, project_key = _active_cell_for_issue(
            runtime.database, command.issue_id
        )
        packet = packets.create(
            issue_id=command.issue_id,
            project_key=project_key,
            cell_id=cell_id,
            session_id=session_id,
            generation=1,
            model_tier=command.model_tier,
            effort=command.effort,
            allowed_files=command.allowed_files,
            worktree=command.worktree,
            depends_on=command.depends_on,
            red_test=command.red_test,
            verification=command.verification,
            invariants=command.invariants,
            resource_note=command.resource_note,
        )
        return _packet_payload(packet)

    def accept_packet(command: Any) -> dict[str, Any]:
        packet = packets.accept(command.packet_id, evidence=command.evidence)
        return _packet_payload(packet)

    def reject_packet(command: Any) -> dict[str, Any]:
        packet = packets.reject(command.packet_id, reason=command.reason)
        return _packet_payload(packet)

    def record_direct_exception(command: Any) -> dict[str, Any]:
        if len(command.expected_files) > 2 or command.expected_lines > 30:
            raise ValueError(
                "direct-work exception exceeds the reviewer-fix scale"
            )
        cell_id, session_id, _project_key = _active_cell_for_issue(
            runtime.database, command.issue_id
        )
        worktree = command.worktree
        measured_lines = _measure_direct_exception_diff(
            git_diff_run, worktree, command.expected_files
        )
        if measured_lines > 30 or measured_lines > command.expected_lines:
            raise ValueError(
                "direct-work exception exceeds the measured diff bound"
            )
        packet = packets.create(
            issue_id=command.issue_id,
            project_key=_project_key,
            cell_id=cell_id,
            session_id=session_id,
            generation=1,
            model_tier="fable",
            effort="high",
            allowed_files=command.expected_files,
            worktree=worktree,
            red_test="direct-work exception",
            verification=[command.verification],
            invariants=command.reason,
            resource_note=f"expected_lines={command.expected_lines}",
        )
        tool_use_id = f"direct:{command.issue_id}"
        packets.reserve(
            packet.packet_id, session_id=session_id, tool_use_id=tool_use_id
        )
        packets.settle(
            packet.packet_id, outcome="completed", tool_use_id=tool_use_id
        )
        packet = packets.accept(
            packet.packet_id,
            evidence={
                "exception_reason": command.reason,
                "expected_lines": str(command.expected_lines),
                "measured_lines": str(measured_lines),
            },
        )
        return _packet_payload(packet)

    return {
        "pending_corrections": pending_corrections,
        "ack_correction": ack_correction,
        "pending_wakes": pending_wakes,
        "ack_wake": ack_wake,
        "apply_operator_decision": apply_operator_decision,
        "qa_reject": qa_reject,
        "report_stall": report_stall,
        "approve_playbook": approve_playbook,
        "pending_consultations": pending_consultations,
        "record_remedy_result": record_remedy_result,
        "create_packet": create_packet,
        "accept_packet": accept_packet,
        "reject_packet": reject_packet,
        "record_direct_exception": record_direct_exception,
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


def _git_head_sha(repo_root: Path) -> str:
    """The exact source revision a smoke run executed from.

    Falls back to ``unknown`` when the repo root is not a git checkout
    (unit fixtures): the evidence document still validates, and a live
    run always records the real candidate sha.
    """

    try:
        completed = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    sha = completed.stdout.strip()
    return sha if completed.returncode == 0 and sha else "unknown"


def _print(payload: Any, *, json_output: bool, human: str) -> None:
    if json_output:
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    else:
        print(human)


_GATE_PACKET_MARKER = re.compile(r"\bpacket:([0-9a-f]{32})\b")


def subagent_gate(
    freeze_dir: Path,
    payload: str,
    *,
    admission: PacketAdmission | None = None,
) -> tuple[int, str]:
    """PreToolUse hook: block Agent tool use while the session is frozen,
    and — once a packet marker is present and packet admission is wired
    into this process — gate the launch through the durable packet ledger.

    Exit 2 blocks the tool call in Claude Code and returns the message to
    the lead; exit 0 allows it. Malformed input never blocks silently: it
    fails closed with exit 2 only when a freeze marker cannot be ruled out.
    A launch with no ``packet:<hex>`` marker in its description/name, or
    one wired into a process that has no ``admission`` service, keeps the
    prior freeze-only behavior unchanged.
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

    tool_input = document.get("tool_input")
    packet_id = None
    if isinstance(tool_input, dict):
        for field_name in ("description", "name"):
            text = tool_input.get(field_name)
            if isinstance(text, str) and (
                match := _GATE_PACKET_MARKER.search(text)
            ):
                packet_id = match.group(1)
                break
    if packet_id is None or admission is None:
        return 0, ""

    tool_use_id = document.get("tool_use_id")
    decision = admission.admit(
        session_id=session_id,
        packet_id=packet_id,
        model=str(tool_input.get("model") or ""),
        effort=str(tool_input.get("effort") or "high"),
        tool_use_id=str(tool_use_id) if tool_use_id else f"gate:{packet_id}",
    )
    if not decision.allowed:
        return 2, f"subagent gate: {decision.reason}"
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


def _deploy_bootstrap_script(args: argparse.Namespace) -> str:
    """The exact bootstrap this deployment renders and installs."""

    import shutil
    from pathlib import PurePosixPath

    from hermes_orchestrator.deploy.launchd import render_bootstrap

    uv_binary = shutil.which("uv") or "/opt/homebrew/bin/uv"
    return render_bootstrap(
        uv_binary=PurePosixPath(uv_binary),
        config_repo=args.config_repo,
        state_dir=args.service_state_dir,
    )


def _deploy_render(args: argparse.Namespace) -> int:
    paths = lifecycle.render_artifacts(
        _deploy_inventory(args),
        args.output_dir,
        console_port=args.console_port,
        bootstrap=_deploy_bootstrap_script(args),
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
    import hashlib as hashlib_module

    steps = lifecycle.plan_install(
        _deploy_inventory(args),
        rendered_dir=args.rendered_dir,
        launch_agents_dir=args.launch_agents_dir,
        uid=args.uid,
        console_port=args.console_port,
        # The worktree-independent bootstrap installs to the exact
        # binary path both launchd jobs execute.
        bootstrap_target=args.binary,
        bootstrap_identity=hashlib_module.sha256(
            _deploy_bootstrap_script(args).encode("utf-8")
        ).hexdigest(),
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


def _intake_state_dir(args: argparse.Namespace) -> Path:
    return (
        args.state_dir
        if args.state_dir is not None
        else Path.home() / ".local" / "share" / "hermes-orchestrator"
    )


def _code_checkout_root() -> Path:
    """The checkout this process's code actually runs from.

    Distinct from the configured repo root (the stable config anchor):
    under the supervised runtime-exec entry point the code checkout is
    whatever the active activation resolved, and identity must come
    from the running package, never from configuration.
    """

    import hermes_orchestrator

    return Path(hermes_orchestrator.__file__).resolve().parents[2]


def _runtime_exec(args: argparse.Namespace) -> int:
    """Exec the daemon from the durable active activation's checkout.

    This is the supervised launchd entry point: launchd owns the
    process lifetime while the activation ledger owns which checkout
    runs. With no activation recorded yet, the configured repo root is
    the bootstrap fallback.
    """

    import shutil
    import sqlite3 as sqlite3_module

    state_dir = _intake_state_dir(args)
    project = str(args.repo_root)
    # Resolution is strictly read-only: the entry point must never
    # migrate the database (that is a deliberate activation concern).
    with suppress(Exception):
        connection = sqlite3_module.connect(
            f"file:{state_dir / 'state.db'}?mode=ro", uri=True
        )
        try:
            row = connection.execute(
                "SELECT checkout_root FROM runtime_activations "
                "WHERE state = 'active'"
            ).fetchone()
            if row is not None:
                project = str(row[0])
        finally:
            connection.close()
    uv_binary = shutil.which("uv") or "/opt/homebrew/bin/uv"
    argv = [
        uv_binary,
        "run",
        "--project",
        project,
        "hermes-orchestrator",
        "--repo-root",
        str(args.repo_root),
        "--state-dir",
        str(state_dir),
        "daemon",
        "--interval",
        str(args.interval),
    ]
    os.execvp(argv[0], argv)
    return 1  # pragma: no cover — exec never returns


def _warm_artifact(artifact: Path) -> None:
    """Build the artifact environment from source and prove it runnable.

    The forced source reinstall defeats any stale cached wheel for the
    unchanged package version, and the help proof requires the entry
    point the supervised job will exec — a wrongly built artifact can
    never become the active runtime.
    """

    import shutil
    import subprocess

    uv_binary = shutil.which("uv") or "/opt/homebrew/bin/uv"
    subprocess.run(
        (
            uv_binary,
            "sync",
            "--project",
            str(artifact),
            "--reinstall-package",
            "hermes-orchestrator",
        ),
        check=True,
        capture_output=True,
        timeout=600,
    )
    proof = subprocess.run(
        (
            uv_binary,
            "run",
            "--project",
            str(artifact),
            "hermes-orchestrator",
            "--help",
        ),
        check=True,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if "runtime-exec" not in proof.stdout:
        raise RuntimeError(
            "the artifact CLI does not expose runtime-exec; a stale "
            "build was produced"
        )


def _runtime_activate(args: argparse.Namespace, settings: Settings) -> int:
    """Deliberate approved-version activation with safe rollback."""

    from hermes_orchestrator.activation import (
        ActivationRefused,
        RuntimeActivator,
    )
    from hermes_orchestrator.events import EventStore

    if args.apply:
        return _runtime_apply(args, settings)
    database = Database.open(settings.state_dir / "state.db")
    try:
        activator = RuntimeActivator(database, events=EventStore(database))
        try:
            activation = activator.activate_artifact(
                source_checkout=_code_checkout_root(),
                state_dir=settings.state_dir,
                prepare=_warm_artifact,
            )
        except ActivationRefused as error:
            current = activator.current()
            _print(
                {
                    "activated": False,
                    "reason": str(error),
                    "active_generation": (
                        None if current is None else current.generation
                    ),
                },
                json_output=args.json,
                human=(
                    f"activation refused: {error}; the prior activation "
                    "stays active"
                ),
            )
            return 1
    finally:
        database.close()
    _print(
        {
            "activated": True,
            "generation": activation.generation,
            "git_sha": activation.git_sha,
            "checkout_root": activation.checkout_root,
            "binary_path": activation.binary_path,
            "database_schema": activation.database_schema,
        },
        json_output=args.json,
        human=(
            f"activated generation {activation.generation}: "
            f"{activation.checkout_root} @ {activation.git_sha[:12]} "
            f"(schema {activation.database_schema})"
        ),
    )
    return 0


def _runtime_apply(args: argparse.Namespace, settings: Settings) -> int:
    """Journaled apply: activate, kickstart, verify, or proven rollback."""

    import subprocess as subprocess_module

    from hermes_orchestrator.activation import (
        ActivationApplier,
        ActivationRefused,
        ApplyFailed,
        RuntimeActivator,
    )
    from hermes_orchestrator.deploy.launchd import (
        OPERATIONS_LABEL,
        ORCHESTRATOR_LABEL,
    )
    from hermes_orchestrator.events import EventStore

    def kickstart() -> None:
        # The fleet restarts as one: both activation-bound jobs. A
        # kickstart shortly after a prior one can block on launchd's
        # ThrottleInterval (30s floor), so the client timeout must ride
        # it out — observed live: a 30s timeout during a rollback
        # restart turned a recoverable throttle wait into an ambiguous
        # apply.
        for label in (ORCHESTRATOR_LABEL, OPERATIONS_LABEL):
            subprocess_module.run(
                (
                    "launchctl",
                    "kickstart",
                    "-k",
                    f"gui/{os.getuid()}/{label}",
                ),
                check=True,
                capture_output=True,
                timeout=90,
            )

    database = Database.open(settings.state_dir / "state.db")
    try:
        applier = ActivationApplier(
            RuntimeActivator(database, events=EventStore(database)),
            database,
            kickstart=kickstart,
            verify_services=("daemon", "console"),
        )
        try:
            report = applier.apply(
                checkout_root=_code_checkout_root(),
                artifact_state_dir=settings.state_dir,
                prepare=_warm_artifact,
            )
        except (ActivationRefused, ApplyFailed) as error:
            _print(
                {"applied": False, "reason": str(error)},
                json_output=args.json,
                human=f"apply failed closed: {error}",
            )
            return 1
    finally:
        database.close()
    _print(
        {
            "applied": report.state == "verified",
            "apply_id": report.apply_id,
            "state": report.state,
            "generation": report.target_generation,
            "verified_pid": report.verified_pid,
            "reason": report.reason,
        },
        json_output=args.json,
        human=(
            f"apply {report.apply_id}: {report.state} "
            f"(generation {report.target_generation}, "
            f"pid {report.verified_pid})"
        ),
    )
    return 0 if report.state == "verified" else 1


def _hooks_install(args: argparse.Namespace, settings: Settings) -> int:
    """Install the Hermes hooks into every classic profile, idempotently."""

    import shutil

    from hermes_orchestrator.hook_install import (
        HookCommandSet,
        HookInstaller,
    )
    from hermes_orchestrator.profiles import ProfileRegistry

    profile_path = settings.repo_root / "config" / "profiles.yaml"
    if not profile_path.exists():
        print(
            "hooks-install requires config/profiles.yaml", file=sys.stderr
        )
        return 1
    registry = ProfileRegistry.load(profile_path)
    uv_binary = shutil.which("uv") or "uv"
    base = (
        f"{uv_binary} run --project {settings.repo_root} "
        f"hermes-orchestrator --repo-root {settings.repo_root} "
        f"--state-dir {settings.state_dir}"
    )
    installer = HookInstaller(
        profiles={
            profile.alias: profile.config_dir
            for profile in registry.profiles
        },
        commands=HookCommandSet(
            stop=f"{base} intake-poll",
            subagent_stop=f"{base} child-stop",
            child_start=f"{base} child-start",
        ),
    )
    reports = installer.install()
    if args.json:
        print(
            json.dumps(
                {"profiles": [report.as_dict() for report in reports]}
            )
        )
    else:
        for report in reports:
            status = (
                "updated" if report.changed else "already installed"
            )
            print(f"{report.alias}: {status} ({report.path})")
    return 0


def _verify(args: argparse.Namespace, settings: Settings) -> int:
    """Any-agent trusted verification runner (INFRA-186): execute the
    trailing command under ``--cwd`` and record a signed,
    content-addressed receipt. Callable by the lead, an implementation
    subagent, or Sol alike — nothing here binds to a session or role."""

    import shlex

    from hermes_orchestrator.verifier import Verifier

    command = list(args.gate_command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        _print(
            {"error": "no command given"},
            json_output=args.json,
            human=(
                "verify: a trailing command is required, e.g. "
                "`verify --gate g -- pytest -q`"
            ),
        )
        return 2
    database = Database.open(settings.state_dir / "state.db")
    try:
        verifier = Verifier(
            database, events=EventStore(database), state_dir=settings.state_dir
        )
        # Each already-split argv element is re-quoted before being
        # rejoined: ``Verifier`` takes one command STRING and
        # ``shlex.split``s it itself, so an unquoted join would corrupt
        # any argument containing whitespace or shell metacharacters
        # (e.g. an inline ``python3 -c "print('1 passed')"``).
        receipt = verifier.run_verified(
            args.cwd,
            gate_id=args.gate,
            command=" ".join(shlex.quote(part) for part in command),
        )
    finally:
        database.close()
    _print(
        {
            "receipt_id": receipt.receipt_id,
            "gate_id": receipt.gate_id,
            "exit_code": receipt.exit_code,
            "test_counts": receipt.test_counts,
            "fresh": True,
        },
        json_output=args.json,
        human=(
            f"receipt {receipt.receipt_id}: gate {receipt.gate_id} "
            f"exit {receipt.exit_code}"
        ),
    )
    return 0 if receipt.exit_code == 0 else 1


def _verify_check(args: argparse.Namespace, settings: Settings) -> int:
    """Any-agent transition validator (INFRA-186): prove SOME fresh,
    signed receipt for ``--gate`` exists on the current tree at
    ``--cwd``, without restating the gate's own command — the command
    is bound inside the attested receipt and reported to reviewers
    from there."""

    from hermes_orchestrator.verifier import Verifier

    database = Database.open(settings.state_dir / "state.db")
    try:
        verifier = Verifier(
            database, events=EventStore(database), state_dir=settings.state_dir
        )
        valid, reason = verifier.validate_for_tree(
            args.receipt, cwd=args.cwd, gate_id=args.gate
        )
    finally:
        database.close()
    _print(
        {"receipt_id": args.receipt, "valid": valid, "reason": reason},
        json_output=args.json,
        human=f"receipt {args.receipt}: {reason}",
    )
    return 0 if valid else 1


def _child_event(args: argparse.Namespace, *, completed: bool) -> int:
    """Durably count one child start or completion from a lead hook.

    A hook must never break its lead: any failure is swallowed and the
    exit code stays 0. The completion that settles the last
    outstanding child reactivates a waiting continuation exactly once
    through a children.completed control operation.
    """

    session = args.session
    child = args.child
    if session is None or child is None:
        with suppress(Exception):
            payload = json.loads(sys.stdin.read() or "{}")
            session = session or payload.get("session_id")
            # Strictly the shared lifecycle identity: SubagentStart
            # and SubagentStop carry the same agent_id. A tool_use_id
            # (a PreToolUse invocation identity) or any other field
            # can never satisfy it; an event without agent_id fails
            # closed and never advances progress.
            child = child or payload.get("agent_id")
    if not session or not child:
        return 0
    with suppress(Exception):
        from hermes_orchestrator.events import EventStore
        from hermes_orchestrator.lead_children import LeadChildTracker

        database = Database.open(_intake_state_dir(args) / "state.db")
        try:
            tracker = LeadChildTracker(
                database,
                control=ControlOperations(
                    database, events=EventStore(database)
                ),
            )
            if completed:
                tracker.child_completed(str(session), str(child))
            else:
                tracker.child_started(str(session), str(child))
        finally:
            database.close()
    return 0


def _intake_poll(args: argparse.Namespace) -> int:
    """Lease the calling lead's next pending envelope, if any.

    Invoked from the lead's own Claude Code hook: the session id comes
    from the hook payload on stdin (or --session), and the offer —
    envelope plus its opaque token — is returned as hook JSON through
    the application layer, never any terminal buffer. Nothing is
    recorded delivered here: the lead must acknowledge the exact offer
    with intake-ack after retrieving the packet, and an unacknowledged
    offer is re-offered once its lease expires. No offer means empty
    output and exit 0, so the hook is a no-op.
    """

    from hermes_orchestrator.lead_intake import LeadIntakePoll

    session = args.session
    hook_event = None
    if session is None:
        with suppress(Exception):
            payload = json.loads(sys.stdin.read() or "{}")
            session = payload.get("session_id")
            hook_event = payload.get("hook_event_name")
    if not session:
        return 0
    database = Database.open(_intake_state_dir(args) / "state.db")
    try:
        if hook_event == "Stop":
            # A Stop with live background children records the durable
            # continuation the last child completion will reactivate;
            # a Stop with none supersedes any stale promise.
            with suppress(Exception):
                from hermes_orchestrator.events import EventStore
                from hermes_orchestrator.lead_children import (
                    LeadChildTracker,
                )

                LeadChildTracker(
                    database,
                    control=ControlOperations(
                        database, events=EventStore(database)
                    ),
                ).record_turn_stop(str(session))
        # INFRA-201: settle this session's maintenance receipts
        # silently before offering anything — no output either way,
        # for any hook event, so a wake never reaches the primary view
        # for pure transport/lifecycle churn.
        with suppress(Exception):
            from hermes_orchestrator.events import EventStore

            ControlOperations(
                database, events=EventStore(database)
            ).settle_maintenance_for_session(str(session))
        offer = LeadIntakePoll(database=database).next_offer(str(session))
    finally:
        database.close()
    if offer is None:
        return 0
    print(
        json.dumps(
            {
                "decision": "block",
                "reason": (
                    f"{offer.envelope} — Hermes intake offer "
                    f"{offer.offer_token}: retrieve this packet from "
                    "durable state by its id, then acknowledge with "
                    "`hermes-orchestrator intake-ack --session "
                    f"{session} --packet {offer.packet_id} --offer "
                    f"{offer.offer_token}` and act on it now."
                ),
            }
        )
    )
    return 0


@dataclasses.dataclass(frozen=True, slots=True)
class WorktreeState:
    """One bounded snapshot of a project worktree's git state.

    Structurally matches the rotation gate's own ``WorktreeState``
    (``branch``, ``head``, ``origin_head``, ``dirty``): the gate reads
    these fields directly and refuses when ``head != origin_head`` or
    ``dirty`` is true, so an unknown value here must resolve to
    something that fails that comparison rather than to ``None``,
    which the gate never expects.
    """

    branch: str
    head: str
    origin_head: str
    dirty: bool


def _worktree_state(path: Path) -> WorktreeState:
    """The worktree's branch, HEAD, origin HEAD, and dirtiness.

    Read via a handful of bounded ``git`` subprocess calls so the
    rotation gate can verify the submitted handoff names a clean,
    pushed checkpoint before any lead identity moves. Any git failure
    (missing worktree, detached HEAD with no branch, no upstream, git
    itself absent) is mapped fail-closed here rather than left for the
    gate to interpret: a string field that could not be read resolves
    to ``""`` — never coincidentally equal to another unknown value —
    and cleanliness that could not be measured resolves to
    ``dirty=True``; unknown cleanliness must refuse, never pass as
    clean. The gate remains the boundary that acts on these values, but
    the probe itself now only ever hands it fail-closed data.
    """

    def _run(args: list[str]) -> str | None:
        try:
            result = subprocess.run(
                ["git", "-C", str(path), *args],
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        return result.stdout.strip()

    branch = _run(["branch", "--show-current"]) or ""
    head = _run(["rev-parse", "HEAD"]) or ""
    origin_head = (_run(["rev-parse", f"origin/{branch}"]) or "") if branch else ""
    status = _run(["status", "--porcelain"])
    dirty = status is None or status != ""
    return WorktreeState(
        branch=branch,
        head=head,
        origin_head=origin_head,
        dirty=dirty,
    )


def _live_claude_argv(session_id: str) -> list[str]:
    """The live ``claude`` process argv for one exact session.

    Read from ``ps`` at trust-gate time so the gate compares what is
    actually running against the anchored launch template. The classic
    command grammar guarantees metacharacter-free, space-free tokens,
    so a whitespace split reconstructs the argv exactly. Zero or two
    matching processes both return an empty argv — the gate then fails
    closed on the template comparison rather than guessing.
    """

    try:
        listing = subprocess.run(
            ["ps", "-axo", "args="],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return []
    matches = [
        line.strip()
        for line in listing.splitlines()
        if session_id in line
        and "claude" in line
        and (" --resume " in line or " --session-id " in line)
        and "ps -axo" not in line
    ]
    if len(matches) != 1:
        return []
    return matches[0].split()


def _intake_ack(args: argparse.Namespace) -> int:
    """Record the lead's consumption of one exact offer."""

    from hermes_orchestrator.lead_intake import LeadIntakePoll

    database = Database.open(_intake_state_dir(args) / "state.db")
    try:
        accepted = LeadIntakePoll(database=database).acknowledge(
            session_id=args.session,
            packet_id=args.packet,
            offer_token=args.offer,
        )
    finally:
        database.close()
    if not accepted:
        print(
            "the offer is unknown, superseded, or already delivered; "
            "nothing changed",
            file=sys.stderr,
        )
        return 1
    print(json.dumps({"acknowledged": args.packet}))
    return 0


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


class _ReadOnlyDashboardDatabase:
    """Exactly the query surface DashboardSources consumes, read-only.

    Sol L3: one long-lived ``mode=ro`` SQLite connection backs BOTH
    the schema-compatibility probe and every dashboard read — there is
    no second open, so no schema transition can slip between probe and
    reads, and nothing on this path can write: no WAL pragma, no
    migrations, no mkdir, no transactions. ``execute`` is the single
    method DashboardSources calls (SELECT + fetchall); nothing else is
    exposed.
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def execute(
        self, sql: str, parameters: tuple[object, ...] = ()
    ) -> sqlite3.Cursor:
        return self._connection.execute(sql, parameters)


def _dashboard(args: argparse.Namespace, settings: Settings) -> int:
    """Run the lock-free read-only dashboard loop (the upper pane).

    Sol K1/L3: this entry is deliberately NOT the daemon — it takes no
    daemon lock and cannot collide with the one lock-holding
    supervisor — and it is genuinely read-only: a single ``mode=ro``
    SQLite connection (never ``Database.open``: no migrations, no WAL
    pragma, no directory creation) serves the schema-generation check
    and all reads for the process lifetime, so it works even when the
    filesystem forbids writes, and a mismatched checkout refuses
    before anything renders. It ticks the existing dashboard
    sources/renderer/pane machinery unchanged.
    """

    from hermes_orchestrator import db as db_module
    from hermes_orchestrator.profiles import ProfileRegistry

    state_db = settings.state_dir / "state.db"
    if not state_db.exists():
        _print(
            {"error": "state database does not exist"},
            json_output=args.json,
            human=f"no state database at {state_db}.",
        )
        return 1
    migrations_dir = Path(db_module.__file__).with_name("migrations")
    runtime_generation = max(
        (
            int(found.name.split("_", maxsplit=1)[0])
            for found in migrations_dir.glob("[0-9][0-9][0-9][0-9]_*.sql")
        ),
        default=0,
    )
    probe_sql = (
        "SELECT coalesce(max(version), 0) FROM schema_migrations"
    )
    # Sol M2: ordinary COORDINATED mode=ro access only. SQLite's
    # immutable=1 skips locking and change detection entirely, so
    # under the concurrently writing daemon it can serve stale or
    # corrupt reads — a write-forbidding view of this filesystem does
    # not prove the daemon is not writing the same file through
    # another mount or view. When the coordinated open or probe fails
    # (for a WAL database that usually means the reader cannot map a
    # writable -shm index), the dashboard refuses outright: there is
    # no unlocked fallback of any kind. The one surviving connection
    # serves the schema probe AND every read: no probe-to-open window.
    connection: sqlite3.Connection | None = None
    applied: int | None = None
    try:
        connection = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    except sqlite3.Error:
        connection = None
    if connection is not None:
        connection.row_factory = sqlite3.Row
        try:
            applied = int(connection.execute(probe_sql).fetchone()[0])
        except sqlite3.Error:
            connection.close()
            connection = None
    if connection is None or applied is None:
        _print(
            {"error": "read-only probe failed"},
            json_output=args.json,
            human=(
                "coordinated read-only access to the state database "
                "failed; refusing — a daemon may be writing it and no "
                "unlocked fallback exists."
            ),
        )
        return 1
    try:
        if applied != runtime_generation:
            _print(
                {
                    "error": "schema generation mismatch",
                    "runtime_generation": runtime_generation,
                    "database_schema": applied,
                },
                json_output=args.json,
                human=(
                    "refusing: this checkout's migration generation "
                    f"({runtime_generation}) does not match the database "
                    f"schema ({applied}); the dashboard never migrates."
                ),
            )
            return 1
        profiles_path = settings.repo_root / "config" / "profiles.yaml"
        if not profiles_path.exists():
            _print(
                {"error": "config/profiles.yaml is required"},
                json_output=args.json,
                human="the dashboard requires config/profiles.yaml.",
            )
            return 1
        action = DashboardRefreshAction(
            database=_ReadOnlyDashboardDatabase(connection),
            registry=ProfileRegistry.load(profiles_path),
            pane=FramePane(),
            frame=True,
            width=lambda: shutil.get_terminal_size().columns,
            height=lambda: shutil.get_terminal_size().lines,
            color=sys.stdout.isatty(),
            detail=args.detail,
        )

        async def _loop() -> int:
            await action.tick()
            if args.once:
                return 1
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for signum in (signal.SIGINT, signal.SIGTERM):
                with suppress(NotImplementedError, ValueError):
                    loop.add_signal_handler(signum, stop.set)
            ticks = 1
            while not stop.is_set():
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop.wait(), timeout=args.interval
                    )
                    break
                await action.tick()
                ticks += 1
            return ticks

        ticks = asyncio.run(_loop())
    finally:
        connection.close()
    _print(
        {"ticks": ticks, "read_only": True, "daemon_lock": False},
        json_output=args.json,
        human=f"dashboard stopped after {ticks} tick(s).",
    )
    return 0


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
        # The console is activation-bound like the daemon: it confirms
        # the complete runtime identity against the active ledger row
        # (journaling console.started, the apply protocol's health
        # signal) and refuses to serve from an unactivated or
        # mismatched artifact.
        from hermes_orchestrator.activation import RuntimeActivator

        activator = RuntimeActivator(database, events=EventStore(database))
        confirmed = activator.confirm_startup(
            checkout_root=_code_checkout_root(),
            binary_path=Path(sys.argv[0]).resolve(),
            pid=os.getpid(),
            service="console",
            bootstrap=False,
        )
        if confirmed is None and activator.current() is not None:
            print(
                "serve-console refuses: this process's runtime identity "
                "does not match the active activation",
                file=sys.stderr,
            )
            return 1
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
    if args.command == "intake-poll":
        return _intake_poll(args)
    if args.command == "intake-ack":
        return _intake_ack(args)
    if args.command == "child-start":
        return _child_event(args, completed=False)
    if args.command == "child-stop":
        return _child_event(args, completed=True)
    if args.command == "runtime-exec":
        return _runtime_exec(args)
    settings = load_settings(args.repo_root, args.state_dir)
    if args.command == "dashboard":
        return _dashboard(args, settings)
    if args.command == "serve-console":
        return _serve_console(args, settings)
    if args.command == "hooks-install":
        return _hooks_install(args, settings)
    if args.command == "runtime-activate":
        return _runtime_activate(args, settings)
    if args.command == "verify":
        return _verify(args, settings)
    if args.command == "verify-check":
        return _verify_check(args, settings)
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

        if args.command == "intake-signal":
            if settings.cmux is None or runtime.cmux_bindings is None:
                _print(
                    {"error": "cmux is not configured"},
                    json_output=args.json,
                    human="cmux is not configured (config/cmux.yaml).",
                )
                return 1
            from hermes_orchestrator.lead_intake import (
                CORRECTION_READY,
                WORK_READY,
                IntakeRefused,
                ManualIntakeSignal,
            )

            signal = ManualIntakeSignal(
                database=database,
                bindings=runtime.cmux_bindings,
                port=CmuxCliAdapter(
                    settings.cmux.cli,
                    base_env=os.environ,
                    password_source=cmux_password_source(Keychain()),
                ),
            )
            try:
                envelope = asyncio.run(
                    signal.send(
                        kind=(
                            CORRECTION_READY
                            if args.kind == "correction"
                            else WORK_READY
                        ),
                        packet_id=args.packet,
                        cell_id=args.cell,
                        session_id=args.session,
                    )
                )
            except (IntakeRefused, CmuxError) as error:
                _print(
                    {"error": str(error)},
                    json_output=args.json,
                    human=f"signal refused: {error}",
                )
                return 1
            _print(
                {"signalled": envelope},
                json_output=args.json,
                human=f"Signalled: {envelope}",
            )
            return 0

        if args.command == "channel-trust-capture":
            from hermes_orchestrator.channel_trust import (
                ChannelTrustAnchors,
                TrustRefused,
            )

            try:
                evidence = json.loads(args.evidence.read_bytes())
            except (OSError, ValueError) as error:
                _print(
                    {"error": str(error)},
                    json_output=args.json,
                    human=f"evidence unreadable: {error}",
                )
                return 1
            anchors = ChannelTrustAnchors(database, events=EventStore(database))
            entry_path = Path(str(evidence["canonical_entry_path"]))
            try:
                anchor = anchors.capture(
                    cell_id=str(evidence["cell_id"]),
                    profile_alias=str(evidence["profile_alias"]),
                    entry_path=entry_path,
                    package_root=entry_path.parents[2],
                    channel_entry=str(evidence["channel_entry"]),
                    launch_argv_template=list(
                        evidence["launch_argv_template"]
                    ),
                    workspace_uuid=str(evidence["workspace_uuid"]),
                    surface_uuid=str(evidence["surface_uuid"]),
                    session_id=str(evidence["session_id"]),
                    prompt_pattern=evidence.get("prompt_pattern"),
                )
            except (TrustRefused, KeyError, OSError) as error:
                _print(
                    {"error": str(error)},
                    json_output=args.json,
                    human=f"capture refused: {error}",
                )
                return 1
            # The evidence file records what the manual trust event
            # measured; a re-measurement that disagrees means the build
            # drifted between the trust event and this capture, so the
            # anchor is retired again immediately and the capture
            # refuses rather than blessing the drifted build.
            drift = [
                name
                for name, recorded, measured in (
                    ("entry_sha256", evidence.get("entry_sha256"), anchor.entry_sha256),
                    (
                        "dist_tree_sha256",
                        evidence.get("dist_tree_sha256"),
                        anchor.dist_tree_sha256,
                    ),
                    (
                        "entry_owner_uid",
                        evidence.get("entry_owner_uid"),
                        anchor.entry_owner_uid,
                    ),
                )
                if recorded is not None and recorded != measured
            ]
            if drift:
                anchors.retire(anchor.anchor_id)
                _print(
                    {"error": "evidence drift", "fields": drift},
                    json_output=args.json,
                    human=(
                        "capture refused: the build drifted from the "
                        f"recorded trust evidence on {', '.join(drift)}"
                    ),
                )
                return 1
            _print(
                {
                    "anchor_id": anchor.anchor_id,
                    "prompt_evidence": (
                        "bound" if anchor.prompt_pattern else "pending"
                    ),
                },
                json_output=args.json,
                human=(
                    f"Trust anchor {anchor.anchor_id} captured; prompt "
                    "evidence "
                    + ("bound." if anchor.prompt_pattern else "pending.")
                ),
            )
            return 0

        if args.command == "channel-trust-confirm":
            if settings.cmux is None or runtime.cmux_bindings is None:
                _print(
                    {"error": "cmux is not configured"},
                    json_output=args.json,
                    human="cmux is not configured (config/cmux.yaml).",
                )
                return 1
            if runtime.control_operations is None:
                _print(
                    {"error": "control operations are unavailable"},
                    json_output=args.json,
                    human="control operations are unavailable.",
                )
                return 1
            from hermes_orchestrator.channel_trust import (
                ChannelTrustAnchors,
                ChannelTrustGate,
                TrustRefused,
                displayed_channel_entries_valid,
            )
            from hermes_orchestrator.runtime import resolve_sidecar_entry

            binding = runtime.cmux_bindings.active_lead(args.cell)
            if binding is None:
                _print(
                    {"error": "no active lead binding for this cell"},
                    json_output=args.json,
                    human="no active lead binding for this cell.",
                )
                return 1
            events = EventStore(database)
            anchors = ChannelTrustAnchors(database, events=events)
            anchor = anchors.active_for_cell(args.cell)
            if anchor is None:
                _print(
                    {"error": "no active trust anchor for this cell"},
                    json_output=args.json,
                    human="no active trust anchor for this cell.",
                )
                return 1
            port = CmuxCliAdapter(
                settings.cmux.cli,
                base_env=os.environ,
                password_source=cmux_password_source(Keychain()),
            )
            # Bounded watch: the dialog either appears within the
            # window or this invocation exits without any durable
            # trust receipt — a watcher timeout is an absent dialog,
            # not a trust refusal.
            deadline = time.monotonic() + max(1, args.wait_seconds)
            screen = ""
            dialog_visible = False
            while time.monotonic() < deadline:
                try:
                    screen = asyncio.run(
                        port.read_screen(binding.ref, lines=200)
                    )
                except CmuxError:
                    screen = ""
                if (
                    "Loading development channels" in screen
                    and "server:hermes-control" in screen
                ):
                    dialog_visible = True
                    break
                time.sleep(2)
            if not dialog_visible:
                _print(
                    {"error": "dialog not visible within the wait window"},
                    json_output=args.json,
                    human=(
                        "the hermes-control confirmation dialog did not "
                        "appear within the wait window; nothing recorded."
                    ),
                )
                return 1
            entry_path = resolve_sidecar_entry(
                repo_root=settings.repo_root,
                state_dir=settings.state_dir,
            )
            if args.capture_prompt and (
                anchor.prompt_pattern is None
                or anchor.canonical_entry_path != str(entry_path)
            ):
                # The one bounded recapture: this launch's manual Enter
                # IS a full manual trust event (v5.1), so the anchor is
                # (re)bound to the exact LIVE identity showing the
                # dialog — never merely patched onto a stale build. The
                # operator-recorded normalized structure must be present
                # or this refuses; no key is ever pressed here.
                markers = (
                    "Loading development channels",
                    "server:hermes-control",
                    "I am using this for local development",
                    "Enter to confirm",
                )
                missing = [m for m in markers if m not in screen]
                if missing or not displayed_channel_entries_valid(screen):
                    _print(
                        {
                            "error": "live dialog does not match the "
                            "operator-recorded normalized structure",
                            "missing": missing,
                        },
                        json_output=args.json,
                        human=(
                            "prompt capture refused: the live dialog "
                            "does not match the operator-recorded "
                            "normalized structure."
                        ),
                    )
                    return 1
                pattern = r"[\s\S]{0,4000}?".join(
                    re.escape(marker) for marker in markers
                )
                live_argv = _live_claude_argv(str(binding.session_id))
                if not live_argv:
                    _print(
                        {"error": "no unique live claude process argv"},
                        json_output=args.json,
                        human=(
                            "prompt capture refused: no unique live "
                            "claude process for this session."
                        ),
                    )
                    return 1
                try:
                    if anchor.canonical_entry_path != str(entry_path):
                        anchors.retire(anchor.anchor_id)
                        anchor = anchors.capture(
                            cell_id=args.cell,
                            profile_alias=str(binding.profile_alias),
                            entry_path=entry_path,
                            package_root=entry_path.parents[2],
                            channel_entry=CHANNEL_ENTRY,
                            launch_argv_template=live_argv,
                            workspace_uuid=binding.ref.workspace_uuid,
                            surface_uuid=binding.ref.surface_uuid,
                            session_id=str(binding.session_id),
                            prompt_pattern=pattern,
                        )
                    else:
                        anchors.complete_prompt(anchor.anchor_id, pattern)
                except TrustRefused as error:
                    _print(
                        {"error": str(error)},
                        json_output=args.json,
                        human=f"trust (re)binding refused: {error}",
                    )
                    return 1
                _print(
                    {
                        "anchor_id": anchor.anchor_id,
                        "prompt_evidence": "bound",
                        "manual_press_required": True,
                    },
                    json_output=args.json,
                    human=(
                        "Trust anchor bound to the live launch "
                        "identity with prompt evidence; this launch "
                        "still requires the one manual Enter. The "
                        "next launch auto-confirms."
                    ),
                )
                return 0
            gate = ChannelTrustGate(
                database,
                events=events,
                anchors=anchors,
                control=runtime.control_operations,
                read_screen=lambda: screen,
                confirm=lambda: asyncio.run(
                    port.confirm_channel_dialog(binding.ref)
                ),
            )
            verdict = gate.evaluate(
                cell_id=args.cell,
                session_id=str(binding.session_id),
                workspace_uuid=binding.ref.workspace_uuid,
                surface_uuid=binding.ref.surface_uuid,
                profile_alias=str(binding.profile_alias),
                entry_path=entry_path,
                package_root=entry_path.parents[2],
                launch_argv=_live_claude_argv(str(binding.session_id)),
                screen_text=screen,
            )
            payload = {
                "confirmed": verdict.confirmed,
                "receipt_operation_id": verdict.receipt_operation_id,
            }
            if not verdict.confirmed:
                payload["first_failure"] = verdict.first_failure
            _print(
                payload,
                json_output=args.json,
                human=(
                    "Channel confirmed automatically; receipt "
                    f"{verdict.receipt_operation_id}."
                    if verdict.confirmed
                    else "CHANNEL CONFIRMATION REQUIRED "
                    f"({verdict.first_failure}); receipt "
                    f"{verdict.receipt_operation_id}."
                ),
            )
            return 0 if verdict.confirmed else 1

        if args.command == "rotate-lead":
            if settings.cmux is None or runtime.cmux_bindings is None:
                _print(
                    {"error": "cmux is not configured"},
                    json_output=args.json,
                    human="cmux is not configured (config/cmux.yaml).",
                )
                return 1
            from hermes_orchestrator.lead_rotation import LeadRotation

            binding = runtime.cmux_bindings.active_lead(args.cell)
            if binding is not None and binding.project_key is not None:
                project_key = binding.project_key
            else:
                # A lost or never-registered classic seat must not
                # refuse to resume an already-acknowledged durable
                # transition (Sol a06cbce0): fall back to the durable
                # cell row LeadRotation itself reads from, and let it
                # reseat the exact named replacement.
                from hermes_orchestrator.lead_rotation import (
                    live_cell_project_key,
                )

                project_key = live_cell_project_key(database, args.cell)
                if project_key is None:
                    _print(
                        {"error": "no active lead binding for this cell"},
                        json_output=args.json,
                        human="no active lead binding for this cell.",
                    )
                    return 1
            project = settings.projects.get(project_key)
            if project is None:
                _print(
                    {"error": "the cell's project is not configured"},
                    json_output=args.json,
                    human="the cell's project is not configured.",
                )
                return 1
            try:
                cells, seater = _open_rotation_collaborators(settings, runtime)
            except (OSError, ValueError) as error:
                _print(
                    {"error": str(error)},
                    json_output=args.json,
                    human=f"{error}.",
                )
                return 1
            # Leads and candidates work in a dedicated worktree when
            # the project configures one; ``repo_path`` itself must
            # stay the stable primary checkout (see the linked-worktree
            # rejection in ``load_settings``), so the rotation probe
            # validates the lead's actual candidate tree instead. Sol
            # correction c5600e31: derived from the same canonical
            # ``project.lead_cwd`` the seater/cell composition and
            # bootstrap trust in ``_open_rotation_collaborators`` use.
            lead_worktree = project.lead_cwd
            rotation = LeadRotation(
                database=database,
                handoffs=HandoffService(database),
                cells=cells,
                bindings=runtime.cmux_bindings,
                seater=seater,
                # The gate calls this with the project key, not the
                # cell id; the exact worktree path is already resolved
                # above, so the key itself is accepted but unused.
                worktree_state=lambda _project_key: _worktree_state(lead_worktree),
                registration_wait_seconds=ROTATION_REGISTRATION_WAIT_SECONDS,
            )
            report = asyncio.run(rotation.rotate(args.cell))
            payload = dataclasses.asdict(report)
            if report.failure:
                _print(
                    payload,
                    json_output=args.json,
                    human=f"lead rotation refused: {report.failure}",
                )
                return 1
            _print(
                payload,
                json_output=args.json,
                human=(
                    f"Lead rotated for cell {report.cell_id}: session "
                    f"{report.replacement_session} seated on "
                    f"{report.profile} (binding {report.binding_id})."
                ),
            )
            return 0

        if args.command == "decision-import":
            try:
                raw = args.receipt.read_bytes()
            except OSError as error:
                _print(
                    {"error": str(error)},
                    json_output=args.json,
                    human=f"receipt unreadable: {error}",
                )
                return 1
            digest = hashlib.sha256(raw).hexdigest()
            if digest != args.sha256.lower():
                _print(
                    {"error": "sha256 mismatch", "computed": digest},
                    json_output=args.json,
                    human=(
                        "refused: receipt SHA-256 does not match the "
                        "operator-provided digest"
                    ),
                )
                return 1
            try:
                receipt = json.loads(raw)
                decision = OperatorDecisions(database).import_receipt(
                    receipt, receipt_sha256=digest
                )
            except (json.JSONDecodeError, DecisionRefused) as error:
                _print(
                    {"error": str(error)},
                    json_output=args.json,
                    human=f"import refused: {error}",
                )
                return 1
            _print(
                {
                    "decision_id": decision.decision_id,
                    "issue_id": decision.issue_id,
                    "choice": decision.choice,
                    "status": decision.status,
                    "receipt_sha256": decision.receipt_sha256,
                },
                json_output=args.json,
                human=(
                    f"Decision {decision.decision_id} for "
                    f"{decision.issue_id}: {decision.status}."
                ),
            )
            return 0

        if args.command == "orchestrator-workspace":
            from hermes_orchestrator.orchestrator_workspace import (
                SEAT_ENV,
                OrchestratorWorkspaceLifecycle,
                WorkspaceRefused,
                evidence_document,
                file_sha256,
                run_smoke,
            )

            inside_marked_pane = bool(os.environ.get(SEAT_ENV))
            cmux_cli = (
                [str(args.cmux_cli)]
                if args.cmux_cli is not None
                else (settings.cmux.cli if settings.cmux else None)
            )
            if cmux_cli is None or runtime.cmux_bindings is None:
                _print(
                    {"error": "cmux is not configured"},
                    json_output=args.json,
                    human=(
                        "cmux is not configured "
                        "(config/cmux.yaml or --cmux-cli)."
                    ),
                )
                return 1
            # Sol L2: ONE exclusive ownership fence for every
            # workspace-mutating entry. The manual CLI takes the same
            # non-blocking flock the production daemon holds under this
            # state directory: refused means the daemon (or another
            # manual mutator) owns the lifecycle, and this entry exits
            # with zero binding/cmux effects. A smoke pointed at its
            # own isolated --state-dir takes that directory's own
            # fence, so temp-state smokes stay permitted.
            try:
                fence = acquire_workspace_fence(settings.state_dir)
            except DaemonAlreadyRunning:
                _print(
                    {
                        "error": "workspace lifecycle fence is held",
                        "state_dir": str(settings.state_dir),
                    },
                    json_output=args.json,
                    human=(
                        "refusing: the production daemon (or another "
                        "mutator) owns the workspace lifecycle for "
                        f"{settings.state_dir}; stop it or use an "
                        "isolated --state-dir."
                    ),
                )
                return 1
            try:
                port = CmuxCliAdapter(
                    cmux_cli,
                    base_env=os.environ,
                    password_source=cmux_password_source(Keychain()),
                )
                try:
                    lifecycle = OrchestratorWorkspaceLifecycle(
                        port=port,
                        bindings=runtime.cmux_bindings,
                        repo_root=settings.repo_root,
                        state_dir=settings.state_dir,
                        name=args.name,
                        title=args.title,
                        interval=args.interval,
                        session_name=args.session,
                        inside_marked_pane=inside_marked_pane,
                    )
                    if args.action == "ensure":
                        state = asyncio.run(lifecycle.ensure())
                        payload = state.payload()
                        human = (
                            f"Orchestrator workspace {state.outcome}: "
                            f"{state.workspace_uuid}"
                        )
                        _print(payload, json_output=args.json, human=human)
                        return 0
                    # The smoke drives the production ownership topology:
                    # the same owner start/tick reconciliation the daemon
                    # runs — never a standalone lifecycle path (Sol K1).
                    owner = OrchestratorWorkspaceOwner(lifecycle)
                    evidence = asyncio.run(
                        run_smoke(
                            owner,
                            settle_seconds=args.settle_seconds,
                        )
                    )
                except (WorkspaceRefused, CmuxError) as error:
                    _print(
                        {"error": f"{type(error).__name__}: {error}"},
                        json_output=args.json,
                        human=f"orchestrator workspace failed: {error}",
                    )
                    return 1
                if args.evidence_file is not None:
                    # Digest-sealed receipt of this exact run: source sha,
                    # exact invocation, complete evidence. Revalidate with
                    # verify_evidence_document().
                    document = evidence_document(
                        evidence,
                        source_sha=_git_head_sha(settings.repo_root),
                        invocation=[
                            "hermes-orchestrator",
                            *(
                                list(arguments)
                                if arguments is not None
                                else sys.argv[1:]
                            ),
                        ],
                        captured_at=datetime.now(UTC).isoformat(),
                    )
                    evidence_path = args.evidence_file.expanduser()
                    evidence_path.parent.mkdir(parents=True, exist_ok=True)
                    evidence_path.write_text(
                        json.dumps(document, indent=1, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                    evidence = dict(evidence)
                    evidence["evidence_file"] = str(evidence_path)
                    # Both digests, distinguished (Sol K3): the canonical
                    # payload seal inside the receipt, and the byte hash
                    # of the written file (which cannot contain itself).
                    evidence["self_digest"] = document["self_digest"]
                    evidence["self_digest_kind"] = document["self_digest_kind"]
                    evidence["file_sha256"] = file_sha256(evidence_path)
                    evidence["source_sha"] = document["source_sha"]
                _print(
                    evidence,
                    json_output=args.json,
                    human=json.dumps(evidence, indent=2),
                )
                return 0 if evidence["passed"] else 1
            finally:
                fence.release()

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
            packets = SubagentPackets(database, events=events)
            result = HermesCommandService(
                queue,
                qa=QaRouter(database=database, events=events),
                handlers=_hermes_handlers(settings, runtime, outbox, packets),
            ).execute(request)
            print(json.dumps(result.as_dict(), sort_keys=True, separators=(",", ":")))
            rejected = {"invalid_command", "intent_not_allowed"}
            if settings.cmux is not None and result.code not in rejected:
                # Best-effort: any command that just committed a
                # durable packet reaches a registered channel now
                # instead of at the daemon's next repair tick.
                nudge_channel(hub_socket_path(settings.state_dir))
            return 0 if result.code not in rejected else 1

        if args.command == "daemon":
            # The running identity is a durable record, not terminal
            # lore. The daemon never self-activates over an existing
            # activation (that would silently undo a rollback): it
            # bootstraps one only when none exists, confirms a match,
            # and journals a daemon.started event either way — the
            # exact health signal the apply protocol verifies.
            activation = None
            with suppress(Exception):
                from hermes_orchestrator.activation import RuntimeActivator

                activation = RuntimeActivator(
                    runtime.database,
                    events=EventStore(runtime.database),
                ).confirm_startup(
                    checkout_root=_code_checkout_root(),
                    binary_path=Path(sys.argv[0]).resolve(),
                    pid=os.getpid(),
                )
            supervisor = asyncio.run(
                _run_daemon(
                    service,
                    once=args.once,
                    interval=args.interval,
                    activation=activation,
                    dispatch=runtime.dispatch,
                    merge_flow=runtime.merge_flow,
                    projects=tuple(settings.projects),
                    database=runtime.database,
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
                    channel_hub=runtime.channel_hub,
                    control_operations=runtime.control_operations,
                    dashboard_refresh=runtime.dashboard_refresh,
                    orchestrator_workspace=runtime.orchestrator_workspace,
                    fakechat_router=runtime.fakechat_router,
                    post_merge=runtime.post_merge,
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
            pending = OperatorDecisions(runtime.database).pending_for_issue(
                args.issue_id
            )
            if pending:
                print(
                    f"awaiting operator decision {pending[0].decision_id}; "
                    "candidate publication is blocked until the operator "
                    "resolves it",
                    file=sys.stderr,
                )
                return 1
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

        if args.command == "reviewer-fix":
            from hermes_orchestrator.git import SubprocessGitRunner
            from hermes_orchestrator.reviewer_fix import (
                ReviewerFixHelper,
                ReviewerFixRefused,
            )

            helper = ReviewerFixHelper(
                database=database,
                events=EventStore(database),
                projects=settings.projects,
                git=SubprocessGitRunner(),
                manifest_root=settings.state_dir / "manifests",
            )
            try:
                receipt = helper.apply(
                    args.project,
                    args.issue,
                    submitted_sha=args.submitted,
                    message=args.message,
                    verification=tuple(args.verify),
                )
            except ReviewerFixRefused as error:
                _print(
                    {"error": str(error)},
                    json_output=args.json,
                    human=f"reviewer fix refused: {error}",
                )
                return 1
            _print(
                {
                    "fix_id": receipt.fix_id,
                    "submitted_sha": receipt.submitted_sha,
                    "final_sha": receipt.final_sha,
                    "event_id": receipt.event_id,
                    "files": list(receipt.files),
                    "changed_lines": receipt.changed_lines,
                },
                json_output=args.json,
                human=(
                    f"Reviewer fix {receipt.final_sha[:12]} atop "
                    f"{receipt.submitted_sha[:12]} recorded "
                    f"({receipt.changed_lines} lines)."
                ),
            )
            return 0

        if args.command == "merge-settle":
            flow = _open_merge_flow(settings, runtime)
            try:
                if args.review is not None:
                    outcomes = (
                        asyncio.run(flow.reviews.merge_approved(args.review)),
                    )
                else:
                    outcomes = asyncio.run(
                        flow.reviews.resume_settlements(args.project)
                    )
            except Exception as error:
                _print(
                    {"error": str(error)},
                    json_output=args.json,
                    human=f"settlement refused: {error}",
                )
                return 1
            payload = [
                {
                    "review_id": outcome.review_id,
                    "issue_id": outcome.issue_id,
                    "state": outcome.state,
                    "merge_sha": outcome.merge_sha,
                    "reason": outcome.reason,
                }
                for outcome in outcomes
            ]
            _print(
                payload,
                json_output=args.json,
                human=(
                    f"{len(payload)} settlement(s): "
                    + ", ".join(
                        f"{item['review_id']}={item['state']}"
                        for item in payload
                    )
                    if payload
                    else "no resumable settlements"
                ),
            )
            failed = [
                item for item in payload if item["state"] != "merged"
            ]
            return 1 if args.review is not None and failed else 0

        if args.command == "merge-reconcile":
            from hermes_orchestrator.manifests import read_manifest_snapshot

            flow = _open_merge_flow(settings, runtime)
            manifest_path = flow.manifest_root / f"{args.event}.json"
            try:
                snapshot = read_manifest_snapshot(
                    manifest_path, root=flow.manifest_root
                )
                outcome = asyncio.run(
                    flow.reviews.reconcile_external_merge(
                        project_key=args.project,
                        issue_id=args.issue,
                        manifest=snapshot.manifest,
                        pr_number=args.pr,
                    )
                )
            except Exception as error:
                _print(
                    {"error": str(error)},
                    json_output=args.json,
                    human=f"reconciliation refused: {error}",
                )
                return 1
            _print(
                {
                    "review_id": outcome.review_id,
                    "issue_id": outcome.issue_id,
                    "state": outcome.state,
                    "merge_sha": outcome.merge_sha,
                    "reason": outcome.reason,
                },
                json_output=args.json,
                human=(
                    f"Reconciled {outcome.issue_id}: {outcome.state} "
                    f"({outcome.reason})."
                ),
            )
            return 0 if outcome.state == "merged" else 1

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
            if settings.cmux is not None:
                # A settled Merger turn may have journalled correction
                # packets; route them to a registered channel now.
                nudge_channel(hub_socket_path(settings.state_dir))
            return 0

        if args.command == "submit-review":
            if re.fullmatch(r"[0-9a-f]{40}", args.candidate_sha) is None:
                print(
                    "--candidate-sha must be the full 40-hex candidate SHA",
                    file=sys.stderr,
                )
                return 1
            if args.verdict == "-":
                verdict_json = sys.stdin.read()
            else:
                try:
                    verdict_json = Path(args.verdict).read_text(encoding="utf-8")
                except OSError as error:
                    print(
                        f"cannot read verdict document: {error}", file=sys.stderr
                    )
                    return 1
            if not verdict_json.strip():
                print("verdict document is empty", file=sys.stderr)
                return 1
            # Strictly local: the submitted document is settled from
            # durable state — the thread is never pulled, so the App
            # Server is not started, watched, or polled.
            flow = _open_merge_flow(settings, runtime)
            try:
                outcome = asyncio.run(
                    flow.turns.submit_review(
                        args.project,
                        issue_id=args.issue,
                        event_id=args.event,
                        candidate_sha=args.candidate_sha,
                        reviewed_thread_id=args.thread,
                        reviewed_generation=args.generation,
                        verdict_json=verdict_json,
                    )
                )
            except SubmissionRejected as error:
                print(str(error), file=sys.stderr)
                return 1
            _print_merger_turn(outcome)
            if settings.cmux is not None:
                # A settled submission may have journalled correction
                # packets; route them to a registered channel now.
                nudge_channel(hub_socket_path(settings.state_dir))
            return 0

        if args.command == "status":
            payload = {
                "mode": settings.policy.mode,
                "admission_open": False,
                "queue_count": len(queue.list_ranked(datetime.now(UTC))),
                "project_count": len(settings.projects),
                "schema_version": database.schema_version(),
                # INFRA-186: a lead silently doing all implementation is
                # observable — packet counts, tiers, child lifecycle,
                # and the Fable-token share.
                "delegation": runtime.service.status()["delegation"],
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
