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
from typing import Any, cast
from uuid import UUID

from hermes_orchestrator import migration_env as migration_env_module
from hermes_orchestrator.acceptance import AcceptanceGates
from hermes_orchestrator.cells import (
    DEVELOPMENT_LANE,
    HARNESS_LANE,
    DevSeatRecovery,
    ProfileCapacityEvidence,
    ProjectCellService,
    admission_priority_ceiling,
    bind_admitted_issue_worktree,
    development_lane_saturated,
    ensure_harness_checkout,
)
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
    configured_model_tiers,
    control_launch_failure_recorder,
    tier_default_effort,
)
from hermes_orchestrator.cmux import CmuxCliAdapter, CmuxError
from hermes_orchestrator.cmux_surfaces import (
    CHANNEL_ENTRY,
    ChannelTrustConfirmer,
    CmuxDeadLeadSweep,
    CmuxHibernationDriver,
    CmuxLeadSeater,
    CmuxSurfaceReconciler,
    RegistryProfileDirectory,
    managed_claude_worker_alive,
)
from hermes_orchestrator.codex_queue import CANDIDATE_QUEUED
from hermes_orchestrator.config import ProjectConfig, Settings, load_settings
from hermes_orchestrator.context import ContextMonitor
from hermes_orchestrator.control_operations import ControlOperations
from hermes_orchestrator.dashboard_pane import FramePane
from hermes_orchestrator.dashboard_refresh import DashboardRefreshAction
from hermes_orchestrator.db import Database
from hermes_orchestrator.deploy import lifecycle
from hermes_orchestrator.deploy.launchd import standard_inventory
from hermes_orchestrator.domain import AdmissionRequest, IssueState, QueuedIssue
from hermes_orchestrator.emission import (
    EmissionBlocked,
    FreezeFacts,
    ResolvedLane,
    resolve_lane,
    run_freeze_gate,
)
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.fakechat_router import FakechatWakeRouter
from hermes_orchestrator.git import GitError, GitRunner, WorktreeGit
from hermes_orchestrator.handoffs import (
    DerivedFacts,
    HandoffRejected,
    HandoffService,
    LiveCell,
    derived_handoff_document,
    validate_handoff_identity,
)
from hermes_orchestrator.hermes_tools import HermesCommandService
from hermes_orchestrator.issue_targeting import (
    IssueTargetingRefused,
    acknowledge_target,
    target_harness_followup,
    target_issue,
)
from hermes_orchestrator.keychain import Keychain, KeychainWriteError
from hermes_orchestrator.lead_assignments import LeadAssignments
from hermes_orchestrator.lead_intake import LeadIntakeRouter
from hermes_orchestrator.lead_outbox import LeadCorrectionOutbox
from hermes_orchestrator.lead_wakes import (
    CommandWakeTransport,
    LeadTerminalWakes,
    LeadWakeDelivery,
    LeadWakeReconciler,
    TerminalWakeInput,
)
from hermes_orchestrator.linear import (
    LinearIssueReader,
    LinearProjection,
    ProjectLinearRouter,
)
from hermes_orchestrator.merge_flow import (
    DeferredCollaborator,
    MergeFlow,
    build_merge_flow,
)
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
from hermes_orchestrator.remote.serve import (
    _retry_handler,
    build_console_dependencies,
    run_console,
)
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
from hermes_orchestrator.worktrees import WorktreeLeases


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

    target_issue_cmd = commands.add_parser(
        "target-issue",
        help="bind a named existing session to one admitted issue",
    )
    target_issue_cmd.add_argument("issue_id")
    target_issue_cmd.add_argument("--project", required=True)
    target_issue_cmd.add_argument("--cell", required=True)
    target_issue_cmd.add_argument("--session", required=True)
    target_issue_cmd.add_argument("--instruction", required=True)
    # INFRA-215: ``harness`` attaches ONE explicitly admitted follow-up to
    # a harness-bound lead lane-preservingly (same cell/session/profile,
    # no second seat) through ``target_harness_followup``.
    target_issue_cmd.add_argument(
        "--lane", choices=("development", "harness"), default="development"
    )
    target_issue_cmd.add_argument("--json", action="store_true")

    target_issue_ack_cmd = commands.add_parser(
        "target-issue-ack",
        help=(
            "consume the exact-session ACK for a published target-issue "
            "packet; only this advances the issue and journals the "
            "In Development projection (INFRA-220, Sol correction "
            "7ecf4a57)"
        ),
    )
    target_issue_ack_cmd.add_argument("assignment_id")
    target_issue_ack_cmd.add_argument("--session", required=True)
    target_issue_ack_cmd.add_argument("--json", action="store_true")

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
    reconcile.add_argument(
        "--bind-issue-lane",
        default=None,
        help=(
            "<project>:<issue> — bind one already-active issue's "
            "dedicated worktree lane"
        ),
    )
    reconcile.add_argument(
        "--bind-issue-branch",
        default=None,
        help="explicit validated branch for --bind-issue-lane",
    )
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

    # INFRA-203: the deterministic entry point over the SAME publication.
    # The issue is the whole input: the lane, the checkout, the exact
    # HEAD, the branch, the base and the changed files are all derived
    # from durable state, so there is no verification flag, manifest
    # path, evidence document, or attribution to accept. The mechanical
    # fields ``_candidate_ready`` reads are supplied as defaults, which
    # is what keeps this one entry point rather than a second emitter.
    candidate_submit = commands.add_parser(
        "candidate",
        help=(
            "submit the candidate bound to an issue: freezes that issue's "
            "own leased worktree at its exact HEAD, with no caller-supplied "
            "identity or evidence"
        ),
    )
    candidate_submit.add_argument("action", choices=("submit",))
    candidate_submit.add_argument("issue_id")
    candidate_submit.add_argument("--json", action="store_true")
    candidate_submit.set_defaults(
        verified=[CANDIDATE_SUBMIT_VERIFICATION],
        blocker=[],
        status="FABLE_READY",
    )

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
    merge_reconcile.add_argument("--submitted-sha", default=None)
    merge_reconcile.add_argument("--final-integration-sha", default=None)
    merge_reconcile.add_argument("--merge-sha", default=None)
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
            "(Claude Code SubagentStop hook; also the lead's own "
            "--child <agent_id> settlement for a child it stopped "
            "on purpose, whose SubagentStop never fires)"
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

    claude_launch = commands.add_parser(
        "claude-launch",
        help=(
            "INFRA-192: launch Claude on one named provider route "
            "(first-party Max by default, bedrock only by explicit route) "
            "with the child environment sanitized independent of cwd; "
            "probes auth status first and refuses a route whose live "
            "provider disagrees with its kind"
        ),
    )
    claude_launch.add_argument("--route", required=True)
    claude_launch.add_argument("claude_args", nargs=argparse.REMAINDER)

    provider_probe = commands.add_parser(
        "provider-probe",
        help=(
            "INFRA-192: run the live non-inference auth/status probe for "
            "every configured route (or --route) and report identity-free "
            "provider facts; exits 1 when any route fails closed"
        ),
    )
    provider_probe.add_argument("--route", default=None)
    provider_probe.add_argument("--json", action="store_true")
    provider_probe.add_argument(
        "--fable-capacity",
        action="store_true",
        help=(
            "run one bounded Fable inference on exactly one Max route and "
            "atomically persist its available/capped result"
        ),
    )

    commands.add_parser(
        "claude-shell-init",
        help=(
            "INFRA-192: print the zsh/bash functions (claude, claude-max-*, "
            "claude-bedrock) that forward every launch to claude-launch "
            "through the stable launcher; contains no secrets"
        ),
    )

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

    start_lane = commands.add_parser(
        "start-lane",
        help=(
            "start or recover a project's visible lead cell in one lane "
            "(development or harness) through Hermes' existing cell "
            "creation/launch path -- idempotent: a live cell in that "
            "lane is adopted and reported, never duplicated "
            "(INFRA-219 L2)"
        ),
    )
    start_lane.add_argument("--project", required=True)
    start_lane.add_argument(
        "--lane", choices=("development", "harness"), default="development"
    )
    start_lane.add_argument(
        "--issue",
        default=None,
        help=(
            "explicit issue to launch a NEW cell against; omit to only "
            "recover an already-running cell in this lane"
        ),
    )
    start_lane.add_argument(
        "--harness-run",
        default=None,
        help=(
            "explicit harness-run identity; REQUIRED with --lane harness "
            "(and refused with --lane development) -- mirrors "
            "ProjectCellService.dispatch's own harness_run contract "
            "(Sol correction 110ed759 / INFRA-219 L3, R1)"
        ),
    )
    start_lane.add_argument("--json", action="store_true")

    submit_handoff = commands.add_parser(
        "submit-handoff",
        help=(
            "submit the incumbent lead's fresh durable handoff for a "
            "cell: every mechanical field (identity, git position, "
            "issue state, environment) is derived from durable rows "
            "and the worktree probe; the caller provides only "
            "decisions, caveats, risks, and the exact next action. A "
            "lead rotation awaiting this handoff resumes automatically "
            "on the durable submission (INFRA-198, Sol 52d15493)"
        ),
    )
    submit_handoff.add_argument("--cell", required=True)
    submit_handoff.add_argument(
        "--decision",
        action="append",
        required=True,
        dest="decisions",
        help="one decision made so far (repeatable; at least one)",
    )
    submit_handoff.add_argument(
        "--caveat",
        action="append",
        default=[],
        dest="caveats",
        help="one caveat/blocker the replacement must know (repeatable)",
    )
    submit_handoff.add_argument(
        "--risk",
        action="append",
        default=[],
        dest="risks",
        help="one open risk (repeatable)",
    )
    submit_handoff.add_argument(
        "--next-action",
        required=True,
        dest="next_action",
        help="the exact next action the replacement should take",
    )
    submit_handoff.add_argument("--json", action="store_true")

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
    wakes: LeadTerminalWakes | None = None,
    resource_freshness_minutes: int = 5,
    cmux_reconciler: CmuxSurfaceReconciler | None = None,
    cmux_dead_lead_sweep: CmuxDeadLeadSweep | None = None,
    cmux_hibernation: CmuxHibernationDriver | None = None,
    dev_seat_recovery: DevSeatRecovery | None = None,
    lead_intake: LeadIntakeRouter | None = None,
    channel_hub: ChannelHub | None = None,
    control_operations: ControlOperations | None = None,
    fakechat_router: FakechatWakeRouter | None = None,
    activation: object | None = None,
    dashboard_refresh: DashboardRefreshAction | None = None,
    orchestrator_workspace: OrchestratorWorkspaceOwner | None = None,
    post_merge: PostMergeAdvance | None = None,
    lead_rotation_recovery: Callable[[], Awaitable[object]] | None = None,
) -> Supervisor:
    # INFRA-198 P1: bound to the merge flow's lifetime once merge_flow is
    # created below (only in the non-``once`` path); the maintenance
    # closure reads this by reference at tick time, never at definition
    # time, so it always observes the current session.
    session: MergerSession | None = None

    async def _maintenance() -> None:
        if lead_rotation_recovery is not None:
            # Rotation must finish outside the incumbent's process: closing
            # its cmux workspace also terminates any submit-handoff command
            # still running there.  The daemon owns the durable continuation.
            with suppress(Exception):
                await lead_rotation_recovery()
        if cmux_dead_lead_sweep is not None:
            # INFRA-198: the cmux reconciler runs once at startup, so a
            # lead that died mid-run stayed active until the next daemon
            # restart and blocked its replacement. This re-probes on
            # every tick and retires ONLY an authoritatively absent
            # seat: an unreachable socket retires nothing.
            await cmux_dead_lead_sweep.tick()
        if dev_seat_recovery is not None:
            # INFRA-198: the dead-lead sweep above can retire a
            # development lane's only cell, but nothing else in the
            # daemon re-dispatches that lane while the project still
            # has non-done admitted work -- only a human running
            # start-lane could. This closes that gap through the
            # identical dispatch lifecycle, bounded to one recovery
            # dispatch per project per tick; it never raises.
            await dev_seat_recovery.tick(projects)
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
        if wakes is not None:
            # INFRA-215: every other wake kind fires because a turn
            # ENDED, so an idle seat was never told safe admitted work
            # still remained. This is the live boundary that
            # re-evaluates that condition; the runnable-set turn key
            # makes an unchanged condition re-commit nothing, so the
            # per-tick cadence costs one read and no wake.
            for project_key in projects:
                with suppress(Exception):
                    wakes.commit_work_ready(
                        project_key,
                        freshness_minutes=resource_freshness_minutes,
                    )

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
                cmux_dead_lead_sweep is None
                and dev_seat_recovery is None
                and cmux_hibernation is None
                and lead_intake is None
                and channel_hub is None
                and dashboard_refresh is None
                and orchestrator_workspace is None
                and merge_flow is None
                and post_merge is None
                and wakes is None
                and lead_rotation_recovery is None
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
    # INFRA-212: the Linear router validates ``config/linear.yaml`` and
    # reads its Keychain token; both are deferred to the first
    # projection through the same transparent proxy ``build_merge_flow``
    # uses for GitHub and CircleCI, so a strictly local command (a
    # ``corrections_required`` submit-review from Sol's own workspace)
    # composes this graph without touching a credential at all. The
    # router built on first use is byte-for-byte the one this call
    # always built, and any composition failure still raises there.
    linear = cast(
        ProjectLinearRouter,
        DeferredCollaborator(
            lambda: build_linear_router(
                settings,
                database=runtime.database,
                queue=runtime.queue,
                keychain=keychain,
            ),
            surface=("project", "validate"),
        ),
    )
    events = EventStore(runtime.database)
    return build_merge_flow(
        settings,
        database=runtime.database,
        events=events,
        queue=runtime.queue,
        linear=linear,
        keychain=keychain,
        base_env=os.environ,
        processes=runtime.processes,
        # INFRA-198 J1: a CLI-driven settlement publishes the same
        # durable acceptance assignment the daemon would — the packet
        # store is the shared database, not the process.
        assignments=LeadAssignments(runtime.database, events=events),
    )


def _open_acceptance_linear(settings: Any, runtime: Runtime) -> ProjectLinearRouter:
    """Compose only the Linear projector ``satisfy_acceptance`` needs.

    INFRA-198 J2: satisfying an acceptance gate still must project the
    completed issue through Linear's exact idempotent effect path — the
    same :class:`~hermes_orchestrator.linear.ExternalEffectStore`-backed
    ``project()`` every merge settlement already uses, so a replayed
    ``satisfy_acceptance`` call produces no duplicate Linear effect.
    Unlike ``_open_merge_flow`` it never opens the rest of the merge
    graph (GitHub, CircleCI, the Codex RPC client): those collaborators
    would cost extra Keychain reads and process wiring this single
    status projection never touches, so composing the full merge flow
    here would be disproportionate. This builds a fresh router from the
    same validated per-project routing config and Keychain token the
    daemon uses; a fresh object is safe every call because the effect's
    idempotency lives in the durable ``external_effects`` table, keyed
    by ``effect_id``, not in this object's identity.
    """

    return build_linear_router(
        settings,
        database=runtime.database,
        queue=runtime.queue,
        keychain=Keychain(),
    )


def _open_idle_linear_router(
    settings: Settings, *, database: Database, queue: Any
) -> Any:
    """Compose the live Linear router the idle dispatcher projects
    through — Keychain + settings, exactly the way ``_open_merge_flow``
    composes it for the merge flow (INFRA-199 Finding 1).

    Raises on any composition failure (missing ``config/linear.yaml``,
    an unreadable Keychain credential, an unregistered project). Sol
    ec0ed7fe gap 1: the Stop-hook idle path never calls this before its
    local activation commits — ``_LazyIdleLinearProjector`` defers the
    composition into the post-commit projection attempt, where
    ``cells._project_in_development`` swallows a raise here like any
    other Linear failure. Router composition can therefore never block
    local work, and never bypasses Linear either: the pending
    ``external_effects`` row journaled with the activation commit is
    the durable trace reconciliation resolves.
    """

    return build_linear_router(
        settings, database=database, queue=queue, keychain=Keychain()
    )


class _LazyIdleLinearProjector:
    """Compose the Keychain/config-backed Linear router only at
    projection time — strictly AFTER the local activation transaction
    committed (Sol ec0ed7fe gap 1).

    Satisfies ``cells.LinearProjector`` without touching the Keychain,
    ``config/linear.yaml``, or the network at construction time, so an
    eligible local activation commits without ever constructing the
    live Linear client. The first (and only) composition happens inside
    ``cells._project_in_development``'s suppressed projection attempt.
    """

    def __init__(
        self, settings: Settings, *, database: Database, queue: Any
    ) -> None:
        self._settings = settings
        self._database = database
        self._queue = queue

    async def project(
        self, issue_id: str, target: Any, effect_id: str
    ) -> Any:
        router = _open_idle_linear_router(
            self._settings, database=self._database, queue=self._queue
        )
        return await router.project(issue_id, target, effect_id)


class _LazyLinearReads:
    """Read one Linear issue only when a command actually needs one.

    INFRA-227: ``hermes-command`` runs with ``enable_live=False`` (only
    ``daemon`` sets it), so ``open_runtime`` never builds a
    ``LinearIssueReader`` for this path -- ``Runtime`` has nothing to
    reuse. Building one eagerly here would touch the Keychain on every
    ``hermes-command`` invocation, including the vast majority (status,
    pause, resume, ...) that never reach reactivation. The read --
    Keychain included -- is deferred to the first ``get_issue`` call, so
    a Keychain outage only ever surfaces to a command that actually
    needs Linear, and ``HermesCommandService`` turns any exception raised
    here into an ``admission_denied`` result rather than a traceback.
    """

    def __init__(self, keychain: Keychain) -> None:
        self._keychain = keychain
        self._reader: LinearIssueReader | None = None

    def get_issue(self, issue_id: str) -> Any:
        if self._reader is None:
            token = self._keychain.read("hermes-orchestrator-linear", "default")
            self._reader = LinearIssueReader(token)
        return self._reader.get_issue(issue_id)


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


def _harness_lead_cwd(lead_cwd: Path) -> Path:
    """The harness lane's dedicated worktree for a project's ``lead_cwd``.

    INFRA-219 L2: a sibling checkout, never the development lead's own
    directory -- ``start-lane --lane harness`` never launches or
    resumes a lead in the same worktree a development lead uses, so
    the two lanes' sessions can never collide on working-tree state.
    The path is a deterministic convention (no new config surface).

    INFRA-219 R7 / Sol correction 110ed759: L2 left provisioning the
    actual worktree here out of scope -- this naming convention alone
    never created or validated the checkout. ``_open_rotation_collaborators``
    now runs :func:`hermes_orchestrator.cells.ensure_harness_checkout`
    against exactly this path before a harness lane ever launches into
    it.
    """

    return lead_cwd.parent / f"{lead_cwd.name}-harness"


def _open_rotation_collaborators(
    settings: Settings,
    runtime: Runtime,
    *,
    lane_role: str = DEVELOPMENT_LANE,
    harness_project_key: str | None = None,
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

    INFRA-219 R7 / Sol correction 110ed759: when ``lane_role`` is
    ``HARNESS_LANE`` and ``harness_project_key`` names the project being
    started, the dedicated harness checkout
    (:func:`_harness_lead_cwd`) is VALIDATED (or PROVISIONED, if absent)
    against the project's real repository through
    :func:`ensure_harness_checkout` -- the worktree machinery
    ``WorktreeGit`` already provides, never a bespoke subprocess call --
    before the seater/cell service are ever composed to launch into it.
    :class:`hermes_orchestrator.cells.HarnessCheckoutRefused` is a
    ``ValueError`` subclass, so a mismatched or foreign checkout flows
    through this function's existing ``ValueError`` refusal path with no
    new exception handling here. ``harness_project_key`` is ``None`` for
    every pre-R7 caller (development rotation, and the direct unit tests
    that call this function without a project) so this check runs only
    when a caller opts a specific project into it.
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
        alias: (
            _harness_lead_cwd(project.lead_cwd)
            if lane_role == HARNESS_LANE
            else project.lead_cwd
        )
        for alias, project in settings.projects.items()
    }
    if lane_role == HARNESS_LANE and harness_project_key is not None:
        harness_project = settings.projects[harness_project_key]
        ensure_harness_checkout(
            WorktreeGit(),
            repo_path=harness_project.repo_path,
            harness_path=_harness_lead_cwd(harness_project.lead_cwd),
        )
    prompt_name = (
        "claude-harness.md"
        if lane_role == HARNESS_LANE
        else "claude-lead.md"
    )
    from hermes_orchestrator.runtime import resolve_prompt_file

    prompt_file = resolve_prompt_file(
        prompt_name, repo_root=settings.repo_root, state_dir=settings.state_dir
    )
    if not prompt_file.is_file():
        raise FileNotFoundError(
            f"the {lane_role} lead prompt is missing at {prompt_file}; "
            "refusing to launch a lead with an unresolvable prompt asset"
        )
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
        prompt_files={lane_role: prompt_file},
        profile_dirs=RegistryProfileDirectory(registry),
        auth_probe=lambda alias: probe.check(alias).eligible,
        channel_launch=channel_launcher,
        control=runtime.control_operations,
        channel_trust=channel_confirmer,
    )
    if runtime.cells is not None:
        return runtime.cells, seater
    # INFRA-219 R7 / Sol correction 110ed759: an operational-only prompt,
    # distinct from the development lead's ``claude-lead.md`` -- the
    # harness lane owns restart/rotation/wake-confirm/recovery/dedup/
    # acceptance testing only and never selects or implements product
    # issues (see ``prompts/claude-harness.md``).
    runner = ClaudeRunner(
        registry,
        prompt_file=prompt_file,
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
        # INFRA-214 (observed live 2026-09-01): this one-shot composition
        # omitted classic-seat mode, so ``_activate_seat`` passed
        # ``classic_command=None`` -- Hermes created an EMPTY cmux
        # workspace and separately launched a hidden ``claude -p``
        # shadow process instead of the visible channel-enabled classic
        # session. The daemon's own composition (runtime.py) has always
        # set this; ``start-lane``/``rotate-lead`` must agree with it, so
        # the same predicate is used here rather than a second rule.
        classic_seats=(
            settings.cmux is not None
            and settings.cmux.classic_leads
            and seater is not None
        ),
        # INFRA-214: with classic seats enabled,
        # ``_activate_issue_transaction`` publishes the lead's durable
        # assignment through ``self._assignments`` -- omitting it would
        # let the visible harness seat launch correctly and then sit
        # IDLE with no assignment or channel wake, which is the same
        # silent-idle class of failure this issue exists to remove.
        assignments=LeadAssignments(database, events=events),
    )
    return cells, seater


#: DispatchResult statuses that mean "a lead is genuinely running (or
#: just ran a turn) in this lane" -- the only outcomes ``start-lane``
#: may report as success. This is an explicit ALLOW-list, not a deny
#: -list, by deliberate Sol correction 110ed759 / INFRA-219 R1 design:
#: the prior code (``return 0 if result.status not in (...) else 1``)
#: was a deny-list that treated every *unrecognized* status -- including
#: the harness-run refusals L3 later added -- as success. An allow-list
#: cannot repeat that inversion: a new refusal status ``dispatch`` grows
#: in the future is nonzero by construction, with zero further changes
#: here. The two members reflect the two ways ``_dispatch_locked`` ends
#: with a lead actually seated/run: ``"seated"`` (the classic-seats path
#: activates the visible pane lead) and ``"working"`` (the streaming
#: path completed a turn with the session durably confirmed). Every
#: other status -- ``already_completed``, ``handoff_required``,
#: ``seat_failed``, ``start_reconciliation_required``,
#: ``start_unconfirmed``, ``project_busy``, ``awaiting_operator_decision``,
#: ``harness_run_required``, ``harness_run_not_permitted``, and
#: ``waiting_for_profile``/``start_failed`` (the two the old deny-list
#: already caught) -- is a refusal or a non-start terminal state, and
#: exits nonzero here.
_LANE_DISPATCH_SUCCESS_STATUSES = frozenset({"seated", "working"})


def _start_lane(args: Any, settings: Settings, runtime: Runtime) -> int:
    """``start-lane``: launch or recover one project's lead cell in one
    lane through Hermes' existing cell creation/launch path.

    INFRA-219 L2. Reuses :func:`_open_rotation_collaborators` --
    exactly the collaborators (profile pool, ``ClaudeRunner``,
    ``CmuxLeadSeater``) ``rotate-lead`` already composes -- so this
    command is never a bespoke launcher, only a thin wire onto
    :meth:`ProjectCellService.dispatch`. Idempotent: an already-live
    cell in the requested lane is adopted and reported (L1's
    ``(project_key, lane_role)`` uniqueness refuses a duplicate row
    before this command would ever attempt one), never a traceback --
    but only when that cell's worker is not provably dead (INFRA-198;
    see the ``existing is not None`` branch).

    Sol correction 110ed759 / INFRA-219 R1: L3 made ``harness_run`` a
    MANDATORY ``ProjectCellService.dispatch`` argument for a harness
    dispatch (``HARNESS_LANE`` without it refuses
    ``harness_run_required``; a development dispatch that supplies one
    refuses ``harness_run_not_permitted``), but L2 wrote this command
    before L3 existed and never passed it -- so every harness start
    refused, and the old exit line mapped only
    ``waiting_for_profile``/``start_failed`` to nonzero, so the refusal
    silently exited 0. This command now (1) carries an explicit
    ``--harness-run`` argument and mirrors ``dispatch``'s own
    lane/harness_run validation at the CLI boundary -- fail-closed
    before ever calling ``dispatch``, so the dispatch-level refusal is
    never the only guard -- and (2) maps exit codes through the
    :data:`_LANE_DISPATCH_SUCCESS_STATUSES` ALLOW-list instead of a
    deny-list, so any refusal status, present or future, exits nonzero.
    """

    if settings.cmux is None or runtime.cmux_bindings is None:
        _print(
            {"error": "cmux is not configured"},
            json_output=args.json,
            human="cmux is not configured (config/cmux.yaml).",
        )
        return 1
    if settings.projects.get(args.project) is None:
        _print(
            {"error": "unknown project"},
            json_output=args.json,
            human=f"unknown project {args.project!r}.",
        )
        return 1
    lane_role = HARNESS_LANE if args.lane == "harness" else DEVELOPMENT_LANE
    try:
        cells, _seater = _open_rotation_collaborators(
            settings,
            runtime,
            lane_role=lane_role,
            # INFRA-219 R7 / Sol correction 110ed759: only a harness
            # start ever needs its dedicated checkout validated or
            # provisioned; a development start passes no project here
            # and ``_open_rotation_collaborators`` skips the check.
            harness_project_key=args.project if lane_role == HARNESS_LANE else None,
        )
    except (OSError, ValueError, GitError) as error:
        _print({"error": str(error)}, json_output=args.json, human=f"{error}.")
        return 1

    existing = cells.active_cell(args.project, lane_role)
    if existing is not None:
        # INFRA-198 (observed live): an active cell row is NOT proof of a
        # running worker. Workspace C39EBCDC / surface F5B1BD55 still
        # existed while ``claude --resume 9b539c86`` had exited with "No
        # conversation found", so ``start-lane --lane harness`` reported
        # ``already_running`` and exited 0 for a corpse -- the operator's
        # only signal that the lane was down. The worker is measured
        # here, tri-state: ``True`` (running) and ``None`` (unknown --
        # ``ps`` failed or was ambiguous, therefore treated as alive) are
        # both adopted as before; only a DEFINITIVE absence retires.
        if managed_claude_worker_alive(str(existing.session_id)) is False:
            # INFRA-219 (observed live): deferring to "the daemon's
            # dead-lead sweep" left the lane stuck forever -- the sweep
            # only iterates ACTIVE lead bindings, and seat-restore
            # correctly refuses to reactivate a dead worker, so a
            # ``stale`` binding on this exact cell is invisible to it.
            # Retire the cell HERE, through the sweep's own cell-scoped
            # retirement, composed exactly as the daemon composes
            # ``CmuxDeadLeadSweep`` (runtime.py:841). Only a DEFINITIVE
            # False result (a concurrent transition won the CAS) keeps
            # today's refusal.
            environment = dict(os.environ)
            assert settings.cmux is not None
            dead_lead_sweep = CmuxDeadLeadSweep(
                bindings=runtime.cmux_bindings,
                port=CmuxCliAdapter(
                    settings.cmux.cli,
                    base_env=environment,
                    password_source=cmux_password_source(Keychain()),
                ),
                database=runtime.database,
                events=EventStore(runtime.database),
                control=runtime.control_operations,
                leases=runtime.worktree_leases,
                # The in-memory profile pool this one-shot command would
                # use is local to ``_open_rotation_collaborators`` and
                # not exposed on ``Runtime`` -- the durable retirement
                # (cell state, profile-lease row, receipt) does not
                # depend on it; only the daemon's cached pool affinity
                # would, and this process has none to release.
                profiles=None,
            )
            if not dead_lead_sweep.retire_dead_lead_cell(existing.cell_id):
                _print(
                    {
                        "error": "dead_worker",
                        "lane": lane_role,
                        "cell_id": existing.cell_id,
                        "session_id": str(existing.session_id),
                    },
                    json_output=args.json,
                    human=(
                        f"{lane_role} lane cell {existing.cell_id} for "
                        f"{args.project} is still marked active but its "
                        f"worker (session {existing.session_id}) is gone: "
                        "no managed claude process is running for it. "
                        "Nothing was started. Retiring it lost a "
                        "concurrent transition; re-run start-lane."
                    ),
                )
                return 1
            _print(
                {
                    "status": "dead_worker_retired",
                    "lane": lane_role,
                    "cell_id": existing.cell_id,
                    "session_id": str(existing.session_id),
                },
                json_output=args.json,
                human=(
                    f"{lane_role} lane cell {existing.cell_id} for "
                    f"{args.project} was marked active but its worker "
                    f"(session {existing.session_id}) was gone; retired it "
                    "and seating the replacement now."
                ),
            )
            # Fall through to the normal seating path below -- the cell
            # is retired, so this invocation seats the replacement
            # itself rather than requiring a second start-lane call.
        else:
            _print(
                {
                    "status": "already_running",
                    "lane": lane_role,
                    "cell_id": existing.cell_id,
                    "session_id": str(existing.session_id),
                },
                json_output=args.json,
                human=(
                    f"{lane_role} lane already running for {args.project}: "
                    f"cell {existing.cell_id} (session {existing.session_id}) "
                    "-- adopted, not duplicated."
                ),
            )
            return 0

    if not args.issue:
        _print(
            {"error": "no live cell in this lane and no --issue to start one"},
            json_output=args.json,
            human=(
                f"no live {lane_role} lane cell for {args.project}; pass "
                "--issue to start one."
            ),
        )
        return 1

    # Sol correction 110ed759 / INFRA-219 R1: mirror ProjectCellService
    # .dispatch's own lane/harness_run validation HERE, at the CLI
    # boundary, before ever calling dispatch -- fail-closed with a
    # clear message and a nonzero exit, rather than letting the
    # dispatch-level refusal (``harness_run_required`` /
    # ``harness_run_not_permitted``) be the only guard against a
    # harness start that never launches anything.
    if lane_role == HARNESS_LANE and not args.harness_run:
        _print(
            {"error": "harness_run_required"},
            json_output=args.json,
            human=(
                "--lane harness requires --harness-run: the harness lane "
                "requires an explicit harness-run request bound to "
                "lane_role='harness' (Sol correction 110ed759 / "
                "INFRA-219 L3)."
            ),
        )
        return 1
    if lane_role != HARNESS_LANE and args.harness_run:
        _print(
            {"error": "harness_run_not_permitted"},
            json_output=args.json,
            human=(
                "--lane development refuses --harness-run: development "
                "occupancy is proven by explicit issue admission alone "
                "(Sol correction 110ed759 / INFRA-219 L3)."
            ),
        )
        return 1

    result = asyncio.run(
        cells.dispatch(args.issue, lane_role=lane_role, harness_run=args.harness_run)
    )
    payload = dataclasses.asdict(result)
    # INFRA-198 blocker 2: ``DispatchResult.session_id`` is a ``UUID |
    # None`` and ``dataclasses.asdict`` preserves the ``UUID`` object,
    # which ``json.dumps`` cannot encode. Every effect of ``dispatch``
    # has ALREADY landed by the time we get here -- the cell exists and
    # the seat is launched -- so a reporting-side ``TypeError`` turns a
    # succeeded command into a traceback and a nonzero exit, and the
    # natural operator response (run it again) risks a duplicate lane.
    # Coerce at the payload boundary so the documented contract (a JSON
    # object carrying ``session_id``) actually holds; ``None`` stays
    # null rather than becoming the string "None".
    payload["session_id"] = (
        None if result.session_id is None else str(result.session_id)
    )
    _print(
        payload,
        json_output=args.json,
        human=(
            f"{lane_role} lane for {args.project}: {result.status} "
            f"(cell {result.cell_id}, session {result.session_id})."
        ),
    )
    return 0 if result.status in _LANE_DISPATCH_SUCCESS_STATUSES else 1


# INFRA-203: the single verification entry ``candidate submit`` records.
# The manifest is an internal transport artifact that Fable neither
# authors nor audits, so the submitting command supplies this fixed,
# evidence-free entry instead of accepting one from the caller. It is
# written in the same ``COMMAND=RESULT`` form ``--verified`` takes so
# both entry points parse it through the one code path below.
CANDIDATE_SUBMIT_VERIFICATION = (
    "hermes-orchestrator candidate submit=deterministic submission of the "
    "bound lane's exact HEAD"
)


def _pending_operator_decision(database: Database, issue_id: str) -> str | None:
    """The outstanding operator decision blocking publication, if any.

    Shared by both publication entry points so ``candidate submit``
    cannot become a way around a hold ``candidate-ready`` honours.
    """

    pending = OperatorDecisions(database).pending_for_issue(issue_id)
    return pending[0].decision_id if pending else None


def _candidate_submit_lane(database: Database, issue_id: str) -> ResolvedLane:
    """The lane ``candidate submit <issue>`` publishes, from durable state.

    INFRA-203: the issue is the whole input, so even the project is read
    from the durable binding rather than supplied. The one live
    ``worktree_leases`` row for the issue is resolved by
    :func:`resolve_lane` — the codebase's fail-closed "this issue's ONE
    live lease" reader, the same one ``candidate-ready`` and
    :func:`_seat_lane` already publish and hand off through. Nothing
    here consults ``Path.cwd()`` or ``project.lead_cwd``: the caller's
    checkout is never candidate identity.
    """

    leases = WorktreeLeases(database, events=EventStore(database))
    projects = sorted(
        {lease.project_key for lease in leases.active() if lease.issue_id == issue_id}
    )
    if not projects:
        raise EmissionBlocked(
            f"candidate submission refused: no live worktree lease binds "
            f"issue {issue_id!r}, so there is no bound tree to freeze; bind "
            f"the lane (reconcile --bind-issue-lane PROJECT:{issue_id}) and "
            "submit again"
        )
    if len(projects) > 1:
        raise EmissionBlocked(
            f"candidate submission refused: issue {issue_id!r} holds live "
            f"worktree leases in {len(projects)} projects "
            f"({', '.join(projects)}), so its lane is not unambiguous; leave "
            "exactly one and submit again"
        )
    return resolve_lane(leases, projects[0], issue_id)


def _candidate_submit_gate(
    git: GitRunner, project: ProjectConfig, lane: ResolvedLane
) -> FreezeFacts:
    """Prove the bound lane's HEAD is publishable, before anything is written.

    INFRA-203: exactly two refusals belong to this command — a dirty
    bound worktree (intended work would be silently omitted from the
    candidate) and a HEAD that is not pushed and reachable on the
    remote (nothing downstream could fetch the candidate). Both are
    decided by :func:`run_freeze_gate`, the one mandatory candidate gate
    the emitter itself runs, measured in the LANE's worktree; running it
    here just moves the same decision ahead of the merge flow, so a
    refusal writes no manifest and delivers no wake.
    """

    def run(*args: str) -> str:
        try:
            result = git.run(("git", *args), lane.path)
        except GitError as error:
            raise EmissionBlocked(str(error)) from error
        if result.returncode != 0:
            raise EmissionBlocked(
                f"git {args[0]} failed with exit code {result.returncode}"
            )
        return result.stdout

    try:
        return run_freeze_gate(run, project, expect_pushed=True)
    except EmissionBlocked as blocked:
        raise EmissionBlocked(_candidate_submit_refusal(blocked, lane)) from None


def _candidate_submit_refusal(blocked: EmissionBlocked, lane: ResolvedLane) -> str:
    """One concise, actionable line for each of the two gates."""

    reason = str(blocked)
    if "not clean" in reason:
        return (
            f"candidate submission refused: the worktree bound to "
            f"{lane.issue_id} at {lane.path} has uncommitted changes, so the "
            "candidate would omit them; commit or discard them there, push, "
            "and submit again"
        )
    if "pushed branch head" in reason or "fetch failed" in reason:
        return (
            f"candidate submission refused: the HEAD of the worktree bound "
            f"to {lane.issue_id} at {lane.path} is not the head of "
            f"origin/{lane.branch}; push that branch and submit again"
        )
    return f"candidate submission refused: {reason}"


async def _candidate_ready(
    flow: MergeFlow, args: Any, resolved_lane: ResolvedLane
) -> dict[str, Any]:
    verification: list[tuple[str, str]] = []
    for entry in args.verified:
        command, separator, result = entry.partition("=")
        if not separator or not command.strip() or not result.strip():
            raise ValueError("--verified expects COMMAND=RESULT")
        verification.append((command.strip(), result.strip()))
    # The lane IS the publication target: ``resolve_lane`` returned it
    # for this exact project and issue, so reading both from the lane
    # keeps one emission path for a caller that names the project
    # (``candidate-ready``) and one that derives it (``candidate
    # submit``).
    project_key = resolved_lane.project_key
    if not await _start_merge_flow(flow, [project_key]):
        raise RuntimeError("codex app-server unavailable")
    try:
        emitted = await flow.emitter.emit(
            project_key,
            resolved_lane.issue_id,
            verification=tuple(verification),
            blockers=tuple(args.blocker),
            status=args.status,
            # INFRA-219 L5: publication is targeted to the requested
            # issue's OWN live worktree lease -- resolved below, before
            # the flow is even opened -- never whatever checkout the
            # lead currently occupies.
            resolved_lane=resolved_lane,
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

    def fetch_correction(command: Any) -> dict[str, Any]:
        return outbox.fetch(command.correction_id)

    def ack_correction(command: Any) -> dict[str, Any]:
        return outbox.acknowledge(
            command.correction_id,
            observed_count=command.observed_count,
            payload_sha256=command.payload_sha256,
        ).as_dict()

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

    def request_cleanup(command: Any) -> dict[str, Any]:
        custodian = runtime.worktree_custodian
        if custodian is None:
            raise ValueError("worktree cleanup is unavailable")
        rows = runtime.database.execute(
            "SELECT issue_id FROM admitted_issues "
            "WHERE project_key = ? AND state = 'done'",
            (command.project_key,),
        ).fetchall()
        return custodian.reclaim_completed(
            command.project_key, {str(row["issue_id"]) for row in rows}
        )

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

    def require_acceptance(command: Any) -> dict[str, Any]:
        # The gate binds an instruction to a queued issue; an issue that
        # was never admitted has nothing to hold, so this refuses before
        # touching the acceptance table (KeyError -> "rejected", the
        # same idiom every other unknown-issue refusal here uses).
        runtime.queue.get(command.issue_id)
        acceptance = AcceptanceGates(
            runtime.database, events=EventStore(runtime.database)
        )
        gate = acceptance.require(
            command.issue_id,
            instruction_id=command.instruction_id,
            predicates=command.predicates,
        )
        return gate.as_dict()

    def satisfy_acceptance(command: Any) -> dict[str, Any]:
        # Sol 524a38ed finding 1: acceptance completes MERGED work only.
        # The lifecycle checks run BEFORE AcceptanceGates.satisfy so a
        # refused call persists nothing — the gate stays pending and
        # untouched, and no queue or Linear write happens. Both refusals
        # raise ValueError/KeyError, which the command executor maps to
        # the structured "rejected" result. A gate that is ALREADY
        # satisfied is past the lifecycle boundary (the issue is Done or
        # completing): the replay flows through to ``satisfy``'s own
        # byte-identical replay contract instead of being refused.
        acceptance = AcceptanceGates(
            runtime.database, events=EventStore(runtime.database)
        )
        issue = runtime.queue.get(command.issue_id)
        stored = acceptance.get(command.issue_id)
        already_satisfied = stored is not None and stored.state == "satisfied"
        if (
            not already_satisfied
            and issue.state is not IssueState.POST_MERGE_ACCEPTANCE
        ):
            raise ValueError(
                f"issue {command.issue_id} is in state {issue.state.value!r}, "
                "not 'post_merge_acceptance'; acceptance can only complete "
                "an issue whose merge already settled into the acceptance "
                "hold"
            )
        merged = runtime.database.execute(
            "SELECT 1 FROM reviews WHERE issue_id = ? AND state = 'merged' "
            "AND merge_sha IS NOT NULL LIMIT 1",
            (command.issue_id,),
        ).fetchone()
        if merged is None:
            raise ValueError(
                f"issue {command.issue_id} has no proven merged review; "
                "acceptance never completes unmerged work"
            )
        gate = acceptance.satisfy(command.issue_id, evidence=command.evidence)
        runtime.queue.transition(
            command.issue_id,
            IssueState.DONE,
            actor="operator_acceptance",
            reason=f"acceptance satisfied for instruction {gate.instruction_id}",
        )
        linear = _open_acceptance_linear(settings, runtime)
        asyncio.run(
            linear.project(
                command.issue_id,
                LinearProjection(status="Done", assignee_alias="operator"),
                effect_id=(
                    f"linear:{command.issue_id}:acceptance-satisfied:"
                    f"{gate.instruction_id}"
                ),
            )
        )
        return gate.as_dict()

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
            # INFRA-211: the fable tier's configured default, not a literal.
            effort=tier_default_effort("fable"),
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

    def continue_work(command: Any) -> dict[str, Any]:
        # INFRA-198: continue an already-bound seat's already-assigned
        # issue; the caller never names a seat, and this path never
        # assigns, admits, or queues anything. Every refusal below is
        # fail-closed with zero durable writes.
        if runtime.lead_wakes is None:
            raise ValueError("lead terminal wakes are unavailable")
        issue_row = runtime.database.execute(
            "SELECT project_key, priority FROM admitted_issues "
            "WHERE issue_id = ?",
            (command.issue_id,),
        ).fetchone()
        if issue_row is None:
            raise ValueError(
                f"issue {command.issue_id!r} is not admitted; continue_work "
                "never assigns or admits work"
            )
        project_key = str(issue_row["project_key"])
        issue_priority = int(issue_row["priority"])
        cell_rows = runtime.database.execute(
            "SELECT cell_id, session_id, profile_alias FROM project_cells "
            "WHERE project_key = ? AND lane_role = 'development' "
            "AND state = 'active'",
            (project_key,),
        ).fetchall()
        if len(cell_rows) != 1:
            raise ValueError(
                f"project {project_key!r} has {len(cell_rows)} active "
                "development cells; continue_work requires exactly one "
                "unambiguous seat"
            )
        cell = cell_rows[0]
        cell_id = str(cell["cell_id"])
        session_id = str(cell["session_id"])
        profile_alias = str(cell["profile_alias"])
        assigned = runtime.database.execute(
            "SELECT 1 FROM lead_assignments WHERE issue_id = ? "
            "AND session_id = ? AND state != 'superseded' LIMIT 1",
            (command.issue_id, session_id),
        ).fetchone()
        if assigned is None:
            raise ValueError(
                f"issue {command.issue_id!r} is not assigned to the bound "
                "development seat; continue_work never assigns, admits, "
                "or queues work"
            )
        ceiling = admission_priority_ceiling(
            runtime.database,
            now=datetime.now(UTC),
            freshness_minutes=settings.policy.resource_sample_freshness_minutes,
        )
        if ceiling is None:
            raise ValueError(
                "no resource sample fresh enough authorizes continue_work "
                "right now"
            )
        if issue_priority > ceiling:
            raise ValueError(
                f"issue {command.issue_id!r} priority {issue_priority} "
                f"exceeds the resource-authorized ceiling {ceiling}; "
                "continue_work refuses"
            )
        if development_lane_saturated(
            runtime.database, project_key=project_key, issue_id=command.issue_id
        ):
            raise ValueError(
                f"development lane for {project_key!r} is saturated; "
                "continue_work refuses"
            )
        wake = runtime.lead_wakes.commit(
            TerminalWakeInput(
                project_key=project_key,
                issue_id=command.issue_id,
                cell_id=cell_id,
                session_id=UUID(session_id),
                profile_alias=profile_alias,
                turn_key=f"continue-work:{command.issue_id}:{command.request_id}",
                kind="work_ready",
                reason="operator-issued continue_work",
            )
        )
        return wake.as_dict()

    base_retry = _retry_handler(runtime.queue)

    def retry(command: Any) -> dict[str, Any]:
        """Requeue an issue; also redeliver its stalled Merger wake(s).

        Sol correction 538f1b4e: the supported live retry of a wake
        stalled without a structured verdict is this SAME operator
        ``retry`` command, not a new intent or schema -- ``RetryCommand``
        stays ``{intent, issue_id}``. When the named issue has no
        stalled wake this falls straight through to the existing
        requeue handler, byte-for-byte unchanged. Only when it DOES have
        one does this additionally call
        ``MergerTurnService.retry_stalled_wakes_for_issue`` -- the same
        merge-flow composition ``qa_reject``/``require_acceptance`` use
        below -- through the exact production entry point, and reports
        which event_ids were retried and which were actually delivered.

        An issue_id with no ``admitted_issues`` row falls through
        unchanged too (the base handler's own not-found/rejection
        shape), so a nonexistent issue's error is exactly today's.
        """

        try:
            project_key = runtime.queue.get(command.issue_id).project_key
        except KeyError:
            return base_retry(command)
        flow = _open_merge_flow(settings, runtime)
        # INFRA-223 (INFRA-228 blocker): the turn service itself decides
        # whether the issue has a stalled wake OR a legacy delivered wake
        # that ended without a verdict; only when it retried nothing does
        # this fall through to the existing requeue handler unchanged.
        #
        # Generation-131 call-path blocker: the legacy proof reads the
        # exact thread's runtime status through the App Server, so this
        # command opens the SAME bounded connection ``merger-turn`` uses
        # (start, re-verify channels, act, close) around the retry. With
        # no App Server the stalled path still runs and the legacy proof
        # fails closed exactly as before.

        async def _retry_bounded() -> tuple[Any, ...]:
            # Owner path present: the desktop owns the thread and proves
            # eligibility over its IPC socket; no stdio App Server is
            # started here (it could not see, and must never load, the
            # desktop-owned thread).
            owner_path = (
                getattr(getattr(flow, "delivery", None), "_owner_start", None)
                is not None
            )
            started = False if owner_path else await _start_merge_flow(
                flow, [project_key]
            )
            try:
                return tuple(
                    await flow.turns.retry_stalled_wakes_for_issue(
                        project_key, command.issue_id
                    )
                )
            finally:
                if started:
                    await flow.rpc.close()

        results = asyncio.run(_retry_bounded())
        if not results:
            return base_retry(command)
        return {
            "retried": [result.event_id for result in results if result.retried],
            "delivered": [
                result.event_id for result in results if result.delivered
            ],
        }

    return {
        "retry": retry,
        "request_cleanup": request_cleanup,
        "continue_work": continue_work,
        "pending_corrections": pending_corrections,
        "fetch_correction": fetch_correction,
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
        "require_acceptance": require_acceptance,
        "satisfy_acceptance": satisfy_acceptance,
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
        # INFRA-198 blocker 2: ``default=str`` is the backstop for the
        # failure MODE the blocker named -- a value ``json.dumps``
        # cannot natively encode (a ``UUID``, a ``datetime``, a
        # ``Path``) reaching this reporting call AFTER an effectful
        # command has already committed its effects. Rendering it as
        # its string form keeps the command's exit code honest instead
        # of converting a completed operation into a crash. Callers
        # still coerce known identifiers at their own payload boundary
        # so the emitted shape is deliberate, not incidental.
        print(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str))
    else:
        print(human)


_GATE_PACKET_MARKER = re.compile(r"\bpacket:([0-9a-f]{32})\b")


def _requested_tier_default_effort(model: str) -> str:
    """Configured default effort of the tier this launch requests.

    INFRA-211: an Agent launch that names no effort takes its tier's
    configured default rather than an amplifying literal. An unrecognized
    model yields "", so admission refuses on the tier mismatch it already
    checks instead of on an effort nobody asked for.
    """

    for tier in configured_model_tiers().values():
        if tier.model == model:
            return tier.default_effort
    return ""


def subagent_gate(
    freeze_dir: Path,
    payload: str,
    *,
    admission: PacketAdmission | None = None,
    admission_factory: Callable[[], PacketAdmission] | None = None,
) -> tuple[int, str]:
    """PreToolUse hook: block Agent tool use while the session is frozen,
    and — once a packet marker is present — gate the launch through the
    durable packet ledger.

    Exit 2 blocks the tool call in Claude Code and returns the message to
    the lead; exit 0 allows it. Malformed input never blocks silently: it
    fails closed with exit 2 only when a freeze marker cannot be ruled out.
    A launch with no ``packet:<hex>`` marker in its description/name keeps
    freeze-only behavior unchanged, and ``admission_factory`` (built lazily,
    only once the freeze check has already passed and a marker is found —
    so an ordinary unmarked launch never even touches the state database)
    is never invoked for it.

    INFRA-215 (live six-child cap failure): a marked launch that reaches
    this point with no admission service available — ``admission`` is
    ``None`` and either no ``admission_factory`` was given or building one
    raised — is refused (exit 2) rather than let through. Silently
    allowing a marked launch to bypass the ledger is the exact defect
    that let nine planned packets start as eight simultaneous children
    against a limit of six: the packet never left ``planned`` while the
    lead recorded it started. ``admission=None`` is no longer a
    freeze-only opt-out for a marked launch.
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
    if packet_id is None:
        return 0, ""
    if admission is None and admission_factory is not None:
        try:
            admission = admission_factory()
        except Exception as error:  # fail closed: a marked launch must not slip through
            return 2, (
                "subagent gate: packet admission is unavailable "
                f"(packet:{packet_id}); refusing the launch: {error}"
            )
    if admission is None:
        return 2, (
            "subagent gate: packet admission is not wired into this "
            f"process (packet:{packet_id}); refusing the launch"
        )

    tool_use_id = document.get("tool_use_id")
    requested_model = str(tool_input.get("model") or "")
    decision = admission.admit(
        session_id=session_id,
        packet_id=packet_id,
        model=requested_model,
        effort=str(
            tool_input.get("effort")
            or _requested_tier_default_effort(requested_model)
        ),
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


def _provider_routes(settings: Settings) -> Any:
    """Load the closed route set: four Max profiles plus optional Bedrock.

    INFRA-192: both files are local, gitignored selection metadata.
    A missing file is a refusal with a pointer, never a silent
    fallback to whatever the inherited environment would pick.
    """

    from hermes_orchestrator.profiles import ProfileRegistry
    from hermes_orchestrator.provider_routes import ProviderRoutes

    profile_path = settings.repo_root / "config" / "profiles.yaml"
    providers_path = settings.repo_root / "config" / "providers.yaml"
    if not profile_path.exists():
        raise ValueError("config/profiles.yaml is required")
    if not providers_path.exists():
        raise ValueError(
            "config/providers.yaml is required "
            "(start from config/providers.example.yaml)"
        )
    registry = ProfileRegistry.load(profile_path)
    return ProviderRoutes.load(providers_path, registry)


def _claude_launch(args: argparse.Namespace, settings: Settings) -> int:
    from hermes_orchestrator.provider_routes import (
        ProviderProbe,
        launch,
        resolve_executable,
    )

    try:
        routes = _provider_routes(settings)
        executable = resolve_executable(routes)
    except ValueError as error:
        print(f"claude-launch refused: {error}", file=sys.stderr)
        return 2
    arguments = list(args.claude_args)
    if arguments and arguments[0] == "--":
        arguments = arguments[1:]
    base_env = dict(os.environ)
    probe = ProviderProbe(routes, base_env=base_env, executable=executable)
    code, message = launch(
        routes,
        args.route,
        arguments,
        base_env=base_env,
        executable=executable,
        probe=probe,
    )
    if message:
        print(message, file=sys.stderr)
    return code


def _provider_probe(args: argparse.Namespace, settings: Settings) -> int:
    from hermes_orchestrator.provider_routes import (
        FIRST_PARTY_MAX,
        ProviderProbe,
        parse_fable_capacity,
        resolve_executable,
    )

    try:
        routes = _provider_routes(settings)
        executable = resolve_executable(routes)
        names = tuple(routes.names) if args.route is None else (args.route,)
        for name in names:
            routes.route(name)
        if args.fable_capacity and (
            len(names) != 1
            or names[0] == "default"
            or routes.route(names[0]).kind != FIRST_PARTY_MAX
        ):
            raise ValueError(
                "--fable-capacity requires exactly one named Max route"
            )
    except ValueError as error:
        print(f"provider-probe refused: {error}", file=sys.stderr)
        return 2
    probe = ProviderProbe(routes, base_env=dict(os.environ), executable=executable)
    results = [probe.check(name) for name in names]
    ok = all(result.ok for result in results)
    capacity: dict[str, object] | None = None
    if args.fable_capacity and ok:
        observed_at = datetime.now(UTC)
        route_name = names[0]
        environment = routes.launch_env(route_name, dict(os.environ))
        try:
            completed = subprocess.run(
                (
                    executable,
                    "-p",
                    "Reply with exactly CAPACITY_OK and nothing else.",
                    "--model",
                    "fable",
                    "--max-turns",
                    "1",
                    "--output-format",
                    "text",
                    "--no-session-persistence",
                    "--dangerously-skip-permissions",
                ),
                env=environment,
                capture_output=True,
                text=True,
                timeout=120,
            )
            evidence = parse_fable_capacity(
                completed.stdout + completed.stderr, now=observed_at
            )
        except (OSError, subprocess.SubprocessError, ValueError) as error:
            print(f"provider-probe refused: {error}", file=sys.stderr)
            return 2
        database = Database.open(settings.state_dir / "state.db")
        try:
            with database.transaction() as connection:
                connection.execute(
                    "INSERT INTO profile_capacity_observations("
                    "profile_alias, model, state, source, observed_at, "
                    "resets_at, detail) VALUES (?, 'fable', ?, "
                    "'provider_limit', ?, ?, ?)",
                    (
                        route_name,
                        evidence.state,
                        observed_at.isoformat(),
                        (
                            None
                            if evidence.resets_at is None
                            else evidence.resets_at.isoformat()
                        ),
                        evidence.detail,
                    ),
                )
        finally:
            database.close()
        capacity = {
            "route": route_name,
            "state": evidence.state,
            "resets_at": (
                None
                if evidence.resets_at is None
                else evidence.resets_at.isoformat()
            ),
        }
        ok = evidence.state == "available"
    if args.json:
        print(
            json.dumps(
                {
                    "ok": ok,
                    "routes": [result.as_record() for result in results],
                    "metadata": list(routes.metadata()),
                    "capacity": capacity,
                },
                sort_keys=True,
            )
        )
    else:
        for result in results:
            print(
                f"{result.route}\t{result.kind}\t"
                f"{'ok' if result.ok else 'REFUSED'}\t{result.reason}\t"
                f"{result.reported_provider or '-'}"
            )
        if capacity is not None:
            print(
                f"{capacity['route']}\tfable\t{capacity['state']}\t"
                f"{capacity['resets_at'] or '-'}"
            )
    return 0 if ok else 1


def _claude_shell_init(args: argparse.Namespace, settings: Settings) -> int:
    from hermes_orchestrator.provider_routes import shell_init

    try:
        routes = _provider_routes(settings)
    except ValueError as error:
        print(f"claude-shell-init refused: {error}", file=sys.stderr)
        return 2
    launcher = settings.state_dir / "bin" / "hermes-orchestrator"
    if not launcher.exists():
        print(
            "claude-shell-init refused: the stable launcher "
            f"{launcher} does not exist. Deploy the runtime "
            "(`hermes-orchestrator deploy-install`) so the launcher is "
            "rendered, then re-run claude-shell-init; shell functions are "
            "never bound to a mutable checkout.",
            file=sys.stderr,
        )
        return 1
    command = [
        str(launcher),
        "--repo-root",
        str(settings.repo_root),
        "--state-dir",
        str(settings.state_dir),
    ]
    print(shell_init(routes, command), end="")
    return 0


def _hooks_install(args: argparse.Namespace, settings: Settings) -> int:
    """Install the Hermes hooks into every classic profile, idempotently.

    INFRA-204: every hook binds to the STABLE deployed launcher at
    ``<state-dir>/bin/hermes-orchestrator`` — never ``uv run --project
    <checkout>`` and never a commit-specific ``runtimes/<sha>/…``
    path. ``uv run --project`` ran the hooks out of the MUTABLE config
    checkout, which was observed live to lack the command entirely and
    to error the Stop hook into unrelated Claude sessions; a
    ``runtimes/<sha>`` path would pin the settings to one activation
    and go stale on the next. The launcher resolves ``runtimes/ACTIVE``
    itself, so the installed settings survive branch drift and runtime
    replacement without ever being rewritten.

    A missing launcher REFUSES the install, naming the path, and writes
    no profile at all: falling back to ``uv run`` is the exact defect
    this refusal exists to prevent, and a hook bound to a binary that
    is not there is worse than no hook.
    """

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
    launcher = settings.state_dir / "bin" / "hermes-orchestrator"
    if not launcher.exists():
        print(
            "hooks-install refused: the stable launcher "
            f"{launcher} does not exist. Deploy the runtime "
            "(`hermes-orchestrator deploy-install`) so the launcher is "
            "rendered, then re-run hooks-install. No profile was "
            "written; hooks are never bound to `uv run --project` "
            "against a mutable checkout.",
            file=sys.stderr,
        )
        return 1
    registry = ProfileRegistry.load(profile_path)
    base = (
        f"{launcher} --repo-root {settings.repo_root} "
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
            # INFRA-215 (six-child cap): the PreToolUse gate is what
            # reserves a marked packet and enforces the active-child
            # limit at the Agent launch boundary; without it classic
            # seats left every packet ``planned`` while children ran.
            subagent_gate=(
                f"{base} subagent-gate --freeze-dir "
                f"{settings.state_dir / 'freezes'}"
            ),
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


def _canonical_session(
    database: Database, session: str, cwd: str | None
) -> str:
    """The seat's stable session id for ONE whole hook invocation.

    INFRA-198: ``/clear`` hands the payload a new logical id that names
    no active cell. Every hook ENTRY resolves the seat once, here, so
    the entire invocation reads and writes one id -- a method may never
    read a translated id while writing the logical one. An unresolvable
    session comes back unchanged and the hook stays inert as today.
    """

    with suppress(Exception):
        from hermes_orchestrator.lead_children import (
            _bound_cell,
            bound_session_at,
        )

        with database.transaction() as connection:
            if _bound_cell(connection, session) is None:
                return (
                    bound_session_at(
                        connection,
                        cwd,
                        os.environ.get("CMUX_WORKSPACE_ID"),
                        os.environ.get("CMUX_SURFACE_ID"),
                    )
                    or session
                )
    return session


def _child_event(args: argparse.Namespace, *, completed: bool) -> int:
    """Durably count one child start or completion from a lead hook.

    A hook must never break its lead: any failure is swallowed and the
    exit code stays 0. The completion that settles the last
    outstanding child reactivates a waiting continuation exactly once
    through a children.completed control operation.
    """

    session = args.session
    child = args.child
    cwd = None
    if session is None or child is None:
        with suppress(Exception):
            # A deliberate CLI invocation carries no hook payload; a
            # blocking read on a terminal would hang the lead, so the
            # payload is only consulted when one is actually piped in.
            piped = "" if sys.stdin.isatty() else sys.stdin.read()
            payload = json.loads(piped or "{}")
            session = session or payload.get("session_id")
            cwd = payload.get("cwd")
            # Strictly the shared lifecycle identity: SubagentStart
            # and SubagentStop carry the same agent_id. A tool_use_id
            # (a PreToolUse invocation identity) or any other field
            # can never satisfy it; an event without agent_id fails
            # closed and never advances progress.
            child = child or payload.get("agent_id")
    if not child:
        return 0
    with suppress(Exception):
        from hermes_orchestrator.events import EventStore
        from hermes_orchestrator.lead_children import LeadChildTracker

        database = Database.open(_intake_state_dir(args) / "state.db")
        try:
            # INFRA-198: the same one-shot canonicalization the Stop
            # entry does -- a child recorded under a /clear-ed logical
            # id would leave the seat's outstanding set silently empty.
            # A deliberate `child-stop --child <agent_id>` names no
            # session at all: the seat is resolved from the SAME
            # durable evidence (this cwd's one active cell and its one
            # live channel registration), so the lead can settle a
            # child it stopped on purpose without knowing the durable
            # id. An unresolvable seat stays inert exactly as before.
            session = _canonical_session(database, str(session or ""), cwd)
            if not session:
                return 0
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


def _idle_admission_priority(
    executor: Any, *, now: datetime, freshness_minutes: int
) -> int | None:
    """The highest issue priority the freshest resource sample
    authorizes right now, or ``None`` when nothing may be admitted.

    Delegates to :func:`hermes_orchestrator.cells.admission_priority_ceiling`
    — the ONE shared freshness/priority ceiling the daemon dispatch
    path's armed transaction-local guard also runs (Sol ec0ed7fe gap
    3), so the idle and daemon paths can never diverge on what capacity
    evidence admits. A missing, malformed, implausibly future-dated, or
    stale sample (INFRA-199 Finding 2) all fail closed exactly like an
    explicit red/unknown pressure reading does — refuse admission,
    never guess.
    """

    from hermes_orchestrator.cells import admission_priority_ceiling

    return admission_priority_ceiling(
        executor, now=now, freshness_minutes=freshness_minutes
    )


def _dispatch_idle_lead(
    database: Database, session_id: str, args: argparse.Namespace
) -> None:
    """At the calling lead's own genuine idle boundary, hand its live
    cell one ready, non-conflicting, capacity-permitted assignment
    (INFRA-199). Only the session's OWN active ``project_cells`` row
    is ever resolved (mirrors ``LeadIntakePoll.next_offer``'s query).

    The activation itself is never reimplemented here: it runs through
    ``cells.activate_admitted_issue``, the ONE shared durable service
    that also backs ``ProjectCellService``'s explicit dispatch path.
    INFRA-199 v2: that service commits the LOCAL activation transaction
    first — it never consults Linear — and only once that commit
    succeeds does it attempt the idempotent ``In Development``
    projection; a Linear failure there is swallowed rather than
    undoing or retrying the already-durable local activation (see
    ``cells._project_in_development``). Candidate eligibility
    additionally requires a resource sample fresh enough to trust
    (INFRA-199 Finding 2); that freshness is checked once for candidate
    selection and rechecked inside the same atomic transaction that
    activates the candidate — which also re-proves every other mutable
    predicate (occupancy, exact source state, dependency readiness,
    operator decisions, cell/lease identity) at commit time.
    """

    from hermes_orchestrator.cells import activate_admitted_issue
    from hermes_orchestrator.lead_assignments import LeadAssignments
    from hermes_orchestrator.queue import QueueService

    cell = database.execute(
        "SELECT cell_id, project_key, profile_alias FROM project_cells "
        "WHERE session_id = ? AND state = 'active' "
        "ORDER BY updated_at DESC, rowid DESC LIMIT 1",
        (session_id,),
    ).fetchone()
    if cell is None:
        return
    settings = load_settings(args.repo_root, args.state_dir)
    project_key = str(cell["project_key"])
    events = EventStore(database)
    queue = QueueService(database, events, registered_projects=())
    freshness_minutes = settings.policy.resource_sample_freshness_minutes
    max_priority = _idle_admission_priority(
        database, now=datetime.now(UTC), freshness_minutes=freshness_minutes
    )
    if max_priority is None:
        return
    # Project occupancy (INFRA-199 v2 / INFRA-211): a project already
    # mid-flight on another issue — in_development OR review — never gets
    # a second one started here.
    already_working = database.execute(
        "SELECT 1 FROM admitted_issues WHERE project_key = ? "
        "AND state IN (?, ?) LIMIT 1",
        (
            project_key,
            IssueState.IN_DEVELOPMENT.value,
            IssueState.REVIEW.value,
        ),
    ).fetchone()
    if already_working is not None:
        return
    decisions = OperatorDecisions(database)
    ranked = queue.list_ranked(datetime.now(UTC))
    candidate = None
    for issue in ranked:
        if issue.project_key != project_key or not issue.dependency_ready:
            continue
        if issue.linear_priority > max_priority:
            continue
        if decisions.pending_for_issue(issue.issue_id):
            continue
        candidate = issue
        break
    if candidate is None:
        return

    # Sol ec0ed7fe gap 1: the live Keychain/config-backed Linear router
    # is NOT composed here any more. An eligible local activation must
    # commit without ever constructing the live Linear client, so the
    # lazy projector composes it only inside the post-commit projection
    # attempt, where a composition failure (no config/linear.yaml, an
    # unreadable Keychain credential, an unregistered project) is
    # swallowed by ``cells._project_in_development`` exactly like any
    # other Linear failure — it can never block or undo the local work,
    # and the stable pending ``external_effects`` row journaled with the
    # activation commit remains as the durable trace for reconciliation.
    linear = _LazyIdleLinearProjector(settings, database=database, queue=queue)
    assignments = LeadAssignments(database, events=events)

    def _still_eligible(connection: sqlite3.Connection) -> bool:
        current_max = _idle_admission_priority(
            connection, now=datetime.now(UTC), freshness_minutes=freshness_minutes
        )
        row = connection.execute(
            "SELECT priority FROM admitted_issues WHERE issue_id = ?",
            (candidate.issue_id,),
        ).fetchone()
        return (
            row is not None
            and current_max is not None
            and int(row["priority"]) <= current_max
        )

    activated, assignment = asyncio.run(
        activate_admitted_issue(
            database=database,
            events=events,
            linear=linear,
            assignments=assignments,
            cell_id=str(cell["cell_id"]),
            project_key=project_key,
            profile_alias=str(cell["profile_alias"]),
            session_id=session_id,
            issue_id=candidate.issue_id,
            guard=_still_eligible,
        )
    )
    if activated and assignment is not None:
        assignments.notify_committed(assignment)


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
    cwd = None
    if session is None:
        with suppress(Exception):
            payload = json.loads(sys.stdin.read() or "{}")
            session = payload.get("session_id")
            hook_event = payload.get("hook_event_name")
            cwd = payload.get("cwd")
    if not session:
        return 0
    database = Database.open(_intake_state_dir(args) / "state.db")
    try:
        # INFRA-198: canonicalize ONCE, before any downstream call --
        # every use below reads this local and so runs entirely on the
        # seat's stable id, which /clear does not change.
        session = _canonical_session(database, str(session), cwd)
        if hook_event == "Stop":
            # INFRA-215 (Sol acce71fc): one UNCONDITIONAL durable Stop
            # record, in the journal that already exists. A lead running
            # its own foreground turn has an acknowledged assignment and
            # zero child rows -- the same durable shape as an idle seat
            # -- so idleness cannot be inferred from those tables. This
            # event is the only positive proof the session reached Stop,
            # and `_seat_is_idle` requires it to be NEWER than the
            # latest turn-starting input.
            with suppress(Exception):
                from hermes_orchestrator.events import EventInput, EventStore
                from hermes_orchestrator.lead_children import _bound_cell

                events = EventStore(database)
                with database.transaction() as connection:
                    # INFRA-204: unconditional with respect to CHILDREN,
                    # never with respect to binding -- an unbound Claude
                    # session must stay completely inert, so it shares
                    # the one binding predicate every hook path uses.
                    if _bound_cell(connection, str(session)) is not None:
                        events.append(
                            connection,
                            EventInput(
                                event_type="lead_turn.stopped",
                                aggregate_type="lead_session",
                                aggregate_id=str(session),
                                actor="lead",
                                payload={},
                            ),
                        )
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
            # INFRA-199: at the Stop boundary, offer the existing
            # admitted queue one normal durable assignment before
            # anything else, so a freshly published packet rides this
            # same poll response.
            #
            # INFRA-211: outstanding children do NOT gate this. Fable
            # owns child supervision and leads work several issues at
            # once, so an all-or-nothing continuation gate froze the
            # whole seat behind one unsettled child. Capacity alone
            # bounds dispatch, through the caps ``_dispatch_idle_lead``
            # already applies and re-proves in its activation
            # transaction: ``development_lane_saturated``
            # (MAX_DEVELOPMENT_ISSUE_LANES) and the resource-backed
            # ``admission_priority_ceiling``. A stale child can consume
            # a capacity slot; it can no longer freeze the seat.
            with suppress(Exception):
                _dispatch_idle_lead(database, str(session), args)
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
    (``branch``, ``head``, ``origin_head``, ``dirty``,
    ``head_is_integration_ancestor``): the gate reads these fields
    directly and refuses when ``head != origin_head`` or ``dirty`` is
    true (except the narrow zero-work shape keyed on
    ``head_is_integration_ancestor``), so an unknown value here must
    resolve to something that fails that comparison rather than to
    ``None``, which the gate never expects.
    """

    branch: str
    head: str
    origin_head: str
    dirty: bool
    # Whether ``head`` is a PROVEN ancestor of the fetched integration
    # head (``origin/HEAD``). Fails closed to False: absence of proof,
    # a missing ``origin/HEAD``, or a broken git probe must never read
    # as an ancestor relationship.
    head_is_integration_ancestor: bool = False


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
    if not branch and head:
        remote_heads = _run(
            ["for-each-ref", "--format=%(objectname)", "refs/remotes/origin"]
        )
        if remote_heads is not None and head in remote_heads.splitlines():
            origin_head = head
    head_is_integration_ancestor = bool(head) and (
        _run(["merge-base", "--is-ancestor", head, "origin/HEAD"]) is not None
    )
    status = _run(["status", "--porcelain"])
    dirty = status is None or status != ""
    return WorktreeState(
        branch=branch,
        head=head,
        origin_head=origin_head,
        dirty=dirty,
        head_is_integration_ancestor=head_is_integration_ancestor,
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


def _dashboard_replacement_launcher(
    state_dir: Path, current_runtime: Path
) -> Path | None:
    """Return the exec target when ACTIVE moved to a complete runtime.

    INFRA-225: prefer the activated runtime's own console script
    (``<active>/.venv/bin/hermes-orchestrator``) when it exists —
    exec'ing it replaces this process in place, since its shebang is
    the runtime's own venv python. The ``bin/hermes-orchestrator``
    shim instead runs ``uv run``, which spawns a child and waits
    rather than exec'ing, leaving the prior wrapper alive as a stacked
    parent; it remains the fallback for a runtime not yet carrying its
    own console script.
    """

    pointer = state_dir / "runtimes" / "ACTIVE"
    launcher = state_dir / "bin" / "hermes-orchestrator"
    try:
        active = Path(pointer.read_text(encoding="utf-8").strip()).resolve()
    except OSError:
        return None
    if active == current_runtime.resolve() or not (active / "pyproject.toml").is_file():
        return None
    console_script = active / ".venv" / "bin" / "hermes-orchestrator"
    if console_script.is_file():
        return console_script
    if not launcher.is_file():
        return None
    return launcher


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
            current_runtime = _code_checkout_root()
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
                replacement = _dashboard_replacement_launcher(
                    settings.state_dir, current_runtime
                )
                if replacement is not None:
                    os.execv(
                        str(replacement),
                        [str(replacement), *sys.argv[1:]],
                    )
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


class _SeatLaneUnresolved(Exception):
    """The seat's assigned issue does not name exactly one live lane.

    Carries the facts each caller needs to name what was missing —
    the issue the seat is bound to, and how many active worktree leases
    that issue actually has — without fixing the caller's wording.
    """

    def __init__(self, issue_id: str, live_leases: int) -> None:
        super().__init__(issue_id)
        self.issue_id = issue_id
        self.live_leases = live_leases

    @property
    def detail(self) -> str:
        """The lease-count clause both refusals embed verbatim."""

        return (
            "has no active worktree lease"
            if not self.live_leases
            else f"has {self.live_leases} active worktree leases"
        )


def _seat_lane(
    database: Database, *, project_key: str, cell_id: str, session_id: str
) -> ResolvedLane | None:
    """The ONE live worktree lease bound to this exact seat's own issue.

    INFRA-198: the seat, not the project, names the tree. The cell's
    latest lead assignment for its exact ``cell_id`` + ``session_id``
    identifies the issue it is actually working, and that issue's single
    active worktree lease identifies the checkout — the coordinator's
    ``project.lead_cwd`` is frequently detached and frequently dirtied
    by unrelated work, so it describes no lane in particular.

    The assignment row's ``state`` is deliberately NOT filtered.
    Supersession is per-ISSUE, not per-cell: a later assignment of the
    same issue to a DIFFERENT cell marks this row ``superseded`` while
    saying nothing about whether THIS cell is still bound and working,
    so excluding superseded rows discards the one durable fact
    identifying the seat. Freshness for the seat comes from the ordering
    instead, which guarantees a newer assignment for this same
    cell/session wins. Scoping to cell_id AND session_id is also what
    keeps another cell's lease from ever qualifying here.

    Returns ``None`` when no assignment names this exact cell and
    session — the genuinely unassigned caller. Raises
    :class:`_SeatLaneUnresolved` when the seat IS assigned but its issue
    binds zero or several live lanes: an unresolvable lane fails closed
    rather than silently falling back to some other checkout.
    """

    assignment = database.execute(
        "SELECT issue_id FROM lead_assignments "
        "WHERE cell_id = ? AND session_id = ? "
        "ORDER BY created_at DESC, rowid DESC LIMIT 1",
        (cell_id, session_id),
    ).fetchone()
    if assignment is None:
        return None
    issue_id = str(assignment["issue_id"])
    # ``resolve_lane`` is the codebase's fail-closed reader for "this
    # issue's ONE active worktree lease": zero leases and several leases
    # both refuse rather than guess, and it is scoped to the project and
    # the issue, so a lease bound to any other issue never qualifies.
    leases = WorktreeLeases(database, events=EventStore(database))
    try:
        return resolve_lane(leases, project_key, issue_id)
    except EmissionBlocked:
        live = [
            lease for lease in leases.active(project_key) if lease.issue_id == issue_id
        ]
        raise _SeatLaneUnresolved(issue_id, len(live)) from None


def _rotation_worktree(
    database: Database, *, project: Any, project_key: str, cell_id: str | None
) -> Path:
    """The worktree the rotation precondition must measure for ONE seat.

    INFRA-198: the precondition used to close over ``project.lead_cwd``
    unconditionally, so every cell of a project was judged by one shared
    checkout — a third party dirtying an unrelated tree blocked a clean
    lane's rotation, and a coordinator left detached made the
    head-vs-origin comparison meaningless. The rotating cell's OWN bound
    lane is selected instead, by the same derivation ``_submit_handoff``
    uses (:func:`_seat_lane`).

    A project coordinator need not own one issue lane: it may coordinate
    several independently leased worktrees. Such an unassigned seat is
    measured against its own ``project.lead_cwd`` and never against an
    arbitrary issue lane. A cell with no durable row or no incumbent
    session also keeps that fallback — ``LeadRotation``'s own precondition
    is the boundary that names those.

    Raises ``ValueError`` (which both call sites already surface as one
    actionable message) on zero, ambiguous, or mismatched identity.
    """

    if cell_id is None:
        return project.lead_cwd
    row = database.execute(
        "SELECT project_key, session_id FROM project_cells WHERE cell_id = ?",
        (cell_id,),
    ).fetchone()
    if row is None or row["session_id"] is None:
        return project.lead_cwd
    if str(row["project_key"]) != project_key:
        raise ValueError(
            f"lead rotation refused: cell {cell_id!r} belongs to project "
            f"{str(row['project_key'])!r}, not {project_key!r}, so the "
            "rotating seat's worktree cannot be derived"
        )
    try:
        lane = _seat_lane(
            database,
            project_key=project_key,
            cell_id=cell_id,
            session_id=str(row["session_id"]),
        )
    except _SeatLaneUnresolved as unresolved:
        raise ValueError(
            f"lead rotation refused: cell {cell_id!r} is assigned issue "
            f"{unresolved.issue_id!r}, but that issue {unresolved.detail} in "
            f"project {project_key!r}, so the rotating seat's worktree cannot "
            f"be derived; leave exactly one active worktree lease for "
            f"{unresolved.issue_id} and rotate again"
        ) from None
    if lane is not None:
        return lane.path
    return project.lead_cwd


def _compose_lead_rotation(
    settings: Any,
    runtime: Any,
    database: Database,
    project: Any,
    *,
    project_key: str,
    cell_id: str | None,
) -> Any:
    """Build the one-shot ``LeadRotation`` exactly as ``rotate-lead`` does.

    Shared by ``rotate-lead`` and the daemon's handoff continuation so
    both drive the same durable transition with identical collaborators.
    Both callers name the cell being rotated, because the worktree the
    precondition measures is a property of that SEAT, not of the project
    (INFRA-198; see :func:`_rotation_worktree`).

    Raises ``OSError``/``ValueError`` when the collaborators cannot be
    opened or the seat's own worktree cannot be named (surfaced by the
    caller as one actionable message).
    """

    from hermes_orchestrator.lead_rotation import LeadRotation

    cells, seater = _open_rotation_collaborators(settings, runtime)
    # Leads and candidates work in a dedicated worktree when the
    # project configures one; ``repo_path`` itself must stay the stable
    # primary checkout (see the linked-worktree rejection in
    # ``load_settings``). INFRA-198: for an ASSIGNED seat that tree is
    # the cell's own bound issue lane, and only an unassigned single-lane
    # caller still falls back to the canonical ``project.lead_cwd`` Sol
    # correction c5600e31 established for the seater/cell composition.
    # Resolve lazily: after a transfer commits, recovery only needs to seat
    # the replacement and must not rediscover an implementation lane.
    return LeadRotation(
        database=database,
        handoffs=HandoffService(database),
        cells=cells,
        bindings=runtime.cmux_bindings,
        seater=seater,
        # The gate calls this with the project key, not the cell id;
        # the exact worktree path is already resolved above, so the key
        # itself is accepted but unused.
        worktree_state=lambda _project_key: _worktree_state(
            _rotation_worktree(
                database,
                project=project,
                project_key=project_key,
                cell_id=cell_id,
            )
        ),
        registration_wait_seconds=ROTATION_REGISTRATION_WAIT_SECONDS,
    )


def _modified_files(path: Path, base_branch: str) -> list[str]:
    """Paths changed on this worktree's branch relative to the fetched
    integration branch (INFRA-184), read-only.

    Follows the exact fail-closed idiom ``_worktree_state`` already
    uses for its own git probes: any failure (missing worktree, no
    fetched ``origin/<base_branch>``, git itself absent) maps to an
    empty list rather than raising, so a handoff submission is never
    blocked by an unreadable probe -- ``derived_handoff_document``
    renders an honest "no files changed" fallback instead.
    """

    try:
        result = subprocess.run(
            [
                "git",
                "-C",
                str(path),
                "diff",
                "--name-only",
                f"origin/{base_branch}...HEAD",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    return [line for line in result.stdout.splitlines() if line.strip()]


def _derived_pull_request(
    database: Database, *, project_key: str, issue_id: str
) -> str:
    """The most recent durably recorded pull request for this issue.

    Sourced from ``reviews`` (INFRA-166), the one table that binds a
    ``pr_number`` directly to an ``issue_id``: ``ci_merge_ledger`` and
    ``github_merge_effects`` record merges but not issue identity, so
    they cannot be scoped to a single issue's handoff without guessing.
    The GitHub API is never called -- an issue with no recorded review
    reports "none recorded in durable state" rather than reaching out.
    """

    row = database.execute(
        "SELECT repository, pr_number, state FROM reviews "
        "WHERE project_key = ? AND issue_id = ? "
        "ORDER BY created_at DESC, rowid DESC LIMIT 1",
        (project_key, issue_id),
    ).fetchone()
    if row is None:
        return "none recorded in durable state"
    repository = str(row["repository"])
    pr_number = int(row["pr_number"])
    state = str(row["state"])
    return f"#{pr_number} {state} https://github.com/{repository}/pull/{pr_number}"


def _derived_test_results(database: Database, issue_id: str) -> list[str]:
    """Observed test/acceptance evidence durably recorded for this issue.

    Two sources, both scoped by ``issue_id`` (INFRA-184):
    ``subagent_packets.evidence_json`` for each accepted packet (diff
    scope, red/green proof references reviewed at acceptance), and
    ``acceptance_gates.evidence_json`` for a satisfied acceptance gate.
    ``verification_receipts`` is deliberately NOT read here -- it keys
    on ``gate_id``/``tree_hash`` with no ``issue_id`` or ``cell_id``
    column, so there is no durable way to scope a receipt to one
    issue's handoff without guessing at a tree match.
    """

    results: list[str] = []
    for row in database.execute(
        "SELECT packet_id, evidence_json FROM subagent_packets "
        "WHERE issue_id = ? AND state = 'accepted' "
        "ORDER BY updated_at ASC, rowid ASC",
        (issue_id,),
    ).fetchall():
        packet_id = str(row["packet_id"])
        evidence_json = row["evidence_json"]
        evidence = json.loads(str(evidence_json)) if evidence_json else {}
        summary = ", ".join(f"{k}={v}" for k, v in sorted(evidence.items()))
        outcome = f"accepted{': ' + summary if summary else ''}"
        results.append(f"subagent packet {packet_id} — {outcome}")
    for row in database.execute(
        "SELECT instruction_id, evidence_json FROM acceptance_gates "
        "WHERE issue_id = ? AND state = 'satisfied'",
        (issue_id,),
    ).fetchall():
        instruction_id = str(row["instruction_id"])
        evidence_json = row["evidence_json"]
        pairs = json.loads(str(evidence_json)) if evidence_json else []
        summary = ", ".join(f"{k}={v}" for k, v in pairs)
        outcome = f"satisfied{': ' + summary if summary else ''}"
        results.append(f"acceptance gate {instruction_id} — {outcome}")
    return results


def _derived_pending_corrections(
    database: Database, *, project_key: str, issue_id: str
) -> list[str]:
    """Merger correction packets still awaiting the lead's
    acknowledgement for this issue (INFRA-184), read from the
    ``lead_corrections`` outbox via the existing
    :class:`LeadCorrectionOutbox` port -- never queried as raw SQL, and
    never delivered as a Linear comment.
    """

    outbox = LeadCorrectionOutbox(
        database=database,
        events=EventStore(database),
        # Only ``pending()`` is used below, which never resolves this
        # callback: it exists solely to satisfy the port's constructor.
        project_for_issue=lambda _issue_id: project_key,
    )
    return [
        f"{correction.correction_id} ({correction.source}, pr #{correction.pr_number})"
        for correction in outbox.pending(project_key)
        if correction.issue_id == issue_id
    ]


def _pending_rotation_work(
    database: Database,
) -> tuple[tuple[str, str, str, str], ...]:
    """Return daemon-owned rotation continuations, one per cell."""

    work: list[tuple[str, str, str, str]] = []
    seen: set[str] = set()
    attempts = database.execute(
        "SELECT h.cell_id, c.project_key, h.handoff_id FROM handoffs AS h "
        "JOIN project_cells AS c ON c.cell_id = h.cell_id "
        "WHERE c.state IN ('starting', 'active', 'handoff_required', 'paused') "
        "AND EXISTS (SELECT 1 FROM events AS started "
        "WHERE started.event_type = 'lead_rotation.attempt' "
        "AND started.aggregate_id = h.handoff_id) "
        "AND NOT EXISTS (SELECT 1 FROM events AS finished "
        "WHERE finished.event_type = 'lead_rotation.completed' "
        "AND finished.aggregate_id = h.handoff_id) "
        "AND h.rowid = (SELECT MAX(latest.rowid) FROM handoffs AS latest "
        "WHERE latest.cell_id = h.cell_id) "
        "ORDER BY h.created_at DESC, h.rowid DESC"
    ).fetchall()
    for row in attempts:
        cell_id = str(row["cell_id"])
        if cell_id in seen:
            continue
        seen.add(cell_id)
        work.append(
            ("attempt", cell_id, str(row["project_key"]), str(row["handoff_id"]))
        )

    awaiting = database.execute(
        "SELECT h.cell_id, c.project_key, h.handoff_id FROM handoffs AS h "
        "JOIN project_cells AS c ON c.cell_id = h.cell_id "
        "WHERE c.state IN ('starting', 'active', 'handoff_required', 'paused') "
        "AND h.state = 'submitted' AND h.rowid = ("
        "SELECT MAX(latest.rowid) FROM handoffs AS latest "
        "WHERE latest.cell_id = h.cell_id AND latest.state = 'submitted') "
        "AND EXISTS (SELECT 1 FROM events AS waiting "
        "WHERE waiting.event_type = 'lead_rotation.awaiting_handoff' "
        "AND waiting.aggregate_id = h.cell_id "
        "AND waiting.sequence > COALESCE((SELECT MAX(resumed.sequence) "
        "FROM events AS resumed "
        "WHERE resumed.event_type = 'lead_rotation.awaiting_resumed' "
        "AND resumed.aggregate_id = h.cell_id), 0)) "
        "ORDER BY h.created_at ASC, h.rowid ASC"
    ).fetchall()
    for row in awaiting:
        cell_id = str(row["cell_id"])
        if cell_id in seen:
            continue
        seen.add(cell_id)
        work.append(
            ("submission", cell_id, str(row["project_key"]), str(row["handoff_id"]))
        )
    return tuple(work)


async def _recover_pending_lead_rotations(
    settings: Settings, runtime: Runtime, database: Database
) -> tuple[object, ...]:
    """Resume durable rotations from the daemon, never the retiring lead."""

    reports: list[object] = []
    handoffs = HandoffService(database)
    for kind, cell_id, project_key, handoff_id in _pending_rotation_work(database):
        project = settings.projects.get(project_key)
        if project is None:
            continue
        try:
            rotation = _compose_lead_rotation(
                settings,
                runtime,
                database,
                project,
                project_key=project_key,
                cell_id=cell_id,
            )
            if kind == "submission":
                report = await rotation.resume_on_submission(handoffs.get(handoff_id))
            else:
                report = await rotation.rotate(cell_id)
        except Exception as error:
            print(
                f"lead rotation recovery for {cell_id!r} failed: "
                f"{type(error).__name__}: {error}",
                file=sys.stderr,
            )
            continue
        if report is not None:
            reports.append(report)
    return tuple(reports)


def _submit_handoff(
    args: argparse.Namespace, settings: Any, runtime: Any, database: Database
) -> int:
    """Submit the incumbent's fresh durable handoff for daemon continuation.

    Every mechanical handoff field is derived from durable rows and the
    lead-worktree probe; the caller supplies only decisions, caveats,
    risks, and the exact next action. The daemon resumes any awaiting
    rotation from this durable record, so this incumbent process returns
    before its own workspace can be retired.
    """

    cell = database.execute(
        "SELECT project_key, state, session_id, profile_alias "
        "FROM project_cells WHERE cell_id = ?",
        (args.cell,),
    ).fetchone()
    if cell is None:
        _print(
            {"error": f"cell {args.cell!r} does not exist"},
            json_output=args.json,
            human=f"cell {args.cell!r} does not exist.",
        )
        return 1
    project_key = str(cell["project_key"])
    project = settings.projects.get(project_key)
    if project is None:
        _print(
            {"error": "the cell's project is not configured"},
            json_output=args.json,
            human="the cell's project is not configured.",
        )
        return 1
    issue_rows = database.execute(
        "SELECT issue_id, state FROM admitted_issues WHERE project_key = ? "
        "AND state IN ('in_development', 'review') ORDER BY issue_id",
        (project_key,),
    ).fetchall()
    # INFRA-198: derive the issue and the git position from the SEAT
    # being handed off, not from the project as a whole -- the shared
    # ``_seat_lane`` derivation the rotation precondition now uses too.
    try:
        lane = _seat_lane(
            database,
            project_key=project_key,
            cell_id=args.cell,
            session_id=str(cell["session_id"]),
        )
    except _SeatLaneUnresolved as unresolved:
        message = (
            f"handoff refused: cell {args.cell!r} is assigned issue "
            f"{unresolved.issue_id!r}, but that issue {unresolved.detail} in "
            f"project {project_key!r}, so the handoff's branch cannot be "
            f"derived; leave exactly one active worktree lease for "
            f"{unresolved.issue_id} and submit again"
        )
        _print(
            {"error": message},
            json_output=args.json,
            human=f"{message}.",
        )
        return 1
    if lane is not None:
        issue_id = lane.issue_id
        probe_path = lane.path
        worktree = _worktree_state(probe_path)
        issue_state = next(
            (
                str(row["state"])
                for row in issue_rows
                if str(row["issue_id"]) == issue_id
            ),
            "unknown",
        )
    elif len(issue_rows) > 1:
        # No seat-scoped assignment AND a genuinely ambiguous project:
        # the old project-wide derivation would silently emit issue
        # "none" against the coordinator's tree. Refuse instead.
        named = ", ".join(str(row["issue_id"]) for row in issue_rows)
        message = (
            f"handoff refused: cell {args.cell!r} has no live lead "
            f"assignment and project {project_key!r} has "
            f"{len(issue_rows)} active issues ({named}), so the handoff's "
            "issue and branch cannot be derived; assign the cell its issue "
            "before submitting"
        )
        _print(
            {"error": message},
            json_output=args.json,
            human=f"{message}.",
        )
        return 1
    else:
        # Preserved fallback: an unassigned cell in a project with at
        # most one active issue behaves exactly as it did before, down
        # to the coordinator worktree probe.
        probe_path = project.lead_cwd
        worktree = _worktree_state(probe_path)
        issue_id, issue_state = (
            (str(issue_rows[0]["issue_id"]), str(issue_rows[0]["state"]))
            if len(issue_rows) == 1
            else ("none", "unknown")
        )
    # INFRA-184: every remaining mechanical fact -- the pull request,
    # the files actually changed, observed test/acceptance evidence,
    # and pending Merger corrections -- is derived from durable rows
    # and a read-only git probe, never supplied by the incumbent or
    # fetched from the GitHub API.
    facts = DerivedFacts(
        pull_request=_derived_pull_request(
            database, project_key=project_key, issue_id=issue_id
        ),
        modified_files=tuple(_modified_files(probe_path, project.integration_branch)),
        test_results=tuple(_derived_test_results(database, issue_id)),
        pending_corrections=tuple(
            _derived_pending_corrections(
                database, project_key=project_key, issue_id=issue_id
            )
        ),
    )
    document = derived_handoff_document(
        cell_id=args.cell,
        project_key=project_key,
        session_id=str(cell["session_id"]),
        profile_alias=str(cell["profile_alias"]),
        issue_id=issue_id,
        issue_state=issue_state,
        branch=worktree.branch,
        head=worktree.head,
        decisions=list(args.decisions),
        caveats=list(args.caveats),
        risks=list(args.risks),
        next_action=args.next_action,
        facts=facts,
    )
    # INFRA-184: mechanical identity validation, fail-closed, BEFORE
    # any durable write. The document is built from the cell row read
    # above; this re-reads the live row right before submission and
    # refuses on any drift (the cell reassigned, or its session
    # rotated) rather than writing a handoff for a seat that has since
    # moved.
    live_cell_row = database.execute(
        "SELECT cell_id, session_id FROM project_cells WHERE cell_id = ?",
        (args.cell,),
    ).fetchone()
    if live_cell_row is None:
        message = f"handoff refused: cell {args.cell!r} no longer exists"
        _print({"error": message}, json_output=args.json, human=f"{message}.")
        return 1
    live_session = live_cell_row["session_id"]
    try:
        validate_handoff_identity(
            document,
            LiveCell(
                cell_id=str(live_cell_row["cell_id"]),
                session_id=(str(live_session) if live_session is not None else None),
            ),
            requesting_session_id=str(cell["session_id"]),
        )
    except HandoffRejected as error:
        _print(
            {"error": str(error)},
            json_output=args.json,
            human=f"handoff refused: {error}",
        )
        return 1
    handoffs = HandoffService(database)
    try:
        record = handoffs.submit(document)
    except HandoffRejected as error:
        _print(
            {"error": str(error)},
            json_output=args.json,
            human=f"handoff refused: {error}",
        )
        return 1
    payload: dict[str, Any] = {
        "handoff_id": record.handoff_id,
        "cell_id": record.cell_id,
        "state": record.state,
        "resumed_rotation": None,
        "continuation_note": "Hermes daemon will resume any awaiting rotation",
    }
    _print(
        payload,
        json_output=args.json,
        human=f"Handoff {record.handoff_id} submitted for Hermes to resume.",
    )
    return 0


def main(arguments: Sequence[str] | None = None) -> int:
    """Run one CLI command and return its process exit code."""

    args = _parser().parse_args(arguments)
    if args.command == "subagent-gate":
        def _admission_factory() -> PacketAdmission:
            # Built lazily -- only once subagent_gate has already ruled
            # out the freeze marker and found a packet:<hex> marker --
            # so an ordinary unmarked Agent launch never touches the
            # state database at all, exactly like before INFRA-215.
            gate_settings = load_settings(args.repo_root, args.state_dir)
            database = Database.open(gate_settings.state_dir / "state.db")
            return PacketAdmission(
                database,
                packets=SubagentPackets(database, events=EventStore(database)),
                tiers=configured_model_tiers(),
                freeze_dir=args.freeze_dir,
            )

        code, message = subagent_gate(
            args.freeze_dir, sys.stdin.read(), admission_factory=_admission_factory
        )
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
    if args.command == "claude-launch":
        return _claude_launch(args, settings)
    if args.command == "provider-probe":
        return _provider_probe(args, settings)
    if args.command == "claude-shell-init":
        return _claude_shell_init(args, settings)
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
            port = CmuxCliAdapter(
                settings.cmux.cli,
                base_env=os.environ,
                password_source=cmux_password_source(Keychain()),
            )
            if anchor is None and not args.capture_prompt:
                confirmer = ChannelTrustConfirmer(
                    database=database,
                    events=events,
                    control=runtime.control_operations,
                    port=port,
                    entry_resolver=lambda: resolve_sidecar_entry(
                        repo_root=settings.repo_root,
                        state_dir=settings.state_dir,
                    ),
                    wait_seconds=args.wait_seconds,
                )
                verdict = asyncio.run(confirmer.confirm_seat(binding))
                if verdict is None:
                    _print(
                        {"error": "dialog not visible within the wait window"},
                        json_output=args.json,
                        human=(
                            "the hermes-control confirmation dialog did not "
                            "appear within the wait window; nothing recorded."
                        ),
                    )
                    return 1
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
            if anchor is None:
                _print(
                    {"error": "no active trust anchor for this cell"},
                    json_output=args.json,
                    human="no active trust anchor for this cell.",
                )
                return 1
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
                rotation = _compose_lead_rotation(
                    settings,
                    runtime,
                    database,
                    project,
                    project_key=project_key,
                    cell_id=args.cell,
                )
            except (OSError, ValueError) as error:
                _print(
                    {"error": str(error)},
                    json_output=args.json,
                    human=f"{error}.",
                )
                return 1
            report = asyncio.run(rotation.rotate(args.cell))
            payload = dataclasses.asdict(report)
            if report.phase == "awaiting_handoff":
                # Non-terminal (Sol 52d15493): the rotation itself filed
                # the fresh durable handoff request with the incumbent
                # and resumes automatically once it is submitted — no
                # operator action and no second rotate-lead required.
                _print(
                    payload,
                    json_output=args.json,
                    human=(
                        f"Rotation awaiting a fresh handoff for cell "
                        f"{report.cell_id}: durable request "
                        f"{getattr(report, 'request_id', None)} filed with "
                        "the incumbent lead; the rotation resumes "
                        "automatically once the handoff is submitted "
                        "(hermes-orchestrator submit-handoff)."
                    ),
                )
                return 0
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

        if args.command == "start-lane":
            return _start_lane(args, settings, runtime)

        if args.command == "submit-handoff":
            return _submit_handoff(args, settings, runtime, database)

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
                linear_reads=_LazyLinearReads(Keychain()),
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
            if runtime.cells is not None:
                # Sol ec0ed7fe gap 3: the daemon's dispatch path runs
                # the IDENTICAL transaction-local freshness/priority
                # guard the idle dispatcher supplies
                # (cells.admission_priority_ceiling on the activation
                # transaction's own connection), armed with the same
                # policy freshness bound the idle path reads, so
                # capacity evidence that went stale — or was superseded
                # by a red sample — between scheduler planning and the
                # activation commit fails closed.
                runtime.cells.require_dispatch_capacity_guard(
                    settings.policy.resource_sample_freshness_minutes
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
                    wakes=runtime.lead_wakes,
                    resource_freshness_minutes=(
                        settings.policy.resource_sample_freshness_minutes
                    ),
                    cmux_reconciler=runtime.cmux_reconciler,
                    cmux_dead_lead_sweep=runtime.cmux_dead_lead_sweep,
                    dev_seat_recovery=(
                        None
                        if runtime.cells is None
                        else DevSeatRecovery(
                            runtime.cells, runtime.queue, runtime.database
                        )
                    ),
                    cmux_hibernation=runtime.cmux_hibernation,
                    lead_intake=runtime.lead_intake,
                    channel_hub=runtime.channel_hub,
                    control_operations=runtime.control_operations,
                    dashboard_refresh=runtime.dashboard_refresh,
                    orchestrator_workspace=runtime.orchestrator_workspace,
                    fakechat_router=runtime.fakechat_router,
                    post_merge=runtime.post_merge,
                    lead_rotation_recovery=(
                        None
                        if settings.cmux is None or runtime.cmux_bindings is None
                        else lambda: _recover_pending_lead_rotations(
                            settings, runtime, runtime.database
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

        if args.command == "target-issue":
            events = EventStore(database)
            assignments = LeadAssignments(database, events=events)
            targeting = (
                target_harness_followup
                if getattr(args, "lane", "development") == "harness"
                else target_issue
            )
            try:
                result = targeting(
                    database,
                    events=events,
                    assignments=assignments,
                    issue_id=args.issue_id,
                    project_key=args.project,
                    cell_id=args.cell,
                    session_id=args.session,
                    instruction=args.instruction,
                    known_projects=settings.projects.keys(),
                )
            except IssueTargetingRefused as error:
                print(str(error), file=sys.stderr)
                return 1
            _print(
                result.assignment.as_dict(),
                json_output=args.json,
                human=(
                    f"Targeted {result.assignment.issue_id} to session "
                    f"{result.assignment.session_id}"
                    + (" (already pending)" if result.idempotent else ".")
                ),
            )
            return 0

        if args.command == "target-issue-ack":
            from hermes_orchestrator.queue import QueueService

            events = EventStore(database)
            assignments = LeadAssignments(database, events=events)
            queue = QueueService(database, events, registered_projects=())
            linear = _LazyIdleLinearProjector(settings, database=database, queue=queue)
            ack_result = asyncio.run(
                acknowledge_target(
                    database,
                    events=events,
                    linear=linear,
                    assignments=assignments,
                    assignment_id=args.assignment_id,
                    session_id=args.session,
                )
            )
            if not ack_result.activated:
                print(
                    f"assignment {args.assignment_id!r} was not acknowledged "
                    "or could not be activated",
                    file=sys.stderr,
                )
                return 1
            _print(
                {
                    "assignment_id": ack_result.assignment_id,
                    "issue_id": ack_result.issue_id,
                    "activated": ack_result.activated,
                },
                json_output=args.json,
                human=f"Advanced {ack_result.issue_id} to in_development.",
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

        if args.command == "candidate":
            # INFRA-203: ``candidate submit <issue>`` -- one deterministic
            # entry point over the SAME emission. Everything is derived
            # from durable state: the lane, its checkout, and that
            # checkout's exact HEAD. Both gates run before the merge flow
            # opens, so a refusal writes nothing.
            from hermes_orchestrator.git import SubprocessGitRunner

            decision = _pending_operator_decision(runtime.database, args.issue_id)
            if decision is not None:
                print(
                    f"awaiting operator decision {decision}; candidate "
                    "publication is blocked until the operator resolves it",
                    file=sys.stderr,
                )
                return 1
            try:
                submit_lane = _candidate_submit_lane(runtime.database, args.issue_id)
                submit_project = settings.projects.get(submit_lane.project_key)
                if submit_project is None:
                    raise EmissionBlocked(
                        "candidate submission refused: the lane bound to "
                        f"{args.issue_id} names project "
                        f"{submit_lane.project_key!r}, which is not configured"
                    )
                _candidate_submit_gate(
                    SubprocessGitRunner(), submit_project, submit_lane
                )
            except EmissionBlocked as error:
                print(str(error), file=sys.stderr)
                return 1
            try:
                flow = _open_merge_flow(settings, runtime)
                payload = asyncio.run(_candidate_ready(flow, args, submit_lane))
            except (ValueError, RuntimeError) as error:
                print(str(error), file=sys.stderr)
                return 1
            _print(
                payload,
                json_output=args.json,
                human=(
                    f"Candidate {payload['candidate_sha'][:12]} for "
                    f"{payload['issue_id']} from {submit_lane.path}: "
                    f"{payload['delivery_reason']}."
                ),
            )
            return 0 if payload["delivered"] else 1

        if args.command == "candidate-ready":
            decision = _pending_operator_decision(runtime.database, args.issue_id)
            if decision is not None:
                print(
                    f"awaiting operator decision {decision}; candidate "
                    "publication is blocked until the operator resolves it",
                    file=sys.stderr,
                )
                return 1
            # INFRA-219 L5: resolve the requested issue's own live
            # worktree lease BEFORE the merge flow is even opened, so an
            # unbound issue never reaches manifest write or wake
            # publication -- fails closed exactly like the invalid-input
            # checks above.
            leases = WorktreeLeases(
                runtime.database, events=EventStore(runtime.database)
            )
            try:
                resolved_lane = resolve_lane(leases, args.project, args.issue_id)
            except EmissionBlocked as error:
                print(str(error), file=sys.stderr)
                return 1
            try:
                flow = _open_merge_flow(settings, runtime)
                payload = asyncio.run(_candidate_ready(flow, args, resolved_lane))
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
            # INFRA-221: a candidate held behind the reviewer's single
            # active slot published successfully — it is durably queued
            # and is woken automatically once the current verdict
            # settles, so this is not a failure the lead should retry.
            if payload["delivery_reason"] == CANDIDATE_QUEUED:
                return 0
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

            async def _settle_single_review(review_id: str) -> Any:
                # Sol 04d013b0 finding 3: the per-review maintenance
                # entry must repair acceptance drift exactly like the
                # project-wide resume (which composes the pass inside
                # resume_settlements) — a satisfied gate stranded by a
                # crash between satisfaction and completion converges to
                # Done through EITHER supported merge-settle shape. The
                # pass is scoped to this invocation's project and is
                # idempotent, so a review with no gated issue repairs
                # nothing.
                outcome = await flow.reviews.merge_approved(review_id)
                await flow.reviews.reconcile_acceptance(args.project)
                return outcome

            try:
                if args.review is not None:
                    outcomes = (
                        asyncio.run(_settle_single_review(args.review)),
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
                        submitted_sha=args.submitted_sha,
                        final_integration_sha=args.final_integration_sha,
                        merge_sha=args.merge_sha,
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
            # INFRA-214 migration path: issues admitted BEFORE the
            # assignment-time binding existed are already
            # in_development, so that hook will never run for them and
            # their candidates stay unpublishable. This idempotent
            # catch-up binds any occupying issue that still has no live
            # lease, and is a no-op for those that do.
            if args.bind_issue_lane:
                # Targeted and observable: one exact issue, and a failure
                # surfaces rather than being suppressed. A blanket sweep
                # would try to bind legacy issues whose branches are
                # checked out elsewhere and bury the real repair in noise.
                #
                # Bound to the lease store and git directly, NOT to
                # runtime.cells: `reconcile` is a non-live runtime and
                # builds no cell service, so gating on one made this
                # silently do nothing and still exit zero.
                project_key, _, issue_id = args.bind_issue_lane.partition(":")
                project = settings.projects.get(project_key)
                if not issue_id:
                    _print(
                        {"error": "expected --bind-issue-lane <project>:<issue>"},
                        json_output=args.json,
                        human="--bind-issue-lane takes <project>:<issue>.",
                    )
                    return 1
                if (
                    project is None
                    or runtime.worktree_leases is None
                    or runtime.worktree_git is None
                ):
                    _print(
                        {"error": f"no registered project {project_key!r}"},
                        json_output=args.json,
                        human=f"Unknown project {project_key!r}.",
                    )
                    return 1
                try:
                    payload["bound_issue_lanes"] = list(
                        bind_admitted_issue_worktree(
                            database,
                            runtime.worktree_leases,
                            runtime.worktree_git,
                            project_key=project_key,
                            issue_id=issue_id,
                            repo_path=project.repo_path,
                            branch=args.bind_issue_branch,
                            forbidden=(Path.cwd(),),
                            integration_branch=project.integration_branch,
                        )
                    )
                except Exception as error:
                    _print(
                        {"error": f"{type(error).__name__}: {error}"},
                        json_output=args.json,
                        human=f"issue-lane binding failed: {error}",
                    )
                    return 1
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
