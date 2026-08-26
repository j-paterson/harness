# Hermes orchestration system design

Status: Approved proposal  
Date: 2026-08-26  
Target host: Jesse's MacBook Pro, macOS, 24 GiB RAM, 14 logical CPUs

## Purpose

This document defines a local orchestration system that uses Hermes as the operator-facing global supervisor for project work performed by Claude leads and reviewed and merged by Codex. The system coordinates explicitly queued Linear issues, manages four paid Claude Max profiles and one Codex account, protects the host from resource exhaustion, reclaims Git worktrees promptly, and provides restricted remote control from an iPhone and Apple Watch.

This document describes proposed behavior. None of the orchestration components described here are implemented merely because this specification exists.

## Goals

The system must:

- Accept work only when the operator explicitly assigns a Linear issue to Hermes through chat.
- Maintain one persistent Claude lead for each active project.
- Let each Claude lead maximize safe parallel work through issue-scoped subagents.
- Use one independent Codex Merger role for review, CI evaluation, and merging.
- Route review failures back to the Claude lead instead of allowing Codex to repair the implementation.
- Coordinate four Claude Max accounts without moving active work between accounts unexpectedly.
- Preserve minimal, accurate workflow state in Linear while keeping orchestration diagnostics private.
- Detect stalls, learn approved remedies, and automate recurring remedies after the first two operator consultations.
- Rotate long-running or context-heavy sessions through complete handoffs.
- Govern memory, CPU, disk, processes, and worktrees on a resource-constrained Mac.
- Reclaim completed worktrees immediately after verifying that their work is recoverable from Git.
- Provide a restricted phone console over Tailscale and a narrow Photon command channel for iMessage and Apple Watch.
- Recover safely from process restarts without duplicating work, transitions, reviews, or merges.

## Non-goals

The first version will not:

- Discover or start arbitrary open Linear issues, projects, or backlog items.
- Add Hermes-specific comments, labels, diagnostics, or hidden workflow artifacts to Linear.
- Use Codex to correct Claude-authored implementation defects or resolve code conflicts.
- Wait continuously for every CircleCI workflow to finish.
- Expose unrestricted shell access, provider credentials, or the full Hermes dashboard remotely.
- Use Tailscale Funnel or another public ingress mechanism.
- Disable provider safeguards or obscure which paid account performs work.
- Guarantee a fixed number of concurrent workers regardless of current machine pressure.

## Design principles

### Explicit intake

Hermes operates only on a private allowlist populated by direct operator instruction. Linear is a workflow record, not an autonomous work-discovery feed.

### Persistent ownership

An active project belongs to one Claude lead and one Claude profile while both remain healthy. A handoff transfers ownership only at a checkpoint and includes enough evidence for the replacement to continue safely.

### Independent verification

The Codex Merger reviews the submitted state independently. It does not become a second implementation worker.

### Recoverability before cleanup

Disk cleanup can be aggressive because Git is the durable recovery mechanism. The system must prove that a commit is present on a remote before removing a worktree.

### Deterministic control around agent reasoning

Hermes diagnoses and coordinates, but a durable state machine enforces queue, transition, concurrency, cleanup, and authorization rules. Agent output alone cannot authorize a merge, deletion, credential change, or external workflow transition.

### Least privilege remotely

The iPhone and Apple Watch surfaces expose operational commands, not general administration. Credentials and unrestricted system access remain local-only.

## System topology

```text
Operator
├── Local Hermes chat and full local dashboard
├── Restricted iPhone console through Tailscale Serve
└── Restricted Photon/iMessage commands and notifications
         │
         ▼
Hermes global supervisor
         │ intents, diagnoses, approvals, summaries
         ▼
Durable orchestration service
├── Private work queue and event journal
├── Project and worker state machines
├── Account and resource scheduler
├── Stall and handoff manager
├── Git and worktree custodian
└── Service adapters
    ├── Linear
    ├── GitHub
    ├── CircleCI
    ├── Claude Code and cmux
    ├── Codex App Server
    ├── Hermes and Photon
    └── macOS processes and resources
         │
         ▼
Project cell
├── One persistent Claude lead on one Claude Max profile
│   └── Issue-scoped Claude subagents on the same profile
└── One persistent, independent Codex Merger thread
```

Hermes is the conversational supervisor and reasoning layer. The orchestration service is the durable execution substrate. Hermes calls narrow orchestration commands instead of depending on its conversation context to remember leases, issue state, session identifiers, or cleanup obligations.

## Project registry

The repository contains a versioned project registry. Each project entry defines:

- Linear team and project identifiers.
- Local repository path.
- Integration branch.
- GitHub repository and merge policy.
- Validation commands and project-specific acceptance rules.
- Claude launch command and profile-selection constraints.
- Codex Merger instructions and saved thread identifier reference.
- CircleCI project and pipeline lookup settings.
- QA-routing overrides.
- Resource estimates and maximum project-cell concurrency.

The registry contains aliases, identifiers, and non-secret policy. It must not contain login credentials, tokens, account email addresses, or session transcripts.

## Work intake and private queue

### Queue admission

Hermes may add a Linear issue only after the operator explicitly supplies or unambiguously identifies it in chat. The orchestrator validates that the issue exists and has a registered project before admission.

Hermes must not:

- Scan Linear for unassigned work.
- Pick the next issue from an open project automatically.
- Treat assignment to the operator as permission to start work.
- Expand a queued issue into unrelated issues without additional operator instruction.

Dependencies that are already part of the explicitly queued issue may be tracked for readiness. A newly discovered issue requires explicit admission before work begins.

### Queue ordering

The scheduler ranks admitted work using:

1. Linear priority.
2. Dependency readiness.
3. Queue age.
4. Repository and file-overlap risk.
5. Claude account availability.
6. Current memory, CPU, disk, and project-cell capacity.

The operator can override priority from local chat, the restricted phone console, or an authorized Photon command.

### Linear projection

The orchestrator keeps detailed internal state privately and projects only these states to Linear:

| Internal milestone | Linear state | Assignee |
| --- | --- | --- |
| Explicitly queued, not started | Todo | Operator |
| Claude lead or subagent working | In Development | Operator |
| PR submitted to Codex Merger | Review | Operator |
| Ordinary issue merged | Done | Operator |
| QA-designated issue merged and awaiting QA | QA | Ryan |
| QA issue returned with defects | In Development | Operator |
| QA issue accepted | Done | Operator |

Hermes does not add orchestration comments, labels, stall notes, account information, or handoff records to Linear. GitHub remains the detailed record for commits, pull requests, reviews, and CI.

Only an issue originally assigned by Ryan from QA, or an issue explicitly marked for QA by the operator, follows the QA path. The orchestrator privately records the QA return owner so it can assign the merged issue back to Ryan in `QA`. A QA rejection returns the issue to the operator in `In Development`; after correction and merge, it returns to Ryan in `QA` again.

All external transitions are idempotent. Before writing, the adapter reads the current state and records the source revision or update timestamp. A retry must not duplicate or reverse a newer human action.

## Claude project cells

### Lead ownership

Each active project has at most one persistent Claude lead. The lead is responsible for project-level planning, decomposing queued issues, coordinating subagents, integrating their results, and preparing pull requests for the Codex Merger.

The lead should maximize safe parallelism when issues are independent. It must limit or serialize work when tasks overlap, dependencies are unresolved, or host resources are constrained.

Only one Claude lead session runs per project. Its native subagents inherit the same selected Claude profile and account. An issue-scoped subagent must not become a second project lead.

### Profile pool

The host has four separately paid Claude Max 20x subscriptions. Local configuration assigns each account an opaque profile alias. The aliases and authentication state remain outside Git.

When starting a project cell, the scheduler chooses the least-loaded healthy profile that has sufficient remaining capacity. It then maintains project affinity to that profile.

A running project stays pinned because a Claude session's local history, authentication context, rate-limit state, and in-flight tools belong to the profile that launched it. Moving an active task would either discard context or require unsafe sharing of authentication state. Pinning also makes failures attributable and handoffs reproducible.

The scheduler may rotate a project only when:

- The profile reaches a provider limit.
- Authentication becomes unhealthy.
- The account becomes unavailable.
- The operator explicitly requests rotation.

Rotation requires a complete checkpoint and handoff. The replacement profile acknowledges the handoff and restates the next action before the old session is retired. If all profiles are unavailable, the issue remains queued or paused until the earliest profile recovers, and Hermes notifies the operator.

Account scheduling distributes licensed workloads; it must not falsify identity, bypass provider controls, or move a single live request between accounts.

## Codex Merger

### Control surface

The orchestrator uses Codex App Server as the primary integration. The installed Codex CLI exposes durable thread creation and resumption, structured review operations, streamed turn and item events, interruption, goal state, approval requests, and account and rate-limit information.

Each registered project has one persistent Codex Merger thread identifier. The orchestrator resumes the thread for new work and records every turn identifier and terminal event. Codex CLI non-interactive mode remains a recovery and diagnostic path.

The Desktop app is optional for operator visibility and manual intervention. UI automation is not part of the control plane.

References:

- [Codex App Server](https://learn.chatgpt.com/docs/app-server)
- [Codex non-interactive mode](https://learn.chatgpt.com/docs/non-interactive-mode)
- [Codex SDK](https://learn.chatgpt.com/docs/codex-sdk)

### Responsibilities

The Codex Merger:

- Reviews one submitted PR at a time for its project.
- Verifies the branch, commit, base branch, and acceptance evidence.
- Runs or inspects appropriate local validation.
- Evaluates known CI state under the optimistic policy.
- Reports Critical and Important findings to the Claude lead.
- Merges only when review and merge gates are satisfied.
- Proves that the merged commit is reachable from the integration branch.
- Updates the private orchestration record, after which the Linear adapter projects the correct workflow state.

The Codex Merger must not:

- Implement corrections.
- Amend Claude-authored commits.
- Resolve code conflicts.
- Push repair commits.
- Route implementation defects directly to a subagent while bypassing the Claude lead.

### Correction loop

When review fails, Codex produces a structured correction packet containing:

- Severity.
- Repository, branch, PR, and reviewed SHA.
- Reproduction steps or concrete evidence.
- Violated acceptance criterion.
- Required correction.
- Required tests or verification.

The orchestrator sends the packet to the project's Claude lead and returns the issue to active implementation. A new SHA requires a fresh Codex review. Earlier approval does not carry forward automatically.

### Optimistic CircleCI policy

The Merger does not continuously follow CircleCI to completion.

1. When review and local gates pass, the Merger may merge while the current CircleCI pipeline is unresolved.
2. The orchestrator records the pipeline identifier and the merge associated with it.
3. When the next PR becomes merge-ready, the Merger checks the most recent unresolved prior pipeline once.
4. If that prior pipeline failed, the merge gate closes. The failure is sent to the Claude lead for correction before another merge.
5. At most two merged PRs may have unresolved CircleCI outcomes at once.
6. A known failed pipeline always closes the merge gate until addressed.

The system does not poll merely to watch a pipeline turn green. It reconciles unresolved CI at the next decision point, on operator request, or after a failure notification if one is already available.

## Resource governance

### Admission controller

The resource governor continuously samples:

- macOS memory pressure.
- Resident memory for each managed process tree.
- Swap usage and growth.
- CPU saturation and load.
- Available disk space.
- Repository and worktree disk consumption.
- Active Claude leads, subagents, Codex turns, test runners, and development servers.

It classifies the host as green, yellow, or red. Versioned configuration starts
with conservative hard safety floors and is refined continuously from real
managed workloads. Calibration does not require a waiting period or synthetic
load.

- **Green:** Admit independent work that fits its estimated resource envelope.
- **Yellow:** Stop admitting lower-priority work and ask workers to checkpoint at natural boundaries.
- **Red:** Checkpoint and pause the lowest-priority safe candidates, terminate their managed process trees, and reclaim eligible worktrees.

The scheduler makes admission decisions from current measurements, not the theoretical maximum of four Claude accounts. Four paid accounts permit up to four project leads; they do not require four simultaneous project cells.

### Process ownership

Every launched lead, subagent, Codex turn, test process, server, and sidecar belongs to a recorded process group and lease. Cleanup targets the recorded process group, never a broad name match. The orchestrator reaps orphaned managed processes after restart reconciliation.

### Worktree lifecycle

Worktrees are disposable execution environments:

```text
allocate -> work -> checkpoint -> push -> verify remote -> record PR/state
         -> stop managed processes -> remove worktree -> prune metadata
```

There is no default 24-hour retention period. At the current work rate, completed worktrees must be reclaimed immediately after their contents are recoverable.

Before removal, the custodian must verify all of the following:

- The worktree is no longer leased to an active worker.
- No managed process has the worktree as its current directory or data path.
- There are no unexplained dirty or untracked files.
- All required commits have been pushed.
- The expected remote reference contains the checkpoint or final commit.
- The PR and orchestration state identify the recoverable branch and SHA.

If resource pressure requires reclaiming incomplete work, the Claude lead may create and push a clearly labeled WIP checkpoint such as `wip(ENG-431): checkpoint before resource cleanup`. The custodian verifies the remote commit, records the next action in the handoff, stops related processes, removes the worktree through `git worktree remove`, and prunes stale metadata.

The custodian never uses raw recursive deletion as the normal worktree-removal mechanism. Dirty, untracked, uncommitted, or unpushed state blocks removal until it is either checkpointed or explicitly resolved by the operator.

## Stall management

### Detection

A worker is potentially stalled when one or more evidence sources indicate a lack of useful progress:

- No material Git, PR, test, tool, or task-state change within the activity window.
- Repeated identical failures or commands.
- A blocked tool or approval request.
- Provider limit or authentication errors.
- Dependency, conflict, CI, environment, or resource failures.
- The worker reports uncertainty or inability to continue.
- A process remains alive without meaningful output or resource use.

Elapsed time alone is not sufficient to classify an active long-running test or build as stalled. The classifier combines process state, output, Git state, external status, and agent self-report.

### Diagnose, consult, learn, automate

For each normalized stall reason, Hermes follows this cycle:

1. Collect evidence and classify the likely cause.
2. Propose a bounded remedy and its risks.
3. Ask the operator on the first occurrence.
4. Ask again on the second occurrence and refine the remedy from the result.
5. Save the approved playbook with its evidence predicates, permitted actions, verification, timeout, and rollback.
6. Apply the playbook automatically on later matches while reporting the action.

Playbooks are global by default and can have project-specific overrides. Hermes must still ask for credentials, spending, material external communication, destructive exceptions, or actions outside an existing authorization boundary, even if a similar playbook exists.

## Context and session handoffs

The orchestrator uses several signals because no single percentage reliably measures context health:

- Context utilization reported by the worker interface.
- Compaction events.
- Rapid context refill after compaction.
- Active session age.
- Repetition, lost decisions, inconsistent plans, or failure to use recent evidence.

Behavioral symptoms require corroboration from another signal before forced rotation unless the worker reports a context error.

Default policy:

- Below 70% context: healthy.
- From 70% through 80%: prepare a handoff at the next reasonable boundary.
- Above 80%: mark rotation pending and stop assigning new subwork.
- First compaction: record the event and prepare for rotation.
- Repeated compaction or rapid refill: treat as strong rotation evidence.
- Six active hours: require a complete handoff at the next reasonable stopping point.
- Context failure: checkpoint and hand off immediately.

Idle time does not count toward the six-hour limit. A reasonable stopping point includes a commit, completed test cycle, PR update, subtask boundary, or stable diagnosis.

A complete handoff contains:

- Objective and current status.
- Decisions and constraints.
- Branch, commits, PR, and modified files.
- Tests run and results.
- Blockers and remaining work.
- Important commands, logs, and environment details.
- Risks and unresolved questions.
- The next concrete action.

The replacement session must acknowledge the handoff and restate the next action before the orchestrator retires the original session.

## Remote operations

### Network boundaries

The full Hermes dashboard remains bound to `127.0.0.1`. It is not exposed through Tailscale because its administrative routes include provider configuration, API keys, MCP management, sessions, and logs. Local plugin routes must not be assumed to inherit dashboard authentication.

A separate restricted operations service binds locally and is published only with Tailscale Serve. It requires application-level authentication in addition to tailnet membership. Tailscale Funnel and public ingress are prohibited.

Reference: [Tailscale Serve](https://tailscale.com/docs/features/tailscale-serve)

### Restricted phone console

The iPhone console can:

- View queues, projects, workers, resource pressure, stalls, PRs, and known CI state.
- Enqueue an explicitly supplied Linear issue.
- Pause, resume, retry, or reprioritize admitted work.
- Approve ordinary stall remedies and handoffs.
- Request safe cleanup or a WIP checkpoint.
- Open the corresponding Linear issue or GitHub PR.

It cannot:

- Read or modify provider credentials or account authentication.
- Read raw environment variables or secret files.
- Manage arbitrary MCP servers.
- Run an unrestricted shell.
- Disable cleanup safety checks.
- Perform exceptional destructive actions.
- Approve spending or change provider plans.

Every mutating action uses a short-lived confirmation token, an idempotency key, and an audit event.

### Photon and Apple Watch

Photon supplies notifications and a narrow command grammar through iMessage. The system preserves the Messages account already signed into the Mac because Photon does not require BlueBubbles to control that local account.

The Photon profile:

- Accepts commands only from the operator's allowlisted phone number.
- Rejects group conversations.
- Keeps telemetry disabled.
- Exposes only status, queue, pause, resume, approve, retry, priority, checkpoint, and cleanup intents.
- Requires confirmation for mutating commands whose target is ambiguous.
- Never exposes raw logs, secrets, shell access, account configuration, or unrestricted Hermes tools.

Photon is not treated as end-to-end encrypted transport to the local Hermes process because the managed Photon service can process message content. Messages over this channel must remain operational and low sensitivity. Sensitive administration remains local.

## Components

The repository will contain these logical components:

- **Domain engine:** task, project-cell, worker, review, CI, QA, handoff, and cleanup state machines.
- **Supervisor daemon:** schedules work, manages leases, reconciles state, and dispatches adapters.
- **Hermes interface:** narrow tools or commands for queueing, status, diagnosis, approval, and policy management.
- **Claude adapter:** launches profile-scoped Claude Code sessions in cmux or another managed terminal, collects events, and manages handoffs.
- **Codex adapter:** manages project Merger threads through Codex App Server and falls back to non-interactive CLI recovery.
- **Linear adapter:** performs the minimal state and assignee projection.
- **GitHub and CircleCI adapters:** observe PR, merge, ancestry, and pipeline state.
- **Resource governor:** samples the host and controls admission and pausing.
- **Git custodian:** allocates, verifies, removes, and prunes worktrees.
- **Restricted operations API and web console:** provides the Tailscale-only phone surface.
- **Photon gateway:** maps allowlisted message commands to restricted operations.
- **Audit and observability layer:** records decisions, evidence, adapter calls, and state changes without storing secrets.

The implementation should favor narrow adapters with fake implementations so state-machine behavior can be tested without using live provider accounts.

## Durable state

Runtime state lives outside Git in a transactional SQLite database under `~/.local/share/hermes-orchestrator/`. The precise schema is an implementation detail, but it must represent:

- Admitted issues and queue priority.
- Projects, cells, workers, sessions, and profile leases.
- Claude session and Codex thread identifiers.
- Branches, SHAs, PRs, reviews, merges, and remote verification.
- Linear projection state and idempotency records.
- CircleCI unresolved-window state.
- QA origin and return owner.
- Resource samples, process groups, and worktree leases.
- Stall evidence, consultations, and playbook versions.
- Context measurements, checkpoints, and handoffs.
- Remote commands, confirmations, and audit events.

The database uses write-ahead logging, schema migrations, foreign keys, and unique idempotency constraints. An append-only event journal records the cause and evidence for every material state transition. Derived views can be rebuilt from authoritative entities and events.

Configuration and approved playbooks are versioned in Git as YAML. Runtime leases, user identifiers, account identities, transcripts, tokens, and the database are not committed.

Secrets should use macOS Keychain when the dependent tool supports it. If a tool requires a file, the file remains outside the repository with owner-only permissions and only its path or opaque alias appears in configuration.

## Failure handling and reconciliation

On startup, the supervisor enters reconciliation before scheduling new work. It:

1. Reads incomplete state-machine transitions and leases.
2. Inspects recorded process groups and live sessions.
3. Verifies Git branches, worktrees, remote SHAs, and PR state.
4. Reads current Linear state before applying any pending projection.
5. Reads known GitHub merge and CircleCI state.
6. Resumes or safely closes Codex and Claude session records.
7. Marks uncertain actions for operator attention.
8. Opens admission only after invariants pass.

If state cannot be reconciled confidently, the system fails closed. Existing workers may reach a safe stopping point, but the supervisor does not start work, merge, change Linear, or remove worktrees until the inconsistency is resolved.

Adapter calls use timeouts, bounded retries with jitter, idempotency keys where supported, and circuit breakers. A provider outage pauses affected transitions without blocking unrelated healthy project cells.

## Observability

The local dashboard and restricted phone console derive status from the durable state rather than scraping terminal panes. They show:

- Queue and project-cell state.
- Current lead, profile alias, subagent count, and session age.
- Context and handoff readiness.
- Resource pressure and largest managed processes and worktrees.
- PR review and merge gates.
- Known CI outcomes and unresolved merge window.
- Stall evidence and the selected playbook.
- Cleanup eligibility and last verified remote checkpoint.

Logs use structured events with correlation identifiers for issue, project, worker, session, PR, and transition. Secret values and message bodies are redacted. Retention is bounded and configurable so observability does not become a disk-pressure source.

## Rollout

### Foundation and observation

Create the repository, domain model, SQLite state, project registry, event journal, resource sampler, fake adapters, and dry-run mode. Observe existing workflows without controlling them and establish conservative green, yellow, and red bootstrap thresholds.

### Queue and Claude profiles

Enable explicit chat intake, Linear projection, profile health and leases, one lead per project, subagent supervision, checkpoints, handoffs, and account rotation. Continue refining the resource profile from actual worker measurements during normal use.

### Codex Merger

Enable persistent project-specific App Server threads, structured review packets, merge verification, optimistic CircleCI windows, and QA routing.

### Resource and recovery automation

Enable admission control, stall playbooks, context rotation, six-hour handoffs, managed process cleanup, WIP checkpointing, and immediate worktree reclamation.

### Remote operations

Publish the restricted phone console through Tailscale Serve, initially read-only. Enable mutating operations individually after authentication, authorization, confirmation, idempotency, and audit tests pass. Add Photon notifications before enabling Photon commands.

Each phase has an explicit dry-run period and acceptance gate. A phase cannot receive broader authority merely because the prior phase was deployed.

## Acceptance criteria

The system is ready for routine use when all of the following are demonstrated:

- An unqueued Linear issue cannot be started through scanning, polling, or an ambiguous chat message.
- An explicitly queued issue follows the approved Linear state and assignee projection.
- Two issues in one project use one Claude lead, while independent subagents can run safely under it.
- A project remains pinned to one healthy Claude profile and rotates only through an acknowledged handoff.
- Exhausting all four Claude profiles queues work and notifies the operator without losing state.
- A submitted PR moves to `Review`, receives an independent Codex review, and returns defects to the Claude lead.
- Codex cannot create corrective commits through the Merger role.
- The optimistic CircleCI policy permits no more than two unresolved merged PRs and closes on a known failure.
- Ordinary merged work reaches `Done`; designated QA work returns to Ryan in `QA`; a QA rejection returns to the operator in `In Development`.
- A supervisor restart does not duplicate a Linear transition, review, merge, or cleanup.
- A six-active-hour or high-context session produces a complete, acknowledged handoff.
- A recurring stall remedy is consulted on twice and then applied automatically only when its evidence predicate matches.
- Under resource pressure, lower-priority work checkpoints and pauses before the host becomes unusable.
- A completed or WIP-checkpointed worktree is removed only after remote reachability is verified.
- The restricted phone console cannot access a secret, unrestricted shell, or destructive override.
- A non-allowlisted Photon sender or group conversation cannot invoke an operation.
- The full Hermes dashboard remains unreachable from the tailnet-facing endpoint.

## Risks and mitigations

### Provider interface changes

Claude Code, Codex App Server, Linear, GitHub, CircleCI, Photon, and Tailscale can change. Adapters must isolate version-specific behavior, expose health checks, and fail closed on incompatible responses.

### Agent-reported state is incomplete

The orchestrator corroborates agent statements with Git, process, PR, CI, and durable-state evidence before destructive cleanup or workflow completion.

### Account or session ambiguity

Profile aliases are explicit, and every worker lease records its profile. The scheduler does not infer identity from terminal titles or environment residue.

### Resource estimates drift

The governor uses live measurements, records actual high-water marks, and updates admission estimates conservatively. Static concurrency limits remain safety caps, not targets.

### Remote channel compromise

The remote surfaces are separately authenticated and authorized, expose no general-purpose execution, require confirmation for ambiguous mutations, and can be disabled without stopping local orchestration.

### Cleanup races

Worktree leases, process ownership, remote-SHA verification, and transactional cleanup state prevent removal while a worker or retry still depends on the worktree.

## Deferred decisions

Implementation planning must resolve these details without changing the approved behavioral contract:

- The exact programming language and web framework for the durable service and phone console.
- The Claude Code event-capture mechanism supported by the installed version.
- Conservative initial resource safety floors and the policy for tuning them
  from live managed-workload observations.
- The exact SQLite schema and migration library.
- The supported Linear and GitHub authentication mechanisms available locally.
- Whether the Codex adapter uses a managed App Server daemon or a dedicated stdio child process in the first release.
- The Photon onboarding values and the operator's allowlisted phone number, entered interactively and never committed.
