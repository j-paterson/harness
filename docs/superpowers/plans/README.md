# Hermes orchestrator implementation plans

These plans implement the approved [Hermes orchestration system design](../specs/2026-08-26-hermes-orchestration-system-design.md) in reviewable phases. Complete each phase and its exit gate before enabling the next phase.

## Execution order

1. [Foundation and observation](2026-08-26-hermes-orchestrator-foundation.md)
   - Creates the Python package, validated configuration, SQLite event journal, private queue, fake adapters, dry-run scheduler, resource observation, and restart reconciliation.
   - Performs no live provider mutations.
2. [Claude leads and Linear projection](2026-08-26-hermes-orchestrator-claude-linear.md)
   - Creates four isolated first-party Claude Max profiles, one persistent lead per project, resumable Claude Code turns, minimal Linear projection, and acknowledged profile rotation.
3. [Codex Merger](2026-08-26-hermes-orchestrator-codex-merger.md)
   - Adds read-only Codex App Server Merger threads, structured correction packets, deterministic GitHub merging, optimistic CircleCI handling, ancestry proof, and QA routing.
4. [Resource governance and recovery](2026-08-26-hermes-orchestrator-resource-recovery.md)
   - Adds managed process groups, calibrated admission, context rotation, learned stall playbooks, verified WIP checkpoints, immediate worktree cleanup, and crash reconciliation.
5. [Restricted remote operations](2026-08-26-hermes-orchestrator-remote-operations.md)
   - Adds the independently authenticated phone console, loopback launchd services, Tailscale Serve, restricted Photon commands, and Apple Watch verification.

## Authority progression

| Phase | External authority |
| --- | --- |
| Foundation | Read-only observation and local SQLite writes |
| Claude and Linear | Start profile-scoped Claude turns and project approved Linear state and assignee fields |
| Codex Merger | Review, merge through the GitHub adapter, reconcile CI at decision points, and route QA |
| Resource recovery | Checkpoint, stop leased processes, and remove remotely recoverable worktrees |
| Remote operations | Invoke the already-approved narrow operations from authenticated remote surfaces |

An exit gate grants only the authority described by the following plan. It does not waive credential, spending, destructive-exception, or public-exposure restrictions.
