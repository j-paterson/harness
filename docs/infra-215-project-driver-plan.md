# INFRA-215 reopened — project driver, six-child cap, harness follow-up

Authored by the Fable lead (session `5ffd1831-…`, cell `83b52ed1-…`,
profile max-c). Base: `feature/infra-215` reset to `ef5c381` (origin/main
after INFRA-227 #106); worktree
`/Users/josystem/hermes-orchestrator-issue-INFRA-215`. Operator
instruction `finish-infra-215-project-driver-20260902-v1`.

## Lead-verified anchors (base ef5c381)

- `OrchestratorService.tick` returns a non-executing plan; the only
  automatic "next transition" is `LeadTerminalWakes.commit_work_ready`
  (polled per tick, idle seats only) and it wakes with `runnable[0]`
  alone. `post_merge._advance_successor` and
  `lead_children.child_completed` only mark readiness; nothing declares
  a batch complete and the dashboard has no objective / runnable /
  next-action / blocker surface.
- `lead_children.child_started` records `started` unconditionally;
  `PacketAdmission._admit` has no concurrency cap; `hook_install`
  installs Stop/SubagentStop/SubagentStart only — no PreToolUse
  `subagent-gate`, so classic seats never reserve packets (the observed
  `planned` vs `started` split).
- `issue_targeting.target_issue` binds its cell lookup to the
  development lane; `_activate_issue_body` conflates harness lane with
  "never claims a product issue"; `start-lane` reports already_running.

## Packets

| Packet | Boundary | Files | Tier | Wave |
|---|---|---|---|---|
| driver | `project_driver.py`: `batch_status` (all admitted issues terminal, no pending candidate/correction/merge, no live lease) with next automatic action and concrete blocker; work-ready wakes carry every runnable issue up to lane headroom; merge reconciliation and child completion trigger immediate replenishment through an injected callable | project_driver.py, lead_wakes.py, post_merge.py, lead_children.py, runtime.py, tests | Sonnet | 1 |
| cap | PreToolUse `subagent-gate` installed for classic profiles and wired to PacketAdmission; at most six `reserved` packets per cell/session enforced inside the reserve transaction; extra packets stay planned | hook_install.py, packet_admission.py, subagent_packets.py, cli.py (subagent-gate handler), tests | Sonnet | 1 |
| harness | one explicitly admitted, dependency-ready follow-up assigned to a harness-bound lead lane-preservingly (same cell/session/profile, no second seat, one live follow-up, fail closed) | issue_targeting.py, cells.py, tests | Sonnet | 1 |
| dashboard | batch objective, active/runnable issues, next automatic action, blocker rendered from `batch_status` | dashboard_sources.py, dashboard_render.py, tests | Sonnet | 2 |

Out of scope: new protocols or intents, changing the six-lane admission
rule, Linear comments, PR creation.
