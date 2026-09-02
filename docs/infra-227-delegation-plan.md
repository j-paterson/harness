# INFRA-227 delegation plan — reactivate explicitly reopened completed work

Authored by the Fable lead (session `5ffd1831-…`, cell `83b52ed1-…`,
profile max-c). Base: `feature/infra-227` at `4ba49fc` (origin/main),
worktree `/Users/josystem/hermes-orchestrator-issue-INFRA-227`. Issue:
[INFRA-227](https://linear.app/jo-solutions/issue/INFRA-227).

## Lead-verified anchors (base 4ba49fc)

- `QueueService.admit` (queue.py) refuses any existing `admitted_issues`
  row with "is already admitted", regardless of its state; `retry` only
  accepts paused/blocked rows. A reopened Linear issue whose local row is
  `done` (INFRA-215) therefore cannot be routed.
- `HermesCommandService._queue_issue` (hermes_tools.py) maps
  `AdmissionDenied` to `admission_denied`; the service has no Linear read
  port. `runtime.py` already constructs `LinearIssueReader` as
  `linear_reads` for reconciliation.
- Once a row is `queued`, the existing scheduler/dispatch path publishes
  the assignment to the active project lead — no new wake or seat.

## Packet

| Packet | Boundary | Files | Tier | Wave |
|---|---|---|---|---|
| (single) | `queue_issue` on an existing `done` row reactivates it atomically to `queued` with the new instruction, priority, and readiness plus an `issue.reactivated` event, only when the Linear read shows a non-terminal state and project identity matches; refuses when Linear is Done/cancelled, the row is already non-terminal, identity mismatches, or no Linear reader is available; repeating the same instruction is idempotent | queue.py, hermes_tools.py, cli.py (wiring only), tests | Sonnet | 1 |

Out of scope: new intents or workflow states, changes to retry, Linear
mutations, scheduler changes.
