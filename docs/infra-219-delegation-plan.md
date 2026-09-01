# INFRA-219 delegation plan — separate development and harness Fable cells

Authored by the Fable lead (session `8f880073-…`, cell `b29691ef-…`)
as the project-level coordinator. Base: `feature/infra-219` at
`74518c2` (origin/main = ACTIVE runtime), isolated worktree
`/Users/josystem/hermes-orchestrator-infra-219`. Issue:
[INFRA-219](https://linear.app/jo-solutions/issue/INFRA-219) — one
development cell and one harness cell per project, each with its own
role, worktree, session, workspace, lease, and durable binding;
Hermes alone launches/binds/rotates/recovers both; scheduler
uniqueness becomes (project, lane role).

## Lead-verified anchors (base 74518c2)

- `ProjectCellService._find_active_cell(project_key)` (cells.py) is
  the one-cell-per-project uniqueness the issue names as the blocker;
  cell creation (`_create_cell`), issue activation, lease restore,
  and terminal-wake publication all key off the project alone today.
- `Scheduler.plan` (scheduler.py) plans per project; its admission
  snapshot has no lane concept.
- The dashboard and launch/rotate surfaces are downstream consumers —
  deliberately wave 2, after the durable model lands.

## Authoritative operating contract (re-read 2026-09-01, expanded)

The issue now carries the long-term two-lead contract, re-read after
the operator's correction (the first read predated the expansion):

- **Development lane**: one Fable project lead per project authors
  all plans and coordinates up to six explicitly admitted issue
  lanes, each in an isolated worktree/branch, implemented primarily
  by bounded subagents — the lead integrates and decides readiness,
  never becomes the sole implementer. Independent issues progress
  concurrently; only the final heavy gate and the sole PR-to-main
  path serialize.
- **Harness lane**: a separate persistent Fable harness lead owns
  operational testing ONLY (restart, rotation, wake/confirm, durable
  recovery, deduplication, end-to-end acceptance) on a stable
  dedicated harness head; it never interrupts, rotates, mutates, or
  reuses the development lead's worktree/session, and never selects
  product issues — findings return as narrow explicitly admitted
  implementation issues.
- **Hermes ownership**: Hermes alone creates, binds, launches,
  rotates, recovers, and cleans up both lead cells; distinct lane
  role, worktree, session, workspace, lease, and durable binding per
  cell; scheduler uniqueness project+lane; dashboard shows both
  leads, their active issue lanes/subagents, current head/event,
  resource pressure, and blockers; one heavy test and one PR to main
  at a time preserved; PR2 untouched.
- **Acceptance** now requires the development lead actively
  coordinating ≥3 isolated issue lanes while one Hermes command
  starts the visible harness lead for INFRA-198, both observable in
  cmux, independent restart/rotation, daemon-restart restoration of
  both bindings, and a harness restart/rotation/recovery proof that
  never pauses development.

Wave-1 impact: NONE — packet L1's durable lane-role model is exactly
the contract's "scheduler uniqueness is project plus lane role" and
"distinct durable binding per cell" foundation, and its boundary
stands unchanged. Wave-2 scope grows: the dashboard must show per-lead
issue lanes/subagents, head/event, resource pressure, and blockers,
and the harness launch command must bind the dedicated harness head.
The lead re-briefs wave 2 against this section when L1 integrates.

## Packets

| Packet | Boundary | Files | Tier | Wave |
|---|---|---|---|---|
| L1 | Durable lane-role model: `project_cells` gains a `lane_role` column ('development' default, 'harness'; migration — next free number at this base), cell uniqueness and every project-keyed lookup becomes (project, lane role), `ProjectCellService` and the scheduler snapshot/plan carry the lane through creation, activation, lease restore, recovery, and terminal wakes; development-lane behavior is byte-compatible with today when no harness cell exists; explicit admission unchanged | `src/hermes_orchestrator/cells.py`, `src/hermes_orchestrator/scheduler.py`, one new migration, `tests/test_cells.py`, `tests/test_scheduler.py` (schema-pin ripples reported, not fixed) | Sonnet | 1 |
| L2 (deferred until L1 integrates) | Harness-lane lifecycle surface: one Hermes command starting/rotating/recovering the harness cell against a stable test head; dashboard shows both lanes | cli.py, dashboard_sources/render, runtime wiring | Sonnet | 2 |

Out of scope: PR2, transport/protocol/provenance, resource-limit
changes (one heavy test at a time stands), INFRA-220's targeting
transition.

Gate: focused suites in this worktree; the coordinator serializes the
one full-suite gate across lanes and owns push + candidate emission.

## L2 integration record and the blocking gap (2026-09-01)

L2's child was timeboxed after writing the implementation but before
its tests; the lead integrated the diff, bumped L1's schema pins
(53 → 54, mechanical), and authored the regressions.

Landed: `start-lane --project <key> --lane development|harness
[--issue <id>]`, lane-scoped `dispatch(..., lane_role=)` with
per-lane dispatch locks, a `lane_project_paths` worktree override
(harness resolves to a sibling `<lead_cwd>-harness` checkout, so the
lanes can never collide on working-tree state), a public
`active_cell(project, lane)` read, and minimal dashboard lane rows
(`LaneCellFact`, one row per live cell: lane role, active issue,
state).

**Blocking acceptance gap — needs its own packet.** Issue occupancy
is still tracked per PROJECT in `admitted_issues`, with no lane
dimension, and both the coarse `project_busy` pre-check and the
transactional activation predicate read it. So while the development
lead occupies the project with its issue, a harness dispatch for a
different issue is refused `project_busy`. INFRA-219's acceptance —
"with the development lead actively working an issue, one Hermes
command starts the visible harness lead" — therefore CANNOT pass
yet. `test_harness_dispatch_is_still_blocked_by_project_occupancy`
pins this actual behavior deliberately (it also proves the refusal
leaves the development lane untouched and launches no harness
process) rather than asserting the desired behavior. Closing it is a
durable-model change (occupancy gains a lane dimension, or harness
dispatch is exempted from product-issue occupancy per the contract's
"the harness lead does not select or implement unrelated product
issues") and is out of L2's boundary.

Also deferred from the contract's dashboard requirement: per-lead
subagents, current head/event, resource pressure, and blockers.
Harness worktree PROVISIONING (creating `<lead_cwd>-harness`) is a
convention here, not yet automated.
