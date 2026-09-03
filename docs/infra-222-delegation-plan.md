# INFRA-222 delegation plan: separate cell work claims from delivery state

Lead-owned plan (Fable, cell 83b52ed1, session 72789518). Packets are bounded,
disjoint-file subagent units recorded in the `subagent_packets` ledger.

## Shape

The redesign is additive. `lead_assignments` keeps its role as the packet
*delivery* ledger (published / acknowledged / superseded). Two new durable
tables carry what the delivery rows were being misused for:

- `work_claims` — durable issue ownership by cell, child lane and role
  (`development`, `harness`, `review`). Role-scoped, never globally exclusive
  by issue. Closed only by explicit issue completion.
- `worker_bindings` — the replaceable worker generation (session, profile,
  cmux surface) serving a stable `project_cells` row. Rotation retires one
  generation and binds the next in one CAS; the cell and its claims survive.

The worktree lease (`worktree_leases`) gains an exclusive-writer identity
(`writer_role`, `writer_ref`, `writer_generation`, `submitted_candidate_sha`)
so the Fable -> Sol -> Fable hand-over is a single CAS on the existing lease
row rather than a second candidate-copy protocol.

Migrations: 0062 (claims + bindings, with backfill), 0063 (lease writer).
0061 is reserved by the sibling INFRA-224 branch; the gap is harmless to
`db.py` (it applies any file not yet in `schema_migrations`). Sol may renumber
at integration if the branches land in the other order.

## Packets

| # | Scope | Files | Depends |
|---|-------|-------|---------|
| P1 | `work_claims` / `worker_bindings` schema, repositories, backfill | migrations/0062, work_claims.py, tests/test_work_claims.py | — |
| P2 | lease writer identity + atomic transfer CAS | migrations/0063, worktrees.py, tests/test_worktree_writer_transfer.py | — |
| P3 | claims opened at activation / targeting; supersession scoped to the exact target; claims closed on completion | cells.py, lead_assignments.py, issue_targeting.py, reviews.py (completion), their tests | P1 |
| P4 | read sites move from `lead_assignments` to claims: `_seat_lane`, `continue_work`, `_release_issue_lease`, `_pick_issue`, capped-seat handoff | cli.py, cmux_surfaces.py, tests | P1, P3 |
| P5 | rotation: `_finalize_transfer` swaps the worker binding; replacement launches in the leased issue worktree; handoff branch/HEAD refreshed from the lease at transfer, stale handoff refused | cells.py (rotation region), lead_rotation.py, handoffs.py, tests | P1, P3 |
| P6 | candidate submit hands the lease writer to Sol with the candidate SHA; REWORK_REQUIRED returns it unchanged; ACCEPT paths keep Sol as sole writer until merge-settle releases for cleanup | emission.py, merger_turns.py, tests | P2 |
| P7 | dashboard: project -> cells -> claims -> worker bindings, delivery states not shown as workflow | dashboard_sources.py, dashboard_render.py, tests | P1 |

Lead-direct: `tests/test_db.py` schema literal (60 -> 63), this document.

## Evidence

Focused regressions per packet plus the existing suites they touch. No
red-first ceremony; a diagnostic red run only where behaviour is uncertain.
