# INFRA-230 delegation plan: refuse targeting terminal Linear issues, reconcile stale rows

Lead-owned plan (Fable, cell 83b52ed1, session 72789518). No schema change.

## Shape

- `target_issue` / `target_harness_followup` take an optional Linear read port.
  When present, the issue's live Linear status is read BEFORE the targeting
  transaction opens; a terminal status (`queue._LINEAR_TERMINAL_STATUSES`) or a
  failed read refuses with zero writes. The CLI wires the existing lazy
  Keychain-backed reader.
- A new explicitly-mutating pass (`linear_reconcile.py`) walks non-terminal
  `admitted_issues`, reads each issue's Linear status in its own try/except,
  and moves rows whose Linear state is terminal to `done` through
  `QueueService.transition_if` (which already closes work claims), then
  releases the issue's worktree lease through the existing checkpoint ->
  verify -> reclaim chain, fail-soft. Unavailable reads isolate that issue.
  The read-only `Reconciler` is untouched.

## Packets

| # | Scope | Files | Depends |
|---|-------|-------|---------|
| A | Linear-state gate in targeting + CLI wiring | issue_targeting.py, cli.py (target-issue handler), tests/test_issue_targeting.py, tests/test_cli.py | — |
| B | `LinearQueueReconciler` pass | linear_reconcile.py, tests/test_linear_reconcile.py | — |
| C | `reconcile` CLI runs the pass; lead-direct if small | cli.py (reconcile handler), tests/test_cli.py | A, B |
