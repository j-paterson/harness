# INFRA-198 — the bound seat's own assignment, superseded or not

Assignment `a01705f7`, instruction
`infra-198-dual-lane-superseded-handoff-20260901`. One narrow
correction. No schema, no new protocol.

## Observed

On merged `bd779984`, `submit-handoff` for harness cell
`c2e9ec07-864c-4926-975e-b8853b7cdf61` (session
`79002136-eac3-4f56-b9f2-1408e066a064`) refuses:

> no live lead assignment and project 'agent-orchestration' has 4
> active issues …

The cell is still bound to that session, with a confirmed `INFRA-198`
assignment and an active `INFRA-198` lease on
`feature/infra-198-harness-trust`. Its assignment reads `superseded`
only because the DEVELOPMENT cell later took an assignment for the same
issue.

## Root cause — my own filter, and the wrong reading behind it

`_submit_handoff` resolves the seat's assignment with:

```sql
WHERE cell_id = ? AND session_id = ? AND state != 'superseded'
```

I added that clause deliberately, reasoning that a superseded packet is
not a live claim on the seat. That reading is wrong. Supersession here
is per-ISSUE, not per-cell: a later assignment of the same issue to a
different cell supersedes the earlier row while saying nothing about
whether the earlier CELL is still bound and working. Filtering on it
discards the one durable fact that identifies this seat.

## The change

Drop the `state != 'superseded'` predicate. For the exact bound
`cell_id` + `session_id`, take the LATEST durable assignment whatever
its state — the existing `ORDER BY created_at DESC, rowid DESC LIMIT 1`
already guarantees a newer assignment for that same cell/session wins,
which is the property that must be preserved.

Everything downstream is unchanged: the resolved issue must still have
exactly one active worktree lease, and zero/ambiguous identities still
fail closed with the actionable message. A wrong or stale cell/session
still finds no row and still refuses.

## Test

The real dual-lane sequence: the harness cell's `INFRA-198` assignment
is acknowledged, the same issue is then assigned to another cell and
supersedes it, and the originally bound harness cell must still derive
`INFRA-198` and its leased branch. Plus the guards that must not
regress — a newer assignment for the same cell/session wins, a
wrong/stale cell-session refuses, and an ambiguous lease refuses.

## Gate

Focused `submit-handoff` tests, then one full gate, then one candidate
to Sol.
