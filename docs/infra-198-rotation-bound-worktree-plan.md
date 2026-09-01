# INFRA-198 — rotate the seat's own worktree, not the coordinator's

Assignment `a6ce7a4e`, instruction
`infra-198-rotation-bound-worktree-20260901`. One narrow correction to
rotation's worktree SELECTION. No schema, no protocol, no architecture.

## Observed

On merged `d8769b7`, `submit-handoff` now succeeds for harness cell
`c2e9ec07-864c-4926-975e-b8853b7cdf61`, but `rotate-lead` refuses its
precondition — `project HEAD does not match the pushed origin branch
head` (and, in an earlier run against the same tree, `project worktree
has uncommitted changes`).

Neither describes the rotating lane. The harness cell's bound INFRA-198
lease points at `/Users/josystem/hermes-orchestrator-issue-INFRA-198`,
which is clean, on `feature/infra-198-harness-trust`, at pushed
`2a85585`. What both checks actually measured is
`/Users/josystem/hermes-orchestrator-live` — the COORDINATOR's tree,
detached at `ec7ade4` and carrying an untracked
`.sol-integration-infra-198/` workspace that belongs to Sol.

## Root cause

`_compose_lead_rotation` closes over the project's canonical lead cwd
unconditionally:

```python
lead_worktree = project.lead_cwd
… worktree_state=lambda _project_key: _worktree_state(lead_worktree)
```

The probe is keyed by PROJECT, so every cell of the project is judged by
one shared checkout regardless of which lane is rotating. This is the
same wrong-tree defect just fixed in `_submit_handoff`, surviving one
layer up in the rotation precondition: a third party dirtying an
unrelated checkout can block a clean lane's rotation, and a coordinator
left detached makes the head comparison meaningless.

## The change

Select the rotating cell's own worktree, by the derivation
`_submit_handoff` already uses:

1. the target cell's latest assignment for its exact `cell_id` +
   `session_id`, whatever its delivery state (supersession is
   per-issue — see the preceding correction);
2. that issue's exactly-one active worktree lease;
3. run the EXISTING dirty and head-vs-origin checks against that path.

Nothing about those checks changes — only which tree they measure.

Fail closed on zero, ambiguous, or mismatched identity, with the
refusal naming what was missing. A different cell's lease never
qualifies: the lease must belong to the issue the rotating cell is
actually assigned.

Preserve the legacy `project.lead_cwd` probe **only** for a genuinely
unassigned single-lane caller, so existing single-lane rotations behave
exactly as they do today.

## Tests

- harness rotation proceeds while the project coordinator is stale and
  detached, because the bound issue lease is clean and pushed;
- a dirty bound issue lease still refuses;
- an origin-mismatched bound issue lease still refuses;
- another cell's lease never qualifies for this cell;
- the unassigned single-lane fallback still uses `project.lead_cwd`.

## Gate

Focused rotation tests, one full gate, one candidate to Sol.
