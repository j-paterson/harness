# INFRA-198 — derive the handoff's issue and branch from the seat

Assignment `555e8270`, instruction
`infra-198-harness-handoff-derivation-20260901`. Fix only this
derivation. No new table, no protocol, no multi-issue redesign.

## Observed

`submit-handoff` for the live harness cell
`c2e9ec07-864c-4926-975e-b8853b7cdf61` (session
`79002136-eac3-4f56-b9f2-1408e066a064`) refuses with
`handoff field is incomplete: branch`, so the rotation proof cannot
proceed. Both facts it needs are already durable: the cell's lead
assignment names `INFRA-198`, and that issue's active worktree lease
names `feature/infra-198-harness-trust`.

## Root cause — two derivations that ignore the seat

In `_submit_handoff` (`cli.py`):

```python
worktree = _worktree_state(project.lead_cwd)
issue_rows = ...WHERE project_key = ? AND state IN ('in_development','review')
issue_id, issue_state = (rows[0]…) if len(issue_rows) == 1 else (None, None)
… branch=worktree.branch
```

1. **Branch/head come from `project.lead_cwd`** — the coordinator's own
   checkout, which is currently detached, so `branch` is `None` and the
   field is reported incomplete. The seat's real tree is its issue's
   leased worktree, not the coordinator's.
2. **The issue is chosen only when the whole PROJECT has exactly one
   active issue.** There are four (`INFRA-212`, `INFRA-215`,
   `INFRA-220`, `INFRA-221`), so it yields `None` — even though the
   cell's own assignment names its issue unambiguously.

Neither derivation consults the cell being handed off, which is the
one thing that actually determines the answer.

## The change

Resolve from the seat, in this order, in `_submit_handoff`:

1. Take the target cell/session's **latest durable lead assignment** and
   use its `issue_id`.
2. Take that issue's **single active worktree lease** and probe
   `branch`/`head` from the leased path instead of `project.lead_cwd`.
3. **Fail closed on zero or ambiguous identities** — no assignment, or
   not exactly one active lease for that issue — with one actionable
   message naming what was missing. Never guess and never fall back to
   the coordinator's tree.
4. **Preserve the existing single-issue fallback** where it is still
   valid: a project with exactly one active issue and no assignment for
   the cell keeps working exactly as it does today.

## Tests

- two active project issues plus a detached coordinator: the harness
  cell still submits a handoff, carrying the issue from its own
  assignment and the branch from that issue's lease;
- ambiguity refuses: no assignment for the cell, or an issue with zero
  or several active leases, each with its own actionable message and no
  durable write;
- the single-issue fallback still succeeds unchanged.

## Then

One final gate, one candidate to Sol. After merge, use the same live
`max-c` harness cell to submit the handoff and complete `rotate-lead`.
