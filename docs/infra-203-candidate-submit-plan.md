# INFRA-203 — one deterministic candidate submission

Assignment `0dab72dc`. Collapse publication into one transition driven
by the exact candidate commit.

## Why this is the same defect twice already fixed today

The definition of done says: *"Hermes resolves the bound project lead
worktree from durable state and captures its exact HEAD. It never uses
the caller's working directory as candidate identity."*

That is the wrong-tree defect repaired twice today in the same file —
`submit-handoff` derived branch/head from the detached
`project.lead_cwd`, and `rotate-lead` measured that same coordinator
tree for a lane it was not rotating. Publication still has it: today's
publishes only produced correct manifests because the operator happened
to `cd` into the right worktree first, and one earlier candidate was
bound to the wrong SHA precisely when that assumption broke.

So the derivation already exists and is already reviewed. `_seat_lane`
in `cli.py` resolves a cell/session's latest assignment to its issue and
that issue's single active worktree lease; `resolve_lane` is the
canonical fail-closed one-lease reader. This should REUSE them, not add
a third derivation.

## The change

One command, `candidate submit <issue>`, that:

1. Resolves the bound lead worktree for that issue from durable state
   through the existing `_seat_lane`/`resolve_lane` path — never
   `Path.cwd()`, never `project.lead_cwd`.
2. Captures that worktree's exact HEAD as the candidate identity, and
   derives branch, base, changed files and the immutable candidate
   record mechanically from it.
3. Refuses, with one concise actionable message and zero durable
   writes, when the tree is dirty or the commit is not pushed and
   reachable on the remote. These are the only two gates: one dirty
   check exists so intended work is never silently omitted.
4. Requires NO caller-supplied input beyond the issue: no verification
   flags, manifest path, evidence document, provenance binding, or
   packet attribution. The manifest stays an internal transport
   artifact that Fable neither authors nor audits.

The existing `candidate-ready` path keeps working; this is one
deterministic entry point over the same emission, not a second
implementation of it.

## Explicitly removed as requirements

Claimant-freshness audits, per-path agent attribution, provenance
bindings, evidence documents, mandatory verification receipts, and
reconstructed delegation history. None of them improve the merge
decision. Sol runs one final full gate on the exact candidate and CI
remains authoritative; identical checks are not rerun to manufacture
evidence.

## Tests

- submission from an UNRELATED working directory still produces the
  bound lane's HEAD, branch and base — the regression that proves cwd
  is not identity;
- a dirty bound worktree refuses with zero durable writes;
- an unpushed or unreachable commit refuses with zero durable writes;
- the command takes only the issue: no verification flag is required
  and none is accepted as candidate identity;
- the emitted record is byte-equivalent to what the existing path
  produces for the same commit, so this is one transition and not a
  fork.
