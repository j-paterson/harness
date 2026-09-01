# INFRA-223 — stop historical submissions holding the Sol thread

Assignment `a14b6b26`, instruction
`codex-goal:exact-sol-queue-start:20260901`.

## Scope — corrected by Sol c9b74c0b/e09ff825

My first pass took only the last section, reading it as the admitted
slice because the issue labels it a "Required narrow correction". That
scoping was WRONG: the issue's completion scope is **cumulative**, and
narrowing it left the operator-visible MVP contract unmet.

Sol's Critical correction directs: continue with bounded follow-up
commits implementing the omitted cumulative requirements, preserve the
stale-submission fix already delivered, keep one primary Sol writer and
one PR at a time, use `codex queue --thread` for routine wakes, add no
transport, and **backlog the audit-event atomicity hardening** (Sol
c9b74c0b) since it risks no incorrect or lost work, security, or
operator usability today. That backlogged work has been discarded from
the tree, not shipped half-done.

Remaining cumulative requirements, to land as bounded commits:

1. **Exact queue start and recovery.** Codex experimental thread-queue
   APIs behind an explicit capability handshake; after a turn completes
   or is interrupted, list the exact thread queue and start the exact
   idle head; a queued candidate never interrupts an active turn;
   durable queued/started/settled truth so a successful `codex queue`
   exit is never itself proof of a started turn; recover an interrupted
   or unstarted head on reconnect; deduplicate by durable identity.
2. **Recovered-turn role capability.** The primary Sol role's per-turn
   sandbox policy — workspace-scoped writes, approval mode `never`,
   network sufficient for GitHub PR/merge and the guarded Hermes path —
   and Hermes resolved through the recorded active runtime identity,
   never a stale checkout `.venv`.
3. **Bounded helper lifecycle.** The provisioning/recovery app-server
   exits or is reclaimed by exact process lease once no turn is active,
   leaving the pinned thread openable in Codex.

Honest limit: items 2 and 3 carry acceptance that is operator-visible
against the real Codex app (the pinned thread opening without
"open in another app", a recovered turn creating the PR itself). Those
can be driven and asserted in tests, but the final live proof needs the
real app and is not something this lead can assert headlessly.

## What was measured before planning

| check | result |
| --- | --- |
| `submitted_verdicts` rows in state `submitted` | **zero** — all 78 are `settled` |
| the two rows the issue names (INFRA-212, INFRA-220) | already `settled` |
| Hermes-owned lingering `app-server` helper | **none**; the two live `app-server` processes are children of Codex.app's own `node_repl` |

So the observed instance has already resolved. The **code defect has
not**: `MergerTurnService.has_pending_submission` is still

```sql
SELECT 1 FROM submitted_verdicts WHERE project_key = ? AND state = 'submitted'
```

with no reconciliation against the submission's own wake or review. Any
future submission left `submitted` while its exact wake and review are
terminal will again make `MergerSession.review_active()` report live
review work and reopen an App Server over an idle thread.

This must be stated plainly: the fix is **preventive**. It can be
proven in tests against both observed shapes, but it cannot be
demonstrated against current durable state, because the state it
repairs no longer exists. Any claim that startup "repaired the two
observed rows" would be false.

## The change

Before a verdict submission is treated as active review work, reconcile
it against its own exact wake and review:

- if the submission's wake is terminal AND its review is terminal, the
  submission is historical: mark it settled with a derived terminal
  result, durably, and exclude it from active ownership;
- otherwise it is genuinely live and is left exactly as it is.

`has_pending_submission` must not report a historical submission as
active review work. Reconciliation is derived strictly from the durable
wake and review rows — never from transcript text, and never inferring
a verdict that was not submitted.

Preserve, unchanged: a genuinely live `submitted` row still holds
review active; review and merge are never re-run; no approval is ever
inferred.

## Tests

- both observed shapes — a `submitted` row whose wake is `completed`
  and whose review is `merged` — are reconciled to settled at startup,
  with no review or merge re-run and no new PR;
- after reconciliation `has_pending_submission` is false and
  `review_active()` reports no active work, so no App Server is opened;
- a genuinely live submission — wake `delivered`/`admitted`, or review
  not terminal — is preserved and still holds review active;
- reconciliation is idempotent across a restart;
- no verdict is ever derived from transcript text.
