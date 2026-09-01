# INFRA-223 — stop historical submissions holding the Sol thread

Assignment `a14b6b26`, instruction
`codex-goal:exact-sol-queue-start:20260901`.

## Scope of this pass

The issue carries four sections. This pass implements ONLY the last,
which the issue itself labels a **"Required narrow correction"**: the
orphaned `submitted_verdicts` reconciliation that falsely holds the
pinned Sol transcript open.

Deliberately NOT in this pass — each is substantial and none is the
live cause of the observed stale state:

- the Codex experimental thread-queue APIs, capability handshake and
  `thread/queue/start` recovery;
- the per-turn Sol role sandbox policy (approval mode, network for
  GitHub PR/merge, `runtime-exec` identity);
- the bounded provisioning helper's attach/exit lifecycle.

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
