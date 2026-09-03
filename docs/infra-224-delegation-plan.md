# INFRA-224 delegation plan: global operator-decision inbox

Lead-owned plan (Fable, cell 83b52ed1, session 72789518).

## Shape

Reuse `operator_decisions` (migration 0032) and its single `apply()` CAS.
Migration 0061 adds the request context (question, authority reason,
requesting role, facts, options, recommendation, delay impact, paused scope),
an integer urgency, a `request_key` with a pending-only unique index for
deduplication, a category (agent-owned categories are refused at raise time),
and the resolution's `answer` / `next_action`.

Resolution additionally commits one `decision_resolved` terminal wake keyed
by the decision id, so the exact waiting lane wakes exactly once and resumes
from the recorded next action. Pausing stays the existing ad-hoc gate (the
issue keeps its state; dispatch, targeting and candidate publication already
skip an issue with a pending decision), so unrelated work continues.

## Packets

| # | Scope | Files | Depends |
|---|-------|-------|---------|
| A | schema 0061, `DecisionRequest`, `raise_request` (dedup, agent-owned refusal), `inbox` / `next_pending` / `pending_count`, `resolve` | migrations/0061, operator_decisions.py, tests/test_operator_decisions.py, tests/test_db.py | — |
| B | `decision_resolved` wake kind + `DecisionInbox` service (raise, list, next, resolve -> wake once) | lead_wakes.py, decision_inbox.py, tests | A |
| C | hermes-command intents: `raise_operator_decision`, `pending_operator_decisions`, `next_operator_decision`, `apply_operator_decision` extended with answer/next_action; handlers | hermes_tools.py, cli.py, tests | A, B |
| D | dashboard pending count + next-decision summary | dashboard_sources.py, dashboard_render.py, tests | A |

Lead-direct: this document, README note.
