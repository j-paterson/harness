# INFRA-223 reopened — 2026-09-02 Sol turn recurrence (INFRA-228)

Authored by the Fable lead (session `5ffd1831-…`, cell `83b52ed1-…`,
profile max-c). Base: `feature/infra-223` reset to `ef5c381`
(origin/main). Operator instruction
`recover-infra-228-sol-turn-via-infra-223-20260903-v1`.

## Lead-verified anchors (base ef5c381, live store 2026-09-03T00:10Z)

- `CodexQueueDelivery._start_queued_head` (codex_queue.py ~337-378)
  opens a fresh bounded app-server connection and calls only
  `thread/queue/list` and `thread/queue/start`; the Sol policy
  (`approvalPolicy: never`, `sandbox: workspace-write`, guard config) is
  applied only on `thread/start` and `thread/resume` in codex_merger.py,
  never on the bounded connection — so the queued turn can run with a
  restricted sandbox and Sol cannot open the PR or submit the verdict.
- `MergerTurnService.handle_turn` returns `awaiting_submission` when a
  turn completes without a structured verdict, but
  `MergerSession._default_listen` discards it; the wake row stays
  `delivered`, `outstanding_wake` keeps returning it, `review_active()`
  stays true and the persistent helper is never released.
- Live: harness-lab wake `fable_ready-infra-228-…-20260902T235720` is
  `delivered` since 23:57Z, no reviews/verdict rows exist, process lease
  `3c0f37d2…` (`codex_app_server`, PID 73653) is still active.

## Packets

| Packet | Boundary | Files | Tier | Wave |
|---|---|---|---|---|
| policy | before `thread/queue/start` on the bounded connection, send the same `thread/resume` policy the persistent path uses (sandbox workspace-write, guard config, approval never) so the queued turn runs under the primary Sol role | codex_queue.py, tests/test_codex_queue.py | Sonnet | 1 |
| stalled | a turn that completes with no structured verdict marks the exact wake `stalled` with a durable event, which frees `review_active()` so the helper is released; one retry action re-delivers the exact wake | merger_turns.py, merger_session.py, tests/test_merger_turns.py, tests/test_merger_session.py | Sonnet | 1 |

Out of scope: new transports, new RPC methods, protocol generalisation,
inferring verdicts from transcript text.
