# INFRA-195 live acceptance evidence

Captured 2026-08-30 (UTC) against the production state database
(`~/.local/share/hermes-orchestrator/state.db`, schema 44) and
candidate commits `b8bf678d8b94d475e65cb756374d6a50713f7db8` +
follow-up, per Sol correction `032cd4a562ae4107aa30e085dd5b2165`.
Every claim below is bound to a durable row or event id in that
database; nothing was inferred from terminal observation. The entire
exercise used **zero Computer Use** and wrote **zero bytes into any
interactive terminal prompt** — every step ran through CLI
subprocesses, durable SQLite, launchd, and the Stop-hook offer/ack
drain (no `intake-signal`, no cmux `send`, no `/mcp`, no manual
relaunch, no terminal paste).

## 1. External lifecycle owner (launchd), not cmux

- Prior state (Sol's finding, reproduced): daemon PID 75049 parented
  by the cmux login pane (PGID 74939); terminated 2026-08-30T00:05Z.
- Guarded install (`deploy-install --execute`): journaled steps all
  rc 0; plists installed at
  `~/Library/LaunchAgents/com.josystem.hermes-orchestrator.plist` (+
  operations); job runs the stable wrapper binary
  `~/.local/share/hermes-orchestrator/bin/hermes-orchestrator`
  invoking `runtime-exec`, which resolves the durable activation.
- Parentage proof: the supervised chain roots at launchd —
  `ps -o pid,ppid -p 98856` → `98856 1` (and after the apply below,
  the verified daemon PID 99611's chain likewise roots at PID 1).
  Daemon lifetime is independent of cmux panes, worktree shells, and
  model sessions by construction (no ancestor belongs to any of them).

## 2. Activation identity and journaled apply

- Activation generation 1: row `1016a5e2546540cf9ef641c10e0fc73f`
  (`runtime_activations`), checkout
  `/Users/josystem/hermes-orchestrator-live` @
  `b8bf678d8b94d475e65cb756374d6a50713f7db8`, schema 44.
- First supervised start proof: event
  `01a04ffd-e153-7f9f-882a-6f7bd9979734` (`daemon.started`,
  2026-08-30T00:07:13Z, `activation_generation: 1`,
  `matches_active: true`).
- Journaled apply: row `55c0affbad424e0782d467966f83bb3c`
  (`activation_applies`) — intent → activated → restarted →
  **verified**; target generation 2; the freshly supervised daemon
  itself reported the exact generation via event
  `01a05001-f1ac-7a66-8b21-352935fbd3f7` (`daemon.started`,
  2026-08-30T00:11:39Z, `activation_generation: 2`), verified PID
  99611. Rollback and ambiguity paths are proven by
  `tests/test_activation.py::TestApplyProtocol` (process-level
  rollback with health verification before `rolled_back` is recorded;
  unprovable outcomes journal `ambiguous` and fail closed).

## 3. Daemon kill and automatic supervised restart

- `kill -KILL 98310` at 2026-08-30T00:09:10Z.
- launchd restarted the job with no terminal or model intervention:
  event `01a04fff-bdd9-70f9-9249-3bd8398958cd` (`daemon.started`,
  2026-08-30T00:09:15Z, `activation_generation: 1` — the exact active
  generation).
- Restart receipt `3785401c041b4a50bb3a04dc189f81ce`
  (`daemon.restarted`, carrying the full activation identity) was
  fetched from durable state and exactly-ACKed once.

## 4. Forced channel disconnect and non-interactive recovery

- The daemon restarts forcibly dropped the live hub connection; the
  session's sidecar re-registered non-interactively with the correct
  generation and capability: receipts `eeef653433da4ff29ae7982430717a3d`
  and `3c4ab2726c5b4b2a8e3c55709dce4995` (`channel.reregistered`,
  `generation: 2`), each exactly-ACKed once.
- Replay counts receipted explicitly (including the count semantics):
  `910476cd95044e8ebc143cf24d561fee` (`channel.replayed`,
  `{"replay_count": 1}`) and successors, exactly-ACKed.
- This session's sidecar predates the new event kinds (it was
  launched before this candidate existed) and rejected them as
  protocol violations; the deliberate capability retirement then
  produced exactly one durable, actionable blocked receipt
  `567f100b49974f53842e867671b169fb` (`channel.blocked`), exactly-ACKed
  — the fail-closed path the correction requires, with the session
  continuing on the Stop-hook drain throughout. A lead launched under
  the current sidecar build parses all four kinds (23 sidecar tests).
- Dedup repair receipted: `31a01c8ac74c4cd8a4c4c79aac121c52`
  (`intake.dedup_repaired`), exactly-ACKed.

## 5. Real dispatch to the idle lead with exact fetch/ACK

- INFRA-195 was requeued through the queue's own transition API
  (`issue.transitioned`, actor
  `lead:5158cb1c-4ff8-41be-a686-5ace1a81e2a2`, with an explicit
  reason) so the supervised daemon would perform a real dispatch.
- The daemon dispatched and atomically published assignment
  `93916b6c57f34616bf842095ed0f9be9` (`lead_assignments`,
  2026-08-30T00:10:18Z) bound to project `agent-orchestration`, issue
  `INFRA-195`, cell `b29691ef-850b-4461-9af3-c8645252f65e`, session
  `5158cb1c-4ff8-41be-a686-5ace1a81e2a2`, profile `max-c`, instruction
  `codex-goal-infra195-20260829`, transition
  `queued->in_development`.
- The lead fetched the durable packet by id and exactly-ACKed it
  through the offer/ack handshake: ledger `acknowledged` at
  2026-08-30T00:11:03Z; the issue returned to `in_development` by the
  dispatch itself; the lead's turn continued the assigned work
  (this document is part of that turn).

## Notes

- Every ACK above is a rowcount-checked compare-and-swap bound to the
  exact session, packet, and offer token; each receipt moved
  `published → acknowledged` exactly once, and re-polls offered
  nothing further (`drained`).
- The one supervision property not exercised destructively is killing
  cmux and the model sessions themselves: the parentage evidence in
  §1 (launchd is PID 1's direct child chain; no cmux/login/session
  ancestor) establishes the daemon's independence structurally, and a
  lead session cannot safely terminate itself to observe its own
  absence.
