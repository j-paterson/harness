# INFRA-195 live acceptance evidence

Captured 2026-08-30 (UTC) against the production state database
(`~/.local/share/hermes-orchestrator/state.db`, schema 44), per Sol
corrections `032cd4a562ae4107aa30e085dd5b2165` and
`70e2dcf7fb2e415fb3009b2f02c5b37c`. Every claim is bound to a durable
row or event id; the final ledger snapshot below was taken at the
verdict boundary and re-verified after a bounded observation window.
The entire exercise used **zero Computer Use** and wrote **zero bytes
into any interactive terminal prompt**: every step ran through CLI
subprocesses, durable SQLite, launchd, and the dedicated channel. The
assignment and every control receipt in the definitive sequence (§4–5)
were fetched and exactly-ACKed **through the dedicated channel by the
candidate sidecar** — the Stop-hook fallback played no part in it.

## 1. External lifecycle owner: launchd, bound to immutable artifacts

- The prior cmux-pane-owned daemon (PID 75049) was terminated; the
  earlier hand-written wrapper was removed.
- `deploy-render` + `deploy-install --execute` (journaled, completed
  with no compensations) installed the **rendered, worktree-independent
  bootstrap** to `~/.local/share/hermes-orchestrator/bin/hermes-orchestrator`
  and both launchd jobs; the bootstrap resolves the derived
  `runtimes/ACTIVE` pointer the activator maintains and falls back to
  the durable config clone — it references no issue worktree.
- Supervised chain, observed live: `launchctl` job → PID with **PPID
  1** → `runtime-exec` → daemon executing
  `~/.local/share/hermes-orchestrator/runtimes/<sha>/.venv/bin/hermes-orchestrator`
  — every path in the chain lives under the state directory or the
  config clone. Kill-and-restart and both `runtime-activate --apply`
  runs below all restarted onto this chain with no terminal or model
  intervention.

## 2. Activation identity, journaled applies, fail-closed refusals

- Active at the verdict boundary: **generation 8**, immutable artifact
  of `904c9f606714476a43e700af9fa2dc07ca3363b0` (the candidate's
  implementation head), activation row in `runtime_activations`,
  `database_schema 44`.
- Apply journal (`activation_applies`): `55c0affb…` verified (gen 2),
  `09822c64…` verified (gen 3), `449267032d404a269c38ff621e130e92`
  **refused** (a deliberate apply invoked from an artifact context has
  no git source — journaled fail-closed, no process touched), and
  `ab8a90fd4a4d4c979d16a1243cac9f54` verified (gen 8, PID 57731
  self-reported the exact generation via `daemon.started`).
- Full-identity startup confirmation is live: `daemon.started` event
  `01a05023-46f7-733e-9243-d2d99d7f0386` (gen 7) and successors carry
  checkout root, observed commit, binary path, and schema, and
  `matches_active` is computed from all four (see
  `TestFullIdentityConfirmation`).
- Live findings fixed during this acceptance, each now regression
  tested: a stale uv-cached wheel could poison a fresh artifact
  (source-hash `cache-keys` + pre-record `_warm_artifact` proof), and
  a WAL database is not shell-readable (the derived `ACTIVE` pointer).

## 3. Daemon kill and automatic supervised restart

- `kill -KILL` of the supervised daemon at 2026-08-30T00:09:10Z was
  restarted by launchd in five seconds (`daemon.started`
  `01a04fff-bdd9-70f9-9249-3bd8398958cd`, 00:09:15Z, on the exact
  active generation); later kickstarts during the applies repeated the
  proof (e.g. `01a05025-14db-76a2-978a-4d944a836721`, gen 7;
  gen-8 start verified by apply `ab8a90fd…`).

## 4. Candidate sidecar: disconnect, re-registration, direct channel

The exact candidate `hermes-control` build (dist of this branch) ran
for this session, driven over MCP stdio precisely as the host drives
it; its structured log is the per-event record.

- Registration: `channel_registrations` row `bf8c80117999460988bb5aaa1d33c544`
  active at 00:48:58Z with the correct binding **generation 2** and a
  reissued capability.
- Forced disconnect: `launchctl kickstart -k` at 00:49:57Z destroyed
  the hub; the sidecar retried with bounded backoff (logged ENOENT
  attempts 00:49:57–00:50:00) and re-registered **non-interactively**
  after the daemon returned (00:50:02Z), then received and
  channel-ACKed the fresh `daemon.restarted`, `channel.reregistered`,
  and `channel.replayed` receipts (e.g. events `0b68234d…`,
  `d43d167c…`, `c847146e…` at 00:50:04Z, each `ack_ok`).
- The one legacy incompatibility (this Claude session's pre-candidate
  sidecar) was **stopped through its owner path**: SIGTERM at
  00:39:23Z, process exited, and the registration count stayed frozen
  (317 before and after) — it can no longer register or receive.

## 5. Real dispatch to the idle lead over the dedicated channel

- INFRA-195 was requeued through the queue's own transition API
  (journaled `issue.transitioned`, explicit reason).
- The supervised artifact daemon dispatched it: assignment
  **`385da7c92b704227941aa07fbf18922a`** committed atomically with the
  `queued->in_development` transition, bound to project
  `agent-orchestration`, cell `b29691ef-…`, session `5158cb1c-…`,
  profile `max-c`, instruction `codex-goal-infra195-20260829`; the
  consumed prior packet `93916b6c…` was superseded by the fresh epoch.
- Delivery was **direct-channel**: `HERMES_ASSIGNMENT_READY` event
  `733be0943ce94d87b9f36608a6066936` reached the candidate sidecar at
  00:49:36Z and was exactly-ACKed through its own tool in the same
  second; the ledger shows `acknowledged` at 00:49:36.682Z and the
  issue back `in_development`. No Stop-hook offer touched this packet
  (its delivery row records no offer), and the lead's turn continued
  the assigned work.

## 6. Final authoritative snapshot (00:54:34Z, re-verified after a bounded window)

- `control_operations` for this session: 27 receipts, **every one
  `acknowledged`**, zero `published`.
- `lead_assignments`: `385da7c9…` acknowledged; `93916b6c…`
  superseded; nothing published.
- `lead_intake_deliveries`: **zero** non-terminal rows (the router's
  residue repair supersedes any delivery whose packet ledger is
  terminal — added and regression tested after this audit found
  `announced` residue).
- `channel_events`: **zero** pending or published for this session.
- Every ACK is a rowcount-checked compare-and-swap bound to session,
  packet, and event/offer identity: exactly-once by construction.

## 7. One coherent fleet (Sol 3be95878, verified live)

- The ACTIVE pointer publishes only after the ledger commits, a failed
  publish compensates the ledger back to the prior activation, a stale
  pointer self-heals at the next confirmed startup, and every injected
  failure mode is regression tested (`TestFleetCoherence`).
- `serve-console` is activation-bound: it confirms the complete
  runtime identity, journals `console.started`, and refuses a
  mismatched artifact.
- The apply protocol restarts and verifies BOTH launchd jobs. Live:
  apply `4f629ec4d80f44fd8eeb66bf7bff6a78` (generation 10) restarted
  the fleet and both durable proofs landed — `console.started`
  01:08:49Z and `daemon.started` 01:08:53Z, both
  `activation_generation: 10` — and `ps` shows both process chains
  executing the generation's artifact. The final apply recorded in the
  emission manifest repeats the proof at the exact candidate SHA.

## 8. Live fault injection: partial fleet restart (Sol ff45468d)

- A kickstart exception after target activation now enters the same
  journaled rollback protocol as a missing health proof, with a
  terminal-state guarantee for every post-activation exception path
  (`TestFleetRestartExceptions`, five injected variants).
- **Live injection, first run** (apply `97463c17579f4893b7e2b9dafc190443`):
  the daemon restarted onto the target, the operations restart was
  withheld by injection, and the rollback kickstart then hit a REAL
  fault — launchd's 30-second ThrottleInterval blocked the client past
  its timeout — so the protocol journaled **ambiguous** and raised,
  exactly the fail-closed contract; launchd processed the queued
  restart moments later and both chains converged on the prior
  artifact. The throttle finding is fixed (client timeout now exceeds
  the throttle floor) and recorded in the journal row's reason.
- **Live injection, second run** (apply `db00ca270095449fa6e32a62050f0c72`):
  target generation 14 activated, the daemon restarted, the injected
  operations failure fired, and the protocol reactivated the prior
  identity as generation 15, restarted BOTH launchd jobs, verified
  both durable proofs (PID 99454), and recorded **rolled_back** — with
  `ps` showing both process chains on the exact prior artifact
  `6c9de9323a0b`. The subsequent clean apply
  `5d6e14a509b04dd2b3873e25b3a7acfb` (generation 16) verified the
  fixed code fleet-wide.

## Notes

- Killing cmux and this model session itself was not exercised
  destructively; §1's parentage evidence (no cmux/login/session
  ancestor anywhere in the supervised chain) establishes the
  independence structurally, and a lead session cannot observe its own
  absence.
- The evidence commit itself is documentation-only; upon it, one final
  verified `runtime-activate --apply` activates the exact candidate
  SHA, with its apply id and generation recorded in the emission
  manifest's verification entries.
