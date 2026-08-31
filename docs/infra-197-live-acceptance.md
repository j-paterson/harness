# INFRA-197 live acceptance — transport-v2

Recorded by the Fable lead session `5cb45861-cc04-4099-ae31-0367563a34a4`
(profile `max-d`, cell `b29691ef-…`, generation-10 seat binding
`578c4925…`) under reserved provenance-bound packet `56c3ab86…`
(fable, this file measured absent at reservation) and durable handoff
row `58edaab7…` (file handoff
`infra-197-transport-v2-skip-permissions-20260830`, sha256 `909efae6…`).

## Directive-provenance record (required by the handoff)

### Verified-false directive 2b38d073 (rejected)

`channel.blocked` receipt `2b38d073` claimed H4 was already green and
directed the lead to emit `FABLE_MERGE_READY`, citing operations that
are actually `intake.dedup_repaired` receipts (one predating the
fakechat plane) belonging to the retired session `37379ee9…`. Verified
false against durable rows by the retiring session and REJECTED.
`FABLE_MERGE_READY` was not emitted. Binding rule applied: a transport-
or permission-changing command is authenticated only by a hash-bound
`operator_decisions` row applied through `apply_operator_decision`
and/or the operator speaking directly in the lead session.

### Native-inbox envelope provenance (~16:04Z / 16:07Z) — RESOLVED

Two native-inbox envelopes carrying the exact `HERMES_INTAKE` values
baked into `scratchpad/native_inbox_wake.py` were posted although that
script never ran. Forensic resolution (read-only, very high
confidence):

- Poster: the external Codex thread `01a03ad6-9a3b-7571-bfe7-c7fb90d213bf`
  (rollout `/Users/josystem/.codex/archived_sessions/rollout-2026-08-25T16-32-17-….jsonl`).
  It read the script at 16:03:22Z to learn the constants, then posted
  the frame twice with `printf … | /usr/bin/nc -U /tmp/cc-socks/89466.sock`
  at 16:04:12.706Z and 16:05:55.747Z (exit 0 both), 60 ms and 89 ms
  before the two transcript `queue-operation` enqueues
  (`verifiedPeerPid` 16772 and 17850 — two one-shot processes).
- The script definitively did not run: both launch attempts were
  denied by the auto-mode permission classifier (16:03:26Z, 16:04:31Z);
  its unconditional pre-sleep log file was never created; the observed
  cadence (103 s, two PIDs) contradicts the script (45 s, one PID).
- Ruled out: Hermes code (nothing writes to `cc-socks`), hooks (Stop →
  `intake-poll` only), operator shell history, other Claude profiles.
- Durable correlation: `lead_intake_deliveries` `77c984ec…` (delivered
  16:05:09Z) reflects the drain the wake triggered; `wake_deliveries`
  has no rows in the window — the envelope was an out-of-band socket
  poke, exactly what the wake-not-payload invariant assumes.
- Security observation for the fallback plane: the frame was accepted
  with NO auth line; acceptance rests on same-UID peer credentials.
  Any local same-user process can inject a turn into a live session.
  Containment holds because a wake is a signal, never a payload: the
  drain reads only durable Hermes rows, and a forged wake with nothing
  pending is a no-op. This is recorded as a standing constraint on the
  native-inbox FALLBACK, and a reason the authenticated hermes-control
  channel (capability-bound registration) is PRIMARY.

## Candidate-runtime fallback-drain evidence (gen 20, pre-proof)

Runtime activation apply `2b8a5490…` (journaled): generation 20 at
`e700a8b4d0647cbdf214bf1f5e1c8747be98ea77`, schema 48, daemon verified
(PID 69035). The restart minted control op `6c1a14e4…`
(`daemon.restarted`, result records the exact activation identity)
bound to session `5cb45861…` with channel event `3a0fabea…` pending.

- Stop-hook drain leased offer `77d847ce…`; the durable packet was
  fetched by id and exactly acknowledged
  (`intake-ack … --packet 6c1a14e4… --offer 77d847ce…` →
  acknowledged 2026-08-30T17:25:18Z; delivery `d35f9fda…` `delivered`).
- The repair sweep durably superseded channel event `3a0fabea…`
  (packet consumed through another exact path) and minted exactly ONE
  `intake.dedup_repaired` receipt (`88fcadb6…`,
  `{"superseded_events": 1}`), which was itself fetched and exactly
  acknowledged (offer `48a40999…`).
- Treadmill fix (`d67d0a5…`) verified live: for two full daemon ticks
  after that consumption (probe window > 70 s), ZERO new control
  operations were minted for the session — under generation 19 this
  exact pattern minted one fresh receipt per tick.
- Pre-fix residue: three `intake.dedup_repaired` ops bound to retired
  sessions (`ede6e848…`, `4b352141…`, `ba4dd679…`) remain `published`
  and undrainable by any live session; left for a post-197 cleanup
  decision, deliberately not swept by hand.

## Live proof attempt 1 — FAILED (recorded as evidence), root cause fixed

Operator relaunched the lead session as the channel host via
`.hermes/launch-infra-197-channel-host.sh` and approved the
development-channel dialog (the permitted manual trust event). Result:
`CHANNEL_PROOF_FAILED` — sidecar registered, but no wake ever became
visible in-host. Durable timeline (all 2026-08-30):

- 17:28:17.910Z registration `c4394f7e…` (session `5cb45861…`,
  generation 10, state `active`, never closed) — hub accepted the
  sidecar's identity and capability.
- 17:28:17.917Z hub minted control op `a2988313…` (`channel.replayed`,
  `{"replay_count": 0}` — the explicit zero-replay receipt; the queue
  had been drained pre-relaunch).
- 17:28:17.933Z channel event `1c778e00…` (`HERMES_CONTROL_READY` →
  `a2988313…`) created and written to the live socket.
- 17:28:17.939Z the host's MCP client logged "Channel notifications
  registered" — 6 ms AFTER the first forward. The notification hit the
  client inside the ~21 ms window between transport connect (.918) and
  channel-handler registration, and was dropped client-side.
- Through 17:30:53Z the hub republished the event (attempts = 7,
  state stayed `published`, connection stayed open) — and every
  republish was suppressed by the sidecar's process-lifetime
  `seenEventIds` set (`hub-client.ts`), which forwarded each event id
  at most once.

Root cause: the sidecar deduplicated deliveries, but PROTOCOL.md's
replay section places dedup DOWNSTREAM ("the sidecar and lead must
treat a replayed event as the same delivery: the packet is
deduplicated … downstream"). The hub re-sends unacknowledged events on
registration, on every publish pass, and after daemon restart exactly
so a lost notification self-heals; the seen-set defeated all three —
including the proof's own restart/replay leg, which would have failed
the same way. Fix (packet S2 `619e863a…`): remove the seen-set;
forward every validated event; exactly-once effect remains with the
durable drain and the hub ack CAS. Wake duplicates before ACK are
expected and contained by the wake-not-payload invariant.

## Live legs proven in-session after the fix landed (old sidecar, fresh event ids)

The seen-set bug only suppresses REPEATED event ids, so fresh-id legs
were proven live in the same host session (all rows 2026-08-30, session
`5cb45861…`, registration generation 10):

- `launchctl kickstart -k` mid-session (17:35): hub socket dropped;
  registration `c4394f7e…` closed with granular reason `hub_stopped`
  (H2 lifecycle truth live); sidecar re-registered SILENTLY ~8 s later
  (`a8d8a41e…`, active) with zero manual action.
- Post-restart replay delivery: `daemon.restarted` op `7fb3fe05…`
  minted at 17:35:18 while the hub connection was down (event
  `952b7ee8…`, attempts=2 — first attempt found no live channel);
  the REGISTRATION REPLAY delivered it and the envelope
  `HERMES_CONTROL_READY 7fb3fe05…` was VISIBLE in-host.
- Direct push delivery: `channel.reregistered` op `1878b6da…`
  (`{"generation": 10, "prior_registrations": 1}`) pushed over the
  fresh connection (event `f7d1fb75…`, attempts=1) — visible in-host.
- Durable fetch + exact ACK through the channel op: both packets were
  fetched from durable rows by id and acknowledged via the sidecar's
  `hermes_acknowledge_intake` tool — `ack_ok` both; ledger:
  `952b7ee8…` `acked` 17:35:45.921899Z, `f7d1fb75…` `acked`
  17:35:48.567207Z.
- Exactly-once: a deliberate duplicate ACK of `952b7ee8…` was REFUSED
  (`no acknowledgeable event matches`); delivery state unchanged.
- Second full cycle at the gen-21 activation restart (registration
  `a8d8a41e…` closed `hub_stopped` 17:36:54, silent re-register
  `79aa437f…` 17:37:02): `daemon.restarted` `454c85e1…` (activation
  identity records generation 21 at `cd38ffe…`) and
  `channel.reregistered` `16dcdbeb…` (`prior_registrations: 2`) —
  both delivered visibly and exactly acked through the channel op
  (`e69a86f1…`, `ffdeabd0…`).
- Receipt-flow closure: after the fallback drain consumed `a2988313…`,
  the sweep superseded its stale channel event and minted exactly ONE
  `intake.dedup_repaired` receipt (`574f0138…`,
  `{"superseded_events": 1}`, 17:39:01) — delivered visibly through
  the channel (event `4db4d57f…`, attempts=2) and exactly acked via
  the channel op. No churn followed.

Fix landed: commit `cd38ffe…` (packet `619e863a…` accepted, 28/28
sidecar tests, red evidence captured); runtime generation 21 activated
(apply `c71e0819…`, daemon verified PID 82321) with the fixed sidecar
build materialized (zero `seenEventIds` occurrences in the artifact
dist).

## Final leg — startup replay on the FIXED sidecar — GREEN

Interim fallback evidence: event `1c778e00…`'s packet `a2988313…` was
carried by the Stop-hook FALLBACK drain (offer `5c7e6968…`, exactly
acked) — the fallback provably carries a packet its channel lost, and
the repair sweep superseded the stale channel event minting exactly one
receipt (`574f0138…`, itself channel-delivered and exactly acked). No
churn followed.

The relaunch (operator ran `.hermes/launch-infra-197-channel-host.sh`
and approved the development-channel dialog — the manual trust event
permitted by `infra-197-transport-v2-20260830` and anchoring
`infra-197-trusted-channel-auto-approval-20260830-v1`). Durable rows
(all 2026-08-30):

- 17:39:59.850Z registration `79aa437f…` closed `disconnected` — the
  host exited and the old-build sidecar died with its stdio, recorded
  with the correct granular reason.
- 17:40:25.634Z the script's kickstart (host down, NO sidecar alive)
  minted `daemon.restarted` op `72107297…` whose result records the
  exact gen-21 activation identity (`cd38ffe…`, schema 48); its event
  `e98e8e69…` accrued attempts with no live channel — the staged
  pending witness.
- 17:40:30.081Z the FIXED sidecar (node process running the gen-21
  artifact dist, `seenEventIds` absent) registered: `3f2dd1d2…`,
  active. The hub's own replay receipt `9d97874b…`
  (`channel.replayed {"replay_count": 1}`) durably records that the
  registration replay delivered exactly ONE event — the staged
  witness. `channel.reregistered` receipt `b73220c1…`
  (`prior_registrations: 3`) rode the fresh connection.
- All three envelopes were VISIBLE in-host, repeatedly (per-tick
  republish re-forwarded until ACK — the fix's self-heal behavior,
  duplicates contained by the wake-not-payload invariant).
- Each packet was fetched from durable rows by id and exactly acked
  through `hermes_acknowledge_intake`: `e98e8e69…` `acked`
  17:41:02.584Z, `5d9bdd5c…` `acked` 17:41:03.246Z, `6529214e…`
  `acked` 17:41:03.530Z. Queue drained to zero (no unacked events, no
  published ops for the session).

## Proof summary (v5 steps → durable rows)

1. Real `claude` host seat with the hermes-control development channel,
   operator-approved load — sessions relaunched 17:28 and 17:40; MCP
   client logs and sidecar processes recorded above.
2. Sidecar registration against the hub with exact binding and
   capability — `c4394f7e…`, `a8d8a41e…`, `79aa437f…`, `3f2dd1d2…`
   (all generation 10, session `5cb45861…`); refusals fail closed
   (none occurred live; covered by hub tests).
3. Hermes publishes control events; envelope arrives in-host as the
   injected channel event — proven repeatedly (`952b7ee8…`,
   `f7d1fb75…`, `e69a86f1…`, `ffdeabd0…`, `4db4d57f…`, `e98e8e69…`,
   `5d9bdd5c…`, `6529214e…` — all VISIBLE).
4. Host drains the durable packet and ACKs exactly through the channel
   ack op — every event above `acked` by CAS; deliberate duplicate ACK
   REFUSED (`no acknowledgeable event matches`).
5. `launchctl kickstart -k` mid-window — twice live (17:35 manual,
   17:36:54 activation apply) plus the staged host-down restart
   (17:40): silent re-register each time, replay of the undelivered
   event (`952b7ee8…` attempts=2; `e98e8e69…` replay_count=1), exact
   ACK after restart, dedup intact.

Runtime the proof completed on: generation 21, artifact
`cd38ffea4e3500c5522c644a4c1f0569a7ed3f2e` (apply `c71e0819…`,
schema 48). The candidate head appends only this acceptance document.

All transport-v2 delivery, ACK, restart, replay, and dedup legs are
GREEN as recorded above. However, the operator (direct in-session
directive, 2026-08-30) rejected the cold-start VISIBLE BURST — three
distinct control packets each rendered three times before ACK (hub
republish passes × transport-only re-forward) — as MVP behavior, and
gated candidate publication on: (1) the narrowest per-connection
coalescing fix preserving retry after genuine loss or reconnect
(plan v5.2, packet S3 `195ad703…`), and (2) a repeated cold-start
proof showing ONE visible entry per event.

## Cold-start proof repeat with coalescing — GREEN

S3 landed: commit `adc4281…` (packet `195ad703…` accepted, 32/32
sidecar tests, red evidence captured); runtime generation 22 activated
(apply `4fb6edac…`, daemon PID 88621) with the coalescing sidecar in
the artifact.

Cold start (2026-08-30, all durable): prior host closed (registration
`af1c4d58…` `disconnected` 17:49:01); the staged kickstart minted
`daemon.restarted` op `3a84f6c6…` at 17:49:14 with NO host alive
(event `a580f39e…` accruing attempts channel-less); the gen-22
COALESCING sidecar (process on artifact `adc4281…` dist) registered at
17:50:08 (`ca61b527…`, active); `channel.reregistered` `3704a27c…`
(`prior_registrations: 5`) and `channel.replayed` `2e17b2e1…`
(`{"replay_count": 1}`) followed.

The acceptance bar — ONE visible entry per event — held: durable
attempts were 4 (`a580f39e…`), 3 (`d65b2544…`), 3 (`53286f68…`) — ten
hub sends — while each event rendered in-host EXACTLY ONCE (lead
attestation from inside the host session). The attempts counters
durably prove the hub kept re-sending while the sidecar coalesced
every repeat within the window. All three were fetched from durable
rows by id and exactly acked through the channel op. Retry semantics
retained: reconnect clears the epoch map (proven by every
re-registration delivering replays immediately) and a window lapse
re-forwards a still-unacked event (sidecar test suite).

## v5.1 auto-confirmation gate — implementation LANDED

The gen-22 cold start still required the manual development-channel
dialog, and the operator gated candidate publication on implementing
the v5.1 exact-prompt state machine (plan v5.3). Landed:

- Packet T1 `3d98d9bd…` (accepted): `channel_trust.py` — durable
  anchor (migration 0049, one active per cell), fixed-order fail-closed
  gate re-deriving canonical path + no-symlink walk, owner uid, entry
  and dist-tree sha256, manifest, single channel entry, argv template
  (only the session-UUID and config-path tokens vary, shape-validated),
  workspace/surface/session/profile, build mtime, and exact-prompt
  single-match; receipt kinds `channel.auto_confirmed` /
  `channel.approval_required` (reason starts CHANNEL APPROVAL
  REQUIRED); 25 focused tests, red-first.
- Packet T2 `78c42445…` (accepted): `cmux.py` gains bounded
  `read_screen` (read-only, line-capped) and `confirm_channel_dialog`
  (ref-only signature, Enter as a fixed literal inside the argv) — the
  general send/send-key vocabulary stays rejected and tested; no
  generic keystroke capability exists.
- Lead integration (commits `bc799d8…`, `2d5f99c…`): CLI
  `channel-trust-capture` (re-measures from disk; REFUSED a first
  capture on a stale staged tree digest — the drift check working —
  then captured anchor `b2becae3…` from the gen-22 evidence with
  prompt evidence pending) and `channel-trust-confirm` (bounded
  dialog watcher; with the prompt pending or the build moved,
  --capture-prompt re-anchors to the live launch identity WITHOUT
  pressing — the manual Enter of that launch IS the v5.1 trust event —
  and thereafter evaluates the gate, pressing exactly once on a full
  match). Live claude argv is read from ps at gate time. The gen-22
  dialog screenshot named by the operator was unrecoverable (transient
  staging purged; alternate-screen TUI leaves no scrollback), so the
  prompt binds at the one bounded recapture per the operator's
  clarification; the operator-recorded normalized structure (WARNING:
  Loading development channels; Channels: server:hermes-control;
  option 1 local development; option 2 Exit; Enter to confirm) is the
  required shape the live capture must match.

Live sequence remaining (operator-scheduled, post-freeze addenda per
the execution order): one bounded recapture launch (operator
computer-controlled Enter while the watcher re-anchors to the live
identity), then the following restart auto-confirming with no computer
use (`channel.auto_confirmed` receipt). These addenda do not reopen
the frozen code candidate.

The candidate proceeds through the Fable→Sol workflow; Fable does not
merge.

## MVP live proof under scope freeze `18237de4` — 2026-08-30

Recorded by the Fable lead session `3fe1564e` (profile `max-b`, cell
`b29691ef`, generation-11 seat binding `14503f90`). All rows below are
2026-08-30 and hash-verified; every correction and directive in the
chain was delivered via the durable drain with exact acks.

### Correction and scope-freeze chain

- `lead_corrections` `2a74df71` (Critical, rotation failure; directive
  `infra-197-rotation-failure-20260830T195300Z`, receipt `d905cae5`,
  packet `9bd60aa7`) — acked via offer `63fe2303`.
- Scope freeze `18237de4` (directive
  `infra-197-mvp-scope-freeze-20260830T201711Z`, sha256 `50c46182`) —
  acked via offer `7a541e0c`. C3–C6 were not launched; C1/C2 were
  finished in flight.

### C1/C2 correction batch (commit `47bac44`, pushed)

- C1 packet `d781ac9b` (accepted): `lead.launch_failed` durable
  receipts — bounded 8KiB stderr tail, exact argv, cwd,
  `CLAUDE_CONFIG_DIR`, profile alias, pid, exit code, session id;
  wired into both the daemon and rotate-lead paths. Red-first
  (ImportError + unknown-kind refusal observed; 4 failed/55 passed
  pre-implementation).
- C2 packet `c1bd84ce` (accepted): migration 0050
  `profile_capacity_observations`; `reserve_replacement` fails closed
  without current fable evidence; horizon-honoring fable cooldowns
  (`limit_kind` no longer discarded); `reserve_context_only`
  same-profile path. Red-first (8 failed/68 passed
  pre-implementation).
- Direct exceptions: `83f1aae2` (`test_legacy_upgrade`, 4-line,
  49→50) and `5629ac91` (`test_cli`, 1-line). CORRECTION recorded
  here: `5629ac91`'s stored evidence cites "51 passed", but the
  actual run was 44 passed, all green.
- Provenance note (reconstructed reservations): this seat is
  unmanaged (no subagent-gate hook; launched outside the managed
  path), so both packet reservations were reconstructed honestly —
  work diffs saved (C1 sha256 `308ea5d8…`, C2 sha256 `23a4bacc…`),
  files reverted to HEAD `7c783ec`, packets reserved against the true
  pre-work blobs (C2's untracked migration held aside for the absent
  sentinel), patches re-applied byte-identically, then settled.
  Reserved→returned deltas are the true work.
- Full gate at `47bac44`: pytest 2188 passed; ruff clean;
  hermes-control npm test pass.

### Incident — early live-DB migration (self-inflicted, resolved)

The lead helper's `Database.open` (editable worktree package)
auto-applied migration 0050 to the LIVE DB at 20:17:13Z
(schema 49→50) before runtime activation. Additive-only; the running
daemon was unaffected; but a daemon restart would have failed closed
(the gen-24 activation proved schema 49). Resolved by activating
generation 27; no restart occurred inside the exposure window.

### Runtime activation

Generation 27 activated via journaled apply `6301564a` at git sha
`47bac44` (schema 50); daemon restart verified (pid 23534). The
durable `daemon.restarted` control op `c18762ee` was delivered via the
Stop-hook drain and acked (offer `cce79047`); `intake.dedup_repaired`
`943054ec` (1 superseded event, post-restart sweep) was delivered and
acked (offer `cb2b8baa`).

### Identity reconcile to max-b (operator gate `MVP_LAUNCH_BLOCKED` `profile_identity_mismatch`)

- The mismatch: the launcher script carried max-d (the retiring
  session's), the durable cell + binding said max-a, and the session
  transcript was actually owned by `.claude-personal`.
- Resolution to max-b: auth probe first-party Max OK; journaled
  fable-limit observations at 15:20/15:38Z with reset horizon 19:00Z
  (passed) — the strongest current capacity evidence. max-a was
  operator-attested capped to 2026-08-31T13:59Z (recorded as an
  `operator_attestation` observation); max-d excluded (PR2); max-c
  weaker evidence.
- Journaled: `project_cell.profile_reconciled`,
  `cmux_binding.profile_reconciled` (binding `14503f90`),
  `profile_lease.reconciled`; launch script → max-b with launch-time
  quiescent transcript sync. Receipt:
  `.hermes/infra-197-identity-reconcile-receipt.json`.
- Post-launch process env confirms `CLAUDE_CONFIG_DIR=.claude-max-b`
  and `HERMES_PROFILE_ALIAS=max-b`:
  launcher==cell==binding==registration==process.

### S2–S4 live proof receipts (freeze steps 1–4)

- Step 1 (visible managed lead): session `3fe1564e` resumed in the
  visible cmux seat (binding `14503f90`, workspace `4EBF4B2D`,
  surface `9AEC09A1`, generation 11) on max-b.
- Step 2 (auto registration): `channel_registrations` `11e169ca`
  (20:34:18Z, recapture launch; closed 20:40:59 `disconnected`) and
  `9033df38` (20:41:07Z, ACTIVE): project agent-orchestration /
  cell `b29691ef` / session `3fe1564e` / profile max-b /
  generation 11.
- Trust: anchor `b2becae3` (prompt unrecoverable by design) retired
  20:34:03Z; anchor `9703b5e1` captured at the recapture launch,
  bound to this session/max-b/gen-27 build, PROMPT EVIDENCE BOUND —
  the operator's one manual Enter at the 20:34 launch is the trust
  event, per decision `8272377e`.
- Step 3 (compact notification + exact ACK): four
  `HERMES_CONTROL_READY` channel wakes delivered post-registration,
  ONE visible entry per event; each was verified in
  `control_operations` then acked with the exact packet_id+event_id
  through the channel op: `19fa337a`/`7b6fbbab`,
  `0cb74663`/`21c23b6a`, `62c1b63d`/`c6aae54c`,
  `45675f17`/`5260028b`.
- Step 4 (restart + replay + exactly-once + no-computer-use
  auto-confirm): the launch script kickstarted Hermes with the host
  down, staging `daemon.restarted` `19fa337a` (20:41:06Z) durably
  PENDING; the fresh registration's replay delivered it
  (`channel.replayed` `45675f17` replay_count 1;
  `channel.reregistered` `62c1b63d` prior_registrations 1). The
  20:41 relaunch of the Fable lead auto-confirmed the channel dialog
  with NO manual input: `channel.auto_confirmed` receipt `0cb74663`
  (anchor `9703b5e1`, exact gen-27 build), trust-confirm log
  `{"confirmed":true}`. Exactly-once effective intake: an immediate
  duplicate ack of event `7b6fbbab` was REFUSED
  ("no acknowledgeable event matches").

Freeze workflow note, recorded verbatim: "This run is not
intervention-free evidence because the operator manually challenged
Fable's wait" (directive `18237de4`) — the receipts above stand as
the durable proof trail.

### Deferred per the scope freeze

C3 visible-ack transport, C4 rotation phase restructure, C5
rotate-lead caller authorization, extra protocol states, and capacity
analytics. The standing INFRA-197 blockers from the operator
correction (rotation authority/authorization/selection/scope) remain
recorded in handoff `c37a0963` and `lead_corrections` `2a74df71` for
the deferred work.
