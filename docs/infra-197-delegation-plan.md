# INFRA-197 delegation plan — persistent Claude Code host channel

Authored by the Fable lead (session
`5158cb1c-4ff8-41be-a686-5ace1a81e2a2`, profile `max-c`, cell
`b29691ef-…`) under assignment `36aca392…` before any implementation
edit; this document's own authorship is provenance-bound in the
ledger (packet `c51ac047…`, reserved while the file was measurably
absent). Base: `feature/infra-197` at `5750886` (stacked on the
under-review INFRA-186 candidate, whose delegation/receipt gates
this issue's own candidate uses).

## Assignment (durable, exactly-ACKed)

Make the actual Claude Code host channel persistent as the sole
active issue. Fable authors the plan and delegates bounded packets
to smaller models. Prove real host delivery and restart recovery
without Computer Use, manual MCP reconnects, raw cmux text, or
surrogate-only acceptance. Keep unrelated protocol, sandbox, panel,
and hardening expansion out of scope.

## Live forensics (lead-verified, 2026-08-30)

- The `hermes-control` sidecar is spawned by the Claude Code host
  from a generated per-session `--mcp-config`; the host does not
  respawn a dead stdio MCP server mid-session. Both sidecar exit
  paths are `process.exit(1)` (terminal hub refusal; MCP stdio
  close). Nothing in the repo supervises the sidecar.
- Claude's own MCP client log shows ONE connection for this session
  since 22:01Z, notifications delivered and channel-ACKed until
  23:50Z. The ~40-second registration churn recorded 00:48–01:24Z
  (330 lifetime `channel_registrations` rows) was hub-socket-level
  reconnects by that single sidecar during the INFRA-195 live
  fault-injection window (daemon kickstarts), NOT per-subagent
  sidecars — the explorer hypothesis was checked and rejected.
- At ~01:24Z the sidecar process exited terminally (zero `main.js`
  processes since; no registration after the 03:23Z daemon start; a
  pending `HERMES_CONTROL_READY` is undeliverable; this session's
  MCP client reports failed). `close_reason` granularity is too
  coarse ('disconnected') to prove which terminal path fired.
- `channels/hermes-control/dist/` is gitignored: the only working
  build lives in this worktree. The live daemon runs with
  `repo_root=/Users/josystem/hermes-orchestrator`, where `channels/`
  does not exist — every NEW seat fails sidecar launch
  (`FileNotFoundError` → `channel.blocked`) and is born
  channel-less on the Stop-hook drain.

## Defects to correct (persistence, nothing else)

1. **Terminal exit while the host still holds stdio.** A terminal
   hub refusal (protocol version, stale generation) or any internal
   error kills the sidecar for the session's remaining lifetime.
2. **No durable lifecycle truth.** Hub close reasons cannot
   distinguish supersede vs disconnect vs hub shutdown; sidecar exit
   reasons are invisible; churn diagnosis required manual log
   archaeology.
3. **No reproducible sidecar provisioning.** The build is untracked
   and unreachable from the daemon's stable repo root, so seats and
   runtime activations cannot rely on it.

## Packets

| Packet | Boundary | Files (disjoint) | Tier | Wave |
|---|---|---|---|---|
| H1 | Sidecar survivability: no exit while stdio lives — terminal refusals park with slow re-register retry; top-level error containment; outbound notification validated before send (never emit the malformed-content ProtocolError killer); stdio close remains the only exit | `channels/hermes-control/src/main.ts`, `src/hub-client.ts`, `src/mcp.ts`, `src/validate.ts`, `test/harness.ts` (+ any test files under `channels/hermes-control/test/`) | Sonnet | 1 |
| H2 | Hub lifecycle truth: granular `close_reason` values (`superseded` / `disconnected` / `hub_stopped`), supersede only after probing the existing transport (closed → silent replace; live → recorded supersede), journaled `channel.closed` events with reason; replay semantics unchanged | `src/hermes_orchestrator/channel_hub.py`, `tests/test_channel_hub.py` | Sonnet | 1 |
| H3 | Sidecar provisioning: runtime activation materializes the built sidecar into the immutable runtime artifact and `ChannelLauncher` resolves the entry from the active artifact (repo-root fallback preserved); fail-closed `channel.blocked` receipt unchanged | `src/hermes_orchestrator/activation.py`, `src/hermes_orchestrator/runtime.py`, `tests/test_activation.py`, `tests/test_runtime.py` (append) | Sonnet | 1 |
| H4 | Live acceptance: real `claude` host process (headless, real binary — no Computer Use, no cmux text, no surrogate), registration + channel-delivered wake + channel ACK, daemon kickstart mid-session with silent re-register/replay/ACK, terminal-refusal parking proven without process exit; durable evidence document | `docs/infra-197-live-acceptance.md` | Fable (lead) | 2 |

Lead-owned: this plan; per-wave integration, diff/scope/red-green
verification; sidecar `npm` build + dist verification at
integration; live fleet activation and acceptance execution;
candidate publication. All packets provenance-bound: reserved
before work, settled after, credited only for paths measurably
changed inside the window and byte-identical at the candidate head.

## Out of scope (kept out deliberately)

New protocol operations (a keepalive ping was considered and
deferred — supersede probing uses transport state, no wire change),
sandboxing, panel/UI work, credential or privilege hardening
(operator scope `253fdded…` prohibition stands), Stop-hook
rewrite (the fallback drain works; its silent-skip behavior is
INFRA-190 territory).

## Concurrency and resources

Wave 1 runs H1–H3 in parallel (fully disjoint files; H1 is
TypeScript with its own npm toolchain, H2/H3 Python). At most three
concurrent subagents. Lead verifies actual `git diff` scope against
each packet's allowed files before staging.

## Ledger mirror (updated as waves land)

- `c51ac047…` lead plan packet — reserved pre-work (file absent),
  this document is its returned artifact.
- H1 **accepted** (`a1298144…`, Sonnet, provenance-bound): parking
  replaces terminal exit; error containment; send-boundary
  validation; 28 TS tests green; dist rebuilt; found and fixed a
  live reconnect-timer leak. **UNCOMMITTED under operator hold.**
- H2 **accepted** (`a4d88f1c…`, Sonnet, provenance-bound): granular
  close reasons with transport probing; journaled `channel.closed`
  events; 40 hub tests green. Retained per the operator hold.
- H3 **accepted** (`8215807…`, Sonnet, provenance-bound): artifact
  sidecar materialization (fail-closed) + ACTIVE-artifact entry
  resolution; 51 tests green, full suite 2042. **UNCOMMITTED under
  operator hold.**
- `8663934e…` lead amendment packet — this v2 comparison section.

## v2 amendment: architecture decision (operator control `6cddca89…`)

The operator challenged whether a host-owned stdio sidecar is
necessary at all, and directed: hold H1/H3 commits and activation,
hold H4 live acceptance, retain all diffs uncommitted for
comparison, and amend this plan with a comparison of the current
stdio sidecar against a **daemon-hosted loopback HTTP MCP
endpoint**. No HTTP spike is implemented or run until the operator
explicitly approves. Status: this section is that comparison; all
holds are respected (nothing committed, no acceptance begun, no
spike code).

### Architecture A — host-owned stdio sidecar (current, H1-hardened)

A Node child of the Claude Code host, spawned from a generated
per-session `--mcp-config`, bridging stdio MCP to the daemon's unix
socket hub.

- Lifetime is owned by the host's MCP client: Claude Code never
  respawns a dead stdio server mid-session. H1 removes every exit
  path except stdio close, which makes the process as durable as
  the host session — but the coupling itself is intrinsic: nothing
  can supervise or replace the process mid-session if it is ever
  lost (OOM-kill, crash beyond containment).
- Requires a Node runtime and a BUILT, gitignored `dist/` wherever
  seats launch — the entire provisioning problem H3 solves
  (artifact materialization, entry resolution) exists only because
  of this architecture.
- Per-session identity is clean: env vars + capability file at
  spawn; zero network surface (stdio + mode-0600 unix socket).
- Proven live: registration, notification delivery, channel ACK,
  daemon-restart reconnect (INFRA-195 acceptance) all work today;
  H1/H2 close the known survivability and forensics gaps.

### Architecture B — daemon-hosted loopback HTTP MCP endpoint

The launchd-supervised daemon itself serves MCP over streamable
HTTP on `127.0.0.1`; Claude Code connects as an HTTP MCP client
(URL-type `--mcp-config` entry). The hub's delivery semantics
(persist-before-publish, replay, ack CAS) remain; the transport
changes.

- **Lifetime alignment is the decisive property**: the channel
  server lives exactly as long as the daemon, which launchd already
  supervises with proven restart/rollback (INFRA-195). The
  unsupervised-child failure mode — the root cause of this issue —
  is eliminated structurally rather than hardened around. H1
  becomes interim hardening; H3 becomes unnecessary (no Node, no
  dist, no materialization).
- No per-seat process churn; one endpoint serves every session;
  identity moves from spawn-time env to connect-time auth (the
  existing per-session capability token as a bearer header, same
  0600 file, DB stores only its SHA-256 — mechanism unchanged).
- Costs: a loopback network listener (mitigated: bind 127.0.0.1
  only + capability auth per session); the Python daemon must speak
  MCP streamable HTTP (new dependency or hand-rolled endpoint);
  PROTOCOL.md's hub wire layer becomes internal detail.
- **Empirical unknowns that decide it** (spike questions, not
  implementable without approval):
  1. Does Claude Code's HTTP MCP client reconnect after the
     endpoint drops mid-session (daemon restart)? This is the exact
     capability stdio lacks; if HTTP reconnects, restart recovery
     is free. If it does not, B inherits A's worst defect plus new
     surface.
  2. Are server-initiated `notifications/claude/channel` events
     delivered over streamable HTTP the way they are over stdio?
     Wake delivery depends on it.
  3. Does `--mcp-config` accept URL-type servers with per-session
     headers in this Claude Code version, and does the client
     resume or replay after SSE stream interruption?

### Lead recommendation

If spike questions 1–2 answer yes, Architecture B is superior for
the persistence goal and should be adopted: it aligns channel
lifetime with the already-supervised daemon, deletes the
provisioning problem, and shrinks moving parts; H2's lifecycle
truth carries over intact, H1 remains useful interim hardening for
any transition period, and H3 is retired unactivated. If question 1
answers no, Architecture A with H1+H3 is the only design that can
meet the assignment, and the held work should be committed as-is.
The lead requests operator approval to run a minimal read-only
spike answering questions 1–3 before any further implementation.

## v3 amendment: architecture resolved (operator control `3bb84ee6…`)

External research resolved the question without a spike: Anthropic
documents **stdio as a hard requirement** for custom
`claude/channel` delivery during the research preview (operator
sources: code.claude.com channels-reference and mcp docs;
claude-plugins-official#1478; claude-code#43177). A daemon-only
HTTP channel would be undocumented. Selected architecture:
**thin per-session stdio adapter + persistent Hermes daemon** —
i.e. Architecture A with H1's survivability hardening, H2's
lifecycle truth, and H3's provisioning. The HTTP-only spike is NOT
run; spike questions 1–3 are moot for this issue.

Operator-directed integration path (all holds lifted for it):

1. Reconcile the earlier transient full-suite failure before
   integration (one run recorded `1 failed, 2046 passed` with no
   reproduction on rerun; two verifier runs earlier recorded a
   similar single flake implicating cmux/lead-intake/cells timing
   tests). Reconciliation = reproduce-or-characterize with a
   bounded soak, recorded below.

   **Reconciliation record (2026-08-30):** ten consecutive focused
   runs of the implicated files (`test_cells.py`, `test_cmux.py`,
   `test_lead_intake.py`, `test_codex_merger.py` — 178 tests each,
   1,780 executions) and two fresh full-suite runs (2047 passed
   each) plus Ruff, all green with zero reproduction. Every
   recorded occurrence happened while heavy concurrent activity
   shared the worktree and CPU (parallel subagent pytest runs, uv
   package rebuilds, verifier-driven nested suites); the implicated
   tests are event-loop-timing sensitive (seat-creation turn
   isolation, intake race across restart). Characterized as
   CPU-contention flakiness external to the INFRA-197 diffs — none
   of the implicated files are touched by H1/H2/H3. Gate policy
   unchanged: a failing verified gate run still refuses its
   receipt, which is exactly what happened live.
2. Integrate H1/H2/H3 only behind a FRESH full green gate.
3. H4 (live acceptance) scope is expanded and binding: prove
   (a) a real idle wake delivered through the channel into a real
   Claude Code host, (b) daemon restart → silent reconnect →
   replay → channel ACK, and (c) durable detection plus a safe
   session recovery fallback when Claude ITSELF closes the stdio
   pipe (the one exit H1 keeps): the hub/daemon must record the
   channel loss durably and the Stop-hook drain must provably
   carry the session until a new seat/session restores a channel —
   no Computer Use, no manual reconnects, no raw cmux text, no
   surrogate-only evidence.

## v4 amendment: approved signal-plane substitute (operator control `516006a4…`)

### Confirmed constraint (the real failure, recorded as evidence)

Unattended launch of the custom `hermes-control` development
channel is impossible on a personal Max account during the
channels research preview. Operator probe receipts `98cf7f4e…`
and `3d8ddb9c…`: `--dangerously-load-development-channels`
blocks on a full-screen interactive confirmation before the
sidecar starts (`channel_registration_count: 0`); an
"approved"-flag variant exited and the workspace vanished.
Official docs confirm the dialog is per-launch, `--channels`
accepts only the Anthropic-curated allowlist during the preview,
and `allowedChannelPlugins` is Team/Enterprise managed settings
— a Max account cannot allowlist a custom channel. Per the same
operator control, the following are NOT remedies and are not
pursued: asking a human to approve the dialog per launch, or
broadening Bash/permission rules to force the launch through.
H1–H3 remain committed and correct — the stdio hub stays intact
for any future environment that can load the channel — but the
live signal plane for this fleet must substitute.

### Substitute: official `fakechat` channel as a bounded wake plane

Source-verified facts (`external_plugins/fakechat/server.ts`,
claude-plugins-official): binds `127.0.0.1` only; port from
`FAKECHAT_PORT` (default 8787); `POST /upload` takes multipart
`id` (required) + `text`, answers 204, and injects
`notifications/claude/channel` with the text as content; a bind
conflict crashes the process (no recovery inside fakechat); the
server dies when the host closes stdio; no sender auth beyond
the loopback bind. Division of labor is unchanged in kind:
fakechat carries ONLY the bounded `HERMES_* <32hex>` wake
envelope — the same grammar the announcer and Stop-hook drain
already enforce — while durable state remains the sole payload
source and `hermes-orchestrator intake-poll` / `intake-ack`
remains the exactly-once ACK. A forged local POST can therefore
only trigger a drain of durable rows (a no-op when nothing is
pending): the wake is a signal, never a payload, which is the
injection-containment invariant this design keeps.

### Required checks (operator-listed) — provability

1. **Exact session/binding-to-port ownership** — provable: a
   durable per-session port row committed with the seat binding
   (generation-bound, one live row per cell, migration adds the
   table); the sender refuses any port not owned by the exact
   active session/binding it is waking.
2. **Loopback-only** — provable: fakechat's `127.0.0.1` bind is
   source-verified; the sender connects only to `127.0.0.1` and
   never configures another interface.
3. **Schema-only bounded envelope** — provable: the sender
   validates against the existing envelope regex before any
   send; anything else refuses fail-closed. `id` carries the
   delivery id for idempotent re-sends.
4. **Stale/orphan port recovery** — provable in Hermes even
   though fakechat itself has none: connection-refused, timeout,
   or non-204 leaves the durable delivery non-terminal for the
   existing Stop-hook drain and journals one control receipt;
   seat rotation retires the port row with the binding, and a
   fresh seat is issued a fresh probed-free port.
5. **Restart recovery** — provable: the sender is daemon-side,
   stateless HTTP over durable rows, so a daemon restart
   re-derives everything and the existing `publish_pending`
   replay re-attempts unsent envelopes; a host restart kills
   fakechat with the host and resolves through check 4 plus a
   fresh seat's fresh port.
6. **No browser or Computer Use** — provable: `POST /upload`
   only; the UI is never opened; the wake arrives in-session as
   the injected channel event.

### Decision and packet map

All six checks are provable: bounded delegation proceeds.

| Packet | Boundary | Files (disjoint) | Tier | Depends |
|---|---|---|---|---|
| H5 | Fakechat signal adapter: durable port ownership (new table + migration), envelope-validated loopback sender, failure fallback receipts, replay wiring | `src/hermes_orchestrator/fakechat_signal.py`, `tests/test_fakechat_signal.py`, migration | Sonnet | — |
| H6 | Seat launch path: classic command grammar gains the fixed `--channels plugin:fakechat@claude-plugins-official` literal; `FAKECHAT_PORT` joins the workspace env; dev-channel command retired from live launches | `src/hermes_orchestrator/cmux_surfaces.py`, `tests/test_cmux.py` | Sonnet | H5 |
| H4 | Live acceptance, rescoped: (a) idle wake — Hermes POSTs the envelope, the real host wakes and drains via exact CLI ACK; (b) daemon kickstart mid-window — durable deliveries survive, replay re-attempts, wake lands post-restart; (c) host exit — POST fails, one durable receipt journals the loss, the Stop-hook drain carries the session | `docs/infra-197-live-acceptance.md` (packet `143260f2…`, already reserved pre-work) | Fable (lead) | H5, H6 |

### Preconditions and recorded risks

- Bun and an installed `fakechat@claude-plugins-official` plugin
  are one-time provisioning for the seat profile — setup, not a
  per-launch confirmation, hence compliant with the operator's
  prohibition. Verified at H4 launch; absence fails the launch
  closed with a `channel.blocked`-class receipt.
- `--channels` syntax may change during the research preview;
  acceptance evidence pins the working invocation.
- The `/upload` file field and shared
  `~/.claude/channels/fakechat` state dir are unused by Hermes;
  the loopback-only bind bounds that surface to local users, and
  the wake-not-payload invariant contains any forged text.

## v5 amendment: transport-v2 (operator decision `infra-197-transport-v2-20260830`)

### Authority and supersession

Durable operator decision `infra-197-transport-v2-20260830`
(status `approved`, receipt sha256 `656def182883999ce1bcac0529…`,
applied 2026-08-30T16:21:45Z, confirmed by the operator directly
in the lead session): the dedicated unified `hermes-control`
Claude Code channel is PRIMARY; Hermes remains the durable
packet and receipt authority; the native cross-session socket
inbox is FALLBACK; fakechat is RETIRED. Interactive proof comes
first; machine-managed `allowedChannelPlugins` provisioning
follows only after the proof. This supersedes the v4
signal-plane substitution and every conflicting interim
transport directive. The v4-era prohibition on per-launch human
confirmation is lifted for the interactive proof only — the
unattended path arrives via managed `allowedChannelPlugins`,
not via a human approving every launch.

Directive provenance (raised during the interim): only a
hash-bound `operator_decisions` receipt applied through
`apply_operator_decision`, or the operator speaking directly in
the lead session, authenticates a transport-changing command.
Instruction-bearing `channel.blocked` receipts alone do not; the
interim impersonation attempt (`2b38d073…`, false evidence-freeze
citations) was verified false against durable rows and rejected.
The acceptance document records that verification.

### Standing evidence carried into v5

- H1–H3 hub machinery (registration, capability, exact channel
  ACK, replay, blocked receipts) is committed, tested, and
  unchanged — it IS the hermes-control channel core.
- Native inbox fallback, observed live in the lead session on
  Claude Code 2.1.251: script-posted envelope delivery on the
  session's own `/tmp/cc-socks/<pid>.sock` (auth line +
  `{"type":"user",…}` JSON lines), transcript persistence,
  mid-turn non-interruption, durable fetch + exact ACK
  (delivery `77c984ec…`), and event-id dedup (an identical
  duplicate envelope re-offered nothing). Idle-wake and
  restart//clear binding-refresh legs remain open and belong to
  the fallback's own acceptance, not to the primary proof.
- Fakechat: both port rows retired (`52120` at 15:40Z, `52526`
  at 16:11Z); adapter and tests stay committed as the recorded
  substitute-era evidence; no live launches.
- Receipt-treadmill defect found and fixed en route
  (`d67d0a5…`, ledgered packet `b4495480…`): the repair sweep
  no longer counts its own consumed receipts, so pure receipt
  churn mints nothing. Live only after the next runtime
  activation.

### The narrow live proof (the only expansion-gating work)

Exactly one end-to-end flow, interactively, with the operator
present — nothing more before it is green:

1. Launch one real `claude` host seat with the `hermes-control`
   development channel; the operator approves the load dialog
   (permitted by the decision for this proof).
2. The sidecar registers against the hub with the seat's exact
   binding and capability; registration refusals surface as
   durable `channel.blocked` receipts, fail-closed.
3. Hermes publishes ONE control event; the envelope arrives
   in-host as the injected channel event.
4. The host drains the durable packet and ACKs exactly through
   the channel ack op; the ledger row is the proof.
5. `launchctl kickstart -k` the daemon mid-window: silent
   re-register, replay of the undelivered event, exact ACK
   after restart.

Durable rows for every step land in
`docs/infra-197-live-acceptance.md` (reserved lead packet
`912b9d93…`). Machine-managed `allowedChannelPlugins`
provisioning, seat-grammar changes, and any fleet expansion are
explicitly out of scope until this proof is green.

## v5.1 amendment: trusted-build automatic confirmation (operator decision `infra-197-trusted-channel-auto-approval-20260830-v1`)

### Authority

Durable operator decision
`infra-197-trusted-channel-auto-approval-20260830-v1` (status
`approved`, receipt sha256 `8272377e9967b11e0a26268dee7a1336…`,
applied 2026-08-30T16:35:35Z). It extends transport-v2: after ONE
exact manual trust event of ONE exact `hermes-control` build, a
narrowly scoped automatic confirmation may stand in for the
per-launch dialog — and nothing broader.

### The trust binding (all must match, exactly)

An automatic confirmation is permitted only when every one of these
matches the manually trusted build with no substitution:

- canonical channel path and ownership, with no symlink swap;
- plugin and marketplace identity;
- manifest and version;
- source or packaged digest;
- build timestamp;
- exact `claude` launch arguments;
- exact cmux workspace, pane, Claude process, session, profile,
  and prompt shape, with no second channel present.

### Fail-closed rules

- Every automatic confirmation writes a durable approval receipt.
- Any mismatch or drift on any bound field fails closed as
  `CHANNEL APPROVAL REQUIRED` — never an approval.
- No generic keystroke-automation or prompt-approval capability is
  created; this transition confirms exactly one identity-bound
  channel build and nothing else.
- Persistent Claude processes and `/clear` remain the normal
  lifecycle; durable packet validation, exact ACK, replay,
  deduplication, and the channel-as-signal-plane invariant remain
  binding.
- Official marketplace submission of `hermes-control` remains the
  long-term solution; this transition is the bounded interim.

The candidate proceeds through the existing Fable-to-Sol workflow;
Fable does not merge.

## v5.2 amendment: cold-start wake coalescing (operator directive, in-session, 2026-08-30)

The transport-v2 live proof exposed an MVP-visibility defect: at cold
start three distinct control packets were each visibly delivered three
times before ACK. Cause: every commit-triggered `publish_pending` pass
re-sends every pending event over the live connection, and the
transport-only sidecar (v5 fix, packet `619e863a…`) forwards each
send. The operator directed the narrowest readiness-aware /
per-connection coalescing fix preserving retry after genuine loss or
reconnect, plus a repeated cold-start proof showing one visible entry
per event, before candidate publication.

| Packet | Boundary | Files (disjoint) | Tier | Depends |
|---|---|---|---|---|
| S3 | Sidecar per-connection TTL coalescing: forward an event id at most once per coalesce window within a connection epoch; clear the map on each successful registration (reconnect retries immediately); window expiry re-forwards a still-unacked event (genuine-loss retry); env-tunable window, 60 s default | `channels/hermes-control/src/hub-client.ts`, `src/main.ts`, `test/events.test.ts`, `test/harness.ts` | Sonnet | S2 |

Readiness note: the sidecar has no signal for when the host's channel
handler is registered (observed 21 ms after MCP connect), so true
readiness-awareness is unknowable at this layer; the coalescing window
subsumes it — a forward lost to the startup race re-surfaces once the
window lapses, instead of never (the pre-v5 defect) or every pass (the
burst). Hub republish cadence is unchanged; exactly-once effect stays
with the durable drain and ack CAS.

## v5.3 amendment: v5.1 trusted-build auto-confirmation implementation (operator requirement, in-session, 2026-08-30)

The gen-22 cold-start proof still required the manual development-
channel confirmation; the operator gated candidate publication on
implementing the v5.1 state machine exactly as the durable decision
`infra-197-trusted-channel-auto-approval-20260830-v1` specifies.

Architecture (lead-authored): a durable trust anchor captured from ONE
manual trust event binds canonical sidecar entry path + owner with no
symlink substitution, plugin identity, manifest name/version, packaged
digest (deterministic sha256 over the dist tree plus the entry file),
build timestamp, the exact `claude` launch argument template (only the
session UUID and the controller-generated config path vary, both
shape-validated), the exact cmux workspace/surface/profile/session
binding with a live `claude` process match, the single-channel
constraint, and the exact hermes-control confirmation prompt shape. A
confirmation gate re-derives EVERY bound field live; on a full match
it presses exactly one Enter on the exact validated surface through a
dedicated non-parameterizable port op and records a durable
`channel.auto_confirmed` receipt carrying the match evidence; ANY
mismatch, drift, ambiguity, or missing anchor fails closed with a
durable `channel.approval_required` receipt reading
`CHANNEL APPROVAL REQUIRED` and no keypress. No generic keystroke or
prompt-approval capability exists: the port op sends only the fixed
Enter literal and the state machine is its only caller.

| Packet | Boundary | Files (disjoint) | Tier | Wave |
|---|---|---|---|---|
| T1 | Trust-anchor model, migration 0049 (+ schema pins), verification checks, fail-closed state machine core (collaborators injected), auto_confirmed/approval_required receipt kinds | `src/hermes_orchestrator/channel_trust.py`, `src/hermes_orchestrator/migrations/0049_channel_trust_anchors.sql`, `src/hermes_orchestrator/control_operations.py`, `tests/test_channel_trust.py`, `tests/test_control_operations.py`, `tests/test_db.py`, `tests/test_legacy_upgrade.py` | Sonnet | 1 |
| T2 | Narrow cmux ops: `read_screen(ref)` and the fixed-literal `confirm_channel_dialog(ref)` Enter press; general send/send-key vocabulary stays rejected | `src/hermes_orchestrator/cmux.py`, `tests/test_cmux.py` | Sonnet | 1 |

Lead-owned: interface contract between T1 and T2, CLI anchor/confirm
commands, runtime wiring, seat-path hookup, integration gate, and the
live anchor capture from tonight's manual trust events.

## v5.4 amendment: rotation-chain delegation-evidence identity (operator decision, in-session, 2026-08-30)

The first `candidate-ready` run exposed a structural conflict between
two established policies: the INFRA-186 delegation-evidence gate
requires every credited packet to share ONE exact
(session, worktree, generation) identity, while operator rotation
policy deliberately rotates lead sessions at safe boundaries with
durable acknowledged handoffs — this branch was correctly built by
four chained sessions. The operator approved clause 1 only: the gate
accepts credited packets from any session in the candidate cell's
MECHANICALLY VERIFIED rotation chain; the proposed one-shot
grandfather clause for pre-gate-era uncovered paths is NOT approved.

Mechanical chain rule (packet G1): all credited packets must share one
worktree AND one cell_id, and every credited packet's session_id must
be durably recorded for that exact cell — as the cell's current
`project_cells.session_id`, as a `replacement_session_id` on an
acknowledged `handoffs` row, as a `lead_assignments.session_id`, or in
a `project_cell.rotated` event payload. Any credited session outside
that recorded chain, any cell mixture, and any worktree mixture keep
failing closed exactly as before; nothing else about the gate changes.

| Packet | Boundary | Files (disjoint) | Tier | Wave |
|---|---|---|---|---|
| G1 | Rotation-chain identity acceptance, fail-closed tests for out-of-chain sessions, mixed cells, mixed worktrees | `src/hermes_orchestrator/emission.py`, `tests/test_emission.py` | Sonnet | 1 |

Publication remains gated (operator correction, same message) on the
outstanding live sequence: the bounded prompt recapture and the
following no-computer-use auto-confirm restart.

## v5.5 amendment: first-class lead rotation (operator directive e57b1b4c…)

Durable directive `infra-197-first-class-profile-rotation-20260830-v1`
(SHA-validated packet, operator text: "We need that rotation to be a
first class capability, not hacked around with one off scripts",
approval "Yes, go for it"): lead rotation becomes one idempotent
command composed from the EXISTING primitives — the durable handoff
row, profile leases, project cells, the classic cmux seater and
channel launch, and the binding intent/residual machinery. The
generated one-off max-c launcher is retired unrun; the first live
max-d→max-c rotation is performed BY the new command once its focused
tests are green.

Architecture (lead-owned): `rotate-lead --cell <id>` derives its phase
from durable rows on every invocation — never from memory — so any
crash before launch, after launch, before ACK, after ACK, or mid
transfer resumes without duplicate leads or duplicate handoff
execution:

1. Preconditions (fail closed): the cell is active with an incumbent
   session; the newest cell handoff row is complete and `submitted`;
   the project worktree is clean and its HEAD equals the pushed
   origin branch head (the checkpoint).
2. Reserve a DIFFERENT healthy first-party Max profile through the
   existing lease reservation (auth-probed); cancellation on any
   later refusal.
3. Acknowledgment: the replacement session acknowledges the exact
   handoff row and restates the next action through the existing
   rotation core (`ProjectCellService.rotate`'s runner-driven ack
   turn) — reused, not reimplemented.
4. Atomic transfer: one transaction swaps the profile lease, rotates
   `project_cells` to the acknowledged session, and retires the
   incumbent binding while activating the replacement seat via the
   existing write-ahead intent/`activate_residual(replacing=…)`
   primitives — the same one-transaction supersede the binding layer
   already guarantees.
5. Seat: the acknowledged session is seated through the STANDARD
   managed classic-cmux path (`classic_resume_command`, which carries
   `--dangerously-skip-permissions` by the S1 grammar, plus the
   channel/fakechat extension exactly as the seater decides), with
   exact project/cell/session/profile/generation binding and classic
   evidence recorded.
6. Projection: local operator surface only — active rotation or one
   actionable failure; no Linear workflow states.

| Packet | Boundary | Files (disjoint) | Tier | Wave |
|---|---|---|---|---|
| P1 | Idempotent rotation transition: durable phase derivation, precondition gate, lease reserve/cancel, ack via the existing rotate core, atomic transfer + seat activation, crash-resume tests at every phase boundary, duplicate-lead/duplicate-ack refusal tests | `src/hermes_orchestrator/lead_rotation.py` (new), `tests/test_lead_rotation.py` (new) | Sonnet | 1 |
| P2 | CLI surface: `rotate-lead` subcommand wiring the transition with real collaborators; focused CLI tests | `src/hermes_orchestrator/cli.py`, `tests/test_cli.py` | Sonnet | 2 (after P1) |

Provenance note: P2 also becomes `cli.py`/`tests/test_cli.py`'s most
recent in-window claimant, shrinking the approved clause-2 grandfather
binding's residual to `src/hermes_orchestrator/runtime.py` alone.
