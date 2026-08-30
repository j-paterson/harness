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
