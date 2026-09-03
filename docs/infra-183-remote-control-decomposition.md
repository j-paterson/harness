# INFRA-183: remote-control decomposition at security boundaries

Lead-owned (Fable, cell 83b52ed1, session 72789518), base commit 9e5dc28,
worktree `/Users/josystem/hermes-orchestrator-issue-INFRA-183`.
Linear: https://linear.app/jo-solutions/issue/INFRA-183

This document splits the remaining remote-control delivery into review units
that are each narrow and independently reviewable, records the trust
boundaries every unit inherits, and states the admission rule. It creates no
Hermes admissions itself.

## What already exists (INFRA-173 to INFRA-177, merged)

| Area | Delivered | Where |
|---|---|---|
| Authentication | Keychain application token, HMAC session cookie (900 s, fingerprint-bound, revocable), CSRF, login rate limit | `remote/auth.py` |
| Authorization | closed `RemoteIntent` enum, 9-intent allowlist, deny by construction | `remote/policy.py` |
| Sanitized views | forbidden-term screening, structural PR-link re-derivation | `remote/views.py` |
| Confirmed commands | two-step confirm, exact phrase, 120 s expiry, claim-before-execute, idempotency keys, audit in the same transaction | `remote/commands.py`, `remote/api.py` |
| Phone console | server-rendered, loopback-only, CSRF forms, 9 actions bound 1:1 to allow-listed intents | `remote/console.py`, templates |
| Service wiring | `build_console_dependencies` wires the durable graph; only `retry`, `request_checkpoint`, `approve_stall` have executors | `remote/serve.py` |
| Install engine | declarative plans with compensation, write-ahead `FileMutationJournal`, crash recovery before any fresh mutation, ownership refusal | `deploy/lifecycle.py`, `deploy/launchd.py` |
| Tailscale | pure fail-closed `verify_serve_status` (empty or exactly one loopback root proxy, Funnel never allowed), `deploy-serve-enable` self-compensates | `deploy/tailscale.py` |
| Runbook | generated `OPERATOR.md` inside the `deploy-render` output directory only | `lifecycle._render_runbook` |

## What remains

- Four allow-listed intents (`pause`, `resume`, `request_cleanup`,
  `approve_handoff`) render nothing and refuse with `UnsupportedIntent` because
  no executor exists; the console advertises an allowlist it cannot honour.
- The install and Tailscale engines are code-complete with unit coverage but
  have never been exercised against the real machine and tailnet; no
  persisted runbook or operator documentation exists outside the rendered
  output directory.
- Photon (iMessage) and Apple Watch have no code at all.
- No single adversarial acceptance suite runs over the assembled production
  graph, and no trust-boundary document exists.

## Trust boundaries every unit inherits

1. **Phone browser / Watch / Photon relay -> console process.** Untrusted.
   Authenticated by the independent application token only; every mutation
   needs a session, CSRF proof and an exact confirmation phrase. The Photon
   relay is not end-to-end encrypted: nothing above low-sensitivity
   operational status ever crosses it.
2. **Console process -> Hermes daemon.** The console never calls the daemon;
   it writes durable rows (`remote_pending_commands`,
   `remote_command_results`, events) and routes through the strict
   `HermesCommandService` intents. No shell, environment, credential, MCP,
   provider-auth, spend, force-delete or safety-disable path exists.
3. **Tailnet -> loopback.** Only the operations console port is ever a
   Tailscale Serve target; Funnel is never enabled; the Hermes dashboard and
   the daemon are loopback-only and never served.
4. **Installer -> launchd / filesystem.** Every mutation is journaled before
   it happens, owns only resources it created, and compensates in reverse on
   refusal; nothing is clobbered.
5. **Keychain.** Application token, session signing key and (future) Photon
   operator allowlist live in macOS Keychain under distinct service names;
   no secret is ever rendered, logged or echoed in an error.

Every issue below states, under these boundaries, the live-state mutations it
is allowed to make, its rollback, its resource cleanup and its adversarial
acceptance tests.

## Proposed issues

Order is the admission order. One issue is admitted to Hermes at a time and
each produces exactly one PR toward `main`. Nothing is admitted automatically.

### R1 = INFRA-234. Wire or retire the unexecutable console intents (new)

Scope: give `pause`/`resume` a durable per-project primitive and executor;
give `request_cleanup` an executor that requires the existing `RemoteProof`
and refuses without git/process ports; retire `approve_handoff` from the
allowlist and console table (acknowledgement is a session protocol step an
operator console must not mint). Console action table and `available()` end
up equal to the executable set. README gains the operator section for
`remote-auth-init`, the console action set and the command boundary.
Allowed live-state mutations: `admitted_issues`/project pause projection,
`remote_pending_commands`/`remote_command_results`, events. Rollback:
`resume` reverses `pause`; retiring an intent is a policy change with no
state. Cleanup: none. Adversarial tests: every retired or forbidden intent is
unreachable through API and console even with an injected pending row;
pause/resume round-trip is idempotent and audited exactly once;
`request_cleanup` without proof refuses closed. Depends on: nothing.

### R2 = INFRA-235. Prove failure-atomic launchd installation and rollback live (new)

Scope: exercise `deploy-render`/`deploy-install --execute`/`deploy-status`/
`deploy-uninstall --execute` on the real machine, including a kill between
journal claim and apply, and record the evidence; persist the operator
document as `docs/operations/remote-access.md` (install, status, rollback,
recovery) generated from the same source as `OPERATOR.md` so they cannot
drift. Allowed live-state mutations: the two namespaced launchd jobs, their
plists, log and newsyslog files, the install journal, all under the state
directory or the rendered output directory. Rollback: `deploy-uninstall
--execute`, and the journal's own compensation on refusal. Cleanup: no
residual plist, label, log or journal entry after uninstall (asserted by
`deploy-status`). Adversarial tests: foreign plist or label present refuses
install; corrupted or foreign journal entry refuses recovery; identity
mismatch never compensates. Depends on: nothing.

### R3 = INFRA-236. Activate tailnet-only Tailscale Serve and publish the runbook (new)

Scope: run `deploy-serve-enable --execute` against the real tailnet with the
console installed by R2, prove `tailscale serve status --json` shows exactly
one loopback root proxy with Funnel off and no other exposure, prove
`tailscale serve reset` disables cleanly, and prove the Hermes dashboard and
daemon are unreachable from the tailnet. Extend `docs/operations/
remote-access.md` with activation, verification and disablement. Allowed
live-state mutations: the Tailscale Serve configuration for this node only.
Rollback: `serve_reset_argv()`, also self-applied when the post-enable probe is
unsafe. Cleanup: serve configuration empty after disablement. Adversarial
tests: a pre-existing foreign handler, a TCP entry, a non-loopback backend, an
unexpected port or any `AllowFunnel: true` refuses activation; Funnel argv is
never emitted. Depends on: R2.

### R4 = INFRA-178. Photon command authorization (narrowed in place)

Scope: a Photon gateway that is a thin caller into the existing policy,
confirmation and audit path (no parallel authorization stack, no new
intents). Only the operator's one-to-one conversation, allowlisted by a
Keychain-stored number under a distinct service; groups rejected; telemetry
required off and verified at startup; a narrow closed grammar mapped 1:1 to
executable intents (R1 defines that set); the same exact confirmation phrase
and expiry; replies limited to low-sensitivity operational status, screened by
the existing forbidden-term guard and truncated. Allowed live-state
mutations: only those the mapped intents already make. Rollback: disabling
the gateway; no state of its own beyond audit. Cleanup: none. Adversarial
tests: group or unknown sender rejected before parsing; telemetry on refuses
start; grammar rejects extra tokens, quoting and Unicode confusables; a
confirmation phrase from another sender or after expiry fails; replay of a
confirmation is refused; no reply ever contains a forbidden term.
Depends on: R1.

### R5 = INFRA-237. Apple Watch response behaviour (new, split from INFRA-178)

Scope: reply shapes that read on a Watch: bounded length, status and
confirmation semantics first, no links or identifiers that need copying,
notifications before any command availability (the design's rollout order),
and confirmation only by typing the exact phrase (no tappable confirm).
Allowed live-state mutations: none beyond R4. Rollback: message formatting
only. Cleanup: none. Adversarial tests: a Watch dictation transcript with
punctuation or casing drift never matches a confirmation phrase by accident;
truncated replies never cut a secret-bearing field into view; a notification
never carries more than the sanitized status view allows. Depends on: R4.

### R6 = INFRA-179. End-to-end remote security acceptance (narrowed in place)

Scope: one adversarial acceptance suite over the assembled production graph
(`build_console_dependencies`), plus a live sign-off document: every
forbidden or retired intent unreachable through API, console and Photon; the
dashboard and daemon never Serve targets; a prepare/confirm/idempotent-retry
round-trip yields exactly one execution and one audit event; session,
CSRF, rate-limit and replay controls hold end to end; Photon allowlisting and
Watch interactions behave as R4/R5 state; clean remote disablement (serve
reset, uninstall) leaves no residue. Allowed live-state mutations: none in
production; the suite runs against isolated state and a disposable tailnet
node where a live leg is required. Rollback: not applicable. Cleanup: the
suite tears down everything it creates and asserts it. Depends on: R1 to R5.

## How INFRA-178 and INFRA-179 change

- INFRA-178 is narrowed in place to R4 (Photon command authorization); its
  Apple Watch half becomes R5.
- INFRA-179 is narrowed in place to R6 with the explicit trust boundaries,
  allowed mutations and adversarial cases above.
- Relations are recorded as Linear "blocks" links (INFRA-234 -> INFRA-178 ->
  INFRA-237 -> INFRA-179, INFRA-235 -> INFRA-236 -> INFRA-179,
  INFRA-234 -> INFRA-179). No workflow comments are added; states stay as
  they are (Backlog/Todo) and only the assignee projection remains Hermes'
  concern.

## Admission rule

Admit R1 (INFRA-234) first. Admit the next issue only after the previous PR merged and its
live evidence, where required, is recorded. Never admit two remote-control
issues concurrently; they share the console process, the Keychain services
and the tailnet node.
