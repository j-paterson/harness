# INFRA-216 delegation plan — command-center recovery keeps the goal running

Authored by the Fable lead (session `8f880073-9975-4ffe-a292-6550787ce4b1`,
profile `max-c`, cell `b29691ef-…`) under an explicit manual operator
bootstrap (the Hermes operator pane is in a restart loop; there is no
intake packet for this assignment, by operator statement in-session,
2026-08-31). Base: `feature/infra-216` at `5e7d148` — branched from
`origin/main`, which is byte-identical to the live ACTIVE runtime
checkout (`runtimes/5e7d148cb91f…`).

## Assignment (Linear INFRA-216, read live) and operator narrowing

The issue records the observed MVP failure: the daemon replaced the
Orchestrator workspace while the chat pane held an active persistent
`/goal`; SIGHUP interrupted the active Hermes turn; the rebuilt
workspace was malformed (chat pane only); the reattached chat restored
the goal row but sat idle instead of resuming.

Operator directive (in-session, 2026-08-31, twice restated): **at most
two implementation packets**, covering exactly the two observed
behaviors — (1) launch the dashboard from the active runtime and
preserve the two-pane binding without replacing a healthy workspace;
(2) after an actual recovery, resume the persisted active Hermes goal
exactly once — with focused tests for only those behaviors. No broad
lifecycle audit, no adjacent hardening, no additional acceptance
machinery. The issue's remaining required behaviors are deferred and
recorded in *Out of scope* below.

## Observed failure mapped to code (lead-verified)

- `orchestrator_workspace.dashboard_pane_command` composes the upper
  pane as `uv run hermes-orchestrator --repo-root <root> --state-dir
  <state> dashboard …` — the **mutable config checkout**. When that
  checkout's migration generation (52) trails the live schema (53),
  the respawned dashboard dies and a rebuild leaves only the chat
  pane: the malformed replacement the issue records. The deployment
  already renders an active-runtime launcher
  (`<state-dir>/bin/hermes-orchestrator`, a `deploy-render` shim that
  `exec`s `uv run --project $ACTIVE …`), which the pane command does
  not use.
- `hermes_process_shape` (lower role) requires every non-wrapper
  process in the pane tree to be python/`hermes`/`node`. Any tool
  child the chat itself launches (the reproduced case: a
  chat-launched `uv run hermes-orchestrator dashboard`) fails the
  shape, so `_adopt` treats a **healthy** chat pane as failed; the
  workspace is closed and rebuilt (`ensure` →
  `close_workspace` + `_create(replacing=…)`), SIGHUPing the live
  Hermes turn.
- Recovery reattaches the same durable session
  (`hermes chat --continue <name> --create-if-missing`), so the
  `/goal` row survives — but nothing resumes it; the replacement
  idles with an active goal.

## Packets

| Packet | Boundary | Files (disjoint) | Tier | Wave |
|---|---|---|---|---|
| W1 | Active-runtime dashboard command + healthy-workspace preservation: the upper pane launches through the installed active-runtime launcher when present (`uv run` composition retained only when no launcher is installed — isolated/dev state dirs); the lower role is proven by the exact live `hermes chat --continue <session> --create-if-missing` argv lineage on the surface's own correlated tree, and extra non-wrapper descendants (chat tool children) no longer fail a pane whose expected lineage is alive — no respawn, no close, no rebuild. All existing fail-closed refusals (absent lineage, failed/ambiguous correlation, wrong session) unchanged. | `src/hermes_orchestrator/orchestrator_workspace.py`, `tests/test_orchestrator_workspace.py` | Sonnet | 1 |
| W2 | Exactly-once goal resume after an actual recovery: a durable resume wake claimed per (binding_id, generation) — insert-claim, deliver, CAS-settle, crash-resume on redelivery — delivered to the exact named durable Hermes session as one fixed bounded resume literal (a no-op when no goal is active), plus ONE concise human-readable journaled event explaining the recovery; wired where the daemon observes the owner's returned `OrchestratorWorkspaceState`. | `src/hermes_orchestrator/chat_resume.py` (new), `tests/test_chat_resume.py` (new), one new migration, minimal wiring in `src/hermes_orchestrator/runtime.py` | Sonnet | 1 |

Lead-owned: this plan; diff-scope verification against each packet's
allowed files; focused + full test gates and Ruff; integration
commits; candidate publication to Sol. W1 and W2 run in parallel
(fully disjoint files; W2 reads but does not modify
`orchestrator_workspace.py` — the `outcome`/`respawned`/`generation`
fields it consumes already exist).

## Resume-wake semantics (binding for W2)

- **Actual recovery only**: `outcome == "created"` with a replaced
  binding, or `outcome == "recovered"` with the lower role in
  `respawned`. Plain adoption and first-ever creation deliver
  nothing.
- **Exactly once**: the durable claim row is unique per
  (binding_id, generation); a crash after claim but before settled
  delivery re-attempts on the next pass; a settled row never
  redelivers.
- **Delivery**: non-interactive, into the running named session via
  the installed Hermes Agent CLI's send surface (implementer verifies
  the exact invocation against `hermes send --help` on this host and
  pins it in the module docstring; argv grammar-bound: bounded
  session token + one fixed literal, nothing parameterizable). A
  failed delivery journals the failure and leaves the claim
  unsettled for retry — never a duplicate settle.
- **The wake is a signal, never a payload**: the fixed literal
  instructs the chat to resume its own persisted active goal if one
  exists; durable chat-side state remains the only source of what to
  resume.

## Out of scope (deferred, operator-directed)

The chat bootstrap supplying the active runtime binary/repo-root/
state-dir triple; CLI-side refusal of `dashboard`/`daemon`/
workspace-lifecycle commands from inside marked panes; the Harness
Lab live acceptance run; any new transport or protocol. These remain
open INFRA-216 requirements for operator follow-up; this candidate
deliberately does not claim them.

## Ledger note

The packet-provenance ledger could not be reserved for this
assignment (operator pane down; explicit manual bootstrap with no
intake packet). Delegation is recorded here and in the candidate
message to Sol.
