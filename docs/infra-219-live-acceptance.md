# INFRA-219 live dual-lane acceptance — preparation and blocking precondition

Prepared by the Fable lead while the INFRA-217 and INFRA-220
corrections run on their own lanes. This document is the plan and the
evidence frame; the receipt section is filled in only by an actual
live run.

## What the issue requires proven

Verbatim acceptance: "With the development Fable lead actively
coordinating at least three isolated issue lanes, one Hermes command
starts the visible harness lead for INFRA-198. Both leads and their
subagents remain observable in cmux; either lead can restart or rotate
independently; daemon restart restores both bindings; the harness
executes a restart/rotation/recovery proof without pausing
development; and no manual cmux intervention is required."

Sol's correction adds: "Live acceptance proving development
coordinates at least three lanes while the harness starts, rotates,
restarts, and recovers independently with no development pause and no
manual cmux operation."

## BLOCKING PRECONDITION (measured, not assumed)

The live fleet cannot run this proof today:

| Fact | Value |
|---|---|
| ACTIVE runtime | `74518c21e24c08afe0b3ebd59b066355740f3164` |
| Live durable schema | **53** |
| Schema this branch requires | **56** |

Migrations `0054` (project_cells.lane_role), `0055` (lane-scoped
profile_leases) and `0056` (cmux binding + activation-intent lane) are
all on this branch and none are applied live. Every acceptance step —
starting a harness lane, holding two distinct leases, restoring two
bindings across a daemon restart — reads or writes columns the live
database does not have.

**Therefore the proof cannot be produced by a subagent, and cannot be
produced at all until a runtime built from this branch is activated
against the live state directory.** That activation applies three
migrations to the production database. It is an operator-authorized,
hard-to-reverse action on live state, and the lead is deliberately NOT
performing it unilaterally: the correct order is Sol's review of the
corrected candidate, then merge, then a normal runtime activation, and
only then this proof.

Recording this rather than quietly running a surrogate is the point.
The previous Critical arose precisely from claiming a supported path
worked when only module-level behavior had been exercised.

## Acceptance order (operator-authorized, 2026-09-01)

This is a POST-MERGE acceptance gate. It does not block implementation
and it is never satisfied by activating an unmerged branch runtime —
that is explicitly prohibited. The order is:

1. **Code review and merge first.** Sol reviews the corrected INFRA-219
   candidate and merges it to `main` through the normal single-PR path.
2. **Back up the live durable state.** Before any activation, copy the
   production database aside (the established idiom in this state
   directory is a suffixed sibling, e.g.
   `state.db.pre-infra-219-live`, alongside its `-wal`/`-shm`), and
   record the source SHA-256 and the ACTIVE runtime id in the receipt.
3. **Activate ONLY the merged runtime.** Never a branch build. The
   activation applies migrations 0054-0056 to production state; the
   backup from step 2 is what makes that reversible.
4. **Run the six-step dual-lane proof** below, capturing durable rows
   and cmux evidence at every step.
5. **On ANY failure: roll back both.** Re-activate the previous runtime
   (`74518c21e24c08afe0b3ebd59b066355740f3164` at the time of writing —
   re-measure before rolling back) AND restore the database backup from
   step 2. A partially migrated production database with a rolled-back
   runtime is the worst outcome; the two must move together.

INFRA-219 stays OPEN until this proof succeeds. Implementation work
continues meanwhile — the open gate is not a reason to stop.

## Proof procedure (exact, once the precondition clears)

Preconditions to re-measure immediately before starting, and to record
verbatim in the receipt:

1. `hermes-orchestrator status --json` reports `schema_version` 56 and
   the ACTIVE runtime is the merged descendant containing this branch.
2. `git -C <lead worktree> status --porcelain` is clean and HEAD equals
   the pushed head under proof.
3. The development lead cell is live and holds an active profile lease.

Then, capturing durable rows and cmux state at every step:

1. **Three development lanes.** Admit three issues explicitly and
   dispatch them into the development lane. Record `admitted_issues`
   states and `project_cells` — expect three occupying issues and ONE
   development cell (occupancy is per-issue; cell topology is one lead
   per lane). Confirm the bound (`MAX_DEVELOPMENT_ISSUE_LANES` = 6) is
   not exceeded and no heavy test or PR serialization was relaxed.
2. **One command starts the harness.**
   `hermes-orchestrator start-lane --project agent-orchestration
   --lane harness --harness-run <id> --issue INFRA-198`. Record the
   exit code (must be 0 only for a genuine start), the created
   `project_cells` row with `lane_role='harness'`, its distinct
   session, its own worktree, and its own `profile_leases` row with
   `lane_role='harness'` on a DIFFERENT profile from development.
3. **Both observable in cmux.** Capture both panes and the dashboard
   frame showing two lane rows with lane-own issues (harness row must
   show no product issue), subagents, head/event, pressure, blockers.
4. **Independent rotation.** Rotate the harness lead; prove the
   replacement lease carries `lane_role='harness'` and the development
   lease row and session are byte-identical before and after.
5. **Daemon restart.** `launchctl kickstart -k` the daemon; prove both
   `cmux_surface_bindings` rows recover to their exact distinct lane
   worktrees, sessions, and profiles with no manual cmux input.
6. **No development pause.** Show the development lead's session and
   its three lanes progressing across steps 2–5 — no interruption,
   rotation, or worktree reuse.

## Receipt

NOT YET RUN — blocked on the precondition above. This section stays
empty until a real run fills it with durable rows and captured cmux
evidence. It must never be filled from a surrogate, a fixture, or a
dry run.

## Live acceptance failure observed 2026-09-01 (binding)

**The lead was woken manually through cmux after PR #52 merged and
runtime `8ac98e2757ba` was activated.** That is an acceptance failure,
not an operational detail: this issue's whole claim is that Hermes
alone launches, binds, recovers, and drives both lanes with "no manual
cmux intervention".

Measured at the time: the activation durably recorded its receipts —
`daemon.restarted`, `channel.reregistered`, `channel.replayed` — bound
to this exact lead session and sitting in state `published`, i.e. NOT
delivered to the lead. The lead was idle. Nothing converted those
published control operations into a wake of the exact bound session,
and no supported CLI or `hermes-command` surface acknowledges them
either (they were acknowledged for this activation through
`ControlOperations.acknowledge` directly).

**Binding requirement added to this issue's acceptance:** a
merged-runtime or control event (`daemon.restarted`,
`channel.reregistered`, `channel.replayed`, and any future control
kind) published against an ACTIVE cell must wake that exact idle
project lead — the bound session, in its own lane — without cmux
interaction or operator action, and must be acknowledgeable through a
supported surface. Publishing a receipt nobody delivers is the same
class of defect as the projection that was journaled but never
replayed.

Scope note: this is a delivery gap in an existing mechanism, not new
architecture. The durable receipts already exist and are already
bound to the right session; what is missing is the wake and the
acknowledgement surface. No new transport, protocol, or discovery
cycle is warranted.

### Proof step added to the procedure below

7. **Control-event wake.** With the development lead idle, activate a
   merged runtime. Prove the published `daemon.restarted` receipt
   reaches that exact bound session as a wake with no cmux interaction
   and no operator action, and that the lead can acknowledge it
   through a supported command. Repeat for the harness lane.
