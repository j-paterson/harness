# INFRA-186 delegation plan — v2

Plan v2, authored and signed by the selected lead after the
authoritative Linear refresh of 2026-08-30T01:48Z (wake
`66aae39c7af240eba590aa8c32538adb`): **lead identity** session
`5158cb1c-4ff8-41be-a686-5ace1a81e2a2`, profile `max-c`, model Fable
(the most competent currently available lead model, selected
independent of implementation cost). Worker routing under v2 begins
only from this frozen version.

## v2 delta: trusted verification receipts

Focused test execution must produce a Hermes-owned, content-addressed
verification receipt — bound to the exact tree/dirty-patch hash,
normalized gate id and command, dependency/lockfile hashes, runner
fingerprint, exit code, test counts, duration, and output hash;
attestable by the runner, never authored by a model. Receipts are
reused while fresh, invalidated on tree/dependency/command/
environment/runner change, wave integration reruns only affected
cross-packet gates, candidate readiness runs the mandatory complete
gate once and publishes its final receipt, and Sol consumes receipts
and reruns only adversarial, affected, or missing/stale checks.

New packets: **P8** (Sonnet, wave 3-parallel; fully disjoint) — the
`verification_receipts` migration 0046 plus the `verifier.py` trusted
runner and validation; **P9** (lead-integrated after P5 returns, since
it extends P5's files) — receipt hooks in the candidate gate and
wave-gate selection. **Invalidation assessment**: no unstarted packet
is affected — P5/P6 were reserved before v2 and their scope is
unchanged (the receipt hook lands as P9 on top of P5's seams); P1–P4,
P7 are accepted and untouched by the delta.

## v3 amendment: shared verifier ownership (before P8 acceptance)

Authoritative Linear changed again at 2026-08-30T02:03:45Z (wake
`e131fd57645942808f3ebc0aab3e3b16`); this amendment reconciles the
active P8 packet to the v3 contract WITHOUT silently redesigning it:

- **Any agent initiates**: a Fable lead, an implementation subagent,
  or the Sol reviewer may invoke the shared verifier. P8's module was
  already requester-agnostic (nothing in `Verifier` binds to a session
  or role); the any-agent invocation surface is a CLI `verify` /
  `verify-check` pair, which was ALREADY outside P8's allowed files
  and lands in the P9/lead integration scope. P8's in-flight contract
  is therefore unchanged.
- **Trust source**: attestation plus runner-measured bound inputs
  only — exactly P8's design (the runner computes every binding
  itself and HMAC-attests the canonical document; a requester or
  Hermes cannot author a validating receipt). No change.
- **Hermes's role**: tracks and validates receipts at transitions
  (P8's `validate`/`find_fresh` are the validation API Hermes and the
  candidate gate consume) but need not execute tests itself. No
  change to P8; the consuming hooks remain P9.

Assignment `3d251e70268e491eb2b597a08969a3df` (exactly-ACKed). The
Fable lead orchestrates; implementation is delegated to the smallest
capable Claude subagents in disjoint, test-first packets. Base:
`feature/infra-186` at `aa385ed` (stacked on the INFRA-195 candidate,
whose migrations 0039–0044 the live database already carries; PR #29
is under independent Sol review). Direct lead implementation is a
documented exception only.

## Independent boundaries → packets

| Packet | Boundary | Files (disjoint) | Tier | Wave |
|---|---|---|---|---|
| P1 | Durable `subagent_packets` ledger + migration 0045 | `src/hermes_orchestrator/migrations/0045_subagent_packets.sql`, `src/hermes_orchestrator/subagent_packets.py`, `tests/test_subagent_packets.py` | Sonnet | 1 |
| P2 | Packet admission service behind the Agent PreToolUse gate | `src/hermes_orchestrator/packet_admission.py`, `tests/test_packet_admission.py` | Sonnet | 2 |
| P3 | Redacted Agent metadata in `ClaudeEventParser` | `src/hermes_orchestrator/claude.py` (parser region only), `tests/test_claude.py` (append only) | Sonnet | 2 |
| P4 | Exactly-once packet/lease settlement on child terminal events | `src/hermes_orchestrator/cells.py` (worker-lease region), `tests/test_cells.py` (append only) | Sonnet | 2 |
| P5 | Typed Hermes packet commands + candidate-ready delegation-evidence gate + direct-work exception | `src/hermes_orchestrator/hermes_tools.py`, `src/hermes_orchestrator/emission.py`, `src/hermes_orchestrator/cli.py`, appends to their tests | Sonnet | 3 |
| P6 | Operator status/usage view: lead vs child work | `src/hermes_orchestrator/service.py`, `tests/test_service.py` (append only) | Haiku | 3 |
| P7 | Model-tier policy characterized from config, never prose | `config/model-tiers.yaml` (new), `src/hermes_orchestrator/model_tiers.py`, `tests/test_model_tiers.py` | Haiku | 1 |

Lead-owned (Fable, not delegated): issue analysis and this
decomposition; the P1 interface contract below; per-wave integration,
diff/scope/red-green verification, and adversarial review; runtime
assembly wiring (`runtime.py`) and lead-prompt updates; migration
numbering; the handoff-reconstruction test; candidate publication and
Merger corrections. Any tightly coupled residue discovered mid-wave is
pulled back to the lead and recorded here.

## Dependency order

Wave 1 (parallel, fully disjoint): P1, P7.
Wave 2 (parallel after P1's ledger interface lands): P2, P3, P4 —
disjoint files by construction; P2 must not touch `claude.py` or
`cli.py`.
Wave 3 (after Wave 2 integrates): P5 (needs the admission API and
CLI seams), P6.
Lead integration + handoff test + gates after each wave; one immutable
candidate only when the issue gates and the new delegation-evidence
gate are green — including this issue's own packets recorded in its
own ledger once P1 lands (the machinery gates its own candidate).

## P1 interface contract (binding for P2/P4/P5)

`subagent_packets.py` exposes `PACKET_MODEL_TIERS = ("haiku", "sonnet",
"fable")`, dataclass `SubagentPacket` (packet_id, issue_id,
project_key, cell_id, session_id, generation, model_tier, effort,
allowed_files: tuple[str, ...], worktree, depends_on: tuple[str, ...],
red_test, verification: tuple[str, ...], invariants, resource_note,
state, evidence: dict | None, timestamps) and class `SubagentPackets`
with: `create(...) -> SubagentPacket` (state `planned`),
`reserve(packet_id, *, session_id, tool_use_id) -> SubagentPacket`
(CAS `planned→reserved`, exactly-once), `settle(packet_id, *,
outcome: "completed"|"failed"|"capped", tool_use_id) -> SubagentPacket`
(CAS `reserved→returned|failed`), `accept(packet_id, *, evidence:
dict) -> SubagentPacket` (CAS `returned→accepted`),
`reject(packet_id, *, reason) -> SubagentPacket`
(`returned|reserved→rejected`), `expire_orphans(*, live_tool_use_ids:
frozenset[str]) -> tuple[SubagentPacket, ...]` (reserved packets whose
tool-use evidence proves absent), `for_issue(issue_id)`,
`get(packet_id)`. States: `planned, reserved, returned, failed,
rejected, accepted` (accepted/rejected terminal; failed retryable via
a fresh reserve after revision). Every transition is a
rowcount-checked CAS journaled to `events`; prompts and model output
are never stored.

## Concurrency and resources

At most three concurrent subagents (current memory/provider headroom
is ample; the admission machinery being built will later enforce this
durably). All subagents share this worktree with disjoint allowed
files; the lead verifies actual `git diff` scope against each packet's
allowed set before staging anything.

## Packet ledger for this issue (updated as waves land)

Mirrored here and recorded in the live `subagent_packets` table in one
batch at final integration (before candidate publication, so the new
delegation-evidence gate judges this issue by its own ledger):

- P1 **accepted** (Sonnet, `fe64a01a…`): red `ModuleNotFoundError` →
  19 green packet tests, ruff clean; scope exactly its three files;
  interface conformant; integrated in `8b70b39`.
- P7 **accepted** (Haiku, `b20d4e1f…`): red `ModuleNotFoundError` →
  16 green tests, ruff clean; scope exactly its three files;
  integrated in `8b70b39`.
- Lead direct-work exception (recorded through the production
  `record_direct_exception` command, `9068723a…`, **accepted**): the
  two-field `ClaudeEvent` interface stub enabling wave-2 parallelism
  (`dc4d83b`, 5 lines, one file — under the ≤2 files/≤30 lines cap).
- P2 **accepted** (Sonnet, `a14dfad7…`): admission service; red →
  green, scope exact; integrated in `2c8a81d`.
- P3 **accepted** (Sonnet, `ffa66736…`): redacted Agent metadata in
  the parser; integrated in `2c8a81d`.
- P4 **accepted** (Sonnet, `6f8aa240…`): exactly-once lease/packet
  settlement; integrated in `2c8a81d`.
- P5 **accepted** (Sonnet, `907bb0f0…`): typed packet commands,
  delegation-evidence gate, direct-work exception command; reasoned
  red accepted for net-new symbols (append-only diff proof; empirical
  stash red impossible in the shared worktree); integrated in
  `7916fd9`.
- P6 **accepted** (Haiku, `a6d48601…`): delegation status view;
  integrated in `7916fd9`.
- P8 **accepted** (Sonnet, `496d8e30…`): migration 0046 +
  `verifier.py` attested receipts, 13 green tests; reconciled to v3
  without redesign; integrated in `7916fd9`.
- P9 **accepted** (Sonnet, `e507684f…`): `validate_for_tree` /
  `receipt_ids_for_gate`, any-agent `verify`/`verify-check` CLI, and
  the candidate-gate receipt enforcement in `CandidateEmitter`; red
  `AttributeError`/unknown-subcommand → 77 focused green, full suite
  1997 green, ruff clean, scope exactly its six files. Found and
  fixed (with a red test) a real argparse dest collision between the
  subparser `dest="command"` and the spec's literal REMAINDER
  positional.
- Lead wiring after P9: `build_merge_flow` constructs the production
  `Verifier` and passes it to `CandidateEmitter`, so a non-trivial
  candidate fails closed without BOTH accepted-packet delegation
  evidence and a fresh `candidate-full-gate` receipt for exactly the
  candidate tree.

All nine packets plus the direct exception are recorded as
**accepted** in the live `subagent_packets` ledger, so this issue's
own candidate is judged by its own machinery.

## Sol correction cycle (PR #30, correction `f3b912a5…`)

Sol returned two Critical defects against reviewed head `9456d33`,
both corrected through delegation packets on disjoint files:

- C1 **accepted** (Sonnet, `4345f613…`): authorized gate
  specifications live in trusted code (`AUTHORIZED_GATES` in
  `verifier.py`); `run_verified` refuses command substitution for an
  authorized gate before running anything; the normalized authorized
  command (`gate_spec`) is recomputed into the HMAC-signed canonical
  document at validation, so changing the specification — or having
  signed before it existed — invalidates the receipt;
  `validate`/`validate_for_tree` additionally report
  `"stale: command not authorized"`. `RUNNER_VERSION` bumped to 2,
  retroactively invalidating every pre-correction receipt. Red: 6
  failing tests; green: 26 verifier tests, emission/cli untouched.
- C2 **accepted** (Sonnet, `3a6a5fee…`): `_enforce_delegation_evidence`
  reconciles the actual base→head numstat against accepted packet
  scopes — complete path coverage (uncovered path named, fail
  closed), deterministic ownership (most recently accepted regular
  packet wins; a regular packet always beats a direct exception for
  the same path; two exceptions sharing a path fail closed),
  identity agreement on `(session_id, worktree, generation)` among
  credited packets, git-measured exception bounds (≤2 credited
  files, ≤30 and ≤declared lines), unparsable/binary numstat fails
  closed, and per-packet credit entries
  (`packet:<id>`, `files=n;lines=m`) preserved in the immutable
  manifest. `record_direct_exception` gained a `worktree` field,
  measures the live diff through a fakeable `run` seam, refuses
  over-bound measurements before creating any packet, and records
  `measured_lines`. Red: 13 failing tests against restored HEAD
  sources; green: 117 focused, full suite 2015, ruff clean.
- Lead coverage packets L1–L4 (fable, `590631fd…`, `b717af50…`,
  `023e759c…`, `7e2b54b7…`, all **accepted**): honest durable
  ownership of the lead-integration paths (runtime wiring, schema
  pins, handoff test, plan/prompt docs) so the corrected
  path-coverage gate judges this candidate truthfully — lead work at
  beyond-exception scale is recorded as auditable fable-tier
  packets, never hidden under an exception.

## Sol correction cycle 2 (correction `e8bb3772…`, reviewed `44cc5d6`)

Two further Criticals. Critical A (receipt signing has no OS
privilege boundary from same-user agents) is recorded as **not
applicable** under the operator-approved MVP threat model: the
operator narrowed INFRA-186 at 2026-08-30T03:06:41Z (correction
`253fdded…`, source `operator_scope`) — same-user
credential/database compromise is outside the local MVP threat
model, and a sandbox, separate OS account, hardware-backed key,
credential-stripped worker environment, or separately privileged
attestation service is explicitly prohibited. Verifier-produced
integrity and stale-input checks are preserved unchanged; no
privilege boundary is implemented. Critical B (path declarations
are not execution provenance; post-hoc packets — L1–L4
demonstrably — satisfy the gate) remains in scope and is corrected
here via packets D1/D2.

### Binding provenance contract (D1 interface, frozen)

- Migration 0047 adds nullable `reserved_blobs_json` and
  `returned_blobs_json` to `subagent_packets`.
- Measurement: for each of a packet's `allowed_files`, the sha256
  hexdigest of the file's bytes in the packet's recorded worktree;
  a missing file records the sentinel `"absent"`. The reader is an
  injectable seam (default: filesystem) so tests never need real
  worktrees.
- `reserve(...)` measures and stores `reserved_blobs` at the moment
  of reservation; an unreadable worktree refuses the reservation
  entirely (CAS untouched).
- `settle(..., outcome="completed")` measures and stores
  `returned_blobs`; measurement failure refuses the settlement
  (packet stays reserved). `failed`/`capped` settlements tolerate
  missing measurement.
- `SubagentPacket` gains `reserved_blobs: Mapping[str, str] | None`
  and `returned_blobs: Mapping[str, str] | None` (None for legacy
  rows — legacy rows are thereby PERMANENTLY without provenance).

### Emission provenance rules (D2)

A credited packet without both blob maps is refused outright. A
credited path earns credit only when the packet's
`reserved_blobs[path] != returned_blobs[path]` (the work happened
inside the reservation window — a packet created after its claimed
changes already exist measures no change and earns nothing) AND
`returned_blobs[path]` equals the sha256 of that path's content at
the candidate head (the integrated fragment is byte-identical to
what the settled packet returned). Direct exceptions must be
measured at record time — absent worktree, git failure, or
unparsable measurement refuses BEFORE any packet is created — and
their recorded hashes must match the candidate head for credited
paths. Agent-authored evidence JSON is never consulted for credit.

### Bootstrap consequence (reported, not bypassed)

Under these rules every packet accepted before migration 0047 lacks
provenance, so this candidate's own emission fails closed on its
historical credit ("missing or failed provenance measurement leaves
the candidate ineligible" — operator test, correction `253fdded…`).
That is the honest reading of the correction; re-emission of THIS
branch requires an explicit operator/Hermes bootstrap decision,
which the lead has requested rather than building a waiver
mechanism nobody authorized.
