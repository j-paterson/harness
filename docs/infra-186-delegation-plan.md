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

- P1 **accepted** (Sonnet): red `ModuleNotFoundError` →
  19 green packet tests, ruff clean; scope exactly its three files;
  interface conformant; integrated in `8b70b39`.
- P7 **accepted** (Haiku): red `ModuleNotFoundError` → 16 green tests,
  ruff clean; scope exactly its three files; integrated in `8b70b39`.
- Lead direct-work exception (documented, reviewer-fix scale): the
  two-field `ClaudeEvent` interface stub enabling wave-2 parallelism
  (`dc4d83b`, 5 lines, one file).
- P2, P3, P4 **reserved** (Sonnet, wave 2, disjoint files) — in
  flight.
- P5, P6 planned (wave 3, blocked on wave-2 integration).
