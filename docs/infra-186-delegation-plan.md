# INFRA-186 delegation plan

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

Recorded durably in the new `subagent_packets` table once P1 lands;
until then this table row-for-row mirrors it: P1 planned, P7 planned;
P2–P6 blocked on dependencies.
