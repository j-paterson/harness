# INFRA-184 delegation plan — mechanical Claude lead handoffs

Authored by the Fable lead (session `5ffd1831-…`, cell `83b52ed1-…`,
profile max-c). Base: `feature/infra-184` at `794c4d1` (origin/main),
worktree `/Users/josystem/hermes-orchestrator-issue-INFRA-184`. Issue:
[INFRA-184](https://linear.app/jo-solutions/issue/INFRA-184).

## Lead-verified anchors (base 794c4d1)

- `LeadRotation.rotate` (lead_rotation.py) consumes the newest
  `submitted` handoff after a worktree dirty/HEAD gate but never compares
  the handoff's own branch/HEAD/issue facts with current state — the
  INFRA-192 regression (a merged issue's stale handoff was consumed and
  the cell transferred). `_request_fresh_handoff` is the existing
  fresh-handoff path.
- `ProjectCellService._observe_context` (cells.py) derives percent from a
  single top-level assistant usage record; `ContextMonitor.record`
  (context.py) never validates or clamps, so an impossible percentage
  (observed 1704%) flows straight into rotation reasons.
- `derived_handoff_document` (handoffs.py) hardcodes PR/modified-files/
  test evidence placeholders; correction packets and ownership are never
  derived; no identity check at submission.
- `ProfilePool.reserve_context_only` (profiles.py) exists but is never
  called: every rotation switches account via `reserve_replacement`.
- Linear projection (linear.py) exposes assignee/status only; no comment
  mutation exists, and none is added.

## Packets

| Packet | Boundary | Files | Tier | Wave |
|---|---|---|---|---|
| 7b6b024c | Rotation staleness gate: compare submitted handoff branch/HEAD/issue state with durable + worktree state before transfer; stale routes to `_request_fresh_handoff`; handoff stays reusable | lead_rotation.py, tests/test_lead_rotation.py | Sonnet | 1 |
| a925eeee | Context occupancy hardening: single-invocation estimate, authoritative window preferred, clamp + measurement-uncertainty journal, uncertain signal never rotates alone; renewal/compaction/refill/context-error evidence untouched | context.py, cells.py (`_observe_context`), tests/test_context.py, tests/test_cells.py | Sonnet | 1 |
| 241898bc | Derived handoff completeness: issue state, PR, changed files, test results, pending corrections, ownership from durable state/git; mechanical cell/session identity validation at submission | handoffs.py, cli.py (`_submit_handoff` helpers), tests/test_handoffs.py, tests/test_cli.py | Sonnet | 1 |
| 6a0bfb0f | Session vs account rotation: context/renewal/ordinary handoff reuse the healthy profile (`reserve_context_only`), provider limit switches account; ack turn on smallest suitable effort with the compact snapshot | cells.py (`rotate`), claude.py, lead_rotation.py, tests | Sonnet | 2 (after wave 1 packets sharing cells.py / lead_rotation.py) |

Out of scope: Linear comments (forbidden), any new transport, PR
creation (Sol owns it).

Gate: focused suites per packet in this worktree; the lead runs the
full suite once before commit, push, and candidate submission.
