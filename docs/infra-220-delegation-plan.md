# INFRA-220 delegation plan — target an explicitly admitted issue

Authored by the Fable lead (session `8f880073-…`, cell `b29691ef-…`)
as the project-level coordinator. Base: `feature/infra-220` at
`74518c2` (origin/main = ACTIVE runtime), isolated worktree
`/Users/josystem/hermes-orchestrator-infra-220`. Issue:
[INFRA-220](https://linear.app/jo-solutions/issue/INFRA-220) — one
strict transition that assigns an already-admitted issue to a named
existing eligible cell/session, without touching unrelated queue rows.

## Lead-verified anchors (base 74518c2)

- `LeadAssignments` (src/hermes_orchestrator/lead_assignments.py)
  already owns the durable assignment packet lifecycle:
  `publish_in(...)` (transactional publish), `acknowledge(...)`
  (exact ACK), `pending_for_session(...)`. The new transition REUSES
  this — it never invents a second offer/ACK path.
- `ProjectCellService` (src/hermes_orchestrator/cells.py) holds the
  durable cell truth: `_find_active_cell(project_key)`,
  `current_session(cell_id)`, `_activate_issue(...)`, profile-lease
  restoration. The scheduler's preview picks the oldest
  equal-priority queue row; that ordering is untouched.
- The queue (queue.py) admits explicitly; this transition consumes an
  ALREADY-admitted row and reorders nothing.

## Packet

| Packet | Boundary | Files | Tier | Wave |
|---|---|---|---|---|
| T1 | The strict `target-issue` transition: validate issue/project/cell/session/lease/instruction against durable rows, publish the exact assignment packet through the EXISTING LeadAssignments machinery, require the normal durable offer/ACK before any In Progress projection, mutate no unrelated queue rows, exactly-once under duplicates and crash-resume | `src/hermes_orchestrator/issue_targeting.py` (new), `tests/test_issue_targeting.py` (new), minimal CLI wiring in `src/hermes_orchestrator/cli.py` (one subcommand) + its focused additions in `tests/test_cli.py` | Sonnet | 1 |

Out of scope: scheduler ordering changes, lane roles (INFRA-219),
any new transport/protocol/provenance, PR2, Linear workflow changes
beyond the existing supported projection call after ACK.

Gate: focused suites in this worktree; the coordinator serializes the
one full-suite gate across lanes and owns push + candidate emission.
