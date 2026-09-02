# INFRA-187 delegation plan — paired Fable work and Sol merge leads

Authored by the Fable lead (session `5ffd1831-…`, cell `83b52ed1-…`,
profile max-c). Base: `feature/infra-187` at `794c4d1` (origin/main),
worktree `/Users/josystem/hermes-orchestrator-issue-INFRA-187`. Issue:
[INFRA-187](https://linear.app/jo-solutions/issue/INFRA-187).

## Lead-verified anchors (base 794c4d1)

- `project_cells` (migrations 0001/0054) and `reviewer_channels`
  (migration 0003) are independent; no `project_teams` table or pair
  coordinator exists. `project_cells` has no generation column.
- `CodexMerger` passes `MERGER_MODEL = "gpt-5.6-sol"` into
  `thread/start` only; the model/provider are never persisted, never
  validated on recovery, and `complete_replacement` accepts any thread.
- Candidate wakes already route through generation-bound CAS
  (`review_intake._run_checks`, `CodexMerger.admit_wake`).
- `Scheduler.plan` treats a project as active from
  `cells.active_projects()` (a live development cell alone).
- INFRA-215/219 superseded the 2026-08-28 "second issue held" rule: up
  to `MAX_DEVELOPMENT_ISSUE_LANES = 6` lanes dispatch concurrently.
- Sol never leases a worktree; merge_flow operates on the project repo
  directly, so "no nested Sol checkout" holds by construction.

## Packets

| Packet | Boundary | Files | Tier | Wave |
|---|---|---|---|---|
| 7b6a5222 | `project_teams` durable pair coordinator: identity project_key+generation, member binding to exact Fable cell and Sol channel generation/model, recoverable activation saga, uncertain state, rotate/replace one member preserving the counterpart, evidence-gated retirement, cross-project refusal | migrations/0058_project_teams.sql, project_teams.py, tests/test_project_teams.py | Sonnet | 1 |
| a41c0ec6 | Sol model identity: persist model/provider/model_verified_at on reviewer_channels; validate on creation and recovery; legacy NULL rows reconcile on an authenticated resume; mismatched replacement fails closed | migrations/0059_reviewer_channel_model.sql, codex_merger.py, tests/test_codex_merger.py | Sonnet | 1 |
| (wave 2) | Wiring: scheduler active-project source from ready pairs with startup reconciliation of existing cell+channel into a team; candidate/correction/merge operations resolve through the ready pair; pair admission per resource headroom | runtime.py, scheduler.py, review_intake.py, tests | Sonnet | 2 (after wave 1) |

Out of scope: reverting the six-lane rule, Linear comments, nested Sol
checkouts, PR creation (Sol owns it).

Gate: focused suites per packet; the lead runs the full suite once
before commit, push, and candidate submission.
