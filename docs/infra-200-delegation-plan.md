# INFRA-200 delegation plan — forward-implementation-first Sol launches

Authored by the Fable lead (session `5ffd1831-…`, cell `83b52ed1-…`,
profile max-c). Base: `feature/infra-200` at `794c4d1` (origin/main),
worktree `/Users/josystem/hermes-orchestrator-issue-INFRA-200`. Issue:
[INFRA-200](https://linear.app/jo-solutions/issue/INFRA-200).

## Lead-verified anchors (base 794c4d1)

- `prompts/codex-merger.md` is loaded into `CodexMerger._contract` but
  never transmitted to the Codex thread; only the one-paragraph
  `MERGER_GOAL` reaches Sol via `thread/goal/set` on create and resume.
  The stale read-only/never-merge wording is already gone from the file.
- `reviewer_channels` has no contract version column.
- `reviewer_fix.py` enforces `MAX_FILES = 2`, `MAX_CHANGED_LINES = 30`
  with excluded path categories; no consolidation of findings.
- `verdicts.py` validates only `approved`/`corrections_required`;
  `ACCEPT_WITH_REVIEWER_FIX` is a commit label only.
- No administrative-vs-semantic drift concept exists in review intake.
- No NOTICE/attribution file exists.

## Packets

| Packet | Boundary | Files | Tier | Wave |
|---|---|---|---|---|
| 252fd673 | Versioned contract (`fif-1`) delivered to every new Sol thread at launch; existing threads adopt at next real candidate intake or replacement; idle threads never woken for it; one-paragraph prompt rewritten with the forward-implementation-first rules | migrations/0060_reviewer_channel_contract_version.sql, codex_merger.py, merge_flow.py, merger_turns.py, prompts/codex-merger.md, tests | Sonnet | 1 |
| e6ea8a46 | Reviewer-fix budget 3 files / 50 lines / 30 min advisory, category-named refusals, consolidation of findings into one labeled commit; ACCEPT / ACCEPT_WITH_REVIEWER_FIX / REWORK_REQUIRED labels normalized onto the durable verdict values without schema change | reviewer_fix.py, verdicts.py, tests | Sonnet | 1 |
| 768329ec | Administrative-only drift classifier surfaced as a scoped-replay hint on the wake payload with all fail-closed identity checks unchanged; design doc and NOTICE attributing the pinned upstream MIT source | review_drift.py, review_intake.py, docs/infra-200-forward-implementation-first.md, NOTICE, tests | Sonnet | 1 |

Out of scope: executing or depending on the upstream installer, any
change to manifests/verifier receipts, PR creation (Sol owns it).

Gate: focused suites per packet; the lead runs the full suite once
before commit, push, and candidate submission.
