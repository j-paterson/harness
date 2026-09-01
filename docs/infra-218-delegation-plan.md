# INFRA-218 delegation plan — convergent supersession and Linear projection

Authored by the Fable lead (session `8f880073-…`, cell `b29691ef-…`)
as the project-level coordinator, from the current Linear text plus a
LIVE reproduced blocker Sol hit while reviewing INFRA-217. Base:
`feature/infra-218` at `74518c2` (origin/main = ACTIVE runtime),
isolated worktree `/Users/josystem/hermes-orchestrator-infra-218`.
Issue: [INFRA-218](https://linear.app/jo-solutions/issue/INFRA-218).

## The reproduced blocker (live durable rows, 2026-09-01)

Read from the production state database:

| Table | INFRA-216 row |
|---|---|
| `reviews` | `state=merged`, `pr_number=48`, `merge_sha=74518c21e24c` (proven; that commit IS origin/main) |
| `merge_settlements` | `state=merging`, `path=externally_merged`, `pr_number=48`, **`merge_sha=NULL`**, lease expired `06:54:14Z` |
| `wake_deliveries` | `state=admitted` — the OLDEST outstanding wake |
| `wake_deliveries` | INFRA-217 `state=delivered` — masked behind it |

Chain: the INFRA-216 settlement was reconciled as externally merged
and advanced to `merging`, but never reached `settled` (no
`merge_sha`). INFRA-216's own stale-wake reconciliation (shipped in
`outstanding_wake`) is deliberately fail-closed — it requires
`reviews.state='merged'` **AND** `merge_settlements.state='settled'`
— so it correctly declines this row. The stale `admitted` INFRA-216
wake therefore keeps masking INFRA-217's `delivered` wake, and
`submit_review` rejects Sol's INFRA-217 verdict with "submission
event does not match the outstanding wake". **A merged PR is holding
durable settlement hostage** — exactly the class this issue exists to
close.

Note the two Linear clauses map onto this differently: supersession
(clause 1) is per-issue and does NOT rescue this row (INFRA-216 and
INFRA-217 are different issues). What rescues it is clause 2's
principle — durable settlement must follow Git and GitHub truth: a
settlement whose pull request is provably merged must converge to
`settled`, never stick in `merging`.

## Lead-verified anchors (base 74518c2)

- `ReviewService.merge_approved` (reviews.py:397) skips the
  pre-mutation fence for a `merging` row, claims the expired lease,
  and calls `_drive_merge`; it marks `settled` only when the drive
  returns `state == "merged"`. The live row proves that path does not
  converge for an already-externally-merged settlement whose review
  record is ALREADY `merged` — that is the defect to find and fix.
- `MergeSettlements.resumable` (settlement.py:329) already re-offers a
  `merging` row once its lease expires, so the resume boundary is
  reached; convergence, not scheduling, is what fails.
- `reconcile_external_merge` (reviews.py:887) performs NO merge and
  reconstructs receipts under full proof — the correct convergence
  target for an externally merged PR.
- Supersession target: `outstanding_wake` / the wake tables, where a
  newer descendant candidate for the SAME issue must retire that
  issue's obsolete unreviewed or correction-completed wakes.

## Packet

| Packet | Boundary | Files | Tier | Wave |
|---|---|---|---|---|
| S1 | (a) **Convergence**: a settlement whose PR is provably merged at the exact reviewed head converges to `settled` with its real `merge_sha` instead of stranding in `merging` — driven through the existing external-merge reconciliation, performing NO new merge, fully idempotent. (b) **Supersession**: a newer candidate for the same issue whose commit is a descendant of an older candidate retires that issue's obsolete unreviewed/correction-completed wakes. (c) **Linear never blocks**: a Linear projection failure can never prevent durable review or merge settlement; it is reconciled separately afterward through supported states. | `src/hermes_orchestrator/reviews.py`, `src/hermes_orchestrator/settlement.py`, `src/hermes_orchestrator/merger_turns.py`, `tests/test_settlement.py`, `tests/test_merger_turns.py` | Sonnet | 1 |

Priority under the time bound: (a) first — it is the live blocker —
then (b), then (c). Strict manifest-hash and candidate-SHA validation
and exact candidate identity stay binding; no new protocol,
provenance, sandbox, or retry machinery.

Gate: focused suites in this worktree only; the coordinator serializes
the single heavy gate across lanes and owns push + candidate emission.
Sol reviews, may make bounded fixes, creates the sole PR, and merges.
