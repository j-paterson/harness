# INFRA-198 acceptance run — 2026-09-01

Instruction `operator-mvp-acceptance-20260901`, gate predicates read
from `acceptance_gates`. One receipt for this run, as the definition of
done requires. Every fact below is cited from durable state or from git.

**Outcome: the gate is NOT satisfied and was deliberately left
`pending`.** One predicate of four is fully proven. `satisfy` demands
evidence covering *every* predicate and stores it as an immutable
operator record, so satisfying it on partial evidence would forge the
acceptance decision this issue exists to make honestly.

## 1. Three admitted issues complete intervention-free — NOT MET (2 of 3)

The count only starts after the defect that made intervention mandatory
was fixed. INFRA-219 recorded its own manual-cmux wake as a binding
acceptance failure (`20b57ef`), repaired by the control-wake delivery
fix in PR 53 (`b668781`). Completions after that point:

| issue | candidate | verdict path | merge |
| --- | --- | --- | --- |
| INFRA-214 | `4309c00` → rework `2517c71` | Sol correction `d85c374d` retrieved from durable state | PR 54 `eedd7c8` |
| INFRA-198 | `0a6c92d` | Sol review | PR 55 `208d792` |

Both ran assignment → implementation → exact-SHA candidate → Sol
review/correction → merge with packets delivered over the channel and
retrieved from durable state by id: no `/mcp` reconnect, no Computer
Use, no raw payload paste, no screen inference. That is **two**, not
three.

Two caveats the operator should weigh rather than have decided for
them:

- During the INFRA-214 wake the reviewer thread exceeded the
  app-server JSONL intake size and was **replaced** (generation 2,
  `reviewer_channels.replacement_reason`). The definition of done
  permits short-lived app-server use to recover an unreadable thread,
  so this is within contract — but it was a real recovery event, not a
  clean run.
- Both issues took substantive **operator corrections on the work
  itself** (for example: a candidate that added a function with no
  production caller, so a green gate proved nothing about the live
  blocker). These are review interventions, not the transport/payload
  interventions the definition of done prohibits. They are recorded
  here because "intervention-free" should be judged by the operator,
  not quietly interpreted by the lead in its own favour.

## 2. Hermes, Fable and Sol restart recovery — PARTIAL

- **Hermes: proven.** Repeated daemon restarts today, each followed by
  a completed reconciliation and lifecycle receipts delivered to the
  bound lead and acknowledged over the channel — generation 57
  (`eedd7c89`) and the current activation of the merged `208d792`,
  whose `daemon.restarted`, `channel.reregistered` and
  `channel.replayed` receipts were all offered, retrieved by id, and
  settled.
- **Fable: proven for this session.** This lead was seated by a
  Hermes-managed rotation and resumed the exact assigned issue with no
  duplicate work, transition, PR or merge.
- **Sol: not a drill.** The reviewer thread did recover and resume
  delivery (`last_delivered_event_id` is the INFRA-198 candidate), but
  it recovered *reactively* from an oversized-intake failure. No
  deliberate three-way restart drill at a safe boundary has been run.

## 3. Managed account rotation changes profile and session — MET

`project_cell.rotated` at 2026-09-01T04:28:23Z: `from_profile` `max-c`
→ `to_profile` `max-a`, handoff `cc960a7b`, replacement session
`8f880073`, followed by `lead_rotation.completed` for the same handoff.
Hermes generated the handoff, started the replacement, waited for its
confirmation and retired the incumbent. Both profile and session
changed.

## 4. Dashboard shows work, resources, subscriptions, transitions — NOT ASSERTED

`DashboardSnapshot` carries exactly the four required families (tasks/
lanes/workers, resource/capacity, usage/capacity, and human-readable
transitions), so the data exists. But this predicate is about what is
**visible in the cmux pane**, and the only ways for this lead to claim
it are reading the rendered screen or inferring from it — which the
definition of done explicitly disallows as an acceptance method. It
therefore needs the operator's own observation. Marking it met from
here would be exactly the kind of inference this issue forbids.

## Defect found by this run

`bind_issue_worktree`'s idempotent path returns an existing live lease
without checking that the lease's recorded branch still matches the
branch being requested. Rebinding INFRA-198's lane to a new branch
therefore reported success (`bound_issue_lanes: ["INFRA-198"]`) while
the lease still recorded the previous one.

It is not silently harmful — `candidate-ready` cross-checks the lease
branch against the frozen branch and refused the publish outright
("resolved lease branch ... disagrees with the frozen branch"), which
is the guard working. But the bind command reporting success for a
binding it did not perform is the same "reports success while wrong"
class this issue keeps surfacing, and it should either update the lease
deliberately or refuse. Not fixed here: it is outside this acceptance
run's scope, and the choice between updating and refusing deserves its
own decision.

## What remains

1. One more intervention-free issue end to end (third of three).
2. A deliberate restart drill covering Sol at a safe boundary.
3. Operator confirmation of the visible dashboard pane.

The harness no-click blocker that motivated the previous assignment is
fixed and now live: trust adoption (PR 55) is present in the running
runtime `208d792`. It has not yet been **proven** live — that needs a
harness `start-lane` whose seat auto-confirms with zero dialog clicks,
which is the natural first half of item 1.
