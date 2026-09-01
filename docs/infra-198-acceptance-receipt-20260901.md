# INFRA-198 acceptance run — 2026-09-01

Instruction `operator-mvp-acceptance-20260901`, gate predicates read
from `acceptance_gates`. One receipt for this run, as the definition of
done requires. Every fact below is cited from durable state or from git.

**Outcome: the gate is NOT satisfied and was deliberately left
`pending`.** One predicate of four is fully proven. A 14:00Z addendum
claiming the live no-click proof had succeeded was WRONG and is
corrected at 14:20Z below. `satisfy` demands
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

## Correction — 14:20Z: the 14:00Z addendum below was WRONG

**The live no-click proof did NOT succeed. The addendum that follows
overstated it and is retained, struck, as the record of the error.**

What actually happened: the trust receipts fired exactly as described —
the anchor was adopted and the seat auto-confirmed with no clicks, and
that mechanism does work. But the seat behind them is not a running
worker. Workspace `C39EBCDC` and surface `F5B1BD55` still exist, while
`claude --resume 9b539c86` had already exited with "No conversation
found": the pane is a bare SHELL PROMPT. There is no managed process
record for that session at all.

The error was one of inference, not measurement. Trust receipts prove a
trust decision; they do not prove a live worker. The claim required
checking the worker too, and it was not checked before the claim was
made.

The same wrong assumption is what the code made. `surface_alive` was
the only liveness test anywhere, so a surface that outlives its worker
reads as healthy forever: the dead-lead sweep never condemned this
seat, and `start-lane --lane harness` reported `already_running` and
exited 0 for a corpse. Both are fixed in this candidate by measuring
the exact managed claude process for the session, tri-state, retiring
only on a DEFINITIVE absence (an unknown `ps` result is treated as
alive, so a transient `ps` failure can never retire a healthy seat).

Predicate 1's third completion, claimed below, therefore also does not
stand on this evidence.

## ~~Addendum — 14:00Z: the live no-click proof succeeded~~ (superseded, see above)

The harness no-click acceptance that every prior run was blocked on is
now **proven live**, on the daemon generation 59 restart onto merged
runtime `2020a207`:

| time | durable evidence |
| --- | --- |
| 13:58:28 | startup reconcile replaced the dead harness seat: binding `a5b7fe43`, new workspace `C39EBCDC` |
| 13:58:30 | `channel_trust_anchor.adopted` — cell `8369559d` adopted the development cell `b29691ef`'s proven anchor `e6775cab` |
| 13:58:31 | `channel.confirm_claimed` → `channel.auto_confirmed` → `channel_trust.confirmed` |

**Zero manual dialog clicks.** The harness lane that structurally could
never pass `anchor_present` now seats and confirms itself.

The adoption crossed profiles — predecessor `max-a`, adopter `max-b` —
which the safety body permits only through the durable-selection proof,
and the event payload carries exactly that (`selected_binding_id`
`a5b7fe43`, `selected_intent_id` `0f2293d8`). The profile change was
honored as Hermes's own selection, never from caller input.

One expectation of mine was wrong and is corrected here: I looked for
the new per-tick dead-lead sweep to have retired cell `8369559d`, and it
had not. The binding read `stale`, which I first took for residue.
`stale` means REPLACED — the startup reconciler had already relaunched
the seat, so the cell was legitimately active with a working binding and
there was nothing dead left to retire. The sweep correctly did not fire.
The restart path resolved this instance before the per-tick path was
needed; the sweep remains the cover for a worker that dies *between*
restarts, which is the case that produced the original blocker.

### Predicate 1 now has a third completion

`INFRA-198` PR 56 (`2020a207`) completed assignment → candidate → Sol
correction → rework → merge. That makes three since the control-wake
fix, alongside INFRA-214 (PR 54) and INFRA-198 (PR 55).

It is recorded as a **candidate** third, not a claim. During that run
Sol's correction `cde70842` was announced to the dead harness session
instead of this lead and needed an operator reroute — an intervention,
even though the durable delivery itself routed correctly and the fix for
that misroute shipped in the same PR. Whether a run that required a
reroute counts toward "intervention-free" is the operator's call, and it
is left as the operator's call rather than resolved in the lead's own
favour.

## What remains

1. A third intervention-free issue end to end. The PR 56 candidate for
   that slot does not stand: it rested on the 14:00Z live-proof claim,
   which was wrong, and its own run also required an operator reroute
   of Sol's correction.
2. A deliberate restart drill covering Sol at a safe boundary.
3. Operator confirmation of the visible dashboard pane.

The harness no-click blocker that motivated the previous assignment is
fixed and now live: trust adoption (PR 55) is present in the running
runtime `208d792`. It has not yet been **proven** live — that needs a
harness `start-lane` whose seat auto-confirms with zero dialog clicks,
which is the natural first half of item 1.
