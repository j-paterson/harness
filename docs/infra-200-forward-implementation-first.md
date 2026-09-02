# INFRA-200: forward-implementation-first contract, and administrative drift

This is packet 768329ec of the INFRA-200 delegation plan
(`docs/infra-200-delegation-plan.md`). It documents the whole
forward-implementation-first (`fif-1`) contract Sol launches under, and
this packet's own piece of it: administrative-only drift classification as
a scoped-replay hint.

## What the contract is

`fif-1` is a versioned addendum to the Merger's one-paragraph goal,
delivered to every NEW Sol thread at launch. It asks Sol to prefer
forward, additive fixes over wide rework when a candidate's issue is
narrow, and it gives Sol three concrete tools for staying narrow:

- a bounded reviewer-fix budget (below),
- a fixed vocabulary of categories for returning work to Fable instead of
  attempting it under review, and
- the administrative-drift hint on the wake payload (this packet).

The version number exists so a channel's contract can be revised without
silently changing behavior for a thread already mid-review: `reviewer_channels`
carries the contract version it was launched under, and a thread launched
under an older version keeps that version's rules until it is naturally
replaced.

## Versioning: `fif-1`

The first version. A channel's stored contract version is set once, at
launch, from the current version constant. It changes only by launching a
NEW thread (a fresh channel row) -- never by mutating an existing row's
version in place -- so "what rules was this review conducted under" is
always answerable from the durable row itself.

**Existing-thread adoption.** A thread already running under an older (or
no) contract version is never interrupted to pick up a new one. It adopts
the current contract only at its next REAL candidate intake -- the next
time a genuine wake delivers a candidate for review -- never mid-review
and never by a side-channel nudge.

**Idle threads are never woken solely for the contract.** A thread with no
pending candidate is not woken up just to tell it about a new contract
version. The version simply changes for that channel the next time it is
woken for its own reason (a real candidate).

## Reviewer-fix budget

A reviewer-fix stays a REVIEW ACTIVITY, not a rework: **3 files, 50
changed lines, 30 minutes**, with consolidation -- multiple small findings
in one review turn are fixed and committed together, not as separate
commits per finding. The budget is advisory to Sol's own judgment, not a
hard technical enforcement in this repository; `reviewer_fix.py` /
`verdicts.py` (owned by a different packet in this delegation plan) carry
the durable, enforced side of it.

## Return-to-Fable categories

When an issue is outside the reviewer-fix budget or otherwise not a
reviewer's to fix, Sol returns it to the Fable lead as a correction packet
under one of a fixed set of categories (see `reviewer_fix.py` /
`verdicts.py`) rather than attempting an unbounded rework inline. This
packet does not define that vocabulary; it only documents that the
administrative-drift hint below is never itself a category or a reason to
skip returning work.

## Administrative drift: hint semantics (this packet)

`review_drift.py` is a pure classifier: given the previous and current
candidate tree SHAs and the paths that changed between them, it returns a
`DriftVerdict` of `"administrative"`, `"none"`, or `"semantic"`. A change
is `"administrative"` only when every changed path matches a conservative
allow-list (`docs/**`, `*.md` outside `prompts/`, `.hermes/**`, `NOTICE`,
`LICENSE*`, `CHANGELOG*`) -- documentation and repository bookkeeping,
never code or dependencies. A lockfile-only change is deliberately **not**
administrative: a dependency pin can change runtime behavior. Any
uncertainty -- no prior tree on record, or changed paths not reported at
all -- resolves to `"semantic"`, never to `"administrative"`.

`review_intake.py` attaches this verdict to the `AdmittedCandidate` it
returns (its `drift` field) as an advisory hint only, when an optional
`describe_candidate` source is configured; it is absent (`None`) by
default, so every existing caller is unaffected. **The hint never
participates in any admission decision.** Every fail-closed check in
`CandidateAdmission._run_checks` -- the immutable manifest read, the wake
envelope's exact match against it, the ready reviewer channel at the exact
delivered generation, the remote branch head (or its narrowly proven
merged-and-deleted-branch excuse), the base policy, and the intake gate --
runs identically no matter what the hint says. Candidate/base/tree
identity and durable packet integrity always fail closed; the hint exists
only so a caller downstream MAY choose a narrower, scoped semantic replay
instead of a wide one when the only thing that changed since the last
reviewed candidate was administrative.

This preserves everything the issue calls out as must-not-be-invalidated
by stale administrative metadata: verifier attestations, PR/CI/preview/QA
evidence, reviewer-fix receipts, and the submitted-to-merged SHA mapping
are untouched by a "none" or "administrative" verdict, because the hint
never triggers any of the checks or state transitions that would
invalidate them.

## Attribution

The forward-implementation-first idea and its category/budget vocabulary
are adapted from the upstream project
[`Vuk97/forward-implementation-first`](https://github.com/Vuk97/forward-implementation-first),
pinned at commit `91fa46a0108ecfc612a55cf587a2086621a31161` (MIT license).
The upstream `SKILL.md` at that commit has SHA-256
`411a6de1df78dc0bf27572ddd1f88aeca8b1da23537152316e7a581e5e9e5b67`.

This repository adapts the IDEAS and WORDING of that skill into its own
Merger contract and drift classifier; it does not vendor, embed, or fetch
any upstream file, and no code here executes or depends on the upstream
installer at runtime. See the top-level `NOTICE` file for the license
statement.
