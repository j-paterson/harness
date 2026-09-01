# INFRA-205 — honor durable Fable caps on the managed seat path

Assignment `8e1b5078`, instruction `infra-205-live-harness-cap-20260901`.
Scope is exactly the observed managed path. No manual profile override,
no cmux input, no Bedrock, no new protocol.

## Observed

Hermes seated `max-b` for new harness cell
`8c2c53ae-00d8-4762-8bc8-c9d823e20b29` (session
`f8478b07-f248-4e9e-908f-98233025c0b7`). Durable state already knew
better:

| observation | profile | model | state | resets_at |
| --- | --- | --- | --- | --- |
| 4 (`provider_limit`) | `max-b` | fable | **capped** | 2026-09-08T04:21Z |
| 3 (`operator_attestation`) | `max-c` | fable | available | — |

The canary reached the session exactly once, Claude immediately
reported its Fable limit, and assignment
`39a4d3eec6a94171bc86cd9f4dcebd9c` remains `published` (unconfirmed).
`rotate-lead` then failed its precondition — "no handoff has been
submitted for this cell" — because the capped worker died before it
could author one.

## Root causes

**1. The initial reservation never consults capacity.**
`ProfilePool.lease()` admits a profile on `state.eligible`, cooldown
and `active_project_count == 0` alone. The capacity check
(`_capacity_refusal`) exists and is already applied by
`reserve_replacement`, the ROTATION path — so rotation honours a cap
that first seating ignores. `max-b` was capped for another week and was
still seated.

**2. A pre-handoff provider limit leaves no durable trace.** In
`ProjectCellService`'s stream loop, a `provider.limit` event before
session confirmation sets `start_capped` and breaks into
`_fail_unconfirmed_start(capped=True)`. The reservation is released
without recording a NEW capacity observation, so the next seating has
learned nothing from the failure.

**3. A capped worker cannot hand off, so rotation is unreachable.**
`rotate-lead` requires a submitted `HandoffDocument`. A worker that hits
its provider limit at startup never authors one, so the lane is stuck:
the incumbent is capped, and the one command that would replace it
refuses its precondition.

## The change

**A. Capacity gates first seating too.** `lease()` applies the same
`_capacity_refusal` that `reserve_replacement` already applies, so one
rule governs both paths. A profile whose latest fable observation is
`capped` with an unexpired `resets_at` is ineligible even when
authentication is healthy. Keep the existing semantics exactly: no
capacity port wired means unchanged historical behaviour, and a passed
`resets_at` is eligible-to-probe, not confirmed available.

**B. A pre-handoff provider limit is persisted before the reservation
is released.** On the `start_capped` path, record the newest capacity
observation for that (profile, `fable`) from the limit event —
`source=provider_limit`, with the stream's reset horizon when it
carries one and the existing worst-case weekly horizon when it does
not, matching how observation 4 was recorded. Persist BEFORE releasing,
so a concurrent seating cannot re-pick the profile that just died.

**C. Hermes forms the minimal handoff mechanically.** When a seated
worker reports a provider limit before submitting one, Hermes composes
a `HandoffDocument` from durable facts only — the cell, its issue and
lane, the pending assignment, the lane's branch — with every required
list filled from what is durably known and nothing invented. It is
explicitly marked as machine-formed for a capped seat, so it can never
be mistaken for a lead's own account of its work. That unblocks the
EXISTING `rotate-lead` path: select the next healthy first-party Max
profile, wait for its confirmation, transfer the pending packet, retire
the capped seat.

On a failed replacement the incumbent, handoff, cell, lease and cmux
binding stay exactly as they are — the current fail-closed behaviour is
preserved, not replaced.

## Tests

- `lease()` refuses a profile whose latest fable observation is capped
  and unexpired, and admits it once `resets_at` has passed;
- `lease()` with no capacity port behaves exactly as before;
- a pre-confirmation `provider.limit` persists a new capped observation
  for that profile before the reservation is released;
- the mechanically formed handoff validates and carries the exact cell,
  issue, lane and pending assignment from durable state;
- after that handoff exists, `rotate-lead` selects a healthy profile
  (the INFRA-205 shape: capped incumbent, healthy `max-c`) and leaves
  the incumbent untouched when no eligible profile remains.

## Publication blocker (surfaced early)

`INFRA-205` is `done` in `admitted_issues`, so no worktree lease can
bind it (`done` is correctly not lane-bindable) and `candidate-ready`
will refuse. Re-admitting the issue to an active state is Hermes's
transition to make, not this lead's to invent. Flagged now so it can be
resolved in parallel with implementation rather than at publish time.
