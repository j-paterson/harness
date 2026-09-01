# INFRA-215 — wake an idle seat that has safe admitted work

Assignment `212e01c0`, acknowledged by this session. Linear INFRA-215 is
the specification.

## The observed first blocker

An acknowledged, idle development seat with safe admitted work receives
nothing. It waits, and the project stalls at one issue at a time.

The cause is visible in the wake vocabulary itself:

```python
WAKE_KINDS = frozenset(
    {"completed", "provider_capped", "blocked", "handoff_required"}
)
```

Every one of those is a **terminal-boundary** signal — each is committed
because a lead turn ENDED. There is no signal that means "you are idle
and there is runnable admitted work." So a lead that finishes everything
in front of it is never told that the ranked queue still holds safe
lanes, which is exactly the one-issue-at-a-time bottleneck this issue
exists to remove.

Measured now: two admitted issues are runnable (`INFRA-198`,
`INFRA-215`), one is dependency-gated (`INFRA-211`,
`dependency_ready = 0`), two are paused, and the development cell
`b29691ef` is active with no other lane dispatched.

## Slice 1 — the wake (this pass)

One additional kind in the existing wake vocabulary, on the existing
`lead_terminal_wakes` table and the existing delivery path. **No new
transport, table, or protocol** — a new value in a vocabulary that
already has four is not protocol expansion.

Hermes commits exactly one such wake for a project when ALL hold:

- the development cell is `active` and its seat is idle (its assignment
  acknowledged, no turn in flight);
- at least one admitted issue is genuinely runnable — queued,
  `dependency_ready = 1`, not paused, not already lane-bound;
- active issue lanes are fewer than both the runnable count and the
  canonical six-lane bound (`development_lane_saturated`, reused —
  never re-implemented);
- the resource budget permits another lane.

Exactness is what makes this safe to run on every tick. The existing
commit already dedups on
`(project_key, cell_id, session_id, turn_key, kind)`, so the turn key
must be derived from the underutilization CONDITION, not from a clock:
the same idle seat facing the same runnable set must produce the same
row and journal nothing on repeat. A seat that is busy, saturated,
resource-starved, or facing only dependency-gated work gets no wake at
all.

`INFRA-211` is the live regression case: it is queued with
`dependency_ready = 0`, so it must never count as runnable and must
never trigger a wake.

## Deliberately not in this slice

The replenishment behaviour the wake triggers (dispatching lanes,
tracking children, integrating returns), the dashboard lane/budget
surface, and the ranked work graph. Those are separate bounded slices
once a seat can be told it is underutilized at all.

Out of scope entirely: the harness lane and PR2 stay isolated, and no
unrelated Linear work is admitted.

## Tests

- an idle seat with runnable admitted work gets exactly ONE wake, and a
  second evaluation of the same condition journals nothing;
- a seat already at the canonical six-lane bound gets none;
- a project whose only queued issue is `dependency_ready = 0` gets none
  (the `INFRA-211` shape);
- a busy seat gets none;
- a paused issue never counts as runnable;
- the harness lane never triggers or receives one.
