# INFRA-198 — automatic dead-worker recovery

Assignment `a5b508bb`, instruction
`infra-198-live-dead-worker-recovery-20260901`. Scope is exactly the
observed blocker. No architecture expansion.

## Observed

Harness workspace `87B8378D-EBE5-4B5F-A317-1C5198FB8DBE` was closed and
its Claude PID exited. More than two daemon ticks later, durable state
still read:

- cell `8369559d-f4cd-45ca-aa43-aae412853f16` — `state = active`
  (harness, `max-b`)
- binding `beccd2be…` — `state = active` on that dead workspace

A replacement launch was blocked because the cell still looked occupied.

## Root cause

The machinery to detect this already exists and is correct — it simply
never runs again after startup. `cmux_reconciler.reconcile()` is called
exactly once in `_run_daemon`, before the supervisor starts ("Visible
cmux seats are restored from durable identity before any work is
accepted"). Nothing re-probes bindings on subsequent ticks, so a worker
that dies *mid-run* stays active until the next daemon restart. That is
precisely the ">2 daemon ticks" the blocker reports.

The existing pass also already has the right safety shape: `ping()`
first, and a `CmuxError` anywhere returns without mutating durable
state, because "durable bindings are the truth and stay untouched until
cmux is reachable".

## The change

One per-tick sweep, wired into the existing `_maintenance` closure in
`cli.py` (the same slot the hibernation driver, intake router and
workspace owner already ride).

Order, fail-closed at every step:

1. `await port.ping()`. On `CmuxError`, return having retired
   **nothing**. An unreachable socket must never read as "every worker
   died" — that would tear down healthy seats on a transient cmux
   hiccup.
2. For each active **lead** binding, probe `port.surface_alive(ref)`.
3. Alive → leave untouched.
4. Provably absent → retire that exact identity, and only it:
   - mark the binding closed with a dead-worker reason;
   - move its cell out of `active`, CAS'd on the exact `cell_id` and
     its current state so a concurrent transition cannot be clobbered;
   - release that cell's issue worktree lease if one is held;
   - record one durable control receipt naming the retired cell,
     binding and workspace.
5. Any `CmuxError` mid-pass → stop the sweep; retire nothing further.

Absence must come from a successful probe, never from an exception.

## Explicitly not changed

Startup `reconcile()` keeps its current behavior: a missing lead
surface at startup is *replaced* (the seat is relaunched from durable
identity), which is correct for restart recovery. The mid-run case
wants retirement instead, so `start-lane` seats the replacement
deliberately — that is what the assignment asks for. No transport, no
protocol, no new architecture.

## Tests

- a dead lead binding is retired: binding closed, cell out of `active`,
  lease released, receipt recorded;
- an unreachable port (`ping` raises) retires nothing — the exact
  regression against tearing down live seats on a cmux hiccup;
- a live binding is left completely untouched;
- a `CmuxError` raised mid-pass leaves the remaining bindings untouched;
- after the sweep, a harness `start-lane` for that project is no longer
  blocked by the dead cell.

## Then

Re-run the live acceptance the previous assignment could not complete:
no-click trust adoption on a harness seat, and UUID-safe `--json`.
