# INFRA-214 — narrow repair of the observed harness launch path

Authored by the Fable lead from INFRA-214's recorded live acceptance
failure (2026-09-01). Base: `feature/infra-214` at `b668781`
(origin/main, the activated runtime). Repair ONLY the observed path;
no new architecture, no profile retries.

## What actually happened (authoritative, and it corrects my own read)

Two identical `start-lane --lane harness --harness-run
infra-198-acceptance-20260901 --issue INFRA-198` attempts failed. I
first reported this as a `max-b` profile authentication failure. That
was WRONG. The durable `lead.launch_failed` receipts prove Claude
exited 1 because the launch referenced a prompt that does not exist at
the path it was given.

Three coupled defects, all in code this lead landed:

1. **Prompt resolves from a stale source checkout.** `cli.py:1329-1332`
   composes `prompt_file=settings.repo_root / "prompts" / prompt_name`.
   `settings.repo_root` is the stable PRIMARY checkout
   (`/Users/josystem/hermes-orchestrator`), which carries only
   `claude-lead.md` and `codex-merger.md` — the merged
   `claude-harness.md` exists ONLY in the activated runtime
   (`runtimes/b668781.../prompts/`). Verified directly. Assets must
   resolve from the activated runtime or another version-matched
   installed location, never an arbitrarily stale checkout.
2. **Classic-seat mode omitted.** The one-shot `start-lane`
   composition did not request classic-seat mode, so Hermes created an
   EMPTY cmux workspace and separately launched a hidden `claude -p`
   shadow process instead of the visible channel-enabled classic
   harness session.
3. **Lane identity not threaded through the seat.** `CmuxLeadSeater.ensure()`
   and activation never received the lane, so both failed seats
   persisted as `lane_role=development`, and the failed launch did not
   retire the already-active binding/workspace — leaving two dead
   visible workspaces and active binding residue.

## Required outcome (from the issue, binding)

The same command, with no operator click and no profile retry, must
produce exactly ONE visible channel-enabled classic Claude harness
process in the dedicated harness worktree, ONE durable active
`lane_role=harness` cell/binding/lease, and NO hidden `claude -p`
process. A forced launch failure must release or durably retire the
cell, lease, binding, workspace, and channel configuration so an
immediate retry leaves no residue.

## Packet

| Packet | Boundary | Files |
|---|---|---|
| H1 | (a) resolve prompt/config assets from the activated runtime (version-matched), never `settings.repo_root`; (b) compose the classic channel-enabled seat for `start-lane` so no hidden `claude -p` is launched; (c) thread lane identity through `CmuxLeadSeater.ensure()`/activation so seats persist as their true lane; (d) a failed launch releases/retires cell, lease, binding, workspace, and channel config atomically | `src/hermes_orchestrator/cli.py`, `src/hermes_orchestrator/cmux_surfaces.py`, `tests/test_cli.py`, `tests/test_cmux.py` |

Out of scope: profile retries, new transport/protocol, the Harness Lab
multi-project cell itself, and anything outside this launch path.
