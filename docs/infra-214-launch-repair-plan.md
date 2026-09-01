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

## Amendment — INFRA-219 reopened: issue-specific worktrees and leases

INFRA-219 was reopened 2026-09-01 because its own candidate-publication
acceptance is incomplete, and that incompleteness is INFRA-214's live
publication blocker. Verbatim: "`resolve_lane()` requires an
issue-bound worktree lease, but no production path creates
issue-specific worktrees or registers their leases; `worktree_leases`
is empty. A shared project lead checkout cannot substitute because
multiple issue assignments would collide on the unique live-path
constraint and recreate the wrong-head hazard."

That rules out the repair this lead first delegated (registering a
lease against the shared `lead_cwd`), and the reason is durable, not
stylistic: `worktree_leases_live_path_idx` is UNIQUE on `path` where
`state != 'reclaimed'` (migration 0018). One shared checkout therefore
admits exactly ONE live lease for the whole project — a second admitted
issue would either fail to register or displace the first, and every
lane that did resolve would resolve the SAME path, which is precisely
the wrong-head publication hazard L5 exists to prevent. That patch was
discarded unintegrated.

### Smallest correct shape

Hermes owns issue-worktree creation and lease binding AT ASSIGNMENT:

1. **At assignment** (the boundary where an admitted issue is bound to
   a lane), Hermes resolves the issue's own checkout — one worktree per
   admitted issue, never the shared lead checkout — and registers its
   durable lease through the existing `WorktreeLeases.register`
   (`worktrees.py:197`; `WorktreeLeaseInput` needs non-empty
   project_key, issue_id, repo_path, path, branch, remote).
2. **Adoption before provisioning.** When a checkout for that issue's
   branch already exists (INFRA-214's own work currently lives in the
   lead worktree on `feature/infra-214`), ADOPT it — register its lease
   against its real path and branch — rather than provisioning a
   duplicate. Only when none exists does Hermes create one, through the
   existing git worktree surface (`git.py:809 worktree_add_detached`,
   `:834 worktree_list`), never a bespoke subprocess.
3. **Exactly one live lease per issue, and distinct paths per issue.**
   `resolve_lane` refuses zero AND refuses more than one, so
   registration must be idempotent under repeated assignment; and two
   admitted issues must resolve DIFFERENT paths, which the unique
   live-path index enforces durably.
4. Publication guard in `emission.py` is untouched. No new protocol, no
   schema change — migration 0018 already carries the binding.

### Acceptance for this amendment

A regression proving TWO admitted issues resolve DISTINCT bound paths
through `resolve_lane`, and that `candidate-ready` can resolve the
exact INFRA-214 checkout while the other admitted lanes remain
independent.
