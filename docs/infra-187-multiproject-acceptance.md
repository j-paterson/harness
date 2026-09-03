# INFRA-187 reopened — multi-project operational acceptance runbook

Operator instruction `complete-multiproject-acceptance-20260902-v1`.
Acceptance (Linear, 2026-09-02): two separately configured
repositories/projects each receive an explicitly admitted issue, run
concurrently through their own visible Fable lead and bound Sol task,
preserve project-scoped worktrees/leases/routing, and continue
independently when one project restarts or rotates. Use an ordinary
disposable second project; do not migrate PR2.

## Current state (verified 2026-09-02, runtime ef5c381)

- `config/projects.yaml` (local, gitignored) configures exactly one
  project, `agent-orchestration`; `reviewer_channels` and
  `project_teams` each hold one ready row for it.
- The daemon, console, and dashboard run under launchd
  (`com.josystem.hermes-orchestrator`, `com.josystem.hermes-operations`)
  from the activated runtime checkout, reading configuration from
  `/Users/josystem/hermes-orchestrator/config`.
- Sol threads are opened per configured project at merger-session start
  with `cwd = repo_path`; approvals require the project's `github_repo`.

## Disposable second project (prepared by the lead, local only)

- Primary checkout: `/Users/josystem/hermes-disposable-lab` (git, `main`,
  one commit: README, `src/lab.py`, `tests/test_lab.py`).
- Proposed `config/projects.yaml` entry (validated with
  `load_settings`; both projects parse):

```yaml
  disposable-lab:
    linear_team: infrastructure
    repo_path: /Users/josystem/hermes-disposable-lab
    integration_branch: main
    github_repo: Jo-Solutions-Engineering/hermes-disposable-lab
    ci: none
    self_host: false
```

## Steps requiring operator/Hermes confirmation (outward-facing)

1. Create the private GitHub repository
   `Jo-Solutions-Engineering/hermes-disposable-lab` and push `main`
   from the primary checkout (Sol needs it for the sole PR).
2. Append the entry above to `config/projects.yaml`.
3. Restart the launchd daemon so the new project loads (this also
   restarts the merger session, which opens the second Sol thread and
   lets `reconcile_project_teams` form the second pair once the model
   proof is recorded).
4. Admit exactly one Linear issue for `disposable-lab` with a fresh
   operator instruction id (Hermes/operator only; the lead never selects
   work). A second issue may stay queued for the restart step.

## Evidence to capture (durable queries, per project)

```sql
select project_key, generation, state, fable_cell_id, sol_thread_id, sol_generation from project_teams;
select project_key, thread_id, generation, state, model, provider, contract_version from reviewer_channels;
select cell_id, project_key, lane_role, state, profile_alias, session_id from project_cells where state in ('starting','active','handoff_required','paused');
select issue_id, project_key, path, branch, state from worktree_leases where state='active';
select project_key, issue_id, state from admitted_issues where state not in ('done');
select event_id, project_key, issue_id, state from wake_deliveries order by rowid desc limit 10;
```

Concurrency proof: both projects show an active development cell, a
ready pair, an in_development issue, and an active issue lease at the
same time; each candidate wake routes to its own project's Sol thread
(no cross-project `wake_deliveries`).

Independence proof: rotate or restart one project's Fable lead
(`rotate-lead --cell <cell>` or a seat restart) and confirm the other
project's cell, pair generation, lease, and Sol thread are unchanged
while the restarted project recovers its own pair at the next
generation.

## Cleanup

Retire the disposable pair only after its issue merges (or is
cancelled): remove the `projects.yaml` entry, restart the daemon,
delete the GitHub repository and the local checkout.
