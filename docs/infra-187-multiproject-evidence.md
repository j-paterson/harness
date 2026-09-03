# INFRA-187 multi-project acceptance evidence

Captured by the Fable lead at 2026-09-02T23:56:08Z from the live durable store, runtime generation 121 (ef5c381). Projects configured in config/projects.yaml: agent-orchestration (repo /Users/josystem/hermes-orchestrator, github Jo-Solutions-Engineering/harness) and harness-lab (repo /Users/josystem/hermes-disposable-lab, github Jo-Solutions-Engineering/harness-lab).

## project_teams

```
project_key          generation  state  fable_cell_id                         fable_profile_alias  sol_thread_id                         sol_generation  sol_model
-------------------  ----------  -----  ------------------------------------  -------------------  ------------------------------------  --------------  -----------
agent-orchestration  1           ready  83b52ed1-4d3b-4387-a281-d0c8378d2d5b  max-c                01a05ceb-3bda-7ee2-b1c0-b6215cc71d09  2               gpt-5.6-sol
harness-lab          1           ready  9e7e5c87-ced8-46d7-88bc-840ec62baffc  max-a                01a06486-514c-7dd3-8a7c-26e7069486b2  1               gpt-5.6-sol
```

## reviewer_channels

```
project_key          thread_id                             generation  state  integration_branch  model        provider  contract_version
-------------------  ------------------------------------  ----------  -----  ------------------  -----------  --------  ----------------
agent-orchestration  01a05ceb-3bda-7ee2-b1c0-b6215cc71d09  2           ready  main                gpt-5.6-sol  chatgpt   fif-1
harness-lab          01a06486-514c-7dd3-8a7c-26e7069486b2  1           ready  main                gpt-5.6-sol  chatgpt
```

## project_cells (live)

```
cell_id                               project_key          lane_role    state   profile_alias  session_id
------------------------------------  -------------------  -----------  ------  -------------  ------------------------------------
83b52ed1-4d3b-4387-a281-d0c8378d2d5b  agent-orchestration  development  active  max-c          5ffd1831-2eb9-467e-809e-2d534c7ac8f7
9e7e5c87-ced8-46d7-88bc-840ec62baffc  harness-lab          development  active  max-a          8e7035b7-c0e1-4e5a-8edd-a8eff09021df
```

## admitted_issues (non-terminal)

```
issue_id   project_key          state           instruction_id
---------  -------------------  --------------  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
INFRA-206  agent-orchestration  paused          Bootstrap exception only: retrieve the authoritative INFRA-206 scope from the accompanying durable Hermes control packet, not stale local artifacts. Fable must author the implementation plan, delegate bounded implementation, and hand a committed candidate to Sol; Fable must not open a PR. Keep the solution to persistent official Linear MCP reads for managed leads and fail closed if that supported path is unavailable.
INFRA-215  agent-orchestration  in_development  finish-infra-215-project-driver-20260902-v1
INFRA-184  agent-orchestration  review          Continue improving Harness multi-issue operation. Fable owns the plan and delegates this issue to an issue-scoped child while keeping other admitted issues moving.
INFRA-187  agent-orchestration  in_development  complete-multiproject-acceptance-20260902-v1
INFRA-200  agent-orchestration  review          Integrate the existing Sol review skill behavior. Fable owns the plan and delegates bounded implementation while other admitted issues continue.
INFRA-228  harness-lab          in_development  infra-228-harness-lab-canary-20260902-v1
```

## worktree_leases (active)

```
issue_id   project_key          path                                                   branch
---------  -------------------  -----------------------------------------------------  -----------------------------------
INFRA-225  agent-orchestration  /Users/josystem/hermes-orchestrator-issue-INFRA-225    feature/infra-225-dashboard-process
INFRA-184  agent-orchestration  /Users/josystem/hermes-orchestrator-issue-INFRA-184    feature/infra-184
INFRA-187  agent-orchestration  /Users/josystem/hermes-orchestrator-issue-INFRA-187    feature/infra-187
INFRA-200  agent-orchestration  /Users/josystem/hermes-orchestrator-issue-INFRA-200    feature/infra-200
INFRA-227  agent-orchestration  /Users/josystem/hermes-orchestrator-issue-INFRA-227    feature/infra-227
INFRA-215  agent-orchestration  /Users/josystem/hermes-orchestrator-issue-INFRA-215    feature/infra-215
INFRA-228  harness-lab          /Users/josystem/hermes-disposable-lab-issue-INFRA-228  feature/infra-228
```

## lead_assignments (live cells, since 2026-09-02T18:00Z)

```
assignment_id                     project_key          issue_id   cell_id                               profile_alias  state
--------------------------------  -------------------  ---------  ------------------------------------  -------------  ------------
79dcd94f28f54f05b18f9e9f287f5256  agent-orchestration  INFRA-184  83b52ed1-4d3b-4387-a281-d0c8378d2d5b  max-c          acknowledged
efd4c91bafea4158b08a4717884e673f  agent-orchestration  INFRA-187  83b52ed1-4d3b-4387-a281-d0c8378d2d5b  max-c          acknowledged
04f8e37593314e6ba98cb778654c0396  agent-orchestration  INFRA-200  83b52ed1-4d3b-4387-a281-d0c8378d2d5b  max-c          acknowledged
d964c71e76c74c2daffe549673567898  agent-orchestration  INFRA-215  83b52ed1-4d3b-4387-a281-d0c8378d2d5b  max-c          acknowledged
b4b8a8e8167a4318b9407ef8829315ae  agent-orchestration  INFRA-227  83b52ed1-4d3b-4387-a281-d0c8378d2d5b  max-c          acknowledged
372248b820ee4870ac2263dd62669ece  harness-lab          INFRA-228  9e7e5c87-ced8-46d7-88bc-840ec62baffc  max-a          acknowledged
```

## wake_deliveries (latest 6)

```
event_id                                            project_key          issue_id   state
--------------------------------------------------  -------------------  ---------  ---------
fable_ready-infra-227-acee510a1f92-20260902T224314  agent-orchestration  INFRA-227  completed
fable_ready-infra-187-b5a02a068f3b-20260902T192219  agent-orchestration  INFRA-187  completed
fable_ready-infra-187-9b824d38c693-20260902T191326  agent-orchestration  INFRA-187  completed
fable_ready-infra-200-2b81dc3c0b6d-20260902T190839  agent-orchestration  INFRA-200  completed
fable_ready-infra-187-0a7789631f94-20260902T190107  agent-orchestration  INFRA-187  completed
fable_ready-infra-187-b56edffdcb92-20260902T185721  agent-orchestration  INFRA-187  completed
```

## profile_leases (live)

```
profile_alias  project_key          state
-------------  -------------------  ------
max-c          agent-orchestration  active
max-a          harness-lab          active
```

## Concurrency proof

Both configured projects simultaneously hold: one active development cell on distinct profiles (max-c, max-a) and distinct sessions; one ready `project_teams` row binding that cell and the project's own Sol thread (`gpt-5.6-sol`/`chatgpt`); one in_development issue (INFRA-187/INFRA-215 vs INFRA-228); one active issue worktree lease under the project's own checkout root. No `wake_deliveries`, `lead_assignments`, `worktree_leases`, or `reviewer_channels` row crosses a project boundary.

## Independence proof

Pending: rotate or restart one project's Fable lead and re-capture the tables above, expecting the other project's cell, pair generation, lease and Sol thread unchanged.
