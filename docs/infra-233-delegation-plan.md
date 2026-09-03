# INFRA-233 delegation plan: refresh team identity and retire stale cmux surfaces

Lead-owned plan (Fable, cell 83b52ed1, session 72789518). No schema change.

## Root causes (from the code map)

- A lead rotation keeps the same `project_cells.cell_id` and only changes the
  session/profile; `ProjectTeamService.reconcile_existing` compares cell ids only,
  so the team's Fable member is never rebound, and `_finalize_transfer` never
  touches `project_teams`.
- On rotation `CmuxLeadSeater._retire_rotated_seat` closes the old workspace;
  when cmux cannot confirm the close the binding stays `residual`, and nothing
  sweeps residual bindings per tick (only a daemon restart or reseating the same
  cell), so the retired workspace stays visible.

## Packets

| # | Scope | Files | Depends |
|---|-------|-------|---------|
| P1 | `refresh_fable_in(connection, ...)` on the team service; `_finalize_transfer` refreshes the development-lane team member atomically with the binding swap; `reconcile_existing` rebinds a same-cell session/profile change and derives the cell identity from the active worker binding | project_teams.py, cells.py, tests/test_project_teams.py, tests/test_cells.py | — |
| P2 | per-tick residual/stale lead-binding sweep that closes or clearly classifies retired managed surfaces; `status --json` lists them | cmux_surfaces.py, cli.py, tests/test_cmux_surfaces.py, tests/test_cli.py | — |
