BEGIN IMMEDIATE;

-- Migration 0056 added lane_role after harness seats already existed, so
-- SQLite labeled those existing bindings "development" by default. Repair
-- only bindings that match the exact active harness cell/session/profile.
UPDATE cmux_surface_bindings AS binding
SET lane_role = 'harness'
WHERE binding.role = 'lead'
  AND binding.lane_role = 'development'
  AND EXISTS (
      SELECT 1
      FROM project_cells AS cell
      WHERE cell.cell_id = binding.cell_id
        AND cell.state = 'active'
        AND cell.lane_role = 'harness'
        AND cell.session_id = binding.session_id
        AND cell.profile_alias = binding.profile_alias
  );

INSERT INTO schema_migrations(version, applied_at)
VALUES (57, CURRENT_TIMESTAMP);

COMMIT;
