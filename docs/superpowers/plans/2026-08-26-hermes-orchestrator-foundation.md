# Hermes orchestrator foundation implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Build a dry-run orchestration service with validated configuration, durable SQLite state, an explicit private queue, deterministic scheduling, fake adapters, resource observation, and restart-safe reconciliation.

**Architecture:** A Python 3.13 package owns deterministic domain state and persists it through a small SQLite repository. Hermes and later remote surfaces call an application service; provider-specific behavior remains behind adapter protocols. Phase 1 performs no live Linear, Claude, Codex, GitHub, or cleanup mutations.

**Tech Stack:** Python 3.13, uv 0.10.9, Pydantic 2.13.4, PyYAML 6.0.3, psutil 7.2.2, pytest 9.1.1, pytest-asyncio 1.3.0, Ruff 0.15.10, stdlib sqlite3 and asyncio

**Spec:** docs/superpowers/specs/2026-08-26-hermes-orchestration-system-design.md

## Global constraints

- Runtime Python must be >=3.13,<3.14 because the system Python is 3.14 while Hermes currently requires <3.14.
- The repository must not contain credentials, account email addresses, session transcripts, runtime databases, or user identifiers.
- Hermes may operate only on Linear issues explicitly admitted by the operator.
- External adapters are dry-run or fake in this phase.
- Every material transition writes an append-only event in the same database transaction.
- A restart must reconcile incomplete work before opening admission.
- Configuration uses opaque profile and project aliases.
- Every task follows test-driven development and ends with a focused commit.

## File map

- pyproject.toml: package metadata, exact dependency pins, CLI entry point, and test configuration.
- .gitignore: runtime, secret, cache, database, and build exclusions.
- src/hermes_orchestrator/config.py: validated project, policy, and path configuration.
- src/hermes_orchestrator/domain.py: shared enums and immutable domain records.
- src/hermes_orchestrator/db.py: SQLite connection, transactions, and migrations.
- src/hermes_orchestrator/migrations/0001_initial.sql: initial durable schema.
- src/hermes_orchestrator/events.py: append-only event writer and reader.
- src/hermes_orchestrator/queue.py: explicit admission and queue ordering.
- src/hermes_orchestrator/adapters.py: provider protocols and fake implementations.
- src/hermes_orchestrator/scheduler.py: deterministic ranking and admission decisions.
- src/hermes_orchestrator/resources.py: read-only host sampling.
- src/hermes_orchestrator/service.py: application boundary and startup reconciliation.
- src/hermes_orchestrator/cli.py: init, queue, status, reconcile, and observe commands.
- config/projects.example.yaml: non-secret project registry example.
- config/policies.yaml: initial policy values and observation-only resource mode.
- tests/: focused unit and integration tests mirroring the modules.

---

### Task 1: Bootstrap the package and validated configuration

**Files:**
- Create: pyproject.toml
- Create: .gitignore
- Create: src/hermes_orchestrator/__init__.py
- Create: src/hermes_orchestrator/config.py
- Create: config/projects.example.yaml
- Create: config/policies.yaml
- Test: tests/test_config.py

**Interfaces:**
- Produces: load_settings(repo_root: Path, state_dir: Path | None = None) -> Settings
- Produces: ProjectConfig, PolicyConfig, ResourcePolicy, Settings

- [ ] **Step 1: Write the failing configuration tests**

    from pathlib import Path
    import pytest
    from hermes_orchestrator.config import load_settings

    def test_loads_registered_project(tmp_path: Path) -> None:
        (tmp_path / "config").mkdir()
        (tmp_path / "config/projects.yaml").write_text(
            "projects:\n"
            "  polysizer:\n"
            "    linear_team: ENG\n"
            "    repo_path: /tmp/polysizer\n"
            "    integration_branch: polysizer-refactor-2\n"
            "    github_repo: owner/polysizer\n",
            encoding="utf-8",
        )
        (tmp_path / "config/policies.yaml").write_text(
            "mode: observe\nmax_unresolved_ci_merges: 2\n",
            encoding="utf-8",
        )
        settings = load_settings(tmp_path, tmp_path / "state")
        assert settings.projects["polysizer"].integration_branch == "polysizer-refactor-2"
        assert settings.policy.max_unresolved_ci_merges == 2
        assert settings.state_dir == tmp_path / "state"

    def test_rejects_secret_like_project_keys(tmp_path: Path) -> None:
        (tmp_path / "config").mkdir()
        (tmp_path / "config/projects.yaml").write_text(
            "projects:\n  demo:\n    token: secret\n", encoding="utf-8"
        )
        (tmp_path / "config/policies.yaml").write_text("mode: observe\n", encoding="utf-8")
        with pytest.raises(ValueError, match="secret-like"):
            load_settings(tmp_path, tmp_path / "state")

- [ ] **Step 2: Run the tests and verify the import fails**

Run: uv run pytest tests/test_config.py -v

Expected: FAIL because hermes_orchestrator.config does not exist.

- [ ] **Step 3: Add the package, exact dependency pins, configuration models, and example YAML**

Use these project dependencies:

    dependencies = [
      "pydantic==2.13.4",
      "PyYAML==6.0.3",
      "psutil==7.2.2",
    ]
    dev = [
      "pytest==9.1.1",
      "pytest-asyncio==1.3.0",
      "ruff==0.15.10",
    ]

Implement load_settings to read config/projects.yaml when present, otherwise config/projects.example.yaml; reject keys matching token, secret, password, api_key, email, or phone; resolve state_dir without creating it; and validate max_unresolved_ci_merges as exactly 2.

Set config/policies.yaml to:

    mode: observe
    max_unresolved_ci_merges: 2
    context_prepare_percent: 70
    context_rotate_percent: 80
    max_active_session_hours: 6
    stall_consultations_before_automation: 2
    resource_thresholds:
      calibrated: false
      yellow_available_memory_gib: null
      red_available_memory_gib: null
      yellow_available_disk_gib: null
      red_available_disk_gib: null

- [ ] **Step 4: Run configuration tests and lint**

Run: uv sync --group dev && uv run pytest tests/test_config.py -v && uv run ruff check .

Expected: all tests pass and Ruff reports no errors.

- [ ] **Step 5: Commit**

    git add pyproject.toml uv.lock .gitignore src/hermes_orchestrator config tests/test_config.py
    git commit -m "build: bootstrap orchestrator configuration"

### Task 2: Add SQLite migrations and the append-only event journal

**Files:**
- Create: src/hermes_orchestrator/db.py
- Create: src/hermes_orchestrator/events.py
- Create: src/hermes_orchestrator/migrations/0001_initial.sql
- Test: tests/test_db.py
- Test: tests/test_events.py

**Interfaces:**
- Consumes: Settings.state_dir
- Produces: Database.open(path: Path) -> Database
- Produces: Database.transaction() -> context manager yielding sqlite3.Connection
- Produces: Database.schema_version() -> int
- Produces: Database.scalar(sql: str, parameters: tuple = ()) -> object
- Produces: Database.close() -> None
- Produces: EventStore.append(connection, EventInput) -> EventRecord
- Produces: EventStore.list_after(sequence: int) -> list[EventRecord]

- [ ] **Step 1: Write failing migration and atomic-event tests**

    def test_database_applies_migration_once(tmp_path):
        db = Database.open(tmp_path / "state.db")
        assert db.schema_version() == 1
        db.close()
        db = Database.open(tmp_path / "state.db")
        assert db.schema_version() == 1

    def test_domain_write_and_event_are_atomic(database):
        events = EventStore(database)
        with pytest.raises(RuntimeError):
            with database.transaction() as connection:
                connection.execute(
                    "INSERT INTO admitted_issues(issue_id, project_key, priority, state) "
                    "VALUES ('ENG-1', 'demo', 1, 'queued')"
                )
                events.append(
                    connection,
                    EventInput("issue.admitted", "issue", "ENG-1", {"priority": 1}),
                )
                raise RuntimeError("rollback")
        assert database.scalar("SELECT count(*) FROM admitted_issues") == 0
        assert events.list_after(0) == []

- [ ] **Step 2: Run the tests and verify they fail**

Run: uv run pytest tests/test_db.py tests/test_events.py -v

Expected: FAIL because Database and EventStore are undefined.

- [ ] **Step 3: Implement the database and migration**

The initial migration must create schema_migrations, events, admitted_issues, project_cells, worker_leases, profile_leases, external_effects, resource_samples, and reconciliation_runs. Enable foreign_keys, WAL, busy_timeout=5000, and synchronous=NORMAL on every connection.

Event rows contain sequence, event_id, occurred_at, event_type, aggregate_type, aggregate_id, correlation_id, actor, and canonical JSON payload. EventInput assigns a UUIDv7-compatible sortable identifier through a small local helper and serializes JSON with sorted keys and compact separators.

- [ ] **Step 4: Run migration, atomicity, and SQLite integrity tests**

Run: uv run pytest tests/test_db.py tests/test_events.py -v

Expected: all tests pass, including PRAGMA integrity_check returning ok.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/db.py src/hermes_orchestrator/events.py src/hermes_orchestrator/migrations tests/test_db.py tests/test_events.py
    git commit -m "feat: add durable event-backed state"

### Task 3: Implement explicit issue admission and deterministic queue ordering

**Files:**
- Create: src/hermes_orchestrator/domain.py
- Create: src/hermes_orchestrator/queue.py
- Test: tests/test_queue.py

**Interfaces:**
- Consumes: Database, EventStore, Settings.projects
- Produces: QueueService.admit(AdmissionRequest) -> QueuedIssue
- Produces: QueueService.reprioritize(issue_id: str, priority: int) -> QueuedIssue
- Produces: QueueService.list_ranked(now: datetime) -> list[QueuedIssue]
- Produces: AdmissionRequest(issue_id, project_key, linear_priority, admitted_by, instruction_id)

- [ ] **Step 1: Write failing queue tests**

    def test_only_explicit_operator_request_can_admit(queue_service):
        request = AdmissionRequest(
            issue_id="ENG-7",
            project_key="demo",
            linear_priority=2,
            admitted_by="operator",
            instruction_id="chat-123",
        )
        queued = queue_service.admit(request)
        assert queued.issue_id == "ENG-7"
        assert queued.state is IssueState.QUEUED
        assert queue_service.admit(request).issue_id == "ENG-7"

    def test_rejects_discovery_source(queue_service):
        request = AdmissionRequest(
            issue_id="ENG-8",
            project_key="demo",
            linear_priority=1,
            admitted_by="linear_scan",
            instruction_id="poll-1",
        )
        with pytest.raises(AdmissionDenied, match="explicit operator"):
            queue_service.admit(request)

    def test_rank_is_priority_then_readiness_then_age(queue_service, clock):
        admit_fixture_issues(queue_service, clock)
        assert [item.issue_id for item in queue_service.list_ranked(clock.now())] == [
            "ENG-2", "ENG-3", "ENG-1"
        ]

- [ ] **Step 2: Run the tests and verify they fail**

Run: uv run pytest tests/test_queue.py -v

Expected: FAIL because the queue domain does not exist.

- [ ] **Step 3: Implement queue records and transactional operations**

Define IssueState values queued, in_development, review, qa, done, paused, and blocked. Admission accepts only admitted_by=operator, requires a non-empty instruction_id, requires a registered project, and uses instruction_id as an idempotency key. Ranking uses ascending Linear priority, dependency_ready before blocked, admitted_at ascending, overlap_risk ascending, then issue_id for stability.

Every admission and reprioritization appends an event in the same transaction.

- [ ] **Step 4: Run queue and atomicity tests**

Run: uv run pytest tests/test_queue.py tests/test_events.py -v

Expected: all tests pass.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/domain.py src/hermes_orchestrator/queue.py tests/test_queue.py
    git commit -m "feat: enforce explicit private work queue"

### Task 4: Add adapter contracts, fakes, and the dry-run scheduler

**Files:**
- Create: src/hermes_orchestrator/adapters.py
- Create: src/hermes_orchestrator/scheduler.py
- Test: tests/test_adapters.py
- Test: tests/test_scheduler.py

**Interfaces:**
- Consumes: QueueService.list_ranked, ResourceSnapshot, ProjectConfig
- Produces: LinearPort, WorkerPort, ReviewPort, SourceControlPort protocols
- Produces: FakeLinear, FakeWorker, FakeReview, FakeSourceControl
- Produces: Scheduler.plan(snapshot: ResourceSnapshot) -> list[PlannedAction]

- [ ] **Step 1: Write failing scheduler tests**

    def test_observe_mode_never_dispatches(scheduler, green_snapshot):
        actions = scheduler.plan(green_snapshot)
        assert actions
        assert all(action.execute is False for action in actions)

    def test_one_lead_per_project(scheduler_with_two_demo_issues, green_snapshot):
        actions = scheduler_with_two_demo_issues.plan(green_snapshot)
        starts = [a for a in actions if a.kind == "start_project_cell"]
        assert len(starts) == 1
        assert starts[0].project_key == "demo"

    def test_red_pressure_plans_no_new_work(scheduler, red_snapshot):
        assert not any(a.kind.startswith("start_") for a in scheduler.plan(red_snapshot))

- [ ] **Step 2: Run scheduler tests and verify they fail**

Run: uv run pytest tests/test_adapters.py tests/test_scheduler.py -v

Expected: FAIL because adapter protocols and Scheduler are undefined.

- [ ] **Step 3: Implement ports, recording fakes, and planning**

PlannedAction contains kind, project_key, issue_id, reason, execute, and evidence. In observe mode execute is always false. The scheduler groups ranked issues by project, refuses a second lead for an existing cell, and asks ResourceSnapshot.can_admit before proposing a start. Fake adapters record calls and support deterministic injected responses and failures.

- [ ] **Step 4: Run scheduler tests**

Run: uv run pytest tests/test_adapters.py tests/test_scheduler.py -v

Expected: all tests pass.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/adapters.py src/hermes_orchestrator/scheduler.py tests/test_adapters.py tests/test_scheduler.py
    git commit -m "feat: add dry-run scheduling ports"

### Task 5: Add resource observation and the application service

**Files:**
- Create: src/hermes_orchestrator/resources.py
- Create: src/hermes_orchestrator/service.py
- Test: tests/test_resources.py
- Test: tests/test_service.py

**Interfaces:**
- Produces: ResourceSampler.sample() -> ResourceSnapshot
- Produces: ResourceSnapshot.pressure -> PressureLevel
- Produces: OrchestratorService.start() -> ReconciliationResult
- Produces: OrchestratorService.tick() -> TickResult

- [ ] **Step 1: Write failing sampler and startup tests**

    def test_uncalibrated_policy_is_observe_only(fake_psutil, policy):
        policy.resource_thresholds.calibrated = False
        snapshot = ResourceSampler(fake_psutil, policy).sample()
        assert snapshot.pressure is PressureLevel.UNKNOWN
        assert snapshot.can_admit is False

    def test_startup_reconciles_before_admission(service, event_store):
        result = service.start()
        assert result.completed is True
        events = event_store.list_after(0)
        assert [event.event_type for event in events[:2]] == [
            "reconciliation.started", "reconciliation.completed"
        ]
        assert service.admission_open is False

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_resources.py tests/test_service.py -v

Expected: FAIL because ResourceSampler and OrchestratorService are undefined.

- [ ] **Step 3: Implement read-only host sampling and reconciliation**

Sample available and total memory, swap used, one-minute load, logical CPUs, disk free at every registered repository, and managed process RSS when leases exist. Unknown thresholds return PressureLevel.UNKNOWN and prohibit dispatch.

Startup reconciliation checks SQLite integrity, expires leases whose PIDs are absent, records uncertain live PIDs without killing them, and keeps admission closed in observe mode. Tick writes one bounded resource sample, prunes samples older than the configured retention inside the same transaction, and returns planned actions without executing them.

- [ ] **Step 4: Run service tests**

Run: uv run pytest tests/test_resources.py tests/test_service.py -v

Expected: all tests pass.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/resources.py src/hermes_orchestrator/service.py tests/test_resources.py tests/test_service.py
    git commit -m "feat: observe resources and reconcile startup"

### Task 6: Add the operator CLI and Phase 1 acceptance test

**Files:**
- Create: src/hermes_orchestrator/cli.py
- Test: tests/test_cli.py
- Test: tests/integration/test_foundation_acceptance.py
- Create: README.md

**Interfaces:**
- Consumes: load_settings, Database, QueueService, OrchestratorService
- Produces: hermes-orchestrator init|queue-add|queue-list|status|observe|reconcile

- [ ] **Step 1: Write failing CLI tests**

    def test_queue_add_requires_explicit_flag(cli, configured_repo):
        result = cli.invoke(
            ["queue-add", "ENG-7", "--project", "demo", "--priority", "2"]
        )
        assert result.exit_code == 2
        assert "--operator-instruction" in result.output

    def test_acceptance_restart_is_idempotent(cli, configured_repo):
        args = [
            "queue-add", "ENG-7", "--project", "demo", "--priority", "2",
            "--operator-instruction", "chat-123",
        ]
        assert cli.invoke(args).exit_code == 0
        assert cli.invoke(args).exit_code == 0
        assert cli.invoke(["reconcile"]).exit_code == 0
        status = cli.invoke(["status", "--json"])
        assert status.exit_code == 0
        payload = json.loads(status.output)
        assert payload["queue_count"] == 1
        assert payload["mode"] == "observe"

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_cli.py tests/integration/test_foundation_acceptance.py -v

Expected: FAIL because the CLI is undefined.

- [ ] **Step 3: Implement the CLI and minimum README**

Use argparse from the standard library. queue-add requires --operator-instruction and always records admitted_by=operator. --json emits one JSON object and no decorative text. observe performs one sample by default and accepts --watch only when the operator supplies an interval of at least 5 seconds. init creates the state directory and database but never copies secrets.

README must state that Phase 1 is observation-only, show uv sync --group dev, init, queue-add, status, and pytest commands, and link to the approved specification and rollout plans.

- [ ] **Step 4: Run the full Phase 1 verification**

Run: uv run pytest -q && uv run ruff check . && uv run hermes-orchestrator --help

Expected: all tests pass, Ruff is clean, and CLI help lists all six commands.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/cli.py tests/test_cli.py tests/integration/test_foundation_acceptance.py README.md pyproject.toml
    git commit -m "feat: deliver observation-only orchestrator foundation"

## Phase exit gate

Phase 2 implementation may begin immediately. Live Claude dispatch may begin
under conservative hard safety floors while resource observation continuously
refines the profile. Do not enable Linear writes or automated resource cleanup
until their phase-specific credentials and safety gates pass.

- The full test suite passes twice with a supervisor restart between runs.
- Conservative green, yellow, and red bootstrap thresholds are committed to
  config/policies.yaml before the first managed worker launch.
- Real managed-worker samples are retained for continuous threshold tuning;
  synthetic calibration workloads are not required.
- The operator verifies that queue admission cannot happen without an explicit instruction identifier.
- The runtime database and all config directories remain ignored by Git.
