# Resource governance and recovery implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Protect the Mac from memory and disk exhaustion by governing admission, checkpointing and pausing managed workers, rotating context-heavy sessions, learning stall remedies, reclaiming verified worktrees immediately, and reconciling safely after crashes.

**Architecture:** The supervisor owns explicit process and worktree leases and combines continuously calibrated host measurements with task priority. Cleanup is a state machine that requires a remote Git checkpoint before process termination and worktree removal. Stall and context policies request structured handoffs at safe boundaries and fail closed when evidence is incomplete.

**Tech Stack:** Prior phases, psutil 7.2.2, macOS process groups, Git worktree porcelain output, shutil.disk_usage, SQLite leases, pytest fake clocks and fake process/Git adapters

**Spec:** docs/superpowers/specs/2026-08-26-hermes-orchestration-system-design.md

## Global constraints

- Execute the foundation, Claude/Linear, and Codex Merger plans and pass their exit gates first.
- Versioned numeric resource thresholds in config/policies.yaml are the source of truth. They begin with conservative hard floors and are tuned from real managed workloads without synthetic calibration runs.
- Green admits work, yellow stops lower-priority admission, and red checkpoints and pauses the lowest-priority safe candidates.
- The scheduler treats four accounts as a maximum pool, not a concurrency target.
- Every managed process belongs to an exact process-group lease.
- Never kill by process name or broad path match.
- Never use recursive deletion as the normal worktree cleanup path.
- Never remove a worktree with dirty, untracked, uncommitted, or unpushed state.
- A WIP worktree can be reclaimed only after a clearly labeled checkpoint is pushed and its remote reachability is verified.
- Completed worktrees have no 24-hour retention period.
- Sessions rotate at the approved context and six-active-hour boundaries.
- A new stall reason requires two operator consultations before an approved playbook can run automatically.
- Every task follows test-driven development and ends with a focused commit.

## File map

- src/hermes_orchestrator/processes.py: exact process-group leases, checkpoint-aware termination, and orphan detection.
- src/hermes_orchestrator/admission.py: green, yellow, red classification and prioritized actions.
- src/hermes_orchestrator/context.py: context evidence, active time, safe boundaries, and rotation requests.
- src/hermes_orchestrator/stalls.py: evidence normalization, consultation counts, and playbook execution.
- config/playbooks.yaml: approved non-secret stall playbooks.
- src/hermes_orchestrator/worktrees.py: allocation, checkpoint proof, removal, and pruning.
- src/hermes_orchestrator/reconcile.py: crash recovery across database, processes, Git, PRs, and external projections.
- tests/: unit, property, failure-injection, and acceptance tests.

---

### Task 1: Track exact managed process groups

**Files:**
- Create: src/hermes_orchestrator/processes.py
- Modify: src/hermes_orchestrator/claude.py
- Modify: src/hermes_orchestrator/codex_rpc.py
- Test: tests/test_processes.py

**Interfaces:**
- Produces: ProcessRegistry.register(ProcessLeaseInput) -> ProcessLease
- Produces: ProcessRegistry.snapshot(lease_id: str) -> ProcessSnapshot
- Produces: ProcessRegistry.request_stop(lease_id: str, checkpoint_id: str) -> StopResult
- Produces: ProcessRegistry.find_orphans() -> list[ProcessLease]

- [ ] **Step 1: Write failing ownership and stop tests**

    def test_register_requires_process_group(process_registry):
        with pytest.raises(ValueError, match="process group"):
            process_registry.register(
                ProcessLeaseInput(pid=120, pgid=None, project_key="demo", kind="claude")
            )

    def test_stop_targets_only_recorded_group(process_registry, fake_os):
        lease = process_registry.register(
            ProcessLeaseInput(pid=120, pgid=120, project_key="demo", kind="claude")
        )
        result = process_registry.request_stop(lease.lease_id, checkpoint_id="cp-1")
        assert result.signal_sent == signal.SIGTERM
        assert fake_os.killpg_calls == [(120, signal.SIGTERM)]

    def test_stop_requires_checkpoint(process_registry):
        lease = registered_lease(process_registry)
        with pytest.raises(StopBlocked, match="checkpoint"):
            process_registry.request_stop(lease.lease_id, checkpoint_id="")

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_processes.py -v

Expected: FAIL because ProcessRegistry is undefined.

- [ ] **Step 3: Implement process leases and bounded termination**

Record pid, pgid, executable path, cwd, project, worker, start time, process create_time from psutil, and status. Validate create_time before signaling to prevent PID-reuse errors. SIGTERM the exact pgid, wait the configured grace period, and use SIGKILL only for the same validated pgid if it remains alive. Persist every signal and result.

Update Claude and Codex launchers to register the process immediately after create_subprocess_exec with start_new_session=True. A missing registration closes admission for that worker.

- [ ] **Step 4: Run process tests**

Run: uv run pytest tests/test_processes.py -v

Expected: all tests pass, including PID reuse, already-exited, child-tree RSS, and SIGTERM timeout cases.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/processes.py src/hermes_orchestrator/claude.py src/hermes_orchestrator/codex_rpc.py tests/test_processes.py
    git commit -m "feat: own managed process groups"

### Task 2: Enforce calibrated green, yellow, and red admission

**Files:**
- Create: src/hermes_orchestrator/admission.py
- Modify: src/hermes_orchestrator/resources.py
- Modify: src/hermes_orchestrator/scheduler.py
- Test: tests/test_admission.py

**Interfaces:**
- Produces: PressureClassifier.classify(ResourceSnapshot) -> PressureDecision
- Produces: AdmissionController.evaluate(queue: Sequence[QueuedIssue], workers: Sequence[WorkerLease], snapshot: ResourceSnapshot) -> list[ResourceAction]
- Produces: ResourceAction(kind: stop_admission|request_checkpoint|pause_worker, target_id, reason, evidence)

- [ ] **Step 1: Write failing pressure tests**

    def test_green_admits_when_all_metrics_clear(classifier, calibrated_policy):
        decision = classifier.classify(snapshot(memory_gib=10, disk_gib=20, swap_growth=0))
        assert decision.level is PressureLevel.GREEN
        assert decision.can_admit is True

    def test_yellow_stops_low_priority_admission(classifier, calibrated_policy):
        decision = classifier.classify(
            snapshot(memory_gib=calibrated_policy.yellow_memory - 0.1, disk_gib=20)
        )
        assert decision.level is PressureLevel.YELLOW
        assert decision.can_admit is False

    def test_red_pauses_lowest_priority_safe_worker(controller):
        actions = controller.evaluate(
            queue=[],
            workers=[
                worker("high", priority=1, checkpoint_safe=False),
                worker("low", priority=4, checkpoint_safe=True),
            ],
            snapshot=red_snapshot(),
        )
        assert actions[0].target_id == "low"
        assert actions[0].kind == "request_checkpoint"

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_admission.py -v

Expected: FAIL because the classifier and controller are undefined.

- [ ] **Step 3: Implement threshold evaluation and prioritized actions**

Classify red when any red threshold is crossed or macOS memory pressure reports critical. Classify yellow when any yellow threshold is crossed, swap growth exceeds its configured rate, or sustained load exceeds the configured CPU ratio. Otherwise classify green.

At yellow, close lower-priority admission without stopping active work. At red, sort safe pause candidates by Linear priority descending, dependency criticality ascending, recent progress ascending, and RSS descending. Request one checkpoint at a time, re-sample after completion, and continue only while red persists.

- [ ] **Step 4: Run admission tests**

Run: uv run pytest tests/test_admission.py tests/test_scheduler.py -v

Expected: all tests pass, including hysteresis tests that prevent green/yellow flapping.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/admission.py src/hermes_orchestrator/resources.py src/hermes_orchestrator/scheduler.py tests/test_admission.py
    git commit -m "feat: govern resource-based admission"

### Task 3: Detect context pressure and six active hours

**Files:**
- Create: src/hermes_orchestrator/context.py
- Modify: src/hermes_orchestrator/cells.py
- Modify: src/hermes_orchestrator/handoffs.py
- Test: tests/test_context.py

**Interfaces:**
- Produces: ContextMonitor.record(ContextSignal) -> ContextDecision
- Produces: ActiveTimeTracker.open(worker_id: str, at: datetime) -> None
- Produces: ActiveTimeTracker.idle(worker_id: str, at: datetime) -> None
- Produces: ActiveTimeTracker.total(worker_id: str, at: datetime) -> timedelta
- Produces: ContextDecision(state: healthy|prepare|rotation_pending|rotate_now, reasons)

- [ ] **Step 1: Write failing threshold and active-time tests**

    def test_context_thresholds(context_monitor):
        assert context_monitor.record(signal(percent=69)).state == "healthy"
        assert context_monitor.record(signal(percent=70)).state == "prepare"
        assert context_monitor.record(signal(percent=81)).state == "rotation_pending"

    def test_idle_time_does_not_count(active_time, clock):
        active_time.open("worker-1", clock.at("08:00"))
        active_time.idle("worker-1", clock.at("10:00"))
        active_time.open("worker-1", clock.at("16:00"))
        assert active_time.total("worker-1", clock.at("18:00")) == timedelta(hours=4)

    def test_six_active_hours_waits_for_safe_boundary(context_monitor):
        decision = context_monitor.record(
            signal(active_hours=6.2, safe_boundary=False)
        )
        assert decision.state == "rotation_pending"
        decision = context_monitor.record(
            signal(active_hours=6.2, safe_boundary=True)
        )
        assert decision.state == "rotate_now"

    def test_repeated_compaction_is_strong_evidence(context_monitor):
        context_monitor.record(signal(compaction=True))
        decision = context_monitor.record(signal(compaction=True, rapid_refill=True))
        assert decision.state == "rotation_pending"

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_context.py -v

Expected: FAIL because context tracking is undefined.

- [ ] **Step 3: Implement evidence aggregation**

Persist context percentage when available, compaction count, rapid-refill observations, active intervals, context errors, and behavioral warnings. A behavioral warning cannot force rotation without a context percentage, compaction, active-age, or explicit context error signal.

At prepare, request a handoff draft without stopping new work already assigned. At rotation_pending, stop assigning new subwork and wait for a commit, completed test cycle, PR update, subtask completion, or stable diagnosis event. At rotate_now, invoke the existing acknowledged handoff flow. A context error bypasses the safe-boundary wait after requesting an emergency checkpoint.

- [ ] **Step 4: Run context and handoff tests**

Run: uv run pytest tests/test_context.py tests/test_handoffs.py -v

Expected: all tests pass.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/context.py src/hermes_orchestrator/cells.py src/hermes_orchestrator/handoffs.py tests/test_context.py
    git commit -m "feat: rotate context-heavy sessions safely"

### Task 4: Learn and execute approved stall playbooks

**Files:**
- Create: src/hermes_orchestrator/stalls.py
- Create: config/playbooks.yaml
- Modify: src/hermes_orchestrator/hermes_tools.py
- Test: tests/test_stalls.py
- Test: tests/test_playbooks.py

**Interfaces:**
- Produces: StallDetector.evaluate(StallEvidence) -> StallDiagnosis | None
- Produces: PlaybookService.consult(diagnosis: StallDiagnosis) -> Consultation
- Produces: PlaybookService.approve(consultation_id: str, remedy: Remedy) -> Playbook
- Produces: PlaybookService.resolve(diagnosis: StallDiagnosis) -> ResolutionPlan

- [ ] **Step 1: Write failing consultation and automation tests**

    def test_first_two_matches_require_consultation(playbooks, diagnosis):
        first = playbooks.resolve(diagnosis)
        playbooks.record_operator_result(first.consultation_id, approved_remedy())
        second = playbooks.resolve(diagnosis)
        assert first.mode == "ask_operator"
        assert second.mode == "ask_operator"

    def test_third_match_uses_approved_playbook(playbooks, diagnosis):
        approve_twice(playbooks, diagnosis)
        third = playbooks.resolve(diagnosis)
        assert third.mode == "automatic"
        assert third.actions == approved_remedy().actions

    def test_sensitive_action_always_asks(playbooks, diagnosis):
        approve_twice(playbooks, diagnosis, action="change_credentials")
        result = playbooks.resolve(diagnosis)
        assert result.mode == "ask_operator"

    def test_elapsed_time_alone_is_not_a_stall(detector):
        evidence = StallEvidence(no_material_change_seconds=1800, process_alive=True)
        assert detector.evaluate(evidence) is None

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_stalls.py tests/test_playbooks.py -v

Expected: FAIL because stall and playbook services are undefined.

- [ ] **Step 3: Implement evidence normalization and versioned approval**

Normalize reasons to provider_limit, authentication, approval_wait, dependency, merge_conflict, ci_failure, environment_failure, resource_pressure, repeated_command_failure, or agent_uncertainty. Require corroborating evidence for inactivity.

Each playbook includes reason, evidence predicates, allowed actions, verification, timeout_seconds, rollback, project overrides, approval count, and version. approve-playbook writes config/playbooks.yaml atomically only from an explicit operator command, then records its content hash in SQLite. Automatic resolution requires two successful consultations for the same predicate and exact playbook hash.

Credentials, spending, external messages, destructive exceptions, and unregistered commands are always ask_operator actions.

- [ ] **Step 4: Run stall and YAML round-trip tests**

Run: uv run pytest tests/test_stalls.py tests/test_playbooks.py -v

Expected: all tests pass, including changed-predicate, failed-remedy, timeout, and project-override cases.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/stalls.py src/hermes_orchestrator/hermes_tools.py config/playbooks.yaml tests/test_stalls.py tests/test_playbooks.py
    git commit -m "feat: learn bounded stall remedies"

### Task 5: Reclaim worktrees only after verified Git recovery

**Files:**
- Create: src/hermes_orchestrator/worktrees.py
- Modify: src/hermes_orchestrator/git.py
- Test: tests/test_worktrees.py
- Test: tests/integration/test_worktree_cleanup.py

**Interfaces:**
- Produces: WorktreeCustodian.inspect(path: Path) -> WorktreeInspection
- Produces: WorktreeCustodian.checkpoint(lease_id: str, issue_id: str) -> Checkpoint
- Produces: WorktreeCustodian.verify_remote(checkpoint: Checkpoint) -> RemoteProof
- Produces: WorktreeCustodian.reclaim(lease_id: str, proof: RemoteProof) -> CleanupResult

- [ ] **Step 1: Write failing cleanup safety tests**

    def test_dirty_worktree_blocks_direct_reclaim(custodian):
        custodian.git.status = WorktreeStatus(modified=("src/a.py",), untracked=())
        with pytest.raises(CleanupBlocked, match="dirty"):
            custodian.reclaim("lease-1", proof=None)

    def test_wip_checkpoint_uses_clear_commit_message(custodian):
        checkpoint = custodian.checkpoint("lease-1", "ENG-431")
        assert checkpoint.commit_message == (
            "wip(ENG-431): checkpoint before resource cleanup"
        )

    def test_remote_proof_requires_branch_contains_sha(custodian, fake_git):
        fake_git.remote_contains = False
        with pytest.raises(RemoteVerificationFailed):
            custodian.verify_remote(checkpoint_fixture())

    def test_reclaim_uses_git_worktree_remove_then_prune(custodian, fake_git):
        result = custodian.reclaim("lease-1", verified_proof())
        assert result.removed is True
        assert fake_git.commands[-2:] == [
            ("worktree", "remove", "/repo/.worktrees/eng-431"),
            ("worktree", "prune"),
        ]

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_worktrees.py tests/integration/test_worktree_cleanup.py -v

Expected: FAIL because WorktreeCustodian is undefined.

- [ ] **Step 3: Implement inspect, checkpoint, verify, and reclaim**

Inspect with git status --porcelain=v2 --branch, git rev-parse HEAD, git branch --show-current, git rev-list @{upstream}..HEAD, and the process registry. checkpoint stages only paths recorded in the work lease, commits with the exact WIP format, pushes the explicit branch to the configured remote, and records the resulting SHA.

verify_remote runs git fetch REMOTE BRANCH and git merge-base --is-ancestor SHA REMOTE/BRANCH. It records remote, branch, SHA, and fetch timestamp in RemoteProof.

reclaim re-checks status, leases, processes, HEAD, and proof age; stops the exact process group; runs git worktree remove PATH without --force; runs git worktree prune; and verifies that git worktree list --porcelain no longer includes the path. It never invokes rm.

- [ ] **Step 4: Run real temporary-repository integration tests**

Run: uv run pytest tests/test_worktrees.py tests/integration/test_worktree_cleanup.py -v

Expected: all tests pass against a local bare remote, including dirty block, untracked block, WIP push, remote proof, active-process block, removal, and recovery checkout.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/worktrees.py src/hermes_orchestrator/git.py tests/test_worktrees.py tests/integration/test_worktree_cleanup.py
    git commit -m "feat: reclaim remotely recoverable worktrees"

### Task 6: Reconcile crashes and prove the resource-recovery flow

**Files:**
- Create: src/hermes_orchestrator/reconcile.py
- Modify: src/hermes_orchestrator/service.py
- Modify: src/hermes_orchestrator/supervisor.py
- Test: tests/test_reconcile.py
- Test: tests/integration/test_resource_recovery_acceptance.py
- Modify: README.md

**Interfaces:**
- Produces: Reconciler.run() -> ReconciliationReport
- Produces: ReconciliationReport.safe_to_open_admission: bool
- Produces: ReconciliationFinding(kind, aggregate_id, evidence, required_action)

- [ ] **Step 1: Write failing fail-closed tests**

    def test_unknown_live_process_keeps_admission_closed(reconciler):
        reconciler.processes.return_unknown_live_pid(900)
        report = reconciler.run()
        assert report.safe_to_open_admission is False
        assert report.findings[0].kind == "unknown_live_process"

    def test_missing_worktree_with_remote_proof_is_reconciled(reconciler):
        reconciler.worktrees.missing("lease-1")
        reconciler.git.remote_contains("sha-1", True)
        report = reconciler.run()
        assert report.safe_to_open_admission is True
        assert reconciler.events.last.event_type == "worktree.cleanup_reconciled"

    @pytest.mark.asyncio
    async def test_red_pressure_checkpoint_push_remove_restart(acceptance):
        result = await acceptance.run_red_pressure_scenario("ENG-431")
        assert result.checkpoint.remote_verified is True
        assert result.process_stopped is True
        assert result.worktree_removed is True
        assert result.restart.safe_to_open_admission is True
        assert result.recovered_next_action

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_reconcile.py tests/integration/test_resource_recovery_acceptance.py -v

Expected: FAIL because cross-system reconciliation is incomplete.

- [ ] **Step 3: Implement ordered reconciliation**

Reconcile in this order: SQLite integrity and incomplete transitions, process leases, worker sessions, worktrees and remote proofs, GitHub PR and merge state, CircleCI known state, Linear projection, then admission. Never perform a merge, Linear mutation, or worktree removal merely to make records agree. Produce findings and keep admission closed when evidence conflicts.

Supervisor startup calls Reconciler.run before profile probes or scheduling. SIGTERM and crash recovery reuse the same transition records and idempotency keys.

- [ ] **Step 4: Run Phase 4 verification**

Run: uv run pytest -q && uv run ruff check .

Expected: all tests pass, including injected crashes before push, after push, after remote proof, after process stop, after worktree removal, and before cleanup event commit.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/reconcile.py src/hermes_orchestrator/service.py src/hermes_orchestrator/supervisor.py tests README.md
    git commit -m "feat: deliver resource-safe recovery automation"

## Phase exit gate

Do not expose remote mutations until:

- Conservative bootstrap thresholds are committed, real managed-workload samples
  are recorded continuously, and threshold changes remain reviewable.
- A controlled red-pressure drill creates a WIP commit, pushes it, proves it remotely, stops only the leased process group, removes the worktree through Git, and resumes from the recorded next action.
- Deliberately dirty, untracked, unpushed, and active-process worktrees all block removal.
- Six active hours, 70%, 80%, compaction, rapid-refill, and context-error paths produce the approved handoff behavior.
- A new stall reason asks twice and the third exact match runs only the approved playbook.
- Crash injection at every cleanup boundary reconciles without lost work or duplicate external actions.
