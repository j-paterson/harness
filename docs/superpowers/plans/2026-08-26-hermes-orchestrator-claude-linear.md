# Claude leads and Linear projection implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Turn explicitly queued issues into profile-pinned Claude project cells, project only the approved workflow states to Linear, and rotate Claude leads through acknowledged handoffs without using Bedrock or API-key billing.

**Architecture:** The scheduler leases one scrubbed Claude Max profile to each active project and invokes Claude Code in resumable stream-json turns. A Linear adapter performs only idempotent state and assignee projection. The supervisor owns durable leases and session identifiers; cmux or another terminal is optional visibility, not the protocol.

**Tech Stack:** Phase 1 stack plus HTTPX 0.28.1, Claude Code 2.1.246 stream-json CLI, macOS Keychain, asyncio subprocesses, pytest HTTP mocks

**Spec:** docs/superpowers/specs/2026-08-26-hermes-orchestration-system-design.md

## Global constraints

- Execute docs/superpowers/plans/2026-08-26-hermes-orchestrator-foundation.md first. Phase 2 implementation and fake-backed tests may proceed while its resource-calibration gate remains open; live Claude dispatch and Linear writes may not.
- There are exactly four eligible Claude Max profile slots.
- Each profile has a separate CLAUDE_CONFIG_DIR and an opaque alias; account emails never enter Git or SQLite.
- Max-profile processes must unset Bedrock, Vertex, Foundry, AWS, and API-key provider selectors before launch.
- A profile is eligible only when claude auth status --json reports loggedIn=true, authMethod=claude.ai, and apiProvider=firstParty.
- One active Claude lead is allowed per project; its native subagents use the same profile.
- Running work remains pinned to its profile until a cap, auth failure, account failure, or explicit operator rotation.
- Linear receives only state and assignee changes described in the approved specification.
- Every external write is read-before-write, idempotent, and journaled.
- Every task follows test-driven development and ends with a focused commit.

## File map

- src/hermes_orchestrator/profiles.py: opaque profile registry, scrubbed launch environment, health, caps, and leases.
- src/hermes_orchestrator/linear.py: Linear GraphQL reads and minimal projection.
- src/hermes_orchestrator/claude.py: resumable Claude Code turns and stream event parsing.
- src/hermes_orchestrator/cells.py: one-lead-per-project lifecycle and subagent observations.
- src/hermes_orchestrator/handoffs.py: handoff document validation and acknowledgement.
- src/hermes_orchestrator/supervisor.py: async daemon loop and signal-safe shutdown.
- src/hermes_orchestrator/hermes_tools.py: narrow JSON command boundary used by Hermes.
- config/profiles.example.yaml: four opaque profile slots with no identities.
- prompts/claude-lead.md: persistent project-lead contract.
- tests/: unit, contract, and acceptance tests.

---

### Task 1: Isolate and validate four Claude Max profiles

**Files:**
- Modify: pyproject.toml
- Create: src/hermes_orchestrator/profiles.py
- Create: config/profiles.example.yaml
- Test: tests/test_profiles.py

**Interfaces:**
- Produces: ProfileRegistry.load(path: Path) -> ProfileRegistry
- Produces: ProfileRegistry.launch_env(alias: str, base: Mapping[str, str]) -> dict[str, str]
- Produces: ClaudeProfileProbe.check(alias: str) -> ProfileHealth
- Produces: ProfilePool.acquire(project_key: str) -> ProfileLease | None
- Produces: ProfilePool.release(project_key: str, reason: str) -> None

- [ ] **Step 1: Write failing environment and eligibility tests**

    def test_launch_environment_scrubs_non_subscription_providers(registry):
        env = registry.launch_env(
            "max-a",
            {
                "PATH": "/usr/bin",
                "CLAUDE_CODE_USE_BEDROCK": "1",
                "AWS_PROFILE": "work",
                "AWS_REGION": "us-east-1",
                "ANTHROPIC_API_KEY": "secret",
                "CLAUDE_CODE_USE_VERTEX": "1",
            },
        )
        assert env["CLAUDE_CONFIG_DIR"] == "/Users/operator/.claude-max-a"
        assert env["PATH"] == "/usr/bin"
        for key in (
            "CLAUDE_CODE_USE_BEDROCK", "AWS_PROFILE", "AWS_REGION",
            "ANTHROPIC_API_KEY", "CLAUDE_CODE_USE_VERTEX"
        ):
            assert key not in env

    def test_probe_rejects_bedrock_even_when_logged_in(probe):
        probe.command.result = {
            "loggedIn": True, "authMethod": "third_party", "apiProvider": "bedrock"
        }
        assert probe.check("max-a").eligible is False
        assert probe.check("max-a").reason == "not_first_party_subscription"

    def test_project_affinity_survives_repeated_acquire(profile_pool):
        first = profile_pool.acquire("demo")
        second = profile_pool.acquire("demo")
        assert second.profile_alias == first.profile_alias

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_profiles.py -v

Expected: FAIL because profile management is undefined.

- [ ] **Step 3: Implement the profile registry and probe**

Add httpx==0.28.1 to dependencies. Define four entries in profiles.example.yaml:

    profiles:
      - alias: max-a
        config_dir: /Users/OPERATOR/.claude-max-a
      - alias: max-b
        config_dir: /Users/OPERATOR/.claude-max-b
      - alias: max-c
        config_dir: /Users/OPERATOR/.claude-max-c
      - alias: max-d
        config_dir: /Users/OPERATOR/.claude-max-d

Remove these keys from the child environment: ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN, CLAUDE_CODE_USE_BEDROCK, CLAUDE_CODE_USE_VERTEX, CLAUDE_CODE_USE_FOUNDRY, AWS_PROFILE, AWS_REGION, AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, GOOGLE_APPLICATION_CREDENTIALS, and AZURE_CLIENT_SECRET.

Probe with claude auth status --json under the scrubbed environment. Store alias, health, cooldown_until, active_project_count, and last_checked_at; never store the returned account object.

- [ ] **Step 4: Run profile tests and a redacted local probe**

Run: uv run pytest tests/test_profiles.py -v

Then run:

    env -u CLAUDE_CODE_USE_BEDROCK -u AWS_PROFILE -u AWS_REGION \
      CLAUDE_CONFIG_DIR=/Users/josystem/.claude-personal \
      claude auth status --json |
      python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["loggedIn"], d["authMethod"], d["apiProvider"])'

Expected: tests pass and the local probe prints True claude.ai firstParty without printing identity fields.

- [ ] **Step 5: Commit**

    git add pyproject.toml uv.lock src/hermes_orchestrator/profiles.py config/profiles.example.yaml tests/test_profiles.py
    git commit -m "feat: isolate Claude Max profiles"

### Task 2: Implement minimal Linear state projection

**Files:**
- Create: src/hermes_orchestrator/linear.py
- Create: src/hermes_orchestrator/keychain.py
- Test: tests/test_linear.py
- Test: tests/test_keychain.py

**Interfaces:**
- Produces: Keychain.read(service: str, account: str) -> str
- Produces: LinearClient.get_issue(issue_id: str) -> LinearIssue
- Produces: LinearClient.project(issue_id: str, target: LinearProjection, effect_id: str) -> ProjectionResult
- Produces: LinearProjection(status: Todo|In Development|Review|QA|Done, assignee_alias: operator|ryan)

- [ ] **Step 1: Write failing projection tests**

    @pytest.mark.asyncio
    async def test_projection_reads_before_writing(linear_client, transport):
        transport.issue(status="Todo", assignee="operator", revision="r1")
        result = await linear_client.project(
            "ENG-9",
            LinearProjection(status="In Development", assignee_alias="operator"),
            effect_id="effect-1",
        )
        assert result.changed_fields == ("status",)
        assert transport.operations == ["Issue", "IssueUpdate"]

    @pytest.mark.asyncio
    async def test_projection_is_noop_when_target_matches(linear_client, transport):
        transport.issue(status="Review", assignee="operator", revision="r2")
        result = await linear_client.project(
            "ENG-9",
            LinearProjection(status="Review", assignee_alias="operator"),
            effect_id="effect-2",
        )
        assert result.changed_fields == ()
        assert transport.operations == ["Issue"]

    def test_allowed_projection_has_no_comment_or_label_fields():
        assert set(LinearProjection.model_fields) == {"status", "assignee_alias"}

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_linear.py tests/test_keychain.py -v

Expected: FAIL because LinearClient and Keychain are undefined.

- [ ] **Step 3: Implement Keychain retrieval and Linear GraphQL operations**

Use security find-generic-password -w with argument arrays and capture_output; never log stdout. The Linear token lives under service hermes-orchestrator-linear and account default.

Map only Todo, In Development, Review, QA, and Done. Resolve operator and Ryan identifiers during startup and keep them in memory. IssueUpdate includes only stateId and assigneeId values that differ from the read result. Persist effect_id, source revision, requested target, and response revision so retries return the recorded result.

- [ ] **Step 4: Run contract tests**

Run: uv run pytest tests/test_linear.py tests/test_keychain.py -v

Expected: all tests pass, and a request-body assertion proves no description, comment, or label mutation is sent.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/linear.py src/hermes_orchestrator/keychain.py tests/test_linear.py tests/test_keychain.py
    git commit -m "feat: project minimal workflow state to Linear"

### Task 3: Run resumable Claude Code lead turns

**Files:**
- Create: src/hermes_orchestrator/claude.py
- Create: prompts/claude-lead.md
- Test: tests/test_claude.py
- Test: tests/fixtures/claude_stream.jsonl

**Interfaces:**
- Consumes: ProfileRegistry.launch_env
- Produces: ClaudeRunner.start_lead(LeadTurnRequest) -> AsyncIterator[ClaudeEvent]
- Produces: ClaudeRunner.resume_lead(session_id: UUID, LeadTurnRequest) -> AsyncIterator[ClaudeEvent]
- Produces: ClaudeEventParser.feed(line: bytes) -> ClaudeEvent

- [ ] **Step 1: Write failing command and parser tests**

    def test_new_lead_uses_subscription_profile_and_persistent_session(runner):
        command, env = runner.build_command(
            LeadTurnRequest(
                session_id=UUID("11111111-1111-4111-8111-111111111111"),
                cwd=Path("/repo"),
                prompt="Plan ENG-9",
                profile_alias="max-a",
                resume=False,
            )
        )
        assert command[:4] == ["claude", "-p", "--model", "fable"]
        assert "--session-id" in command
        assert "--output-format=stream-json" in command
        assert "--include-hook-events" in command
        assert env["CLAUDE_CONFIG_DIR"].endswith(".claude-max-a")
        assert "CLAUDE_CODE_USE_BEDROCK" not in env

    def test_resume_uses_same_session_id(runner):
        command, _ = runner.build_command(resume_request())
        assert command[command.index("--resume") + 1] == str(resume_request().session_id)
        assert "--session-id" not in command

    def test_parser_extracts_session_subagent_and_limit_events(fixture_lines):
        events = [ClaudeEventParser().feed(line) for line in fixture_lines]
        assert any(event.kind == "session.started" for event in events)
        assert any(event.kind == "subagent.started" for event in events)
        assert any(event.kind == "provider.limit" for event in events)

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_claude.py -v

Expected: FAIL because the Claude runner is undefined.

- [ ] **Step 3: Implement safe commands and stream parsing**

Build the first-turn command from:

    claude -p --model fable --effort high \
      --session-id SESSION_UUID \
      --input-format text \
      --output-format=stream-json \
      --include-hook-events \
      --forward-subagent-text \
      --permission-mode auto \
      --append-system-prompt-file prompts/claude-lead.md \
      PROMPT

Use --resume SESSION_UUID instead of --session-id for later turns. Use create_subprocess_exec with cwd, start_new_session=True, the scrubbed environment, stdout=PIPE, and stderr=PIPE. Parse one JSON object per stdout line; preserve event type, session id, parent tool-use id, timestamp, usage fields, and sanitized error code. Keep only bounded diagnostic excerpts in SQLite.

The lead prompt states: manage only Hermes-supplied issue IDs, maximize safe parallel subagents, keep one project lead, checkpoint at boundaries, submit PRs to Codex, never merge, and return a structured handoff when requested.

- [ ] **Step 4: Run parser and subprocess cancellation tests**

Run: uv run pytest tests/test_claude.py -v

Expected: all tests pass, including SIGTERM followed by a bounded SIGKILL fallback for a fake hung child.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/claude.py prompts/claude-lead.md tests/test_claude.py tests/fixtures/claude_stream.jsonl
    git commit -m "feat: run resumable Claude lead turns"

### Task 4: Enforce one project lead and profile affinity

**Files:**
- Create: src/hermes_orchestrator/cells.py
- Modify: src/hermes_orchestrator/scheduler.py
- Modify: src/hermes_orchestrator/service.py
- Test: tests/test_cells.py
- Test: tests/test_scheduler_cells.py

**Interfaces:**
- Consumes: QueueService, ProfilePool, ClaudeRunner, LinearClient
- Produces: ProjectCellService.start(project_key: str) -> ProjectCell
- Produces: ProjectCellService.dispatch(issue_id: str) -> DispatchResult
- Produces: ProjectCellService.record(event: ClaudeEvent) -> None
- Produces: ProjectCellService.pause(project_key: str, reason: str) -> PauseResult

- [ ] **Step 1: Write failing cell lifecycle tests**

    @pytest.mark.asyncio
    async def test_two_issues_share_one_project_lead(cell_service):
        first = await cell_service.dispatch("ENG-9")
        second = await cell_service.dispatch("ENG-10")
        assert first.cell_id == second.cell_id
        assert first.session_id == second.session_id
        assert cell_service.runner.start_count == 1
        assert cell_service.runner.resume_count == 1

    @pytest.mark.asyncio
    async def test_working_issue_projects_in_development(cell_service):
        await cell_service.dispatch("ENG-9")
        assert cell_service.linear.targets == [
            ("ENG-9", "In Development", "operator")
        ]

    @pytest.mark.asyncio
    async def test_no_profile_available_keeps_issue_queued(cell_service):
        cell_service.profiles.mark_all_capped()
        result = await cell_service.dispatch("ENG-9")
        assert result.status == "waiting_for_profile"
        assert cell_service.linear.targets == []

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_cells.py tests/test_scheduler_cells.py -v

Expected: FAIL because ProjectCellService is undefined.

- [ ] **Step 3: Implement cell and lease transitions**

Within one database transaction, create one active cell per project, acquire a profile lease, assign a UUID session id, and append project_cell.started. Project Linear state only after the Claude process has emitted session.started. A second issue resumes the same logical lead session.

Record subagent events as child worker leases with the same profile alias and project key. Reject any event that attempts to associate a child with another profile. On a provider-limit event, mark the profile cooldown and set the cell to handoff_required rather than silently relaunching.

- [ ] **Step 4: Run lifecycle tests**

Run: uv run pytest tests/test_cells.py tests/test_scheduler_cells.py tests/test_profiles.py -v

Expected: all tests pass.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/cells.py src/hermes_orchestrator/scheduler.py src/hermes_orchestrator/service.py tests/test_cells.py tests/test_scheduler_cells.py
    git commit -m "feat: manage profile-pinned Claude project cells"

### Task 5: Add complete, acknowledged handoffs and profile rotation

**Files:**
- Create: src/hermes_orchestrator/handoffs.py
- Modify: src/hermes_orchestrator/cells.py
- Test: tests/test_handoffs.py
- Test: tests/test_profile_rotation.py

**Interfaces:**
- Produces: HandoffService.request(cell_id: str, reason: str) -> HandoffRequest
- Produces: HandoffService.submit(HandoffDocument) -> HandoffRecord
- Produces: HandoffService.acknowledge(handoff_id: str, session_id: UUID, restated_next_action: str) -> HandoffRecord
- Produces: ProjectCellService.rotate(cell_id: str, handoff_id: str) -> ProjectCell

- [ ] **Step 1: Write failing handoff tests**

    def test_handoff_requires_every_contract_field(handoffs):
        incomplete = valid_handoff().model_copy(update={"tests": []})
        with pytest.raises(HandoffRejected, match="tests"):
            handoffs.submit(incomplete)

    @pytest.mark.asyncio
    async def test_old_lead_retires_only_after_acknowledgement(cell_service, handoffs):
        record = handoffs.submit(valid_handoff())
        with pytest.raises(RotationBlocked, match="acknowledged"):
            await cell_service.rotate("cell-1", record.handoff_id)
        handoffs.acknowledge(
            record.handoff_id,
            UUID("22222222-2222-4222-8222-222222222222"),
            "Run the failing unit test and correct ENG-9.",
        )
        rotated = await cell_service.rotate("cell-1", record.handoff_id)
        assert rotated.profile_alias == "max-b"
        assert cell_service.runner.retired_sessions == [
            UUID("11111111-1111-4111-8111-111111111111")
        ]

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_handoffs.py tests/test_profile_rotation.py -v

Expected: FAIL because handoff validation and rotation are undefined.

- [ ] **Step 3: Implement the handoff contract**

HandoffDocument requires objective, status, decisions, branch, commits, pull_request, modified_files, tests with outcomes, blockers, remaining_steps, commands, environment_notes, risks, and next_action. Empty arrays are valid only for blockers and risks. Store structured fields and a rendered Markdown snapshot.

Rotation acquires a different healthy profile, starts a new UUID session with the handoff as the first prompt, waits for a structured acknowledgement with a non-empty restated_next_action, transfers the project lease transactionally, and only then terminates the old process. A failed acknowledgement leaves the old lead and lease intact.

- [ ] **Step 4: Run handoff and failure-injection tests**

Run: uv run pytest tests/test_handoffs.py tests/test_profile_rotation.py -v

Expected: all tests pass, including replacement-start failure and acknowledgement timeout cases.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/handoffs.py src/hermes_orchestrator/cells.py tests/test_handoffs.py tests/test_profile_rotation.py
    git commit -m "feat: rotate Claude leads through acknowledged handoffs"

### Task 6: Add the supervisor daemon, Hermes commands, and Phase 2 acceptance test

**Files:**
- Create: src/hermes_orchestrator/supervisor.py
- Create: src/hermes_orchestrator/hermes_tools.py
- Modify: src/hermes_orchestrator/cli.py
- Test: tests/test_supervisor.py
- Test: tests/test_hermes_tools.py
- Test: tests/integration/test_claude_linear_acceptance.py
- Modify: README.md

**Interfaces:**
- Produces: hermes-orchestrator daemon
- Produces: hermes-orchestrator hermes-command --json REQUEST
- Produces Hermes intents: queue_issue, status, pause, resume, retry, reprioritize, approve_handoff

- [ ] **Step 1: Write failing daemon and command-boundary tests**

    def test_hermes_cannot_discover_work(command_service):
        result = command_service.execute({"intent": "scan_linear"})
        assert result.code == "intent_not_allowed"

    def test_queue_intent_requires_issue_and_instruction(command_service):
        result = command_service.execute(
            {"intent": "queue_issue", "issue_id": "ENG-9", "project": "demo"}
        )
        assert result.code == "operator_instruction_required"

    @pytest.mark.asyncio
    async def test_shutdown_stops_admission_before_children(supervisor):
        await supervisor.start()
        await supervisor.shutdown()
        assert supervisor.events[-3:] == [
            "admission.closed", "workers.checkpoint_requested", "supervisor.stopped"
        ]

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_supervisor.py tests/test_hermes_tools.py tests/integration/test_claude_linear_acceptance.py -v

Expected: FAIL because the daemon and Hermes command boundary are undefined.

- [ ] **Step 3: Implement the daemon and narrow command schema**

The daemon reconciles, probes profiles, samples resources, plans work, executes only enabled Phase 2 actions, and sleeps with jitter. SIGTERM closes admission, requests checkpoints, waits up to 30 seconds, persists leases, and exits.

Hermes commands are strict Pydantic discriminated unions. Unknown fields and intents fail validation. queue_issue requires issue_id, project_key, priority, and operator_instruction_id. The command returns JSON with correlation_id, result code, and sanitized state; it never returns account identity or raw process output.

- [ ] **Step 4: Run Phase 2 verification**

Run: uv run pytest -q && uv run ruff check .

Expected: all tests pass, including a fake end-to-end flow from explicit queue admission through one project lead and an In Development Linear projection.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/supervisor.py src/hermes_orchestrator/hermes_tools.py src/hermes_orchestrator/cli.py tests README.md
    git commit -m "feat: deliver Claude and Linear orchestration"

## Phase exit gate

Do not begin Codex merging until:

- Four opaque profile directories have been created and each passes the scrubbed firstParty subscription probe.
- A live smoke test confirms the primary claude command remains Bedrock while every Max profile command remains firstParty.
- One test project runs two queued issues through a single persistent lead session.
- A forced cap rotates only after a complete handoff and replacement acknowledgement.
- Linear changes contain only allowed status and assignee fields.
- The supervisor can restart mid-turn without duplicating a lead or Linear transition.

Until the Phase 1 resource-calibration gate is satisfied, run this phase with
fake adapters or explicit dry-run actions only. Keep live Claude dispatch,
Linear writes, and automated resource cleanup disabled.
