# Codex Merger implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Add persistent, project-specific Codex Merger threads that independently review Claude pull requests, return structured defects to the Claude lead, merge approved work, apply the optimistic CircleCI window, and route QA issues correctly.

**Architecture:** A local stdio JSON-RPC client controls Codex App Server 0.149.0-alpha.4.1 and persists project thread identifiers. Merger threads run read-only; a separate GitHub adapter performs merges only after the deterministic review state machine opens the gate. CircleCI is reconciled at merge decision points rather than continuously polled.

**Tech Stack:** Prior phases, Codex CLI/App Server 0.149.0-alpha.4.1, JSON-RPC over stdio, HTTPX 0.28.1, Git CLI, GitHub REST API, CircleCI v2 API, Pydantic structured outputs

**Spec:** docs/superpowers/specs/2026-08-26-hermes-orchestration-system-design.md

## Global constraints

- Execute the foundation and Claude/Linear plans and pass both exit gates first.
- Use ChatGPT-authenticated Codex App Server; do not configure an OpenAI API key provider.
- Remove OPENAI_API_KEY from the App Server child environment and require account/read to report account.type=chatgpt before opening the merge gate.
- Each project has one durable Merger thread, and each project reviews one PR at a time.
- Start Merger threads with approvalPolicy=never and a readOnly sandbox.
- Codex never edits, amends, resolves conflicts, pushes, or merges.
- GitHub merges occur only through the deterministic source-control adapter.
- Every new reviewed SHA requires a fresh review.
- Critical or Important findings always return to the Claude project lead.
- No more than two merged PRs may have unresolved CircleCI results.
- CircleCI is checked when another PR becomes merge-ready, on operator request, or from an already-received failure event; it is not polled to completion.
- QA routing applies only to Ryan-originated or explicitly QA-designated issues.
- Every task follows test-driven development and ends with a focused commit.

## File map

- src/hermes_orchestrator/codex_rpc.py: stdio JSON-RPC lifecycle, requests, notifications, timeouts, and reconnects.
- src/hermes_orchestrator/codex_merger.py: project thread management and review turns.
- src/hermes_orchestrator/reviews.py: review state machine, verdicts, and correction packets.
- src/hermes_orchestrator/github.py: PR reads, merge mutation, and ancestry verification.
- src/hermes_orchestrator/circleci.py: pipeline lookups and unresolved merge window.
- src/hermes_orchestrator/qa.py: QA origin and assignee projection rules.
- prompts/codex-merger.md: immutable Merger role contract.
- schemas/review-verdict.json: structured verdict schema.
- schemas/correction-packet.json: correction packet schema.
- tests/: protocol, state-machine, adapter, and acceptance tests.

---

### Task 1: Implement a restartable Codex App Server JSON-RPC client

**Files:**
- Create: src/hermes_orchestrator/codex_rpc.py
- Test: tests/test_codex_rpc.py
- Test: tests/fixtures/codex_rpc.jsonl

**Interfaces:**
- Produces: CodexRpcClient.start() -> None
- Produces: CodexRpcClient.request(method: str, params: dict | None, timeout: float) -> dict
- Produces: CodexRpcClient.notifications() -> AsyncIterator[RpcNotification]
- Produces: CodexRpcClient.close() -> None

- [ ] **Step 1: Write failing handshake, correlation, and crash tests**

    @pytest.mark.asyncio
    async def test_initializes_before_other_requests(fake_server):
        client = CodexRpcClient(fake_server.command)
        await client.start()
        assert fake_server.received[:2] == [
            {
                "method": "initialize",
                "id": 1,
                "params": {
                    "clientInfo": {
                        "name": "hermes_orchestrator",
                        "title": "Hermes Orchestrator",
                        "version": "0.1.0",
                    }
                },
            },
            {"method": "initialized", "params": {}},
        ]

    @pytest.mark.asyncio
    async def test_matches_out_of_order_responses_by_id(fake_server):
        client = await started_client(fake_server)
        first, second = await asyncio.gather(
            client.request("thread/read", {"threadId": "a"}, 1),
            client.request("thread/read", {"threadId": "b"}, 1),
        )
        assert first["thread"]["id"] == "a"
        assert second["thread"]["id"] == "b"

    @pytest.mark.asyncio
    async def test_child_exit_fails_pending_requests(fake_server):
        client = await started_client(fake_server)
        pending = asyncio.create_task(client.request("thread/read", {"threadId": "a"}, 5))
        fake_server.exit(12)
        with pytest.raises(CodexUnavailable, match="exit code 12"):
            await pending

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_codex_rpc.py -v

Expected: FAIL because CodexRpcClient is undefined.

- [ ] **Step 3: Implement the stable stdio protocol**

Launch codex app-server --listen stdio:// with create_subprocess_exec, start_new_session=True, stdin=PIPE, stdout=PIPE, stderr=PIPE. Send compact newline-delimited JSON. Perform initialize followed by initialized before exposing request().

Use a monotonically increasing integer request id, one Future per id, a bounded notification queue, a 64 KiB maximum JSONL line, and explicit request timeouts. Reject experimental methods. Redact stderr and store no auth payloads. On close, fail pending requests, close stdin, wait five seconds, then terminate the process group.

- [ ] **Step 4: Run RPC tests**

Run: uv run pytest tests/test_codex_rpc.py -v

Expected: all tests pass, including malformed JSON, oversized line, request timeout, and child crash cases.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/codex_rpc.py tests/test_codex_rpc.py tests/fixtures/codex_rpc.jsonl
    git commit -m "feat: control Codex App Server over stdio"

### Task 2: Create and resume read-only project Merger threads

**Files:**
- Create: src/hermes_orchestrator/codex_merger.py
- Create: prompts/codex-merger.md
- Test: tests/test_codex_merger.py

**Interfaces:**
- Consumes: CodexRpcClient, ProjectConfig
- Produces: CodexMerger.ensure_thread(project_key: str) -> MergerThread
- Produces: CodexMerger.read_status(project_key: str) -> MergerStatus
- Produces: CodexMerger.verify_chatgpt_auth() -> CodexAuthHealth
- Produces: CodexMerger.read_rate_limits() -> CodexRateLimits
- Produces: CodexMerger.interrupt(project_key: str, turn_id: str) -> None

- [ ] **Step 1: Write failing thread configuration tests**

    @pytest.mark.asyncio
    async def test_new_merger_is_read_only_and_persistent(merger, rpc):
        thread = await merger.ensure_thread("demo")
        request = rpc.request_for("thread/start")
        assert request["params"] == {
            "model": "gpt-5.6-sol",
            "cwd": "/repo/demo",
            "approvalPolicy": "never",
            "sandbox": "readOnly",
            "serviceName": "hermes_orchestrator",
        }
        assert thread.thread_id == "thr_demo"
        assert rpc.request_for("thread/name/set")["params"]["name"] == "Merger: demo"

    @pytest.mark.asyncio
    async def test_existing_thread_is_resumed_not_recreated(merger, rpc, stored_thread):
        await merger.ensure_thread("demo")
        assert rpc.methods == ["thread/resume", "thread/goal/set"]

    @pytest.mark.asyncio
    async def test_rate_limit_state_uses_chatgpt_surface(merger, rpc):
        rpc.respond("account/rateLimits/read", rate_limit_fixture())
        limits = await merger.read_rate_limits()
        assert limits.primary_used_percent == 25
        assert limits.reached is False

    @pytest.mark.asyncio
    async def test_merge_gate_requires_chatgpt_auth(merger, rpc):
        rpc.respond(
            "account/read",
            {"account": {"type": "apiKey"}, "requiresOpenaiAuth": True},
        )
        health = await merger.verify_chatgpt_auth()
        assert health.eligible is False
        assert health.reason == "chatgpt_auth_required"

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_codex_merger.py -v

Expected: FAIL because CodexMerger is undefined.

- [ ] **Step 3: Implement thread persistence and the Merger prompt**

Start App Server with a child environment that removes OPENAI_API_KEY. Before creating or resuming a Merger turn, call account/read with refreshToken=false and require account.type=chatgpt. Do not persist the returned email or account identity.

For a new project, call thread/start, thread/name/set with Merger: PROJECT_ALIAS, thread/metadata/update with isPinned=true and the integration branch, and thread/goal/set with the project Merger objective. Persist thread id only after all required calls succeed.

For an existing id, call thread/read first. Resume only when a new turn is needed. If thread/read reports missing state, mark the record uncertain and require operator reconciliation instead of silently replacing it.

The prompt requires independent review, one PR at a time, local evidence, no corrective edits, no conflict resolution, structured Critical and Important findings, optimistic CircleCI behavior, ancestry proof, and live-state re-verification.

- [ ] **Step 4: Run thread and rate-limit tests**

Run: uv run pytest tests/test_codex_merger.py -v

Expected: all tests pass.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/codex_merger.py prompts/codex-merger.md tests/test_codex_merger.py
    git commit -m "feat: persist project Codex Merger threads"

### Task 3: Add review verdicts and correction packets

**Files:**
- Create: src/hermes_orchestrator/reviews.py
- Create: schemas/review-verdict.json
- Create: schemas/correction-packet.json
- Test: tests/test_reviews.py

**Interfaces:**
- Produces: ReviewService.submit(ReviewSubmission) -> ReviewRecord
- Produces: ReviewService.record_review_output(review_id: str, text: str) -> ReviewRecord
- Produces: ReviewService.request_structured_verdict(review_id: str) -> str
- Produces: ReviewService.return_to_lead(review_id: str, packet: CorrectionPacket) -> None
- Produces: CorrectionPacket(severity, repository, branch, pr_number, reviewed_sha, evidence, acceptance_criterion, required_correction, required_tests)

- [ ] **Step 1: Write failing review-boundary tests**

    @pytest.mark.asyncio
    async def test_submission_moves_issue_to_review(review_service):
        record = await review_service.submit(submission("ENG-9", "abc123"))
        assert record.state == "reviewing"
        assert review_service.linear.targets == [("ENG-9", "Review", "operator")]

    @pytest.mark.asyncio
    async def test_review_uses_commit_target(review_service, rpc):
        await review_service.submit(submission("ENG-9", "abc123"))
        params = rpc.request_for("review/start")["params"]
        assert params["delivery"] == "inline"
        assert params["target"] == {
            "type": "commit", "sha": "abc123", "title": "ENG-9"
        }

    @pytest.mark.asyncio
    async def test_important_finding_returns_packet_to_lead(review_service):
        packet = correction_packet(severity="Important", reviewed_sha="abc123")
        await review_service.return_to_lead("review-1", packet)
        assert review_service.claude.messages == [("demo", packet)]
        assert review_service.linear.targets[-1] == ("ENG-9", "In Development", "operator")

    def test_new_sha_invalidates_prior_approval(review_repository):
        review_repository.approve("ENG-9", "abc123")
        assert review_repository.is_approved("ENG-9", "def456") is False

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_reviews.py -v

Expected: FAIL because ReviewService is undefined.

- [ ] **Step 3: Implement the review state machine**

States are submitted, reviewing, corrections_required, approved, merge_blocked, merged, and superseded. submission persists issue, project, PR, branch, base, and exact SHA, then projects Review.

Call review/start with the commit target and wait for item/completed containing exitedReviewMode plus turn/completed. Send a follow-up turn/start on the same thread with the review text and outputSchema=schemas/review-verdict.json. The verdict schema permits only approved or corrections_required and an array of exact CorrectionPacket objects. Any Critical or Important finding yields corrections_required.

Before and after every Codex turn, run git status --porcelain through the host adapter. Any repository change closes the gate, records merger_mutation_detected, and requires operator reconciliation.

- [ ] **Step 4: Run review tests**

Run: uv run pytest tests/test_reviews.py -v

Expected: all tests pass, including timeout, missing exitedReviewMode, malformed structured verdict, and mutated-worktree cases.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/reviews.py schemas tests/test_reviews.py
    git commit -m "feat: return structured Codex findings to Claude leads"

### Task 4: Add GitHub merge and ancestry verification

**Files:**
- Create: src/hermes_orchestrator/github.py
- Create: src/hermes_orchestrator/git.py
- Test: tests/test_github.py
- Test: tests/test_git.py

**Interfaces:**
- Produces: GitHubClient.get_pull_request(repo: str, number: int) -> PullRequest
- Produces: GitHubClient.merge(repo: str, number: int, expected_head_sha: str, effect_id: str) -> MergeResult
- Produces: GitVerifier.fetch(repo_path: Path, remote: str) -> None
- Produces: GitVerifier.is_ancestor(repo_path: Path, commit: str, ref: str) -> bool

- [ ] **Step 1: Write failing stale-SHA and ancestry tests**

    @pytest.mark.asyncio
    async def test_merge_refuses_changed_head(github, transport):
        transport.pull_request(head_sha="new456", mergeable=True)
        with pytest.raises(MergeBlocked, match="head changed"):
            await github.merge("owner/demo", 14, "old123", "effect-merge-14")
        assert transport.merge_calls == []

    def test_ancestry_requires_fetched_remote(git_verifier, fake_git):
        fake_git.responses = {
            ("fetch", "origin", "main"): 0,
            ("merge-base", "--is-ancestor", "abc123", "origin/main"): 0,
        }
        git_verifier.fetch(Path("/repo"), "origin")
        assert git_verifier.is_ancestor(Path("/repo"), "abc123", "origin/main") is True

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_github.py tests/test_git.py -v

Expected: FAIL because the source-control adapters are undefined.

- [ ] **Step 3: Implement compare-and-merge**

Read the PR immediately before merging. Require open state, matching expected head SHA, matching configured base branch, no conflict, and the approved review record for that SHA. Send the GitHub merge request with the expected SHA. Persist the effect id and response before retrying.

After merge, run git fetch origin INTEGRATION_BRANCH and git merge-base --is-ancestor MERGED_SHA origin/INTEGRATION_BRANCH using argument arrays. Mark merged only when ancestry succeeds. A failed proof leaves the issue in Review and raises reconciliation_required.

- [ ] **Step 4: Run source-control tests**

Run: uv run pytest tests/test_github.py tests/test_git.py -v

Expected: all tests pass.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/github.py src/hermes_orchestrator/git.py tests/test_github.py tests/test_git.py
    git commit -m "feat: merge reviewed SHAs and prove ancestry"

### Task 5: Implement the optimistic CircleCI merge window

**Files:**
- Create: src/hermes_orchestrator/circleci.py
- Modify: src/hermes_orchestrator/reviews.py
- Test: tests/test_circleci.py
- Test: tests/test_merge_gate.py

**Interfaces:**
- Produces: CircleCIClient.read_pipeline(pipeline_id: str) -> PipelineResult
- Produces: CiWindow.before_next_merge(project_key: str) -> CiGate
- Produces: CiWindow.record_merge(project_key: str, pr_number: int, pipeline_id: str | None) -> None
- Produces: CiWindow.record_known_failure(project_key: str, pipeline_id: str) -> None

- [ ] **Step 1: Write failing optimistic-window tests**

    @pytest.mark.asyncio
    async def test_first_merge_does_not_wait_for_pending_ci(ci_window):
        ci_window.client.result = PipelineResult("pipe-1", "running")
        gate = await ci_window.before_next_merge("demo")
        assert gate.open is True
        assert ci_window.client.calls == []

    @pytest.mark.asyncio
    async def test_next_candidate_checks_last_unresolved_once(ci_window):
        ci_window.record_merge("demo", 10, "pipe-1")
        ci_window.client.result = PipelineResult("pipe-1", "success")
        gate = await ci_window.before_next_merge("demo")
        assert gate.open is True
        assert ci_window.client.calls == ["pipe-1"]

    @pytest.mark.asyncio
    async def test_known_failure_closes_gate(ci_window):
        ci_window.record_merge("demo", 10, "pipe-1")
        ci_window.client.result = PipelineResult("pipe-1", "failed")
        gate = await ci_window.before_next_merge("demo")
        assert gate.open is False
        assert gate.reason == "prior_ci_failed"

    def test_third_unresolved_merge_is_blocked(ci_window):
        ci_window.record_merge("demo", 10, "pipe-1")
        ci_window.record_merge("demo", 11, "pipe-2")
        assert ci_window.capacity("demo") == 0

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_circleci.py tests/test_merge_gate.py -v

Expected: FAIL because CiWindow is undefined.

- [ ] **Step 3: Implement decision-point reconciliation**

Store merge, pipeline id, last known status, last checked at, and resolved at. before_next_merge checks only the newest unresolved prior pipeline that has not already been checked for this candidate review. Success resolves it. Failure closes the gate and creates a correction packet for the Claude lead. Running remains unresolved.

If two earlier merges remain unresolved, close the gate before creating a third. Do not create a background CircleCI polling loop. record_known_failure closes the gate immediately when a failure event is already available.

- [ ] **Step 4: Run CI policy tests**

Run: uv run pytest tests/test_circleci.py tests/test_merge_gate.py -v

Expected: all tests pass and a fake clock proves no scheduled polling occurs.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/circleci.py src/hermes_orchestrator/reviews.py tests/test_circleci.py tests/test_merge_gate.py
    git commit -m "feat: enforce optimistic CircleCI merge window"

### Task 6: Add QA routing and the end-to-end merge flow

**Files:**
- Create: src/hermes_orchestrator/qa.py
- Modify: src/hermes_orchestrator/reviews.py
- Modify: src/hermes_orchestrator/service.py
- Test: tests/test_qa.py
- Test: tests/integration/test_codex_merge_acceptance.py
- Modify: README.md

**Interfaces:**
- Produces: QaRouter.record_origin(issue_id: str, origin: QaOrigin) -> None
- Produces: QaRouter.after_merge(issue_id: str) -> LinearProjection
- Produces: QaRouter.after_rejection(issue_id: str) -> LinearProjection
- Produces: ReviewService.merge_approved(review_id: str) -> MergeOutcome

- [ ] **Step 1: Write failing QA and acceptance tests**

    def test_ordinary_merge_routes_done(qa_router):
        assert qa_router.after_merge("ENG-9") == LinearProjection(
            status="Done", assignee_alias="operator"
        )

    def test_ryan_origin_routes_back_to_ryan_in_qa(qa_router):
        qa_router.record_origin("ENG-10", QaOrigin("ryan_assigned"))
        assert qa_router.after_merge("ENG-10") == LinearProjection(
            status="QA", assignee_alias="ryan"
        )

    def test_qa_rejection_returns_to_operator(qa_router):
        qa_router.record_origin("ENG-10", QaOrigin("ryan_assigned"))
        assert qa_router.after_rejection("ENG-10") == LinearProjection(
            status="In Development", assignee_alias="operator"
        )

    @pytest.mark.asyncio
    async def test_codex_defect_then_corrected_merge(acceptance):
        first = await acceptance.submit("ENG-10", sha="bad111", qa_origin="ryan_assigned")
        assert first.state == "corrections_required"
        assert acceptance.claude.last_packet.reviewed_sha == "bad111"
        second = await acceptance.submit("ENG-10", sha="good222")
        assert second.state == "merged"
        assert acceptance.linear.last_target == ("ENG-10", "QA", "ryan")
        assert acceptance.git.ancestry_checks == [("good222", "origin/main")]

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/test_qa.py tests/integration/test_codex_merge_acceptance.py -v

Expected: FAIL because QA routing and merged orchestration are incomplete.

- [ ] **Step 3: Implement QA persistence and merge orchestration**

QaOrigin permits ordinary, ryan_assigned, and operator_designated. Only the latter two route to QA. Persist the origin when the issue is admitted or explicitly designated; do not infer it from later assignment changes.

merge_approved re-reads PR and SHA, evaluates the CI window, performs the GitHub merge, proves ancestry, records unresolved pipeline state, and then projects Done or QA. Any failure before ancestry proof leaves Linear in Review. A QA rejection marks the current review superseded, projects In Development to the operator, and dispatches the rejection to the Claude lead.

- [ ] **Step 4: Run Phase 3 verification**

Run: uv run pytest -q && uv run ruff check .

Expected: all tests pass, including correction, stale SHA, CI failure, two-unresolved capacity, ordinary completion, QA return, and restart idempotency.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/qa.py src/hermes_orchestrator/reviews.py src/hermes_orchestrator/service.py tests README.md
    git commit -m "feat: deliver independent Codex merge workflow"

## Phase exit gate

Do not enable automatic resource cleanup until:

- A real Codex App Server smoke test creates, names, pins, reads, resumes, and reviews through a dedicated test project thread.
- The thread uses ChatGPT auth and account/rateLimits/read succeeds.
- A deliberate defect returns a complete correction packet to the Claude lead and Codex leaves the worktree unchanged.
- An approved test PR merges only through the GitHub adapter and passes fetched ancestry verification.
- The CI window permits two unresolved merges, blocks a third, and closes immediately on a known failure.
- Ordinary and QA-designated test issues reach their exact approved Linear states and assignees.
