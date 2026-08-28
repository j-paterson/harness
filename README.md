# Hermes orchestrator

Hermes orchestrator is a local control plane for explicitly assigned coding work. It maintains a private queue, durable project cells, isolated Claude Max profiles, minimal Linear workflow projection, complete lead handoffs, and continuous host-resource observation.

The checked-in policy remains in `observe` mode. Running the CLI does not start Claude, update Linear, stop processes, or remove worktrees unless those actions are separately configured and enabled. Live Phase 2 assembly requires an ignored local policy override plus complete project, profile, and Linear routing configuration; missing or inconsistent inputs fail closed.

## Set up the orchestrator

The project requires Python 3.13 and uv.

```bash
uv sync --python 3.13 --group dev
```

Copy `config/projects.example.yaml` to the ignored `config/projects.yaml` and add only non-secret project routing fields. Keep tokens, account identities, phone numbers, and runtime state outside this repository.

Copy `config/profiles.example.yaml` to the ignored `config/profiles.yaml`. Keep exactly four opaque aliases and point each one at a separate Claude configuration directory. Do not put account email addresses in the aliases, configuration file, or runtime database.

Before a profile can receive work, its scrubbed probe must report `loggedIn=true`, `authMethod=claude.ai`, and `apiProvider=firstParty`. Provider selectors such as Bedrock, AWS, Vertex, Foundry, and Anthropic API-key variables are removed from every Max-profile child process.

Copy `config/linear.example.yaml` to the ignored `config/linear.yaml`. Register only the team IDs and exact state IDs used by projects in `projects.yaml`, plus the operator and QA assignee IDs. The runtime reads the issue first and proves that its Linear team matches the selected project before starting Claude.

The final live switch is an ignored `config/policies.local.yaml` containing `mode: active`. The tracked `config/policies.yaml` stays observation-only, so a fresh clone cannot produce external effects. Active startup probes all four Max profiles and reads the Linear token from macOS Keychain; it does not persist account identity or the token.

Initialize the local database:

```bash
uv run hermes-orchestrator init
```

## Admit explicit work

Add only an issue that the operator explicitly assigned to Hermes:

```bash
uv run hermes-orchestrator queue-add ENG-7 \
  --project PROJECT_ALIAS \
  --priority 2 \
  --operator-instruction CHAT_INSTRUCTION_ID
```

`PROJECT_ALIAS` must exist in `config/projects.yaml`. `CHAT_INSTRUCTION_ID` is the durable idempotency identifier for the operator's instruction; it is required and cannot be reused for different work.

Inspect the queue and state:

```bash
uv run hermes-orchestrator queue-list
uv run hermes-orchestrator status
uv run hermes-orchestrator reconcile
uv run hermes-orchestrator observe
uv run hermes-orchestrator daemon --once
```

`observe` records one resource sample and prints a non-executing plan, even when the local active override exists. Use `observe --watch 60` to continue at an interval of at least five seconds. Resource thresholds begin with conservative hard floors and are refined from real managed work; no waiting period or synthetic load is required. An active daemon starts or resumes Claude only while the resource snapshot is green and startup reconciliation has explicitly opened admission.

Hermes can use the same strict JSON boundary as the future phone console:

```bash
uv run hermes-orchestrator hermes-command --json \
  '{"intent":"queue_issue","issue_id":"ENG-7","project_key":"PROJECT_ALIAS","priority":2,"operator_instruction_id":"CHAT_INSTRUCTION_ID"}'
```

The boundary accepts only `queue_issue`, `status`, `pause`, `resume`, `retry`, `reprioritize`, `approve_handoff`, `qa_reject`, `pending_corrections`, and `ack_correction`. It has no command for discovering or claiming Linear work.

For Hermes chat intake, trust this repository once with:

```bash
hermes skills trust /PATH/TO/hermes-orchestrator
```

New Hermes sessions started inside the trusted repository can load the repo-local `hermes-orchestrator-intake` skill. The skill reads only the explicitly supplied issue through the official Linear MCP, routes Infrastructure issues to the `agent-orchestration` project, submits the strict `queue_issue` command, and verifies the private queue read-back. It does not replace or wrap Hermes's direct Linear MCP access.

Linear status keys in `config/linear.yaml` are logical orchestrator milestones. A team may map `In Development` and `Review` to differently named workflow states such as Infrastructure's `In Progress` and `In Review`. `QA` may be omitted for teams without a QA state; requesting a QA projection on such a route fails closed.

QA routing is explicit and durable. `queue_issue` accepts an optional `qa_origin` of `ordinary` (default), `ryan_assigned`, or `operator_designated`; it is recorded at admission and never inferred from later assignment changes. After the Codex Merger's proven merge, ordinary work projects `Done` to the operator, operator-designated work projects `QA` to the operator, and work Ryan assigned from QA returns to Ryan in `QA`. A QA rejection supersedes the merged review, projects `In Development` to the operator, and returns a Critical correction packet to the Claude lead; the corrected work returns to Ryan in QA again after its next merge. Every merge path before ancestry proof leaves Linear in `Review`, and the merged item is journaled in the CircleCI window without any CI query.

The Codex merge flow is event-driven end to end. At a freeze boundary the lead runs `hermes-orchestrator candidate-ready ISSUE --project ALIAS --verified "uv run pytest -q=N passed"`, which proves the clean, pushed feature-branch head, publishes an immutable manifest under the state directory, registers the wake durably, and queues the typed `FABLE_READY` envelope onto the project's Merger thread through `codex queue`. When the Merger's turn completes, the daemon's App Server listener (or `hermes-orchestrator merger-turn --project ALIAS`) admits the delivered wake through the CircleCI reconciliation and one-open-pull-request gates, reads the thread's structured report, and drives the review service: correction packets land in the durable `lead_corrections` outbox (`pending_corrections` / `ack_correction` intents) for Hermes to return to the exact lead, approved candidates merge only through the GitHub adapter with ancestry proof, and the merged item is journaled unresolved until the next candidate boundary reconciles it. A full merge window marks the wake `deferred`; the candidate is re-emitted as a new event once a prior pipeline resolves. A prior CircleCI failure blocks every intake until an explicitly bound rework arrives: the lead emits `candidate-ready ... --status FABLE_REWORK_READY` for the failed issue on its branch, the intake gate admits it only because the durable `ci_failure` correction packet for that exact reviewed SHA authorizes it, and the failed ledger row transitions to `corrected` (recording the rework event, SHA, and correction id) only after full admission; ordinary, stale, foreign, or rejected candidates never change the failed state and replays are idempotent. A newly created Merger task is `notLoaded` until it is opened once in Codex; a queued wake is held until then and the completed turn settles it — `candidate-ready` reports `awaiting_load`, and nothing polls or re-wakes. Persisted tasks are re-verified by `thread/read` alone; `thread/resume` is attempted only for `notLoaded` tasks and a rejected resume never marks a still-readable channel uncertain. `qa_reject` returns merged QA work to the lead with a Critical packet.

Legacy durable databases whose migration 3 predates reviewer channels are upgraded forward-only by migration 9: `reviewer_channels` is created if missing, any legacy `merger_threads` row is carried over as a `configuring` channel that becomes ready only after live re-verification, and already-applied migrations are never edited or re-run. GitHub and CircleCI tokens are read from Keychain services `hermes-orchestrator-github` and `hermes-orchestrator-circleci` (account `default`).

Each project declares an explicit CI policy in `projects.yaml`: `ci: circleci` (the default) reconciles merged candidates against CircleCI at intake boundaries and keeps every non-success — including a 404 for a repository that is not configured in CircleCI — ambiguous and fail-closed; `ci: none` declares that the repository has no CI, so merged items resolve durably as `ci_not_configured` with zero CircleCI calls and never permanently consume the merge window. No-CI is never inferred from a CI response. The `hermes-orchestrator-circleci` Keychain token is required only when at least one configured project uses CircleCI.

The Linear token is read at runtime from macOS Keychain service `hermes-orchestrator-linear`, account `default`. Linear projections read the issue first and can change only the approved state and assignee fields. Team validation happens before Claude starts, and reconciliation can veto every otherwise executable dispatch.

Every managed process — Claude lead turns, the Codex App Server, and `codex queue` invocations — is registered in `process_leases` immediately after it is spawned in its own session, recording pid, process group, executable, working directory, project, worker, and psutil `create_time`. A child that cannot be registered is terminated and its launch fails closed. Stops require a checkpoint id, re-validate the recorded identity before any signal (a reused PID is never signaled), SIGTERM the exact recorded group, wait the bounded grace period, and SIGKILL only the same validated group; every signal and result is journaled. Reconciliation expires leases whose process is gone and reports live orphans as findings that keep admission closed. Nothing is ever killed by name or path. A stop is a durable, owned claim: the checkpoint, owner token, phase, and claim expiry are journaled before any signal, exactly one caller may advance each phase (`term_claimed` → `term_sent` → `kill_claimed` → `kill_sent`), identity is re-validated immediately before each signal, and a stale claim is recovered only after its lease expires without re-sending a possibly-sent signal. `candidate-ready` runs the durable intake gate before queuing the wake, so a deferred or blocked candidate never wakes the Merger; the turn-time gate replays that event-bound decision with zero CI calls.

Admission is governed by the calibrated thresholds in `config/policies.yaml` (`resource_thresholds`): green admits work; yellow — any yellow floor, swap growth above `yellow_swap_growth_gib_per_hour`, sustained load above `yellow_load_ratio`, or macOS memory-pressure warning — closes admission for priority 2–4 work while priority 1 and all active work continue; red — any red floor or critical memory pressure — additionally requests exactly one checkpoint per tick from the lowest-priority checkpoint-safe lead (ordered by priority, dependency criticality, recent progress, then RSS), re-sampling before any further request. Levels worsen immediately and relax only after `hysteresis_samples` consecutive calmer samples, so green and yellow never flap. Uncalibrated thresholds keep admission closed. Nothing here terminates a process. A red checkpoint targets only a lead with current durable safe-boundary evidence (`checkpoint_safety`, recorded when its turn completes normally, bound to its exact session, and invalidated the moment new work is dispatched, the session rotates, or the start fails); missing, stale, or prior-session evidence is unsafe, so admission closes but nothing is requested. Checkpoint requests are durable (`checkpoint_requests`) with an idempotent identity per proven boundary; exactly one may be pending host-wide, new requests are suppressed while one is pending or until a fresh red sample follows its completion/failure/stale transition, and the rule is reconstructed from the database after a restart. Delivery is a separate durable step: the supervisor delivers each reserved request through a dispatcher that revalidates the exact cell and session at execution time and records `delivered_at`; a missing callback, a session/target mismatch, a cell that is no longer active, cancellation, or any exception transitions that exact request to `failed`/`stale` with bounded evidence before the error propagates, and a request reserved just before a restart is delivered exactly once on the next tick.

## Verify the system

```bash
uv run pytest -q
uv run ruff check .
```

Read the [approved system design](docs/superpowers/specs/2026-08-26-hermes-orchestration-system-design.md) and [phased implementation plans](docs/superpowers/plans/README.md) before expanding the service's authority.
