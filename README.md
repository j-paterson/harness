# Hermes orchestrator

Hermes orchestrator is a local control plane for explicitly assigned coding work. It maintains a private queue, durable project cells, isolated Claude Max profiles, minimal Linear workflow projection, complete lead handoffs, and continuous host-resource observation.

The checked-in policy remains in `observe` mode. Running the CLI does not start Claude, update Linear, stop processes, or remove worktrees unless those actions are separately configured and enabled. The Phase 2 adapters and supervisor flow are covered with local fakes; no live Linear mutation is part of the default setup.

## Set up the orchestrator

The project requires Python 3.13 and uv.

```bash
uv sync --python 3.13 --group dev
```

Copy `config/projects.example.yaml` to the ignored `config/projects.yaml` and add only non-secret project routing fields. Keep tokens, account identities, phone numbers, and runtime state outside this repository.

Copy `config/profiles.example.yaml` to the ignored `config/profiles.yaml`. Keep exactly four opaque aliases and point each one at a separate Claude configuration directory. Do not put account email addresses in the aliases, configuration file, or runtime database.

Before a profile can receive work, its scrubbed probe must report `loggedIn=true`, `authMethod=claude.ai`, and `apiProvider=firstParty`. Provider selectors such as Bedrock, AWS, Vertex, Foundry, and Anthropic API-key variables are removed from every Max-profile child process.

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

`observe` records one resource sample and prints a non-executing plan. Use `observe --watch 60` to continue at an interval of at least five seconds. Resource thresholds begin with conservative hard floors and are refined from real managed work; no waiting period or synthetic load is required.

Hermes can use the same strict JSON boundary as the future phone console:

```bash
uv run hermes-orchestrator hermes-command --json \
  '{"intent":"queue_issue","issue_id":"ENG-7","project_key":"PROJECT_ALIAS","priority":2,"operator_instruction_id":"CHAT_INSTRUCTION_ID"}'
```

The boundary accepts only `queue_issue`, `status`, `pause`, `resume`, `retry`, `reprioritize`, and `approve_handoff`. It has no command for discovering or claiming Linear work.

The Linear token is read at runtime from macOS Keychain service `hermes-orchestrator-linear`, account `default`. Linear projections read the issue first and can change only the approved state and assignee fields. Keep the daemon in `observe` mode until the token, workflow identifiers, and all four profile probes have been verified.

## Verify the system

```bash
uv run pytest -q
uv run ruff check .
```

Read the [approved system design](docs/superpowers/specs/2026-08-26-hermes-orchestration-system-design.md) and [phased implementation plans](docs/superpowers/plans/README.md) before expanding the service's authority.
