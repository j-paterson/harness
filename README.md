# Hermes orchestrator

Hermes orchestrator is a local control plane for explicitly assigned coding work. The current foundation release is observation-only: it can validate configuration, maintain a private queue, reconcile local SQLite state, and measure host resources. It cannot start Claude or Codex, update Linear, merge pull requests, stop processes, or remove worktrees.

## Set up the foundation

The project requires Python 3.13 and uv.

```bash
uv sync --python 3.13 --group dev
```

Copy `config/projects.example.yaml` to the ignored `config/projects.yaml` and add only non-secret project routing fields. Keep tokens, account identities, phone numbers, and runtime state outside this repository.

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
```

`observe` records one resource sample and prints a non-executing plan. Use `observe --watch 60` to continue at an interval of at least five seconds.

## Verify the foundation

```bash
uv run pytest -q
uv run ruff check .
```

Read the [approved system design](docs/superpowers/specs/2026-08-26-hermes-orchestration-system-design.md) and [phased implementation plans](docs/superpowers/plans/README.md) before expanding the service's authority.
