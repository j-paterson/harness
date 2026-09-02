# INFRA-192 delegation plan — Bedrock as an explicit opt-in route

Authored by the Fable lead (session `dd2ef4ba-…`, cell `143a7985-…`,
profile max-a). Base: `feature/infra-192` fast-forwarded to
`origin/main` (`5f2a584`), isolated worktree
`/Users/josystem/hermes-orchestrator-issue-INFRA-192`. Issue:
[INFRA-192](https://linear.app/jo-solutions/issue/INFRA-192) — an
ordinary `claude` launch inherited Bedrock selection from the shell
outside the personal project tree; the provider depended on `$PWD`.

## Lead-verified anchors (base 5f2a584)

- `ProfileRegistry.launch_env` (profiles.py) already scrubs a fixed
  set of provider variables and pins `CLAUDE_CONFIG_DIR` for the four
  managed Max slots; `ClaudeProfileProbe.check` already refuses any
  profile whose `claude auth status --json` is not
  `firstParty` + `claude.ai` + `max`. Managed Fable seats therefore
  never inherited Bedrock. The gap is the operator's ordinary
  `claude` and the absence of a named Bedrock route.
- The live operator shell (`~/.zshrc`) works around the gap with a
  `$PWD` case statement: `~/Projects` → max-c, elsewhere → max-d.
  That is exactly the cwd dependence the issue forbids.
- `~/.claude-bedrock/settings.json` selects Bedrock via its own `env`
  block (`CLAUDE_CODE_USE_BEDROCK`, `AWS_REGION`, a bearer token).
  `claude auth status --json` under that config dir reports
  `apiProvider: bedrock`, `authMethod: third_party`, no
  `subscriptionType` — the shape the fail-closed classifier keys on.

## Design (lead-owned, security-sensitive)

`src/hermes_orchestrator/provider_routes.py` (new):

- Route names: `default`, `max-a`…`max-d` (kind `first_party_max`,
  `subscription: true`) and optional `bedrock` (kind `bedrock`,
  `subscription: false`). Bedrock is never one of the four Max slots.
- `ProviderRoutes.load(config/providers.yaml, registry)`: selection
  metadata only (`default_max_alias`, optional `bedrock.config_dir`,
  `aws_profile`, `aws_region`, optional `claude_executable`).
  Unknown keys, a Bedrock `config_dir` that collides with a Max
  profile, or a non-name AWS value fail closed.
- `launch_env(route, base)` is a pure function of the route and the
  caller environment; `$PWD` is never read. Max routes drop every
  Bedrock/AWS/Vertex/Foundry selector, static AWS credentials, and
  `ANTHROPIC_API_KEY`/`AUTH_TOKEN`/`BASE_URL`, then set exactly one
  `CLAUDE_CONFIG_DIR`. The Bedrock route starts from the same clean
  slate and re-adds only `CLAUDE_CODE_USE_BEDROCK=1`, the config dir,
  and the configured profile/region. No credential is ever injected.
- `classify(route, status)`: a Max route reporting Bedrock (or any
  non-first-party provider), and a Bedrock route reporting a
  subscription login, are refused with a named reason.
- `launch(...)` probes first and `execve`s only on a matching verdict;
  refusal messages name the route and reason, never the environment.
- `shell_init(...)` renders `claude`, `claude-max-*`, `claude-bedrock`
  as functions that forward to `claude-launch --route <fixed>`.

CLI (cli.py): `claude-launch --route R -- …`, `provider-probe
[--route R] [--json]`, `claude-shell-init`. `profiles.py` now imports
its scrub set from `provider_routes` (one source of truth), which
widens managed-seat sanitization to `AWS_BEARER_TOKEN_BEDROCK`,
`ANTHROPIC_BEDROCK_BASE_URL`, `ANTHROPIC_BASE_URL`, and the
skip-auth flags.

## Packets

| Packet | Boundary | Files | Tier | Wave |
|---|---|---|---|---|
| `52b42c8c…` | Contract coverage for the module: default route, each Max alias, explicit Bedrock, cwd independence, inherited-variable cleanup, config validation, classifier fail-closed both ways, launcher refusal, secret-free shell snippet | `tests/test_provider_routes.py` (new) | Sonnet | 1 |
| `65a7e9aa…` | CLI coverage through `cli.main` with a fake probe command and fake `execve`: launch refusal exit codes, `provider-probe` text/JSON and exit 1 on a failing route, `claude-shell-init` output and stable-launcher refusal | `tests/test_cli_provider_routes.py` (new) | Sonnet | 1 |

Out of scope: editing the operator's `~/.zshrc` (adoption is the
operator's step, at a session boundary, by sourcing the generated
snippet); moving the Bedrock bearer token out of
`~/.claude-bedrock/settings.json`; any inference call.

## Live acceptance (lead)

`provider-probe --json` against the real local config must report
`first_party_max` for `default` and all four aliases and `bedrock`
only for `bedrock`, with `ok: true` for every route. Existing active
Fable sessions are untouched: nothing here changes a running process,
and the managed-seat scrub widening applies only to launches after
the next runtime activation.
