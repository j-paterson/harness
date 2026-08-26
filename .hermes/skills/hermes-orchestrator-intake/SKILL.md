---
name: hermes-orchestrator-intake
description: Admit assigned Linear issues to the private queue.
version: 0.1.0
author: Jesse, Hermes Agent
license: MIT
platforms: [linux, macos]
metadata:
  hermes:
    tags: [linear, orchestration, intake, queue]
    related_skills: []
    requires_tools: [terminal]
---

# Hermes Orchestrator Intake

Admit only a Linear issue that the operator explicitly assigns in the current chat. Use the official Linear MCP for issue context and the orchestrator's strict local command boundary for private queue mutation.

## When to Use

- The operator explicitly supplies or unambiguously identifies a Linear issue ID and assigns it to Hermes.
- The issue belongs to a registered project route.

Do not use this skill to discover, select, infer, or claim work. Never scan Linear for candidate issues.

## Prerequisites

- The official Linear MCP is enabled and authenticated.
- The repository has `config/projects.yaml` with the target project registration.
- `uv` and Python 3.13 are available.

## Procedure

1. Call the official Linear MCP `get_issue` tool with exactly the supplied issue ID. Do not list or search for other issues. Completion: the returned identifier exactly matches the operator-supplied identifier.
2. Validate the route from the issue's team. Infrastructure issues (`INFRA-*`, team `Infrastructure`) route to project key `agent-orchestration`. Stop if the identifier prefix and team disagree or no route exists. Completion: one project key is proven without guessing.
3. Read the issue priority. Accept only Linear priority values 1 through 4; ask the operator for a queue priority if Linear has no supported value. Completion: one integer priority in the allowed range is selected.
4. Create one instruction identifier for this assignment in the form `hermes-chat:<issue-id>:<uuid>`. Generate it once and reuse it for every retry in the current assignment. Completion: the identifier is non-empty and stable for the attempt.
5. Resolve the repository root with `terminal(command="git rev-parse --show-toplevel")`, then call the strict boundary from that root:

   ```text
   terminal(command="uv run hermes-orchestrator --repo-root <repo-root> hermes-command --json '<request-json>'")
   ```

   Build `<request-json>` as exactly this object with the placeholders replaced:

   ```json
   {"intent":"queue_issue","issue_id":"<issue-id>","project_key":"agent-orchestration","priority":1,"operator_instruction_id":"<instruction-id>"}
   ```

   Replace the example priority with the validated priority from step 3.

   Completion: parse the JSON response and require `code` to equal `queued`. Treat every other code as not admitted.
6. Verify with `terminal(command="uv run hermes-orchestrator --repo-root <repo-root> queue-list --json")`. Completion: exactly one queue record has the supplied issue ID and project key.
7. Return the issue ID, project key, priority, queue state, and correlation ID. Do not report success from command exit status alone.

## Pitfalls

- Assignment to a Linear user is not operator authorization; the current chat must explicitly assign the issue.
- Do not substitute the orchestrator's private Linear GraphQL adapter for the official Linear MCP read.
- Do not reuse an instruction identifier for another issue, project, or priority.
- Do not broaden one assigned issue into dependencies or related issues.

## Verification

Admission is complete only when the command returns `code: queued` and the read-back contains exactly one matching queue record. A Linear read without queue read-back is not sufficient.
