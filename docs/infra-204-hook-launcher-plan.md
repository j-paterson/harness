# INFRA-204 — bind lifecycle hooks to the stable launcher

Assignment `30d71c39`. Narrow scope: hook composition and the unbound
session path. No new protocol.

## Measured starting state

Before planning, the live system was inspected rather than assumed:

| fact | state |
| --- | --- |
| `max-a/b/c/d` Stop, SubagentStart, SubagentStop | already the stable launcher |
| `.hermes/intake-poll-hook.sh` | present on disk, referenced by **no** profile |
| `ACTIVE` pointer write | already atomic (staging file + `rename`) |

So two of the issue's requirements are already satisfied in the running
system, and one — the ACTIVE pointer — is already correct in code. The
generation 69→70 reversion observed today was therefore NOT a torn
write; it was a caller re-activating an older artifact (plausibly this
lead's own racing `runtime-activate`). Nothing here should "fix" that
write.

## The real defect

`hooks-install` (`cli.py`) composes its base command as:

```python
uv_binary = shutil.which("uv") or "uv"
base = (
    f"{uv_binary} run --project {settings.repo_root} "
    f"hermes-orchestrator --repo-root {settings.repo_root} …"
)
```

That is precisely the observed failure — `uv run --project
/Users/josystem/hermes-orchestrator … intake-poll` against a mutable
checkout that may not carry the command. The live settings were repaired
by hand, so the symptom is currently invisible; **re-running
`hooks-install` would rewrite every profile straight back to the broken
form.** The repair is one-shot and undone by the tool meant to maintain
it.

## The change

1. Compose the hook base from the **stable deployed launcher**
   (`<state-dir>/bin/hermes-orchestrator`), never `uv run --project
   <checkout>` and never a commit-specific runtime path. The launcher
   already resolves `runtimes/ACTIVE`, so hook settings stay valid
   across branch drift and runtime replacement without being rewritten.
2. Refuse to install if that launcher is absent, naming it — a hook
   written against a missing binary is the failure this issue exists to
   remove, and silently falling back to `uv run` would reintroduce it.
3. Retire the legacy `.hermes/intake-poll-hook.sh` **entry** wherever a
   profile still references it, leaving unrelated hooks untouched and
   preserving one canonical hook per event/profile.
4. Verify the unbound-session path: Stop, SubagentStart and
   SubagentStop must exit quietly with no durable mutation unless the
   exact Claude session is bound to an active managed cell. If any of
   the three writes a row or emits a diagnosis for an unbound session,
   make it quiet and inert.

## Tests

- the installed command is the stable launcher, and contains no
  `uv run --project` and no runtime SHA;
- installation refuses, naming the launcher, when it is absent;
- a legacy `.hermes/intake-poll-hook.sh` entry is retired while an
  unrelated hook in the same profile survives, and exactly one hook per
  event/profile remains;
- each of the three hooks, for a session bound to NO active cell, exits
  quietly and writes nothing durable;
- the same hooks for a bound session still perform their one
  continuation exactly once.

## Not in scope

The `ACTIVE` pointer write (already atomic) and the activation-race
question behind the 69→70 reversion. That is worth its own issue and is
not a hook defect.
