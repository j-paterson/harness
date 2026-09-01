# INFRA-198 — harness no-click acceptance: two observed blockers

Assignment `a3b6f90e`, instruction
`infra-198-live-harness-blocker-20260901`. Scope is exactly the two
blockers observed on the live run. No adjacent hardening, no new
protocol.

Observed run: `start-lane` created visible harness cell
`8369559d-f4cd-45ca-aa43-aae412853f16` / session
`9b539c86-c52f-43b2-b077-57491066ebcf`. The cell came up, then
no-click acceptance failed, and `--json` crashed after the effects had
already landed.

## Blocker 1 — channel trust is cell-scoped, so a new cell can never
## pass `anchor_present`

`ChannelTrustGate.evaluate` resolves its anchor with
`self._anchors.active_for_cell(cell_id)` and refuses `anchor_present`
when that returns `None` (`channel_trust.py:1007-1012`).

Measured durable state at the time of the failure:

| anchors | cell | note |
| --- | --- | --- |
| 8 rows, all of them | `b29691ef` (**development**) | 1 active, prompt bound |
| 0 rows | `8369559d` (**harness**) | the new visible cell |

The single active fully-proven anchor `e6775cab` (prompt pattern
bound) belongs to the development cell. The harness cell has no anchor
and no path to acquire one, because `capture` records a *manual* trust
event and no such event exists for it. So `anchor_present` fails on
every attempt, and retrying can never change the outcome. This is a
structural dead end, not a flake.

### Why the fix is to widen the anchor's scope, not to capture a new one

Auto-capturing an anchor for the new cell at start would forge an
operator trust decision that never happened — `capture`'s contract is
"record one anchor from a manual trust event's measured facts". That
is a genuine safety regression and is rejected.

What the operator actually trusted is a *program identity*: canonical
entry path, entry owner uid, entry sha256, dist-tree sha256, manifest
name/version, the channel entry appearing exactly once, and the launch
argv template. None of those are properties of a cell. The cell is
Hermes's own lane bookkeeping, and binding trust to it is what makes a
second visible lane permanently untrusted.

Requiring a fresh manual dialog per cell is also the exact operator
intervention INFRA-198's definition of done forbids ("without manual
`/mcp` reconnects ... or operator payload intervention").

### The change

Add ONE explicit, validated adoption operation on
`ChannelTrustAnchors` that carries a fully-proven anchor to a new cell.
It reuses `rebind`'s existing safety body verbatim rather than
inventing a second, weaker one:

- the predecessor must be `active` **and** have a bound
  `prompt_pattern` — an anchor whose trust is not fully proven is never
  carried forward;
- every measured fact is **re-measured from disk** at adoption time
  (entry path canonical + no symlinks, owner uid, entry sha256,
  dist-tree sha256, manifest) and must match;
- the new argv must equal the trusted template token for token except
  at the existing bounded session-UUID and session-scoped MCP
  config-path slots;
- the profile must match the predecessor's, or be proven as Hermes's
  own durable selection for the seat via the existing
  `_selected_replacement` path — never from caller input;
- retire-and-capture stay atomic in one transaction, failing closed on
  a concurrent adopt/retire, exactly as `capture` and `rebind` do.

The predecessor is **not** retired by adoption: the development cell
keeps its trust. Adoption copies proven material to a second cell
rather than moving it — that is the one deliberate difference from
`rebind`, which rotates a single seat.

`evaluate` is NOT given a silent fallback to a project-wide lookup. A
read path must not mint trust as a side effect; adoption is an explicit
step on the harness start path.

## Blocker 2 — `--json` crashes after the effects have landed

`cli.py:1555` does `payload = dataclasses.asdict(result)` and hands it
to `_print`, which calls `json.dumps`. `DispatchResult.session_id` is
typed `UUID | None` (`cells.py:338`), and `asdict` preserves the `UUID`
object, so `json.dumps` raises `TypeError: Object of type UUID is not
JSON serializable`.

The ordering is what makes this harmful. `cells.dispatch(...)` has
already run at line 1553: the cell exists, the seat is launched, the
durable rows are written. Only *reporting* fails. The operator sees a
traceback and a nonzero exit from a command that in fact succeeded,
and the natural response — run it again — risks a duplicate lane.

Two changes, both narrow:

1. Coerce the identifier at the payload boundary so the documented
   contract (a JSON object with `session_id`) actually holds.
2. Make `_print` fall back to `str` for values it cannot otherwise
   encode, so that a reporting bug can never again turn a completed
   effectful command into a crash. This is the failure *mode* named by
   the blocker, not adjacent scope.

## Evidence to preserve

The current harness dialog and cell `8369559d` are the live evidence
for blocker 1 and are **left untouched** until the replacement path is
ready. No workspace close, no cell retire, no anchor retire.

## Tests

- adoption succeeds for a new cell and leaves the predecessor active;
- adoption refuses a predecessor with no bound `prompt_pattern`;
- adoption refuses on any re-measured mismatch (entry sha256 stands in
  for the shared body) and on argv drift outside the bounded slots;
- `evaluate` still refuses `anchor_present` for a cell with no anchor —
  i.e. no implicit fallback was introduced;
- `start-lane --json` emits a parseable object carrying the session id
  as a string, asserted through the real CLI.

## Gate

One full `uv run pytest -q` plus `uv run ruff check src/ tests/` after
integration, then the candidate goes to Sol.
