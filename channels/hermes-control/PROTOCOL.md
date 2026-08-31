# hermes-control wire protocol (version 1)

The closed control path between the Hermes channel hub (Python, Unix
domain socket server under the private state directory) and the
`hermes-control` MCP channel sidecar (Node, spawned over stdio by the
visible classic Claude session).

This channel is not a chat surface. It carries only the two bounded
intake notifications and their exact acknowledgments. The authoritative
packet never crosses this socket; the lead fetches it by id from the
durable Hermes store.

## Framing

Newline-delimited JSON, UTF-8, one message per line. A line longer
than 4096 bytes is a protocol violation: the reader closes the
connection without processing it. Unknown `op` values, non-object
messages, and messages violating any rule below are protocol
violations handled the same way. Both sides fail closed; there is no
lenient mode.

## Registration (sidecar → hub, first message)

```json
{"op": "register", "proto": 1, "project": "<project key>",
 "cell_id": "<cell id>", "session_id": "<claude session uuid>",
 "profile": "<profile alias>", "generation": <integer>,
 "capability": "<64 lowercase hex>"}
```

The hub verifies every field against durable state: the session must
be the active classic lead for that exact project/cell with the
current generation, and the capability's SHA-256 must match the
issued, unretired session capability. On success the hub answers
`{"op": "registered", "proto": 1}` and the connection stays open. On
any mismatch it answers `{"op": "refused", "reason": "<short>"}` and
closes; the reason never echoes the capability.

Only one live registration per session: a new successful registration
supersedes and closes the previous connection.

## Events (hub → sidecar, only after registration)

```json
{"op": "event", "event_id": "<opaque id, <=128 chars>",
 "kind": "HERMES_CORRECTION_READY" | "HERMES_WORK_READY",
 "packet_id": "<32 lowercase hex>",
 "session_id": "<claude session uuid>"}
```

The hub persists the event durably before writing it to the socket.
The sidecar re-validates: known kind, exact packet id shape, its own
session id. Valid events are surfaced to Claude as a
`notifications/claude/channel` notification with the client's exact
params contract — a required string `content` holding the bounded
envelope `<KIND> <packet_id>`, plus `meta` limited to the
identifier-keyed strings `kind`, `packet_id`, and `event_id` (the ids
the lead needs to fetch the durable packet and call the ACK tool).
Nothing else rides along: a malformed shape is a client-side
ProtocolError that drops the whole MCP connection (observed live),
and when that stdio pipe closes the sidecar exits so the hub sees the
channel go away instead of feeding a zombie. Invalid events are
protocol violations.

## Acknowledgment (sidecar → hub)

Sent only from the sidecar's single MCP tool, after the lead has
retrieved and validated the durable packet:

```json
{"op": "ack", "event_id": "<id>", "packet_id": "<32 hex>",
 "session_id": "<claude session uuid>"}
```

The hub replies `{"op": "ack_ok", "event_id": "<id>"}` after a
compare-and-set transition of that exact event for that exact
session, or `{"op": "ack_refused", "event_id": "<id>",
"reason": "<short>"}` when identity, state, or ids do not match.
Acknowledging an already-acknowledged event is refused; the delivery
state does not change (exactly-once effective intake).

## Replay

On every successful registration the hub re-sends, oldest first,
each of that session's events not yet acknowledged. The sidecar and
lead must treat a replayed event as the same delivery: the packet is
deduplicated by packet/event plus session identity downstream.

The hub's replay semantics above are unchanged. What the sidecar does
with a replay is a presentation-layer concern only: it presents each
still-unacknowledged event to Claude at most once per bounded window
(default 15 minutes, configurable) across any number of reconnects
and hub re-sends, and it holds any notification until the host has
answered `initialize`, flushing queued ones in order immediately
afterward.

## Sidecar configuration (environment only)

- `HERMES_CONTROL_SOCKET` — absolute path of the hub's Unix socket.
- `HERMES_CONTROL_PROJECT`, `HERMES_CONTROL_CELL`,
  `HERMES_CONTROL_SESSION`, `HERMES_CONTROL_PROFILE`,
  `HERMES_CONTROL_GENERATION` — the exact registration identity.
- `HERMES_CONTROL_CAPABILITY_FILE` — path to the mode-0600 file
  holding the 64-hex capability. The sidecar reads it at connect
  time and never writes it to argv, logs, stdout, or events.

The capability appears on the wire only inside the register message
over the private socket. Everything the sidecar prints for humans
goes to stderr; stdout belongs exclusively to MCP.
