// Shared validation rules for the hermes-control wire protocol (PROTOCOL.md v1)
// and the sidecar's single MCP tool.

export const PACKET_ID_RE = /^[0-9a-f]{32}$/;
export const CAPABILITY_RE = /^[0-9a-f]{64}$/;
export const MAX_LINE_BYTES = 4096;

export const EVENT_KINDS = new Set<string>([
  "HERMES_CORRECTION_READY",
  "HERMES_WORK_READY",
  "HERMES_ASSIGNMENT_READY",
]);

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value)
  );
}

export interface HubEvent {
  event_id: string;
  kind: string;
  packet_id: string;
  session_id: string;
}

/**
 * Validate a parsed hub "event" message against PROTOCOL.md's rules.
 * Returns the validated fields on success, or null if the message is a
 * protocol violation and must be treated as such (drop the connection).
 */
export function validateHubEvent(
  msg: unknown,
  expectedSessionId: string
): HubEvent | null {
  if (!isPlainObject(msg)) return null;
  if (msg["op"] !== "event") return null;

  const eventId = msg["event_id"];
  if (typeof eventId !== "string" || eventId.length < 1 || eventId.length > 128) {
    return null;
  }

  const kind = msg["kind"];
  if (typeof kind !== "string" || !EVENT_KINDS.has(kind)) {
    return null;
  }

  const packetId = msg["packet_id"];
  if (typeof packetId !== "string" || !PACKET_ID_RE.test(packetId)) {
    return null;
  }

  const sessionId = msg["session_id"];
  if (typeof sessionId !== "string" || sessionId !== expectedSessionId) {
    return null;
  }

  return { event_id: eventId, kind, packet_id: packetId, session_id: sessionId };
}

export interface AckToolArgs {
  packet_id: string;
  event_id: string;
}

/**
 * Validate arguments for the hermes_acknowledge_intake tool against its
 * declared inputSchema. Returns the validated args, or an error message
 * describing the first violation found.
 */
export function validateAckArgs(
  args: unknown
): { ok: true; value: AckToolArgs } | { ok: false; message: string } {
  if (!isPlainObject(args)) {
    return { ok: false, message: "arguments must be an object" };
  }

  const keys = Object.keys(args);
  const allowed = new Set(["packet_id", "event_id"]);
  for (const key of keys) {
    if (!allowed.has(key)) {
      return { ok: false, message: `unexpected property: ${key}` };
    }
  }

  const packetId = args["packet_id"];
  if (typeof packetId !== "string") {
    return { ok: false, message: "packet_id is required and must be a string" };
  }
  if (!PACKET_ID_RE.test(packetId)) {
    return { ok: false, message: "packet_id must be 32 lowercase hex characters" };
  }

  const eventId = args["event_id"];
  if (typeof eventId !== "string") {
    return { ok: false, message: "event_id is required and must be a string" };
  }
  if (eventId.length < 1 || eventId.length > 128) {
    return { ok: false, message: "event_id must be between 1 and 128 characters" };
  }

  return { ok: true, value: { packet_id: packetId, event_id: eventId } };
}
