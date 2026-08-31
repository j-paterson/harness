import readline from "node:readline";
import { composeNotificationContent, validateAckArgs } from "./validate.js";
import type { HubClient } from "./hub-client.js";

const SERVER_NAME = "hermes-control";
const SERVER_VERSION = "0.1.0";
const DEFAULT_PROTOCOL_VERSION = "2025-06-18";

const TOOL_NAME = "hermes_acknowledge_intake";

const TOOL_DEFINITION = {
  name: TOOL_NAME,
  description:
    "Acknowledges one exact Hermes intake event after the durable packet has been retrieved and validated.",
  inputSchema: {
    type: "object",
    properties: {
      packet_id: { type: "string", pattern: "^[0-9a-f]{32}$" },
      event_id: { type: "string", minLength: 1, maxLength: 128 },
    },
    required: ["packet_id", "event_id"],
    additionalProperties: false,
  },
};

interface JsonRpcRequest {
  jsonrpc?: unknown;
  id?: unknown;
  method?: unknown;
  params?: unknown;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// Bound on how many pre-initialize notifications are held in memory.
// The event stays durably unacknowledged on the hub either way (the
// sidecar never auto-acks), so overflowing this only delays delivery
// via the hub's own replay/resend — it never loses the event.
const MAX_QUEUED_NOTIFICATIONS = 256;

export class McpServer {
  private readonly hub: HubClient;
  private readonly onLog: (message: string) => void;
  private readonly writeLine: (line: string) => void;
  private rl: readline.Interface | null = null;
  // Whether `initialize` has been answered yet. Until it has, the
  // host has not registered its `notifications/claude/channel`
  // handler, so a notification written to stdout early is a
  // client-side ProtocolError observed live — instead it is queued
  // (see `pendingNotifications`) and flushed once initialize is
  // answered.
  private initialized: boolean;
  private readonly pendingNotifications: unknown[] = [];

  constructor(
    hub: HubClient,
    onLog: (message: string) => void,
    writeLine: (line: string) => void = (line) => {
      process.stdout.write(line);
    },
    // Not part of the wire/MCP protocol and undocumented in
    // PROTOCOL.md: lets tests that drive McpServer directly
    // in-process (containment.test.ts), without a real
    // JSON-RPC `initialize` round-trip, skip the pre-initialize
    // queue.
    startInitialized = false
  ) {
    this.hub = hub;
    this.onLog = onLog;
    this.writeLine = writeLine;
    this.initialized = startInitialized;
  }

  start(onStdioClosed: () => void): void {
    this.rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
    this.rl.on("line", (line: string) => {
      // A throw anywhere in request handling must never take the
      // whole stdio connection (and with it, the host's only wake
      // channel) down. Contain it, log it, and keep serving.
      try {
        this.handleLine(line);
      } catch (err) {
        this.safeLog(`hermes-control: internal error handling client message: ${String(err)}`);
      }
    });
    // Fail closed with the host: once the MCP stdio pipe is gone the
    // channel cannot reach Claude, so lingering would only hold a live
    // hub registration that masks the outage. Exiting drops the hub
    // socket, closes the registration, and lets pending packets fall
    // back to the Stop-hook poll. This is the ONLY exit path left in
    // the sidecar.
    this.rl.on("close", onStdioClosed);
    process.stdin.on("error", onStdioClosed);
    process.stdout.on("error", onStdioClosed);
  }

  notifyChannelEvent(kind: string, packetId: string, eventId: string): void {
    try {
      // The client contract for notifications/claude/channel requires
      // a string `content` plus optional string-valued `meta` whose
      // keys are identifiers (anything else is dropped, and a missing
      // or malformed `content` is a ProtocolError that kills the
      // whole stdio connection — a live-observed failure). Validate
      // the exact envelope grammar before ever composing the message:
      // a hub event that would violate it is logged and dropped here
      // rather than sent, and — since the sidecar never auto-acks —
      // it stays pending and replayable on the hub side rather than
      // being lost.
      const content = composeNotificationContent(kind, packetId);
      if (content === null) {
        this.safeLog(
          `hermes-control: refusing to send malformed channel notification for event ${eventId} ` +
            `(kind=${JSON.stringify(kind)}, packet_id=${JSON.stringify(packetId)}); leaving it unacknowledged`
        );
        return;
      }

      const message = {
        jsonrpc: "2.0",
        method: "notifications/claude/channel",
        params: {
          content,
          meta: {
            kind,
            packet_id: packetId,
            event_id: eventId,
          },
        },
      };

      if (!this.initialized) {
        // The host has not answered `initialize` yet, so it has not
        // registered its channel notification handler: writing now
        // would be a live-observed client-side ProtocolError. Queue
        // it instead — flushed in order, each exactly once,
        // immediately after the initialize result is written (see
        // `handleInitialize`). The event stays unacknowledged on the
        // hub in the meantime, so nothing is lost even if it never
        // gets flushed (process exit, etc.).
        this.pendingNotifications.push(message);
        if (this.pendingNotifications.length > MAX_QUEUED_NOTIFICATIONS) {
          this.pendingNotifications.shift();
          this.safeLog(
            `hermes-control: dropped oldest queued pre-initialize channel notification ` +
              `(queue exceeded ${MAX_QUEUED_NOTIFICATIONS} entries); it remains ` +
              `unacknowledged on the hub and will be replayed later`
          );
        }
        return;
      }

      this.writeMessage(message);
    } catch (err) {
      this.safeLog(
        `hermes-control: internal error sending channel notification for event ${eventId}: ${String(err)}`
      );
    }
  }

  private safeLog(message: string): void {
    try {
      this.onLog(message);
    } catch {
      // Best-effort logging only.
    }
  }

  private writeMessage(msg: unknown): void {
    this.writeLine(JSON.stringify(msg) + "\n");
  }

  private writeResult(id: unknown, result: unknown): void {
    this.writeMessage({ jsonrpc: "2.0", id, result });
  }

  private writeError(id: unknown, code: number, message: string): void {
    this.writeMessage({ jsonrpc: "2.0", id, error: { code, message } });
  }

  private handleLine(rawLine: string): void {
    const line = rawLine.trim();
    if (line.length === 0) return;

    let msg: unknown;
    try {
      msg = JSON.parse(line);
    } catch {
      this.writeError(null, -32700, "Parse error");
      return;
    }

    if (!isPlainObject(msg)) {
      this.writeError(null, -32700, "Parse error");
      return;
    }

    const req = msg as JsonRpcRequest;
    const id = "id" in req ? req.id : undefined;
    const hasId = id !== undefined;
    const method = req.method;

    if (typeof method !== "string") {
      if (hasId) this.writeError(id, -32600, "Invalid Request");
      return;
    }

    switch (method) {
      case "initialize":
        this.handleInitialize(id, req.params);
        return;

      case "notifications/initialized":
        return;

      case "ping":
        if (hasId) this.writeResult(id, {});
        return;

      case "tools/list":
        if (hasId) this.writeResult(id, { tools: [TOOL_DEFINITION] });
        return;

      case "tools/call":
        this.handleToolsCall(id, hasId, req.params);
        return;

      default:
        if (hasId) this.writeError(id, -32601, "Method not found");
        return;
    }
  }

  private handleInitialize(id: unknown, params: unknown): void {
    let protocolVersion: string = DEFAULT_PROTOCOL_VERSION;
    if (isPlainObject(params)) {
      const requested = params["protocolVersion"];
      if (typeof requested === "string" && requested.length > 0) {
        protocolVersion = requested;
      }
    }

    this.writeResult(id, {
      protocolVersion,
      capabilities: {
        tools: {},
        experimental: { "claude/channel": {} },
      },
      serverInfo: { name: SERVER_NAME, version: SERVER_VERSION },
    });

    this.initialized = true;
    this.flushPendingNotifications();
  }

  /** Writes every notification queued before `initialize` was
   * answered, in order, each exactly once. */
  private flushPendingNotifications(): void {
    const queued = this.pendingNotifications.splice(0, this.pendingNotifications.length);
    for (const message of queued) {
      this.writeMessage(message);
    }
  }

  private handleToolsCall(id: unknown, hasId: boolean, params: unknown): void {
    if (!isPlainObject(params)) {
      if (hasId) this.writeError(id, -32602, "Invalid params");
      return;
    }

    const name = params["name"];
    const args = params["arguments"];

    if (name !== TOOL_NAME) {
      if (hasId) {
        this.writeResult(id, {
          content: [{ type: "text", text: `unknown tool: ${String(name)}` }],
          isError: true,
        });
      }
      return;
    }

    const validated = validateAckArgs(args);
    if (!validated.ok) {
      if (hasId) {
        this.writeResult(id, {
          content: [{ type: "text", text: validated.message }],
          isError: true,
        });
      }
      return;
    }

    const { packet_id, event_id } = validated.value;

    this.hub
      .ack(event_id, packet_id)
      .then((result) => {
        if (!hasId) return;
        switch (result.kind) {
          case "ok":
            this.writeResult(id, {
              content: [{ type: "text", text: `acknowledged ${event_id}` }],
            });
            return;
          case "refused":
            this.writeResult(id, {
              content: [{ type: "text", text: `refused: ${result.reason}` }],
              isError: true,
            });
            return;
          case "timeout":
            this.writeResult(id, {
              content: [
                { type: "text", text: "timed out waiting for hub acknowledgment" },
              ],
              isError: true,
            });
            return;
          case "disconnected":
            this.writeResult(id, {
              content: [{ type: "text", text: "channel unavailable: not connected to hub" }],
              isError: true,
            });
            return;
        }
      })
      .catch((err: unknown) => {
        this.safeLog(`hermes-control: unexpected error during ack: ${String(err)}`);
        try {
          if (hasId) {
            this.writeResult(id, {
              content: [{ type: "text", text: "internal error while acknowledging" }],
              isError: true,
            });
          }
        } catch (writeErr) {
          this.safeLog(`hermes-control: internal error reporting ack failure: ${String(writeErr)}`);
        }
      });
  }
}
