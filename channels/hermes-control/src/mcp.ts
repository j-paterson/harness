import readline from "node:readline";
import { validateAckArgs } from "./validate.js";
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

export class McpServer {
  private readonly hub: HubClient;
  private readonly onLog: (message: string) => void;
  private rl: readline.Interface | null = null;

  constructor(hub: HubClient, onLog: (message: string) => void) {
    this.hub = hub;
    this.onLog = onLog;
  }

  start(): void {
    this.rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
    this.rl.on("line", (line: string) => {
      this.handleLine(line);
    });
  }

  notifyChannelEvent(kind: string, packetId: string): void {
    this.writeMessage({
      jsonrpc: "2.0",
      method: "notifications/claude/channel",
      params: {
        channel: "hermes-control",
        kind,
        packet_id: packetId,
        envelope: `${kind} ${packetId}`,
      },
    });
  }

  private writeMessage(msg: unknown): void {
    process.stdout.write(JSON.stringify(msg) + "\n");
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
        this.onLog(`mcp: unexpected error during ack: ${String(err)}`);
        if (hasId) {
          this.writeResult(id, {
            content: [{ type: "text", text: "internal error while acknowledging" }],
            isError: true,
          });
        }
      });
  }
}
