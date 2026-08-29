import fs from "node:fs";
import { CAPABILITY_RE } from "./validate.js";
import { HubClient } from "./hub-client.js";
import { McpServer } from "./mcp.js";

function fail(message: string): never {
  process.stderr.write(`hermes-control: ${message}\n`);
  process.exit(1);
}

function requireNonEmpty(name: string): string {
  const value = process.env[name];
  if (typeof value !== "string" || value.length === 0) {
    fail(`missing required environment variable ${name}`);
  }
  return value as string;
}

function requireAbsolutePath(name: string): string {
  const value = requireNonEmpty(name);
  if (!value.startsWith("/")) {
    fail(`environment variable ${name} must be an absolute path`);
  }
  return value;
}

function requireInteger(name: string): number {
  const value = requireNonEmpty(name);
  if (!/^-?\d+$/.test(value)) {
    fail(`environment variable ${name} must be an integer`);
  }
  return Number.parseInt(value, 10);
}

function readCapability(path: string): string {
  let raw: string;
  try {
    raw = fs.readFileSync(path, "utf8");
  } catch (err) {
    fail(`unable to read capability file ${path}: ${String(err)}`);
  }
  const capability = raw.trim();
  if (!CAPABILITY_RE.test(capability)) {
    fail(`capability file ${path} does not contain 64 lowercase hex characters`);
  }
  return capability;
}

function main(): void {
  const socketPath = requireAbsolutePath("HERMES_CONTROL_SOCKET");
  const project = requireNonEmpty("HERMES_CONTROL_PROJECT");
  const cell = requireNonEmpty("HERMES_CONTROL_CELL");
  const session = requireNonEmpty("HERMES_CONTROL_SESSION");
  const profile = requireNonEmpty("HERMES_CONTROL_PROFILE");
  const generation = requireInteger("HERMES_CONTROL_GENERATION");
  const capabilityFile = requireAbsolutePath("HERMES_CONTROL_CAPABILITY_FILE");
  const capability = readCapability(capabilityFile);

  const onLog = (message: string): void => {
    process.stderr.write(`${message}\n`);
  };

  let mcp: McpServer | null = null;

  const hub = new HubClient({
    socketPath,
    project,
    cell,
    session,
    profile,
    generation,
    capability,
    onEvent: (kind, packetId, eventId) => {
      mcp?.notifyChannelEvent(kind, packetId, eventId);
    },
    onLog,
  });

  mcp = new McpServer(hub, onLog);

  hub.start();
  mcp.start(() => {
    onLog("hermes-control: MCP stdio closed; exiting so the hub sees the channel go away");
    process.exit(1);
  });
}

main();
