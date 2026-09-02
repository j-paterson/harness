import fs from "node:fs";
import { CAPABILITY_RE } from "./validate.js";
import { HubClient } from "./hub-client.js";
import { McpServer } from "./mcp.js";

function fail(message: string): never {
  process.stderr.write(`hermes-control: ${message}\n`);
  process.exit(1);
}

function logContained(prefix: string, err: unknown): void {
  try {
    const detail = err instanceof Error ? err.stack ?? err.message : String(err);
    process.stderr.write(`hermes-control: ${prefix}: ${detail}\n`);
  } catch {
    // Best-effort logging only; if stderr itself is broken there is
    // nothing further we can do here.
  }
}

// The host is the sidecar's only wake channel and it never respawns a
// dead stdio MCP server mid-session: any uncaught exception here would
// permanently sever that channel for the rest of the session. These
// are a last-resort backstop behind the per-call containment in
// hub-client.ts and mcp.ts — contain, log, and keep serving. The only
// path that may still exit the process once running is the MCP stdio
// "close" handler below (the host itself is gone, so there is nothing
// left to keep serving for).
process.on("uncaughtException", (err) => {
  logContained("uncaught exception (contained)", err);
});
process.on("unhandledRejection", (reason) => {
  logContained("unhandled rejection (contained)", reason);
});

function optionalPositiveInteger(name: string, defaultValue: number): number {
  const value = process.env[name];
  if (typeof value !== "string" || value.length === 0) {
    return defaultValue;
  }
  if (!/^\d+$/.test(value) || Number.parseInt(value, 10) <= 0) {
    fail(`environment variable ${name} must be a positive integer`);
  }
  return Number.parseInt(value, 10);
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
  // Not part of the documented sidecar configuration (PROTOCOL.md):
  // an internal knob so tests don't have to wait a full minute for
  // the parked retry cadence. Absent in normal operation, where the
  // 60s default applies.
  const parkedRetryMs = optionalPositiveInteger("HERMES_CONTROL_PARK_RETRY_MS", 60000);
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
    parkedRetryMs,
    readCapability: () => {
      // A reissued capability heals a running session on the next
      // registration attempt; a failed re-read falls back to the
      // startup value rather than killing the channel.
      const raw = fs.readFileSync(capabilityFile, "utf8").trim();
      return CAPABILITY_RE.test(raw) ? raw : "";
    },
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
