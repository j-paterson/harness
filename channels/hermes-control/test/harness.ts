import { spawn, ChildProcessWithoutNullStreams } from "node:child_process";
import net from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import readline from "node:readline";

export const MAIN_JS = path.resolve(import.meta.dirname, "..", "src", "main.js");

export interface FakeHubConnection {
  socket: net.Socket;
  lines: string[];
  waitForLine(predicate: (msg: any) => boolean, timeoutMs?: number): Promise<any>;
  send(msg: unknown): void;
  writeRaw(data: string): void;
  waitForClose(timeoutMs?: number): Promise<void>;
}

export class FakeHub {
  readonly socketPath: string;
  readonly server: net.Server;
  readonly connections: FakeHubConnection[] = [];
  private connectionWaiters: Array<(conn: FakeHubConnection) => void> = [];
  private consumedCount = 0;

  private constructor(socketPath: string, server: net.Server) {
    this.socketPath = socketPath;
    this.server = server;
  }

  static async create(): Promise<FakeHub> {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "hermes-control-test-"));
    const socketPath = path.join(dir, "hub.sock");
    const server = net.createServer();
    const hub = new FakeHub(socketPath, server);

    server.on("connection", (socket) => {
      let closed = false;
      let closeResolve: (() => void) | null = null;
      const closedPromise = new Promise<void>((resolve) => {
        closeResolve = resolve;
      });
      socket.on("close", () => {
        closed = true;
        closeResolve?.();
      });

      const conn: FakeHubConnection = {
        socket,
        lines: [],
        waitForLine: (predicate, timeoutMs = 5000) =>
          hub.waitForLineOn(conn, predicate, timeoutMs),
        send: (msg) => {
          socket.write(JSON.stringify(msg) + "\n");
        },
        writeRaw: (data) => {
          socket.write(data);
        },
        waitForClose: (timeoutMs = 5000) => {
          if (closed) return Promise.resolve();
          return Promise.race([
            closedPromise,
            new Promise<void>((_, reject) => {
              setTimeout(() => reject(new Error("timed out waiting for connection close")), timeoutMs);
            }),
          ]);
        },
      };

      let buffer = "";
      socket.on("data", (chunk) => {
        buffer += chunk.toString("utf8");
        for (;;) {
          const idx = buffer.indexOf("\n");
          if (idx === -1) break;
          const line = buffer.slice(0, idx);
          buffer = buffer.slice(idx + 1);
          if (line.length > 0) {
            conn.lines.push(line);
          }
        }
      });

      hub.connections.push(conn);
      if (hub.connectionWaiters.length > 0) {
        const waiter = hub.connectionWaiters.shift()!;
        hub.consumedCount = hub.connections.length;
        waiter(conn);
      }
    });

    await new Promise<void>((resolve) => {
      server.listen(socketPath, resolve);
    });

    return hub;
  }

  async nextConnection(timeoutMs = 5000): Promise<FakeHubConnection> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error("timed out waiting for a hub connection"));
      }, timeoutMs);
      this.connectionWaiters.push((conn) => {
        clearTimeout(timer);
        resolve(conn);
      });
    });
  }

  private async waitForLineOn(
    conn: FakeHubConnection,
    predicate: (msg: any) => boolean,
    timeoutMs: number
  ): Promise<any> {
    const deadline = Date.now() + timeoutMs;
    let idx = 0;
    for (;;) {
      while (idx < conn.lines.length) {
        const raw = conn.lines[idx];
        idx++;
        let parsed: any;
        try {
          parsed = JSON.parse(raw);
        } catch {
          continue;
        }
        if (predicate(parsed)) return parsed;
      }
      if (Date.now() > deadline) {
        throw new Error("timed out waiting for expected line from sidecar");
      }
      await sleep(20);
    }
  }

  async close(): Promise<void> {
    for (const conn of this.connections) {
      conn.socket.destroy();
    }
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
    fs.rmSync(path.dirname(this.socketPath), { recursive: true, force: true });
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export interface TestEnv {
  socketPath: string;
  capabilityFile: string;
  capability: string;
  session: string;
  project: string;
  cell: string;
  profile: string;
  generation: number;
}

export function makeCapabilityFile(dir: string, capability?: string): string {
  const cap = capability ?? "a".repeat(64);
  const file = path.join(dir, "capability");
  fs.writeFileSync(file, cap + "\n", { mode: 0o600 });
  return file;
}

export function baseEnv(overrides: Partial<Record<string, string>> = {}): NodeJS.ProcessEnv {
  return {
    PATH: process.env.PATH,
    ...overrides,
  };
}

export interface Sidecar {
  child: ChildProcessWithoutNullStreams;
  rl: readline.Interface;
  stderr: string[];
  send(msg: unknown): void;
  nextMessage(predicate?: (msg: any) => boolean, timeoutMs?: number): Promise<any>;
  waitForExit(timeoutMs?: number): Promise<number | null>;
}

export function spawnSidecar(env: NodeJS.ProcessEnv): Sidecar {
  const child = spawn(process.execPath, [MAIN_JS], {
    env,
    stdio: ["pipe", "pipe", "pipe"],
  });

  const stderr: string[] = [];
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk: string) => {
    stderr.push(chunk);
  });

  const rl = readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
  const messageQueue: any[] = [];
  const waiters: Array<{
    predicate?: (msg: any) => boolean;
    resolve: (msg: any) => void;
  }> = [];

  rl.on("line", (line: string) => {
    let parsed: any;
    try {
      parsed = JSON.parse(line);
    } catch {
      return;
    }
    for (let i = 0; i < waiters.length; i++) {
      const w = waiters[i];
      if (!w.predicate || w.predicate(parsed)) {
        waiters.splice(i, 1);
        w.resolve(parsed);
        return;
      }
    }
    messageQueue.push(parsed);
  });

  function tryDrainQueue(predicate?: (msg: any) => boolean): any | undefined {
    for (let i = 0; i < messageQueue.length; i++) {
      const msg = messageQueue[i];
      if (!predicate || predicate(msg)) {
        messageQueue.splice(i, 1);
        return msg;
      }
    }
    return undefined;
  }

  return {
    child,
    rl,
    stderr,
    send(msg: unknown) {
      child.stdin.write(JSON.stringify(msg) + "\n");
    },
    nextMessage(predicate?: (msg: any) => boolean, timeoutMs = 5000): Promise<any> {
      const found = tryDrainQueue(predicate);
      if (found !== undefined) return Promise.resolve(found);
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          const idx = waiters.findIndex((w) => w.resolve === resolveWrapped);
          if (idx !== -1) waiters.splice(idx, 1);
          reject(new Error("timed out waiting for MCP message"));
        }, timeoutMs);
        const resolveWrapped = (msg: any) => {
          clearTimeout(timer);
          resolve(msg);
        };
        waiters.push({ predicate, resolve: resolveWrapped });
      });
    },
    waitForExit(timeoutMs = 5000): Promise<number | null> {
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("process did not exit")), timeoutMs);
        child.on("exit", (code) => {
          clearTimeout(timer);
          resolve(code);
        });
      });
    },
  };
}
