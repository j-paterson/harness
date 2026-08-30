import net from "node:net";
import { MAX_LINE_BYTES, validateHubEvent } from "./validate.js";

export interface HubClientOptions {
  socketPath: string;
  project: string;
  cell: string;
  session: string;
  profile: string;
  generation: number;
  capability: string;
  /**
   * Re-read the capability before each registration attempt, so a
   * capability reissued for a running session heals without a process
   * restart. Falls back to the static `capability` when absent or
   * when the read fails.
   */
  readCapability?: () => string;
  /**
   * Cadence for re-attempting registration while parked after a
   * terminal refusal. Defaults to 60s; overridable so tests don't
   * have to wait a full minute.
   */
  parkedRetryMs?: number;
  onEvent: (kind: string, packetId: string, eventId: string) => void;
  onLog: (message: string) => void;
}

export type AckResult =
  | { kind: "ok" }
  | { kind: "refused"; reason: string }
  | { kind: "timeout" }
  | { kind: "disconnected" };

const INITIAL_BACKOFF_MS = 250;
const MAX_BACKOFF_MS = 5000;
const ACK_TIMEOUT_MS = 10000;
const DEFAULT_PARKED_RETRY_MS = 60000;

// Refusals no single retry can cure from inside this process: the
// wire protocol itself is wrong, or this process belongs to a
// superseded seat generation. Neither can be fixed by reconnecting
// with the same identity — but the host never respawns a dead stdio
// MCP server mid-session, so this process must NOT exit either: doing
// so would permanently sever the host's only wake channel for the
// rest of the session. Instead it parks (see below) and keeps
// retrying slowly, so a later daemon fix, generation realignment, or
// capability rotation restores the channel with no manual reconnect.
// Everything else (a lagging seat binding, a capability being
// reissued, a hub mid-restart) is retried with the ordinary doubling
// backoff and a fresh capability read — the supported non-interactive
// recovery path for transient refusals.
const TERMINAL_REFUSALS = new Set<string>([
  "unsupported protocol version",
  "stale binding generation",
]);

type State = "connecting" | "registered" | "parked";

interface PendingAck {
  resolve: (result: AckResult) => void;
  timer: NodeJS.Timeout;
}

export class HubClient {
  private readonly opts: HubClientOptions;
  private socket: net.Socket | null = null;
  private state: State = "connecting";
  private backoff = INITIAL_BACKOFF_MS;
  private recvBuffer: Buffer = Buffer.alloc(0);
  private readonly seenEventIds = new Set<string>();
  private readonly pendingAcks = new Map<string, PendingAck>();
  private stopped = false;
  private reconnectTimer: NodeJS.Timeout | null = null;

  constructor(opts: HubClientOptions) {
    this.opts = opts;
  }

  start(): void {
    this.connect();
  }

  /** Stop reconnecting and tear down the socket. Used by tests that
   * construct a HubClient directly in-process (rather than inside a
   * spawned sidecar, which is simply killed) so no reconnect timer is
   * left running after the test ends. */
  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.socket) {
      this.socket.destroy();
    }
  }

  getState(): State {
    return this.state;
  }

  private connect(): void {
    if (this.stopped) return;
    const socket = net.connect(this.opts.socketPath);
    this.socket = socket;
    this.recvBuffer = Buffer.alloc(0);

    socket.on("connect", () => {
      this.sendRegister(socket);
    });

    socket.on("data", (chunk: Buffer) => {
      this.onData(chunk);
    });

    socket.on("error", (err: Error) => {
      this.safeLog(`hub-client: socket error: ${err.message}`);
    });

    socket.on("close", () => {
      this.onClose();
    });
  }

  private currentCapability(): string {
    if (this.opts.readCapability) {
      try {
        const fresh = this.opts.readCapability();
        if (fresh) return fresh;
      } catch (err) {
        this.safeLog(
          `hub-client: capability re-read failed: ${String(err)}`
        );
      }
    }
    return this.opts.capability;
  }

  private sendRegister(socket: net.Socket): void {
    const msg = {
      op: "register",
      proto: 1,
      project: this.opts.project,
      cell_id: this.opts.cell,
      session_id: this.opts.session,
      profile: this.opts.profile,
      generation: this.opts.generation,
      capability: this.currentCapability(),
    };
    socket.write(JSON.stringify(msg) + "\n");
  }

  private onData(chunk: Buffer): void {
    this.recvBuffer = Buffer.concat([this.recvBuffer, chunk]);

    for (;;) {
      const idx = this.recvBuffer.indexOf(0x0a);
      if (idx === -1) {
        if (this.recvBuffer.length > MAX_LINE_BYTES) {
          this.violate("line exceeds 4096 bytes without newline");
        }
        return;
      }

      const line = this.recvBuffer.subarray(0, idx);
      this.recvBuffer = this.recvBuffer.subarray(idx + 1);

      if (line.length > MAX_LINE_BYTES) {
        this.violate("line exceeds 4096 bytes");
        return;
      }

      if (line.length === 0) {
        continue;
      }

      if (!this.handleLine(line)) {
        this.violate("malformed or invalid hub message");
        return;
      }
    }
  }

  /** Returns false if the line is a protocol violation. */
  private handleLine(line: Buffer): boolean {
    let msg: unknown;
    try {
      msg = JSON.parse(line.toString("utf8"));
    } catch {
      return false;
    }

    if (typeof msg !== "object" || msg === null || Array.isArray(msg)) {
      return false;
    }

    const op = (msg as Record<string, unknown>)["op"];

    switch (op) {
      case "registered":
        this.state = "registered";
        this.backoff = INITIAL_BACKOFF_MS;
        return true;

      case "refused": {
        const reasonRaw = (msg as Record<string, unknown>)["reason"];
        const reason = typeof reasonRaw === "string" ? reasonRaw : "refused";
        this.safeLog(`hub-client: registration refused: ${reason}`);
        this.failAllPending({ kind: "refused", reason });
        if (TERMINAL_REFUSALS.has(reason)) {
          // Park: stay alive, keep MCP stdio fully responsive, and
          // keep retrying registration on a slow fixed cadence. A
          // second (or later) terminal refusal while already parked
          // is a no-op beyond this log line — it does not exit and
          // does not fall back to the fast backoff.
          const retryMs = this.opts.parkedRetryMs ?? DEFAULT_PARKED_RETRY_MS;
          this.safeLog(
            `hermes-control: parked after terminal refusal (${reason}); ` +
              `retrying registration every ${retryMs}ms until it clears`
          );
          this.state = "parked";
          if (this.socket) {
            this.socket.destroy();
          }
          return true;
        }
        // Transient refusal: keep reconnecting (with the ordinary
        // doubling backoff and a fresh capability read) until the
        // hub's state catches up — the supported non-interactive
        // recovery path.
        this.state = "connecting";
        if (this.socket) {
          this.socket.destroy();
        }
        return true;
      }

      case "event": {
        const validated = validateHubEvent(msg, this.opts.session);
        if (!validated) return false;
        if (!this.seenEventIds.has(validated.event_id)) {
          this.seenEventIds.add(validated.event_id);
          // onEvent ultimately drives sending a notification to the
          // host over MCP stdio: it must never be allowed to take the
          // whole process down. Contain, log, and keep the socket
          // (and the stdio channel) alive for the next event.
          try {
            this.opts.onEvent(validated.kind, validated.packet_id, validated.event_id);
          } catch (err) {
            this.safeLog(
              `hermes-control: internal error handling event ${validated.event_id}: ${String(err)}`
            );
          }
        }
        return true;
      }

      case "ack_ok": {
        const eventId = (msg as Record<string, unknown>)["event_id"];
        if (typeof eventId !== "string") return false;
        this.resolvePending(eventId, { kind: "ok" });
        return true;
      }

      case "ack_refused": {
        const eventId = (msg as Record<string, unknown>)["event_id"];
        const reasonRaw = (msg as Record<string, unknown>)["reason"];
        if (typeof eventId !== "string") return false;
        const reason = typeof reasonRaw === "string" ? reasonRaw : "refused";
        this.resolvePending(eventId, { kind: "refused", reason });
        return true;
      }

      default:
        return false;
    }
  }

  private resolvePending(eventId: string, result: AckResult): void {
    const pending = this.pendingAcks.get(eventId);
    if (!pending) return;
    clearTimeout(pending.timer);
    this.pendingAcks.delete(eventId);
    pending.resolve(result);
  }

  private failAllPending(result: AckResult): void {
    for (const [eventId, pending] of this.pendingAcks) {
      clearTimeout(pending.timer);
      pending.resolve(result);
      this.pendingAcks.delete(eventId);
    }
  }

  private violate(reason: string): void {
    this.safeLog(`hub-client: protocol violation: ${reason}`);
    if (this.socket) {
      this.socket.destroy();
    }
  }

  /**
   * Route every log line through here rather than calling
   * `this.opts.onLog` directly: a throwing logger (e.g. a broken
   * stderr pipe) must not be able to take the process down either.
   */
  private safeLog(message: string): void {
    try {
      this.opts.onLog(message);
    } catch {
      // Best-effort logging only; nothing further to do if even this
      // fails.
    }
  }

  private onClose(): void {
    this.socket = null;
    this.recvBuffer = Buffer.alloc(0);
    this.failAllPending({ kind: "disconnected" });

    if (this.stopped) {
      return;
    }

    if (this.state === "parked") {
      // Slow fixed cadence, no doubling: this keeps trying forever
      // until a daemon fix, generation realignment, or capability
      // rotation lets registration succeed again, with no manual
      // reconnect required.
      const delay = this.opts.parkedRetryMs ?? DEFAULT_PARKED_RETRY_MS;
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = null;
        this.connect();
      }, delay);
      return;
    }

    const delay = this.backoff;
    this.backoff = Math.min(this.backoff * 2, MAX_BACKOFF_MS);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  ack(eventId: string, packetId: string): Promise<AckResult> {
    if (this.state !== "registered" || !this.socket) {
      return Promise.resolve({ kind: "disconnected" });
    }

    const socket = this.socket;
    return new Promise<AckResult>((resolve) => {
      const timer = setTimeout(() => {
        this.pendingAcks.delete(eventId);
        resolve({ kind: "timeout" });
      }, ACK_TIMEOUT_MS);

      this.pendingAcks.set(eventId, { resolve, timer });

      const msg = {
        op: "ack",
        event_id: eventId,
        packet_id: packetId,
        session_id: this.opts.session,
      };
      socket.write(JSON.stringify(msg) + "\n");
    });
  }
}
