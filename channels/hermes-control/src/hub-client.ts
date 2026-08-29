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

type State = "connecting" | "registered" | "refused";

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
      this.opts.onLog(`hub-client: socket error: ${err.message}`);
    });

    socket.on("close", () => {
      this.onClose();
    });
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
      capability: this.opts.capability,
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
        this.state = "refused";
        this.opts.onLog(`hub-client: registration refused: ${reason}`);
        this.failAllPending({ kind: "refused", reason });
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
          this.opts.onEvent(validated.kind, validated.packet_id, validated.event_id);
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
    this.opts.onLog(`hub-client: protocol violation: ${reason}`);
    if (this.socket) {
      this.socket.destroy();
    }
  }

  private onClose(): void {
    this.socket = null;
    this.recvBuffer = Buffer.alloc(0);
    this.failAllPending({ kind: "disconnected" });

    if (this.stopped || this.state === "refused") {
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
    if (this.state === "refused") {
      return Promise.resolve({ kind: "refused", reason: "channel refused" });
    }
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
