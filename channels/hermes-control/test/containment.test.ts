import test from "node:test";
import assert from "node:assert/strict";
import { HubClient } from "../src/hub-client.js";
import { McpServer } from "../src/mcp.js";
import { FakeHub, sleep } from "./harness.js";

// These tests drive HubClient and McpServer directly, in-process,
// rather than through a spawned sidecar: they need to inject a
// throwing callback or a broken writer, which only the classes'
// constructor options make possible. Before containment was added,
// a thrown exception here would propagate out of a synchronous
// EventEmitter callback and crash this entire test process, not just
// fail one test.

test("HubClient contains an exception thrown from the onEvent callback and keeps serving later events", async () => {
  const hub = await FakeHub.create();
  let client: HubClient | null = null;
  try {
    const logs: string[] = [];
    const seen: string[] = [];

    client = new HubClient({
      socketPath: hub.socketPath,
      project: "proj-1",
      cell: "cell-1",
      session: "session-1",
      profile: "profile-1",
      generation: 1,
      capability: "a".repeat(64),
      onEvent: (kind, packetId, eventId) => {
        seen.push(eventId);
        if (eventId === "evt-boom") {
          throw new Error("boom from onEvent");
        }
      },
      onLog: (message) => logs.push(message),
    });

    client.start();

    const conn = await hub.nextConnection();
    await conn.waitForLine((m) => m.op === "register");
    conn.send({ op: "registered", proto: 1 });
    await sleep(100);

    conn.send({
      op: "event",
      event_id: "evt-boom",
      kind: "HERMES_WORK_READY",
      packet_id: "a".repeat(32),
      session_id: "session-1",
    });
    await sleep(100);

    // The throw must not have killed this process (we're still here),
    // must have been logged, and the connection must still be alive
    // and registered so later events keep flowing.
    assert.deepEqual(seen, ["evt-boom"]);
    assert.ok(
      logs.some((l) => l.includes("evt-boom")),
      `expected a log line mentioning the failing event, got: ${JSON.stringify(logs)}`
    );
    assert.equal(client.getState(), "registered");

    conn.send({
      op: "event",
      event_id: "evt-after",
      kind: "HERMES_WORK_READY",
      packet_id: "b".repeat(32),
      session_id: "session-1",
    });
    await sleep(100);

    assert.deepEqual(seen, ["evt-boom", "evt-after"]);
  } finally {
    client?.stop();
    await hub.close();
  }
});

function stubHub(): { ack: () => Promise<never> } {
  return {
    ack: () => Promise.reject(new Error("not used in these tests")),
  };
}

test("McpServer.notifyChannelEvent contains a throwing writer and keeps working for later events", () => {
  const logs: string[] = [];
  let calls = 0;
  const written: string[] = [];

  const mcp = new McpServer(
    stubHub() as any,
    (message) => logs.push(message),
    (line) => {
      calls++;
      if (calls === 1) {
        throw new Error("stdout write boom");
      }
      written.push(line);
    },
    // Drive notifyChannelEvent directly without a JSON-RPC
    // `initialize` round-trip: treat the server as already
    // initialized so notifications write immediately instead of
    // queueing.
    true
  );

  const packetId = "c".repeat(32);

  // First call: the writer throws. This must not propagate out of
  // notifyChannelEvent (it would otherwise be an uncaught exception
  // in the real process, in the middle of handling a hub event).
  mcp.notifyChannelEvent("HERMES_WORK_READY", packetId, "evt-1");
  assert.ok(logs.some((l) => l.includes("evt-1")), `expected a log line, got ${JSON.stringify(logs)}`);

  // Second call must go through normally: containment on the first
  // call did not leave the server unable to serve later events.
  mcp.notifyChannelEvent("HERMES_WORK_READY", packetId, "evt-2");
  assert.equal(written.length, 1);
  const parsed = JSON.parse(written[0]);
  assert.equal(parsed.method, "notifications/claude/channel");
  assert.equal(
    parsed.params.content,
    "Work ready · confirm and continue."
  );
  assert.equal(parsed.params.meta.event_id, "evt-2");
});

test("McpServer.notifyChannelEvent refuses malformed content: not sent, logged, and a following well-formed event still flows", () => {
  const logs: string[] = [];
  const written: string[] = [];

  const mcp = new McpServer(
    stubHub() as any,
    (message) => logs.push(message),
    (line) => written.push(line),
    // See the previous test: no JSON-RPC `initialize` round-trip
    // here, so treat the server as already initialized.
    true
  );

  // Unknown kind: the composed envelope would not match the client's
  // grammar. Must be dropped before it ever reaches stdout.
  mcp.notifyChannelEvent("NOT_A_REAL_KIND", "d".repeat(32), "evt-bad-kind");
  assert.equal(written.length, 0);
  assert.ok(logs.some((l) => l.includes("evt-bad-kind")));

  // Malformed packet id: same guard.
  mcp.notifyChannelEvent("HERMES_WORK_READY", "not-32-hex", "evt-bad-packet");
  assert.equal(written.length, 0);
  assert.ok(logs.some((l) => l.includes("evt-bad-packet")));

  // A subsequent well-formed event flows normally: the guard does not
  // leave the server stuck.
  const packetId = "e".repeat(32);
  mcp.notifyChannelEvent("HERMES_WORK_READY", packetId, "evt-good");
  assert.equal(written.length, 1);
  const parsed = JSON.parse(written[0]);
  assert.equal(
    parsed.params.content,
    "Work ready · confirm and continue."
  );
  assert.equal(parsed.params.meta.event_id, "evt-good");
});
