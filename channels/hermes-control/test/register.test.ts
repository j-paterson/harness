import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { startFixture, initializeSidecar } from "./setup.js";

test("first socket message is a correct register line; capability never on stdout", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);

    const conn = await fx.hub.nextConnection();
    const registerMsg = await conn.waitForLine((m) => m.op === "register");

    assert.equal(registerMsg.op, "register");
    assert.equal(registerMsg.proto, 1);
    assert.equal(registerMsg.project, "proj-1");
    assert.equal(registerMsg.cell_id, "cell-1");
    assert.equal(registerMsg.session_id, fx.session);
    assert.equal(registerMsg.profile, "profile-1");
    assert.equal(registerMsg.generation, 3);
    assert.equal(registerMsg.capability, fx.capability);

    // It must be the very first line the fake hub ever saw on this connection.
    assert.equal(conn.lines.length >= 1, true);
    assert.equal(JSON.parse(conn.lines[0]).op, "register");

    // Drive some MCP traffic and make sure the capability is never echoed on stdout.
    fx.sidecar.send({ jsonrpc: "2.0", id: 9, method: "tools/list" });
    await fx.sidecar.nextMessage((m) => m.id === 9);

    conn.send({ op: "registered", proto: 1 });

    for (const chunk of fx.sidecar.stderr) {
      assert.equal(chunk.includes(fx.capability), false, "capability leaked to stderr");
    }
  } finally {
    await fx.teardown();
  }
});

test("a transient refusal retries registration with a fresh capability read", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);

    const first = await fx.hub.nextConnection();
    await first.waitForLine((m) => m.op === "register");
    // The hub's binding state is lagging (e.g. the seat row lands a
    // moment after the sidecar spawns): a capability reissued while
    // the sidecar retries must be picked up without any restart.
    const reissued = "c".repeat(64);
    fs.writeFileSync(fx.capabilityFile, reissued);
    first.send({ op: "refused", reason: "no active seat binding for this cell" });

    const second = await fx.hub.nextConnection();
    const reregister = await second.waitForLine((m) => m.op === "register");
    assert.equal(reregister.capability, reissued);
    second.send({ op: "registered", proto: 1 });
  } finally {
    await fx.teardown();
  }
});

test("a terminal refusal parks instead of exiting: MCP stdio stays alive and registration keeps retrying on the slow cadence", async () => {
  // The host never respawns a dead stdio MCP server mid-session, so an
  // exit here would permanently sever the channel for the rest of the
  // session. A terminal refusal must park instead: stay alive, keep
  // stdio fully responsive, and keep retrying registration.
  const fx = await startFixture({ HERMES_CONTROL_PARK_RETRY_MS: "50" });
  try {
    await initializeSidecar(fx.sidecar);

    const conn = await fx.hub.nextConnection();
    await conn.waitForLine((m) => m.op === "register");
    conn.send({ op: "refused", reason: "stale binding generation" });

    // The MCP server must remain fully responsive while parked.
    fx.sidecar.send({ jsonrpc: "2.0", id: 501, method: "ping" });
    const pong = await fx.sidecar.nextMessage((m) => m.id === 501);
    assert.deepEqual(pong.result, {});
    assert.equal(fx.sidecar.child.exitCode, null);

    // It keeps retrying registration on the slow cadence with no
    // manual reconnect, re-reading the capability each attempt.
    const second = await fx.hub.nextConnection();
    const reregister = await second.waitForLine((m) => m.op === "register");
    assert.equal(reregister.capability, fx.capability);

    assert.equal(fx.sidecar.child.exitCode, null);

    const stderrText = fx.sidecar.stderr.join("");
    assert.match(stderrText, /park/i);
  } finally {
    await fx.teardown();
  }
});

test("a second terminal refusal while parked stays parked: no exit, no fast-backoff storm", async () => {
  const fx = await startFixture({ HERMES_CONTROL_PARK_RETRY_MS: "80" });
  try {
    await initializeSidecar(fx.sidecar);

    const first = await fx.hub.nextConnection();
    await first.waitForLine((m) => m.op === "register");
    first.send({ op: "refused", reason: "stale binding generation" });

    const second = await fx.hub.nextConnection();
    await second.waitForLine((m) => m.op === "register");
    second.send({ op: "refused", reason: "unsupported protocol version" });

    // Wait several multiples of the slow cadence. A fast-backoff storm
    // (the 250ms-doubling reconnect path) would produce far more
    // connections in this window than the slow cadence would.
    await new Promise((resolve) => setTimeout(resolve, 80 * 4));
    assert.ok(
      fx.hub.connections.length <= 5,
      `expected a slow trickle of reconnects while parked, got ${fx.hub.connections.length}`
    );
    assert.equal(fx.sidecar.child.exitCode, null);
  } finally {
    await fx.teardown();
  }
});

test("after parking, a subsequent successful registration restores normal event flow", async () => {
  const fx = await startFixture({ HERMES_CONTROL_PARK_RETRY_MS: "50" });
  try {
    await initializeSidecar(fx.sidecar);

    const first = await fx.hub.nextConnection();
    await first.waitForLine((m) => m.op === "register");
    first.send({ op: "refused", reason: "stale binding generation" });

    const second = await fx.hub.nextConnection();
    await second.waitForLine((m) => m.op === "register");
    second.send({ op: "registered", proto: 1 });

    const packetId = "a".repeat(32);
    second.send({
      op: "event",
      event_id: "evt-park-1",
      kind: "HERMES_WORK_READY",
      packet_id: packetId,
      session_id: fx.session,
    });

    const notif = await fx.sidecar.nextMessage(
      (m) => m.method === "notifications/claude/channel"
    );
    assert.equal(
      notif.params.content,
      `Hermes: work is ready to continue. Retrieve and confirm packet ${packetId}, then proceed.`
    );

    fx.sidecar.send({
      jsonrpc: "2.0",
      id: 502,
      method: "tools/call",
      params: {
        name: "hermes_acknowledge_intake",
        arguments: { packet_id: packetId, event_id: "evt-park-1" },
      },
    });

    const ackLine = await second.waitForLine((m) => m.op === "ack");
    assert.deepEqual(ackLine, {
      op: "ack",
      event_id: "evt-park-1",
      packet_id: packetId,
      session_id: fx.session,
    });
    second.send({ op: "ack_ok", event_id: "evt-park-1" });

    const resp = await fx.sidecar.nextMessage((m) => m.id === 502);
    assert.equal(resp.result.isError, undefined);
    assert.deepEqual(resp.result.content, [
      { type: "text", text: "acknowledged evt-park-1" },
    ]);
  } finally {
    await fx.teardown();
  }
});
