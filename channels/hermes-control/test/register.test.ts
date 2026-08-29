import test from "node:test";
import assert from "node:assert/strict";
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

test("registration refused is fatal: no reconnect, ack tool returns isError", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);

    const conn = await fx.hub.nextConnection();
    await conn.waitForLine((m) => m.op === "register");
    conn.send({ op: "refused", reason: "capability mismatch" });

    // Wait a bit longer than a couple of backoff cycles would need, then assert
    // exactly one connection was ever made.
    await new Promise((resolve) => setTimeout(resolve, 1000));
    assert.equal(fx.hub.connections.length, 1);

    fx.sidecar.send({
      jsonrpc: "2.0",
      id: 10,
      method: "tools/call",
      params: {
        name: "hermes_acknowledge_intake",
        arguments: { packet_id: "a".repeat(32), event_id: "evt-1" },
      },
    });
    const resp = await fx.sidecar.nextMessage((m) => m.id === 10);
    assert.equal(resp.result.isError, true);
    assert.ok(resp.result.content[0].text.includes("channel refused"));
  } finally {
    await fx.teardown();
  }
});
