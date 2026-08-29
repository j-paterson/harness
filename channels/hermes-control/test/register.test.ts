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

test("a terminal refusal exits the sidecar with no reconnect", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);

    const conn = await fx.hub.nextConnection();
    await conn.waitForLine((m) => m.op === "register");
    conn.send({ op: "refused", reason: "stale binding generation" });

    // This process belongs to a superseded seat generation: no retry
    // can cure it, so it exits and the host surfaces the failed
    // server while the hub's durable blocked receipt carries recovery.
    const deadline = Date.now() + 3000;
    while (fx.sidecar.child.exitCode === null && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    assert.notEqual(fx.sidecar.child.exitCode, null);
    assert.equal(fx.hub.connections.length, 1);
  } finally {
    await fx.teardown();
  }
});
