import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { startFixture, initializeSidecar } from "./setup.js";
import { baseEnv, makeCapabilityFile, spawnSidecar, sleep } from "./harness.js";

async function registerAndGetConn(fx: Awaited<ReturnType<typeof startFixture>>) {
  const conn = await fx.hub.nextConnection();
  await conn.waitForLine((m) => m.op === "register");
  conn.send({ op: "registered", proto: 1 });
  // The sidecar flips to "registered" asynchronously as it reads the socket,
  // on an event source independent from the stdin/MCP side that is about to
  // drive tools/call. Give it a moment so the ensuing ack isn't raced against
  // that internal state transition (local Unix-socket round trips are well
  // under a millisecond in practice).
  await sleep(100);
  return conn;
}

test("tools/call happy path: ack_ok yields success result and exact ack wire line", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    const conn = await registerAndGetConn(fx);

    const packetId = "e".repeat(32);
    fx.sidecar.send({
      jsonrpc: "2.0",
      id: 20,
      method: "tools/call",
      params: {
        name: "hermes_acknowledge_intake",
        arguments: { packet_id: packetId, event_id: "evt-happy" },
      },
    });

    const ackLine = await conn.waitForLine((m) => m.op === "ack");
    assert.deepEqual(ackLine, {
      op: "ack",
      event_id: "evt-happy",
      packet_id: packetId,
      session_id: fx.session,
    });

    conn.send({ op: "ack_ok", event_id: "evt-happy" });

    const resp = await fx.sidecar.nextMessage((m) => m.id === 20);
    assert.equal(resp.result.isError, undefined);
    assert.deepEqual(resp.result.content, [{ type: "text", text: "acknowledged evt-happy" }]);
  } finally {
    await fx.teardown();
  }
});

test("tools/call with ack_refused yields isError with the reason", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    const conn = await registerAndGetConn(fx);

    fx.sidecar.send({
      jsonrpc: "2.0",
      id: 21,
      method: "tools/call",
      params: {
        name: "hermes_acknowledge_intake",
        arguments: { packet_id: "f".repeat(32), event_id: "evt-refused" },
      },
    });

    await conn.waitForLine((m) => m.op === "ack" && m.event_id === "evt-refused");
    conn.send({ op: "ack_refused", event_id: "evt-refused", reason: "already acknowledged" });

    const resp = await fx.sidecar.nextMessage((m) => m.id === 21);
    assert.equal(resp.result.isError, true);
    assert.ok(resp.result.content[0].text.includes("already acknowledged"));
  } finally {
    await fx.teardown();
  }
});

test("tools/call rejects malformed arguments without crashing (isError, no socket traffic)", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    await registerAndGetConn(fx);

    fx.sidecar.send({
      jsonrpc: "2.0",
      id: 22,
      method: "tools/call",
      params: {
        name: "hermes_acknowledge_intake",
        arguments: { packet_id: "not-hex", event_id: "evt-bad", extra: "nope" },
      },
    });

    const resp = await fx.sidecar.nextMessage((m) => m.id === 22);
    assert.equal(resp.result.isError, true);
  } finally {
    await fx.teardown();
  }
});

test("tools/call when hub is down/never registered: isError, no crash", async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "hermes-control-nohub-"));
  const capabilityFile = makeCapabilityFile(tmpDir);
  const socketPath = path.join(tmpDir, "no-such-hub.sock");

  const env = baseEnv({
    HERMES_CONTROL_SOCKET: socketPath,
    HERMES_CONTROL_PROJECT: "proj-1",
    HERMES_CONTROL_CELL: "cell-1",
    HERMES_CONTROL_SESSION: "11111111-1111-1111-1111-111111111111",
    HERMES_CONTROL_PROFILE: "profile-1",
    HERMES_CONTROL_GENERATION: "1",
    HERMES_CONTROL_CAPABILITY_FILE: capabilityFile,
  });

  const sidecar = spawnSidecar(env);
  try {
    await initializeSidecar(sidecar);

    sidecar.send({
      jsonrpc: "2.0",
      id: 23,
      method: "tools/call",
      params: {
        name: "hermes_acknowledge_intake",
        arguments: { packet_id: "1".repeat(32), event_id: "evt-nohub" },
      },
    });

    const resp = await sidecar.nextMessage((m) => m.id === 23, 3000);
    assert.equal(resp.result.isError, true);
    assert.equal(sidecar.child.exitCode, null);
  } finally {
    sidecar.child.kill();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});
