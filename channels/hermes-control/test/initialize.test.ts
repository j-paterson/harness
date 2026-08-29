import test from "node:test";
import assert from "node:assert/strict";
import { startFixture, initializeSidecar } from "./setup.js";

test("initialize declares only tools + experimental claude/channel capabilities", async () => {
  const fx = await startFixture();
  try {
    const resp = await initializeSidecar(fx.sidecar);
    assert.equal(resp.jsonrpc, "2.0");
    assert.equal(resp.id, 1);
    assert.ok(resp.result, "expected a result");

    const caps = resp.result.capabilities;
    assert.deepEqual(Object.keys(caps).sort(), ["experimental", "tools"]);
    assert.deepEqual(caps.tools, {});
    assert.deepEqual(Object.keys(caps.experimental), ["claude/channel"]);
    assert.deepEqual(caps.experimental["claude/channel"], {});

    // Explicitly assert none of the forbidden capability keys are present.
    for (const forbidden of ["sampling", "prompts", "resources", "roots", "permissions"]) {
      assert.equal(caps[forbidden], undefined, `should not declare ${forbidden}`);
    }

    assert.equal(resp.result.protocolVersion, "2025-06-18");
    assert.equal(resp.result.serverInfo.name, "hermes-control");
  } finally {
    await fx.teardown();
  }
});

test("initialize echoes the client's requested protocolVersion", async () => {
  const fx = await startFixture();
  try {
    fx.sidecar.send({
      jsonrpc: "2.0",
      id: 7,
      method: "initialize",
      params: { protocolVersion: "2099-01-01" },
    });
    const resp = await fx.sidecar.nextMessage((m) => m.id === 7);
    assert.equal(resp.result.protocolVersion, "2099-01-01");
  } finally {
    await fx.teardown();
  }
});

test("tools/list returns exactly one tool with the exact schema", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);

    fx.sidecar.send({ jsonrpc: "2.0", id: 2, method: "tools/list" });
    const resp = await fx.sidecar.nextMessage((m) => m.id === 2);

    assert.equal(resp.result.tools.length, 1);
    const tool = resp.result.tools[0];
    assert.equal(tool.name, "hermes_acknowledge_intake");
    assert.equal(typeof tool.description, "string");
    assert.ok(tool.description.length > 0);

    assert.deepEqual(tool.inputSchema, {
      type: "object",
      properties: {
        packet_id: { type: "string", pattern: "^[0-9a-f]{32}$" },
        event_id: { type: "string", minLength: 1, maxLength: 128 },
      },
      required: ["packet_id", "event_id"],
      additionalProperties: false,
    });
  } finally {
    await fx.teardown();
  }
});

test("ping responds with an empty object result", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    fx.sidecar.send({ jsonrpc: "2.0", id: 3, method: "ping" });
    const resp = await fx.sidecar.nextMessage((m) => m.id === 3);
    assert.deepEqual(resp.result, {});
  } finally {
    await fx.teardown();
  }
});

test("unknown method returns JSON-RPC error -32601", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    fx.sidecar.send({ jsonrpc: "2.0", id: 4, method: "not/a/real/method" });
    const resp = await fx.sidecar.nextMessage((m) => m.id === 4);
    assert.equal(resp.error.code, -32601);
  } finally {
    await fx.teardown();
  }
});

test("malformed JSON on stdin produces a JSON-RPC parse error, not a crash", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    fx.sidecar.child.stdin.write("{not valid json\n");
    const resp = await fx.sidecar.nextMessage((m) => m.error && m.error.code === -32700);
    assert.equal(resp.error.code, -32700);

    // Confirm the process is still alive and answering requests afterward.
    fx.sidecar.send({ jsonrpc: "2.0", id: 5, method: "ping" });
    const pong = await fx.sidecar.nextMessage((m) => m.id === 5);
    assert.deepEqual(pong.result, {});
  } finally {
    await fx.teardown();
  }
});
