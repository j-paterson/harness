import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { baseEnv, makeCapabilityFile, spawnSidecar } from "./harness.js";

function fullValidEnv(tmpDir: string): NodeJS.ProcessEnv {
  const capabilityFile = makeCapabilityFile(tmpDir);
  return baseEnv({
    HERMES_CONTROL_SOCKET: path.join(tmpDir, "hub.sock"),
    HERMES_CONTROL_PROJECT: "proj-1",
    HERMES_CONTROL_CELL: "cell-1",
    HERMES_CONTROL_SESSION: "11111111-1111-1111-1111-111111111111",
    HERMES_CONTROL_PROFILE: "profile-1",
    HERMES_CONTROL_GENERATION: "1",
    HERMES_CONTROL_CAPABILITY_FILE: capabilityFile,
  });
}

test("missing required env var: exits nonzero before answering initialize", async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "hermes-control-missing-env-"));
  try {
    const env = fullValidEnv(tmpDir);
    delete env.HERMES_CONTROL_SESSION;

    const sidecar = spawnSidecar(env);
    try {
      sidecar.send({
        jsonrpc: "2.0",
        id: 1,
        method: "initialize",
        params: { protocolVersion: "2025-06-18" },
      });

      const exitCode = await sidecar.waitForExit(3000);
      assert.notEqual(exitCode, 0);

      await assert.rejects(
        () => sidecar.nextMessage((m) => m.id === 1, 200),
        /timed out/
      );

      const stderrText = sidecar.stderr.join("");
      assert.ok(stderrText.length > 0, "expected a stderr diagnostic message");
    } finally {
      sidecar.child.kill();
    }
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test("malformed capability file: exits nonzero before answering initialize", async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "hermes-control-bad-cap-"));
  try {
    const capabilityFile = path.join(tmpDir, "capability");
    fs.writeFileSync(capabilityFile, "not-64-hex\n", { mode: 0o600 });

    const env = baseEnv({
      HERMES_CONTROL_SOCKET: path.join(tmpDir, "hub.sock"),
      HERMES_CONTROL_PROJECT: "proj-1",
      HERMES_CONTROL_CELL: "cell-1",
      HERMES_CONTROL_SESSION: "11111111-1111-1111-1111-111111111111",
      HERMES_CONTROL_PROFILE: "profile-1",
      HERMES_CONTROL_GENERATION: "1",
      HERMES_CONTROL_CAPABILITY_FILE: capabilityFile,
    });

    const sidecar = spawnSidecar(env);
    try {
      sidecar.send({
        jsonrpc: "2.0",
        id: 1,
        method: "initialize",
        params: { protocolVersion: "2025-06-18" },
      });

      const exitCode = await sidecar.waitForExit(3000);
      assert.notEqual(exitCode, 0);
    } finally {
      sidecar.child.kill();
    }
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test("missing generation env var: exits nonzero before answering initialize", async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "hermes-control-bad-gen-"));
  try {
    const env = fullValidEnv(tmpDir);
    env.HERMES_CONTROL_GENERATION = "not-an-integer";

    const sidecar = spawnSidecar(env);
    try {
      const exitCode = await sidecar.waitForExit(3000);
      assert.notEqual(exitCode, 0);
    } finally {
      sidecar.child.kill();
    }
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test("MCP stdio close: the sidecar exits and the hub sees the socket drop", async () => {
  const { startFixture, initializeSidecar } = await import("./setup.js");
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    const conn = await fx.hub.nextConnection();
    await conn.waitForLine((m: Record<string, unknown>) => m.op === "register");
    conn.send({ op: "registered", proto: 1 });

    const exited = new Promise<number | null>((resolve) => {
      fx.sidecar.child.once("exit", (code: number | null) => resolve(code));
    });
    fx.sidecar.child.stdin!.end();

    // Fail closed with the host: no lingering zombie holding a live
    // hub registration once Claude's side of the pipe is gone.
    const code = await exited;
    assert.notEqual(code, 0);
    await conn.waitForClose();
  } finally {
    await fx.teardown();
  }
});
