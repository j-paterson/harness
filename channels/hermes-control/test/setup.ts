import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { FakeHub, Sidecar, baseEnv, makeCapabilityFile, spawnSidecar } from "./harness.js";

export interface TestFixture {
  hub: FakeHub;
  sidecar: Sidecar;
  session: string;
  tmpDir: string;
  capability: string;
  capabilityFile: string;
  teardown(): Promise<void>;
}

export async function startFixture(
  envOverrides: Partial<Record<string, string>> = {}
): Promise<TestFixture> {
  const hub = await FakeHub.create();
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "hermes-control-cap-"));
  const capability = "b".repeat(64);
  const capabilityFile = makeCapabilityFile(tmpDir, capability);
  const session = randomUUID();

  const env = baseEnv({
    HERMES_CONTROL_SOCKET: hub.socketPath,
    HERMES_CONTROL_PROJECT: "proj-1",
    HERMES_CONTROL_CELL: "cell-1",
    HERMES_CONTROL_SESSION: session,
    HERMES_CONTROL_PROFILE: "profile-1",
    HERMES_CONTROL_GENERATION: "3",
    HERMES_CONTROL_CAPABILITY_FILE: capabilityFile,
    ...envOverrides,
  });

  const sidecar = spawnSidecar(env);

  return {
    hub,
    sidecar,
    session,
    tmpDir,
    capability,
    capabilityFile,
    async teardown() {
      sidecar.child.kill();
      await hub.close();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    },
  };
}

export async function initializeSidecar(sidecar: Sidecar): Promise<any> {
  sidecar.send({
    jsonrpc: "2.0",
    id: 1,
    method: "initialize",
    params: { protocolVersion: "2025-06-18" },
  });
  const resp = await sidecar.nextMessage((m) => m.id === 1);
  sidecar.send({ jsonrpc: "2.0", method: "notifications/initialized" });
  return resp;
}
