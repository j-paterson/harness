import test from "node:test";
import assert from "node:assert/strict";
import { startFixture, initializeSidecar } from "./setup.js";
import { FakeHubConnection } from "./harness.js";

async function registerAndGetConn(fx: Awaited<ReturnType<typeof startFixture>>) {
  const conn = await fx.hub.nextConnection();
  await conn.waitForLine((m) => m.op === "register");
  conn.send({ op: "registered", proto: 1 });
  return conn;
}

test("valid event produces exactly one notification; replayed event_id is deduped", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    const conn = await registerAndGetConn(fx);

    conn.send({
      op: "event",
      event_id: "evt-1",
      kind: "HERMES_WORK_READY",
      packet_id: "a".repeat(32),
      session_id: fx.session,
    });

    const notif = await fx.sidecar.nextMessage(
      (m) => m.method === "notifications/claude/channel"
    );
    assert.equal(notif.params.channel, "hermes-control");
    assert.equal(notif.params.kind, "HERMES_WORK_READY");
    assert.equal(notif.params.packet_id, "a".repeat(32));
    assert.equal(notif.params.envelope, `HERMES_WORK_READY ${"a".repeat(32)}`);

    // Replay the same event_id: must not produce a second notification.
    conn.send({
      op: "event",
      event_id: "evt-1",
      kind: "HERMES_WORK_READY",
      packet_id: "a".repeat(32),
      session_id: fx.session,
    });

    // Prove no second notification arrives by racing a ping response, which
    // is guaranteed to come after anything already in flight.
    fx.sidecar.send({ jsonrpc: "2.0", id: 99, method: "ping" });
    await fx.sidecar.nextMessage((m) => m.id === 99);

    await assert.rejects(
      () => fx.sidecar.nextMessage((m) => m.method === "notifications/claude/channel", 300),
      /timed out/
    );
  } finally {
    await fx.teardown();
  }
});

async function expectProtocolViolation(
  fx: Awaited<ReturnType<typeof startFixture>>,
  conn: FakeHubConnection,
  send: () => void
) {
  send();
  await conn.waitForClose();

  await assert.rejects(
    () => fx.sidecar.nextMessage((m) => m.method === "notifications/claude/channel", 300),
    /timed out/
  );

  const nextConn = await fx.hub.nextConnection();
  await nextConn.waitForLine((m) => m.op === "register");
}

test("wrong session_id event: zero notifications, connection dropped, reconnect observed", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    const conn = await registerAndGetConn(fx);
    await expectProtocolViolation(fx, conn, () =>
      conn.send({
        op: "event",
        event_id: "evt-2",
        kind: "HERMES_WORK_READY",
        packet_id: "b".repeat(32),
        session_id: "not-the-right-session",
      })
    );
  } finally {
    await fx.teardown();
  }
});

test("unknown kind event: zero notifications, connection dropped, reconnect observed", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    const conn = await registerAndGetConn(fx);
    await expectProtocolViolation(fx, conn, () =>
      conn.send({
        op: "event",
        event_id: "evt-3",
        kind: "NOT_A_REAL_KIND",
        packet_id: "c".repeat(32),
        session_id: fx.session,
      })
    );
  } finally {
    await fx.teardown();
  }
});

test("malformed packet_id event: zero notifications, connection dropped, reconnect observed", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    const conn = await registerAndGetConn(fx);
    await expectProtocolViolation(fx, conn, () =>
      conn.send({
        op: "event",
        event_id: "evt-4",
        kind: "HERMES_WORK_READY",
        packet_id: "NOT-HEX",
        session_id: fx.session,
      })
    );
  } finally {
    await fx.teardown();
  }
});

test("unknown op: zero notifications, connection dropped, reconnect observed", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    const conn = await registerAndGetConn(fx);
    await expectProtocolViolation(fx, conn, () =>
      conn.send({
        op: "not_a_real_op",
        event_id: "evt-5",
      })
    );
  } finally {
    await fx.teardown();
  }
});

test(">4096 byte line: zero notifications, connection dropped, reconnect observed", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    const conn = await registerAndGetConn(fx);
    await expectProtocolViolation(fx, conn, () => {
      const filler = "x".repeat(5000);
      const oversized =
        JSON.stringify({
          op: "event",
          event_id: "evt-6",
          kind: "HERMES_WORK_READY",
          packet_id: "d".repeat(32),
          session_id: fx.session,
          filler,
        }) + "\n";
      conn.writeRaw(oversized);
    });
  } finally {
    await fx.teardown();
  }
});
