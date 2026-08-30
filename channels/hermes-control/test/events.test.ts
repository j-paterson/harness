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

test("valid event produces a notification; a replayed event_id is re-forwarded, not deduped", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    const conn = await registerAndGetConn(fx);

    const sendEvent = () =>
      conn.send({
        op: "event",
        event_id: "evt-1",
        kind: "HERMES_WORK_READY",
        packet_id: "a".repeat(32),
        session_id: fx.session,
      });

    sendEvent();

    const notif1 = await fx.sidecar.nextMessage(
      (m) => m.method === "notifications/claude/channel"
    );
    // The client contract: a required string `content` (the bounded
    // envelope) plus identifier-keyed string meta. A missing `content`
    // is a live-proven ProtocolError that drops the whole connection.
    assert.equal(notif1.params.content, `HERMES_WORK_READY ${"a".repeat(32)}`);
    assert.deepEqual(notif1.params.meta, {
      kind: "HERMES_WORK_READY",
      packet_id: "a".repeat(32),
      event_id: "evt-1",
    });
    for (const key of Object.keys(notif1.params)) {
      assert.ok(["content", "meta"].includes(key), `unexpected param ${key}`);
    }
    for (const [key, value] of Object.entries(notif1.params.meta)) {
      assert.match(key, /^[A-Za-z0-9_]+$/);
      assert.equal(typeof value, "string");
    }

    // The hub re-sends every unacknowledged event on each registration and
    // each publish pass, by design, so that a notification dropped on the
    // client side (e.g. lost to a startup race) self-heals on the next
    // resend. The sidecar is transport only: it must forward the replay
    // again rather than suppress it by event_id.
    sendEvent();

    const notif2 = await fx.sidecar.nextMessage(
      (m) => m.method === "notifications/claude/channel"
    );
    assert.deepEqual(notif2.params.meta, notif1.params.meta);
    assert.equal(notif2.params.content, notif1.params.content);
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
