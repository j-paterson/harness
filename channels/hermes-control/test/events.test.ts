import test from "node:test";
import assert from "node:assert/strict";
import { startFixture, initializeSidecar } from "./setup.js";
import { FakeHubConnection } from "./harness.js";
import { sleep } from "./harness.js";

async function registerAndGetConn(fx: Awaited<ReturnType<typeof startFixture>>) {
  const conn = await fx.hub.nextConnection();
  await conn.waitForLine((m) => m.op === "register");
  conn.send({ op: "registered", proto: 1 });
  return conn;
}

test("valid event produces a notification with the correct envelope shape", async () => {
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
  } finally {
    await fx.teardown();
  }
});

test("distinct event ids always forward independently", async () => {
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    const conn = await registerAndGetConn(fx);

    conn.send({
      op: "event",
      event_id: "evt-a",
      kind: "HERMES_WORK_READY",
      packet_id: "a".repeat(32),
      session_id: fx.session,
    });
    const notifA = await fx.sidecar.nextMessage(
      (m) => m.method === "notifications/claude/channel"
    );
    assert.equal(notifA.params.meta.event_id, "evt-a");

    conn.send({
      op: "event",
      event_id: "evt-b",
      kind: "HERMES_WORK_READY",
      packet_id: "b".repeat(32),
      session_id: fx.session,
    });
    const notifB = await fx.sidecar.nextMessage(
      (m) => m.method === "notifications/claude/channel"
    );
    assert.equal(notifB.params.meta.event_id, "evt-b");
  } finally {
    await fx.teardown();
  }
});

test("a replayed event_id within the coalesce window forwards exactly once", async () => {
  // The hub re-sends every unacknowledged event on each registration and
  // on each publish pass (which fires per maintenance tick AND per
  // outbox commit), so at cold start the same event can arrive several
  // times in quick succession before the lead has a chance to ACK.
  // Forwarding every one of those as a separate visible notification is
  // an operator-rejected burst; the sidecar coalesces repeats of the
  // same event_id within one connection epoch inside a bounded window.
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
    assert.equal(notif1.params.meta.event_id, "evt-1");

    // Immediate re-send (well inside the default 60s coalesce window):
    // must NOT produce a second visible notification.
    sendEvent();

    await assert.rejects(
      () => fx.sidecar.nextMessage((m) => m.method === "notifications/claude/channel", 300),
      /timed out/
    );
  } finally {
    await fx.teardown();
  }
});

test("a replayed event_id forwards again once the coalesce window lapses (genuine-loss retry)", async () => {
  // A genuinely lost, still-unacknowledged event must still self-heal
  // on the next hub resend once the bounded coalescing window has
  // passed — coalescing must never turn into permanent dedup.
  const fx = await startFixture({ HERMES_CONTROL_COALESCE_MS: "150" });
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
    assert.equal(notif1.params.meta.event_id, "evt-1");

    await sleep(200);
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

test("re-registration after reconnect replays the same event_id immediately (reconnect retry)", async () => {
  // A reconnect begins a new connection epoch: the coalescing map is
  // per-epoch, so a replay following re-registration must forward
  // immediately even though the prior forward of the same event_id was
  // very recent — the coalesce window must never suppress delivery
  // across a genuine reconnect.
  const fx = await startFixture();
  try {
    await initializeSidecar(fx.sidecar);
    const first = await registerAndGetConn(fx);

    const packetId = "a".repeat(32);
    first.send({
      op: "event",
      event_id: "evt-1",
      kind: "HERMES_WORK_READY",
      packet_id: packetId,
      session_id: fx.session,
    });
    const notif1 = await fx.sidecar.nextMessage(
      (m) => m.method === "notifications/claude/channel"
    );
    assert.equal(notif1.params.meta.event_id, "evt-1");

    // Force a reconnect via a transient (non-terminal) refusal, the
    // same pattern used in register.test.ts.
    first.send({ op: "refused", reason: "no active seat binding for this cell" });

    const second = await fx.hub.nextConnection();
    await second.waitForLine((m) => m.op === "register");
    second.send({ op: "registered", proto: 1 });

    // Replay of the same event_id immediately after re-registration:
    // must forward right away, not be coalesced against the pre-reconnect
    // forward.
    second.send({
      op: "event",
      event_id: "evt-1",
      kind: "HERMES_WORK_READY",
      packet_id: packetId,
      session_id: fx.session,
    });

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
