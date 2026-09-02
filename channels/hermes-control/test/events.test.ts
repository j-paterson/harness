import test from "node:test";
import assert from "node:assert/strict";
import { startFixture, initializeSidecar } from "./setup.js";
import { FakeHubConnection } from "./harness.js";
import { sleep } from "./harness.js";
import { composeNotificationContent } from "../src/validate.js";

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
    assert.equal(
      notif1.params.content,
      `Hermes: work is ready to continue. Retrieve and confirm packet ${"a".repeat(32)}, then proceed.`
    );
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

test("every visible event kind uses concise action-oriented text", () => {
  const packet = "a".repeat(32);
  assert.equal(
    composeNotificationContent("HERMES_ASSIGNMENT_READY", packet),
    `Hermes: a new assignment is ready. Retrieve and confirm packet ${packet}, then begin it.`
  );
  assert.equal(
    composeNotificationContent("HERMES_CORRECTION_READY", packet),
    `Hermes: Sol returned corrections. Retrieve and confirm packet ${packet}, then resume work.`
  );
  assert.equal(
    composeNotificationContent("HERMES_WORK_READY", packet),
    `Hermes: work is ready to continue. Retrieve and confirm packet ${packet}, then proceed.`
  );
  assert.equal(
    composeNotificationContent("HERMES_CONTROL_READY", packet),
    `Hermes: a control update needs attention. Retrieve and confirm packet ${packet}.`
  );
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

test("an event delivered before initialize is queued and flushed exactly once after initialize; a within-window resend is not forwarded again", async () => {
  // Hub registration is independent of MCP `initialize`: the sidecar
  // connects to the hub at startup regardless of the JSON-RPC
  // handshake state. Register and deliver an event first, without
  // ever calling initializeSidecar, to prove the notification is
  // held rather than written (or dropped).
  const fx = await startFixture();
  try {
    const conn = await registerAndGetConn(fx);

    const packetId = "a".repeat(32);
    conn.send({
      op: "event",
      event_id: "evt-1",
      kind: "HERMES_WORK_READY",
      packet_id: packetId,
      session_id: fx.session,
    });

    // Not yet answered `initialize`: nothing should reach stdout yet.
    await assert.rejects(
      () => fx.sidecar.nextMessage((m) => m.method === "notifications/claude/channel", 300),
      /timed out/
    );

    await initializeSidecar(fx.sidecar);

    const notif1 = await fx.sidecar.nextMessage(
      (m) => m.method === "notifications/claude/channel"
    );
    assert.equal(
      notif1.params.content,
      `Hermes: work is ready to continue. Retrieve and confirm packet ${packetId}, then proceed.`
    );
    assert.deepEqual(notif1.params.meta, {
      kind: "HERMES_WORK_READY",
      packet_id: packetId,
      event_id: "evt-1",
    });

    // A resend of the same event_id in this sidecar process must
    // not be forwarded again.
    conn.send({
      op: "event",
      event_id: "evt-1",
      kind: "HERMES_WORK_READY",
      packet_id: packetId,
      session_id: fx.session,
    });
    await assert.rejects(
      () => fx.sidecar.nextMessage((m) => m.method === "notifications/claude/channel", 300),
      /timed out/
    );
  } finally {
    await fx.teardown();
  }
});

test("a replayed event_id forwards exactly once per sidecar process", async () => {
  // The hub re-sends every unacknowledged event on each registration and
  // on each publish pass (which fires per maintenance tick AND per
  // outbox commit), so at cold start the same event can arrive several
  // times in quick succession before the lead has a chance to ACK.
  // Forwarding every one of those as a separate visible notification is
  // an operator-rejected burst; the sidecar coalesces repeats of the
  // same event_id for the sidecar process lifetime.
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

    // Immediate re-send in the same sidecar process:
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

test("a replayed event_id stays quiet for the sidecar process lifetime", async () => {
  // The MCP server queues notifications until Claude initializes, so
  // repeatedly surfacing the same event cannot heal a loss. Recovery is
  // a sidecar restart, which gets a fresh process-local set and hub replay.
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

    await sleep(200);
    sendEvent();

    await assert.rejects(
      () => fx.sidecar.nextMessage((m) => m.method === "notifications/claude/channel", 300),
      /timed out/
    );
  } finally {
    await fx.teardown();
  }
});

test("re-registration does not re-surface an event already shown by this sidecar, but a new event forwards immediately", async () => {
  // forwarded is process-scoped (not cleared on registration):
  // a reconnect must not cause a still-unacknowledged event to
  // re-surface. A brand-new
  // event_id on the new connection is unaffected and still forwards
  // right away.
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

    // Replay of the SAME event_id right after re-registration: still
    // in the same sidecar process, so it must NOT produce a second
    // visible notification.
    second.send({
      op: "event",
      event_id: "evt-1",
      kind: "HERMES_WORK_READY",
      packet_id: packetId,
      session_id: fx.session,
    });
    await assert.rejects(
      () => fx.sidecar.nextMessage((m) => m.method === "notifications/claude/channel", 300),
      /timed out/
    );

    // A NEW event_id on the same (new) connection still forwards
    // immediately: coalescing never blocks a genuinely new event.
    second.send({
      op: "event",
      event_id: "evt-2",
      kind: "HERMES_WORK_READY",
      packet_id: "b".repeat(32),
      session_id: fx.session,
    });
    const notif2 = await fx.sidecar.nextMessage(
      (m) => m.method === "notifications/claude/channel"
    );
    assert.equal(notif2.params.meta.event_id, "evt-2");
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
