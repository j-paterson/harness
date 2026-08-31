"""Durable fakechat port ownership and the fail-closed loopback sender."""

from __future__ import annotations

import re
import socket
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.fakechat_signal import (
    FakechatSignalPort,
    FakechatSignalPorts,
    FakechatSignalSender,
)
from hermes_orchestrator.lead_intake import CORRECTION_READY, WORK_READY

CELL = "c" * 8
BINDING = "b" * 8
WAKE_ID = "a" * 32
DELIVERY_ID = "d" * 32
NOW = datetime(2026, 8, 29, 12, 0, tzinfo=UTC)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def ports(database: Database) -> FakechatSignalPorts:
    return FakechatSignalPorts(database=database, now=lambda: NOW)


def _probe_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _bindable(port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", port))
    except OSError:
        return False
    return True


class _CountingHandler(BaseHTTPRequestHandler):
    """A sentinel server: any request it receives is a test failure."""

    def do_POST(self) -> None:
        self.server.count += 1  # type: ignore[attr-defined]
        self.send_response(204)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


def _parse_multipart(body: bytes, content_type: str) -> dict[str, str]:
    boundary = content_type.split("boundary=", 1)[1].encode()
    delimiter = b"--" + boundary
    fields: dict[str, str] = {}
    for part in body.split(delimiter):
        part = part.strip(b"\r\n")
        if not part or part == b"--":
            continue
        header_blob, _, value = part.partition(b"\r\n\r\n")
        match = re.search(r'name="([^"]*)"', header_blob.decode("utf-8", "replace"))
        if match is not None:
            fields[match.group(1)] = value.decode("utf-8")
    return fields


class _CapturingHandler(BaseHTTPRequestHandler):
    """Records the exact multipart fields of every POST, answers 204."""

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        content_type = self.headers.get("Content-Type", "")
        self.server.captured.append(  # type: ignore[attr-defined]
            _parse_multipart(body, content_type)
        )
        self.send_response(204)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def sentinel_server() -> Iterator[ThreadingHTTPServer]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _CountingHandler)
    server.count = 0  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.fixture
def capturing_server() -> Iterator[ThreadingHTTPServer]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _CapturingHandler)
    server.captured = []  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class _StubControl:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def record(self, **kwargs: object) -> None:
        self.calls.append(kwargs)


# --- FakechatSignalPorts ------------------------------------------------


def test_issue_creates_a_durable_active_row(
    database: Database, ports: FakechatSignalPorts
) -> None:
    row = ports.issue(
        cell_id=CELL, session_id="s1", binding_id=BINDING, generation=1, port=9999
    )
    assert row == FakechatSignalPort(
        session_id="s1",
        cell_id=CELL,
        binding_id=BINDING,
        generation=1,
        port=9999,
        state="active",
        created_at=NOW.isoformat(),
        retired_at=None,
    )
    stored = database.execute(
        "SELECT * FROM fakechat_signal_ports WHERE session_id = ?", ("s1",)
    ).fetchone()
    assert stored is not None
    assert stored["state"] == "active"
    assert stored["port"] == 9999


def test_allocated_port_is_free_at_issue_time(ports: FakechatSignalPorts) -> None:
    row = ports.issue(cell_id=CELL, session_id="s1", binding_id=BINDING, generation=1)
    assert _bindable(row.port)


def test_issuing_for_a_new_session_retires_the_old_cell_row(
    ports: FakechatSignalPorts,
) -> None:
    first = ports.issue(
        cell_id=CELL, session_id="s1", binding_id=BINDING, generation=1, port=9001
    )
    second = ports.issue(
        cell_id=CELL, session_id="s2", binding_id="b2" * 4, generation=2, port=9002
    )
    assert ports.active_for_session("s1") is None
    assert ports.active_for_session("s2") == second
    assert second.port != first.port


def test_at_most_one_active_row_per_cell_in_the_database(
    database: Database, ports: FakechatSignalPorts
) -> None:
    ports.issue(cell_id=CELL, session_id="s1", binding_id=BINDING, generation=1)
    ports.issue(
        cell_id=CELL, session_id="s2", binding_id="b2" * 4, generation=2
    )
    active_rows = database.execute(
        "SELECT session_id FROM fakechat_signal_ports "
        "WHERE cell_id = ? AND state = 'active'",
        (CELL,),
    ).fetchall()
    assert [row["session_id"] for row in active_rows] == ["s2"]


def test_retire_is_idempotent(
    database: Database, ports: FakechatSignalPorts
) -> None:
    ports.issue(cell_id=CELL, session_id="s1", binding_id=BINDING, generation=1)
    ports.retire("s1")
    ports.retire("s1")  # second call: no active row, must not raise
    assert ports.active_for_session("s1") is None
    row = database.execute(
        "SELECT state, retired_at FROM fakechat_signal_ports WHERE session_id = ?",
        ("s1",),
    ).fetchone()
    assert row["state"] == "retired"
    assert row["retired_at"] is not None


def test_retire_of_unknown_session_does_nothing(ports: FakechatSignalPorts) -> None:
    ports.retire("never-issued")  # must not raise
    assert ports.active_for_session("never-issued") is None


def test_reissue_rotates_the_port_row(
    database: Database, ports: FakechatSignalPorts
) -> None:
    first = ports.issue(
        cell_id=CELL, session_id="s1", binding_id=BINDING, generation=1, port=9101
    )
    second = ports.issue(
        cell_id=CELL, session_id="s1", binding_id=BINDING, generation=2, port=9102
    )
    assert second.port != first.port
    assert second.generation == 2
    rows = database.execute(
        "SELECT COUNT(*) AS n FROM fakechat_signal_ports WHERE session_id = ?",
        ("s1",),
    ).fetchone()
    assert rows["n"] == 1
    assert ports.active_for_session("s1") == second


# --- FakechatSignalSender: refusals (never touch the network) ----------


def _sender(database: Database, ports: FakechatSignalPorts) -> FakechatSignalSender:
    return FakechatSignalSender(database=database, ports=ports)


def test_refuses_unknown_kind(
    database: Database,
    ports: FakechatSignalPorts,
    sentinel_server: ThreadingHTTPServer,
) -> None:
    ports.issue(
        cell_id=CELL,
        session_id="s1",
        binding_id=BINDING,
        generation=1,
        port=sentinel_server.server_address[1],
    )
    outcome = _sender(database, ports).send(
        session_id="s1",
        kind="HERMES_BOGUS_READY",
        packet_id=WAKE_ID,
        delivery_id=DELIVERY_ID,
    )
    assert outcome.status == "refused"
    assert sentinel_server.count == 0  # type: ignore[attr-defined]


def test_refuses_malformed_packet_id(
    database: Database,
    ports: FakechatSignalPorts,
    sentinel_server: ThreadingHTTPServer,
) -> None:
    ports.issue(
        cell_id=CELL,
        session_id="s1",
        binding_id=BINDING,
        generation=1,
        port=sentinel_server.server_address[1],
    )
    outcome = _sender(database, ports).send(
        session_id="s1",
        kind=WORK_READY,
        packet_id="not-32-hex",
        delivery_id=DELIVERY_ID,
    )
    assert outcome.status == "refused"
    assert sentinel_server.count == 0  # type: ignore[attr-defined]


def test_refuses_malformed_delivery_id(
    database: Database,
    ports: FakechatSignalPorts,
    sentinel_server: ThreadingHTTPServer,
) -> None:
    ports.issue(
        cell_id=CELL,
        session_id="s1",
        binding_id=BINDING,
        generation=1,
        port=sentinel_server.server_address[1],
    )
    outcome = _sender(database, ports).send(
        session_id="s1",
        kind=WORK_READY,
        packet_id=WAKE_ID,
        delivery_id="TOO-SHORT",
    )
    assert outcome.status == "refused"
    assert sentinel_server.count == 0  # type: ignore[attr-defined]


def test_refuses_session_with_no_active_port(
    database: Database, ports: FakechatSignalPorts
) -> None:
    outcome = _sender(database, ports).send(
        session_id="no-such-session",
        kind=WORK_READY,
        packet_id=WAKE_ID,
        delivery_id=DELIVERY_ID,
    )
    assert outcome.status == "refused"


def test_refuses_retired_port_row(
    database: Database,
    ports: FakechatSignalPorts,
    sentinel_server: ThreadingHTTPServer,
) -> None:
    ports.issue(
        cell_id=CELL,
        session_id="s1",
        binding_id=BINDING,
        generation=1,
        port=sentinel_server.server_address[1],
    )
    ports.retire("s1")
    outcome = _sender(database, ports).send(
        session_id="s1",
        kind=WORK_READY,
        packet_id=WAKE_ID,
        delivery_id=DELIVERY_ID,
    )
    assert outcome.status == "refused"
    assert sentinel_server.count == 0  # type: ignore[attr-defined]


# --- FakechatSignalSender: delivered path -------------------------------


def test_delivers_the_exact_bounded_envelope(
    database: Database,
    ports: FakechatSignalPorts,
    capturing_server: ThreadingHTTPServer,
) -> None:
    ports.issue(
        cell_id=CELL,
        session_id="s1",
        binding_id=BINDING,
        generation=1,
        port=capturing_server.server_address[1],
    )
    outcome = _sender(database, ports).send(
        session_id="s1",
        kind=WORK_READY,
        packet_id=WAKE_ID,
        delivery_id=DELIVERY_ID,
    )
    assert outcome.status == "delivered"
    captured = capturing_server.captured  # type: ignore[attr-defined]
    assert len(captured) == 1
    assert captured[0] == {
        "id": DELIVERY_ID,
        "text": f"{WORK_READY} {WAKE_ID}",
    }
    # Exactness: no trailing newline, no extra whitespace, no extra
    # fields rode along with the bounded envelope.
    assert captured[0]["text"] == "HERMES_WORK_READY " + WAKE_ID
    assert not captured[0]["text"].endswith("\n")
    assert captured[0]["text"].count(" ") == 1


def test_delivers_correction_ready_kind_too(
    database: Database,
    ports: FakechatSignalPorts,
    capturing_server: ThreadingHTTPServer,
) -> None:
    ports.issue(
        cell_id=CELL,
        session_id="s1",
        binding_id=BINDING,
        generation=1,
        port=capturing_server.server_address[1],
    )
    outcome = _sender(database, ports).send(
        session_id="s1",
        kind=CORRECTION_READY,
        packet_id=WAKE_ID,
        delivery_id=DELIVERY_ID,
    )
    assert outcome.status == "delivered"
    captured = capturing_server.captured  # type: ignore[attr-defined]
    assert captured[-1]["text"] == f"{CORRECTION_READY} {WAKE_ID}"


# --- FakechatSignalSender: failure path ---------------------------------


def test_closed_port_fails_without_raising_and_records_no_control_by_default(
    database: Database, ports: FakechatSignalPorts
) -> None:
    dead_port = _probe_free_port()  # bound then released: nothing listens
    ports.issue(
        cell_id=CELL, session_id="s1", binding_id=BINDING, generation=1, port=dead_port
    )
    outcome = _sender(database, ports).send(
        session_id="s1",
        kind=WORK_READY,
        packet_id=WAKE_ID,
        delivery_id=DELIVERY_ID,
    )
    assert outcome.status == "failed"
    assert outcome.detail  # a bounded, non-empty reason


def test_closed_port_records_exactly_one_signal_failed_receipt(
    database: Database, ports: FakechatSignalPorts
) -> None:
    dead_port = _probe_free_port()
    ports.issue(
        cell_id=CELL, session_id="s1", binding_id=BINDING, generation=1, port=dead_port
    )
    control = _StubControl()
    sender = FakechatSignalSender(database=database, ports=ports, control=control)
    outcome = sender.send(
        session_id="s1",
        kind=WORK_READY,
        packet_id=WAKE_ID,
        delivery_id=DELIVERY_ID,
    )
    assert outcome.status == "failed"
    assert len(control.calls) == 1
    call = control.calls[0]
    assert call["kind"] == "signal.failed"
    assert call["session_id"] == "s1"
    assert call["cell_id"] == CELL
    result = call["result"]
    assert isinstance(result, dict)
    assert result["port"] == dead_port
    assert result["delivery_id"] == DELIVERY_ID
    assert len(str(result["reason"])) <= 200
    assert "Stop-hook drain" in str(call["reason"])


def test_refusals_never_touch_control(
    database: Database, ports: FakechatSignalPorts
) -> None:
    control = _StubControl()
    sender = FakechatSignalSender(database=database, ports=ports, control=control)
    outcome = sender.send(
        session_id="no-such-session",
        kind=WORK_READY,
        packet_id=WAKE_ID,
        delivery_id=DELIVERY_ID,
    )
    assert outcome.status == "refused"
    assert control.calls == []
