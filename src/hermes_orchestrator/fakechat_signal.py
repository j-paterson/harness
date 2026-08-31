"""Bounded loopback wake signal over the official fakechat channel.

INFRA-197 (v4 amendment): unattended launch of the custom
``hermes-control`` development channel is impossible on a personal Max
account during the channels research preview, so the live idle-wake
signal plane for this fleet substitutes the official ``fakechat``
Claude Code channel plugin. Fakechat runs inside the Claude host,
binds ``127.0.0.1`` only, listens on the port named by
``FAKECHAT_PORT``, and injects any text POSTed to its ``/upload``
endpoint into the session as a channel event, answering 204 — with no
sender authentication beyond the loopback bind itself.

Division of labor is unchanged in kind from the stdio hub it
substitutes: fakechat carries ONLY the same bounded
``HERMES_* <32hex>`` wake envelope the announcer and Stop-hook drain
already enforce (:data:`INTAKE_ENVELOPE_PATTERN`, imported here, never
redefined) — durable state remains the sole payload source, and
``hermes-orchestrator intake-poll``/``intake-ack`` remain the
exactly-once ACK. A forged local POST can therefore only trigger a
drain of durable rows, a no-op when nothing is pending: the wake is a
signal, never a payload.

:class:`FakechatSignalPorts` gives that signal a durable, exclusive
destination: one row per session names the exact cell, seat binding,
and generation that own a probed-free loopback port, and a partial
unique index enforces at most one live row per cell, so issuing a
fresh seat's port always retires whatever a prior seat left behind, in
the same transaction as the new row.

:class:`FakechatSignalSender` is the fail-closed, never-raising sender:
it validates the envelope and delivery id against the exact grammar
before any external effect, refuses any port not owned by the exact
active session it is waking, and treats every transport failure
(refusal, timeout, non-2xx) as a bounded, recorded outcome rather than
an exception — the durable delivery stays non-terminal for the
existing Stop-hook drain either way.
"""

from __future__ import annotations

import re
import socket
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from hermes_orchestrator.control_operations import ControlOperations
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.lead_intake import INTAKE_ENVELOPE_PATTERN

_DELIVERY_ID = re.compile(r"^[0-9a-f]{32}$")


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _free_loopback_port() -> int:
    """Probe one free loopback port by binding, then closing, a socket.

    A narrow race remains between the close here and fakechat's own
    bind (inherent to "probe a free port" on any platform); a lost race
    surfaces to the sender as an ordinary connection refusal, which is
    already a bounded, non-raising failure rather than a crash.
    """

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _encode_multipart(fields: dict[str, str]) -> tuple[bytes, str]:
    """Hand-build the exact two-field ``multipart/form-data`` body
    fakechat's ``/upload`` expects, using only the standard library."""

    boundary = uuid.uuid4().hex
    chunks: list[bytes] = []
    for name, value in fields.items():
        chunks.append(f"--{boundary}\r\n".encode())
        chunks.append(
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
        )
        chunks.append(value.encode("utf-8"))
        chunks.append(b"\r\n")
    chunks.append(f"--{boundary}--\r\n".encode())
    return b"".join(chunks), f"multipart/form-data; boundary={boundary}"


@dataclass(frozen=True, slots=True)
class FakechatSignalPort:
    """One durable row binding a session to its fakechat loopback port."""

    session_id: str
    cell_id: str
    binding_id: str
    generation: int
    port: int
    state: str
    created_at: str
    retired_at: str | None


def _row_to_port(row: Any) -> FakechatSignalPort:
    return FakechatSignalPort(
        session_id=str(row["session_id"]),
        cell_id=str(row["cell_id"]),
        binding_id=str(row["binding_id"]),
        generation=int(row["generation"]),
        port=int(row["port"]),
        state=str(row["state"]),
        created_at=str(row["created_at"]),
        retired_at=row["retired_at"],
    )


class FakechatSignalPorts:
    """Issue, retire, and read durable per-session fakechat port ownership.

    At most one row is ``active`` per cell — the partial unique index
    enforces it durably. Issuing a fresh session's port retires whatever
    active row a prior seat left behind, in the same transaction as the
    new row's upsert, so ownership transfers atomically and a sender
    can never observe two live claims for one cell.
    """

    def __init__(
        self,
        *,
        database: Database,
        now: Callable[[], datetime] | None = None,
        free_port: Callable[[], int] | None = None,
    ) -> None:
        self._database = database
        self._events = EventStore(database)
        self._now = now or _now_utc
        self._free_port = free_port or _free_loopback_port

    def issue(
        self,
        *,
        cell_id: str,
        session_id: str,
        binding_id: str,
        generation: int,
        port: int | None = None,
    ) -> FakechatSignalPort:
        chosen = self._free_port() if port is None else port
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE fakechat_signal_ports SET state = 'retired', "
                "retired_at = ? WHERE cell_id = ? AND state = 'active'",
                (stamp, cell_id),
            )
            connection.execute(
                "INSERT INTO fakechat_signal_ports("
                "session_id, cell_id, binding_id, generation, port, "
                "state, created_at, retired_at"
                ") VALUES (?, ?, ?, ?, ?, 'active', ?, NULL) "
                "ON CONFLICT(session_id) DO UPDATE SET "
                "cell_id = excluded.cell_id, "
                "binding_id = excluded.binding_id, "
                "generation = excluded.generation, "
                "port = excluded.port, state = 'active', "
                "created_at = excluded.created_at, retired_at = NULL",
                (session_id, cell_id, binding_id, generation, chosen, stamp),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="fakechat_port.issued",
                    aggregate_type="fakechat_port",
                    aggregate_id=session_id,
                    payload={
                        "cell_id": cell_id,
                        "session_id": session_id,
                        "binding_id": binding_id,
                        "generation": generation,
                        "port": chosen,
                    },
                ),
            )
        return FakechatSignalPort(
            session_id=session_id,
            cell_id=cell_id,
            binding_id=binding_id,
            generation=generation,
            port=chosen,
            state="active",
            created_at=stamp,
            retired_at=None,
        )

    def retire(self, session_id: str) -> None:
        """Retire the session's active row, or do nothing (idempotent)."""

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT cell_id FROM fakechat_signal_ports "
                "WHERE session_id = ? AND state = 'active'",
                (session_id,),
            ).fetchone()
            if row is None:
                return
            connection.execute(
                "UPDATE fakechat_signal_ports SET state = 'retired', "
                "retired_at = ? WHERE session_id = ? AND state = 'active'",
                (stamp, session_id),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="fakechat_port.retired",
                    aggregate_type="fakechat_port",
                    aggregate_id=session_id,
                    payload={
                        "session_id": session_id,
                        "cell_id": str(row["cell_id"]),
                    },
                ),
            )

    def active_for_session(self, session_id: str) -> FakechatSignalPort | None:
        row = self._database.execute(
            "SELECT * FROM fakechat_signal_ports "
            "WHERE session_id = ? AND state = 'active'",
            (session_id,),
        ).fetchone()
        return None if row is None else _row_to_port(row)


@dataclass(frozen=True, slots=True)
class SignalOutcome:
    """The bounded result of one send attempt; :meth:`send` never raises."""

    status: str  # "delivered" | "refused" | "failed"
    detail: str


class FakechatSignalSender:
    """Send the bounded wake envelope to one session's owned fakechat port.

    Fail-closed and never raising: an envelope that does not match
    :data:`INTAKE_ENVELOPE_PATTERN`, a malformed delivery id, or a
    session with no active (or a retired) port row is refused before
    any network I/O. Only two names of anything ever posted are
    hard-coded — the loopback host ``127.0.0.1`` and the multipart
    fields ``id``/``text`` fakechat's ``/upload`` expects — nothing
    about the destination is configurable. A connection refusal,
    timeout, or non-2xx status is a bounded ``failed`` outcome: the
    durable delivery stays non-terminal for the existing Stop-hook
    drain, and when a control collaborator is supplied, exactly one
    ``signal.failed`` receipt is recorded.
    """

    def __init__(
        self,
        *,
        database: Database,
        ports: FakechatSignalPorts,
        control: ControlOperations | None = None,
        timeout_seconds: float = 2.0,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._ports = ports
        self._control = control
        self._timeout_seconds = timeout_seconds
        self._now = now or _now_utc

    def send(
        self,
        *,
        session_id: str,
        kind: str,
        packet_id: str,
        delivery_id: str,
    ) -> SignalOutcome:
        envelope = f"{kind} {packet_id}"
        if INTAKE_ENVELOPE_PATTERN.fullmatch(envelope) is None:
            return SignalOutcome(
                status="refused",
                detail="the composed envelope fails the bounded wake grammar",
            )
        if _DELIVERY_ID.fullmatch(delivery_id) is None:
            return SignalOutcome(
                status="refused",
                detail="the delivery id must be 32 lowercase hex characters",
            )
        port_row = self._ports.active_for_session(session_id)
        if port_row is None:
            return SignalOutcome(
                status="refused",
                detail="no active fakechat port is owned by this exact session",
            )
        body, content_type = _encode_multipart(
            {"id": delivery_id, "text": envelope}
        )
        request = urllib.request.Request(
            f"http://127.0.0.1:{port_row.port}/upload",
            data=body,
            headers={"Content-Type": content_type},
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self._timeout_seconds
            ) as response:
                status = int(response.status)
        except urllib.error.HTTPError as error:
            reason = f"http status {error.code}"
            self._record_failure(
                port=port_row.port,
                cell_id=port_row.cell_id,
                session_id=session_id,
                delivery_id=delivery_id,
                reason=reason,
            )
            return SignalOutcome(status="failed", detail=reason)
        except Exception as error:
            # Connection refused, timeout, or any other transport
            # surprise: bounded and never raised past this method.
            reason = f"{type(error).__name__}: {error}"[:200]
            self._record_failure(
                port=port_row.port,
                cell_id=port_row.cell_id,
                session_id=session_id,
                delivery_id=delivery_id,
                reason=reason,
            )
            return SignalOutcome(status="failed", detail=reason)
        if 200 <= status < 300:
            return SignalOutcome(status="delivered", detail=f"http status {status}")
        reason = f"http status {status}"
        self._record_failure(
            port=port_row.port,
            cell_id=port_row.cell_id,
            session_id=session_id,
            delivery_id=delivery_id,
            reason=reason,
        )
        return SignalOutcome(status="failed", detail=reason)

    def _record_failure(
        self,
        *,
        port: int,
        cell_id: str,
        session_id: str,
        delivery_id: str,
        reason: str,
    ) -> None:
        """One bounded, best-effort receipt per transport failure.

        The receipt is pure evidence, never the recovery path itself:
        the durable delivery this signal was meant to wake already
        remains for the existing Stop-hook drain regardless of whether
        a control collaborator is wired up or this call succeeds.
        """

        if self._control is None:
            return
        with suppress(Exception):
            self._control.record(
                kind="signal.failed",
                project_key=self._project_key_for(cell_id),
                cell_id=cell_id,
                session_id=session_id,
                result={
                    "port": port,
                    "delivery_id": delivery_id,
                    "reason": reason[:200],
                },
                reason=(
                    "fakechat delivery failed; the durable delivery "
                    "remains pending for the existing Stop-hook drain"
                ),
            )

    def _project_key_for(self, cell_id: str) -> str:
        row = self._database.execute(
            "SELECT project_key FROM project_cells WHERE cell_id = ?",
            (cell_id,),
        ).fetchone()
        return "" if row is None else str(row["project_key"])
