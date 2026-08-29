"""The Hermes side of the dedicated Claude Code channel.

INFRA-190: a Unix-domain socket server under the private state
directory accepts exactly one kind of peer — the ``hermes-control``
MCP sidecar spawned by the visible classic Claude lead — plus a local
``nudge`` trigger from the process that just committed a durable
packet. The wire contract is ``channels/hermes-control/PROTOCOL.md``
(newline-delimited JSON, 4096-byte line cap, fail closed on
everything unexpected).

Delivery is persist-before-publish with compare-and-set transitions:
an event row exists durably before any byte reaches the socket, is
unique per (kind, packet, session), replays on every reconnect until
acknowledged, and acknowledges at most once — so restarts, duplicate
deliveries, and lost ACK replies are exactly-once effective. The
per-session capability lives in one mode-0600 file; the database
stores only its SHA-256, and refusal reasons never echo it.

The channel is an accelerator, not the source of truth: a packet with
no registered channel simply stays pending for the Stop-hook poll and
the metadata announcements, which remain the automatic fallback.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
import socket
import uuid
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
from hermes_orchestrator.control_operations import (
    CONTROL_READY,
    ControlOperations,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.lead_assignments import ASSIGNMENT_READY
from hermes_orchestrator.lead_intake import CORRECTION_READY, WORK_READY

PROTOCOL_VERSION = 1
MAX_LINE_BYTES = 4096

_PACKET_ID = re.compile(r"^[0-9a-f]{32}$")
_CAPABILITY = re.compile(r"^[0-9a-f]{64}$")
_KINDS = (CORRECTION_READY, WORK_READY, ASSIGNMENT_READY, CONTROL_READY)


def _utc_now() -> datetime:
    return datetime.now(UTC)


class ChannelCapabilities:
    """Issue and verify per-session channel capabilities.

    The token itself exists only in one mode-0600 file under the
    private state directory (for the sidecar to read) — the database
    keeps its SHA-256, so no durable record, log, or event can leak
    it.
    """

    def __init__(
        self,
        *,
        database: Database,
        state_dir: Path,
        now: Callable[[], datetime] | None = None,
        tokens: Callable[[], str] | None = None,
    ) -> None:
        self._database = database
        self._dir = state_dir / "channels"
        self._now = now or _utc_now
        self._tokens = tokens or (lambda: secrets.token_hex(32))

    def path_for(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.capability"

    def issue(self, session_id: str) -> Path:
        token = self._tokens()
        if _CAPABILITY.fullmatch(token) is None:
            raise ValueError("capability tokens must be 64 lowercase hex")
        self._dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = self.path_for(session_id)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(descriptor, token.encode("ascii"))
        finally:
            os.close(descriptor)
        digest = hashlib.sha256(token.encode("ascii")).hexdigest()
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO channel_capabilities("
                "session_id, capability_sha256, created_at, retired_at"
                ") VALUES (?, ?, ?, NULL) "
                "ON CONFLICT(session_id) DO UPDATE SET "
                "capability_sha256 = excluded.capability_sha256, "
                "created_at = excluded.created_at, retired_at = NULL",
                (session_id, digest, stamp),
            )
        return path

    def verify(self, session_id: str, capability: str) -> bool:
        if _CAPABILITY.fullmatch(capability) is None:
            return False
        row = self._database.execute(
            "SELECT capability_sha256 FROM channel_capabilities "
            "WHERE session_id = ? AND retired_at IS NULL",
            (session_id,),
        ).fetchone()
        if row is None:
            return False
        digest = hashlib.sha256(capability.encode("ascii")).hexdigest()
        return hmac.compare_digest(digest, str(row["capability_sha256"]))

    def retire(self, session_id: str) -> None:
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE channel_capabilities SET retired_at = ? "
                "WHERE session_id = ? AND retired_at IS NULL",
                (self._now().isoformat(), session_id),
            )
        self.path_for(session_id).unlink(missing_ok=True)


class ChannelLauncher:
    """Generate the session-scoped MCP config for one classic seat.

    Everything lives outside the worktree under the private state
    directory: the mode-0600 config names the exact Node entry point,
    the hub socket, the seat's full identity, and the path of the
    session's mode-0600 capability file — the capability itself never
    appears in the config, argv, or logs. A missing sidecar build or
    Node binary raises before anything is written; the caller then
    launches the plain classic command and the channel simply stays
    absent (packets remain pending for the fallback paths).
    """

    def __init__(
        self,
        *,
        state_dir: Path,
        capabilities: ChannelCapabilities,
        sidecar_entry: Path,
        node_binary: Path,
    ) -> None:
        self._state_dir = state_dir
        self._capabilities = capabilities
        self._sidecar_entry = sidecar_entry
        self._node_binary = node_binary

    def config_path_for(self, session_id: str) -> Path:
        canonical = str(uuid.UUID(str(session_id)))
        return self._state_dir / "channels" / f"{canonical}.mcp.json"

    def generate(
        self,
        *,
        project_key: str,
        cell_id: str,
        session_id: str,
        profile_alias: str,
        generation: int,
    ) -> Path:
        canonical = str(uuid.UUID(str(session_id)))
        if not self._sidecar_entry.is_file():
            raise FileNotFoundError("the hermes-control sidecar build is missing")
        if not self._node_binary.is_file():
            raise FileNotFoundError("the Node binary is missing")
        capability_path = self._capabilities.issue(canonical)
        config = {
            "mcpServers": {
                "hermes-control": {
                    "type": "stdio",
                    "command": str(self._node_binary),
                    "args": [str(self._sidecar_entry)],
                    "env": {
                        "HERMES_CONTROL_SOCKET": str(hub_socket_path(self._state_dir)),
                        "HERMES_CONTROL_PROJECT": project_key,
                        "HERMES_CONTROL_CELL": cell_id,
                        "HERMES_CONTROL_SESSION": canonical,
                        "HERMES_CONTROL_PROFILE": profile_alias,
                        "HERMES_CONTROL_GENERATION": str(generation),
                        "HERMES_CONTROL_CAPABILITY_FILE": str(capability_path),
                    },
                }
            }
        }
        path = self.config_path_for(canonical)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(
                descriptor,
                json.dumps(config, indent=2, sort_keys=True).encode("utf-8"),
            )
        finally:
            os.close(descriptor)
        return path

    def cleanup(self, session_id: str) -> None:
        """Remove the config and retire the capability — only called
        after the seat's safe retirement."""

        canonical = str(uuid.UUID(str(session_id)))
        self.config_path_for(canonical).unlink(missing_ok=True)
        self._capabilities.retire(canonical)


class ChannelHub:
    """Socket server, durable event ledger, and replay engine."""

    def __init__(
        self,
        *,
        database: Database,
        bindings: CmuxSurfaceBindings,
        capabilities: ChannelCapabilities,
        socket_path: Path,
        ids: Callable[[], str] | None = None,
        now: Callable[[], datetime] | None = None,
        control: ControlOperations | None = None,
    ) -> None:
        self._database = database
        self._bindings = bindings
        self._capabilities = capabilities
        self._socket_path = socket_path
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._now = now or _utc_now
        self._control = control
        self._server: asyncio.AbstractServer | None = None
        self._connections: dict[str, asyncio.StreamWriter] = {}
        self._peers: set[asyncio.StreamWriter] = set()

    @property
    def socket_path(self) -> Path:
        return self._socket_path

    def registered_sessions(self) -> frozenset[str]:
        return frozenset(self._connections)

    async def start(self) -> None:
        self._socket_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._socket_path.unlink(missing_ok=True)
        self._server = await asyncio.start_unix_server(
            self._handle,
            path=str(self._socket_path),
            limit=MAX_LINE_BYTES,
        )
        os.chmod(self._socket_path, 0o600)

    async def stop(self) -> None:
        # Close every peer, registered or not: a connection still
        # blocked in its first read would otherwise keep the server's
        # wait_closed() from ever returning.
        for writer in list(self._peers):
            writer.close()
        self._peers.clear()
        self._connections.clear()
        if self._server is not None:
            self._server.close()
            with suppress(Exception):
                await self._server.wait_closed()
            self._server = None
        self._socket_path.unlink(missing_ok=True)

    async def publish(
        self,
        *,
        kind: str,
        packet_id: str,
        cell_id: str,
        session_id: str,
    ) -> str:
        """Persist one bounded event, then push it if a channel is live.

        Returns ``"published"``, ``"pending"`` (durable, no live
        channel or the write failed — replay covers it), or
        ``"deduplicated"`` (already acknowledged or superseded).
        Invalid inputs raise ``ValueError`` before any durable or
        external effect.
        """

        if kind not in _KINDS:
            raise ValueError("unknown channel event kind")
        if _PACKET_ID.fullmatch(packet_id) is None:
            raise ValueError("the packet id must be 32 lowercase hex")
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO channel_events("
                "event_id, kind, packet_id, cell_id, session_id, "
                "state, attempts, created_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?)",
                (
                    self._ids(),
                    kind,
                    packet_id,
                    cell_id,
                    session_id,
                    stamp,
                    stamp,
                ),
            )
        row = self._database.execute(
            "SELECT event_id, state FROM channel_events "
            "WHERE kind = ? AND packet_id = ? AND session_id = ?",
            (kind, packet_id, session_id),
        ).fetchone()
        if row is None or str(row["state"]) in ("acked", "superseded"):
            return "deduplicated"
        writer = self._connections.get(session_id)
        if writer is None:
            return "pending"
        sent = await self._send_event(
            writer,
            event_id=str(row["event_id"]),
            kind=kind,
            packet_id=packet_id,
            session_id=session_id,
        )
        return "published" if sent else "pending"

    async def publish_pending(self) -> tuple[str, ...]:
        """Route every durable pending packet toward its channel.

        Derivation, not memory: pending corrections and undelivered
        wakes are re-read from their durable sources each pass, so
        this doubles as the low-frequency repair tick and as the
        direct route taken right after a packet commits (via the
        ``nudge`` op or an in-process call).
        """

        published: list[str] = []
        # Repair first, in the same derivation pass: an unacknowledged
        # channel event whose packet was already consumed through
        # another path (Stop-hook poll, prior session, operator
        # signal) must never replay as a fresh wake — live evidence: a
        # day-old delivered INFRA-185 wake resurfaced through the
        # channel. Superseding is durable and terminal for the event
        # while the packet's own ledger remains the truth.
        stamp = self._now().isoformat()
        repaired = 0
        with self._database.transaction() as connection:
            repaired += connection.execute(
                "UPDATE channel_events SET state = 'superseded', "
                "updated_at = ? WHERE kind = 'HERMES_WORK_READY' "
                "AND state IN ('pending', 'published') AND packet_id IN ("
                "SELECT wake_id FROM lead_terminal_wakes "
                "WHERE state != 'pending')",
                (stamp,),
            ).rowcount
            repaired += connection.execute(
                "UPDATE channel_events SET state = 'superseded', "
                "updated_at = ? "
                "WHERE kind = 'HERMES_CORRECTION_READY' "
                "AND state IN ('pending', 'published') AND packet_id IN ("
                "SELECT correction_id FROM lead_corrections "
                "WHERE state != 'pending')",
                (stamp,),
            ).rowcount
            # An assignment supersedes only when its own ledger is
            # terminal (acknowledged through any exact path, or
            # replaced) — never merely because a transport marked a
            # delivery, so the exact-ACK contract survives races that
            # consumed the wake-based bootstrap events before ACK.
            repaired += connection.execute(
                "UPDATE channel_events SET state = 'superseded', "
                "updated_at = ? "
                "WHERE kind = 'HERMES_ASSIGNMENT_READY' "
                "AND state IN ('pending', 'published') AND packet_id IN ("
                "SELECT assignment_id FROM lead_assignments "
                "WHERE state != 'published')",
                (stamp,),
            ).rowcount
            repaired += connection.execute(
                "UPDATE channel_events SET state = 'superseded', "
                "updated_at = ? "
                "WHERE kind = 'HERMES_CONTROL_READY' "
                "AND state IN ('pending', 'published') AND packet_id IN ("
                "SELECT operation_id FROM control_operations "
                "WHERE state != 'published')",
                (stamp,),
            ).rowcount
        self._record_dedup_repair(repaired)
        corrections = self._database.execute(
            "SELECT correction_id, project_key FROM lead_corrections "
            "WHERE state = 'pending' ORDER BY created_at ASC, rowid ASC"
        ).fetchall()
        for row in corrections:
            target = self._database.execute(
                "SELECT cell_id, session_id FROM project_cells "
                "WHERE project_key = ? AND state = 'active' "
                "ORDER BY updated_at DESC, rowid DESC LIMIT 1",
                (str(row["project_key"]),),
            ).fetchone()
            if target is None:
                continue
            status = await self.publish(
                kind=CORRECTION_READY,
                packet_id=str(row["correction_id"]),
                cell_id=str(target["cell_id"]),
                session_id=str(target["session_id"]),
            )
            if status == "published":
                published.append(f"{CORRECTION_READY} {row['correction_id']}")
        wakes = self._database.execute(
            "SELECT w.wake_id, w.cell_id, w.session_id "
            "FROM lead_terminal_wakes AS w "
            "WHERE w.state = 'pending' AND NOT EXISTS ("
            "SELECT 1 FROM channel_events AS e "
            "WHERE e.kind = 'HERMES_WORK_READY' "
            "AND e.packet_id = w.wake_id "
            "AND e.session_id = w.session_id "
            "AND e.state IN ('acked', 'superseded')"
            ") ORDER BY w.created_at ASC, w.rowid ASC"
        ).fetchall()
        for row in wakes:
            status = await self.publish(
                kind=WORK_READY,
                packet_id=str(row["wake_id"]),
                cell_id=str(row["cell_id"]),
                session_id=str(row["session_id"]),
            )
            if status == "published":
                published.append(f"{WORK_READY} {row['wake_id']}")
        assignments = self._database.execute(
            "SELECT a.assignment_id, a.cell_id, a.session_id "
            "FROM lead_assignments AS a "
            "WHERE a.state = 'published' AND NOT EXISTS ("
            "SELECT 1 FROM channel_events AS e "
            "WHERE e.kind = 'HERMES_ASSIGNMENT_READY' "
            "AND e.packet_id = a.assignment_id "
            "AND e.session_id = a.session_id "
            "AND e.state IN ('acked', 'superseded')"
            ") ORDER BY a.created_at ASC, a.rowid ASC"
        ).fetchall()
        for row in assignments:
            status = await self.publish(
                kind=ASSIGNMENT_READY,
                packet_id=str(row["assignment_id"]),
                cell_id=str(row["cell_id"]),
                session_id=str(row["session_id"]),
            )
            if status == "published":
                published.append(f"{ASSIGNMENT_READY} {row['assignment_id']}")
        operations = self._database.execute(
            "SELECT o.operation_id, o.cell_id, o.session_id "
            "FROM control_operations AS o "
            "WHERE o.state = 'published' AND NOT EXISTS ("
            "SELECT 1 FROM channel_events AS e "
            "WHERE e.kind = 'HERMES_CONTROL_READY' "
            "AND e.packet_id = o.operation_id "
            "AND e.session_id = o.session_id "
            "AND e.state IN ('acked', 'superseded')"
            ") ORDER BY o.created_at ASC, o.rowid ASC"
        ).fetchall()
        for row in operations:
            status = await self.publish(
                kind=CONTROL_READY,
                packet_id=str(row["operation_id"]),
                cell_id=str(row["cell_id"]),
                session_id=str(row["session_id"]),
            )
            if status == "published":
                published.append(f"{CONTROL_READY} {row['operation_id']}")
        return tuple(published)

    def _record_dedup_repair(self, repaired: int) -> None:
        """Receipt a sweep that superseded stale events, per active lead.

        A zero-change sweep is routine maintenance and receipts
        nothing; the explicit zero-valued absence receipt belongs to
        operations a lead must be able to distinguish, such as a
        replay that delivered no events.
        """

        if self._control is None or repaired <= 0:
            return
        with suppress(Exception):
            self._control.record_for_active_cells(
                kind="intake.dedup_repaired",
                result={"superseded_events": repaired},
            )

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        session: str | None = None
        self._peers.add(writer)
        try:
            while True:
                message = await self._read_message(reader)
                if message is None:
                    return
                op = message.get("op")
                if session is None:
                    if op == "nudge":
                        published = await self.publish_pending()
                        await self._write(
                            writer,
                            {"op": "nudged", "published": len(published)},
                        )
                        return
                    if op != "register":
                        return
                    reason = self._refuse_registration(message)
                    if reason is not None:
                        self._record_channel_blocked(message, reason)
                        await self._write(writer, {"op": "refused", "reason": reason})
                        return
                    session = str(message["session_id"])
                    self._record_registration(message)
                    prior = self._connections.get(session)
                    self._connections[session] = writer
                    if prior is not None and prior is not writer:
                        prior.close()
                    await self._write(
                        writer,
                        {"op": "registered", "proto": PROTOCOL_VERSION},
                    )
                    await self._replay(session, writer)
                    continue
                if op == "ack":
                    await self._write(writer, self._acknowledge(session, message))
                    continue
                # Anything else after registration is a violation.
                return
        finally:
            self._peers.discard(writer)
            if session is not None and self._connections.get(session) is writer:
                del self._connections[session]
                self._close_registration(session, reason="disconnected")
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    async def _read_message(
        self, reader: asyncio.StreamReader
    ) -> dict[str, object] | None:
        """One line as a dict, or None on EOF/violation (close)."""

        try:
            line = await reader.readline()
        except (asyncio.LimitOverrunError, ValueError):
            return None
        if not line or len(line) > MAX_LINE_BYTES:
            return None
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            return None
        if not isinstance(message, dict):
            return None
        return message

    def _refuse_registration(self, message: dict[str, object]) -> str | None:
        if message.get("proto") != PROTOCOL_VERSION:
            return "unsupported protocol version"
        project = str(message.get("project") or "")
        cell_id = str(message.get("cell_id") or "")
        session_id = str(message.get("session_id") or "")
        profile = str(message.get("profile") or "")
        generation = message.get("generation")
        capability = str(message.get("capability") or "")
        if not all((project, cell_id, session_id, profile)):
            return "incomplete identity"
        if not isinstance(generation, int) or isinstance(generation, bool):
            return "generation must be an integer"
        if not self._capabilities.verify(session_id, capability):
            return "capability rejected"
        binding = self._bindings.active_lead(cell_id)
        if binding is None:
            return "no active seat binding for this cell"
        if (
            binding.session_id != session_id
            or binding.project_key != project
            or binding.profile_alias != profile
        ):
            return "identity mismatch with the active binding"
        if binding.generation != generation:
            return "stale binding generation"
        if not self._bindings.is_classic(binding.binding_id, session_id):
            return "no classic-seat evidence for this session"
        return None

    def _record_channel_blocked(
        self, message: dict[str, object], reason: str
    ) -> None:
        """One durable, actionable blocked receipt per refused channel.

        Routine recovery must never require /mcp, a manual relaunch,
        Computer Use, or a terminal paste — so the refusal itself
        becomes a durable control operation the lead (or the fallback
        drain) can fetch and act on. A successful registration later
        supersedes it.
        """

        if self._control is None:
            return
        project = str(message.get("project") or "")
        cell_id = str(message.get("cell_id") or "")
        session_id = str(message.get("session_id") or "")
        if not (project and cell_id and session_id):
            return
        with suppress(Exception):
            self._control.record(
                kind="channel.blocked",
                project_key=project,
                cell_id=cell_id,
                session_id=session_id,
                result={"refusal": reason},
                reason=(
                    f"channel registration refused: {reason}; the "
                    "sidecar retries transient refusals itself, a "
                    "stale generation needs the seat relaunched by "
                    "the seater, and a rejected capability needs a "
                    "reissued capability file"
                ),
            )

    def _record_registration(self, message: dict[str, object]) -> None:
        stamp = self._now().isoformat()
        session_id = str(message["session_id"])
        with self._database.transaction() as connection:
            prior = connection.execute(
                "SELECT COUNT(*) FROM channel_registrations "
                "WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            # A live blocked receipt is healed by the registration it
            # was blocking on; supersede it so it never wakes a lead
            # whose channel already recovered.
            connection.execute(
                "UPDATE control_operations SET state = 'superseded', "
                "updated_at = ? WHERE session_id = ? "
                "AND kind = 'channel.blocked' AND state = 'published'",
                (stamp, session_id),
            )
            connection.execute(
                "UPDATE channel_registrations SET state = 'superseded', "
                "closed_at = ?, close_reason = 'superseded' "
                "WHERE session_id = ? AND state = 'active'",
                (stamp, session_id),
            )
            connection.execute(
                "INSERT INTO channel_registrations("
                "registration_id, project_key, cell_id, session_id, "
                "profile_alias, generation, state, connected_at"
                ") VALUES (?, ?, ?, ?, ?, ?, 'active', ?)",
                (
                    self._ids(),
                    str(message["project"]),
                    str(message["cell_id"]),
                    str(message["session_id"]),
                    str(message["profile"]),
                    int(message["generation"]),  # type: ignore[arg-type]
                    stamp,
                ),
            )
        # A registration that follows any earlier one is a recovery:
        # the lead gets a durable, ACKable receipt instead of the
        # operator having to notice a reconnect in a terminal.
        if self._control is not None and int(prior[0]) > 0:
            with suppress(Exception):
                self._control.record(
                    kind="channel.reregistered",
                    project_key=str(message["project"]),
                    cell_id=str(message["cell_id"]),
                    session_id=session_id,
                    result={
                        "generation": int(message["generation"]),  # type: ignore[arg-type]
                        "prior_registrations": int(prior[0]),
                    },
                )

    def _close_registration(self, session_id: str, *, reason: str) -> None:
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE channel_registrations SET state = 'closed', "
                "closed_at = ?, close_reason = ? "
                "WHERE session_id = ? AND state = 'active'",
                (self._now().isoformat(), reason, session_id),
            )

    def _acknowledge(
        self, session: str, message: dict[str, object]
    ) -> dict[str, object]:
        event_id = str(message.get("event_id") or "")
        packet_id = str(message.get("packet_id") or "")
        session_id = str(message.get("session_id") or "")
        if not event_id or len(event_id) > 128:
            return {
                "op": "ack_refused",
                "event_id": event_id,
                "reason": "malformed event id",
            }
        if session_id != session:
            return {
                "op": "ack_refused",
                "event_id": event_id,
                "reason": "session mismatch",
            }
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE channel_events SET state = 'acked', "
                "acked_at = ?, updated_at = ? "
                "WHERE event_id = ? AND packet_id = ? "
                "AND session_id = ? "
                "AND state IN ('pending', 'published')",
                (stamp, stamp, event_id, packet_id, session_id),
            )
            if cursor.rowcount == 1:
                # An assignment's or control operation's exact channel
                # ACK is the lead's acknowledgement of the packet
                # itself; both records move in one transaction.
                connection.execute(
                    "UPDATE lead_assignments SET state = 'acknowledged', "
                    "acknowledged_at = ?, updated_at = ? "
                    "WHERE assignment_id = ? AND session_id = ? "
                    "AND state = 'published'",
                    (stamp, stamp, packet_id, session_id),
                )
                connection.execute(
                    "UPDATE control_operations "
                    "SET state = 'acknowledged', "
                    "acknowledged_at = ?, updated_at = ? "
                    "WHERE operation_id = ? AND session_id = ? "
                    "AND state = 'published'",
                    (stamp, stamp, packet_id, session_id),
                )
                return {"op": "ack_ok", "event_id": event_id}
        return {
            "op": "ack_refused",
            "event_id": event_id,
            "reason": "no acknowledgeable event matches",
        }

    async def _replay(self, session_id: str, writer: asyncio.StreamWriter) -> None:
        rows = self._database.execute(
            "SELECT event_id, kind, packet_id FROM channel_events "
            "WHERE session_id = ? AND state IN ('pending', 'published') "
            "ORDER BY created_at ASC, rowid ASC",
            (session_id,),
        ).fetchall()
        for row in rows:
            await self._send_event(
                writer,
                event_id=str(row["event_id"]),
                kind=str(row["kind"]),
                packet_id=str(row["packet_id"]),
                session_id=session_id,
            )
        self._record_replay(session_id, len(rows))

    def _record_replay(self, session_id: str, replay_count: int) -> None:
        """Receipt every replay's exact count — zero is a recorded fact.

        The lead can then prove "nothing was waiting" durably instead
        of inferring it from silence after a reconnect.
        """

        if self._control is None:
            return
        registration = self._database.execute(
            "SELECT project_key, cell_id FROM channel_registrations "
            "WHERE session_id = ? AND state = 'active' "
            "ORDER BY connected_at DESC, rowid DESC LIMIT 1",
            (session_id,),
        ).fetchone()
        if registration is None:
            return
        with suppress(Exception):
            self._control.record(
                kind="channel.replayed",
                project_key=str(registration["project_key"]),
                cell_id=str(registration["cell_id"]),
                session_id=session_id,
                result={"replay_count": replay_count},
            )

    async def _send_event(
        self,
        writer: asyncio.StreamWriter,
        *,
        event_id: str,
        kind: str,
        packet_id: str,
        session_id: str,
    ) -> bool:
        try:
            await self._write(
                writer,
                {
                    "op": "event",
                    "event_id": event_id,
                    "kind": kind,
                    "packet_id": packet_id,
                    "session_id": session_id,
                },
            )
        except Exception:
            return False
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE channel_events SET state = 'published', "
                "attempts = attempts + 1, published_at = ?, "
                "updated_at = ? WHERE event_id = ? "
                "AND state IN ('pending', 'published')",
                (stamp, stamp, event_id),
            )
        return True

    async def _write(
        self, writer: asyncio.StreamWriter, message: dict[str, object]
    ) -> None:
        writer.write((json.dumps(message) + "\n").encode("utf-8"))
        await writer.drain()


def hub_socket_path(state_dir: Path) -> Path:
    """The one canonical hub socket location under the state dir."""

    return state_dir / "channels" / "hub.sock"


def nudge(socket_path: Path, *, timeout: float = 1.0) -> bool:
    """Best-effort trigger: tell a running hub to route pending packets.

    Called by whichever process just committed a durable packet. The
    packet is already durable, so every failure here — no daemon, no
    socket, timeout — is absorbed and the repair tick recovers; a
    nudge carries no data and can never lose or duplicate work.
    """

    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(timeout)
            client.connect(str(socket_path))
            client.sendall(b'{"op": "nudge"}\n')
            reply = client.recv(MAX_LINE_BYTES)
        message = json.loads(reply.split(b"\n", 1)[0])
        return isinstance(message, dict) and message.get("op") == "nudged"
    except (OSError, ValueError):
        return False


class ChannelPacketRouter:
    """Route packets to the channel the moment they commit in-process.

    Subscribes to the wake and correction outboxes inside the daemon:
    each post-commit signal schedules one ``publish_pending`` pass,
    which derives everything from durable state and deduplicates — so
    a committed packet reaches a registered channel immediately
    instead of waiting for the low-frequency repair tick. Outside a
    running event loop the signal is skipped; the durable row and the
    tick remain the truth.
    """

    def __init__(self, hub: ChannelHub) -> None:
        self._hub = hub
        self._tasks: set[asyncio.Task[object]] = set()

    def attach(self, outbox: object) -> None:
        outbox.subscribe(self._on_commit)  # type: ignore[attr-defined]

    def _on_commit(self, _packet: object) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(self._route())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _route(self) -> None:
        with suppress(Exception):
            await self._hub.publish_pending()
