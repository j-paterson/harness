"""Focus-preserving Hermes lead-intake transport.

INFRA-190: the in-cmux Hermes daemon hands work to a classic in-pane
Claude lead by typing exactly one generated, schema-validated envelope —
``HERMES_CORRECTION_READY <id>`` or ``HERMES_WORK_READY <id>`` — into
the lead's exact bound surface and submitting Return. The id names a
durable packet (a lead correction or a terminal wake) that must already
exist; the lead retrieves the actual packet from SQLite by that id, so
the transport never carries packet contents, prompts, or credentials,
never exposes arbitrary text or keystroke injection (the adapter's
envelope grammar is the complete vocabulary), and never reads terminal
contents. Delivery targets the exact active cell/session/surface
binding, requires recorded classic-seat evidence and a live surface,
deduplicates durably per (kind, packet, session), and fails closed on
anything stale, ambiguous, mismatched, or non-classic. The target
surface is never focused: the operator's focused workspace, selection,
and in-progress typing stay untouched.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from hermes_orchestrator.cmux import CmuxControlPort
from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
from hermes_orchestrator.db import Database

CORRECTION_READY = "HERMES_CORRECTION_READY"
WORK_READY = "HERMES_WORK_READY"

_PACKET_ID = re.compile(r"^[0-9a-f]{32}$")

# Which durable table must already hold the packet for each kind.
_PACKET_SOURCES = {
    CORRECTION_READY: ("lead_corrections", "correction_id"),
    WORK_READY: ("lead_terminal_wakes", "wake_id"),
}


class IntakeRefused(RuntimeError):
    """The envelope may not be delivered; nothing was typed."""


@dataclass(frozen=True, slots=True)
class IntakeDelivery:
    """The outcome of one envelope delivery."""

    status: str  # "delivered" | "deduplicated"
    envelope: str
    binding_id: str
    surface_uuid: str


class LeadIntakeTransport:
    """Deliver one durable packet id to one exact classic lead surface."""

    def __init__(
        self,
        *,
        database: Database,
        bindings: CmuxSurfaceBindings,
        port: CmuxControlPort,
        ids: Callable[[], str] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._bindings = bindings
        self._port = port
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._now = now or (lambda: datetime.now(UTC))

    async def deliver(
        self,
        *,
        kind: str,
        packet_id: str,
        cell_id: str,
        session_id: str,
    ) -> IntakeDelivery:
        source = _PACKET_SOURCES.get(kind)
        if source is None:
            raise IntakeRefused(
                "only HERMES_CORRECTION_READY and HERMES_WORK_READY "
                "envelopes exist"
            )
        if _PACKET_ID.fullmatch(packet_id) is None:
            raise IntakeRefused(
                "the packet id must be one 32-hex durable identity"
            )
        table, column = source
        packet = self._database.execute(
            f"SELECT 1 FROM {table} WHERE {column} = ?",
            (packet_id,),
        ).fetchone()
        if packet is None:
            raise IntakeRefused(
                "no durable packet carries this id; nothing to announce"
            )
        binding = self._bindings.active_lead(cell_id)
        if binding is None:
            raise IntakeRefused(
                "no active seat binding exists for this cell"
            )
        if binding.session_id != session_id:
            raise IntakeRefused(
                "the active seat belongs to a different session; "
                "refusing the stale target"
            )
        if not self._bindings.is_classic(binding.binding_id, session_id):
            raise IntakeRefused(
                "the bound surface has no recorded classic-seat "
                "evidence; envelopes are never typed into a non-classic "
                "surface"
            )
        if not await self._port.surface_alive(binding.ref):
            raise IntakeRefused(
                "the bound surface is no longer live; refusing the "
                "stale binding"
            )
        envelope = f"{kind} {packet_id}"
        duplicate = self._database.execute(
            "SELECT 1 FROM lead_intake_deliveries "
            "WHERE kind = ? AND packet_id = ? AND session_id = ?",
            (kind, packet_id, session_id),
        ).fetchone()
        if duplicate is not None:
            return IntakeDelivery(
                status="deduplicated",
                envelope=envelope,
                binding_id=binding.binding_id,
                surface_uuid=binding.surface_uuid,
            )
        await self._port.deliver_intake_envelope(binding.ref, envelope)
        # Recorded after the send: a crash in between can at worst
        # repeat one envelope, which the id-based packet fetch dedups;
        # recording first could silently lose a delivery forever.
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO lead_intake_deliveries("
                "delivery_id, kind, packet_id, cell_id, session_id, "
                "surface_uuid, delivered_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    self._ids(),
                    kind,
                    packet_id,
                    cell_id,
                    session_id,
                    binding.surface_uuid,
                    self._now().isoformat(),
                ),
            )
        return IntakeDelivery(
            status="delivered",
            envelope=envelope,
            binding_id=binding.binding_id,
            surface_uuid=binding.surface_uuid,
        )
