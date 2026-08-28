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
    """Deliver one durable packet id to one exact classic lead surface.

    Delivery is a durable state machine per (kind, packet, session):
    ``claimed`` (a compare-and-swap insert BEFORE any external effect),
    ``attempted`` (recorded before each external typing sequence, and
    what an uncertain or failed attempt durably remains), and
    ``delivered`` (only after cmux acknowledged the whole sequence).
    A concurrent caller finds a fresh claim and types nothing; a stale
    claim or attempt — a crashed or failed owner — is taken over through
    an optimistic compare-and-swap on its exact ``updated_at`` token
    once ``retry_after`` has passed. The adapter's line-reset preamble
    makes every retry idempotent against a text-without-Return partial.
    ``superseded`` rows (migration backfill for packets that predate the
    router) are terminal and never typed.
    """

    def __init__(
        self,
        *,
        database: Database,
        bindings: CmuxSurfaceBindings,
        port: CmuxControlPort,
        ids: Callable[[], str] | None = None,
        now: Callable[[], datetime] | None = None,
        retry_after_seconds: float = 60.0,
    ) -> None:
        self._database = database
        self._bindings = bindings
        self._port = port
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._now = now or (lambda: datetime.now(UTC))
        self._retry_after_seconds = retry_after_seconds

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

        def outcome(status: str) -> IntakeDelivery:
            return IntakeDelivery(
                status=status,
                envelope=envelope,
                binding_id=binding.binding_id,
                surface_uuid=binding.surface_uuid,
            )

        delivery_id = self._claim(
            kind=kind,
            packet_id=packet_id,
            cell_id=cell_id,
            session_id=session_id,
            surface_uuid=binding.surface_uuid,
        )
        if delivery_id is None:
            row = self._database.execute(
                "SELECT delivery_id, state, updated_at "
                "FROM lead_intake_deliveries "
                "WHERE kind = ? AND packet_id = ? AND session_id = ?",
                (kind, packet_id, session_id),
            ).fetchone()
            if row is None or str(row["state"]) in (
                "delivered",
                "superseded",
            ):
                return outcome("deduplicated")
            delivery_id = self._take_over_stale(
                str(row["delivery_id"]), str(row["updated_at"])
            )
            if delivery_id is None:
                # Another owner holds a fresh claim or attempt; this
                # caller types nothing and the packet stays pending.
                return outcome("pending")
        # Record the attempt durably BEFORE the external sequence: an
        # interruption from here on leaves a distinguishable
        # 'attempted' row that a later pass takes over and retries.
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE lead_intake_deliveries "
                "SET state = 'attempted', attempts = attempts + 1, "
                "updated_at = ? WHERE delivery_id = ?",
                (self._now().isoformat(), delivery_id),
            )
        try:
            await self._port.deliver_intake_envelope(binding.ref, envelope)
        except Exception:
            # The external effect is uncertain (text may or may not
            # have landed); the durable 'attempted' row is exactly that
            # evidence, and the retry's line reset prevents any
            # concatenation.
            return outcome("attempt_failed")
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE lead_intake_deliveries "
                "SET state = 'delivered', delivered_at = ?, "
                "updated_at = ? WHERE delivery_id = ?",
                (
                    self._now().isoformat(),
                    self._now().isoformat(),
                    delivery_id,
                ),
            )
        return outcome("delivered")

    def _claim(
        self,
        *,
        kind: str,
        packet_id: str,
        cell_id: str,
        session_id: str,
        surface_uuid: str,
    ) -> str | None:
        """Compare-and-swap the delivery claim; None when already owned."""

        delivery_id = self._ids()
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO lead_intake_deliveries("
                "delivery_id, kind, packet_id, cell_id, session_id, "
                "surface_uuid, state, attempts, claimed_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, 'claimed', 0, ?, ?)",
                (
                    delivery_id,
                    kind,
                    packet_id,
                    cell_id,
                    session_id,
                    surface_uuid,
                    stamp,
                    stamp,
                ),
            )
            if cursor.rowcount == 1:
                return delivery_id
        return None

    def _take_over_stale(
        self, delivery_id: str, observed_updated_at: str
    ) -> str | None:
        """Adopt a crashed or failed owner's claim, or None.

        Eligibility requires the row's last transition to be older than
        ``retry_after``; ownership itself transfers only through a
        compare-and-swap on the exact observed ``updated_at`` token, so
        two takers can never both win.
        """

        try:
            last = datetime.fromisoformat(observed_updated_at)
        except ValueError:
            return None
        age = (self._now() - last).total_seconds()
        if age < self._retry_after_seconds:
            return None
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE lead_intake_deliveries SET updated_at = ? "
                "WHERE delivery_id = ? AND updated_at = ? "
                "AND state IN ('claimed', 'attempted')",
                (
                    self._now().isoformat(),
                    delivery_id,
                    observed_updated_at,
                ),
            )
            if cursor.rowcount == 1:
                return delivery_id
        return None


class LeadIntakeRouter:
    """Route every durable pending packet to its classic seat.

    Restart-safe by derivation, not by memory: each pass re-reads the
    durable sources — corrections still ``pending`` and terminal wakes
    without a delivered (or superseded) intake row — so a crash between
    packet publication and typing can never separate them permanently;
    the next pass finds the packet again. A refused delivery (stale,
    mismatched, non-classic, or dead seat, or no active cell) types
    nothing and leaves the durable packet pending, and the transport's
    state machine keeps repeated passes idempotent.
    """

    def __init__(
        self, *, database: Database, transport: LeadIntakeTransport
    ) -> None:
        self._database = database
        self._transport = transport

    async def tick(self) -> tuple[str, ...]:
        delivered: list[str] = []
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
            await self._route(
                CORRECTION_READY,
                str(row["correction_id"]),
                str(target["cell_id"]),
                str(target["session_id"]),
                delivered,
            )
        wakes = self._database.execute(
            "SELECT w.wake_id, w.cell_id, w.session_id "
            "FROM lead_terminal_wakes AS w "
            "WHERE NOT EXISTS ("
            "SELECT 1 FROM lead_intake_deliveries AS d "
            "WHERE d.kind = 'HERMES_WORK_READY' "
            "AND d.packet_id = w.wake_id "
            "AND d.session_id = w.session_id "
            "AND d.state IN ('delivered', 'superseded')"
            ") ORDER BY w.created_at ASC, w.rowid ASC"
        ).fetchall()
        for row in wakes:
            await self._route(
                WORK_READY,
                str(row["wake_id"]),
                str(row["cell_id"]),
                str(row["session_id"]),
                delivered,
            )
        return tuple(delivered)

    async def _route(
        self,
        kind: str,
        packet_id: str,
        cell_id: str,
        session_id: str,
        delivered: list[str],
    ) -> None:
        try:
            result = await self._transport.deliver(
                kind=kind,
                packet_id=packet_id,
                cell_id=cell_id,
                session_id=session_id,
            )
        except IntakeRefused:
            # Fail closed without losing anything: the durable packet
            # stays pending for a later pass with a valid seat.
            return
        if result.status == "delivered":
            delivered.append(packet_id)
