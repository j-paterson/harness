"""Buffer-free Hermes lead intake: metadata announcements plus a
lead-owned poll handshake.

INFRA-190: work reaches a classic in-pane Claude lead through two
channels, neither of which can touch the operator's editable prompt
buffer — the adapter has no keystroke vocabulary at all, so writing
into a shared interactive buffer is structurally impossible:

* The in-cmux Hermes daemon ANNOUNCES pending packets on the lead's
  bound workspace using already-allow-listed metadata commands only
  (``set-status`` and ``notify``). Nothing is typed anywhere.
* The lead itself POLLS for the next envelope through an
  application-level handshake (``hermes-orchestrator intake-poll``,
  invoked from the lead's own Claude Code hook at its own turn
  boundary). The poll returns exactly one schema-validated envelope —
  ``HERMES_CORRECTION_READY <id>`` or ``HERMES_WORK_READY <id>`` —
  through the hook channel, which does not share the interactive
  buffer; the lead then retrieves the actual packet from SQLite by its
  id. Because the lead asks, the handshake is exclusively lead-owned
  by construction.

Announcements run a durable claimed/attempted/announced state machine
per (kind, packet, session): the claim is a compare-and-swap insert
before any external effect, uncertain metadata attempts remain durable
and retry after a window (a repeated notification is harmless), and
``announced`` is terminal for the announcer. The poll leases an
OFFER under an opaque token; only the lead's explicit
acknowledgement — bound to the exact session, packet, and token —
records ``delivered``, and an unacknowledged offer is re-offered once
its lease expires. Stale, mismatched, non-classic, or dead seats,
unknown packets, and every cmux probe failure are refused with the
durable packet retained — losing an envelope never loses work, because
the packet itself stays recoverable in durable state.
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

# The complete envelope grammar: one kind, one 32-hex durable packet
# id. This is the only shape the poll may ever hand to a lead.
INTAKE_ENVELOPE_PATTERN = re.compile(
    r"^(HERMES_CORRECTION_READY|HERMES_WORK_READY) [0-9a-f]{32}$"
)

_PACKET_ID = re.compile(r"^[0-9a-f]{32}$")

# Which durable table must already hold the packet for each kind.
_PACKET_SOURCES = {
    CORRECTION_READY: ("lead_corrections", "correction_id"),
    WORK_READY: ("lead_terminal_wakes", "wake_id"),
}


class IntakeRefused(RuntimeError):
    """The announcement may not proceed; nothing external happened."""


@dataclass(frozen=True, slots=True)
class IntakeDelivery:
    """The outcome of one announcement attempt."""

    status: str  # "announced" | "deduplicated" | "pending" | "attempt_failed"
    envelope: str
    binding_id: str
    surface_uuid: str


class LeadIntakeTransport:
    """Announce one durable packet on its classic lead's workspace.

    The external effect is metadata only — a workspace status and one
    notification — so no state of any prompt buffer is relevant and no
    operator input can ever be altered. The durable state machine still
    guarantees at-most-one announcer per (kind, packet, session):
    ``claimed`` rows are compare-and-swapped before the external
    effect, uncertain attempts stay ``attempted`` and become eligible
    again after ``retry_after`` (repeated metadata is harmless), and
    ``announced``/``delivered``/``superseded`` are terminal here.
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
                "evidence; refusing the non-classic seat"
            )
        envelope = f"{kind} {packet_id}"

        def outcome(status: str) -> IntakeDelivery:
            return IntakeDelivery(
                status=status,
                envelope=envelope,
                binding_id=binding.binding_id,
                surface_uuid=binding.surface_uuid,
            )

        # Terminal dedup before any external probe.
        existing = self._database.execute(
            "SELECT delivery_id, state, updated_at "
            "FROM lead_intake_deliveries "
            "WHERE kind = ? AND packet_id = ? AND session_id = ?",
            (kind, packet_id, session_id),
        ).fetchone()
        if existing is not None and str(existing["state"]) in (
            "announced",
            "offered",
            "delivered",
            "superseded",
        ):
            return outcome("deduplicated")
        # Optional-cmux containment: a denial, timeout, protocol
        # failure, or any other adapter error during the liveness probe
        # refuses this announcement and retains the durable packet — it
        # can never escape into the daemon's startup or maintenance
        # pass.
        try:
            alive = await self._port.surface_alive(binding.ref)
        except Exception as error:
            raise IntakeRefused(
                "cmux was unavailable during liveness validation "
                f"({type(error).__name__}); the packet is retained"
            ) from None
        if not alive:
            raise IntakeRefused(
                "the bound surface is no longer live; refusing the "
                "stale binding"
            )
        if existing is None:
            delivery_id = self._claim(
                kind=kind,
                packet_id=packet_id,
                cell_id=cell_id,
                session_id=session_id,
                surface_uuid=binding.surface_uuid,
            )
            if delivery_id is None:
                # Lost the claim race since the read above; the winner
                # owns the announcement.
                return outcome("pending")
        else:
            delivery_id = self._take_over_stale(
                str(existing["delivery_id"]), str(existing["updated_at"])
            )
            if delivery_id is None:
                return outcome("pending")
        # Record the attempt durably BEFORE the external effect; the
        # state guard plus rowcount check abandon the pass if a poll
        # meanwhile offered this row to the lead.
        with self._database.transaction() as connection:
            attempted = connection.execute(
                "UPDATE lead_intake_deliveries "
                "SET state = 'attempted', attempts = attempts + 1, "
                "updated_at = ? WHERE delivery_id = ? "
                "AND state IN ('claimed', 'attempted')",
                (self._now().isoformat(), delivery_id),
            )
            if attempted.rowcount != 1:
                return outcome("pending")
        try:
            await self._port.set_status(
                binding.workspace_uuid, "intake", envelope
            )
            await self._port.notify(
                binding.workspace_uuid,
                "Hermes intake pending",
                envelope,
            )
        except Exception:
            # The metadata effect is uncertain; the durable 'attempted'
            # row is exactly that evidence and a later pass retries.
            return outcome("attempt_failed")
        with self._database.transaction() as connection:
            announced = connection.execute(
                "UPDATE lead_intake_deliveries "
                "SET state = 'announced', updated_at = ? "
                "WHERE delivery_id = ? AND state = 'attempted'",
                (self._now().isoformat(), delivery_id),
            )
            if announced.rowcount != 1:
                return outcome("pending")
        return outcome("announced")

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
        """Adopt a crashed or failed announcer's row, or None.

        Eligibility requires the row's last transition to be older than
        ``retry_after``; ownership transfers only through a
        compare-and-swap on the exact observed ``updated_at`` token, so
        two takers can never both win. Retrying an uncertain metadata
        announcement is harmless — at worst a notification repeats.
        """

        try:
            last = datetime.fromisoformat(observed_updated_at)
        except ValueError:
            return None
        if (self._now() - last).total_seconds() < self._retry_after_seconds:
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


@dataclass(frozen=True, slots=True)
class IntakeOffer:
    """One leased envelope offer awaiting the lead's acknowledgement."""

    envelope: str
    kind: str
    packet_id: str
    offer_token: str


class LeadIntakePoll:
    """The lead-owned handshake: an offer/acknowledgement lease.

    A poll compare-and-swaps one packet's delivery row into ``offered``
    under an opaque token and returns the envelope plus that token
    through the application layer (the lead's own hook) — never any
    terminal buffer. Nothing is marked delivered by the poll itself: a
    crash, broken stdout pipe, serialization failure, or hook-host
    rejection after the offer leaves the row ``offered``, and once the
    lease expires the packet is safely re-offered under a fresh token.
    Only the lead's explicit :meth:`acknowledge` — bound to the exact
    session, packet, and offer token — records ``delivered``. Every
    acquisition and acknowledgement is a rowcount-checked
    compare-and-swap, so two polls can never own one offer and a stale,
    foreign-session, wrong-packet, duplicate, or superseded
    acknowledgement is rejected without effect.
    """

    def __init__(
        self,
        *,
        database: Database,
        ids: Callable[[], str] | None = None,
        now: Callable[[], datetime] | None = None,
        offer_ttl_seconds: float = 300.0,
    ) -> None:
        self._database = database
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._now = now or (lambda: datetime.now(UTC))
        self._offer_ttl_seconds = offer_ttl_seconds

    def next_offer(self, session_id: str) -> IntakeOffer | None:
        cell = self._database.execute(
            "SELECT cell_id, project_key FROM project_cells "
            "WHERE session_id = ? AND state = 'active' "
            "ORDER BY updated_at DESC, rowid DESC LIMIT 1",
            (session_id,),
        ).fetchone()
        if cell is None:
            return None
        candidates: list[tuple[str, str, str]] = []
        for row in self._database.execute(
            "SELECT correction_id, created_at FROM lead_corrections "
            "WHERE state = 'pending' AND project_key = ? "
            "ORDER BY created_at ASC, rowid ASC",
            (str(cell["project_key"]),),
        ).fetchall():
            candidates.append(
                (
                    str(row["created_at"]),
                    CORRECTION_READY,
                    str(row["correction_id"]),
                )
            )
        for row in self._database.execute(
            "SELECT wake_id, created_at FROM lead_terminal_wakes "
            "WHERE session_id = ? ORDER BY created_at ASC, rowid ASC",
            (session_id,),
        ).fetchall():
            candidates.append(
                (str(row["created_at"]), WORK_READY, str(row["wake_id"]))
            )
        for _, kind, packet_id in sorted(candidates):
            envelope = f"{kind} {packet_id}"
            if INTAKE_ENVELOPE_PATTERN.fullmatch(envelope) is None:
                continue
            token = self._acquire(
                kind=kind,
                packet_id=packet_id,
                cell_id=str(cell["cell_id"]),
                session_id=session_id,
            )
            if token is not None:
                return IntakeOffer(
                    envelope=envelope,
                    kind=kind,
                    packet_id=packet_id,
                    offer_token=token,
                )
        return None

    def acknowledge(
        self, *, session_id: str, packet_id: str, offer_token: str
    ) -> bool:
        """Record the lead's consumption of one exact offer.

        The compare-and-swap binds all three identities: only the row
        currently ``offered`` under exactly this token, for exactly
        this packet and session, becomes ``delivered``. Anything else —
        an unknown or superseded token, a foreign session, the wrong
        packet, or a duplicate acknowledgement — changes nothing and
        returns False.
        """

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE lead_intake_deliveries "
                "SET state = 'delivered', delivered_at = ?, "
                "updated_at = ?, offer_token = NULL, offered_at = NULL "
                "WHERE session_id = ? AND packet_id = ? "
                "AND offer_token = ? AND state = 'offered'",
                (stamp, stamp, session_id, packet_id, offer_token),
            )
            return cursor.rowcount == 1

    def _acquire(
        self,
        *,
        kind: str,
        packet_id: str,
        cell_id: str,
        session_id: str,
    ) -> str | None:
        """Lease one packet as an offer, or None when it is unavailable.

        A missing row is claimed with a rowcount-checked insert; an
        existing non-terminal row is taken only when no unexpired offer
        lease holds it, through a compare-and-swap on its exact
        ``updated_at`` token. Both losers walk away empty-handed.
        """

        token = self._ids()
        stamp = self._now().isoformat()
        row = self._database.execute(
            "SELECT delivery_id, state, offered_at, updated_at "
            "FROM lead_intake_deliveries "
            "WHERE kind = ? AND packet_id = ? AND session_id = ?",
            (kind, packet_id, session_id),
        ).fetchone()
        if row is None:
            with self._database.transaction() as connection:
                cursor = connection.execute(
                    "INSERT OR IGNORE INTO lead_intake_deliveries("
                    "delivery_id, kind, packet_id, cell_id, session_id, "
                    "surface_uuid, state, attempts, offer_token, "
                    "offered_at, claimed_at, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, '', 'offered', 0, ?, ?, "
                    "?, ?)",
                    (
                        self._ids(),
                        kind,
                        packet_id,
                        cell_id,
                        session_id,
                        token,
                        stamp,
                        stamp,
                        stamp,
                    ),
                )
                if cursor.rowcount == 1:
                    return token
            return None
        state = str(row["state"])
        if state in ("delivered", "superseded"):
            return None
        if state == "offered":
            offered_at = row["offered_at"]
            try:
                age = (
                    self._now()
                    - datetime.fromisoformat(str(offered_at))
                ).total_seconds()
            except ValueError:
                age = None
            if age is not None and age < self._offer_ttl_seconds:
                # An unexpired lease has exactly one owner already.
                return None
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE lead_intake_deliveries "
                "SET state = 'offered', offer_token = ?, "
                "offered_at = ?, updated_at = ? "
                "WHERE delivery_id = ? AND updated_at = ? "
                "AND state NOT IN ('delivered', 'superseded')",
                (
                    token,
                    stamp,
                    stamp,
                    str(row["delivery_id"]),
                    str(row["updated_at"]),
                ),
            )
            if cursor.rowcount == 1:
                return token
        return None


class LeadIntakeRouter:
    """Announce every durable pending packet on its classic seat.

    Restart-safe by derivation, not by memory: each pass re-reads the
    durable sources — corrections still ``pending`` and terminal wakes
    without a terminal intake row — so a crash between packet
    publication and its announcement can never separate them
    permanently. A refused announcement (stale, mismatched,
    non-classic, or dead seat, no active cell, or any cmux failure)
    does nothing external and leaves the durable packet pending.
    """

    def __init__(
        self, *, database: Database, transport: LeadIntakeTransport
    ) -> None:
        self._database = database
        self._transport = transport

    async def tick(self) -> tuple[str, ...]:
        announced: list[str] = []
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
                announced,
            )
        wakes = self._database.execute(
            "SELECT w.wake_id, w.cell_id, w.session_id "
            "FROM lead_terminal_wakes AS w "
            "WHERE NOT EXISTS ("
            "SELECT 1 FROM lead_intake_deliveries AS d "
            "WHERE d.kind = 'HERMES_WORK_READY' "
            "AND d.packet_id = w.wake_id "
            "AND d.session_id = w.session_id "
            "AND d.state IN ('announced', 'offered', 'delivered', 'superseded')"
            ") ORDER BY w.created_at ASC, w.rowid ASC"
        ).fetchall()
        for row in wakes:
            await self._route(
                WORK_READY,
                str(row["wake_id"]),
                str(row["cell_id"]),
                str(row["session_id"]),
                announced,
            )
        return tuple(announced)

    async def _route(
        self,
        kind: str,
        packet_id: str,
        cell_id: str,
        session_id: str,
        announced: list[str],
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
        except Exception:
            # The intake channel is optional: no adapter or database
            # surprise on one packet may take the daemon's startup or
            # maintenance pass down. The durable packet stays pending.
            return
        if result.status == "announced":
            announced.append(packet_id)
