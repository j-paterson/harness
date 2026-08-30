"""Route committed packets to the fakechat wake signal the moment they land.

INFRA-197 (v4 amendment): an IDLE Claude host cannot be woken by any
buffer write, only by the official fakechat channel's loopback
``/upload`` — so a durable ``announced`` delivery must reach that
signal without waiting for a periodic tick. Mirrors
:class:`hermes_orchestrator.channel_hub.ChannelPacketRouter`:
subscribing to the durable outbox that already backs the intake
announcer, scheduling one bounded async pass per commit signal inside
the running daemon loop, and skipping cleanly outside one — the
durable rows and the low-frequency tick remain the sole authority.

:meth:`FakechatWakeRouter.signal_pending` is a pure signaler, never a
mutator: it reads ``announced`` deliveries — durably told to the lead
but not yet leased through the offer poll — joined to sessions that
currently own an active fakechat port, and sends the bounded envelope
to each. It never writes ``lead_intake_deliveries``; that state
machine belongs exclusively to the CLI offer/ack drain
(:mod:`hermes_orchestrator.lead_intake`). A per-row surprise, or a
sender that raises, is contained to that row so one bad delivery can
never block the rest of a pass, and the pass itself is bounded so a
pathological backlog cannot flood fakechat.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any, Protocol

from hermes_orchestrator.db import Database

_PENDING_LIMIT = 32


def _now_utc() -> datetime:
    return datetime.now(UTC)


class _SignalSender(Protocol):
    def send(
        self,
        *,
        session_id: str,
        kind: str,
        packet_id: str,
        delivery_id: str,
    ) -> Any: ...


class FakechatWakeRouter:
    """Signal the fakechat wake for every announced, unleased delivery.

    Subscribes to the same durable outbox the intake announcer already
    journals through (``attach``); each post-commit signal schedules
    one bounded :meth:`signal_pending` pass on the running loop.
    ``signal_pending`` is also directly callable from a periodic daemon
    tick — it derives everything fresh from durable state each time,
    so the two callers can never disagree.
    """

    def __init__(
        self,
        *,
        database: Database,
        sender: _SignalSender,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._sender = sender
        self._now = now or _now_utc
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
            self.signal_pending()

    def signal_pending(self) -> tuple[str, ...]:
        """Send the wake to every announced delivery with an active port.

        Reads, never writes, ``lead_intake_deliveries`` — the offer/ack
        drain owns every transition of that row. Bounded to the oldest
        :data:`_PENDING_LIMIT` rows by ``updated_at`` so a pathological
        backlog cannot flood fakechat in one pass. Returns the delivery
        ids the sender reported ``delivered`` for; a ``refused`` or
        ``failed`` outcome, or a sender that raises outright, is
        contained to its own row and simply omitted.
        """

        rows = self._database.execute(
            "SELECT d.session_id AS session_id, d.kind AS kind, "
            "d.packet_id AS packet_id, d.delivery_id AS delivery_id "
            "FROM lead_intake_deliveries AS d "
            "JOIN fakechat_signal_ports AS p "
            "ON p.session_id = d.session_id AND p.state = 'active' "
            "WHERE d.state = 'announced' "
            "ORDER BY d.updated_at ASC, d.rowid ASC "
            "LIMIT ?",
            (_PENDING_LIMIT,),
        ).fetchall()
        delivered: list[str] = []
        for row in rows:
            session_id = str(row["session_id"])
            kind = str(row["kind"])
            packet_id = str(row["packet_id"])
            delivery_id = str(row["delivery_id"])
            with suppress(Exception):
                outcome = self._sender.send(
                    session_id=session_id,
                    kind=kind,
                    packet_id=packet_id,
                    delivery_id=delivery_id,
                )
                if getattr(outcome, "status", None) == "delivered":
                    delivered.append(delivery_id)
        return tuple(delivered)
