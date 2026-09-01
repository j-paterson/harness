"""Durable outbox of Claude lead terminal wakes.

INFRA-181: Hermes never polls a lead's progress. When a lead turn reaches a
terminal boundary — completed, provider capped, blocked, or handoff
required — exactly one schema-versioned wake row is committed here, bound to
the authoritative project/cell/session/turn identity and deduplicated by
that identity. Subscribers receive the wake in-process only after the
durable commit; rows still pending at startup are replayed for delivery
exactly once. Rows carry orchestration metadata only, never prompts or
responses.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import uuid
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

WAKE_SCHEMA_VERSION = 1

WAKE_KINDS = frozenset(
    {
        "completed",
        "provider_capped",
        "blocked",
        "handoff_required",
        # INFRA-215: the four above are TERMINAL-boundary signals, each
        # committed because a turn ended; none can tell an idle seat the
        # queue still holds safe admitted work.
        "work_ready",
    }
)


@dataclass(frozen=True, slots=True)
class TerminalWakeInput:
    """The terminal-boundary identity and metadata for one lead turn."""

    project_key: str
    issue_id: str
    cell_id: str
    session_id: UUID
    profile_alias: str
    turn_key: str
    kind: str
    reason: str
    reset_at: str | None = None


@dataclass(frozen=True, slots=True)
class TerminalWake:
    """One durable pending or delivered terminal wake."""

    wake_id: str
    schema_version: int
    project_key: str
    issue_id: str
    cell_id: str
    session_id: str
    profile_alias: str
    turn_key: str
    kind: str
    reason: str
    reset_at: str | None
    state: str
    created_at: str
    delivered_at: str | None

    def as_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


class LeadTerminalWakes:
    """Commit, signal, and replay lead terminal wakes exactly once."""

    def __init__(
        self,
        *,
        database: Database,
        events: EventStore,
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._now = now or (lambda: datetime.now(UTC))
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._listeners: list[Callable[[TerminalWake], None]] = []

    def subscribe(self, listener: Callable[[TerminalWake], None]) -> None:
        """Register an in-process delivery signal for newly committed wakes."""

        self._listeners.append(listener)

    def commit(self, wake: TerminalWakeInput) -> TerminalWake:
        """Durably commit one wake per turn identity, then signal it.

        A repeated commit for the same project/cell/session/turn/kind
        returns the existing row, journals nothing, and signals nothing:
        the wake was already published. Listeners run only after the
        transaction committed, and a listener failure never rolls back or
        marks the row — the row stays pending for replay.
        """

        if wake.kind not in WAKE_KINDS:
            raise ValueError(f"unknown terminal wake kind: {wake.kind}")
        wake_id = self._ids()
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            # The unique turn-identity index is the dedup authority; the
            # conflict clause makes check-and-insert one atomic statement.
            cursor = connection.execute(
                "INSERT INTO lead_terminal_wakes("
                "wake_id, schema_version, project_key, issue_id, cell_id, "
                "session_id, profile_alias, turn_key, kind, reason, reset_at, "
                "state, created_at, delivered_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL) "
                "ON CONFLICT(project_key, cell_id, session_id, turn_key, kind) "
                "DO NOTHING",
                (
                    wake_id,
                    WAKE_SCHEMA_VERSION,
                    wake.project_key,
                    wake.issue_id,
                    wake.cell_id,
                    str(wake.session_id),
                    wake.profile_alias,
                    wake.turn_key,
                    wake.kind,
                    wake.reason,
                    wake.reset_at,
                    stamp,
                ),
            )
            inserted = cursor.rowcount == 1
            if inserted:
                self._events.append(
                    connection,
                    EventInput(
                        event_type="lead_wake.committed",
                        aggregate_type="lead_wake",
                        aggregate_id=wake_id,
                        correlation_id=wake.turn_key,
                        payload={
                            "project_key": wake.project_key,
                            "issue_id": wake.issue_id,
                            "cell_id": wake.cell_id,
                            "session_id": str(wake.session_id),
                            "kind": wake.kind,
                            "reason": wake.reason,
                            "reset_at": wake.reset_at,
                        },
                    ),
                )
        if not inserted:
            row = self._database.execute(
                "SELECT wake_id FROM lead_terminal_wakes WHERE project_key = ? "
                "AND cell_id = ? AND session_id = ? AND turn_key = ? "
                "AND kind = ?",
                (
                    wake.project_key,
                    wake.cell_id,
                    str(wake.session_id),
                    wake.turn_key,
                    wake.kind,
                ),
            ).fetchone()
            return self.get(str(row["wake_id"]))
        committed = self.get(wake_id)
        self._signal(committed)
        return committed

    def pending(self, project_key: str | None = None) -> tuple[TerminalWake, ...]:
        if project_key is None:
            rows = self._database.execute(
                "SELECT * FROM lead_terminal_wakes WHERE state = 'pending' "
                "ORDER BY created_at ASC, rowid ASC"
            ).fetchall()
        else:
            rows = self._database.execute(
                "SELECT * FROM lead_terminal_wakes WHERE state = 'pending' "
                "AND project_key = ? ORDER BY created_at ASC, rowid ASC",
                (project_key,),
            ).fetchall()
        return tuple(_row_to_wake(row) for row in rows)

    def replay_undelivered(self) -> tuple[TerminalWake, ...]:
        """Return wakes committed but not yet delivered, oldest first.

        Startup replay: the caller delivers each wake and acknowledges it
        with :meth:`mark_delivered`, so a wake that survives a crash between
        commit and delivery is delivered exactly once overall.
        """

        return self.pending()

    def mark_delivered(self, wake_id: str) -> TerminalWake:
        """Acknowledge one delivery; idempotent for an already delivered row."""

        current = self.get(wake_id)
        if current.state != "pending":
            return current
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE lead_terminal_wakes SET state = 'delivered', "
                "delivered_at = ? WHERE wake_id = ? AND state = 'pending'",
                (stamp, wake_id),
            )
            if cursor.rowcount == 1:
                self._events.append(
                    connection,
                    EventInput(
                        event_type="lead_wake.delivered",
                        aggregate_type="lead_wake",
                        aggregate_id=wake_id,
                        payload={},
                    ),
                )
        return self.get(wake_id)

    def get(self, wake_id: str) -> TerminalWake:
        row = self._database.execute(
            "SELECT * FROM lead_terminal_wakes WHERE wake_id = ?",
            (wake_id,),
        ).fetchone()
        if row is None:
            raise KeyError(wake_id)
        return _row_to_wake(row)

    def _seat_is_idle(self, session_id: str) -> bool:
        """True only when a Stop is NEWER than the last turn start.

        INFRA-215 (Sol acce71fc): an acknowledged assignment with no
        started child and no waiting continuation is NOT proof of
        idleness -- a lead executing its own foreground turn has exactly
        that durable shape, because ``lead_children`` and
        ``lead_continuations`` track only background work around Stop.
        Treating it as idle delivered ``work_ready`` into a busy turn.

        Idleness is therefore a COMPARISON over the existing journal,
        not a second definition: the session's latest unconditional
        ``lead_turn.stopped`` record must be strictly newer than its
        latest managed turn-STARTING input -- an assignment
        acknowledgement or a channel event that reached a lead. A seat
        that has started a turn since its last Stop is busy; a seat that
        has never stopped is not yet proven idle.

        Sol 6c08a91e: the channel side is ordered by the durable
        PUBLICATION/ACKNOWLEDGEMENT transition, not by ``created_at``,
        and spans ``published`` AND ``acked``. Acknowledgement is what
        hands the packet to a foreground turn, so an ``acked`` event is
        the strongest evidence of a start -- yet it used to leave the
        query the instant it was consumed, and an event created before
        the last Stop but published or acked after it used to order
        before that Stop. Both let a stale Stop look newest while a
        just-consumed wake drove a live turn. The per-row timestamp
        falls back ``acked_at`` -> ``published_at`` -> ``updated_at``:
        an ack may transition straight from ``pending`` (leaving
        ``published_at`` NULL), and ``updated_at`` is NOT NULL, so a row
        in either state can never contribute a NULL start and silently
        drop out of the comparison.

        Sol 3c73856e: the ordering above is necessary but not
        sufficient. A ``published`` assignment is work already handed to
        this lead and NOT yet consumed, and until it is acknowledged it
        contributes no start timestamp at all -- neither does its
        channel event while that event is still ``pending``. An older
        Stop was therefore free to look newest and win, committing a
        second ``work_ready`` onto a seat that already holds unconsumed
        work. Any such assignment fails the seat closed, reusing that
        durable state rather than inferring delivery from an
        unpublished event.

        Sol 62424477: that guard is an EXISTENCE test over the session,
        never a look at its newest row. ``lead_assignments_live`` is
        unique by ``(issue_id, session_id)``, so one session legitimately
        holds several live assignments at once; ordering by recency let
        a newer acknowledged one hide an older still-published packet.
        ``state = 'published'`` already excludes ``superseded``, which
        stays irrelevant here.
        """

        unconsumed = self._database.scalar(
            "SELECT 1 FROM lead_assignments "
            "WHERE session_id = ? AND state = 'published' LIMIT 1",
            (session_id,),
        )
        if unconsumed is not None:
            return False
        stopped = self._database.scalar(
            "SELECT MAX(occurred_at) FROM events "
            "WHERE aggregate_type = 'lead_session' AND aggregate_id = ? "
            "AND event_type = 'lead_turn.stopped'",
            (session_id,),
        )
        if not stopped:
            return False
        started = self._database.scalar(
            "SELECT MAX(started) FROM ("
            "SELECT MAX(acknowledged_at) AS started FROM lead_assignments "
            "WHERE session_id = ? AND state = 'acknowledged' "
            "UNION ALL "
            "SELECT MAX(COALESCE(acked_at, published_at, updated_at)) "
            "AS started FROM channel_events "
            "WHERE session_id = ? AND state IN ('published', 'acked'))",
            (session_id, session_id),
        )
        return started is None or str(stopped) > str(started)

    def commit_work_ready(
        self, project_key: str, *, freshness_minutes: int
    ) -> TerminalWake | None:
        """Wake an IDLE development seat that has safe admitted work.

        Silent (``None``) unless every durable predicate holds: an
        ACTIVE development cell (never the harness lane) whose seat is
        genuinely idle by :meth:`_seat_is_idle`; a resource sample
        fresh enough to authorize the issue's priority
        (:func:`cells.admission_priority_ceiling`, the ONE shared
        budget the daemon dispatch path and the Stop-hook idle
        dispatcher already run, and which fails closed on a missing,
        stale, or red sample); an admitted issue genuinely runnable
        (``queued`` AND ``dependency_ready``) that is NOT already bound
        to a live ``worktree_leases`` row — a bound lane is work
        already in progress, not work to dispatch again; and the
        canonical :func:`cells.development_lane_saturated` bound.

        The turn key is the runnable SET, never a clock, so an
        unchanged condition re-commits nothing however often this
        runs; a changed set is a new condition and wakes once more.
        """

        from hermes_orchestrator.cells import (
            admission_priority_ceiling,
            development_lane_saturated,
        )

        cell = self._database.execute(
            "SELECT cell_id, session_id, profile_alias FROM project_cells "
            "WHERE project_key = ? AND lane_role = 'development' "
            "AND state = 'active' "
            "ORDER BY updated_at DESC, rowid DESC LIMIT 1",
            (project_key,),
        ).fetchone()
        if cell is None or not self._seat_is_idle(str(cell["session_id"])):
            return None
        ceiling = admission_priority_ceiling(
            self._database,
            now=self._now(),
            freshness_minutes=freshness_minutes,
        )
        if ceiling is None:
            return None
        runnable = [
            str(row["issue_id"])
            for row in self._database.execute(
                "SELECT issue_id FROM admitted_issues WHERE project_key = ? "
                "AND state = 'queued' AND dependency_ready = 1 "
                "AND priority <= ? AND issue_id NOT IN ("
                "SELECT issue_id FROM worktree_leases "
                "WHERE project_key = ? AND state != 'reclaimed') "
                "ORDER BY issue_id",
                (project_key, ceiling, project_key),
            ).fetchall()
        ]
        if not runnable:
            return None
        if development_lane_saturated(
            self._database, project_key=project_key, issue_id=runnable[0]
        ):
            return None
        return self.commit(
            TerminalWakeInput(
                project_key=project_key,
                issue_id=runnable[0],
                cell_id=str(cell["cell_id"]),
                session_id=UUID(str(cell["session_id"])),
                profile_alias=str(cell["profile_alias"]),
                turn_key="work-ready:" + ",".join(runnable),
                kind="work_ready",
                reason=f"{len(runnable)} admitted issue(s) runnable now",
            )
        )

    def _signal(self, wake: TerminalWake) -> None:
        for listener in self._listeners:
            try:
                listener(wake)
            except Exception:
                # The durable row is the truth; a failed in-process signal
                # leaves it pending for startup replay or the repair sweep.
                continue


WakeTransport = Callable[[TerminalWake], Awaitable[bool]]

ProcessFactory = Callable[..., Awaitable[asyncio.subprocess.Process]]


class CommandWakeTransport:
    """Push one wake to the configured Hermes consumer command.

    The command receives the wake's orchestration metadata as one JSON
    document on stdin — ``wake_id`` is the consumer's idempotency key —
    and exit status zero is the only acceptance signal. A non-zero exit,
    spawn failure, or timeout reports the wake unaccepted so the row stays
    pending; the supervisor interval is the metadata-only retry backoff.
    """

    def __init__(
        self,
        argv: Sequence[str],
        *,
        process_factory: ProcessFactory = asyncio.create_subprocess_exec,
        timeout_seconds: float = 30.0,
    ) -> None:
        if not argv:
            raise ValueError("wake transport requires a consumer command")
        if timeout_seconds <= 0:
            raise ValueError("wake transport timeout must be positive")
        self._argv = tuple(argv)
        self._process_factory = process_factory
        self._timeout_seconds = timeout_seconds

    async def __call__(self, wake: TerminalWake) -> bool:
        payload = json.dumps(
            wake.as_dict(), sort_keys=True, separators=(",", ":")
        )
        process = await self._process_factory(
            *self._argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            await asyncio.wait_for(
                process.communicate(payload.encode("utf-8")),
                timeout=self._timeout_seconds,
            )
        except TimeoutError:
            # A hung consumer must not block the supervisor tick; the
            # unacknowledged row retries on the next interval.
            with suppress(ProcessLookupError):
                process.kill()
            await process.wait()
            return False
        return process.returncode == 0

# Only forward progress may end the supervisor's interval wait early: a
# completed turn schedules follow-on work now, and a required handoff needs
# a prompt rotation. A blocked or capped turn must keep the interval as its
# retry backoff, or a fast-failing lead start would hot-loop the daemon.
_IMMEDIATE_WAKE_KINDS = frozenset({"completed", "handoff_required"})


class LeadWakeDelivery:
    """Drive exactly-once acknowledgement of committed wakes.

    The outbox rows are the truth; this driver only converts them into
    deliveries. Committing a forward-progress wake sets the in-process
    ``signal`` so the event-driven loop reacts immediately instead of
    polling. ``drain`` runs once per tick: it pushes still-pending rows
    through the transport in commit order and acknowledges each success;
    without a transport it costs nothing and the rows stay pending for the
    Hermes ``pending_wakes``/``ack_wake`` surface — the metadata-only
    fallback when direct delivery is unavailable. ``replay_startup``
    re-arms the signal for rows that survived a crash between commit and
    delivery.
    """

    def __init__(
        self,
        wakes: LeadTerminalWakes,
        *,
        transport: WakeTransport | None = None,
    ) -> None:
        self._wakes = wakes
        self._transport = transport
        self.signal = asyncio.Event()
        wakes.subscribe(self._on_commit)

    def _on_commit(self, wake: TerminalWake) -> None:
        if wake.kind in _IMMEDIATE_WAKE_KINDS:
            self.signal.set()

    def replay_startup(self) -> int:
        """Arm the signal for undelivered rows; returns how many exist."""

        pending = self._wakes.replay_undelivered()
        if pending:
            self.signal.set()
        return len(pending)

    async def drain(self) -> tuple[str, ...]:
        """Deliver pending wakes in commit order, acknowledging successes.

        The first failed or raising transport call ends the pass: rows keep
        their commit order and retry on the next tick, and a broken
        transport can never take the supervisor loop down with it.
        """

        self.signal.clear()
        if self._transport is None:
            return ()
        delivered: list[str] = []
        for wake in self._wakes.pending():
            try:
                accepted = await self._transport(wake)
            except Exception:
                break
            if not accepted:
                break
            self._wakes.mark_delivered(wake.wake_id)
            delivered.append(wake.wake_id)
        return tuple(delivered)


_EVIDENCE_EVENT_TYPES = (
    "checkpoint_safety.safe",
    "project_cell.handoff_required",
    "project_cell.start_failed",
    "project_cell.issue_already_completed",
)

_SUPERSEDING_EVENT_TYPES = (
    "project_cell.handoff_required",
    "project_cell.start_failed",
)


class LeadWakeReconciler:
    """Reconstruct wakes lost between a terminal commit and outbox insert.

    Every terminal lead boundary journals identity-complete durable
    evidence at its transition: a completed turn writes
    ``checkpoint_safety.safe``, a handoff or in-session provider cap
    writes ``project_cell.handoff_required``, a failed start writes
    ``project_cell.start_failed``, and a turn that observed its issue
    already done writes ``project_cell.issue_already_completed``. This
    pass replays that evidence above a migration-recorded floor and
    inserts any missing wake through the same deduplicated turn identity
    the direct path binds, so repeated repair, startup replay, and a raced
    direct commit all converge on one row. Evidence below the floor
    (journaled before the outbox schema existed) is never resurrected, and
    evidence whose session no longer matches the cell is skipped: rotating
    past a boundary proves its consumer saw it.
    """

    def __init__(
        self,
        *,
        database: Database,
        events: EventStore,
        wakes: LeadTerminalWakes,
    ) -> None:
        self._database = database
        self._events = events
        self._wakes = wakes

    def reconcile(self) -> tuple[TerminalWake, ...]:
        """Insert every derivable missing wake; returns only new rows."""

        floor = int(
            self._database.scalar(
                "SELECT floor_sequence FROM lead_wake_repair WHERE id = 1"
            )
            or 0
        )
        placeholders = ",".join("?" for _ in _EVIDENCE_EVENT_TYPES)
        rows = self._database.execute(
            f"SELECT sequence, event_id, aggregate_id, event_type, "
            f"payload_json FROM events WHERE sequence > ? "
            f"AND event_type IN ({placeholders}) ORDER BY sequence ASC",
            (floor, *_EVIDENCE_EVENT_TYPES),
        ).fetchall()
        reconstructed: list[TerminalWake] = []
        advanced = floor
        for row in rows:
            try:
                wake = self._reconstruct(row)
            except Exception:
                # Repair must never wedge the daemon. The floor stays below
                # this evidence, so the next startup retries it.
                with suppress(Exception), (
                    self._database.transaction()
                ) as connection:
                    self._events.append(
                        connection,
                        EventInput(
                            event_type="lead_wake.repair_failed",
                            aggregate_type="lead_wake",
                            aggregate_id=str(row["event_id"]),
                            payload={"event_type": str(row["event_type"])},
                        ),
                    )
                break
            if wake is not None:
                reconstructed.append(wake)
            advanced = int(row["sequence"])
        if advanced != floor:
            with self._database.transaction() as connection:
                connection.execute(
                    "UPDATE lead_wake_repair SET floor_sequence = ? "
                    "WHERE id = 1 AND floor_sequence = ?",
                    (advanced, floor),
                )
        return tuple(reconstructed)

    def _reconstruct(self, row: Any) -> TerminalWake | None:
        cell_id = str(row["aggregate_id"])
        cell = self._database.execute(
            "SELECT project_key, state, profile_alias, session_id "
            "FROM project_cells WHERE cell_id = ?",
            (cell_id,),
        ).fetchone()
        if cell is None:
            return None
        payload = json.loads(row["payload_json"])
        event_type = str(row["event_type"])
        if event_type == "checkpoint_safety.safe":
            wake = self._completed_evidence(row, payload, cell, cell_id)
        elif event_type == "project_cell.handoff_required":
            wake = self._handoff_evidence(row, payload, cell, cell_id)
        elif event_type == "project_cell.issue_already_completed":
            wake = self._already_completed_evidence(row, payload, cell, cell_id)
        else:
            wake = self._start_failed_evidence(row, payload, cell, cell_id)
        if wake is None or self._exists(wake):
            return None
        return self._wakes.commit(wake)

    def _completed_evidence(
        self, row: Any, payload: dict[str, Any], cell: Any, cell_id: str
    ) -> TerminalWakeInput | None:
        session = payload.get("session_id")
        evidence_id = payload.get("evidence_id")
        if payload.get("boundary_kind") != "turn_completed":
            return None
        if not session or not evidence_id:
            return None
        if str(cell["session_id"]) != str(session):
            return None
        # A later terminal transition on the same cell supersedes the safe
        # boundary: the direct path's status override published that kind.
        placeholders = ",".join("?" for _ in _SUPERSEDING_EVENT_TYPES)
        superseded = self._database.scalar(
            f"SELECT count(*) FROM events "
            f"WHERE aggregate_type = 'project_cell' AND aggregate_id = ? "
            f"AND sequence > ? AND event_type IN ({placeholders})",
            (cell_id, int(row["sequence"]), *_SUPERSEDING_EVENT_TYPES),
        )
        if int(superseded or 0):
            return None
        issue_id = self._issue_for(cell_id, int(row["sequence"]))
        if issue_id is None:
            return None
        return TerminalWakeInput(
            project_key=str(cell["project_key"]),
            issue_id=issue_id,
            cell_id=cell_id,
            session_id=UUID(str(session)),
            profile_alias=str(cell["profile_alias"]),
            turn_key=str(evidence_id),
            kind="completed",
            reason="turn_completed",
        )

    def _handoff_evidence(
        self, row: Any, payload: dict[str, Any], cell: Any, cell_id: str
    ) -> TerminalWakeInput | None:
        if str(cell["state"]) != "handoff_required":
            return None
        session = payload.get("session_id")
        if not session or str(cell["session_id"]) != str(session):
            return None
        issue_id = self._issue_for(cell_id, int(row["sequence"]))
        if issue_id is None:
            return None
        if str(payload.get("reason", "")) == "subscription_limit":
            kind, reason = "provider_capped", "subscription_limit"
            reset_at = self._reset_at(str(cell["profile_alias"]))
        else:
            kind, reason, reset_at = (
                "handoff_required",
                "context_rotation",
                None,
            )
        return TerminalWakeInput(
            project_key=str(cell["project_key"]),
            issue_id=issue_id,
            cell_id=cell_id,
            session_id=UUID(str(session)),
            profile_alias=str(cell["profile_alias"]),
            turn_key=f"handoff:{row['event_id']}",
            kind=kind,
            reason=reason,
            reset_at=reset_at,
        )

    def _already_completed_evidence(
        self, row: Any, payload: dict[str, Any], cell: Any, cell_id: str
    ) -> TerminalWakeInput | None:
        issue_id = payload.get("issue_id")
        session = payload.get("session_id")
        if not issue_id or not session:
            return None
        if str(cell["session_id"]) != str(session):
            return None
        return TerminalWakeInput(
            project_key=str(cell["project_key"]),
            issue_id=str(issue_id),
            cell_id=cell_id,
            session_id=UUID(str(session)),
            profile_alias=str(cell["profile_alias"]),
            turn_key=f"already_completed:{row['event_id']}",
            kind="completed",
            reason="issue_already_completed",
        )

    def _start_failed_evidence(
        self, row: Any, payload: dict[str, Any], cell: Any, cell_id: str
    ) -> TerminalWakeInput | None:
        if str(cell["state"]) != "failed":
            return None
        issue_id = payload.get("issue_id")
        session = payload.get("session_id")
        if not issue_id or not session:
            return None
        # A start that failed because the issue was already done published
        # a completed wake through its dedicated terminal evidence; this
        # row must not also derive a blocked or capped wake.
        if str(payload.get("reason", "")) == "issue_already_completed":
            return None
        if str(cell["session_id"]) != str(session):
            return None
        if str(payload.get("reason", "")) == "subscription_limit":
            kind, reason = "provider_capped", "subscription_limit"
            reset_at = self._reset_at(str(cell["profile_alias"]))
        else:
            kind, reason, reset_at = "blocked", "start_unconfirmed", None
        return TerminalWakeInput(
            project_key=str(cell["project_key"]),
            issue_id=str(issue_id),
            cell_id=cell_id,
            session_id=UUID(str(session)),
            profile_alias=str(cell["profile_alias"]),
            turn_key=f"start_failed:{row['event_id']}",
            kind=kind,
            reason=reason,
            reset_at=reset_at,
        )

    def _issue_for(self, cell_id: str, sequence: int) -> str | None:
        row = self._database.execute(
            "SELECT aggregate_id FROM events "
            "WHERE event_type = 'issue.started' "
            "AND json_extract(payload_json, '$.cell_id') = ? "
            "AND sequence <= ? ORDER BY sequence DESC LIMIT 1",
            (cell_id, sequence),
        ).fetchone()
        return None if row is None else str(row["aggregate_id"])

    def _reset_at(self, profile_alias: str) -> str | None:
        row = self._database.execute(
            "SELECT cooldown_until FROM profile_leases "
            "WHERE profile_alias = ?",
            (profile_alias,),
        ).fetchone()
        if row is not None and row["cooldown_until"]:
            return str(row["cooldown_until"])
        return None

    def _exists(self, wake: TerminalWakeInput) -> bool:
        return bool(
            self._database.scalar(
                "SELECT count(*) FROM lead_terminal_wakes "
                "WHERE project_key = ? AND cell_id = ? AND session_id = ? "
                "AND turn_key = ? AND kind = ?",
                (
                    wake.project_key,
                    wake.cell_id,
                    str(wake.session_id),
                    wake.turn_key,
                    wake.kind,
                ),
            )
        )


def _row_to_wake(row: Any) -> TerminalWake:
    return TerminalWake(
        wake_id=str(row["wake_id"]),
        schema_version=int(row["schema_version"]),
        project_key=str(row["project_key"]),
        issue_id=str(row["issue_id"]),
        cell_id=str(row["cell_id"]),
        session_id=str(row["session_id"]),
        profile_alias=str(row["profile_alias"]),
        turn_key=str(row["turn_key"]),
        kind=str(row["kind"]),
        reason=str(row["reason"]),
        reset_at=(None if row["reset_at"] is None else str(row["reset_at"])),
        state=str(row["state"]),
        created_at=str(row["created_at"]),
        delivered_at=(
            None if row["delivered_at"] is None else str(row["delivered_at"])
        ),
    )
