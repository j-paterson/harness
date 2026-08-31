"""Exact managed process-group leases and bounded, validated termination.

INFRA-167: every managed process belongs to one recorded process-group
lease. Registration happens immediately after the child is spawned in its
own session and records pid, pgid, executable, cwd, project, worker, and
the psutil ``create_time``. A stop request requires a checkpoint id,
re-validates the recorded identity (create_time and group) before any
signal so a reused PID is never signaled, sends SIGTERM to the exact
recorded group, waits the bounded grace period, and escalates to SIGKILL
only for the same validated group. Every signal and result is persisted.
Nothing here ever kills by process name or path.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import psutil

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

ACTIVE_STATES = ("active", "stopping")


class OsPort(Protocol):
    """Signal and group boundary; exact pgid only."""

    def killpg(self, pgid: int, sig: int) -> None: ...

    def getpgid(self, pid: int) -> int: ...


class ProcessInfoPort(Protocol):
    """Identity-validated process evidence."""

    def create_time(self, pid: int) -> float | None: ...

    def is_running(self, pid: int, create_time: float) -> bool: ...

    def tree_rss_bytes(self, pid: int, create_time: float) -> int: ...

    def wait_exit(self, pid: int, create_time: float, timeout: float) -> bool: ...


class StdOsPort:
    def killpg(self, pgid: int, sig: int) -> None:
        os.killpg(pgid, sig)

    def getpgid(self, pid: int) -> int:
        return os.getpgid(pid)


class PsutilProcessInfo:
    """psutil-backed evidence; identity is always (pid, create_time)."""

    def _process(self, pid: int, create_time: float) -> psutil.Process | None:
        try:
            process = psutil.Process(pid)
            if abs(process.create_time() - create_time) > 1e-3:
                return None
            return process
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            return None

    def create_time(self, pid: int) -> float | None:
        try:
            return float(psutil.Process(pid).create_time())
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            return None

    def is_running(self, pid: int, create_time: float) -> bool:
        process = self._process(pid, create_time)
        if process is None:
            return False
        try:
            return process.is_running() and (
                process.status() != psutil.STATUS_ZOMBIE
            )
        except psutil.Error:
            return False

    def tree_rss_bytes(self, pid: int, create_time: float) -> int:
        process = self._process(pid, create_time)
        if process is None:
            return 0
        total = 0
        try:
            members = [process, *process.children(recursive=True)]
        except psutil.Error:
            members = [process]
        for member in members:
            try:
                total += int(member.memory_info().rss)
            except psutil.Error:
                continue
        return total

    def wait_exit(self, pid: int, create_time: float, timeout: float) -> bool:
        process = self._process(pid, create_time)
        if process is None:
            return True
        try:
            process.wait(timeout=timeout)
            return True
        except psutil.TimeoutExpired:
            return False
        except psutil.Error:
            return True


class StopBlocked(RuntimeError):
    """A stop request lacks its required evidence and sends nothing."""


@dataclass(frozen=True, slots=True)
class ProcessLeaseInput:
    pid: int
    pgid: int | None
    project_key: str
    kind: str
    worker_id: str | None = None
    executable: str | None = None
    cwd: str | None = None


@dataclass(frozen=True, slots=True)
class ProcessLease:
    lease_id: str
    pid: int
    pgid: int
    project_key: str
    kind: str
    worker_id: str | None
    executable: str | None
    cwd: str | None
    create_time: float
    state: str
    last_signal: int | None
    stop_reason: str | None
    acquired_at: str
    stop_owner: str | None = None
    stop_phase: str | None = None
    stop_claim_expires_at: str | None = None
    stop_checkpoint_id: str | None = None


@dataclass(frozen=True, slots=True)
class ProcessSnapshot:
    lease_id: str
    state: str
    alive: bool
    identity_valid: bool
    rss_bytes: int


@dataclass(frozen=True, slots=True)
class StopResult:
    lease_id: str
    signal_sent: int | None
    escalated: bool
    exited: bool
    reason: str


class ProcessRegistry:
    """Own exact process-group leases in durable state."""

    def __init__(
        self,
        database: Database,
        events: EventStore,
        *,
        os_port: OsPort | None = None,
        info: ProcessInfoPort | None = None,
        grace_seconds: float = 5.0,
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
    ) -> None:
        if grace_seconds <= 0:
            raise ValueError("grace_seconds must be positive")
        self._database = database
        self._events = events
        self._os = os_port if os_port is not None else StdOsPort()
        self._info = info if info is not None else PsutilProcessInfo()
        self._grace = grace_seconds
        self._claim_seconds = 2 * grace_seconds + 5.0
        self._now = now or (lambda: datetime.now(UTC))
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._owned: set[str] = set()

    # -- registration -----------------------------------------------------

    def register(self, request: ProcessLeaseInput) -> ProcessLease:
        with self._database.transaction() as connection:
            return self.register_on(connection, request)

    def register_on(
        self, connection: sqlite3.Connection, request: ProcessLeaseInput
    ) -> ProcessLease:
        """Register within the caller's open transaction.

        Sol correction 57c46faa: a launcher whose durable identity
        binding must be crash-safe registers the lease and appends its
        own binding journal in ONE caller-held transaction, so the lease
        row and the binding can never diverge — they commit together or
        roll back together. Validation is identical to :meth:`register`;
        the caller owns the commit, and its rollback leaves no lease row
        behind (a stale in-memory ownership mark for a rolled-back
        lease_id is harmless — ownership is only ever read against
        durable rows).
        """

        if request.pid <= 0:
            raise ValueError("pid must be positive")
        if request.pgid is None or request.pgid <= 0:
            raise ValueError("a managed process requires an exact process group")
        if not request.project_key.strip() or not request.kind.strip():
            raise ValueError("project key and kind are required")
        create_time = self._info.create_time(request.pid)
        if create_time is None:
            raise ValueError(f"process {request.pid} is not running")
        try:
            actual_group = self._os.getpgid(request.pid)
        except ProcessLookupError as error:
            raise ValueError(f"process {request.pid} is not running") from error
        if actual_group != request.pgid:
            raise ValueError(
                f"process {request.pid} is in process group {actual_group}, "
                f"not the recorded process group {request.pgid}"
            )
        lease_id = self._ids()
        stamp = self._now().isoformat()
        # A worktree claimed for cleanup (INFRA-171) admits no new
        # attachment; checking inside the write transaction serializes
        # this refusal against the cleanup claim itself.
        if request.cwd is not None:
            claimed = connection.execute(
                "SELECT path FROM worktree_leases "
                "WHERE state = 'reclaiming'"
            ).fetchall()
            for row in claimed:
                if Path(request.cwd).is_relative_to(Path(str(row["path"]))):
                    raise ValueError(
                        f"cwd {request.cwd} is inside worktree "
                        f"{row['path']}, which is claimed for cleanup"
                    )
        connection.execute(
            "INSERT INTO process_leases("
            "lease_id, worker_id, project_key, kind, pid, pgid, executable, "
            "cwd, create_time, state, acquired_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)",
            (
                lease_id,
                request.worker_id,
                request.project_key,
                request.kind,
                request.pid,
                request.pgid,
                request.executable,
                request.cwd,
                create_time,
                stamp,
                stamp,
            ),
        )
        self._events.append(
            connection,
            EventInput(
                event_type="process.registered",
                aggregate_type="process_lease",
                aggregate_id=lease_id,
                payload={
                    "pid": request.pid,
                    "pgid": request.pgid,
                    "kind": request.kind,
                    "project_key": request.project_key,
                    "worker_id": request.worker_id,
                },
            ),
        )
        self._owned.add(lease_id)
        row = connection.execute(
            "SELECT * FROM process_leases WHERE lease_id = ?", (lease_id,)
        ).fetchone()
        return _row_to_lease(row)

    def get(self, lease_id: str) -> ProcessLease:
        row = self._database.execute(
            "SELECT * FROM process_leases WHERE lease_id = ?", (lease_id,)
        ).fetchone()
        if row is None:
            raise KeyError(lease_id)
        return _row_to_lease(row)

    def active(self, project_key: str | None = None) -> tuple[ProcessLease, ...]:
        placeholders = ",".join("?" for _ in ACTIVE_STATES)
        if project_key is None:
            rows = self._database.execute(
                f"SELECT * FROM process_leases WHERE state IN ({placeholders}) "
                "ORDER BY acquired_at ASC, rowid ASC",
                ACTIVE_STATES,
            ).fetchall()
        else:
            rows = self._database.execute(
                f"SELECT * FROM process_leases WHERE state IN ({placeholders}) "
                "AND project_key = ? ORDER BY acquired_at ASC, rowid ASC",
                (*ACTIVE_STATES, project_key),
            ).fetchall()
        return tuple(_row_to_lease(row) for row in rows)

    # -- observation ------------------------------------------------------

    def snapshot(self, lease_id: str) -> ProcessSnapshot:
        lease = self.get(lease_id)
        valid = self._identity_valid(lease)
        alive = valid and self._info.is_running(lease.pid, lease.create_time)
        rss = self._info.tree_rss_bytes(lease.pid, lease.create_time) if alive else 0
        return ProcessSnapshot(
            lease_id=lease_id,
            state=lease.state,
            alive=alive,
            identity_valid=valid,
            rss_bytes=rss,
        )

    def managed_rss_bytes(self) -> int:
        """Sum of validated process-tree RSS over active leases; never signals."""

        return sum(self.snapshot(lease.lease_id).rss_bytes for lease in self.active())

    def mark_exited(
        self, lease_id: str, *, exit_code: int | None = None
    ) -> ProcessLease:
        """Record a natural exit observed by the launcher."""

        self._transition(
            lease_id,
            "stopped",
            reason="exited",
            exit_code=exit_code,
            from_states=ACTIVE_STATES,
        )
        self._owned.discard(lease_id)
        return self.get(lease_id)

    # -- termination ------------------------------------------------------

    def request_stop(self, lease_id: str, checkpoint_id: str) -> StopResult:
        """Stop exactly the recorded group through a durable, owned claim.

        Order is contractual: (1) a compare-and-swap claim journals the
        checkpoint, owner token, phase, and claim expiry — no signal is
        ever sent without it and a losing caller sends nothing; (2) the
        identity (create_time and group) is re-validated immediately before
        each signal; (3) SIGTERM is sent to the exact group and its result
        journaled; (4) after the bounded grace period a separate KILL claim
        and re-validation precede SIGKILL. A crash between a signal and
        its journal leaves a durable ``*_claimed`` phase whose recovery
        never re-sends that signal. Every attempt and result is auditable.
        """

        if not checkpoint_id or not checkpoint_id.strip():
            raise StopBlocked("a stop request requires a checkpoint id")
        lease = self.get(lease_id)
        if lease.state not in ACTIVE_STATES:
            raise StopBlocked(f"lease {lease_id} is {lease.state}, not active")
        owner = self._ids()
        recovered = False
        if lease.state == "active":
            if not self._claim(
                lease, owner, phase="term_claimed", checkpoint_id=checkpoint_id
            ):
                return StopResult(lease_id, None, False, False, "stop_owned_elsewhere")
            phase = "term_claimed"
        else:
            if (
                lease.stop_claim_expires_at is not None
                and lease.stop_claim_expires_at > self._now().isoformat()
            ):
                return StopResult(lease_id, None, False, False, "stop_in_progress")
            if not self._claim(
                lease, owner, phase=lease.stop_phase or "term_claimed",
                checkpoint_id=checkpoint_id, recover_from=lease.stop_owner,
            ):
                return StopResult(lease_id, None, False, False, "stop_in_progress")
            phase = lease.stop_phase or "term_claimed"
            recovered = True
        lease = self.get(lease_id)

        signal_sent: int | None = None
        if phase == "term_claimed" and not recovered:
            outcome = self._send(lease, owner, signal.SIGTERM, "term_sent")
            if outcome is not None:
                return outcome
            phase = "term_sent"
            signal_sent = int(signal.SIGTERM)
        elif phase == "term_claimed":
            # Recovery of a claim whose SIGTERM may or may not have been
            # sent before the crash: never re-send it; proceed to wait.
            signal_sent = None
        elif phase == "term_sent":
            signal_sent = int(signal.SIGTERM)

        if phase in ("term_claimed", "term_sent"):
            if self._info.wait_exit(lease.pid, lease.create_time, self._grace):
                self._finish(lease_id, owner, "stopped", "terminated")
                return StopResult(lease_id, signal_sent, False, True, "terminated")
            if not self._alive(lease):
                self._finish(lease_id, owner, "stopped", "terminated")
                return StopResult(lease_id, signal_sent, False, True, "terminated")
            if not self._advance(lease_id, owner, "kill_claimed", from_phase=phase):
                return StopResult(
                    lease_id, signal_sent, False, False, "stop_in_progress"
                )
            phase = "kill_claimed"
            recovered = False

        if phase == "kill_claimed" and not recovered:
            outcome = self._send(lease, owner, signal.SIGKILL, "kill_sent")
            if outcome is not None:
                return outcome
            phase = "kill_sent"
            signal_sent = int(signal.SIGKILL)
        elif phase == "kill_claimed":
            signal_sent = None
        else:
            signal_sent = int(signal.SIGKILL)

        if self._info.wait_exit(lease.pid, lease.create_time, self._grace):
            self._finish(lease_id, owner, "stopped", "killed")
            return StopResult(lease_id, signal_sent, True, True, "killed")
        self._release(lease_id, owner, "kill_unconfirmed")
        return StopResult(lease_id, signal_sent, True, False, "kill_unconfirmed")

    def _alive(self, lease: ProcessLease) -> bool:
        return self._identity_valid(lease) and self._info.is_running(
            lease.pid, lease.create_time
        )

    def _send(
        self, lease: ProcessLease, owner: str, sig: int, sent_phase: str
    ) -> StopResult | None:
        """Re-validate immediately before one signal; journal the result."""

        if not self._identity_valid(lease):
            self._finish(lease.lease_id, owner, "expired", "pid_reused")
            return StopResult(
                lease.lease_id, None, sig == signal.SIGKILL, True, "pid_reused"
            )
        if not self._info.is_running(lease.pid, lease.create_time):
            reason = "already_exited" if sig == signal.SIGTERM else "terminated"
            self._finish(lease.lease_id, owner, "stopped", reason)
            return StopResult(
                lease.lease_id,
                None if sig == signal.SIGTERM else int(signal.SIGTERM),
                False,
                True,
                reason,
            )
        try:
            self._os.killpg(lease.pgid, sig)
        except ProcessLookupError:
            reason = "already_exited" if sig == signal.SIGTERM else "terminated"
            self._finish(lease.lease_id, owner, "stopped", reason)
            return StopResult(
                lease.lease_id,
                None if sig == signal.SIGTERM else int(signal.SIGTERM),
                False,
                True,
                reason,
            )
        # A failure here leaves the durable *_claimed phase; recovery never
        # re-sends this signal.
        self._advance(
            lease.lease_id, owner, sent_phase, from_phase=sent_phase.replace(
                "_sent", "_claimed"
            ), last_signal=int(sig),
        )
        return None

    def _claim(
        self,
        lease: ProcessLease,
        owner: str,
        *,
        phase: str,
        checkpoint_id: str,
        recover_from: str | None = None,
    ) -> bool:
        stamp = self._now()
        expires = (stamp + timedelta(seconds=self._claim_seconds)).isoformat()
        with self._database.transaction() as connection:
            if recover_from is None:
                cursor = connection.execute(
                    "UPDATE process_leases SET state = 'stopping', stop_owner = ?, "
                    "stop_phase = ?, stop_claim_expires_at = ?, "
                    "stop_checkpoint_id = ?, stop_reason = 'claimed', "
                    "updated_at = ? WHERE lease_id = ? AND state = 'active'",
                    (owner, phase, expires, checkpoint_id, stamp.isoformat(),
                     lease.lease_id),
                )
            else:
                cursor = connection.execute(
                    "UPDATE process_leases SET stop_owner = ?, "
                    "stop_claim_expires_at = ?, stop_checkpoint_id = ?, "
                    "stop_reason = 'recovered', updated_at = ? "
                    "WHERE lease_id = ? AND state = 'stopping' "
                    "AND stop_owner = ? AND stop_claim_expires_at <= ?",
                    (owner, expires, checkpoint_id, stamp.isoformat(),
                     lease.lease_id, recover_from, stamp.isoformat()),
                )
            if cursor.rowcount != 1:
                return False
            self._events.append(
                connection,
                EventInput(
                    event_type=(
                        "process.stop_claimed"
                        if recover_from is None
                        else "process.stop_recovered"
                    ),
                    aggregate_type="process_lease",
                    aggregate_id=lease.lease_id,
                    correlation_id=owner,
                    payload={
                        "checkpoint_id": checkpoint_id,
                        "phase": phase,
                        "claim_expires_at": expires,
                        "recovered_from": recover_from,
                    },
                ),
            )
        return True

    def _advance(
        self,
        lease_id: str,
        owner: str,
        phase: str,
        *,
        from_phase: str,
        last_signal: int | None = None,
    ) -> bool:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE process_leases SET stop_phase = ?, "
                "last_signal = COALESCE(?, last_signal), stop_reason = ?, "
                "updated_at = ? WHERE lease_id = ? AND state = 'stopping' "
                "AND stop_owner = ? AND stop_phase = ?",
                (phase, last_signal, phase, stamp, lease_id, owner, from_phase),
            )
            if cursor.rowcount != 1:
                return False
            self._events.append(
                connection,
                EventInput(
                    event_type=f"process.{phase}",
                    aggregate_type="process_lease",
                    aggregate_id=lease_id,
                    correlation_id=owner,
                    payload={"signal": last_signal, "from_phase": from_phase},
                ),
            )
        return True

    def _finish(self, lease_id: str, owner: str, state: str, reason: str) -> None:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE process_leases SET state = ?, stop_reason = ?, "
                "stop_claim_expires_at = NULL, updated_at = ? "
                "WHERE lease_id = ? AND state = 'stopping' AND stop_owner = ?",
                (state, reason, stamp, lease_id, owner),
            )
            if cursor.rowcount != 1:
                raise StopBlocked(f"lease {lease_id} stop ownership was lost")
            self._events.append(
                connection,
                EventInput(
                    event_type=f"process.{state}",
                    aggregate_type="process_lease",
                    aggregate_id=lease_id,
                    correlation_id=owner,
                    payload={"reason": reason},
                ),
            )
        self._owned.discard(lease_id)

    def _release(self, lease_id: str, owner: str, reason: str) -> None:
        """Keep the lease stopping but let any caller recover immediately."""

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE process_leases SET stop_reason = ?, "
                "stop_claim_expires_at = ?, updated_at = ? "
                "WHERE lease_id = ? AND state = 'stopping' AND stop_owner = ?",
                (reason, stamp, stamp, lease_id, owner),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="process.stop_unconfirmed",
                    aggregate_type="process_lease",
                    aggregate_id=lease_id,
                    correlation_id=owner,
                    payload={"reason": reason},
                ),
            )

    # -- reconciliation ---------------------------------------------------

    def expire_dead(self) -> list[str]:
        """Expire active leases whose process is gone or whose PID was reused."""

        expired: list[str] = []
        for lease in self.active():
            if self._identity_valid(lease) and self._info.is_running(
                lease.pid, lease.create_time
            ):
                continue
            reason = "pid_reused" if self._info.create_time(lease.pid) else "exited"
            self._transition(
                lease.lease_id, "expired", reason=reason, from_states=ACTIVE_STATES
            )
            self._owned.discard(lease.lease_id)
            expired.append(lease.lease_id)
        return expired

    def find_orphans(self) -> list[ProcessLease]:
        """Active leases with a live, identity-valid process that no running
        launcher in this registry owns — managed processes that survived a
        restart. Detection only; reaping is an explicit later step."""

        orphans: list[ProcessLease] = []
        for lease in self.active():
            if lease.lease_id in self._owned:
                continue
            if self._identity_valid(lease) and self._info.is_running(
                lease.pid, lease.create_time
            ):
                orphans.append(lease)
        return orphans

    # -- internals --------------------------------------------------------

    def _identity_valid(self, lease: ProcessLease) -> bool:
        current = self._info.create_time(lease.pid)
        if current is None or abs(current - lease.create_time) > 1e-3:
            return False
        try:
            return self._os.getpgid(lease.pid) == lease.pgid
        except ProcessLookupError:
            return False

    def _transition(
        self,
        lease_id: str,
        state: str,
        *,
        reason: str,
        from_states: tuple[str, ...],
        checkpoint_id: str | None = None,
        last_signal: int | None = None,
        exit_code: int | None = None,
    ) -> None:
        placeholders = ",".join("?" for _ in from_states)
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE process_leases SET state = ?, stop_reason = ?, "
                "stop_checkpoint_id = COALESCE(?, stop_checkpoint_id), "
                "last_signal = COALESCE(?, last_signal), "
                "exit_code = COALESCE(?, exit_code), updated_at = ? "
                f"WHERE lease_id = ? AND state IN ({placeholders})",
                (
                    state,
                    reason,
                    checkpoint_id,
                    last_signal,
                    exit_code,
                    stamp,
                    lease_id,
                    *from_states,
                ),
            )
            if cursor.rowcount != 1:
                raise StopBlocked(f"lease {lease_id} changed state concurrently")
            self._events.append(
                connection,
                EventInput(
                    event_type=f"process.{state}",
                    aggregate_type="process_lease",
                    aggregate_id=lease_id,
                    payload={
                        "reason": reason,
                        "signal": last_signal,
                        "checkpoint_id": checkpoint_id,
                        "exit_code": exit_code,
                    },
                ),
            )


def _row_to_lease(row: Any) -> ProcessLease:
    return ProcessLease(
        lease_id=str(row["lease_id"]),
        pid=int(row["pid"]),
        pgid=int(row["pgid"]),
        project_key=str(row["project_key"]),
        kind=str(row["kind"]),
        worker_id=row["worker_id"],
        executable=row["executable"],
        cwd=row["cwd"],
        create_time=float(row["create_time"]),
        state=str(row["state"]),
        last_signal=row["last_signal"],
        stop_reason=row["stop_reason"],
        acquired_at=str(row["acquired_at"]),
        stop_owner=row["stop_owner"],
        stop_phase=row["stop_phase"],
        stop_claim_expires_at=row["stop_claim_expires_at"],
        stop_checkpoint_id=row["stop_checkpoint_id"],
    )


async def register_spawned(
    registry: ProcessRegistry | None,
    process: Any,
    *,
    project_key: str,
    kind: str,
    worker_id: str | None = None,
    executable: str | None = None,
    cwd: str | None = None,
    terminate: Callable[[Any], Any],
) -> str | None:
    """Register a just-spawned session leader or stop it and fail closed.

    Launchers call this immediately after ``create_subprocess_exec`` with
    ``start_new_session=True``: the child is its own group leader, so the
    recorded pgid is its pid. A missing registration terminates the child
    and raises, closing admission for that worker.
    """

    if registry is None:
        return None
    try:
        lease = registry.register(
            ProcessLeaseInput(
                pid=int(process.pid),
                pgid=int(process.pid),
                project_key=project_key,
                kind=kind,
                worker_id=worker_id,
                executable=executable,
                cwd=cwd,
            )
        )
    except BaseException as error:
        # Every failure — validation, SQLite, event journal, unexpected, or
        # cancellation — stops and reaps the exact new group before
        # propagating. Exception text is redacted to its type.
        await terminate(process)
        if isinstance(error, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
            raise
        raise RuntimeError(
            f"managed {kind} process could not be registered: "
            f"{type(error).__name__}"
        ) from None
    return lease.lease_id
