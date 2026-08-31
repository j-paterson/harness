"""Durable merge settlements: Sol's verdict becomes resumable state.

INFRA-194: a live review exposed the gap — an approved verdict merged
through raw ``gh pr merge`` before ingestion left ``reviews``,
``github_merge_effects``, ``ci_merge_ledger``, and Linear silently
unsynchronized. The settlement ledger closes it: the strict verdict is
bound to every identity (project, issue, repository, branch, PR, base,
candidate SHA, wake event, reviewer thread + generation, manifest
version) and persisted in the SAME transaction as the approved review,
before anything crosses the GitHub boundary. The exclusive journaled
merge claim is an owner-token lease compare-and-set on this row; every
later stage advances by CAS, so a crash at any journal boundary leaves
one resumable row and the deterministic downstream effect ids
(``merge:{review_id}``, ``linear:...``) make each replay exactly-once
effective. ``path`` records guarded completion versus externally-merged
reconciliation.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from sqlite3 import Connection
from typing import Any

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

GUARDED = "guarded"
EXTERNALLY_MERGED = "externally_merged"

# States that still need driving after a restart.
RESUMABLE_STATES = ("recorded", "merging", "merged")


class SettlementConflict(RuntimeError):
    """The settlement is owned elsewhere or bound to another identity."""


@dataclass(frozen=True, slots=True)
class SettlementBinding:
    """Every identity a completion operation must bind before mutation."""

    settlement_id: str
    project_key: str
    issue_id: str
    event_id: str
    repository: str
    branch: str
    pr_number: int
    base_sha: str
    candidate_sha: str
    thread_id: str
    thread_generation: int
    manifest_version: int


@dataclass(frozen=True, slots=True)
class Settlement:
    """One durable settlement row."""

    settlement_id: str
    project_key: str
    issue_id: str
    event_id: str
    repository: str
    branch: str
    pr_number: int
    base_sha: str
    candidate_sha: str
    thread_id: str
    thread_generation: int
    manifest_version: int
    path: str
    state: str
    merge_sha: str | None
    reason: str | None
    owner_token: str | None
    lease_expires_at: str | None
    created_at: str
    updated_at: str


class MergeSettlements:
    """The append-guarded settlement ledger with owner-token leases."""

    def __init__(
        self,
        database: Database,
        events: EventStore,
        *,
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
        lease_seconds: float = 300.0,
    ) -> None:
        self._database = database
        self._events = events
        self._now = now or (lambda: datetime.now(UTC))
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._lease_seconds = lease_seconds

    # -- recording ---------------------------------------------------------

    def record_in(
        self,
        connection: Connection,
        binding: SettlementBinding,
        *,
        path: str = GUARDED,
    ) -> None:
        """Insert the settlement inside the caller's open transaction.

        Called from the same transaction that persists the approved
        review, so verdict and settlement can never exist separately.
        A replayed insert for the exact same binding is a no-op; the
        same id bound to anything else refuses.
        """

        stamp = self._now().isoformat()
        cursor = connection.execute(
            "INSERT OR IGNORE INTO merge_settlements("
            "settlement_id, project_key, issue_id, event_id, repository, "
            "branch, pr_number, base_sha, candidate_sha, thread_id, "
            "thread_generation, manifest_version, path, state, "
            "created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "'recorded', ?, ?)",
            (
                binding.settlement_id,
                binding.project_key,
                binding.issue_id,
                binding.event_id,
                binding.repository,
                binding.branch,
                binding.pr_number,
                binding.base_sha,
                binding.candidate_sha,
                binding.thread_id,
                binding.thread_generation,
                binding.manifest_version,
                path,
                stamp,
                stamp,
            ),
        )
        if cursor.rowcount == 1:
            self._events.append(
                connection,
                EventInput(
                    event_type="settlement.recorded",
                    aggregate_type="merge_settlement",
                    aggregate_id=binding.settlement_id,
                    correlation_id=binding.event_id,
                    payload={
                        "issue_id": binding.issue_id,
                        "candidate_sha": binding.candidate_sha,
                        "pr_number": binding.pr_number,
                        "thread_generation": binding.thread_generation,
                        "path": path,
                    },
                ),
            )
            return
        existing = connection.execute(
            "SELECT * FROM merge_settlements WHERE settlement_id = ?",
            (binding.settlement_id,),
        ).fetchone()
        if existing is None:
            raise SettlementConflict("settlement insert lost without a surviving row")
        row = _row_to_settlement(existing)
        if (
            row.project_key != binding.project_key
            or row.issue_id != binding.issue_id
            or row.event_id != binding.event_id
            or row.repository != binding.repository
            or row.branch != binding.branch
            or row.pr_number != binding.pr_number
            or row.base_sha != binding.base_sha
            or row.candidate_sha != binding.candidate_sha
        ):
            raise SettlementConflict(
                "settlement id is already bound to a different candidate"
            )

    # -- the exclusive merge claim ----------------------------------------

    def claim(self, settlement_id: str) -> str | None:
        """Acquire the exclusive merge claim, or None when unavailable.

        ``recorded`` rows are claimed directly; ``merged`` rows are
        re-claimed to finish the settlement tail after a crash (the
        GitHub effect journal, not this state, is the authoritative
        mutation record and dedupes the replay); a ``merging`` row is
        adopted only after its lease expired (its previous owner
        crashed mid-boundary). A live lease, or any terminal state,
        yields None and the caller must not cross the GitHub boundary.
        """

        token = self._ids()
        now = self._now()
        stamp = now.isoformat()
        expiry = datetime.fromtimestamp(
            now.timestamp() + self._lease_seconds, tz=UTC
        ).isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE merge_settlements SET state = 'merging', "
                "owner_token = ?, lease_expires_at = ?, updated_at = ? "
                "WHERE settlement_id = ? AND ("
                "state IN ('recorded', 'merged') OR ("
                "state = 'merging' AND lease_expires_at IS NOT NULL "
                "AND lease_expires_at < ?))",
                (token, expiry, stamp, settlement_id, stamp),
            )
            if cursor.rowcount != 1:
                return None
            self._append(
                connection,
                settlement_id,
                "settlement.claimed",
                {"lease_expires_at": expiry},
            )
        return token

    def mark_merged(
        self,
        settlement_id: str,
        *,
        token: str,
        merge_sha: str,
        path: str | None = None,
    ) -> None:
        """Advance past the mutation boundary, recording how it landed.

        ``path`` records whether completion followed the normal
        guarded path or externally-merged reconciliation (a permitted
        direct exact-head merge detected by the driver).
        """

        self._advance(
            settlement_id,
            token=token,
            from_state="merging",
            to_state="merged",
            merge_sha=merge_sha,
            path=path,
        )

    def mark_settled(self, settlement_id: str, *, token: str) -> None:
        self._advance(
            settlement_id,
            token=token,
            from_state="merged",
            to_state="settled",
        )

    def mark_failed(self, settlement_id: str, *, token: str, reason: str) -> None:
        self._advance(
            settlement_id,
            token=token,
            from_state="merging",
            to_state="failed",
            reason=reason,
        )

    def reopen_failed(self, settlement_id: str, *, reason: str) -> None:
        """CAS a ``failed`` settlement back to ``recorded`` for repair.

        Used only when a post-merge proof failure is later reconciled
        against the exact same completed merge effect: the row becomes
        claimable again (owner token and lease cleared, ``merge_sha``
        stays NULL, ``path`` untouched) so the caller can drive it to
        ``merged`` -> ``settled`` through the ordinary claim path. Any
        state other than ``failed`` refuses.
        """

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE merge_settlements SET state = 'recorded', "
                "owner_token = NULL, lease_expires_at = NULL, "
                "updated_at = ? WHERE settlement_id = ? AND state = 'failed'",
                (stamp, settlement_id),
            )
            if cursor.rowcount != 1:
                raise SettlementConflict(
                    f"settlement {settlement_id} is not in state 'failed'"
                )
            self._append(
                connection, settlement_id, "settlement.reopened", {"reason": reason}
            )

    def release(self, settlement_id: str, *, token: str) -> None:
        """Return a claimed settlement to ``recorded`` (e.g. a full
        merge window); nothing external happened under this claim."""

        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE merge_settlements SET state = 'recorded', "
                "owner_token = NULL, lease_expires_at = NULL, "
                "updated_at = ? WHERE settlement_id = ? "
                "AND state = 'merging' AND owner_token = ?",
                (stamp, settlement_id, token),
            )
            if cursor.rowcount == 1:
                self._append(connection, settlement_id, "settlement.released", {})

    # -- reads -------------------------------------------------------------

    def get(self, settlement_id: str) -> Settlement:
        row = self._database.execute(
            "SELECT * FROM merge_settlements WHERE settlement_id = ?",
            (settlement_id,),
        ).fetchone()
        if row is None:
            raise KeyError(settlement_id)
        return _row_to_settlement(row)

    def find(self, settlement_id: str) -> Settlement | None:
        row = self._database.execute(
            "SELECT * FROM merge_settlements WHERE settlement_id = ?",
            (settlement_id,),
        ).fetchone()
        return None if row is None else _row_to_settlement(row)

    def resumable(self, project_key: str | None = None) -> tuple[Settlement, ...]:
        """Settlements a restart must re-drive, oldest first.

        ``recorded`` and ``merged`` rows always resume; ``merging``
        rows resume only once their lease expired.
        """

        stamp = self._now().isoformat()
        placeholders = ",".join("?" for _ in RESUMABLE_STATES)
        sql = (
            f"SELECT * FROM merge_settlements WHERE state IN ({placeholders}) "
            "AND (state != 'merging' OR lease_expires_at IS NULL "
            "OR lease_expires_at < ?)"
        )
        parameters: list[object] = [*RESUMABLE_STATES, stamp]
        if project_key is not None:
            sql += " AND project_key = ?"
            parameters.append(project_key)
        sql += " ORDER BY created_at ASC, rowid ASC"
        rows = self._database.execute(sql, tuple(parameters)).fetchall()
        return tuple(_row_to_settlement(row) for row in rows)

    # -- internals ---------------------------------------------------------

    def _advance(
        self,
        settlement_id: str,
        *,
        token: str,
        from_state: str,
        to_state: str,
        merge_sha: str | None = None,
        reason: str | None = None,
        path: str | None = None,
    ) -> None:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE merge_settlements SET state = ?, "
                "merge_sha = COALESCE(?, merge_sha), "
                "reason = COALESCE(?, reason), "
                "path = COALESCE(?, path), updated_at = ? "
                "WHERE settlement_id = ? AND state = ? "
                "AND owner_token = ?",
                (
                    to_state,
                    merge_sha,
                    reason,
                    path,
                    stamp,
                    settlement_id,
                    from_state,
                    token,
                ),
            )
            if cursor.rowcount != 1:
                raise SettlementConflict(
                    f"settlement {settlement_id} is not owned in state "
                    f"{from_state!r} by this claim"
                )
            self._append(
                connection,
                settlement_id,
                f"settlement.{to_state}",
                {"merge_sha": merge_sha, "reason": reason},
            )

    def _append(
        self,
        connection: Connection,
        settlement_id: str,
        event_type: str,
        payload: dict[str, object],
    ) -> None:
        self._events.append(
            connection,
            EventInput(
                event_type=event_type,
                aggregate_type="merge_settlement",
                aggregate_id=settlement_id,
                payload=payload,
            ),
        )


def _row_to_settlement(row: Any) -> Settlement:
    def optional(key: str) -> str | None:
        value = row[key]
        return None if value is None else str(value)

    return Settlement(
        settlement_id=str(row["settlement_id"]),
        project_key=str(row["project_key"]),
        issue_id=str(row["issue_id"]),
        event_id=str(row["event_id"]),
        repository=str(row["repository"]),
        branch=str(row["branch"]),
        pr_number=int(row["pr_number"]),
        base_sha=str(row["base_sha"]),
        candidate_sha=str(row["candidate_sha"]),
        thread_id=str(row["thread_id"]),
        thread_generation=int(row["thread_generation"]),
        manifest_version=int(row["manifest_version"]),
        path=str(row["path"]),
        state=str(row["state"]),
        merge_sha=optional("merge_sha"),
        reason=optional("reason"),
        owner_token=optional("owner_token"),
        lease_expires_at=optional("lease_expires_at"),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )
