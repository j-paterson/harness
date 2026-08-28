"""Optimistic CircleCI merge window, reconciled once per intake boundary.

INFRA-165: CircleCI is never polled, watched, or slept on, and no reviewer
turn is kept alive for it. After a proven merge the merged item is recorded
as unresolved and the turn ends terminal-idle; the next validated FABLE_*
candidate wake triggers exactly one durable, event-bound reconciliation
before the new candidate may be processed. A known prior failure returns
the prior item to its Claude lead and stops the intake; a full unresolved
window defers the intake; ambiguity stays optimistic within the window but
is never resolved as success.
"""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from hermes_orchestrator.circleci import CiCheck, CircleCiError, sanitize_text
from hermes_orchestrator.db import Database
from hermes_orchestrator.manifests import CandidateManifest
from hermes_orchestrator.merge import ProvenMerge
from hermes_orchestrator.review_intake import CandidateRejected
from hermes_orchestrator.verdicts import CorrectionPacket

# Owner tokens are generated with uuid.uuid4().hex: exactly 32 lowercase
# hexadecimal characters. Anything else in a durable claim row is corrupt.
_TOKEN_PATTERN = re.compile(r"^[0-9a-f]{32}$")


class ReworkAuthorizer(Protocol):
    """Prove a FABLE_REWORK_READY candidate is bound to a failure's packet."""

    def authorized_rework(
        self, project_key: str, candidate: CandidateManifest, packet: CorrectionPacket
    ) -> str | None: ...


class CiStatusPort(Protocol):
    """The single-shot CI classification boundary the window depends on."""

    def check(
        self, project_slug: str, branch: str, merge_sha: str
    ) -> CiCheck: ...


@dataclass(frozen=True, slots=True)
class UnresolvedMerge:
    """One merged candidate whose CI outcome is not yet resolved.

    Binds the full reviewed identity — candidate SHA and feature branch —
    in addition to the merge result identity, so correction packets can
    return to the exact reviewed work.
    """

    project_key: str
    repository: str
    pr_number: int
    merge_sha: str
    integration_branch: str
    candidate_sha: str
    candidate_branch: str
    recorded_at: str


@dataclass(frozen=True, slots=True)
class IntakeDecision:
    """The bounded outcome of one intake-boundary reconciliation pass."""

    kind: str  # "clear" | "blocked_prior_failure" | "deferred_window_full"
    resolved: tuple[str, ...]
    unresolved: tuple[str, ...]
    packet: CorrectionPacket | None = None
    reason: str = ""


class PriorMergeFailed(CandidateRejected):
    """A prior merged item failed CI; it returns to its Claude lead first."""

    def __init__(self, message: str, *, packet: CorrectionPacket) -> None:
        super().__init__(message)
        self.packet = packet


class MergeWindowExhausted(CandidateRejected):
    """Two merged items are still unresolved; no third may be created."""

    def __init__(self, message: str, *, unresolved: tuple[str, ...]) -> None:
        super().__init__(message)
        self.unresolved = unresolved


class ReconciliationPending(CandidateRejected):
    """Another live attempt owns this event's reconciliation claim.

    Raised with zero CI queries while the claim's lease is live. After the
    lease expires, exactly one recovery attempt may atomically take
    ownership and reconcile again — the external reads are idempotent and
    read-only, so the re-read is crash recovery, not polling.
    """


class ReconciliationIdentityMismatch(CandidateRejected):
    """An event id was reused with a different immutable wake identity.

    Fails closed with zero CI queries and never replays a decision: the
    claim is bound to the full candidate identity, not the event id alone.
    """


class ReconciliationClaimCorrupt(CandidateRejected):
    """A durable reconciliation claim row is malformed.

    Raised before any CircleCI call and without mutating the row: an
    empty or invalid state, owner token, identity, or lease timestamp
    (including naive or unparseable datetimes) requires operator
    reconciliation, never a guess.
    """


class _OwnershipLost(Exception):
    """Internal: this worker's claim ownership was recovered by another."""


@dataclass(frozen=True, slots=True)
class _ClaimGuard:
    """The claim identity every ledger mutation of one event must carry."""

    project_key: str
    event_id: str
    owner_token: str
    identity_json: str


@dataclass(frozen=True, slots=True)
class _ClaimRow:
    """One strictly validated reconciliation-claim row."""

    identity_json: str
    state: str
    owner_token: str
    lease_raw: str
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class ReconciliationIdentity:
    """The immutable wake/candidate identity one reconciliation binds.

    Carries the candidate fields available at the intake boundary; a claim
    may be replayed or recovered only under the exact same identity.
    """

    status: str
    candidate_sha: str
    base_sha: str
    branch: str
    issues: tuple[str, ...]

    def as_json(self) -> str:
        return json.dumps(
            {
                "status": self.status,
                "candidate_sha": self.candidate_sha,
                "base_sha": self.base_sha,
                "branch": self.branch,
                "issues": sorted(self.issues),
            },
            sort_keys=True,
            separators=(",", ":"),
        )


class CiWindow:
    """Durable ledger of merged candidates and their one-shot reconciliation.

    ``record_merge`` performs zero CI queries: the merged item is journaled
    and the Merger turn ends terminal-idle. ``reconcile_event`` is the
    event-bound authoritative boundary: exactly one claim per (project,
    event) may consult CircleCI, duplicates replay the recorded decision
    silently, and every returned decision is derived from committed ledger
    state.
    """

    def __init__(
        self,
        *,
        database: Database,
        status: CiStatusPort,
        max_unresolved: int = 2,
        now: Callable[[], datetime] | None = None,
        claim_lease_seconds: float = 300.0,
    ) -> None:
        if max_unresolved < 1:
            raise ValueError("max_unresolved must be positive")
        if claim_lease_seconds <= 0:
            raise ValueError("claim_lease_seconds must be positive")
        if status is None or not hasattr(status, "check"):
            raise ValueError("the merge window requires a CI status port")
        self._database = database
        self._status = status
        self._max_unresolved = max_unresolved
        self._now = now if now is not None else lambda: datetime.now(UTC)
        self._claim_lease_seconds = claim_lease_seconds

    def record_merge(self, merge: ProvenMerge) -> None:
        """Durably record one proven merge as unresolved; never query CI.

        Idempotent by exact identity: re-recording the same proven merge is
        a no-op, while a conflicting identity — including a different
        reviewed candidate SHA or feature branch — for the same merge SHA
        fails.
        """

        stamp = self._now().isoformat()
        try:
            with self._database.transaction() as connection:
                connection.execute(
                    "INSERT INTO ci_merge_ledger("
                    "project_key, merge_sha, repository, pr_number, "
                    "integration_branch, candidate_sha, candidate_branch, "
                    "state, reason, packet_json, recorded_at, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, 'unresolved', NULL, "
                    "NULL, ?, ?)",
                    (
                        merge.project_key,
                        merge.merge_sha,
                        merge.repository,
                        merge.pr_number,
                        merge.integration_branch,
                        merge.candidate_sha,
                        merge.candidate_branch,
                        stamp,
                        stamp,
                    ),
                )
        except sqlite3.IntegrityError as error:
            row = self._database.execute(
                "SELECT repository, pr_number, integration_branch, "
                "candidate_sha, candidate_branch "
                "FROM ci_merge_ledger WHERE project_key = ? AND merge_sha = ?",
                (merge.project_key, merge.merge_sha),
            ).fetchone()
            if row is None or (
                row["repository"] != merge.repository
                or row["pr_number"] != merge.pr_number
                or row["integration_branch"] != merge.integration_branch
                or row["candidate_sha"] != merge.candidate_sha
                or row["candidate_branch"] != merge.candidate_branch
            ):
                raise ValueError(
                    "merge SHA is already recorded with a different identity"
                ) from error

    def has_capacity(self, project_key: str) -> bool:
        """True when one more merged item fits the unresolved window.

        Read-only: this never consults CI. It lets the merge step refuse to
        create a third unresolved item even if a stale intake decision
        slipped past a concurrent merge.
        """

        return len(self.unresolved_items(project_key)) < self._max_unresolved

    def unresolved_items(self, project_key: str) -> tuple[UnresolvedMerge, ...]:
        """List unresolved merged items in durable recording order."""

        rows = self._database.execute(
            "SELECT project_key, repository, pr_number, merge_sha, "
            "integration_branch, candidate_sha, candidate_branch, "
            "recorded_at FROM ci_merge_ledger "
            "WHERE project_key = ? AND state = 'unresolved' "
            "ORDER BY recorded_at ASC, rowid ASC",
            (project_key,),
        ).fetchall()
        return tuple(
            UnresolvedMerge(
                project_key=str(row["project_key"]),
                repository=str(row["repository"]),
                pr_number=int(row["pr_number"]),
                merge_sha=str(row["merge_sha"]),
                integration_branch=str(row["integration_branch"]),
                candidate_sha=str(row["candidate_sha"]),
                candidate_branch=str(row["candidate_branch"]),
                recorded_at=str(row["recorded_at"]),
            )
            for row in rows
        )

    def reconcile_event(
        self,
        project_key: str,
        event_id: str,
        identity: ReconciliationIdentity,
    ) -> IntakeDecision:
        """Reconcile under a durable claim bound to one full wake identity.

        Only the claim winner queries CircleCI. A duplicate of a decided
        event replays with zero CI calls, but the replayed decision is
        always re-derived from current committed ledger state — a stored
        ``clear`` can never override a durable failure or a full window. A
        live claim fails closed as :class:`ReconciliationPending` with zero
        CI calls; once its lease expires, exactly one recovery attempt may
        atomically take ownership and reconcile (read-only crash recovery).
        Reusing an event id with a different identity fails closed with
        zero CI calls and never replays.
        """

        if not event_id:
            raise ValueError("reconciliation requires a wake event id")
        identity_json = identity.as_json()
        now = self._now()
        if now.tzinfo is None:
            raise ValueError("the injected clock must return aware datetimes")
        stamp = now.isoformat()
        expires = (
            now + timedelta(seconds=self._claim_lease_seconds)
        ).isoformat()
        token = uuid.uuid4().hex
        try:
            with self._database.transaction() as connection:
                connection.execute(
                    "INSERT INTO ci_reconciliation_claims("
                    "project_key, event_id, identity_json, state, "
                    "owner_token, lease_expires_at, decision_json, "
                    "created_at, updated_at"
                    ") VALUES (?, ?, ?, 'claimed', ?, ?, NULL, ?, ?)",
                    (
                        project_key,
                        event_id,
                        identity_json,
                        token,
                        expires,
                        stamp,
                        stamp,
                    ),
                )
        except sqlite3.IntegrityError:
            return self._replay_or_recover(
                project_key, event_id, identity_json, token, now, expires
            )
        return self._decide(project_key, event_id, token, identity_json)

    def _decide(
        self,
        project_key: str,
        event_id: str,
        token: str,
        identity_json: str,
    ) -> IntakeDecision:
        guard = _ClaimGuard(
            project_key=project_key,
            event_id=event_id,
            owner_token=token,
            identity_json=identity_json,
        )
        try:
            decision = self._reconcile(project_key, guard)
        except _OwnershipLost:
            # Another attempt recovered the claim mid-reconcile: this stale
            # worker performed no further ledger mutation and reports only
            # the committed state, with zero additional CI queries.
            return self._decision_from_committed(project_key)
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE ci_reconciliation_claims SET state = 'decided', "
                "decision_json = ?, updated_at = ? "
                "WHERE project_key = ? AND event_id = ? "
                "AND state = 'claimed' AND owner_token = ?",
                (
                    _decision_to_json(decision),
                    self._now().isoformat(),
                    project_key,
                    event_id,
                    token,
                ),
            )
        if cursor.rowcount != 1:
            # Ownership was recovered by another attempt mid-reconcile;
            # committed ledger state is the only authority.
            return self._decision_from_committed(project_key)
        return decision

    def _replay_or_recover(
        self,
        project_key: str,
        event_id: str,
        identity_json: str,
        token: str,
        now: datetime,
        expires: str,
    ) -> IntakeDecision:
        row = self._database.execute(
            "SELECT identity_json, state, owner_token, lease_expires_at "
            "FROM ci_reconciliation_claims "
            "WHERE project_key = ? AND event_id = ?",
            (project_key, event_id),
        ).fetchone()
        if row is None:
            raise ReconciliationPending(
                "reconciliation claim state is unreadable; no CI queries "
                "were made"
            )
        claim = _validated_claim(row)
        if claim.identity_json != identity_json:
            raise ReconciliationIdentityMismatch(
                "event id was reused with a different wake identity; no CI "
                "queries were made and no decision was replayed"
            )
        if claim.state == "decided":
            return self._decision_from_committed(project_key)
        if claim.lease_expires_at > now:
            raise ReconciliationPending(
                "reconciliation for this event is claimed by a live owner; "
                "no CI queries were made"
            )
        # Expired lease (decided on parsed aware datetimes, never string
        # order): exactly one recovery attempt may take ownership, via a
        # CAS on the exact observed owner token and lease value.
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE ci_reconciliation_claims SET owner_token = ?, "
                "lease_expires_at = ?, updated_at = ? "
                "WHERE project_key = ? AND event_id = ? "
                "AND state = 'claimed' AND owner_token = ? "
                "AND lease_expires_at = ?",
                (
                    token,
                    expires,
                    now.isoformat(),
                    project_key,
                    event_id,
                    claim.owner_token,
                    claim.lease_raw,
                ),
            )
        if cursor.rowcount == 1:
            return self._decide(project_key, event_id, token, identity_json)
        refreshed = self._database.execute(
            "SELECT state FROM ci_reconciliation_claims "
            "WHERE project_key = ? AND event_id = ?",
            (project_key, event_id),
        ).fetchone()
        if refreshed is not None and refreshed["state"] == "decided":
            return self._decision_from_committed(project_key)
        raise ReconciliationPending(
            "reconciliation recovery was won by another attempt; no CI "
            "queries were made"
        )

    def _decision_from_committed(self, project_key: str) -> IntakeDecision:
        """Authoritative zero-CI decision from current committed state.

        Used for every replay: an existing durable failure returns blocked
        with its packet and a full unresolved window returns deferred, so a
        cached ``clear`` can never override more restrictive durable state.
        """

        stored = self._stored_failure(project_key)
        if stored is not None:
            return self._blocked(project_key, stored, resolved=())
        unresolved = tuple(
            item.merge_sha for item in self.unresolved_items(project_key)
        )
        if len(unresolved) >= self._max_unresolved:
            return IntakeDecision(
                kind="deferred_window_full",
                resolved=(),
                unresolved=unresolved,
                reason=(
                    "the unresolved merged-candidate window is full; "
                    "intake deferred until a prior pipeline resolves"
                ),
            )
        return IntakeDecision(kind="clear", resolved=(), unresolved=unresolved)

    def reconcile_at_intake(self, project_key: str) -> IntakeDecision:
        """Reconcile every prior unresolved merged item exactly once.

        A durably recorded prior failure blocks immediately with its stored
        packet and no new CI query. Otherwise each unresolved item is
        checked once: success resolves durably, failure records the packet
        and stops before later items, and anything nonterminal or ambiguous
        stays optimistic — never resolved as success and still occupying
        the window. The returned decision is derived from committed ledger
        state, so a competing durable transition always wins over a local
        observation.
        """

        return self._reconcile(project_key, None)

    def _reconcile(
        self, project_key: str, guard: _ClaimGuard | None
    ) -> IntakeDecision:
        stored = self._stored_failure(project_key)
        if stored is not None:
            return self._blocked(project_key, stored, resolved=())
        resolved: list[str] = []
        for item in self.unresolved_items(project_key):
            slug = f"gh/{item.repository}"
            try:
                check = self._status.check(
                    slug, item.integration_branch, item.merge_sha
                )
            except CircleCiError:
                # Unavailable or malformed CI is ambiguous: stay optimistic
                # inside the window and never fabricate success.
                continue
            if check.outcome == "success":
                changed = self._transition(
                    project_key,
                    item.merge_sha,
                    "resolved",
                    check.reason,
                    None,
                    guard=guard,
                )
                if changed:
                    resolved.append(item.merge_sha)
                else:
                    self._require_ownership(guard)
                    if self._row_state(project_key, item.merge_sha) == "failed":
                        break
            elif check.outcome == "failure":
                recorded = self._transition(
                    project_key,
                    item.merge_sha,
                    "failed",
                    check.reason,
                    _failure_packet(item, check),
                    guard=guard,
                )
                if not recorded:
                    self._require_ownership(guard)
                break
        stored = self._stored_failure(project_key)
        unresolved = tuple(
            item.merge_sha for item in self.unresolved_items(project_key)
        )
        if stored is not None:
            return self._blocked(
                project_key, stored, resolved=tuple(resolved)
            )
        if len(unresolved) >= self._max_unresolved:
            return IntakeDecision(
                kind="deferred_window_full",
                resolved=tuple(resolved),
                unresolved=unresolved,
                reason=(
                    "the unresolved merged-candidate window is full; "
                    "intake deferred until a prior pipeline resolves"
                ),
            )
        return IntakeDecision(
            kind="clear", resolved=tuple(resolved), unresolved=unresolved
        )

    @property
    def capacity(self) -> int:
        return self._max_unresolved

    def stored_failure(self, project_key: str) -> CorrectionPacket | None:
        """The oldest durable CircleCI failure packet blocking intake, if any."""

        return self._stored_failure(project_key)

    def close_failure_for_rework(
        self,
        project_key: str,
        *,
        failed_candidate_sha: str,
        rework: CandidateManifest,
        correction_id: str,
    ) -> bool:
        """Durably close the failure that an admitted, authorized rework binds.

        Called only after the FABLE_REWORK_READY candidate passed immutable
        manifest, channel generation, branch head, base, issue, and pull
        request validation and was admitted. Exactly the failed item with
        the bound candidate SHA on the rework's branch transitions to
        ``corrected``; the binding (event, rework SHA, correction id) is
        recorded. Replaying the same event is idempotent; a different event
        or item never changes another row. Never queries CI.
        """

        if rework.status != "FABLE_REWORK_READY":
            raise ValueError("only a FABLE_REWORK_READY candidate can close a failure")
        binding = {
            "event_id": rework.event_id,
            "rework_sha": rework.candidate_sha,
            "correction_id": correction_id,
            "issues": list(rework.linear_issues),
        }
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE ci_merge_ledger SET state = 'corrected', "
                "correction_json = ?, updated_at = ? "
                "WHERE project_key = ? AND candidate_sha = ? "
                "AND candidate_branch = ? AND state = 'failed'",
                (
                    json.dumps(binding, sort_keys=True, separators=(",", ":")),
                    stamp,
                    project_key,
                    failed_candidate_sha,
                    rework.branch,
                ),
            )
            if cursor.rowcount == 1:
                return True
            row = connection.execute(
                "SELECT state, correction_json FROM ci_merge_ledger "
                "WHERE project_key = ? AND candidate_sha = ? "
                "AND candidate_branch = ?",
                (project_key, failed_candidate_sha, rework.branch),
            ).fetchone()
        if row is None or row["state"] != "corrected" or row["correction_json"] is None:
            return False
        recorded = json.loads(str(row["correction_json"]))
        return recorded.get("event_id") == rework.event_id

    def mark_corrected(self, project_key: str, merge_sha: str) -> None:
        """Durably close a failed item once its correction is completed."""

        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE ci_merge_ledger SET state = 'corrected', "
                "updated_at = ? WHERE project_key = ? AND merge_sha = ? "
                "AND state = 'failed'",
                (self._now().isoformat(), project_key, merge_sha),
            )
        if cursor.rowcount != 1:
            raise ValueError("only a failed merged item can be marked corrected")

    def _blocked(
        self,
        project_key: str,
        packet: CorrectionPacket,
        *,
        resolved: tuple[str, ...],
    ) -> IntakeDecision:
        return IntakeDecision(
            kind="blocked_prior_failure",
            resolved=resolved,
            unresolved=tuple(
                item.merge_sha for item in self.unresolved_items(project_key)
            ),
            packet=packet,
            reason="a prior merged candidate failed CircleCI",
        )

    def _stored_failure(self, project_key: str) -> CorrectionPacket | None:
        row = self._database.execute(
            "SELECT packet_json FROM ci_merge_ledger "
            "WHERE project_key = ? AND state = 'failed' "
            "ORDER BY recorded_at ASC, rowid ASC LIMIT 1",
            (project_key,),
        ).fetchone()
        if row is None or row["packet_json"] is None:
            return None
        return _packet_from_dict(json.loads(str(row["packet_json"])))

    def _row_state(self, project_key: str, merge_sha: str) -> str | None:
        row = self._database.execute(
            "SELECT state FROM ci_merge_ledger "
            "WHERE project_key = ? AND merge_sha = ?",
            (project_key, merge_sha),
        ).fetchone()
        return None if row is None else str(row["state"])

    def _transition(
        self,
        project_key: str,
        merge_sha: str,
        state: str,
        reason: str,
        packet: CorrectionPacket | None,
        *,
        guard: _ClaimGuard | None,
    ) -> bool:
        """Compare-and-swap one unresolved item; report whether we won.

        When the transition happens during an event reconciliation, the
        same transaction requires the event's claim to still be held by
        this worker's exact owner token and identity; a stale owner can
        therefore never mutate the ledger.
        """

        packet_json = (
            json.dumps(
                _packet_to_dict(packet), sort_keys=True, separators=(",", ":")
            )
            if packet is not None
            else None
        )
        sql = (
            "UPDATE ci_merge_ledger SET state = ?, reason = ?, "
            "packet_json = ?, updated_at = ? "
            "WHERE project_key = ? AND merge_sha = ? "
            "AND state = 'unresolved'"
        )
        parameters: list[object] = [
            state,
            sanitize_text(reason),
            packet_json,
            self._now().isoformat(),
            project_key,
            merge_sha,
        ]
        if guard is not None:
            sql += (
                " AND EXISTS (SELECT 1 FROM ci_reconciliation_claims "
                "WHERE project_key = ? AND event_id = ? "
                "AND state = 'claimed' AND owner_token = ? "
                "AND identity_json = ?)"
            )
            parameters.extend(
                [
                    guard.project_key,
                    guard.event_id,
                    guard.owner_token,
                    guard.identity_json,
                ]
            )
        with self._database.transaction() as connection:
            cursor = connection.execute(sql, tuple(parameters))
        return cursor.rowcount == 1

    def _require_ownership(self, guard: _ClaimGuard | None) -> None:
        """Abort a stale event reconciliation the moment ownership is gone."""

        if guard is None:
            return
        row = self._database.execute(
            "SELECT 1 FROM ci_reconciliation_claims "
            "WHERE project_key = ? AND event_id = ? "
            "AND state = 'claimed' AND owner_token = ? "
            "AND identity_json = ?",
            (
                guard.project_key,
                guard.event_id,
                guard.owner_token,
                guard.identity_json,
            ),
        ).fetchone()
        if row is None:
            raise _OwnershipLost


class CircleCiIntakeGate:
    """INFRA-165 implementation of the review-intake validation port.

    Ordered before the GitHub pull-request gate so all prior merged items
    are reconciled before the new candidate is reviewed, prepared, or its
    pull request considered. The reconciliation is bound to the candidate's
    wake event: duplicate or stale events replay the recorded decision with
    zero CI calls. Fails closed by raising typed
    :class:`CandidateRejected` subclasses that carry the routing payload.
    """

    def __init__(
        self, window: CiWindow, *, corrections: ReworkAuthorizer | None = None
    ) -> None:
        self._window = window
        self._corrections = corrections

    def validate(
        self, project_key: str, candidate: CandidateManifest
    ) -> None:
        identity = ReconciliationIdentity(
            status=candidate.status,
            candidate_sha=candidate.candidate_sha,
            base_sha=candidate.base_sha,
            branch=candidate.branch,
            issues=tuple(sorted(candidate.linear_issues)),
        )
        decision = self._window.reconcile_event(
            project_key, candidate.event_id, identity
        )
        if decision.kind == "blocked_prior_failure":
            packet = decision.packet
            if (
                packet is not None
                and self._corrections is not None
                and self._corrections.authorized_rework(project_key, candidate, packet)
                is not None
            ):
                # An explicitly authorized rework of the failed item may be
                # admitted; the failure is closed only after full admission.
                if len(decision.unresolved) >= self._window.capacity:
                    raise MergeWindowExhausted(
                        "the unresolved merged-candidate window is full; "
                        "rework intake deferred until a prior pipeline resolves",
                        unresolved=decision.unresolved,
                    )
                return
            raise PriorMergeFailed(
                "a prior merged candidate failed CircleCI and must be "
                "corrected before any new intake",
                packet=decision.packet,
            )
        if decision.kind == "deferred_window_full":
            raise MergeWindowExhausted(
                "two merged candidates are still unresolved; intake is "
                "deferred until a prior pipeline resolves",
                unresolved=decision.unresolved,
            )


def _validated_claim(row: Any) -> _ClaimRow:
    """Strictly validate one claim row before any decision or CI call.

    Malformed values fail closed as :class:`ReconciliationClaimCorrupt`;
    the lease timestamp must parse as an aware datetime — expiry is never
    decided by string comparison.
    """

    identity_json = row["identity_json"]
    state = row["state"]
    owner_token = row["owner_token"]
    lease_raw = row["lease_expires_at"]
    if not isinstance(identity_json, str) or not identity_json:
        raise ReconciliationClaimCorrupt(
            "claim identity is malformed; no CI queries were made"
        )
    if state not in ("claimed", "decided"):
        raise ReconciliationClaimCorrupt(
            "claim state is malformed; no CI queries were made"
        )
    if not isinstance(owner_token, str) or _TOKEN_PATTERN.match(owner_token) is None:
        raise ReconciliationClaimCorrupt(
            "claim owner token is malformed; no CI queries were made"
        )
    if not isinstance(lease_raw, str) or not lease_raw:
        raise ReconciliationClaimCorrupt(
            "claim lease timestamp is missing; no CI queries were made"
        )
    try:
        parsed = datetime.fromisoformat(lease_raw)
    except ValueError as error:
        raise ReconciliationClaimCorrupt(
            "claim lease timestamp is unparseable; no CI queries were made"
        ) from error
    if parsed.tzinfo is None:
        raise ReconciliationClaimCorrupt(
            "claim lease timestamp is not timezone-aware; no CI queries "
            "were made"
        )
    return _ClaimRow(
        identity_json=identity_json,
        state=str(state),
        owner_token=owner_token,
        lease_raw=lease_raw,
        lease_expires_at=parsed,
    )


def _failure_packet(item: UnresolvedMerge, check: CiCheck) -> CorrectionPacket:
    """Bind a CI failure to the reviewed candidate identity.

    The packet's branch and reviewed SHA are the prior lead's feature
    branch and candidate SHA; the merge commit is named separately inside
    the evidence. Every free-text field is sanitized and bounded.
    """

    details = "; ".join(check.evidence) if check.evidence else check.reason
    evidence = sanitize_text(f"merge {item.merge_sha}: {details}", 600)
    required_tests = tuple(
        sanitize_text(f"CircleCI: {line}") for line in check.evidence[:5]
    ) or ("CircleCI: re-run the failed workflow to success",)
    return CorrectionPacket(
        severity="Critical",
        repository=item.repository,
        branch=item.candidate_branch,
        pr_number=item.pr_number,
        reviewed_sha=item.candidate_sha,
        evidence=evidence,
        acceptance_criterion=(
            "CircleCI workflows for the merged candidate succeed on the "
            "integration branch"
        ),
        required_correction=sanitize_text(
            f"Fix the CircleCI failure for merged PR #{item.pr_number} "
            f"({check.reason}) and deliver a corrected candidate",
            400,
        ),
        required_tests=required_tests,
    )


def _packet_to_dict(packet: CorrectionPacket) -> dict[str, Any]:
    return {
        "severity": packet.severity,
        "repository": packet.repository,
        "branch": packet.branch,
        "pr_number": packet.pr_number,
        "reviewed_sha": packet.reviewed_sha,
        "evidence": packet.evidence,
        "acceptance_criterion": packet.acceptance_criterion,
        "required_correction": packet.required_correction,
        "required_tests": list(packet.required_tests),
    }


def _packet_from_dict(value: dict[str, Any]) -> CorrectionPacket:
    return CorrectionPacket(
        severity=value["severity"],
        repository=value["repository"],
        branch=value["branch"],
        pr_number=value["pr_number"],
        reviewed_sha=value["reviewed_sha"],
        evidence=value["evidence"],
        acceptance_criterion=value["acceptance_criterion"],
        required_correction=value["required_correction"],
        required_tests=tuple(value["required_tests"]),
    )


def _decision_to_json(decision: IntakeDecision) -> str:
    """Serialize a decision as a durable audit record.

    Replays never deserialize this back into a decision: every replay
    derives its result from current committed ledger state via
    ``CiWindow._decision_from_committed``.
    """

    return json.dumps(
        {
            "kind": decision.kind,
            "resolved": list(decision.resolved),
            "unresolved": list(decision.unresolved),
            "reason": decision.reason,
            "packet": (
                _packet_to_dict(decision.packet)
                if decision.packet is not None
                else None
            ),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
