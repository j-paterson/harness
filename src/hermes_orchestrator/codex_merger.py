"""Persistent, project-scoped Codex Merger reviewer channels."""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from hermes_orchestrator.codex_ponytail_guard import session_guard_config
from hermes_orchestrator.codex_rpc import (
    CodexRateLimits,
    CodexRequestFailed,
    parse_rate_limits,
)
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.manifests import (
    ManifestError,
    ManifestIdentity,
    ManifestSnapshot,
    WakeEvent,
    read_manifest_snapshot,
)


class _WakeCasMiss(Exception):
    """Internal: roll back the combined delivery transaction on a CAS miss."""

MERGER_MODEL = "gpt-5.6-sol"

# INFRA-200: the versioned forward-implementation-first review contract
# (prompts/codex-merger.md, adapted from Vuk97/forward-implementation-first
# at pinned commit 91fa46a0108ecfc612a55cf587a2086621a31161, MIT license; no
# runtime dependency). Bumping this string is the ONLY signal that forces
# every reviewer channel to redeliver the contract -- a new thread gets it
# at launch, and an existing thread whose ``contract_version`` no longer
# matches adopts it at its next real candidate intake, never from an idle
# wake.
MERGER_CONTRACT_VERSION = "fif-1"

_CONTRACT_HEADER_PREFIX = "Hermes Sol Merger contract"


# The narrow writable Codex workspace mode (INFRA-194 operator scope):
# the bounded ACCEPT_WITH_REVIEWER_FIX path must be able to write and
# commit inside the project workspace, and nothing beyond it — never
# the dangerous unrestricted mode. Passing the same mode on every
# thread/resume is the recoverable configuration path for a task
# started under an older mode: the next load corrects it without
# waking the thread or duplicating review intake. The session-scoped
# Ponytail review guard (operator correction c3f4aad5) rides the same
# path: the thread config override on every start and resume is the
# only place the guard is bound, so exactly the managed Sol session —
# and no other Codex session — gates git commit and git push on a
# Ponytail review of the current diff.
SANDBOX_MODE = "workspace-write"

# The durable thread goal is deliberately one succinct paragraph: the
# role, its boundaries, and the idle token. Detailed protocol
# mechanics live in the durable contract artifact
# (prompts/codex-merger.md) and in Hermes code — never in the goal.
MERGER_GOAL = (
    "You are this project's independent reviewer and final merge "
    "authority: review exactly one immutable candidate at a time from "
    "durable Hermes intake; you may make only bounded mechanical, "
    "unambiguous, auditable fixes as a single labeled "
    "ACCEPT_WITH_REVIEWER_FIX commit under the durable repair policy, "
    "and every larger or judgment-bearing Critical or Important "
    "defect returns to the Fable lead as structured corrections; you "
    "complete the exact approved pull-request merge yourself, "
    "preferably through the guarded Hermes settlement helper because "
    "it journals atomically (an exact-head direct merge is permitted "
    "and is reconciled into the same durable receipts); CircleCI is "
    "checked optimistically at intake and merge boundaries only; and "
    "when no eligible intake exists you report exactly "
    "BLOCKED_ON_EXTERNAL_INTAKE and complete the turn. The detailed "
    "role contract and protocol mechanics live durably in the versioned "
    "prompts/codex-merger.md contract (fif-1) and in Hermes code, never "
    "in this goal."
)
_SERVICE_NAME = "hermes_orchestrator"

# The installed codex-cli 0.149.0-alpha.4.1 schema pins threads by moving them
# into the built-in "Pinned" thread section (threadSection/list +
# thread/section/move); the section id is resolved by name at configuration
# time and never hard-coded. Channel metadata such as the integration branch
# is persisted in the reviewer_channels record.
PINNED_SECTION_NAME = "Pinned"
_SECTION_PAGE_LIMIT = 16

# ThreadGoalStatus value that leaves the durable objective recorded but idle,
# matching the BLOCKED_ON_EXTERNAL_INTAKE turn contract: no active goal
# continuation until a validated explicit wake arrives.
IDLE_GOAL_STATUS = "blocked"


def _reviewer_is_held(
    connection: sqlite3.Connection,
    project_key: str,
    *,
    except_event_id: str,
    stamp: str,
) -> bool:
    """True iff another candidate currently holds the primary Sol Merger.

    INFRA-221: exactly ONE candidate may be active at the reviewer at a
    time. A candidate holds the reviewer while it is genuinely occupying
    the thread with an unsettled verdict: an unexpired in-flight delivery
    claim (``claimed``), a wake delivered into the thread whose verdict
    has not settled (``delivered``), or the single admitted candidate
    (``admitted``). Every other durable state -- ``pending`` (queued, not
    yet delivered) and the terminal ``completed``/``deferred``/
    ``rejected`` written when a verdict settles -- leaves the reviewer
    free. An EXPIRED claim is an abandoned delivery, not a held reviewer,
    so a crashed deliverer can never wedge the queue past its bounded
    lease -- but every caller must first run
    :func:`_requeue_expired_claims` in the same transaction, so that an
    abandoned claim is INVALIDATED rather than merely ignored before any
    other candidate is allowed out. The row for ``except_event_id`` is
    never counted: a wake never blocks itself (its own reopen and
    re-claim must still work).
    """

    row = connection.execute(
        "SELECT 1 FROM wake_deliveries WHERE project_key = ? "
        "AND event_id != ? AND (state IN ('delivered', 'admitted') "
        "OR (state = 'claimed' AND claim_expires_at IS NOT NULL "
        "AND claim_expires_at > ?)) LIMIT 1",
        (project_key, except_event_id, stamp),
    ).fetchone()
    return row is not None


def _requeue_expired_claims(
    connection: sqlite3.Connection, project_key: str, *, stamp: str
) -> None:
    """Invalidate every lapsed delivery claim, returning it to the queue.

    Sol correction 370eb20c: honouring expiry when deciding whether the
    reviewer is held is what stops a crashed deliverer from wedging the
    queue, but ignoring an expired claim while leaving its token usable
    opened a window in which a LATE success from that abandoned claimant
    could deliver a second candidate. So expiry is applied as a durable
    write, never as a read-time exemption: the lapsed row goes back to
    ``pending`` and its ``claim_token`` is dropped, which both keeps the
    candidate queued (never skipped) and makes the abandoned token
    permanently unusable at :meth:`CodexMerger.record_wake_delivery_success`.

    Callers run this inside the SAME ``BEGIN IMMEDIATE`` transaction as
    the :func:`_reviewer_is_held` check and the release it guards, so no
    late success can slip between the reap and the release.
    """

    connection.execute(
        "UPDATE wake_deliveries SET state = 'pending', claim_token = NULL, "
        "claim_expires_at = NULL, updated_at = ? "
        "WHERE project_key = ? AND state = 'claimed' "
        "AND (claim_expires_at IS NULL OR claim_expires_at <= ?)",
        (stamp, project_key, stamp),
    )


class RpcRequester(Protocol):
    """The stable request surface the Merger needs from the RPC client."""

    async def request(
        self, method: str, params: dict[str, Any] | None, timeout: float
    ) -> dict[str, Any]: ...


class MergerThreadUncertain(RuntimeError):
    """Raised when a persisted thread cannot be trusted without an operator."""


class MergerAuthRequired(RuntimeError):
    """Raised when the App Server is not ChatGPT-authenticated."""


class StaleChannelError(RuntimeError):
    """Raised when a compare-and-swap expectation no longer matches."""


@dataclass(frozen=True, slots=True)
class MergerThread:
    """One usable project Merger thread."""

    project_key: str
    thread_id: str


@dataclass(frozen=True, slots=True)
class MergerStatus:
    """Summary state for one project channel."""

    project_key: str
    thread_id: str | None
    state: str


@dataclass(frozen=True, slots=True)
class ReviewerChannel:
    """Durable source of truth for one project's Merger reviewer channel."""

    project_key: str
    thread_id: str
    generation: int
    state: str
    integration_branch: str
    prior_thread_id: str | None
    replacement_reason: str | None
    last_delivered_event_id: str | None
    last_delivered_candidate_sha: str | None
    last_delivery_failure_at: str | None
    heartbeat_enabled: bool
    contract_version: str | None


@dataclass(frozen=True, slots=True)
class WakeRegistration:
    """Outcome of one durable wake registration, with the live channel.

    The channel identity is resolved inside the registration transaction —
    callers must never act on their own earlier channel snapshot. A
    ``claim_token`` is present only when this caller atomically acquired
    the exclusive delivery claim and may invoke the queue CLI.
    """

    state: str
    channel_state: str | None
    thread_id: str | None
    generation: int | None
    claim_token: str | None = None


@dataclass(frozen=True, slots=True)
class CodexAuthHealth:
    """Merge-gate eligibility of the App Server account, identity-free."""

    eligible: bool
    reason: str


class CodexMerger:
    """Create, resume, and interrogate one durable Merger thread per project."""

    def __init__(
        self,
        *,
        rpc: RpcRequester,
        database: Database,
        projects: Mapping[str, ProjectConfig],
        prompt_file: Path,
        now: Callable[[], datetime] | None = None,
        request_timeout: float = 60.0,
        model: str = MERGER_MODEL,
        wake_claim_lease_seconds: float = 300.0,
    ) -> None:
        if request_timeout <= 0:
            raise ValueError("request timeout must be positive")
        if wake_claim_lease_seconds < 0:
            raise ValueError("wake claim lease must not be negative")
        self._wake_claim_lease_seconds = wake_claim_lease_seconds
        self._rpc = rpc
        self._database = database
        self._projects = dict(projects)
        self._contract = prompt_file.read_text(encoding="utf-8")
        self._now = now or (lambda: datetime.now(UTC))
        self._timeout = request_timeout
        self._model = model

    async def ensure_thread(self, project_key: str) -> MergerThread:
        """Return a usable persisted thread, creating or resuming as needed."""

        project = self._project(project_key)
        health = await self.verify_chatgpt_auth()
        if not health.eligible:
            raise MergerAuthRequired(
                "codex merger requires chatgpt authentication before any "
                "thread operation"
            )
        channel = self.read_channel(project_key)
        if channel is None:
            return await self._create(project_key, project)
        if channel.state == "configuring":
            return await self._finish_configuration(
                project_key, channel.thread_id
            )
        if channel.state != "ready":
            raise MergerThreadUncertain(
                f"reviewer channel for {project_key} is {channel.state} and "
                "requires operator reconciliation"
            )
        return await self._resume_current(project_key)

    def read_channel(self, project_key: str) -> ReviewerChannel | None:
        """Read the durable reviewer-channel record without provider effects."""

        self._project(project_key)
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT thread_id, generation, state, integration_branch, "
                "prior_thread_id, "
                "replacement_reason, last_delivered_event_id, "
                "last_delivered_candidate_sha, last_delivery_failure_at, "
                "heartbeat_enabled, contract_version FROM reviewer_channels "
                "WHERE project_key = ?",
                (project_key,),
            ).fetchone()
        if row is None:
            return None
        return ReviewerChannel(
            project_key=project_key,
            thread_id=str(row["thread_id"]),
            generation=int(row["generation"]),
            state=str(row["state"]),
            integration_branch=str(row["integration_branch"]),
            prior_thread_id=row["prior_thread_id"],
            replacement_reason=row["replacement_reason"],
            last_delivered_event_id=row["last_delivered_event_id"],
            last_delivered_candidate_sha=row["last_delivered_candidate_sha"],
            last_delivery_failure_at=row["last_delivery_failure_at"],
            heartbeat_enabled=bool(row["heartbeat_enabled"]),
            contract_version=row["contract_version"],
        )

    def read_status(self, project_key: str) -> MergerStatus:
        """Summarize the persisted channel as missing, ready, or otherwise."""

        channel = self.read_channel(project_key)
        if channel is None:
            return MergerStatus(
                project_key=project_key, thread_id=None, state="missing"
            )
        return MergerStatus(
            project_key=project_key,
            thread_id=channel.thread_id,
            state=channel.state,
        )

    def begin_replacement(
        self,
        project_key: str,
        *,
        expected_thread_id: str,
        expected_generation: int,
        reason: str,
    ) -> ReviewerChannel:
        """Atomically mark the channel replacing when expectations still hold."""

        self._project(project_key)
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET state = 'replacing', "
                "replacement_reason = ?, updated_at = ? "
                "WHERE project_key = ? AND thread_id = ? AND generation = ? "
                "AND state != 'replacing'",
                (
                    reason,
                    self._now().isoformat(),
                    project_key,
                    expected_thread_id,
                    expected_generation,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleChannelError(
                    f"reviewer channel for {project_key} does not match the "
                    "expected thread and generation"
                )
        channel = self.read_channel(project_key)
        assert channel is not None
        return channel

    def complete_replacement(
        self,
        project_key: str,
        *,
        expected_thread_id: str,
        expected_generation: int,
        new_thread_id: str,
    ) -> ReviewerChannel:
        """Atomically install the new thread and increment the generation."""

        self._project(project_key)
        if not new_thread_id:
            raise ValueError("replacement thread id must not be empty")
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET thread_id = ?, "
                "generation = generation + 1, state = 'ready', "
                "prior_thread_id = ?, contract_version = NULL, "
                "updated_at = ? "
                "WHERE project_key = ? AND thread_id = ? AND generation = ? "
                "AND state = 'replacing'",
                (
                    new_thread_id,
                    expected_thread_id,
                    stamp,
                    project_key,
                    expected_thread_id,
                    expected_generation,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleChannelError(
                    f"reviewer channel for {project_key} does not match the "
                    "expected replacing thread and generation"
                )
            self._recompute_heartbeat(connection, project_key, stamp)
        channel = self.read_channel(project_key)
        assert channel is not None
        return channel

    def record_delivery_success(
        self,
        project_key: str,
        *,
        thread_id: str,
        generation: int,
        event_id: str | None = None,
        candidate_sha: str | None = None,
    ) -> bool:
        """Record a delivery for the exact ready channel that was invoked.

        Returns False on a compare-and-swap miss so a wake delivered to a
        replaced thread is never claimed on the new channel.
        """

        self._project(project_key)
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET "
                "last_delivered_event_id = coalesce(?, last_delivered_event_id), "
                "last_delivered_candidate_sha = "
                "coalesce(?, last_delivered_candidate_sha), "
                "heartbeat_enabled = 0, updated_at = ? "
                "WHERE project_key = ? AND thread_id = ? AND generation = ? "
                "AND state = 'ready'",
                (
                    event_id,
                    candidate_sha,
                    self._now().isoformat(),
                    project_key,
                    thread_id,
                    generation,
                ),
            )
        return cursor.rowcount == 1

    def record_delivery_failure(
        self, project_key: str, *, thread_id: str, generation: int
    ) -> bool:
        """Record a failure for the exact channel identity that was invoked."""

        self._project(project_key)
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET last_delivery_failure_at = ?, "
                "heartbeat_enabled = 1, updated_at = ? "
                "WHERE project_key = ? AND thread_id = ? AND generation = ?",
                (stamp, stamp, project_key, thread_id, generation),
            )
        return cursor.rowcount == 1

    def record_contract_delivered(
        self, project_key: str, *, thread_id: str, generation: int
    ) -> bool:
        """CAS-record that the versioned contract reached this exact thread.

        INFRA-200: written once a delivery that carried the contract text
        (new-thread launch or an existing thread's next real candidate
        intake) is confirmed, never speculatively. The compare-and-swap on
        the exact ``thread_id``/``generation`` invoked keeps a late write
        from a replaced or stale channel from ever marking the WRONG
        generation adopted -- exactly the discipline every other
        per-generation delivery fact on this table already follows.
        """

        self._project(project_key)
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET "
                "contract_version = ?, updated_at = ? "
                "WHERE project_key = ? AND thread_id = ? AND generation = ?",
                (
                    MERGER_CONTRACT_VERSION,
                    self._now().isoformat(),
                    project_key,
                    thread_id,
                    generation,
                ),
            )
        return cursor.rowcount == 1

    def register_wake(
        self,
        project_key: str,
        event: WakeEvent,
        *,
        manifest: ManifestSnapshot,
    ) -> WakeRegistration:
        """Durably claim one wake for delivery, binding payload and identity.

        The live reviewer channel and the canonical candidate owner are
        resolved inside this transaction, never from a caller snapshot, and
        the transaction-current thread and generation are returned.
        Registration or reopening of a pending wake atomically arms
        heartbeat fallback before any external call. When the channel is
        ready, exactly one caller acquires the exclusive delivery claim and
        receives its token; an unexpired claim held elsewhere returns
        ``in_flight`` without any invoke, and an expired claim is durably
        invalidated and requeued before anything is released, so a crashed
        deliverer recovers after the bounded lease while its abandoned
        token can never complete afterwards. A wake (or
        the canonical owning event of the same candidate identity) that was
        delivered to a stale thread or generation is atomically reopened. A
        payload, digest, or durable file-identity divergence is a conflict
        that changes nothing.
        """

        self._project(project_key)
        if (
            event.manifest_digest != manifest.digest
            or event.event_id != manifest.manifest.event_id
            or event.status != manifest.manifest.status
            or event.candidate_sha != manifest.manifest.candidate_sha
            or event.base_sha != manifest.manifest.base_sha
            or event.issue_id not in manifest.manifest.linear_issues
        ):
            return WakeRegistration(
                state="conflict",
                channel_state=None,
                thread_id=None,
                generation=None,
            )
        moment = self._now()
        stamp = moment.isoformat()
        lease_deadline = (
            moment + timedelta(seconds=self._wake_claim_lease_seconds)
        ).isoformat()
        identity = manifest.identity
        with self._database.transaction() as connection:
            # Sol correction 370eb20c: invalidate abandoned claims FIRST,
            # in this same transaction, so that any claim this
            # registration is about to treat as expired has already lost
            # its token before another candidate can be released.
            _requeue_expired_claims(connection, project_key, stamp=stamp)
            channel_row = connection.execute(
                "SELECT thread_id, generation, state FROM reviewer_channels "
                "WHERE project_key = ?",
                (project_key,),
            ).fetchone()
            channel_state = (
                str(channel_row["state"]) if channel_row is not None else None
            )
            live_thread = (
                str(channel_row["thread_id"])
                if channel_row is not None
                else None
            )
            live_generation = (
                int(channel_row["generation"])
                if channel_row is not None
                else None
            )

            def outcome(
                state: str, token: str | None = None
            ) -> WakeRegistration:
                return WakeRegistration(
                    state=state,
                    channel_state=channel_state,
                    thread_id=live_thread,
                    generation=live_generation,
                    claim_token=token,
                )

            def arm_heartbeat() -> None:
                connection.execute(
                    "UPDATE reviewer_channels SET heartbeat_enabled = 1, "
                    "updated_at = ? WHERE project_key = ?",
                    (stamp, project_key),
                )

            def release_or_queue(event_id: str) -> WakeRegistration:
                """Claim this wake for delivery, or leave it durably queued.

                INFRA-221: the one-candidate-at-a-time gate. While another
                candidate holds the reviewer with an unsettled verdict, this
                wake keeps its durable ``pending`` row -- it IS the queue --
                and no delivery claim is issued, so nothing is rendered into
                the thread. ``MergerTurnService`` releases it through the
                same delivery path once the current verdict settles.
                """

                if channel_state == "ready" and _reviewer_is_held(
                    connection, project_key, except_event_id=event_id, stamp=stamp
                ):
                    return outcome("queued")
                return outcome("pending", claim(event_id))

            def claim(event_id: str) -> str | None:
                if channel_state != "ready":
                    return None
                token = uuid.uuid4().hex
                cursor = connection.execute(
                    "UPDATE wake_deliveries SET state = 'claimed', "
                    "claim_token = ?, claim_expires_at = ?, updated_at = ? "
                    "WHERE project_key = ? AND event_id = ? "
                    "AND state = 'pending'",
                    (token, lease_deadline, stamp, project_key, event_id),
                )
                return token if cursor.rowcount == 1 else None

            def stale_delivery(row: sqlite3.Row) -> bool:
                return (
                    live_generation is not None
                    and channel_state == "ready"
                    and (
                        row["generation"] is None
                        or int(row["generation"]) != live_generation
                        or row["thread_id"] is None
                        or str(row["thread_id"]) != live_thread
                    )
                )

            def payload_conflicts(row: sqlite3.Row) -> bool:
                return (
                    str(row["status"]) != event.status
                    or str(row["issue_id"]) != event.issue_id
                    or str(row["candidate_sha"]) != event.candidate_sha
                    or str(row["base_sha"]) != event.base_sha
                    or str(row["branch"]) != manifest.manifest.branch
                    or str(row["manifest_path"]) != event.manifest_path
                    or str(row["manifest_digest"]) != event.manifest_digest
                    or int(row["manifest_device"]) != identity.device
                    or int(row["manifest_inode"]) != identity.inode
                    or int(row["manifest_size"]) != identity.size
                    or int(row["manifest_mtime_ns"]) != identity.mtime_ns
                    or int(row["manifest_mode"]) != identity.mode
                )

            row = connection.execute(
                "SELECT status, issue_id, candidate_sha, base_sha, branch, "
                "manifest_path, manifest_digest, manifest_device, "
                "manifest_inode, manifest_size, manifest_mtime_ns, "
                "manifest_mode, state, thread_id, generation "
                "FROM wake_deliveries "
                "WHERE project_key = ? AND event_id = ?",
                (project_key, event.event_id),
            ).fetchone()
            if row is not None:
                if payload_conflicts(row):
                    return outcome("conflict")
                state = str(row["state"])
                if state == "delivered" and stale_delivery(row):
                    connection.execute(
                        "UPDATE wake_deliveries SET state = 'pending', "
                        "claim_token = NULL, claim_expires_at = NULL, "
                        "updated_at = ? WHERE project_key = ? "
                        "AND event_id = ? AND state = 'delivered'",
                        (stamp, project_key, event.event_id),
                    )
                    arm_heartbeat()
                    return release_or_queue(event.event_id)
                if state == "claimed":
                    # Only an UNEXPIRED claim can still be read here: the
                    # reap above already returned any lapsed claim to
                    # 'pending' and voided its token.
                    arm_heartbeat()
                    return outcome("in_flight")
                if state == "pending":
                    arm_heartbeat()
                    return release_or_queue(event.event_id)
                return outcome(state)
            try:
                connection.execute(
                    "INSERT INTO wake_deliveries("
                    "project_key, event_id, status, issue_id, candidate_sha, "
                    "base_sha, branch, manifest_path, manifest_digest, "
                    "manifest_device, manifest_inode, manifest_size, "
                    "manifest_mtime_ns, manifest_mode, state, attempts, "
                    "created_at, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "'pending', 0, ?, ?)",
                    (
                        project_key,
                        event.event_id,
                        event.status,
                        event.issue_id,
                        event.candidate_sha,
                        event.base_sha,
                        manifest.manifest.branch,
                        event.manifest_path,
                        event.manifest_digest,
                        identity.device,
                        identity.inode,
                        identity.size,
                        identity.mtime_ns,
                        identity.mode,
                        stamp,
                        stamp,
                    ),
                )
            except sqlite3.IntegrityError:
                owner = connection.execute(
                    "SELECT event_id, state, thread_id, generation "
                    "FROM wake_deliveries "
                    "WHERE project_key = ? AND status = ? "
                    "AND candidate_sha = ?",
                    (project_key, event.status, event.candidate_sha),
                ).fetchone()
                if owner is None:
                    return outcome("conflict")
                state = str(owner["state"])
                if state == "delivered" and stale_delivery(owner):
                    connection.execute(
                        "UPDATE wake_deliveries SET state = 'pending', "
                        "claim_token = NULL, claim_expires_at = NULL, "
                        "updated_at = ? WHERE project_key = ? "
                        "AND event_id = ? AND state = 'delivered'",
                        (stamp, project_key, str(owner["event_id"])),
                    )
                    arm_heartbeat()
                    return outcome("pending_elsewhere")
                if state in ("pending", "claimed"):
                    return outcome("pending_elsewhere")
                if state == "deferred":
                    # The owning wake spent its event-bound reconciliation on
                    # a full window; adopt the fresh event onto the canonical
                    # candidate row so the new boundary reconciles anew.
                    connection.execute(
                        "UPDATE wake_deliveries SET event_id = ?, "
                        "issue_id = ?, base_sha = ?, branch = ?, "
                        "manifest_path = ?, manifest_digest = ?, "
                        "manifest_device = ?, manifest_inode = ?, "
                        "manifest_size = ?, manifest_mtime_ns = ?, "
                        "manifest_mode = ?, state = 'pending', "
                        "thread_id = NULL, generation = NULL, "
                        "claim_token = NULL, claim_expires_at = NULL, "
                        "updated_at = ? WHERE project_key = ? "
                        "AND event_id = ? AND state = 'deferred'",
                        (
                            event.event_id,
                            event.issue_id,
                            event.base_sha,
                            manifest.manifest.branch,
                            event.manifest_path,
                            event.manifest_digest,
                            identity.device,
                            identity.inode,
                            identity.size,
                            identity.mtime_ns,
                            identity.mode,
                            stamp,
                            project_key,
                            str(owner["event_id"]),
                        ),
                    )
                    arm_heartbeat()
                    return release_or_queue(event.event_id)
                return outcome(state)
            arm_heartbeat()
            return release_or_queue(event.event_id)

    def record_wake_delivery_success(
        self,
        project_key: str,
        *,
        thread_id: str,
        generation: int,
        event_id: str,
        claim_token: str,
        candidate_sha: str | None = None,
    ) -> bool:
        """Atomically record channel evidence and wake delivery together.

        The reviewer-channel compare-and-swap (exact thread and generation),
        the claimed-to-delivered transition bound to the exact claim token,
        and the heartbeat recomputation over all outstanding wakes happen in
        one transaction: a stale token cannot complete, a crash leaves the
        pre-armed fallback intact, and heartbeat is disabled only when no
        pending or claimed wake remains for the project.

        Sol correction 370eb20c: this transition also FAILS CLOSED for a
        LATE success arriving from a claim that has since lapsed or been
        invalidated. State and token alone were not enough — an expired
        claimant kept a usable token, so once another candidate had been
        released its late success could still mark a second row
        ``delivered``. The transition now additionally requires that the
        row's claim has not lapsed as of this instant, and that no OTHER
        candidate holds the reviewer. Both are read inside the same
        ``BEGIN IMMEDIATE`` transaction as the write, so a late success
        cannot slip between the read and the write of a concurrent
        release, and the single-delivered-candidate invariant holds.
        """

        self._project(project_key)
        stamp = self._now().isoformat()
        try:
            with self._database.transaction() as connection:
                if _reviewer_is_held(
                    connection,
                    project_key,
                    except_event_id=event_id,
                    stamp=stamp,
                ):
                    raise _WakeCasMiss
                channel = connection.execute(
                    "UPDATE reviewer_channels SET "
                    "last_delivered_event_id = ?, "
                    "last_delivered_candidate_sha = "
                    "coalesce(?, last_delivered_candidate_sha), "
                    "updated_at = ? "
                    "WHERE project_key = ? AND thread_id = ? "
                    "AND generation = ? AND state = 'ready'",
                    (
                        event_id,
                        candidate_sha,
                        stamp,
                        project_key,
                        thread_id,
                        generation,
                    ),
                )
                if channel.rowcount != 1:
                    raise _WakeCasMiss
                # A claim is usable only up to and including its deadline
                # instant; past it the lease has lapsed, and any release
                # that observed the lapse already voided this token via
                # _requeue_expired_claims, so the token test fails too.
                wake = connection.execute(
                    "UPDATE wake_deliveries SET state = 'delivered', "
                    "thread_id = ?, generation = ?, claim_token = NULL, "
                    "claim_expires_at = NULL, "
                    "attempts = attempts + 1, updated_at = ? "
                    "WHERE project_key = ? AND event_id = ? "
                    "AND state = 'claimed' AND claim_token = ? "
                    "AND claim_expires_at IS NOT NULL "
                    "AND claim_expires_at >= ?",
                    (
                        thread_id,
                        generation,
                        stamp,
                        project_key,
                        event_id,
                        claim_token,
                        stamp,
                    ),
                )
                if wake.rowcount != 1:
                    raise _WakeCasMiss
                self._recompute_heartbeat(connection, project_key, stamp)
        except _WakeCasMiss:
            return False
        return True

    def record_wake_attempt_failed(
        self, project_key: str, event_id: str, *, claim_token: str
    ) -> None:
        """Release the delivery claim after an explicit failed attempt."""

        self._project(project_key)
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE wake_deliveries SET state = 'pending', "
                "claim_token = NULL, claim_expires_at = NULL, "
                "attempts = attempts + 1, updated_at = ? "
                "WHERE project_key = ? AND event_id = ? "
                "AND state = 'claimed' AND claim_token = ?",
                (self._now().isoformat(), project_key, event_id, claim_token),
            )

    @staticmethod
    def _recompute_heartbeat(
        connection: sqlite3.Connection, project_key: str, stamp: str
    ) -> None:
        outstanding = connection.execute(
            "SELECT EXISTS(SELECT 1 FROM wake_deliveries "
            "WHERE project_key = ? AND state IN ('pending', 'claimed'))",
            (project_key,),
        ).fetchone()
        connection.execute(
            "UPDATE reviewer_channels SET heartbeat_enabled = ?, "
            "updated_at = ? WHERE project_key = ?",
            (1 if outstanding[0] else 0, stamp, project_key),
        )

    def admit_wake(
        self,
        project_key: str,
        event: WakeEvent,
        *,
        thread_id: str,
        generation: int,
        manifest: ManifestSnapshot,
    ) -> bool:
        """Admit one delivered wake exactly once, bound to the durable row.

        The compare-and-swap transitions only when every immutable identity
        field of the delivered record — status, candidate, base, branch,
        issue, path, digest, and the full manifest file identity — matches
        the validated snapshot, the stored delivery thread and generation
        equal the expected current channel, the reviewer channel still
        matches in the same transaction, and no other candidate is
        currently admitted for the project. No mismatch can transition.
        """

        self._project(project_key)
        identity = manifest.identity
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE wake_deliveries SET state = 'admitted', "
                "updated_at = ? "
                "WHERE project_key = ? AND event_id = ? "
                "AND state = 'delivered' "
                "AND status = ? AND candidate_sha = ? AND base_sha = ? "
                "AND branch = ? AND issue_id = ? AND manifest_path = ? "
                "AND manifest_digest = ? AND manifest_device = ? "
                "AND manifest_inode = ? AND manifest_size = ? "
                "AND manifest_mtime_ns = ? AND manifest_mode = ? "
                "AND thread_id = ? AND generation = ? "
                "AND EXISTS (SELECT 1 FROM reviewer_channels "
                "WHERE project_key = ? AND thread_id = ? "
                "AND generation = ? AND state = 'ready') "
                "AND NOT EXISTS (SELECT 1 FROM wake_deliveries "
                "WHERE project_key = ? AND state = 'admitted')",
                (
                    self._now().isoformat(),
                    project_key,
                    event.event_id,
                    event.status,
                    event.candidate_sha,
                    event.base_sha,
                    manifest.manifest.branch,
                    event.issue_id,
                    event.manifest_path,
                    event.manifest_digest,
                    identity.device,
                    identity.inode,
                    identity.size,
                    identity.mtime_ns,
                    identity.mode,
                    thread_id,
                    generation,
                    project_key,
                    thread_id,
                    generation,
                    project_key,
                ),
            )
        return cursor.rowcount == 1

    def release_delivered_wake(
        self, project_key: str, event_id: str, *, outcome: str
    ) -> bool:
        """Settle a delivered wake whose candidate was not admitted.

        ``deferred`` marks the wake as consumed by a full merge window: its
        event-bound reconciliation is spent, so the same candidate must be
        re-emitted as a new event, which :meth:`register_wake` adopts onto
        this row. ``rejected`` is terminal for this candidate identity — a
        corrected candidate arrives as a new SHA and a new event.
        """

        if outcome not in ("deferred", "rejected"):
            raise ValueError(f"unknown wake release outcome {outcome!r}")
        self._project(project_key)
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE wake_deliveries SET state = ?, claim_token = NULL, "
                "claim_expires_at = NULL, updated_at = ? "
                "WHERE project_key = ? AND event_id = ? "
                "AND state = 'delivered'",
                (outcome, stamp, project_key, event_id),
            )
        return cursor.rowcount == 1

    def complete_admitted_wake(self, project_key: str, event_id: str) -> bool:
        """Release the single admitted slot once its review turn finished."""

        self._project(project_key)
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE wake_deliveries SET state = 'completed', "
                "updated_at = ? WHERE project_key = ? AND event_id = ? "
                "AND state = 'admitted'",
                (self._now().isoformat(), project_key, event_id),
            )
        return cursor.rowcount == 1

    def next_releasable_candidate(self, project_key: str) -> WakeEvent | None:
        """The oldest queued candidate, only while the reviewer is free.

        INFRA-221: the read half of the one-candidate-at-a-time gate.
        ``None`` while any candidate still holds the primary Sol Merger
        (see :func:`_reviewer_is_held`) -- a candidate whose verdict is
        not durably settled keeps its slot, so an ambiguous or failed
        settlement releases nothing and the same candidate stays current.
        Otherwise the oldest durably ``pending`` wake for the project is
        returned so the caller can wake it through the ordinary delivery
        path; delivery's own claim compare-and-swap keeps that release
        exactly-once.
        """

        self._project(project_key)
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            # Sol correction 370eb20c: an abandoned claim is invalidated
            # and requeued here, in the same transaction that decides
            # whether the reviewer is free, so its token is already dead
            # before this release can hand the reviewer to anyone else.
            # The requeued candidate rejoins the queue in its original
            # order, so an expired claim still never wedges the queue.
            _requeue_expired_claims(connection, project_key, stamp=stamp)
            if _reviewer_is_held(
                connection, project_key, except_event_id="", stamp=stamp
            ):
                return None
            row = connection.execute(
                "SELECT status, issue_id, candidate_sha, base_sha, "
                "manifest_path, event_id, manifest_digest FROM wake_deliveries "
                "WHERE project_key = ? AND state = 'pending' "
                "ORDER BY created_at ASC, rowid ASC LIMIT 1",
                (project_key,),
            ).fetchone()
        if row is None:
            return None
        return WakeEvent(
            status=str(row["status"]),
            issue_id=str(row["issue_id"]),
            candidate_sha=str(row["candidate_sha"]),
            base_sha=str(row["base_sha"]),
            manifest_path=str(row["manifest_path"]),
            event_id=str(row["event_id"]),
            manifest_digest=str(row["manifest_digest"]),
        )

    def pending_heartbeat_wakes(
        self, project_key: str, *, manifest_root: Path
    ) -> list[WakeEvent]:
        """List undelivered wakes whose immutable manifests still validate.

        The heartbeat fallback boundary: it reads only the durable reviewer
        channel and wake records, returns nothing while direct delivery is
        healthy or the channel is unusable, and re-reads every referenced
        manifest inside the configured state root, dropping any wake whose
        manifest is missing, mutable, out of root, or no longer matches the
        stored immutable payload. It can therefore never wake the Merger on
        an empty, advisory, or replaced candidate.
        """

        channel = self.read_channel(project_key)
        if (
            channel is None
            or channel.state != "ready"
            or not channel.heartbeat_enabled
        ):
            return []
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            # INFRA-221: the fallback is a delivery path too, so it obeys
            # the same one-candidate-at-a-time gate -- while a candidate
            # holds the reviewer with an unsettled verdict, the queued
            # wakes behind it are not eligible to be woken. Sol correction
            # 370eb20c: it is a release path too, so it invalidates lapsed
            # claims first -- which is also how an abandoned delivery
            # becomes eligible for the fallback at all.
            _requeue_expired_claims(connection, project_key, stamp=stamp)
            if _reviewer_is_held(
                connection, project_key, except_event_id="", stamp=stamp
            ):
                return []
            rows = connection.execute(
                "SELECT status, issue_id, candidate_sha, base_sha, "
                "manifest_path, manifest_digest, manifest_device, "
                "manifest_inode, manifest_size, manifest_mtime_ns, "
                "manifest_mode, event_id FROM wake_deliveries "
                "WHERE project_key = ? AND state = 'pending' "
                "ORDER BY created_at",
                (project_key,),
            ).fetchall()
        wakes: list[WakeEvent] = []
        for row in rows:
            try:
                snapshot = read_manifest_snapshot(
                    Path(str(row["manifest_path"])),
                    root=manifest_root,
                    expected_digest=str(row["manifest_digest"]),
                    expected_identity=ManifestIdentity(
                        device=int(row["manifest_device"]),
                        inode=int(row["manifest_inode"]),
                        size=int(row["manifest_size"]),
                        mtime_ns=int(row["manifest_mtime_ns"]),
                        mode=int(row["manifest_mode"]),
                    ),
                )
            except ManifestError:
                continue
            manifest = snapshot.manifest
            if (
                manifest.event_id != str(row["event_id"])
                or manifest.status != str(row["status"])
                or manifest.candidate_sha != str(row["candidate_sha"])
                or manifest.base_sha != str(row["base_sha"])
                or str(row["issue_id"]) not in manifest.linear_issues
            ):
                continue
            wakes.append(
                WakeEvent(
                    status=str(row["status"]),
                    issue_id=str(row["issue_id"]),
                    candidate_sha=str(row["candidate_sha"]),
                    base_sha=str(row["base_sha"]),
                    manifest_path=str(row["manifest_path"]),
                    event_id=str(row["event_id"]),
                    manifest_digest=str(row["manifest_digest"]),
                )
            )
        return wakes

    async def verify_chatgpt_auth(self) -> CodexAuthHealth:
        """Check merge-gate auth without persisting account identity."""

        result = await self._rpc.request(
            "account/read", {"refreshToken": False}, self._timeout
        )
        account = result.get("account")
        if not isinstance(account, dict):
            account = {}
        if account.get("type") == "chatgpt":
            return CodexAuthHealth(eligible=True, reason="eligible")
        return CodexAuthHealth(eligible=False, reason="chatgpt_auth_required")

    async def read_rate_limits(self) -> CodexRateLimits:
        """Read the ChatGPT rate-limit snapshot for the Merger account."""

        result = await self._rpc.request(
            "account/rateLimits/read", None, self._timeout
        )
        return parse_rate_limits(result)

    async def interrupt(self, project_key: str, turn_id: str) -> None:
        """Interrupt one exact turn on the project's persisted thread."""

        channel = self.read_channel(project_key)
        if channel is None or channel.state != "ready":
            raise ValueError(
                f"no usable merger thread for project {project_key}"
            )
        await self._rpc.request(
            "turn/interrupt",
            {"threadId": channel.thread_id, "turnId": turn_id},
            self._timeout,
        )

    async def _resume_current(self, project_key: str) -> MergerThread:
        for _ in range(2):
            channel = self.read_channel(project_key)
            if channel is None or channel.state != "ready":
                raise MergerThreadUncertain(
                    f"reviewer channel for {project_key} is not ready and "
                    "requires operator reconciliation"
                )
            thread_id = channel.thread_id
            try:
                await self._load_persisted_thread(thread_id)
            except CodexRequestFailed as error:
                self._mark_uncertain(
                    project_key,
                    thread_id=thread_id,
                    generation=channel.generation,
                )
                raise MergerThreadUncertain(
                    f"merger thread for {project_key} could not be read "
                    "and requires operator reconciliation"
                ) from error
            if not self._channel_is_current(project_key, channel):
                continue
            await self._set_goal(project_key, thread_id)
            if self._channel_is_current(project_key, channel):
                return MergerThread(
                    project_key=project_key, thread_id=thread_id
                )
        raise StaleChannelError(
            f"reviewer channel for {project_key} was replaced while resuming"
        )

    async def _load_persisted_thread(self, thread_id: str) -> str | None:
        """Verify the persisted thread by reading it; load only if needed.

        A readable persisted thread is authoritative: ``thread/read`` is the
        only required call and its status is returned. Only a ``notLoaded``
        thread is asked to load via ``thread/resume``; the resume always
        re-applies the bounded writable workspace mode and the
        session-scoped Ponytail guard binding, which is the recoverable
        configuration path for a task started under a stale
        mode — no wake, no duplicate intake. If the App Server rejects
        the resume (observed live as ``-32600`` for an already-persisted
        readable task) the thread is re-read and stays usable when still
        readable. Only an unreadable thread propagates
        :class:`CodexRequestFailed`, which marks the channel uncertain.
        """

        status = await self.thread_status(thread_id)
        if status != "notLoaded":
            return status
        try:
            await self._rpc.request(
                "thread/resume",
                {
                    "threadId": thread_id,
                    "sandbox": SANDBOX_MODE,
                    "config": session_guard_config(),
                },
                self._timeout,
            )
        except CodexRequestFailed:
            return await self.thread_status(thread_id)
        return await self.thread_status(thread_id)

    async def thread_status(self, thread_id: str) -> str | None:
        """Read one thread and return its runtime status type, if reported.

        Raises :class:`CodexRequestFailed` when the thread is unreadable.
        A newly created task reports ``notLoaded`` in a fresh App Server
        process; a ``codex queue`` wake delivered to such a task is held by
        Codex and consumed once the task is opened, so no re-delivery,
        polling, or advisory wake is ever issued for it.
        """

        result = await self._rpc.request(
            "thread/read", {"threadId": thread_id}, self._timeout
        )
        thread = result.get("thread")
        if not isinstance(thread, dict):
            return None
        status = thread.get("status")
        if isinstance(status, dict) and isinstance(status.get("type"), str):
            return str(status["type"])
        return None

    def _channel_is_current(
        self, project_key: str, channel: ReviewerChannel
    ) -> bool:
        current = self.read_channel(project_key)
        return (
            current is not None
            and current.state == "ready"
            and current.thread_id == channel.thread_id
            and current.generation == channel.generation
        )

    async def _create(
        self, project_key: str, project: ProjectConfig
    ) -> MergerThread:
        stamp = self._now().isoformat()
        try:
            with self._database.transaction() as connection:
                connection.execute(
                    "INSERT INTO reviewer_channels("
                    "project_key, thread_id, generation, state, "
                    "integration_branch, created_at, updated_at"
                    ") VALUES (?, '', 0, 'creating', ?, ?, ?)",
                    (project_key, project.integration_branch, stamp, stamp),
                )
        except sqlite3.IntegrityError:
            current = self.read_channel(project_key)
            if current is not None and current.state == "ready":
                return await self._resume_current(project_key)
            if current is not None and current.state == "configuring":
                return await self._finish_configuration(
                    project_key, current.thread_id
                )
            raise MergerThreadUncertain(
                f"reviewer channel for {project_key} is being created by "
                "another caller and requires operator reconciliation if it "
                "stays reserved"
            ) from None
        try:
            started = await self._rpc.request(
                "thread/start",
                {
                    "model": self._model,
                    "cwd": str(project.repo_path),
                    "approvalPolicy": "never",
                    "sandbox": SANDBOX_MODE,
                    "serviceName": _SERVICE_NAME,
                    "config": session_guard_config(),
                },
                self._timeout,
            )
        except CodexRequestFailed:
            # The server definitively rejected the request, so no remote
            # thread exists and the empty reservation is safe to release.
            with self._database.transaction() as connection:
                connection.execute(
                    "DELETE FROM reviewer_channels "
                    "WHERE project_key = ? AND state = 'creating' "
                    "AND thread_id = ''",
                    (project_key,),
                )
            raise
        except BaseException:
            # Timeouts, disconnects, cancellation, and other transport or
            # process outcomes are ambiguous: the server may have created a
            # thread we never heard about. Keep the empty reservation as a
            # durable uncertain record so a retry cannot start a duplicate.
            with self._database.transaction() as connection:
                connection.execute(
                    "UPDATE reviewer_channels SET state = 'uncertain', "
                    "updated_at = ? "
                    "WHERE project_key = ? AND state = 'creating'",
                    (self._now().isoformat(), project_key),
                )
            raise
        thread = started.get("thread")
        thread_id = thread.get("id") if isinstance(thread, dict) else None
        if not isinstance(thread_id, str) or not thread_id:
            with self._database.transaction() as connection:
                connection.execute(
                    "UPDATE reviewer_channels SET state = 'uncertain', "
                    "updated_at = ? "
                    "WHERE project_key = ? AND state = 'creating'",
                    (self._now().isoformat(), project_key),
                )
            raise ValueError("codex did not return a thread id")
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET thread_id = ?, "
                "state = 'configuring', updated_at = ? "
                "WHERE project_key = ? AND state = 'creating'",
                (thread_id, self._now().isoformat(), project_key),
            )
            if cursor.rowcount != 1:
                raise StaleChannelError(
                    f"reviewer channel reservation for {project_key} "
                    "disappeared during creation"
                )
        return await self._finish_configuration(project_key, thread_id)

    async def _finish_configuration(
        self, project_key: str, thread_id: str
    ) -> MergerThread:
        """Complete or recover thread setup; the channel stays configuring
        (recoverable, never a duplicate) until every call succeeds."""

        await self._rpc.request(
            "thread/name/set",
            {"threadId": thread_id, "name": f"Merger: {project_key}"},
            self._timeout,
        )
        pinned_section_id = await self._resolve_pinned_section()
        await self._rpc.request(
            "thread/section/move",
            {"sectionId": pinned_section_id, "threadId": thread_id},
            self._timeout,
        )
        await self._set_goal(project_key, thread_id)
        stamp = self._now().isoformat()
        project = self._project(project_key)
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET generation = 1, "
                "state = 'ready', integration_branch = ?, updated_at = ? "
                "WHERE project_key = ? AND state = 'configuring' "
                "AND thread_id = ?",
                (project.integration_branch, stamp, project_key, thread_id),
            )
            if cursor.rowcount != 1:
                raise StaleChannelError(
                    f"reviewer channel for {project_key} changed while "
                    "completing configuration"
                )
            # A wake registered before the channel existed could not arm the
            # fallback flag, so every transition to ready derives it from
            # the outstanding recoverable wakes in the same transaction.
            self._recompute_heartbeat(connection, project_key, stamp)
        # INFRA-200: the thread just became ready for the first time, so
        # this is exactly the "new thread receives the versioned contract
        # at launch" boundary -- generation is always 1 here, since this
        # transaction only ever sets it to 1. Never called from
        # _resume_current or any heartbeat/idle path: an existing thread
        # that already went ready under an older deploy adopts the
        # contract later, at its next real candidate intake (see
        # ContractAwareDelivery), not by being woken here.
        await self.deliver_contract(
            project_key, thread_id=thread_id, generation=1
        )
        return MergerThread(project_key=project_key, thread_id=thread_id)

    async def _resolve_pinned_section(self) -> str:
        """Resolve exactly one built-in Pinned section id, fail closed."""

        matches: list[str] = []
        cursor: str | None = None
        for _ in range(_SECTION_PAGE_LIMIT):
            params: dict[str, Any] = {}
            if cursor is not None:
                params["cursor"] = cursor
            result = await self._rpc.request(
                "threadSection/list", params, self._timeout
            )
            data = result.get("data")
            if not isinstance(data, list):
                raise ValueError("threadSection/list returned no data")
            for section in data:
                if (
                    isinstance(section, dict)
                    and section.get("name") == PINNED_SECTION_NAME
                ):
                    section_id = section.get("id")
                    if not isinstance(section_id, str) or not section_id:
                        raise ValueError(
                            "Pinned thread section has no usable id"
                        )
                    matches.append(section_id)
            next_cursor = result.get("nextCursor")
            if next_cursor is None:
                break
            cursor = str(next_cursor)
        else:
            raise ValueError(
                "threadSection/list pagination did not terminate"
            )
        if len(matches) != 1:
            raise ValueError(
                "expected exactly one built-in Pinned thread section, "
                f"found {len(matches)}"
            )
        return matches[0]

    def _contract_message(self) -> str:
        """The versioned contract text, headed for delivery into a thread.

        The header names the exact version so a durable transcript (or an
        operator reading the thread) can see which contract text an old
        turn actually carried, even after :data:`MERGER_CONTRACT_VERSION`
        moves on.
        """

        header = f"{_CONTRACT_HEADER_PREFIX} {MERGER_CONTRACT_VERSION}"
        return f"{header}\n\n{self._contract}"

    async def deliver_contract(
        self, project_key: str, *, thread_id: str, generation: int
    ) -> None:
        """Send the full versioned contract into the thread, once.

        INFRA-200: called only at the exact moment a NEW thread first
        becomes ready (from :meth:`_finish_configuration`) -- never from
        :meth:`_resume_current` and never from any heartbeat or idle path,
        so an already-ready thread is never woken solely to restate the
        rule. An existing thread that predates this feature (or whose
        launch-time delivery below failed) instead adopts the contract at
        its next real candidate intake, prepended to that intake message
        by :class:`ContractAwareDelivery` -- not by a second call to this
        method. A transport failure here propagates like every other
        ``_finish_configuration`` step; the channel is already durably
        ``ready`` by the time this runs, so a failure here is recovered
        by that same next-real-intake adoption rather than by retrying
        thread setup.
        """

        await self._rpc.request(
            "turn/start",
            {"threadId": thread_id, "message": self._contract_message()},
            self._timeout,
        )
        self.record_contract_delivered(
            project_key, thread_id=thread_id, generation=generation
        )

    async def _set_goal(self, project_key: str, thread_id: str) -> None:
        project = self._project(project_key)
        objective = (
            f"Project: {project_key}. "
            f"Integration branch: {project.integration_branch}. "
            f"{MERGER_GOAL}"
        )
        await self._rpc.request(
            "thread/goal/set",
            {
                "threadId": thread_id,
                "objective": objective,
                "status": IDLE_GOAL_STATUS,
            },
            self._timeout,
        )

    def _mark_uncertain(
        self, project_key: str, *, thread_id: str, generation: int
    ) -> bool:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET state = 'uncertain', "
                "updated_at = ? WHERE project_key = ? AND thread_id = ? "
                "AND generation = ? AND state = 'ready'",
                (stamp, project_key, thread_id, generation),
            )
        return cursor.rowcount == 1

    def _project(self, project_key: str) -> ProjectConfig:
        project = self._projects.get(project_key)
        if project is None:
            raise ValueError(f"unknown merger project {project_key}")
        return project


class _PrefixedWakeEvent:
    """A ``WakeEvent`` view whose rendered text carries the contract too.

    INFRA-200: every field the delivery path reads (status, issue_id,
    candidate_sha, base_sha, manifest_path, event_id, manifest_digest) is
    forwarded unchanged to the wrapped event, so this is transparent to
    :func:`register_wake`, manifest verification, and every other
    consumer -- the immutable :class:`~hermes_orchestrator.manifests.
    WakeEvent` shape itself is never touched. Only :meth:`render`
    differs, so the ONE message that ever reaches the thread for this
    delivery already carries the contract; nothing sends a second turn
    for it.
    """

    __slots__ = ("_inner", "_prefix")

    def __init__(self, inner: WakeEvent, prefix: str) -> None:
        self._inner = inner
        self._prefix = prefix

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def render(self, generation: int) -> str:
        return f"{self._prefix}\n\n{self._inner.render(generation)}"


class _WakeDeliveryPort(Protocol):
    """The existing wake delivery boundary this wraps, unchanged.

    Structurally identical to ``merger_turns.WakeDeliverer`` and
    ``emission.WakeDeliverer`` -- both are satisfied by an instance of
    this class without importing either module, avoiding a cycle.
    """

    async def deliver(self, project_key: str, event: WakeEvent) -> object: ...


class ContractAwareDelivery:
    """Wrap the ordinary wake delivery adapter with contract adoption.

    INFRA-200: existing threads adopt the versioned contract at their
    next real candidate intake, never from a standalone wake. This is
    the one seam every real candidate delivery already passes through --
    a brand-new admission from :class:`~hermes_orchestrator.emission.
    CandidateEmitter` and a queued release from
    :meth:`MergerTurnService.release_next_candidate` alike -- so wrapping
    it here, once, in ``build_merge_flow``, covers both without adding a
    second delivery path. When the live channel's ``contract_version``
    already matches, this is a transparent pass-through: exactly the
    caller's own event, exactly one call to the wrapped adapter, nothing
    extra recorded.
    """

    def __init__(self, *, merger: CodexMerger, inner: _WakeDeliveryPort) -> None:
        self._merger = merger
        self._inner = inner

    async def deliver(self, project_key: str, event: WakeEvent) -> object:
        channel = self._merger.read_channel(project_key)
        if (
            channel is None
            or channel.state != "ready"
            or channel.contract_version == MERGER_CONTRACT_VERSION
        ):
            return await self._inner.deliver(project_key, event)
        prefixed = _PrefixedWakeEvent(event, self._merger._contract_message())
        result = await self._inner.deliver(project_key, prefixed)
        if getattr(result, "delivered", False):
            thread_id = getattr(result, "thread_id", None) or channel.thread_id
            generation = getattr(result, "generation", None)
            if generation is None:
                generation = channel.generation
            self._merger.record_contract_delivered(
                project_key, thread_id=thread_id, generation=generation
            )
        return result
