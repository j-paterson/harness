"""Deterministic, idempotent post-merge activation and successor advance.

INFRA-198 P2: after INFRA-210 merged, the daemon completed the review
settlement and Linear projection but nothing else advanced — the runtime
stayed on the prior generation, the paused acceptance issue never
resumed, and the queue stayed empty until an operator-side session ran a
manual ``runtime-activate --apply`` and requeued the work. Hermes, not an
operator-side session, must own this deterministically:

* A terminal merge of a *self-host* project (whose merged integration
  branch IS this orchestrator's own runtime) durably intends an
  activation: an existing ``activation_applies`` row keyed
  ``activate:<merge_sha>``, inserted idempotently.
* Every project's blocked, not-yet-ready dependents flip
  ``dependency_ready`` so the existing ranked dispatch admits the next
  explicitly admitted successor. ``paused`` issues are an operator hold
  and are never touched. A non-self-host merge releases immediately; a
  self-host merge records a durable ``merge.successor_pending`` marker
  instead and stays blocked until its own ``activate:<merge_sha>``
  intent is durably ``verified`` — refused, rolled back, or ambiguous
  never releases it (Sol correction e716a420 / INFRA-198 P3).
* The daemon's 30s :meth:`PostMergeAdvance.tick` discovers any merge the
  fast :meth:`PostMergeAdvance.on_merged` path missed (a crash, or the
  ``ReviewService`` construction site being out of reach for direct
  wiring), materializes a clean detached checkout at the merge SHA, and
  spawns the existing ``runtime-activate --apply`` CLI as a registered,
  session-detached process that survives this daemon's own
  kickstart-triggered death. The applier's own journaled apply record
  (``activated`` -> ``restarted`` -> ``verified``/``rolled_back``/
  ``ambiguous``/``refused``) is authoritative; this module only starts
  it once and propagates its terminal outcome onto the durable intent.

Nothing here selects, starts, or restarts application work itself — the
existing scheduler/dispatch tick already admits the highest-ranked
queued/blocked dependency-ready issue. Every decision is derived from
durable SQLite rows only, so a restart at any point resumes exactly the
remaining step, and nothing here is ever done twice.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import sqlite3
import sys
import uuid
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.git import GitError, GitVerifier, WorktreeGit
from hermes_orchestrator.processes import ProcessLeaseInput, ProcessRegistry
from hermes_orchestrator.queue import QueueService

_TERMINAL_APPLIER_STATES = frozenset(
    {"verified", "rolled_back", "ambiguous", "refused"}
)


class WorktreeGitPort(Protocol):
    """The narrow local-git boundary this module needs."""

    def fetch(self, path: Path, remote: str, branch: str) -> None: ...

    def worktree_add_detached(self, repo_path: Path, path: Path, sha: str) -> None: ...


class AncestryPort(Protocol):
    """The exact commit-reachability proof this module needs.

    ``is_ancestor`` returns a decision only for git's documented exit
    codes 0 and 1; anything else — an unknown commit, a corrupt
    repository — raises :class:`GitError` and the caller fails closed.
    """

    def is_ancestor(self, repo_path: Path, commit: str, ref: str) -> bool: ...


Spawn = Callable[..., Awaitable[Any]]


class PostMergeAdvance:
    """Own the durable post-merge activation intent and successor advance."""

    def __init__(
        self,
        *,
        database: Database,
        events: EventStore,
        projects: Mapping[str, ProjectConfig],
        queue: QueueService,
        repo_root: Path,
        state_dir: Path,
        registry: ProcessRegistry | None = None,
        git: WorktreeGitPort | None = None,
        ancestry: AncestryPort | None = None,
        uv_binary: str | None = None,
        spawn: Spawn | None = None,
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
        replenish: Callable[[str], None] | None = None,
    ) -> None:
        self.database = database
        self.events = events
        self.projects = dict(projects)
        self.queue = queue
        self.repo_root = repo_root
        self.state_dir = state_dir
        self.registry = registry
        self.git: WorktreeGitPort = git if git is not None else WorktreeGit()
        self.ancestry: AncestryPort = (
            ancestry if ancestry is not None else GitVerifier()
        )
        self.uv_binary = uv_binary or (shutil.which("uv") or "/opt/homebrew/bin/uv")
        self.spawn: Spawn = spawn or asyncio.create_subprocess_exec
        self._now = now or (lambda: datetime.now(UTC))
        self._ids = ids or (lambda: uuid.uuid4().hex)
        # INFRA-215: an injectable immediate-replenishment hook, called
        # right after a successor's dependency flips ready. ``None`` is
        # the ordinary non-live/observe shape -- the daemon's per-tick
        # ``commit_work_ready`` sweep still discovers the same runnable
        # work, just not immediately.
        self.replenish = replenish

    # -- fast path (optional) ---------------------------------------------

    def on_merged(
        self,
        *,
        project_key: str,
        issue_id: str,
        review_id: str,
        merge_sha: str,
    ) -> None:
        """Record the durable post-merge intents for one terminal merge.

        Called synchronously as an optional accelerator right after a
        review's ``merged`` transition commits, and again, idempotently,
        whenever :meth:`tick` discovers this exact merge from durable
        rows after a crash or restart. Never spawns or restarts
        anything itself, and never raises into its caller; a failure
        here is repaired by the next tick.

        A non-self-host merge releases its successor immediately. A
        self-host merge never does (Sol correction e716a420 / INFRA-198
        P3): it records the durable ``merge.successor_pending`` marker
        instead, and its successor stays blocked until
        ``activate:<merge_sha>`` is durably verified — see
        :meth:`_advance_verified_successor`.
        """

        del issue_id  # not needed here; kept for the documented API shape
        project = self.projects.get(project_key)
        if project is not None and project.self_host:
            try:
                self._record_self_host_intent(
                    project_key, review_id=review_id, merge_sha=merge_sha
                )
            except Exception as error:
                print(
                    "post-merge advance: intent recording failed for "
                    f"{merge_sha!r}: {type(error).__name__}: {error}",
                    file=sys.stderr,
                )
            try:
                self._record_successor_pending(
                    review_id, project_key=project_key, merge_sha=merge_sha
                )
            except Exception as error:
                print(
                    "post-merge advance: successor-pending marker failed "
                    f"for {review_id!r}: {type(error).__name__}: {error}",
                    file=sys.stderr,
                )
            return
        try:
            self._advance_successor(
                project_key, review_id=review_id, reason=f"merge:{review_id}"
            )
        except Exception as error:
            print(
                "post-merge advance: successor advance failed for "
                f"{project_key!r}: {type(error).__name__}: {error}",
                file=sys.stderr,
            )

    def _record_self_host_intent(
        self, project_key: str, *, review_id: str, merge_sha: str
    ) -> None:
        project = self.projects.get(project_key)
        if project is None or not project.self_host:
            return
        apply_id = f"activate:{merge_sha}"
        target_checkout = str(self.state_dir / "checkouts" / merge_sha)
        current = self._current_activation()
        prior_generation = None if current is None else int(current["generation"])
        stamp = self._now().isoformat()
        with self.database.transaction() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO activation_applies("
                "apply_id, target_checkout, prior_generation, "
                "target_generation, state, reason, created_at, updated_at"
                ") VALUES (?, ?, ?, NULL, 'intended', ?, ?, ?)",
                (
                    apply_id,
                    target_checkout,
                    prior_generation,
                    f"merge:{review_id}",
                    stamp,
                    stamp,
                ),
            )
            if cursor.rowcount == 0:
                # A row for this apply_id already exists in some state
                # (any prior intended/activated/.../terminal attempt):
                # never re-record.
                return
            self.events.append(
                connection,
                EventInput(
                    event_type="activation.intended",
                    aggregate_type="activation_apply",
                    aggregate_id=apply_id,
                    correlation_id=review_id,
                    payload={"merge_sha": merge_sha, "review_id": review_id},
                ),
            )

    def _record_successor_pending(
        self, review_id: str, *, project_key: str, merge_sha: str
    ) -> None:
        """Journal that a self-host merge's successor advance is pending.

        Idempotent: a ``merge.successor_pending`` event already recorded
        for this review means a prior call — this run or a previous
        life — already marked it, so nothing is appended twice. The
        successor stays blocked until :meth:`_advance_verified_successor`
        releases it.
        """

        existing = self.database.execute(
            "SELECT 1 FROM events WHERE event_type = 'merge.successor_pending' "
            "AND aggregate_id = ? LIMIT 1",
            (review_id,),
        ).fetchone()
        if existing is not None:
            return
        with self.database.transaction() as connection:
            self.events.append(
                connection,
                EventInput(
                    event_type="merge.successor_pending",
                    aggregate_type="review",
                    aggregate_id=review_id,
                    payload={"project_key": project_key, "merge_sha": merge_sha},
                ),
            )

    def _advance_successor(
        self, project_key: str, *, review_id: str, reason: str
    ) -> None:
        """Release one project's blocked dependents and journal the advance.

        Idempotent across restarts: a ``merge.advanced`` event already
        recorded for this review means this exact merge already
        released its successors, so nothing is repeated.
        ``QueueService.mark_dependency_ready`` is independently
        idempotent, but the guard here also keeps the durable
        ``merge.advanced`` event from ever being journaled twice.

        INFRA-215: once readiness is durably marked, :attr:`replenish`
        (when wired) is called immediately so a project with runnable
        work and an idle seat does not wait for the next maintenance
        tick to be told. It never raises into this method -- a failure
        here is repaired by the daemon's own per-tick sweep.
        """

        existing = self.database.execute(
            "SELECT 1 FROM events WHERE event_type = 'merge.advanced' "
            "AND aggregate_id = ? LIMIT 1",
            (review_id,),
        ).fetchone()
        if existing is not None:
            return
        self.queue.mark_dependency_ready(
            project_key, actor="post_merge_advance", reason=reason
        )
        with self.database.transaction() as connection:
            self.events.append(
                connection,
                EventInput(
                    event_type="merge.advanced",
                    aggregate_type="review",
                    aggregate_id=review_id,
                    payload={"project_key": project_key, "reason": reason},
                ),
            )
        if self.replenish is not None:
            try:
                self.replenish(project_key)
            except Exception as error:
                print(
                    "post-merge advance: immediate replenish failed for "
                    f"{project_key!r}: {type(error).__name__}: {error}",
                    file=sys.stderr,
                )

    def _advance_verified_successor(self, apply_id: str) -> None:
        """Release a self-host merge's successor once its activation verifies.

        Refused, rolled back, or ambiguous never call this — only a
        verified ``activate:<sha>`` ever releases a self-host merge's
        successor (INFRA-198 P3), and it stays released forever once
        the idempotent ``merge.advanced`` guard in
        :meth:`_advance_successor` has fired.
        """

        merge_sha = apply_id.removeprefix("activate:")
        row = self.database.execute(
            "SELECT review_id, project_key FROM reviews WHERE merge_sha = ? "
            "ORDER BY updated_at DESC LIMIT 1",
            (merge_sha,),
        ).fetchone()
        if row is None:
            return
        self._advance_successor(
            str(row["project_key"]),
            review_id=str(row["review_id"]),
            reason=f"activation:{apply_id}",
        )

    # -- tick ---------------------------------------------------------------

    async def tick(self) -> None:
        """Run one bounded, SQLite-only maintenance pass; never raises."""

        try:
            await self._tick()
        except Exception as error:
            print(
                f"post-merge advance: tick failed: {type(error).__name__}: "
                f"{error}",
                file=sys.stderr,
            )

    async def _tick(self) -> None:
        self._discover_unadvanced_merges()
        self._recover_verified_successor_releases()
        current = self._current_activation()
        current_sha = None if current is None else str(current["git_sha"])
        rows = self.database.execute(
            "SELECT * FROM activation_applies WHERE state = 'intended' "
            "ORDER BY created_at ASC"
        ).fetchall()
        for row in rows:
            apply_id = str(row["apply_id"])
            if not apply_id.startswith("activate:"):
                continue
            try:
                await self._advance_intent(row, current_sha=current_sha)
            except Exception as error:
                print(
                    f"post-merge advance: could not advance {apply_id!r}: "
                    f"{type(error).__name__}: {error}",
                    file=sys.stderr,
                )

    def _discover_unadvanced_merges(self) -> None:
        """Discover every merged review whose successor advance never landed.

        Every configured project is eligible, not only self-host ones
        (Sol correction e716a420 / INFRA-198 P3): the durable
        ``merge.advanced`` event on the review is the sole discovery
        gate, so this stays correct forever, across every restart. A
        self-host merge the active generation does not provably contain
        gates through the ordinary intent path (:meth:`on_merged`); a
        self-host merge the active generation provably already contains
        — the exact active ``git_sha``, or a verified ancestor of it —
        is advanced directly, with reason ``predates_active_generation``,
        and never creates — or re-creates — an activation intent:
        reactivating an already-superseded checkout would be wrong.

        Sol correction c5600e31: containment is never inferred from
        timestamp ordering — a later unrelated activation, or a
        rollback, orders after the merge without containing it. Only
        :meth:`_proven_in_active_generation`'s exact-SHA or verified-
        ancestry proof releases here; absent proof, the merge falls
        through to :meth:`on_merged` and its successor stays blocked
        until its own ``activate:<sha>`` intent durably verifies.
        """

        if not self.projects:
            return
        current = self._current_activation()
        active_sha = None if current is None else str(current["git_sha"])
        placeholders = ",".join("?" for _ in self.projects)
        rows = self.database.execute(
            "SELECT review_id, project_key, issue_id, merge_sha, updated_at "
            f"FROM reviews WHERE state = 'merged' AND merge_sha IS NOT NULL "
            f"AND project_key IN ({placeholders}) AND review_id NOT IN ("
            "SELECT aggregate_id FROM events "
            "WHERE event_type = 'merge.advanced') ORDER BY updated_at ASC",
            tuple(self.projects),
        ).fetchall()
        for row in rows:
            project_key = str(row["project_key"])
            project = self.projects.get(project_key)
            review_id = str(row["review_id"])
            merge_sha = str(row["merge_sha"])
            if (
                project is not None
                and project.self_host
                and active_sha is not None
            ):
                # Only a merge with no existing intent at all predates
                # tracking entirely: the merge that produced the very
                # activation now active always has an 'activate:<sha>'
                # row already (created by the original on_merged call),
                # and that row's own outcome — verified or otherwise —
                # must keep driving the ordinary path below, never this
                # one-shot direct advance.
                existing_intent = self.database.execute(
                    "SELECT 1 FROM activation_applies WHERE apply_id = ?",
                    (f"activate:{merge_sha}",),
                ).fetchone()
                if existing_intent is None and self._proven_in_active_generation(
                    merge_sha, active_sha=active_sha
                ):
                    self._advance_successor(
                        project_key,
                        review_id=review_id,
                        reason="predates_active_generation",
                    )
                    continue
            self.on_merged(
                project_key=project_key,
                issue_id=str(row["issue_id"]),
                review_id=review_id,
                merge_sha=merge_sha,
            )

    def _proven_in_active_generation(
        self, merge_sha: str, *, active_sha: str
    ) -> bool:
        """Whether the active generation provably contains this merge.

        Proof is exact: the merge SHA is the active generation's own
        ``git_sha``, or ``git merge-base --is-ancestor`` verifies it is
        reachable from that exact SHA in ``repo_root`` (never from a
        branch tip, which could have moved past the activation). A
        :class:`GitError` — an unknown commit, a corrupt repository, any
        undocumented exit — is not a decision and fails closed: no
        release, and the ordinary intent path keeps the successor
        blocked until the activation itself verifies (Sol correction
        c5600e31).
        """

        if merge_sha == active_sha:
            return True
        try:
            return self.ancestry.is_ancestor(
                self.repo_root, merge_sha, active_sha
            )
        except GitError:
            return False

    def _recover_verified_successor_releases(self) -> None:
        """Recover a verified self-host activation whose release never landed.

        A crash between journaling ``activate:<sha>`` verified and
        releasing its successor would otherwise strand the successor
        forever: the apply-row scan below only ever revisits
        ``intended`` rows. Replaying this over every already-verified
        row is always safe — :meth:`_advance_successor`'s own
        ``merge.advanced`` guard makes it a no-op once the release
        already landed.
        """

        rows = self.database.execute(
            "SELECT apply_id FROM activation_applies "
            "WHERE state = 'verified' AND apply_id LIKE 'activate:%'"
        ).fetchall()
        for row in rows:
            self._advance_verified_successor(str(row["apply_id"]))

    async def _advance_intent(
        self, row: sqlite3.Row, *, current_sha: str | None
    ) -> None:
        apply_id = str(row["apply_id"])
        merge_sha = apply_id.removeprefix("activate:")
        target_checkout = str(row["target_checkout"])
        if current_sha is not None and merge_sha == current_sha:
            self._mark_intent(
                apply_id, state="verified", reason="active generation matches"
            )
            return
        applier_row = self.database.execute(
            "SELECT apply_id, state FROM activation_applies "
            "WHERE target_checkout = ? AND apply_id != ? "
            "ORDER BY created_at DESC LIMIT 1",
            (target_checkout, apply_id),
        ).fetchone()
        lease = self._active_applier_lease(merge_sha, target_checkout)
        unbound_running = self._unbound_applier_running(merge_sha)
        if applier_row is not None:
            applier_state = str(applier_row["state"])
            if applier_state in _TERMINAL_APPLIER_STATES:
                self._mark_intent(
                    apply_id,
                    state=applier_state,
                    reason=f"applier:{applier_row['apply_id']}",
                )
                return
            if lease is None and not unbound_running:
                self._mark_intent(
                    apply_id,
                    state="ambiguous",
                    reason="applier exited without a terminal apply record",
                )
            # Otherwise an applier is mid-flight under a live lease —
            # the journal-bound one, or an unprovable survivor whose own
            # apply row may still turn terminal; nothing to do this tick.
            return
        if lease is not None or unbound_running:
            # Spawned and still running, but the applier has not written
            # its own row yet; wait for it. An unbound live
            # runtime_applier for this sha is never "the applier" (Sol
            # correction c5600e31) yet also forbids spawning beside it:
            # two concurrent applies must be impossible.
            return
        if self._has_spawn_event(apply_id):
            # We spawned an applier before, its lease is gone, and it
            # never wrote a terminal (or any) apply record: unprovable,
            # fails closed rather than risking a second concurrent apply.
            self._mark_intent(
                apply_id,
                state="ambiguous",
                reason="applier exited without a terminal apply record",
            )
            return
        if self._open_applier_claim(apply_id):
            # Sol correction 57c46faa: a durable write-ahead claim with
            # no completed binding means a prior life crashed somewhere
            # between claiming and the atomic lease+binding commit —
            # whether its child ever spawned, or still runs unregistered,
            # is unprovable from durable state. Never spawn beside it;
            # resolve to ambiguous exactly once. A live unregistered
            # survivor is the reconciler's unknown-process scan's to
            # report, and admission stays closed on it.
            self._mark_intent(
                apply_id,
                state="ambiguous",
                reason="applier claim never completed",
            )
            return
        project_key = self._project_for_merge(merge_sha)
        if project_key is None:
            return
        await self._spawn_applier(
            apply_id=apply_id,
            project_key=project_key,
            merge_sha=merge_sha,
            target_checkout=Path(target_checkout),
        )

    async def _spawn_applier(
        self,
        *,
        apply_id: str,
        project_key: str,
        merge_sha: str,
        target_checkout: Path,
    ) -> None:
        """Spawn the applier behind one crash-safe durable identity binding.

        Sol correction 57c46faa: the OS spawn itself can never be
        transactional, so durability brackets it on both sides. A
        write-ahead ``activation.applier_claimed`` event — carrying the
        exact identity the child will hold: apply_id, the owning
        project_key, worker_id (the merge sha), and the target checkout
        — commits BEFORE the spawn; the process-lease registration and
        the ``activation.applier_spawned`` binding then commit together
        in ONE transaction after it (:meth:`_bind_spawned_applier`), so
        the lease row and the journaled binding can never diverge. A
        crash anywhere between the claim and that atomic commit leaves a
        claim-without-completion the next tick resolves to ambiguous
        exactly once — never a second spawn. A spawn that provably never
        started (OSError) resolves its own claim with
        ``activation.applier_spawn_failed`` so a later tick may retry.
        """

        project = self.projects.get(project_key)
        if project is None:
            return
        if not target_checkout.exists():
            self.git.fetch(self.repo_root, "origin", project.integration_branch)
            self.git.worktree_add_detached(self.repo_root, target_checkout, merge_sha)
        log_dir = self.state_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"runtime-applier-{merge_sha}.log"
        with self.database.transaction() as connection:
            self.events.append(
                connection,
                EventInput(
                    event_type="activation.applier_claimed",
                    aggregate_type="activation_apply",
                    aggregate_id=apply_id,
                    payload={
                        "apply_id": apply_id,
                        "project_key": project_key,
                        "worker_id": merge_sha,
                        "target_checkout": str(target_checkout),
                    },
                ),
            )
        with open(log_path, "ab") as log_handle:
            try:
                process = await self.spawn(
                    self.uv_binary,
                    "run",
                    "--project",
                    str(target_checkout),
                    "hermes-orchestrator",
                    "--repo-root",
                    str(self.repo_root),
                    "--state-dir",
                    str(self.state_dir),
                    "runtime-activate",
                    "--apply",
                    "--json",
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=log_handle,
                    stderr=log_handle,
                    start_new_session=True,
                )
            except OSError as error:
                print(
                    f"post-merge advance: applier spawn failed for "
                    f"{merge_sha!r}: {error}",
                    file=sys.stderr,
                )
                # The OSError proves no child was created: the claim
                # resolves durably so the next tick may retry with a
                # fresh claim instead of failing closed to ambiguous.
                with self.database.transaction() as connection:
                    self.events.append(
                        connection,
                        EventInput(
                            event_type="activation.applier_spawn_failed",
                            aggregate_type="activation_apply",
                            aggregate_id=apply_id,
                            payload={
                                "apply_id": apply_id,
                                "error": type(error).__name__,
                            },
                        ),
                    )
                return
        await self._bind_spawned_applier(
            process,
            apply_id=apply_id,
            project_key=project_key,
            merge_sha=merge_sha,
            target_checkout=target_checkout,
        )

    async def _bind_spawned_applier(
        self,
        process: Any,
        *,
        apply_id: str,
        project_key: str,
        merge_sha: str,
        target_checkout: Path,
    ) -> None:
        """Commit the lease row and the durable binding in one transaction.

        Sol correction 57c46faa: ``ProcessRegistry.register_on`` runs the
        process-lease insert on this method's own transaction, so the
        lease and the ``activation.applier_spawned`` binding — which
        carries the exact lease_id AND the owning project_key — commit
        atomically: no crash point can leave a registered applier with
        no binding, or a binding naming no lease. On any failure the
        exact new group is reaped before the error propagates (mirroring
        ``register_spawned``'s fail-closed contract), the rollback
        leaves nothing durable, and the still-open claim resolves the
        intent to ambiguous on a later tick.
        """

        try:
            with self.database.transaction() as connection:
                lease_id: str | None = None
                if self.registry is not None:
                    lease = self.registry.register_on(
                        connection,
                        ProcessLeaseInput(
                            pid=int(process.pid),
                            pgid=int(process.pid),
                            project_key=project_key,
                            kind="runtime_applier",
                            worker_id=merge_sha,
                            executable=self.uv_binary,
                            cwd=str(target_checkout),
                        ),
                    )
                    lease_id = lease.lease_id
                self.events.append(
                    connection,
                    EventInput(
                        event_type="activation.applier_spawned",
                        aggregate_type="activation_apply",
                        aggregate_id=apply_id,
                        payload={
                            "apply_id": apply_id,
                            "pid": int(process.pid),
                            "lease_id": lease_id,
                            "project_key": project_key,
                            "worker_id": merge_sha,
                            "target_checkout": str(target_checkout),
                        },
                    ),
                )
        except BaseException as error:
            # The transaction already rolled back: stop and reap the
            # exact new group before propagating. Exception text is
            # redacted to its type.
            await self._terminate_group(process)
            if isinstance(
                error, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)
            ):
                raise
            raise RuntimeError(
                "managed runtime_applier process could not be bound: "
                f"{type(error).__name__}"
            ) from None

    async def _terminate_group(self, process: Any) -> None:
        """Stop the whole owned process group: TERM, bounded wait, KILL."""

        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        with suppress(TimeoutError, asyncio.TimeoutError):
            await asyncio.wait_for(process.wait(), timeout=5.0)
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        with suppress(Exception):
            await process.wait()

    # -- shared reads ---------------------------------------------------

    def _current_activation(self) -> sqlite3.Row | None:
        return self.database.execute(
            "SELECT generation, git_sha, activated_at FROM runtime_activations "
            "WHERE state = 'active'"
        ).fetchone()

    def _active_applier_lease(
        self, merge_sha: str, target_checkout: str
    ) -> Any | None:
        """Resolve exactly the journal-bound live applier lease, or None.

        Sol correction c5600e31: "the applier" is never resolved by a
        ``(kind, worker_id)`` scan — two live leases can share a
        worker_id, and only one of them is the process this module
        spawned. The lease counts if and only if its ``lease_id`` equals
        the non-null ``lease_id`` journaled by this module's own latest
        ``activation.applier_spawned`` event for ``activate:<sha>`` AND
        its kind, worker_id, cwd, and project_key match the applier's
        exact identity (``runtime_applier``, the merge sha, the intent's
        target checkout, the binding's own journaled project_key — Sol
        correction 57c46faa: project ownership is part of the exact
        binding). A null or absent journaled lease_id or project_key
        excuses nothing and resolves nothing — fail closed.

        A bound lease whose process is gone with nothing recording its
        exit is reaped here, by exact lease_id, so the ambiguous rule in
        :meth:`_advance_intent` fires deterministically instead of
        blocking admission on this lease forever (Sol correction
        e716a420).
        """

        if self.registry is None:
            return None
        binding = self._journaled_applier_binding(f"activate:{merge_sha}")
        if binding is None:
            return None
        bound_lease_id, bound_project = binding
        for lease in self.registry.active():
            if lease.lease_id != bound_lease_id:
                continue
            if (
                lease.kind != "runtime_applier"
                or lease.worker_id != merge_sha
                or lease.cwd != target_checkout
                or lease.project_key != bound_project
            ):
                # The journaled binding no longer describes this lease's
                # exact identity — project ownership included: unprovable,
                # never "the applier".
                return None
            if not self.registry.snapshot(lease.lease_id).alive:
                self.registry.mark_exited(lease.lease_id)
                return None
            return lease
        return None

    def _unbound_applier_running(self, merge_sha: str) -> bool:
        """Whether a live ``runtime_applier`` for this sha lacks the binding.

        Such a survivor is never "the applier" (Sol correction
        c5600e31) — reconciliation reports it as a blocking orphan — but
        the tick must neither spawn a second applier beside it nor
        declare the intent ambiguous while its own apply row may still
        turn terminal: it only ever waits.
        """

        if self.registry is None:
            return False
        binding = self._journaled_applier_binding(f"activate:{merge_sha}")
        bound_lease_id = None if binding is None else binding[0]
        for lease in self.registry.active():
            if lease.kind != "runtime_applier" or lease.worker_id != merge_sha:
                continue
            if lease.lease_id == bound_lease_id:
                continue
            if self.registry.snapshot(lease.lease_id).alive:
                return True
        return False

    def _journaled_applier_binding(
        self, apply_id: str
    ) -> tuple[str, str] | None:
        """The (lease_id, project_key) from the latest journaled binding.

        :meth:`_bind_spawned_applier` journals
        ``activation.applier_spawned`` atomically with the lease
        registration itself, carrying the exact lease_id and the owning
        project_key; that durable record is the only activation-to-lease
        binding, and project ownership is part of it (Sol correction
        57c46faa). A missing event, an unreadable payload, a null
        lease_id (a spawn with no registry), or a null project_key binds
        nothing — fail closed.
        """

        row = self.database.execute(
            "SELECT payload_json FROM events "
            "WHERE event_type = 'activation.applier_spawned' "
            "AND aggregate_id = ? ORDER BY sequence DESC LIMIT 1",
            (apply_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, ValueError):
            return None
        if not isinstance(payload, dict):
            return None
        lease_id = payload.get("lease_id")
        project_key = payload.get("project_key")
        if (
            isinstance(lease_id, str)
            and lease_id
            and isinstance(project_key, str)
            and project_key
        ):
            return lease_id, project_key
        return None

    def _open_applier_claim(self, apply_id: str) -> bool:
        """Whether the latest durable applier claim is still unresolved.

        :meth:`_spawn_applier` claims before it spawns and resolves the
        claim with either the atomic ``activation.applier_spawned``
        binding or an ``activation.applier_spawn_failed`` record. A
        claim that is the latest of the three marks a crash between the
        claim and its resolution — whether the child ever spawned can
        never be proven from durable state, so the claim's resolver
        (:meth:`_advance_intent`) fails closed to ambiguous, exactly
        once, and never spawns beside it (Sol correction 57c46faa).
        """

        row = self.database.execute(
            "SELECT event_type FROM events WHERE aggregate_id = ? "
            "AND event_type IN ('activation.applier_claimed', "
            "'activation.applier_spawned', 'activation.applier_spawn_failed') "
            "ORDER BY sequence DESC LIMIT 1",
            (apply_id,),
        ).fetchone()
        return row is not None and (
            str(row["event_type"]) == "activation.applier_claimed"
        )

    def _has_spawn_event(self, apply_id: str) -> bool:
        row = self.database.execute(
            "SELECT 1 FROM events WHERE event_type = 'activation.applier_spawned' "
            "AND aggregate_id = ? LIMIT 1",
            (apply_id,),
        ).fetchone()
        return row is not None

    def _project_for_merge(self, merge_sha: str) -> str | None:
        row = self.database.execute(
            "SELECT project_key FROM reviews WHERE merge_sha = ? "
            "ORDER BY updated_at DESC LIMIT 1",
            (merge_sha,),
        ).fetchone()
        return None if row is None else str(row["project_key"])

    def _mark_intent(self, apply_id: str, *, state: str, reason: str) -> None:
        stamp = self._now().isoformat()
        with self.database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE activation_applies SET state = ?, reason = ?, "
                "updated_at = ? WHERE apply_id = ? AND state = 'intended'",
                (state, reason, stamp, apply_id),
            )
            if cursor.rowcount == 0:
                return
            self.events.append(
                connection,
                EventInput(
                    event_type=f"activation.{state}",
                    aggregate_type="activation_apply",
                    aggregate_id=apply_id,
                    payload={"reason": reason},
                ),
            )
        if state == "verified":
            self._advance_verified_successor(apply_id)
