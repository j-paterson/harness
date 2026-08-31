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
  and are never touched.
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
from hermes_orchestrator.git import WorktreeGit
from hermes_orchestrator.processes import ProcessRegistry, register_spawned
from hermes_orchestrator.queue import QueueService

_TERMINAL_APPLIER_STATES = frozenset(
    {"verified", "rolled_back", "ambiguous", "refused"}
)


class WorktreeGitPort(Protocol):
    """The narrow local-git boundary this module needs."""

    def fetch(self, path: Path, remote: str, branch: str) -> None: ...

    def worktree_add_detached(self, repo_path: Path, path: Path, sha: str) -> None: ...


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
        uv_binary: str | None = None,
        spawn: Spawn | None = None,
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
    ) -> None:
        self.database = database
        self.events = events
        self.projects = dict(projects)
        self.queue = queue
        self.repo_root = repo_root
        self.state_dir = state_dir
        self.registry = registry
        self.git: WorktreeGitPort = git if git is not None else WorktreeGit()
        self.uv_binary = uv_binary or (shutil.which("uv") or "/opt/homebrew/bin/uv")
        self.spawn: Spawn = spawn or asyncio.create_subprocess_exec
        self._now = now or (lambda: datetime.now(UTC))
        self._ids = ids or (lambda: uuid.uuid4().hex)

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
        rows after a crash or restart. Records intents only — never
        spawns or restarts anything itself — and never raises into its
        caller; a failure here is repaired by the next tick.
        """

        del issue_id  # not needed here; kept for the documented API shape
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
            self.queue.mark_dependency_ready(
                project_key,
                actor="post_merge_advance",
                reason=f"merge:{review_id}",
            )
        except Exception as error:
            print(
                "post-merge advance: dependency-ready flip failed for "
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
        """Discover self-host merges whose activation intent never landed.

        Bounded to reviews updated after the current active activation
        (or every merged review lacking an intent when none is active
        yet), so the query stays cheap forever. Reusing :meth:`on_merged`
        keeps the dependency-ready flip and the intent insert identical
        to the fast path, and fully idempotent.
        """

        self_host_projects = tuple(
            key for key, project in self.projects.items() if project.self_host
        )
        if not self_host_projects:
            return
        current = self._current_activation()
        threshold = None if current is None else str(current["activated_at"])
        placeholders = ",".join("?" for _ in self_host_projects)
        if threshold is None:
            rows = self.database.execute(
                "SELECT review_id, project_key, issue_id, merge_sha FROM "
                f"reviews WHERE state = 'merged' AND merge_sha IS NOT NULL "
                f"AND project_key IN ({placeholders}) ORDER BY updated_at ASC",
                self_host_projects,
            ).fetchall()
        else:
            rows = self.database.execute(
                "SELECT review_id, project_key, issue_id, merge_sha FROM "
                f"reviews WHERE state = 'merged' AND merge_sha IS NOT NULL "
                f"AND project_key IN ({placeholders}) AND updated_at > ? "
                "ORDER BY updated_at ASC",
                (*self_host_projects, threshold),
            ).fetchall()
        for row in rows:
            merge_sha = str(row["merge_sha"])
            existing = self.database.execute(
                "SELECT 1 FROM activation_applies WHERE apply_id = ?",
                (f"activate:{merge_sha}",),
            ).fetchone()
            if existing is not None:
                continue
            self.on_merged(
                project_key=str(row["project_key"]),
                issue_id=str(row["issue_id"]),
                review_id=str(row["review_id"]),
                merge_sha=merge_sha,
            )

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
        lease = self._active_applier_lease(merge_sha)
        if applier_row is not None:
            applier_state = str(applier_row["state"])
            if applier_state in _TERMINAL_APPLIER_STATES:
                self._mark_intent(
                    apply_id,
                    state=applier_state,
                    reason=f"applier:{applier_row['apply_id']}",
                )
                return
            if lease is None:
                self._mark_intent(
                    apply_id,
                    state="ambiguous",
                    reason="applier exited without a terminal apply record",
                )
            # Otherwise the applier is mid-flight under its own live
            # lease; nothing to do this tick.
            return
        if lease is not None:
            # Spawned and still running, but the applier has not written
            # its own row yet; wait for it.
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
        project = self.projects.get(project_key)
        if project is None:
            return
        if not target_checkout.exists():
            self.git.fetch(self.repo_root, "origin", project.integration_branch)
            self.git.worktree_add_detached(self.repo_root, target_checkout, merge_sha)
        log_dir = self.state_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"runtime-applier-{merge_sha}.log"
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
                return
        await register_spawned(
            self.registry,
            process,
            project_key=project_key,
            kind="runtime_applier",
            worker_id=merge_sha,
            executable=self.uv_binary,
            cwd=str(target_checkout),
            terminate=self._terminate_group,
        )
        with self.database.transaction() as connection:
            self.events.append(
                connection,
                EventInput(
                    event_type="activation.applier_spawned",
                    aggregate_type="activation_apply",
                    aggregate_id=apply_id,
                    payload={"apply_id": apply_id, "pid": int(process.pid)},
                ),
            )

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

    def _active_applier_lease(self, merge_sha: str) -> Any | None:
        if self.registry is None:
            return None
        for lease in self.registry.active():
            if lease.kind == "runtime_applier" and lease.worker_id == merge_sha:
                return lease
        return None

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
