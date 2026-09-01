"""Production wiring for the standalone operations console (INFRA-177).

``build_console_dependencies`` assembles the real service graph — durable
queue, event, checkpoint, and playbook stores over one sqlite database, the
strict Hermes command boundary with bounded handlers for every advertised
mutation, a production target catalog, durable read adapters for workers,
reviews with their CI outcome, and stalls, a transactional audit sink, and
the keychain-backed authentication services — into the
``RemoteDependencies`` the operations app consumes. Capabilities that
cannot be wired safely from durable state are absent from the handler
table, so they neither render in the console nor survive prepare.
``run_console`` serves that app with uvicorn on the loopback address only;
``create_operations_app`` keeps ``BindHostNotLoopback`` as the structural
guard. No import-time side effects: nothing here reads the environment,
the clock, or the keychain until it is called.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import uvicorn
from pydantic import BaseModel

from hermes_orchestrator.checkpoints import CheckpointRequests, CheckpointSafetyStore
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import IssueState
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.hermes_tools import HermesCommandService
from hermes_orchestrator.keychain import Keychain
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.remote.api import (
    LOOPBACK_HOST,
    RemoteDependencies,
    create_operations_app,
)
from hermes_orchestrator.remote.auth import (
    CsrfService,
    LoginRateLimiter,
    RemoteCredentialService,
    SecretStore,
    SessionService,
)
from hermes_orchestrator.remote.commands import ConfirmationService
from hermes_orchestrator.remote.policy import RemotePolicy
from hermes_orchestrator.remote.views import (
    ContextBand,
    ProjectStatusInput,
    QueueItemInput,
    ResourceBand,
    ResourceInput,
    ReviewInput,
    StallInput,
    StatusViewService,
    WorkerStatusInput,
)
from hermes_orchestrator.resources import PressureLevel, ResourceSampler
from hermes_orchestrator.stalls import PlaybookService

_GIB = 1024**3

# Fail-closed pressure mapping: any level absent here — including UNKNOWN
# itself and any member added later — renders as ResourceBand.UNKNOWN.
_PRESSURE_BANDS: Mapping[PressureLevel, ResourceBand] = {
    PressureLevel.GREEN: ResourceBand.GREEN,
    PressureLevel.YELLOW: ResourceBand.YELLOW,
    PressureLevel.RED: ResourceBand.RED,
}

# The only issue states an operator retry may leave; queue.transition
# itself is any-to-any and idempotent, so the bound lives HERE.
_RETRYABLE_STATES = frozenset({IssueState.PAUSED, IssueState.BLOCKED})

# Cell states that count as the one active lead of a project. These mirror
# the daemon's OrchestratorService._worker_states read.
_ACTIVE_CELL_SQL = "state IN ('starting', 'active')"

# Review states worth surfacing on a phone; a closed list keeps unknown
# future states off the rendered surface until deliberately added.
_RENDERED_REVIEW_STATES = (
    "approved",
    "corrections_required",
    "merged",
    "blocked",
    "reconciliation_required",
)


def _age_seconds(stamp: str, now: datetime) -> int:
    """Whole seconds since a stored timestamp, clamped non-negative."""

    try:
        recorded = datetime.fromisoformat(stamp)
    except ValueError:
        return 0
    if recorded.tzinfo is None:
        recorded = recorded.replace(tzinfo=UTC)
    return max(0, int((now - recorded).total_seconds()))


def _single_active_cell(
    database: Database, project_key: str
) -> tuple[str, str] | None:
    """The (cell_id, session_id) of the project's one active lead, if any."""

    rows = database.execute(
        "SELECT cell_id, session_id FROM project_cells "
        f"WHERE project_key = ? AND {_ACTIVE_CELL_SQL}",
        (project_key,),
    ).fetchall()
    if len(rows) != 1 or rows[0]["session_id"] is None:
        return None
    return str(rows[0]["cell_id"]), str(rows[0]["session_id"])


def _retry_handler(queue: QueueService) -> Callable[[BaseModel], dict[str, Any]]:
    def retry(command: Any) -> dict[str, Any]:
        requeued = queue.transition_if(
            command.issue_id,
            IssueState.QUEUED,
            from_states=_RETRYABLE_STATES,
            actor="remote-operator",
            reason="remote operator confirmation",
        )
        if requeued is not None:
            # A pause hold can clear dependency_ready out-of-band (INFRA-198);
            # this requeue is the operator's readiness authority, so repair
            # it in the same confirmed action. None here just means nothing
            # needed repairing.
            queue.restore_readiness_after_requeue(
                command.issue_id,
                actor="remote-operator",
                reason="remote operator confirmation",
            )
            return {"issue_id": requeued.issue_id, "state": requeued.state.value}
        # Not in a retryable state now — but a PREVIOUS requeue may already
        # have landed the row as queued-and-not-ready with no supported way
        # out. Retry is that way out: repair it through this same command.
        # The queued+not-ready shape alone does NOT authorize that, since
        # admission permits exactly it for a legitimately dependency-gated
        # issue; restore_readiness_after_requeue additionally requires
        # durable provenance of a prior journaled requeue, and returns None
        # (leaving the row untouched) without it.
        repaired = queue.restore_readiness_after_requeue(
            command.issue_id,
            actor="remote-operator",
            reason="remote operator confirmation",
        )
        if repaired is None:
            # Static, value-free reason code; the current state never leaks.
            raise ValueError("retry_not_applicable")
        return {"issue_id": repaired.issue_id, "state": repaired.state.value}

    return retry


def _checkpoint_handler(
    database: Database,
    safety: CheckpointSafetyStore,
    requests: CheckpointRequests,
) -> Callable[[BaseModel], dict[str, Any]]:
    def request_checkpoint(command: Any) -> dict[str, Any]:
        cell = _single_active_cell(database, command.project_key)
        if cell is None:
            raise ValueError("no_single_active_worker")
        evidence = safety.current(*cell)
        if evidence is None:
            raise ValueError("no_safe_boundary")
        request = requests.request(
            evidence, reason="remote operator confirmation"
        )
        if request is None:
            raise ValueError("checkpoint_already_pending")
        return {"request_id": request.request_id, "state": request.state}

    return request_checkpoint


def _approve_stall_handler(
    playbooks: PlaybookService,
) -> Callable[[BaseModel], dict[str, Any]]:
    def approve_stall(command: Any) -> dict[str, Any]:
        consultations = playbooks.pending(command.project_key)
        if len(consultations) != 1 or consultations[0].proposed is None:
            # Ambiguous or proposal-less consultations require the local
            # operator flow, which can name a consultation and a remedy.
            raise ValueError("no_single_actionable_consultation")
        consultation = consultations[0]
        playbooks.record_operator_result(
            consultation.consultation_id, consultation.proposed, approved=True
        )
        return {
            "consultation_id": consultation.consultation_id,
            "approved": True,
        }

    return approve_stall


@dataclass(frozen=True)
class _ConsoleTargetCatalog:
    """Existence checks over the console's durable stores; fail closed."""

    database: Database
    project_aliases: frozenset[str]

    def exists(self, kind: str, name: str) -> bool:
        if kind == "project":
            return name in self.project_aliases
        if kind == "issue":
            row = self.database.scalar(
                "SELECT 1 FROM admitted_issues WHERE issue_id = ?", (name,)
            )
            return row is not None
        if kind == "handoff":
            row = self.database.scalar(
                "SELECT 1 FROM handoffs WHERE handoff_id = ?", (name,)
            )
            return row is not None
        # No other kind has a durable backing store in the standalone
        # console process, so no such target exists.
        return False


@dataclass(frozen=True)
class _TransactionAuditSink:
    """Append each audit event through its own committed transaction."""

    database: Database
    events: EventStore

    def append(self, event: EventInput) -> None:
        with self.database.transaction() as connection:
            self.events.append(connection, event)


@dataclass(frozen=True)
class _ConsoleStatusSource:
    """Durable read adapters onto the sanitized status view inputs.

    Every string that reaches a view is either static vocabulary or comes
    from a durable store the daemon writes; the views.py guard still
    screens each value fail-closed before anything renders.
    """

    queue: QueueService
    project_aliases: tuple[str, ...]
    sampler: ResourceSampler
    database: Database
    safety: CheckpointSafetyStore
    github_repos: Mapping[str, str]

    def projects(self) -> tuple[ProjectStatusInput, ...]:
        # Static registration: configuration has no durable registration
        # timestamp to age against, so every configured alias reads as
        # freshly registered.
        return tuple(
            ProjectStatusInput(project_alias=alias, state="registered", age_seconds=0)
            for alias in self.project_aliases
        )

    def workers(self) -> tuple[WorkerStatusInput, ...]:
        # Mirrors the daemon's OrchestratorService._worker_states read:
        # active leads from the durable project_cells table only.
        now = datetime.now(UTC)
        rows = self.database.execute(
            "SELECT cell_id, project_key, state, profile_alias, updated_at "
            f"FROM project_cells WHERE {_ACTIVE_CELL_SQL} "
            "ORDER BY project_key, cell_id"
        ).fetchall()
        return tuple(
            WorkerStatusInput(
                worker_alias=str(row["cell_id"]),
                # A cell before profile binding has no alias yet; the
                # static placeholder keeps the row renderable without
                # synthesizing a value.
                profile_alias=(
                    "unassigned"
                    if row["profile_alias"] is None
                    else str(row["profile_alias"])
                ),
                project_alias=str(row["project_key"]),
                state=str(row["state"]),
                age_seconds=_age_seconds(str(row["updated_at"]), now),
                # ContextMonitor needs the daemon's PolicyConfig thresholds,
                # which this standalone process does not load; without that
                # evidence the truthful band is UNKNOWN.
                context_band=ContextBand.UNKNOWN,
            )
            for row in rows
        )

    def queue_items(self) -> tuple[QueueItemInput, ...]:
        now = datetime.now(UTC)
        return tuple(
            QueueItemInput(
                issue_alias=issue.issue_id,
                project_alias=issue.project_key,
                state=issue.state.value,
                age_seconds=max(
                    0, int((now - issue.admitted_at).total_seconds())
                ),
            )
            for issue in self.queue.list_ranked(now)
        )

    def resources(self) -> ResourceInput:
        snapshot = self.sampler.sample()
        disk_free_gib = (
            min(snapshot.disk_free_bytes.values()) / _GIB
            if snapshot.disk_free_bytes
            else 0.0
        )
        return ResourceInput(
            band=_PRESSURE_BANDS.get(snapshot.pressure, ResourceBand.UNKNOWN),
            available_memory_gib=snapshot.available_memory_bytes / _GIB,
            total_memory_gib=snapshot.total_memory_bytes / _GIB,
            load_one=snapshot.load_one,
            logical_cpus=snapshot.logical_cpus,
            disk_free_gib=disk_free_gib,
        )

    def reviews(self) -> tuple[ReviewInput, ...]:
        now = datetime.now(UTC)
        placeholders = ",".join("?" for _ in _RENDERED_REVIEW_STATES)
        rows = self.database.execute(
            "SELECT project_key, issue_id, pr_number, state, merge_sha, "
            "updated_at FROM reviews "
            f"WHERE state IN ({placeholders}) ORDER BY updated_at, rowid",
            _RENDERED_REVIEW_STATES,
        ).fetchall()
        reviews: list[ReviewInput] = []
        for row in rows:
            repository = self.github_repos.get(str(row["project_key"]))
            if repository is None:
                # Fail closed: a review outside the configured projects has
                # no provable pull-request provenance to render.
                continue
            ci_status = "unknown"
            if row["merge_sha"] is not None:
                ledger_state = self.database.scalar(
                    "SELECT state FROM ci_merge_ledger "
                    "WHERE project_key = ? AND merge_sha = ?",
                    (str(row["project_key"]), str(row["merge_sha"])),
                )
                if ledger_state is not None:
                    ci_status = str(ledger_state)
            reviews.append(
                ReviewInput(
                    issue_alias=str(row["issue_id"]),
                    project_alias=str(row["project_key"]),
                    state=str(row["state"]),
                    # Synthesized from the configured repository and the
                    # stored integer only: the review row's own repository
                    # column never flows into a view, so the exact
                    # provenance check in views.py holds structurally.
                    pr_url=(
                        f"https://github.com/{repository}/pull/"
                        f"{int(row['pr_number'])}"
                    ),
                    ci_status=ci_status,
                    age_seconds=_age_seconds(str(row["updated_at"]), now),
                )
            )
        return tuple(reviews)

    def stalls(self) -> tuple[StallInput, ...]:
        if not self.project_aliases:
            return ()
        now = datetime.now(UTC)
        placeholders = ",".join("?" for _ in self.project_aliases)
        rows = self.database.execute(
            "SELECT consultation_id, project_key, reason, asked_at "
            "FROM stall_consultations WHERE state = 'asked' "
            f"AND project_key IN ({placeholders}) ORDER BY asked_at, rowid",
            tuple(self.project_aliases),
        ).fetchall()
        return tuple(
            StallInput(
                # The consultation id is a value-safe uuid; the summary is
                # the normalized closed-set reason, never operator free
                # text. The view guard still screens both fail-closed.
                worker_alias=str(row["consultation_id"]),
                project_alias=str(row["project_key"]),
                stall_summary=str(row["reason"]),
                age_seconds=_age_seconds(str(row["asked_at"]), now),
                checkpoint_eligible=self._checkpoint_eligible(
                    str(row["project_key"])
                ),
            )
            for row in rows
        )

    def _checkpoint_eligible(self, project_key: str) -> bool:
        # Eligible only when the project has exactly one active cell whose
        # safe boundary is durably proven for its current session.
        cell = _single_active_cell(self.database, project_key)
        return cell is not None and self.safety.current(*cell) is not None


def build_console_dependencies(
    *,
    database: Database,
    github_repos: Mapping[str, str],
    playbook_path: Path,
    secret_store: SecretStore | None = None,
) -> RemoteDependencies:
    """Wire the production console graph over one open database.

    ``secret_store`` exists solely so integration tests can substitute the
    macOS-keychain I/O boundary with an isolated in-memory store; every
    service above that boundary is always the production implementation.
    ``playbook_path`` is the operator-approved ``config/playbooks.yaml``
    the stall approval flow writes atomically.
    """

    store: SecretStore = secret_store if secret_store is not None else Keychain()
    events = EventStore(database)
    queue = QueueService(database, events, github_repos.keys())
    safety = CheckpointSafetyStore(database, events)
    checkpoint_requests = CheckpointRequests(database, events)
    playbooks = PlaybookService(database, events, playbook_path=playbook_path)
    # The wired production action surface: the inline intents (queue_issue,
    # status, reprioritize) plus a bounded durable handler for each of
    # retry, request_checkpoint, and approve_stall. REMOVED deliberately —
    # pause/resume (no durable per-project pause/resume primitive exists
    # anywhere), request_cleanup (unimplemented: worktree reclaim needs a
    # RemoteProof plus git and process ports this console does not hold),
    # and approve_handoff (HandoffService has no approve; acknowledge() is
    # the replacement session's protocol step an operator console must not
    # mint). Their absence makes supports() false, so they neither render
    # in the console nor survive ConfirmationService.prepare.
    executor = HermesCommandService(
        queue,
        handlers={
            "retry": _retry_handler(queue),
            "request_checkpoint": _checkpoint_handler(
                database, safety, checkpoint_requests
            ),
            "approve_stall": _approve_stall_handler(playbooks),
        },
    )
    policy = RemotePolicy()
    audit = _TransactionAuditSink(database=database, events=events)
    source = _ConsoleStatusSource(
        queue=queue,
        project_aliases=tuple(github_repos),
        sampler=ResourceSampler(),
        database=database,
        safety=safety,
        github_repos=dict(github_repos),
    )
    return RemoteDependencies(
        sessions=SessionService(keychain=store),
        csrf=CsrfService(keychain=store),
        policy=policy,
        status=StatusViewService(
            source=source, audit=audit, github_repos=github_repos
        ),
        confirmations=ConfirmationService(
            database=database,
            events=events,
            policy=policy,
            targets=_ConsoleTargetCatalog(
                database=database, project_aliases=frozenset(github_repos)
            ),
            executor=executor,
        ),
        credentials=RemoteCredentialService(keychain=store),
        login_limiter=LoginRateLimiter(),
    )


def run_console(dependencies: RemoteDependencies, *, port: int) -> None:
    """Serve the operations console on the loopback address, blocking."""

    app = create_operations_app(dependencies)
    config = uvicorn.Config(
        app, host=LOOPBACK_HOST, port=port, log_level="warning"
    )
    uvicorn.Server(config).run()
