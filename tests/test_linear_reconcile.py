"""Reconcile stale local queue projections from live Linear reads (INFRA-230)."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.linear import LinearIssue, LinearProjection
from hermes_orchestrator.linear_reconcile import (
    LinearQueueReconciler,
    build_linear_queue_reconciler,
)
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.work_claims import WorkClaims

NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
NOW_ISO = NOW.isoformat()


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


def seed_issue(
    database: Database,
    issue_id: str,
    *,
    project_key: str = "demo",
    state: str = "review",
    admitted_at: str = NOW_ISO,
    updated_at: str | None = None,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO admitted_issues(issue_id, project_key, priority, "
            "state, instruction_id, dependency_ready, overlap_risk, "
            "admitted_at, updated_at) VALUES (?, ?, 1, ?, ?, 1, 0, ?, ?)",
            (
                issue_id,
                project_key,
                state,
                f"chat-{issue_id}",
                admitted_at,
                updated_at or admitted_at,
            ),
        )


def seed_lease(
    database: Database,
    *,
    issue_id: str,
    project_key: str = "demo",
    lease_id: str | None = None,
    state: str = "active",
) -> str:
    lid = lease_id or f"lease-{issue_id}"
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO worktree_leases(lease_id, project_key, issue_id, "
            "repo_path, path, branch, remote, state, acquired_at, "
            "updated_at) VALUES (?, ?, ?, '/repo', '/repo/wt', 'feature/x', "
            "'origin', ?, ?, ?)",
            (lid, project_key, issue_id, state, NOW_ISO, NOW_ISO),
        )
    return lid


INFRA_210_MERGE_SHA = "f5dee512039d197d1c8f5061a52a75f454b3decc"


def seed_review(
    database: Database,
    *,
    review_id: str,
    issue_id: str,
    project_key: str = "demo",
    state: str = "merged",
    merge_sha: str | None = INFRA_210_MERGE_SHA,
    pr_number: int = 40,
    repository: str = "j-paterson/demo",
    branch: str = "feature/x",
    event_id: str | None = None,
    reviewed_sha: str = "a" * 40,
    created_at: str = NOW_ISO,
    updated_at: str = NOW_ISO,
) -> None:
    """One raw ``reviews`` row -- exact column list from migration 0008."""

    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO reviews("
            "review_id, project_key, issue_id, event_id, repository, "
            "branch, pr_number, reviewed_sha, state, merge_sha, reason, "
            "projection_json, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)",
            (
                review_id,
                project_key,
                issue_id,
                event_id or f"event-{review_id}",
                repository,
                branch,
                pr_number,
                reviewed_sha,
                state,
                merge_sha,
                created_at,
                updated_at,
            ),
        )


def seed_settlement(
    database: Database,
    *,
    settlement_id: str,
    issue_id: str,
    project_key: str = "demo",
    state: str = "settled",
    event_id: str | None = None,
    repository: str = "j-paterson/demo",
    branch: str = "feature/x",
    pr_number: int = 40,
    base_sha: str = "b" * 40,
    candidate_sha: str = "c" * 40,
    thread_id: str = "thread-1",
    thread_generation: int = 1,
    manifest_version: int = 1,
    path: str = "guarded",
    created_at: str = NOW_ISO,
    updated_at: str = NOW_ISO,
) -> None:
    """One raw ``merge_settlements`` row -- exact column list from
    migration 0034. ``settlement_id`` must equal the paired review's
    ``review_id`` for ``_completion_proof``'s join to find it."""

    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO merge_settlements("
            "settlement_id, project_key, issue_id, event_id, repository, "
            "branch, pr_number, base_sha, candidate_sha, thread_id, "
            "thread_generation, manifest_version, path, state, "
            "created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                settlement_id,
                project_key,
                issue_id,
                event_id or f"event-{settlement_id}",
                repository,
                branch,
                pr_number,
                base_sha,
                candidate_sha,
                thread_id,
                thread_generation,
                manifest_version,
                path,
                state,
                created_at,
                updated_at,
            ),
        )


def read_row(database: Database, issue_id: str) -> Any:
    return database.execute(
        "SELECT * FROM admitted_issues WHERE issue_id = ?", (issue_id,)
    ).fetchone()


def event_count(database: Database, event_type: str, aggregate_id: str) -> int:
    return int(
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = ? AND aggregate_id = ?",
            (event_type, aggregate_id),
        )
    )


def linear_issue(**overrides: Any) -> LinearIssue:
    values: dict[str, Any] = {
        "issue_id": "ENG-431",
        "linear_id": "lin-1",
        "status": "Done",
        "state_id": "state-done",
        "assignee_id": "user-ryan",
        "team_id": "team-eng",
        "revision": "rev-1",
    }
    values.update(overrides)
    return LinearIssue(**values)


@dataclass
class ScriptedLinearReads:
    """Read-only Linear port scripted per issue id."""

    issues: dict[str, LinearIssue] = field(default_factory=dict)
    errors: dict[str, Exception] = field(default_factory=dict)
    calls: list[str] = field(default_factory=list)

    def get_issue(self, issue_id: str) -> LinearIssue:
        self.calls.append(issue_id)
        if issue_id in self.errors:
            raise self.errors[issue_id]
        return self.issues[issue_id]


@dataclass
class FakeCustodian:
    """Records the checkpoint/verify_remote/reclaim chain; database-backed."""

    database: Database
    fail_lease_ids: frozenset[str] = field(default_factory=frozenset)
    checkpoint_calls: list[tuple[str, str]] = field(default_factory=list)
    reclaim_calls: list[str] = field(default_factory=list)

    def checkpoint(self, lease_id: str, issue_id: str) -> tuple[str, str]:
        self.checkpoint_calls.append((lease_id, issue_id))
        if lease_id in self.fail_lease_ids:
            raise RuntimeError(f"checkpoint blocked for {lease_id}")
        return (lease_id, issue_id)

    def verify_remote(self, checkpoint: tuple[str, str]) -> tuple[str, str]:
        return checkpoint

    def reclaim(self, lease_id: str, proof: tuple[str, str]) -> None:
        self.reclaim_calls.append(lease_id)
        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE worktree_leases SET state = 'reclaimed' "
                "WHERE lease_id = ?",
                (lease_id,),
            )


@dataclass
class RecordingLinearWriter:
    """Idempotent fake ``LinearProjector`` mirroring ``RecordingLinear``
    in ``tests/integration/test_codex_merge_acceptance.py``: replays a
    previously seen effect id as a no-op, and can be told to raise for
    one issue id so a failure can be proven isolated."""

    fail_issue_ids: frozenset[str] = field(default_factory=frozenset)
    calls: list[tuple[str, str | None, str, str]] = field(default_factory=list)
    effect_ids: list[str] = field(default_factory=list)

    async def project(
        self, issue_id: str, target: LinearProjection, effect_id: str
    ) -> object:
        if issue_id in self.fail_issue_ids:
            raise RuntimeError(f"linear write failed for {issue_id}")
        if effect_id in self.effect_ids:
            return object()
        self.effect_ids.append(effect_id)
        self.calls.append((issue_id, target.status, target.assignee_alias, effect_id))
        return object()


def make_reconciler(
    database: Database,
    *,
    linear_reads: ScriptedLinearReads,
    custodian: FakeCustodian | None = None,
    max_reads: int | None = None,
    linear_writer: RecordingLinearWriter | None = None,
) -> LinearQueueReconciler:
    events = EventStore(database)
    claims = WorkClaims(database, events=events)
    queue = QueueService(database, events, registered_projects=(), claims=claims)
    from hermes_orchestrator.worktrees import WorktreeLeases

    leases = WorktreeLeases(database, events)
    return LinearQueueReconciler(
        database,
        queue=queue,
        linear_reads=linear_reads,
        events=events,
        custodian=custodian,
        leases=leases,
        max_reads=max_reads,
        linear_writer=linear_writer,
    )


def test_review_row_done_in_linear_completes_and_closes_claims(
    database: Database,
) -> None:
    seed_issue(database, "ENG-431", state="review")
    events = EventStore(database)
    claims = WorkClaims(database, events=events)
    claims.open(
        project_key="demo",
        issue_id="ENG-431",
        cell_id="cell-1",
        role="review",
    )
    linear_reads = ScriptedLinearReads(issues={"ENG-431": linear_issue(status="Done")})
    reconciler = make_reconciler(database, linear_reads=linear_reads)

    report = reconciler.run()

    row = read_row(database, "ENG-431")
    assert row["state"] == "done"
    assert claims.active_for_issue("ENG-431") == []
    assert event_count(database, "issue.reconciled_from_linear", "ENG-431") == 1
    assert report.completed == 1
    assert report.unchanged == 0
    assert report.unavailable == 0
    outcome = report.outcomes[0]
    assert outcome.action == "completed"
    assert outcome.linear_status == "Done"


def test_lease_is_released_through_custodian_chain(database: Database) -> None:
    seed_issue(database, "ENG-431", state="review")
    lease_id = seed_lease(database, issue_id="ENG-431")
    linear_reads = ScriptedLinearReads(issues={"ENG-431": linear_issue(status="Done")})
    custodian = FakeCustodian(database=database)
    reconciler = make_reconciler(
        database, linear_reads=linear_reads, custodian=custodian
    )

    report = reconciler.run()

    assert custodian.checkpoint_calls == [(lease_id, "ENG-431")]
    assert custodian.reclaim_calls == [lease_id]
    lease_row = database.execute(
        "SELECT state FROM worktree_leases WHERE lease_id = ?", (lease_id,)
    ).fetchone()
    assert lease_row["state"] == "reclaimed"
    assert report.completed == 1


def test_custodian_exception_leaves_lease_and_still_completes(
    database: Database,
) -> None:
    seed_issue(database, "ENG-431", state="review")
    lease_id = seed_lease(database, issue_id="ENG-431")
    linear_reads = ScriptedLinearReads(issues={"ENG-431": linear_issue(status="Done")})
    custodian = FakeCustodian(database=database, fail_lease_ids=frozenset({lease_id}))
    reconciler = make_reconciler(
        database, linear_reads=linear_reads, custodian=custodian
    )

    report = reconciler.run()

    assert custodian.checkpoint_calls == [(lease_id, "ENG-431")]
    assert custodian.reclaim_calls == []
    lease_row = database.execute(
        "SELECT state FROM worktree_leases WHERE lease_id = ?", (lease_id,)
    ).fetchone()
    assert lease_row["state"] == "active"
    row = read_row(database, "ENG-431")
    assert row["state"] == "done"
    assert report.completed == 1
    assert report.unavailable == 0


def test_queued_row_done_in_linear_completes(database: Database) -> None:
    seed_issue(database, "ENG-500", state="queued")
    linear_reads = ScriptedLinearReads(issues={"ENG-500": linear_issue(status="Done")})
    reconciler = make_reconciler(database, linear_reads=linear_reads)

    report = reconciler.run()

    row = read_row(database, "ENG-500")
    assert row["state"] == "done"
    assert report.completed == 1


def test_non_terminal_status_leaves_row_untouched(database: Database) -> None:
    seed_issue(database, "ENG-600", state="review", updated_at=NOW_ISO)
    linear_reads = ScriptedLinearReads(
        issues={"ENG-600": linear_issue(status="In Development")}
    )
    reconciler = make_reconciler(database, linear_reads=linear_reads)

    before = read_row(database, "ENG-600")
    report = reconciler.run()
    after = read_row(database, "ENG-600")

    assert after["state"] == before["state"] == "review"
    assert after["updated_at"] == before["updated_at"] == NOW_ISO
    assert report.completed == 0
    assert report.unchanged == 1
    assert report.outcomes[0].action == "unchanged"
    assert report.outcomes[0].linear_status == "In Development"


def test_one_issue_read_failure_isolates_and_other_project_still_completes(
    database: Database,
) -> None:
    seed_issue(database, "ENG-1", project_key="alpha", state="review")
    seed_issue(database, "ENG-2", project_key="beta", state="review")
    linear_reads = ScriptedLinearReads(
        issues={"ENG-2": linear_issue(status="Done")},
        errors={"ENG-1": RuntimeError("linear timed out")},
    )
    reconciler = make_reconciler(database, linear_reads=linear_reads)

    report = reconciler.run()

    row_1 = read_row(database, "ENG-1")
    row_2 = read_row(database, "ENG-2")
    assert row_1["state"] == "review"
    assert row_2["state"] == "done"
    assert report.completed == 1
    assert report.unavailable == 1
    assert event_count(database, "issue.linear_unavailable", "ENG-1") == 1

    outcomes_by_id = {outcome.issue_id: outcome for outcome in report.outcomes}
    assert outcomes_by_id["ENG-1"].action == "unavailable"
    assert outcomes_by_id["ENG-2"].action == "completed"


def test_done_row_is_never_read_or_moved(database: Database) -> None:
    seed_issue(database, "ENG-9", state="done", updated_at=NOW_ISO)
    linear_reads = ScriptedLinearReads()
    reconciler = make_reconciler(database, linear_reads=linear_reads)

    report = reconciler.run()

    assert linear_reads.calls == []
    row = read_row(database, "ENG-9")
    assert row["state"] == "done"
    assert row["updated_at"] == NOW_ISO
    assert report.outcomes == ()


def test_max_reads_bounds_the_pass(database: Database) -> None:
    seed_issue(database, "ENG-1", project_key="alpha", state="review")
    seed_issue(database, "ENG-2", project_key="beta", state="review")
    linear_reads = ScriptedLinearReads(
        issues={
            "ENG-1": linear_issue(status="Done"),
            "ENG-2": linear_issue(status="Done"),
        }
    )
    reconciler = make_reconciler(database, linear_reads=linear_reads, max_reads=1)

    report = reconciler.run()

    assert len(linear_reads.calls) == 1
    assert report.completed == 1
    assert report.unchanged == 1
    budget_exhausted = [
        outcome
        for outcome in report.outcomes
        if outcome.detail == "read budget exhausted"
    ]
    assert len(budget_exhausted) == 1


def test_report_counts_add_up(database: Database) -> None:
    seed_issue(database, "ENG-1", project_key="alpha", state="review")
    seed_issue(database, "ENG-2", project_key="beta", state="queued")
    seed_issue(database, "ENG-3", project_key="gamma", state="in_development")
    linear_reads = ScriptedLinearReads(
        issues={
            "ENG-1": linear_issue(status="Done"),
            "ENG-2": linear_issue(status="In Development"),
        },
        errors={"ENG-3": RuntimeError("boom")},
    )
    reconciler = make_reconciler(database, linear_reads=linear_reads)

    report = reconciler.run()

    assert len(report.outcomes) == 3
    assert report.completed + report.unchanged + report.unavailable == 3
    assert report.completed == 1
    assert report.unchanged == 1
    assert report.unavailable == 1
    payload = report.as_dict()
    assert payload["completed"] == 1
    assert len(payload["outcomes"]) == 3


def test_build_linear_queue_reconciler_constructs_working_instance(
    database: Database,
) -> None:
    seed_issue(database, "ENG-1", state="review")
    linear_reads = ScriptedLinearReads(issues={"ENG-1": linear_issue(status="Done")})
    reconciler = build_linear_queue_reconciler(database, linear_reads=linear_reads)

    report = reconciler.run()

    row = read_row(database, "ENG-1")
    assert row["state"] == "done"
    assert report.completed == 1


# --- INFRA-230 (Sol d3b5c972): project_completed, the reverse direction ----


def test_local_done_with_settled_merge_restores_linear_to_done(
    database: Database,
) -> None:
    """The live INFRA-210 case: local ``admitted_issues`` row already
    ``done``, Linear still reporting a non-terminal status, and an
    exact settled-merge proof (review merged with a merge sha, joined
    settlement settled) -- restores Linear to Done exactly once without
    touching the local row or reopening implementation. A second pass
    once Linear itself reports Done is unchanged with no second write.
    """

    seed_issue(database, "ENG-210", state="done", updated_at=NOW_ISO)
    seed_review(database, review_id="rev-210", issue_id="ENG-210")
    seed_settlement(database, settlement_id="rev-210", issue_id="ENG-210")
    linear_reads = ScriptedLinearReads(
        issues={"ENG-210": linear_issue(status="In Progress")}
    )
    writer = RecordingLinearWriter()
    reconciler = make_reconciler(
        database, linear_reads=linear_reads, linear_writer=writer
    )

    before = read_row(database, "ENG-210")
    report = asyncio.run(reconciler.project_completed())
    after = read_row(database, "ENG-210")

    assert after["state"] == before["state"] == "done"
    assert after["updated_at"] == before["updated_at"] == NOW_ISO
    assert report.completed == 1
    assert report.unchanged == 0
    assert report.unavailable == 0
    assert report.refused == 0
    assert writer.calls == [
        ("ENG-210", "Done", "operator", "linear:ENG-210:merge-settled-done:rev-210")
    ]
    assert event_count(database, "issue.linear_restored_done", "ENG-210") == 1
    outcome = report.outcomes[0]
    assert outcome.action == "completed"
    assert outcome.linear_status == "In Progress"

    # A second pass, with Linear now genuinely Done, is a no-op: no
    # second write and no second journal entry.
    linear_reads.issues["ENG-210"] = linear_issue(status="Done")
    second_report = asyncio.run(reconciler.project_completed())

    assert second_report.completed == 0
    assert second_report.unchanged == 1
    assert len(writer.calls) == 1
    assert event_count(database, "issue.linear_restored_done", "ENG-210") == 1


def test_local_done_without_authoritative_proof_never_touches_linear(
    database: Database,
) -> None:
    """A ``done`` issue is only a candidate with an exact settled-merge
    proof: no reviews row at all, a review that never merged, and a
    settlement that never settled are all excluded before any Linear
    read or write is attempted."""

    seed_issue(database, "ENG-1", state="done")  # no reviews row at all

    seed_issue(database, "ENG-2", state="done")
    seed_review(
        database,
        review_id="rev-2",
        issue_id="ENG-2",
        state="approved",
        merge_sha=None,
    )
    seed_settlement(database, settlement_id="rev-2", issue_id="ENG-2")

    seed_issue(database, "ENG-3", state="done")
    seed_review(database, review_id="rev-3", issue_id="ENG-3")
    seed_settlement(
        database, settlement_id="rev-3", issue_id="ENG-3", state="recorded"
    )

    linear_reads = ScriptedLinearReads()
    writer = RecordingLinearWriter()
    reconciler = make_reconciler(
        database, linear_reads=linear_reads, linear_writer=writer
    )

    report = asyncio.run(reconciler.project_completed())

    assert linear_reads.calls == []
    assert writer.calls == []
    assert report.outcomes == ()
    assert report.completed == 0


def test_writer_failure_isolates_one_issue_and_other_project_still_completes(
    database: Database,
) -> None:
    seed_issue(database, "ENG-1", project_key="alpha", state="done")
    seed_review(database, review_id="rev-1", issue_id="ENG-1")
    seed_settlement(database, settlement_id="rev-1", issue_id="ENG-1")

    seed_issue(database, "ENG-2", project_key="beta", state="done")
    seed_review(database, review_id="rev-2", issue_id="ENG-2")
    seed_settlement(database, settlement_id="rev-2", issue_id="ENG-2")

    linear_reads = ScriptedLinearReads(
        issues={
            "ENG-1": linear_issue(status="In Development"),
            "ENG-2": linear_issue(status="In Development"),
        }
    )
    writer = RecordingLinearWriter(fail_issue_ids=frozenset({"ENG-1"}))
    reconciler = make_reconciler(
        database, linear_reads=linear_reads, linear_writer=writer
    )

    report = asyncio.run(reconciler.project_completed())

    outcomes_by_id = {outcome.issue_id: outcome for outcome in report.outcomes}
    assert outcomes_by_id["ENG-1"].action == "unavailable"
    assert outcomes_by_id["ENG-2"].action == "completed"
    assert report.completed == 1
    assert report.unavailable == 1
    assert event_count(database, "issue.linear_unavailable", "ENG-1") == 1
    assert writer.calls == [
        ("ENG-2", "Done", "operator", "linear:ENG-2:merge-settled-done:rev-2")
    ]
    row_1 = read_row(database, "ENG-1")
    assert row_1["state"] == "done"


def test_no_linear_writer_configured_refuses_without_crashing(
    database: Database,
) -> None:
    seed_issue(database, "ENG-1", state="done")
    seed_review(database, review_id="rev-1", issue_id="ENG-1")
    seed_settlement(database, settlement_id="rev-1", issue_id="ENG-1")

    linear_reads = ScriptedLinearReads(
        issues={"ENG-1": linear_issue(status="In Development")}
    )
    reconciler = make_reconciler(
        database, linear_reads=linear_reads, linear_writer=None
    )

    report = asyncio.run(reconciler.project_completed())

    assert report.refused == 1
    assert report.completed == 0
    assert report.outcomes[0].action == "refused"
    assert report.outcomes[0].detail == "no linear writer"
