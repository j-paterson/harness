"""The durable project-completion contract (INFRA-215)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.project_driver import batch_status

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
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
    state: str = "queued",
    dependency_ready: int = 1,
    priority: int = 1,
    admitted_at: str = NOW_ISO,
    overlap_risk: int = 0,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO admitted_issues(issue_id, project_key, priority, "
            "state, instruction_id, dependency_ready, overlap_risk, "
            "admitted_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                issue_id,
                project_key,
                priority,
                state,
                f"chat-{issue_id}",
                dependency_ready,
                overlap_risk,
                admitted_at,
                admitted_at,
            ),
        )


def seed_lease(
    database: Database,
    *,
    issue_id: str,
    project_key: str = "demo",
    state: str = "active",
    lease_id: str | None = None,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO worktree_leases(lease_id, project_key, issue_id, "
            "repo_path, path, branch, remote, state, acquired_at, "
            "updated_at) VALUES (?, ?, ?, '/repo', '/repo/wt', 'feature/x', "
            "'origin', ?, ?, ?)",
            (
                lease_id or f"lease-{issue_id}",
                project_key,
                issue_id,
                state,
                NOW_ISO,
                NOW_ISO,
            ),
        )


def seed_correction(
    database: Database,
    *,
    correction_id: str,
    project_key: str = "demo",
    issue_id: str = "INFRA-1",
    state: str = "pending",
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO lead_corrections(correction_id, project_key, "
            "issue_id, source, repository, branch, pr_number, "
            "reviewed_sha, packets_json, state, created_at) VALUES "
            "(?, ?, ?, 'sol', 'acme/repo', 'main', 1, 'sha0', '[]', "
            "?, ?)",
            (correction_id, project_key, issue_id, state, NOW_ISO),
        )


def seed_wake_delivery(
    database: Database,
    *,
    event_id: str,
    project_key: str = "demo",
    issue_id: str = "INFRA-1",
    state: str = "pending",
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO wake_deliveries(project_key, event_id, status, "
            "issue_id, candidate_sha, base_sha, branch, manifest_path, "
            "manifest_digest, manifest_device, manifest_inode, "
            "manifest_size, manifest_mtime_ns, manifest_mode, state, "
            "created_at, updated_at) VALUES (?, ?, 'open', ?, ?, 'base', "
            "'main', '/tmp/manifest', 'digest', 0, 0, 0, 0, 0, ?, ?, ?)",
            (
                project_key,
                event_id,
                issue_id,
                "c" * 40,
                state,
                NOW_ISO,
                NOW_ISO,
            ),
        )


def seed_merge_settlement(
    database: Database,
    *,
    settlement_id: str,
    project_key: str = "demo",
    issue_id: str = "INFRA-1",
    state: str = "recorded",
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO merge_settlements(settlement_id, project_key, "
            "issue_id, event_id, repository, branch, pr_number, base_sha, "
            "candidate_sha, thread_id, thread_generation, "
            "manifest_version, path, state, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, 'acme/repo', 'main', 1, 'base', "
            "'candidate', 'thread-1', 1, 1, 'guarded', ?, ?, ?)",
            (
                settlement_id,
                project_key,
                issue_id,
                f"evt-{settlement_id}",
                state,
                NOW_ISO,
                NOW_ISO,
            ),
        )


def seed_submitted_verdict(
    database: Database,
    *,
    event_id: str,
    project_key: str = "demo",
    issue_id: str = "INFRA-1",
    state: str = "submitted",
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO submitted_verdicts(event_id, project_key, "
            "issue_id, candidate_sha, reviewed_thread_id, "
            "reviewed_generation, verdict_json, state, created_at, "
            "updated_at) VALUES (?, ?, ?, 'candidate', 'thread-1', 1, "
            "'{}', ?, ?, ?)",
            (event_id, project_key, issue_id, state, NOW_ISO, NOW_ISO),
        )


def seed_operator_decision(
    database: Database,
    *,
    decision_id: str,
    issue_id: str,
    project_key: str = "demo",
    status: str = "pending",
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO operator_decisions(decision_id, issue_id, "
            "project_key, cell_id, session_id, actor, choice, status, "
            "recorded_at) VALUES (?, ?, ?, 'cell-demo', 'session-1', "
            "'lead', 'proceed', ?, ?)",
            (decision_id, issue_id, project_key, status, NOW_ISO),
        )


def test_batch_is_complete_only_when_all_issues_terminal_and_nothing_pending(
    database: Database,
) -> None:
    """Every admitted issue terminal and every pending count at zero is
    complete; a live lease or a pending correction on an otherwise
    fully-terminal batch keeps it incomplete."""

    seed_issue(database, "INFRA-1", state="done")

    status = batch_status(database, "demo", now=NOW)
    assert status.complete is True
    assert status.next_action == "complete"

    seed_lease(database, issue_id="INFRA-1")
    status = batch_status(database, "demo", now=NOW)
    assert status.complete is False
    assert status.live_leases == 1

    with database.transaction() as connection:
        connection.execute(
            "UPDATE worktree_leases SET state = 'reclaimed' "
            "WHERE issue_id = 'INFRA-1'"
        )
    status = batch_status(database, "demo", now=NOW)
    assert status.complete is True

    seed_correction(database, correction_id="corr-1", issue_id="INFRA-1")
    status = batch_status(database, "demo", now=NOW)
    assert status.complete is False
    assert status.pending_corrections == 1


def test_next_action_replenish_lists_runnable_up_to_headroom(
    database: Database,
) -> None:
    """Seven runnable issues and one already occupying a development
    lane leaves headroom for five -- exactly those five, ranked, are
    named in ``next_action``, though ``runnable`` itself lists all
    seven."""

    for index in range(1, 8):
        seed_issue(
            database,
            f"INFRA-{index}",
            admitted_at=f"2026-09-01T12:00:0{index}+00:00",
        )
    seed_issue(database, "INFRA-100", state="in_development")

    status = batch_status(database, "demo", now=NOW)

    assert len(status.runnable) == 7
    assert status.next_action == "replenish:INFRA-1,INFRA-2,INFRA-3,INFRA-4,INFRA-5"


def test_next_action_awaits_review_when_only_candidates_pending(
    database: Database,
) -> None:
    """An issue held in review with a candidate still in flight through
    the reviewer pipeline, and nothing runnable, awaits review."""

    seed_issue(database, "INFRA-1", state="review")
    seed_wake_delivery(database, event_id="evt-1", issue_id="INFRA-1")

    status = batch_status(database, "demo", now=NOW)

    assert status.runnable == ()
    assert status.pending_candidates == 1
    assert status.next_action == "await_review"
    assert status.complete is False


def test_blocker_names_pending_operator_decision(database: Database) -> None:
    """The sole otherwise-runnable issue is held by a pending operator
    decision: it is excluded from ``runnable``, and both the blocker and
    the next action name it."""

    seed_issue(database, "INFRA-1")
    seed_operator_decision(database, decision_id="dec-1", issue_id="INFRA-1")

    status = batch_status(database, "demo", now=NOW)

    assert status.runnable == ()
    assert status.blocker == "operator_decision:INFRA-1"
    assert status.next_action == "await_operator_decision:INFRA-1"
    assert status.complete is False


def test_batch_never_includes_unadmitted_issues(database: Database) -> None:
    """A different project's admitted issues never leak into this
    project's batch, in any field."""

    seed_issue(database, "INFRA-1", project_key="demo", state="done")
    seed_issue(database, "OTHER-1", project_key="other", state="queued")
    seed_wake_delivery(
        database, event_id="evt-other", project_key="other", issue_id="OTHER-1"
    )

    status = batch_status(database, "demo", now=NOW)

    assert status.admitted == ("INFRA-1",)
    assert status.terminal == ("INFRA-1",)
    assert status.active == ()
    assert status.runnable == ()
    assert status.pending_candidates == 0
    assert status.complete is True
