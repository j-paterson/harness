"""Verify the optimistic CircleCI merge window at the intake boundary."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.ci_window import (
    CiCheck,
    CircleCiIntakeGate,
    CiWindow,
    MergeWindowExhausted,
    PriorMergeFailed,
    ReconciliationIdentity,
    ReconciliationIdentityMismatch,
    ReconciliationPending,
)
from hermes_orchestrator.circleci import CircleCiError
from hermes_orchestrator.db import Database
from hermes_orchestrator.manifests import MANIFEST_VERSION, CandidateManifest
from hermes_orchestrator.merge import ProvenMerge
from hermes_orchestrator.review_intake import (
    CandidateRejected,
    CompositeIntakeGate,
)

REPOSITORY = "j-paterson/demo"
SLUG = "gh/j-paterson/demo"
CANDIDATE = "1" * 40
MERGE_A = "a1" * 20
MERGE_B = "b2" * 20
MERGE_C = "c3" * 20
NOW = datetime(2026, 8, 28, tzinfo=UTC)
IDENTITY = ReconciliationIdentity(
    status="FABLE_READY",
    candidate_sha="2" * 40,
    base_sha="4" * 40,
    branch="feature/eng-10",
    issues=("ENG-10",),
)


@dataclass
class Clock:
    """Deterministic advancing clock for lease expiry tests."""

    value: datetime

    def now(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value = self.value + timedelta(seconds=seconds)


def proven(
    merge_sha: str = MERGE_A,
    pr_number: int = 14,
    candidate_sha: str = CANDIDATE,
    candidate_branch: str = "feature/eng-9",
) -> ProvenMerge:
    return ProvenMerge(
        project_key="demo",
        repository=REPOSITORY,
        pr_number=pr_number,
        candidate_sha=candidate_sha,
        candidate_branch=candidate_branch,
        merge_sha=merge_sha,
        integration_branch="main",
        relation="reviewed_tree_equivalent",
    )


def candidate_manifest() -> CandidateManifest:
    return CandidateManifest(
        manifest_version=MANIFEST_VERSION,
        event_id="evt-2",
        status="FABLE_READY",
        candidate_sha="2" * 40,
        base_sha="4" * 40,
        branch="feature/eng-10",
        linear_issues=("ENG-10",),
        changed_files=("src/app.py",),
        verification=(("uv run pytest -q", "passed"),),
        blockers=(),
        created_at="2026-08-28T00:00:00+00:00",
    )


@dataclass
class FakeStatusPort:
    """Recording CI status port with programmable per-SHA outcomes."""

    results: dict[str, CiCheck | Exception] = field(default_factory=dict)
    calls: list[tuple[str, str, str]] = field(default_factory=list)

    def check(self, project_slug: str, branch: str, merge_sha: str) -> CiCheck:
        self.calls.append((project_slug, branch, merge_sha))
        result = self.results[merge_sha]
        if isinstance(result, Exception):
            raise result
        return result


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def port() -> FakeStatusPort:
    return FakeStatusPort()


@pytest.fixture
def window(database: Database, port: FakeStatusPort) -> CiWindow:
    return CiWindow(
        database=database,
        status=port,
        max_unresolved=2,
        now=lambda: NOW,
    )


def success() -> CiCheck:
    return CiCheck(outcome="success", reason="all workflows succeeded")

def running() -> CiCheck:
    return CiCheck(outcome="nonterminal", reason="workflow build-test running")


def failed() -> CiCheck:
    return CiCheck(
        outcome="failure",
        reason="workflow build-test failed",
        evidence=("pipeline 42 workflow build-test failed", "job pytest failed"),
    )


def test_record_merge_is_durable_idempotent_and_queries_nothing(
    database: Database, port: FakeStatusPort, window: CiWindow
) -> None:
    window.record_merge(proven())
    window.record_merge(proven())
    restarted = CiWindow(
        database=database, status=port, max_unresolved=2, now=lambda: NOW
    )
    assert [i.merge_sha for i in restarted.unresolved_items("demo")] == [MERGE_A]
    assert port.calls == []


@pytest.mark.parametrize(
    "overrides",
    [
        {"pr_number": 15},
        {"candidate_sha": "9" * 40},
        {"candidate_branch": "feature/other"},
    ],
)
def test_record_merge_rejects_conflicting_identity(
    window: CiWindow, overrides: dict[str, Any]
) -> None:
    window.record_merge(proven())
    with pytest.raises(ValueError, match="identity"):
        window.record_merge(proven(**overrides))


def test_reconcile_with_no_unresolved_is_clear_and_silent(
    window: CiWindow, port: FakeStatusPort
) -> None:
    decision = window.reconcile_at_intake("demo")
    assert decision.kind == "clear"
    assert port.calls == []


def test_successful_prior_merge_resolves_durably_once(
    database: Database, port: FakeStatusPort, window: CiWindow
) -> None:
    window.record_merge(proven())
    port.results[MERGE_A] = success()
    decision = window.reconcile_at_intake("demo")
    assert decision.kind == "clear"
    assert decision.resolved == (MERGE_A,)
    assert port.calls == [(SLUG, "main", MERGE_A)]
    restarted = CiWindow(
        database=database, status=port, max_unresolved=2, now=lambda: NOW
    )
    second = restarted.reconcile_at_intake("demo")
    assert second.kind == "clear"
    assert port.calls == [(SLUG, "main", MERGE_A)]


def test_failed_prior_merge_blocks_with_actionable_packet(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven())
    port.results[MERGE_A] = failed()
    decision = window.reconcile_at_intake("demo")
    assert decision.kind == "blocked_prior_failure"
    packet = decision.packet
    assert packet is not None
    assert packet.severity == "Critical"
    assert packet.repository == REPOSITORY
    assert packet.pr_number == 14
    assert packet.reviewed_sha == CANDIDATE
    assert packet.branch == "feature/eng-9"
    assert MERGE_A in packet.evidence
    assert "build-test" in packet.evidence
    assert packet.required_tests


def test_failure_is_durable_and_blocks_without_requerying_ci(
    database: Database, port: FakeStatusPort, window: CiWindow
) -> None:
    window.record_merge(proven())
    port.results[MERGE_A] = failed()
    window.reconcile_at_intake("demo")
    assert len(port.calls) == 1
    restarted = CiWindow(
        database=database, status=port, max_unresolved=2, now=lambda: NOW
    )
    decision = restarted.reconcile_at_intake("demo")
    assert decision.kind == "blocked_prior_failure"
    assert decision.packet is not None
    assert decision.packet.reviewed_sha == CANDIDATE
    assert decision.packet.branch == "feature/eng-9"
    assert len(port.calls) == 1


def test_mark_corrected_unblocks_after_the_lead_fixes_the_failure(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven())
    port.results[MERGE_A] = failed()
    assert window.reconcile_at_intake("demo").kind == "blocked_prior_failure"
    window.mark_corrected("demo", MERGE_A)
    assert window.reconcile_at_intake("demo").kind == "clear"


def test_mark_corrected_requires_a_failed_item(window: CiWindow) -> None:
    window.record_merge(proven())
    with pytest.raises(ValueError, match="failed"):
        window.mark_corrected("demo", MERGE_A)


def test_two_nonterminal_merges_defer_a_third_with_bounded_queries(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven(MERGE_A, pr_number=14))
    window.record_merge(proven(MERGE_B, pr_number=15))
    port.results[MERGE_A] = running()
    port.results[MERGE_B] = running()
    decision = window.reconcile_at_intake("demo")
    assert decision.kind == "deferred_window_full"
    assert set(decision.unresolved) == {MERGE_A, MERGE_B}
    assert len(port.calls) == 2


def test_one_resolution_reopens_the_window(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven(MERGE_A, pr_number=14))
    window.record_merge(proven(MERGE_B, pr_number=15))
    port.results[MERGE_A] = success()
    port.results[MERGE_B] = running()
    decision = window.reconcile_at_intake("demo")
    assert decision.kind == "clear"
    assert decision.resolved == (MERGE_A,)
    assert decision.unresolved == (MERGE_B,)


def test_ambiguous_circleci_never_fabricates_success(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven())
    port.results[MERGE_A] = CircleCiError("connection refused")
    decision = window.reconcile_at_intake("demo")
    assert decision.kind == "clear"
    assert decision.resolved == ()
    assert decision.unresolved == (MERGE_A,)
    assert [i.merge_sha for i in window.unresolved_items("demo")] == [MERGE_A]


def test_ambiguous_circleci_with_full_window_defers(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven(MERGE_A, pr_number=14))
    window.record_merge(proven(MERGE_B, pr_number=15))
    port.results[MERGE_A] = CircleCiError("connection refused")
    port.results[MERGE_B] = CircleCiError("connection refused")
    decision = window.reconcile_at_intake("demo")
    assert decision.kind == "deferred_window_full"


def test_prior_failure_stops_before_checking_later_items(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven(MERGE_A, pr_number=14))
    window.record_merge(proven(MERGE_B, pr_number=15))
    port.results[MERGE_A] = failed()
    decision = window.reconcile_at_intake("demo")
    assert decision.kind == "blocked_prior_failure"
    assert len(port.calls) == 1


def test_window_requires_positive_capacity(
    database: Database, port: FakeStatusPort
) -> None:
    with pytest.raises(ValueError, match="max_unresolved"):
        CiWindow(database=database, status=port, max_unresolved=0, now=lambda: NOW)


def test_projects_are_isolated(window: CiWindow, port: FakeStatusPort) -> None:
    window.record_merge(proven())
    port.results[MERGE_A] = running()
    other = window.reconcile_at_intake("other-project")
    assert other.kind == "clear"
    assert port.calls == []


def test_gate_blocks_new_candidate_on_prior_failure(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven())
    port.results[MERGE_A] = failed()
    gate = CircleCiIntakeGate(window)
    with pytest.raises(PriorMergeFailed) as excinfo:
        gate.validate("demo", candidate_manifest())
    assert isinstance(excinfo.value, CandidateRejected)
    assert excinfo.value.packet.reviewed_sha == CANDIDATE


def test_gate_defers_new_candidate_when_window_is_full(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven(MERGE_A, pr_number=14))
    window.record_merge(proven(MERGE_B, pr_number=15))
    port.results[MERGE_A] = running()
    port.results[MERGE_B] = running()
    gate = CircleCiIntakeGate(window)
    with pytest.raises(MergeWindowExhausted) as excinfo:
        gate.validate("demo", candidate_manifest())
    assert isinstance(excinfo.value, CandidateRejected)
    assert set(excinfo.value.unresolved) == {MERGE_A, MERGE_B}


def test_gate_admits_when_window_permits(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven())
    port.results[MERGE_A] = success()
    CircleCiIntakeGate(window).validate("demo", candidate_manifest())


def test_composite_gate_runs_ci_reconciliation_before_github(
    window: CiWindow, port: FakeStatusPort
) -> None:
    order: list[str] = []

    class RecordingGithubGate:
        def validate(self, project_key: str, candidate: Any) -> None:
            order.append("github")

    class RecordingCiGate(CircleCiIntakeGate):
        def validate(self, project_key: str, candidate: Any) -> None:
            order.append("ci")
            super().validate(project_key, candidate)

    composite = CompositeIntakeGate(
        (RecordingCiGate(window), RecordingGithubGate())
    )
    composite.validate("demo", candidate_manifest())
    assert order == ["ci", "github"]


def test_composite_gate_stops_at_first_failure(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven())
    port.results[MERGE_A] = failed()

    class MustNotRun:
        def validate(self, project_key: str, candidate: Any) -> None:
            raise AssertionError("the GitHub gate must not run after CI blocks")

    composite = CompositeIntakeGate((CircleCiIntakeGate(window), MustNotRun()))
    with pytest.raises(PriorMergeFailed):
        composite.validate("demo", candidate_manifest())


def test_composite_gate_requires_at_least_one_gate() -> None:
    with pytest.raises(ValueError, match="gate"):
        CompositeIntakeGate(())


def test_duplicate_event_replays_the_decision_with_zero_ci_calls(
    window: CiWindow, port: FakeStatusPort
) -> None:
    """Only one eligible event boundary may reconcile; replays are silent."""

    window.record_merge(proven())
    port.results[MERGE_A] = running()
    first = window.reconcile_event("demo", "evt-9", IDENTITY)
    assert first.kind == "clear"
    assert len(port.calls) == 1
    second = window.reconcile_event("demo", "evt-9", IDENTITY)
    assert second.kind == "clear"
    assert second.unresolved == first.unresolved
    assert len(port.calls) == 1


def test_duplicate_failure_event_replays_the_packet_without_ci(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven())
    port.results[MERGE_A] = failed()
    gate = CircleCiIntakeGate(window)
    with pytest.raises(PriorMergeFailed):
        gate.validate("demo", candidate_manifest())
    calls = len(port.calls)
    with pytest.raises(PriorMergeFailed) as excinfo:
        gate.validate("demo", candidate_manifest())
    assert len(port.calls) == calls
    assert excinfo.value.packet.reviewed_sha == CANDIDATE


def test_live_lease_claim_fails_closed_without_ci(
    database: Database, window: CiWindow, port: FakeStatusPort
) -> None:
    """A live claim held by another owner never yields a second query."""

    window.record_merge(proven())
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO ci_reconciliation_claims("
            "project_key, event_id, identity_json, state, owner_token, "
            "lease_expires_at, decision_json, created_at, updated_at"
            ") VALUES ('demo', 'evt-9', ?, 'claimed', ?, ?, "
            "NULL, ?, ?)",
            (
                IDENTITY.as_json(),
                "f" * 32,
                (NOW + timedelta(seconds=300)).isoformat(),
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
    with pytest.raises(ReconciliationPending):
        window.reconcile_event("demo", "evt-9", IDENTITY)
    assert port.calls == []


def test_concurrent_same_event_yields_one_authoritative_reconciliation(
    tmp_path: Path,
) -> None:
    import threading

    db_path = tmp_path / "state.db"
    seed = Database.open(db_path)
    port = FakeStatusPort()
    port.results[MERGE_A] = running()
    CiWindow(
        database=seed, status=port, max_unresolved=2, now=lambda: NOW
    ).record_merge(proven())
    seed.close()

    barrier = threading.Barrier(2)
    results: dict[str, str] = {}

    def attempt(name: str) -> None:
        database = Database.open(db_path)
        window = CiWindow(
            database=database, status=port, max_unresolved=2, now=lambda: NOW
        )
        barrier.wait()
        try:
            decision = window.reconcile_event("demo", "evt-race", IDENTITY)
            results[name] = decision.kind
        except ReconciliationPending:
            results[name] = "pending"
        finally:
            database.close()

    threads = [
        threading.Thread(target=attempt, args=(f"worker-{index}",))
        for index in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(port.calls) == 1
    decided = [kind for kind in results.values() if kind != "pending"]
    assert decided
    assert set(decided) == {"clear"}


def test_no_intake_admits_past_a_durable_failure_across_connections(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    port = FakeStatusPort()
    port.results[MERGE_A] = failed()
    first_db = Database.open(db_path)
    first = CiWindow(
        database=first_db, status=port, max_unresolved=2, now=lambda: NOW
    )
    first.record_merge(proven())
    decision_one = first.reconcile_event("demo", "evt-1", IDENTITY)
    assert decision_one.kind == "blocked_prior_failure"
    first_db.close()
    calls = len(port.calls)

    port.results[MERGE_A] = success()
    second_db = Database.open(db_path)
    second = CiWindow(
        database=second_db, status=port, max_unresolved=2, now=lambda: NOW
    )
    decision = second.reconcile_event("demo", "evt-2", IDENTITY)
    second_db.close()
    assert decision.kind == "blocked_prior_failure"
    assert decision.packet is not None
    assert len(port.calls) == calls


def test_decision_derives_from_committed_state_not_local_observation(
    database: Database,
) -> None:
    """A success observation loses to a durably committed competing failure."""

    import json as jsonlib
    import sqlite3

    packet_json = jsonlib.dumps(
        {
            "severity": "Critical",
            "repository": REPOSITORY,
            "branch": "feature/eng-9",
            "pr_number": 14,
            "reviewed_sha": CANDIDATE,
            "evidence": f"merge {MERGE_A}: workflow build-test failed",
            "acceptance_criterion": "CI must succeed",
            "required_correction": "fix CI",
            "required_tests": ["CircleCI: rerun"],
        },
        sort_keys=True,
        separators=(",", ":"),
    )

    class CompetingPort:
        def __init__(self, path: Path) -> None:
            self.path = path
            self.calls = 0

        def check(
            self, project_slug: str, branch: str, merge_sha: str
        ) -> CiCheck:
            self.calls += 1
            competing = sqlite3.connect(self.path)
            competing.execute(
                "UPDATE ci_merge_ledger SET state = 'failed', "
                "packet_json = ? WHERE project_key = 'demo' "
                "AND merge_sha = ?",
                (packet_json, merge_sha),
            )
            competing.commit()
            competing.close()
            return success()

    port = CompetingPort(database.path)
    window = CiWindow(
        database=database, status=port, max_unresolved=2, now=lambda: NOW
    )
    window.record_merge(proven())
    decision = window.reconcile_at_intake("demo")
    assert decision.kind == "blocked_prior_failure"
    assert decision.packet is not None
    assert decision.packet.reviewed_sha == CANDIDATE
    assert port.calls == 1


def test_packet_fields_are_sanitized_and_bounded(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven())
    port.results[MERGE_A] = CiCheck(
        outcome="failure",
        reason="workflow evil\r\n\x1b[31m" + "z" * 400,
        evidence=(
            "line one\nline two\x00" + "q" * 400,
            "Circle-Token: leaked-secret\r\n",
        ),
    )
    decision = window.reconcile_at_intake("demo")
    packet = decision.packet
    assert packet is not None
    for text in (
        packet.evidence,
        packet.required_correction,
        *packet.required_tests,
    ):
        assert "\n" not in text and "\r" not in text
        assert "\x1b" not in text and "\x00" not in text
        assert len(text) <= 700
        assert "leaked-secret" not in text


def test_replay_never_overrides_more_restrictive_durable_state(
    window: CiWindow, port: FakeStatusPort
) -> None:
    """The exact reviewed regression: stale clear must yield to failure."""

    window.record_merge(proven())
    port.results[MERGE_A] = running()
    first = window.reconcile_event("demo", "evt-a", IDENTITY)
    assert first.kind == "clear"
    port.results[MERGE_A] = failed()
    second = window.reconcile_event("demo", "evt-b", IDENTITY)
    assert second.kind == "blocked_prior_failure"
    calls = len(port.calls)
    replay = window.reconcile_event("demo", "evt-a", IDENTITY)
    assert replay.kind == "blocked_prior_failure"
    assert replay.packet is not None
    assert replay.packet.reviewed_sha == CANDIDATE
    assert len(port.calls) == calls


def test_success_then_later_failure_blocks_the_stale_replay(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven(MERGE_A, pr_number=14))
    port.results[MERGE_A] = success()
    first = window.reconcile_event("demo", "evt-a", IDENTITY)
    assert first.kind == "clear"
    assert first.resolved == (MERGE_A,)
    window.record_merge(proven(MERGE_B, pr_number=15))
    port.results[MERGE_B] = failed()
    assert (
        window.reconcile_event("demo", "evt-b", IDENTITY).kind
        == "blocked_prior_failure"
    )
    calls = len(port.calls)
    replay = window.reconcile_event("demo", "evt-a", IDENTITY)
    assert replay.kind == "blocked_prior_failure"
    assert len(port.calls) == calls


def test_replayed_clear_becomes_deferred_when_window_filled(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven(MERGE_A, pr_number=14))
    port.results[MERGE_A] = running()
    first = window.reconcile_event("demo", "evt-a", IDENTITY)
    assert first.kind == "clear"
    window.record_merge(proven(MERGE_B, pr_number=15))
    calls = len(port.calls)
    replay = window.reconcile_event("demo", "evt-a", IDENTITY)
    assert replay.kind == "deferred_window_full"
    assert set(replay.unresolved) == {MERGE_A, MERGE_B}
    assert len(port.calls) == calls


def test_gate_never_reports_clear_when_durable_state_defers(
    window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven(MERGE_A, pr_number=14))
    port.results[MERGE_A] = running()
    gate = CircleCiIntakeGate(window)
    gate.validate("demo", candidate_manifest())
    window.record_merge(proven(MERGE_B, pr_number=15))
    calls = len(port.calls)
    with pytest.raises(MergeWindowExhausted):
        gate.validate("demo", candidate_manifest())
    assert len(port.calls) == calls


def test_expired_lease_allows_exactly_one_recovery_reconciliation(
    tmp_path: Path,
) -> None:
    """Post-crash recovery re-reads read-only CI state once; never polls."""

    clock = Clock(NOW)
    db_path = tmp_path / "state.db"
    database = Database.open(db_path)
    port = FakeStatusPort()
    port.results[MERGE_A] = running()
    window = CiWindow(
        database=database, status=port, max_unresolved=2, now=clock.now
    )
    window.record_merge(proven())
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO ci_reconciliation_claims("
            "project_key, event_id, identity_json, state, owner_token, "
            "lease_expires_at, decision_json, created_at, updated_at"
            ") VALUES ('demo', 'evt-9', ?, 'claimed', ?, ?, "
            "NULL, ?, ?)",
            (
                IDENTITY.as_json(),
                "e" * 32,
                (NOW + timedelta(seconds=300)).isoformat(),
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
    with pytest.raises(ReconciliationPending):
        window.reconcile_event("demo", "evt-9", IDENTITY)
    assert port.calls == []
    clock.advance(301)
    restarted = CiWindow(
        database=database, status=port, max_unresolved=2, now=clock.now
    )
    decision = restarted.reconcile_event("demo", "evt-9", IDENTITY)
    assert decision.kind == "clear"
    assert len(port.calls) == 1
    replay = restarted.reconcile_event("demo", "evt-9", IDENTITY)
    assert replay.kind == "clear"
    assert len(port.calls) == 1
    database.close()


def test_two_connection_expired_lease_recovery_has_one_winner(
    tmp_path: Path,
) -> None:
    import threading

    clock = Clock(NOW)
    db_path = tmp_path / "state.db"
    seed = Database.open(db_path)
    port = FakeStatusPort()
    port.results[MERGE_A] = running()
    CiWindow(
        database=seed, status=port, max_unresolved=2, now=clock.now
    ).record_merge(proven())
    with seed.transaction() as connection:
        connection.execute(
            "INSERT INTO ci_reconciliation_claims("
            "project_key, event_id, identity_json, state, owner_token, "
            "lease_expires_at, decision_json, created_at, updated_at"
            ") VALUES ('demo', 'evt-9', ?, 'claimed', ?, ?, "
            "NULL, ?, ?)",
            (
                IDENTITY.as_json(),
                "e" * 32,
                NOW.isoformat(),
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
    seed.close()
    clock.advance(1)

    barrier = threading.Barrier(2)
    results: dict[str, str] = {}

    def attempt(name: str) -> None:
        database = Database.open(db_path)
        recovering = CiWindow(
            database=database, status=port, max_unresolved=2, now=clock.now
        )
        barrier.wait()
        try:
            results[name] = recovering.reconcile_event(
                "demo", "evt-9", IDENTITY
            ).kind
        except ReconciliationPending:
            results[name] = "pending"
        finally:
            database.close()

    threads = [
        threading.Thread(target=attempt, args=(f"worker-{index}",))
        for index in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(port.calls) == 1
    decided = [kind for kind in results.values() if kind != "pending"]
    assert decided
    assert set(decided) == {"clear"}


def test_event_reuse_with_different_identity_fails_closed(
    window: CiWindow, port: FakeStatusPort
) -> None:
    """An event id may never replay under a different wake identity."""

    window.record_merge(proven())
    port.results[MERGE_A] = running()
    window.reconcile_event("demo", "evt-9", IDENTITY)
    calls = len(port.calls)
    foreign = ReconciliationIdentity(
        status="FABLE_READY",
        candidate_sha="9" * 40,
        base_sha="4" * 40,
        branch="feature/eng-10",
        issues=("ENG-10",),
    )
    with pytest.raises(ReconciliationIdentityMismatch):
        window.reconcile_event("demo", "evt-9", foreign)
    assert len(port.calls) == calls


def test_identity_mismatch_beats_pending_on_a_live_claim(
    database: Database, window: CiWindow, port: FakeStatusPort
) -> None:
    window.record_merge(proven())
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO ci_reconciliation_claims("
            "project_key, event_id, identity_json, state, owner_token, "
            "lease_expires_at, decision_json, created_at, updated_at"
            ") VALUES ('demo', 'evt-9', ?, 'claimed', ?, ?, "
            "NULL, ?, ?)",
            (
                IDENTITY.as_json(),
                "f" * 32,
                (NOW + timedelta(seconds=300)).isoformat(),
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
    foreign = ReconciliationIdentity(
        status="FABLE_REWORK_READY",
        candidate_sha="2" * 40,
        base_sha="4" * 40,
        branch="feature/eng-10",
        issues=("ENG-10",),
    )
    with pytest.raises(ReconciliationIdentityMismatch):
        window.reconcile_event("demo", "evt-9", foreign)
    assert port.calls == []


@dataclass
class InterleavingPort:
    """Port whose check suspends the caller while another owner acts."""

    result: CiCheck
    before_return: Any
    calls: int = 0

    def check(self, project_slug: str, branch: str, merge_sha: str) -> CiCheck:
        self.calls += 1
        self.before_return()
        return self.result


def test_stale_owner_success_cannot_resolve_after_recovery(
    tmp_path: Path,
) -> None:
    """Reviewed reproduction: stale success must not mask current failure."""

    clock = Clock(NOW)
    db_path = tmp_path / "state.db"
    db_stale = Database.open(db_path)
    db_current = Database.open(db_path)
    port_current = FakeStatusPort()
    port_current.results[MERGE_A] = failed()
    window_current = CiWindow(
        database=db_current, status=port_current, max_unresolved=2,
        now=clock.now,
    )
    outcome: dict[str, Any] = {}

    def while_stale_is_suspended() -> None:
        clock.advance(301)
        outcome["current"] = window_current.reconcile_event(
            "demo", "evt-9", IDENTITY
        )

    port_stale = InterleavingPort(success(), while_stale_is_suspended)
    window_stale = CiWindow(
        database=db_stale, status=port_stale, max_unresolved=2, now=clock.now
    )
    window_stale.record_merge(proven())
    decision_stale = window_stale.reconcile_event("demo", "evt-9", IDENTITY)

    assert outcome["current"].kind == "blocked_prior_failure"
    assert port_current.calls == [(SLUG, "main", MERGE_A)]
    assert decision_stale.kind == "blocked_prior_failure"
    row = db_stale.execute(
        "SELECT state FROM ci_merge_ledger "
        "WHERE project_key = 'demo' AND merge_sha = ?",
        (MERGE_A,),
    ).fetchone()
    assert row["state"] == "failed"
    db_stale.close()
    db_current.close()


def test_stale_owner_failure_cannot_persist_after_current_success(
    tmp_path: Path,
) -> None:
    """Inverse ordering: a stale failure must not override current success."""

    clock = Clock(NOW)
    db_path = tmp_path / "state.db"
    db_stale = Database.open(db_path)
    db_current = Database.open(db_path)
    port_current = FakeStatusPort()
    port_current.results[MERGE_A] = success()
    window_current = CiWindow(
        database=db_current, status=port_current, max_unresolved=2,
        now=clock.now,
    )
    outcome: dict[str, Any] = {}

    def while_stale_is_suspended() -> None:
        clock.advance(301)
        outcome["current"] = window_current.reconcile_event(
            "demo", "evt-9", IDENTITY
        )

    port_stale = InterleavingPort(failed(), while_stale_is_suspended)
    window_stale = CiWindow(
        database=db_stale, status=port_stale, max_unresolved=2, now=clock.now
    )
    window_stale.record_merge(proven())
    decision_stale = window_stale.reconcile_event("demo", "evt-9", IDENTITY)

    assert outcome["current"].kind == "clear"
    assert outcome["current"].resolved == (MERGE_A,)
    assert decision_stale.kind == "clear"
    row = db_stale.execute(
        "SELECT state FROM ci_merge_ledger "
        "WHERE project_key = 'demo' AND merge_sha = ?",
        (MERGE_A,),
    ).fetchone()
    assert row["state"] == "resolved"
    db_stale.close()
    db_current.close()


@pytest.mark.parametrize(
    "column,value",
    [
        ("lease_expires_at", ""),
        ("lease_expires_at", "not-a-timestamp"),
        ("lease_expires_at", "2026-08-28T00:00:00"),
        ("lease_expires_at", "2026-13-99T99:99:99+00:00"),
        ("owner_token", ""),
        ("owner_token", "not-a-generated-token"),
        ("owner_token", "a" * 31),
        ("owner_token", "a" * 33),
        ("owner_token", "A" * 32),
        ("owner_token", "g" * 32),
        ("owner_token", "a" * 31 + " "),
        ("state", "weird"),
        ("identity_json", ""),
    ],
)
def test_malformed_claim_state_fails_closed_before_any_ci_call(
    database: Database,
    window: CiWindow,
    port: FakeStatusPort,
    column: str,
    value: str,
) -> None:
    from hermes_orchestrator.ci_window import ReconciliationClaimCorrupt

    window.record_merge(proven())
    port.results[MERGE_A] = running()
    values = {
        "identity_json": IDENTITY.as_json(),
        "state": "claimed",
        "owner_token": "ffffffffffffffffffffffffffffffff",
        "lease_expires_at": (NOW + timedelta(seconds=300)).isoformat(),
    }
    values[column] = value
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO ci_reconciliation_claims("
            "project_key, event_id, identity_json, state, owner_token, "
            "lease_expires_at, decision_json, created_at, updated_at"
            ") VALUES ('demo', 'evt-9', ?, ?, ?, ?, NULL, ?, ?)",
            (
                values["identity_json"],
                values["state"],
                values["owner_token"],
                values["lease_expires_at"],
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
    with pytest.raises(ReconciliationClaimCorrupt):
        window.reconcile_event("demo", "evt-9", IDENTITY)
    assert port.calls == []
    row = database.execute(
        "SELECT identity_json, state, owner_token, lease_expires_at "
        "FROM ci_reconciliation_claims "
        "WHERE project_key = 'demo' AND event_id = 'evt-9'"
    ).fetchone()
    assert {
        "identity_json": row["identity_json"],
        "state": row["state"],
        "owner_token": row["owner_token"],
        "lease_expires_at": row["lease_expires_at"],
    } == values


def test_credential_shaped_evidence_is_redacted_in_the_packet(
    window: CiWindow, port: FakeStatusPort
) -> None:
    """Controlled remote names must never persist secrets verbatim."""

    window.record_merge(proven())
    port.results[MERGE_A] = CiCheck(
        outcome="failure",
        reason="workflow Circle-Token: controlled-secret failed",
        evidence=(
            "job deploy Authorization: Bearer controlled.jwt failed",
            "auth Authorization: Basic dXNlcjpwYXNz was sent",
            'auth Authorization: Digest username="Mufasa", realm="test", '
            'nonce="abc123nonce", response="deadbeef"',
            "auth Authorization=CustomScheme part-one part-two part-three",
            "api_key=controlled-key was rejected",
            "password: controlled-pass expired",
        ),
    )
    decision = window.reconcile_at_intake("demo")
    packet = decision.packet
    assert packet is not None
    persisted = " ".join(
        (
            packet.evidence,
            packet.required_correction,
            *packet.required_tests,
        )
    )
    assert "controlled-secret" not in persisted
    assert "controlled.jwt" not in persisted
    assert "dXNlcjpwYXNz" not in persisted
    assert "Basic" not in persisted
    for leaked in (
        "Mufasa",
        "abc123nonce",
        "deadbeef",
        "Digest",
        "CustomScheme",
        "part-one",
        "part-two",
        "part-three",
    ):
        assert leaked not in persisted
    assert "controlled-key" not in persisted
    assert "controlled-pass" not in persisted
    assert "[redacted]" in persisted
    assert "deploy" in persisted


def test_expired_claim_with_malformed_token_never_recovers(
    database: Database, window: CiWindow, port: FakeStatusPort
) -> None:
    """The reviewed reproduction: a non-generated token must not recover."""

    from hermes_orchestrator.ci_window import ReconciliationClaimCorrupt

    window.record_merge(proven())
    port.results[MERGE_A] = running()
    expired = (NOW - timedelta(seconds=100)).isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO ci_reconciliation_claims("
            "project_key, event_id, identity_json, state, owner_token, "
            "lease_expires_at, decision_json, created_at, updated_at"
            ") VALUES ('demo', 'evt-9', ?, 'claimed', "
            "'not-a-generated-token', ?, NULL, ?, ?)",
            (IDENTITY.as_json(), expired, NOW.isoformat(), NOW.isoformat()),
        )
    with pytest.raises(ReconciliationClaimCorrupt):
        window.reconcile_event("demo", "evt-9", IDENTITY)
    assert port.calls == []
    row = database.execute(
        "SELECT state, owner_token, lease_expires_at "
        "FROM ci_reconciliation_claims "
        "WHERE project_key = 'demo' AND event_id = 'evt-9'"
    ).fetchone()
    assert row["state"] == "claimed"
    assert row["owner_token"] == "not-a-generated-token"
    assert row["lease_expires_at"] == expired
