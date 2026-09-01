"""Sol merges are durable settlements: claimed, journaled, resumable.

INFRA-194 regressions: an approved verdict once merged through raw
``gh pr merge`` before ingestion, leaving reviews, merge effects, the
CI ledger, and Linear unsynchronized. These tests prove the guarded
path is the only path — no settlement, no merge — and that a crash at
any journal boundary leaves one resumable row that settles exactly
once.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest, IssueState
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.git import AmbiguousHunkError, GitError
from hermes_orchestrator.github import MergeResult
from hermes_orchestrator.manifests import read_manifest_snapshot
from hermes_orchestrator.merge import ReconciliationRequired
from hermes_orchestrator.settlement import (
    MergeSettlements,
    SettlementBinding,
    SettlementConflict,
)
from tests.integration.test_codex_merge_acceptance import (
    BASE,
    GOOD,
    THIRD,
    Acceptance,
    merge_sha_for,
)
from tests.test_merge import open_pull

NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)


@pytest.fixture
def acceptance(tmp_path: Path) -> Any:
    harness = Acceptance(tmp_path)
    try:
        yield harness
    finally:
        harness.close()


def binding(settlement_id: str = "review:demo:evt-9") -> SettlementBinding:
    return SettlementBinding(
        settlement_id=settlement_id,
        project_key="demo",
        issue_id="ENG-9",
        event_id="evt-9",
        repository="j-paterson/demo",
        branch="feature/eng-9",
        pr_number=14,
        base_sha=BASE,
        candidate_sha=GOOD,
        thread_id="thread-1",
        thread_generation=1,
        manifest_version=1,
    )


class Clock:
    def __init__(self) -> None:
        self.now = NOW

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now = datetime.fromtimestamp(self.now.timestamp() + seconds, tz=UTC)


class TestSettlementLedger:
    """Unit semantics of the exclusive journaled merge claim."""

    @pytest.fixture
    def database(self, tmp_path: Path) -> Any:
        value = Database.open(tmp_path / "ledger.db")
        try:
            yield value
        finally:
            value.close()

    @pytest.fixture
    def clock(self) -> Clock:
        return Clock()

    @pytest.fixture
    def settlements(self, database: Database, clock: Clock) -> MergeSettlements:
        store = MergeSettlements(
            database, EventStore(database), now=clock, lease_seconds=300
        )
        with database.transaction() as connection:
            store.record_in(connection, binding())
        return store

    def test_the_claim_is_exclusive_while_its_lease_lives(
        self, settlements: MergeSettlements, clock: Clock
    ) -> None:
        first = settlements.claim("review:demo:evt-9")
        assert first is not None

        assert settlements.claim("review:demo:evt-9") is None
        clock.advance(301)
        adopted = settlements.claim("review:demo:evt-9")

        assert adopted is not None
        assert adopted != first
        with pytest.raises(SettlementConflict):
            settlements.mark_merged(
                "review:demo:evt-9", token=first, merge_sha="e" * 40
            )

    def test_advances_require_the_exact_owner_and_state(
        self, settlements: MergeSettlements
    ) -> None:
        token = settlements.claim("review:demo:evt-9")
        assert token is not None

        with pytest.raises(SettlementConflict):
            settlements.mark_settled("review:demo:evt-9", token=token)
        settlements.mark_merged("review:demo:evt-9", token=token, merge_sha="e" * 40)
        settlements.mark_settled("review:demo:evt-9", token=token)

        settled = settlements.get("review:demo:evt-9")
        assert settled.state == "settled"
        assert settlements.claim("review:demo:evt-9") is None
        assert settlements.resumable() == ()

    def test_release_returns_an_unmutated_claim_to_recorded(
        self, settlements: MergeSettlements
    ) -> None:
        token = settlements.claim("review:demo:evt-9")
        assert token is not None

        settlements.release("review:demo:evt-9", token=token)

        row = settlements.get("review:demo:evt-9")
        assert row.state == "recorded"
        assert row.owner_token is None
        [resumable] = settlements.resumable("demo")
        assert resumable.settlement_id == "review:demo:evt-9"

    def test_a_conflicting_binding_for_the_same_id_refuses(
        self, database: Database, settlements: MergeSettlements
    ) -> None:
        conflicting = dataclasses.replace(binding(), candidate_sha=THIRD)

        with (
            pytest.raises(SettlementConflict),
            database.transaction() as connection,
        ):
            settlements.record_in(connection, conflicting)

    def test_reopen_failed_cas_returns_a_failed_row_to_recorded(
        self, settlements: MergeSettlements
    ) -> None:
        token = settlements.claim("review:demo:evt-9")
        assert token is not None
        settlements.mark_failed(
            "review:demo:evt-9", token=token, reason="merge blocked by GitHub"
        )

        settlements.reopen_failed("review:demo:evt-9", reason="proof now succeeds")

        row = settlements.get("review:demo:evt-9")
        assert row.state == "recorded"
        assert row.owner_token is None
        assert row.lease_expires_at is None
        assert row.merge_sha is None
        assert row.path == "guarded"
        [resumable] = settlements.resumable("demo")
        assert resumable.settlement_id == "review:demo:evt-9"

    def test_reopen_failed_journals_one_settlement_reopened_event(
        self, database: Database, settlements: MergeSettlements
    ) -> None:
        token = settlements.claim("review:demo:evt-9")
        assert token is not None
        settlements.mark_failed("review:demo:evt-9", token=token, reason="boom")

        settlements.reopen_failed("review:demo:evt-9", reason="reconciled")

        row = database.execute(
            "SELECT payload_json FROM events WHERE event_type = "
            "'settlement.reopened' AND aggregate_id = 'review:demo:evt-9'"
        ).fetchone()
        assert row is not None
        assert json.loads(row["payload_json"]) == {"reason": "reconciled"}

    def test_reopen_failed_refuses_a_recorded_row(
        self, settlements: MergeSettlements
    ) -> None:
        with pytest.raises(SettlementConflict):
            settlements.reopen_failed("review:demo:evt-9", reason="x")

    def test_reopen_failed_refuses_a_merging_row(
        self, settlements: MergeSettlements
    ) -> None:
        token = settlements.claim("review:demo:evt-9")
        assert token is not None

        with pytest.raises(SettlementConflict):
            settlements.reopen_failed("review:demo:evt-9", reason="x")

    def test_reopen_failed_refuses_a_merged_row(
        self, settlements: MergeSettlements
    ) -> None:
        token = settlements.claim("review:demo:evt-9")
        assert token is not None
        settlements.mark_merged("review:demo:evt-9", token=token, merge_sha="e" * 40)

        with pytest.raises(SettlementConflict):
            settlements.reopen_failed("review:demo:evt-9", reason="x")

    def test_reopen_failed_refuses_a_settled_row(
        self, settlements: MergeSettlements
    ) -> None:
        token = settlements.claim("review:demo:evt-9")
        assert token is not None
        settlements.mark_merged("review:demo:evt-9", token=token, merge_sha="e" * 40)
        settlements.mark_settled("review:demo:evt-9", token=token)

        with pytest.raises(SettlementConflict):
            settlements.reopen_failed("review:demo:evt-9", reason="x")

    def test_a_reopened_settlement_claims_and_settles_again(
        self, settlements: MergeSettlements
    ) -> None:
        token = settlements.claim("review:demo:evt-9")
        assert token is not None
        settlements.mark_failed("review:demo:evt-9", token=token, reason="boom")
        settlements.reopen_failed("review:demo:evt-9", reason="reconciled")

        second = settlements.claim("review:demo:evt-9")
        assert second is not None
        settlements.mark_merged(
            "review:demo:evt-9", token=second, merge_sha="e" * 40
        )
        settlements.mark_settled("review:demo:evt-9", token=second)

        assert settlements.get("review:demo:evt-9").state == "settled"


@pytest.mark.asyncio
class TestGuardedCompletion:
    async def test_an_approved_verdict_binds_its_settlement_atomically(
        self, acceptance: Any
    ) -> None:
        outcome = await acceptance.submit("ENG-9", GOOD)

        assert outcome.state == "merged"
        settlement = acceptance.settlements.get("review:demo:evt-1")
        assert settlement.state == "settled"
        assert settlement.project_key == "demo"
        assert settlement.issue_id == "ENG-9"
        assert settlement.event_id == "evt-1"
        assert settlement.repository == "j-paterson/demo"
        assert settlement.branch == "feature/eng-9"
        assert settlement.pr_number == 14
        assert settlement.base_sha == BASE
        assert settlement.candidate_sha == GOOD
        assert settlement.thread_id
        assert settlement.thread_generation == 1
        assert settlement.manifest_version >= 1
        assert settlement.merge_sha == merge_sha_for(GOOD)
        assert settlement.path == "guarded"

    async def test_corrections_record_no_settlement(self, acceptance: Any) -> None:
        await acceptance.submit("ENG-9", GOOD, defect=True)

        assert acceptance.settlements.find("review:demo:evt-1") is None

    async def test_a_pre_settlement_merge_is_structurally_impossible(
        self, acceptance: Any
    ) -> None:
        """The observed incident, replayed against the guarded path.

        An approved review row that exists WITHOUT its recorded
        settlement (the raw pre-settlement world) can never reach the
        GitHub boundary: the guarded path fails closed before any
        mutation.
        """

        with acceptance.database.transaction() as connection:
            connection.execute(
                "INSERT INTO reviews("
                "review_id, project_key, issue_id, event_id, repository, "
                "branch, pr_number, reviewed_sha, state, created_at, "
                "updated_at) VALUES ('review:demo:raw', 'demo', 'ENG-9', "
                "'raw-evt', 'j-paterson/demo', 'feature/eng-9', 14, ?, "
                "'approved', ?, ?)",
                (GOOD, NOW.isoformat(), NOW.isoformat()),
            )

        with pytest.raises(SettlementConflict):
            await acceptance.service.merge_approved("review:demo:raw")

        assert acceptance.github.merge_calls == []

    async def test_a_held_claim_defers_without_touching_github(
        self, acceptance: Any
    ) -> None:
        event, branch, number = acceptance.prepare("ENG-9", GOOD)
        admitted = acceptance.admission.admit("demo", event, received_generation=1)
        record = await acceptance.service.record_verdict(
            admitted,
            "ENG-9",
            verdict_for(branch, number),
        )
        held = acceptance.settlements.claim(record.review_id)
        assert held is not None

        outcome = await acceptance.service.merge_approved(record.review_id)

        assert outcome.state == "deferred"
        assert "claim is held elsewhere" in outcome.reason
        assert acceptance.github.merge_calls == []

    async def test_duplicate_completion_replays_without_a_second_merge(
        self, acceptance: Any
    ) -> None:
        first = await acceptance.submit("ENG-9", GOOD)
        assert first.state == "merged"

        replay = await acceptance.service.merge_approved(first.review_id)

        assert replay.state == "merged"
        assert len(acceptance.github.merge_calls) == 1

    async def test_a_crash_at_the_post_merge_boundary_resumes_exactly_once(
        self, acceptance: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Restart between the GitHub mutation and the ledger.

        The settlement stays 'merging' under its lease; after expiry a
        restart re-drives it: the effect journal replays the completed
        merge (no second mutation), the window journals, the review
        transitions, Linear projects — exactly once.
        """

        original = acceptance.window.record_merge
        calls = {"count": 0}

        def crash_once(proven: Any) -> None:
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("simulated crash before the ledger")
            original(proven)

        monkeypatch.setattr(acceptance.window, "record_merge", crash_once)
        with pytest.raises(RuntimeError, match="simulated crash"):
            await acceptance.submit("ENG-9", GOOD)

        interrupted = acceptance.settlements.get("review:demo:evt-1")
        assert interrupted.state == "merging"
        assert len(acceptance.github.merge_calls) == 1

        # A resume during the live lease must not steal the claim.
        assert acceptance.settlements.resumable("demo") == ()

        with acceptance.database.transaction() as connection:
            connection.execute(
                "UPDATE merge_settlements SET lease_expires_at = ? "
                "WHERE settlement_id = 'review:demo:evt-1'",
                ("2000-01-01T00:00:00+00:00",),
            )
        [outcome] = await acceptance.service.resume_settlements("demo")

        assert outcome.state == "merged"
        assert len(acceptance.github.merge_calls) == 1
        settled = acceptance.settlements.get("review:demo:evt-1")
        assert settled.state == "settled"
        assert settled.merge_sha == merge_sha_for(GOOD)
        ledger = acceptance.database.scalar(
            "SELECT COUNT(*) FROM ci_merge_ledger WHERE merge_sha = ?",
            (merge_sha_for(GOOD),),
        )
        assert ledger == 1
        review_state = acceptance.database.scalar(
            "SELECT state FROM reviews WHERE review_id = 'review:demo:evt-1'"
        )
        assert str(review_state) == "merged"

    async def test_a_full_window_releases_the_claim_for_later_resume(
        self, acceptance: Any
    ) -> None:
        """Approved while the window still had room, deferred at merge.

        The window filled between admission and settlement; the claim
        releases with nothing external done, the settlement stays
        ``recorded``, and once a prior merge resolves the resume pass
        completes it.
        """

        assert (await acceptance.submit("ENG-1", GOOD)).state == "merged"
        third_sha = "555".ljust(40, "e")
        event, branch, number = acceptance.prepare("ENG-3", third_sha, pr_number=16)
        admitted = acceptance.admission.admit("demo", event, received_generation=1)
        record = await acceptance.service.record_verdict(
            admitted, "ENG-3", verdict_for(branch, number, third_sha)
        )
        assert acceptance.merger.complete_admitted_wake("demo", event.event_id)
        assert (await acceptance.submit("ENG-2", THIRD, pr_number=15)).state == "merged"
        # ENG-2's submission rewrote the fake PR fixtures; restore
        # ENG-3's open pull request and merge result for its retry.
        acceptance.github.full_pulls[16] = open_pull(
            number=16, head_sha=third_sha, head_ref=branch
        )
        acceptance.github.merge_result = MergeResult(
            merge_sha_for(third_sha), already_merged=False
        )

        deferred = await acceptance.service.merge_approved(record.review_id)

        assert deferred.state == "deferred"
        assert "window" in deferred.reason
        settlement = acceptance.settlements.get(record.review_id)
        assert settlement.state == "recorded"
        assert len(acceptance.github.merge_calls) == 2

        with acceptance.database.transaction() as connection:
            connection.execute(
                "UPDATE ci_merge_ledger SET state = 'resolved' WHERE merge_sha = ?",
                (merge_sha_for(GOOD),),
            )
        [outcome] = await acceptance.service.resume_settlements("demo")

        assert outcome.state == "merged"
        assert len(acceptance.github.merge_calls) == 3
        assert acceptance.settlements.get(record.review_id).state == "settled"


def verdict_for(branch: str, number: int, sha: str = GOOD) -> Any:
    import json

    from hermes_orchestrator.verdicts import VerdictBinding, parse_verdict

    return parse_verdict(
        json.dumps(
            {
                "verdict": "approved",
                "repository": "j-paterson/demo",
                "branch": branch,
                "reviewed_sha": sha,
                "packets": [],
            }
        ),
        expected=VerdictBinding(
            repository="j-paterson/demo",
            branch=branch,
            reviewed_sha=sha,
        ),
    ).with_pr_number(number)


def replace_reviewer_channel(acceptance: Any) -> None:
    """Rotate the ready channel: new thread, generation 1 -> 2."""

    acceptance.merger.begin_replacement(
        "demo",
        expected_thread_id="thr_stored",
        expected_generation=1,
        reason="reviewer channel rotated",
    )
    acceptance.merger.complete_replacement(
        "demo",
        expected_thread_id="thr_stored",
        expected_generation=1,
        new_thread_id="thr_stored_2",
    )


async def approved_generation_one(acceptance: Any) -> Any:
    """A durable approved review + settlement bound to generation 1."""

    event, branch, number = acceptance.prepare("ENG-9", GOOD)
    admitted = acceptance.admission.admit("demo", event, received_generation=1)
    return await acceptance.service.record_verdict(
        admitted, "ENG-9", verdict_for(branch, number)
    )


def merge_effect_counts(acceptance: Any) -> tuple[int, int]:
    return (
        int(acceptance.database.scalar("SELECT COUNT(*) FROM github_merge_effects")),
        int(acceptance.database.scalar("SELECT COUNT(*) FROM ci_merge_ledger")),
    )


@pytest.mark.asyncio
class TestStaleChannelSettlementFence:
    """Sol 165f5ee6 packet 3: a settlement settles only under the live
    ready reviewer channel that approved it."""

    async def test_replaced_channel_leaves_startup_recovery_non_settling(
        self, acceptance: Any
    ) -> None:
        # Required test 1: approved generation-1 review + settlement,
        # channel replaced with generation 2, startup recovery refuses
        # with zero merge calls and zero merge effects, stably.
        record = await approved_generation_one(acceptance)
        replace_reviewer_channel(acceptance)

        [outcome] = await acceptance.service.resume_settlements("demo")

        assert outcome.state == "stale_settlement"
        assert "no longer match the ready reviewer channel" in outcome.reason
        assert acceptance.github.merge_calls == []
        assert merge_effect_counts(acceptance) == (0, 0)
        settlement = acceptance.settlements.get(record.review_id)
        assert settlement.state == "recorded"
        assert (settlement.thread_id, settlement.thread_generation) == (
            "thr_stored",
            1,
        )
        review_state = acceptance.database.scalar(
            "SELECT state FROM reviews WHERE review_id = ?", (record.review_id,)
        )
        assert str(review_state) == "approved"
        assert acceptance.linear.targets == [("ENG-9", "Review", "operator")]
        # Resumable-but-non-settling: recovery keeps finding it and keeps
        # refusing, with no effect drift.
        [resumable] = acceptance.settlements.resumable("demo")
        assert resumable.settlement_id == record.review_id
        [again] = await acceptance.service.resume_settlements("demo")
        assert again.state == "stale_settlement"
        assert acceptance.github.merge_calls == []
        assert merge_effect_counts(acceptance) == (0, 0)

    async def test_the_merge_settle_cli_entries_refuse_the_stale_binding(
        self, acceptance: Any
    ) -> None:
        # Required test 2 (CLI path): `merge-settle --review` drives
        # merge_approved and `merge-settle --project` drives
        # resume_settlements; both refuse the stale binding identically.
        record = await approved_generation_one(acceptance)
        replace_reviewer_channel(acceptance)

        direct = await acceptance.service.merge_approved(record.review_id)
        [by_project] = await acceptance.service.resume_settlements("demo")

        for outcome in (direct, by_project):
            assert outcome.state == "stale_settlement"
            assert "non-settling" in outcome.reason
        assert acceptance.github.merge_calls == []
        assert merge_effect_counts(acceptance) == (0, 0)
        assert acceptance.settlements.get(record.review_id).state == "recorded"

    async def test_replacement_racing_the_claim_never_reaches_github(
        self, acceptance: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Required test 3: the channel is replaced after the claim fence
        # passed — inside the claimed drive's live pull-request read —
        # and the final pre-mutation revalidation refuses before the
        # GitHub merge call. The claim is released with nothing done.
        record = await approved_generation_one(acceptance)
        original = acceptance.github.get_pull_request
        fired: list[bool] = []

        def replace_then_read(repository: str, number: int) -> Any:
            if not fired:
                fired.append(True)
                replace_reviewer_channel(acceptance)
            return original(repository, number)

        monkeypatch.setattr(
            acceptance.github, "get_pull_request", replace_then_read
        )

        outcome = await acceptance.service.merge_approved(record.review_id)

        assert fired == [True]
        assert outcome.state == "stale_settlement"
        # No stale generation crossed the merge boundary: zero mutations.
        assert acceptance.github.merge_calls == []
        assert merge_effect_counts(acceptance) == (0, 0)
        settlement = acceptance.settlements.get(record.review_id)
        assert settlement.state == "recorded"
        assert settlement.owner_token is None
        assert (settlement.thread_id, settlement.thread_generation) == (
            "thr_stored",
            1,
        )
        review_state = acceptance.database.scalar(
            "SELECT state FROM reviews WHERE review_id = ?", (record.review_id,)
        )
        assert str(review_state) == "approved"

    async def test_a_fresh_generation_bound_approval_supersedes_and_settles_once(
        self, acceptance: Any
    ) -> None:
        # Required test 4 (service level): after the stale refusal, a
        # fresh generation-2 approval of the same event re-binds the one
        # settlement row and settles exactly once.
        from hermes_orchestrator.manifests import read_manifest_snapshot
        from hermes_orchestrator.review_intake import AdmittedCandidate

        event, branch, number = acceptance.prepare("ENG-9", GOOD)
        admitted = acceptance.admission.admit("demo", event, received_generation=1)
        record = await acceptance.service.record_verdict(
            admitted, "ENG-9", verdict_for(branch, number)
        )
        replace_reviewer_channel(acceptance)
        [stale] = await acceptance.service.resume_settlements("demo")
        assert stale.state == "stale_settlement"
        assert acceptance.github.merge_calls == []

        snapshot = read_manifest_snapshot(
            acceptance.root / f"{event.event_id}.json", root=acceptance.root
        )
        fresh = AdmittedCandidate(
            project_key="demo",
            manifest=snapshot.manifest,
            thread_id="thr_stored_2",
            generation=2,
        )
        rebound = await acceptance.service.record_verdict(
            fresh, "ENG-9", verdict_for(branch, number)
        )
        assert rebound.review_id == record.review_id
        settlement = acceptance.settlements.get(record.review_id)
        assert (settlement.thread_id, settlement.thread_generation) == (
            "thr_stored_2",
            2,
        )

        outcome = await acceptance.service.merge_approved(record.review_id)

        assert outcome.state == "merged"
        assert len(acceptance.github.merge_calls) == 1
        assert acceptance.settlements.get(record.review_id).state == "settled"
        # Exactly once: the replay returns the settled outcome.
        replay = await acceptance.service.merge_approved(record.review_id)
        assert replay.state == "merged"
        assert len(acceptance.github.merge_calls) == 1

    async def test_externally_merged_settlements_are_exempt_from_the_fence(
        self, acceptance: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # An interrupted external reconciliation carries no live-channel
        # binding — the mutation already exists on GitHub — so its
        # resume completes receipts even after a channel replacement.
        from hermes_orchestrator.manifests import read_manifest_snapshot

        event, _branch, _number = acceptance.prepare("ENG-9", GOOD)
        acceptance.github.full_pulls[14] = open_pull(
            number=14,
            head_sha=GOOD,
            head_ref="feature/eng-9",
            state="closed",
            merged=True,
            mergeable=None,
            merge_commit_sha=merge_sha_for(GOOD),
        )
        snapshot = read_manifest_snapshot(
            acceptance.root / f"{event.event_id}.json", root=acceptance.root
        )
        journal = acceptance.guarded_github.journal
        original = journal.record_external
        calls: list[str] = []

        def crash_once(effect_id: str, *, request: Any, response: Any) -> Any:
            calls.append(effect_id)
            if len(calls) == 1:
                raise RuntimeError("simulated crash before the receipt")
            return original(effect_id, request=request, response=response)

        monkeypatch.setattr(journal, "record_external", crash_once)
        with pytest.raises(RuntimeError, match="simulated crash"):
            await acceptance.service.reconcile_external_merge(
                project_key="demo",
                issue_id="ENG-9",
                manifest=snapshot.manifest,
                pr_number=14,
            )
        assert acceptance.settlements.get("review:demo:evt-1").state == "recorded"
        replace_reviewer_channel(acceptance)
        with acceptance.database.transaction() as connection:
            connection.execute(
                "UPDATE merge_settlements SET lease_expires_at = ? "
                "WHERE settlement_id = 'review:demo:evt-1'",
                ("2000-01-01T00:00:00+00:00",),
            )

        [outcome] = await acceptance.service.resume_settlements("demo")

        assert outcome.state == "merged"
        assert acceptance.github.merge_calls == []
        settlement = acceptance.settlements.get("review:demo:evt-1")
        assert settlement.state == "settled"
        assert settlement.path == "externally_merged"


def expire_settlement_lease(
    acceptance: Any, settlement_id: str = "review:demo:evt-1"
) -> None:
    with acceptance.database.transaction() as connection:
        connection.execute(
            "UPDATE merge_settlements SET lease_expires_at = ? "
            "WHERE settlement_id = ?",
            ("2000-01-01T00:00:00+00:00", settlement_id),
        )


async def crashed_in_flight_merge(
    acceptance: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """GitHub completed the merge; the process died before the ledger.

    Leaves Sol eadf249b packet 2's durable truth: the settlement is
    ``merging`` under its lease, the merge-effect journal is completed,
    the review row is still ``approved`` (no ledger, no review receipt),
    and GitHub's authoritative pull request is merged at the exact
    reviewed head.
    """

    original = acceptance.window.record_merge
    calls = {"count": 0}

    def crash_once(proven: Any) -> None:
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("simulated crash before the ledger")
        original(proven)

    monkeypatch.setattr(acceptance.window, "record_merge", crash_once)
    with pytest.raises(RuntimeError, match="simulated crash"):
        await acceptance.submit("ENG-9", GOOD)
    assert acceptance.settlements.get("review:demo:evt-1").state == "merging"
    assert len(acceptance.github.merge_calls) == 1
    assert str(
        acceptance.database.scalar(
            "SELECT state FROM reviews WHERE review_id = 'review:demo:evt-1'"
        )
    ) == "approved"
    # GitHub's authoritative state after the completed squash merge.
    acceptance.github.full_pulls[14] = open_pull(
        number=14,
        head_sha=GOOD,
        head_ref="feature/eng-9",
        state="closed",
        merged=True,
        mergeable=None,
        merge_commit_sha=merge_sha_for(GOOD),
    )


@pytest.mark.asyncio
class TestExpiredInFlightRecovery:
    """Sol eadf249b packet 2: the immediate stale refusal is scoped to
    pre-mutation ``recorded`` settlements; an expired in-flight
    ``merging`` row is claimed and driven so an already-completed
    exact-head merge reconciles instead of stranding."""

    async def test_a_completed_merge_reconciles_despite_a_replaced_channel(
        self, acceptance: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Required test 1: crash after GitHub merged but before the
        # ledger/review receipts, expire the lease, replace the reviewer
        # channel, restart recovery — the existing merge reconciles with
        # exactly one GitHub merge call total.
        await crashed_in_flight_merge(acceptance, monkeypatch)
        replace_reviewer_channel(acceptance)
        expire_settlement_lease(acceptance)

        [outcome] = await acceptance.service.resume_settlements("demo")

        assert outcome.state == "merged"
        assert "externally merged; reconciled" in outcome.reason
        # Exactly one GitHub merge call total: the pre-crash mutation.
        assert len(acceptance.github.merge_calls) == 1
        settlement = acceptance.settlements.get("review:demo:evt-1")
        assert settlement.state == "settled"
        assert settlement.merge_sha == merge_sha_for(GOOD)
        assert merge_effect_counts(acceptance) == (1, 1)
        review_state = acceptance.database.scalar(
            "SELECT state FROM reviews WHERE review_id = 'review:demo:evt-1'"
        )
        assert str(review_state) == "merged"

    @pytest.mark.parametrize("entry", ["merge_settle_review", "merge_settle_project"])
    async def test_every_service_entry_completes_receipts_without_a_second_mutation(
        self, acceptance: Any, monkeypatch: pytest.MonkeyPatch, entry: str
    ) -> None:
        # Required test 2: the same expired in-flight recovery through
        # direct resume_settlements (`merge-settle --project`) and the
        # direct review drive (`merge-settle --review`) completes the
        # receipts without a second mutation, and replays stably.
        await crashed_in_flight_merge(acceptance, monkeypatch)
        replace_reviewer_channel(acceptance)
        expire_settlement_lease(acceptance)

        if entry == "merge_settle_review":
            outcome = await acceptance.service.merge_approved("review:demo:evt-1")
        else:
            [outcome] = await acceptance.service.resume_settlements("demo")

        assert outcome.state == "merged"
        assert len(acceptance.github.merge_calls) == 1
        assert merge_effect_counts(acceptance) == (1, 1)
        assert acceptance.settlements.get("review:demo:evt-1").state == "settled"
        # Replay through both entries: settled, and never a second
        # mutation or duplicated receipt.
        replay = await acceptance.service.merge_approved("review:demo:evt-1")
        assert replay.state == "merged"
        assert await acceptance.service.resume_settlements("demo") == ()
        assert len(acceptance.github.merge_calls) == 1
        assert merge_effect_counts(acceptance) == (1, 1)

    async def test_an_unmerged_in_flight_row_is_blocked_by_the_final_fence(
        self, acceptance: Any
    ) -> None:
        # Required test 3: an expired in-flight row whose PR was NOT
        # merged (the owner crashed before the mutation) is claimed and
        # driven, and the final stale-generation fence refuses before
        # any new GitHub merge call.
        record = await approved_generation_one(acceptance)
        token = acceptance.settlements.claim(record.review_id)
        assert token is not None
        expire_settlement_lease(acceptance, record.review_id)
        replace_reviewer_channel(acceptance)

        [outcome] = await acceptance.service.resume_settlements("demo")

        assert outcome.state == "stale_settlement"
        assert "no longer match the ready reviewer channel" in outcome.reason
        # No stale generation crossed the merge boundary: zero mutations.
        assert acceptance.github.merge_calls == []
        assert merge_effect_counts(acceptance) == (0, 0)
        settlement = acceptance.settlements.get(record.review_id)
        assert settlement.state == "recorded"
        assert settlement.owner_token is None
        assert (settlement.thread_id, settlement.thread_generation) == (
            "thr_stored",
            1,
        )
        # Once released to ``recorded`` the pre-mutation claim fence
        # keeps refusing immediately, stably.
        [again] = await acceptance.service.resume_settlements("demo")
        assert again.state == "stale_settlement"
        assert acceptance.github.merge_calls == []
        assert merge_effect_counts(acceptance) == (0, 0)

    async def test_a_fresh_generation_rebinds_the_released_in_flight_row(
        self, acceptance: Any
    ) -> None:
        # Required test 5 (expired in-flight variant): after the final
        # fence releases the unmerged in-flight row back to ``recorded``,
        # a fresh generation-bound approval of the same event re-binds
        # the settlement and settles exactly once.
        from hermes_orchestrator.manifests import read_manifest_snapshot
        from hermes_orchestrator.review_intake import AdmittedCandidate

        event, branch, number = acceptance.prepare("ENG-9", GOOD)
        admitted = acceptance.admission.admit("demo", event, received_generation=1)
        record = await acceptance.service.record_verdict(
            admitted, "ENG-9", verdict_for(branch, number)
        )
        token = acceptance.settlements.claim(record.review_id)
        assert token is not None
        expire_settlement_lease(acceptance, record.review_id)
        replace_reviewer_channel(acceptance)
        [stale] = await acceptance.service.resume_settlements("demo")
        assert stale.state == "stale_settlement"
        assert acceptance.github.merge_calls == []

        snapshot = read_manifest_snapshot(
            acceptance.root / f"{event.event_id}.json", root=acceptance.root
        )
        fresh = AdmittedCandidate(
            project_key="demo",
            manifest=snapshot.manifest,
            thread_id="thr_stored_2",
            generation=2,
        )
        rebound = await acceptance.service.record_verdict(
            fresh, "ENG-9", verdict_for(branch, number)
        )
        assert rebound.review_id == record.review_id
        settlement = acceptance.settlements.get(record.review_id)
        assert (settlement.thread_id, settlement.thread_generation) == (
            "thr_stored_2",
            2,
        )

        outcome = await acceptance.service.merge_approved(record.review_id)

        assert outcome.state == "merged"
        assert len(acceptance.github.merge_calls) == 1
        assert acceptance.settlements.get(record.review_id).state == "settled"


@pytest.mark.asyncio
class TestExternalReconciliation:
    """The PR-merged-before-settlement recovery path (the live gap)."""

    def externally_merge(self, acceptance: Any, event: Any, sha: str) -> Any:
        from hermes_orchestrator.manifests import read_manifest_snapshot

        acceptance.github.full_pulls[14] = open_pull(
            number=14,
            head_sha=sha,
            head_ref="feature/eng-9",
            state="closed",
            merged=True,
            mergeable=None,
            merge_commit_sha=merge_sha_for(sha),
        )
        snapshot = read_manifest_snapshot(
            acceptance.root / f"{event.event_id}.json", root=acceptance.root
        )
        return snapshot.manifest

    async def test_receipts_are_reconstructed_without_a_second_merge(
        self, acceptance: Any
    ) -> None:
        event, _branch, _number = acceptance.prepare("ENG-9", GOOD)
        manifest = self.externally_merge(acceptance, event, GOOD)

        outcome = await acceptance.service.reconcile_external_merge(
            project_key="demo",
            issue_id="ENG-9",
            manifest=manifest,
            pr_number=14,
        )

        assert outcome.state == "merged"
        assert acceptance.github.merge_calls == []
        review_state, review_reason = acceptance.database.execute(
            "SELECT state, reason FROM reviews WHERE review_id = 'review:demo:evt-1'"
        ).fetchone()
        assert str(review_state) == "merged"
        assert "externally merged" in str(review_reason)
        settlement = acceptance.settlements.get("review:demo:evt-1")
        assert settlement.state == "settled"
        assert settlement.path == "externally_merged"
        assert settlement.thread_id == "external_reconciliation"
        assert settlement.merge_sha == merge_sha_for(GOOD)
        effect = acceptance.guarded_github.journal.get("merge:review:demo:evt-1")
        assert effect is not None
        assert effect.state == "completed"
        assert effect.response == {
            "merged": True,
            "sha": merge_sha_for(GOOD),
            "external": True,
        }
        ledger = acceptance.database.scalar(
            "SELECT COUNT(*) FROM ci_merge_ledger WHERE merge_sha = ?",
            (merge_sha_for(GOOD),),
        )
        assert ledger == 1
        assert acceptance.linear.targets[-1][0] == "ENG-9"

    async def test_reconciliation_is_idempotent(self, acceptance: Any) -> None:
        event, _, _ = acceptance.prepare("ENG-9", GOOD)
        manifest = self.externally_merge(acceptance, event, GOOD)
        first = await acceptance.service.reconcile_external_merge(
            project_key="demo", issue_id="ENG-9", manifest=manifest, pr_number=14
        )

        replay = await acceptance.service.reconcile_external_merge(
            project_key="demo", issue_id="ENG-9", manifest=manifest, pr_number=14
        )

        assert first.state == replay.state == "merged"
        assert acceptance.github.merge_calls == []
        assert acceptance.database.scalar("SELECT COUNT(*) FROM reviews") == 1
        assert (
            acceptance.database.scalar("SELECT COUNT(*) FROM github_merge_effects") == 1
        )
        assert acceptance.database.scalar("SELECT COUNT(*) FROM ci_merge_ledger") == 1

    async def test_a_stranded_merging_settlement_converges_despite_a_projection_failure(
        self, acceptance: Any
    ) -> None:
        # INFRA-218 (a): reproduces the live INFRA-216 blocker exactly —
        # a settlement stuck ``merging``/``externally_merged`` with a
        # NULL ``merge_sha`` and an expired lease, whose review record
        # is ALREADY ``merged`` with a real merge_sha. Root cause: in
        # ``_drive_merge``'s (and ``reconcile_external_merge``'s)
        # already-merged replay branch, an unguarded downstream
        # projection call (``_project_after_merge``) could raise and
        # propagate out of ``merge_approved``, leaving the settlement
        # under its claimed lease forever — "merging" for good, because
        # every resumer hits the identical raise. Here the review's
        # stored projection is missing, which is exactly what
        # ``_project_after_merge`` raises on
        # (``RuntimeError("merged review has no stored projection")``).
        event, _, _ = acceptance.prepare("ENG-9", GOOD)
        manifest = self.externally_merge(acceptance, event, GOOD)
        settled = await acceptance.service.reconcile_external_merge(
            project_key="demo", issue_id="ENG-9", manifest=manifest, pr_number=14
        )
        assert settled.state == "merged"
        assert acceptance.settlements.get("review:demo:evt-1").state == "settled"
        assert acceptance.github.merge_calls == []

        # Simulate the crash shape: the settlement never reached
        # ``settled`` (NULL merge_sha, stuck ``merging``, expired
        # lease) even though the review record already proved
        # ``merged``; the stored projection is also lost, reproducing
        # the exact raise that stranded INFRA-216.
        with acceptance.database.transaction() as connection:
            connection.execute(
                "UPDATE merge_settlements SET state = 'merging', "
                "merge_sha = NULL, owner_token = 'stale-owner', "
                "lease_expires_at = '2000-01-01T00:00:00+00:00' "
                "WHERE settlement_id = 'review:demo:evt-1'"
            )
            connection.execute(
                "UPDATE reviews SET projection_json = NULL "
                "WHERE review_id = 'review:demo:evt-1'"
            )
        stranded = acceptance.settlements.get("review:demo:evt-1")
        assert stranded.state == "merging"
        assert stranded.merge_sha is None

        [outcome] = await acceptance.service.resume_settlements("demo")

        assert outcome.state == "merged"
        assert acceptance.github.merge_calls == []
        converged = acceptance.settlements.get("review:demo:evt-1")
        assert converged.state == "settled"
        assert converged.merge_sha == merge_sha_for(GOOD)
        assert converged.path == "externally_merged"

        # A second resume is a stable no-op: nothing left resumable,
        # zero merge calls, no duplicated receipts.
        assert await acceptance.service.resume_settlements("demo") == ()
        assert acceptance.github.merge_calls == []
        assert (
            acceptance.database.scalar("SELECT COUNT(*) FROM github_merge_effects")
            == 1
        )
        assert acceptance.database.scalar("SELECT COUNT(*) FROM ci_merge_ledger") == 1

    async def test_a_fresh_merge_settles_despite_a_projection_failure(
        self, acceptance: Any
    ) -> None:
        # INFRA-218 (S3): closes the same class of stall one step
        # earlier than the S1(a) regression above. ``_settle_proven``
        # is the ONE fresh caller of ``_project_after_merge`` — the
        # docstring on that method names it exactly — reached right
        # after the review row is durably transitioned to ``merged``
        # with its real ``merge_sha``. Before this packet, that call
        # was unguarded, so a downstream projection failure on the
        # very FIRST pass (never mind a replay) propagated out of
        # ``_drive_merge`` before ``merge_approved`` ever reached its
        # ``mark_merged``/``mark_settled`` tail, leaving the
        # just-claimed settlement stuck ``merging`` under its lease.
        event, branch, number = acceptance.prepare("ENG-9", GOOD)
        admitted = acceptance.admission.admit("demo", event, received_generation=1)
        record = await acceptance.service.record_verdict(
            admitted, "ENG-9", verdict_for(branch, number)
        )

        real_project = acceptance.linear.project

        async def flaky_project(issue_id: str, target: Any, effect_id: str) -> Any:
            if "after-merge" in effect_id:
                raise RuntimeError("linear outage")
            return await real_project(issue_id, target, effect_id)

        acceptance.linear.project = flaky_project

        outcome = await acceptance.service.merge_approved(record.review_id)

        assert outcome.state == "merged"
        assert outcome.merge_sha == merge_sha_for(GOOD)
        assert len(acceptance.github.merge_calls) == 1
        settled = acceptance.settlements.get(record.review_id)
        assert settled.state == "settled"
        assert settled.merge_sha == merge_sha_for(GOOD)

        # A second resume is a stable no-op: nothing left resumable
        # (the settlement already reached its terminal durable state),
        # zero additional merge calls.
        assert await acceptance.service.resume_settlements("demo") == ()
        assert len(acceptance.github.merge_calls) == 1

    async def test_reviewer_fix_chain_preserves_submitted_and_final_identities(
        self, acceptance: Any
    ) -> None:
        event, _, number = acceptance.prepare("ENG-9", GOOD, pr_number=14)
        manifest = _manifest_for(acceptance, event)
        final_sha = THIRD
        merge_sha = merge_sha_for(final_sha)
        merge_parent = "9" * 40
        acceptance.github.full_pulls[number] = open_pull(
            number=number,
            head_sha=final_sha,
            head_ref="sol/eng-9-integration",
            state="closed",
            merged=True,
            mergeable=None,
            merge_commit_sha=merge_sha,
        )
        acceptance.git.ancestor[(merge_sha, "origin/main")] = True
        acceptance.git.ancestor[(GOOD, merge_sha)] = False
        acceptance.git.ancestor[(final_sha, merge_sha)] = False
        acceptance.git.trees[merge_sha] = "tree-final"
        acceptance.git.trees[final_sha] = "tree-final"
        acceptance.git.trees[GOOD] = "tree-submitted"
        acceptance.git.parents[merge_sha] = merge_parent
        acceptance.git.ancestor[(BASE, merge_parent)] = True
        acceptance.git.paths[(BASE, GOOD)] = ("src/app.py",)
        acceptance.git.paths[(merge_parent, merge_sha)] = ("src/app.py",)
        acceptance.git.applied_trees[(BASE, GOOD, merge_parent)] = "tree-final"

        outcome = await acceptance.service.reconcile_external_merge(
            project_key="demo",
            issue_id="ENG-9",
            manifest=manifest,
            pr_number=number,
            submitted_sha=GOOD,
            final_integration_sha=final_sha,
            merge_sha=merge_sha,
        )

        assert outcome.state == "merged"
        assert outcome.merge_sha == merge_sha
        review = acceptance.database.execute(
            "SELECT reviewed_sha, merge_sha FROM reviews WHERE review_id = ?",
            (outcome.review_id,),
        ).fetchone()
        assert tuple(review) == (GOOD, merge_sha)
        effect = acceptance.guarded_github.journal.get(f"merge:{outcome.review_id}")
        assert effect is not None
        assert effect.request["sha"] == final_sha
        assert effect.request["head_ref"] == "sol/eng-9-integration"
        payload = _merge_proven_payload(acceptance, outcome.review_id)
        assert payload["submitted_sha"] == GOOD
        assert payload["final_integration_sha"] == final_sha
        assert payload["merge_sha"] == merge_sha

    @pytest.mark.parametrize(
        ("merge_paths", "apply_error", "fragment"),
        [
            (("src/other.py",), None, "merge changed paths differ"),
            (
                ("src/app.py",),
                AmbiguousHunkError("ambiguous submitted patch"),
                "ambiguous submitted patch",
            ),
        ],
    )
    async def test_reviewer_fix_chain_refuses_an_unproven_submitted_candidate(
        self,
        acceptance: Any,
        merge_paths: tuple[str, ...],
        apply_error: GitError | None,
        fragment: str,
    ) -> None:
        event, _, number = acceptance.prepare("ENG-9", GOOD, pr_number=14)
        manifest = _manifest_for(acceptance, event)
        final_sha = THIRD
        merge_sha = merge_sha_for(final_sha)
        merge_parent = "9" * 40
        acceptance.github.full_pulls[number] = open_pull(
            number=number,
            head_sha=final_sha,
            head_ref="sol/eng-9-integration",
            state="closed",
            merged=True,
            mergeable=None,
            merge_commit_sha=merge_sha,
        )
        acceptance.git.ancestor[(merge_sha, "origin/main")] = True
        acceptance.git.ancestor[(final_sha, merge_sha)] = False
        acceptance.git.trees[merge_sha] = "tree-final"
        acceptance.git.trees[final_sha] = "tree-final"
        acceptance.git.ancestor[(GOOD, merge_sha)] = False
        acceptance.git.trees[GOOD] = "tree-submitted"
        acceptance.git.parents[merge_sha] = merge_parent
        acceptance.git.ancestor[(BASE, merge_parent)] = True
        acceptance.git.paths[(BASE, GOOD)] = ("src/app.py",)
        acceptance.git.paths[(merge_parent, merge_sha)] = merge_paths
        acceptance.git.apply_to_tree_error = apply_error

        with pytest.raises(ReconciliationRequired, match=fragment):
            await acceptance.service.reconcile_external_merge(
                project_key="demo",
                issue_id="ENG-9",
                manifest=manifest,
                pr_number=number,
                submitted_sha=GOOD,
                final_integration_sha=final_sha,
                merge_sha=merge_sha,
            )

        assert acceptance.database.scalar("SELECT COUNT(*) FROM reviews") == 0
        assert acceptance.database.scalar(
            "SELECT COUNT(*) FROM merge_settlements"
        ) == 0
        assert acceptance.database.scalar(
            "SELECT COUNT(*) FROM github_merge_effects"
        ) == 0

    async def test_reviewer_fix_chain_resumes_after_a_claimed_crash(
        self, acceptance: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        event, _, number = acceptance.prepare("ENG-9", GOOD, pr_number=14)
        manifest = _manifest_for(acceptance, event)
        final_sha = THIRD
        merge_sha = merge_sha_for(final_sha)
        acceptance.github.full_pulls[number] = open_pull(
            number=number,
            head_sha=final_sha,
            head_ref="sol/eng-9-integration",
            state="closed",
            merged=True,
            mergeable=None,
            merge_commit_sha=merge_sha,
        )
        acceptance.git.ancestor[(merge_sha, "origin/main")] = True
        acceptance.git.ancestor[(GOOD, merge_sha)] = True
        acceptance.git.ancestor[(final_sha, merge_sha)] = False
        acceptance.git.trees[merge_sha] = "tree-final"
        acceptance.git.trees[final_sha] = "tree-final"
        original = acceptance.service._settle_proven

        async def crash(*args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("simulated crash after external effect")

        monkeypatch.setattr(acceptance.service, "_settle_proven", crash)
        with pytest.raises(RuntimeError, match="simulated crash"):
            await acceptance.service.reconcile_external_merge(
                project_key="demo",
                issue_id="ENG-9",
                manifest=manifest,
                pr_number=number,
                submitted_sha=GOOD,
                final_integration_sha=final_sha,
                merge_sha=merge_sha,
            )
        monkeypatch.setattr(acceptance.service, "_settle_proven", original)
        expire_settlement_lease(acceptance)

        [outcome] = await acceptance.service.resume_settlements("demo")

        assert outcome.state == "merged"
        assert outcome.merge_sha == merge_sha
        assert acceptance.settlements.get(outcome.review_id).state == "settled"

    @pytest.mark.parametrize(
        ("submitted_sha", "final_sha", "merge_sha", "fragment"),
        [
            (THIRD, THIRD, merge_sha_for(THIRD), "submitted sha"),
            (GOOD, "d" * 40, merge_sha_for(THIRD), "final integration sha"),
            (GOOD, THIRD, "e" * 40, "merge sha"),
            (GOOD, None, None, "provided together"),
        ],
    )
    async def test_reviewer_fix_chain_mismatch_writes_zero_receipts(
        self,
        acceptance: Any,
        submitted_sha: str,
        final_sha: str | None,
        merge_sha: str | None,
        fragment: str,
    ) -> None:
        event, _, number = acceptance.prepare("ENG-9", GOOD, pr_number=14)
        manifest = _manifest_for(acceptance, event)
        actual_merge = merge_sha_for(THIRD)
        acceptance.github.full_pulls[number] = open_pull(
            number=number,
            head_sha=THIRD,
            head_ref="sol/eng-9-integration",
            state="closed",
            merged=True,
            mergeable=None,
            merge_commit_sha=actual_merge,
        )

        with pytest.raises(ValueError, match=fragment):
            await acceptance.service.reconcile_external_merge(
                project_key="demo",
                issue_id="ENG-9",
                manifest=manifest,
                pr_number=number,
                submitted_sha=submitted_sha,
                final_integration_sha=final_sha,
                merge_sha=merge_sha,
            )

        assert acceptance.database.scalar("SELECT COUNT(*) FROM reviews") == 0
        assert acceptance.database.scalar("SELECT COUNT(*) FROM merge_settlements") == 0
        assert (
            acceptance.database.scalar("SELECT COUNT(*) FROM github_merge_effects")
            == 0
        )

    @pytest.mark.parametrize(
        ("mutation", "error_type", "fragment"),
        [
            ({"head_sha": THIRD}, ValueError, "not the reviewed candidate"),
            ({"head_repository": "fork/demo"}, ValueError, "repository"),
            ({"merged": False, "state": "open"}, ValueError, "not merged"),
            ({"base_ref": "develop"}, ValueError, "integration branch"),
            ({"merge_commit_sha": None}, Exception, "no merge commit"),
        ],
    )
    async def test_mismatched_external_state_writes_zero_receipts(
        self,
        acceptance: Any,
        mutation: dict[str, Any],
        error_type: type[Exception],
        fragment: str,
    ) -> None:
        event, _, _ = acceptance.prepare("ENG-9", GOOD)
        manifest = self.externally_merge(acceptance, event, GOOD)
        acceptance.github.full_pulls[14] = dataclasses.replace(
            acceptance.github.full_pulls[14], **mutation
        )

        with pytest.raises(error_type, match=fragment):
            await acceptance.service.reconcile_external_merge(
                project_key="demo",
                issue_id="ENG-9",
                manifest=manifest,
                pr_number=14,
            )

        assert acceptance.database.scalar("SELECT COUNT(*) FROM reviews") == 0
        assert acceptance.database.scalar("SELECT COUNT(*) FROM merge_settlements") == 0
        assert (
            acceptance.database.scalar("SELECT COUNT(*) FROM github_merge_effects") == 0
        )

    async def test_an_unprovable_merge_commit_writes_zero_receipts(
        self, acceptance: Any
    ) -> None:
        from hermes_orchestrator.merge import ReconciliationRequired

        event, _, _ = acceptance.prepare("ENG-9", GOOD)
        manifest = self.externally_merge(acceptance, event, GOOD)
        acceptance.git.ancestor[(merge_sha_for(GOOD), "origin/main")] = False

        with pytest.raises(ReconciliationRequired, match="not reachable"):
            await acceptance.service.reconcile_external_merge(
                project_key="demo",
                issue_id="ENG-9",
                manifest=manifest,
                pr_number=14,
            )

        assert acceptance.database.scalar("SELECT COUNT(*) FROM reviews") == 0
        assert acceptance.database.scalar("SELECT COUNT(*) FROM merge_settlements") == 0


@pytest.mark.asyncio
class TestPostMergeProjectionRecovery:
    """INFRA-218 Sol correction deb5ec49.

    Sol's finding: the S1(a)/S3 fail-soft settlement path
    (``_project_fail_soft``) always converges a durably-proven merge or
    settlement to its terminal state even when a downstream Linear
    projection raises -- but a successfully settled ORDINARY (non
    acceptance-gated) merge is never returned by ``resume_settlements``
    again once its settlement reaches ``settled``, and
    ``reconcile_acceptance`` only repairs acceptance-GATED issues. Since
    ``LinearClient.project`` only journals its own pending
    ``external_effects`` row AFTER its first live read succeeds
    (``validate_issue``), an outage on that very first read used to
    leave nothing durable to retry: Git/GitHub converged, but Linear
    could stay permanently stale.

    The correction: ``_project_after_merge``'s ordinary branch now
    journals the exact target-only projection request through
    ``ReviewService._effects`` (the same ``ExternalEffectStore``
    primitive activation already relies on) BEFORE calling
    ``self._linear.project``, and ``reconcile_post_merge_projections``
    -- riding the identical ``resume_settlements`` recovery boundary as
    ``reconcile_acceptance`` -- replays whatever is left pending.
    """

    async def _merge_with_linear_outage(
        self, acceptance: Any
    ) -> tuple[Any, str, Any]:
        """Merge a fresh, non-acceptance-gated ENG-9 review whose first
        post-merge Linear projection read fails, mirroring
        ``TestExternalReconciliation.test_a_fresh_merge_settles_despite_a_projection_failure``.
        Returns the review record, the exact target-only effect id the
        ordinary post-merge branch journals, and the real (unpatched)
        ``project`` callable for tests that go on to restore it.
        """

        event, branch, number = acceptance.prepare("ENG-9", GOOD)
        admitted = acceptance.admission.admit("demo", event, received_generation=1)
        record = await acceptance.service.record_verdict(
            admitted, "ENG-9", verdict_for(branch, number)
        )
        real_project = acceptance.linear.project

        async def failing_project(issue_id: str, target: Any, effect_id: str) -> Any:
            if "after-merge" in effect_id:
                raise RuntimeError("linear outage")
            return await real_project(issue_id, target, effect_id)

        acceptance.linear.project = failing_project

        outcome = await acceptance.service.merge_approved(record.review_id)

        assert outcome.state == "merged"
        assert outcome.merge_sha == merge_sha_for(GOOD)
        effect_id = f"linear:ENG-9:after-merge:{record.review_id}"
        return record, effect_id, real_project

    async def test_pending_target_only_effect_survives_a_first_read_outage(
        self, acceptance: Any
    ) -> None:
        record, effect_id, _real_project = await self._merge_with_linear_outage(
            acceptance
        )

        review_state = acceptance.database.scalar(
            "SELECT state FROM reviews WHERE review_id = ?", (record.review_id,)
        )
        assert str(review_state) == "merged"
        settled = acceptance.settlements.get(record.review_id)
        assert settled.state == "settled"

        rows = acceptance.database.execute(
            "SELECT state, request_json FROM external_effects WHERE effect_id = ?",
            (effect_id,),
        ).fetchall()
        assert len(rows) == 1
        assert str(rows[0]["state"]) == "pending"
        request = json.loads(rows[0]["request_json"])
        assert request == {
            "issue_id": "ENG-9",
            "target": {"status": "Done", "assignee_alias": "operator"},
        }

    async def test_recovery_boundary_completes_the_pending_projection(
        self, acceptance: Any
    ) -> None:
        _record, effect_id, real_project = await self._merge_with_linear_outage(
            acceptance
        )
        acceptance.linear.project = real_project
        merge_calls_before = len(acceptance.github.merge_calls)

        outcomes = await acceptance.service.resume_settlements("demo")

        # No settlement is resumable any more (it already settled from
        # Git/GitHub truth) -- the recovered projection rides the same
        # boundary but is not one of this tuple's outcomes.
        assert outcomes == ()
        assert len(acceptance.github.merge_calls) == merge_calls_before
        assert acceptance.linear.targets[-1] == ("ENG-9", "Done", "operator")
        state = acceptance.database.scalar(
            "SELECT state FROM external_effects WHERE effect_id = ?", (effect_id,)
        )
        assert str(state) == "completed"

    async def test_recovery_boundary_replay_is_idempotent(
        self, acceptance: Any
    ) -> None:
        _record, effect_id, real_project = await self._merge_with_linear_outage(
            acceptance
        )
        acceptance.linear.project = real_project
        first = await acceptance.service.resume_settlements("demo")
        assert first == ()
        targets_after_first_recovery = list(acceptance.linear.targets)
        merge_calls_after_first_recovery = len(acceptance.github.merge_calls)

        replay = await acceptance.service.resume_settlements("demo")

        assert replay == ()
        assert acceptance.linear.targets == targets_after_first_recovery
        assert len(acceptance.github.merge_calls) == merge_calls_after_first_recovery
        assert acceptance.database.scalar("SELECT COUNT(*) FROM reviews") == 1
        assert (
            acceptance.database.scalar("SELECT COUNT(*) FROM merge_settlements") == 1
        )
        assert (
            acceptance.database.scalar(
                "SELECT COUNT(*) FROM external_effects WHERE effect_id = ?",
                (effect_id,),
            )
            == 1
        )


@pytest.mark.asyncio
class TestExternalReconciliationExactBinding:
    """INFRA-198: Sol correction b30c55f3 (live PR #43) -- reconciliation
    of an advanced-base squash merge whose exact GitHub PR binding
    (repository, base, PR number, reviewed head SHA == candidate SHA,
    merged, merge commit reachable from main) is already fully proven
    must not park in ``reconciliation_required`` merely because the
    fourth-relation patch reconstruction hits an ambiguous duplicate
    hunk. That binding is sufficient proof on its own; the hunk
    reconstruction is corroboration, not a gate."""

    def _wire_ambiguous_patch(
        self, acceptance: Any, candidate: str, merge_sha: str, *, base: str = BASE
    ) -> None:
        """Force the first three relations to fail and the fourth's
        positional proof to hit exactly the ambiguous-duplicate-hunk
        class (a stand-in for the live tests/test_db.py duplicate
        hunk) -- never a genuine tree mismatch."""

        acceptance.git.ancestor[(merge_sha, "origin/main")] = True
        acceptance.git.ancestor[(candidate, merge_sha)] = False
        acceptance.git.trees[merge_sha] = STABLE_APPLIED_TREE
        acceptance.git.trees[candidate] = "tree-" + candidate
        acceptance.git.ancestor[(base, PARENT_SHA)] = True
        acceptance.git.parents[merge_sha] = PARENT_SHA
        acceptance.git.paths[(base, candidate)] = ADVANCED_PATHS
        acceptance.git.paths[(PARENT_SHA, merge_sha)] = ADVANCED_PATHS
        acceptance.git.apply_to_tree_error = AmbiguousHunkError(
            "reviewed hunk is ambiguous or relocated on the merge parent: "
            "tests/test_db.py hunk #1 (occurrences=2, expected line 10)"
        )

    async def test_ambiguous_duplicate_hunk_settles_on_the_exact_pr_binding(
        self, acceptance: Any
    ) -> None:
        event, branch, number = acceptance.prepare("ENG-9", GOOD, pr_number=14)
        merge_sha = merge_sha_for(GOOD)
        self._wire_ambiguous_patch(acceptance, GOOD, merge_sha)
        acceptance.github.full_pulls[number] = open_pull(
            number=number,
            head_sha=GOOD,
            head_ref=branch,
            state="closed",
            merged=True,
            mergeable=None,
            merge_commit_sha=merge_sha,
        )
        manifest = _manifest_for(acceptance, event)

        outcome = await acceptance.service.reconcile_external_merge(
            project_key="demo", issue_id="ENG-9", manifest=manifest, pr_number=number
        )

        assert outcome.state == "merged"
        assert outcome.merge_sha == merge_sha
        review_id = "review:demo:evt-1"
        payload = _merge_proven_payload(acceptance, review_id)
        assert payload["relation"] == "exact_binding_ambiguous_patch"
        assert payload["base_sha"] == BASE
        assert payload["merge_parent_sha"] == PARENT_SHA
        settlement = acceptance.settlements.get(review_id)
        assert settlement.state == "settled"
        assert settlement.merge_sha == merge_sha
        review_state = acceptance.database.scalar(
            "SELECT state FROM reviews WHERE review_id = ?", (review_id,)
        )
        assert str(review_state) == "merged"

        replay = await acceptance.service.reconcile_external_merge(
            project_key="demo", issue_id="ENG-9", manifest=manifest, pr_number=number
        )

        assert replay.state == "merged"
        assert acceptance.github.merge_calls == []
        assert acceptance.database.scalar("SELECT COUNT(*) FROM reviews") == 1
        assert (
            acceptance.database.scalar("SELECT COUNT(*) FROM github_merge_effects")
            == 1
        )
        assert (
            acceptance.database.scalar(
                "SELECT COUNT(*) FROM events WHERE event_type = 'merge.proven' "
                "AND aggregate_id = ?",
                (review_id,),
            )
            == 1
        )


@pytest.mark.asyncio
async def test_a_merge_conflict_fails_the_settlement_terminally(
    acceptance: Any,
) -> None:
    """GitHub refuses the mutation (405 conflict-class): the review
    records blocked, the settlement records failed with the reason,
    and nothing retries without a fresh operator/lead decision."""

    from hermes_orchestrator.github import MergeBlocked

    event, branch, number = acceptance.prepare("ENG-9", GOOD)
    acceptance.github.merge_error = MergeBlocked(
        "merge blocked by GitHub: base branch protection"
    )
    admitted = acceptance.admission.admit("demo", event, received_generation=1)

    outcome = await acceptance.service.complete_review(
        admitted, "ENG-9", verdict_for(branch, number)
    )

    assert outcome.state == "blocked"
    settlement = acceptance.settlements.get("review:demo:evt-1")
    assert settlement.state == "failed"
    assert "branch protection" in str(settlement.reason)
    assert acceptance.settlements.resumable("demo") == ()
    review_state = acceptance.database.scalar(
        "SELECT state FROM reviews WHERE review_id = 'review:demo:evt-1'"
    )
    assert str(review_state) == "blocked"


@pytest.mark.asyncio
class TestDirectExactHeadMerge:
    """A permitted direct Sol merge reconciles instead of going stale."""

    async def test_the_driver_reconciles_a_direct_merge_into_receipts(
        self, acceptance: Any
    ) -> None:
        event, branch, number = acceptance.prepare("ENG-9", GOOD)
        admitted = acceptance.admission.admit("demo", event, received_generation=1)
        record = await acceptance.service.record_verdict(
            admitted, "ENG-9", verdict_for(branch, number)
        )
        # Sol merges the exact approved head directly (bypassing the
        # guarded helper) before the settlement driver runs.
        acceptance.github.full_pulls[number] = open_pull(
            number=number,
            head_sha=GOOD,
            head_ref=branch,
            state="closed",
            merged=True,
            mergeable=None,
            merge_commit_sha=merge_sha_for(GOOD),
        )

        outcome = await acceptance.service.merge_approved(record.review_id)

        assert outcome.state == "merged"
        assert "externally merged; reconciled" in outcome.reason
        # Never a second merge: the transport was never touched.
        assert acceptance.github.merge_calls == []
        settlement = acceptance.settlements.get(record.review_id)
        assert settlement.state == "settled"
        assert settlement.path == "externally_merged"
        effect = acceptance.guarded_github.journal.get(f"merge:{record.review_id}")
        assert effect is not None and effect.state == "completed"
        assert effect.response is not None and effect.response["external"]
        ledger = acceptance.database.scalar(
            "SELECT COUNT(*) FROM ci_merge_ledger WHERE merge_sha = ?",
            (merge_sha_for(GOOD),),
        )
        assert ledger == 1
        review_state = acceptance.database.scalar(
            "SELECT state FROM reviews WHERE review_id = ?",
            (record.review_id,),
        )
        assert str(review_state) == "merged"

    async def test_an_unprovable_direct_merge_requires_reconciliation(
        self, acceptance: Any
    ) -> None:
        event, branch, number = acceptance.prepare("ENG-9", GOOD)
        admitted = acceptance.admission.admit("demo", event, received_generation=1)
        record = await acceptance.service.record_verdict(
            admitted, "ENG-9", verdict_for(branch, number)
        )
        acceptance.github.full_pulls[number] = open_pull(
            number=number,
            head_sha=GOOD,
            head_ref=branch,
            state="closed",
            merged=True,
            mergeable=None,
            merge_commit_sha=merge_sha_for(GOOD),
        )
        acceptance.git.ancestor[(merge_sha_for(GOOD), "origin/main")] = False

        outcome = await acceptance.service.merge_approved(record.review_id)

        assert outcome.state == "reconciliation_required"
        assert acceptance.github.merge_calls == []
        assert acceptance.settlements.get(record.review_id).state == "failed"


PARENT_SHA = "7" * 40
ADVANCED_PATHS = ("A\tsrc/new.py", "M\tsrc/app.py")
STABLE_APPLIED_TREE = "8" * 40


def _wire_patch_equivalence(
    acceptance: Any, candidate: str, merge_sha: str, *, base: str = BASE
) -> None:
    """Force the three existing proofs to fail and only the fourth —
    patch equivalence against an advanced base — to succeed, mirroring
    the live INFRA-209/PR#39 shape (main advanced past the candidate's
    recorded base before the guarded merge's post-merge proof ran)."""

    acceptance.git.ancestor[(merge_sha, "origin/main")] = True
    acceptance.git.ancestor[(candidate, merge_sha)] = False
    acceptance.git.trees[merge_sha] = STABLE_APPLIED_TREE
    acceptance.git.trees[candidate] = "tree-" + candidate
    acceptance.git.ancestor[(base, PARENT_SHA)] = True
    acceptance.git.parents[merge_sha] = PARENT_SHA
    acceptance.git.paths[(base, candidate)] = ADVANCED_PATHS
    acceptance.git.paths[(PARENT_SHA, merge_sha)] = ADVANCED_PATHS
    acceptance.git.applied_trees[(base, candidate, PARENT_SHA)] = (
        STABLE_APPLIED_TREE
    )


def _manifest_for(acceptance: Any, event: Any) -> Any:
    snapshot = read_manifest_snapshot(
        acceptance.root / f"{event.event_id}.json", root=acceptance.root
    )
    return snapshot.manifest


def _merge_proven_payload(acceptance: Any, review_id: str) -> dict[str, Any]:
    rows = acceptance.database.execute(
        "SELECT payload_json FROM events WHERE event_type = 'merge.proven' "
        "AND aggregate_id = ?",
        (review_id,),
    ).fetchall()
    assert len(rows) == 1, f"expected exactly one merge.proven event, found {rows}"
    return dict(json.loads(rows[0]["payload_json"]))


@pytest.mark.asyncio
class TestReconciliationAfterProofFailure:
    """INFRA-210: the observed PR #39 shape — a guarded merge's own
    completed merge effect whose post-merge proof failed (main advanced
    past the recorded base) is later reconciled, exactly once, from a
    fresh proof bound to that same recorded base."""

    async def _guarded_merge_that_fails_proof(self, acceptance: Any) -> Any:
        event, branch, number = acceptance.prepare("ENG-9", GOOD, pr_number=14)
        merge_sha = merge_sha_for(GOOD)
        admitted = acceptance.admission.admit("demo", event, received_generation=1)
        record = await acceptance.service.record_verdict(
            admitted, "ENG-9", verdict_for(branch, number)
        )
        _wire_patch_equivalence(acceptance, GOOD, merge_sha)
        # The historical incident: the post-merge proof could not be
        # computed at all (stand-in for "main advanced past what the old
        # three-relation proof could reconcile"); the guarded merge itself
        # still completed and journaled.
        acceptance.git.first_parent_error = GitError("git rev-parse failed")

        outcome = await acceptance.service.merge_approved(record.review_id)

        # GitHub itself now reports the pull request merged with this exact
        # commit, as it would for the real guarded mutation that just ran.
        acceptance.github.full_pulls[number] = open_pull(
            number=number,
            head_sha=GOOD,
            head_ref=branch,
            state="closed",
            merged=True,
            mergeable=None,
            merge_commit_sha=merge_sha,
        )
        assert outcome.state == "reconciliation_required"
        assert len(acceptance.github.merge_calls) == 1
        settlement = acceptance.settlements.get(record.review_id)
        assert settlement.state == "failed"
        effect = acceptance.guarded_github.journal.get(f"merge:{record.review_id}")
        assert effect is not None
        assert effect.state == "completed"
        assert effect.response == {"merged": True, "sha": merge_sha}
        acceptance.git.first_parent_error = None
        return record, event, branch, number, merge_sha

    async def test_a_failed_post_merge_proof_reconciles_via_patch_equivalence(
        self, acceptance: Any
    ) -> None:
        record, event, _branch, number, merge_sha = (
            await self._guarded_merge_that_fails_proof(acceptance)
        )
        manifest = _manifest_for(acceptance, event)

        outcome = await acceptance.service.reconcile_external_merge(
            project_key="demo", issue_id="ENG-9", manifest=manifest, pr_number=number
        )

        assert outcome.state == "merged"
        assert outcome.merge_sha == merge_sha
        review_state, review_sha = acceptance.database.execute(
            "SELECT state, merge_sha FROM reviews WHERE review_id = ?",
            (record.review_id,),
        ).fetchone()
        assert str(review_state) == "merged"
        assert str(review_sha) == merge_sha
        settlement = acceptance.settlements.get(record.review_id)
        assert settlement.state == "settled"
        assert settlement.path == "guarded"
        assert settlement.merge_sha == merge_sha
        # Never a second GitHub mutation: the original guarded merge call
        # is the only one, and its journal row is untouched, not duplicated.
        assert len(acceptance.github.merge_calls) == 1
        assert (
            acceptance.database.scalar("SELECT COUNT(*) FROM github_merge_effects")
            == 1
        )
        assert (
            acceptance.database.scalar(
                "SELECT COUNT(*) FROM ci_merge_ledger WHERE merge_sha = ?",
                (merge_sha,),
            )
            == 1
        )
        done_targets = [
            target for target in acceptance.linear.targets if target[1] == "Done"
        ]
        assert done_targets == [("ENG-9", "Done", "operator")]
        payload = _merge_proven_payload(acceptance, record.review_id)
        assert payload["relation"] == "patch_equivalent"
        assert payload["base_sha"] == BASE
        assert payload["merge_sha"] == merge_sha

    async def test_a_second_reconcile_call_is_a_no_op(self, acceptance: Any) -> None:
        record, event, _branch, number, _merge_sha = (
            await self._guarded_merge_that_fails_proof(acceptance)
        )
        manifest = _manifest_for(acceptance, event)
        first = await acceptance.service.reconcile_external_merge(
            project_key="demo", issue_id="ENG-9", manifest=manifest, pr_number=number
        )

        replay = await acceptance.service.reconcile_external_merge(
            project_key="demo", issue_id="ENG-9", manifest=manifest, pr_number=number
        )

        assert first.state == replay.state == "merged"
        assert len(acceptance.github.merge_calls) == 1
        assert acceptance.database.scalar("SELECT COUNT(*) FROM reviews") == 1
        assert (
            acceptance.database.scalar("SELECT COUNT(*) FROM github_merge_effects")
            == 1
        )
        assert acceptance.database.scalar("SELECT COUNT(*) FROM ci_merge_ledger") == 1
        assert len(
            [t for t in acceptance.linear.targets if t[1] == "Done"]
        ) == 1
        assert acceptance.database.scalar(
            "SELECT COUNT(*) FROM events WHERE event_type = 'merge.proven' "
            "AND aggregate_id = ?",
            (record.review_id,),
        ) == 1

    async def test_resume_settlements_after_reconciliation_changes_nothing(
        self, acceptance: Any
    ) -> None:
        record, event, _branch, number, _merge_sha = (
            await self._guarded_merge_that_fails_proof(acceptance)
        )
        manifest = _manifest_for(acceptance, event)
        await acceptance.service.reconcile_external_merge(
            project_key="demo", issue_id="ENG-9", manifest=manifest, pr_number=number
        )

        outcomes = await acceptance.service.resume_settlements()

        assert outcomes == ()
        assert len(acceptance.github.merge_calls) == 1
        assert (
            acceptance.database.scalar("SELECT COUNT(*) FROM github_merge_effects")
            == 1
        )
        assert acceptance.settlements.get(record.review_id).state == "settled"

    async def test_a_failed_settlement_without_a_completed_effect_is_not_reopened(
        self, acceptance: Any
    ) -> None:
        """A direct-Sol-merge proof failure never journals a merge effect
        for this review at all; a later successful proof must not reopen
        a settlement with nothing completed to reconcile against."""

        event, branch, number = acceptance.prepare("ENG-9", GOOD, pr_number=14)
        merge_sha = merge_sha_for(GOOD)
        admitted = acceptance.admission.admit("demo", event, received_generation=1)
        record = await acceptance.service.record_verdict(
            admitted, "ENG-9", verdict_for(branch, number)
        )
        acceptance.github.full_pulls[number] = open_pull(
            number=number,
            head_sha=GOOD,
            head_ref=branch,
            state="closed",
            merged=True,
            mergeable=None,
            merge_commit_sha=merge_sha,
        )
        acceptance.git.ancestor[(merge_sha, "origin/main")] = False

        outcome = await acceptance.service.merge_approved(record.review_id)

        assert outcome.state == "reconciliation_required"
        assert acceptance.github.merge_calls == []
        assert acceptance.settlements.get(record.review_id).state == "failed"
        assert (
            acceptance.guarded_github.journal.get(f"merge:{record.review_id}")
            is None
        )

        # The ancestry read is fixed and a fresh proof now succeeds, but
        # the failed settlement owns no completed merge effect to
        # reconcile against, so it must stay exactly as it was.
        acceptance.git.ancestor[(merge_sha, "origin/main")] = True
        manifest = _manifest_for(acceptance, event)

        outcome2 = await acceptance.service.reconcile_external_merge(
            project_key="demo", issue_id="ENG-9", manifest=manifest, pr_number=number
        )

        assert outcome2.state == "reconciliation_required"
        settlement = acceptance.settlements.get(record.review_id)
        assert settlement.state == "failed"
        assert settlement.merge_sha is None
        assert acceptance.github.merge_calls == []
        assert (
            acceptance.database.scalar("SELECT COUNT(*) FROM github_merge_effects")
            == 0
        )

    async def test_guarded_merge_threads_base_sha_into_the_proof(
        self, acceptance: Any
    ) -> None:
        """(e) guarded path: patch equivalence only succeeds because the
        settlement's recorded base_sha reaches ``merge_approved``."""

        event, branch, number = acceptance.prepare("ENG-9", GOOD, pr_number=14)
        merge_sha = merge_sha_for(GOOD)
        _wire_patch_equivalence(acceptance, GOOD, merge_sha)
        admitted = acceptance.admission.admit("demo", event, received_generation=1)

        outcome = await acceptance.service.complete_review(
            admitted, "ENG-9", verdict_for(branch, number)
        )

        assert outcome.state == "merged"
        payload = _merge_proven_payload(acceptance, outcome.review_id)
        assert payload["relation"] == "patch_equivalent"
        assert payload["base_sha"] == BASE
        assert payload["merge_parent_sha"] == PARENT_SHA

    async def test_direct_sol_merge_threads_base_sha_into_the_proof(
        self, acceptance: Any
    ) -> None:
        """(e) direct-Sol path: ``prove_landed`` inside ``_drive_merge``'s
        direct-merge branch is given the settlement's recorded base_sha."""

        event, branch, number = acceptance.prepare("ENG-9", GOOD, pr_number=14)
        merge_sha = merge_sha_for(GOOD)
        _wire_patch_equivalence(acceptance, GOOD, merge_sha)
        admitted = acceptance.admission.admit("demo", event, received_generation=1)
        record = await acceptance.service.record_verdict(
            admitted, "ENG-9", verdict_for(branch, number)
        )
        acceptance.github.full_pulls[number] = open_pull(
            number=number,
            head_sha=GOOD,
            head_ref=branch,
            state="closed",
            merged=True,
            mergeable=None,
            merge_commit_sha=merge_sha,
        )

        outcome = await acceptance.service.merge_approved(record.review_id)

        assert outcome.state == "merged"
        assert "externally merged; reconciled patch_equivalent" in outcome.reason
        assert acceptance.github.merge_calls == []
        payload = _merge_proven_payload(acceptance, record.review_id)
        assert payload["relation"] == "patch_equivalent"
        assert payload["base_sha"] == BASE
        assert payload["merge_parent_sha"] == PARENT_SHA


@pytest.mark.asyncio
class TestAcceptanceGatedSettlement:
    """INFRA-198 J1: a merge records implementation completion, not
    operator acceptance. A pending acceptance gate holds the settled
    issue in ``post_merge_acceptance``, projects In Development to the
    operator, and dispatches exactly one durable acceptance assignment;
    replays and crash-resumes stay exactly-once."""

    def _gate(self, acceptance: Any, *, seat: bool = True) -> None:
        if seat:
            acceptance.seat_cell()
        acceptance.gates.require(
            "ENG-9",
            instruction_id="chat-accept-eng-9",
            predicates=("live_smoke",),
        )

    @staticmethod
    def _assignments(acceptance: Any) -> list[Any]:
        return acceptance.database.execute(
            "SELECT * FROM lead_assignments WHERE issue_id = 'ENG-9' "
            "ORDER BY rowid"
        ).fetchall()

    @staticmethod
    def _hold_transitions(acceptance: Any) -> int:
        return int(
            acceptance.database.scalar(
                "SELECT COUNT(*) FROM events WHERE event_type = "
                "'issue.transitioned' AND aggregate_id = 'ENG-9' AND "
                "payload_json LIKE '%\"to\":\"post_merge_acceptance\"%'"
            )
        )

    def _assert_held_exactly_once(self, acceptance: Any) -> None:
        assert (
            acceptance.queue.get("ENG-9").state
            is IssueState.POST_MERGE_ACCEPTANCE
        )
        assert acceptance.linear.targets == [
            ("ENG-9", "Review", "operator"),
            ("ENG-9", "In Development", "operator"),
        ]
        rows = self._assignments(acceptance)
        assert len(rows) == 1
        assert rows[0]["state"] == "published"
        assert rows[0]["instruction_id"] == "chat-accept-eng-9"
        assert rows[0]["queue_transition"] == "review->post_merge_acceptance"
        assert self._hold_transitions(acceptance) == 1

    async def test_a_gated_merge_holds_and_dispatches_exactly_once(
        self, acceptance: Any
    ) -> None:
        self._gate(acceptance)

        outcome = await acceptance.submit("ENG-9", GOOD)

        assert outcome.state == "merged"
        assert acceptance.settlements.get("review:demo:evt-1").state == "settled"
        self._assert_held_exactly_once(acceptance)
        # A settled replay changes nothing: one transition, one Linear
        # effect, one durable assignment.
        replay = await acceptance.service.merge_approved(outcome.review_id)
        assert replay.state == "merged"
        assert len(acceptance.github.merge_calls) == 1
        self._assert_held_exactly_once(acceptance)

    async def test_a_crash_before_the_ledger_resumes_into_one_gated_hold(
        self, acceptance: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The pinned post-merge-boundary crash, gated: the resume applies
        # the acceptance hold instead of Done, exactly once.
        self._gate(acceptance)
        await crashed_in_flight_merge(acceptance, monkeypatch)
        expire_settlement_lease(acceptance)

        [outcome] = await acceptance.service.resume_settlements("demo")

        assert outcome.state == "merged"
        assert len(acceptance.github.merge_calls) == 1
        assert acceptance.settlements.get("review:demo:evt-1").state == "settled"
        self._assert_held_exactly_once(acceptance)

    async def test_a_pre_settle_crash_projects_nothing_and_resumes_once(
        self, acceptance: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Sol Critical a626cf1f: the settlement's ``mark_merged`` durably
        # persists (state 'merged'), the lead ACKNOWLEDGES the dispatched
        # acceptance assignment, and only then does the process crash
        # before ``mark_settled``. On resume, ``merge_approved`` re-claims
        # the 'merged' row and ``_drive_merge`` takes the already-merged
        # replay branch straight into ``_project_after_merge`` ->
        # ``_hold_for_acceptance`` -> ``_dispatch_acceptance_assignment``
        # — which must dedup against the acknowledged row instead of
        # opening a fresh dispatch epoch that supersedes it.
        self._gate(acceptance)
        original_mark_settled = acceptance.settlements.mark_settled
        calls = {"count": 0}

        def crash_before_settle(settlement_id: str, *, token: str) -> None:
            # INFRA-218 (Sol correction 44eb2806): the projection now runs
            # only AFTER the settlement is terminal, so at this point
            # nothing has been published yet — the ack-before-settle
            # interleaving this hook used to construct no longer exists,
            # which is exactly the stranding window the fix removes.
            calls["count"] += 1
            if calls["count"] == 1:
                assert self._assignments(acceptance) == []
                raise RuntimeError("simulated crash before mark_settled")
            original_mark_settled(settlement_id, token=token)

        monkeypatch.setattr(
            acceptance.settlements, "mark_settled", crash_before_settle
        )
        with pytest.raises(RuntimeError, match="simulated crash"):
            await acceptance.submit("ENG-9", GOOD)

        # INFRA-218 (Sol correction 44eb2806): the post-merge projection
        # no longer runs while the merge lease is open — the settlement
        # reaches its terminal state FIRST and the projection is separate
        # recoverable work. So at this crash point the merge is proven but
        # nothing has been projected yet: no assignment exists, which is
        # precisely why an interruption here can no longer strand anything.
        assert acceptance.settlements.get("review:demo:evt-1").state == "merged"
        assert self._assignments(acceptance) == []

        [first] = await acceptance.service.resume_settlements("demo")
        assert first.state == "merged"
        assert acceptance.settlements.get("review:demo:evt-1").state == "settled"
        for _ in range(2):
            # Now terminal: further resumes find nothing to drive and
            # change nothing.
            assert await acceptance.service.resume_settlements("demo") == ()
        # The resume settles the merge AND applies the hold exactly once:
        # one assignment, never a duplicate or a superseded predecessor.
        rows = self._assignments(acceptance)
        assert len(rows) == 1
        assert int(
            acceptance.database.scalar(
                "SELECT COUNT(*) FROM events "
                "WHERE event_type = 'assignment.superseded'"
            )
        ) == 0
        assert (
            acceptance.queue.get("ENG-9").state
            is IssueState.POST_MERGE_ACCEPTANCE
        )
        assert acceptance.linear.targets == [
            ("ENG-9", "Review", "operator"),
            ("ENG-9", "In Development", "operator"),
        ]
        assert self._hold_transitions(acceptance) == 1

    async def test_a_crash_inside_the_hold_projection_repairs_without_stranding(
        self, acceptance: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # INFRA-218 (S3): before this packet, ``_settle_proven``'s call
        # into ``_project_after_merge`` (here, ``_hold_for_acceptance``'s
        # Linear effect) was unguarded on the FRESH merge path, so this
        # exact failure propagated out of ``submit`` and left the
        # settlement stuck 'merging' until a resumed drive replayed the
        # hold. Now the durable queue hold + assignment (both already
        # written before the Linear call) and the settlement's own
        # 'settled' convergence never depend on that projection: the
        # pass still settles, and the missed 'acceptance-hold' effect is
        # repaired by ``reconcile_acceptance`` at the very next resume
        # boundary — dedup-aware, so it lands exactly once, not twice.
        self._gate(acceptance)
        original = acceptance.linear.project
        calls = {"count": 0}

        async def crash_once(issue_id: str, target: Any, effect_id: str) -> Any:
            if "acceptance-hold" in effect_id:
                calls["count"] += 1
                if calls["count"] == 1:
                    raise RuntimeError("simulated crash before the projection")
            return await original(issue_id, target, effect_id)

        monkeypatch.setattr(acceptance.linear, "project", crash_once)

        outcome = await acceptance.submit("ENG-9", GOOD)

        assert outcome.state == "merged"
        assert acceptance.settlements.get("review:demo:evt-1").state == "settled"
        assert (
            acceptance.queue.get("ENG-9").state
            is IssueState.POST_MERGE_ACCEPTANCE
        )
        assert len(self._assignments(acceptance)) == 1
        # The failed effect never landed on this pass.
        assert ("ENG-9", "In Development", "operator") not in acceptance.linear.targets

        resumed = await acceptance.service.resume_settlements("demo")

        assert resumed == ()
        assert len(acceptance.github.merge_calls) == 1
        self._assert_held_exactly_once(acceptance)

    async def test_without_a_live_cell_the_issue_still_holds(
        self, acceptance: Any
    ) -> None:
        # No live project cell: the assignment is skipped (packet K's
        # reconciliation re-derives it) but the hold still applies.
        self._gate(acceptance, seat=False)

        outcome = await acceptance.submit("ENG-9", GOOD)

        assert outcome.state == "merged"
        assert (
            acceptance.queue.get("ENG-9").state
            is IssueState.POST_MERGE_ACCEPTANCE
        )
        assert acceptance.linear.targets[-1] == (
            "ENG-9",
            "In Development",
            "operator",
        )
        assert self._assignments(acceptance) == []


@pytest.mark.asyncio
class TestAcceptanceReconciliation:
    """INFRA-198 packet K (Sol 524a38ed finding 2): the durable
    ``acceptance_gates`` table is read back at every recovery boundary.
    ``ReviewService.reconcile_acceptance`` rides ``resume_settlements``
    — the exact pass daemon startup, the intake-boundary resumes, and
    ``merge-settle --project`` already invoke — and idempotently repairs
    a premature Done, a missing acceptance assignment, and a
    satisfied-but-never-completed gate."""

    @staticmethod
    def _assignments(acceptance: Any) -> list[Any]:
        return acceptance.database.execute(
            "SELECT * FROM lead_assignments WHERE issue_id = 'ENG-9' "
            "ORDER BY rowid"
        ).fetchall()

    @staticmethod
    def _transition_count(acceptance: Any, to_state: str) -> int:
        return int(
            acceptance.database.scalar(
                "SELECT COUNT(*) FROM events WHERE event_type = "
                "'issue.transitioned' AND aggregate_id = 'ENG-9' AND "
                f"payload_json LIKE '%\"to\":\"{to_state}\"%'"
            )
        )

    def _gate(self, acceptance: Any) -> None:
        acceptance.gates.require(
            "ENG-9",
            instruction_id="chat-accept-eng-9",
            predicates=("live_smoke",),
        )

    async def test_reconciliation_repairs_a_premature_done_to_the_hold(
        self, acceptance: Any
    ) -> None:
        # Required test 2: an ungated merge completed to Done; only then
        # does the operator require acceptance — a pending gate beside a
        # Done issue with a proven merged review. Restart reconciliation
        # repairs the hold: post_merge_acceptance, the In Development
        # Linear effect, and exactly one acceptance assignment.
        acceptance.seat_cell()
        outcome = await acceptance.submit("ENG-9", GOOD)
        assert outcome.state == "merged"
        assert acceptance.queue.get("ENG-9").state is IssueState.DONE
        self._gate(acceptance)

        assert await acceptance.service.resume_settlements("demo") == ()

        assert (
            acceptance.queue.get("ENG-9").state
            is IssueState.POST_MERGE_ACCEPTANCE
        )
        assert acceptance.linear.targets[-1] == (
            "ENG-9",
            "In Development",
            "operator",
        )
        rows = self._assignments(acceptance)
        assert len(rows) == 1
        assert rows[0]["state"] == "published"
        assert rows[0]["instruction_id"] == "chat-accept-eng-9"
        assert rows[0]["queue_transition"] == "done->post_merge_acceptance"
        assert self._transition_count(acceptance, "post_merge_acceptance") == 1
        targets_after_repair = list(acceptance.linear.targets)

        # Idempotent, exactly-once: a second recovery pass repeats
        # nothing.
        await acceptance.service.resume_settlements("demo")
        assert len(self._assignments(acceptance)) == 1
        assert self._transition_count(acceptance, "post_merge_acceptance") == 1
        assert acceptance.linear.targets == targets_after_repair

    async def test_a_cell_appearing_later_gets_exactly_one_assignment(
        self, acceptance: Any
    ) -> None:
        # Required test 3: a gated merge settled while NO live cell
        # existed — zero assignments at settlement time. Reconciliation
        # publishes exactly one assignment once a cell appears.
        self._gate(acceptance)
        outcome = await acceptance.submit("ENG-9", GOOD)
        assert outcome.state == "merged"
        assert (
            acceptance.queue.get("ENG-9").state
            is IssueState.POST_MERGE_ACCEPTANCE
        )
        assert self._assignments(acceptance) == []

        # Recovery with still no cell: the hold stays, no assignment.
        assert await acceptance.service.resume_settlements("demo") == ()
        assert self._assignments(acceptance) == []

        acceptance.seat_cell()
        assert await acceptance.service.resume_settlements("demo") == ()

        rows = self._assignments(acceptance)
        assert len(rows) == 1
        assert rows[0]["state"] == "published"
        assert rows[0]["cell_id"] == "cell-demo"
        assert (
            rows[0]["session_id"] == "11111111-1111-4111-8111-111111111111"
        )
        assert rows[0]["instruction_id"] == "chat-accept-eng-9"

        # Exactly once across replays.
        await acceptance.service.resume_settlements("demo")
        assert len(self._assignments(acceptance)) == 1

    async def test_satisfaction_crash_converges_to_done_exactly_once(
        self, acceptance: Any
    ) -> None:
        # Required test 4: the gate satisfaction persisted, then the
        # process died before BOTH the queue transition and the Linear
        # projection. Restart reconciliation completes the issue to Done
        # with the satisfy path's exact effect-id convention — once.
        acceptance.seat_cell()
        self._gate(acceptance)
        outcome = await acceptance.submit("ENG-9", GOOD)
        assert outcome.state == "merged"
        assert (
            acceptance.queue.get("ENG-9").state
            is IssueState.POST_MERGE_ACCEPTANCE
        )

        acceptance.gates.satisfy("ENG-9", evidence={"live_smoke": "receipt-1"})
        # The crash point: nothing after satisfaction ran.
        assert (
            acceptance.queue.get("ENG-9").state
            is IssueState.POST_MERGE_ACCEPTANCE
        )
        assert not any(
            target[1] == "Done" for target in acceptance.linear.targets
        )

        assert await acceptance.service.resume_settlements("demo") == ()

        assert acceptance.queue.get("ENG-9").state is IssueState.DONE
        done_targets = [
            target for target in acceptance.linear.targets if target[1] == "Done"
        ]
        assert done_targets == [("ENG-9", "Done", "operator")]
        assert "linear:ENG-9:acceptance-satisfied:chat-accept-eng-9" in (
            acceptance.linear.effect_ids
        )
        assert self._transition_count(acceptance, "done") == 1

        # Replays converge with no second transition or projection.
        await acceptance.service.resume_settlements("demo")
        assert self._transition_count(acceptance, "done") == 1
        assert [
            target for target in acceptance.linear.targets if target[1] == "Done"
        ] == [("ENG-9", "Done", "operator")]

    async def test_reconciliation_never_replaces_an_acknowledged_assignment(
        self, acceptance: Any
    ) -> None:
        # Sol 04d013b0 finding 1 (required test 1): the gated merge
        # published the acceptance assignment and the lead ACKNOWLEDGED
        # it. Recovery replays of the hold must treat that acknowledged
        # row as already-dispatched — publish_in's consumed-epoch
        # supersession is for genuine re-queues, not reconciliation —
        # so repeated restart/maintenance passes leave exactly one
        # assignment row and no replacement packet.
        acceptance.seat_cell()
        self._gate(acceptance)
        outcome = await acceptance.submit("ENG-9", GOOD)
        assert outcome.state == "merged"
        [published] = self._assignments(acceptance)
        assert published["state"] == "published"
        assert acceptance.assignments.acknowledge(
            str(published["assignment_id"]),
            session_id="11111111-1111-4111-8111-111111111111",
        )

        for _ in range(3):
            await acceptance.service.resume_settlements("demo")

        rows = self._assignments(acceptance)
        assert len(rows) == 1
        assert rows[0]["assignment_id"] == published["assignment_id"]
        assert rows[0]["state"] == "acknowledged"
        assert int(
            acceptance.database.scalar(
                "SELECT COUNT(*) FROM events "
                "WHERE event_type = 'assignment.superseded'"
            )
        ) == 0
        assert self._transition_count(acceptance, "post_merge_acceptance") == 1

    async def test_a_late_gate_on_a_qa_routed_merge_reaches_the_hold(
        self, acceptance: Any
    ) -> None:
        # Sol 04d013b0 finding 2, option (a) (required test 2): the
        # merge routed to QA before any gate existed; the operator then
        # requires acceptance. The next recovery boundary reconciles the
        # QA-routed issue into the acceptance hold — the ("QA", "In
        # Development") Linear pair is already allowed (the qa_reject
        # path projects it) — and the gate is then satisfiable through
        # the ordinary completion path.
        acceptance.seat_cell()
        outcome = await acceptance.submit(
            "ENG-9", GOOD, qa_origin="ryan_assigned"
        )
        assert outcome.state == "merged"
        assert acceptance.queue.get("ENG-9").state is IssueState.QA
        assert acceptance.linear.targets[-1] == ("ENG-9", "QA", "ryan")
        self._gate(acceptance)

        assert await acceptance.service.resume_settlements("demo") == ()

        assert (
            acceptance.queue.get("ENG-9").state
            is IssueState.POST_MERGE_ACCEPTANCE
        )
        assert acceptance.linear.targets[-1] == (
            "ENG-9",
            "In Development",
            "operator",
        )
        rows = self._assignments(acceptance)
        assert len(rows) == 1
        assert rows[0]["state"] == "published"
        assert rows[0]["queue_transition"] == "qa->post_merge_acceptance"
        assert self._transition_count(acceptance, "post_merge_acceptance") == 1

        # Deterministic and exactly-once: another pass repeats nothing.
        await acceptance.service.resume_settlements("demo")
        assert len(self._assignments(acceptance)) == 1
        assert self._transition_count(acceptance, "post_merge_acceptance") == 1

        # The held gate is satisfiable: satisfaction plus the next
        # recovery boundary completes the issue exactly once.
        acceptance.gates.satisfy("ENG-9", evidence={"live_smoke": "receipt-1"})
        await acceptance.service.resume_settlements("demo")
        assert acceptance.queue.get("ENG-9").state is IssueState.DONE
        assert [
            target for target in acceptance.linear.targets if target[1] == "Done"
        ] == [("ENG-9", "Done", "operator")]
        assert self._transition_count(acceptance, "done") == 1

    async def test_a_gate_on_unmerged_work_never_advances(
        self, acceptance: Any
    ) -> None:
        # No proven merged review: the reconciler advances nothing, even
        # for a satisfied gate.
        acceptance.queue.admit(
            AdmissionRequest(
                issue_id="ENG-9",
                project_key="demo",
                linear_priority=1,
                admitted_by="operator",
                instruction_id="chat-ENG-9",
            )
        )
        self._gate(acceptance)
        acceptance.gates.satisfy("ENG-9", evidence={"live_smoke": "receipt-1"})

        repairs = await acceptance.service.reconcile_acceptance("demo")

        assert repairs == ()
        assert acceptance.queue.get("ENG-9").state is IssueState.QUEUED
        assert acceptance.linear.targets == []
        assert self._assignments(acceptance) == []
