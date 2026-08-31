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
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.github import MergeResult
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
                "pr_number": number,
                "reviewed_sha": sha,
                "packets": [],
            }
        ),
        expected=VerdictBinding(
            repository="j-paterson/demo",
            branch=branch,
            pr_number=number,
            reviewed_sha=sha,
        ),
    )


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
        assert acceptance.settlements.get("review:demo:evt-1").state == "merging"
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

    @pytest.mark.parametrize(
        ("mutation", "error_type", "fragment"),
        [
            ({"head_sha": THIRD}, ValueError, "not the reviewed candidate"),
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
