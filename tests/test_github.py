"""Verify deterministic GitHub pull-request reads and compare-and-merge."""

from __future__ import annotations

import threading
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.github import (
    DiscoveredPull,
    GitHubClient,
    GitHubError,
    GitHubResponse,
    MergeAmbiguous,
    MergeBlocked,
    MergeEffectJournal,
    MergeInFlight,
    MergeRaceLost,
)
from hermes_orchestrator.linear import ExternalEffectStore

REPOSITORY = "j-paterson/demo"
HEAD = "1" * 40
OLD_HEAD = "2" * 40
MERGE_SHA = "3" * 40
PULLS_PATH = "/repos/j-paterson/demo/pulls/14"
MERGE_PATH = "/repos/j-paterson/demo/pulls/14/merge"
LIST_PATH = "/repos/j-paterson/demo/pulls"
EFFECT_ID = "github:demo:merge:14:" + HEAD

MERGE_REQUEST = {
    "repository": REPOSITORY,
    "number": 14,
    "sha": HEAD,
    "head_ref": "feature/eng-9",
    "base": "main",
    "merge_method": "squash",
}

NOW = datetime(2026, 8, 28, tzinfo=UTC)


def pull_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "number": 14,
        "state": "open",
        "draft": False,
        "merged": False,
        "mergeable": True,
        "merge_commit_sha": None,
        "head": {
            "sha": HEAD,
            "ref": "feature/eng-9",
            "repo": {"full_name": REPOSITORY},
        },
        "base": {
            "ref": "main",
            "repo": {"full_name": REPOSITORY},
        },
    }
    payload.update(overrides)
    return payload


def list_payload(**overrides: Any) -> dict[str, Any]:
    """One realistic pull-request-simple list entry.

    GitHub's list schema omits ``merged`` and ``mergeable`` entirely; the
    summary parser must accept this shape and never require merge-decision
    fields from it.
    """

    payload: dict[str, Any] = {
        "number": 14,
        "state": "open",
        "draft": False,
        "locked": False,
        "merged_at": None,
        "auto_merge": None,
        "merge_commit_sha": "9" * 40,
        "head": {
            "sha": HEAD,
            "ref": "feature/eng-9",
            "repo": {"full_name": REPOSITORY},
        },
        "base": {
            "ref": "main",
            "repo": {"full_name": REPOSITORY},
        },
    }
    payload.update(overrides)
    return payload


@dataclass
class FakeTransport:
    """Recording transport returning queued responses per method and path.

    A queued Exception is raised instead of returned, modeling an ambiguous
    network failure whose mutation outcome is unknown.
    """

    responses: dict[tuple[str, str], list[GitHubResponse | Exception]] = field(
        default_factory=dict
    )
    calls: list[tuple[str, str, Mapping[str, str] | None, Any]] = field(
        default_factory=list
    )

    def queue(
        self, method: str, path: str, response: GitHubResponse | Exception
    ) -> None:
        self.responses.setdefault((method, path), []).append(response)

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, str] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> GitHubResponse:
        self.calls.append((method, path, query, body))
        queued = self.responses.get((method, path))
        if not queued:
            raise AssertionError(f"unexpected GitHub call {method} {path}")
        item = queued.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def merge_calls(self) -> list[Any]:
        return [call for call in self.calls if call[0] == "PUT"]


@dataclass
class Clock:
    """Deterministic advancing clock for lease expiry tests."""

    value: datetime

    def now(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value = self.value + timedelta(seconds=seconds)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def transport() -> FakeTransport:
    return FakeTransport()


@pytest.fixture
def clock() -> Clock:
    return Clock(NOW)


@pytest.fixture
def journal(database: Database, clock: Clock) -> MergeEffectJournal:
    return MergeEffectJournal(database, now=clock.now, lease_seconds=300)


@pytest.fixture
def github(
    transport: FakeTransport, journal: MergeEffectJournal
) -> GitHubClient:
    return GitHubClient(transport=transport, journal=journal)


def merge(github: GitHubClient, **overrides: Any) -> Any:
    arguments: dict[str, Any] = {
        "repository": REPOSITORY,
        "number": 14,
        "expected_head_sha": HEAD,
        "expected_head_ref": "feature/eng-9",
        "expected_base": "main",
        "effect_id": EFFECT_ID,
    }
    arguments.update(overrides)
    repository = arguments.pop("repository")
    number = arguments.pop("number")
    return github.merge(repository, number, **arguments)


def test_get_pull_request_parses_validated_fields(
    github: GitHubClient, transport: FakeTransport
) -> None:
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    pull = github.get_pull_request(REPOSITORY, 14)
    assert pull.number == 14
    assert pull.head_sha == HEAD
    assert pull.head_ref == "feature/eng-9"
    assert pull.base_ref == "main"
    assert pull.merged is False
    assert transport.calls == [("GET", PULLS_PATH, None, None)]


def test_get_pull_request_rejects_stale_number_echo(
    github: GitHubClient, transport: FakeTransport
) -> None:
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload(number=15)))
    with pytest.raises(GitHubError, match="stale"):
        github.get_pull_request(REPOSITORY, 14)


def test_get_pull_request_rejects_foreign_base_repository(
    github: GitHubClient, transport: FakeTransport
) -> None:
    payload = pull_payload(
        base={"ref": "main", "repo": {"full_name": "someone/else"}}
    )
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, payload))
    with pytest.raises(GitHubError, match="repository"):
        github.get_pull_request(REPOSITORY, 14)


def test_get_pull_request_rejects_invalid_head_sha(
    github: GitHubClient, transport: FakeTransport
) -> None:
    payload = pull_payload()
    payload["head"]["sha"] = "not-a-sha"
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, payload))
    with pytest.raises(GitHubError, match="head"):
        github.get_pull_request(REPOSITORY, 14)


def test_get_pull_request_rejects_non_200(
    github: GitHubClient, transport: FakeTransport
) -> None:
    transport.queue("GET", PULLS_PATH, GitHubResponse(404, {"message": "gone"}))
    with pytest.raises(GitHubError, match="404"):
        github.get_pull_request(REPOSITORY, 14)


def test_list_open_pulls_parses_realistic_summaries(
    github: GitHubClient, transport: FakeTransport
) -> None:
    """The official list schema has no merged/mergeable fields at all."""

    transport.queue("GET", LIST_PATH, GitHubResponse(200, [list_payload()]))
    pulls = github.list_open_pulls(REPOSITORY, base="main")
    assert len(pulls) == 1
    summary = pulls[0]
    assert summary.number == 14
    assert summary.head_sha == HEAD
    assert summary.head_ref == "feature/eng-9"
    assert summary.base_ref == "main"
    assert transport.calls == [
        ("GET", LIST_PATH, {"state": "open", "base": "main", "per_page": "100"}, None)
    ]


def test_list_open_pulls_rejects_invalid_summary_head(
    github: GitHubClient, transport: FakeTransport
) -> None:
    payload = list_payload()
    payload["head"]["sha"] = "not-a-sha"
    transport.queue("GET", LIST_PATH, GitHubResponse(200, [payload]))
    with pytest.raises(GitHubError, match="head"):
        github.list_open_pulls(REPOSITORY, base="main")


def test_list_open_pulls_rejects_mismatched_base_echo(
    github: GitHubClient, transport: FakeTransport
) -> None:
    payload = list_payload(
        base={"ref": "develop", "repo": {"full_name": REPOSITORY}}
    )
    transport.queue("GET", LIST_PATH, GitHubResponse(200, [payload]))
    with pytest.raises(GitHubError, match="stale"):
        github.list_open_pulls(REPOSITORY, base="main")


def test_discover_pull_request_returns_open_match(
    github: GitHubClient, transport: FakeTransport
) -> None:
    transport.queue("GET", LIST_PATH, GitHubResponse(200, [list_payload()]))
    discovered = github.discover_pull_request(
        REPOSITORY, branch="feature/eng-9", head_sha=HEAD
    )
    assert discovered == DiscoveredPull(
        number=14,
        state="open",
        merged=False,
        head_sha=HEAD,
        merge_sha=None,
        repository=REPOSITORY,
        head_repository=REPOSITORY,
        base_ref="main",
    )
    assert transport.calls == [
        (
            "GET",
            LIST_PATH,
            {
                "state": "all",
                "head": "j-paterson:feature/eng-9",
                "per_page": "100",
            },
            None,
        )
    ]


def test_discover_pull_request_returns_merged_match_with_merge_sha(
    github: GitHubClient, transport: FakeTransport
) -> None:
    """A closed exact-head match is re-read in full to fill merge_sha."""

    transport.queue(
        "GET", LIST_PATH, GitHubResponse(200, [list_payload(state="closed")])
    )
    transport.queue(
        "GET",
        PULLS_PATH,
        GitHubResponse(
            200,
            pull_payload(state="closed", merged=True, merge_commit_sha=MERGE_SHA),
        ),
    )
    discovered = github.discover_pull_request(
        REPOSITORY, branch="feature/eng-9", head_sha=HEAD
    )
    assert discovered == DiscoveredPull(
        number=14,
        state="closed",
        merged=True,
        head_sha=HEAD,
        merge_sha=MERGE_SHA,
        repository=REPOSITORY,
        head_repository=REPOSITORY,
        base_ref="main",
    )


def test_discover_pull_request_returns_closed_unmerged_match(
    github: GitHubClient, transport: FakeTransport
) -> None:
    transport.queue(
        "GET", LIST_PATH, GitHubResponse(200, [list_payload(state="closed")])
    )
    transport.queue(
        "GET",
        PULLS_PATH,
        GitHubResponse(200, pull_payload(state="closed", merged=False)),
    )
    discovered = github.discover_pull_request(
        REPOSITORY, branch="feature/eng-9", head_sha=HEAD
    )
    assert discovered == DiscoveredPull(
        number=14,
        state="closed",
        merged=False,
        head_sha=HEAD,
        merge_sha=None,
        repository=REPOSITORY,
        head_repository=REPOSITORY,
        base_ref="main",
    )


def test_discover_pull_request_returns_none_for_no_match(
    github: GitHubClient, transport: FakeTransport
) -> None:
    transport.queue("GET", LIST_PATH, GitHubResponse(200, []))
    assert (
        github.discover_pull_request(
            REPOSITORY, branch="feature/eng-9", head_sha=HEAD
        )
        is None
    )


def test_discover_pull_request_returns_none_for_different_head_sha(
    github: GitHubClient, transport: FakeTransport
) -> None:
    """Same branch, different SHA: not a match even though the branch is."""

    mismatched = list_payload(
        head={
            "sha": OLD_HEAD,
            "ref": "feature/eng-9",
            "repo": {"full_name": REPOSITORY},
        }
    )
    transport.queue("GET", LIST_PATH, GitHubResponse(200, [mismatched]))
    assert (
        github.discover_pull_request(
            REPOSITORY, branch="feature/eng-9", head_sha=HEAD
        )
        is None
    )


def test_discover_pull_request_raises_on_two_exact_matches(
    github: GitHubClient, transport: FakeTransport
) -> None:
    first = list_payload(number=14)
    second = list_payload(number=15)
    transport.queue("GET", LIST_PATH, GitHubResponse(200, [first, second]))
    with pytest.raises(MergeAmbiguous, match="more than one"):
        github.discover_pull_request(
            REPOSITORY, branch="feature/eng-9", head_sha=HEAD
        )


def test_merge_refuses_changed_head(
    github: GitHubClient, transport: FakeTransport
) -> None:
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    with pytest.raises(MergeBlocked, match="head changed"):
        merge(github, expected_head_sha=OLD_HEAD)
    assert transport.merge_calls() == []


@pytest.mark.parametrize(
    "overrides,reason",
    [
        ({"state": "closed"}, "not open"),
        ({"draft": True}, "draft"),
        ({"mergeable": False}, "mergeable"),
        ({"mergeable": None}, "mergeable"),
        (
            {"base": {"ref": "develop", "repo": {"full_name": REPOSITORY}}},
            "base",
        ),
        (
            {
                "head": {
                    "sha": HEAD,
                    "ref": "feature/other",
                    "repo": {"full_name": REPOSITORY},
                }
            },
            "branch",
        ),
        (
            {
                "head": {
                    "sha": HEAD,
                    "ref": "feature/eng-9",
                    "repo": {"full_name": "fork/demo"},
                }
            },
            "repository",
        ),
    ],
)
def test_merge_fails_closed_on_wrong_pull_identity(
    github: GitHubClient,
    transport: FakeTransport,
    overrides: dict[str, Any],
    reason: str,
) -> None:
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload(**overrides)))
    with pytest.raises(MergeBlocked, match=reason):
        merge(github)
    assert transport.merge_calls() == []


def test_merge_sends_exact_expected_sha_and_method(
    github: GitHubClient, transport: FakeTransport
) -> None:
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue(
        "PUT", MERGE_PATH, GitHubResponse(200, {"merged": True, "sha": MERGE_SHA})
    )
    result = merge(github)
    assert result.merge_sha == MERGE_SHA
    assert result.already_merged is False
    assert transport.merge_calls() == [
        ("PUT", MERGE_PATH, None, {"sha": HEAD, "merge_method": "squash"})
    ]


def test_merge_race_conflict_fails_closed(
    github: GitHubClient, transport: FakeTransport
) -> None:
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue(
        "PUT", MERGE_PATH, GitHubResponse(409, {"message": "Head branch was modified"})
    )
    with pytest.raises(MergeRaceLost, match="head"):
        merge(github)


def test_merge_405_not_mergeable_fails_closed(
    github: GitHubClient, transport: FakeTransport
) -> None:
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue(
        "PUT", MERGE_PATH, GitHubResponse(405, {"message": "Not mergeable"})
    )
    with pytest.raises(MergeBlocked, match="405"):
        merge(github)


def test_definitive_merged_false_releases_for_safe_retry(
    github: GitHubClient,
    transport: FakeTransport,
    journal: MergeEffectJournal,
) -> None:
    """A structurally valid merged=false proves no mutation happened."""

    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue(
        "PUT", MERGE_PATH, GitHubResponse(200, {"merged": False, "sha": MERGE_SHA})
    )
    with pytest.raises(GitHubError, match="no merge"):
        merge(github)
    effect = journal.get(EFFECT_ID)
    assert effect.state == "pending"
    assert effect.claim_token is None
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue(
        "PUT", MERGE_PATH, GitHubResponse(200, {"merged": True, "sha": MERGE_SHA})
    )
    result = merge(github)
    assert result.merge_sha == MERGE_SHA
    assert len(transport.merge_calls()) == 2


@pytest.mark.parametrize(
    "payload",
    [
        {"merged": True, "sha": "not-a-sha"},
        {"merged": True},
        {"sha": MERGE_SHA},
        {"merged": "yes", "sha": MERGE_SHA},
        ["merged"],
        "merged",
        None,
    ],
)
def test_unprovable_200_success_stays_fenced_as_ambiguous(
    github: GitHubClient,
    transport: FakeTransport,
    journal: MergeEffectJournal,
    payload: Any,
) -> None:
    """A 200 that cannot prove commit identity may conceal a mutation."""

    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue("PUT", MERGE_PATH, GitHubResponse(200, payload))
    with pytest.raises(MergeAmbiguous, match="reconciliation"):
        merge(github)
    effect = journal.get(EFFECT_ID)
    assert effect.state == "attempted"
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    with pytest.raises(MergeAmbiguous):
        merge(github)
    assert len(transport.merge_calls()) == 1


def test_unknown_status_keeps_the_mutation_fence(
    github: GitHubClient,
    transport: FakeTransport,
    journal: MergeEffectJournal,
) -> None:
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue("PUT", MERGE_PATH, GitHubResponse(500, {"message": "oops"}))
    with pytest.raises(GitHubError, match="500"):
        merge(github)
    assert journal.get(EFFECT_ID).state == "attempted"


def test_completed_effect_replays_without_second_mutation(
    github: GitHubClient, transport: FakeTransport
) -> None:
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue(
        "PUT", MERGE_PATH, GitHubResponse(200, {"merged": True, "sha": MERGE_SHA})
    )
    first = merge(github)
    second = merge(github)
    assert second.merge_sha == first.merge_sha == MERGE_SHA
    assert second.already_merged is True
    assert len(transport.merge_calls()) == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {"repository": "j-paterson/other", "number": 14},
        {"number": 15},
        {"expected_head_sha": OLD_HEAD},
        {"expected_head_ref": "feature/other"},
        {"expected_base": "develop"},
        {"merge_method": "merge"},
    ],
)
def test_completed_replay_conflicts_on_any_changed_binding_field(
    github: GitHubClient, transport: FakeTransport, overrides: dict[str, Any]
) -> None:
    """Same effect id with any changed immutable field must never replay."""

    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue(
        "PUT", MERGE_PATH, GitHubResponse(200, {"merged": True, "sha": MERGE_SHA})
    )
    merge(github)
    calls_before = len(transport.calls)
    with pytest.raises(GitHubError, match="effect"):
        merge(github, **overrides)
    assert len(transport.calls) == calls_before


def test_pending_replay_conflicts_on_changed_branch_with_same_sha(
    github: GitHubClient,
    transport: FakeTransport,
    journal: MergeEffectJournal,
) -> None:
    journal.claim(EFFECT_ID, request=MERGE_REQUEST)
    with pytest.raises(GitHubError, match="effect"):
        merge(github, expected_head_ref="feature/other")
    assert transport.calls == []


def test_pending_intent_then_foreign_merged_pull_is_ambiguous(
    github: GitHubClient,
    transport: FakeTransport,
    journal: MergeEffectJournal,
) -> None:
    """A pre-send pending intent is never proof this journal merged the PR."""

    journal.claim(EFFECT_ID, request=MERGE_REQUEST)
    payload = pull_payload(merged=True, state="closed", merge_commit_sha=MERGE_SHA)
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, payload))
    with pytest.raises(MergeAmbiguous, match="reconciliation"):
        merge(github)
    assert transport.merge_calls() == []
    assert journal.get(EFFECT_ID).state == "pending"


@pytest.mark.parametrize(
    "overrides,reason",
    [
        (
            {"base": {"ref": "develop", "repo": {"full_name": REPOSITORY}}},
            "base",
        ),
        (
            {
                "head": {
                    "sha": HEAD,
                    "ref": "feature/other",
                    "repo": {"full_name": REPOSITORY},
                }
            },
            "branch",
        ),
        (
            {
                "head": {
                    "sha": HEAD,
                    "ref": "feature/eng-9",
                    "repo": {"full_name": "fork/demo"},
                }
            },
            "repository",
        ),
        (
            {
                "head": {
                    "sha": OLD_HEAD,
                    "ref": "feature/eng-9",
                    "repo": {"full_name": REPOSITORY},
                }
            },
            "head",
        ),
    ],
)
def test_merged_pull_identity_is_validated_before_any_recovery(
    github: GitHubClient,
    transport: FakeTransport,
    journal: MergeEffectJournal,
    overrides: dict[str, Any],
    reason: str,
) -> None:
    journal.claim(EFFECT_ID, request=MERGE_REQUEST)
    payload = pull_payload(
        merged=True, state="closed", merge_commit_sha=MERGE_SHA, **overrides
    )
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, payload))
    with pytest.raises(MergeBlocked, match=reason):
        merge(github)
    assert transport.merge_calls() == []


def test_already_merged_pull_without_effect_is_foreign(
    github: GitHubClient, transport: FakeTransport
) -> None:
    payload = pull_payload(merged=True, state="closed", merge_commit_sha=MERGE_SHA)
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, payload))
    with pytest.raises(MergeBlocked, match="outside"):
        merge(github)
    assert transport.merge_calls() == []


def test_competing_effect_id_for_same_candidate_cannot_mutate(
    database: Database, transport: FakeTransport, github: GitHubClient
) -> None:
    """One canonical owner per repository/PR/exact head across effect ids."""

    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue(
        "PUT", MERGE_PATH, GitHubResponse(200, {"merged": True, "sha": MERGE_SHA})
    )
    merge(github)

    competing_transport = FakeTransport()
    # The competitor observes a stale still-open pull request.
    competing_transport.queue(
        "GET", PULLS_PATH, GitHubResponse(200, pull_payload())
    )
    competitor = GitHubClient(
        transport=competing_transport,
        journal=MergeEffectJournal(database, now=lambda: NOW),
    )
    with pytest.raises(GitHubError, match="own"):
        competitor.merge(
            REPOSITORY,
            14,
            expected_head_sha=HEAD,
            expected_head_ref="feature/eng-9",
            expected_base="main",
            effect_id="competing-effect",
        )
    assert competing_transport.merge_calls() == []


def test_two_connections_race_yields_one_owner_and_one_mutation(
    tmp_path: Path,
) -> None:
    """Two real connections, two effect ids, one candidate: one PUT total."""

    db_path = tmp_path / "state.db"
    Database.open(db_path).close()
    barrier = threading.Barrier(2)
    results: dict[str, tuple[str, int]] = {}

    def attempt(name: str, effect_id: str) -> None:
        database = Database.open(db_path)
        transport = FakeTransport()
        transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
        transport.queue(
            "PUT",
            MERGE_PATH,
            GitHubResponse(200, {"merged": True, "sha": MERGE_SHA}),
        )
        client = GitHubClient(
            transport=transport,
            journal=MergeEffectJournal(database, now=lambda: NOW),
        )
        barrier.wait()
        try:
            client.merge(
                REPOSITORY,
                14,
                expected_head_sha=HEAD,
                expected_head_ref="feature/eng-9",
                expected_base="main",
                effect_id=effect_id,
            )
            results[name] = ("merged", len(transport.merge_calls()))
        except GitHubError:
            results[name] = ("blocked", len(transport.merge_calls()))
        finally:
            database.close()

    threads = [
        threading.Thread(target=attempt, args=(f"worker-{index}", f"effect-{index}"))
        for index in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(state for state, _ in results.values()) == ["blocked", "merged"]
    assert sum(mutations for _, mutations in results.values()) == 1


def test_live_claim_same_effect_fails_closed_in_flight(
    github: GitHubClient,
    transport: FakeTransport,
    journal: MergeEffectJournal,
) -> None:
    """A live pending claim means another attempt owns the mutation."""

    journal.claim(EFFECT_ID, request=MERGE_REQUEST)
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    with pytest.raises(MergeInFlight, match="in flight"):
        merge(github)
    assert transport.merge_calls() == []


def test_expired_lease_reclaims_after_restart_with_open_pull(
    database: Database, clock: Clock
) -> None:
    """A crashed owner's expired lease permits one safe exact-head retry."""

    crashed = MergeEffectJournal(database, now=clock.now, lease_seconds=300)
    crashed.claim(EFFECT_ID, request=MERGE_REQUEST)
    clock.advance(301)
    restarted = MergeEffectJournal(database, now=clock.now, lease_seconds=300)
    transport = FakeTransport()
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue(
        "PUT", MERGE_PATH, GitHubResponse(200, {"merged": True, "sha": MERGE_SHA})
    )
    client = GitHubClient(transport=transport, journal=restarted)
    result = client.merge(
        REPOSITORY,
        14,
        expected_head_sha=HEAD,
        expected_head_ref="feature/eng-9",
        expected_base="main",
        effect_id=EFFECT_ID,
    )
    assert result.merge_sha == MERGE_SHA
    assert result.already_merged is False
    assert restarted.get(EFFECT_ID).state == "completed"


def test_stale_token_cannot_complete_or_alter_the_row(
    journal: MergeEffectJournal, clock: Clock
) -> None:
    first = journal.claim(EFFECT_ID, request=MERGE_REQUEST)
    clock.advance(301)
    second = journal.claim(EFFECT_ID, request=MERGE_REQUEST)
    assert first.token != second.token
    with pytest.raises(GitHubError, match="stale"):
        journal.mark_attempted(EFFECT_ID, token=first.token)
    with pytest.raises(GitHubError, match="stale"):
        journal.complete(
            EFFECT_ID, {"merged": True, "sha": MERGE_SHA}, token=first.token
        )
    assert journal.get(EFFECT_ID).state == "pending"
    journal.release(EFFECT_ID, token=first.token)
    assert journal.get(EFFECT_ID).claim_token == second.token
    journal.mark_attempted(EFFECT_ID, token=second.token)
    with pytest.raises(GitHubError, match="stale"):
        journal.complete(
            EFFECT_ID, {"merged": True, "sha": MERGE_SHA}, token=first.token
        )
    journal.complete(
        EFFECT_ID, {"merged": True, "sha": MERGE_SHA}, token=second.token
    )
    assert journal.get(EFFECT_ID).state == "completed"


def test_completion_requires_the_attempted_boundary_state(
    journal: MergeEffectJournal,
) -> None:
    """A claim that never reached the mutation boundary cannot complete."""

    claim = journal.claim(EFFECT_ID, request=MERGE_REQUEST)
    with pytest.raises(GitHubError, match="stale"):
        journal.complete(
            EFFECT_ID, {"merged": True, "sha": MERGE_SHA}, token=claim.token
        )
    assert journal.get(EFFECT_ID).state == "pending"


def test_attempted_state_survives_restart_and_is_never_reclaimed(
    database: Database, clock: Clock
) -> None:
    """No time passage or restart grants another claim after the boundary."""

    journal = MergeEffectJournal(database, now=clock.now, lease_seconds=300)
    claim = journal.claim(EFFECT_ID, request=MERGE_REQUEST)
    journal.mark_attempted(EFFECT_ID, token=claim.token)
    clock.advance(1_000_000)
    restarted = MergeEffectJournal(database, now=clock.now, lease_seconds=300)
    with pytest.raises(MergeAmbiguous, match="reconciliation"):
        restarted.claim(EFFECT_ID, request=MERGE_REQUEST)
    assert restarted.get(EFFECT_ID).state == "attempted"
    transport = FakeTransport()
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    client = GitHubClient(transport=transport, journal=restarted)
    with pytest.raises(MergeAmbiguous):
        client.merge(
            REPOSITORY,
            14,
            expected_head_sha=HEAD,
            expected_head_ref="feature/eng-9",
            expected_base="main",
            effect_id=EFFECT_ID,
        )
    assert transport.merge_calls() == []


def test_mark_attempted_requires_the_exact_live_token(
    journal: MergeEffectJournal,
) -> None:
    journal.claim(EFFECT_ID, request=MERGE_REQUEST)
    with pytest.raises(GitHubError, match="stale"):
        journal.mark_attempted(EFFECT_ID, token="not-the-token")
    assert journal.get(EFFECT_ID).state == "pending"


@pytest.mark.parametrize("lease", [0.0, -1.0])
def test_journal_requires_a_positive_lease_duration(
    database: Database, lease: float
) -> None:
    with pytest.raises(ValueError, match="positive"):
        MergeEffectJournal(database, lease_seconds=lease)


@dataclass
class SlowPutTransport:
    """Delegate transport whose PUT blocks until released."""

    inner: FakeTransport
    put_started: threading.Event
    put_release: threading.Event

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, str] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> GitHubResponse:
        if method == "PUT":
            self.put_started.set()
            assert self.put_release.wait(timeout=10)
        return self.inner.request(method, path, query=query, body=body)

    def merge_calls(self) -> list[Any]:
        return self.inner.merge_calls()


def test_slow_put_owner_keeps_exclusive_mutation_after_lease_expiry(
    tmp_path: Path,
) -> None:
    """Reviewer probe: expiry during a running PUT grants nobody a second PUT."""

    db_path = tmp_path / "state.db"
    Database.open(db_path).close()
    clock = Clock(NOW)
    put_started = threading.Event()
    put_release = threading.Event()
    outcome: dict[str, Any] = {}

    def owner_a() -> None:
        database = Database.open(db_path)
        inner = FakeTransport()
        inner.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
        inner.queue(
            "PUT",
            MERGE_PATH,
            GitHubResponse(200, {"merged": True, "sha": MERGE_SHA}),
        )
        client = GitHubClient(
            transport=SlowPutTransport(inner, put_started, put_release),
            journal=MergeEffectJournal(
                database, now=clock.now, lease_seconds=300
            ),
        )
        try:
            outcome["result"] = client.merge(
                REPOSITORY,
                14,
                expected_head_sha=HEAD,
                expected_head_ref="feature/eng-9",
                expected_base="main",
                effect_id=EFFECT_ID,
            )
            outcome["puts"] = len(inner.merge_calls())
        finally:
            database.close()

    thread = threading.Thread(target=owner_a)
    thread.start()
    try:
        assert put_started.wait(timeout=10)
        clock.advance(301)
        database_b = Database.open(db_path)
        try:
            transport_b = FakeTransport()
            transport_b.queue(
                "GET", PULLS_PATH, GitHubResponse(200, pull_payload())
            )
            client_b = GitHubClient(
                transport=transport_b,
                journal=MergeEffectJournal(
                    database_b, now=clock.now, lease_seconds=300
                ),
            )
            with pytest.raises(MergeAmbiguous):
                client_b.merge(
                    REPOSITORY,
                    14,
                    expected_head_sha=HEAD,
                    expected_head_ref="feature/eng-9",
                    expected_base="main",
                    effect_id=EFFECT_ID,
                )
            assert transport_b.merge_calls() == []
        finally:
            database_b.close()
    finally:
        put_release.set()
        thread.join()

    assert outcome["result"].merge_sha == MERGE_SHA
    assert outcome["result"].already_merged is False
    assert outcome["puts"] == 1


def test_definitive_rejection_releases_claim_and_allows_retry(
    github: GitHubClient,
    transport: FakeTransport,
    journal: MergeEffectJournal,
) -> None:
    """A definitive GitHub rejection frees the lease for a fresh attempt."""

    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue(
        "PUT", MERGE_PATH, GitHubResponse(409, {"message": "Head was modified"})
    )
    with pytest.raises(MergeRaceLost):
        merge(github)
    effect = journal.get(EFFECT_ID)
    assert effect.state == "pending"
    assert effect.claim_token is None
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue(
        "PUT", MERGE_PATH, GitHubResponse(200, {"merged": True, "sha": MERGE_SHA})
    )
    result = merge(github)
    assert result.merge_sha == MERGE_SHA
    assert len(transport.merge_calls()) == 2


def test_ambiguous_transport_failure_preserves_reconciliation(
    database: Database, clock: Clock
) -> None:
    """An unknown mutation outcome keeps the lease and never adopts a merge."""

    journal = MergeEffectJournal(database, now=clock.now, lease_seconds=300)
    transport = FakeTransport()
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    transport.queue("PUT", MERGE_PATH, RuntimeError("connection reset"))
    client = GitHubClient(transport=transport, journal=journal)

    def attempt() -> Any:
        return client.merge(
            REPOSITORY,
            14,
            expected_head_sha=HEAD,
            expected_head_ref="feature/eng-9",
            expected_base="main",
            effect_id=EFFECT_ID,
        )

    with pytest.raises(GitHubError, match="unknown"):
        attempt()
    effect = journal.get(EFFECT_ID)
    assert effect.state == "attempted"
    assert effect.claim_token is not None

    # The boundary was crossed, so another attempt is ambiguous, not merely
    # in flight, and no expiry can ever change that.
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
    with pytest.raises(MergeAmbiguous):
        attempt()

    # After expiry, a merged pull request without completed ownership is
    # ambiguous — the attempted intent is never adopted.
    clock.advance(301)
    merged = pull_payload(merged=True, state="closed", merge_commit_sha=MERGE_SHA)
    transport.queue("GET", PULLS_PATH, GitHubResponse(200, merged))
    with pytest.raises(MergeAmbiguous):
        attempt()
    assert journal.get(EFFECT_ID).state == "attempted"


def test_same_effect_two_connection_race_sends_exactly_one_put(
    tmp_path: Path,
) -> None:
    """Two real connections sharing one effect id produce one PUT total."""

    db_path = tmp_path / "state.db"
    Database.open(db_path).close()
    barrier = threading.Barrier(2)
    results: dict[str, tuple[str, int]] = {}

    def attempt(name: str) -> None:
        database = Database.open(db_path)
        transport = FakeTransport()
        transport.queue("GET", PULLS_PATH, GitHubResponse(200, pull_payload()))
        transport.queue(
            "PUT",
            MERGE_PATH,
            GitHubResponse(200, {"merged": True, "sha": MERGE_SHA}),
        )
        client = GitHubClient(
            transport=transport,
            journal=MergeEffectJournal(
                database, now=lambda: NOW, lease_seconds=300
            ),
        )
        barrier.wait()
        try:
            result = client.merge(
                REPOSITORY,
                14,
                expected_head_sha=HEAD,
                expected_head_ref="feature/eng-9",
                expected_base="main",
                effect_id=EFFECT_ID,
            )
            state = "replayed" if result.already_merged else "merged"
            results[name] = (state, len(transport.merge_calls()))
        except GitHubError:
            results[name] = ("blocked", len(transport.merge_calls()))
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

    states = sorted(state for state, _ in results.values())
    assert states in (["blocked", "merged"], ["merged", "replayed"])
    assert sum(mutations for _, mutations in results.values()) == 1


def test_linear_external_effects_are_unaffected_by_merge_journal(
    database: Database, journal: MergeEffectJournal
) -> None:
    """The dedicated merge journal never touches Linear's effect journal."""

    store = ExternalEffectStore(database)
    store.begin("shared-id", target="ENG-9", request={"status": "Done"})
    claim = journal.claim("shared-id", request=MERGE_REQUEST)
    assert store.get("shared-id").request == {"status": "Done"}
    assert journal.get("shared-id").request == MERGE_REQUEST
    journal.mark_attempted("shared-id", token=claim.token)
    journal.complete(
        "shared-id", {"merged": True, "sha": MERGE_SHA}, token=claim.token
    )
    assert store.get("shared-id").state == "pending"


@pytest.mark.parametrize(
    "repository,number,sha",
    [
        ("no-slash", 14, HEAD),
        ("-evil/demo", 14, HEAD),
        (REPOSITORY, 0, HEAD),
        (REPOSITORY, 14, "HEAD"),
    ],
)
def test_merge_validates_identity_before_any_call(
    github: GitHubClient,
    transport: FakeTransport,
    repository: str,
    number: int,
    sha: str,
) -> None:
    with pytest.raises(GitHubError):
        github.merge(
            repository,
            number,
            expected_head_sha=sha,
            expected_head_ref="feature/eng-9",
            expected_base="main",
            effect_id="effect-1",
        )
    assert transport.calls == []
