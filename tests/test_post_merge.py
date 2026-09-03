"""Verify deterministic, idempotent post-merge activation and advance."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest, IssueState
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.git import GitError
from hermes_orchestrator.post_merge import PostMergeAdvance
from hermes_orchestrator.processes import ProcessLeaseInput, ProcessRegistry
from hermes_orchestrator.queue import QueueService
from tests.test_processes import FakeInfo, FakeOs

MERGE_SHA = "d" * 40
FAKE_PID = 4321

SELF_HOST = ProjectConfig(
    linear_team="infra",
    repo_path=Path("/repo/self-host"),
    integration_branch="main",
    github_repo="acme/self-host",
    self_host=True,
)
PLAIN = ProjectConfig(
    linear_team="infra",
    repo_path=Path("/repo/plain"),
    integration_branch="main",
    github_repo="acme/plain",
)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def events(database: Database) -> EventStore:
    return EventStore(database)


def _request(
    issue_id: str,
    instruction_id: str,
    *,
    project_key: str = "demo",
    dependency_ready: bool = True,
) -> AdmissionRequest:
    return AdmissionRequest(
        issue_id=issue_id,
        project_key=project_key,
        linear_priority=2,
        admitted_by="operator",
        instruction_id=instruction_id,
        dependency_ready=dependency_ready,
    )


@dataclass
class FakeProcess:
    pid: int
    returncode: int | None = 0

    async def wait(self) -> int:
        return self.returncode or 0


@dataclass
class FakeSpawn:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = field(default_factory=list)
    pid: int = FAKE_PID

    async def __call__(self, *args: Any, **kwargs: Any) -> FakeProcess:
        self.calls.append((args, kwargs))
        return FakeProcess(pid=self.pid)


@dataclass
class FakeGit:
    fetch_calls: list[tuple[Path, str, str]] = field(default_factory=list)
    worktree_calls: list[tuple[Path, Path, str]] = field(default_factory=list)

    def fetch(self, path: Path, remote: str, branch: str) -> None:
        self.fetch_calls.append((path, remote, branch))

    def worktree_add_detached(self, repo_path: Path, path: Path, sha: str) -> None:
        self.worktree_calls.append((repo_path, path, sha))
        path.mkdir(parents=True, exist_ok=True)


@dataclass
class FakeAncestry:
    """Scripted ancestry proof; defaults to fail-closed (not an ancestor)."""

    result: bool = False
    error: Exception | None = None
    calls: list[tuple[Path, str, str]] = field(default_factory=list)

    def is_ancestor(self, repo_path: Path, commit: str, ref: str) -> bool:
        self.calls.append((repo_path, commit, ref))
        if self.error is not None:
            raise self.error
        return self.result


def _registry(database: Database, events: EventStore) -> ProcessRegistry:
    os_port = FakeOs()
    info = FakeInfo()
    os_port.info = info
    os_port.groups[FAKE_PID] = FAKE_PID
    info.create_times[FAKE_PID] = 1.0
    info.running.add(FAKE_PID)
    return ProcessRegistry(database, events, os_port=os_port, info=info)


def _registry_with_info(
    database: Database, events: EventStore
) -> tuple[ProcessRegistry, FakeInfo]:
    """Like :func:`_registry`, but exposes the fake liveness evidence so a
    test can make the leased process vanish without ever calling
    ``mark_exited`` itself."""

    os_port = FakeOs()
    info = FakeInfo()
    os_port.info = info
    os_port.groups[FAKE_PID] = FAKE_PID
    info.create_times[FAKE_PID] = 1.0
    info.running.add(FAKE_PID)
    return ProcessRegistry(database, events, os_port=os_port, info=info), info


def make_advance(
    database: Database,
    events: EventStore,
    *,
    projects: dict[str, ProjectConfig],
    queue: QueueService,
    tmp_path: Path,
    registry: ProcessRegistry | None = None,
    spawn: FakeSpawn | None = None,
    git: FakeGit | None = None,
    ancestry: FakeAncestry | None = None,
    replenish: Any = None,
) -> PostMergeAdvance:
    return PostMergeAdvance(
        database=database,
        events=events,
        projects=projects,
        queue=queue,
        repo_root=tmp_path / "repo",
        state_dir=tmp_path / "state",
        registry=registry,
        git=git if git is not None else FakeGit(),
        ancestry=ancestry if ancestry is not None else FakeAncestry(),
        uv_binary="/usr/bin/uv",
        spawn=spawn if spawn is not None else FakeSpawn(),
        replenish=replenish,
    )


def seed_merged_review(
    database: Database,
    *,
    review_id: str,
    project_key: str,
    issue_id: str,
    merge_sha: str,
    updated_at: str,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO reviews("
            "review_id, project_key, issue_id, event_id, repository, branch, "
            "pr_number, reviewed_sha, state, merge_sha, reason, "
            "projection_json, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'merged', ?, 'proven', NULL, "
            "?, ?)",
            (
                review_id,
                project_key,
                issue_id,
                f"evt:{review_id}",
                "acme/repo",
                "main",
                1,
                merge_sha,
                merge_sha,
                updated_at,
                updated_at,
            ),
        )


def seed_active_generation(
    database: Database, *, generation: int, git_sha: str, activated_at: str
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO runtime_activations("
            "activation_id, schema_version, generation, binary_path, "
            "checkout_root, git_sha, database_schema, state, reason, "
            "activated_at, updated_at"
            ") VALUES (?, 1, ?, '/bin/hermes-orchestrator', '/checkout', ?, "
            "1, 'active', NULL, ?, ?)",
            (
                f"activation-{generation}",
                generation,
                git_sha,
                activated_at,
                activated_at,
            ),
        )


def seed_applier_row(
    database: Database,
    *,
    apply_id: str,
    target_checkout: str,
    state: str,
    created_at: str,
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO activation_applies("
            "apply_id, target_checkout, prior_generation, target_generation, "
            "state, reason, created_at, updated_at"
            ") VALUES (?, ?, NULL, NULL, ?, 'test', ?, ?)",
            (apply_id, target_checkout, state, created_at, created_at),
        )


# -- (i) terminal merge of a self-host project ---------------------------


def test_on_merged_self_host_records_intent_and_defers_dependency_ready(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """A self-host merge intends the activation but never itself flips
    dependency_ready — INFRA-198 P3 / Sol correction e716a420: the
    successor stays blocked until the activation intent verifies."""

    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    queue.admit(_request("ENG-1", "chat-1", dependency_ready=False))
    queue.transition("ENG-1", IssueState.BLOCKED, actor="op", reason="dep")
    queue.admit(_request("ENG-2", "chat-2", dependency_ready=False))
    queue.transition("ENG-2", IssueState.BLOCKED, actor="op", reason="dep")
    queue.admit(_request("ENG-3", "chat-3", dependency_ready=False))
    queue.transition("ENG-3", IssueState.PAUSED, actor="op", reason="hold")

    advance = make_advance(
        database, events, projects={"demo": SELF_HOST}, queue=queue, tmp_path=tmp_path
    )

    advance.on_merged(
        project_key="demo",
        issue_id="ENG-0",
        review_id="review:demo:evt-1",
        merge_sha=MERGE_SHA,
    )

    apply_id = f"activate:{MERGE_SHA}"
    row = database.execute(
        "SELECT * FROM activation_applies WHERE apply_id = ?", (apply_id,)
    ).fetchone()
    assert row is not None
    assert row["state"] == "intended"
    assert row["target_checkout"] == str(
        tmp_path / "state" / "checkouts" / MERGE_SHA
    )
    assert (
        database.scalar(
            "SELECT count(*) FROM activation_applies WHERE apply_id = ?",
            (apply_id,),
        )
        == 1
    )
    # Nothing releases yet: the durable pending marker is recorded but
    # every dependent, including the two non-paused ones, stays blocked.
    assert queue.get("ENG-1").dependency_ready is False
    assert queue.get("ENG-2").dependency_ready is False
    assert queue.get("ENG-3").dependency_ready is False  # paused: never touched
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'issue.dependency_ready'"
        )
        == 0
    )
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'merge.successor_pending' "
            "AND aggregate_id = 'review:demo:evt-1'"
        )
        == 1
    )

    # Idempotent: calling again records nothing new.
    advance.on_merged(
        project_key="demo",
        issue_id="ENG-0",
        review_id="review:demo:evt-1",
        merge_sha=MERGE_SHA,
    )
    assert (
        database.scalar(
            "SELECT count(*) FROM activation_applies WHERE apply_id = ?",
            (apply_id,),
        )
        == 1
    )
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'merge.successor_pending'"
        )
        == 1
    )


# -- (ix) self-host successor releases only once the intent verifies -----


@pytest.mark.asyncio
async def test_self_host_successor_releases_when_activation_verifies(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    queue.admit(_request("ENG-1", "chat-1", dependency_ready=False))
    queue.transition("ENG-1", IssueState.BLOCKED, actor="op", reason="dep")
    advance = make_advance(
        database, events, projects={"demo": SELF_HOST}, queue=queue, tmp_path=tmp_path
    )
    seed_merged_review(
        database,
        review_id="review:demo:evt-1",
        project_key="demo",
        issue_id="ENG-1",
        merge_sha=MERGE_SHA,
        updated_at="2026-08-31T10:00:00+00:00",
    )
    advance.on_merged(
        project_key="demo",
        issue_id="ENG-1",
        review_id="review:demo:evt-1",
        merge_sha=MERGE_SHA,
    )
    assert queue.get("ENG-1").dependency_ready is False

    # The active generation now matches this merge's sha: verified.
    seed_active_generation(
        database,
        generation=1,
        git_sha=MERGE_SHA,
        activated_at="2026-08-31T11:00:00+00:00",
    )
    await advance.tick()

    assert queue.get("ENG-1").dependency_ready is True
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'merge.advanced' "
            "AND aggregate_id = 'review:demo:evt-1'"
        )
        == 1
    )

    # A restart over the same durable rows releases nothing new.
    restarted = make_advance(
        database, events, projects={"demo": SELF_HOST}, queue=queue, tmp_path=tmp_path
    )
    await restarted.tick()
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'merge.advanced'"
        )
        == 1
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal_state", ["refused", "rolled_back", "ambiguous"]
)
async def test_self_host_successor_stays_blocked_after_non_verified_terminal(
    database: Database, events: EventStore, tmp_path: Path, terminal_state: str
) -> None:
    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    queue.admit(_request("ENG-1", "chat-1", dependency_ready=False))
    queue.transition("ENG-1", IssueState.BLOCKED, actor="op", reason="dep")
    advance = make_advance(
        database, events, projects={"demo": SELF_HOST}, queue=queue, tmp_path=tmp_path
    )
    seed_merged_review(
        database,
        review_id="review:demo:evt-1",
        project_key="demo",
        issue_id="ENG-1",
        merge_sha=MERGE_SHA,
        updated_at="2026-08-31T10:00:00+00:00",
    )
    advance.on_merged(
        project_key="demo",
        issue_id="ENG-1",
        review_id="review:demo:evt-1",
        merge_sha=MERGE_SHA,
    )
    seed_applier_row(
        database,
        apply_id="applier-uuid-1",
        target_checkout=str(tmp_path / "state" / "checkouts" / MERGE_SHA),
        state=terminal_state,
        created_at="2026-08-31T10:05:00+00:00",
    )

    await advance.tick()
    assert queue.get("ENG-1").dependency_ready is False
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'merge.advanced'"
        )
        == 0
    )

    # Permanently: repeated ticks (including after a restart) never
    # release it, since the apply row is already terminal and non-verified.
    restarted = make_advance(
        database, events, projects={"demo": SELF_HOST}, queue=queue, tmp_path=tmp_path
    )
    await restarted.tick()
    await restarted.tick()
    assert queue.get("ENG-1").dependency_ready is False
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'merge.advanced'"
        )
        == 0
    )


# -- (v) non-self-host project --------------------------------------------


def test_on_merged_non_self_host_flips_dependencies_without_intent(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    queue = QueueService(
        database=database, events=events, registered_projects={"plain"}
    )
    queue.admit(
        _request("ENG-9", "chat-9", project_key="plain", dependency_ready=False)
    )
    queue.transition("ENG-9", IssueState.BLOCKED, actor="op", reason="dep")

    advance = make_advance(
        database, events, projects={"plain": PLAIN}, queue=queue, tmp_path=tmp_path
    )

    advance.on_merged(
        project_key="plain",
        issue_id="ENG-8",
        review_id="review:plain:evt-1",
        merge_sha=MERGE_SHA,
    )

    assert database.scalar("SELECT count(*) FROM activation_applies") == 0
    assert queue.get("ENG-9").dependency_ready is True


def test_merge_reconciliation_replenishes_immediately(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """INFRA-215: once a merge reconciliation flips a successor's
    dependency ready, the injected replenishment hook fires immediately
    with that exact project -- the same live boundary the daemon's
    per-tick sweep otherwise only reaches on its next pass."""

    queue = QueueService(
        database=database, events=events, registered_projects={"plain"}
    )
    queue.admit(
        _request("ENG-9", "chat-9", project_key="plain", dependency_ready=False)
    )
    queue.transition("ENG-9", IssueState.BLOCKED, actor="op", reason="dep")

    calls: list[str] = []
    advance = make_advance(
        database,
        events,
        projects={"plain": PLAIN},
        queue=queue,
        tmp_path=tmp_path,
        replenish=calls.append,
    )

    advance.on_merged(
        project_key="plain",
        issue_id="ENG-8",
        review_id="review:plain:evt-1",
        merge_sha=MERGE_SHA,
    )

    assert calls == ["plain"]
    assert queue.get("ENG-9").dependency_ready is True

    # Idempotent: replaying the identical merge advances nothing new,
    # so it never re-fires the hook either.
    advance.on_merged(
        project_key="plain",
        issue_id="ENG-8",
        review_id="review:plain:evt-1",
        merge_sha=MERGE_SHA,
    )
    assert calls == ["plain"]


def test_merge_reconciliation_replenish_failure_is_swallowed(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """A replenish hook that raises never surfaces into the merge
    advance path -- the durable dependency-ready flip already landed
    and the daemon's own per-tick sweep repairs the missed signal."""

    queue = QueueService(
        database=database, events=events, registered_projects={"plain"}
    )
    queue.admit(
        _request("ENG-9", "chat-9", project_key="plain", dependency_ready=False)
    )
    queue.transition("ENG-9", IssueState.BLOCKED, actor="op", reason="dep")

    def _explode(project_key: str) -> None:
        raise RuntimeError("boom")

    advance = make_advance(
        database,
        events,
        projects={"plain": PLAIN},
        queue=queue,
        tmp_path=tmp_path,
        replenish=_explode,
    )

    advance.on_merged(
        project_key="plain",
        issue_id="ENG-8",
        review_id="review:plain:evt-1",
        merge_sha=MERGE_SHA,
    )

    assert queue.get("ENG-9").dependency_ready is True


@pytest.mark.asyncio
async def test_tick_discovers_non_self_host_merge_and_releases_once_across_restarts(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """No fast ``on_merged`` callback ever ran for this merge (a crash,
    or the accelerator being out of reach); the tick's own discovery
    must find it, release its successor exactly once, and never repeat
    that release across restarts — proven by the durable
    ``merge.advanced`` event on the review."""

    queue = QueueService(
        database=database, events=events, registered_projects={"plain"}
    )
    queue.admit(
        _request("ENG-9", "chat-9", project_key="plain", dependency_ready=False)
    )
    queue.transition("ENG-9", IssueState.BLOCKED, actor="op", reason="dep")
    seed_merged_review(
        database,
        review_id="review:plain:evt-1",
        project_key="plain",
        issue_id="ENG-9",
        merge_sha=MERGE_SHA,
        updated_at="2026-08-31T10:00:00+00:00",
    )
    advance = make_advance(
        database, events, projects={"plain": PLAIN}, queue=queue, tmp_path=tmp_path
    )

    await advance.tick()

    assert queue.get("ENG-9").dependency_ready is True
    assert database.scalar("SELECT count(*) FROM activation_applies") == 0
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'merge.advanced' "
            "AND aggregate_id = 'review:plain:evt-1'"
        )
        == 1
    )

    # A restart (a fresh PostMergeAdvance over the same durable rows)
    # releases nothing new and journals nothing new.
    restarted = make_advance(
        database, events, projects={"plain": PLAIN}, queue=queue, tmp_path=tmp_path
    )
    await restarted.tick()
    await restarted.tick()
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'merge.advanced'"
        )
        == 1
    )


# -- historical self-host merges the active generation provably contains --


@pytest.mark.asyncio
async def test_tick_advances_self_host_merge_matching_active_sha_exactly(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """A self-host merge with no existing activation intent whose SHA IS
    the active generation's git_sha is proven contained without any
    ancestry call: journaled advanced with ``predates_active_generation``
    exactly once, never re-activated, and a restart replays nothing."""

    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    queue.admit(_request("ENG-1", "chat-1", dependency_ready=False))
    queue.transition("ENG-1", IssueState.BLOCKED, actor="op", reason="dep")
    seed_merged_review(
        database,
        review_id="review:demo:evt-old",
        project_key="demo",
        issue_id="ENG-1",
        merge_sha=MERGE_SHA,
        updated_at="2026-08-31T09:00:00+00:00",
    )
    seed_active_generation(
        database,
        generation=5,
        git_sha=MERGE_SHA,
        activated_at="2026-08-31T11:00:00+00:00",
    )
    ancestry = FakeAncestry(result=False)
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        ancestry=ancestry,
    )

    await advance.tick()

    assert database.scalar("SELECT count(*) FROM activation_applies") == 0
    row = database.execute(
        "SELECT payload_json FROM events WHERE event_type = 'merge.advanced' "
        "AND aggregate_id = 'review:demo:evt-old'"
    ).fetchone()
    assert row is not None
    assert "predates_active_generation" in row["payload_json"]
    assert queue.get("ENG-1").dependency_ready is True
    assert advance.spawn.calls == []  # type: ignore[attr-defined]
    # The exact-SHA proof needs no ancestry call at all.
    assert ancestry.calls == []

    # Released exactly once: a restart over the same durable rows
    # creates no intent, spawns nothing, and journals nothing new.
    restarted = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        ancestry=ancestry,
    )
    await restarted.tick()
    assert database.scalar("SELECT count(*) FROM activation_applies") == 0
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'merge.advanced'"
        )
        == 1
    )
    assert restarted.spawn.calls == []  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_tick_advances_proven_ancestor_self_host_merge_exactly_once(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """A historical self-host merge with no existing activation intent
    releases only on verified ancestry against the active generation's
    exact git_sha (Sol correction c5600e31) — and exactly once: a
    restart replay never double-releases."""

    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    queue.admit(_request("ENG-1", "chat-1", dependency_ready=False))
    queue.transition("ENG-1", IssueState.BLOCKED, actor="op", reason="dep")
    old_sha = "b" * 40
    seed_merged_review(
        database,
        review_id="review:demo:evt-old",
        project_key="demo",
        issue_id="ENG-1",
        merge_sha=old_sha,
        updated_at="2026-08-31T09:00:00+00:00",
    )
    seed_active_generation(
        database,
        generation=5,
        git_sha=MERGE_SHA,
        activated_at="2026-08-31T11:00:00+00:00",
    )
    ancestry = FakeAncestry(result=True)
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        ancestry=ancestry,
    )

    await advance.tick()

    # Ancestry was proven against the active git_sha, never a branch tip.
    assert ancestry.calls == [(tmp_path / "repo", old_sha, MERGE_SHA)]
    assert database.scalar("SELECT count(*) FROM activation_applies") == 0
    row = database.execute(
        "SELECT payload_json FROM events WHERE event_type = 'merge.advanced' "
        "AND aggregate_id = 'review:demo:evt-old'"
    ).fetchone()
    assert row is not None
    assert "predates_active_generation" in row["payload_json"]
    assert queue.get("ENG-1").dependency_ready is True
    assert advance.spawn.calls == []  # type: ignore[attr-defined]

    # Never re-activated or double-released across restarts.
    restarted = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        ancestry=ancestry,
    )
    await restarted.tick()
    assert database.scalar("SELECT count(*) FROM activation_applies") == 0
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'merge.advanced'"
        )
        == 1
    )
    assert restarted.spawn.calls == []  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_tick_keeps_unproven_self_host_merge_blocked_behind_an_intent(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """A later UNRELATED active generation orders after the merge by
    timestamp but does not contain it (ancestry false): the tick must
    never release with ``predates_active_generation`` — it creates the
    activation intent and the successor stays blocked until that intent
    durably verifies (Sol correction c5600e31)."""

    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    queue.admit(_request("ENG-1", "chat-1", dependency_ready=False))
    queue.transition("ENG-1", IssueState.BLOCKED, actor="op", reason="dep")
    old_sha = "b" * 40
    seed_merged_review(
        database,
        review_id="review:demo:evt-old",
        project_key="demo",
        issue_id="ENG-1",
        merge_sha=old_sha,
        updated_at="2026-08-31T09:00:00+00:00",
    )
    # An unrelated branch's (or rolled-forward) activation with a LATER
    # timestamp whose history does not reach this merge.
    seed_active_generation(
        database,
        generation=5,
        git_sha=MERGE_SHA,
        activated_at="2026-08-31T11:00:00+00:00",
    )
    ancestry = FakeAncestry(result=False)
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        ancestry=ancestry,
    )

    await advance.tick()

    assert ancestry.calls == [(tmp_path / "repo", old_sha, MERGE_SHA)]
    # The intent path was taken: the durable activate:<sha> row exists
    # and nothing released.
    assert (
        database.scalar(
            "SELECT count(*) FROM activation_applies WHERE apply_id = ?",
            (f"activate:{old_sha}",),
        )
        == 1
    )
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'merge.advanced'"
        )
        == 0
    )
    assert queue.get("ENG-1").dependency_ready is False


@pytest.mark.asyncio
async def test_tick_ancestry_git_error_fails_closed_to_the_intent_path(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """A GitError from the ancestry proof (unknown commit, corrupt
    repository) is never a decision: no release, and the merge falls
    through to the ordinary activation-intent path."""

    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    queue.admit(_request("ENG-1", "chat-1", dependency_ready=False))
    queue.transition("ENG-1", IssueState.BLOCKED, actor="op", reason="dep")
    old_sha = "b" * 40
    seed_merged_review(
        database,
        review_id="review:demo:evt-old",
        project_key="demo",
        issue_id="ENG-1",
        merge_sha=old_sha,
        updated_at="2026-08-31T09:00:00+00:00",
    )
    seed_active_generation(
        database,
        generation=5,
        git_sha=MERGE_SHA,
        activated_at="2026-08-31T11:00:00+00:00",
    )
    ancestry = FakeAncestry(error=GitError("missing object"))
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        ancestry=ancestry,
    )

    await advance.tick()

    assert (
        database.scalar(
            "SELECT count(*) FROM activation_applies WHERE apply_id = ?",
            (f"activate:{old_sha}",),
        )
        == 1
    )
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = 'merge.advanced'"
        )
        == 0
    )
    assert queue.get("ENG-1").dependency_ready is False


# -- (ii) tick spawns exactly one registered runtime_applier --------------


@pytest.mark.asyncio
async def test_tick_spawns_registered_applier_once(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    registry = _registry(database, events)
    spawn = FakeSpawn()
    git = FakeGit()
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        registry=registry,
        spawn=spawn,
        git=git,
    )
    apply_id = f"activate:{MERGE_SHA}"
    seed_merged_review(
        database,
        review_id="review:demo:evt-1",
        project_key="demo",
        issue_id="ENG-1",
        merge_sha=MERGE_SHA,
        updated_at="2026-08-31T10:00:00+00:00",
    )
    advance.on_merged(
        project_key="demo",
        issue_id="ENG-1",
        review_id="review:demo:evt-1",
        merge_sha=MERGE_SHA,
    )

    await advance.tick()

    target_checkout = tmp_path / "state" / "checkouts" / MERGE_SHA
    assert git.fetch_calls == [(tmp_path / "repo", "origin", "main")]
    assert git.worktree_calls == [(tmp_path / "repo", target_checkout, MERGE_SHA)]
    assert len(spawn.calls) == 1
    args, kwargs = spawn.calls[0]
    assert args == (
        "/usr/bin/uv",
        "run",
        "--project",
        str(target_checkout),
        "hermes-orchestrator",
        "--repo-root",
        str(tmp_path / "repo"),
        "--state-dir",
        str(tmp_path / "state"),
        "runtime-activate",
        "--apply",
        "--json",
    )
    assert kwargs["start_new_session"] is True
    leases = registry.active()
    assert len(leases) == 1
    assert leases[0].kind == "runtime_applier"
    assert leases[0].worker_id == MERGE_SHA
    assert leases[0].project_key == "demo"
    row = database.execute(
        "SELECT state FROM activation_applies WHERE apply_id = ?", (apply_id,)
    ).fetchone()
    assert row["state"] == "intended"

    # A second tick must spawn nothing while the lease is active.
    await advance.tick()
    assert len(spawn.calls) == 1

    # A restart (a fresh PostMergeAdvance over the same durable rows)
    # must spawn nothing either, for the same reason.
    restarted = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        registry=registry,
        spawn=spawn,
        git=git,
    )
    await restarted.tick()
    assert len(spawn.calls) == 1


# -- (iii) applier terminal state propagates to our intent -----------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal_state", ["verified", "rolled_back", "ambiguous", "refused"]
)
async def test_tick_propagates_applier_terminal_state(
    database: Database, events: EventStore, tmp_path: Path, terminal_state: str
) -> None:
    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    advance = make_advance(
        database, events, projects={"demo": SELF_HOST}, queue=queue, tmp_path=tmp_path
    )
    apply_id = f"activate:{MERGE_SHA}"
    advance.on_merged(
        project_key="demo",
        issue_id="ENG-1",
        review_id="review:demo:evt-1",
        merge_sha=MERGE_SHA,
    )
    target_checkout = str(tmp_path / "state" / "checkouts" / MERGE_SHA)
    seed_applier_row(
        database,
        apply_id="applier-uuid-1",
        target_checkout=target_checkout,
        state=terminal_state,
        created_at="2026-08-31T10:05:00+00:00",
    )

    await advance.tick()

    row = database.execute(
        "SELECT state, reason FROM activation_applies WHERE apply_id = ?",
        (apply_id,),
    ).fetchone()
    assert row["state"] == terminal_state
    assert row["reason"] == "applier:applier-uuid-1"
    assert advance.spawn.calls == []  # type: ignore[attr-defined]


# -- (iv) active generation already matches ---------------------------------


@pytest.mark.asyncio
async def test_tick_marks_verified_when_active_generation_matches(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    advance = make_advance(
        database, events, projects={"demo": SELF_HOST}, queue=queue, tmp_path=tmp_path
    )
    apply_id = f"activate:{MERGE_SHA}"
    advance.on_merged(
        project_key="demo",
        issue_id="ENG-1",
        review_id="review:demo:evt-1",
        merge_sha=MERGE_SHA,
    )
    seed_active_generation(
        database,
        generation=37,
        git_sha=MERGE_SHA,
        activated_at="2026-08-31T11:00:00+00:00",
    )

    await advance.tick()

    row = database.execute(
        "SELECT state, reason FROM activation_applies WHERE apply_id = ?",
        (apply_id,),
    ).fetchone()
    assert row["state"] == "verified"
    assert row["reason"] == "active generation matches"
    assert advance.spawn.calls == []  # type: ignore[attr-defined]


# -- (vii) nothing pending: a full no-op ------------------------------------


@pytest.mark.asyncio
async def test_tick_is_a_noop_with_nothing_pending(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    advance = make_advance(
        database, events, projects={"demo": SELF_HOST}, queue=queue, tmp_path=tmp_path
    )
    before_events = database.scalar("SELECT count(*) FROM events")

    await advance.tick()

    assert advance.spawn.calls == []  # type: ignore[attr-defined]
    assert database.scalar("SELECT count(*) FROM activation_applies") == 0
    assert database.scalar("SELECT count(*) FROM events") == before_events


# -- (viii) restart between merge and intent --------------------------------


@pytest.mark.asyncio
async def test_tick_discovers_merge_without_intent_after_restart(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    seed_merged_review(
        database,
        review_id="review:demo:evt-2",
        project_key="demo",
        issue_id="ENG-2",
        merge_sha=MERGE_SHA,
        updated_at="2026-08-31T10:07:00+00:00",
    )
    registry = _registry(database, events)
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        registry=registry,
    )
    apply_id = f"activate:{MERGE_SHA}"
    assert database.scalar("SELECT count(*) FROM activation_applies") == 0

    await advance.tick()
    await advance.tick()

    count = database.scalar(
        "SELECT count(*) FROM activation_applies WHERE apply_id = ?", (apply_id,)
    )
    assert count == 1
    intent_events = database.scalar(
        "SELECT count(*) FROM events WHERE event_type = 'activation.intended' "
        "AND aggregate_id = ?",
        (apply_id,),
    )
    assert intent_events == 1


# -- applier lease exits without ever writing a terminal row ---------------


@pytest.mark.asyncio
async def test_tick_marks_ambiguous_when_applier_lease_exits_without_a_row(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    registry = _registry(database, events)
    spawn = FakeSpawn()
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        registry=registry,
        spawn=spawn,
    )
    apply_id = f"activate:{MERGE_SHA}"
    seed_merged_review(
        database,
        review_id="review:demo:evt-1",
        project_key="demo",
        issue_id="ENG-1",
        merge_sha=MERGE_SHA,
        updated_at="2026-08-31T10:00:00+00:00",
    )
    advance.on_merged(
        project_key="demo",
        issue_id="ENG-1",
        review_id="review:demo:evt-1",
        merge_sha=MERGE_SHA,
    )

    await advance.tick()
    assert len(spawn.calls) == 1
    lease = registry.active()[0]
    registry.mark_exited(lease.lease_id, exit_code=1)

    await advance.tick()

    row = database.execute(
        "SELECT state, reason FROM activation_applies WHERE apply_id = ?",
        (apply_id,),
    ).fetchone()
    assert row["state"] == "ambiguous"
    assert row["reason"] == "applier exited without a terminal apply record"
    assert len(spawn.calls) == 1  # never respawned


@pytest.mark.asyncio
async def test_tick_reaps_dead_applier_lease_without_manual_mark_exited(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """The applier process vanishes without anyone ever calling
    ``mark_exited`` — the tick itself must observe the exit through the
    registry, reap the exact lease, and only then let the existing rule
    turn the unproven intent into ``ambiguous``."""

    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    registry, info = _registry_with_info(database, events)
    spawn = FakeSpawn()
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        registry=registry,
        spawn=spawn,
    )
    apply_id = f"activate:{MERGE_SHA}"
    seed_merged_review(
        database,
        review_id="review:demo:evt-1",
        project_key="demo",
        issue_id="ENG-1",
        merge_sha=MERGE_SHA,
        updated_at="2026-08-31T10:00:00+00:00",
    )
    advance.on_merged(
        project_key="demo",
        issue_id="ENG-1",
        review_id="review:demo:evt-1",
        merge_sha=MERGE_SHA,
    )

    await advance.tick()
    assert len(spawn.calls) == 1
    lease = registry.active()[0]
    assert lease.state == "active"

    # The applier process is simply gone — nothing records the exit.
    info.running.discard(FAKE_PID)

    await advance.tick()

    stopped = registry.get(lease.lease_id)
    assert (stopped.state, stopped.stop_reason) == ("stopped", "exited")
    row = database.execute(
        "SELECT state, reason FROM activation_applies WHERE apply_id = ?",
        (apply_id,),
    ).fetchone()
    assert row["state"] == "ambiguous"
    assert row["reason"] == "applier exited without a terminal apply record"
    assert len(spawn.calls) == 1  # never respawned

    # Ambiguous exactly once: further ticks journal nothing new and
    # never respawn.
    await advance.tick()
    assert len(spawn.calls) == 1
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = "
            "'activation.ambiguous' AND aggregate_id = ?",
            (apply_id,),
        )
        == 1
    )


# -- the applier is resolved by exact journaled lease binding --------------


IMPOSTOR_PID = 5555


def _registry_with_impostor_room(
    database: Database, events: EventStore
) -> tuple[ProcessRegistry, FakeInfo]:
    """A registry whose fake liveness evidence knows two pids, so a test
    can register a second runtime_applier-shaped lease beside the one the
    tick spawns."""

    os_port = FakeOs()
    info = FakeInfo()
    os_port.info = info
    for pid in (FAKE_PID, IMPOSTOR_PID):
        os_port.groups[pid] = pid
        info.create_times[pid] = 1.0
        info.running.add(pid)
    return ProcessRegistry(database, events, os_port=os_port, info=info), info


def _register_impostor(
    registry: ProcessRegistry, *, cwd: str
) -> str:
    """Register a live lease matching the applier's (kind, worker_id) —
    but never journaled as the spawned applier's binding."""

    lease = registry.register(
        ProcessLeaseInput(
            pid=IMPOSTOR_PID,
            pgid=IMPOSTOR_PID,
            project_key="demo",
            kind="runtime_applier",
            worker_id=MERGE_SHA,
            executable="/usr/bin/uv",
            cwd=cwd,
        )
    )
    return lease.lease_id


@pytest.mark.asyncio
async def test_tick_never_treats_an_unbound_kind_worker_match_as_the_applier(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """A live runtime_applier lease matching (kind, worker_id) with no
    journaled ``activation.applier_spawned`` binding is never "the
    applier" (Sol correction c5600e31) — yet the tick also never spawns
    a second applier beside it, and never declares the intent ambiguous
    while it may still write its own terminal row: it waits."""

    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    registry, _info = _registry_with_impostor_room(database, events)
    spawn = FakeSpawn()
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        registry=registry,
        spawn=spawn,
    )
    apply_id = f"activate:{MERGE_SHA}"
    seed_merged_review(
        database,
        review_id="review:demo:evt-1",
        project_key="demo",
        issue_id="ENG-1",
        merge_sha=MERGE_SHA,
        updated_at="2026-08-31T10:00:00+00:00",
    )
    advance.on_merged(
        project_key="demo",
        issue_id="ENG-1",
        review_id="review:demo:evt-1",
        merge_sha=MERGE_SHA,
    )
    target_checkout = str(tmp_path / "state" / "checkouts" / MERGE_SHA)
    _register_impostor(registry, cwd=target_checkout)

    await advance.tick()
    await advance.tick()

    assert spawn.calls == []
    row = database.execute(
        "SELECT state FROM activation_applies WHERE apply_id = ?", (apply_id,)
    ).fetchone()
    assert row["state"] == "intended"


@pytest.mark.asyncio
async def test_tick_reaps_exactly_the_bound_lease_beside_a_live_duplicate(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """Two live runtime_applier leases share the worker_id; only the
    journaled binding names the real applier. When the bound one exits,
    the tick reaps exactly that lease_id — the duplicate is never
    signaled or reaped, and its unprovable survival keeps the intent
    out of ``ambiguous``."""

    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    registry, info = _registry_with_impostor_room(database, events)
    spawn = FakeSpawn()
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        registry=registry,
        spawn=spawn,
    )
    apply_id = f"activate:{MERGE_SHA}"
    seed_merged_review(
        database,
        review_id="review:demo:evt-1",
        project_key="demo",
        issue_id="ENG-1",
        merge_sha=MERGE_SHA,
        updated_at="2026-08-31T10:00:00+00:00",
    )
    advance.on_merged(
        project_key="demo",
        issue_id="ENG-1",
        review_id="review:demo:evt-1",
        merge_sha=MERGE_SHA,
    )

    await advance.tick()
    assert len(spawn.calls) == 1
    bound = registry.active()[0]
    # The journaled binding names exactly this lease.
    journaled = database.execute(
        "SELECT payload_json FROM events "
        "WHERE event_type = 'activation.applier_spawned' AND aggregate_id = ?",
        (apply_id,),
    ).fetchone()
    assert f'"{bound.lease_id}"' in journaled["payload_json"]

    target_checkout = str(tmp_path / "state" / "checkouts" / MERGE_SHA)
    duplicate_id = _register_impostor(registry, cwd=target_checkout)

    # With both alive, nothing respawns and nothing turns ambiguous.
    await advance.tick()
    assert len(spawn.calls) == 1

    # The bound applier process exits; the duplicate stays alive.
    info.running.discard(FAKE_PID)
    await advance.tick()

    reaped = registry.get(bound.lease_id)
    assert (reaped.state, reaped.stop_reason) == ("stopped", "exited")
    assert registry.get(duplicate_id).state == "active"
    row = database.execute(
        "SELECT state FROM activation_applies WHERE apply_id = ?", (apply_id,)
    ).fetchone()
    # The unprovable duplicate forbids both a second spawn and a
    # premature ambiguous verdict; the intent simply waits.
    assert row["state"] == "intended"
    assert len(spawn.calls) == 1


# -- crash-safe durable identity binding (Sol correction 57c46faa) ---------


def _seed_and_intend(
    advance: PostMergeAdvance, database: Database
) -> str:
    """Seed one merged self-host review and record its durable intent."""

    seed_merged_review(
        database,
        review_id="review:demo:evt-1",
        project_key="demo",
        issue_id="ENG-1",
        merge_sha=MERGE_SHA,
        updated_at="2026-08-31T10:00:00+00:00",
    )
    advance.on_merged(
        project_key="demo",
        issue_id="ENG-1",
        review_id="review:demo:evt-1",
        merge_sha=MERGE_SHA,
    )
    return f"activate:{MERGE_SHA}"


def _journal_applier_claim(
    database: Database, events: EventStore, *, apply_id: str, target_checkout: str
) -> None:
    """Journal the write-ahead claim the way a crashed prior life did."""

    with database.transaction() as connection:
        events.append(
            connection,
            EventInput(
                event_type="activation.applier_claimed",
                aggregate_type="activation_apply",
                aggregate_id=apply_id,
                payload={
                    "apply_id": apply_id,
                    "project_key": "demo",
                    "worker_id": MERGE_SHA,
                    "target_checkout": target_checkout,
                },
            ),
        )


@pytest.mark.asyncio
async def test_spawn_claims_before_and_binds_atomically_after(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """One write-ahead ``activation.applier_claimed`` event commits
    before the OS spawn and one atomic transaction commits the lease row
    with the ``activation.applier_spawned`` binding after it. Both carry
    the full identity — the owning project_key included — and the claim
    durably orders before the binding."""

    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    registry = _registry(database, events)
    spawn = FakeSpawn()
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        registry=registry,
        spawn=spawn,
    )
    apply_id = _seed_and_intend(advance, database)
    target_checkout = str(tmp_path / "state" / "checkouts" / MERGE_SHA)

    await advance.tick()

    assert len(spawn.calls) == 1
    lease = registry.active()[0]
    claims = database.execute(
        "SELECT sequence, payload_json FROM events "
        "WHERE event_type = 'activation.applier_claimed' AND aggregate_id = ?",
        (apply_id,),
    ).fetchall()
    assert len(claims) == 1
    assert json.loads(claims[0]["payload_json"]) == {
        "apply_id": apply_id,
        "project_key": "demo",
        "worker_id": MERGE_SHA,
        "target_checkout": target_checkout,
    }
    spawned = database.execute(
        "SELECT sequence, payload_json FROM events "
        "WHERE event_type = 'activation.applier_spawned' AND aggregate_id = ?",
        (apply_id,),
    ).fetchall()
    assert len(spawned) == 1
    assert json.loads(spawned[0]["payload_json"]) == {
        "apply_id": apply_id,
        "pid": FAKE_PID,
        "lease_id": lease.lease_id,
        "project_key": "demo",
        "worker_id": MERGE_SHA,
        "target_checkout": target_checkout,
    }
    assert int(claims[0]["sequence"]) < int(spawned[0]["sequence"])


@pytest.mark.asyncio
async def test_restart_after_atomic_binding_commit_recognizes_the_applier(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """A crash immediately after the atomic lease+binding commit (the
    former lease-committed-but-journal-missing window no longer exists —
    the two can never diverge): a restarted advance over the durable
    rows recognizes exactly the one bound applier, spawns nothing
    beside it, and keeps the intent open while it verifies."""

    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    registry = _registry(database, events)
    spawn = FakeSpawn()
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        registry=registry,
        spawn=spawn,
    )
    apply_id = _seed_and_intend(advance, database)

    await advance.tick()
    assert len(spawn.calls) == 1

    # The daemon dies right after the commit; the restarted life builds a
    # fresh registry over the same durable rows and the applier survives.
    restarted_registry = _registry(database, events)
    restarted = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        registry=restarted_registry,
        spawn=spawn,
    )
    await restarted.tick()
    await restarted.tick()

    assert len(spawn.calls) == 1
    row = database.execute(
        "SELECT state FROM activation_applies WHERE apply_id = ?", (apply_id,)
    ).fetchone()
    assert row["state"] == "intended"
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = "
            "'activation.applier_spawned' AND aggregate_id = ?",
            (apply_id,),
        )
        == 1
    )


@pytest.mark.asyncio
async def test_open_claim_without_completion_resolves_ambiguous_exactly_once(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """A prior life crashed between its write-ahead claim and the atomic
    lease+binding commit: whether its child ever spawned — or still runs
    unregistered, which the reconciler's unknown-process scan reports —
    can never be proven from durable state. The restarted tick resolves
    the claim to ambiguous exactly once and never spawns beside it."""

    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    registry = _registry(database, events)
    spawn = FakeSpawn()
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        registry=registry,
        spawn=spawn,
    )
    apply_id = _seed_and_intend(advance, database)
    _journal_applier_claim(
        database,
        events,
        apply_id=apply_id,
        target_checkout=str(tmp_path / "state" / "checkouts" / MERGE_SHA),
    )

    await advance.tick()

    row = database.execute(
        "SELECT state, reason FROM activation_applies WHERE apply_id = ?",
        (apply_id,),
    ).fetchone()
    assert row["state"] == "ambiguous"
    assert row["reason"] == "applier claim never completed"
    assert spawn.calls == []

    await advance.tick()
    assert spawn.calls == []
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = "
            "'activation.ambiguous' AND aggregate_id = ?",
            (apply_id,),
        )
        == 1
    )


@pytest.mark.asyncio
async def test_failed_binding_reaps_the_child_and_the_claim_fails_closed(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """The atomic lease+binding transaction cannot commit (registration
    refuses the spawned pid): the exact new child is reaped before the
    error propagates, the rollback leaves no lease row and no binding,
    and the still-open claim resolves the intent to ambiguous on the
    next tick — never a second spawn."""

    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    # A registry whose evidence knows no pid at all refuses registration.
    registry = ProcessRegistry(database, events, os_port=FakeOs(), info=FakeInfo())
    spawn = FakeSpawn()
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        registry=registry,
        spawn=spawn,
    )
    reaped: list[int] = []

    async def fake_terminate(process: Any) -> None:
        reaped.append(int(process.pid))

    advance._terminate_group = fake_terminate  # type: ignore[method-assign]
    apply_id = _seed_and_intend(advance, database)

    await advance.tick()

    assert len(spawn.calls) == 1
    assert reaped == [FAKE_PID]
    assert registry.active() == ()
    assert database.scalar("SELECT count(*) FROM process_leases") == 0
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = "
            "'activation.applier_spawned'"
        )
        == 0
    )

    await advance.tick()

    row = database.execute(
        "SELECT state, reason FROM activation_applies WHERE apply_id = ?",
        (apply_id,),
    ).fetchone()
    assert row["state"] == "ambiguous"
    assert row["reason"] == "applier claim never completed"
    assert len(spawn.calls) == 1  # never respawned


@pytest.mark.asyncio
async def test_bound_lease_owned_by_another_project_is_never_adopted(
    database: Database, events: EventStore, tmp_path: Path
) -> None:
    """The journaled binding names a lease whose row is owned by a
    different project (same worker_id and cwd): project ownership is
    part of the exact binding (Sol correction 57c46faa), so that lease
    is never "the applier" — the tick adopts nothing, spawns nothing,
    fails closed to ambiguous, and never signals or reaps the foreign
    lease (reconciliation reports it as a blocking orphan)."""

    queue = QueueService(database=database, events=events, registered_projects={"demo"})
    registry, _info = _registry_with_impostor_room(database, events)
    spawn = FakeSpawn()
    advance = make_advance(
        database,
        events,
        projects={"demo": SELF_HOST},
        queue=queue,
        tmp_path=tmp_path,
        registry=registry,
        spawn=spawn,
    )
    apply_id = _seed_and_intend(advance, database)
    target_checkout = str(tmp_path / "state" / "checkouts" / MERGE_SHA)
    foreign = registry.register(
        ProcessLeaseInput(
            pid=IMPOSTOR_PID,
            pgid=IMPOSTOR_PID,
            project_key="other",
            kind="runtime_applier",
            worker_id=MERGE_SHA,
            executable="/usr/bin/uv",
            cwd=target_checkout,
        )
    )
    _journal_applier_claim(
        database, events, apply_id=apply_id, target_checkout=target_checkout
    )
    with database.transaction() as connection:
        events.append(
            connection,
            EventInput(
                event_type="activation.applier_spawned",
                aggregate_type="activation_apply",
                aggregate_id=apply_id,
                payload={
                    "apply_id": apply_id,
                    "pid": IMPOSTOR_PID,
                    "lease_id": foreign.lease_id,
                    "project_key": "demo",
                    "worker_id": MERGE_SHA,
                    "target_checkout": target_checkout,
                },
            ),
        )

    await advance.tick()
    await advance.tick()

    assert spawn.calls == []
    assert registry.get(foreign.lease_id).state == "active"
    row = database.execute(
        "SELECT state, reason FROM activation_applies WHERE apply_id = ?",
        (apply_id,),
    ).fetchone()
    assert row["state"] == "ambiguous"
    assert row["reason"] == "applier exited without a terminal apply record"
    assert (
        database.scalar(
            "SELECT count(*) FROM events WHERE event_type = "
            "'activation.ambiguous' AND aggregate_id = ?",
            (apply_id,),
        )
        == 1
    )
