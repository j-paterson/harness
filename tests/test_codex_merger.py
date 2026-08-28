"""Verify persistent read-only project Codex Merger threads."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

import pytest

from hermes_orchestrator.codex_merger import (
    CodexMerger,
    MergerAuthRequired,
    MergerThreadUncertain,
    StaleChannelError,
)
from hermes_orchestrator.codex_rpc import (
    CodexRequestFailed,
    CodexTimeout,
    CodexUnavailable,
)
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "codex-merger.md"


class FakeRpc:
    """Schema-faithful fake for the installed App Server surface.

    Field allowlists, required keys, and enums below are transcribed from the
    installed ``codex-cli 0.149.0-alpha.4.1`` output of
    ``codex app-server generate-json-schema`` (v2 protocol). Unknown fields
    or enum values raise instead of passing silently.
    """

    _PARAM_KEYS: ClassVar[dict[str, set[str]]] = {
        "account/read": {"refreshToken"},
        "account/rateLimits/read": set(),
        "thread/start": {
            "approvalPolicy",
            "approvalsReviewer",
            "baseInstructions",
            "config",
            "cwd",
            "developerInstructions",
            "ephemeral",
            "model",
            "modelProvider",
            "personality",
            "sandbox",
            "serviceName",
            "serviceTier",
            "sessionStartSource",
            "threadSource",
        },
        "thread/read": {"threadId", "includeTurns"},
        "thread/resume": {
            "threadId",
            "approvalPolicy",
            "approvalsReviewer",
            "baseInstructions",
            "config",
            "cwd",
            "developerInstructions",
            "model",
            "modelProvider",
            "personality",
            "sandbox",
            "serviceTier",
        },
        "thread/name/set": {"threadId", "name"},
        "thread/goal/set": {"threadId", "objective", "status", "tokenBudget"},
        "thread/metadata/update": {"threadId", "gitInfo"},
        "threadSection/list": {"cursor", "limit"},
        "thread/section/move": {"beforeThreadId", "sectionId", "threadId"},
        "turn/interrupt": {"threadId", "turnId"},
    }
    _REQUIRED_KEYS: ClassVar[dict[str, set[str]]] = {
        "thread/read": {"threadId"},
        "thread/resume": {"threadId"},
        "thread/name/set": {"threadId", "name"},
        "thread/goal/set": {"threadId"},
        "thread/metadata/update": {"threadId"},
        "thread/section/move": {"sectionId", "threadId"},
        "turn/interrupt": {"threadId", "turnId"},
    }
    _SANDBOX_MODES = frozenset(
        {"read-only", "workspace-write", "danger-full-access"}
    )
    _APPROVAL_POLICIES = frozenset({"untrusted", "on-request", "never"})
    _GOAL_STATUSES = frozenset(
        {
            "active",
            "paused",
            "blocked",
            "usageLimited",
            "budgetLimited",
            "complete",
        }
    )

    def __init__(self) -> None:
        self.requests: list[tuple[str, dict[str, Any] | None]] = []
        self._results: dict[str, dict[str, Any]] = {}
        self._sequences: dict[str, list[dict[str, Any]]] = {}
        self._failures: dict[str, BaseException] = {}
        self.on_request: Any = None

    def respond(self, method: str, result: dict[str, Any]) -> None:
        self._results[method] = result

    def respond_sequence(
        self, method: str, results: list[dict[str, Any]]
    ) -> None:
        self._sequences[method] = list(results)

    def fail(self, method: str, error: BaseException) -> None:
        self._failures[method] = error

    def clear_failures(self) -> None:
        self._failures.clear()

    @property
    def methods(self) -> list[str]:
        return [method for method, _ in self.requests]

    def request_for(self, method: str) -> dict[str, Any]:
        params = next(
            params for name, params in self.requests if name == method
        )
        return {"method": method, "params": params}

    def _validate(self, method: str, params: dict[str, Any] | None) -> None:
        if method not in self._PARAM_KEYS:
            raise CodexRequestFailed(method, -32601)
        supplied = set(params or {})
        if not supplied <= self._PARAM_KEYS[method]:
            raise CodexRequestFailed(method, -32602)
        if not self._REQUIRED_KEYS.get(method, set()) <= supplied:
            raise CodexRequestFailed(method, -32602)
        values = params or {}
        if "sandbox" in values and values["sandbox"] not in self._SANDBOX_MODES:
            raise CodexRequestFailed(method, -32602)
        if (
            "approvalPolicy" in values
            and values["approvalPolicy"] not in self._APPROVAL_POLICIES
        ):
            raise CodexRequestFailed(method, -32602)
        if (
            method == "thread/goal/set"
            and values.get("status") is not None
            and values["status"] not in self._GOAL_STATUSES
        ):
            raise CodexRequestFailed(method, -32602)

    async def request(
        self, method: str, params: dict[str, Any] | None, timeout: float
    ) -> dict[str, Any]:
        assert timeout > 0
        self._validate(method, params)
        self.requests.append((method, params))
        if self.on_request is not None:
            await self.on_request(method)
        if method in self._failures:
            raise self._failures[method]
        if self._sequences.get(method):
            return self._sequences[method].pop(0)
        if method in self._results:
            return self._results[method]
        if method == "account/read":
            return {
                "account": {"type": "chatgpt"},
                "requiresOpenaiAuth": False,
            }
        if method == "thread/start":
            return {"thread": {"id": "thr_demo"}}
        if method == "thread/resume":
            return {"thread": {"id": "thr_stored"}}
        if method == "threadSection/list":
            return {"data": [{"id": "sec_pinned", "name": "Pinned"}]}
        return {}


def rate_limit_fixture() -> dict[str, Any]:
    return {
        "rateLimits": {
            "primary": {"usedPercent": 25, "resetsAt": 1767225600},
            "secondary": {"usedPercent": 10},
        }
    }


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def rpc() -> FakeRpc:
    return FakeRpc()


@pytest.fixture
def merger(database: Database, rpc: FakeRpc) -> CodexMerger:
    return CodexMerger(
        rpc=rpc,
        database=database,
        projects={
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=Path("/repo/demo"),
                integration_branch="main",
                github_repo="j-paterson/demo",
            )
        },
        prompt_file=PROMPT_PATH,
        now=lambda: datetime(2026, 8, 27, tzinfo=UTC),
    )


def stored_thread(
    database: Database,
    state: str = "ready",
    thread_id: str = "thr_stored",
    generation: int = 1,
) -> None:
    stamp = datetime(2026, 8, 27, tzinfo=UTC).isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO reviewer_channels("
            "project_key, thread_id, generation, state, created_at, updated_at"
            ") VALUES ('demo', ?, ?, ?, ?, ?)",
            (thread_id, generation, state, stamp, stamp),
        )


@pytest.mark.asyncio
async def test_new_merger_is_read_only_and_persistent(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    thread = await merger.ensure_thread("demo")

    assert rpc.methods[0] == "account/read"
    request = rpc.request_for("thread/start")
    assert request["params"] == {
        "model": "gpt-5.6-sol",
        "cwd": "/repo/demo",
        "approvalPolicy": "never",
        "sandbox": "read-only",
        "serviceName": "hermes_orchestrator",
    }
    assert thread.thread_id == "thr_demo"
    assert thread.project_key == "demo"
    assert rpc.request_for("thread/name/set")["params"]["name"] == "Merger: demo"
    assert "thread/metadata/update" not in rpc.methods
    assert rpc.request_for("thread/section/move")["params"] == {
        "sectionId": "sec_pinned",
        "threadId": "thr_demo",
    }
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.integration_branch == "main"
    assert database.scalar(
        "SELECT thread_id FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "thr_demo"
    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "ready"


@pytest.mark.asyncio
async def test_existing_thread_is_resumed_not_recreated(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    stored_thread(database)

    thread = await merger.ensure_thread("demo")

    assert rpc.methods == [
        "account/read",
        "thread/read",
        "thread/resume",
        "thread/goal/set",
    ]
    assert rpc.request_for("thread/read")["params"] == {
        "threadId": "thr_stored"
    }
    assert rpc.request_for("thread/resume")["params"] == {
        "threadId": "thr_stored"
    }
    assert thread.thread_id == "thr_stored"


@pytest.mark.asyncio
async def test_goal_carries_the_immutable_merger_contract(
    merger: CodexMerger, rpc: FakeRpc
) -> None:
    await merger.ensure_thread("demo")

    params = rpc.request_for("thread/goal/set")["params"]
    assert set(params) == {"threadId", "objective", "status"}
    assert params["threadId"] == "thr_demo"
    assert params["status"] == "blocked"
    goal = params["objective"]
    assert "demo" in goal
    lowered = goal.lower()
    for clause in (
        "independent",
        "one pull request at a time",
        "never merge",
        "no corrective edits",
        "conflict",
        "circleci",
        "ancestry",
        "live-state",
    ):
        assert clause in lowered


@pytest.mark.asyncio
async def test_rate_limit_state_uses_chatgpt_surface(
    merger: CodexMerger, rpc: FakeRpc
) -> None:
    rpc.respond("account/rateLimits/read", rate_limit_fixture())

    limits = await merger.read_rate_limits()

    assert limits.primary_used_percent == 25
    assert limits.secondary_used_percent == 10
    assert limits.primary_resets_at == 1767225600
    assert limits.reached is False


@pytest.mark.asyncio
async def test_merge_gate_requires_chatgpt_auth(
    merger: CodexMerger, rpc: FakeRpc
) -> None:
    rpc.respond(
        "account/read",
        {"account": {"type": "apiKey"}, "requiresOpenaiAuth": True},
    )

    health = await merger.verify_chatgpt_auth()

    assert health.eligible is False
    assert health.reason == "chatgpt_auth_required"


@pytest.mark.asyncio
async def test_chatgpt_auth_is_eligible_without_identity(
    merger: CodexMerger, rpc: FakeRpc
) -> None:
    rpc.respond(
        "account/read",
        {
            "account": {"type": "chatgpt", "email": "merger@example.com"},
            "requiresOpenaiAuth": False,
        },
    )

    health = await merger.verify_chatgpt_auth()

    assert health.eligible is True
    assert health.reason == "eligible"
    assert rpc.request_for("account/read")["params"] == {"refreshToken": False}
    assert "merger@example.com" not in repr(health)


@pytest.mark.asyncio
async def test_resume_failure_marks_thread_uncertain(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    stored_thread(database)
    rpc.fail("thread/resume", CodexRequestFailed("thread/resume", -32600))

    with pytest.raises(MergerThreadUncertain, match="operator"):
        await merger.ensure_thread("demo")

    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "uncertain"
    calls_before = list(rpc.methods)
    with pytest.raises(MergerThreadUncertain, match="operator"):
        await merger.ensure_thread("demo")
    assert rpc.methods == [*calls_before, "account/read"]


@pytest.mark.asyncio
async def test_fake_rpc_is_schema_faithful() -> None:
    rpc = FakeRpc()
    with pytest.raises(CodexRequestFailed):
        await rpc.request("thread/start", {"sandbox": "readOnly"}, 5)
    with pytest.raises(CodexRequestFailed):
        await rpc.request(
            "thread/metadata/update",
            {"threadId": "t", "isPinned": True},
            5,
        )
    with pytest.raises(CodexRequestFailed):
        await rpc.request("thread/goal/set", {"threadId": "t", "goal": "x"}, 5)
    with pytest.raises(CodexRequestFailed):
        await rpc.request(
            "thread/goal/set", {"threadId": "t", "status": "idle"}, 5
        )
    with pytest.raises(CodexRequestFailed):
        await rpc.request("threadSection/list", {"name": "Pinned"}, 5)
    with pytest.raises(CodexRequestFailed):
        await rpc.request("thread/section/move", {"threadId": "t"}, 5)
    assert rpc.requests == []


@pytest.mark.asyncio
async def test_pinned_section_is_resolved_across_pagination(
    merger: CodexMerger, rpc: FakeRpc
) -> None:
    rpc.respond_sequence(
        "threadSection/list",
        [
            {
                "data": [{"id": "sec_work", "name": "Work"}],
                "nextCursor": "cursor-1",
            },
            {"data": [{"id": "sec_pinned_2", "name": "Pinned"}]},
        ],
    )

    await merger.ensure_thread("demo")

    listed = [
        params
        for method, params in rpc.requests
        if method == "threadSection/list"
    ]
    assert listed == [{}, {"cursor": "cursor-1"}]
    assert rpc.request_for("thread/section/move")["params"] == {
        "sectionId": "sec_pinned_2",
        "threadId": "thr_demo",
    }


@pytest.mark.asyncio
async def test_missing_pinned_section_fails_closed_then_recovers(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    rpc.respond("threadSection/list", {"data": [{"id": "s", "name": "Work"}]})

    with pytest.raises(ValueError, match="Pinned"):
        await merger.ensure_thread("demo")

    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "configuring"
    assert "thread/section/move" not in rpc.methods

    rpc.respond(
        "threadSection/list",
        {"data": [{"id": "sec_pinned", "name": "Pinned"}]},
    )
    thread = await merger.ensure_thread("demo")

    assert thread.thread_id == "thr_demo"
    assert rpc.methods.count("thread/start") == 1
    assert rpc.request_for("thread/section/move")["params"]["threadId"] == (
        "thr_demo"
    )
    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "ready"


@pytest.mark.asyncio
async def test_ambiguous_pinned_sections_fail_closed(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    rpc.respond(
        "threadSection/list",
        {
            "data": [
                {"id": "sec_a", "name": "Pinned"},
                {"id": "sec_b", "name": "Pinned"},
            ]
        },
    )

    with pytest.raises(ValueError, match="Pinned"):
        await merger.ensure_thread("demo")

    assert "thread/section/move" not in rpc.methods
    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "configuring"


@pytest.mark.asyncio
async def test_timeout_during_thread_start_blocks_duplicate_creation(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    rpc.fail(
        "thread/start", CodexTimeout("thread/start timed out after 60 seconds")
    )

    with pytest.raises(CodexTimeout):
        await merger.ensure_thread("demo")

    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "uncertain"
    assert database.scalar(
        "SELECT thread_id FROM reviewer_channels WHERE project_key = 'demo'"
    ) == ""

    rpc.clear_failures()
    with pytest.raises(MergerThreadUncertain, match="operator"):
        await merger.ensure_thread("demo")
    assert rpc.methods.count("thread/start") == 1


@pytest.mark.asyncio
async def test_disconnect_during_thread_start_blocks_duplicate_creation(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    rpc.fail(
        "thread/start",
        CodexUnavailable("codex app-server exited with exit code 1"),
    )

    with pytest.raises(CodexUnavailable):
        await merger.ensure_thread("demo")

    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "uncertain"

    rpc.clear_failures()
    with pytest.raises(MergerThreadUncertain, match="operator"):
        await merger.ensure_thread("demo")
    assert rpc.methods.count("thread/start") == 1


@pytest.mark.asyncio
async def test_cancellation_during_thread_start_blocks_duplicate_creation(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    rpc.fail("thread/start", asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await merger.ensure_thread("demo")

    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "uncertain"

    rpc.clear_failures()
    with pytest.raises(MergerThreadUncertain, match="operator"):
        await merger.ensure_thread("demo")
    assert rpc.methods.count("thread/start") == 1


@pytest.mark.asyncio
async def test_thread_start_failure_removes_the_empty_reservation(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    rpc.fail("thread/start", CodexRequestFailed("thread/start", -32000))

    with pytest.raises(CodexRequestFailed):
        await merger.ensure_thread("demo")

    assert database.scalar(
        "SELECT count(*) FROM reviewer_channels WHERE project_key = 'demo'"
    ) == 0

    rpc.clear_failures()
    thread = await merger.ensure_thread("demo")
    assert thread.thread_id == "thr_demo"


@pytest.mark.asyncio
async def test_setup_failure_leaves_a_recoverable_configuring_channel(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    rpc.fail("thread/name/set", CodexRequestFailed("thread/name/set", -32000))

    with pytest.raises(CodexRequestFailed):
        await merger.ensure_thread("demo")

    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "configuring"
    assert database.scalar(
        "SELECT thread_id FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "thr_demo"

    rpc.clear_failures()
    thread = await merger.ensure_thread("demo")

    assert thread.thread_id == "thr_demo"
    assert rpc.methods.count("thread/start") == 1
    assert rpc.methods.count("thread/name/set") == 2
    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "ready"
    assert database.scalar(
        "SELECT generation FROM reviewer_channels WHERE project_key = 'demo'"
    ) == 1


@pytest.mark.asyncio
async def test_restart_recovers_a_configuring_channel_without_duplicates(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    stamp = datetime(2026, 8, 27, tzinfo=UTC).isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO reviewer_channels("
            "project_key, thread_id, generation, state, created_at, updated_at"
            ") VALUES ('demo', 'thr_demo', 0, 'configuring', ?, ?)",
            (stamp, stamp),
        )

    thread = await merger.ensure_thread("demo")

    assert thread.thread_id == "thr_demo"
    assert "thread/start" not in rpc.methods
    assert rpc.request_for("thread/name/set")["params"]["threadId"] == "thr_demo"
    assert rpc.request_for("thread/goal/set")["params"]["threadId"] == "thr_demo"
    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "ready"
    assert database.scalar(
        "SELECT generation FROM reviewer_channels WHERE project_key = 'demo'"
    ) == 1


@pytest.mark.asyncio
async def test_thread_start_without_id_fails_closed_as_uncertain(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    rpc.respond("thread/start", {"thread": {}})

    with pytest.raises(ValueError, match="thread id"):
        await merger.ensure_thread("demo")

    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "uncertain"
    with pytest.raises(MergerThreadUncertain, match="operator"):
        await merger.ensure_thread("demo")
    assert rpc.methods.count("thread/start") == 1


@pytest.mark.asyncio
async def test_replacement_during_resume_never_finalizes_the_old_thread(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    stored_thread(database)
    fired = False

    async def replace_on_first_resume(method: str) -> None:
        nonlocal fired
        if method != "thread/resume" or fired:
            return
        fired = True
        merger.begin_replacement(
            "demo",
            expected_thread_id="thr_stored",
            expected_generation=1,
            reason="thread lost",
        )
        merger.complete_replacement(
            "demo",
            expected_thread_id="thr_stored",
            expected_generation=1,
            new_thread_id="thr_new",
        )

    rpc.on_request = replace_on_first_resume

    thread = await merger.ensure_thread("demo")

    assert thread.thread_id == "thr_new"
    resumed = [
        params["threadId"]
        for method, params in rpc.requests
        if method == "thread/resume" and params is not None
    ]
    assert resumed == ["thr_stored", "thr_new"]
    goal_threads = [
        params["threadId"]
        for method, params in rpc.requests
        if method == "thread/goal/set" and params is not None
    ]
    assert goal_threads == ["thr_new"]


@pytest.mark.asyncio
async def test_persistent_replacement_during_resume_fails_closed(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    stored_thread(database)
    counter = 0

    async def always_replace(method: str) -> None:
        nonlocal counter
        if method != "thread/resume":
            return
        counter += 1
        channel = merger.read_channel("demo")
        assert channel is not None
        merger.begin_replacement(
            "demo",
            expected_thread_id=channel.thread_id,
            expected_generation=channel.generation,
            reason="race",
        )
        merger.complete_replacement(
            "demo",
            expected_thread_id=channel.thread_id,
            expected_generation=channel.generation,
            new_thread_id=f"thr_race_{counter}",
        )

    rpc.on_request = always_replace

    with pytest.raises(StaleChannelError):
        await merger.ensure_thread("demo")

    assert "thread/goal/set" not in rpc.methods


@pytest.mark.asyncio
async def test_read_status_reports_missing_ready_and_uncertain(
    merger: CodexMerger, database: Database
) -> None:
    missing = merger.read_status("demo")
    assert missing.state == "missing"
    assert missing.thread_id is None

    stored_thread(database, state="ready")
    ready = merger.read_status("demo")
    assert ready.state == "ready"
    assert ready.thread_id == "thr_stored"


@pytest.mark.asyncio
async def test_interrupt_targets_the_project_thread(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    stored_thread(database)

    await merger.interrupt("demo", "turn_7")

    assert rpc.request_for("turn/interrupt")["params"] == {
        "threadId": "thr_stored",
        "turnId": "turn_7",
    }


@pytest.mark.asyncio
async def test_interrupt_without_thread_fails_closed(
    merger: CodexMerger, rpc: FakeRpc
) -> None:
    with pytest.raises(ValueError, match="thread"):
        await merger.interrupt("demo", "turn_7")
    assert rpc.methods == []


@pytest.mark.asyncio
async def test_unknown_project_fails_closed(
    merger: CodexMerger, rpc: FakeRpc
) -> None:
    with pytest.raises(ValueError, match="project"):
        await merger.ensure_thread("nope")
    with pytest.raises(ValueError, match="project"):
        merger.read_status("nope")
    assert rpc.methods == []


@pytest.mark.asyncio
async def test_channel_replacement_is_compare_and_swap_safe(
    merger: CodexMerger, database: Database
) -> None:
    stored_thread(database)

    replacing = merger.begin_replacement(
        "demo",
        expected_thread_id="thr_stored",
        expected_generation=1,
        reason="thread lost",
    )
    assert replacing.state == "replacing"
    assert replacing.replacement_reason == "thread lost"
    assert replacing.thread_id == "thr_stored"

    completed = merger.complete_replacement(
        "demo",
        expected_thread_id="thr_stored",
        expected_generation=1,
        new_thread_id="thr_new",
    )
    assert completed.thread_id == "thr_new"
    assert completed.generation == 2
    assert completed.state == "ready"
    assert completed.prior_thread_id == "thr_stored"
    assert completed.replacement_reason == "thread lost"


@pytest.mark.asyncio
async def test_stale_replacement_expectations_fail_closed(
    merger: CodexMerger, database: Database
) -> None:
    stored_thread(database)

    with pytest.raises(StaleChannelError):
        merger.begin_replacement(
            "demo",
            expected_thread_id="thr_other",
            expected_generation=1,
            reason="stale thread",
        )
    with pytest.raises(StaleChannelError):
        merger.begin_replacement(
            "demo",
            expected_thread_id="thr_stored",
            expected_generation=7,
            reason="stale generation",
        )
    with pytest.raises(StaleChannelError):
        merger.complete_replacement(
            "demo",
            expected_thread_id="thr_stored",
            expected_generation=1,
            new_thread_id="thr_new",
        )

    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.thread_id == "thr_stored"
    assert channel.generation == 1
    assert channel.state == "ready"


@pytest.mark.asyncio
async def test_replacing_channel_is_not_silently_recreated(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    stored_thread(database, state="replacing")

    with pytest.raises(MergerThreadUncertain, match="operator"):
        await merger.ensure_thread("demo")

    assert rpc.methods == ["account/read"]
    status = merger.read_status("demo")
    assert status.state == "replacing"


@pytest.mark.asyncio
async def test_delivery_metadata_is_generation_bound(
    merger: CodexMerger, database: Database
) -> None:
    stored_thread(database)

    assert merger.record_delivery_failure(
        "demo", thread_id="thr_stored", generation=1
    ) is True
    failed = merger.read_channel("demo")
    assert failed is not None
    assert failed.last_delivery_failure_at is not None
    assert failed.heartbeat_enabled is True
    assert failed.last_delivered_event_id is None

    assert merger.record_delivery_success(
        "demo",
        thread_id="thr_stored",
        generation=1,
        event_id="evt-1",
        candidate_sha="a" * 40,
    ) is True
    delivered = merger.read_channel("demo")
    assert delivered is not None
    assert delivered.last_delivered_event_id == "evt-1"
    assert delivered.last_delivered_candidate_sha == "a" * 40
    assert delivered.heartbeat_enabled is False


@pytest.mark.asyncio
async def test_stale_delivery_metadata_mutations_miss_without_writing(
    merger: CodexMerger, database: Database
) -> None:
    stored_thread(database)

    assert merger.record_delivery_success(
        "demo", thread_id="thr_other", generation=1, event_id="evt-9"
    ) is False
    assert merger.record_delivery_success(
        "demo", thread_id="thr_stored", generation=9, event_id="evt-9"
    ) is False
    assert merger.record_delivery_failure(
        "demo", thread_id="thr_stored", generation=9
    ) is False

    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.last_delivered_event_id is None
    assert channel.last_delivery_failure_at is None
    assert channel.heartbeat_enabled is False


@pytest.mark.asyncio
async def test_ensure_thread_requires_chatgpt_auth_first(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    rpc.respond(
        "account/read",
        {"account": {"type": "apiKey"}, "requiresOpenaiAuth": True},
    )

    with pytest.raises(MergerAuthRequired, match="chatgpt"):
        await merger.ensure_thread("demo")

    assert rpc.methods == ["account/read"]
    assert database.scalar("SELECT count(*) FROM reviewer_channels") == 0

    stored_thread(database)
    with pytest.raises(MergerAuthRequired, match="chatgpt"):
        await merger.ensure_thread("demo")
    assert "thread/read" not in rpc.methods
    assert "thread/resume" not in rpc.methods


@pytest.mark.asyncio
async def test_unreadable_thread_preflight_marks_uncertain(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    stored_thread(database)
    rpc.fail("thread/read", CodexRequestFailed("thread/read", -32600))

    with pytest.raises(MergerThreadUncertain, match="operator"):
        await merger.ensure_thread("demo")

    assert "thread/resume" not in rpc.methods
    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "uncertain"


@pytest.mark.asyncio
async def test_uncertain_marking_cannot_clobber_a_replaced_channel(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    stored_thread(database)
    rpc.fail("thread/read", CodexRequestFailed("thread/read", -32600))

    async def replace_during_preflight(method: str) -> None:
        if method != "thread/read":
            return
        rpc.on_request = None
        merger.begin_replacement(
            "demo",
            expected_thread_id="thr_stored",
            expected_generation=1,
            reason="thread lost",
        )
        merger.complete_replacement(
            "demo",
            expected_thread_id="thr_stored",
            expected_generation=1,
            new_thread_id="thr_new",
        )

    rpc.on_request = replace_during_preflight

    with pytest.raises(MergerThreadUncertain, match="operator"):
        await merger.ensure_thread("demo")

    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.thread_id == "thr_new"
    assert channel.generation == 2
    assert channel.state == "ready"


@pytest.mark.asyncio
async def test_concurrent_creation_reserves_the_channel_durably(
    merger: CodexMerger,
    rpc: FakeRpc,
    database: Database,
    tmp_path: Path,
) -> None:
    rpc_b = FakeRpc()
    database_b = Database.open(tmp_path / "state.db")
    try:
        merger_b = CodexMerger(
            rpc=rpc_b,
            database=database_b,
            projects={
                "demo": ProjectConfig(
                    linear_team="infrastructure",
                    repo_path=Path("/repo/demo"),
                    integration_branch="main",
                    github_repo="j-paterson/demo",
                )
            },
            prompt_file=PROMPT_PATH,
            now=lambda: datetime(2026, 8, 27, tzinfo=UTC),
        )

        async def race_second_caller(method: str) -> None:
            if method != "thread/start":
                return
            rpc.on_request = None
            with pytest.raises(MergerThreadUncertain, match="operator"):
                await merger_b.ensure_thread("demo")

        rpc.on_request = race_second_caller

        thread = await merger.ensure_thread("demo")

        assert thread.thread_id == "thr_demo"
        assert rpc.methods.count("thread/start") == 1
        assert "thread/start" not in rpc_b.methods
        assert "thread/name/set" not in rpc_b.methods
        assert database.scalar(
            "SELECT count(*) FROM reviewer_channels"
        ) == 1
        assert database.scalar(
            "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
        ) == "ready"

        reused = await merger_b.ensure_thread("demo")
        assert reused.thread_id == "thr_demo"
        assert "thread/start" not in rpc_b.methods
        assert "thread/resume" in rpc_b.methods
    finally:
        database_b.close()


def test_prompt_contract_exists_and_is_read_only() -> None:
    text = PROMPT_PATH.read_text(encoding="utf-8").lower()
    for clause in (
        "independent",
        "one pull request at a time",
        "never merge",
        "read-only",
        "critical",
        "important",
        "circleci",
        "ancestry",
        "never poll",
        "cmux",
        "outbox",
        "mutable worktree",
        "child agents",
        "one immutable candidate",
        "metadata-only",
        "generation",
        "no automatic goal continuation",
        "fable_ready",
        "fable_rework_ready",
        "fable_blocked",
        "fable_complete",
        "advisory",
        "rule-restatement",
    ):
        assert clause in text


def test_prompt_idle_terminal_token_is_exact_and_pause_token_is_gone() -> None:
    text = PROMPT_PATH.read_text(encoding="utf-8")
    assert "BLOCKED_ON_EXTERNAL_INTAKE" in text
    assert "PAUSED_NO_ELIGIBLE_WORK" not in text
    without_token = text.replace("BLOCKED_ON_EXTERNAL_INTAKE", "")
    assert "blocked_on_external_intake" not in without_token.lower()
    assert "revalidate" in text.lower()


def test_prompt_requires_prior_ci_reconciliation_before_new_candidates() -> None:
    text = PROMPT_PATH.read_text(encoding="utf-8").lower()
    assert "reconcile" in text
    assert "previous pull request" in text
    assert "never watch or poll ci" in text
    assert "one pull request" in text and "in flight" in text
    assert "pr2" in text
