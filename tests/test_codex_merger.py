"""Verify persistent read-only project Codex Merger threads."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

import pytest

from hermes_orchestrator.codex_merger import (
    MERGER_CONTRACT_VERSION,
    MERGER_MODEL,
    MERGER_PROVIDER,
    CodexMerger,
    ContractAwareDelivery,
    MergerAuthRequired,
    MergerModelMismatch,
    MergerThreadUncertain,
    StaleChannelError,
)
from hermes_orchestrator.codex_ponytail_guard import session_guard_config
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
        # INFRA-200: used only to deliver the versioned Sol contract text
        # into a thread as a plain message turn (deliver_contract).
        "turn/start": {"threadId", "message"},
    }
    _REQUIRED_KEYS: ClassVar[dict[str, set[str]]] = {
        "thread/read": {"threadId"},
        "thread/resume": {"threadId"},
        "thread/name/set": {"threadId", "name"},
        "thread/goal/set": {"threadId"},
        "thread/metadata/update": {"threadId"},
        "thread/section/move": {"sectionId", "threadId"},
        "turn/interrupt": {"threadId", "turnId"},
        "turn/start": {"threadId", "message"},
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
    model: str | None = None,
    provider: str | None = None,
    model_verified_at: str | None = None,
) -> None:
    stamp = datetime(2026, 8, 27, tzinfo=UTC).isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO reviewer_channels("
            "project_key, thread_id, generation, state, model, provider, "
            "model_verified_at, created_at, updated_at"
            ") VALUES ('demo', ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                thread_id,
                generation,
                state,
                model,
                provider,
                model_verified_at,
                stamp,
                stamp,
            ),
        )


@pytest.mark.asyncio
async def test_new_merger_uses_the_writable_workspace_and_persists(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    """INFRA-194 operator scope: the bounded reviewer-fix path needs
    the narrow writable workspace mode — never read-only, and never
    the dangerous unrestricted mode."""

    thread = await merger.ensure_thread("demo")

    assert rpc.methods[0] == "account/read"
    request = rpc.request_for("thread/start")
    assert request["params"] == {
        "model": "gpt-5.6-sol",
        "cwd": "/repo/demo",
        "approvalPolicy": "never",
        "sandbox": "workspace-write",
        "serviceName": "hermes_orchestrator",
        "config": session_guard_config(),
    }
    assert request["params"]["sandbox"] != "danger-full-access"
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
async def test_new_thread_receives_versioned_contract_at_launch(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    """INFRA-200: a brand-new thread receives the full versioned
    forward-implementation-first contract as its own turn at launch, and
    the delivery is durably recorded against the exact (thread_id,
    generation) the channel just went ready under."""

    await merger.ensure_thread("demo")

    turn = rpc.request_for("turn/start")["params"]
    assert turn["threadId"] == "thr_demo"
    assert turn["message"].startswith(
        f"Hermes Sol Merger contract {MERGER_CONTRACT_VERSION}"
    )
    assert "Sol merge lead" in turn["message"]
    # The contract turn is sent AFTER the channel is durably ready, not
    # before -- a launch-time RPC hiccup here must never leave a brand
    # new thread stuck in "configuring".
    assert rpc.methods.index("turn/start") > rpc.methods.index("thread/goal/set")
    assert database.scalar(
        "SELECT contract_version FROM reviewer_channels "
        "WHERE project_key = 'demo'"
    ) == MERGER_CONTRACT_VERSION
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.contract_version == MERGER_CONTRACT_VERSION
    assert channel.generation == 1


@pytest.mark.asyncio
async def test_idle_thread_not_woken_for_contract(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    """INFRA-200: resuming an already-ready thread sends only the durable
    goal, exactly as before -- an idle thread is never woken solely to
    restate the contract, and its (NULL, meaning never-delivered)
    ``contract_version`` is left untouched for a real intake to adopt
    later."""

    stored_thread(database)  # state="ready"; contract_version stays NULL

    thread = await merger.ensure_thread("demo")

    assert thread.thread_id == "thr_stored"
    assert rpc.methods == [
        "account/read",
        "thread/read",
        "thread/goal/set",
    ]
    assert "turn/start" not in rpc.methods
    assert database.scalar(
        "SELECT contract_version FROM reviewer_channels "
        "WHERE project_key = 'demo'"
    ) is None


@pytest.mark.asyncio
async def test_existing_thread_is_resumed_not_recreated(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    stored_thread(database)

    thread = await merger.ensure_thread("demo")

    assert rpc.methods == [
        "account/read",
        "thread/read",
        "thread/goal/set",
    ]
    assert rpc.request_for("thread/read")["params"] == {
        "threadId": "thr_stored"
    }
    assert "thread/resume" not in rpc.methods
    assert thread.thread_id == "thr_stored"


@pytest.mark.asyncio
async def test_not_loaded_thread_is_loaded_then_readable(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    stored_thread(database)
    rpc.respond_sequence(
        "thread/read",
        [
            {"thread": {"id": "thr_stored", "status": {"type": "notLoaded"}}},
            {"thread": {"id": "thr_stored", "status": {"type": "idle"}}},
        ],
    )

    thread = await merger.ensure_thread("demo")

    assert thread.thread_id == "thr_stored"
    assert rpc.methods == [
        "account/read",
        "thread/read",
        "thread/resume",
        "thread/read",
        "thread/goal/set",
    ]
    # Recovery re-applies the bounded writable workspace mode and the
    # session-scoped Ponytail guard binding, so a task started under a
    # stale configuration is corrected on its next load — without a
    # duplicate thread or review intake.
    assert rpc.request_for("thread/resume")["params"] == {
        "threadId": "thr_stored",
        "sandbox": "workspace-write",
        "config": session_guard_config(),
    }
    assert "thread/start" not in rpc.methods


@pytest.mark.asyncio
async def test_rejected_resume_of_a_readable_thread_stays_ready(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    """Live: a fresh App Server reads the persisted task but rejects resume."""

    stored_thread(database)
    rpc.respond(
        "thread/read", {"thread": {"id": "thr_stored", "status": {"type": "notLoaded"}}}
    )
    rpc.fail("thread/resume", CodexRequestFailed("thread/resume", -32600))

    thread = await merger.ensure_thread("demo")

    assert thread.thread_id == "thr_stored"
    assert database.scalar(
        "SELECT state FROM reviewer_channels WHERE project_key = 'demo'"
    ) == "ready"
    assert rpc.methods == [
        "account/read",
        "thread/read",
        "thread/resume",
        "thread/read",
        "thread/goal/set",
    ]


@pytest.mark.asyncio
async def test_goal_is_one_succinct_paragraph_of_role_and_boundaries(
    merger: CodexMerger, rpc: FakeRpc
) -> None:
    """INFRA-194 operator requirement: the durable goal states only the
    role, its boundaries, and the idle token in exactly one paragraph;
    detailed protocol mechanics live in durable artifacts and code."""

    await merger.ensure_thread("demo")

    params = rpc.request_for("thread/goal/set")["params"]
    assert set(params) == {"threadId", "objective", "status"}
    assert params["threadId"] == "thr_demo"
    assert params["status"] == "blocked"
    goal = params["objective"]
    assert "demo" in goal
    # Exactly one paragraph: no blank lines, no headings, no lists.
    assert "\n" not in goal
    lowered = goal.lower()
    for clause in (
        "independent reviewer",
        "one immutable candidate at a time",
        "accept_with_reviewer_fix",
        "durable repair policy",
        "judgment-bearing",
        "returns to the fable lead as structured corrections",
        "complete the exact approved pull-request merge yourself",
        "guarded hermes settlement helper",
        "direct merge is permitted",
        "reconciled into the same durable receipts",
        "circleci is checked optimistically at intake and merge",
        "blocked_on_external_intake",
    ):
        assert clause in lowered
    # Mechanics stay out of the goal; the durable artifact is named.
    assert "prompts/codex-merger.md" in goal
    assert "## " not in goal


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
    """Only an unreadable thread is uncertain; a rejected resume alone is not."""

    stored_thread(database)
    rpc.respond_sequence(
        "thread/read",
        [{"thread": {"id": "thr_stored", "status": {"type": "notLoaded"}}}],
    )
    rpc.fail("thread/resume", CodexRequestFailed("thread/resume", -32600))

    async def unreadable_after_resume(method: str) -> None:
        if method == "thread/resume":
            rpc.fail("thread/read", CodexRequestFailed("thread/read", -32600))

    rpc.on_request = unreadable_after_resume

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
        if method != "thread/read" or fired:
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
    read = [
        params["threadId"]
        for method, params in rpc.requests
        if method == "thread/read" and params is not None
    ]
    assert read == ["thr_stored", "thr_new"]
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
        if method != "thread/read":
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
async def test_replacement_resets_contract_version(
    merger: CodexMerger, database: Database
) -> None:
    """INFRA-200: a replacement thread is a new durable identity, so it
    must re-adopt the contract at its own next real intake rather than
    silently inheriting the prior thread's delivery receipt."""

    stored_thread(database)
    assert merger.record_contract_delivered(
        "demo", thread_id="thr_stored", generation=1
    ) is True
    delivered = merger.read_channel("demo")
    assert delivered is not None
    assert delivered.contract_version == MERGER_CONTRACT_VERSION

    merger.begin_replacement(
        "demo",
        expected_thread_id="thr_stored",
        expected_generation=1,
        reason="thread lost",
    )
    completed = merger.complete_replacement(
        "demo",
        expected_thread_id="thr_stored",
        expected_generation=1,
        new_thread_id="thr_new",
    )

    assert completed.thread_id == "thr_new"
    assert completed.generation == 2
    assert completed.contract_version is None


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
        assert "thread/read" in rpc_b.methods
        assert "thread/resume" not in rpc_b.methods
    finally:
        database_b.close()


def wake_fixture(root: Path) -> Any:
    from hermes_orchestrator.manifests import (
        MANIFEST_VERSION,
        CandidateManifest,
        wake_event_for,
        write_manifest,
    )

    root.mkdir(exist_ok=True)
    manifest = CandidateManifest(
        manifest_version=MANIFEST_VERSION,
        event_id="evt-1",
        status="FABLE_READY",
        candidate_sha="1" * 40,
        base_sha="2" * 40,
        branch="feature/eng-9",
        linear_issues=("ENG-9",),
        changed_files=("src/app.py",),
        verification=(("uv run pytest -q", "142 passed"),),
        blockers=(),
        created_at="2026-08-27T12:00:00+00:00",
    )
    path = root / "evt-1.json"
    if not path.exists():
        write_manifest(root, manifest, head_sha="1" * 40)
    return wake_event_for(manifest, path)


@dataclass(frozen=True, slots=True)
class _FakeDeliveryResult:
    delivered: bool
    thread_id: str | None
    generation: int | None
    reason: str = "delivered"


class _FakeDelivery:
    """A ``WakeDeliverer`` double that never touches the network.

    Records exactly what ``ContractAwareDelivery`` hands it, so a test
    can assert both the message it received and that it was called
    exactly once -- proving no extra turn was ever sent for the
    contract alone.
    """

    def __init__(self, *, thread_id: str, generation: int) -> None:
        self.calls: list[tuple[str, Any]] = []
        self._thread_id = thread_id
        self._generation = generation

    async def deliver(self, project_key: str, event: Any) -> _FakeDeliveryResult:
        self.calls.append((project_key, event))
        return _FakeDeliveryResult(
            delivered=True,
            thread_id=self._thread_id,
            generation=self._generation,
        )


@pytest.mark.asyncio
async def test_existing_thread_adopts_contract_at_next_real_intake(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    """INFRA-200: an existing thread whose ``contract_version`` is still
    NULL (it predates this feature, or its launch-time delivery failed)
    adopts the contract prepended to its next real candidate intake
    message -- one delivery call, one message, no separate turn for the
    contract alone -- and the column is only updated once that delivery
    is confirmed."""

    stored_thread(database)  # state="ready"; contract_version is NULL
    event = wake_fixture(tmp_path / "wake-manifests")
    inner = _FakeDelivery(thread_id="thr_stored", generation=1)
    wrapped = ContractAwareDelivery(merger=merger, inner=inner)

    result = await wrapped.deliver("demo", event)

    assert result.delivered is True
    assert len(inner.calls) == 1
    delivered_project_key, delivered_event = inner.calls[0]
    assert delivered_project_key == "demo"
    rendered = delivered_event.render(1)
    assert rendered.startswith(f"Hermes Sol Merger contract {MERGER_CONTRACT_VERSION}")
    assert "Sol merge lead" in rendered
    # The wrapped event still carries the exact original wake fields.
    assert delivered_event.event_id == event.event_id
    assert delivered_event.status == event.status
    assert database.scalar(
        "SELECT contract_version FROM reviewer_channels "
        "WHERE project_key = 'demo'"
    ) == MERGER_CONTRACT_VERSION

    # A second real intake, now that the channel has adopted the
    # contract, is delivered completely unprefixed -- a transparent
    # pass-through of the caller's own event.
    inner.calls.clear()
    await wrapped.deliver("demo", event)
    assert len(inner.calls) == 1
    _, second_event = inner.calls[0]
    assert second_event is event
    assert second_event.render(1) == event.render(1)


@pytest.mark.asyncio
async def test_channel_activation_recomputes_heartbeat_for_early_wakes(
    merger: CodexMerger, rpc: FakeRpc, database: Database, tmp_path: Path
) -> None:
    from hermes_orchestrator.codex_queue import CodexQueueDelivery

    root = tmp_path / "wake-manifests"
    event = wake_fixture(root)
    spawned: list[tuple[str, ...]] = []

    async def forbidden_factory(*command: str, **kwargs: Any) -> Any:
        spawned.append(command)
        raise AssertionError("no process may spawn before fallback")

    delivery = CodexQueueDelivery(
        channels=merger,
        manifest_root=root,
        process_factory=forbidden_factory,
    )

    early = await delivery.deliver("demo", event)
    assert early.delivered is False
    assert early.reason == "channel_unavailable"
    assert spawned == []
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "pending"
    assert merger.pending_heartbeat_wakes("demo", manifest_root=root) == []

    await merger.ensure_thread("demo")

    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.state == "ready"
    assert channel.heartbeat_enabled is True
    assert merger.pending_heartbeat_wakes("demo", manifest_root=root) == [
        event
    ]
    assert spawned == []

    restarted_db = Database.open(tmp_path / "state.db")
    try:
        restarted = CodexMerger(
            rpc=FakeRpc(),
            database=restarted_db,
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
        rechannel = restarted.read_channel("demo")
        assert rechannel is not None
        assert rechannel.heartbeat_enabled is True
        assert restarted.pending_heartbeat_wakes(
            "demo", manifest_root=root
        ) == [event]
    finally:
        restarted_db.close()


def test_replacement_preserves_heartbeat_for_outstanding_wakes(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    from hermes_orchestrator.manifests import read_manifest_snapshot

    stored_thread(database)
    root = tmp_path / "wake-manifests"
    event = wake_fixture(root)
    snapshot = read_manifest_snapshot(
        Path(event.manifest_path),
        root=root,
        expected_digest=event.manifest_digest,
    )
    registration = merger.register_wake("demo", event, manifest=snapshot)
    assert registration.claim_token is not None
    merger.record_wake_attempt_failed(
        "demo", event.event_id, claim_token=registration.claim_token
    )

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

    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.heartbeat_enabled is True
    assert merger.pending_heartbeat_wakes("demo", manifest_root=root) == [
        event
    ]


def test_replacement_without_outstanding_wakes_clears_heartbeat(
    merger: CodexMerger, database: Database
) -> None:
    stored_thread(database)
    merger.record_delivery_failure("demo", thread_id="thr_stored", generation=1)

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

    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.heartbeat_enabled is False


def test_goal_and_contract_authorize_only_bounded_fix_and_exact_merge() -> None:
    """INFRA-194 operator scope: Sol's authority is exactly the bounded
    labeled reviewer fix plus completing the exact approved merge, and
    neither document claims an OS-enforced read-only sandbox. The
    one-paragraph prompt contract itself (operator correction 719cd2ad)
    is pinned in tests/test_merger_prompt.py."""

    from hermes_orchestrator.codex_merger import MERGER_GOAL

    goal = MERGER_GOAL.lower()
    assert "only bounded mechanical" in goal
    assert "accept_with_reviewer_fix" in goal
    assert "complete the exact approved pull-request merge yourself" in goal
    assert "read-only" not in goal
    assert "\n" not in MERGER_GOAL  # still exactly one paragraph

    contract = " ".join(PROMPT_PATH.read_text(encoding="utf-8").lower().split())
    assert "read-only sandbox" not in contract


def test_ponytail_guard_binding_shape_is_the_minimal_session_hook() -> None:
    """Operator correction c3f4aad5: the guard binding is one
    session-scoped ``hooks.PreToolUse`` config override — a single
    synchronous command hook running the guard script — and nothing
    else: no global git hook, no shared Codex config write, no receipt
    or manifest machinery."""

    config = session_guard_config(python="/opt/venv/bin/python")
    assert set(config) == {"hooks.PreToolUse"}
    (group,) = config["hooks.PreToolUse"]
    assert set(group) == {"hooks"}  # no matcher: the guard filters itself
    (handler,) = group["hooks"]
    assert handler["type"] == "command"
    assert handler["async"] is False
    assert handler["command"] == (
        "/opt/venv/bin/python -m hermes_orchestrator.codex_ponytail_guard hook"
    )


@pytest.mark.asyncio
async def test_managed_sol_thread_carries_the_ponytail_guard_binding(
    merger: CodexMerger, rpc: FakeRpc
) -> None:
    """Operator correction c3f4aad5: the managed Sol thread — created or
    resumed — always carries the session-scoped Ponytail guard in its
    thread configuration, alongside the bounded writable workspace."""

    await merger.ensure_thread("demo")

    params = rpc.request_for("thread/start")["params"]
    assert params["config"] == session_guard_config()
    assert params["sandbox"] == "workspace-write"


@pytest.mark.asyncio
async def test_resumed_sol_thread_reapplies_the_ponytail_guard_binding(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    stored_thread(database)
    rpc.respond_sequence(
        "thread/read",
        [
            {"thread": {"id": "thr_stored", "status": {"type": "notLoaded"}}},
            {"thread": {"id": "thr_stored", "status": {"type": "idle"}}},
        ],
    )

    await merger.ensure_thread("demo")

    params = rpc.request_for("thread/resume")["params"]
    assert params["config"] == session_guard_config()


def test_ponytail_guard_binds_nowhere_but_the_managed_sol_boundary() -> None:
    """Only managed-Sol launch boundaries reference the guard, so no other
    Codex session, launch surface, or agent lead is subject to it."""

    import hermes_orchestrator

    package_root = Path(hermes_orchestrator.__file__).parent
    binders = {
        "codex_merger.py",
        "codex_ponytail_guard.py",
        "codex_queue.py",
    }
    for module in sorted(package_root.rglob("*.py")):
        if module.name in binders:
            continue
        source = module.read_text(encoding="utf-8")
        assert "codex_ponytail_guard" not in source, module.name


# --- INFRA-187: Sol model identity persisted and validated -----------------


@pytest.mark.asyncio
async def test_creation_persists_sol_model_and_provider(
    merger: CodexMerger, database: Database
) -> None:
    """A freshly created channel is proven Sol as soon as it is ready."""

    await merger.ensure_thread("demo")

    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.model == MERGER_MODEL
    assert channel.provider == MERGER_PROVIDER
    assert channel.model_verified_at is not None
    assert merger.model_proven("demo") is True


@pytest.mark.asyncio
async def test_recovery_refuses_mismatched_model(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    """A channel proven under a different model fails closed on recovery,
    before any RPC beyond the auth check, and its row stays untouched."""

    stored_thread(
        database,
        model="gpt-5.6-other",
        provider=MERGER_PROVIDER,
        model_verified_at=datetime(2026, 8, 1, tzinfo=UTC).isoformat(),
    )

    with pytest.raises(MergerModelMismatch):
        await merger.ensure_thread("demo")

    assert rpc.methods == ["account/read"]
    assert "thread/read" not in rpc.methods
    assert "thread/resume" not in rpc.methods
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.model == "gpt-5.6-other"
    assert channel.state == "ready"
    assert merger.model_proven("demo") is False


@pytest.mark.asyncio
async def test_recovery_reconciles_unproven_legacy_channel(
    merger: CodexMerger, rpc: FakeRpc, database: Database
) -> None:
    """A NULL-model pre-migration channel is reconciled: once the
    authenticated resume succeeds under our Sol configuration, the
    reconciliation proof is durably written."""

    stored_thread(database)  # model/provider/model_verified_at all NULL
    rpc.respond_sequence(
        "thread/read",
        [
            {"thread": {"id": "thr_stored", "status": {"type": "notLoaded"}}},
            {"thread": {"id": "thr_stored", "status": {"type": "idle"}}},
        ],
    )

    before = merger.read_channel("demo")
    assert before is not None
    assert before.model is None
    assert merger.model_proven("demo") is False

    thread = await merger.ensure_thread("demo")

    assert thread.thread_id == "thr_stored"
    assert "thread/resume" in rpc.methods
    after = merger.read_channel("demo")
    assert after is not None
    assert after.model == MERGER_MODEL
    assert after.provider == MERGER_PROVIDER
    assert after.model_verified_at is not None
    assert merger.model_proven("demo") is True


@pytest.mark.asyncio
async def test_replacement_with_non_sol_model_fails_closed(
    merger: CodexMerger, database: Database
) -> None:
    stored_thread(
        database,
        model=MERGER_MODEL,
        provider=MERGER_PROVIDER,
        model_verified_at=datetime(2026, 8, 1, tzinfo=UTC).isoformat(),
    )
    merger.begin_replacement(
        "demo",
        expected_thread_id="thr_stored",
        expected_generation=1,
        reason="thread lost",
    )

    with pytest.raises(MergerModelMismatch):
        merger.complete_replacement(
            "demo",
            expected_thread_id="thr_stored",
            expected_generation=1,
            new_thread_id="thr_new",
            model="gpt-5.6-other",
        )

    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.state == "replacing"
    assert channel.generation == 1
    assert channel.thread_id == "thr_stored"
    assert channel.model == MERGER_MODEL


def test_constructor_refuses_non_sol_model(
    database: Database, rpc: FakeRpc
) -> None:
    with pytest.raises(ValueError, match="gpt-5\\.6-sol"):
        CodexMerger(
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
            model="gpt-5.6-other",
        )


def test_contract_aware_delivery_exposes_the_owner_path_of_its_inner_delivery() -> None:
    # INFRA-223: the turn service reads the desktop owner adapter through
    # this wrapper, so the facts must pass through unchanged.
    import types

    from hermes_orchestrator.codex_merger import ContractAwareDelivery

    inner = types.SimpleNamespace(owner_endpoint_available=False, _owner_start=None)
    wrapper = ContractAwareDelivery(merger=types.SimpleNamespace(), inner=inner)  # type: ignore[arg-type]
    assert wrapper.owner_endpoint_available is False
    assert wrapper._owner_start is None
    owner = object()
    inner = types.SimpleNamespace(owner_endpoint_available=True, _owner_start=owner)
    wrapper = ContractAwareDelivery(merger=types.SimpleNamespace(), inner=inner)  # type: ignore[arg-type]
    assert wrapper.owner_endpoint_available is True
    assert wrapper._owner_start is owner
