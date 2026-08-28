"""Verify the argv-safe local codex queue delivery adapter."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.codex_merger import CodexMerger
from hermes_orchestrator.codex_queue import (
    CODEX_QUEUE_BINARY,
    WAKE_STATUSES,
    CodexQueueDelivery,
    WakeEvent,
)
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database


class NullRpc:
    async def request(
        self, method: str, params: dict[str, Any] | None, timeout: float
    ) -> dict[str, Any]:
        raise AssertionError("queue delivery must not use the RPC surface")


class FakeProcess:
    def __init__(self, returncode: int) -> None:
        self.returncode = returncode

    async def wait(self) -> int:
        return self.returncode


class FakeQueueProcessFactory:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], dict[str, Any]]] = []
        self.returncodes: dict[str, int] = {}
        self.on_call: Callable[[str], None] | None = None

    async def __call__(self, *command: str, **kwargs: Any) -> FakeProcess:
        self.calls.append((command, kwargs))
        thread_id = command[command.index("--thread") + 1]
        if self.on_call is not None:
            self.on_call(thread_id)
        return FakeProcess(self.returncodes.get(thread_id, 0))

    def thread_targets(self) -> list[str]:
        return [
            command[command.index("--thread") + 1]
            for command, _ in self.calls
        ]

    def messages(self) -> list[str]:
        return [
            command[command.index("--message") + 1]
            for command, _ in self.calls
        ]


def wake_event() -> WakeEvent:
    return WakeEvent(
        status="FABLE_READY",
        issue_id="ENG-9",
        candidate_sha="1" * 40,
        base_sha="2" * 40,
        manifest_path="/state/manifests/evt-1.json",
        event_id="evt-1",
    )


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def merger(database: Database) -> CodexMerger:
    prompt = Path(__file__).parent.parent / "prompts" / "codex-merger.md"
    return CodexMerger(
        rpc=NullRpc(),
        database=database,
        projects={
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=Path("/repo/demo"),
                integration_branch="main",
                github_repo="j-paterson/demo",
            )
        },
        prompt_file=prompt,
        now=lambda: datetime(2026, 8, 27, tzinfo=UTC),
    )


@pytest.fixture
def factory() -> FakeQueueProcessFactory:
    return FakeQueueProcessFactory()


@pytest.fixture
def delivery(
    merger: CodexMerger, factory: FakeQueueProcessFactory
) -> CodexQueueDelivery:
    return CodexQueueDelivery(channels=merger, process_factory=factory)


def stored_channel(
    database: Database, thread_id: str = "thr_stored", generation: int = 1
) -> None:
    stamp = datetime(2026, 8, 27, tzinfo=UTC).isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO reviewer_channels("
            "project_key, thread_id, generation, state, created_at, updated_at"
            ") VALUES ('demo', ?, ?, 'ready', ?, ?)",
            (thread_id, generation, stamp, stamp),
        )


def test_wake_events_accept_only_real_fable_statuses() -> None:
    assert frozenset(
        {
            "FABLE_READY",
            "FABLE_REWORK_READY",
            "FABLE_BLOCKED",
            "FABLE_COMPLETE",
        }
    ) == WAKE_STATUSES
    with pytest.raises(ValueError, match="status"):
        WakeEvent(
            status="HEARTBEAT",
            issue_id="ENG-9",
            candidate_sha="1" * 40,
            base_sha="2" * 40,
            manifest_path="/state/manifests/evt-1.json",
            event_id="evt-1",
        )
    with pytest.raises(ValueError, match="empty"):
        WakeEvent(
            status="FABLE_READY",
            issue_id="",
            candidate_sha="1" * 40,
            base_sha="2" * 40,
            manifest_path="/state/manifests/evt-1.json",
            event_id="evt-1",
        )


@pytest.mark.asyncio
async def test_delivery_uses_exact_argv_without_a_shell(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
) -> None:
    stored_channel(database)

    result = await delivery.deliver("demo", wake_event())

    assert result.delivered is True
    assert result.attempts == 1
    assert result.thread_id == "thr_stored"
    assert result.generation == 1
    command, kwargs = factory.calls[0]
    assert command == (
        CODEX_QUEUE_BINARY,
        "queue",
        "--thread",
        "thr_stored",
        "--message",
        "FABLE_READY issue=ENG-9 candidate=" + "1" * 40 + " base="
        + "2" * 40
        + " manifest=/state/manifests/evt-1.json event=evt-1 generation=1",
    )
    assert "shell" not in kwargs
    assert "exec" not in command
    assert "resume" not in command
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.last_delivered_event_id == "evt-1"
    assert channel.last_delivered_candidate_sha == "1" * 40


@pytest.mark.asyncio
async def test_message_renders_against_the_invoked_generation(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    database: Database,
) -> None:
    stored_channel(database, thread_id="thr_new", generation=3)

    result = await delivery.deliver("demo", wake_event())

    assert result.delivered is True
    assert result.generation == 3
    assert factory.messages()[0].endswith("generation=3")


@pytest.mark.asyncio
async def test_cli_failure_is_bounded_and_never_claims_delivery(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
) -> None:
    stored_channel(database)
    factory.returncodes["thr_stored"] = 2

    result = await delivery.deliver("demo", wake_event())

    assert result.delivered is False
    assert result.reason == "queue_cli_failed"
    assert result.attempts == 1
    assert len(factory.calls) == 1
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.last_delivered_event_id is None
    assert channel.last_delivery_failure_at is not None
    assert channel.heartbeat_enabled is True


@pytest.mark.asyncio
async def test_spawn_error_records_bounded_failure(
    merger: CodexMerger, database: Database
) -> None:
    stored_channel(database)

    async def broken_factory(*command: str, **kwargs: Any) -> FakeProcess:
        raise FileNotFoundError("codex binary missing")

    delivery = CodexQueueDelivery(
        channels=merger, process_factory=broken_factory
    )
    result = await delivery.deliver("demo", wake_event())

    assert result.delivered is False
    assert result.reason == "spawn_failed"
    assert result.attempts == 1
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.last_delivered_event_id is None
    assert channel.last_delivery_failure_at is not None
    assert channel.heartbeat_enabled is True


@pytest.mark.asyncio
async def test_failed_delivery_retries_once_with_rebuilt_message(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
) -> None:
    stored_channel(database, thread_id="thr_old", generation=1)
    factory.returncodes["thr_old"] = 1

    def replace_when_old_thread_is_used(thread_id: str) -> None:
        if thread_id != "thr_old":
            return
        merger.begin_replacement(
            "demo",
            expected_thread_id="thr_old",
            expected_generation=1,
            reason="thread lost",
        )
        merger.complete_replacement(
            "demo",
            expected_thread_id="thr_old",
            expected_generation=1,
            new_thread_id="thr_new",
        )

    factory.on_call = replace_when_old_thread_is_used

    result = await delivery.deliver("demo", wake_event())

    assert result.delivered is True
    assert result.attempts == 2
    assert result.thread_id == "thr_new"
    assert result.generation == 2
    assert factory.thread_targets() == ["thr_old", "thr_new"]
    assert factory.messages()[0].endswith("generation=1")
    assert factory.messages()[1].endswith("generation=2")


@pytest.mark.asyncio
async def test_old_thread_success_is_never_claimed_on_the_new_channel(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
) -> None:
    stored_channel(database, thread_id="thr_old", generation=1)

    def replace_during_successful_old_wake(thread_id: str) -> None:
        if thread_id != "thr_old":
            return
        merger.begin_replacement(
            "demo",
            expected_thread_id="thr_old",
            expected_generation=1,
            reason="thread lost",
        )
        merger.complete_replacement(
            "demo",
            expected_thread_id="thr_old",
            expected_generation=1,
            new_thread_id="thr_new",
        )

    factory.on_call = replace_during_successful_old_wake

    result = await delivery.deliver("demo", wake_event())

    assert result.delivered is True
    assert result.attempts == 2
    assert result.thread_id == "thr_new"
    assert result.generation == 2
    assert factory.thread_targets() == ["thr_old", "thr_new"]
    assert factory.messages()[1].endswith("generation=2")
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.generation == 2
    assert channel.last_delivered_event_id == "evt-1"


@pytest.mark.asyncio
async def test_stale_success_without_redelivery_path_reports_reconciliation(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
) -> None:
    stored_channel(database, thread_id="thr_old", generation=1)

    def begin_replacing_during_wake(thread_id: str) -> None:
        if thread_id == "thr_old" and factory.on_call is not None:
            factory.on_call = None
            merger.begin_replacement(
                "demo",
                expected_thread_id="thr_old",
                expected_generation=1,
                reason="thread lost",
            )

    factory.on_call = begin_replacing_during_wake

    result = await delivery.deliver("demo", wake_event())

    assert result.delivered is False
    assert result.reason == "redelivery_required"
    assert result.attempts == 1
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.last_delivered_event_id is None


@pytest.mark.asyncio
async def test_failure_without_generation_change_does_not_retry(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    database: Database,
) -> None:
    stored_channel(database)
    factory.returncodes["thr_stored"] = 1

    result = await delivery.deliver("demo", wake_event())

    assert result.delivered is False
    assert result.attempts == 1
    assert len(factory.calls) == 1


@pytest.mark.asyncio
async def test_unusable_channel_fails_closed_without_invocation(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    database: Database,
) -> None:
    missing = await delivery.deliver("demo", wake_event())
    assert missing.delivered is False
    assert missing.reason == "channel_unavailable"
    assert missing.attempts == 0

    stamp = datetime(2026, 8, 27, tzinfo=UTC).isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO reviewer_channels("
            "project_key, thread_id, generation, state, created_at, updated_at"
            ") VALUES ('demo', 'thr_stored', 1, 'uncertain', ?, ?)",
            (stamp, stamp),
        )
    uncertain = await delivery.deliver("demo", wake_event())
    assert uncertain.delivered is False
    assert uncertain.reason == "channel_unavailable"
    assert factory.calls == []


@pytest.mark.asyncio
async def test_timeout_kills_the_entire_owned_process_group(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    marker = tmp_path / "late-delivery.txt"
    pid_file = tmp_path / "queue-child.pid"
    script = tmp_path / "fake_codex.py"
    write_group_script(
        script, marker, pid_file, parent_ignores_term=False
    )

    delivery = CodexQueueDelivery(
        channels=merger,
        process_factory=script_factory(script),
        timeout=0.3,
        termination_timeout=0.2,
    )

    result = await delivery.deliver("demo", wake_event())

    assert result.delivered is False
    child_pid = await read_pid(pid_file)
    await asyncio.sleep(1.2)
    assert not marker.exists()
    with pytest.raises(ProcessLookupError):
        os.kill(child_pid, 0)


@pytest.mark.asyncio
async def test_cancellation_kills_the_group_and_propagates(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    marker = tmp_path / "late-delivery.txt"
    pid_file = tmp_path / "queue-child.pid"
    script = tmp_path / "fake_codex.py"
    write_group_script(script, marker, pid_file, parent_ignores_term=True)

    delivery = CodexQueueDelivery(
        channels=merger,
        process_factory=script_factory(script),
        timeout=30.0,
        termination_timeout=0.2,
    )

    task = asyncio.create_task(delivery.deliver("demo", wake_event()))
    child_pid = await read_pid(pid_file)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.sleep(1.2)
    assert not marker.exists()
    with pytest.raises(ProcessLookupError):
        os.kill(child_pid, 0)


def write_group_script(
    script: Path, marker: Path, pid_file: Path, *, parent_ignores_term: bool
) -> None:
    """Write a stand-in codex binary whose descendant ignores SIGTERM."""

    child_code = (
        "import signal, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        f'time.sleep(1.0); open("{marker}", "a").write("child-late")'
    )
    parent_signal = (
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        if parent_ignores_term
        else ""
    )
    script.write_text(
        "import signal, subprocess, sys, time\n"
        + parent_signal
        + f"child = subprocess.Popen([sys.executable, '-c', '''{child_code}'''])\n"
        f"open('{pid_file}', 'w').write(str(child.pid))\n"
        "time.sleep(1.0)\n"
        f"open('{marker}', 'a').write('parent-late')\n",
        encoding="utf-8",
    )


def script_factory(script: Path) -> Any:
    async def real_factory(
        *command: str, **kwargs: Any
    ) -> asyncio.subprocess.Process:
        return await asyncio.create_subprocess_exec(
            sys.executable, str(script), *command[1:], **kwargs
        )

    return real_factory


async def read_pid(pid_file: Path) -> int:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if pid_file.exists():
            text = pid_file.read_text(encoding="utf-8")
            if text.strip():
                return int(text)
        await asyncio.sleep(0.02)
    raise AssertionError("stand-in codex child pid was never written")
