"""Verify launchers register exact process groups and fail closed."""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from hermes_orchestrator.claude import ClaudeRunner, LeadTurnRequest
from hermes_orchestrator.codex_merger import CodexMerger
from hermes_orchestrator.codex_queue import CodexQueueDelivery
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.processes import ProcessRegistry
from hermes_orchestrator.profiles import ProfileRegistry
from hermes_orchestrator.service import OrchestratorService
from tests.test_codex_merger import FakeRpc
from tests.test_review_intake import stored_channel


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def processes(database: Database) -> ProcessRegistry:
    return ProcessRegistry(database, EventStore(database), grace_seconds=1.0)


def registry(tmp_path: Path) -> ProfileRegistry:
    path = tmp_path / "profiles.yaml"
    path.write_text(
        "profiles:\n"
        + "".join(
            f"  - alias: max-{name}\n"
            f"    config_dir: {tmp_path / f'.claude-max-{name}'}\n"
            for name in "abcd"
        ),
        encoding="utf-8",
    )
    return ProfileRegistry.load(path)


class RealChildFactory:
    """Spawns a real python child that prints one init event and lingers."""

    def __init__(self, linger: float = 30.0) -> None:
        self.process: asyncio.subprocess.Process | None = None
        self.linger = linger

    async def __call__(
        self, *command: str, **kwargs: Any
    ) -> asyncio.subprocess.Process:
        child_code = (
            "import json,time;"
            "print(json.dumps({'type':'system','subtype':'init',"
            "'session_id':'11111111-1111-4111-8111-111111111111'}),flush=True);"
            f"time.sleep({self.linger})"
        )
        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            child_code,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        return self.process


@pytest.mark.asyncio
async def test_claude_lead_is_registered_in_its_own_group_and_settled(
    tmp_path: Path, processes: ProcessRegistry
) -> None:
    prompt = tmp_path / "lead.md"
    prompt.write_text("# lead\n", encoding="utf-8")
    factory = RealChildFactory()
    runner = ClaudeRunner(
        registry(tmp_path),
        prompt_file=prompt,
        base_env={},
        process_factory=factory,
        termination_timeout=2.0,
        processes=processes,
    )
    request = LeadTurnRequest(
        session_id=UUID("11111111-1111-4111-8111-111111111111"),
        cwd=tmp_path,
        prompt="work",
        profile_alias="max-a",
        project_key="demo",
    )
    stream = runner.start_lead(request)
    first = await stream.__anext__()
    assert first.kind == "session.started"
    assert factory.process is not None
    leases = processes.active("demo")
    assert len(leases) == 1
    lease = leases[0]
    assert (lease.pid, lease.pgid, lease.kind) == (
        factory.process.pid,
        os.getpgid(factory.process.pid),
        "claude_lead",
    )
    assert lease.pgid == factory.process.pid
    assert lease.worker_id == "11111111-1111-4111-8111-111111111111"
    assert lease.cwd == str(tmp_path)
    assert processes.snapshot(lease.lease_id).alive is True
    assert processes.managed_rss_bytes() > 0

    await stream.aclose()
    assert factory.process.returncode is not None
    settled = processes.get(lease.lease_id)
    assert settled.state == "stopped"
    assert settled.stop_reason == "exited"
    assert processes.active("demo") == ()


@pytest.mark.asyncio
async def test_unregistrable_claude_child_is_terminated_and_fails_closed(
    tmp_path: Path, database: Database
) -> None:
    class RefusingInfo:
        def create_time(self, pid: int) -> float | None:
            return None

        def is_running(self, pid: int, create_time: float) -> bool:
            return False

        def tree_rss_bytes(self, pid: int, create_time: float) -> int:
            return 0

        def wait_exit(self, pid: int, create_time: float, timeout: float) -> bool:
            return True

    refusing = ProcessRegistry(database, EventStore(database), info=RefusingInfo())
    prompt = tmp_path / "lead.md"
    prompt.write_text("# lead\n", encoding="utf-8")
    factory = RealChildFactory()
    runner = ClaudeRunner(
        registry(tmp_path),
        prompt_file=prompt,
        base_env={},
        process_factory=factory,
        termination_timeout=2.0,
        processes=refusing,
    )
    request = LeadTurnRequest(
        session_id=UUID("11111111-1111-4111-8111-111111111111"),
        cwd=tmp_path,
        prompt="work",
        profile_alias="max-a",
        project_key="demo",
    )
    stream = runner.start_lead(request)
    with pytest.raises(RuntimeError, match="could not be registered"):
        await stream.__anext__()
    assert factory.process is not None
    assert factory.process.returncode is not None
    assert database.scalar("SELECT count(*) FROM process_leases") == 0


class PidProcess:
    def __init__(self, pid: int, returncode: int) -> None:
        self.pid = pid
        self.returncode: int | None = None
        self._code = returncode

    async def wait(self) -> int:
        self.returncode = self._code
        return self._code


@pytest.mark.asyncio
async def test_codex_queue_cli_is_registered_and_settled(
    tmp_path: Path, database: Database
) -> None:

    class FakeInfo:
        def create_time(self, pid: int) -> float | None:
            return 42.0

        def is_running(self, pid: int, create_time: float) -> bool:
            return True

        def tree_rss_bytes(self, pid: int, create_time: float) -> int:
            return 0

        def wait_exit(self, pid: int, create_time: float, timeout: float) -> bool:
            return True

    class FakeOs:
        def killpg(self, pgid: int, sig: int) -> None:
            raise AssertionError("queue delivery must not signal")

        def getpgid(self, pid: int) -> int:
            return pid

    processes = ProcessRegistry(
        database, EventStore(database), os_port=FakeOs(), info=FakeInfo()
    )
    merger = CodexMerger(
        rpc=FakeRpc(),
        database=database,
        projects={
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=tmp_path,
                integration_branch="main",
                github_repo="j-paterson/demo",
            )
        },
        prompt_file=Path("prompts/codex-merger.md"),
    )
    stored_channel(database)
    spawned: list[PidProcess] = []

    async def factory(*command: str, **kwargs: Any) -> PidProcess:
        process = PidProcess(pid=4242, returncode=0)
        spawned.append(process)
        return process

    delivery = CodexQueueDelivery(
        channels=merger,
        manifest_root=tmp_path,
        process_factory=factory,
        processes=processes,
    )
    event = candidate_manifest_event(tmp_path)
    result = await delivery.deliver("demo", event)
    assert result.delivered is True
    rows = database.execute(
        "SELECT kind, pid, pgid, state, stop_reason, exit_code FROM process_leases"
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("codex_queue", 4242, 4242, "stopped", "exited", 0)
    ]


def candidate_manifest_event(root: Path) -> Any:
    from hermes_orchestrator.manifests import wake_event_for, write_manifest
    from tests.test_codex_queue import CANDIDATE, candidate_manifest

    manifest = candidate_manifest()
    path = write_manifest(root, manifest, head_sha=CANDIDATE)
    return wake_event_for(manifest, path)


def test_reconciliation_expires_dead_and_reports_orphans(
    database: Database,
) -> None:
    from tests.test_processes import FakeInfo, FakeOs

    info = FakeInfo(create_times={120: 1.0, 130: 2.0}, running={120, 130})
    fake_os = FakeOs(groups={120: 120, 130: 130}, info=info)
    events = EventStore(database)
    launcher_registry = ProcessRegistry(database, events, os_port=fake_os, info=info)
    from hermes_orchestrator.processes import ProcessLeaseInput

    live = launcher_registry.register(
        ProcessLeaseInput(pid=120, pgid=120, project_key="demo", kind="claude_lead")
    )
    dead = launcher_registry.register(
        ProcessLeaseInput(pid=130, pgid=130, project_key="demo", kind="codex_queue")
    )
    info.running.discard(130)

    class GreenSampler:
        def sample(self) -> Any:
            raise AssertionError("reconciliation must not sample")

    class NoPlan:
        def plan(self, snapshot: Any) -> list[Any]:
            return []

    from hermes_orchestrator.config import PolicyConfig

    restarted = ProcessRegistry(database, events, os_port=fake_os, info=info)
    service = OrchestratorService(
        database=database,
        events=events,
        sampler=GreenSampler(),
        scheduler=NoPlan(),
        policy=PolicyConfig(mode="active"),
        pid_exists=lambda pid: False,
        processes=restarted,
    )
    result = service.start()
    assert result.completed is True
    assert result.findings == (f"orphan_process:{live.lease_id}",)
    assert result.admission_open is False
    assert restarted.get(dead.lease_id).state == "expired"
    assert restarted.get(live.lease_id).state == "active"
    assert fake_os.killpg_calls == []


# --- registration failure cleanup (every launcher fails closed) ------------


class BrokenInfo:
    def create_time(self, pid: int) -> float | None:
        return 1.0

    def is_running(self, pid: int, create_time: float) -> bool:
        return True

    def tree_rss_bytes(self, pid: int, create_time: float) -> int:
        return 0

    def wait_exit(self, pid: int, create_time: float, timeout: float) -> bool:
        return True


class PassOs:
    def killpg(self, pgid: int, sig: int) -> None:
        raise AssertionError("registration cleanup must use the launcher's group stop")

    def getpgid(self, pid: int) -> int:
        return pid


class RaisingEvents:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def append(self, connection: Any, event: Any) -> Any:
        raise self.error


def lead_request(tmp_path: Path) -> LeadTurnRequest:
    return LeadTurnRequest(
        session_id=UUID("11111111-1111-4111-8111-111111111111"),
        cwd=tmp_path,
        prompt="work",
        profile_alias="max-a",
        project_key="demo",
    )


def claude_runner(
    tmp_path: Path, factory: Any, processes: ProcessRegistry
) -> ClaudeRunner:
    prompt = tmp_path / "lead.md"
    prompt.write_text("# lead\n", encoding="utf-8")
    return ClaudeRunner(
        registry(tmp_path),
        prompt_file=prompt,
        base_env={},
        process_factory=factory,
        termination_timeout=2.0,
        processes=processes,
    )


@pytest.mark.asyncio
async def test_sqlite_failure_during_registration_terminates_and_reaps(
    tmp_path: Path, database: Database
) -> None:
    import sqlite3

    from tests.test_processes import FailingDatabase

    failing = FailingDatabase(database, fail_on={1})
    processes = ProcessRegistry(
        failing, EventStore(database), os_port=PassOs(), info=BrokenInfo()
    )
    factory = RealChildFactory()
    stream = claude_runner(tmp_path, factory, processes).start_lead(
        lead_request(tmp_path)
    )
    with pytest.raises(RuntimeError) as error:
        await stream.__anext__()
    assert str(error.value) == (
        "managed claude_lead process could not be registered: OperationalError"
    )
    assert "locked" not in str(error.value)
    assert error.value.__cause__ is None
    assert factory.process is not None and factory.process.returncode is not None
    assert database.scalar("SELECT count(*) FROM process_leases") == 0
    del sqlite3


@pytest.mark.asyncio
async def test_event_journal_failure_rolls_back_and_terminates(
    tmp_path: Path, database: Database
) -> None:
    processes = ProcessRegistry(
        database,
        RaisingEvents(RuntimeError("journal offline: token=secret")),  # type: ignore[arg-type]
        os_port=PassOs(),
        info=BrokenInfo(),
    )
    factory = RealChildFactory()
    stream = claude_runner(tmp_path, factory, processes).start_lead(
        lead_request(tmp_path)
    )
    with pytest.raises(RuntimeError) as error:
        await stream.__anext__()
    assert str(error.value).endswith("RuntimeError")
    assert "secret" not in str(error.value)
    assert factory.process is not None and factory.process.returncode is not None
    assert database.scalar("SELECT count(*) FROM process_leases") == 0
    assert database.scalar("SELECT count(*) FROM events") == 0


@pytest.mark.asyncio
async def test_cancellation_during_registration_cannot_leak_a_child(
    tmp_path: Path, database: Database
) -> None:
    processes = ProcessRegistry(
        database,
        RaisingEvents(asyncio.CancelledError()),  # type: ignore[arg-type]
        os_port=PassOs(),
        info=BrokenInfo(),
    )
    factory = RealChildFactory()
    stream = claude_runner(tmp_path, factory, processes).start_lead(
        lead_request(tmp_path)
    )
    with pytest.raises(asyncio.CancelledError):
        await stream.__anext__()
    assert factory.process is not None and factory.process.returncode is not None
    assert database.scalar("SELECT count(*) FROM process_leases") == 0


class SleepingServerFactory:
    """A real child with all three pipes that never speaks JSON-RPC."""

    def __init__(self) -> None:
        self.process: asyncio.subprocess.Process | None = None

    async def __call__(
        self, *command: str, **kwargs: Any
    ) -> asyncio.subprocess.Process:
        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "import time; time.sleep(30)",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        return self.process


@pytest.mark.asyncio
async def test_codex_app_server_launch_fails_closed_on_registration_error(
    database: Database,
) -> None:
    from hermes_orchestrator.codex_rpc import CodexRpcClient
    from tests.test_processes import FailingDatabase

    processes = ProcessRegistry(
        FailingDatabase(database, fail_on={1}),
        EventStore(database),
        os_port=PassOs(),
        info=BrokenInfo(),
    )
    factory = SleepingServerFactory()
    client = CodexRpcClient(
        ["codex", "app-server"],
        base_env={},
        process_factory=factory,
        handshake_timeout=2.0,
        termination_timeout=2.0,
        processes=processes,
    )
    with pytest.raises(RuntimeError, match=r"codex_app_server.*OperationalError"):
        await client.start()
    assert factory.process is not None and factory.process.returncode is not None
    assert database.scalar("SELECT count(*) FROM process_leases") == 0
    await client.close()


@pytest.mark.asyncio
async def test_codex_queue_launch_fails_closed_on_registration_error(
    tmp_path: Path, database: Database
) -> None:
    from tests.test_processes import FailingDatabase

    processes = ProcessRegistry(
        FailingDatabase(database, fail_on={1}),
        EventStore(database),
        os_port=PassOs(),
        info=BrokenInfo(),
    )
    merger = CodexMerger(
        rpc=FakeRpc(),
        database=database,
        projects={
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=tmp_path,
                integration_branch="main",
                github_repo="j-paterson/demo",
            )
        },
        prompt_file=Path("prompts/codex-merger.md"),
    )
    stored_channel(database)
    spawned: list[Any] = []

    class TrackedProcess(PidProcess):
        def __init__(self) -> None:
            super().__init__(pid=4243, returncode=0)
            self.waits = 0

        async def wait(self) -> int:
            self.waits += 1
            return await super().wait()

    async def factory(*command: str, **kwargs: Any) -> TrackedProcess:
        process = TrackedProcess()
        spawned.append(process)
        return process

    delivery = CodexQueueDelivery(
        channels=merger,
        manifest_root=tmp_path,
        process_factory=factory,
        processes=processes,
    )
    processes_transactions_before = 0
    del processes_transactions_before
    result = await delivery.deliver("demo", candidate_manifest_event(tmp_path))
    assert result.delivered is False
    assert result.reason == "registration_failed"
    assert len(spawned) == 1 and spawned[0].waits >= 1
    assert database.scalar("SELECT count(*) FROM process_leases") == 0
