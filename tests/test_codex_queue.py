"""Verify the argv-safe local codex queue delivery adapter."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.codex_merger import CodexMerger
from hermes_orchestrator.codex_queue import (
    CANDIDATE_QUEUED,
    CODEX_QUEUE_BINARY,
    WAKE_STATUSES,
    CodexQueueDelivery,
    WakeEvent,
)
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.manifests import (
    MANIFEST_VERSION,
    CandidateManifest,
    manifest_digest_for,
    read_manifest_snapshot,
    wake_event_for,
    write_manifest,
)

CANDIDATE = "1" * 40
BASE = "2" * 40


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


def candidate_manifest(
    event_id: str = "evt-1", candidate_sha: str = CANDIDATE
) -> CandidateManifest:
    return CandidateManifest(
        manifest_version=MANIFEST_VERSION,
        event_id=event_id,
        status="FABLE_READY",
        candidate_sha=candidate_sha,
        base_sha=BASE,
        branch="feature/eng-9",
        linear_issues=("ENG-9",),
        changed_files=("src/app.py",),
        verification=(("uv run pytest -q", "142 passed"),),
        blockers=(),
        created_at="2026-08-28T12:00:00+00:00",
    )


def wake_event(
    root: Path, event_id: str = "evt-1", candidate_sha: str = CANDIDATE
) -> WakeEvent:
    manifest = candidate_manifest(event_id, candidate_sha)
    path = root / f"{event_id}.json"
    if not path.exists():
        write_manifest(root, manifest, head_sha=candidate_sha)
    return wake_event_for(manifest, path)


def snapshot_for(root: Path, event: WakeEvent) -> Any:
    return read_manifest_snapshot(
        Path(event.manifest_path),
        root=root,
        expected_digest=event.manifest_digest,
    )


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def manifest_root(tmp_path: Path) -> Path:
    root = tmp_path / "manifests"
    root.mkdir()
    return root


class MovingClock:
    """A wall clock the test can advance past a delivery claim's lease."""

    def __init__(self) -> None:
        self._moment = datetime(2026, 8, 28, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self._moment

    def advance(self, seconds: float) -> None:
        self._moment = self._moment + timedelta(seconds=seconds)


def make_merger(
    database: Database,
    lease_seconds: float = 300.0,
    now: Callable[[], datetime] | None = None,
) -> CodexMerger:
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
        now=now or (lambda: datetime(2026, 8, 28, tzinfo=UTC)),
        wake_claim_lease_seconds=lease_seconds,
    )


@pytest.fixture
def merger(database: Database) -> CodexMerger:
    return make_merger(database)


@pytest.fixture
def factory() -> FakeQueueProcessFactory:
    return FakeQueueProcessFactory()


@pytest.fixture
def delivery(
    merger: CodexMerger,
    factory: FakeQueueProcessFactory,
    manifest_root: Path,
) -> CodexQueueDelivery:
    return CodexQueueDelivery(
        channels=merger,
        manifest_root=manifest_root,
        process_factory=factory,
    )


def stored_channel(
    database: Database,
    thread_id: str = "thr_stored",
    generation: int = 1,
    state: str = "ready",
) -> None:
    stamp = datetime(2026, 8, 28, tzinfo=UTC).isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO reviewer_channels("
            "project_key, thread_id, generation, state, created_at, updated_at"
            ") VALUES ('demo', ?, ?, ?, ?, ?)",
            (thread_id, generation, state, stamp, stamp),
        )


def test_wake_events_accept_only_real_fable_statuses(
    manifest_root: Path,
) -> None:
    assert frozenset(
        {
            "FABLE_READY",
            "FABLE_REWORK_READY",
            "FABLE_BLOCKED",
            "FABLE_COMPLETE",
        }
    ) == WAKE_STATUSES
    digest = manifest_digest_for(candidate_manifest())
    with pytest.raises(ValueError, match="status"):
        WakeEvent(
            status="HEARTBEAT",
            issue_id="ENG-9",
            candidate_sha=CANDIDATE,
            base_sha=BASE,
            manifest_path=str(manifest_root / "evt-1.json"),
            event_id="evt-1",
            manifest_digest=digest,
        )
    with pytest.raises(ValueError, match="Linear"):
        WakeEvent(
            status="FABLE_READY",
            issue_id=" ",
            candidate_sha=CANDIDATE,
            base_sha=BASE,
            manifest_path=str(manifest_root / "evt-1.json"),
            event_id="evt-1",
            manifest_digest=digest,
        )


@pytest.mark.asyncio
async def test_delivery_uses_exact_argv_without_a_shell(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database)
    event = wake_event(manifest_root)

    result = await delivery.deliver("demo", event)

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
        event.render(1),
    )
    message = command[-1]
    assert message.startswith(
        f"FABLE_READY issue=ENG-9 candidate={CANDIDATE} base={BASE} "
    )
    assert message.endswith("event=evt-1 generation=1")
    assert "shell" not in kwargs
    assert "exec" not in command
    assert "resume" not in command
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.last_delivered_event_id == "evt-1"
    assert channel.last_delivered_candidate_sha == CANDIDATE


@pytest.mark.asyncio
async def test_message_renders_against_the_invoked_generation(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database, thread_id="thr_new", generation=3)

    result = await delivery.deliver("demo", wake_event(manifest_root))

    assert result.delivered is True
    assert result.generation == 3
    assert factory.messages()[0].endswith("generation=3")


@pytest.mark.asyncio
async def test_cli_failure_is_bounded_and_never_claims_delivery(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database)
    factory.returncodes["thr_stored"] = 2

    result = await delivery.deliver("demo", wake_event(manifest_root))

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
    merger: CodexMerger, database: Database, manifest_root: Path
) -> None:
    stored_channel(database)

    async def broken_factory(*command: str, **kwargs: Any) -> FakeProcess:
        raise FileNotFoundError("codex binary missing")

    delivery = CodexQueueDelivery(
        channels=merger,
        manifest_root=manifest_root,
        process_factory=broken_factory,
    )
    result = await delivery.deliver("demo", wake_event(manifest_root))

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
    manifest_root: Path,
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

    result = await delivery.deliver("demo", wake_event(manifest_root))

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
    manifest_root: Path,
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

    result = await delivery.deliver("demo", wake_event(manifest_root))

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
    manifest_root: Path,
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

    result = await delivery.deliver("demo", wake_event(manifest_root))

    assert result.delivered is False
    assert result.reason == "redelivery_required"
    assert result.attempts == 1
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.last_delivered_event_id is None
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "pending"
    assert channel.heartbeat_enabled is True


@pytest.mark.asyncio
async def test_failure_without_generation_change_does_not_retry(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database)
    factory.returncodes["thr_stored"] = 1

    result = await delivery.deliver("demo", wake_event(manifest_root))

    assert result.delivered is False
    assert result.attempts == 1
    assert len(factory.calls) == 1


@pytest.mark.asyncio
async def test_unusable_channel_keeps_a_recoverable_pending_wake(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
) -> None:
    event = wake_event(manifest_root)

    missing = await delivery.deliver("demo", event)
    assert missing.delivered is False
    assert missing.reason == "channel_unavailable"
    assert missing.attempts == 0
    assert factory.calls == []
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "pending"

    stored_channel(database, state="uncertain")
    uncertain = await delivery.deliver("demo", event)
    assert uncertain.delivered is False
    assert uncertain.reason == "channel_unavailable"
    assert factory.calls == []


@pytest.mark.asyncio
async def test_arrival_during_replacement_arms_recoverable_fallback(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database, thread_id="thr_old", generation=1, state="replacing")
    event = wake_event(manifest_root)

    result = await delivery.deliver("demo", event)

    assert result.delivered is False
    assert result.reason == "channel_unavailable"
    assert factory.calls == []
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "pending"

    merger.complete_replacement(
        "demo",
        expected_thread_id="thr_old",
        expected_generation=1,
        new_thread_id="thr_new",
    )

    assert merger.pending_heartbeat_wakes(
        "demo", manifest_root=manifest_root
    ) == [event]

    recovered = await delivery.deliver("demo", event)
    assert recovered.delivered is True
    assert recovered.thread_id == "thr_new"
    assert recovered.generation == 2


@pytest.mark.asyncio
async def test_crash_after_cli_success_leaves_a_recoverable_wake(
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored_channel(database)
    event = wake_event(manifest_root)
    merger = make_merger(database, lease_seconds=0.0)
    delivery = CodexQueueDelivery(
        channels=merger,
        manifest_root=manifest_root,
        process_factory=factory,
    )

    real_success = merger.record_wake_delivery_success

    def crash(*args: Any, **kwargs: Any) -> bool:
        raise RuntimeError("simulated crash after the CLI exited zero")

    monkeypatch.setattr(merger, "record_wake_delivery_success", crash)

    with pytest.raises(RuntimeError, match="simulated crash"):
        await delivery.deliver("demo", event)

    assert len(factory.calls) == 1
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "claimed"
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.heartbeat_enabled is True
    assert merger.pending_heartbeat_wakes(
        "demo", manifest_root=manifest_root
    ) == [event]

    monkeypatch.setattr(
        merger, "record_wake_delivery_success", real_success
    )
    recovered = await delivery.deliver("demo", event)
    assert recovered.delivered is True
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "delivered"


@pytest.mark.asyncio
async def test_registration_resolves_the_live_channel_in_transaction(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database, thread_id="thr_old", generation=1)
    event = wake_event(manifest_root)
    delivered = await delivery.deliver("demo", event)
    assert delivered.delivered is True

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

    registration = merger.register_wake(
        "demo", event, manifest=snapshot_for(manifest_root, event)
    )
    assert registration.state == "pending"
    assert registration.thread_id == "thr_new"
    assert registration.generation == 2
    assert registration.claim_token is not None
    merger.record_wake_attempt_failed(
        "demo", event.event_id, claim_token=registration.claim_token
    )

    redelivered = await delivery.deliver("demo", event)
    assert redelivered.delivered is True
    assert redelivered.thread_id == "thr_new"
    assert redelivered.generation == 2
    assert factory.messages()[-1].endswith("generation=2")
    assert database.scalar(
        "SELECT generation FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == 2


@pytest.mark.asyncio
async def test_candidate_alias_after_replacement_reopens_the_owner(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database, thread_id="thr_old", generation=1)
    owner = wake_event(manifest_root)
    assert (await delivery.deliver("demo", owner)).delivered is True

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

    alias = wake_event(manifest_root, event_id="evt-2")
    result = await delivery.deliver("demo", alias)

    assert result.delivered is False
    assert result.reason == "duplicate_pending"
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "pending"
    assert merger.pending_heartbeat_wakes(
        "demo", manifest_root=manifest_root
    ) == [owner]


@pytest.mark.asyncio
async def test_pending_candidate_identity_blocks_concurrent_events(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database)
    factory.returncodes["thr_stored"] = 1
    first = await delivery.deliver("demo", wake_event(manifest_root))
    assert first.delivered is False

    twin = wake_event(manifest_root, event_id="evt-2")
    result = await delivery.deliver("demo", twin)

    assert result.delivered is False
    assert result.reason == "duplicate_pending"
    assert result.attempts == 0
    assert len(factory.calls) == 1
    assert database.scalar("SELECT count(*) FROM wake_deliveries") == 1


@pytest.mark.asyncio
async def test_same_event_with_different_payload_is_a_conflict(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
    tmp_path: Path,
) -> None:
    stored_channel(database)
    await delivery.deliver("demo", wake_event(manifest_root))

    altered_root = tmp_path / "altered"
    altered_root.mkdir()
    altered = wake_event(altered_root, candidate_sha="9" * 40)
    altered_delivery = CodexQueueDelivery(
        channels=merger,
        manifest_root=altered_root,
        process_factory=factory,
    )
    result = await altered_delivery.deliver("demo", altered)

    assert result.delivered is False
    assert result.reason == "event_conflict"
    assert len(factory.calls) == 1
    assert database.scalar(
        "SELECT candidate_sha FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == CANDIDATE


@pytest.mark.asyncio
async def test_duplicate_delivered_wake_is_a_noop(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database)

    first = await delivery.deliver("demo", wake_event(manifest_root))
    duplicate = await delivery.deliver("demo", wake_event(manifest_root))

    assert first.delivered is True
    assert duplicate.delivered is True
    assert duplicate.attempts == 0
    assert duplicate.reason == "duplicate_delivered"
    assert len(factory.calls) == 1


@pytest.mark.asyncio
async def test_duplicate_candidate_identity_is_a_noop(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database)

    await delivery.deliver("demo", wake_event(manifest_root))
    twin = wake_event(manifest_root, event_id="evt-2")
    duplicate = await delivery.deliver("demo", twin)

    assert duplicate.delivered is True
    assert duplicate.attempts == 0
    assert duplicate.reason == "duplicate_delivered"
    assert len(factory.calls) == 1


@pytest.mark.asyncio
async def test_failed_wake_stays_pending_and_can_be_redelivered(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database)
    factory.returncodes["thr_stored"] = 1

    failed = await delivery.deliver("demo", wake_event(manifest_root))
    assert failed.delivered is False
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "pending"

    factory.returncodes.clear()
    redelivered = await delivery.deliver("demo", wake_event(manifest_root))

    assert redelivered.delivered is True
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "delivered"
    assert database.scalar(
        "SELECT attempts FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == 2


@pytest.mark.asyncio
async def test_heartbeat_is_armed_from_registration_until_delivery(
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database)
    event = wake_event(manifest_root)
    merger = make_merger(database, lease_seconds=0.0)
    delivery = CodexQueueDelivery(
        channels=merger,
        manifest_root=manifest_root,
        process_factory=factory,
    )

    merger.register_wake(
        "demo", event, manifest=snapshot_for(manifest_root, event)
    )
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.heartbeat_enabled is True
    assert merger.pending_heartbeat_wakes(
        "demo", manifest_root=manifest_root
    ) == [event]

    await delivery.deliver("demo", event)
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.heartbeat_enabled is False
    assert merger.pending_heartbeat_wakes(
        "demo", manifest_root=manifest_root
    ) == []


@pytest.mark.asyncio
async def test_heartbeat_revalidates_the_immutable_manifest(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database)
    event = wake_event(manifest_root)
    factory.returncodes["thr_stored"] = 1
    await delivery.deliver("demo", event)

    manifest_path = Path(event.manifest_path)
    os.chmod(manifest_path, 0o644)
    assert merger.pending_heartbeat_wakes(
        "demo", manifest_root=manifest_root
    ) == []
    os.chmod(manifest_path, 0o444)
    assert merger.pending_heartbeat_wakes(
        "demo", manifest_root=manifest_root
    ) == [event]

    with database.transaction() as connection:
        connection.execute(
            "UPDATE wake_deliveries SET candidate_sha = ? "
            "WHERE event_id = 'evt-1'",
            ("9" * 40,),
        )
    assert merger.pending_heartbeat_wakes(
        "demo", manifest_root=manifest_root
    ) == []


@pytest.mark.asyncio
async def test_fabricated_or_mutated_events_never_spawn_a_process(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
    tmp_path: Path,
) -> None:
    stored_channel(database)
    digest = manifest_digest_for(candidate_manifest())

    fabricated = WakeEvent(
        status="FABLE_READY",
        issue_id="ENG-9",
        candidate_sha=CANDIDATE,
        base_sha=BASE,
        manifest_path=str(manifest_root / "evt-9.json"),
        event_id="evt-9",
        manifest_digest=digest,
    )
    missing = await delivery.deliver("demo", fabricated)
    assert missing.delivered is False
    assert missing.reason == "manifest_invalid"

    outside_root = tmp_path / "outside"
    outside_root.mkdir()
    outside_event = wake_event(outside_root)
    outside = await delivery.deliver("demo", outside_event)
    assert outside.delivered is False
    assert outside.reason == "manifest_invalid"

    event = wake_event(manifest_root)
    manifest_path = Path(event.manifest_path)
    os.chmod(manifest_path, 0o644)
    manifest_path.unlink()
    write_manifest(
        manifest_root,
        CandidateManifest(
            manifest_version=MANIFEST_VERSION,
            event_id="evt-1",
            status="FABLE_READY",
            candidate_sha=CANDIDATE,
            base_sha=BASE,
            branch="feature/eng-9",
            linear_issues=("ENG-9",),
            changed_files=("evil.py",),
            verification=(("uv run pytest -q", "142 passed"),),
            blockers=(),
            created_at="2026-08-28T12:00:00+00:00",
        ),
        head_sha=CANDIDATE,
    )
    mutated = await delivery.deliver("demo", event)
    assert mutated.delivered is False
    assert mutated.reason == "manifest_invalid"

    assert factory.calls == []
    assert database.scalar("SELECT count(*) FROM wake_deliveries") == 0


@pytest.mark.asyncio
async def test_replacement_reopens_a_delivered_wake_for_redelivery(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database, thread_id="thr_old", generation=1)

    delivered = await delivery.deliver("demo", wake_event(manifest_root))
    assert delivered.delivered is True
    duplicate = await delivery.deliver("demo", wake_event(manifest_root))
    assert duplicate.reason == "duplicate_delivered"
    assert len(factory.calls) == 1

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

    redelivered = await delivery.deliver("demo", wake_event(manifest_root))

    assert redelivered.delivered is True
    assert redelivered.thread_id == "thr_new"
    assert redelivered.generation == 2
    assert factory.messages()[-1].endswith("generation=2")
    assert database.scalar(
        "SELECT generation FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == 2


@pytest.mark.asyncio
async def test_pending_wake_survives_restart_for_redelivery(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
    tmp_path: Path,
) -> None:
    stored_channel(database)
    factory.returncodes["thr_stored"] = 1
    await delivery.deliver("demo", wake_event(manifest_root))

    restarted_db = Database.open(tmp_path / "state.db")
    try:
        restarted = make_merger(restarted_db)
        factory.returncodes.clear()
        redelivery = CodexQueueDelivery(
            channels=restarted,
            manifest_root=manifest_root,
            process_factory=factory,
        )

        result = await redelivery.deliver("demo", wake_event(manifest_root))

        assert result.delivered is True
        assert restarted_db.scalar(
            "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
        ) == "delivered"
    finally:
        restarted_db.close()


@pytest.mark.asyncio
async def test_success_for_one_event_keeps_fallback_for_another(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
    tmp_path: Path,
) -> None:
    stored_channel(database)
    blocked = wake_event(manifest_root)
    healthy = wake_event(
        manifest_root, event_id="evt-2", candidate_sha="5" * 40
    )

    factory.returncodes["thr_stored"] = 1
    assert (await delivery.deliver("demo", blocked)).delivered is False
    factory.returncodes.clear()

    assert (await delivery.deliver("demo", healthy)).delivered is True

    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.heartbeat_enabled is True
    # INFRA-221: the fallback is a delivery path too, so while the healthy
    # event holds the reviewer with an unsettled verdict the blocked wake
    # is NOT eligible to be woken — its durable pending row simply waits.
    assert merger.pending_heartbeat_wakes(
        "demo", manifest_root=manifest_root
    ) == []
    assert merger.complete_admitted_wake("demo", "evt-2") is False
    assert merger.release_delivered_wake(
        "demo", "evt-2", outcome="rejected"
    ) is True
    assert merger.pending_heartbeat_wakes(
        "demo", manifest_root=manifest_root
    ) == [blocked]

    restarted_db = Database.open(tmp_path / "state.db")
    try:
        restarted = make_merger(restarted_db)
        rechannel = restarted.read_channel("demo")
        assert rechannel is not None
        assert rechannel.heartbeat_enabled is True
        assert restarted.pending_heartbeat_wakes(
            "demo", manifest_root=manifest_root
        ) == [blocked]
    finally:
        restarted_db.close()


@pytest.mark.asyncio
async def test_concurrent_deliver_calls_invoke_exactly_once(
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database)
    event = wake_event(manifest_root)

    class SlowFactory(FakeQueueProcessFactory):
        async def __call__(self, *command: str, **kwargs: Any) -> FakeProcess:
            process = await super().__call__(*command, **kwargs)
            await asyncio.sleep(0.05)
            return process

    factory = SlowFactory()
    delivery = CodexQueueDelivery(
        channels=merger,
        manifest_root=manifest_root,
        process_factory=factory,
    )

    first, second = await asyncio.gather(
        delivery.deliver("demo", event),
        delivery.deliver("demo", event),
    )

    outcomes = sorted([first.reason, second.reason])
    assert outcomes == ["delivered", "delivery_in_flight"]
    assert len(factory.calls) == 1
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "delivered"


@pytest.mark.asyncio
async def test_unexpired_claim_returns_in_flight_without_invoking(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database)
    event = wake_event(manifest_root)
    registration = merger.register_wake(
        "demo", event, manifest=snapshot_for(manifest_root, event)
    )
    assert registration.claim_token is not None

    result = await delivery.deliver("demo", event)

    assert result.delivered is False
    assert result.reason == "delivery_in_flight"
    assert factory.calls == []


@pytest.mark.asyncio
async def test_stale_claim_token_cannot_complete(
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
    tmp_path: Path,
) -> None:
    stored_channel(database)
    event = wake_event(manifest_root)
    merger_a = make_merger(database, lease_seconds=0.0)
    stale = merger_a.register_wake(
        "demo", event, manifest=snapshot_for(manifest_root, event)
    )
    assert stale.claim_token is not None

    restarted_db = Database.open(tmp_path / "state.db")
    try:
        merger_b = make_merger(restarted_db, lease_seconds=0.0)
        redelivery = CodexQueueDelivery(
            channels=merger_b,
            manifest_root=manifest_root,
            process_factory=factory,
        )
        recovered = await redelivery.deliver("demo", event)
        assert recovered.delivered is True

        assert merger_b.record_wake_delivery_success(
            "demo",
            thread_id="thr_stored",
            generation=1,
            event_id=event.event_id,
            claim_token=stale.claim_token,
        ) is False
    finally:
        restarted_db.close()


@pytest.mark.asyncio
async def test_identical_byte_replacement_never_spawns(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database)
    event = wake_event(manifest_root)
    factory.returncodes["thr_stored"] = 1
    await delivery.deliver("demo", event)
    factory.returncodes.clear()

    path = Path(event.manifest_path)
    raw = path.read_bytes()
    os.chmod(path, 0o644)
    path.unlink()
    path.write_bytes(raw)
    os.chmod(path, 0o444)

    result = await delivery.deliver("demo", event)

    assert result.delivered is False
    assert result.reason == "event_conflict"
    assert len(factory.calls) == 1
    assert merger.pending_heartbeat_wakes(
        "demo", manifest_root=manifest_root
    ) == []


@pytest.mark.asyncio
async def test_timeout_kills_the_entire_owned_process_group(
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
    tmp_path: Path,
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
        manifest_root=manifest_root,
        process_factory=script_factory(script),
        timeout=0.3,
        termination_timeout=0.2,
    )

    result = await delivery.deliver("demo", wake_event(manifest_root))

    assert result.delivered is False
    child_pid = await read_pid(pid_file)
    await asyncio.sleep(1.2)
    assert not marker.exists()
    with pytest.raises(ProcessLookupError):
        os.kill(child_pid, 0)


@pytest.mark.asyncio
async def test_cancellation_kills_the_group_and_propagates(
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
    tmp_path: Path,
) -> None:
    stored_channel(database)
    marker = tmp_path / "late-delivery.txt"
    pid_file = tmp_path / "queue-child.pid"
    script = tmp_path / "fake_codex.py"
    write_group_script(script, marker, pid_file, parent_ignores_term=True)

    delivery = CodexQueueDelivery(
        channels=merger,
        manifest_root=manifest_root,
        process_factory=script_factory(script),
        timeout=30.0,
        termination_timeout=0.2,
    )

    task = asyncio.create_task(
        delivery.deliver("demo", wake_event(manifest_root))
    )
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


@pytest.mark.asyncio
async def test_a_second_candidate_is_queued_until_the_first_settles(
    delivery: CodexQueueDelivery,
    factory: FakeQueueProcessFactory,
    merger: CodexMerger,
    database: Database,
    manifest_root: Path,
) -> None:
    # INFRA-221: exactly ONE candidate is active at the primary Sol
    # Merger. The second candidate is durably registered but stays
    # 'pending' -- queued, never rendered into the thread -- until the
    # first candidate's verdict settles and frees the reviewer.
    stored_channel(database)
    first = wake_event(manifest_root)
    second = wake_event(manifest_root, event_id="evt-2", candidate_sha="5" * 40)

    assert (await delivery.deliver("demo", first)).delivered is True
    queued = await delivery.deliver("demo", second)

    assert (queued.delivered, queued.reason) == (False, CANDIDATE_QUEUED)
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-2'"
    ) == "pending"
    assert database.scalar(
        "SELECT thread_id FROM wake_deliveries WHERE event_id = 'evt-2'"
    ) is None
    # Exactly one invoke: nothing was rendered into the thread for evt-2.
    assert factory.messages() == [first.render(1)]
    assert merger.next_releasable_candidate("demo") is None

    # The first verdict settles; only now is the queued candidate
    # releasable, and the ordinary wake path delivers it exactly once.
    assert merger.release_delivered_wake(
        "demo", "evt-1", outcome="rejected"
    ) is True
    assert merger.next_releasable_candidate("demo") == second
    assert (await delivery.deliver("demo", second)).delivered is True
    assert factory.messages() == [first.render(1), second.render(1)]
    assert merger.next_releasable_candidate("demo") is None


def delivered_event_ids(database: Database) -> list[str]:
    rows = database.execute(
        "SELECT event_id FROM wake_deliveries WHERE state = 'delivered' "
        "ORDER BY event_id"
    ).fetchall()
    return [str(row["event_id"]) for row in rows]


@pytest.mark.asyncio
async def test_a_late_success_from_an_expired_claim_cannot_deliver(
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
) -> None:
    # Sol correction 370eb20c: treating an expired claim as an abandoned
    # delivery is what stops a crashed deliverer wedging the queue, but
    # the abandoned claimant must not keep a usable token. Once another
    # candidate has been released, a LATE success reported for the
    # expired claim is refused, and EXACTLY ONE candidate is delivered.
    clock = MovingClock()
    stored_channel(database)
    first = wake_event(manifest_root)
    second = wake_event(manifest_root, event_id="evt-2", candidate_sha="5" * 40)
    merger = make_merger(database, lease_seconds=300.0, now=clock)
    delivery = CodexQueueDelivery(
        channels=merger,
        manifest_root=manifest_root,
        process_factory=factory,
    )

    abandoned = merger.register_wake(
        "demo", first, manifest=snapshot_for(manifest_root, first)
    )
    assert abandoned.claim_token is not None
    clock.advance(301.0)

    # The lease has lapsed, so the second candidate is released -- and
    # releasing it invalidates the first claim in the same transaction.
    assert (await delivery.deliver("demo", second)).delivered is True
    assert database.scalar(
        "SELECT claim_token FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) is None

    assert merger.record_wake_delivery_success(
        "demo",
        thread_id="thr_stored",
        generation=1,
        event_id=first.event_id,
        claim_token=abandoned.claim_token,
        candidate_sha=first.candidate_sha,
    ) is False

    assert delivered_event_ids(database) == ["evt-2"]
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "pending"
    # The refusal rewrote no channel evidence and rendered nothing.
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.last_delivered_event_id == "evt-2"
    assert factory.messages() == [second.render(1)]
    # The abandoned candidate is queued, not lost: it is releasable again
    # as soon as the delivered candidate's verdict settles.
    assert merger.next_releasable_candidate("demo") is None
    assert merger.release_delivered_wake(
        "demo", "evt-2", outcome="rejected"
    ) is True
    assert merger.next_releasable_candidate("demo") == first


@pytest.mark.asyncio
async def test_restart_after_expiry_keeps_exactly_one_delivered_candidate(
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
    tmp_path: Path,
) -> None:
    # Sol correction 370eb20c: a restart after the lease lapses recovers
    # the abandoned candidate (the queue never wedges) and still admits
    # only ONE candidate to the reviewer -- and the pre-restart token is
    # dead, so its late success cannot deliver a second.
    clock = MovingClock()
    stored_channel(database)
    first = wake_event(manifest_root)
    second = wake_event(manifest_root, event_id="evt-2", candidate_sha="5" * 40)
    merger_a = make_merger(database, lease_seconds=300.0, now=clock)
    abandoned = merger_a.register_wake(
        "demo", first, manifest=snapshot_for(manifest_root, first)
    )
    assert abandoned.claim_token is not None
    clock.advance(301.0)

    restarted_db = Database.open(tmp_path / "state.db")
    try:
        merger_b = make_merger(restarted_db, lease_seconds=300.0, now=clock)
        redelivery = CodexQueueDelivery(
            channels=merger_b,
            manifest_root=manifest_root,
            process_factory=factory,
        )

        # Not wedged: the abandoned candidate is releasable after restart.
        assert merger_b.next_releasable_candidate("demo") == first
        assert (await redelivery.deliver("demo", first)).delivered is True

        # Still exactly one candidate at the reviewer.
        queued = await redelivery.deliver("demo", second)
        assert (queued.delivered, queued.reason) == (False, CANDIDATE_QUEUED)

        # The claim abandoned before the restart can never complete.
        assert merger_b.record_wake_delivery_success(
            "demo",
            thread_id="thr_stored",
            generation=1,
            event_id=first.event_id,
            claim_token=abandoned.claim_token,
            candidate_sha=first.candidate_sha,
        ) is False

        assert delivered_event_ids(restarted_db) == ["evt-1"]
        assert restarted_db.scalar(
            "SELECT state FROM wake_deliveries WHERE event_id = 'evt-2'"
        ) == "pending"
        assert factory.messages() == [first.render(1)]
    finally:
        restarted_db.close()


@pytest.mark.asyncio
async def test_a_lapsed_claim_alone_cannot_record_a_delivery_success(
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
) -> None:
    # Sol correction 370eb20c, second half: the success path fails closed
    # on the claim's own expiry, before any other candidate is released.
    # State and token still match here -- only the lapsed lease refuses.
    clock = MovingClock()
    stored_channel(database)
    event = wake_event(manifest_root)
    merger = make_merger(database, lease_seconds=300.0, now=clock)

    registration = merger.register_wake(
        "demo", event, manifest=snapshot_for(manifest_root, event)
    )
    assert registration.claim_token is not None
    clock.advance(301.0)

    assert merger.record_wake_delivery_success(
        "demo",
        thread_id="thr_stored",
        generation=1,
        event_id=event.event_id,
        claim_token=registration.claim_token,
        candidate_sha=event.candidate_sha,
    ) is False

    assert delivered_event_ids(database) == []
    channel = merger.read_channel("demo")
    assert channel is not None
    assert channel.last_delivered_event_id is None
    # Still not wedged: the abandoned candidate is redelivered whole.
    delivery = CodexQueueDelivery(
        channels=merger,
        manifest_root=manifest_root,
        process_factory=factory,
    )
    assert (await delivery.deliver("demo", event)).delivered is True
    assert delivered_event_ids(database) == ["evt-1"]


# ---------------------------------------------------------------------------
# INFRA-223: the queued turn on the idle pinned thread must actually start
# ---------------------------------------------------------------------------


class FakeQueueRpc:
    """One short-lived App Server connection, recorded end to end."""

    def __init__(
        self,
        listing: dict[str, Any] | None = None,
        *,
        fail_on: str | None = None,
    ) -> None:
        self.listing = listing if listing is not None else {"items": []}
        self.fail_on = fail_on
        self.started = False
        self.closed = 0
        self.requests: list[tuple[str, dict[str, Any] | None]] = []

    async def start(self) -> None:
        if self.fail_on == "start":
            raise RuntimeError("codex app-server unavailable")
        self.started = True

    async def request(
        self, method: str, params: dict[str, Any] | None, timeout: float
    ) -> dict[str, Any]:
        self.requests.append((method, params))
        if self.fail_on == method:
            raise RuntimeError(f"{method} is not supported")
        if method == "thread/queue/list":
            return self.listing
        return {}

    async def close(self) -> None:
        self.closed += 1

    def methods(self) -> list[str]:
        return [method for method, _ in self.requests]


def starting_delivery(
    merger: CodexMerger, manifest_root: Path, factory: Any, rpc: FakeQueueRpc
) -> CodexQueueDelivery:
    return CodexQueueDelivery(
        channels=merger,
        manifest_root=manifest_root,
        process_factory=factory,
        rpc_factory=lambda: rpc,
    )


@pytest.mark.asyncio
async def test_a_queued_head_carrying_this_event_is_started_then_released(
    merger: CodexMerger,
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
) -> None:
    """The observed blocker, closed: `codex queue` persisted the envelope
    for the idle pinned thread but never started the turn. After a
    successful delivery the exact queued head is listed, started, and the
    connection is dropped immediately."""

    stored_channel(database)
    event = wake_event(manifest_root)
    rpc = FakeQueueRpc(
        {"items": [{"id": "qi-1", "text": event.render(1)}]}
    )
    delivery = starting_delivery(merger, manifest_root, factory, rpc)

    result = await delivery.deliver("demo", event)

    assert result.delivered is True
    assert rpc.methods() == ["thread/queue/list", "thread/queue/start"]
    assert rpc.requests[0][1] == {"threadId": "thr_stored"}
    assert rpc.requests[1][1] == {
        "threadId": "thr_stored",
        "queueItemId": "qi-1",
    }
    # Never left attached to an idle Sol.
    assert rpc.closed == 1
    assert delivered_event_ids(database) == ["evt-1"]


@pytest.mark.asyncio
async def test_an_existing_delivered_wake_retries_only_the_bounded_start(
    merger: CodexMerger,
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
) -> None:
    stored_channel(database)
    event = wake_event(manifest_root)
    delivery = CodexQueueDelivery(
        channels=merger,
        manifest_root=manifest_root,
        process_factory=factory,
    )
    assert (await delivery.deliver("demo", event)).delivered

    rpc = FakeQueueRpc(
        {"items": [{"id": "qi-1", "text": event.render(1)}]}
    )
    delivery = starting_delivery(merger, manifest_root, factory, rpc)
    await delivery.deliver("demo", event)
    assert len(factory.calls) == 1
    assert rpc.methods() == ["thread/queue/list", "thread/queue/start"]


@pytest.mark.asyncio
async def test_a_head_that_is_not_this_candidates_is_never_started(
    merger: CodexMerger,
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
) -> None:
    """Identity comes only from what is already durable: the wake's own
    event id inside the rendered envelope. A head that does not carry it
    is someone else's turn and is left exactly where it is."""

    stored_channel(database)
    event = wake_event(manifest_root)
    other = wake_event(manifest_root, event_id="evt-9", candidate_sha="5" * 40)
    rpc = FakeQueueRpc(
        {"items": [{"id": "qi-other", "text": other.render(1)}]}
    )
    delivery = starting_delivery(merger, manifest_root, factory, rpc)

    result = await delivery.deliver("demo", event)

    assert result.delivered is True
    assert rpc.methods() == ["thread/queue/list"]
    assert rpc.closed == 1
    assert delivered_event_ids(database) == ["evt-1"]


@pytest.mark.asyncio
async def test_an_unavailable_queue_surface_changes_nothing_and_raises_nothing(
    merger: CodexMerger,
    factory: FakeQueueProcessFactory,
    database: Database,
    manifest_root: Path,
) -> None:
    """A Codex without the two methods, and a connection that will not
    open at all, are the same thing: do nothing, silently."""

    stored_channel(database)
    event = wake_event(manifest_root)
    refusing = FakeQueueRpc(
        {"items": [{"id": "qi-1", "text": event.render(1)}]},
        fail_on="thread/queue/list",
    )
    delivery = starting_delivery(merger, manifest_root, factory, refusing)

    result = await delivery.deliver("demo", event)

    assert result.delivered is True
    assert result.reason == "delivered"
    assert refusing.methods() == ["thread/queue/list"]
    assert refusing.closed == 1
    assert delivered_event_ids(database) == ["evt-1"]

    unreachable = FakeQueueRpc(fail_on="start")
    second = wake_event(manifest_root, event_id="evt-2", candidate_sha="7" * 40)
    assert merger.release_delivered_wake(
        "demo", "evt-1", outcome="rejected"
    ) is True
    retry = starting_delivery(merger, manifest_root, factory, unreachable)

    again = await retry.deliver("demo", second)

    assert again.delivered is True
    assert unreachable.requests == []
    assert unreachable.closed == 1
    assert delivered_event_ids(database) == ["evt-2"]
