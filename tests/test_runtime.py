from __future__ import annotations

import asyncio
import contextlib
import json
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import hermes_orchestrator.runtime as runtime_module
from hermes_orchestrator.cells import DEVELOPMENT_LANE, HARNESS_LANE
from hermes_orchestrator.channel_hub import ChannelLauncher
from hermes_orchestrator.cmux_surfaces import (
    CHANNEL_ENTRY,
    SKIP_PERMISSIONS_FLAG,
    CmuxLeadSeater,
    CmuxSurfaceRef,
)
from hermes_orchestrator.codex_merger import MERGER_MODEL, MERGER_PROVIDER
from hermes_orchestrator.config import load_settings
from hermes_orchestrator.db import Database
from hermes_orchestrator.domain import AdmissionRequest
from hermes_orchestrator.profiles import JsonCommand
from hermes_orchestrator.project_teams import ProjectTeamService
from hermes_orchestrator.runtime import (
    DaemonAlreadyRunning,
    _harness_lead_cwd,
    open_runtime,
    resolve_sidecar_entry,
)
from hermes_orchestrator.scheduler import Scheduler


class EligibleProfileCommand(JsonCommand):
    def __init__(self) -> None:
        self.config_dirs: list[str] = []

    def run_json(
        self,
        command: list[str],
        env: dict[str, str],
    ) -> dict[str, object]:
        assert command == ["claude", "auth", "status", "--json"]
        self.config_dirs.append(env["CLAUDE_CONFIG_DIR"])
        return {
            "loggedIn": True,
            "authMethod": "claude.ai",
            "apiProvider": "firstParty",
            "subscriptionType": "max",
            "email": "must-not-be-persisted@example.test",
        }


class FakeKeychain:
    def __init__(self) -> None:
        self.reads: list[tuple[str, str]] = []

    def read(self, service: str, account: str) -> str:
        self.reads.append((service, account))
        return "linear-token"


def active_repo(
    tmp_path: Path, lead_worktree: Path | None = None
) -> tuple[Path, Path]:
    config = tmp_path / "config"
    config.mkdir()
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts/codex-merger.md").write_text("# merger\n", encoding="utf-8")
    (tmp_path / "prompts/claude-lead.md").write_text(
        "Work only on explicitly queued work.\n",
        encoding="utf-8",
    )
    lead_worktree_line = (
        f"    lead_worktree: {lead_worktree}\n" if lead_worktree is not None else ""
    )
    (config / "projects.yaml").write_text(
        "projects:\n"
        "  demo:\n"
        "    linear_team: engineering\n"
        f"    repo_path: {tmp_path}\n"
        f"{lead_worktree_line}"
        "    integration_branch: main\n"
        "    github_repo: owner/demo\n",
        encoding="utf-8",
    )
    (config / "policies.yaml").write_text("mode: observe\n", encoding="utf-8")
    (config / "policies.local.yaml").write_text(
        "mode: active\n",
        encoding="utf-8",
    )
    (config / "linear.yaml").write_text(
        "assignee_ids: {operator: user-operator, ryan: user-ryan}\n"
        "teams:\n"
        "  engineering:\n"
        "    team_id: team-engineering\n"
        "    status_ids:\n"
        "      Todo: state-todo\n"
        "      In Development: state-development\n"
        "      Review: state-review\n"
        "      QA: state-qa\n"
        "      Done: state-done\n",
        encoding="utf-8",
    )
    (config / "profiles.yaml").write_text(
        "profiles:\n"
        f"  - {{alias: max-a, config_dir: {tmp_path / 'max-a'}}}\n"
        f"  - {{alias: max-b, config_dir: {tmp_path / 'max-b'}}}\n"
        f"  - {{alias: max-c, config_dir: {tmp_path / 'max-c'}}}\n"
        f"  - {{alias: max-d, config_dir: {tmp_path / 'max-d'}}}\n",
        encoding="utf-8",
    )
    return tmp_path, tmp_path / "state"


@dataclass(frozen=True)
class _FakeSnapshot:
    pressure: str
    can_admit: bool


def seed_live_fable_cell(
    state_dir: Path,
    *,
    cell_id: str = "cell-1",
    project_key: str = "demo",
) -> None:
    """Seed a durable, already-live development-lane cell before the
    daemon is opened, so ``open_runtime``'s INFRA-187 reconciliation has
    an existing Fable member to converge onto immediately."""

    stamp = datetime(2026, 8, 28, tzinfo=UTC).isoformat()
    database = Database.open(state_dir / "state.db")
    try:
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO project_cells("
                "cell_id, project_key, state, profile_alias, session_id, "
                "created_at, updated_at, lane_role) "
                "VALUES (?, ?, 'active', 'max-c', "
                "'11111111-1111-1111-1111-111111111111', ?, ?, "
                "'development')",
                (cell_id, project_key, stamp, stamp),
            )
    finally:
        database.close()


def seed_ready_reviewer_channel(
    state_dir: Path,
    *,
    project_key: str = "demo",
    thread_id: str = "thr-1",
    generation: int = 1,
    proven: bool,
) -> None:
    """Seed a durable ``ready`` reviewer channel before the daemon is
    opened. ``proven=False`` reproduces a legacy pre-INFRA-187 channel
    (NULL model/provider/model_verified_at) that requires the merger's
    own recovery-time reconciliation before it may ever bind."""

    stamp = datetime(2026, 8, 28, tzinfo=UTC).isoformat()
    database = Database.open(state_dir / "state.db")
    try:
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO reviewer_channels("
                "project_key, thread_id, generation, state, "
                "integration_branch, model, provider, model_verified_at, "
                "created_at, updated_at) "
                "VALUES (?, ?, ?, 'ready', 'main', ?, ?, ?, ?, ?)",
                (
                    project_key,
                    thread_id,
                    generation,
                    MERGER_MODEL if proven else None,
                    MERGER_PROVIDER if proven else None,
                    stamp if proven else None,
                    stamp,
                    stamp,
                ),
            )
    finally:
        database.close()


def test_active_runtime_assembles_live_dispatch_without_identity_persistence(
    tmp_path: Path,
) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    profiles = EligibleProfileCommand()
    keychain = FakeKeychain()

    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=profiles,
        keychain=keychain,
        base_env={},
    )
    try:
        assert runtime.dispatch is not None
        assert runtime.cells is not None
        assert [health.profile_alias for health in runtime.profile_health] == [
            "max-a",
            "max-b",
            "max-c",
            "max-d",
        ]
        assert all(health.eligible for health in runtime.profile_health)
        assert len(profiles.config_dirs) == 4
        # Sol correction a06cbce0: the daemon's own seeding bootstraps
        # each profile, so eligibility implies no first-run dialogs.
        for alias in ("max-a", "max-b", "max-c", "max-d"):
            document = json.loads(
                (tmp_path / alias / ".claude.json").read_text(encoding="utf-8")
            )
            assert document["hasCompletedOnboarding"] is True
            trusted = document["projects"][str(tmp_path)]
            assert trusted["hasTrustDialogAccepted"] is True
        assert keychain.reads == [
            ("hermes-orchestrator-linear", "default"),
            ("hermes-orchestrator-github", "default"),
            ("hermes-orchestrator-circleci", "default"),
        ]
        assert runtime.merge_flow is not None
        assert (settings.state_dir / "manifests").is_dir()
        assert runtime.database.scalar("SELECT count(*) FROM profile_leases") == 0
    finally:
        runtime.close()


def test_only_one_live_runtime_can_own_daemon_state(tmp_path: Path) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    first = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        with pytest.raises(DaemonAlreadyRunning, match="already running"):
            open_runtime(
                settings,
                enable_live=True,
                profile_command=EligibleProfileCommand(),
                keychain=FakeKeychain(),
                base_env={},
            )
    finally:
        first.close()

    replacement = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    replacement.close()


def test_daemon_lock_closes_handle_when_acquire_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Handle:
        closed = False

        def fileno(self) -> int:
            return 42

        def close(self) -> None:
            self.closed = True

    handle = Handle()
    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: handle)

    def fail_flock(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise OSError("flock failed")

    monkeypatch.setattr(runtime_module.fcntl, "flock", fail_flock)
    lock = runtime_module._DaemonLock(tmp_path / "daemon.lock")

    with pytest.raises(OSError, match="flock failed"):
        lock.acquire()

    assert handle.closed is True


def test_daemon_lock_closes_handle_when_unlock_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Handle:
        closed = False

        def fileno(self) -> int:
            return 42

        def close(self) -> None:
            self.closed = True

    handle = Handle()
    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: handle)
    calls = 0

    def flaky_flock(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("unlock failed")

    monkeypatch.setattr(runtime_module.fcntl, "flock", flaky_flock)
    lock = runtime_module._DaemonLock(tmp_path / "daemon.lock")
    lock.acquire()

    with pytest.raises(OSError, match="unlock failed"):
        lock.release()

    assert handle.closed is True


def test_runtime_releases_daemon_lock_when_database_close_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)

    class RecordingLock:
        acquired = False
        released = False

        def acquire(self) -> None:
            self.acquired = True

        def release(self) -> None:
            self.released = True

    class ClosingDatabase:
        def close(self) -> None:
            raise RuntimeError("database close failed")

    lock = RecordingLock()
    database = ClosingDatabase()
    monkeypatch.setattr(runtime_module, "_DaemonLock", lambda path: lock)
    monkeypatch.setattr(
        runtime_module.Database,
        "open",
        classmethod(lambda cls, path: database),
    )
    monkeypatch.setattr(
        runtime_module,
        "EventStore",
        lambda value: (_ for _ in ()).throw(ValueError("assembly failed")),
    )

    with pytest.raises(RuntimeError, match="database close failed"):
        open_runtime(settings, enable_live=True)

    assert lock.acquired is True
    assert lock.released is True


def test_observation_runtime_never_loads_live_credentials(tmp_path: Path) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    keychain = FakeKeychain()

    runtime = open_runtime(settings, enable_live=False, keychain=keychain)
    try:
        assert runtime.dispatch is None
        assert runtime.cells is None
        assert runtime.profile_health == ()
        assert keychain.reads == []
    finally:
        runtime.close()


def test_active_runtime_fails_closed_without_lead_contract(tmp_path: Path) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    (repo_root / "prompts/claude-lead.md").unlink()
    settings = load_settings(repo_root, state_dir)

    with pytest.raises(ValueError, match="claude-lead"):
        open_runtime(
            settings,
            enable_live=True,
            profile_command=EligibleProfileCommand(),
            keychain=FakeKeychain(),
            base_env={},
        )


def test_circleci_token_is_read_only_when_a_project_uses_circleci(
    tmp_path: Path,
) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    projects = repo_root / "config" / "projects.yaml"
    projects.write_text(
        projects.read_text(encoding="utf-8").replace(
            "    github_repo:", "    ci: none\n    github_repo:"
        ),
        encoding="utf-8",
    )
    settings = load_settings(repo_root, state_dir)
    assert all(project.ci == "none" for project in settings.projects.values())
    keychain = FakeKeychain()
    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=keychain,
        base_env={},
    )
    try:
        assert keychain.reads == [
            ("hermes-orchestrator-linear", "default"),
            ("hermes-orchestrator-github", "default"),
        ]
        assert runtime.merge_flow is not None
    finally:
        runtime.close()


def test_active_runtime_wires_lead_terminal_wakes_as_completion_sink(
    tmp_path: Path,
) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)

    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        assert runtime.lead_wakes is not None
        assert runtime.cells is not None
        assert runtime.cells._completion_sink is runtime.lead_wakes
    finally:
        runtime.close()


def test_observation_runtime_still_exposes_lead_wakes(tmp_path: Path) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)

    runtime = open_runtime(settings, enable_live=False)
    try:
        # The Hermes pending_wakes surface works even without a live lead.
        assert runtime.lead_wakes is not None
        assert runtime.lead_wakes.pending() == ()
    finally:
        runtime.close()


def seed_idle_runnable_seat(
    state_dir: Path,
    *,
    project_key: str = "demo",
    issue_id: str = "INFRA-1",
    session_id: str = "22222222-2222-4222-8222-222222222222",
) -> None:
    """Seed a durable ACTIVE development cell that is genuinely idle
    (its session recorded a Stop and never started a turn since), one
    queued dependency-ready admitted issue, and a fresh green resource
    sample -- every predicate ``commit_work_ready`` requires -- before
    the daemon opens its own persistent connection over the same file.
    """

    now = datetime.now(UTC).isoformat()
    database = Database.open(state_dir / "state.db")
    try:
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO project_cells(cell_id, project_key, state, "
                "profile_alias, session_id, created_at, updated_at, "
                "lane_role) VALUES ('cell-idle', ?, 'active', 'max-c', "
                "?, ?, ?, 'development')",
                (project_key, session_id, now, now),
            )
            connection.execute(
                "INSERT INTO admitted_issues(issue_id, project_key, "
                "priority, state, instruction_id, dependency_ready, "
                "overlap_risk, admitted_at, updated_at) VALUES "
                "(?, ?, 1, 'queued', 'chat-1', 1, 0, ?, ?)",
                (issue_id, project_key, now, now),
            )
            connection.execute(
                "INSERT INTO resource_samples(sample_id, sampled_at, "
                "pressure, available_memory_bytes, total_memory_bytes, "
                "swap_used_bytes, load_one, logical_cpus, disk_json, "
                "managed_rss_bytes) VALUES ('sample-1', ?, 'green', 1, 2, "
                "0, 0.1, 8, '{}', 0)",
                (now,),
            )
            connection.execute(
                "INSERT INTO events(event_id, event_type, aggregate_type, "
                "aggregate_id, occurred_at, actor, payload_json) VALUES "
                "('evt-stop', 'lead_turn.stopped', 'lead_session', ?, ?, "
                "'lead', '{}')",
                (session_id, now),
            )
    finally:
        database.close()


def test_runtime_wires_replenishment_to_work_ready_wakes(tmp_path: Path) -> None:
    """INFRA-215: both the post-merge advance path and child settlement
    are wired, in ``open_runtime``, to the SAME immediate-replenishment
    function -- the one that makes the daemon's per-tick
    ``commit_work_ready`` call right away instead of waiting for the
    next maintenance tick."""

    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    seed_idle_runnable_seat(state_dir)

    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        assert runtime.post_merge is not None
        assert runtime.lead_children is not None
        assert runtime.post_merge.replenish is not None
        assert runtime.post_merge.replenish is runtime.lead_children._replenish

        runtime.post_merge.replenish("demo")

        assert runtime.lead_wakes is not None
        pending = runtime.lead_wakes.pending()
        assert len(pending) == 1
        assert pending[0].kind == "work_ready"
        assert pending[0].issue_id == "INFRA-1"
    finally:
        runtime.close()


def test_active_runtime_assembles_cmux_visibility_when_configured(
    tmp_path: Path,
) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    (repo_root / "config/cmux.yaml").write_text(
        "cli:\n  - /apps/cmux\n", encoding="utf-8"
    )
    settings = load_settings(repo_root, state_dir)
    keychain = FakeKeychain()

    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=keychain,
        base_env={},
    )
    try:
        assert runtime.cmux_bindings is not None
        assert runtime.cmux_reconciler is not None
        assert runtime.cmux_hibernation is not None
        # The lead-intake router is part of production composition, so
        # published corrections and wakes actually reach classic seats.
        assert runtime.lead_intake is not None
        # The cmux socket password is read lazily at call time, never
        # during assembly: the documented credential read order holds.
        assert keychain.reads == [
            ("hermes-orchestrator-linear", "default"),
            ("hermes-orchestrator-github", "default"),
            ("hermes-orchestrator-circleci", "default"),
        ]
    finally:
        runtime.close()


def test_observation_runtime_exposes_bindings_without_a_cmux_port(
    tmp_path: Path,
) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    (repo_root / "config/cmux.yaml").write_text(
        "cli:\n  - /apps/cmux\n", encoding="utf-8"
    )
    settings = load_settings(repo_root, state_dir)

    runtime = open_runtime(settings, enable_live=False)
    try:
        assert runtime.cmux_bindings is not None
        assert runtime.cmux_reconciler is None
        assert runtime.cmux_hibernation is None
    finally:
        runtime.close()


# ---------------------------------------------------------------------------
# Sol correction b4b545f3 packet 1 (v5): fakechat is retired from
# production seat composition — open_runtime-composed classic seats
# attach the generated hermes-control MCP configuration, and the
# documented plain-classic fallback is the only degradation path.
# ---------------------------------------------------------------------------

SEAT_SESSION = "99999999-9999-4999-8999-999999999999"
SEAT_WORKSPACE = CmuxSurfaceRef(
    workspace_uuid="33333333-3333-4333-8333-333333333333",
    surface_uuid="33333333-3333-4333-8333-444444444444",
)
HUB_SESSION = "11111111-2222-4333-8444-555555555555"
HUB_SEAT = CmuxSurfaceRef(
    workspace_uuid="55555555-5555-4555-8555-555555555555",
    surface_uuid="55555555-5555-4555-8555-666666666666",
)
PACKET_ID = "b" * 32


def cmux_repo(
    base: Path, lead_worktree: Path | None = None
) -> tuple[Path, Path]:
    """An active repo with cmux configured and a sidecar build present."""

    repo_root, state_dir = active_repo(base, lead_worktree=lead_worktree)
    (repo_root / "config/cmux.yaml").write_text(
        "cli:\n  - /apps/cmux\n", encoding="utf-8"
    )
    entry = repo_root / "channels/hermes-control/dist/src/main.js"
    entry.parent.mkdir(parents=True)
    entry.write_text("// sidecar build\n", encoding="utf-8")
    return repo_root, state_dir


def fake_node(base: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Make the Node resolution deterministic for launcher composition."""

    node = base / "node-bin"
    node.write_text("#!/bin/sh\n", encoding="utf-8")
    real_which = shutil.which
    monkeypatch.setattr(
        runtime_module.shutil,
        "which",
        lambda name: str(node) if name == "node" else real_which(name),
    )
    return node


@dataclass
class FakeSeatPort:
    """Minimal fake CmuxControlPort driving one fresh-seat activation."""

    next_refs: list[CmuxSurfaceRef] = field(default_factory=list)
    live: set[CmuxSurfaceRef] = field(default_factory=set)
    created: list[dict[str, object]] = field(default_factory=list)
    resumes: list[tuple[CmuxSurfaceRef, str]] = field(default_factory=list)

    async def ping(self) -> None:
        return None

    async def create_workspace(
        self,
        *,
        title: str,
        cwd: Path,
        command: str | None = None,
        env: dict[str, str] | None = None,
        resolve_marker: str | None = None,
    ) -> CmuxSurfaceRef:
        self.created.append(
            {"title": title, "cwd": cwd, "command": command, "env": env}
        )
        ref = self.next_refs.pop(0)
        self.live.add(ref)
        return ref

    async def live_workspace_uuids(self) -> frozenset[str]:
        return frozenset(ref.workspace_uuid for ref in self.live)

    async def surface_alive(self, ref: CmuxSurfaceRef) -> bool:
        return ref in self.live

    async def close_workspace(self, workspace_uuid: str) -> None:
        self.live = {
            ref for ref in self.live if ref.workspace_uuid != workspace_uuid
        }

    async def set_surface_resume(self, ref: CmuxSurfaceRef, command: str) -> None:
        self.resumes.append((ref, command))

    async def set_status(self, workspace_uuid: str, key: str, value: str) -> None:
        return None

    async def rename_workspace(self, workspace_uuid: str, title: str) -> None:
        return None

    async def find_workspace_uuids(self, *, title_marker: str) -> frozenset[str]:
        raise NotImplementedError("no reconciliation in these tests")


class HubClient:
    """A protocol-exact stand-in for the hermes-control sidecar."""

    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.reader = reader
        self.writer = writer

    @classmethod
    async def connect(cls, path: Path) -> HubClient:
        reader, writer = await asyncio.open_unix_connection(str(path))
        return cls(reader, writer)

    async def send(self, message: dict[str, object]) -> None:
        self.writer.write((json.dumps(message) + "\n").encode())
        await self.writer.drain()

    async def receive(self) -> dict[str, object]:
        line = await asyncio.wait_for(self.reader.readline(), timeout=2)
        assert line, "connection closed while a reply was expected"
        return json.loads(line)

    async def receive_matching(self, **expected: object) -> dict[str, object]:
        """The next message with these fields; the production-composed
        hub interleaves derived control-ready events (registration
        receipts and the like), which are simply drained past."""

        for _ in range(10):
            message = await self.receive()
            if all(message.get(key) == value for key, value in expected.items()):
                return message
        raise AssertionError(f"no message matching {expected!r} arrived")

    async def close(self) -> None:
        self.writer.close()
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            await self.writer.wait_closed()


def register_message(
    *, session_id: str, generation: int, capability: str
) -> dict[str, object]:
    return {
        "op": "register",
        "proto": 1,
        "project": "demo",
        "cell_id": "cell-demo",
        "session_id": session_id,
        "profile": "max-a",
        "generation": generation,
        "capability": capability,
    }


def test_active_runtime_composes_seats_on_hermes_control_without_fakechat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """v5: production composition wires no fakechat plane at all — the
    hub plus the hermes-control channel launcher are the wake path."""

    repo_root, state_dir = cmux_repo(tmp_path)
    fake_node(tmp_path, monkeypatch)
    settings = load_settings(repo_root, state_dir)

    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        # The fakechat wake router is no longer part of production
        # composition; the field stays only as an always-None socket.
        assert runtime.fakechat_router is None
        assert runtime.channel_hub is not None
        assert runtime.channel_capabilities is not None
        assert runtime.cells is not None
        seater = runtime.cells._surfaces
        assert isinstance(seater, CmuxLeadSeater)
        # The seat composer carries the hermes-control launcher and no
        # fakechat port source of any kind.
        assert isinstance(seater._channel_launch, ChannelLauncher)
        assert getattr(seater, "_signal_ports", None) is None
        # Sol correction a06cbce0: startup reconciliation reseats a
        # missing lead through the same channel launcher the seater
        # uses — with the control ledger for the fail-closed
        # channel.blocked receipt — never a blank replacement terminal.
        assert runtime.cmux_reconciler is not None
        assert runtime.cmux_reconciler._channel_launch is seater._channel_launch
        assert runtime.cmux_reconciler._control is not None
        # Sol correction c5600e31: restart recovery completes the same
        # bounded channel-trust confirmation and registration path as
        # normal seating — the reconciler is composed with the exact
        # same trust trigger the seater uses, never a separate or
        # missing one.
        assert runtime.cmux_reconciler._channel_trust is seater._channel_trust
        assert runtime.cmux_reconciler._channel_trust is not None
    finally:
        runtime.close()


def test_live_runtime_composes_every_lead_cwd_from_the_dedicated_worktree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sol correction c5600e31: with a dedicated ``lead_worktree``
    configured, bootstrap trust and every seat/cell composition site
    must agree on ``project.lead_cwd`` — never a mix of ``lead_worktree``
    for bootstrap and ``repo_path`` for the actual seat/cell cwd, which
    would let an eligible profile still hit the repository trust prompt
    in the launched seat.
    """

    lead_worktree = tmp_path / "demo-lead"
    lead_worktree.mkdir()
    repo_root, state_dir = cmux_repo(tmp_path, lead_worktree=lead_worktree)
    fake_node(tmp_path, monkeypatch)
    settings = load_settings(repo_root, state_dir)
    assert settings.projects["demo"].lead_cwd == lead_worktree

    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        assert runtime.cells is not None
        seater = runtime.cells._surfaces
        assert isinstance(seater, CmuxLeadSeater)
        assert seater._project_paths == {"demo": lead_worktree}
        assert runtime.cmux_reconciler is not None
        assert runtime.cmux_reconciler._project_paths == {"demo": lead_worktree}
        assert runtime.cells._project_paths == {"demo": lead_worktree}
        # The bootstrap trust document is written for the exact
        # directory the seat launches from (lead_worktree), not the
        # stable primary checkout (repo_path) — otherwise an eligible
        # profile would still stop at the trust prompt in the seat.
        document = json.loads(
            (tmp_path / "max-a" / ".claude.json").read_text(encoding="utf-8")
        )
        assert document["hasCompletedOnboarding"] is True
        trusted = document["projects"][str(lead_worktree)]
        assert trusted["hasTrustDialogAccepted"] is True
        assert str(tmp_path) not in document["projects"]
    finally:
        runtime.close()


def test_harness_lead_cwd_derives_the_sibling_worktree_convention() -> None:
    """INFRA-219 R5b: the pure helper runtime.py duplicates from
    ``cli.py``'s ``_harness_lead_cwd`` (Sol correction 110ed759) --
    the harness lane's dedicated checkout is always a sibling of the
    development lead's own directory, never that directory itself."""

    lead_cwd = Path("/repos/demo-lead")
    harness_cwd = _harness_lead_cwd(lead_cwd)
    assert harness_cwd == Path("/repos/demo-lead-harness")
    assert harness_cwd != lead_cwd


def test_live_runtime_composes_a_distinct_harness_lane_project_path(
    tmp_path: Path,
) -> None:
    """INFRA-219 R5b (Sol correction 110ed759): the composed
    ``ProjectCellService`` must resolve the HARNESS lane to its own
    dedicated checkout, not the development ``lead_cwd`` runtime.py's
    plain ``project_paths`` map still carries -- otherwise a restored
    harness cell would launch/resume into the development lead's own
    worktree."""

    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    lead_cwd = settings.projects["demo"].lead_cwd

    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        assert runtime.cells is not None
        harness_cwd = runtime.cells._lane_project_paths[("demo", HARNESS_LANE)]
        assert harness_cwd == _harness_lead_cwd(lead_cwd)
        assert harness_cwd != lead_cwd
        # Lane-aware resolution: the harness lane's private ``_cell_cwd``
        # seam picks up the dedicated checkout...
        assert runtime.cells._cell_cwd("demo", HARNESS_LANE) == harness_cwd
        # ...while development-lane resolution is untouched -- no
        # development entry was ever added to ``_lane_project_paths``,
        # so it still falls back to the historical ``project_paths``
        # map, byte-compatible with pre-R5b composition.
        assert ("demo", DEVELOPMENT_LANE) not in runtime.cells._lane_project_paths
        assert runtime.cells._cell_cwd("demo", DEVELOPMENT_LANE) == lead_cwd
        assert runtime.cells._project_paths == {"demo": lead_cwd}
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_composed_classic_seat_attaches_hermes_control_and_registers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sol tests 1 and 2: the open_runtime-composed classic seat uses
    the generated hermes-control MCP configuration and registers
    through the hub; the same production path creates no fakechat port
    and never adds the fakechat channel command."""

    with tempfile.TemporaryDirectory() as short:
        base = Path(short)
        repo_root, state_dir = cmux_repo(base)
        fake_node(base, monkeypatch)
        settings = load_settings(repo_root, state_dir)
        runtime = open_runtime(
            settings,
            enable_live=True,
            profile_command=EligibleProfileCommand(),
            keychain=FakeKeychain(),
            base_env={},
        )
        hub_started = False
        try:
            assert runtime.cells is not None
            assert runtime.channel_hub is not None
            seater = runtime.cells._surfaces
            assert isinstance(seater, CmuxLeadSeater)
            # Only the external cmux process is faked; the bindings,
            # launcher, capabilities, and hub are production-composed.
            port = FakeSeatPort(next_refs=[SEAT_WORKSPACE])
            seater._port = port

            seat = await seater.ensure(
                project_key="demo",
                cell_id="cell-demo",
                session_id=SEAT_SESSION,
                profile_alias="max-a",
                classic_command=(
                    f"claude --session-id {SEAT_SESSION} {SKIP_PERMISSIONS_FLAG}"
                ),
            )

            assert seat is not None
            [created] = port.created
            command = str(created["command"])
            # load_settings resolves the state dir (/var -> /private/var
            # on macOS), so the expectation derives from the settings.
            config_path = (
                settings.state_dir / "channels" / f"{SEAT_SESSION}.mcp.json"
            )
            prompt_path = settings.repo_root / "prompts" / "claude-lead.md"
            assert command == (
                f"claude --session-id {SEAT_SESSION} {SKIP_PERMISSIONS_FLAG} "
                f"--append-system-prompt-file {prompt_path} "
                f"--mcp-config {config_path} "
                f"--dangerously-load-development-channels {CHANNEL_ENTRY}"
            )
            # Sol test 2: no fakechat channel command, no fakechat port.
            assert "fakechat" not in command
            env = created["env"]
            assert isinstance(env, dict)
            assert "FAKECHAT_PORT" not in env
            assert (
                runtime.database.scalar(
                    "SELECT count(*) FROM fakechat_signal_ports"
                )
                == 0
            )
            # The generated config is the hermes-control MCP server.
            config = json.loads(config_path.read_text(encoding="utf-8"))
            server = config["mcpServers"]["hermes-control"]
            assert server["env"]["HERMES_CONTROL_SESSION"] == SEAT_SESSION

            # Sol test 1: the same seat registers through the hub with
            # the capability the composed launcher issued for it.
            await runtime.channel_hub.start()
            hub_started = True
            capability = Path(
                server["env"]["HERMES_CONTROL_CAPABILITY_FILE"]
            ).read_text(encoding="ascii")
            client = await HubClient.connect(runtime.channel_hub.socket_path)
            try:
                await client.send(
                    register_message(
                        session_id=SEAT_SESSION,
                        generation=seat.generation,
                        capability=capability,
                    )
                )
                reply = await client.receive()
                assert reply == {"op": "registered", "proto": 1}
                assert SEAT_SESSION in runtime.channel_hub.registered_sessions()
            finally:
                await client.close()
        finally:
            if hub_started:
                await runtime.channel_hub.stop()
            runtime.close()


@pytest.mark.asyncio
async def test_hermes_control_delivery_fetch_and_ack_through_composition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sol test 3: hermes-control delivery, durable fetch, and exact
    ACK all remain covered through the production composition."""

    with tempfile.TemporaryDirectory() as short:
        base = Path(short)
        repo_root, state_dir = cmux_repo(base)
        fake_node(base, monkeypatch)
        settings = load_settings(repo_root, state_dir)
        runtime = open_runtime(
            settings,
            enable_live=True,
            profile_command=EligibleProfileCommand(),
            keychain=FakeKeychain(),
            base_env={},
        )
        hub_started = False
        try:
            hub = runtime.channel_hub
            assert hub is not None
            assert runtime.cmux_bindings is not None
            assert runtime.channel_capabilities is not None
            binding = runtime.cmux_bindings.bind_lead(
                project_key="demo",
                cell_id="cell-demo",
                session_id=HUB_SESSION,
                profile_alias="max-a",
                ref=HUB_SEAT,
            )
            runtime.cmux_bindings.record_classic(binding.binding_id, HUB_SESSION)
            capability = runtime.channel_capabilities.issue(
                HUB_SESSION
            ).read_text(encoding="ascii")
            await hub.start()
            hub_started = True

            # Durable fetch: a publish with no live channel stays a
            # durable pending event, never lost.
            status = await hub.publish(
                kind="HERMES_CORRECTION_READY",
                packet_id=PACKET_ID,
                cell_id="cell-demo",
                session_id=HUB_SESSION,
            )
            assert status == "pending"
            assert (
                str(
                    runtime.database.scalar(
                        "SELECT state FROM channel_events WHERE packet_id = ?",
                        (PACKET_ID,),
                    )
                )
                == "pending"
            )

            # Delivery: registration replays the durable pending event.
            client = await HubClient.connect(hub.socket_path)
            try:
                await client.send(
                    register_message(
                        session_id=HUB_SESSION,
                        generation=binding.generation,
                        capability=capability,
                    )
                )
                reply = await client.receive()
                assert reply == {"op": "registered", "proto": 1}
                event = await client.receive_matching(
                    op="event", packet_id=PACKET_ID
                )

                # Exact ACK: only the exact event id flips the durable
                # state to acked.
                await client.send(
                    {
                        "op": "ack",
                        "event_id": event["event_id"],
                        "packet_id": PACKET_ID,
                        "session_id": HUB_SESSION,
                    }
                )
                reply = await client.receive_matching(op="ack_ok")
                assert reply == {"op": "ack_ok", "event_id": event["event_id"]}
                assert (
                    str(
                        runtime.database.scalar(
                            "SELECT state FROM channel_events WHERE event_id = ?",
                            (event["event_id"],),
                        )
                    )
                    == "acked"
                )
            finally:
                await client.close()
        finally:
            if hub_started:
                assert runtime.channel_hub is not None
                await runtime.channel_hub.stop()
            runtime.close()


class TestResolveSidecarEntry:
    """INFRA-197: the sidecar build must be resolved where the daemon
    can actually launch it — the ACTIVE runtime artifact, when one
    exists, over the historical (and gitignored, so often absent)
    repo_root build."""

    def test_prefers_the_active_artifacts_own_sidecar_build(
        self, tmp_path: Path
    ) -> None:
        repo_root = tmp_path / "repo"
        repo_entry = (
            repo_root / "channels/hermes-control/dist/src/main.js"
        )
        repo_entry.parent.mkdir(parents=True)
        repo_entry.write_text("// repo_root build\n", encoding="utf-8")

        state_dir = tmp_path / "state"
        artifact = state_dir / "runtimes" / "deadbeef"
        artifact_entry = (
            artifact / "channels/hermes-control/dist/src/main.js"
        )
        artifact_entry.parent.mkdir(parents=True)
        artifact_entry.write_text("// artifact build\n", encoding="utf-8")
        (state_dir / "runtimes" / "ACTIVE").write_text(
            str(artifact) + "\n", encoding="utf-8"
        )

        resolved = resolve_sidecar_entry(
            repo_root=repo_root, state_dir=state_dir
        )

        assert resolved == artifact_entry

    def test_an_active_artifact_missing_its_sidecar_never_falls_back(
        self, tmp_path: Path
    ) -> None:
        """Sol correction f0a5a403 (packet 2): with an ACTIVE runtime
        recorded, the artifact-derived entry is returned even when the
        artifact's sidecar build is missing — the mutable, gitignored
        repo_root build must NEVER stand in for the active artifact
        identity; ChannelLauncher then refuses the missing entry."""

        repo_root = tmp_path / "repo"
        repo_entry = (
            repo_root / "channels/hermes-control/dist/src/main.js"
        )
        repo_entry.parent.mkdir(parents=True)
        repo_entry.write_text("// repo_root build\n", encoding="utf-8")

        state_dir = tmp_path / "state"
        artifact = state_dir / "runtimes" / "deadbeef"
        artifact.mkdir(parents=True)  # no channels/ inside this artifact
        (state_dir / "runtimes" / "ACTIVE").write_text(
            str(artifact) + "\n", encoding="utf-8"
        )

        resolved = resolve_sidecar_entry(
            repo_root=repo_root, state_dir=state_dir
        )

        # The repo build exists and is still not chosen: the resolved
        # path is the (absent) artifact entry, so the launcher's
        # file-exists guard fails the channel closed.
        assert resolved == (
            artifact / "channels/hermes-control/dist/src/main.js"
        )
        assert not resolved.exists()
        assert repo_entry.is_file()

    def test_an_empty_active_pointer_keeps_the_pre_activation_fallback(
        self, tmp_path: Path
    ) -> None:
        """A pointer file with no recorded runtime is the documented
        pre-activation state: the historical repo_root path remains
        available."""

        repo_root = tmp_path / "repo"
        state_dir = tmp_path / "state"
        (state_dir / "runtimes").mkdir(parents=True)
        (state_dir / "runtimes" / "ACTIVE").write_text(
            "\n", encoding="utf-8"
        )

        resolved = resolve_sidecar_entry(
            repo_root=repo_root, state_dir=state_dir
        )

        assert resolved == (
            repo_root / "channels/hermes-control/dist/src/main.js"
        )

    def test_yields_the_repo_root_path_when_neither_build_exists(
        self, tmp_path: Path
    ) -> None:
        """No ACTIVE pointer at all (a daemon that never activated an
        artifact): the resolved path is the historical repo_root
        location even though nothing exists there — the existing
        shutil.which("node") / ChannelLauncher file-exists guard is
        the fail-closed boundary, unchanged."""

        repo_root = tmp_path / "repo"
        state_dir = tmp_path / "state"

        resolved = resolve_sidecar_entry(
            repo_root=repo_root, state_dir=state_dir
        )

        assert resolved == (
            repo_root / "channels/hermes-control/dist/src/main.js"
        )
        assert not resolved.exists()


class BedrockOnMaxDProfileCommand(JsonCommand):
    """First-party auth everywhere except max-d, which probes as Bedrock."""

    def run_json(
        self,
        command: list[str],
        env: dict[str, str],
    ) -> dict[str, object]:
        assert command == ["claude", "auth", "status", "--json"]
        bedrock = env["CLAUDE_CONFIG_DIR"].endswith("max-d")
        return {
            "loggedIn": True,
            "authMethod": "claude.ai",
            "apiProvider": "bedrock" if bedrock else "firstParty",
            "subscriptionType": "max",
        }


def _insert_capacity_observation(
    runtime: object,
    alias: str,
    state: str,
    *,
    observed_at: datetime,
    resets_at: datetime | None = None,
) -> None:
    """Append one durable fable-capacity observation for the daemon pool."""

    with runtime.database.transaction() as connection:  # type: ignore[attr-defined]
        connection.execute(
            "INSERT INTO profile_capacity_observations("
            "profile_alias, model, state, source, observed_at, resets_at"
            ") VALUES (?, 'fable', ?, ?, ?, ?)",
            (
                alias,
                state,
                "provider_limit" if state == "capped" else "operator_attestation",
                observed_at.isoformat(),
                resets_at.isoformat() if resets_at is not None else None,
            ),
        )


def test_daemon_pool_refuses_a_replacement_with_a_current_fable_cap(
    tmp_path: Path,
) -> None:
    """INFRA-205 regression 1: a non-expired capped observation makes an
    authenticated (auth-eligible) profile ineligible for replacement
    selection — the daemon runtime's own pool must read the durable
    ``profile_capacity_observations`` rows, not auth health alone."""

    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        now = datetime.now(UTC)
        _insert_capacity_observation(
            runtime,
            "max-b",
            "capped",
            observed_at=now - timedelta(hours=1),
            resets_at=now + timedelta(hours=11),
        )
        _insert_capacity_observation(
            runtime, "max-c", "available", observed_at=now - timedelta(hours=1)
        )
        assert runtime.cells is not None
        pool = runtime.cells._profiles
        pool.restore("demo", "max-a", now)

        replacement = pool.reserve_replacement("demo", "max-a")

        # max-b is auth-eligible and first in registry order, but its
        # unexpired fable cap must exclude it; selection lands on max-c.
        assert replacement is not None
        assert replacement.profile_alias == "max-c"
    finally:
        runtime.close()


def test_daemon_pool_admits_a_profile_after_its_cap_reset_passed(
    tmp_path: Path,
) -> None:
    """INFRA-205 regression 2: once ``resets_at`` has passed, the stale
    cap no longer blocks that profile — the budget cycled and the same
    observation evidences availability again."""

    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        now = datetime.now(UTC)
        _insert_capacity_observation(
            runtime,
            "max-b",
            "capped",
            observed_at=now - timedelta(days=3),
            resets_at=now - timedelta(hours=1),
        )
        assert runtime.cells is not None
        pool = runtime.cells._profiles
        pool.restore("demo", "max-a", now)

        replacement = pool.reserve_replacement("demo", "max-a")

        assert replacement is not None
        assert replacement.profile_alias == "max-b"
    finally:
        runtime.close()


def test_daemon_pool_selects_deterministically_and_only_first_party_profiles(
    tmp_path: Path,
) -> None:
    """INFRA-205 regression 6: selection among eligible profiles is
    deterministic (first registry-ordered candidate passing every gate)
    and a non-first-party (Bedrock) profile is never a candidate, even
    when it holds current available capacity evidence."""

    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=BedrockOnMaxDProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        by_alias = {
            health.profile_alias: health for health in runtime.profile_health
        }
        assert by_alias["max-d"].eligible is False
        assert by_alias["max-d"].reason == "not_first_party_subscription"

        now = datetime.now(UTC)
        for alias in ("max-b", "max-c", "max-d"):
            _insert_capacity_observation(
                runtime, alias, "available", observed_at=now - timedelta(hours=1)
            )
        assert runtime.cells is not None
        pool = runtime.cells._profiles
        pool.restore("demo", "max-a", now)

        first = pool.reserve_replacement("demo", "max-a")
        assert first is not None
        # Deterministic: the first registry-ordered candidate that passes
        # both the auth and the capacity gate.
        assert first.profile_alias == "max-b"
        pool.cancel_replacement("demo")

        # Newer capped observations exclude every first-party candidate;
        # Bedrock max-d still holds available evidence but must never be
        # selected, so the reservation fails closed and the refusal names
        # only real candidates.
        for alias in ("max-b", "max-c"):
            _insert_capacity_observation(
                runtime,
                alias,
                "capped",
                observed_at=now,
                resets_at=now + timedelta(hours=11),
            )
        second = pool.reserve_replacement("demo", "max-a")

        assert second is None
        assert pool.last_refusal is not None
        assert "max-b: fable-capped until" in pool.last_refusal
        assert "max-c: fable-capped until" in pool.last_refusal
        assert "max-d" not in pool.last_refusal
    finally:
        runtime.close()


def test_resolve_prompt_file_prefers_the_activated_runtime(
    tmp_path: Path,
) -> None:
    """INFRA-214 (observed live 2026-09-01): the harness launch failed
    because the prompt resolved from the stale PRIMARY checkout, which
    does not carry the merged ``claude-harness.md``. Assets must come
    from the activated runtime so they are version-matched to the code
    actually running."""

    from hermes_orchestrator.runtime import resolve_prompt_file

    repo_root = tmp_path / "checkout"
    (repo_root / "prompts").mkdir(parents=True)
    state_dir = tmp_path / "state"
    runtime_root = state_dir / "runtimes" / "abc123"
    (runtime_root / "prompts").mkdir(parents=True)
    (runtime_root / "prompts" / "claude-harness.md").write_text("x")
    (state_dir / "runtimes" / "ACTIVE").write_text(str(runtime_root))

    resolved = resolve_prompt_file(
        "claude-harness.md", repo_root=repo_root, state_dir=state_dir
    )

    assert resolved == runtime_root / "prompts" / "claude-harness.md"
    assert resolved.is_file()
    # The stale checkout is never consulted while an ACTIVE runtime exists.
    assert not (repo_root / "prompts" / "claude-harness.md").exists()


def test_resolve_prompt_file_falls_back_only_before_activation(
    tmp_path: Path,
) -> None:
    """With no ACTIVE pointer recorded the documented pre-activation
    fallback applies; an empty pointer is treated the same way."""

    from hermes_orchestrator.runtime import resolve_prompt_file

    repo_root = tmp_path / "checkout"
    (repo_root / "prompts").mkdir(parents=True)
    state_dir = tmp_path / "state"
    (state_dir / "runtimes").mkdir(parents=True)

    expected = repo_root / "prompts" / "claude-lead.md"
    assert (
        resolve_prompt_file(
            "claude-lead.md", repo_root=repo_root, state_dir=state_dir
        )
        == expected
    )

    (state_dir / "runtimes" / "ACTIVE").write_text("   ")
    assert (
        resolve_prompt_file(
            "claude-lead.md", repo_root=repo_root, state_dir=state_dir
        )
        == expected
    )


def test_runtime_reconciles_existing_cell_and_proven_channel_into_ready_pair(
    tmp_path: Path,
) -> None:
    """INFRA-187 wave 2: a live Fable cell and an already-proven Sol
    reviewer channel, both created before this daemon ever started
    (e.g. a restart, or a pre-INFRA-187 project), converge into a
    ready pair the moment ``open_runtime`` runs -- with no scheduler
    cycle or dispatch required first."""

    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    seed_live_fable_cell(state_dir)
    seed_ready_reviewer_channel(state_dir, proven=True)

    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        teams = ProjectTeamService(runtime.database)
        assert "demo" in teams.ready_projects()
        team = teams.live_team("demo")
        assert team is not None
        assert team.state == "ready"
        assert team.fable_cell_id == "cell-1"
        assert team.sol_thread_id == "thr-1"
        assert team.sol_model == MERGER_MODEL
        assert team.sol_provider == MERGER_PROVIDER
    finally:
        runtime.close()


def test_unproven_channel_leaves_pair_not_ready_and_scheduler_holds(
    tmp_path: Path,
) -> None:
    """A reviewer channel that is durably ``ready`` but has never been
    proven (NULL model/provider/model_verified_at -- exactly what a
    legacy pre-INFRA-187 channel looks like) must never bind as the
    pair's Sol member on reconciliation alone; the pair stays short of
    ``ready`` and the scheduler holds queued work for that project
    behind ``pair_not_ready`` rather than resuming it off the live
    Fable cell alone."""

    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    seed_live_fable_cell(state_dir)
    seed_ready_reviewer_channel(state_dir, proven=False)

    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        teams = ProjectTeamService(runtime.database)
        assert "demo" not in teams.ready_projects()
        team = teams.live_team("demo")
        assert team is not None
        assert team.state == "fable_bound"
        assert team.fable_cell_id == "cell-1"
        assert team.sol_thread_id is None

        assert runtime.cells is not None
        runtime.queue.admit(
            AdmissionRequest(
                issue_id="ENG-1",
                project_key="demo",
                linear_priority=1,
                admitted_by="operator",
                instruction_id="chat-ENG-1",
            )
        )
        # The exact ``ready_pairs`` shape ``open_runtime`` wires into its
        # own scheduler -- a live cell is active for "demo", but the
        # unproven channel never let the pair become ready, so this
        # must hold rather than resume.
        scheduler = Scheduler(
            runtime.queue,
            mode="active",
            active_projects=runtime.cells.active_projects,
            ready_pairs=teams.ready_projects,
        )

        actions = scheduler.plan(_FakeSnapshot(pressure="green", can_admit=True))

        assert [(action.kind, action.execute) for action in actions] == [
            ("pair_not_ready", False)
        ]
        assert actions[0].project_key == "demo"
    finally:
        runtime.close()
