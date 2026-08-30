from __future__ import annotations

import asyncio
import contextlib
import json
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import pytest

import hermes_orchestrator.runtime as runtime_module
from hermes_orchestrator.channel_hub import ChannelLauncher
from hermes_orchestrator.cmux_surfaces import (
    CHANNEL_ENTRY,
    SKIP_PERMISSIONS_FLAG,
    CmuxLeadSeater,
    CmuxSurfaceRef,
)
from hermes_orchestrator.config import load_settings
from hermes_orchestrator.profiles import JsonCommand
from hermes_orchestrator.runtime import (
    DaemonAlreadyRunning,
    open_runtime,
    resolve_sidecar_entry,
)


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


def active_repo(tmp_path: Path) -> tuple[Path, Path]:
    config = tmp_path / "config"
    config.mkdir()
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts/codex-merger.md").write_text("# merger\n", encoding="utf-8")
    (tmp_path / "prompts/claude-lead.md").write_text(
        "Work only on explicitly queued work.\n",
        encoding="utf-8",
    )
    (config / "projects.yaml").write_text(
        "projects:\n"
        "  demo:\n"
        "    linear_team: engineering\n"
        f"    repo_path: {tmp_path}\n"
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


def cmux_repo(base: Path) -> tuple[Path, Path]:
    """An active repo with cmux configured and a sidecar build present."""

    repo_root, state_dir = active_repo(base)
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
            assert command == (
                f"claude --session-id {SEAT_SESSION} {SKIP_PERMISSIONS_FLAG} "
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

    def test_falls_back_to_repo_root_when_the_active_artifact_lacks_one(
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
        artifact.mkdir(parents=True)  # no channels/ inside this artifact
        (state_dir / "runtimes" / "ACTIVE").write_text(
            str(artifact) + "\n", encoding="utf-8"
        )

        resolved = resolve_sidecar_entry(
            repo_root=repo_root, state_dir=state_dir
        )

        assert resolved == repo_entry

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
