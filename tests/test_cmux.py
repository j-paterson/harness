from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from hermes_orchestrator.cmux import (
    CMUX_SOCKET_PASSWORD_ENV,
    CmuxAccessDenied,
    CmuxCliAdapter,
    CmuxProtocolError,
    CmuxSurfaceRef,
    CmuxUnavailable,
)
from hermes_orchestrator.cmux_surfaces import (
    _CLASSIC_COMMAND,
    CHANNEL_ENTRY,
    FAKECHAT_CHANNEL_ENTRY,
    CmuxLeadSeater,
    CmuxSurfaceBindings,
    classic_fakechat_command,
)
from hermes_orchestrator.control_operations import ControlOperations
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore

WORKSPACE = "11111111-2222-4333-8444-555555555555"
SURFACE = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"

DENIAL = b"Error: ERROR: Access denied - only processes started inside cmux can connect"


@dataclass
class FakeProcess:
    returncode: int = 0
    stdout: bytes = b""
    stderr: bytes = b""
    hang: bool = False
    killed: bool = False

    async def communicate(self) -> tuple[bytes, bytes]:
        if self.hang:
            await asyncio.sleep(3600)
        return self.stdout, self.stderr

    def kill(self) -> None:
        self.killed = True
        self.hang = False

    async def wait(self) -> int:
        return self.returncode


@dataclass
class FakeFactory:
    results: list[FakeProcess] = field(default_factory=list)
    calls: list[tuple[tuple[str, ...], dict[str, str]]] = field(
        default_factory=list
    )

    async def __call__(self, *argv: str, **kwargs: object) -> FakeProcess:
        env = dict(kwargs.get("env") or {})
        self.calls.append((argv, env))
        return self.results.pop(0)


def adapter(
    factory: FakeFactory, **kwargs: object
) -> CmuxCliAdapter:
    return CmuxCliAdapter(
        ("/apps/cmux",), process_factory=factory, **kwargs
    )


@pytest.mark.asyncio
async def test_password_travels_only_through_the_environment() -> None:
    factory = FakeFactory(results=[FakeProcess(stdout=b"pong")])
    port = adapter(factory, password_source=lambda: "socket-secret")

    await port.ping()

    argv, env = factory.calls[0]
    assert env[CMUX_SOCKET_PASSWORD_ENV] == "socket-secret"
    assert all("socket-secret" not in part for part in argv)
    assert argv[1:4] == ("--id-format", "uuids", "ping")
    assert env["CMUX_QUIET"] == "1"


@pytest.mark.asyncio
async def test_missing_password_source_sends_no_password_variable() -> None:
    factory = FakeFactory(results=[FakeProcess()])
    port = adapter(
        factory,
        base_env={CMUX_SOCKET_PASSWORD_ENV: "inherited", "PATH": "/bin"},
    )

    await port.ping()

    _, env = factory.calls[0]
    # An inherited password is scrubbed: the secret flows only from the
    # explicit Keychain-backed source, never ambient process state.
    assert CMUX_SOCKET_PASSWORD_ENV not in env
    assert env["PATH"] == "/bin"


@pytest.mark.asyncio
async def test_socket_denial_fails_closed_without_leaking_output() -> None:
    factory = FakeFactory(
        results=[FakeProcess(returncode=1, stderr=DENIAL)]
    )
    port = adapter(factory, password_source=lambda: "socket-secret")

    with pytest.raises(CmuxAccessDenied) as denial:
        await port.ping()

    assert "socket-secret" not in str(denial.value)
    assert "inside cmux" not in str(denial.value)


@pytest.mark.asyncio
async def test_other_failures_report_exit_code_only() -> None:
    factory = FakeFactory(
        results=[FakeProcess(returncode=3, stderr=b"/private/path oops")]
    )
    port = adapter(factory)

    with pytest.raises(CmuxUnavailable) as failure:
        await port.ping()

    assert "exit 3" in str(failure.value)
    assert "oops" not in str(failure.value)


@pytest.mark.asyncio
async def test_hung_cli_is_killed_and_reported_unavailable() -> None:
    hung = FakeProcess(hang=True)
    factory = FakeFactory(results=[hung])
    port = adapter(factory, timeout_seconds=0.01)

    with pytest.raises(CmuxUnavailable, match="timed out"):
        await port.ping()

    assert hung.killed


@pytest.mark.asyncio
async def test_spawn_failure_reports_type_only() -> None:
    class Exploding:
        async def __call__(self, *argv: str, **kwargs: object) -> None:
            raise OSError("/apps/cmux: no such file")

    port = CmuxCliAdapter(("/apps/cmux",), process_factory=Exploding())

    with pytest.raises(CmuxUnavailable) as failure:
        await port.ping()

    assert "OSError" in str(failure.value)
    assert "/apps/cmux" not in str(failure.value)


@pytest.mark.asyncio
async def test_create_workspace_returns_typed_identities() -> None:
    factory = FakeFactory(
        results=[
            FakeProcess(stdout=f"workspace {WORKSPACE}\n".encode()),
            FakeProcess(
                stdout=f"{WORKSPACE} pane surface {SURFACE}\n".encode()
            ),
        ]
    )
    port = adapter(factory)

    ref = await port.create_workspace(
        title="Orchestrator",
        cwd=Path("/repos/demo"),
        command="hermes-orchestrator daemon",
        env={"CLAUDE_CONFIG_DIR": "/profiles/max-a"},
    )

    assert ref == CmuxSurfaceRef(
        workspace_uuid=WORKSPACE, surface_uuid=SURFACE
    )
    create_argv = factory.calls[0][0]
    assert "--cwd" in create_argv and "/repos/demo" in create_argv
    assert "--env" in create_argv
    assert "CLAUDE_CONFIG_DIR=/profiles/max-a" in create_argv
    focus_at = create_argv.index("--focus")
    assert create_argv[focus_at + 1] == "false"


MARKER = "[hermes:op-1]"


@pytest.mark.asyncio
async def test_short_ack_resolves_through_the_durable_marker() -> None:
    # cmux 0.64.22 acknowledges the mutation with a short workspace ref
    # instead of a UUID; the adapter accepts it only as an intermediate
    # response and resolves the exact workspace through the metadata
    # listing and the durable activation marker.
    factory = FakeFactory(
        results=[
            FakeProcess(stdout=b"OK workspace:14\n"),
            FakeProcess(
                stdout=f"{WORKSPACE} demo lead {MARKER}\n".encode()
            ),
            FakeProcess(
                stdout=f"{WORKSPACE} pane surface {SURFACE}\n".encode()
            ),
        ]
    )
    port = adapter(factory)

    ref = await port.create_workspace(
        title=f"demo lead {MARKER}",
        cwd=Path("/repos/demo"),
        resolve_marker=MARKER,
    )

    assert ref == CmuxSurfaceRef(
        workspace_uuid=WORKSPACE, surface_uuid=SURFACE
    )
    commands = [call[0][3] for call in factory.calls]
    assert commands == [
        "new-workspace",
        "list-workspaces",
        "list-pane-surfaces",
    ]


@pytest.mark.asyncio
async def test_short_ack_resolution_matches_the_live_cmux_output_shapes() -> None:
    # Byte-shapes captured verbatim from the installed cmux (0.64.20)
    # during live characterization: uppercase UUIDs, leading spaces in
    # the workspace listing, and a starred [selected] surface line.
    live_workspace = "F4414CAE-4FBD-447A-AF99-48F1E85C3E63"
    live_surface = "F2D6008C-7D5F-4E7A-9E23-F3938A0FFD91"
    factory = FakeFactory(
        results=[
            FakeProcess(stdout=b"OK workspace:17\n"),
            FakeProcess(
                stdout=(
                    f"  {live_workspace}  hermes-probe {MARKER}\n"
                ).encode()
            ),
            FakeProcess(
                stdout=(
                    f"* {live_surface}  /tmp  [selected]\n"
                ).encode()
            ),
        ]
    )

    ref = await adapter(factory).create_workspace(
        title=f"hermes-probe {MARKER}",
        cwd=Path("/tmp"),
        resolve_marker=MARKER,
    )

    assert ref == CmuxSurfaceRef(
        workspace_uuid=live_workspace, surface_uuid=live_surface
    )


@pytest.mark.asyncio
async def test_short_ack_with_zero_marker_matches_fails_closed() -> None:
    factory = FakeFactory(
        results=[
            FakeProcess(stdout=b"OK workspace:14\n"),
            FakeProcess(stdout=f"{WORKSPACE} other lead\n".encode()),
        ]
    )

    with pytest.raises(CmuxProtocolError, match="exactly one"):
        await adapter(factory).create_workspace(
            title=f"demo lead {MARKER}",
            cwd=Path("/repos/demo"),
            resolve_marker=MARKER,
        )


@pytest.mark.asyncio
async def test_short_ack_with_multiple_marker_matches_fails_closed() -> None:
    other = "99999999-8888-4777-8666-555555555544"
    factory = FakeFactory(
        results=[
            FakeProcess(stdout=b"OK workspace:14\n"),
            FakeProcess(
                stdout=(
                    f"{WORKSPACE} demo lead {MARKER}\n"
                    f"{other} demo lead {MARKER}\n"
                ).encode()
            ),
        ]
    )

    with pytest.raises(CmuxProtocolError, match="exactly one"):
        await adapter(factory).create_workspace(
            title=f"demo lead {MARKER}",
            cwd=Path("/repos/demo"),
            resolve_marker=MARKER,
        )


@pytest.mark.asyncio
async def test_short_ack_without_a_marker_fails_closed() -> None:
    factory = FakeFactory(results=[FakeProcess(stdout=b"OK workspace:14\n")])

    with pytest.raises(CmuxProtocolError, match="marker"):
        await adapter(factory).create_workspace(
            title="demo lead", cwd=Path("/repos/demo")
        )


@pytest.mark.asyncio
async def test_arbitrary_ack_text_is_never_treated_as_a_short_ref() -> None:
    # Only the exact short mutation acknowledgement shape is accepted as
    # an intermediate response; anything else stays a protocol failure
    # even when a marker could have resolved it.
    factory = FakeFactory(
        results=[FakeProcess(stdout=b"workspace created just fine\n")]
    )

    with pytest.raises(CmuxProtocolError, match="workspace identity"):
        await adapter(factory).create_workspace(
            title=f"demo lead {MARKER}",
            cwd=Path("/repos/demo"),
            resolve_marker=MARKER,
        )


@pytest.mark.asyncio
async def test_create_workspace_without_identities_fails_closed() -> None:
    factory = FakeFactory(results=[FakeProcess(stdout=b"created ok")])
    port = adapter(factory)

    with pytest.raises(CmuxProtocolError):
        await port.create_workspace(title="x", cwd=Path("/tmp"))

    factory.results.append(FakeProcess(stdout=f"{WORKSPACE}\n".encode()))
    factory.results.append(FakeProcess(stdout=f"{WORKSPACE}\n".encode()))
    with pytest.raises(CmuxProtocolError):
        await port.create_workspace(title="x", cwd=Path("/tmp"))


@pytest.mark.asyncio
async def test_find_workspace_uuids_matches_only_exact_marker_tokens() -> None:
    other = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    embedded = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    superstring = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    listing = (
        f"{WORKSPACE} demo lead [hermes:op-1]\n"
        f"{other} demo lead\n"
        f"{embedded} demo lead x[hermes:op-1]y\n"
        f"{superstring} demo lead [hermes:op-12]\n"
        "no identity on this line [hermes:op-1]\n"
    )
    factory = FakeFactory(results=[FakeProcess(stdout=listing.encode())])
    port = adapter(factory)

    found = await port.find_workspace_uuids(title_marker="[hermes:op-1]")

    # Only the workspace whose own listing line carries the marker as an
    # exact whitespace-delimited token is returned: superstrings, markers
    # embedded in longer tokens, and unrelated titles never match, and
    # the query speaks the already allow-listed metadata listing.
    assert found == frozenset({WORKSPACE})
    assert factory.calls[0][0][3] == "list-workspaces"
    with pytest.raises(ValueError, match="requires a marker"):
        await port.find_workspace_uuids(title_marker="   ")


@pytest.mark.asyncio
async def test_find_workspace_uuids_returns_every_exact_duplicate() -> None:
    duplicate = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    listing = (
        f"{WORKSPACE} demo lead [hermes:op-1]\n"
        f"{duplicate} demo lead [hermes:op-1]\n"
    )
    factory = FakeFactory(results=[FakeProcess(stdout=listing.encode())])
    port = adapter(factory)

    found = await port.find_workspace_uuids(title_marker="[hermes:op-1]")

    # Duplicated markers are all reported so the caller can refuse the
    # ambiguity; the adapter never picks one arbitrarily.
    assert found == frozenset({WORKSPACE, duplicate})


@pytest.mark.asyncio
async def test_surface_liveness_requires_exact_workspace_and_surface() -> None:
    ref = CmuxSurfaceRef(workspace_uuid=WORKSPACE, surface_uuid=SURFACE)
    factory = FakeFactory(
        results=[
            FakeProcess(stdout=f"{WORKSPACE}\n".encode()),
            FakeProcess(stdout=f"{SURFACE}\n".encode()),
            FakeProcess(stdout=b"no workspaces"),
        ]
    )
    port = adapter(factory)

    assert await port.surface_alive(ref) is True
    assert await port.surface_alive(ref) is False


@pytest.mark.asyncio
async def test_screen_and_input_commands_are_structurally_rejected() -> None:
    port = adapter(FakeFactory())

    for forbidden in (
        "read-screen",
        "capture-pane",
        "send",
        "send-key",
        "pipe-pane",
    ):
        with pytest.raises(ValueError, match="not allow-listed"):
            await port._run(forbidden)


@pytest.mark.asyncio
async def test_metadata_commands_build_expected_argv() -> None:
    factory = FakeFactory(results=[FakeProcess() for _ in range(5)])
    port = adapter(factory)
    ref = CmuxSurfaceRef(workspace_uuid=WORKSPACE, surface_uuid=SURFACE)

    await port.set_status(WORKSPACE, "issue", "INFRA-185")
    await port.set_progress(WORKSPACE, 1.5, "review")
    await port.notify(WORKSPACE, "Lead completed", "INFRA-185")
    await port.focus_workspace(WORKSPACE)
    await port.set_surface_resume(ref, "claude --resume abc")

    commands = [call[0][3] for call in factory.calls]
    assert commands == [
        "set-status",
        "set-progress",
        "notify",
        "select-workspace",
        "surface",
    ]
    progress_argv = factory.calls[1][0]
    assert "1.00" in progress_argv
    resume_argv = factory.calls[4][0]
    assert resume_argv[-1] == "claude --resume abc"
    assert SURFACE in resume_argv


@pytest.mark.asyncio
async def test_the_only_text_path_is_the_closed_signal_grammar() -> None:
    # The bounded signal (operator-approved 2026-08-29) is the sole
    # route by which any text reaches a terminal, and it accepts only
    # the closed grammar. The general vocabulary still rejects every
    # raw input command outright, so arbitrary typing remains
    # structurally impossible.
    port = adapter(FakeFactory())

    for forbidden in ("send", "send-key", "send-panel", "paste-buffer"):
        with pytest.raises(ValueError, match="not allow-listed"):
            await port._run(forbidden)


VALID_ID = "0123456789abcdef0123456789abcdef"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind", ["HERMES_CORRECTION_READY", "HERMES_WORK_READY"]
)
async def test_signal_builds_exactly_one_bounded_send(kind: str) -> None:
    # cmux is the bounded signal plane: exactly one send operation
    # carrying the envelope and Return together (the trailing newline),
    # addressed to the exact workspace and surface. SQLite remains the
    # authoritative data plane; no payload ever rides this channel.
    factory = FakeFactory(results=[FakeProcess()])
    port = adapter(factory)
    ref = CmuxSurfaceRef(workspace_uuid=WORKSPACE, surface_uuid=SURFACE)
    envelope = f"{kind} {VALID_ID}\n"

    await port.deliver_intake_envelope(ref, envelope)

    assert len(factory.calls) == 1
    assert factory.calls[0][0] == (
        "/apps/cmux",
        "send",
        "--workspace",
        WORKSPACE,
        "--surface",
        SURFACE,
        envelope,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rejected",
    [
        "run this payload",
        f"HERMES_CORRECTION_READY {VALID_ID}",  # missing Return
        f"HERMES_CORRECTION_READY {VALID_ID}\n\n",  # double Return
        f"HERMES_CORRECTION_READY {VALID_ID} extra\n",
        f"HERMES_CORRECTION_READY {VALID_ID[:31]}\n",  # short id
        f"HERMES_CORRECTION_READY {VALID_ID}0\n",  # long id
        f"HERMES_CORRECTION_READY {'G' * 32}\n",  # non-hex
        f"HERMES_CORRECTION_READY {VALID_ID.upper()}\n",
        f" HERMES_CORRECTION_READY {VALID_ID}\n",  # leading space
        f"HERMES_CORRECTION_READY {VALID_ID} \n",  # trailing space
        f"HERMES_CORRECTION_READY\n{VALID_ID}\n",  # embedded newline
        f"HERMES_CORRECTION_READY {VALID_ID}; rm -rf /\n",
        f"HERMES_CORRECTION_READY {VALID_ID} && echo pwn\n",
        f"HERMES_CORRECTION_READY $({VALID_ID})\n",
        f"send-key {VALID_ID}\n",
        f"HERMES_FEEDBACK_ACK {VALID_ID}\n",  # foreign kind
        f"HERMES_ANYTHING {VALID_ID}\n",
        "",
    ],
)
async def test_everything_outside_the_signal_grammar_is_refused(
    rejected: str,
) -> None:
    factory = FakeFactory()
    port = adapter(factory)
    ref = CmuxSurfaceRef(workspace_uuid=WORKSPACE, surface_uuid=SURFACE)

    with pytest.raises(ValueError, match="signal grammar"):
        await port.deliver_intake_envelope(ref, rejected)

    # The subprocess runner never ran: refusal happens before any
    # external effect, and raw send/send-key stay rejected generally.
    assert factory.calls == []
    with pytest.raises(ValueError, match="not allow-listed"):
        await port._run("send")


# ---------------------------------------------------------------------------
# H6 (INFRA-197 v4 amendment): CmuxLeadSeater's fakechat channel launch path.
# ---------------------------------------------------------------------------

LEAD_SESSION = "99999999-9999-4999-8999-999999999999"
ROTATED_SESSION = "88888888-8888-4888-8888-888888888888"
LEAD_CELL = "cell-demo"
LEAD_PROJECT = "demo"
LEAD_PROFILE = "max-a"
LEAD_WORKSPACE = CmuxSurfaceRef(
    workspace_uuid="33333333-3333-4333-8333-333333333333",
    surface_uuid="33333333-3333-4333-8333-444444444444",
)
ROTATED_WORKSPACE = CmuxSurfaceRef(
    workspace_uuid="66666666-6666-4666-8666-666666666666",
    surface_uuid="66666666-6666-4666-8666-777777777777",
)


@dataclass(frozen=True)
class FakeIssuedPort:
    port: int


@dataclass
class FakeSeaterPort:
    """Minimal fake CmuxControlPort sufficient to drive CmuxLeadSeater
    through a single fresh-seat activation (no reconciliation surfaces)."""

    next_refs: list[CmuxSurfaceRef] = field(default_factory=list)
    live: set[CmuxSurfaceRef] = field(default_factory=set)
    created: list[dict[str, object]] = field(default_factory=list)
    resumes: list[tuple[CmuxSurfaceRef, str]] = field(default_factory=list)
    closed: list[str] = field(default_factory=list)

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
        self.closed.append(workspace_uuid)
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
        raise NotImplementedError(
            "these tests never exercise reconciliation or residual/"
            "pending-intent resolution"
        )


class FakeProfileDirs:
    def __init__(self, dirs: dict[str, Path]) -> None:
        self._dirs = dirs

    def config_dir(self, alias: str) -> Path:
        return self._dirs[alias]


@dataclass
class FakeSignalPorts:
    """Records fakechat port issuance/retirement like FakechatSignalPorts,
    without any durable storage of its own."""

    port: int = 40123
    error: Exception | None = None
    issued: list[dict[str, object]] = field(default_factory=list)
    retired: list[str] = field(default_factory=list)

    def issue(
        self,
        *,
        cell_id: str,
        session_id: str,
        binding_id: str,
        generation: int,
        port: int | None = None,
    ) -> FakeIssuedPort:
        self.issued.append(
            {
                "cell_id": cell_id,
                "session_id": session_id,
                "binding_id": binding_id,
                "generation": generation,
                "port": port,
            }
        )
        if self.error is not None:
            raise self.error
        return FakeIssuedPort(self.port)

    def retire(self, session_id: str) -> None:
        self.retired.append(session_id)


class FakeChannelLaunch:
    """Records dev-channel launch-material generation and retirement."""

    def __init__(self, config: Path | None = None) -> None:
        self.config = config
        self.generated: list[dict[str, object]] = []
        self.cleaned: list[str] = []

    def generate(self, **kwargs: object) -> Path:
        self.generated.append(kwargs)
        assert self.config is not None
        return self.config

    def cleanup(self, session_id: str) -> None:
        self.cleaned.append(session_id)


@pytest.fixture
def seater_database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "seater-state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def seater_bindings(seater_database: Database) -> CmuxSurfaceBindings:
    counter = iter(range(1, 100))
    return CmuxSurfaceBindings(
        database=seater_database,
        events=EventStore(seater_database),
        ids=lambda: f"binding-{next(counter)}",
    )


def make_seater(
    bindings: CmuxSurfaceBindings,
    port: FakeSeaterPort,
    *,
    signal_ports: FakeSignalPorts | None = None,
    channel_launch: FakeChannelLaunch | None = None,
    control: ControlOperations | None = None,
) -> CmuxLeadSeater:
    return CmuxLeadSeater(
        bindings=bindings,
        port=port,
        project_paths={LEAD_PROJECT: Path("/repos/demo")},
        profile_dirs=FakeProfileDirs({LEAD_PROFILE: Path("/profiles/max-a")}),
        signal_ports=signal_ports,
        channel_launch=channel_launch,
        control=control,
    )


def test_classic_fakechat_command_is_exact_and_grammar_bound() -> None:
    command = classic_fakechat_command(LEAD_SESSION, resume=True)

    assert command == (
        f"claude --resume {LEAD_SESSION} --channels {FAKECHAT_CHANNEL_ENTRY}"
    )
    assert _CLASSIC_COMMAND.fullmatch(command) is not None


def test_classic_fakechat_command_canonicalizes_the_session_uuid() -> None:
    command = classic_fakechat_command(LEAD_SESSION.upper(), resume=False)

    assert command == (
        f"claude --session-id {LEAD_SESSION} --channels {FAKECHAT_CHANNEL_ENTRY}"
    )
    with pytest.raises(ValueError):
        classic_fakechat_command("not-a-uuid", resume=True)


def test_classic_fakechat_grammar_rejects_anything_extra() -> None:
    base = classic_fakechat_command(LEAD_SESSION, resume=True)
    other_plugin = base.replace(
        FAKECHAT_CHANNEL_ENTRY, "plugin:other@claude-plugins-official"
    )

    for rogue in (
        base + " --extra flag",
        other_plugin,
        base + f" --channels {FAKECHAT_CHANNEL_ENTRY}",
        base + "; rm -rf /",
        base.replace("claude ", "bash -c 'claude '"),
    ):
        assert _CLASSIC_COMMAND.fullmatch(rogue) is None


@pytest.mark.asyncio
async def test_ensure_prefers_fakechat_when_signal_ports_present(
    seater_bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeSeaterPort(next_refs=[LEAD_WORKSPACE])
    signal_ports = FakeSignalPorts(port=40123)
    ensure = make_seater(seater_bindings, port, signal_ports=signal_ports)

    seat = await ensure.ensure(
        project_key=LEAD_PROJECT,
        cell_id=LEAD_CELL,
        session_id=LEAD_SESSION,
        profile_alias=LEAD_PROFILE,
        classic_command=f"claude --session-id {LEAD_SESSION}",
    )

    assert seat is not None
    [created] = port.created
    assert created["command"] == (
        f"claude --session-id {LEAD_SESSION} --channels {FAKECHAT_CHANNEL_ENTRY}"
    )
    assert created["env"] == {
        "CLAUDE_CONFIG_DIR": "/profiles/max-a",
        "FAKECHAT_PORT": "40123",
    }
    # The port was issued exactly once, with this exact cell/session
    # identity and no other.
    assert len(signal_ports.issued) == 1
    [issued] = signal_ports.issued
    assert issued["cell_id"] == LEAD_CELL
    assert issued["session_id"] == LEAD_SESSION


@pytest.mark.asyncio
async def test_signal_port_issue_failure_falls_back_and_records_one_receipt(
    seater_database: Database, seater_bindings: CmuxSurfaceBindings
) -> None:
    port = FakeSeaterPort(next_refs=[LEAD_WORKSPACE])
    signal_ports = FakeSignalPorts(error=RuntimeError("no free loopback port"))
    control = ControlOperations(seater_database, events=EventStore(seater_database))
    ensure = make_seater(
        seater_bindings, port, signal_ports=signal_ports, control=control
    )

    seat = await ensure.ensure(
        project_key=LEAD_PROJECT,
        cell_id=LEAD_CELL,
        session_id=LEAD_SESSION,
        profile_alias=LEAD_PROFILE,
        classic_command=f"claude --session-id {LEAD_SESSION}",
    )

    # The failure never raises past ensure(): the seat still comes up,
    # channel-less, on the plain classic command.
    assert seat is not None
    [created] = port.created
    assert created["command"] == f"claude --session-id {LEAD_SESSION}"
    assert created["env"] == {"CLAUDE_CONFIG_DIR": "/profiles/max-a"}
    [receipt] = control.pending_for_session(LEAD_SESSION)
    assert receipt.kind == "channel.blocked"
    assert receipt.result["launcher_error"] == "no free loopback port"


@pytest.mark.asyncio
async def test_signal_ports_none_preserves_the_dev_channel_launch(
    seater_bindings: CmuxSurfaceBindings,
) -> None:
    # Production wiring today (signal_ports=None) must stay
    # byte-identical to the pre-H6 dev-channel behavior.
    port = FakeSeaterPort(next_refs=[LEAD_WORKSPACE])
    channel_launch = FakeChannelLaunch(
        config=Path(f"/state/channels/{LEAD_SESSION}.mcp.json")
    )
    ensure = make_seater(seater_bindings, port, channel_launch=channel_launch)

    seat = await ensure.ensure(
        project_key=LEAD_PROJECT,
        cell_id=LEAD_CELL,
        session_id=LEAD_SESSION,
        profile_alias=LEAD_PROFILE,
        classic_command=f"claude --session-id {LEAD_SESSION}",
    )

    assert seat is not None
    [created] = port.created
    assert created["command"] == (
        f"claude --session-id {LEAD_SESSION} "
        f"--mcp-config /state/channels/{LEAD_SESSION}.mcp.json "
        f"--dangerously-load-development-channels {CHANNEL_ENTRY}"
    )
    assert created["env"] == {"CLAUDE_CONFIG_DIR": "/profiles/max-a"}


@pytest.mark.asyncio
async def test_rotation_retires_the_fakechat_port_row(
    seater_bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeSeaterPort(next_refs=[LEAD_WORKSPACE, ROTATED_WORKSPACE])
    signal_ports = FakeSignalPorts(port=40123)
    ensure = make_seater(seater_bindings, port, signal_ports=signal_ports)
    await ensure.ensure(
        project_key=LEAD_PROJECT,
        cell_id=LEAD_CELL,
        session_id=LEAD_SESSION,
        profile_alias=LEAD_PROFILE,
        classic_command=f"claude --session-id {LEAD_SESSION}",
    )

    await ensure.ensure(
        project_key=LEAD_PROJECT,
        cell_id=LEAD_CELL,
        session_id=ROTATED_SESSION,
        profile_alias=LEAD_PROFILE,
        classic_command=f"claude --session-id {ROTATED_SESSION}",
    )

    assert signal_ports.retired == [LEAD_SESSION]
