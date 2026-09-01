from __future__ import annotations

import asyncio
import inspect
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
    SKIP_PERMISSIONS_FLAG,
    CmuxLeadSeater,
    CmuxSurfaceBindings,
    classic_fakechat_command,
    classic_resume_command,
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
# T2 (INFRA-197 v5.1 amendment): the channel-trust gate's bounded read-screen
# and single-Enter confirmation, per decision
# infra-197-trusted-channel-auto-approval-20260830-v1.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_screen_builds_exact_argv_and_returns_output() -> None:
    factory = FakeFactory(results=[FakeProcess(stdout=b"pane text here\n")])
    port = adapter(factory)
    ref = CmuxSurfaceRef(workspace_uuid=WORKSPACE, surface_uuid=SURFACE)

    output = await port.read_screen(ref, lines=42)

    assert output == "pane text here\n"
    assert factory.calls[0][0] == (
        "/apps/cmux",
        "read-screen",
        "--workspace",
        WORKSPACE,
        "--surface",
        SURFACE,
        "--lines",
        "42",
    )


@pytest.mark.asyncio
async def test_read_screen_default_lines_is_sixty() -> None:
    factory = FakeFactory(results=[FakeProcess(stdout=b"")])
    port = adapter(factory)
    ref = CmuxSurfaceRef(workspace_uuid=WORKSPACE, surface_uuid=SURFACE)

    await port.read_screen(ref)

    assert factory.calls[0][0][-1] == "60"


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_lines", [0, -1, -60, 2001, 10_000])
async def test_read_screen_bounds_refuse_before_any_subprocess(
    bad_lines: int,
) -> None:
    factory = FakeFactory()
    port = adapter(factory)
    ref = CmuxSurfaceRef(workspace_uuid=WORKSPACE, surface_uuid=SURFACE)

    with pytest.raises(ValueError, match="positive int"):
        await port.read_screen(ref, lines=bad_lines)

    # Refusal happens before any external effect.
    assert factory.calls == []


@pytest.mark.asyncio
async def test_read_screen_at_the_upper_bound_is_accepted() -> None:
    factory = FakeFactory(results=[FakeProcess(stdout=b"")])
    port = adapter(factory)
    ref = CmuxSurfaceRef(workspace_uuid=WORKSPACE, surface_uuid=SURFACE)

    await port.read_screen(ref, lines=2000)

    assert factory.calls[0][0][-1] == "2000"


@pytest.mark.asyncio
async def test_read_screen_targets_only_the_exact_ref_given() -> None:
    other_workspace = "77777777-7777-4777-8777-777777777777"
    other_surface = "77777777-7777-4777-8777-888888888888"
    factory = FakeFactory(results=[FakeProcess(stdout=b"")])
    port = adapter(factory)
    ref = CmuxSurfaceRef(
        workspace_uuid=other_workspace, surface_uuid=other_surface
    )

    await port.read_screen(ref)

    argv = factory.calls[0][0]
    assert WORKSPACE not in argv and SURFACE not in argv
    assert other_workspace in argv and other_surface in argv


@pytest.mark.asyncio
async def test_confirm_channel_dialog_sends_exactly_one_enter() -> None:
    factory = FakeFactory(results=[FakeProcess()])
    port = adapter(factory)
    ref = CmuxSurfaceRef(workspace_uuid=WORKSPACE, surface_uuid=SURFACE)

    await port.confirm_channel_dialog(ref)

    assert len(factory.calls) == 1
    assert factory.calls[0][0] == (
        "/apps/cmux",
        "send-key",
        "--workspace",
        WORKSPACE,
        "--surface",
        SURFACE,
        "enter",
    )


@pytest.mark.asyncio
async def test_confirm_channel_dialog_targets_only_the_exact_ref_given() -> None:
    other_workspace = "77777777-7777-4777-8777-777777777777"
    other_surface = "77777777-7777-4777-8777-888888888888"
    factory = FakeFactory(results=[FakeProcess()])
    port = adapter(factory)
    ref = CmuxSurfaceRef(
        workspace_uuid=other_workspace, surface_uuid=other_surface
    )

    await port.confirm_channel_dialog(ref)

    argv = factory.calls[0][0]
    assert WORKSPACE not in argv and SURFACE not in argv
    assert other_workspace in argv and other_surface in argv


def test_confirm_channel_dialog_signature_takes_only_the_ref() -> None:
    # The Enter key is a fixed literal inside the method, never a
    # parameter: no caller can express any other key or any text
    # through this method's signature.
    signature = inspect.signature(CmuxCliAdapter.confirm_channel_dialog)
    parameters = list(signature.parameters)
    assert parameters == ["self", "ref"]
    for name in parameters:
        param = signature.parameters[name]
        assert param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )


@pytest.mark.asyncio
async def test_screen_and_key_ops_stay_out_of_the_general_vocabulary() -> None:
    # The general command surface (_run / _ALLOWED_COMMANDS) still
    # rejects read-screen and send-key outright; these two bounded,
    # fixed-shape operations are the sole exceptions, reached only
    # through their own dedicated methods.
    port = adapter(FakeFactory())

    for forbidden in ("read-screen", "send-key"):
        with pytest.raises(ValueError, match="not allow-listed"):
            await port._run(forbidden)


# ---------------------------------------------------------------------------
# H6 (INFRA-197, Sol correction b4b545f3 v5): the fakechat seat-command
# substitution is retired — hermes-control channel launch is the primary
# classic-seat path. The fakechat command builder and its grammar
# alternative survive only so legacy commands stay validate-or-refuse.
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
    channel_launch: FakeChannelLaunch | None = None,
    control: ControlOperations | None = None,
) -> CmuxLeadSeater:
    return CmuxLeadSeater(
        bindings=bindings,
        port=port,
        project_paths={LEAD_PROJECT: Path("/repos/demo")},
        profile_dirs=FakeProfileDirs({LEAD_PROFILE: Path("/profiles/max-a")}),
        channel_launch=channel_launch,
        control=control,
    )


def test_classic_fakechat_command_is_exact_and_grammar_bound() -> None:
    command = classic_fakechat_command(LEAD_SESSION, resume=True)

    assert command == (
        f"claude --resume {LEAD_SESSION} {SKIP_PERMISSIONS_FLAG} "
        f"--channels {FAKECHAT_CHANNEL_ENTRY}"
    )
    assert _CLASSIC_COMMAND.fullmatch(command) is not None


def test_classic_fakechat_command_canonicalizes_the_session_uuid() -> None:
    command = classic_fakechat_command(LEAD_SESSION.upper(), resume=False)

    assert command == (
        f"claude --session-id {LEAD_SESSION} {SKIP_PERMISSIONS_FLAG} "
        f"--channels {FAKECHAT_CHANNEL_ENTRY}"
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
        # The fixed flag cannot be dropped, duplicated, or displaced —
        # a caller-supplied command missing it (the pre-INFRA-197 shape)
        # or repeating it must fail closed too.
        base.replace(f" {SKIP_PERMISSIONS_FLAG}", ""),
        base.replace(
            f" {SKIP_PERMISSIONS_FLAG}",
            f" {SKIP_PERMISSIONS_FLAG} {SKIP_PERMISSIONS_FLAG}",
        ),
        (
            f"claude --resume {LEAD_SESSION} "
            f"--channels {FAKECHAT_CHANNEL_ENTRY} {SKIP_PERMISSIONS_FLAG}"
        ),
    ):
        assert _CLASSIC_COMMAND.fullmatch(rogue) is None


def test_classic_resume_command_carries_the_fixed_flag_before_extensions() -> None:
    # INFRA-197 operator decision
    # infra-197-managed-claude-skip-permissions-20260830-v1: every
    # Hermes-managed classic launch carries the fixed flag, positioned
    # immediately after the UUID so every extension built on top of the
    # builder inherits it by construction.
    assert classic_resume_command(LEAD_SESSION, resume=True) == (
        f"claude --resume {LEAD_SESSION} {SKIP_PERMISSIONS_FLAG}"
    )
    assert classic_resume_command(LEAD_SESSION, resume=False) == (
        f"claude --session-id {LEAD_SESSION} {SKIP_PERMISSIONS_FLAG}"
    )
    assert _CLASSIC_COMMAND.fullmatch(
        classic_resume_command(LEAD_SESSION, resume=True)
    ) is not None
    # No caller input reaches this builder except the session id
    # (already required to parse as a UUID); the flag itself is never a
    # parameter, so nothing expressible through this function's
    # signature can change, remove, or duplicate it.
    assert "resume" not in SKIP_PERMISSIONS_FLAG


def test_classic_command_without_the_fixed_flag_no_longer_validates() -> None:
    # The pre-INFRA-197 shape (no flag at all) must be refused now that
    # the grammar requires it.
    for legacy in (
        f"claude --resume {LEAD_SESSION}",
        f"claude --session-id {LEAD_SESSION}",
        f"claude --resume {LEAD_SESSION} --channels {FAKECHAT_CHANNEL_ENTRY}",
    ):
        assert _CLASSIC_COMMAND.fullmatch(legacy) is None


def test_seater_accepts_no_fakechat_signal_ports_collaborator() -> None:
    """Sol correction b4b545f3 (v5): the seat-command substitution is
    structurally gone — CmuxLeadSeater exposes no signal_ports port
    source, so no composition can ever re-enable the fakechat form."""

    parameters = inspect.signature(CmuxLeadSeater.__init__).parameters
    assert "signal_ports" not in parameters
    assert "channel_launch" in parameters


@pytest.mark.asyncio
async def test_channel_launch_is_the_primary_classic_seat_path(
    seater_bindings: CmuxSurfaceBindings,
) -> None:
    # v5: the hermes-control channel launch is the primary path for a
    # composed classic seat; nothing fakechat-shaped rides along.
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
        classic_command=f"claude --session-id {LEAD_SESSION} {SKIP_PERMISSIONS_FLAG}",
    )

    assert seat is not None
    [created] = port.created
    assert created["command"] == (
        f"claude --session-id {LEAD_SESSION} {SKIP_PERMISSIONS_FLAG} "
        f"--mcp-config /state/channels/{LEAD_SESSION}.mcp.json "
        f"--dangerously-load-development-channels {CHANNEL_ENTRY}"
    )
    assert "fakechat" not in str(created["command"])
    assert created["env"] == {"CLAUDE_CONFIG_DIR": "/profiles/max-a"}



# ---------------------------------------------------------------------------
# INFRA-191 W3: the two-pane create / respawn / process-metadata vocabulary
# for the real two-pane Orchestrator workspace. respawn-pane and top join
# the closed vocabulary as bounded validated methods, the layout form of
# new-workspace creates both side-by-side panes eagerly, and every input
# command and screen read stays structurally rejected.
# ---------------------------------------------------------------------------

LOWER_SURFACE = "12121212-3434-4545-8666-787878787878"
UPPER_PANE = "aaaa1111-0000-4000-8000-000000000001"
LOWER_PANE = "aaaa1111-0000-4000-8000-000000000002"


def test_the_complete_vocabulary_is_pinned() -> None:
    # The whole closed vocabulary, verbatim: any extension must land
    # here deliberately. Screen and input verbs remain absent from the
    # general surface, and new-split/new-pane stay out — characterized
    # live, a split pane in an unfocused workspace never materializes
    # its terminal, so the lifecycle speaks new-workspace --layout.
    from hermes_orchestrator.cmux import _ALLOWED_COMMANDS

    assert frozenset(
        {
            "ping",
            "new-workspace",
            "respawn-pane",
            "top",
            "list-workspaces",
            "list-pane-surfaces",
            "close-workspace",
            "rename-workspace",
            "select-workspace",
            "surface",
            "set-status",
            "clear-status",
            "set-progress",
            "clear-progress",
            "notify",
            "agent-hibernation",
        }
    ) == _ALLOWED_COMMANDS


def _pane_snapshot(*surface_processes: tuple[str, str, list[str]]) -> bytes:
    """A structured process snapshot in the live-characterized shape."""

    import json as json_module

    return json_module.dumps(
        {
            "windows": [
                {
                    "workspaces": [
                        {
                            "id": WORKSPACE,
                            "panes": [
                                {
                                    "id": pane,
                                    "surfaces": [
                                        {
                                            "id": surface,
                                            "processes": [
                                                {"name": name}
                                                for name in names
                                            ],
                                        }
                                    ],
                                }
                                for pane, surface, names in surface_processes
                            ],
                        }
                    ]
                }
            ]
        }
    ).encode()


@pytest.mark.asyncio
async def test_two_pane_create_builds_the_fixed_horizontal_layout() -> None:
    import json as json_module

    factory = FakeFactory(
        results=[
            FakeProcess(stdout=b"OK workspace:11\n"),
            FakeProcess(
                stdout=f"{WORKSPACE} Orchestrator {MARKER}\n".encode()
            ),
            FakeProcess(
                stdout=_pane_snapshot(
                    (UPPER_PANE, SURFACE, ["zsh", "uv"]),
                    (LOWER_PANE, LOWER_SURFACE, ["zsh", "hermes"]),
                )
            ),
        ]
    )
    port = adapter(factory)

    upper, lower = await port.create_two_pane_workspace(
        title=f"Orchestrator {MARKER}",
        cwd=Path("/repos/demo"),
        upper_command="uv run hermes-orchestrator daemon",
        lower_command="hermes chat --continue orch --create-if-missing",
        resolve_marker=MARKER,
    )

    assert upper == CmuxSurfaceRef(
        workspace_uuid=WORKSPACE, surface_uuid=SURFACE
    )
    assert lower == CmuxSurfaceRef(
        workspace_uuid=WORKSPACE, surface_uuid=LOWER_SURFACE
    )
    create_argv = factory.calls[0][0]
    assert create_argv[3] == "new-workspace"
    focus_at = create_argv.index("--focus")
    assert create_argv[focus_at + 1] == "false"
    layout = json_module.loads(create_argv[create_argv.index("--layout") + 1])
    assert layout["direction"] == "horizontal"
    assert layout["split"] == 0.38
    children = layout["children"]
    assert [
        child["pane"]["surfaces"][0]["type"] for child in children
    ] == ["terminal", "terminal"]
    assert children[0]["pane"]["surfaces"][0]["command"] == (
        "uv run hermes-orchestrator daemon"
    )
    assert children[1]["pane"]["surfaces"][0]["command"] == (
        "hermes chat --continue orch --create-if-missing"
    )
    # Identity resolution: short ack -> durable marker -> structured
    # process listing for both panes.
    commands = [call[0][3] for call in factory.calls]
    assert commands == ["new-workspace", "list-workspaces", "top"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "command", ["", "run\nrun", "beep\x07", "x" * 501]
)
async def test_two_pane_create_refuses_unbounded_commands(
    command: str,
) -> None:
    factory = FakeFactory()
    port = adapter(factory)

    with pytest.raises(ValueError, match="bounded printable line"):
        await port.create_two_pane_workspace(
            title="Orchestrator",
            cwd=Path("/repos/demo"),
            upper_command=command,
            lower_command="hermes chat --continue orch --create-if-missing",
        )
    with pytest.raises(ValueError, match="bounded printable line"):
        await port.create_two_pane_workspace(
            title="Orchestrator",
            cwd=Path("/repos/demo"),
            upper_command="uv run hermes-orchestrator daemon",
            lower_command=command,
        )

    assert factory.calls == []


@pytest.mark.asyncio
async def test_two_pane_create_waits_for_both_panes_to_report() -> None:
    import hermes_orchestrator.cmux as cmux_module

    factory = FakeFactory(
        results=[
            FakeProcess(stdout=f"workspace {WORKSPACE}\n".encode()),
            FakeProcess(stdout=_pane_snapshot()),
            FakeProcess(
                stdout=_pane_snapshot(
                    (UPPER_PANE, SURFACE, ["zsh"]),
                    (LOWER_PANE, LOWER_SURFACE, ["zsh"]),
                )
            ),
        ]
    )
    port = adapter(factory)
    original = cmux_module._PANE_POLL_DELAY_SECONDS
    cmux_module._PANE_POLL_DELAY_SECONDS = 0.0
    try:
        upper, lower = await port.create_two_pane_workspace(
            title="Orchestrator",
            cwd=Path("/repos/demo"),
            upper_command="uv run hermes-orchestrator daemon",
            lower_command="hermes chat --continue orch --create-if-missing",
        )
    finally:
        cmux_module._PANE_POLL_DELAY_SECONDS = original

    assert upper.surface_uuid == SURFACE
    assert lower.surface_uuid == LOWER_SURFACE


@pytest.mark.asyncio
async def test_two_pane_create_fails_closed_without_two_distinct_panes() -> (
    None
):
    import hermes_orchestrator.cmux as cmux_module

    factory = FakeFactory(
        results=[FakeProcess(stdout=f"workspace {WORKSPACE}\n".encode())]
        + [
            FakeProcess(
                stdout=_pane_snapshot(
                    (UPPER_PANE, SURFACE, ["zsh"]),
                    (UPPER_PANE, LOWER_SURFACE, ["zsh"]),
                )
            )
            for _ in range(cmux_module._PANE_POLL_ATTEMPTS)
        ]
    )
    port = adapter(factory)
    original = cmux_module._PANE_POLL_DELAY_SECONDS
    cmux_module._PANE_POLL_DELAY_SECONDS = 0.0
    try:
        with pytest.raises(
            CmuxProtocolError, match="two side-by-side panes"
        ):
            await port.create_two_pane_workspace(
                title="Orchestrator",
                cwd=Path("/repos/demo"),
                upper_command="uv run hermes-orchestrator daemon",
                lower_command=(
                    "hermes chat --continue orch --create-if-missing"
                ),
            )
    finally:
        cmux_module._PANE_POLL_DELAY_SECONDS = original


@pytest.mark.asyncio
async def test_respawn_surface_builds_exact_argv() -> None:
    factory = FakeFactory(results=[FakeProcess()])
    port = adapter(factory)
    ref = CmuxSurfaceRef(workspace_uuid=WORKSPACE, surface_uuid=SURFACE)

    await port.respawn_surface(
        ref, "hermes chat --continue orchestrator --create-if-missing"
    )

    argv = factory.calls[0][0]
    assert argv[3:] == (
        "respawn-pane",
        "--workspace",
        WORKSPACE,
        "--surface",
        SURFACE,
        "--command",
        "hermes chat --continue orchestrator --create-if-missing",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "command",
    ["", "run\nrun", "beep\x07", "tab\tsplit", "x" * 501],
)
async def test_respawn_refuses_unbounded_or_multiline_commands(
    command: str,
) -> None:
    factory = FakeFactory()
    port = adapter(factory)
    ref = CmuxSurfaceRef(workspace_uuid=WORKSPACE, surface_uuid=SURFACE)

    with pytest.raises(ValueError, match="bounded printable line"):
        await port.respawn_surface(ref, command)

    assert factory.calls == []


def _top_document() -> bytes:
    import json as json_module

    return json_module.dumps(
        {
            "windows": [
                {
                    "kind": "window",
                    "id": "feedface-0000-4000-8000-000000000000",
                    "workspaces": [
                        {
                            "kind": "workspace",
                            "id": WORKSPACE,
                            "title": "Orchestrator",
                            "panes": [
                                {
                                    "kind": "pane",
                                    "id": UPPER_PANE,
                                    "surfaces": [
                                        {
                                            "kind": "surface",
                                            "id": SURFACE,
                                            "processes": [
                                                {
                                                    "kind": "process",
                                                    "name": "zsh",
                                                    "pid": 10,
                                                    "children": [
                                                        {
                                                            "kind": "process",
                                                            "name": "uv",
                                                            "pid": 11,
                                                            "children": [
                                                                {
                                                                    "name": (
                                                                        "python3.13"
                                                                    ),
                                                                    "pid": 12,
                                                                }
                                                            ],
                                                        }
                                                    ],
                                                }
                                            ],
                                        }
                                    ],
                                },
                                {
                                    "kind": "pane",
                                    "id": LOWER_PANE,
                                    "surfaces": [
                                        {
                                            "kind": "surface",
                                            "id": LOWER_SURFACE,
                                            "processes": [
                                                {"name": "zsh", "pid": 20},
                                                {"name": "hermes", "pid": 21},
                                            ],
                                        }
                                    ],
                                },
                            ],
                        },
                        {
                            "kind": "workspace",
                            "id": "0badc0de-0000-4000-8000-000000000000",
                            "panes": [
                                {
                                    "kind": "pane",
                                    "id": "ffff0000-0000-4000-8000-000000000009",
                                    "surfaces": [
                                        {
                                            "id": (
                                                "ffff0000-0000-4000-8000-"
                                                "00000000000a"
                                            ),
                                            "processes": [
                                                {"name": "vim", "pid": 30}
                                            ],
                                        }
                                    ],
                                }
                            ],
                        },
                    ],
                }
            ]
        }
    ).encode()


@pytest.mark.asyncio
async def test_workspace_processes_returns_typed_metadata_only() -> None:
    factory = FakeFactory(results=[FakeProcess(stdout=_top_document())])
    port = adapter(factory)

    rows = await port.workspace_processes(WORKSPACE.lower())

    argv = factory.calls[0][0]
    assert argv[3:] == (
        "top",
        "--workspace",
        WORKSPACE.lower(),
        "--processes",
        "--json",
    )
    assert [
        (row.pane_uuid, row.surface_uuid, row.process_names) for row in rows
    ] == [
        (UPPER_PANE, SURFACE, ("zsh", "uv", "python3.13")),
        (LOWER_PANE, LOWER_SURFACE, ("zsh", "hermes")),
    ]
    # Sol L1: the adapter exposes the PIDs the top tree already
    # carries, parallel to the names, so lineage can be correlated to
    # exactly this surface's processes and their descendants.
    assert [row.process_ids for row in rows] == [(10, 11, 12), (20, 21)]


@pytest.mark.asyncio
async def test_workspace_processes_refuses_unstructured_output() -> None:
    factory = FakeFactory(results=[FakeProcess(stdout=b"not json at all")])
    port = adapter(factory)

    with pytest.raises(CmuxProtocolError, match="structured process"):
        await port.workspace_processes(WORKSPACE)


def test_activation_intent_carries_the_seat_lane(
    seater_bindings: CmuxSurfaceBindings, seater_database: Database
) -> None:
    """INFRA-214 (observed live 2026-09-01): the lane never reached the
    activation intent, so BOTH failed harness seats persisted as
    ``lane_role=development`` and their residue was indistinguishable
    from the development lane's own binding. ``record_intent`` always
    accepted the lane; the seat simply never passed it, so this pins
    the durable row rather than the parameter default.
    """

    intent = seater_bindings.record_intent(
        project_key="demo",
        cell_id="cell-harness",
        session_id="11111111-1111-4111-8111-111111111111",
        profile_alias="max-b",
        lane_role="harness",
    )

    stored = seater_database.execute(
        "SELECT lane_role FROM cmux_activation_intents WHERE intent_id = ?",
        (intent.intent_id,),
    ).fetchone()
    assert stored["lane_role"] == "harness"

    # A development activation is unchanged and never mislabelled.
    development = seater_bindings.record_intent(
        project_key="demo",
        cell_id="cell-dev",
        session_id="22222222-2222-4222-8222-222222222222",
        profile_alias="max-a",
    )
    stored_dev = seater_database.execute(
        "SELECT lane_role FROM cmux_activation_intents WHERE intent_id = ?",
        (development.intent_id,),
    ).fetchone()
    assert stored_dev["lane_role"] == "development"


@pytest.mark.asyncio
async def test_retire_failed_seat_closes_the_workspace_then_marks_closed(
    seater_bindings: CmuxSurfaceBindings, seater_database: Database
) -> None:
    """INFRA-214 (observed live 2026-09-01): the two failed harness
    starts left TWO dead visible cmux workspaces plus active binding
    residue. Marking only the durable row closed would HIDE that
    residue; the workspace must actually close first, and the session's
    channel config must be cleaned once its surface is gone.
    """

    port = FakeSeaterPort()
    channel = FakeChannelLaunch(config=Path("/tmp/x.mcp.json"))
    seater = make_seater(seater_bindings, port, channel_launch=channel)
    ref = CmuxSurfaceRef(workspace_uuid="ws-1", surface_uuid="sf-1")
    binding = seater_bindings.bind_lead(
        ref=ref,
        project_key=LEAD_PROJECT,
        cell_id="cell-harness",
        session_id=LEAD_SESSION,
        profile_alias=LEAD_PROFILE,
    )

    closed = await seater.retire_failed_seat(
        cell_id="cell-harness",
        session_id=LEAD_SESSION,
        reason="lead_start_failed",
    )

    assert closed is True
    # The real workspace was closed through the port, not just the row.
    assert port.closed == ["ws-1"]
    state = seater_database.execute(
        "SELECT state FROM cmux_surface_bindings WHERE binding_id = ?",
        (binding.binding_id,),
    ).fetchone()
    assert state["state"] == "closed"
    # Channel configuration is cleaned once the surface is gone.
    assert channel.cleaned == [LEAD_SESSION]
    # An immediate retry finds no active binding residue.
    assert seater_bindings.active_lead("cell-harness") is None


@pytest.mark.asyncio
async def test_retire_failed_seat_holds_residual_when_close_is_unconfirmed(
    seater_bindings: CmuxSurfaceBindings, seater_database: Database
) -> None:
    """An unconfirmed close must NOT be recorded as closed: the binding
    is held as residual ownership evidence so a later reconciliation
    reclaims the exact surface instead of leaking it (INFRA-214, same
    idiom as the channel-trust close path).

    Sol correction d85c374d: the session's channel configuration must
    ALSO survive. The workspace may still be alive, and stripping a live
    surface of its configuration is worse than the residue -- it stays
    on screen with no way to reach it, and the residual binding
    reconciliation depends on can no longer be resolved. This assertion
    was missing, which is exactly why the unsafe cleanup passed."""

    class RefusingPort(FakeSeaterPort):
        async def close_workspace(self, workspace_uuid: str) -> None:
            from hermes_orchestrator.cmux import CmuxError as _CmuxError

            raise _CmuxError("close could not be confirmed")

    port = RefusingPort()
    channel = FakeChannelLaunch(config=Path("/tmp/x.mcp.json"))
    seater = make_seater(seater_bindings, port, channel_launch=channel)
    ref = CmuxSurfaceRef(workspace_uuid="ws-2", surface_uuid="sf-2")
    binding = seater_bindings.bind_lead(
        ref=ref,
        project_key=LEAD_PROJECT,
        cell_id="cell-harness",
        session_id=LEAD_SESSION,
        profile_alias=LEAD_PROFILE,
    )

    closed = await seater.retire_failed_seat(
        cell_id="cell-harness",
        session_id=LEAD_SESSION,
        reason="lead_start_failed",
    )

    assert closed is False
    state = seater_database.execute(
        "SELECT state FROM cmux_surface_bindings WHERE binding_id = ?",
        (binding.binding_id,),
    ).fetchone()
    assert state["state"] == "residual"
    # The surface may still be alive: its configuration and capability
    # are PRESERVED so the residual binding stays reconcilable.
    assert channel.cleaned == []
    assert seater_bindings.active_lead("cell-harness") is None


@pytest.mark.asyncio
async def test_retire_failed_seat_cleans_channel_when_no_binding_exists(
    seater_bindings: CmuxSurfaceBindings,
) -> None:
    """Sol correction d85c374d, the other half: with NO binding there is
    no surface to retain, so the orphaned session configuration is safe
    -- and correct -- to remove. Gating cleanup on a confirmed close
    alone would strand it forever."""

    port = FakeSeaterPort()
    channel = FakeChannelLaunch(config=Path("/tmp/x.mcp.json"))
    seater = make_seater(seater_bindings, port, channel_launch=channel)

    closed = await seater.retire_failed_seat(
        cell_id="cell-harness",
        session_id=LEAD_SESSION,
        reason="lead_start_failed",
    )

    assert closed is False
    assert port.closed == []
    assert channel.cleaned == [LEAD_SESSION]
