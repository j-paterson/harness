from __future__ import annotations

import asyncio
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
