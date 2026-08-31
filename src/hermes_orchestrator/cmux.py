"""Typed cmux control port and argv-safe local CLI adapter.

INFRA-185: Hermes and Claude project leads become directly observable in
cmux while durable SQLite/event state stays authoritative. This module is
the only place that touches the cmux CLI. Business services consume the
:class:`CmuxControlPort` protocol with typed workspace/surface identities —
never shell strings and never terminal screen content. The adapter builds
argv lists from a fixed allow-list of metadata commands, so arbitrary
screen reads, free text, and caller-chosen keystrokes remain structurally
impossible except for three bounded fixed-shape operations:
``deliver_intake_envelope`` (closed-grammar intake signal),
``read_screen`` (read-only channel verification), and
``confirm_channel_dialog`` (exactly one non-parameterizable Enter
keypress for INFRA-197 v5.1 operator-decision approval). None can carry
caller-chosen keys or free text. It authenticates through the
``CMUX_SOCKET_PASSWORD`` environment variable only: the socket password
never appears in argv, exceptions, logs, or durable payloads.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

CMUX_SOCKET_PASSWORD_ENV = "CMUX_SOCKET_PASSWORD"

CMUX_KEYCHAIN_SERVICE = "hermes-orchestrator-cmux"

_ACCESS_DENIED_MARKER = b"Access denied"

_UUID_PATTERN = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)

# cmux 0.64.22 acknowledges a mutation with a short numeric ref such as
# "OK workspace:14" instead of a UUID. Exactly this shape — and nothing
# else — may be accepted as an intermediate response.
_SHORT_WORKSPACE_ACK = re.compile(r"^OK\s+workspace:\d+$")

# The complete grammar of the bounded intake signal: one envelope kind,
# one 32-hex durable packet id, and exactly one trailing newline (the
# Return, submitted together with the envelope in one send operation).
# cmux is the signal plane only — the packet itself lives in durable
# SQLite and never rides this channel.
INTAKE_SIGNAL_PATTERN = re.compile(
    r"^(HERMES_CORRECTION_READY|HERMES_WORK_READY"
    r"|HERMES_ASSIGNMENT_READY|HERMES_CONTROL_READY) [0-9a-f]{32}\n$"
)


# The complete cmux vocabulary this orchestrator may speak. Everything is
# workspace/surface lifecycle or sanitized metadata display. Screen and
# input commands (read-screen, capture-pane, send, send-key, pipe-pane)
# are deliberately absent: cmux output can never become orchestration
# input, so screen content cannot advance issue state. INFRA-191 adds
# exactly two verbs, characterized against the live CLI: ``respawn-pane``
# (restart a validated command in an existing materialized terminal, the
# same trust level as ``new-workspace --command``) and ``top``
# (structured JSON process metadata per pane/surface — never screen
# content). The stale "no pane-split verb" premise is retired: cmux
# exposes ``new-split``/``new-pane``, but a split pane in an unfocused
# workspace never materializes its terminal (characterized live:
# "Surface is not a terminal", and the pane is dropped), so the
# two-pane lifecycle speaks the eager ``new-workspace --layout`` form
# of the already-allowed create verb instead.
_ALLOWED_COMMANDS = frozenset(
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
)

# A pane command is one printable-ASCII line: no newlines, control
# characters, or unbounded length, so a single validated command — and
# nothing more — reaches a surface's shell.
_RESPAWN_COMMAND = re.compile(r"^[ -~]{1,500}$")

# How long the adapter waits for a layout-created workspace to report
# both panes in the structured listing before failing closed.
_PANE_POLL_ATTEMPTS = 10
_PANE_POLL_DELAY_SECONDS = 0.5


class CmuxError(RuntimeError):
    """Base failure talking to cmux; messages never carry command output."""


class CmuxAccessDenied(CmuxError):
    """The socket rejected the caller; every operation fails closed."""


class CmuxUnavailable(CmuxError):
    """cmux could not be reached or the command did not complete."""


class CmuxProtocolError(CmuxError):
    """cmux answered without the typed identities the caller requires."""


@dataclass(frozen=True, slots=True)
class CmuxSurfaceRef:
    """One exact cmux surface identity: workspace UUID plus surface UUID."""

    workspace_uuid: str
    surface_uuid: str


@dataclass(frozen=True, slots=True)
class CmuxSurfaceProcesses:
    """Process metadata for one surface, from cmux's structured listing.

    ``process_names`` and ``process_ids`` are parallel, in tree order:
    the surface's attributed process names and their PIDs (macOS
    process accounting via ``top --json``, whose process nodes carry
    ``name``/``pid``/``ppid``) — exact identities only, never screen
    content or command output. The PIDs give every consumer a stable
    surface-scoped process identity, so argv lineage can be correlated
    strictly to THIS surface's processes and their descendants instead
    of anything else on the host (Sol L1). A node cmux reports without
    a usable pid carries ``0`` and is ignored by correlation.
    """

    pane_uuid: str
    surface_uuid: str
    process_names: tuple[str, ...]
    process_ids: tuple[int, ...]


class CmuxControlPort(Protocol):
    """Metadata-only cmux operations consumed by orchestration services."""

    async def ping(self) -> None: ...

    async def create_workspace(
        self,
        *,
        title: str,
        cwd: Path,
        command: str | None = None,
        env: Mapping[str, str] | None = None,
        resolve_marker: str | None = None,
    ) -> CmuxSurfaceRef: ...

    async def live_workspace_uuids(self) -> frozenset[str]: ...

    async def find_workspace_uuids(
        self, *, title_marker: str
    ) -> frozenset[str]: ...

    async def surface_alive(self, ref: CmuxSurfaceRef) -> bool: ...

    async def create_two_pane_workspace(
        self,
        *,
        title: str,
        cwd: Path,
        upper_command: str,
        lower_command: str,
        env: Mapping[str, str] | None = None,
        resolve_marker: str | None = None,
    ) -> tuple[CmuxSurfaceRef, CmuxSurfaceRef]: ...

    async def respawn_surface(
        self, ref: CmuxSurfaceRef, command: str
    ) -> None: ...

    async def workspace_processes(
        self, workspace_uuid: str
    ) -> tuple[CmuxSurfaceProcesses, ...]: ...

    async def close_workspace(self, workspace_uuid: str) -> None: ...

    async def rename_workspace(
        self, workspace_uuid: str, title: str
    ) -> None: ...

    async def set_surface_resume(
        self, ref: CmuxSurfaceRef, command: str
    ) -> None: ...

    async def set_status(
        self, workspace_uuid: str, key: str, value: str
    ) -> None: ...

    async def set_progress(
        self, workspace_uuid: str, fraction: float, label: str
    ) -> None: ...

    async def notify(
        self, workspace_uuid: str, title: str, body: str
    ) -> None: ...

    async def focus_workspace(self, workspace_uuid: str) -> None: ...

    async def set_hibernation(self, enabled: bool) -> None: ...

    async def deliver_intake_envelope(
        self, ref: CmuxSurfaceRef, envelope: str
    ) -> None: ...

    async def read_screen(
        self, ref: CmuxSurfaceRef, *, lines: int = 60
    ) -> str: ...

    async def confirm_channel_dialog(self, ref: CmuxSurfaceRef) -> None: ...


ProcessFactory = Callable[..., "asyncio.Future[asyncio.subprocess.Process]"]


class CmuxCliAdapter:
    """Drive the bundled cmux CLI through argv lists and typed results.

    The socket password is resolved lazily per call from ``password_source``
    (a Keychain-backed callable) and injected only as the
    ``CMUX_SOCKET_PASSWORD`` environment variable. A process started inside
    cmux needs no password; outside cmux a missing or wrong password
    surfaces as :class:`CmuxAccessDenied` and the caller fails closed.
    Exceptions never include argv, environment, or command output.
    """

    def __init__(
        self,
        argv_base: Sequence[str],
        *,
        process_factory: ProcessFactory = asyncio.create_subprocess_exec,
        timeout_seconds: float = 20.0,
        base_env: Mapping[str, str] | None = None,
        password_source: Callable[[], str | None] | None = None,
    ) -> None:
        if not argv_base:
            raise ValueError("cmux adapter requires the CLI command")
        if timeout_seconds <= 0:
            raise ValueError("cmux adapter timeout must be positive")
        self._argv_base = tuple(str(part) for part in argv_base)
        self._process_factory = process_factory
        self._timeout_seconds = timeout_seconds
        self._base_env = dict(base_env or {})
        self._password_source = password_source

    async def ping(self) -> None:
        await self._run("ping")

    async def create_workspace(
        self,
        *,
        title: str,
        cwd: Path,
        command: str | None = None,
        env: Mapping[str, str] | None = None,
        resolve_marker: str | None = None,
    ) -> CmuxSurfaceRef:
        arguments = [
            "new-workspace",
            "--name",
            title,
            "--cwd",
            str(cwd),
            "--focus",
            "false",
        ]
        if command is not None:
            arguments.extend(["--command", command])
        for key, value in sorted((env or {}).items()):
            arguments.extend(["--env", f"{key}={value}"])
        created = await self._run(*arguments)
        workspace_uuid = _first_uuid(created)
        if workspace_uuid is None:
            workspace_uuid = await self._resolve_short_ack(
                created, resolve_marker
            )
        listed = await self._run(
            "list-pane-surfaces", "--workspace", workspace_uuid
        )
        surface_uuid = next(
            (
                found
                for found in _UUID_PATTERN.findall(listed)
                if found.lower() != workspace_uuid.lower()
            ),
            None,
        )
        if surface_uuid is None:
            raise CmuxProtocolError(
                "cmux did not return a surface identity"
            )
        return CmuxSurfaceRef(
            workspace_uuid=workspace_uuid, surface_uuid=surface_uuid
        )

    async def _resolve_short_ack(
        self, output: str, resolve_marker: str | None
    ) -> str:
        """Resolve a short mutation acknowledgement to exactly one UUID.

        Only the exact ``OK workspace:<n>`` shape is accepted, and only
        as an intermediate response: the workspace identity itself comes
        from the metadata listing through the caller's durable
        activation marker — never from cwd, generic titles, list
        position, substrings, or the focused workspace — and zero or
        multiple exact marker matches fail closed. No terminal screen
        content is ever read.
        """

        if not _SHORT_WORKSPACE_ACK.fullmatch(output.strip()):
            raise CmuxProtocolError(
                "cmux did not return a workspace identity"
            )
        if not resolve_marker:
            raise CmuxProtocolError(
                "cmux returned a short mutation acknowledgement and no "
                "durable marker was provided to resolve it"
            )
        matches = await self.find_workspace_uuids(
            title_marker=resolve_marker
        )
        if len(matches) != 1:
            raise CmuxProtocolError(
                f"the short acknowledgement resolved to {len(matches)} "
                "durable marker matches; exactly one is required"
            )
        return next(iter(matches))

    async def live_workspace_uuids(self) -> frozenset[str]:
        output = await self._run("list-workspaces")
        return frozenset(_UUID_PATTERN.findall(output))

    async def find_workspace_uuids(
        self, *, title_marker: str
    ) -> frozenset[str]:
        """Live workspaces whose title carries a caller-generated marker.

        This reads the workspace metadata listing only — never screen
        content — and exists solely so an interrupted activation can be
        correlated back to the exact workspace its write-ahead intent
        created. The marker matches only as an exact whitespace-delimited
        token: superstrings and markers embedded in longer tokens are
        different identities and never match. Every exact duplicate is
        reported, so ambiguous ownership is the caller's to refuse — the
        adapter never picks one arbitrarily.
        """

        if not title_marker.strip():
            raise ValueError("cmux workspace search requires a marker")
        pattern = re.compile(rf"(?<!\S){re.escape(title_marker)}(?!\S)")
        output = await self._run("list-workspaces")
        found: set[str] = set()
        for line in output.splitlines():
            if pattern.search(line) is None:
                continue
            workspace_uuid = _first_uuid(line)
            if workspace_uuid is not None:
                found.add(workspace_uuid)
        return frozenset(found)

    async def surface_alive(self, ref: CmuxSurfaceRef) -> bool:
        if ref.workspace_uuid not in await self.live_workspace_uuids():
            return False
        listed = await self._run(
            "list-pane-surfaces", "--workspace", ref.workspace_uuid
        )
        return ref.surface_uuid in set(_UUID_PATTERN.findall(listed))

    async def create_two_pane_workspace(
        self,
        *,
        title: str,
        cwd: Path,
        upper_command: str,
        lower_command: str,
        env: Mapping[str, str] | None = None,
        resolve_marker: str | None = None,
    ) -> tuple[CmuxSurfaceRef, CmuxSurfaceRef]:
        """Create one workspace with two vertically stacked terminal
        panes, each eagerly running its validated command.

        Characterized live against cmux 0.64.22: a pane added to an
        unfocused workspace with ``new-split``/``new-pane`` never
        materializes its terminal ("Surface is not a terminal"), while
        layout surfaces created through ``new-workspace --layout``
        spawn real ttys and their commands immediately, focused or
        not. The layout JSON is a fixed vertical two-terminal template
        built here — only the two bounded single-line commands are
        caller data, validated before any subprocess exists — so no
        caller can express a different geometry or surface type. The
        workspace identity resolves like :meth:`create_workspace`
        (UUID or short-ack via the durable marker); both pane/surface
        identities come from the structured process listing, which
        must report exactly two panes (upper first, matching layout
        child order) within a bounded readiness window.
        """

        for command in (upper_command, lower_command):
            if _RESPAWN_COMMAND.fullmatch(command) is None:
                raise ValueError(
                    "a pane command must be one bounded printable line"
                )
        layout = json.dumps(
            {
                "direction": "vertical",
                "split": 0.5,
                "children": [
                    {
                        "pane": {
                            "surfaces": [
                                {
                                    "type": "terminal",
                                    "command": upper_command,
                                }
                            ]
                        }
                    },
                    {
                        "pane": {
                            "surfaces": [
                                {
                                    "type": "terminal",
                                    "command": lower_command,
                                }
                            ]
                        }
                    },
                ],
            }
        )
        arguments = [
            "new-workspace",
            "--name",
            title,
            "--cwd",
            str(cwd),
            "--focus",
            "false",
            "--layout",
            layout,
        ]
        for key, value in sorted((env or {}).items()):
            arguments.extend(["--env", f"{key}={value}"])
        created = await self._run(*arguments)
        workspace_uuid = _first_uuid(created)
        if workspace_uuid is None:
            workspace_uuid = await self._resolve_short_ack(
                created, resolve_marker
            )
        rows: tuple[CmuxSurfaceProcesses, ...] = ()
        for attempt in range(_PANE_POLL_ATTEMPTS):
            rows = await self.workspace_processes(workspace_uuid)
            if len(rows) == 2:
                break
            if attempt + 1 < _PANE_POLL_ATTEMPTS:
                await asyncio.sleep(_PANE_POLL_DELAY_SECONDS)
        if (
            len(rows) != 2
            or not rows[0].surface_uuid
            or not rows[1].surface_uuid
            or rows[0].pane_uuid.lower() == rows[1].pane_uuid.lower()
        ):
            raise CmuxProtocolError(
                "the layout create did not yield two stacked panes with "
                "typed identities"
            )
        return (
            CmuxSurfaceRef(
                workspace_uuid=workspace_uuid,
                surface_uuid=rows[0].surface_uuid,
            ),
            CmuxSurfaceRef(
                workspace_uuid=workspace_uuid,
                surface_uuid=rows[1].surface_uuid,
            ),
        )

    async def respawn_surface(
        self, ref: CmuxSurfaceRef, command: str
    ) -> None:
        """Start one validated command in one exact existing surface.

        ``respawn-pane --command`` is the CLI's sanctioned way to hand
        a surface's shell a command — the same trust level as
        ``new-workspace --command``, which the vocabulary already
        speaks. The command must be a single bounded printable-ASCII
        line (no newlines or control characters), validated before any
        subprocess exists, so exactly one caller-validated command and
        nothing else reaches the terminal.
        """

        if _RESPAWN_COMMAND.fullmatch(command) is None:
            raise ValueError(
                "a respawn command must be one bounded printable line"
            )
        await self._run(
            "respawn-pane",
            "--workspace",
            ref.workspace_uuid,
            "--surface",
            ref.surface_uuid,
            "--command",
            command,
        )

    async def workspace_processes(
        self, workspace_uuid: str
    ) -> tuple[CmuxSurfaceProcesses, ...]:
        """Typed pane/surface/process metadata for one workspace.

        Speaks ``top --processes --json`` (characterized against cmux
        0.64.22: windows → workspaces → panes → surfaces → process
        trees with names and pids) and returns only typed identities
        and process names. This is metadata from macOS process
        accounting — never terminal screen content.
        """

        output = await self._run(
            "top",
            "--workspace",
            workspace_uuid,
            "--processes",
            "--json",
        )
        try:
            document = json.loads(output)
        except json.JSONDecodeError:
            raise CmuxProtocolError(
                "cmux top did not return structured process metadata"
            ) from None
        rows: list[CmuxSurfaceProcesses] = []
        windows = document.get("windows")
        for window in windows if isinstance(windows, list) else []:
            for workspace in window.get("workspaces") or []:
                identity = str(workspace.get("id", ""))
                if identity.lower() != workspace_uuid.lower():
                    continue
                for pane in workspace.get("panes") or []:
                    for surface in pane.get("surfaces") or []:
                        names, pids = _process_entries(
                            surface.get("processes")
                        )
                        rows.append(
                            CmuxSurfaceProcesses(
                                pane_uuid=str(pane.get("id", "")),
                                surface_uuid=str(surface.get("id", "")),
                                process_names=names,
                                process_ids=pids,
                            )
                        )
        return tuple(rows)

    async def close_workspace(self, workspace_uuid: str) -> None:
        await self._run("close-workspace", "--workspace", workspace_uuid)

    async def rename_workspace(
        self, workspace_uuid: str, title: str
    ) -> None:
        await self._run(
            "rename-workspace", "--workspace", workspace_uuid, title
        )

    async def set_surface_resume(
        self, ref: CmuxSurfaceRef, command: str
    ) -> None:
        await self._run(
            "surface",
            "resume",
            "set",
            "--workspace",
            ref.workspace_uuid,
            "--surface",
            ref.surface_uuid,
            command,
        )

    async def set_status(
        self, workspace_uuid: str, key: str, value: str
    ) -> None:
        await self._run(
            "set-status", key, value, "--workspace", workspace_uuid
        )

    async def set_progress(
        self, workspace_uuid: str, fraction: float, label: str
    ) -> None:
        await self._run(
            "set-progress",
            f"{min(max(fraction, 0.0), 1.0):.2f}",
            "--label",
            label,
            "--workspace",
            workspace_uuid,
        )

    async def notify(
        self, workspace_uuid: str, title: str, body: str
    ) -> None:
        await self._run(
            "notify",
            "--title",
            title,
            "--body",
            body,
            "--workspace",
            workspace_uuid,
        )

    async def focus_workspace(self, workspace_uuid: str) -> None:
        await self._run("select-workspace", "--workspace", workspace_uuid)

    async def set_hibernation(self, enabled: bool) -> None:
        await self._run("agent-hibernation", "on" if enabled else "off")

    async def _run(self, command: str, *arguments: str) -> str:
        if command not in _ALLOWED_COMMANDS:
            raise ValueError(f"cmux command is not allow-listed: {command}")
        return await self._execute(command, *arguments)

    async def deliver_intake_envelope(
        self, ref: CmuxSurfaceRef, envelope: str
    ) -> None:
        """Submit one bounded wake signal to one exact surface.

        The single ``send`` operation carries the envelope and its
        Return (the trailing newline) together, addressed to the exact
        validated workspace and surface. Only the closed signal grammar
        — one allowed kind, one 32-hex durable packet id, one trailing
        newline — is accepted; anything else is refused before any
        subprocess exists. Raw ``send``/``send-key`` remain rejected by
        the general vocabulary, so no other text can ever reach a
        terminal. The target surface is never focused and no screen
        content is read.
        """

        if INTAKE_SIGNAL_PATTERN.fullmatch(envelope) is None:
            raise ValueError(
                "only the closed intake signal grammar may be submitted"
            )
        await self._spawn(
            (
                *self._argv_base,
                "send",
                "--workspace",
                ref.workspace_uuid,
                "--surface",
                ref.surface_uuid,
                envelope,
            )
        )

    async def read_screen(
        self, ref: CmuxSurfaceRef, *, lines: int = 60
    ) -> str:
        """Return the pane's current visible text for one exact surface.

        This is read-only and exists solely so the channel-trust gate
        (INFRA-197 v5.1 amendment) can verify the exact development-
        channel confirmation prompt is on screen before anything is
        pressed. It never focuses the surface and never types or sends
        anything. ``lines`` must be a positive int no greater than 2000
        and is validated before any subprocess is started.
        """

        if lines <= 0 or lines > 2000:
            raise ValueError(
                "cmux read-screen line count must be a positive int no "
                "greater than 2000"
            )
        return await self._spawn(
            (
                *self._argv_base,
                "read-screen",
                "--workspace",
                ref.workspace_uuid,
                "--surface",
                ref.surface_uuid,
                "--lines",
                str(lines),
            )
        )

    async def confirm_channel_dialog(self, ref: CmuxSurfaceRef) -> None:
        """Send exactly one Enter keypress to one exact surface.

        This is the single, non-parameterizable keypress the channel-
        trust gate may send after an exact-build match, under operator
        decision infra-197-trusted-channel-auto-approval-20260830-v1:
        the key is a fixed literal inside this method, never a
        parameter, so no caller can express any other key or any text
        through it. The general ``send``/``send-key`` vocabulary stays
        rejected by the allow-listed command surface (:meth:`_run`);
        this bounded, fixed-shape operation is the sole exception, and
        it addresses only the exact workspace/surface given.
        """

        await self._spawn(
            (
                *self._argv_base,
                "send-key",
                "--workspace",
                ref.workspace_uuid,
                "--surface",
                ref.surface_uuid,
                "enter",
            )
        )

    async def _execute(self, command: str, *arguments: str) -> str:
        return await self._spawn(
            (
                *self._argv_base,
                "--id-format",
                "uuids",
                command,
                *arguments,
            )
        )

    async def _spawn(self, argv: tuple[str, ...]) -> str:
        environment = {
            key: value
            for key, value in self._base_env.items()
            if key != CMUX_SOCKET_PASSWORD_ENV
        }
        environment["CMUX_QUIET"] = "1"
        if self._password_source is not None:
            password = self._password_source()
            if password:
                environment[CMUX_SOCKET_PASSWORD_ENV] = password
        try:
            process = await self._process_factory(
                *argv,
                env=environment,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except CmuxError:
            raise
        except Exception as error:
            # The launch failure is reported by type only so no path,
            # argv, or environment fragment can leak through it.
            raise CmuxUnavailable(
                f"cmux CLI could not start: {type(error).__name__}"
            ) from None
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=self._timeout_seconds
            )
        except TimeoutError:
            with suppress(ProcessLookupError):
                process.kill()
            await process.wait()
            raise CmuxUnavailable("cmux command timed out") from None
        if process.returncode != 0:
            if _ACCESS_DENIED_MARKER in (stderr or b""):
                raise CmuxAccessDenied("cmux socket denied this process")
            raise CmuxUnavailable(
                f"cmux command failed with exit {process.returncode}"
            )
        return (stdout or b"").decode("utf-8", errors="replace")


def _first_uuid(output: str) -> str | None:
    found = _UUID_PATTERN.search(output)
    return None if found is None else found.group(0)


def _process_entries(
    processes: object,
) -> tuple[tuple[str, ...], tuple[int, ...]]:
    """Flatten a cmux process tree to parallel names and PIDs.

    Tree order is preserved. A node without a name is skipped; a node
    whose pid is missing or malformed keeps its name with pid ``0`` so
    the parallel arrays never drift.
    """

    names: list[str] = []
    pids: list[int] = []
    stack: list[object] = (
        list(reversed(processes)) if isinstance(processes, list) else []
    )
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        name = node.get("name")
        if name:
            names.append(str(name))
            raw_pid = node.get("pid")
            pids.append(raw_pid if isinstance(raw_pid, int) else 0)
        children = node.get("children")
        if isinstance(children, list):
            stack.extend(reversed(children))
    return tuple(names), tuple(pids)
