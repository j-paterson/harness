"""Recoverable two-pane Orchestrator workspace lifecycle.

INFRA-191 W3 (Hermes directive channel.blocked 2833d6d0, Sol packets
575fe76c and a6bc7ca2): the Orchestrator becomes one real cmux
workspace with two stacked normal terminal panes.

Ownership topology (Sol K1, explicit): production has exactly one
effective supervisor of the durable state — the single lock-holding
``hermes-orchestrator daemon`` process (launchd), which is never
inside a pane. It composes the one
:class:`OrchestratorWorkspaceOwner` and drives this lifecycle from
startup and its bounded maintenance slot. The upper pane runs the
distinct, deliberately non-supervising ``hermes-orchestrator
dashboard`` entry: token-free, read-only over the same durable
database, no daemon lock — so it can never collide with the daemon's
exclusive state-directory lock (the DaemonAlreadyRunning respawn loop
Sol evidenced is structurally impossible). The lower pane runs the
installed Nous Hermes classic durable session (``hermes chat
--continue <name> --create-if-missing``: the same named session is
reattached on every recovery, never duplicated). The stale
single-surface ANSI-region design in ``dashboard_pane.py`` is not
touched; the dashboard entry reuses its renderer unchanged.

Durable recovery identity needs no schema change: the workspace and
its upper (supervisor) surface are journaled through the existing
``cmux_surface_bindings`` orchestrator role (INFRA-185 migration 0025)
— :meth:`CmuxSurfaceBindings.bind_orchestrator` /
:meth:`~CmuxSurfaceBindings.replace` — and the workspace title carries
an exact-token marker (the established ``[hermes:...]`` idiom) so a
crash between the external create and the durable bind still leaves
the orphan correlatable and closeable. The lower (Hermes) surface is
never stored: with the two-pane invariant it is always "the one
surface that is not the bound upper", so the durable row can never
disagree with the live layout about it.

``ensure`` is idempotent: a live workspace with both roles proven is
adopted unchanged; a dead or wrong pane process is respawned into the
exact same surface (no duplication); a dead or partial workspace is
closed and rebuilt under the binding's successor generation.

Role proof (Sol K2) combines two typed observations and no screen
content: the pane's cmux process-name tree must have the role's
expected interpreter/wrapper shape, AND the exact expected command
lineage must be observed running via local process argv (``ps``),
which carries the full command line through legitimate wrappers —
``…/hermes-agent/venv/bin/python …/hermes-agent/hermes chat
--continue <session> --create-if-missing`` for the lower role and
``uv run hermes-orchestrator --repo-root … --state-dir … dashboard``
(plus its console-script child) for the upper. A bare interpreter
name proves nothing; a wrong session name or an unrelated
python/uv+python fails the role and fails closed.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import shlex
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from hermes_orchestrator.cmux import (
    CmuxError,
    CmuxSurfaceProcesses,
    CmuxSurfaceRef,
)
from hermes_orchestrator.cmux_surfaces import CmuxBinding, CmuxSurfaceBindings

#: The only geometry this lifecycle creates: two vertically stacked
#: normal terminal panes — supervisor above, Hermes below.
STACK_DIRECTION = "vertical"

UPPER_ROLE = "dashboard"
LOWER_ROLE = "hermes"

#: Pane marker (Sol K1 repurposed the former seated-adopt marker):
#: every workspace this lifecycle creates stamps this environment
#: variable on both panes. No daemon behavior branches on it any more
#: — the supervising daemon is never inside a pane — it survives as a
#: fail-closed operator-error guard: a lifecycle told it is running
#: inside a marked pane refuses every operation outright, because the
#: only process that may own this lifecycle is the outside daemon.
SEAT_ENV = "HERMES_ORCH_SEAT"

#: Version tag of the live-smoke evidence document schema.
EVIDENCE_SCHEMA = "infra-191-live-smoke/v1"

# One bounded name serves as workspace marker token and Hermes session
# name: no whitespace, quotes, or metacharacters, so it is inert in a
# workspace title, a marker search, and a pane command line.
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

# Paths embedded in the supervisor pane command may hold nothing a
# shell or argv parser could reinterpret (same rule as the classic
# channel-config path in cmux_surfaces.py).
_SAFE_PATH = re.compile(r"^/[A-Za-z0-9._/-]+$")

# Processes that may appear in any pane's typed tree without carrying
# role identity: login shells, and the sleep helpers both launchers
# park. Everything else must belong to the pane's expected launch
# identity or the pane is not that role.
_WRAPPER_NAMES = frozenset(
    {"zsh", "bash", "sh", "fish", "tcsh", "login", "sleep"}
)


class WorkspaceRefused(RuntimeError):
    """The lifecycle refused an invalid or unsafe composition."""


def dashboard_pane_command(
    *, repo_root: Path, state_dir: Path, interval: int
) -> str:
    """The exact upper-pane command: the non-daemon dashboard entry.

    Sol K1: the upper pane deliberately does NOT run the daemon — a
    second full daemon against the same state directory loses the
    exclusive daemon lock, exits ``DaemonAlreadyRunning``, and puts
    the outer maintenance loop into a respawn loop. Instead it runs
    ``hermes-orchestrator dashboard``: token-free, read-only over the
    durable database, lock-free, ticking the existing dashboard
    sources/renderer/pane machinery. Both paths must be absolute and
    metacharacter-free and the interval bounded, so exactly this
    command shape and nothing else can be composed.
    """

    root = str(repo_root)
    state = str(state_dir)
    if _SAFE_PATH.fullmatch(root) is None:
        raise WorkspaceRefused(
            "the dashboard repo root must be an absolute "
            "metacharacter-free path"
        )
    if _SAFE_PATH.fullmatch(state) is None:
        raise WorkspaceRefused(
            "the dashboard state dir must be an absolute "
            "metacharacter-free path"
        )
    if not 5 <= int(interval) <= 86400:
        raise WorkspaceRefused(
            "the dashboard interval must be between 5 and 86400 seconds"
        )
    return (
        f"uv run hermes-orchestrator --repo-root {root} "
        f"--state-dir {state} dashboard --interval {int(interval)}"
    )


def hermes_pane_command(session_name: str) -> str:
    """The exact lower-pane command: the installed Nous Hermes classic
    durable session.

    ``hermes chat --continue <name> --create-if-missing`` (verified
    against the installed Hermes Agent CLI) reattaches the named
    durable session, creating it exactly once if it does not exist yet
    — so every recovery reaches the same session and duplication is
    structurally impossible. The name is grammar-bound; nothing else
    can ride the command line.
    """

    if _NAME.fullmatch(session_name) is None:
        raise WorkspaceRefused(
            "a Hermes session name must be one bounded token"
        )
    return f"hermes chat --continue {session_name} --create-if-missing"


def state_namespace(state_dir: Path) -> str:
    """The stable ownership namespace for one canonical state directory.

    Sol packet 21a9edaf: ownership tokens were name-only, so an
    isolated-state smoke sharing the production cmux could discover
    and close the production workspace. The namespace is a short
    stable digest (12 hex of sha256) of the CANONICAL resolved state
    directory path: two different state directories can never produce
    the same ownership token for the same human label, and only the
    database that owns a state directory can address its workspaces.
    """

    canonical = str(state_dir.expanduser().resolve(strict=False))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]


_NAMESPACE = re.compile(r"^[0-9a-f]{12}$")


def workspace_marker(name: str, *, namespace: str) -> str:
    """The exact ownership token correlating workspace to lifecycle.

    ``[hermes-orch:<namespace>:<name>]`` — the namespace binds the
    token to one canonical state directory, the name stays the
    cosmetic human label (the visible title carries both, separately:
    ``<title> <marker>``). ALL discovery, adoption, and orphan
    cleanup match only this full token; a marker-compatible workspace
    with a foreign or missing namespace is invisible to them.
    """

    if _NAME.fullmatch(name) is None:
        raise WorkspaceRefused(
            "an orchestrator workspace name must be one bounded token"
        )
    if _NAMESPACE.fullmatch(namespace) is None:
        raise WorkspaceRefused(
            "an ownership namespace must be 12 lowercase hex digits"
        )
    return f"[hermes-orch:{namespace}:{name}]"


def legacy_workspace_marker(name: str) -> str:
    """The retired unnamespaced token, recognized only for migration.

    A workspace carrying this token is adopted or cleaned up by
    NOBODY: the one exception is deterministic migration — when its
    workspace is the one bound in THIS state directory's own database,
    it is renamed to the namespaced token; any other bearer is ignored
    entirely, visibly, and never closed.
    """

    if _NAME.fullmatch(name) is None:
        raise WorkspaceRefused(
            "an orchestrator workspace name must be one bounded token"
        )
    return f"[hermes-orch:{name}]"


def _meaningful(processes: Sequence[str]) -> tuple[str, ...]:
    """The tree's identity-bearing names: wrappers stripped."""

    return tuple(
        name
        for name in processes
        if name.lower().lstrip("-") not in _WRAPPER_NAMES
    )


def _is_python(name: str) -> bool:
    return name.lower().startswith("python")


def dashboard_process_shape(processes: Sequence[str]) -> bool:
    """The upper pane's cmux name-tree shape for the dashboard role.

    Shape only — necessary, never sufficient (Sol K2: a bare
    interpreter name is not role proof). Characterized live: ``uv run
    hermes-orchestrator … dashboard`` shows ``uv`` (or the console
    entry) with python interpreters beneath it, plus wrappers. Any
    other non-wrapper process fails the shape.
    """

    meaningful = _meaningful(processes)
    signature = any(
        name.lower() in ("uv", "hermes-orchestrator") for name in meaningful
    )
    return signature and all(
        name.lower() in ("uv", "hermes-orchestrator") or _is_python(name)
        for name in meaningful
    )


def hermes_process_shape(processes: Sequence[str]) -> bool:
    """The lower pane's cmux name-tree shape for the Hermes role.

    Shape only — necessary, never sufficient. Characterized live: the
    installed ``hermes`` entry is a python script, so the tree shows a
    python interpreter (or a ``hermes`` binary name), optionally with
    ``node`` tool children, plus wrappers.
    """

    meaningful = _meaningful(processes)
    signature = any(
        _is_python(name) or name.lower() == "hermes" for name in meaningful
    )
    return signature and all(
        _is_python(name) or name.lower() in ("hermes", "node")
        for name in meaningful
    )


ROLE_SHAPES = {
    UPPER_ROLE: dashboard_process_shape,
    LOWER_ROLE: hermes_process_shape,
}


@dataclass(frozen=True, slots=True)
class ProcessRecord:
    """One host process observation: pid, parent pid, full argv line."""

    pid: int
    ppid: int
    command: str


class ProcessLineage(Protocol):
    """Local observed process table (pid/ppid/argv), for role proof."""

    def process_table(self) -> tuple[ProcessRecord, ...]: ...


class PsProcessLineage:
    """Production lineage source: local ``ps`` table, no cmux, no screen.

    cmux panes are local processes on the daemon's own host (the cmux
    CLI and socket are local by construction), so ``ps -axo
    pid=,ppid=,command=`` exposes each process's identity, parentage,
    and full argv — including the session name inside ``… hermes chat
    --continue <session> --create-if-missing`` and the exact ``…
    hermes-orchestrator … dashboard`` line — through every legitimate
    wrapper. Sol L1: argv is never consumed host-wide; the lifecycle
    correlates it strictly through the PIDs cmux attributes to each
    surface (and their descendants), so a matching command running
    anywhere else can never prove a pane. This is typed OS process
    metadata, never terminal screen content.
    """

    def process_table(self) -> tuple[ProcessRecord, ...]:
        import subprocess

        try:
            completed = subprocess.run(
                ["ps", "-axo", "pid=,ppid=,command="],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return ()
        if completed.returncode != 0:
            return ()
        records: list[ProcessRecord] = []
        for line in completed.stdout.splitlines():
            parts = line.split(None, 2)
            if len(parts) != 3:
                continue
            try:
                pid, ppid = int(parts[0]), int(parts[1])
            except ValueError:
                continue
            command = parts[2].strip()
            if command:
                records.append(
                    ProcessRecord(pid=pid, ppid=ppid, command=command)
                )
        return tuple(records)


class LineageCorrelationUnavailable(WorkspaceRefused):
    """Surface-to-argv correlation was unavailable or ambiguous.

    Sol L1: when the two typed reads (cmux surface PIDs, ps table)
    cannot be correlated — every surface process disappeared between
    the reads, or a surface PID's observed argv no longer matches its
    reported process name (suspected PID reuse) — the lifecycle fails
    closed: nothing is adopted, respawned, closed, or created, and the
    next bounded cadence retries from fresh reads.
    """


@dataclass(frozen=True, slots=True)
class SurfaceCorrelation:
    """One surface's correlation outcome and its scoped command lines."""

    status: str  # "ok" | "idle" | "disappeared" | "suspected_pid_reuse"
    pids: tuple[int, ...]
    commands: tuple[str, ...]

    @property
    def failed(self) -> bool:
        return self.status in ("disappeared", "suspected_pid_reuse")


def _argv_name_consistent(name: str, command: str) -> bool:
    """Whether a surface process name plausibly owns this argv line.

    Guards against PID reuse between the cmux read and the ps read:
    the executable basename and the cmux-reported name must share a
    prefix relation (``Python``/``python3.13``, ``zsh``/``-zsh``,
    ``uv``/``uv``); a pid whose argv now belongs to something else
    fails, and the correlation fails closed with it.
    """

    argv0 = command.split()[0] if command.split() else ""
    base = argv0.rsplit("/", 1)[-1].lstrip("-").lower()
    expected = name.lstrip("-").lower()
    if not base or not expected:
        return False
    return base.startswith(expected) or expected.startswith(base)


def correlate_surface(
    surface: CmuxSurfaceProcesses, table: Sequence[ProcessRecord]
) -> SurfaceCorrelation:
    """Scope observed argv strictly to one surface's process tree.

    The surface's cmux-attributed PIDs anchor the correlation; their
    ps records plus every ps-walked descendant form the one process
    tree that shape and lineage are evaluated on. A transient child
    exiting between the reads is tolerated (dropped), but a surface
    whose reported processes ALL vanished, or any anchored pid whose
    argv is inconsistent with its reported name (suspected reuse),
    yields a failed correlation the caller must treat as
    fail-closed-and-retry, never as evidence.
    """

    anchors = [
        (name, pid)
        for name, pid in zip(
            surface.process_names, surface.process_ids, strict=True
        )
        if pid > 0
    ]
    if not anchors:
        return SurfaceCorrelation(status="idle", pids=(), commands=())
    by_pid = {record.pid: record for record in table}
    present = [
        (name, pid) for name, pid in anchors if pid in by_pid
    ]
    if not present:
        return SurfaceCorrelation(
            status="disappeared",
            pids=tuple(pid for _, pid in anchors),
            commands=(),
        )
    for name, pid in present:
        if not _argv_name_consistent(name, by_pid[pid].command):
            return SurfaceCorrelation(
                status="suspected_pid_reuse",
                pids=tuple(pid for _, pid in anchors),
                commands=(),
            )
    children: dict[int, list[int]] = {}
    for record in table:
        children.setdefault(record.ppid, []).append(record.pid)
    scoped: set[int] = set()
    stack = [pid for _, pid in present]
    while stack:
        pid = stack.pop()
        if pid in scoped:
            continue
        scoped.add(pid)
        stack.extend(children.get(pid, ()))
    commands = tuple(
        record.command for record in table if record.pid in scoped
    )
    return SurfaceCorrelation(
        status="ok", pids=tuple(sorted(scoped)), commands=commands
    )


@dataclass(frozen=True, slots=True)
class RoleExpectation:
    """The exact executable identity and argument tokens a role runs.

    ``executable`` is the required basename at the argv executable
    position (``hermes-orchestrator`` console script, or the installed
    ``hermes`` script); ``arguments`` are the exact tokens that must
    follow it, compared by token equality — never containment.
    """

    executable: str
    arguments: tuple[str, ...]

    @property
    def display(self) -> str:
        return " ".join((self.executable, *self.arguments))


# The only interpreter names accepted as a wrapper before the expected
# script: python in its platform spellings (Python, python3, python3.13).
_INTERPRETER_NAME = re.compile(r"^python(\d+(\.\d+)*)?$")


def _token_basename(token: str) -> str:
    return token.rsplit("/", 1)[-1]


def argv_matches_expectation(
    command_line: str, expectation: RoleExpectation
) -> bool:
    """Whether one observed argv line IS the expected command (Sol M1).

    The line is shlex-tokenized (an untokenizable line fails closed —
    it matches nothing), then exactly one optional, explicitly bounded
    wrapper form is consumed:

    * ``uv run`` — argv0 basename ``uv`` followed by the literal token
      ``run`` (how the typed dashboard command appears at uv itself);
    * one python interpreter (``Python``/``python3``/``python3.13``…)
      immediately followed by the expected script at the executable
      position (how the hermes bash launcher and console scripts
      actually exec: interpreter + script path + args, characterized
      live).

    The token at the executable position must have exactly the
    expected basename, and every remaining token must equal the
    expected argument sequence — full consumption, token equality.
    Occurrences inside ``-c`` payloads, quoted text, comments,
    environment values, or later arguments of an unrelated command
    can therefore never match: they are never at the executable
    position of an accepted chain.
    """

    try:
        tokens = shlex.split(command_line)
    except ValueError:
        return False
    if not tokens:
        return False
    index = 0
    head = _token_basename(tokens[0]).lstrip("-").lower()
    if head == "uv":
        if len(tokens) < 2 or tokens[1] != "run":
            return False
        index = 2
    elif _INTERPRETER_NAME.fullmatch(head):
        index = 1
    if index >= len(tokens):
        return False
    if _token_basename(tokens[index]) != expectation.executable:
        return False
    return tuple(tokens[index + 1 :]) == expectation.arguments


def lineage_matches(
    commands: Sequence[str], expectation: RoleExpectation
) -> tuple[str, ...]:
    """The observed command lines that ARE the expected command."""

    return tuple(
        command
        for command in commands
        if argv_matches_expectation(command, expectation)
    )


class OrchestratorWorkspacePort(Protocol):
    """The bounded cmux operations this lifecycle consumes."""

    async def create_two_pane_workspace(
        self,
        *,
        title: str,
        cwd: Path,
        upper_command: str,
        lower_command: str,
        env: Any = None,
        resolve_marker: str | None = None,
    ) -> tuple[CmuxSurfaceRef, CmuxSurfaceRef]: ...

    async def live_workspace_uuids(self) -> frozenset[str]: ...

    async def find_workspace_uuids(
        self, *, title_marker: str
    ) -> frozenset[str]: ...

    async def rename_workspace(
        self, workspace_uuid: str, title: str
    ) -> None: ...

    async def respawn_surface(
        self, ref: CmuxSurfaceRef, command: str
    ) -> None: ...

    async def workspace_processes(
        self, workspace_uuid: str
    ) -> tuple[CmuxSurfaceProcesses, ...]: ...

    async def close_workspace(self, workspace_uuid: str) -> None: ...

    async def read_screen(
        self, ref: CmuxSurfaceRef, *, lines: int = 60
    ) -> str: ...


@dataclass(frozen=True, slots=True)
class OrchestratorWorkspaceState:
    """One ensured workspace: exact identities plus what happened."""

    workspace_uuid: str
    upper: CmuxSurfaceRef
    lower: CmuxSurfaceRef
    binding_id: str
    generation: int
    outcome: str  # "created" | "adopted" | "recovered"
    respawned: tuple[str, ...]
    #: Legacy unnamespaced marker bearers this ensure refused to touch
    #: (not bound in this database): visible in owner state, never
    #: adopted, never closed.
    ignored_legacy: tuple[str, ...] = ()

    def payload(self) -> dict[str, Any]:
        return {
            "workspace_uuid": self.workspace_uuid,
            "upper_surface_uuid": self.upper.surface_uuid,
            "lower_surface_uuid": self.lower.surface_uuid,
            "binding_id": self.binding_id,
            "generation": self.generation,
            "outcome": self.outcome,
            "respawned": list(self.respawned),
            "ignored_legacy": list(self.ignored_legacy),
        }


class OrchestratorWorkspaceLifecycle:
    """Own exactly one recoverable two-pane Orchestrator workspace."""

    def __init__(
        self,
        *,
        port: OrchestratorWorkspacePort,
        bindings: CmuxSurfaceBindings,
        repo_root: Path,
        state_dir: Path,
        name: str = "orchestrator",
        title: str = "Orchestrator",
        interval: int = 30,
        session_name: str | None = None,
        lineage: ProcessLineage | None = None,
        inside_marked_pane: bool = False,
    ) -> None:
        self._port = port
        self._bindings = bindings
        self._repo_root = repo_root
        self._name = name
        self._title = title
        self._lineage = lineage if lineage is not None else PsProcessLineage()
        self._inside_marked_pane = bool(inside_marked_pane)
        self._dashboard_command = dashboard_pane_command(
            repo_root=repo_root, state_dir=state_dir, interval=interval
        )
        self._hermes_command = hermes_pane_command(session_name or name)
        self._namespace = state_namespace(state_dir)
        self._marker = workspace_marker(name, namespace=self._namespace)
        self._legacy_marker = legacy_workspace_marker(name)
        self._last_ignored_legacy: tuple[str, ...] = ()

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def legacy_marker(self) -> str:
        return self._legacy_marker

    @property
    def last_ignored_legacy(self) -> tuple[str, ...]:
        return self._last_ignored_legacy

    @property
    def marker(self) -> str:
        return self._marker

    @property
    def port(self) -> OrchestratorWorkspacePort:
        return self._port

    @property
    def inside_marked_pane(self) -> bool:
        """Whether this process was told it runs inside a marked pane.

        The former seated-adopt branch is retired (Sol K1): the one
        supervising daemon is never inside a pane, so a lifecycle
        constructed inside a marked pane can only be an operator error
        — it refuses every operation, fail closed, instead of
        adopting, creating, or closing anything.
        """

        return self._inside_marked_pane

    @property
    def dashboard_command(self) -> str:
        return self._dashboard_command

    @property
    def hermes_command(self) -> str:
        return self._hermes_command

    @property
    def upper_expectation(self) -> RoleExpectation:
        """The exact upper-role argv the role proof demands.

        Derived from the composed (grammar-validated, single-spaced)
        dashboard command: ``uv run`` stripped, the console-script
        identity at the executable position, every argument token
        exact.
        """

        tokens = self._dashboard_command.split()
        return RoleExpectation(
            executable=tokens[2], arguments=tuple(tokens[3:])
        )

    @property
    def lower_expectation(self) -> RoleExpectation:
        """The exact lower-role argv the role proof demands."""

        tokens = self._hermes_command.split()
        return RoleExpectation(
            executable=tokens[0], arguments=tuple(tokens[1:])
        )

    def observed_table(self) -> tuple[ProcessRecord, ...]:
        return self._lineage.process_table()

    def role_proof(
        self,
        role: str,
        surface: CmuxSurfaceProcesses,
        table: Sequence[ProcessRecord],
    ) -> dict[str, Any]:
        """One role's complete proof, surface-bound (Sol L1).

        Shape and exact command/session lineage are evaluated on the
        SAME process tree: the surface's cmux-attributed names/PIDs
        and the ps argv correlated strictly through those PIDs and
        their descendants. A matching command running anywhere else on
        the host proves nothing. A failed correlation proves nothing
        either and is reported for the caller to fail closed on.
        """

        shape = ROLE_SHAPES.get(role)
        expectation = (
            self.upper_expectation
            if role == UPPER_ROLE
            else self.lower_expectation
            if role == LOWER_ROLE
            else None
        )
        shape_ok = shape is not None and shape(surface.process_names)
        correlation = correlate_surface(surface, table)
        matched = (
            lineage_matches(correlation.commands, expectation)
            if expectation is not None and correlation.status == "ok"
            else ()
        )
        return {
            "shape_ok": shape_ok,
            "expected_lineage": (
                expectation.display if expectation is not None else None
            ),
            "correlation": correlation.status,
            "surface_pids": list(correlation.pids),
            "lineage_matches": [line[:200] for line in matched],
            "role_proven": (
                shape_ok
                and correlation.status == "ok"
                and bool(matched)
            ),
        }

    async def ensure(self) -> OrchestratorWorkspaceState:
        """Create, adopt, or recover the one two-pane workspace.

        Idempotent: a healthy workspace is adopted with unchanged
        identities; a dead pane process is respawned into the same
        surface; anything partial is closed and rebuilt under the
        binding's successor generation. Marker-titled workspaces the
        durable binding does not own (the create-then-crash window)
        are closed before anything else, so exactly one Orchestrator
        workspace can survive an ensure.
        """

        self._refuse_if_marked()
        binding = self._bindings.active_orchestrator()
        binding = await self._reconcile_legacy(binding)
        await self._close_orphans(binding)
        if binding is None:
            return await self._create(replacing=None)
        adopted = await self._adopt(binding)
        if adopted is not None:
            return adopted
        live = await self._port.live_workspace_uuids()
        if _contains(live, binding.workspace_uuid):
            await self._port.close_workspace(binding.workspace_uuid)
        return await self._create(replacing=binding)

    async def _reconcile_legacy(
        self, binding: CmuxBinding | None
    ) -> CmuxBinding | None:
        """Deterministic handling of retired unnamespaced markers.

        A legacy bearer whose workspace is the one bound in THIS state
        directory's own database migrates: its title is renamed to the
        namespaced token and the migration is journaled through
        ``replace()`` (same surfaces, successor generation). Every
        other legacy bearer — production seen from an isolated
        database included — is ignored entirely: refused for adoption
        AND for cleanup, recorded visibly on the returned state.
        """

        legacy = await self._port.find_workspace_uuids(
            title_marker=self._legacy_marker
        )
        ignored: list[str] = []
        migrated = binding
        for workspace_uuid in sorted(legacy):
            if (
                binding is not None
                and workspace_uuid.lower()
                == binding.workspace_uuid.lower()
            ):
                await self._port.rename_workspace(
                    workspace_uuid, f"{self._title} {self._marker}"
                )
                migrated = self._bindings.replace(
                    binding.binding_id,
                    binding.ref,
                    reason="orchestrator_marker_namespace_migrated",
                )
            else:
                ignored.append(workspace_uuid)
        self._last_ignored_legacy = tuple(ignored)
        return migrated

    def _refuse_if_marked(self) -> None:
        if self._inside_marked_pane:
            raise WorkspaceRefused(
                "this process runs inside a marked Orchestrator pane; "
                "only the outside lock-holding daemon may own the "
                "workspace lifecycle"
            )

    async def close(self) -> tuple[str, ...]:
        """Tear down the workspace and retire its durable binding."""

        self._refuse_if_marked()
        binding = self._bindings.active_orchestrator()
        marked = await self._port.find_workspace_uuids(
            title_marker=self._marker
        )
        live = await self._port.live_workspace_uuids()
        targets = {uuid.lower(): uuid for uuid in marked}
        if binding is not None:
            targets.setdefault(
                binding.workspace_uuid.lower(), binding.workspace_uuid
            )
        closed: list[str] = []
        for workspace_uuid in sorted(targets.values()):
            if _contains(live, workspace_uuid):
                await self._port.close_workspace(workspace_uuid)
                closed.append(workspace_uuid)
        if binding is not None:
            self._bindings.mark_closed(
                binding.binding_id,
                reason="orchestrator_workspace_teardown",
            )
        return tuple(closed)

    async def _adopt(
        self, binding: CmuxBinding
    ) -> OrchestratorWorkspaceState | None:
        live = await self._port.live_workspace_uuids()
        if not _contains(live, binding.workspace_uuid):
            return None
        surfaces = await self._port.workspace_processes(
            binding.workspace_uuid
        )
        if len(surfaces) != 2:
            return None
        if surfaces[0].pane_uuid.lower() == surfaces[1].pane_uuid.lower():
            # Two surfaces sharing one pane is a tabbed layout, not the
            # required two stacked panes.
            return None
        upper_key = binding.surface_uuid.lower()
        upper = next(
            (
                surface
                for surface in surfaces
                if surface.surface_uuid.lower() == upper_key
            ),
            None,
        )
        if upper is None:
            return None
        lower = next(
            surface
            for surface in surfaces
            if surface.surface_uuid.lower() != upper_key
        )
        respawned: list[str] = []
        upper_ref = CmuxSurfaceRef(
            workspace_uuid=binding.workspace_uuid,
            surface_uuid=upper.surface_uuid,
        )
        lower_ref = CmuxSurfaceRef(
            workspace_uuid=binding.workspace_uuid,
            surface_uuid=lower.surface_uuid,
        )
        # A pane that does not prove its role — dead, wrong session,
        # or occupied by something else (Sol K2/L1: shape plus exact
        # argv lineage on the surface's own process tree, both
        # mandatory) — fails closed: the exact expected command is
        # respawned into the exact surface. But an UNAVAILABLE or
        # AMBIGUOUS correlation (processes vanished between the typed
        # reads, or suspected PID reuse) is not evidence of anything:
        # respawning on it could kill a live pane, so it refuses the
        # whole adopt — nothing mutated — and the owner's next bounded
        # cadence retries from fresh reads.
        table = self.observed_table()
        upper_proof = self.role_proof(UPPER_ROLE, upper, table)
        lower_proof = self.role_proof(LOWER_ROLE, lower, table)
        for role, proof in (
            (UPPER_ROLE, upper_proof),
            (LOWER_ROLE, lower_proof),
        ):
            if proof["correlation"] in (
                "disappeared",
                "suspected_pid_reuse",
            ):
                raise LineageCorrelationUnavailable(
                    f"the {role} pane's process correlation is "
                    f"{proof['correlation']}; retrying without adopting"
                )
        if not upper_proof["role_proven"]:
            await self._port.respawn_surface(
                upper_ref, self._dashboard_command
            )
            respawned.append(UPPER_ROLE)
        if not lower_proof["role_proven"]:
            await self._port.respawn_surface(
                lower_ref, self._hermes_command
            )
            respawned.append(LOWER_ROLE)
        return OrchestratorWorkspaceState(
            workspace_uuid=binding.workspace_uuid,
            upper=upper_ref,
            lower=lower_ref,
            binding_id=binding.binding_id,
            generation=binding.generation,
            outcome="recovered" if respawned else "adopted",
            respawned=tuple(respawned),
            ignored_legacy=self._last_ignored_legacy,
        )

    async def _create(
        self, replacing: CmuxBinding | None
    ) -> OrchestratorWorkspaceState:
        upper, lower = await self._port.create_two_pane_workspace(
            title=f"{self._title} {self._marker}",
            cwd=self._repo_root,
            upper_command=self._dashboard_command,
            lower_command=self._hermes_command,
            # Pane marker: no daemon branches on it (the supervisor is
            # never inside a pane); it identifies pane processes as
            # lifecycle-owned and arms the fail-closed operator-error
            # guard (inside_marked_pane).
            env={SEAT_ENV: self._name},
            resolve_marker=self._marker,
        )
        if replacing is not None:
            binding = self._bindings.replace(
                replacing.binding_id,
                upper,
                reason="orchestrator_workspace_rebuilt",
            )
        else:
            binding = self._bindings.bind_orchestrator(upper)
        return OrchestratorWorkspaceState(
            workspace_uuid=upper.workspace_uuid,
            upper=upper,
            lower=lower,
            binding_id=binding.binding_id,
            generation=binding.generation,
            outcome="created",
            respawned=(),
            ignored_legacy=self._last_ignored_legacy,
        )

    async def _close_orphans(self, binding: CmuxBinding | None) -> None:
        marked = await self._port.find_workspace_uuids(
            title_marker=self._marker
        )
        owned = binding.workspace_uuid.lower() if binding else None
        for workspace_uuid in sorted(marked):
            if workspace_uuid.lower() == owned:
                continue
            await self._port.close_workspace(workspace_uuid)


async def inspect_workspace(
    lifecycle: OrchestratorWorkspaceLifecycle,
    state: OrchestratorWorkspaceState,
) -> dict[str, Any]:
    """One structural inspection: panes, identities, per-role proof.

    Each row carries the role's complete surface-bound evidence: the
    cmux name-tree shape verdict, the surface's correlated PIDs and
    correlation status, the exact expected argv lineage, and the
    surface-scoped command lines that matched it (Sol K2/L1).
    """

    port = lifecycle.port
    surfaces = await port.workspace_processes(state.workspace_uuid)
    table = lifecycle.observed_table()
    rows = []
    for surface in surfaces:
        key = surface.surface_uuid.lower()
        if key == state.upper.surface_uuid.lower():
            role = UPPER_ROLE
        elif key == state.lower.surface_uuid.lower():
            role = LOWER_ROLE
        else:
            role = "unexpected"
        proof = lifecycle.role_proof(role, surface, table)
        rows.append(
            {
                "role": role,
                "pane_uuid": surface.pane_uuid,
                "surface_uuid": surface.surface_uuid,
                "process_names": list(surface.process_names),
                "process_ids": list(surface.process_ids),
                **proof,
            }
        )
    pane_uuids = {surface.pane_uuid.lower() for surface in surfaces}
    return {
        "pane_count": len(surfaces),
        "distinct_panes": len(pane_uuids),
        "surfaces": rows,
        "both_panes_present": len(surfaces) == 2 and len(pane_uuids) == 2,
        "both_roles_proven": (
            len(rows) == 2
            and all(row["role_proven"] for row in rows)
            and {row["role"] for row in rows} == {UPPER_ROLE, LOWER_ROLE}
        ),
    }


async def _screen_evidence(
    port: OrchestratorWorkspacePort,
    state: OrchestratorWorkspaceState,
    *,
    lines: int,
    keep: int = 6,
) -> dict[str, list[str]]:
    evidence: dict[str, list[str]] = {}
    for role, ref in ((UPPER_ROLE, state.upper), (LOWER_ROLE, state.lower)):
        screen = await port.read_screen(ref, lines=lines)
        trimmed = [
            line.rstrip()[:160]
            for line in screen.splitlines()
            if line.strip()
        ]
        evidence[role] = trimmed[-keep:]
    return evidence


async def run_smoke(
    owner: OrchestratorWorkspaceOwner,
    *,
    settle_seconds: float = 0.0,
    screen_lines: int = 40,
) -> dict[str, Any]:
    """Drive the production ownership topology and return evidence.

    Sol K1: the smoke exercises the exact path production runs — the
    owner's ``start()``/``tick()`` reconciliation, not a standalone
    lifecycle call — so the evidence proves the owner-driven topology:
    start → verify both stacked panes with command-specific role proof
    (typed metadata twice for identity stability, plus read-only
    screen evidence) → restart-recovery (close the workspace
    externally, tick again) → teardown through the owner. An owner
    that absorbs a failure fails the smoke loudly here instead.
    """

    lifecycle = owner.lifecycle
    port = lifecycle.port
    # Sol packet 21a9edaf, test 5: record every workspace that already
    # lives on the shared cmux before this smoke touches anything —
    # the production Orchestrator included — so the receipt itself can
    # prove isolated teardown removed only the smoke workspace.
    preexisting_live = await port.live_workspace_uuids()
    preexisting_legacy = await port.find_workspace_uuids(
        title_marker=lifecycle.legacy_marker
    )
    preexisting_own_marker = await port.find_workspace_uuids(
        title_marker=lifecycle.marker
    )
    foreign_preexisting = sorted(
        workspace_uuid
        for workspace_uuid in preexisting_live
        if not _contains(preexisting_own_marker, workspace_uuid)
    )
    first = await owner.start()
    if first is None:
        raise WorkspaceRefused(
            f"the owner could not ensure the workspace: {owner.last_error}"
        )
    if settle_seconds > 0:
        await asyncio.sleep(settle_seconds)
    inspection = await inspect_workspace(lifecycle, first)
    inspection_again = await inspect_workspace(lifecycle, first)
    stable_identities = _identities(inspection) == _identities(
        inspection_again
    )
    screens = await _screen_evidence(port, first, lines=screen_lines)

    await port.close_workspace(first.workspace_uuid)
    second = await owner.tick()
    if second is None:
        raise WorkspaceRefused(
            f"the owner could not recover the workspace: {owner.last_error}"
        )
    if settle_seconds > 0:
        await asyncio.sleep(settle_seconds)
    recovery_inspection = await inspect_workspace(lifecycle, second)

    closed = await owner.teardown()
    remaining = await port.live_workspace_uuids()
    survivors = {found.lower() for found in remaining}
    preexisting_untouched = all(
        workspace_uuid.lower() in survivors
        for workspace_uuid in foreign_preexisting
    )
    evidence: dict[str, Any] = {
        "marker": lifecycle.marker,
        "stack_direction": STACK_DIRECTION,
        "topology": "owner-driven",
        "dashboard_command": lifecycle.dashboard_command,
        "hermes_command": lifecycle.hermes_command,
        "role_expectations": {
            UPPER_ROLE: lifecycle.upper_expectation.display,
            LOWER_ROLE: lifecycle.lower_expectation.display,
        },
        "first_ensure": first.payload(),
        "inspection": inspection,
        "stable_identities": stable_identities,
        "screens": screens,
        "restart_recovery": {
            "ensure": second.payload(),
            "inspection": recovery_inspection,
            "workspace_replaced": (
                second.workspace_uuid.lower()
                != first.workspace_uuid.lower()
            ),
            "generation_advanced": second.generation > first.generation,
        },
        "teardown": {
            "closed_workspaces": list(closed),
            "workspace_remains": _contains(
                remaining, second.workspace_uuid
            ),
        },
        "preexisting": {
            "workspaces": foreign_preexisting,
            "legacy_marker_workspaces": sorted(preexisting_legacy),
            "survived_untouched": preexisting_untouched,
        },
    }
    evidence["passed"] = smoke_passed(evidence)
    return evidence


def smoke_passed(evidence: dict[str, Any]) -> bool:
    """The complete smoke pass condition (Sol packet 3).

    Every clause is mandatory: both stacked panes with both role
    identities proven on the first ensure AND after restart recovery,
    stable identities across inspections, an actual workspace
    replacement with an advanced binding generation, a confirmed
    teardown, AND every pre-existing foreign workspace on the shared
    cmux surviving untouched (Sol 21a9edaf: an isolated smoke that
    disturbs anything it does not own — the production Orchestrator
    workspace included — fails). A smoke that adopts instead of
    rebuilding, or proves processes without their role identities,
    fails.
    """

    inspection = evidence["inspection"]
    recovery = evidence["restart_recovery"]
    return bool(
        inspection["both_panes_present"]
        and inspection["both_roles_proven"]
        and evidence["stable_identities"]
        and recovery["inspection"]["both_panes_present"]
        and recovery["inspection"]["both_roles_proven"]
        and recovery["workspace_replaced"]
        and recovery["generation_advanced"]
        and not evidence["teardown"]["workspace_remains"]
        and evidence["preexisting"]["survived_untouched"]
    )


def _identities(inspection: dict[str, Any]) -> list[tuple[str, str, str]]:
    """The inspection's identity projection: roles and exact UUIDs only.

    Process trees legitimately churn between two inspections of a live
    workspace; pane and surface identities must not.
    """

    return [
        (
            str(row["role"]),
            str(row["pane_uuid"]).lower(),
            str(row["surface_uuid"]).lower(),
        )
        for row in inspection["surfaces"]
    ]


def _contains(uuids: frozenset[str], workspace_uuid: str) -> bool:
    lowered = workspace_uuid.lower()
    return any(found.lower() == lowered for found in uuids)


class OrchestratorWorkspaceOwner:
    """The one stable lifecycle owner driven by the daemon.

    Sol packet 1: the daemon composes exactly one owner (runtime.py,
    Optional=None as usual), calls :meth:`start` once at startup so
    the workspace is ensured autonomously — no manual CLI — and calls
    :meth:`tick` from the exception-suppressed maintenance slot as the
    bounded reconciliation cadence. Every reconciliation is one
    ``ensure()``: adopt a healthy workspace unchanged, respawn a dead
    or wrong-role pane process in place, rebuild a dead or partial
    workspace under the successor generation. Ownership is singular by
    construction (Sol K1): the owner lives only inside the one
    lock-holding daemon, the upper pane runs the lock-free read-only
    dashboard entry, and a lifecycle marked as running inside a pane
    refuses everything. The owner adds determinism at the edges: cmux
    failures and refusals are absorbed and recorded (the daemon never
    crashes over terminal visibility), and :meth:`stop` /
    :meth:`teardown` end reconciliation permanently so a stopping
    daemon cannot recreate what it just tore down.
    """

    def __init__(self, lifecycle: OrchestratorWorkspaceLifecycle) -> None:
        self._lifecycle = lifecycle
        self._stopped = False
        self.ensures = 0
        self.last_state: OrchestratorWorkspaceState | None = None
        self.last_error: str | None = None

    @property
    def lifecycle(self) -> OrchestratorWorkspaceLifecycle:
        return self._lifecycle

    @property
    def stopped(self) -> bool:
        return self._stopped

    async def start(self) -> OrchestratorWorkspaceState | None:
        """Ensure the workspace once at daemon startup."""

        return await self._reconcile()

    async def tick(self) -> OrchestratorWorkspaceState | None:
        """One bounded maintenance reconciliation; no-op once stopped."""

        return await self._reconcile()

    async def _reconcile(self) -> OrchestratorWorkspaceState | None:
        if self._stopped:
            return None
        try:
            state = await self._lifecycle.ensure()
        except (CmuxError, WorkspaceRefused) as error:
            # Bounded: visibility failures are recorded, never raised
            # into the daemon; durable state is untouched and the next
            # cadence retries from scratch.
            self.last_error = f"{type(error).__name__}: {error}"
            return None
        self.ensures += 1
        self.last_state = state
        self.last_error = None
        return state

    def stop(self) -> None:
        """Stop reconciliation deterministically; ticks become no-ops."""

        self._stopped = True

    async def teardown(self) -> tuple[str, ...]:
        """Stop reconciliation, then close the workspace and binding.

        Stopping first makes teardown deterministic: no concurrent or
        subsequent tick can recreate the workspace this call closes.
        """

        self.stop()
        return await self._lifecycle.close()


def evidence_document(
    evidence: dict[str, Any],
    *,
    source_sha: str,
    invocation: Sequence[str],
    captured_at: str,
) -> dict[str, Any]:
    """Bind one smoke run into a digest-sealed evidence document.

    Sol packet 3: the document carries the exact source revision the
    smoke ran from, the exact CLI invocation, and the complete smoke
    evidence (workspace/pane identities, generations, per-role process
    metadata, teardown result), sealed with a sha256 self-digest over
    the canonical JSON payload so any holder can revalidate it against
    the candidate and invocation without trusting the file.
    """

    document: dict[str, Any] = {
        "schema": EVIDENCE_SCHEMA,
        "source_sha": source_sha,
        "invocation": list(invocation),
        "captured_at": captured_at,
        "evidence": evidence,
        # Two distinct digests exist for this receipt and a manifest
        # may reference either unambiguously (Sol K3): ``self_digest``
        # (kind declared here) seals the canonical JSON payload and
        # lives inside the document; the file-byte sha256 of the
        # written receipt is computed over the file with
        # :func:`file_sha256` and reported alongside — it cannot live
        # inside the file it hashes.
        "self_digest_kind": "sha256-of-canonical-json",
    }
    document["self_digest"] = _canonical_digest(document)
    return document


def verify_evidence_document(
    document: dict[str, Any], *, expected_source_sha: str | None = None
) -> bool:
    """Recompute the canonical digest and validate the binding.

    Verification refuses (returns False) when the seal does not match
    — any tampering with evidence, invocation, or sha — and, when the
    caller supplies the candidate sha it expects, when the document's
    ``source_sha`` is any other revision (Sol K3): a receipt from the
    wrong candidate never validates against this one.
    """

    recorded = document.get("self_digest")
    if not (
        isinstance(recorded, str)
        and recorded == _canonical_digest(document)
    ):
        return False
    if expected_source_sha is not None:
        return document.get("source_sha") == expected_source_sha
    return True


def file_sha256(path: Path) -> str:
    """The written receipt's file-byte sha256 (kind: sha256-of-file)."""

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_digest(document: dict[str, Any]) -> str:
    trimmed = {
        key: value
        for key, value in document.items()
        if key != "self_digest"
    }
    canonical = json.dumps(
        trimmed, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
