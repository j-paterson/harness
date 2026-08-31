"""Recoverable two-pane Orchestrator workspace lifecycle.

INFRA-191 W3 (Hermes directive channel.blocked 2833d6d0, Sol correction
f28e8484): the Orchestrator becomes one real cmux workspace with two
stacked normal terminal panes — an upper pane running the actual
supervisor/dashboard lifecycle (the ``hermes-orchestrator daemon``
entry, whose maintenance loop carries the existing dashboard renderer
and durable sources unchanged) and a lower pane running the installed
Nous Hermes classic durable session (``hermes chat --continue <name>
--create-if-missing``: the same named session is reattached on every
recovery, never duplicated). The stale single-surface ANSI-region
design in ``dashboard_pane.py`` is not touched.

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

``ensure`` is idempotent: a live workspace with both panes and both
processes is adopted unchanged; a dead pane process is respawned into
the exact same surface (no duplication); a dead or partial workspace
is closed and rebuilt under the binding's successor generation.
Liveness comes only from cmux's typed process metadata
(``workspace_processes``) — never from screen content: a pane is live
when its process tree holds anything beyond a login shell, which in an
orchestrator-owned workspace is by construction the process this
lifecycle launched there.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
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

UPPER_ROLE = "supervisor"
LOWER_ROLE = "hermes"

#: Recursion guard (Sol packet 1): every workspace this lifecycle
#: creates carries this environment marker, so the supervisor daemon
#: running in the upper pane recognizes its own seat. A seated daemon
#: adopts-in-place and reconciles the lower session, but never creates
#: a workspace whose upper pane would launch another supervisor.
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


def supervisor_pane_command(
    *, repo_root: Path, state_dir: Path, interval: int
) -> str:
    """The exact upper-pane command: the real supervisor lifecycle.

    This is the daemon entry the Orchestrator terminal runs today
    (``uv run hermes-orchestrator … daemon``), whose maintenance loop
    owns the existing dashboard renderer and durable dashboard sources
    — not a placeholder shell. Both paths must be absolute and
    metacharacter-free and the interval bounded, so exactly this
    command shape and nothing else can be composed.
    """

    root = str(repo_root)
    state = str(state_dir)
    if _SAFE_PATH.fullmatch(root) is None:
        raise WorkspaceRefused(
            "the supervisor repo root must be an absolute "
            "metacharacter-free path"
        )
    if _SAFE_PATH.fullmatch(state) is None:
        raise WorkspaceRefused(
            "the supervisor state dir must be an absolute "
            "metacharacter-free path"
        )
    if not 5 <= int(interval) <= 86400:
        raise WorkspaceRefused(
            "the supervisor interval must be between 5 and 86400 seconds"
        )
    return (
        f"uv run hermes-orchestrator --repo-root {root} "
        f"--state-dir {state} daemon --interval {int(interval)} --json"
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


def workspace_marker(name: str) -> str:
    """The exact-token title marker correlating workspace to lifecycle."""

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


def supervisor_process_identity(processes: Sequence[str]) -> bool:
    """The upper pane proves the supervisor role, from typed metadata.

    Expected launch identity, characterized live: the daemon starts as
    ``uv run hermes-orchestrator … daemon``, so the tree carries ``uv``
    (or the console entry itself) with python interpreters beneath it,
    plus wrappers. Any other non-wrapper process — or none at all —
    is not the supervisor, and the pane fails closed. Screen content
    plays no part (Sol packet 2)."""

    meaningful = _meaningful(processes)
    signature = any(
        name.lower() in ("uv", "hermes-orchestrator") for name in meaningful
    )
    return signature and all(
        name.lower() in ("uv", "hermes-orchestrator") or _is_python(name)
        for name in meaningful
    )


def hermes_process_identity(processes: Sequence[str]) -> bool:
    """The lower pane proves the Nous Hermes role, from typed metadata.

    Expected launch identity, characterized live: the installed
    ``hermes`` entry is a python script, so the tree shows a python
    interpreter (or a ``hermes`` binary name), optionally with ``node``
    tool children, plus wrappers. ``uv`` or any other non-wrapper
    process is not the Hermes session, and the pane fails closed."""

    meaningful = _meaningful(processes)
    signature = any(
        _is_python(name) or name.lower() == "hermes" for name in meaningful
    )
    return signature and all(
        _is_python(name) or name.lower() in ("hermes", "node")
        for name in meaningful
    )


ROLE_IDENTITIES = {
    UPPER_ROLE: supervisor_process_identity,
    LOWER_ROLE: hermes_process_identity,
}


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

    def payload(self) -> dict[str, Any]:
        return {
            "workspace_uuid": self.workspace_uuid,
            "upper_surface_uuid": self.upper.surface_uuid,
            "lower_surface_uuid": self.lower.surface_uuid,
            "binding_id": self.binding_id,
            "generation": self.generation,
            "outcome": self.outcome,
            "respawned": list(self.respawned),
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
        interval: int = 300,
        session_name: str | None = None,
        seated: bool = False,
    ) -> None:
        self._port = port
        self._bindings = bindings
        self._repo_root = repo_root
        self._name = name
        self._title = title
        self._seated = bool(seated)
        self._supervisor_command = supervisor_pane_command(
            repo_root=repo_root, state_dir=state_dir, interval=interval
        )
        self._hermes_command = hermes_pane_command(session_name or name)
        self._marker = workspace_marker(name)

    @property
    def marker(self) -> str:
        return self._marker

    @property
    def seated(self) -> bool:
        """Whether this lifecycle runs inside its own upper pane.

        A seated lifecycle (the daemon launched with the ``SEAT_ENV``
        marker its own creation stamped on the workspace) may adopt
        and reconcile the lower Hermes session, but never create a
        workspace, close its own, or respawn the upper pane — each of
        those would duplicate or kill the very supervisor doing the
        reconciling.
        """

        return self._seated

    @property
    def supervisor_command(self) -> str:
        return self._supervisor_command

    @property
    def hermes_command(self) -> str:
        return self._hermes_command

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

        binding = self._bindings.active_orchestrator()
        if self._seated:
            # Recursion guard: a seated supervisor reconciles in place
            # only. It closes no workspaces (an orphan sweep could take
            # down its own seat under a fresh database) and creates
            # none (the created upper pane would launch a second
            # supervisor). Anything it cannot adopt is left for an
            # unseated owner, deterministically, by refusing.
            if binding is None:
                raise WorkspaceRefused(
                    "a seated supervisor has no durable seat to adopt "
                    "and never creates one"
                )
            adopted = await self._adopt(binding)
            if adopted is None:
                raise WorkspaceRefused(
                    "a seated supervisor cannot rebuild its own "
                    "workspace; an unseated owner must recreate it"
                )
            return adopted
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

    async def close(self) -> tuple[str, ...]:
        """Tear down the workspace and retire its durable binding."""

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
        # A tree that does not prove the supervisor identity — dead, or
        # occupied by something else — fails closed: the exact expected
        # command is respawned into the exact surface. A seated
        # supervisor skips this (the upper pane is itself) and leaves
        # the mismatch for an unseated owner.
        if not self._seated and not supervisor_process_identity(
            upper.process_names
        ):
            await self._port.respawn_surface(
                upper_ref, self._supervisor_command
            )
            respawned.append(UPPER_ROLE)
        if not hermes_process_identity(lower.process_names):
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
        )

    async def _create(
        self, replacing: CmuxBinding | None
    ) -> OrchestratorWorkspaceState:
        upper, lower = await self._port.create_two_pane_workspace(
            title=f"{self._title} {self._marker}",
            cwd=self._repo_root,
            upper_command=self._supervisor_command,
            lower_command=self._hermes_command,
            # Recursion guard: the workspace-level marker tells the
            # upper-pane daemon it is the seated supervisor, so its own
            # lifecycle adopts-in-place and never creates a second
            # workspace (and with it a second daemon).
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
    port: OrchestratorWorkspacePort, state: OrchestratorWorkspaceState
) -> dict[str, Any]:
    """One structural inspection: panes, identities, process evidence."""

    surfaces = await port.workspace_processes(state.workspace_uuid)
    rows = []
    for surface in surfaces:
        key = surface.surface_uuid.lower()
        if key == state.upper.surface_uuid.lower():
            role = UPPER_ROLE
        elif key == state.lower.surface_uuid.lower():
            role = LOWER_ROLE
        else:
            role = "unexpected"
        matcher = ROLE_IDENTITIES.get(role)
        rows.append(
            {
                "role": role,
                "pane_uuid": surface.pane_uuid,
                "surface_uuid": surface.surface_uuid,
                "process_names": list(surface.process_names),
                "role_proven": (
                    matcher is not None
                    and matcher(surface.process_names)
                ),
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
    lifecycle: OrchestratorWorkspaceLifecycle,
    port: OrchestratorWorkspacePort,
    *,
    settle_seconds: float = 0.0,
    screen_lines: int = 40,
) -> dict[str, Any]:
    """Drive the full lifecycle and return a JSON-able evidence summary.

    ensure → verify both stacked panes and both processes (typed
    process metadata twice for identity stability, plus read-only
    screen evidence) → restart-recovery (close the workspace
    externally, ensure again, verify) → teardown — entirely
    non-interactive, with no staged operator TTY action.
    """

    first = await lifecycle.ensure()
    if settle_seconds > 0:
        await asyncio.sleep(settle_seconds)
    inspection = await inspect_workspace(port, first)
    inspection_again = await inspect_workspace(port, first)
    stable_identities = _identities(inspection) == _identities(
        inspection_again
    )
    screens = await _screen_evidence(port, first, lines=screen_lines)

    await port.close_workspace(first.workspace_uuid)
    second = await lifecycle.ensure()
    if settle_seconds > 0:
        await asyncio.sleep(settle_seconds)
    recovery_inspection = await inspect_workspace(port, second)

    closed = await lifecycle.close()
    remaining = await port.live_workspace_uuids()
    evidence: dict[str, Any] = {
        "marker": lifecycle.marker,
        "stack_direction": STACK_DIRECTION,
        "supervisor_command": lifecycle.supervisor_command,
        "hermes_command": lifecycle.hermes_command,
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
    }
    evidence["passed"] = smoke_passed(evidence)
    return evidence


def smoke_passed(evidence: dict[str, Any]) -> bool:
    """The complete smoke pass condition (Sol packet 3).

    Every clause is mandatory: both stacked panes with both role
    identities proven on the first ensure AND after restart recovery,
    stable identities across inspections, an actual workspace
    replacement with an advanced binding generation, and a confirmed
    teardown. A smoke that adopts instead of rebuilding, or proves
    processes without their role identities, fails.
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
    workspace under the successor generation. The recursion guard
    lives in the lifecycle's ``seated`` mode; the owner adds
    determinism at the edges: cmux failures and seated refusals are
    absorbed and recorded (the daemon never crashes over terminal
    visibility), and :meth:`stop` / :meth:`teardown` end
    reconciliation permanently so a stopping daemon cannot recreate
    what it just tore down.
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
    }
    document["self_digest"] = _canonical_digest(document)
    return document


def verify_evidence_document(document: dict[str, Any]) -> bool:
    """Recompute the canonical digest and compare it to the seal."""

    recorded = document.get("self_digest")
    return isinstance(recorded, str) and recorded == _canonical_digest(
        document
    )


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
