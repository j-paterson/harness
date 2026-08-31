"""INFRA-191 W3: recoverable two-pane Orchestrator workspace lifecycle.

Fake-port lifecycle tests (create, adopt, partial recovery, dead-session
respawn, idempotence, teardown) plus CLI dispatch and the CLI-level
two-real-pane evidence test (Sol correction f28e8484)."""

from __future__ import annotations

import json
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path

import pytest

from hermes_orchestrator.cmux import CmuxSurfaceProcesses, CmuxSurfaceRef
from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.orchestrator_workspace import (
    LOWER_ROLE,
    STACK_DIRECTION,
    UPPER_ROLE,
    OrchestratorWorkspaceLifecycle,
    WorkspaceRefused,
    hermes_pane_command,
    inspect_workspace,
    run_smoke,
    supervisor_pane_command,
    workspace_marker,
)

REPO_ROOT = Path("/repo/orchestrator")
STATE_DIR = Path("/state/orchestrator")

SUPERVISOR_COMMAND = (
    "uv run hermes-orchestrator --repo-root /repo/orchestrator "
    "--state-dir /state/orchestrator daemon --interval 300 --json"
)
HERMES_COMMAND = "hermes chat --continue orch --create-if-missing"


@dataclass
class FakePane:
    pane_uuid: str
    surface_uuid: str
    processes: list[str]


@dataclass
class FakeWorkspace:
    title: str
    panes: list[FakePane]


@dataclass
class FakeWorkspacePort:
    """A structural stand-in for the cmux adapter's workspace surface."""

    workspaces: dict[str, FakeWorkspace] = field(default_factory=dict)
    created: list[dict[str, object]] = field(default_factory=list)
    respawns: list[tuple[CmuxSurfaceRef, str]] = field(default_factory=list)
    closed: list[str] = field(default_factory=list)
    counter: int = 0

    def _next(self, prefix: str) -> str:
        self.counter += 1
        return f"{prefix}-{self.counter}"

    def _pane(self, surface_uuid: str) -> FakePane:
        for workspace in self.workspaces.values():
            for pane in workspace.panes:
                if pane.surface_uuid == surface_uuid:
                    return pane
        raise AssertionError(f"unknown surface {surface_uuid}")

    def seed_workspace(self, title: str) -> str:
        """A marker-titled workspace nothing durable owns (orphan)."""

        workspace_uuid = self._next("ws")
        self.workspaces[workspace_uuid] = FakeWorkspace(
            title=title,
            panes=[FakePane(self._next("pane"), self._next("surf"), ["zsh"])],
        )
        return workspace_uuid

    def kill_process(self, surface_uuid: str) -> None:
        self._pane(surface_uuid).processes = ["zsh"]

    def drop_pane(self, workspace_uuid: str, surface_uuid: str) -> None:
        workspace = self.workspaces[workspace_uuid]
        workspace.panes = [
            pane
            for pane in workspace.panes
            if pane.surface_uuid != surface_uuid
        ]

    def merge_panes(self, workspace_uuid: str) -> None:
        """Collapse both surfaces into one pane (a tabbed layout)."""

        workspace = self.workspaces[workspace_uuid]
        shared = workspace.panes[0].pane_uuid
        for pane in workspace.panes:
            pane.pane_uuid = shared

    async def create_two_pane_workspace(
        self,
        *,
        title: str,
        cwd: Path,
        upper_command: str,
        lower_command: str,
        env: object = None,
        resolve_marker: str | None = None,
    ) -> tuple[CmuxSurfaceRef, CmuxSurfaceRef]:
        workspace_uuid = self._next("ws")
        upper = FakePane(
            self._next("pane"),
            self._next("surf"),
            ["zsh", upper_command.split()[0]],
        )
        lower = FakePane(
            self._next("pane"),
            self._next("surf"),
            ["zsh", lower_command.split()[0]],
        )
        self.workspaces[workspace_uuid] = FakeWorkspace(
            title=title, panes=[upper, lower]
        )
        self.created.append(
            {
                "workspace_uuid": workspace_uuid,
                "title": title,
                "cwd": cwd,
                "upper_command": upper_command,
                "lower_command": lower_command,
                "resolve_marker": resolve_marker,
            }
        )
        return (
            CmuxSurfaceRef(
                workspace_uuid=workspace_uuid,
                surface_uuid=upper.surface_uuid,
            ),
            CmuxSurfaceRef(
                workspace_uuid=workspace_uuid,
                surface_uuid=lower.surface_uuid,
            ),
        )

    async def live_workspace_uuids(self) -> frozenset[str]:
        return frozenset(self.workspaces)

    async def find_workspace_uuids(
        self, *, title_marker: str
    ) -> frozenset[str]:
        return frozenset(
            workspace_uuid
            for workspace_uuid, workspace in self.workspaces.items()
            if title_marker in workspace.title.split()
        )

    async def respawn_surface(
        self, ref: CmuxSurfaceRef, command: str
    ) -> None:
        self.respawns.append((ref, command))
        self._pane(ref.surface_uuid).processes = ["zsh", command.split()[0]]

    async def workspace_processes(
        self, workspace_uuid: str
    ) -> tuple[CmuxSurfaceProcesses, ...]:
        workspace = self.workspaces.get(workspace_uuid)
        if workspace is None:
            return ()
        return tuple(
            CmuxSurfaceProcesses(
                pane_uuid=pane.pane_uuid,
                surface_uuid=pane.surface_uuid,
                process_names=tuple(pane.processes),
            )
            for pane in workspace.panes
        )

    async def close_workspace(self, workspace_uuid: str) -> None:
        self.closed.append(workspace_uuid)
        self.workspaces.pop(workspace_uuid, None)

    async def read_screen(
        self, ref: CmuxSurfaceRef, *, lines: int = 60
    ) -> str:
        processes = " ".join(self._pane(ref.surface_uuid).processes)
        return f"pane {ref.surface_uuid}\nrunning: {processes}\n"


@pytest.fixture
def database(tmp_path: Path) -> Database:
    opened = Database.open(tmp_path / "state.db")
    yield opened
    opened.close()


@pytest.fixture
def bindings(database: Database) -> CmuxSurfaceBindings:
    return CmuxSurfaceBindings(
        database=database, events=EventStore(database)
    )


def lifecycle(
    port: FakeWorkspacePort, bindings: CmuxSurfaceBindings
) -> OrchestratorWorkspaceLifecycle:
    return OrchestratorWorkspaceLifecycle(
        port=port,
        bindings=bindings,
        repo_root=REPO_ROOT,
        state_dir=STATE_DIR,
        name="orch",
        title="Orchestrator",
    )


# ---------------------------------------------------------------------------
# Command grammars
# ---------------------------------------------------------------------------


def test_supervisor_command_is_the_actual_daemon_entry() -> None:
    assert (
        supervisor_pane_command(
            repo_root=REPO_ROOT, state_dir=STATE_DIR, interval=300
        )
        == SUPERVISOR_COMMAND
    )


@pytest.mark.parametrize(
    ("root", "state", "interval"),
    [
        (Path("relative/repo"), STATE_DIR, 300),
        (Path("/repo;rm -rf /"), STATE_DIR, 300),
        (REPO_ROOT, Path("/state dir/with space"), 300),
        (REPO_ROOT, STATE_DIR, 4),
        (REPO_ROOT, STATE_DIR, 86401),
    ],
)
def test_supervisor_command_grammar_refuses_unsafe_input(
    root: Path, state: Path, interval: int
) -> None:
    with pytest.raises(WorkspaceRefused):
        supervisor_pane_command(
            repo_root=root, state_dir=state, interval=interval
        )


def test_hermes_command_is_the_installed_durable_session_launch() -> None:
    assert hermes_pane_command("orch") == HERMES_COMMAND


@pytest.mark.parametrize(
    "name", ["", "two words", "semi;colon", "-leading", "x" * 70]
)
def test_hermes_session_name_is_grammar_bound(name: str) -> None:
    with pytest.raises(WorkspaceRefused):
        hermes_pane_command(name)
    with pytest.raises(WorkspaceRefused):
        workspace_marker(name)


# ---------------------------------------------------------------------------
# Lifecycle: create / adopt / recover
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_builds_two_stacked_panes_and_binds_durably(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = lifecycle(port, bindings)

    state = await owner.ensure()

    assert state.outcome == "created"
    [created] = port.created
    assert created["title"] == "Orchestrator [hermes-orch:orch]"
    assert created["resolve_marker"] == "[hermes-orch:orch]"
    assert created["cwd"] == REPO_ROOT
    assert created["upper_command"] == SUPERVISOR_COMMAND
    assert created["lower_command"] == HERMES_COMMAND
    assert port.respawns == []
    workspace = port.workspaces[state.workspace_uuid]
    assert len(workspace.panes) == 2
    binding = bindings.active_orchestrator()
    assert binding is not None
    assert binding.workspace_uuid == state.workspace_uuid
    assert binding.surface_uuid == state.upper.surface_uuid
    assert state.upper.surface_uuid != state.lower.surface_uuid


@pytest.mark.asyncio
async def test_upper_pane_runs_the_actual_supervisor_dashboard_lifecycle(
    bindings: CmuxSurfaceBindings,
) -> None:
    # Sol correction f28e8484: the upper pane runs the real
    # supervisor/dashboard process lifecycle — the daemon entry whose
    # maintenance loop owns the dashboard renderer — not a placeholder
    # shell.
    port = FakeWorkspacePort()
    owner = lifecycle(port, bindings)

    state = await owner.ensure()

    [created] = port.created
    assert created["upper_command"] == SUPERVISOR_COMMAND
    inspection = await inspect_workspace(port, state)
    upper = next(
        row
        for row in inspection["surfaces"]
        if row["role"] == UPPER_ROLE
    )
    assert upper["process_live"]
    assert "uv" in upper["process_names"]


@pytest.mark.asyncio
async def test_ensure_is_idempotent_and_adopts_the_live_workspace(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = lifecycle(port, bindings)

    first = await owner.ensure()
    second = await owner.ensure()

    assert second.outcome == "adopted"
    assert second.respawned == ()
    assert second.workspace_uuid == first.workspace_uuid
    assert second.upper == first.upper
    assert second.lower == first.lower
    assert second.generation == first.generation
    assert len(port.created) == 1
    assert port.respawns == []
    assert port.closed == []


@pytest.mark.asyncio
async def test_dead_lower_hermes_session_is_respawned_without_duplication(
    bindings: CmuxSurfaceBindings,
) -> None:
    # Sol correction f28e8484: a dead lower Hermes session is restored
    # idempotently — same surface, same durable session command, no new
    # workspace, pane, or session.
    port = FakeWorkspacePort()
    owner = lifecycle(port, bindings)
    first = await owner.ensure()
    port.kill_process(first.lower.surface_uuid)

    recovered = await owner.ensure()

    assert recovered.outcome == "recovered"
    assert recovered.respawned == (LOWER_ROLE,)
    assert recovered.workspace_uuid == first.workspace_uuid
    assert recovered.lower == first.lower
    assert port.respawns[-1] == (first.lower, HERMES_COMMAND)
    assert len(port.created) == 1
    assert len(port.respawns) == 1
    # And restored liveness makes the next ensure a pure adoption.
    settled = await owner.ensure()
    assert settled.outcome == "adopted"
    assert len(port.respawns) == 1


@pytest.mark.asyncio
async def test_dead_upper_supervisor_is_respawned_in_place(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = lifecycle(port, bindings)
    first = await owner.ensure()
    port.kill_process(first.upper.surface_uuid)

    recovered = await owner.ensure()

    assert recovered.outcome == "recovered"
    assert recovered.respawned == (UPPER_ROLE,)
    assert port.respawns[-1] == (first.upper, SUPERVISOR_COMMAND)
    assert len(port.created) == 1


@pytest.mark.asyncio
async def test_partial_workspace_is_closed_and_rebuilt(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = lifecycle(port, bindings)
    first = await owner.ensure()
    port.drop_pane(first.workspace_uuid, first.lower.surface_uuid)

    rebuilt = await owner.ensure()

    assert rebuilt.outcome == "created"
    assert rebuilt.workspace_uuid != first.workspace_uuid
    assert first.workspace_uuid in port.closed
    assert rebuilt.generation == first.generation + 1
    assert bindings.active_orchestrator().workspace_uuid == (
        rebuilt.workspace_uuid
    )


@pytest.mark.asyncio
async def test_tabbed_single_pane_layout_is_not_two_stacked_panes(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = lifecycle(port, bindings)
    first = await owner.ensure()
    port.merge_panes(first.workspace_uuid)

    rebuilt = await owner.ensure()

    assert rebuilt.outcome == "created"
    assert rebuilt.workspace_uuid != first.workspace_uuid


@pytest.mark.asyncio
async def test_dead_workspace_is_recreated_under_successor_generation(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = lifecycle(port, bindings)
    first = await owner.ensure()
    await port.close_workspace(first.workspace_uuid)

    rebuilt = await owner.ensure()

    assert rebuilt.outcome == "created"
    assert rebuilt.generation == first.generation + 1
    assert rebuilt.workspace_uuid != first.workspace_uuid
    assert bindings.get(first.binding_id).state == "stale"


@pytest.mark.asyncio
async def test_unowned_marker_workspaces_are_closed_as_orphans(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    orphan = port.seed_workspace("Orchestrator [hermes-orch:orch]")
    unrelated = port.seed_workspace("Sol lead [hermes:intent-9]")
    owner = lifecycle(port, bindings)

    state = await owner.ensure()

    assert orphan in port.closed
    assert unrelated not in port.closed
    assert state.outcome == "created"
    assert len(await port.live_workspace_uuids()) == 2


@pytest.mark.asyncio
async def test_close_tears_down_and_retires_the_binding(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = lifecycle(port, bindings)
    state = await owner.ensure()

    closed = await owner.close()

    assert closed == (state.workspace_uuid,)
    assert state.workspace_uuid not in port.workspaces
    assert bindings.active_orchestrator() is None
    assert bindings.get(state.binding_id).state == "closed"


# ---------------------------------------------------------------------------
# Smoke driver on fakes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_smoke_produces_complete_two_pane_evidence(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = lifecycle(port, bindings)

    evidence = await run_smoke(owner, port)

    assert evidence["passed"] is True
    assert evidence["stack_direction"] == STACK_DIRECTION
    assert evidence["supervisor_command"] == SUPERVISOR_COMMAND
    assert evidence["hermes_command"] == HERMES_COMMAND
    inspection = evidence["inspection"]
    assert inspection["pane_count"] == 2
    assert inspection["distinct_panes"] == 2
    assert inspection["both_panes_present"] is True
    assert inspection["both_processes_live"] is True
    assert {row["role"] for row in inspection["surfaces"]} == {
        UPPER_ROLE,
        LOWER_ROLE,
    }
    assert evidence["stable_identities"] is True
    assert evidence["screens"][UPPER_ROLE]
    assert evidence["screens"][LOWER_ROLE]
    recovery = evidence["restart_recovery"]
    assert recovery["workspace_replaced"] is True
    assert recovery["generation_advanced"] is True
    assert recovery["inspection"]["both_processes_live"] is True
    assert evidence["teardown"]["workspace_remains"] is False
    assert port.workspaces == {}


# ---------------------------------------------------------------------------
# CLI dispatch
# ---------------------------------------------------------------------------


def invoke(arguments: list[str]) -> tuple[int, str]:
    from hermes_orchestrator.cli import main

    stdout = StringIO()
    stderr = StringIO()
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = main(arguments)
    except SystemExit as error:
        exit_code = int(error.code)
    return exit_code, stdout.getvalue() + stderr.getvalue()


@pytest.fixture
def cli_port(monkeypatch: pytest.MonkeyPatch) -> FakeWorkspacePort:
    port = FakeWorkspacePort()

    def build_adapter(*args: object, **kwargs: object) -> FakeWorkspacePort:
        return port

    monkeypatch.setattr(
        "hermes_orchestrator.cli.CmuxCliAdapter", build_adapter
    )
    return port


def cli_arguments(
    configured_repo: tuple[Path, Path], *tail: str
) -> list[str]:
    repo_root, state_dir = configured_repo
    return [
        "--repo-root",
        str(repo_root),
        "--state-dir",
        str(state_dir),
        "orchestrator-workspace",
        *tail,
        "--cmux-cli",
        "/fake/cmux",
        "--json",
    ]


def test_cli_ensure_dispatches_and_reports_identities(
    configured_repo: tuple[Path, Path], cli_port: FakeWorkspacePort
) -> None:
    exit_code, output = invoke(
        cli_arguments(configured_repo, "ensure", "--name", "cli-test")
    )

    assert exit_code == 0
    payload = json.loads(output)
    assert payload["outcome"] == "created"
    workspace = cli_port.workspaces[payload["workspace_uuid"]]
    assert len(workspace.panes) == 2
    assert payload["upper_surface_uuid"] != payload["lower_surface_uuid"]


def test_cli_smoke_proves_two_real_stacked_panes_with_stable_identities(
    configured_repo: tuple[Path, Path], cli_port: FakeWorkspacePort
) -> None:
    # Sol correction f28e8484: CLI-level proof of exactly two real
    # panes in one workspace, vertically stacked, with stable
    # identities across inspection calls — and teardown.
    exit_code, output = invoke(
        cli_arguments(
            configured_repo,
            "smoke",
            "--name",
            "cli-smoke",
            "--settle-seconds",
            "0",
        )
    )

    assert exit_code == 0
    evidence = json.loads(output)
    assert evidence["passed"] is True
    assert evidence["stack_direction"] == "vertical"
    assert evidence["inspection"]["pane_count"] == 2
    assert evidence["inspection"]["distinct_panes"] == 2
    assert evidence["stable_identities"] is True
    assert evidence["teardown"]["workspace_remains"] is False
    surfaces = {
        row["role"]: row["surface_uuid"]
        for row in evidence["inspection"]["surfaces"]
    }
    assert surfaces[UPPER_ROLE] == (
        evidence["first_ensure"]["upper_surface_uuid"]
    )
    assert surfaces[LOWER_ROLE] == (
        evidence["first_ensure"]["lower_surface_uuid"]
    )
    assert cli_port.workspaces == {}


def test_cli_refuses_without_cmux_configuration(
    configured_repo: tuple[Path, Path],
) -> None:
    repo_root, state_dir = configured_repo
    exit_code, output = invoke(
        [
            "--repo-root",
            str(repo_root),
            "--state-dir",
            str(state_dir),
            "orchestrator-workspace",
            "ensure",
            "--json",
        ]
    )

    assert exit_code == 1
    assert "cmux is not configured" in json.loads(output)["error"]
