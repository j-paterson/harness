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

    def set_processes(self, surface_uuid: str, names: list[str]) -> None:
        self._pane(surface_uuid).processes = list(names)

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
                "env": dict(env or {}),  # type: ignore[arg-type]
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
    assert upper["role_proven"]
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
    assert inspection["both_roles_proven"] is True
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
    assert recovery["inspection"]["both_roles_proven"] is True
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


# ---------------------------------------------------------------------------
# Sol packet 2 (575fe76c): role-specific process identity from typed
# metadata — never screen content.
# ---------------------------------------------------------------------------

from hermes_orchestrator.orchestrator_workspace import (  # noqa: E402
    SEAT_ENV,
    OrchestratorWorkspaceOwner,
    evidence_document,
    hermes_process_identity,
    smoke_passed,
    supervisor_process_identity,
    verify_evidence_document,
)


@pytest.mark.parametrize(
    "names",
    [
        ["Python", "sleep", "uv", "zsh", "zsh"],
        ["zsh", "uv", "python3.13"],
        ["hermes-orchestrator"],
    ],
)
def test_supervisor_identity_accepts_expected_trees_with_wrappers(
    names: list[str],
) -> None:
    assert supervisor_process_identity(names)
    assert not hermes_process_identity(names)


@pytest.mark.parametrize(
    "names",
    [
        ["python3.11", "sleep", "zsh", "zsh"],
        ["zsh", "hermes", "node"],
        ["Python"],
    ],
)
def test_hermes_identity_accepts_expected_trees_with_wrappers(
    names: list[str],
) -> None:
    assert hermes_process_identity(names)
    assert not supervisor_process_identity(names)


@pytest.mark.parametrize(
    "names",
    [
        ["zsh"],
        [],
        ["zsh", "vim"],
        ["zsh", "uv", "vim"],
        ["zsh", "python3.11", "ssh"],
    ],
)
def test_neither_role_accepts_unrelated_or_empty_trees(
    names: list[str],
) -> None:
    assert not supervisor_process_identity(names)
    assert not hermes_process_identity(names)


@pytest.mark.asyncio
async def test_unrelated_process_in_a_pane_is_not_adopted(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = lifecycle(port, bindings)
    first = await owner.ensure()
    port.set_processes(first.lower.surface_uuid, ["zsh", "vim"])

    recovered = await owner.ensure()

    assert recovered.outcome == "recovered"
    assert recovered.respawned == (LOWER_ROLE,)
    assert port.respawns[-1] == (first.lower, HERMES_COMMAND)
    assert len(port.created) == 1
    assert len(port.workspaces[first.workspace_uuid].panes) == 2


@pytest.mark.asyncio
async def test_wrong_role_trees_are_recovered_without_new_resources(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = lifecycle(port, bindings)
    first = await owner.ensure()
    # Swapped identities: a bare python (Hermes-shaped) upper, a
    # uv-launched (supervisor-shaped) lower.
    port.set_processes(first.upper.surface_uuid, ["zsh", "python3.11"])
    port.set_processes(first.lower.surface_uuid, ["zsh", "uv", "Python"])

    recovered = await owner.ensure()

    assert recovered.outcome == "recovered"
    assert set(recovered.respawned) == {UPPER_ROLE, LOWER_ROLE}
    respawned = {ref.surface_uuid: command for ref, command in port.respawns}
    assert respawned[first.upper.surface_uuid] == SUPERVISOR_COMMAND
    assert respawned[first.lower.surface_uuid] == HERMES_COMMAND
    assert len(port.created) == 1
    assert len(port.workspaces[first.workspace_uuid].panes) == 2


class StubbornRolePort(FakeWorkspacePort):
    """Respawns are recorded but never restore role identity."""

    async def respawn_surface(
        self, ref: CmuxSurfaceRef, command: str
    ) -> None:
        self.respawns.append((ref, command))

    async def create_two_pane_workspace(self, **kwargs: object):
        upper, lower = await super().create_two_pane_workspace(**kwargs)
        self.set_processes(lower.surface_uuid, ["zsh", "vim"])
        return upper, lower


@pytest.mark.asyncio
async def test_smoke_fails_unless_both_role_identities_are_proven(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = StubbornRolePort()
    owner = lifecycle(port, bindings)

    evidence = await run_smoke(owner, port)

    assert evidence["passed"] is False
    assert evidence["inspection"]["both_roles_proven"] is False


# ---------------------------------------------------------------------------
# Sol packet 1 (575fe76c): seated recursion guard and the stable owner.
# ---------------------------------------------------------------------------


def seated_lifecycle(
    port: FakeWorkspacePort, bindings: CmuxSurfaceBindings
) -> OrchestratorWorkspaceLifecycle:
    return OrchestratorWorkspaceLifecycle(
        port=port,
        bindings=bindings,
        repo_root=REPO_ROOT,
        state_dir=STATE_DIR,
        name="orch",
        title="Orchestrator",
        seated=True,
    )


@pytest.mark.asyncio
async def test_created_workspace_carries_the_seat_marker_env(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    await lifecycle(port, bindings).ensure()

    [created] = port.created
    assert created["env"] == {SEAT_ENV: "orch"}


@pytest.mark.asyncio
async def test_seated_supervisor_adopts_and_reconciles_lower_only(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    first = await lifecycle(port, bindings).ensure()
    seated = seated_lifecycle(port, bindings)

    adopted = await seated.ensure()
    assert adopted.outcome == "adopted"
    assert adopted.workspace_uuid == first.workspace_uuid

    port.kill_process(first.lower.surface_uuid)
    recovered = await seated.ensure()
    assert recovered.outcome == "recovered"
    assert recovered.respawned == (LOWER_ROLE,)
    assert port.respawns[-1] == (first.lower, HERMES_COMMAND)
    assert len(port.created) == 1


@pytest.mark.asyncio
async def test_seated_supervisor_never_respawns_its_own_upper_pane(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    first = await lifecycle(port, bindings).ensure()
    port.set_processes(first.upper.surface_uuid, ["zsh"])

    adopted = await seated_lifecycle(port, bindings).ensure()

    assert adopted.outcome == "adopted"
    assert all(
        ref.surface_uuid != first.upper.surface_uuid
        for ref, _ in port.respawns
    )


@pytest.mark.asyncio
async def test_seated_supervisor_never_creates_or_closes_workspaces(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    seated = seated_lifecycle(port, bindings)

    # No durable seat: refuse, create nothing.
    with pytest.raises(WorkspaceRefused, match="never creates"):
        await seated.ensure()
    assert port.created == []

    # A partial workspace: refuse to rebuild, close nothing.
    first = await lifecycle(port, bindings).ensure()
    port.drop_pane(first.workspace_uuid, first.lower.surface_uuid)
    with pytest.raises(WorkspaceRefused, match="cannot rebuild"):
        await seated.ensure()
    assert port.closed == []
    assert len(port.created) == 1


@pytest.mark.asyncio
async def test_owner_start_ensures_and_ticks_adopt_idempotently(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = OrchestratorWorkspaceOwner(lifecycle(port, bindings))

    started = await owner.start()
    assert started is not None and started.outcome == "created"

    ticked = await owner.tick()
    assert ticked is not None and ticked.outcome == "adopted"
    assert ticked.workspace_uuid == started.workspace_uuid
    assert owner.ensures == 2
    assert len(port.created) == 1


@pytest.mark.asyncio
async def test_owner_tick_respawns_dead_lower_session_in_place(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = OrchestratorWorkspaceOwner(lifecycle(port, bindings))
    started = await owner.start()
    port.kill_process(started.lower.surface_uuid)

    ticked = await owner.tick()

    assert ticked is not None and ticked.outcome == "recovered"
    assert port.respawns[-1] == (started.lower, HERMES_COMMAND)
    assert len(port.created) == 1


@pytest.mark.asyncio
async def test_owner_tick_rebuilds_dead_workspace_generation_advancing(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = OrchestratorWorkspaceOwner(lifecycle(port, bindings))
    started = await owner.start()
    await port.close_workspace(started.workspace_uuid)

    rebuilt = await owner.tick()

    assert rebuilt is not None and rebuilt.outcome == "created"
    assert rebuilt.generation == started.generation + 1

    port.drop_pane(rebuilt.workspace_uuid, rebuilt.lower.surface_uuid)
    repaired = await owner.tick()
    assert repaired is not None and repaired.outcome == "created"
    assert repaired.generation == rebuilt.generation + 1


@pytest.mark.asyncio
async def test_restarted_owner_adopts_exactly_once_without_duplicates(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    first_owner = OrchestratorWorkspaceOwner(lifecycle(port, bindings))
    started = await first_owner.start()

    # An owner restart composes a fresh owner over the same durable
    # bindings and the same live cmux: it adopts, never re-creates.
    second_owner = OrchestratorWorkspaceOwner(lifecycle(port, bindings))
    adopted = await second_owner.start()

    assert adopted is not None and adopted.outcome == "adopted"
    assert adopted.workspace_uuid == started.workspace_uuid
    assert len(port.created) == 1
    assert len(port.workspaces) == 1


@pytest.mark.asyncio
async def test_owner_teardown_stops_reconciliation_deterministically(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = OrchestratorWorkspaceOwner(lifecycle(port, bindings))
    started = await owner.start()

    closed = await owner.teardown()

    assert closed == (started.workspace_uuid,)
    assert owner.stopped is True
    assert await owner.tick() is None
    assert len(port.created) == 1
    assert port.workspaces == {}
    assert bindings.active_orchestrator() is None


@pytest.mark.asyncio
async def test_owner_absorbs_refusals_and_records_them(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    owner = OrchestratorWorkspaceOwner(seated_lifecycle(port, bindings))

    assert await owner.start() is None
    assert owner.last_error is not None
    assert "WorkspaceRefused" in owner.last_error
    assert port.created == []


# ---------------------------------------------------------------------------
# Sol packet 3 (575fe76c): mandatory replacement/generation clauses and
# the digest-sealed evidence document.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_smoke_pass_condition_requires_every_clause(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    evidence = await run_smoke(lifecycle(port, bindings), port)
    assert smoke_passed(evidence) is True

    import copy

    def mutated(apply) -> dict:
        clone = copy.deepcopy(evidence)
        apply(clone)
        return clone

    failures = [
        mutated(
            lambda e: e["restart_recovery"].__setitem__(
                "workspace_replaced", False
            )
        ),
        mutated(
            lambda e: e["restart_recovery"].__setitem__(
                "generation_advanced", False
            )
        ),
        mutated(
            lambda e: e["inspection"].__setitem__(
                "both_roles_proven", False
            )
        ),
        mutated(
            lambda e: e["restart_recovery"]["inspection"].__setitem__(
                "both_roles_proven", False
            )
        ),
        mutated(
            lambda e: e.__setitem__("stable_identities", False)
        ),
        mutated(
            lambda e: e["teardown"].__setitem__("workspace_remains", True)
        ),
    ]
    for broken in failures:
        assert smoke_passed(broken) is False


class NonClosingPort(FakeWorkspacePort):
    """close-workspace acks but the workspace survives (a stuck close)."""

    async def close_workspace(self, workspace_uuid: str) -> None:
        self.closed.append(workspace_uuid)


@pytest.mark.asyncio
async def test_smoke_fails_when_the_workspace_is_not_replaced(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = NonClosingPort()
    evidence = await run_smoke(lifecycle(port, bindings), port)

    assert evidence["restart_recovery"]["workspace_replaced"] is False
    assert evidence["restart_recovery"]["generation_advanced"] is False
    assert evidence["passed"] is False


@pytest.mark.asyncio
async def test_evidence_document_binds_the_run_and_revalidates(
    bindings: CmuxSurfaceBindings,
) -> None:
    port = FakeWorkspacePort()
    evidence = await run_smoke(lifecycle(port, bindings), port)
    document = evidence_document(
        evidence,
        source_sha="52cd59dcafe",
        invocation=["hermes-orchestrator", "orchestrator-workspace", "smoke"],
        captured_at="2026-08-30T20:00:00+00:00",
    )

    assert document["schema"] == "infra-191-live-smoke/v1"
    assert document["source_sha"] == "52cd59dcafe"
    assert document["invocation"][1] == "orchestrator-workspace"
    body = document["evidence"]
    assert body["first_ensure"]["generation"] == 1
    assert body["restart_recovery"]["ensure"]["generation"] == 2
    assert body["inspection"]["surfaces"][0]["process_names"]
    assert body["teardown"]["workspace_remains"] is False
    assert verify_evidence_document(document) is True

    tampered = json.loads(json.dumps(document))
    tampered["evidence"]["passed"] = False
    assert verify_evidence_document(tampered) is False
    reforged = json.loads(json.dumps(document))
    reforged["self_digest"] = "0" * 64
    assert verify_evidence_document(reforged) is False


def test_cli_smoke_writes_a_digest_sealed_evidence_file(
    configured_repo: tuple[Path, Path],
    cli_port: FakeWorkspacePort,
    tmp_path: Path,
) -> None:
    evidence_file = tmp_path / "receipts" / "live-smoke.json"
    exit_code, output = invoke(
        cli_arguments(
            configured_repo,
            "smoke",
            "--name",
            "cli-evidence",
            "--settle-seconds",
            "0",
            "--evidence-file",
            str(evidence_file),
        )
    )

    assert exit_code == 0
    printed = json.loads(output)
    assert printed["evidence_file"] == str(evidence_file)
    document = json.loads(evidence_file.read_text(encoding="utf-8"))
    assert verify_evidence_document(document) is True
    assert printed["self_digest"] == document["self_digest"]
    assert document["invocation"][0] == "hermes-orchestrator"
    assert "smoke" in document["invocation"]
    assert isinstance(document["source_sha"], str)
    assert document["evidence"]["passed"] is True
