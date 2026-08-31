"""Runtime composition and daemon wiring for the Orchestrator dashboard.

The dashboard MVP modules are composed in runtime.py and ticked from the
daemon's exception-suppressed maintenance slot in cli.py; these tests pin
that wiring end to end over a tmp database. Nothing here calls a model.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.cli import _run_daemon
from hermes_orchestrator.config import load_settings
from hermes_orchestrator.dashboard_pane import DashboardPane
from hermes_orchestrator.dashboard_refresh import DashboardRefreshAction
from hermes_orchestrator.dashboard_sources import (
    CodexFact,
    DashboardSnapshot,
    ProfileUsage,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.profiles import ProfileConfig, ProfileRegistry
from hermes_orchestrator.runtime import open_runtime
from tests.test_runtime import EligibleProfileCommand, FakeKeychain, active_repo
from tests.test_supervisor import FakeService

_ALIASES = ("max-a", "max-b", "max-c", "max-d")


class _TtyStream(io.StringIO):
    """StringIO that reports a TTY so the pane writes for real."""

    def isatty(self) -> bool:
        return True


class _ScriptedSources:
    """Fail on scripted ticks, then serve a fixed snapshot."""

    def __init__(self, failures: int) -> None:
        self.failures = failures

    async def collect(self, now: datetime) -> DashboardSnapshot:
        if self.failures > 0:
            self.failures -= 1
            raise TimeoutError("sources unavailable")
        return DashboardSnapshot(
            generated_at=now.isoformat(),
            usage=tuple(
                ProfileUsage(alias, fable_tokens=0, overall_tokens=0)
                for alias in _ALIASES
            ),
            leases=(),
            codex=CodexFact(
                available=False,
                unavailable_since=now.isoformat(),
            ),
        )


def _clock() -> datetime:
    return datetime(2026, 8, 30, 12, 0, tzinfo=UTC)


def _registry(tmp_path: Path) -> ProfileRegistry:
    return ProfileRegistry(
        tuple(ProfileConfig(alias, tmp_path / alias) for alias in _ALIASES)
    )


def _seeded_database(tmp_path: Path) -> Database:
    database = Database.open(tmp_path / "state.db")
    events = EventStore(database)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO profile_leases("
            "profile_alias, project_key, state, acquired_at, cooldown_until"
            ") VALUES (?, ?, ?, ?, ?)",
            ("max-b", "demo", "active", "2026-08-30T08:00:00+00:00", None),
        )
        events.append(
            connection,
            EventInput(
                event_type="stream.assistant",
                aggregate_type="project_cell",
                aggregate_id="cell-1",
                payload={
                    "profile_alias": "max-b",
                    "parent_tool_use_id": None,
                    "usage": {"input_tokens": 12, "output_tokens": 3},
                },
            ),
        )
    return database


def _composed_action(
    database: Database, tmp_path: Path, stream: _TtyStream
) -> DashboardRefreshAction:
    """Compose the daemon's pieces exactly as runtime.py does, on a
    fake TTY with a fixed size and clock for determinism."""

    return DashboardRefreshAction(
        database=database,
        registry=_registry(tmp_path),
        pane=DashboardPane(stream, rows=lambda: 50),
        now=_clock,
    )


def test_open_runtime_with_registry_composes_dashboard_refresh(
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
        assert isinstance(runtime.dashboard_refresh, DashboardRefreshAction)
    finally:
        runtime.close()


def test_open_runtime_without_registry_composes_no_dashboard(
    tmp_path: Path,
) -> None:
    # Observation-only assembly never loads profiles.yaml, so there is
    # no registry to read from: the field stays None and the daemon
    # fails open to no dashboard instead of crashing.
    repo_root, state_dir = active_repo(tmp_path)
    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(settings, enable_live=False)
    try:
        assert runtime.dashboard_refresh is None
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_daemon_maintenance_tick_drives_read_render_draw(
    tmp_path: Path,
) -> None:
    database = _seeded_database(tmp_path)
    try:
        stream = _TtyStream()
        action = _composed_action(database, tmp_path, stream)
        service = FakeService()

        await _run_daemon(
            service, once=True, interval=60, dashboard_refresh=action
        )

        output = stream.getvalue()
        # The scroll region was established exactly once (DECSTBM to the
        # fake 50-row terminal) even though both the startup hook and the
        # maintenance tick drew.
        assert output.count(";50r") == 1
        # Seeded durable rows made it through read -> render -> draw.
        assert "max-b" in output
        assert "demo/active" in output
        assert "last tick ok" in output
        assert service.ticks == 1
    finally:
        database.close()


@pytest.mark.asyncio
async def test_restart_recomposes_pane_and_renders_identical_content(
    tmp_path: Path,
) -> None:
    database = _seeded_database(tmp_path)
    try:
        first_stream = _TtyStream()
        await _run_daemon(
            FakeService(),
            once=True,
            interval=60,
            dashboard_refresh=_composed_action(database, tmp_path, first_stream),
        )

        # A daemon restart composes fresh pieces over the same durable
        # state; the fresh writer's idempotent establishment recovers the
        # region without any dedicated recovery path, and the full-journal
        # re-read reproduces the exact same frame.
        second_stream = _TtyStream()
        await _run_daemon(
            FakeService(),
            once=True,
            interval=60,
            dashboard_refresh=_composed_action(
                database, tmp_path, second_stream
            ),
        )

        assert second_stream.getvalue().count(";50r") == 1
        assert second_stream.getvalue() == first_stream.getvalue()
    finally:
        database.close()


@pytest.mark.asyncio
async def test_tick_exception_is_contained_and_surfaces_next_render(
    tmp_path: Path,
) -> None:
    stream = _TtyStream()
    action = DashboardRefreshAction(
        sources=_ScriptedSources(failures=1),
        pane=DashboardPane(stream, rows=lambda: 50),
        now=_clock,
    )
    service = FakeService()

    # The startup hook's tick fails: nothing propagates out of the
    # daemon and nothing is drawn for that tick. The maintenance tick
    # then renders the recorded failure as an explicit fact.
    supervisor = await _run_daemon(
        service, once=True, interval=60, dashboard_refresh=action
    )

    assert supervisor.ticks == 1
    output = stream.getvalue()
    assert "failed at 2026-08-30T12:00:00+00:00" in output
    assert "TimeoutError" in output


# ---------------------------------------------------------------------------
# INFRA-191 (Sol packet 575fe76c #1): the daemon owns the two-pane
# Orchestrator workspace autonomously — composed in runtime.py, ensured
# at startup, reconciled from the bounded maintenance slot.
# ---------------------------------------------------------------------------

from hermes_orchestrator.cmux_surfaces import (  # noqa: E402
    CmuxSurfaceBindings,
)
from hermes_orchestrator.orchestrator_workspace import (  # noqa: E402
    SEAT_ENV,
    OrchestratorWorkspaceLifecycle,
    OrchestratorWorkspaceOwner,
)
from tests.test_orchestrator_workspace import (  # noqa: E402
    FakeWorkspacePort,
)


def _cmux_configured(repo_root: Path) -> None:
    (repo_root / "config" / "cmux.yaml").write_text(
        "cli:\n  - /apps/cmux\n", encoding="utf-8"
    )


def test_open_runtime_composes_the_workspace_owner_outside_panes(
    tmp_path: Path,
) -> None:
    repo_root, state_dir = active_repo(tmp_path)
    _cmux_configured(repo_root)
    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={},
    )
    try:
        owner = runtime.orchestrator_workspace
        assert isinstance(owner, OrchestratorWorkspaceOwner)
        assert owner.lifecycle.inside_marked_pane is False
    finally:
        runtime.close()


def test_open_runtime_marks_a_daemon_inside_a_pane_as_fail_closed(
    tmp_path: Path,
) -> None:
    # Sol K1: no daemon may run inside a pane. One launched with the
    # pane marker composes a lifecycle that refuses every operation —
    # never a second workspace or supervisor, fail closed.
    repo_root, state_dir = active_repo(tmp_path)
    _cmux_configured(repo_root)
    settings = load_settings(repo_root, state_dir)
    runtime = open_runtime(
        settings,
        enable_live=True,
        profile_command=EligibleProfileCommand(),
        keychain=FakeKeychain(),
        base_env={SEAT_ENV: "orchestrator"},
    )
    try:
        owner = runtime.orchestrator_workspace
        assert isinstance(owner, OrchestratorWorkspaceOwner)
        assert owner.lifecycle.inside_marked_pane is True
    finally:
        runtime.close()


def test_open_runtime_without_cmux_composes_no_owner(
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
        assert runtime.orchestrator_workspace is None
    finally:
        runtime.close()


def _owner_over(tmp_path: Path, port: FakeWorkspacePort):
    database = Database.open(tmp_path / "owner-state.db")
    bindings = CmuxSurfaceBindings(
        database=database, events=EventStore(database)
    )
    owner = OrchestratorWorkspaceOwner(
        OrchestratorWorkspaceLifecycle(
            port=port,
            bindings=bindings,
            repo_root=Path("/repo/orchestrator"),
            state_dir=Path("/state/orchestrator"),
            name="daemonized",
            lineage=port,
        )
    )
    return database, owner


@pytest.mark.asyncio
async def test_daemon_startup_autonomously_ensures_the_workspace(
    tmp_path: Path,
) -> None:
    port = FakeWorkspacePort()
    database, owner = _owner_over(tmp_path, port)
    try:
        service = FakeService()

        await _run_daemon(
            service, once=True, interval=60, orchestrator_workspace=owner
        )

        # No manual CLI step: the startup hook created the two-pane
        # workspace and the maintenance tick adopted it unchanged.
        assert service.ticks == 1
        assert len(port.created) == 1
        [workspace] = port.workspaces.values()
        assert len(workspace.panes) == 2
        assert owner.ensures == 2
        assert owner.last_state is not None
        assert owner.last_state.outcome == "adopted"
    finally:
        database.close()


@pytest.mark.asyncio
async def test_daemon_restart_adopts_once_and_reconciles_the_lower_pane(
    tmp_path: Path,
) -> None:
    port = FakeWorkspacePort()
    database, first_owner = _owner_over(tmp_path, port)
    try:
        await _run_daemon(
            FakeService(),
            once=True,
            interval=60,
            orchestrator_workspace=first_owner,
        )
        started = first_owner.last_state
        assert started is not None
        port.kill_process(started.lower.surface_uuid)

        # A daemon restart composes a fresh owner over the same durable
        # bindings: it adopts the surviving workspace exactly once and
        # respawns the dead Hermes session in place — no second
        # workspace, no second daemon.
        second_owner = OrchestratorWorkspaceOwner(first_owner.lifecycle)
        await _run_daemon(
            FakeService(),
            once=True,
            interval=60,
            orchestrator_workspace=second_owner,
        )

        assert len(port.created) == 1
        assert len(port.workspaces) == 1
        assert port.respawns[-1][0].surface_uuid == (
            started.lower.surface_uuid
        )
    finally:
        database.close()


# ---------------------------------------------------------------------------
# Sol K1 (a6bc7ca2): the upper pane is the lock-free read-only dashboard
# entry; the one daemon keeps the exclusive state lock; no
# DaemonAlreadyRunning, no respawn loop.
# ---------------------------------------------------------------------------

import json  # noqa: E402
import sqlite3  # noqa: E402
from contextlib import redirect_stderr, redirect_stdout  # noqa: E402
from io import StringIO  # noqa: E402

from hermes_orchestrator.cli import main  # noqa: E402
from hermes_orchestrator.runtime import _DaemonLock  # noqa: E402


def _invoke(arguments: list[str]) -> tuple[int, str]:
    stdout = StringIO()
    stderr = StringIO()
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = main(arguments)
    except SystemExit as error:
        exit_code = int(error.code)
    return exit_code, stdout.getvalue() + stderr.getvalue()


def test_dashboard_entry_runs_readonly_while_the_daemon_holds_the_lock(
    tmp_path: Path,
) -> None:
    # The production topology: the one daemon owns the exclusive state
    # lock the entire time; the upper-pane dashboard entry still works
    # because it never takes the lock and never migrates.
    repo_root, state_dir = active_repo(tmp_path)
    Database.open(state_dir / "state.db").close()
    lock = _DaemonLock(state_dir / "daemon.lock")
    lock.acquire()
    try:
        exit_code, output = _invoke(
            [
                "--repo-root",
                str(repo_root),
                "--state-dir",
                str(state_dir),
                "dashboard",
                "--once",
                "--json",
            ]
        )
    finally:
        lock.release()

    assert exit_code == 0
    payload = json.loads(output)
    assert payload["ticks"] == 1
    assert payload["read_only"] is True
    assert payload["daemon_lock"] is False


def test_dashboard_refuses_any_schema_generation_mismatch(
    tmp_path: Path,
) -> None:
    # Database.open always applies pending migrations, so the
    # dashboard proves the open is a no-op first — a checkout ahead of
    # or behind the database refuses instead of mutating daemon state.
    repo_root, state_dir = active_repo(tmp_path)
    Database.open(state_dir / "state.db").close()
    connection = sqlite3.connect(state_dir / "state.db")
    try:
        connection.execute(
            "INSERT INTO schema_migrations(version, applied_at) "
            "VALUES (9999, '2026-08-31T00:00:00+00:00')"
        )
        connection.commit()
    finally:
        connection.close()

    exit_code, output = _invoke(
        [
            "--repo-root",
            str(repo_root),
            "--state-dir",
            str(state_dir),
            "dashboard",
            "--once",
            "--json",
        ]
    )

    assert exit_code == 1
    payload = json.loads(output)
    assert payload["error"] == "schema generation mismatch"
    assert payload["database_schema"] == 9999


@pytest.mark.asyncio
async def test_upper_pane_stays_live_across_maintenance_intervals(
    tmp_path: Path,
) -> None:
    # With the daemon holding the REAL exclusive lock for the entire
    # run, the dashboard-topology upper pane keeps proving its role
    # tick after tick: exactly one daemon owns the state, and the
    # DaemonAlreadyRunning respawn loop Sol evidenced cannot exist —
    # zero respawns across multiple maintenance intervals.
    port = FakeWorkspacePort()
    database, owner = _owner_over(tmp_path, port)
    lock = _DaemonLock(tmp_path / "owner-state-daemon.lock")
    lock.acquire()
    try:
        started = await owner.start()
        assert started is not None and started.outcome == "created"
        assert " dashboard " in f'{port.created[0]["upper_command"]} '
        assert " daemon" not in str(port.created[0]["upper_command"])
        for _ in range(3):
            ticked = await owner.tick()
            assert ticked is not None and ticked.outcome == "adopted"
        assert port.respawns == []
        assert len(port.created) == 1
        assert len(port.workspaces) == 1
    finally:
        lock.release()
        database.close()


# ---------------------------------------------------------------------------
# Sol L3 (9cbe7613): the dashboard is genuinely read-only — one
# long-lived mode=ro connection for the schema probe and every read.
# ---------------------------------------------------------------------------

import os  # noqa: E402
import stat  # noqa: E402

from hermes_orchestrator.cli import _ReadOnlyDashboardDatabase  # noqa: E402


def _recording_connect(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record every sqlite URI/path the CLI opens, passing through."""

    recorded: list[str] = []
    real_connect = sqlite3.connect

    def recorder(target, *args, **kwargs):  # type: ignore[no-untyped-def]
        recorded.append(str(target))
        return real_connect(target, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", recorder)
    return recorded


def test_write_forbidden_filesystem_fails_closed_no_immutable_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Sol M2: a WAL reader that cannot map its -shm index must refuse.
    # A write-forbidding view of the filesystem does not prove the
    # daemon is not writing the same file through another view, so an
    # unlocked immutable=1 retry is forbidden — the entry fails closed.
    repo_root, state_dir = active_repo(tmp_path)
    Database.open(state_dir / "state.db").close()
    recorded = _recording_connect(monkeypatch)
    os.chmod(state_dir / "state.db", stat.S_IRUSR)
    os.chmod(state_dir, stat.S_IRUSR | stat.S_IXUSR)
    try:
        exit_code, output = _invoke(
            [
                "--repo-root",
                str(repo_root),
                "--state-dir",
                str(state_dir),
                "dashboard",
                "--once",
                "--json",
            ]
        )
    finally:
        os.chmod(state_dir, stat.S_IRWXU)
        os.chmod(state_dir / "state.db", stat.S_IRUSR | stat.S_IWUSR)

    assert exit_code == 1
    assert json.loads(output)["error"] == "read-only probe failed"
    ro_attempts = [uri for uri in recorded if "mode=ro" in uri]
    assert len(ro_attempts) == 1
    assert all("immutable" not in uri for uri in recorded)


def test_transient_probe_error_never_downgrades_to_immutable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A database whose probe errors (no schema_migrations table here)
    # refuses; it must never be retried through an unlocked
    # immutable=1 connection.
    repo_root, state_dir = active_repo(tmp_path)
    state_dir.mkdir(parents=True, exist_ok=True)
    bogus = sqlite3.connect(state_dir / "state.db")
    bogus.execute("CREATE TABLE unrelated (x INTEGER)")
    bogus.commit()
    bogus.close()
    recorded = _recording_connect(monkeypatch)

    exit_code, output = _invoke(
        [
            "--repo-root",
            str(repo_root),
            "--state-dir",
            str(state_dir),
            "dashboard",
            "--once",
            "--json",
        ]
    )

    assert exit_code == 1
    assert json.loads(output)["error"] == "read-only probe failed"
    assert all("immutable" not in uri for uri in recorded)


def test_concurrent_daemon_commits_are_visible_without_reopening(
    tmp_path: Path,
) -> None:
    # Coordinated mode=ro WAL reads observe the writing daemon's
    # commits on the SAME long-lived connection — the property the
    # immutable contract would forfeit.
    writer = Database.open(tmp_path / "state.db")
    try:
        connection = sqlite3.connect(
            f"file:{tmp_path / 'state.db'}?mode=ro", uri=True
        )
        connection.row_factory = sqlite3.Row
        try:
            wrapper = _ReadOnlyDashboardDatabase(connection)
            count_sql = "SELECT count(*) FROM profile_leases"
            assert wrapper.execute(count_sql).fetchone()[0] == 0

            with writer.transaction() as writing:
                writing.execute(
                    "INSERT INTO profile_leases("
                    "profile_alias, project_key, state, acquired_at, "
                    "cooldown_until) VALUES (?, ?, ?, ?, ?)",
                    (
                        "max-b",
                        "demo",
                        "active",
                        "2026-08-31T00:00:00+00:00",
                        None,
                    ),
                )

            assert wrapper.execute(count_sql).fetchone()[0] == 1
        finally:
            connection.close()
    finally:
        writer.close()


def test_dashboard_leaves_the_database_and_fence_untouched(
    tmp_path: Path,
) -> None:
    # No migrations, no transactions, no lock file: the database bytes
    # are identical after a dashboard run and the ownership fence is
    # never created. (A WAL-journal reader inherently touches the
    # -shm index; that is SQLite reader behavior, not a write by this
    # entry, and the main database file never changes.)
    import hashlib

    repo_root, state_dir = active_repo(tmp_path)
    Database.open(state_dir / "state.db").close()
    before = hashlib.sha256((state_dir / "state.db").read_bytes()).hexdigest()

    exit_code, _ = _invoke(
        [
            "--repo-root",
            str(repo_root),
            "--state-dir",
            str(state_dir),
            "dashboard",
            "--once",
            "--json",
        ]
    )

    assert exit_code == 0
    after = hashlib.sha256((state_dir / "state.db").read_bytes()).hexdigest()
    assert after == before
    assert not (state_dir / "daemon.lock").exists()


def test_readonly_wrapper_exposes_exactly_the_query_surface(
    tmp_path: Path,
) -> None:
    # One shared mode=ro connection serves the schema probe AND the
    # sources' reads; the wrapper offers execute() and nothing that
    # could mutate — no transaction, no scalar, no close-and-reopen.
    database = Database.open(tmp_path / "state.db")
    database.close()
    connection = sqlite3.connect(
        f"file:{tmp_path / 'state.db'}?mode=ro", uri=True
    )
    connection.row_factory = sqlite3.Row
    try:
        wrapper = _ReadOnlyDashboardDatabase(connection)
        applied = wrapper.execute(
            "SELECT coalesce(max(version), 0) FROM schema_migrations"
        ).fetchone()[0]
        assert applied > 0
        rows = wrapper.execute(
            "SELECT profile_alias FROM profile_leases", ()
        ).fetchall()
        assert rows == []
        assert not hasattr(wrapper, "transaction")
        assert not hasattr(wrapper, "scalar")
        with pytest.raises(sqlite3.OperationalError):
            wrapper.execute(
                "INSERT INTO schema_migrations(version, applied_at) "
                "VALUES (12345, 'never')"
            )
    finally:
        connection.close()
