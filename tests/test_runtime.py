from __future__ import annotations

from pathlib import Path

import pytest

import hermes_orchestrator.runtime as runtime_module
from hermes_orchestrator.config import load_settings
from hermes_orchestrator.profiles import JsonCommand
from hermes_orchestrator.runtime import DaemonAlreadyRunning, open_runtime


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
