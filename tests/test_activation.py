"""Durable runtime activation with safe rollback (INFRA-195)."""

from __future__ import annotations

import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.activation import (
    ActivationRefused,
    RuntimeActivator,
    checkout_migration_max,
)
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def activator(database: Database) -> RuntimeActivator:
    return RuntimeActivator(
        database, events=EventStore(database), now=lambda: NOW
    )


def git(checkout: Path, *args: str) -> None:
    subprocess.run(
        ("git", "-C", str(checkout), *args),
        check=True,
        capture_output=True,
        env={
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.invalid",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.invalid",
            "PATH": "/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin",
            "HOME": str(checkout),
        },
    )


def build_checkout(root: Path, *, schema: int) -> Path:
    checkout = root / "checkout"
    migrations = checkout / "src" / "hermes_orchestrator" / "migrations"
    migrations.mkdir(parents=True)
    for version in (1, schema):
        (migrations / f"{version:04d}_x.sql").write_text("-- x\n")
    (checkout / "pyproject.toml").write_text("[project]\nname='x'\n")
    git(checkout, "init", "-q")
    git(checkout, "add", "-A")
    git(checkout, "commit", "-qm", "init")
    return checkout


def test_a_clean_matched_checkout_activates_with_full_identity(
    database: Database, activator: RuntimeActivator, tmp_path: Path
) -> None:
    checkout = build_checkout(tmp_path, schema=database.schema_version())

    activation = activator.activate(
        checkout_root=checkout, binary_path=Path("/usr/local/bin/hermes")
    )

    assert activation.state == "active"
    assert activation.generation == 1
    assert len(activation.git_sha) == 40
    assert activation.checkout_root == str(checkout)
    assert activation.binary_path == "/usr/local/bin/hermes"
    assert activation.database_schema == database.schema_version()
    assert activator.current() == activation


def test_a_new_activation_supersedes_the_prior_one(
    database: Database, activator: RuntimeActivator, tmp_path: Path
) -> None:
    checkout = build_checkout(tmp_path, schema=database.schema_version())
    first = activator.activate(
        checkout_root=checkout, binary_path=Path("/bin/a")
    )
    second = activator.activate(
        checkout_root=checkout, binary_path=Path("/bin/b")
    )

    assert second.generation == first.generation + 1
    current = activator.current()
    assert current is not None
    assert current.activation_id == second.activation_id
    superseded = database.scalar(
        "SELECT state FROM runtime_activations WHERE activation_id = ?",
        (first.activation_id,),
    )
    assert str(superseded) == "superseded"


def test_a_dirty_checkout_is_refused_and_rolls_back_safely(
    database: Database, activator: RuntimeActivator, tmp_path: Path
) -> None:
    checkout = build_checkout(tmp_path, schema=database.schema_version())
    good = activator.activate(
        checkout_root=checkout, binary_path=Path("/bin/a")
    )
    (checkout / "src" / "hermes_orchestrator" / "extra.py").write_text("x\n")

    with pytest.raises(ActivationRefused, match="untracked"):
        activator.activate(checkout_root=checkout, binary_path=Path("/bin/b"))

    # The safe rollback: the prior activation is still the active one,
    # and the failed attempt is a durable record.
    current = activator.current()
    assert current is not None
    assert current.activation_id == good.activation_id
    failed = database.scalar(
        "SELECT COUNT(*) FROM runtime_activations WHERE state = 'failed'"
    )
    assert failed == 1


def test_a_schema_mismatched_checkout_is_refused(
    database: Database, activator: RuntimeActivator, tmp_path: Path
) -> None:
    checkout = build_checkout(
        tmp_path, schema=database.schema_version() + 5
    )

    with pytest.raises(ActivationRefused, match="mismatched"):
        activator.activate(checkout_root=checkout, binary_path=Path("/bin/a"))

    assert activator.current() is None


def test_a_stale_binary_without_git_is_refused(
    database: Database, activator: RuntimeActivator, tmp_path: Path
) -> None:
    checkout = tmp_path / "not-a-repo"
    checkout.mkdir()

    with pytest.raises(ActivationRefused, match="git HEAD"):
        activator.activate(checkout_root=checkout, binary_path=Path("/bin/a"))


def test_checkout_migration_max_reads_filename_prefixes(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "src" / "hermes_orchestrator" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0001_a.sql").write_text("-- a\n")
    (migrations / "0042_b.sql").write_text("-- b\n")
    (migrations / "notes.txt").write_text("ignored\n")

    assert checkout_migration_max(tmp_path) == 42
    assert checkout_migration_max(tmp_path / "missing") == 0


class TestApplyProtocol:
    """Journaled apply with proven process rollback (Sol 032cd4a5)."""

    @staticmethod
    def build(
        database: Database, tmp_path: Path, name: str, *, schema: int
    ) -> Path:
        checkout = tmp_path / name
        migrations = checkout / "src" / "hermes_orchestrator" / "migrations"
        migrations.mkdir(parents=True)
        for version in (1, schema):
            (migrations / f"{version:04d}_x.sql").write_text("-- x\n")
        (checkout / "pyproject.toml").write_text("[project]\nname='x'\n")
        git(checkout, "init", "-q")
        git(checkout, "add", "-A")
        git(checkout, "commit", "-qm", "init")
        return checkout

    @staticmethod
    def applier_for(
        database: Database, kickstart: object
    ) -> tuple[RuntimeActivator, object]:
        from hermes_orchestrator.activation import ActivationApplier

        activator = RuntimeActivator(database, events=EventStore(database))
        applier = ActivationApplier(
            activator,
            database,
            kickstart=kickstart,  # type: ignore[arg-type]
            sleep=lambda _seconds: None,
            verify_timeout_seconds=0.0,
        )
        return activator, applier

    def journal_state(self, database: Database) -> str:
        return str(
            database.scalar(
                "SELECT state FROM activation_applies "
                "ORDER BY created_at DESC, rowid DESC LIMIT 1"
            )
        )

    def test_a_healthy_apply_is_journaled_and_verified(
        self, database: Database, tmp_path: Path
    ) -> None:
        checkout = self.build(
            database, tmp_path, "co-a", schema=database.schema_version()
        )
        holder: dict[str, object] = {}

        def kickstart() -> None:
            # The freshly supervised daemon proves itself durably.
            holder["activator"].confirm_startup(  # type: ignore[attr-defined]
                checkout_root=checkout,
                binary_path=Path("/bin/hermes"),
                pid=4242,
            )

        activator, applier = self.applier_for(database, kickstart)
        holder["activator"] = activator

        report = applier.apply(  # type: ignore[attr-defined]
            checkout_root=checkout, binary_path=Path("/bin/hermes")
        )

        assert report.state == "verified"
        assert report.verified_pid == 4242
        assert self.journal_state(database) == "verified"
        current = activator.current()
        assert current is not None
        assert current.generation == report.target_generation

    def test_a_dead_new_runtime_rolls_back_with_proof(
        self, database: Database, tmp_path: Path
    ) -> None:
        checkout_a = self.build(
            database, tmp_path, "co-a", schema=database.schema_version()
        )
        checkout_b = self.build(
            database, tmp_path, "co-b", schema=database.schema_version()
        )
        holder: dict[str, object] = {}
        kicks: list[int] = []

        def kickstart() -> None:
            kicks.append(1)
            if len(kicks) == 1:
                return  # the new runtime never comes up
            # The rollback restart proves the restored identity.
            holder["activator"].confirm_startup(  # type: ignore[attr-defined]
                checkout_root=checkout_a,
                binary_path=Path("/bin/a"),
                pid=777,
            )

        activator, applier = self.applier_for(database, kickstart)
        holder["activator"] = activator
        prior = activator.activate(
            checkout_root=checkout_a, binary_path=Path("/bin/a")
        )

        report = applier.apply(  # type: ignore[attr-defined]
            checkout_root=checkout_b, binary_path=Path("/bin/b")
        )

        assert report.state == "rolled_back"
        assert len(kicks) == 2
        assert self.journal_state(database) == "rolled_back"
        current = activator.current()
        assert current is not None
        assert current.checkout_root == str(checkout_a)
        assert current.git_sha == prior.git_sha
        assert current.generation > prior.generation

    def test_an_unprovable_rollback_is_ambiguous_and_fails_closed(
        self, database: Database, tmp_path: Path
    ) -> None:
        from hermes_orchestrator.activation import ApplyFailed

        checkout_a = self.build(
            database, tmp_path, "co-a", schema=database.schema_version()
        )
        checkout_b = self.build(
            database, tmp_path, "co-b", schema=database.schema_version()
        )
        activator, applier = self.applier_for(
            database, lambda: None
        )
        activator.activate(
            checkout_root=checkout_a, binary_path=Path("/bin/a")
        )

        with pytest.raises(ApplyFailed, match="ambiguous"):
            applier.apply(  # type: ignore[attr-defined]
                checkout_root=checkout_b, binary_path=Path("/bin/b")
            )

        assert self.journal_state(database) == "ambiguous"

    def test_a_refused_target_journals_and_never_touches_the_process(
        self, database: Database, tmp_path: Path
    ) -> None:
        checkout = tmp_path / "not-a-repo"
        checkout.mkdir()
        kicks: list[int] = []
        _activator, applier = self.applier_for(
            database, lambda: kicks.append(1)
        )

        with pytest.raises(ActivationRefused):
            applier.apply(  # type: ignore[attr-defined]
                checkout_root=checkout, binary_path=Path("/bin/a")
            )

        assert kicks == []
        assert self.journal_state(database) == "refused"


class TestConfirmStartup:
    def test_bootstrap_activates_and_journals_the_start(
        self, database: Database, tmp_path: Path
    ) -> None:
        checkout = TestApplyProtocol.build(
            database, tmp_path, "co-a", schema=database.schema_version()
        )
        activator = RuntimeActivator(database, events=EventStore(database))

        confirmed = activator.confirm_startup(
            checkout_root=checkout, binary_path=Path("/bin/a"), pid=101
        )

        assert confirmed is not None
        assert confirmed.checkout_root == str(checkout)
        started = database.execute(
            "SELECT payload_json FROM events "
            "WHERE event_type = 'daemon.started'"
        ).fetchall()
        assert len(started) == 1

    def test_a_mismatched_daemon_never_self_activates(
        self, database: Database, tmp_path: Path
    ) -> None:
        """A daemon running foreign code must not supersede the active
        activation — that would silently undo a rollback."""

        checkout_a = TestApplyProtocol.build(
            database, tmp_path, "co-a", schema=database.schema_version()
        )
        checkout_b = TestApplyProtocol.build(
            database, tmp_path, "co-b", schema=database.schema_version()
        )
        activator = RuntimeActivator(database, events=EventStore(database))
        active = activator.activate(
            checkout_root=checkout_a, binary_path=Path("/bin/a")
        )

        confirmed = activator.confirm_startup(
            checkout_root=checkout_b, binary_path=Path("/bin/b"), pid=102
        )

        assert confirmed is None
        current = activator.current()
        assert current is not None
        assert current.activation_id == active.activation_id


def test_runtime_exec_resolves_the_active_checkout(
    database: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The supervised entry point execs the daemon from the durable
    active activation, falling back to the configured repo root only
    when nothing was ever activated."""

    import argparse

    from hermes_orchestrator import cli as cli_module

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        cli_module.os,
        "execvp",
        lambda file, argv: captured.update(file=file, argv=list(argv)),
    )
    arguments = argparse.Namespace(
        state_dir=tmp_path, repo_root=tmp_path / "stable", interval=30
    )

    assert cli_module._runtime_exec(arguments) == 1
    argv = captured["argv"]
    assert argv[argv.index("--project") + 1] == str(tmp_path / "stable")

    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO runtime_activations("
            "activation_id, schema_version, generation, binary_path, "
            "checkout_root, git_sha, database_schema, state, reason, "
            "activated_at, updated_at) VALUES ('act-1', 1, 1, '/bin/h', "
            "?, ?, 44, 'active', NULL, 't0', 't0')",
            (str(tmp_path / "live"), "f" * 40),
        )

    assert cli_module._runtime_exec(arguments) == 1
    argv = captured["argv"]
    assert argv[argv.index("--project") + 1] == str(tmp_path / "live")
    assert argv[argv.index("--repo-root") + 1] == str(tmp_path / "stable")
    assert argv[-2:] == ["--interval", "30"]


class TestFullIdentityConfirmation:
    """Sol 70e2dcf7: startup confirms only the complete identity."""

    def activate_at(
        self, database: Database, checkout: Path
    ) -> tuple[RuntimeActivator, object]:
        activator = RuntimeActivator(database, events=EventStore(database))
        activation = activator.activate(
            checkout_root=checkout, binary_path=Path("/bin/hermes")
        )
        return activator, activation

    def test_the_exact_recorded_identity_confirms(
        self, database: Database, tmp_path: Path
    ) -> None:
        checkout = build_checkout(tmp_path, schema=database.schema_version())
        activator, activation = self.activate_at(database, checkout)

        confirmed = activator.confirm_startup(
            checkout_root=checkout,
            binary_path=Path("/bin/hermes"),
            pid=1,
        )

        assert confirmed is not None
        assert confirmed.activation_id == activation.activation_id  # type: ignore[attr-defined]

    def test_an_advanced_head_at_the_same_path_fails_confirmation(
        self, database: Database, tmp_path: Path
    ) -> None:
        """The live worktree is mutable: changed code at the same path
        must never confirm an obsolete generation."""

        checkout = build_checkout(tmp_path, schema=database.schema_version())
        activator, _activation = self.activate_at(database, checkout)
        (checkout / "pyproject.toml").write_text("[project]\nname='y'\n")
        git(checkout, "add", "-A")
        git(checkout, "commit", "-qm", "advance")

        confirmed = activator.confirm_startup(
            checkout_root=checkout,
            binary_path=Path("/bin/hermes"),
            pid=2,
        )

        assert confirmed is None

    def test_a_changed_binary_path_fails_confirmation(
        self, database: Database, tmp_path: Path
    ) -> None:
        checkout = build_checkout(tmp_path, schema=database.schema_version())
        activator, _activation = self.activate_at(database, checkout)

        assert (
            activator.confirm_startup(
                checkout_root=checkout,
                binary_path=Path("/somewhere/else"),
                pid=3,
            )
            is None
        )

    def test_a_changed_database_schema_fails_confirmation(
        self, database: Database, tmp_path: Path
    ) -> None:
        checkout = build_checkout(tmp_path, schema=database.schema_version())
        activator, _activation = self.activate_at(database, checkout)
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO schema_migrations(version, applied_at) "
                "VALUES (9999, CURRENT_TIMESTAMP)"
            )

        assert (
            activator.confirm_startup(
                checkout_root=checkout,
                binary_path=Path("/bin/hermes"),
                pid=4,
            )
            is None
        )


class TestImmutableArtifacts:
    """Sol 70e2dcf7: activations run immutable exported artifacts."""

    def test_activate_artifact_exports_and_marks_the_exact_commit(
        self, database: Database, tmp_path: Path
    ) -> None:
        from hermes_orchestrator.activation import materialize_artifact

        checkout = build_checkout(tmp_path, schema=database.schema_version())
        state_dir = tmp_path / "state"
        activator = RuntimeActivator(database, events=EventStore(database))

        activation = activator.activate_artifact(
            source_checkout=checkout, state_dir=state_dir
        )

        artifact = Path(activation.checkout_root)
        assert artifact == state_dir / "runtimes" / activation.git_sha
        assert (artifact / "RUNTIME_SHA").read_text().strip() == (
            activation.git_sha
        )
        assert (artifact / "pyproject.toml").exists()
        assert activation.binary_path == str(
            artifact / ".venv" / "bin" / "hermes-orchestrator"
        )
        # Materialization is idempotent for the same commit.
        again = materialize_artifact(
            state_dir=state_dir,
            checkout_root=checkout,
            git_sha=activation.git_sha,
        )
        assert again == artifact

    def test_rollback_survives_the_source_worktree_moving(
        self, database: Database, tmp_path: Path
    ) -> None:
        import shutil

        checkout = build_checkout(tmp_path, schema=database.schema_version())
        state_dir = tmp_path / "state"
        activator = RuntimeActivator(database, events=EventStore(database))
        prior = activator.activate_artifact(
            source_checkout=checkout, state_dir=state_dir
        )
        (checkout / "pyproject.toml").write_text("[project]\nname='y'\n")
        git(checkout, "add", "-A")
        git(checkout, "commit", "-qm", "advance")
        activator.activate_artifact(
            source_checkout=checkout, state_dir=state_dir
        )
        shutil.rmtree(checkout)  # the disposable worktree disappears

        restored = activator.reactivate(prior)

        assert restored.checkout_root == prior.checkout_root
        assert restored.git_sha == prior.git_sha
        current = activator.current()
        assert current is not None
        assert current.activation_id == restored.activation_id

    def test_a_schema_advancing_rollback_fails_closed(
        self, database: Database, tmp_path: Path
    ) -> None:
        checkout = build_checkout(tmp_path, schema=database.schema_version())
        state_dir = tmp_path / "state"
        activator = RuntimeActivator(database, events=EventStore(database))
        prior = activator.activate_artifact(
            source_checkout=checkout, state_dir=state_dir
        )
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO schema_migrations(version, applied_at) "
                "VALUES (9999, CURRENT_TIMESTAMP)"
            )

        with pytest.raises(ActivationRefused, match="database snapshot"):
            activator.reactivate(prior)

    def test_a_schema_advancing_target_is_refused_before_migration(
        self, database: Database, tmp_path: Path
    ) -> None:
        """A target carrying migrations beyond the live schema is
        refused before anything irreversible; the journal records the
        refusal and no process is touched."""

        from hermes_orchestrator.activation import ActivationApplier

        checkout = build_checkout(
            tmp_path, schema=database.schema_version() + 3
        )
        state_dir = tmp_path / "state"
        kicks: list[int] = []
        activator = RuntimeActivator(database, events=EventStore(database))
        applier = ActivationApplier(
            activator,
            database,
            kickstart=lambda: kicks.append(1),
            sleep=lambda _s: None,
            verify_timeout_seconds=0.0,
        )

        with pytest.raises(ActivationRefused, match="mismatched"):
            applier.apply(
                checkout_root=checkout, artifact_state_dir=state_dir
            )

        assert kicks == []
        state = database.scalar(
            "SELECT state FROM activation_applies "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1"
        )
        assert str(state) == "refused"

    def test_a_failed_artifact_target_restores_the_prior_artifact(
        self, database: Database, tmp_path: Path
    ) -> None:
        """Distinct target failure causes a supervised restart with an
        exact prior-identity proof before rollback is recorded."""

        from hermes_orchestrator.activation import ActivationApplier

        checkout = build_checkout(tmp_path, schema=database.schema_version())
        state_dir = tmp_path / "state"
        activator = RuntimeActivator(database, events=EventStore(database))
        prior = activator.activate_artifact(
            source_checkout=checkout, state_dir=state_dir
        )
        (checkout / "pyproject.toml").write_text("[project]\nname='y'\n")
        git(checkout, "add", "-A")
        git(checkout, "commit", "-qm", "advance")
        kicks: list[int] = []

        def kickstart() -> None:
            kicks.append(1)
            if len(kicks) == 1:
                return  # the distinct new artifact never comes up
            activator.confirm_startup(
                checkout_root=Path(prior.checkout_root),
                binary_path=Path(prior.binary_path),
                pid=555,
            )

        applier = ActivationApplier(
            activator,
            database,
            kickstart=kickstart,
            sleep=lambda _s: None,
            verify_timeout_seconds=0.0,
        )

        report = applier.apply(
            checkout_root=checkout, artifact_state_dir=state_dir
        )

        assert report.state == "rolled_back"
        assert len(kicks) == 2
        current = activator.current()
        assert current is not None
        assert current.checkout_root == prior.checkout_root
        assert current.git_sha == prior.git_sha


def test_activation_maintains_the_derived_active_pointer(
    database: Database, tmp_path: Path
) -> None:
    """The shell bootstrap reads runtimes/ACTIVE; every artifact
    activation and rollback refreshes it atomically."""

    checkout = build_checkout(tmp_path, schema=database.schema_version())
    state_dir = tmp_path / "state"
    activator = RuntimeActivator(database, events=EventStore(database))

    prior = activator.activate_artifact(
        source_checkout=checkout, state_dir=state_dir
    )
    pointer = state_dir / "runtimes" / "ACTIVE"
    assert pointer.read_text().strip() == prior.checkout_root

    (checkout / "pyproject.toml").write_text("[project]\nname='y'\n")
    git(checkout, "add", "-A")
    git(checkout, "commit", "-qm", "advance")
    fresh = activator.activate_artifact(
        source_checkout=checkout, state_dir=state_dir
    )
    assert pointer.read_text().strip() == fresh.checkout_root

    activator.reactivate(prior)
    assert pointer.read_text().strip() == prior.checkout_root


class TestFleetCoherence:
    """Sol 3be95878: ledger, pointer, and supervised fleet move as one."""

    def test_a_ledger_failure_leaves_the_pointer_on_the_prior_artifact(
        self, database: Database, tmp_path: Path
    ) -> None:
        """An injected activation-ledger failure after artifact
        preparation must leave ACTIVE naming the prior artifact, so no
        service can ever resolve the uncommitted target."""

        checkout = build_checkout(tmp_path, schema=database.schema_version())
        state_dir = tmp_path / "state"
        events = EventStore(database)
        activator = RuntimeActivator(database, events=events)
        prior = activator.activate_artifact(
            source_checkout=checkout, state_dir=state_dir
        )
        (checkout / "pyproject.toml").write_text("[project]\nname='y'\n")
        git(checkout, "add", "-A")
        git(checkout, "commit", "-qm", "advance")

        class ExplodingEvents:
            def append(self, connection, event):  # type: ignore[no-untyped-def]
                if event.event_type == "runtime_activation.active":
                    raise RuntimeError("ledger write refused")
                return events.append(connection, event)

        broken = RuntimeActivator(
            database,
            events=ExplodingEvents(),  # type: ignore[arg-type]
        )

        with pytest.raises(RuntimeError, match="ledger write refused"):
            broken.activate_artifact(
                source_checkout=checkout, state_dir=state_dir
            )

        pointer = state_dir / "runtimes" / "ACTIVE"
        assert pointer.read_text().strip() == prior.checkout_root
        current = activator.current()
        assert current is not None
        assert current.activation_id == prior.activation_id

    def test_a_pointer_publish_failure_restores_the_prior_activation(
        self,
        database: Database,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        checkout = build_checkout(tmp_path, schema=database.schema_version())
        state_dir = tmp_path / "state"
        activator = RuntimeActivator(database, events=EventStore(database))
        prior = activator.activate_artifact(
            source_checkout=checkout, state_dir=state_dir
        )
        (checkout / "pyproject.toml").write_text("[project]\nname='y'\n")
        git(checkout, "add", "-A")
        git(checkout, "commit", "-qm", "advance")

        def refuse_pointer(_artifact: Path) -> None:
            raise OSError("disk said no")

        monkeypatch.setattr(
            RuntimeActivator, "_write_active_pointer", staticmethod(refuse_pointer)
        )
        with pytest.raises(ActivationRefused, match="prior activation was restored"):
            activator.activate_artifact(
                source_checkout=checkout, state_dir=state_dir
            )
        monkeypatch.undo()

        current = activator.current()
        assert current is not None
        assert current.activation_id == prior.activation_id
        pointer = state_dir / "runtimes" / "ACTIVE"
        assert pointer.read_text().strip() == prior.checkout_root
        failed = database.scalar(
            "SELECT COUNT(*) FROM runtime_activations "
            "WHERE state = 'failed' AND reason LIKE '%pointer%'"
        )
        assert failed == 1

    @staticmethod
    def fleet_applier(
        database: Database, activator: RuntimeActivator, kickstart
    ):
        from hermes_orchestrator.activation import ActivationApplier

        return ActivationApplier(
            activator,
            database,
            kickstart=kickstart,
            sleep=lambda _s: None,
            verify_timeout_seconds=0.0,
            verify_services=("daemon", "console"),
        )

    def confirm_both(
        self, activator: RuntimeActivator, root: Path, binary: Path
    ) -> None:
        activator.confirm_startup(
            checkout_root=root, binary_path=binary, pid=100, service="daemon"
        )
        activator.confirm_startup(
            checkout_root=root,
            binary_path=binary,
            pid=101,
            service="console",
            bootstrap=False,
        )

    def test_a_verified_apply_requires_both_services_on_the_target(
        self, database: Database, tmp_path: Path
    ) -> None:
        checkout = build_checkout(tmp_path, schema=database.schema_version())
        state_dir = tmp_path / "state"
        activator = RuntimeActivator(database, events=EventStore(database))

        def kickstart() -> None:
            active = activator.current()
            assert active is not None
            self.confirm_both(
                activator,
                Path(active.checkout_root),
                Path(active.binary_path),
            )

        applier = self.fleet_applier(database, activator, kickstart)
        report = applier.apply(
            checkout_root=checkout, artifact_state_dir=state_dir
        )

        assert report.state == "verified"

    def test_an_operations_failure_rolls_both_services_back(
        self, database: Database, tmp_path: Path
    ) -> None:
        """The daemon proves the target but the console never does:
        the whole fleet rolls back to the prior activation, and the
        rollback itself is verified on both services."""

        checkout = build_checkout(tmp_path, schema=database.schema_version())
        state_dir = tmp_path / "state"
        activator = RuntimeActivator(database, events=EventStore(database))
        prior = activator.activate_artifact(
            source_checkout=checkout, state_dir=state_dir
        )
        (checkout / "pyproject.toml").write_text("[project]\nname='y'\n")
        git(checkout, "add", "-A")
        git(checkout, "commit", "-qm", "advance")
        kicks: list[int] = []

        def kickstart() -> None:
            kicks.append(1)
            active = activator.current()
            assert active is not None
            root = Path(active.checkout_root)
            binary = Path(active.binary_path)
            if len(kicks) == 1:
                # Only the daemon comes up on the target generation.
                activator.confirm_startup(
                    checkout_root=root,
                    binary_path=binary,
                    pid=200,
                    service="daemon",
                )
                return
            self.confirm_both(activator, root, binary)

        applier = self.fleet_applier(database, activator, kickstart)
        report = applier.apply(
            checkout_root=checkout, artifact_state_dir=state_dir
        )

        assert report.state == "rolled_back"
        assert len(kicks) == 2
        current = activator.current()
        assert current is not None
        assert current.git_sha == prior.git_sha

    def test_an_unverifiable_fleet_rollback_is_ambiguous(
        self, database: Database, tmp_path: Path
    ) -> None:
        from hermes_orchestrator.activation import ApplyFailed

        checkout = build_checkout(tmp_path, schema=database.schema_version())
        state_dir = tmp_path / "state"
        activator = RuntimeActivator(database, events=EventStore(database))
        activator.activate_artifact(
            source_checkout=checkout, state_dir=state_dir
        )
        (checkout / "pyproject.toml").write_text("[project]\nname='y'\n")
        git(checkout, "add", "-A")
        git(checkout, "commit", "-qm", "advance")

        def kickstart() -> None:
            active = activator.current()
            assert active is not None
            # The console never reports on either generation.
            activator.confirm_startup(
                checkout_root=Path(active.checkout_root),
                binary_path=Path(active.binary_path),
                pid=300,
                service="daemon",
            )

        applier = self.fleet_applier(database, activator, kickstart)
        with pytest.raises(ApplyFailed, match="ambiguous"):
            applier.apply(
                checkout_root=checkout, artifact_state_dir=state_dir
            )

    def test_serve_console_refuses_a_mismatched_identity(
        self, database: Database, tmp_path: Path, capsys
    ) -> None:
        import argparse
        from types import SimpleNamespace

        from hermes_orchestrator import cli as cli_module

        checkout = build_checkout(tmp_path, schema=database.schema_version())
        activator = RuntimeActivator(database, events=EventStore(database))
        activator.activate(
            checkout_root=checkout, binary_path=Path("/bin/other")
        )
        arguments = argparse.Namespace(host="127.0.0.1", port=8787)
        settings = SimpleNamespace(
            state_dir=tmp_path,
            projects={},
            repo_root=tmp_path,
        )

        code = cli_module._serve_console(arguments, settings)  # type: ignore[arg-type]

        assert code == 1
        assert "does not match the active activation" in (
            capsys.readouterr().err
        )
        journaled = database.execute(
            "SELECT payload_json FROM events "
            "WHERE event_type = 'console.started'"
        ).fetchall()
        assert len(journaled) == 1


class TestFleetRestartExceptions:
    """Sol ff45468d: every partial fleet-restart failure enters the
    same journaled rollback protocol as a missing health proof."""

    @staticmethod
    def setup_fleet(
        database: Database, tmp_path: Path
    ) -> tuple[RuntimeActivator, Path, Path, object]:
        activator = RuntimeActivator(database, events=EventStore(database))
        checkout = build_checkout(tmp_path, schema=database.schema_version())
        state_dir = tmp_path / "state"
        prior = activator.activate_artifact(
            source_checkout=checkout, state_dir=state_dir
        )
        (checkout / "pyproject.toml").write_text("[project]\nname='y'\n")
        git(checkout, "add", "-A")
        git(checkout, "commit", "-qm", "advance")
        return activator, checkout, state_dir, prior

    @staticmethod
    def confirm_fleet(activator: RuntimeActivator) -> None:
        active = activator.current()
        assert active is not None
        for pid, service in ((900, "daemon"), (901, "console")):
            activator.confirm_startup(
                checkout_root=Path(active.checkout_root),
                binary_path=Path(active.binary_path),
                pid=pid,
                service=service,
                bootstrap=False,
            )

    def applier(self, database, activator, kickstart):
        return TestFleetCoherence.fleet_applier(
            database, activator, kickstart
        )

    def last_state(self, database: Database) -> str:
        return str(
            database.scalar(
                "SELECT state FROM activation_applies "
                "ORDER BY created_at DESC, rowid DESC LIMIT 1"
            )
        )

    def test_a_partial_target_restart_rolls_the_fleet_back(
        self, database: Database, tmp_path: Path
    ) -> None:
        """The daemon kickstarted but the operations restart raised:
        both services restart onto the prior generation and the apply
        records rolled_back."""

        activator, checkout, state_dir, prior = self.setup_fleet(
            database, tmp_path
        )
        kicks: list[int] = []

        def kickstart() -> None:
            kicks.append(1)
            if len(kicks) == 1:
                active = activator.current()
                assert active is not None
                activator.confirm_startup(
                    checkout_root=Path(active.checkout_root),
                    binary_path=Path(active.binary_path),
                    pid=800,
                    service="daemon",
                    bootstrap=False,
                )
                raise RuntimeError("operations kickstart refused")
            self.confirm_fleet(activator)

        report = self.applier(database, activator, kickstart).apply(
            checkout_root=checkout, artifact_state_dir=state_dir
        )

        assert report.state == "rolled_back"
        assert len(kicks) == 2
        current = activator.current()
        assert current is not None
        assert current.git_sha == prior.git_sha  # type: ignore[attr-defined]
        assert self.last_state(database) == "rolled_back"

    def test_a_total_target_restart_failure_restores_the_prior(
        self, database: Database, tmp_path: Path
    ) -> None:
        activator, checkout, state_dir, prior = self.setup_fleet(
            database, tmp_path
        )
        kicks: list[int] = []

        def kickstart() -> None:
            kicks.append(1)
            if len(kicks) == 1:
                raise RuntimeError("launchctl refused entirely")
            self.confirm_fleet(activator)

        report = self.applier(database, activator, kickstart).apply(
            checkout_root=checkout, artifact_state_dir=state_dir
        )

        assert report.state == "rolled_back"
        current = activator.current()
        assert current is not None
        assert current.git_sha == prior.git_sha  # type: ignore[attr-defined]

    def test_a_failed_rollback_restart_is_ambiguous(
        self, database: Database, tmp_path: Path
    ) -> None:
        from hermes_orchestrator.activation import ApplyFailed

        activator, checkout, state_dir, _prior = self.setup_fleet(
            database, tmp_path
        )

        def kickstart() -> None:
            raise RuntimeError("launchctl always refuses")

        with pytest.raises(ApplyFailed, match="ambiguous"):
            self.applier(database, activator, kickstart).apply(
                checkout_root=checkout, artifact_state_dir=state_dir
            )

        assert self.last_state(database) == "ambiguous"

    def test_incomplete_prior_proofs_after_target_failure_are_ambiguous(
        self, database: Database, tmp_path: Path
    ) -> None:
        from hermes_orchestrator.activation import ApplyFailed

        activator, checkout, state_dir, _prior = self.setup_fleet(
            database, tmp_path
        )
        kicks: list[int] = []

        def kickstart() -> None:
            kicks.append(1)
            if len(kicks) == 1:
                raise RuntimeError("target restart refused")
            active = activator.current()
            assert active is not None
            # Only the daemon proves the restored generation.
            activator.confirm_startup(
                checkout_root=Path(active.checkout_root),
                binary_path=Path(active.binary_path),
                pid=810,
                service="daemon",
                bootstrap=False,
            )

        with pytest.raises(ApplyFailed, match="ambiguous"):
            self.applier(database, activator, kickstart).apply(
                checkout_root=checkout, artifact_state_dir=state_dir
            )

        assert self.last_state(database) == "ambiguous"

    def test_every_post_activation_exception_ends_terminal(
        self, database: Database, tmp_path: Path, monkeypatch
    ) -> None:
        """Even an unexpected internal failure after activation leaves
        activation_applies terminal, never activated or restarted."""

        from hermes_orchestrator.activation import ApplyFailed

        activator, checkout, state_dir, _prior = self.setup_fleet(
            database, tmp_path
        )
        applier = self.applier(database, activator, lambda: None)
        monkeypatch.setattr(
            applier,
            "_await_services",
            lambda *_a, **_k: (_ for _ in ()).throw(
                OSError("verification substrate failed")
            ),
        )

        with pytest.raises(ApplyFailed, match="ambiguous"):
            applier.apply(
                checkout_root=checkout, artifact_state_dir=state_dir
            )

        assert self.last_state(database) == "ambiguous"
