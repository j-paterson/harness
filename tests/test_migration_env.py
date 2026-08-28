from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from hermes_orchestrator.migration_env import (
    DEFAULT_IMAGE,
    ISOLATION_MARKER,
    EnvRunResult,
    LoopbackDsn,
    MigrationEnvConfig,
    MigrationEnvRefusal,
    disposable_pair,
    is_disposable_name,
    load_provision_record,
    loopback_proof,
    mark_isolated_worktree,
    plan_provision,
    plan_teardown,
    provision_disposable,
    require_isolated_worktree,
    teardown_disposable,
    write_provision_record,
)

SECRET = "sekret-not-for-logs"


def loopback_url(
    *,
    host: str = "127.0.0.1",
    port: int = 5439,
    database: str = "jo_local_fable_infra_189_source",
    password: str = "postgres",
) -> str:
    return f"postgresql://postgres:{password}@{host}:{port}/{database}"


class TestLoopbackDsn:
    def test_parses_loopback_url_fields(self) -> None:
        dsn = LoopbackDsn.parse(loopback_url())

        assert dsn.host == "127.0.0.1"
        assert dsn.port == 5439
        assert dsn.database == "jo_local_fable_infra_189_source"
        assert dsn.user == "postgres"

    @pytest.mark.parametrize(
        "host",
        [
            "db.example.neon.tech",
            "prod-cluster.us-east-1.rds.amazonaws.com",
            "10.0.0.5",
            "192.168.1.20",
            "host.docker.internal",
        ],
    )
    def test_rejects_every_non_loopback_host(self, host: str) -> None:
        with pytest.raises(MigrationEnvRefusal):
            LoopbackDsn.parse(loopback_url(host=host))

    def test_rejects_non_postgres_scheme_and_missing_database(self) -> None:
        with pytest.raises(MigrationEnvRefusal):
            LoopbackDsn.parse("mysql://postgres:postgres@127.0.0.1:5439/x")
        with pytest.raises(MigrationEnvRefusal):
            LoopbackDsn.parse("postgresql://postgres:postgres@127.0.0.1:5439/")

    def test_password_never_appears_in_repr_or_refusals(self) -> None:
        dsn = LoopbackDsn.parse(loopback_url(password=SECRET))
        assert SECRET not in repr(dsn)
        assert SECRET not in str(dsn)

        with pytest.raises(MigrationEnvRefusal) as refusal:
            LoopbackDsn.parse(
                loopback_url(host="db.example.neon.tech", password=SECRET)
            )
        assert SECRET not in str(refusal.value)

    def test_url_round_trips_for_command_injection_only(self) -> None:
        raw = loopback_url()
        assert LoopbackDsn.parse(raw).url == raw


class TestDisposableNaming:
    def test_pair_follows_the_target_repos_guarded_convention(self) -> None:
        source, target = disposable_pair("INFRA-189")

        assert source == "jo_local_fable_infra_189_source"
        assert target == "jo_local_fable_infra_189_target"
        assert is_disposable_name(source)
        assert is_disposable_name(target)

    @pytest.mark.parametrize(
        "name",
        [
            "jo_local",
            "test_jo_local",
            "jo_local_main",
            "jo_local_fable_x",
            "jo_prod",
            "jo_staging",
            "jo_preview",
            "cre_local",
            "",
        ],
    )
    def test_shared_or_unsuffixed_names_are_never_disposable(
        self, name: str
    ) -> None:
        assert not is_disposable_name(name)

    def test_slug_rejects_empty_and_traversal_input(self) -> None:
        with pytest.raises(MigrationEnvRefusal):
            disposable_pair("")
        with pytest.raises(MigrationEnvRefusal):
            disposable_pair("   ")


def config(tmp_path: Path) -> MigrationEnvConfig:
    return MigrationEnvConfig(
        repo_path=tmp_path / "worktree",
        slug="infra-189",
        container="jo-fable-migration-db",
        port=5439,
    )


class TestProvisionPlan:
    def test_container_publishes_only_a_loopback_port(
        self, tmp_path: Path
    ) -> None:
        steps = plan_provision(config(tmp_path))

        run = next(s for s in steps if s.code == "container_run")
        publish_at = run.argv.index("-p")
        assert run.argv[publish_at + 1] == "127.0.0.1:5439:5432"
        assert DEFAULT_IMAGE in run.argv
        assert "0.0.0.0" not in " ".join(run.argv)

    def test_plan_creates_both_disposable_databases_idempotently(
        self, tmp_path: Path
    ) -> None:
        steps = plan_provision(config(tmp_path))
        creates = [s for s in steps if s.code.startswith("create_database")]

        joined = [" ".join(s.argv) for s in creates]
        assert any("jo_local_fable_infra_189_source" in c for c in joined)
        assert any("jo_local_fable_infra_189_target" in c for c in joined)
        for step in creates:
            # Idempotent single step: check pg_database, then create
            # only when absent. psql -c cannot process \gexec, so the
            # composition runs inside the container's shell.
            assert step.argv[3:5] == ("sh", "-c")
            assert "pg_database" in step.argv[5]
            assert "CREATE DATABASE" in step.argv[5]

    def test_readiness_probe_retries_inside_the_container(
        self, tmp_path: Path
    ) -> None:
        steps = plan_provision(config(tmp_path))
        ready = next(s for s in steps if s.code == "postgres_ready")

        # First-boot initialization takes a few seconds; a single-shot
        # probe would fail closed spuriously.
        assert "sh" in ready.argv
        assert "pg_isready" in " ".join(ready.argv)
        assert "sleep" in " ".join(ready.argv)

    def test_teardown_plan_validates_identity_before_any_removal(
        self, tmp_path: Path
    ) -> None:
        steps = plan_teardown(config(tmp_path))

        assert [s.code for s in steps] == [
            "container_identity",
            "container_remove",
        ]
        inspect_argv = " ".join(steps[0].argv)
        assert "jo-fable-migration-db" in inspect_argv
        # Removal in the description targets the validated immutable
        # identity, never the raw name.
        assert "<validated-owned-container-id>" in steps[1].argv




OWNED_ID = "c" * 64
FOREIGN_ID = "d" * 64
CONTAINER = "jo-fable-migration-db"


def identity_line(
    *,
    container_id: str = OWNED_ID,
    image: str = "postgres:17-alpine",
    ownership: str = "fable-migration-env",
    slug: str = "infra_189",
    running: str = "true",
) -> EnvRunResult:
    return EnvRunResult(
        0,
        f"{container_id}\t{image}\t{ownership}\t{slug}\t{running}\n",
        "",
    )


def seed_record(
    cfg: MigrationEnvConfig, container_id: str = OWNED_ID
) -> None:
    cfg.repo_path.mkdir(parents=True, exist_ok=True)
    write_provision_record(cfg, container_id)


class ScriptedEnvRunner:
    """Token-matched scripted runner recording every call."""

    def __init__(self) -> None:
        self.scripts: list[tuple[str, EnvRunResult]] = []
        self.calls: list[tuple[str, ...]] = []

    def script(self, token: str, result: EnvRunResult) -> None:
        self.scripts.append((token, result))

    def run(
        self,
        argv: tuple[str, ...],
        *,
        timeout: float,
        env: dict[str, str] | None = None,
        cwd: Path | None = None,
    ) -> EnvRunResult:
        self.calls.append(tuple(argv))
        joined = " ".join(argv)
        for token, result in self.scripts:
            if token in joined:
                return result
        return EnvRunResult(returncode=0, stdout="", stderr="")

    def commands(self) -> list[str]:
        return [" ".join(argv) for argv in self.calls]


def owned_runner() -> ScriptedEnvRunner:
    runner = ScriptedEnvRunner()
    runner.script("docker inspect", identity_line())
    runner.script(
        "docker port", EnvRunResult(0, "5432/tcp -> 127.0.0.1:5439\n", "")
    )
    return runner


class TestProvisionOwnership:
    def test_creation_captures_the_id_from_run_and_binds_the_record(
        self, tmp_path: Path
    ) -> None:
        cfg = config(tmp_path)
        cfg.repo_path.mkdir(parents=True)
        runner = ScriptedEnvRunner()
        # Token order matters: run and port before the broad id token,
        # and the bare container-name token last (it only ever matches
        # the initial by-name discovery inspect).
        runner.script("docker run", EnvRunResult(0, OWNED_ID + "\n", ""))
        runner.script(
            "docker port",
            EnvRunResult(0, "5432/tcp -> 127.0.0.1:5439\n", ""),
        )
        runner.script(OWNED_ID[:12], identity_line())
        runner.script(CONTAINER, EnvRunResult(1, "", "No such object"))

        report = provision_disposable(cfg, runner)

        assert report.completed is True
        assert report.container_id == OWNED_ID
        record = load_provision_record(cfg)
        assert record is not None
        assert record.container_id == OWNED_ID
        assert record.slug == "infra_189"
        run_command = next(
            c for c in runner.commands() if c.startswith("docker run")
        )
        assert "hermes.disposable=fable-migration-env" in run_command
        assert "hermes.disposable.slug=infra_189" in run_command
        assert "127.0.0.1:5439:5432" in run_command
        assert "POSTGRES_PASSWORD=" not in run_command

    def test_every_post_capture_operation_targets_the_immutable_id(
        self, tmp_path: Path
    ) -> None:
        cfg = config(tmp_path)
        seed_record(cfg)
        runner = owned_runner()

        report = provision_disposable(cfg, runner)

        assert report.completed is True
        post_capture = [
            c
            for c in runner.commands()
            if c.startswith(("docker exec", "docker port"))
        ]
        assert post_capture, "expected id-addressed operations"
        for command in post_capture:
            assert OWNED_ID in command
            assert CONTAINER not in command

    def test_recorded_owned_container_is_reused_by_id(
        self, tmp_path: Path
    ) -> None:
        cfg = config(tmp_path)
        seed_record(cfg)
        runner = owned_runner()

        report = provision_disposable(cfg, runner)

        assert report.completed is True
        assert report.container_id == OWNED_ID
        joined = " ".join(runner.commands())
        assert "docker run" not in joined
        inspects = [
            c for c in runner.commands() if c.startswith("docker inspect")
        ]
        assert all(OWNED_ID in c for c in inspects)

    def test_unrecorded_same_name_container_is_refused(
        self, tmp_path: Path
    ) -> None:
        # Even an owned-looking container is not this environment's
        # without a durable provision record binding its identity.
        cfg = config(tmp_path)
        cfg.repo_path.mkdir(parents=True)
        runner = ScriptedEnvRunner()
        runner.script("docker inspect", identity_line())

        report = provision_disposable(cfg, runner)

        assert report.completed is False
        assert report.refusal_code == "container_conflict"
        joined = " ".join(runner.commands())
        assert "docker start" not in joined
        assert "docker exec" not in joined
        assert "docker run" not in joined

    def test_replacement_under_the_name_never_receives_exec(
        self, tmp_path: Path
    ) -> None:
        # The recorded container is gone; a replacement squats the
        # name. Provision must refuse without touching the replacement.
        cfg = config(tmp_path)
        seed_record(cfg)
        runner = ScriptedEnvRunner()
        runner.script(OWNED_ID[:12], EnvRunResult(1, "", "No such object"))
        runner.script(
            "docker inspect",
            identity_line(container_id=FOREIGN_ID),
        )

        report = provision_disposable(cfg, runner)

        assert report.completed is False
        assert report.refusal_code == "container_conflict"
        joined = " ".join(runner.commands())
        assert "docker exec" not in joined
        assert "docker start" not in joined

    def test_stale_record_with_free_name_recreates_and_rebinds(
        self, tmp_path: Path
    ) -> None:
        cfg = config(tmp_path)
        seed_record(cfg, container_id=FOREIGN_ID)
        runner = ScriptedEnvRunner()
        runner.script(FOREIGN_ID[:12], EnvRunResult(1, "", "No such"))
        runner.script("docker run", EnvRunResult(0, OWNED_ID + "\n", ""))
        runner.script(
            "docker port",
            EnvRunResult(0, "5432/tcp -> 127.0.0.1:5439\n", ""),
        )
        runner.script(OWNED_ID[:12], identity_line())
        runner.script(CONTAINER, EnvRunResult(1, "", "No such object"))

        report = provision_disposable(cfg, runner)

        assert report.completed is True
        record = load_provision_record(cfg)
        assert record is not None and record.container_id == OWNED_ID

    def test_wildcard_binding_refused_before_database_creation(
        self, tmp_path: Path
    ) -> None:
        cfg = config(tmp_path)
        seed_record(cfg)
        runner = ScriptedEnvRunner()
        runner.script("docker inspect", identity_line())
        runner.script(
            "docker port",
            EnvRunResult(0, "5432/tcp -> 0.0.0.0:5439\n", ""),
        )

        report = provision_disposable(cfg, runner)

        assert report.completed is False
        assert report.refusal_code == "loopback_proof"
        joined = " ".join(runner.commands())
        assert "pg_isready" not in joined
        assert "CREATE DATABASE" not in joined

    def test_failed_docker_run_is_a_hard_refusal(
        self, tmp_path: Path
    ) -> None:
        cfg = config(tmp_path)
        cfg.repo_path.mkdir(parents=True)
        runner = ScriptedEnvRunner()
        runner.script("docker inspect", EnvRunResult(1, "", "No such"))
        runner.script("docker run", EnvRunResult(125, "", "conflict"))

        report = provision_disposable(cfg, runner)

        assert report.completed is False
        assert report.refusal_code == "container_run"
        assert "CREATE DATABASE" not in " ".join(runner.commands())

    def test_wrong_slug_label_is_a_cross_environment_refusal(
        self, tmp_path: Path
    ) -> None:
        cfg = config(tmp_path)
        seed_record(cfg)
        runner = ScriptedEnvRunner()
        runner.script(
            "docker inspect", identity_line(slug="infra_190")
        )

        report = provision_disposable(cfg, runner)

        assert report.completed is False
        assert report.refusal_code == "container_ownership"
        assert "docker exec" not in " ".join(runner.commands())


class TestTeardownOwnership:
    def test_missing_record_refuses_before_any_docker_call(
        self, tmp_path: Path
    ) -> None:
        cfg = config(tmp_path)
        cfg.repo_path.mkdir(parents=True)
        runner = ScriptedEnvRunner()

        report = teardown_disposable(cfg, runner)

        assert report.completed is False
        assert report.refusal_code == "provision_record"
        assert runner.commands() == []

    def test_foreign_environment_record_is_refused(
        self, tmp_path: Path
    ) -> None:
        other = MigrationEnvConfig(
            repo_path=tmp_path / "worktree", slug="infra-190"
        )
        seed_record(other)
        cfg = config(tmp_path)
        runner = ScriptedEnvRunner()

        report = teardown_disposable(cfg, runner)

        assert report.completed is False
        assert report.refusal_code == "provision_record"
        assert runner.commands() == []

    @pytest.mark.parametrize(
        "inspect_result,refusal",
        [
            (EnvRunResult(1, "", "No such object"), "container_missing"),
            (identity_line(ownership=""), "container_ownership"),
            (identity_line(ownership="other-label"), "container_ownership"),
            (
                identity_line(image="postgres:16-alpine"),
                "container_ownership",
            ),
            (identity_line(slug="infra_190"), "container_ownership"),
            (
                EnvRunResult(0, identity_line().stdout * 2, ""),
                "container_identity",
            ),
        ],
        ids=[
            "missing",
            "unlabeled",
            "relabeled",
            "foreign-image",
            "foreign-slug",
            "ambiguous",
        ],
    )
    def test_teardown_refuses_everything_unowned_and_never_removes(
        self,
        tmp_path: Path,
        inspect_result: EnvRunResult,
        refusal: str,
    ) -> None:
        cfg = config(tmp_path)
        seed_record(cfg)
        runner = ScriptedEnvRunner()
        runner.script("docker inspect", inspect_result)

        report = teardown_disposable(cfg, runner)

        assert report.completed is False
        assert report.refusal_code == refusal
        assert not any(
            c.startswith("docker rm") for c in runner.commands()
        )

    def test_teardown_removes_by_recorded_id_and_clears_the_record(
        self, tmp_path: Path
    ) -> None:
        cfg = config(tmp_path)
        seed_record(cfg)
        runner = owned_runner()

        report = teardown_disposable(cfg, runner)

        assert report.completed is True
        assert report.container_id == OWNED_ID
        removal = next(
            c for c in runner.commands() if c.startswith("docker rm")
        )
        # Removal targets the exact immutable identity, never the name.
        assert OWNED_ID in removal
        assert CONTAINER not in removal
        inspects = [
            c for c in runner.commands() if c.startswith("docker inspect")
        ]
        assert all(OWNED_ID in c for c in inspects)
        assert load_provision_record(cfg) is None


class TestLoopbackProof:
    def test_only_loopback_bindings_prove_clean(self) -> None:
        good = "5432/tcp -> 127.0.0.1:5439\n"
        assert loopback_proof(good).proven is True

        wild = "5432/tcp -> 0.0.0.0:5439\n5432/tcp -> [::]:5439\n"
        proof = loopback_proof(wild)
        assert proof.proven is False
        assert "0.0.0.0" in proof.evidence

    def test_empty_port_output_is_not_proof(self) -> None:
        assert loopback_proof("").proven is False


def git(*arguments: str, cwd: Path) -> None:
    subprocess.run(
        ["git", *arguments], cwd=cwd, check=True, capture_output=True
    )


def make_linked_worktree(tmp_path: Path) -> tuple[Path, Path]:
    """A real primary checkout plus one genuinely linked worktree."""

    primary = tmp_path / "primary"
    primary.mkdir(parents=True)
    git("init", "-q", cwd=primary)
    git(
        "-c",
        "user.email=test@example.invalid",
        "-c",
        "user.name=test",
        "commit",
        "--allow-empty",
        "-q",
        "-m",
        "seed",
        cwd=primary,
    )
    linked = tmp_path / "linked"
    git("worktree", "add", "--detach", "-q", str(linked), cwd=primary)
    return primary, linked


class TestIsolationMarker:
    def test_primary_checkout_can_never_be_marked_or_gated(
        self, tmp_path: Path
    ) -> None:
        primary, _ = make_linked_worktree(tmp_path)

        with pytest.raises(MigrationEnvRefusal):
            mark_isolated_worktree(primary, slug="infra-189")
        # Even a hand-written marker never validates the primary.
        (primary / ISOLATION_MARKER).write_text("slug=infra_189\n")
        with pytest.raises(MigrationEnvRefusal):
            require_isolated_worktree(primary, slug="infra-189")

    def test_linked_worktree_marks_and_validates_with_matching_slug(
        self, tmp_path: Path
    ) -> None:
        _, linked = make_linked_worktree(tmp_path)

        with pytest.raises(MigrationEnvRefusal):
            require_isolated_worktree(linked, slug="infra-189")

        mark_isolated_worktree(linked, slug="infra-189")
        require_isolated_worktree(linked, slug="infra-189")
        assert (linked / ISOLATION_MARKER).exists()

    def test_wrong_slug_is_rejected(self, tmp_path: Path) -> None:
        _, linked = make_linked_worktree(tmp_path)
        mark_isolated_worktree(linked, slug="infra-189")

        with pytest.raises(MigrationEnvRefusal):
            require_isolated_worktree(linked, slug="other-issue")

    def test_malformed_and_handcrafted_markers_are_rejected(
        self, tmp_path: Path
    ) -> None:
        _, linked = make_linked_worktree(tmp_path)

        (linked / ISOLATION_MARKER).write_text("not a marker at all")
        with pytest.raises(MigrationEnvRefusal):
            require_isolated_worktree(linked, slug="infra-189")

        # A handcrafted marker naming the right slug but no repository
        # identity is stale/foreign evidence, never proof.
        (linked / ISOLATION_MARKER).write_text("slug=infra_189\n")
        with pytest.raises(MigrationEnvRefusal):
            require_isolated_worktree(linked, slug="infra-189")

    def test_foreign_repository_marker_is_rejected(
        self, tmp_path: Path
    ) -> None:
        _, linked_a = make_linked_worktree(tmp_path / "a")
        _, linked_b = make_linked_worktree(tmp_path / "b")
        mark_isolated_worktree(linked_a, slug="infra-189")

        # Copying the owned marker into another repository's worktree
        # must not transfer the isolation proof.
        (linked_b / ISOLATION_MARKER).write_text(
            (linked_a / ISOLATION_MARKER).read_text()
        )
        with pytest.raises(MigrationEnvRefusal):
            require_isolated_worktree(linked_b, slug="infra-189")

    def test_plain_directories_are_never_isolated(
        self, tmp_path: Path
    ) -> None:
        plain = tmp_path / "plain"
        plain.mkdir()

        with pytest.raises(MigrationEnvRefusal):
            mark_isolated_worktree(plain, slug="infra-189")
        (plain / ISOLATION_MARKER).write_text("slug=infra_189\n")
        with pytest.raises(MigrationEnvRefusal):
            require_isolated_worktree(plain, slug="infra-189")

    def test_marker_never_validates_a_missing_directory(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(MigrationEnvRefusal):
            require_isolated_worktree(tmp_path / "missing", slug="x")
