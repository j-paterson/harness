from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from hermes_orchestrator.migration_env import (
    EnvRunResult,
    MigrationEnvConfig,
    mark_isolated_worktree,
)
from hermes_orchestrator.migration_gate import (
    GateCommands,
    MigrationGate,
    load_verdict,
    tree_digest,
)

SECRET_URL_MARKER = "postgres:postgres@"

HEAD40 = "3f2c" * 10

MIGRATIONS = (
    "20250327092823_add_loan_config_model",
    "20250421195943_add_bridge_loan_config_model",
    "20260818150000_retire_sizer_version_eligibility_rules",
)


def build_repo(tmp_path: Path) -> Path:
    from hermes_orchestrator.migration_env import write_provision_record
    from tests.test_migration_env import make_linked_worktree

    _, repo = make_linked_worktree(tmp_path)
    write_provision_record(
        MigrationEnvConfig(repo_path=repo, slug="infra-189"), GATE_ID
    )
    migrations = repo / "packages/database/prisma/migrations"
    migrations.mkdir(parents=True)
    for name in MIGRATIONS:
        (migrations / name).mkdir()
        (migrations / name / "migration.sql").write_text("SELECT 1;\n")
    (migrations / "migration_lock.toml").write_text('provider = "postgresql"\n')
    fixtures = repo / "packages/database/scripts/fixtures"
    fixtures.mkdir(parents=True)
    (fixtures / "generated.json").write_text('{"stable": true}\n')
    mark_isolated_worktree(repo, slug="infra-189")
    return repo


def commands() -> GateCommands:
    return GateCommands(
        migrate_deploy=(
            "pnpm", "--filter", "@jo/database",
            "exec", "prisma", "migrate", "deploy",
        ),
        generator=(
            "pnpm", "--filter", "@jo/database",
            "run", "db:generate-starters",
        ),
        generated_paths=("packages/database/scripts/fixtures",),
        corpus=(
            (
                "corpus_seed",
                ("pnpm", "--filter", "@jo/database", "run", "db:seed"),
            ),
        ),
        historical=(
            (
                "loan_config_v3",
                (
                    "pnpm", "--filter", "@jo/database",
                    "run", "db:migrate-loan-config-v3",
                ),
            ),
        ),
    )


class ScriptedRunner:
    """Returns scripted results by matching a token in the argv."""

    def __init__(self) -> None:
        self.scripts: list[tuple[str, EnvRunResult]] = []
        self.queues: list[tuple[str, list[EnvRunResult]]] = []
        self.hooks: list[tuple[str, Callable[[], None]]] = []
        self.calls: list[
            tuple[tuple[str, ...], dict[str, str] | None]
        ] = []

    def script(self, token: str, result: EnvRunResult) -> None:
        self.scripts.append((token, result))

    def script_queue(
        self, token: str, results: list[EnvRunResult]
    ) -> None:
        self.queues.append((token, list(results)))

    def hook(self, token: str, action: Callable[[], None]) -> None:
        self.hooks.append((token, action))

    def run(
        self,
        argv: tuple[str, ...],
        *,
        timeout: float,
        env: dict[str, str] | None = None,
        cwd: Path | None = None,
    ) -> EnvRunResult:
        self.calls.append((tuple(argv), env))
        joined = " ".join(argv)
        for token, action in self.hooks:
            if token in joined:
                action()
        for token, queue in self.queues:
            if token in joined and queue:
                return queue.pop(0)
        for token, result in self.scripts:
            if token in joined:
                return result
        return EnvRunResult(returncode=0, stdout="", stderr="")


def applied(names: tuple[str, ...]) -> EnvRunResult:
    return EnvRunResult(returncode=0, stdout="\n".join(names) + "\n", stderr="")


GATE_ID = "c" * 64

OWNED_IDENTITY = EnvRunResult(
    0,
    f"{GATE_ID}\tpostgres:17-alpine\tfable-migration-env\t"
    "infra_189\ttrue\n",
    "",
)


def scripted_happy_runner() -> ScriptedRunner:
    runner = ScriptedRunner()
    runner.script("docker inspect", OWNED_IDENTITY)
    runner.script(
        "docker port", EnvRunResult(0, "5432/tcp -> 127.0.0.1:5439\n", "")
    )
    runner.script("rev-parse HEAD", EnvRunResult(0, HEAD40 + "\n", ""))
    # Before deploy nothing is applied; after deploy everything is.
    runner.script("_prisma_migrations", applied(MIGRATIONS))
    return runner


def gate(repo: Path, runner: ScriptedRunner, verdict: Path) -> MigrationGate:
    return MigrationGate(
        config=MigrationEnvConfig(repo_path=repo, slug="infra-189"),
        commands=commands(),
        runner=runner,
        verdict_path=verdict,
    )


class TestGreenPath:
    def test_all_checks_pass_and_verdict_is_green(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()
        verdict_path = tmp_path / "verdict.json"

        verdict = gate(repo, runner, verdict_path).run()

        assert verdict.green is True
        codes = [f.code for f in verdict.findings]
        assert codes == [
            "repo_head_resolved",
            "isolated_worktree",
            "container_identity",
            "loopback_bindings",
            "disposable_naming",
            "pending_before",
            "migrate_deploy",
            "pending_after",
            "deploy_idempotent",
            "corpus_corpus_seed",
            "historical_loan_config_v3",
            "generation_first",
            "generation_byte_clean",
            "container_identity_stable",
            "repo_head_stable",
        ]
        assert all(f.status == "pass" for f in verdict.findings)
        assert verdict.repo_head == HEAD40

    def test_verdict_persists_and_never_contains_credentials(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        verdict_path = tmp_path / "verdict.json"

        gate(repo, scripted_happy_runner(), verdict_path).run()

        raw = verdict_path.read_text()
        assert SECRET_URL_MARKER not in raw
        loaded = load_verdict(verdict_path)
        assert loaded.green is True
        assert loaded.repo_head == HEAD40
        payload = json.loads(raw)
        assert payload["slug"] == "infra-189"


class TestFailClosed:
    def test_deploy_failure_turns_the_verdict_red_and_skips_the_rest(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()
        runner.script(
            "migrate deploy", EnvRunResult(1, "", "connection refused")
        )

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        by_code = {f.code: f.status for f in verdict.findings}
        assert by_code["migrate_deploy"] == "fail"
        assert by_code["generation_byte_clean"] == "skipped"
        assert by_code["corpus_corpus_seed"] == "skipped"

    def test_foreign_container_identity_is_red_before_any_command(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()
        runner.scripts = [
            s for s in runner.scripts if s[0] != "docker inspect"
        ]
        runner.script(
            "docker inspect",
            EnvRunResult(
                0,
                f"{'d' * 64}\tpostgres:17-alpine\t\tinfra_189\ttrue\n",
                "",
            ),
        )

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        finding = next(
            f for f in verdict.findings if f.code == "container_identity"
        )
        assert finding.status == "fail"
        assert "not owned" in finding.evidence
        assert not any(
            "pnpm" in argv for argv, _ in runner.calls
        )

    def test_every_container_operation_targets_the_recorded_id(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is True
        container_commands = [
            " ".join(argv)
            for argv, _ in runner.calls
            if argv[0] == "docker"
        ]
        assert container_commands, "expected docker operations"
        for command in container_commands:
            assert GATE_ID in command
            assert "jo-fable-migration-db" not in command

    def test_same_name_replacement_is_refused_by_the_record(
        self, tmp_path: Path
    ) -> None:
        # The recorded container was replaced by one with the same
        # name, image, and public label but a new immutable id: the
        # inspect of the recorded id finds nothing.
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()
        runner.scripts = [
            s for s in runner.scripts if s[0] != "docker inspect"
        ]
        runner.script(
            "docker inspect", EnvRunResult(1, "", "No such object")
        )

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        finding = next(
            f for f in verdict.findings if f.code == "container_identity"
        )
        assert finding.status == "fail"
        assert not any("pnpm" in argv for argv, _ in runner.calls)

    def test_cross_environment_container_is_refused(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()
        runner.scripts = [
            s for s in runner.scripts if s[0] != "docker inspect"
        ]
        runner.script(
            "docker inspect",
            EnvRunResult(
                0,
                f"{GATE_ID}\tpostgres:17-alpine\tfable-migration-env\t"
                "infra_190\ttrue\n",
                "",
            ),
        )

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        finding = next(
            f for f in verdict.findings if f.code == "container_identity"
        )
        assert finding.status == "fail"
        assert "different environment slug" in finding.evidence

    def test_missing_provision_record_is_red(self, tmp_path: Path) -> None:
        repo = build_repo(tmp_path)
        (repo / ".fable-provision-record").unlink()

        verdict = gate(
            repo, scripted_happy_runner(), tmp_path / "verdict.json"
        ).run()

        assert verdict.green is False
        finding = next(
            f for f in verdict.findings if f.code == "container_identity"
        )
        assert finding.status == "fail"
        assert "provision record" in finding.evidence

    def test_container_vanishing_during_the_gate_is_red(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()
        runner.scripts = [
            s for s in runner.scripts if s[0] != "docker inspect"
        ]
        runner.script_queue(
            "docker inspect",
            [OWNED_IDENTITY, EnvRunResult(1, "", "No such object")],
        )

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        finding = next(
            f
            for f in verdict.findings
            if f.code == "container_identity_stable"
        )
        assert finding.status == "fail"

    def test_wildcard_binding_disproves_loopback(self, tmp_path: Path) -> None:
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()
        runner.scripts = [
            s for s in runner.scripts if s[0] != "docker port"
        ]
        runner.script(
            "docker port", EnvRunResult(0, "5432/tcp -> 0.0.0.0:5439\n", "")
        )

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        finding = next(
            f for f in verdict.findings if f.code == "loopback_bindings"
        )
        assert finding.status == "fail"

    def test_gate_refuses_the_primary_checkout_before_any_command(
        self, tmp_path: Path
    ) -> None:
        from tests.test_migration_env import make_linked_worktree

        primary, _ = make_linked_worktree(tmp_path)
        runner = scripted_happy_runner()

        verdict = gate(primary, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        finding = next(
            f for f in verdict.findings if f.code == "isolated_worktree"
        )
        assert finding.status == "fail"
        # No repository or database command ever ran against the
        # primary checkout.
        assert not any(
            "pnpm" in argv or "psql" in " ".join(argv)
            for argv, _ in runner.calls
        )

    def test_missing_isolation_marker_fails_the_first_check(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        (repo / ".fable-isolated-worktree").unlink()

        verdict = gate(
            repo, scripted_happy_runner(), tmp_path / "verdict.json"
        ).run()

        assert verdict.green is False
        finding = next(
            f for f in verdict.findings if f.code == "isolated_worktree"
        )
        assert finding.status == "fail"

    def test_incomplete_migration_set_after_deploy_fails(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()
        runner.scripts = [
            s for s in runner.scripts if s[0] != "_prisma_migrations"
        ]
        # One migration never lands, before or after deploy.
        runner.script("_prisma_migrations", applied(MIGRATIONS[:2]))

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        finding = next(
            f for f in verdict.findings if f.code == "pending_after"
        )
        assert finding.status == "fail"
        assert MIGRATIONS[2] in finding.evidence

    def test_generator_rewriting_output_on_second_pass_fails(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()
        drifting = repo / "packages/database/scripts/fixtures/generated.json"
        runner.hook(
            "db:generate-starters",
            lambda: drifting.write_text(drifting.read_text() + "x"),
        )

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        finding = next(
            f
            for f in verdict.findings
            if f.code == "generation_byte_clean"
        )
        assert finding.status == "fail"

    def test_check_exceptions_record_only_the_exception_type(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()

        def explode() -> None:
            raise OSError(f"tried postgresql://postgres:{'s3cr3t'}@x/db")

        runner.hook("migrate deploy", explode)

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        finding = next(
            f for f in verdict.findings if f.code == "migrate_deploy"
        )
        assert finding.status == "fail"
        assert "OSError" in finding.evidence
        assert "s3cr3t" not in finding.evidence


class TestHeadBinding:
    def test_unresolvable_head_is_red_before_any_mutation(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()
        runner.scripts = [
            s for s in runner.scripts if s[0] != "rev-parse HEAD"
        ]
        runner.script(
            "rev-parse HEAD", EnvRunResult(128, "", "fatal: not a repo")
        )

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        assert verdict.findings[0].code == "repo_head_resolved"
        assert verdict.findings[0].status == "fail"
        assert verdict.repo_head == "unresolved"
        # Every later check was skipped: nothing mutated without an
        # exact head binding.
        assert all(
            f.status == "skipped" for f in verdict.findings[1:]
        )

    def test_a_short_or_malformed_head_is_never_accepted(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()
        runner.scripts = [
            s for s in runner.scripts if s[0] != "rev-parse HEAD"
        ]
        runner.script("rev-parse HEAD", EnvRunResult(0, "abc123\n", ""))

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        assert verdict.findings[0].code == "repo_head_resolved"
        assert verdict.findings[0].status == "fail"
        assert verdict.repo_head == "unresolved"

    def test_head_drift_during_the_gate_is_red(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        drifted = "ab" * 20
        reads = {"count": 0}

        class DriftingHead(ScriptedRunner):
            def run(self, argv, *, timeout, env=None, cwd=None):  # type: ignore[no-untyped-def]
                if "rev-parse" in argv:
                    reads["count"] += 1
                    head = HEAD40 if reads["count"] == 1 else drifted
                    return EnvRunResult(0, head + "\n", "")
                return super().run(argv, timeout=timeout, env=env, cwd=cwd)

        runner = DriftingHead()
        runner.script("docker inspect", OWNED_IDENTITY)
        runner.script(
            "docker port",
            EnvRunResult(0, "5432/tcp -> 127.0.0.1:5439\n", ""),
        )
        runner.script("_prisma_migrations", applied(MIGRATIONS))

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        drift = next(
            f for f in verdict.findings if f.code == "repo_head_stable"
        )
        assert drift.status == "fail"
        assert HEAD40 in drift.evidence and drifted in drift.evidence
        # The verdict binds the head the gate started from.
        assert verdict.repo_head == HEAD40


class TestExactSetProofs:
    def sequenced(
        self, applied_reads: list[EnvRunResult]
    ) -> ScriptedRunner:
        runner = scripted_happy_runner()
        runner.scripts = [
            s for s in runner.scripts if s[0] != "_prisma_migrations"
        ]
        runner.script_queue("_prisma_migrations", applied_reads)
        return runner

    def test_swapped_applied_migration_with_same_count_is_red(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        rogue = "20990101000000_rogue_replacement"
        runner = self.sequenced(
            [
                applied(()),
                applied(MIGRATIONS),
                # Second deployment swaps one applied identity while
                # preserving the count.
                applied((*MIGRATIONS[:2], rogue)),
            ]
        )

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        finding = next(
            f for f in verdict.findings if f.code == "deploy_idempotent"
        )
        assert finding.status == "fail"
        assert rogue in finding.evidence
        assert MIGRATIONS[2] in finding.evidence

    @pytest.mark.parametrize(
        "second_read",
        [MIGRATIONS[:2], (*MIGRATIONS, "20990101000000_extra")],
        ids=["removed", "added"],
    )
    def test_added_or_removed_applied_migration_is_red(
        self, tmp_path: Path, second_read: tuple[str, ...]
    ) -> None:
        repo = build_repo(tmp_path)
        runner = self.sequenced(
            [applied(()), applied(MIGRATIONS), applied(second_read)]
        )

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        finding = next(
            f for f in verdict.findings if f.code == "deploy_idempotent"
        )
        assert finding.status == "fail"

    def test_stable_exact_applied_set_is_green_with_set_evidence(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = self.sequenced(
            [applied(()), applied(MIGRATIONS), applied(MIGRATIONS)]
        )

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is True
        by_code = {f.code: f.evidence for f in verdict.findings}
        # Every proof is derived from the captured exact name sets, and
        # the persisted evidence carries their auditable digests.
        assert "sha256:" in by_code["pending_before"]
        assert "sha256:" in by_code["pending_after"]
        assert "sha256:" in by_code["deploy_idempotent"]
        assert "set-equal" in by_code["deploy_idempotent"]

    def test_unexpected_applied_identity_fails_pending_after(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        rogue = "20990101000000_never_in_repository"
        runner = self.sequenced(
            [applied(()), applied((*MIGRATIONS, rogue))]
        )

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        finding = next(
            f for f in verdict.findings if f.code == "pending_after"
        )
        assert finding.status == "fail"
        assert rogue in finding.evidence

    def test_duplicate_applied_identity_is_red(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = self.sequenced(
            [applied((MIGRATIONS[0], MIGRATIONS[0]))]
        )

        verdict = gate(repo, runner, tmp_path / "verdict.json").run()

        assert verdict.green is False
        finding = next(
            f for f in verdict.findings if f.code == "pending_before"
        )
        assert finding.status == "fail"
        assert "duplicate" in finding.evidence


class TestPendingSetDerivation:
    def test_fresh_database_reports_every_migration_pending(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)

        # The first applied-set read hits a fresh database (the relation
        # does not exist yet); every later read sees all migrations.
        state = {"fresh": True}

        class FreshThenApplied(ScriptedRunner):
            def run(self, argv, *, timeout, env=None, cwd=None):  # type: ignore[no-untyped-def]
                if "_prisma_migrations" in " ".join(argv) and state["fresh"]:
                    state["fresh"] = False
                    return EnvRunResult(
                        1, "", 'relation "_prisma_migrations" does not exist'
                    )
                return super().run(argv, timeout=timeout, env=env, cwd=cwd)

        fresh_runner = FreshThenApplied()
        fresh_runner.script("docker inspect", OWNED_IDENTITY)
        fresh_runner.script(
            "docker port",
            EnvRunResult(0, "5432/tcp -> 127.0.0.1:5439\n", ""),
        )
        fresh_runner.script(
            "rev-parse HEAD", EnvRunResult(0, HEAD40 + "\n", "")
        )
        fresh_runner.script("_prisma_migrations", applied(MIGRATIONS))

        verdict = gate(repo, fresh_runner, tmp_path / "verdict.json").run()

        assert verdict.green is True
        before = next(
            f for f in verdict.findings if f.code == "pending_before"
        )
        assert "3 pending" in before.evidence


class TestJoLoansCommands:
    def factory(self, tmp_path: Path):
        from hermes_orchestrator.migration_gate import jo_loans_commands

        del tmp_path
        return jo_loans_commands()

    def all_argvs(self, cmds: GateCommands) -> list[tuple[str, ...]]:
        return [
            cmds.migrate_deploy,
            cmds.generator,
            *[argv for _, argv in cmds.corpus],
            *[argv for _, argv in cmds.historical],
        ]

    def test_every_command_is_a_package_filtered_script(
        self, tmp_path: Path
    ) -> None:
        # Root wrappers load dotenv files and chain a staging WorkOS
        # reset; only @jo/database package scripts are allowed.
        for argv in self.all_argvs(self.factory(tmp_path)):
            script = argv[2] if argv[:2] == ("sh", "-c") else " ".join(argv)
            assert script.startswith("pnpm --filter @jo/database ")
            assert ".env" not in script

    def test_no_argv_ever_carries_a_connection_string(
        self, tmp_path: Path
    ) -> None:
        for argv in self.all_argvs(self.factory(tmp_path)):
            joined = " ".join(argv)
            assert "postgresql://" not in joined
            assert "postgres:postgres" not in joined

    def test_dry_runs_precede_every_apply(self, tmp_path: Path) -> None:
        cmds = self.factory(tmp_path)
        codes = [code for code, _ in (*cmds.corpus, *cmds.historical)]
        for code in codes:
            if code.endswith("_apply"):
                dry = code.removesuffix("_apply") + "_dry"
                assert codes.index(dry) < codes.index(code)

    def test_schema_authority_reads_the_shadow_from_the_environment(
        self, tmp_path: Path
    ) -> None:
        cmds = self.factory(tmp_path)
        authority = dict(cmds.historical)["schema_authority"]
        # Prisma 7 resolves both the datasource and the shadow database
        # from prisma.config.ts (DATABASE_URL / SHADOW_DATABASE_URL in
        # the injected environment); argv carries no URL flag at all.
        assert "--shadow-database-url" not in authority
        assert "--from-migrations" in authority
        assert "--to-schema" in authority
        assert "--exit-code" in authority

    def test_repo_commands_inject_both_disposable_dsns_via_env(
        self, tmp_path: Path
    ) -> None:
        repo = build_repo(tmp_path)
        runner = scripted_happy_runner()

        gate(repo, runner, tmp_path / "verdict.json").run()

        repo_envs = [env for _, env in runner.calls if env is not None]
        assert repo_envs, "expected repo commands with injected env"
        for env in repo_envs:
            assert "jo_local_fable_infra_189_target" in env["DATABASE_URL"]
            assert (
                "jo_local_fable_infra_189_source"
                in env["SHADOW_DATABASE_URL"]
            )

    def test_generated_paths_cover_only_gitignored_generator_output(
        self, tmp_path: Path
    ) -> None:
        cmds = self.factory(tmp_path)
        assert cmds.generated_paths == (
            "packages/database/scripts/fixtures/generated",
        )

    def test_historical_commands_reference_only_scripts_mainline_keeps(
        self, tmp_path: Path
    ) -> None:
        codes = [code for code, _ in self.factory(tmp_path).historical]
        # The v3 early-checks sidecar was deleted from origin/main; its
        # dangling package script must never be part of the gate.
        assert "loan_config_v3_early_checks" not in codes
        assert codes == [
            "schema_authority",
            "legacy_loan_config_dry",
            "legacy_loan_config_apply",
            "product_config_dry",
            "product_config_apply",
        ]

    def test_eligibility_corpus_uses_the_mainline_lock_script(
        self, tmp_path: Path
    ) -> None:
        # jo-loans origin/main carries db:migrate-lock-eligibility; the
        # loan-product hydration script exists only on newer branches.
        corpus = dict(self.factory(tmp_path).corpus)
        assert corpus["eligibility_keys_dry"][-1] == (
            "db:migrate-lock-eligibility:dry"
        )
        assert corpus["eligibility_keys_apply"][-1] == (
            "db:migrate-lock-eligibility"
        )


def test_tree_digest_is_order_independent_and_content_sensitive(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    (root / "a").mkdir(parents=True)
    (root / "a/one.txt").write_text("1")
    (root / "a/two.txt").write_text("2")

    first = tree_digest(root, ("a",))
    assert first == tree_digest(root, ("a",))

    (root / "a/two.txt").write_text("2!")
    assert first != tree_digest(root, ("a",))
