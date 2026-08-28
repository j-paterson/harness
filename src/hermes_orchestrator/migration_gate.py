"""Deterministic pre-candidate gate for migration-heavy lead work.

INFRA-189: before migration- or generation-heavy work may claim a
merge-ready boundary, every applicable environment proof must be green
in one deterministic, ordered pass: the run targets an explicitly
isolated worktree, the disposable database container publishes only
loopback bindings, the databases carry guarded disposable names, the
exact pending-migration set is captured before and after ``migrate
deploy``, a second deploy proves idempotence, the repository's own
historical data migrations and corpus seeds succeed against disposable
data, and the fixture generator produces a byte-clean second pass. The
gate fails closed: the first failure marks every remaining check
``skipped`` and the verdict red, an exception surfaces only as its type
name (never a connection string), and the verdict is persisted as a
durable JSON artifact keyed to the exact repository head.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from hermes_orchestrator.migration_env import (
    ISOLATION_MARKER,
    EnvCommandRunner,
    MigrationEnvConfig,
    MigrationEnvRefusal,
    inspect_container,
    loopback_proof,
    require_disposable,
    require_isolated_worktree,
    require_owned,
    require_provision_record,
)

_MIGRATIONS_DIR = "packages/database/prisma/migrations"

_APPLIED_SQL = (
    "SELECT migration_name FROM _prisma_migrations "
    "WHERE finished_at IS NOT NULL ORDER BY migration_name"
)

_HEAD_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class GateCheckFailed(RuntimeError):
    """A check failed with curated, credential-free evidence."""


def _set_digest(names: frozenset[str]) -> str:
    """A stable, auditable digest of one exact migration-name set."""

    return hashlib.sha256(
        "\n".join(sorted(names)).encode()
    ).hexdigest()[:16]


def _name_list(names: frozenset[str] | set[str] | list[str]) -> str:
    ordered = sorted(names)
    if len(ordered) > 20:
        return ", ".join(ordered[:20]) + f", +{len(ordered) - 20} more"
    return ", ".join(ordered)


@dataclass(frozen=True, slots=True)
class GateCommands:
    """The target repository's own commands the gate drives.

    Every command runs with the isolated worktree as its working
    directory and receives the disposable target database exclusively
    through an injected ``DATABASE_URL`` environment variable — never
    through dotenv files and never on the command line.
    """

    migrate_deploy: tuple[str, ...]
    generator: tuple[str, ...]
    generated_paths: tuple[str, ...]
    corpus: tuple[tuple[str, tuple[str, ...]], ...] = ()
    historical: tuple[tuple[str, tuple[str, ...]], ...] = ()


@dataclass(frozen=True, slots=True)
class GateFinding:
    code: str
    status: str  # "pass" | "fail" | "skipped"
    evidence: str


@dataclass(frozen=True, slots=True)
class GateVerdict:
    green: bool
    slug: str
    repo_head: str
    created_at: str
    findings: tuple[GateFinding, ...]


def tree_digest(root: Path, paths: tuple[str, ...]) -> str:
    """One stable content digest over the named generated trees.

    Files hash in sorted relative order with their exact bytes, so any
    rewrite — content, addition, or removal — changes the digest. A
    completely absent tree is refused rather than hashed as empty.
    """

    digest = hashlib.sha256()
    seen = False
    for rel in sorted(paths):
        base = root / rel
        if not base.exists():
            continue
        files = sorted(
            p for p in base.rglob("*") if p.is_file()
        )
        for file in files:
            seen = True
            digest.update(str(file.relative_to(root)).encode())
            digest.update(b"\0")
            digest.update(file.read_bytes())
            digest.update(b"\0")
    if not seen:
        raise MigrationEnvRefusal(
            "no generated output exists under the configured paths"
        )
    return digest.hexdigest()


class MigrationGate:
    """Run every applicable proof in order and persist one verdict."""

    def __init__(
        self,
        *,
        config: MigrationEnvConfig,
        commands: GateCommands,
        runner: EnvCommandRunner,
        verdict_path: Path,
        timeout: float = 1800.0,
    ) -> None:
        self._config = config
        self._commands = commands
        self._runner = runner
        self._verdict_path = verdict_path
        self._timeout = timeout
        self._first_generation_digest: str | None = None
        self._repository_set: frozenset[str] | None = None
        self._applied_after_set: frozenset[str] | None = None
        self._resolved_head: str | None = None
        self._container_id: str | None = None

    def run(self) -> GateVerdict:
        checks: list[tuple[str, Callable[[], str]]] = [
            # Head resolution is the first fail-closed check: no
            # mutation may happen without one exact commit to bind the
            # verdict to.
            ("repo_head_resolved", self._check_repo_head_resolved),
            ("isolated_worktree", self._check_isolated_worktree),
            ("container_identity", self._check_container_identity),
            ("loopback_bindings", self._check_loopback_bindings),
            ("disposable_naming", self._check_disposable_naming),
            ("pending_before", self._check_pending_before),
            ("migrate_deploy", self._check_migrate_deploy),
            ("pending_after", self._check_pending_after),
            ("deploy_idempotent", self._check_deploy_idempotent),
        ]
        # The synthetic corpus seeds first: the historical data
        # migrations and the generator read the rows it creates.
        checks.extend(
            (f"corpus_{code}", self._command_check(argv))
            for code, argv in self._commands.corpus
        )
        checks.extend(
            (f"historical_{code}", self._command_check(argv))
            for code, argv in self._commands.historical
        )
        checks.append(("generation_first", self._check_generation_first))
        checks.append(
            ("generation_byte_clean", self._check_generation_byte_clean)
        )
        # Terminal revalidation: the recorded container identity and
        # the repository head must both be unchanged when the verdict
        # is issued; drift during the run voids every proof above.
        checks.append(
            (
                "container_identity_stable",
                self._check_container_identity_stable,
            )
        )
        checks.append(("repo_head_stable", self._check_repo_head_stable))

        findings: list[GateFinding] = []
        failed = False
        for code, check in checks:
            if failed:
                findings.append(
                    GateFinding(
                        code=code,
                        status="skipped",
                        evidence="not evaluated after an earlier failure",
                    )
                )
                continue
            try:
                evidence = check()
            except GateCheckFailed as failure:
                failed = True
                findings.append(
                    GateFinding(
                        code=code, status="fail", evidence=str(failure)
                    )
                )
                continue
            except Exception as error:
                # Fail closed and reveal only the exception type: raw
                # messages may carry connection strings or paths.
                failed = True
                findings.append(
                    GateFinding(
                        code=code,
                        status="fail",
                        evidence=f"exception: {type(error).__name__}",
                    )
                )
                continue
            findings.append(
                GateFinding(code=code, status="pass", evidence=evidence)
            )

        verdict = GateVerdict(
            green=all(f.status == "pass" for f in findings),
            slug=self._config.slug,
            repo_head=self._resolved_head or "unresolved",
            created_at=datetime.now(UTC).isoformat(),
            findings=tuple(findings),
        )
        self._persist(verdict)
        return verdict

    # -- individual checks -------------------------------------------------

    def _check_repo_head_resolved(self) -> str:
        head = self._read_head()
        if head is None:
            raise GateCheckFailed(
                "the repository head could not be resolved to one exact "
                "40-character commit"
            )
        self._resolved_head = head
        return f"HEAD {head}"

    def _check_repo_head_stable(self) -> str:
        head = self._read_head()
        if head != self._resolved_head:
            raise GateCheckFailed(
                "repository head drifted during the gate: "
                f"{self._resolved_head} -> {head or 'unresolved'}"
            )
        return "head unchanged across the entire run"

    def _check_isolated_worktree(self) -> str:
        require_isolated_worktree(
            self._config.repo_path, slug=self._config.slug
        )
        return (
            "isolation marker validated against linked worktree identity"
        )

    def _check_container_identity(self) -> str:
        try:
            record = require_provision_record(self._config)
            identity = inspect_container(
                self._runner, record.container_id, timeout=self._timeout
            )
            if identity is None:
                raise GateCheckFailed(
                    "the recorded container does not exist; a same-name "
                    "replacement is never accepted"
                )
            require_owned(identity, self._config)
        except MigrationEnvRefusal as refusal:
            # Refusal messages are curated and credential-free.
            raise GateCheckFailed(str(refusal)) from None
        self._container_id = identity.container_id
        return (
            f"owned container {identity.container_id[:12]} bound by "
            "the provision record"
        )

    def _check_container_identity_stable(self) -> str:
        container_id = self._require_container_id()
        try:
            identity = inspect_container(
                self._runner, container_id, timeout=self._timeout
            )
            if identity is None:
                raise GateCheckFailed(
                    "the recorded container vanished during the gate"
                )
            require_owned(identity, self._config)
        except MigrationEnvRefusal as refusal:
            raise GateCheckFailed(str(refusal)) from None
        return "container identity unchanged across the entire run"

    def _require_container_id(self) -> str:
        if self._container_id is None:
            raise GateCheckFailed(
                "the container identity was never captured"
            )
        return self._container_id

    def _check_loopback_bindings(self) -> str:
        result = self._runner.run(
            ("docker", "port", self._require_container_id()),
            timeout=self._timeout,
        )
        if result.returncode != 0:
            raise GateCheckFailed(
                "the disposable container's bindings could not be read"
            )
        proof = loopback_proof(result.stdout)
        if not proof.proven:
            raise GateCheckFailed(proof.evidence)
        return proof.evidence

    def _check_disposable_naming(self) -> str:
        require_disposable(self._config.source_database)
        require_disposable(self._config.target_database)
        return (
            f"{self._config.source_database}, "
            f"{self._config.target_database}"
        )

    def _check_pending_before(self) -> str:
        names = self._migration_names()
        if not names:
            raise GateCheckFailed(
                "the repository has no migrations to verify"
            )
        repository = frozenset(names)
        applied = self._applied_set()
        unexpected = applied - repository
        if unexpected:
            raise GateCheckFailed(
                "applied migration identities missing from the "
                "repository set: " + _name_list(unexpected)
            )
        pending = repository - applied
        self._repository_set = repository
        return (
            f"{len(pending)} pending of {len(repository)} migrations; "
            f"repository set sha256:{_set_digest(repository)}; "
            f"applied set sha256:{_set_digest(applied)}"
        )

    def _check_migrate_deploy(self) -> str:
        self._run_repo_command(self._commands.migrate_deploy)
        return "migrate deploy exited 0"

    def _check_pending_after(self) -> str:
        repository = self._repository_set
        if repository is None:
            raise GateCheckFailed(
                "the repository migration set was never captured"
            )
        applied = self._applied_set()
        missing = repository - applied
        unexpected = applied - repository
        if missing or unexpected:
            raise GateCheckFailed(
                "applied set does not equal the repository set; "
                "missing: [" + _name_list(missing) + "]; "
                "unexpected: [" + _name_list(unexpected) + "]"
            )
        self._applied_after_set = applied
        return (
            f"applied set equals repository set ({len(applied)} names, "
            f"sha256:{_set_digest(applied)})"
        )

    def _check_deploy_idempotent(self) -> str:
        self._run_repo_command(self._commands.migrate_deploy)
        applied = self._applied_set()
        expected = self._applied_after_set
        if expected is None:
            raise GateCheckFailed(
                "the post-deploy applied set was never captured"
            )
        if applied != expected:
            raise GateCheckFailed(
                "second deploy changed the applied set; added: ["
                + _name_list(applied - expected)
                + "]; removed: ["
                + _name_list(expected - applied)
                + "]"
            )
        return (
            f"second deploy left the applied set set-equal "
            f"({len(applied)} names, sha256:{_set_digest(applied)})"
        )

    def _command_check(
        self, argv: tuple[str, ...]
    ) -> Callable[[], str]:
        def check() -> str:
            self._run_repo_command(argv)
            return "exited 0"

        return check

    def _check_generation_first(self) -> str:
        self._run_repo_command(self._commands.generator)
        self._first_generation_digest = tree_digest(
            self._config.repo_path, self._commands.generated_paths
        )
        return f"digest {self._first_generation_digest[:16]}"

    def _check_generation_byte_clean(self) -> str:
        self._run_repo_command(self._commands.generator)
        second = tree_digest(
            self._config.repo_path, self._commands.generated_paths
        )
        if second != self._first_generation_digest:
            raise GateCheckFailed(
                "second generator pass rewrote generated output: "
                f"{self._first_generation_digest[:16]} -> {second[:16]}"
            )
        return "second pass byte-identical"

    # -- helpers -----------------------------------------------------------

    def _read_head(self) -> str | None:
        result = self._runner.run(
            (
                "git",
                "-C",
                str(self._config.repo_path),
                "rev-parse",
                "HEAD",
            ),
            timeout=self._timeout,
        )
        if result.returncode != 0:
            return None
        head = result.stdout.strip()
        return head if _HEAD_PATTERN.fullmatch(head) else None

    def _migration_names(self) -> tuple[str, ...]:
        migrations = self._config.repo_path / _MIGRATIONS_DIR
        if not migrations.is_dir():
            return ()
        return tuple(
            sorted(p.name for p in migrations.iterdir() if p.is_dir())
        )

    def _applied_set(self) -> frozenset[str]:
        result = self._runner.run(
            (
                "docker",
                "exec",
                self._require_container_id(),
                "psql",
                "-U",
                "postgres",
                "-d",
                self._config.target_database,
                "-v",
                "ON_ERROR_STOP=1",
                "-tAc",
                _APPLIED_SQL,
            ),
            timeout=self._timeout,
        )
        if result.returncode != 0:
            if "does not exist" in result.stderr:
                # A fresh database has no migrations table yet: nothing
                # is applied, everything is pending.
                return frozenset()
            raise GateCheckFailed(
                "the applied-migration set could not be read"
            )
        lines = [
            line.strip()
            for line in result.stdout.splitlines()
            if line.strip()
        ]
        if len(lines) != len(set(lines)):
            duplicates = sorted(
                {name for name in lines if lines.count(name) > 1}
            )
            raise GateCheckFailed(
                "duplicate applied migration identity: "
                + _name_list(duplicates)
            )
        return frozenset(lines)

    def _run_repo_command(self, argv: tuple[str, ...]) -> None:
        # Both disposable identities travel only through the injected
        # environment: the target as DATABASE_URL, the source as the
        # prisma shadow. No connection string ever enters argv.
        result = self._runner.run(
            argv,
            timeout=self._timeout,
            env={
                "DATABASE_URL": self._config.dsn(
                    self._config.target_database
                ).url,
                "SHADOW_DATABASE_URL": self._config.dsn(
                    self._config.source_database
                ).url,
            },
            cwd=self._config.repo_path,
        )
        if result.returncode != 0:
            raise GateCheckFailed(
                f"command exited {result.returncode}"
            )

    def _persist(self, verdict: GateVerdict) -> None:
        payload = {
            "green": verdict.green,
            "slug": verdict.slug,
            "repo_head": verdict.repo_head,
            "created_at": verdict.created_at,
            "findings": [
                {
                    "code": f.code,
                    "status": f.status,
                    "evidence": f.evidence,
                }
                for f in verdict.findings
            ],
        }
        self._verdict_path.parent.mkdir(parents=True, exist_ok=True)
        self._verdict_path.write_text(json.dumps(payload, indent=2) + "\n")


def jo_loans_commands() -> GateCommands:
    """The target repository's own commands for every gate check.

    Every entry is a ``pnpm --filter @jo/database`` package script —
    never a repository-root wrapper. The root wrappers load dotenv files
    (which could silently retarget a preview, staging, or neon database)
    and the root ``db:seed`` additionally chains ``@jo/auth
    reset-staging``, which mutates the shared staging WorkOS
    environment. Dry-run variants run before every apply. The
    schema-authority diff uses the disposable *source* database as the
    prisma shadow so the pair proves the migrations directory alone
    reproduces the schema; Prisma 7 resolves it from
    ``prisma.config.ts`` through the injected ``SHADOW_DATABASE_URL``,
    so no argv ever carries a connection string.
    """

    package = ("pnpm", "--filter", "@jo/database")
    return GateCommands(
        migrate_deploy=(*package, "run", "db:migrate:deploy"),
        generator=(*package, "run", "db:generate-starters"),
        generated_paths=(
            "packages/database/scripts/fixtures/generated",
        ),
        corpus=(
            ("synthetic_seed", (*package, "run", "db:seed")),
            # origin/main's historical eligibility-key backfill; the
            # loan-product hydration script exists only on newer
            # branches and would fail with no-such-script here.
            (
                "eligibility_keys_dry",
                (*package, "run", "db:migrate-lock-eligibility:dry"),
            ),
            (
                "eligibility_keys_apply",
                (*package, "run", "db:migrate-lock-eligibility"),
            ),
        ),
        historical=(
            (
                "schema_authority",
                (
                    *package,
                    "exec",
                    "prisma",
                    "migrate",
                    "diff",
                    "--from-migrations",
                    "prisma/migrations",
                    "--to-schema",
                    "prisma/schema.prisma",
                    "--exit-code",
                ),
            ),
            # db:migrate-loan-config-v3 still exists in package.json but
            # its data-migration sidecar was deleted from origin/main —
            # a dangling script that can never run. LoanConfig history
            # is covered by the legacy flat-to-nested migration below
            # and the lock-eligibility corpus check above.
            (
                "legacy_loan_config_dry",
                (*package, "run", "db:migrate-legacy:loan-config:dry"),
            ),
            (
                "legacy_loan_config_apply",
                (*package, "run", "db:migrate-legacy:loan-config"),
            ),
            (
                "product_config_dry",
                (*package, "run", "db:add-product-config:dry-run"),
            ),
            (
                "product_config_apply",
                (*package, "run", "db:add-product-config"),
            ),
        ),
    )


def render_handoff(
    *,
    config: MigrationEnvConfig,
    commands: GateCommands,
    verdict: GateVerdict | None = None,
) -> str:
    """One durable local handoff: exact commands, env var names only.

    Connection strings never appear here — commands receive the
    disposable database exclusively through an injected ``DATABASE_URL``
    whose value the tooling derives itself from the validated loopback
    identity.
    """

    lines: list[str] = [
        "# INFRA-189 disposable migration environment — handoff",
        "",
        "## Safety assertions",
        "",
        "- The disposable PostgreSQL container publishes only "
        f"`127.0.0.1:{config.port}`; the gate re-proves the binding on "
        "every run and fails closed on any wildcard binding.",
        f"- Databases `{config.source_database}` and "
        f"`{config.target_database}` follow the target repository's "
        "guarded `jo_local_*` convention with a disposable infix; the "
        "tooling structurally refuses every other name — including "
        "`jo_local`, `test_*`, previews, staging, neon, and production.",
        "- Every repository command is a `pnpm --filter @jo/database` "
        "package script. Repository-root wrappers are never used: they "
        "load dotenv files that can silently retarget a remote "
        "database, and the root `db:seed` chains `@jo/auth "
        "reset-staging`, which mutates the shared staging WorkOS "
        "environment.",
        "- The corpus is synthetic (faker-generated seed plus committed "
        "lender config blobs). No production dump, snapshot, or "
        "customer data enters this environment.",
        "- Gate and generator runs refuse any checkout that does not "
        f"carry the `{ISOLATION_MARKER}` isolation marker file, so "
        "the operator's primary checkout cannot be mutated.",
        "",
        "## Exact commands",
        "",
        "```",
        f"hermes-orchestrator migration-env provision "
        f"--target-repo {config.repo_path} --slug {config.slug} --execute",
        f"hermes-orchestrator migration-env mark "
        f"--target-repo {config.repo_path} --slug {config.slug}",
        f"hermes-orchestrator migration-env gate "
        f"--target-repo {config.repo_path} --slug {config.slug} "
        "--out <verdict.json> --execute",
        f"hermes-orchestrator migration-env teardown "
        f"--target-repo {config.repo_path} --slug {config.slug} --execute",
        "```",
        "",
        "Underlying repository commands the gate drives, in order "
        "(each with `DATABASE_URL` injected to the disposable target):",
        "",
        "```",
        " ".join(commands.migrate_deploy),
    ]
    for _, argv in commands.corpus:
        lines.append(" ".join(argv))
    lines.extend(" ".join(argv) for _, argv in commands.historical)
    lines.extend(
        (
            " ".join(commands.generator) + "  # runs twice; byte-clean",
            "```",
            "",
            "## Environment variable names (names only, never values)",
            "",
            "- `DATABASE_URL` — injected per command by the tooling; "
            "always the disposable loopback target.",
            "- `TEST_DATABASE_URL` — integration tests; must name a "
            "`test_`-prefixed database (the harness refuses others).",
            "- `SHADOW_DATABASE_URL` — injected per command by the "
            "tooling; always the disposable loopback source, serving "
            "as the prisma shadow.",
            "- `DATABASE_URL_READ_ONLY_STAGE` — required only by the "
            "remote stage restore; never set in this environment.",
            "- `RECONCILE_WORKOS_SKIP` — set to `1` by the repository's "
            "own hydration rehearsal to keep WorkOS untouched.",
            "- `WORKOS_API_KEY` — never set in this environment.",
            "- `NEXT_PUBLIC_FAY_LENDER_IDS`, "
            "`NEXT_PUBLIC_FUTURES_LENDER_ID` — ambient generator "
            "inputs; keep them fixed across both generator passes.",
            "",
            "## Isolated worktrees without duplicated installs",
            "",
            "- Create Fable and reviewer worktrees with `git worktree "
            "add`; `pnpm install` links from pnpm's content-addressable "
            "store, so a second worktree costs hard links, not another "
            "multi-gigabyte download.",
            "- After install or any schema change, run `pnpm --filter "
            "@jo/database run db:generate:types` in that worktree so "
            "its Prisma client matches its checkout.",
            "",
            "## Remote-only evidence and recorded limitations",
            "",
            "- The repository's preview hydration is usable locally via "
            "`sh packages/database/scripts/rehearse-preview-hydrate.sh` "
            "with a loopback `*_rehearsal` database and `--dump "
            "<sanitized-snapshot>`; the initial stage restore "
            "(`hydrate-from-stage.sh`) needs "
            "`DATABASE_URL_READ_ONLY_STAGE` and therefore stays "
            "remote-only evidence.",
            "- Raw production dumps are customer data and are excluded "
            "from this environment by policy; only the synthetic seed "
            "corpus and committed config blobs are used.",
            "- While any applicable gate check is red or unproven, "
            "migration-heavy work must not claim a merge-ready "
            "boundary (emit FABLE_BLOCKED instead).",
            "",
        )
    )
    if verdict is not None:
        status = "GREEN" if verdict.green else "RED"
        lines.extend(
            (
                "## Latest gate verdict",
                "",
                f"- {status} at repository head `{verdict.repo_head}` "
                f"({verdict.created_at})",
            )
        )
        lines.extend(
            f"- {f.code}: {f.status} — {f.evidence}"
            for f in verdict.findings
        )
        lines.append("")
    return "\n".join(lines)


def load_verdict(path: Path) -> GateVerdict:
    payload = json.loads(path.read_text())
    return GateVerdict(
        green=bool(payload["green"]),
        slug=str(payload["slug"]),
        repo_head=str(payload["repo_head"]),
        created_at=str(payload["created_at"]),
        findings=tuple(
            GateFinding(
                code=str(f["code"]),
                status=str(f["status"]),
                evidence=str(f["evidence"]),
            )
            for f in payload["findings"]
        ),
    )
