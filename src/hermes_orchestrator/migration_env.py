"""Safe disposable database environment for migration-heavy lead work.

INFRA-189: before a lead may publish migration- or generation-heavy work,
it needs a local completion environment that is provably harmless: a
dedicated loopback-only PostgreSQL container holding a disposable
source/target database pair named under the target repository's guarded
``jo_local_*`` convention, exercised only from an explicitly marked
isolated worktree. Everything here fails closed — a host that is not
loopback, a database name that is not disposable, a worktree without the
isolation marker, or a port binding that is not 127.0.0.1 refuses the
operation rather than proceeding. Connection strings never appear in
reprs, refusals, or logs; commands receive them only through injected
environment variables.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Protocol
from urllib.parse import urlsplit

DEFAULT_IMAGE = "postgres:17-alpine"
DEFAULT_CONTAINER = "jo-fable-migration-db"
DEFAULT_PORT = 5439

# The single loopback bind address the disposable container may publish
# on, and the only hosts a validated connection string may name.
LOOPBACK_BIND = "127.0.0.1"
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

# Databases this tooling may create, migrate, seed, or drop follow the
# target repository's guarded branch-database convention (jo_local_*)
# with an explicit disposable infix and role suffix. Anything else —
# the shared dev database, test databases, previews, staging, prod —
# is structurally out of reach.
_DISPOSABLE_NAME = re.compile(
    r"^jo_local_fable_[a-z0-9_]+_(source|target)$"
)

# The local development password baked into the target repository's own
# docker compose file; it is a fixed public convention, not a secret.
_LOCAL_PASSWORD = "postgres"

ISOLATION_MARKER = ".fable-isolated-worktree"


class MigrationEnvRefusal(RuntimeError):
    """A safety precondition failed; the operation must not proceed."""


@dataclass(frozen=True, slots=True, repr=False)
class LoopbackDsn:
    """A validated loopback-only PostgreSQL connection identity.

    The password is carried only for command environment injection; it
    never appears in ``repr``, ``str``, or refusal messages.
    """

    host: str
    port: int
    database: str
    user: str
    _password: str

    @classmethod
    def parse(cls, raw: str) -> LoopbackDsn:
        parts = urlsplit(raw)
        if parts.scheme not in ("postgresql", "postgres"):
            raise MigrationEnvRefusal(
                "only postgresql connection strings are accepted"
            )
        host = parts.hostname or ""
        if host not in _LOOPBACK_HOSTS:
            raise MigrationEnvRefusal(
                f"host {host!r} is not loopback; refusing every "
                "non-local database"
            )
        database = parts.path.lstrip("/")
        if not database:
            raise MigrationEnvRefusal(
                "a database name is required; refusing the server default"
            )
        return cls(
            host=host,
            port=parts.port or 5432,
            database=database,
            user=parts.username or "postgres",
            _password=parts.password or _LOCAL_PASSWORD,
        )

    @property
    def url(self) -> str:
        return (
            f"postgresql://{self.user}:{self._password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def __repr__(self) -> str:
        return (
            f"LoopbackDsn(host={self.host!r}, port={self.port}, "
            f"database={self.database!r}, user={self.user!r})"
        )

    def __str__(self) -> str:
        return f"{self.host}:{self.port}/{self.database}"


def _slug_token(slug: str) -> str:
    token = re.sub(r"[^a-z0-9]+", "_", slug.strip().lower()).strip("_")
    if not token:
        raise MigrationEnvRefusal("a non-empty environment slug is required")
    return token


def disposable_pair(slug: str) -> tuple[str, str]:
    """The guarded source/target database names for one environment."""

    token = _slug_token(slug)
    return (
        f"jo_local_fable_{token}_source",
        f"jo_local_fable_{token}_target",
    )


def is_disposable_name(name: str) -> bool:
    """Whether this tooling is allowed to own the named database."""

    return _DISPOSABLE_NAME.fullmatch(name) is not None


def require_disposable(name: str) -> str:
    if not is_disposable_name(name):
        raise MigrationEnvRefusal(
            f"database {name!r} is not a guarded disposable name"
        )
    return name


@dataclass(frozen=True, slots=True)
class MigrationEnvConfig:
    """One disposable environment's complete identity."""

    repo_path: Path
    slug: str
    container: str = DEFAULT_CONTAINER
    port: int = DEFAULT_PORT
    image: str = DEFAULT_IMAGE

    @property
    def source_database(self) -> str:
        return disposable_pair(self.slug)[0]

    @property
    def target_database(self) -> str:
        return disposable_pair(self.slug)[1]

    def dsn(self, database: str) -> LoopbackDsn:
        return LoopbackDsn.parse(
            f"postgresql://postgres:{_LOCAL_PASSWORD}"
            f"@{LOOPBACK_BIND}:{self.port}/{require_disposable(database)}"
        )


OWNERSHIP_LABEL = "hermes.disposable"
OWNERSHIP_VALUE = "fable-migration-env"
SLUG_LABEL = "hermes.disposable.slug"

PROVISION_RECORD = ".fable-provision-record"

_CONTAINER_ID = re.compile(r"^[0-9a-f]{64}$")

_INSPECT_FORMAT = (
    "{{.Id}}\t{{.Config.Image}}\t"
    '{{index .Config.Labels "' + OWNERSHIP_LABEL + '"}}\t'
    '{{index .Config.Labels "' + SLUG_LABEL + '"}}\t'
    "{{.State.Running}}"
)


@dataclass(frozen=True, slots=True)
class ContainerIdentity:
    """One container's immutable identity and ownership evidence."""

    container_id: str
    image: str
    ownership: str
    slug: str
    running: bool


@dataclass(frozen=True, slots=True)
class ProvisionRecord:
    """The durable environment binding for one provisioned container.

    Written by provisioning immediately after the immutable container
    id is captured and validated, stored inside the isolated worktree
    (so it is environment-bound), consumed by the gate and by teardown,
    and cleared only by a completed teardown. Gate and teardown act on
    exactly this immutable id — never on a name — so a same-name,
    same-image, same-label replacement or another environment's
    container can never be mutated or deleted.
    """

    slug: str
    container: str
    container_id: str
    image: str
    port: int


def _record_path(config: MigrationEnvConfig) -> Path:
    return config.repo_path / PROVISION_RECORD


def write_provision_record(
    config: MigrationEnvConfig, container_id: str
) -> Path:
    if not _CONTAINER_ID.fullmatch(container_id):
        raise MigrationEnvRefusal(
            "the provision record requires one immutable 64-hex "
            "container id"
        )
    path = _record_path(config)
    path.write_text(
        f"slug={_slug_token(config.slug)}\n"
        f"container={config.container}\n"
        f"container_id={container_id}\n"
        f"image={config.image}\n"
        f"port={config.port}\n"
        f"created_at={datetime.now(UTC).isoformat()}\n"
    )
    return path


def load_provision_record(
    config: MigrationEnvConfig,
) -> ProvisionRecord | None:
    """Load this environment's record, or None when absent.

    A malformed record, or one bound to a different slug, container
    name, image, or port, is refused rather than reinterpreted: it is
    another environment's evidence, never this one's.
    """

    path = _record_path(config)
    if not path.is_file():
        return None
    fields: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
            fields[key.strip()] = value.strip()
    try:
        record = ProvisionRecord(
            slug=fields["slug"],
            container=fields["container"],
            container_id=fields["container_id"],
            image=fields["image"],
            port=int(fields["port"]),
        )
    except (KeyError, ValueError):
        raise MigrationEnvRefusal(
            "the provision record is malformed; refusing"
        ) from None
    expected = (
        _slug_token(config.slug),
        config.container,
        config.image,
        config.port,
    )
    actual = (record.slug, record.container, record.image, record.port)
    if actual != expected:
        raise MigrationEnvRefusal(
            "the provision record is bound to a different environment; "
            "refusing"
        )
    if not _CONTAINER_ID.fullmatch(record.container_id):
        raise MigrationEnvRefusal(
            "the provision record does not carry an immutable "
            "container id; refusing"
        )
    return record


def require_provision_record(config: MigrationEnvConfig) -> ProvisionRecord:
    record = load_provision_record(config)
    if record is None:
        raise MigrationEnvRefusal(
            "no provision record binds a container to this "
            "environment; refusing"
        )
    return record


def clear_provision_record(config: MigrationEnvConfig) -> None:
    path = _record_path(config)
    if path.is_file():
        path.unlink()


@dataclass(frozen=True, slots=True)
class EnvStep:
    """One externally observable command; planners never execute."""

    argv: tuple[str, ...]
    kind: Literal["probe", "mutate"]
    code: str


@dataclass(frozen=True, slots=True)
class EnvRunResult:
    returncode: int
    stdout: str
    stderr: str


class EnvCommandRunner(Protocol):
    """Boundary through which every external command executes.

    ``env`` entries are merged over the inherited environment; secrets
    travel only here, never in argv.
    """

    def run(
        self,
        argv: tuple[str, ...],
        *,
        timeout: float,
        env: dict[str, str] | None = None,
        cwd: Path | None = None,
    ) -> EnvRunResult: ...


class EnvSubprocessRunner:
    """Real list-argv execution, constructed only behind an explicit
    execute flag."""

    def run(
        self,
        argv: tuple[str, ...],
        *,
        timeout: float,
        env: dict[str, str] | None = None,
        cwd: Path | None = None,
    ) -> EnvRunResult:
        import os

        merged = None if env is None else {**os.environ, **env}
        try:
            completed = subprocess.run(
                list(argv),
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
                env=merged,
                cwd=cwd,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            return EnvRunResult(
                returncode=1, stdout="", stderr=type(error).__name__
            )
        return EnvRunResult(
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


@dataclass(frozen=True, slots=True)
class EnvActionReport:
    """The outcome of one semantically validated environment action."""

    completed: bool
    refusal_code: str | None
    records: tuple[tuple[str, int], ...]
    container_id: str | None = None


def provision_environment() -> dict[str, str]:
    """The environment injected into provisioning commands.

    The local development password is the target repository's own fixed
    docker-compose convention; it still travels only through the process
    environment, never through argv.
    """

    return {"POSTGRES_PASSWORD": _LOCAL_PASSWORD}


def inspect_container(
    runner: EnvCommandRunner, name: str, *, timeout: float = 120.0
) -> ContainerIdentity | None:
    """Read one container's immutable identity, or None when absent.

    Anything that does not parse as exactly one identity with a 64-hex
    immutable id is refused as ambiguous rather than guessed at.
    """

    result = runner.run(
        ("docker", "inspect", "--format", _INSPECT_FORMAT, name),
        timeout=timeout,
    )
    if result.returncode != 0:
        return None
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        raise MigrationEnvRefusal(
            "the container identity is ambiguous; refusing to act on it"
        )
    parts = lines[0].split("\t")
    if len(parts) != 5 or not _CONTAINER_ID.fullmatch(parts[0]):
        raise MigrationEnvRefusal(
            "the container identity could not be parsed as one "
            "immutable id; refusing to act on it"
        )
    return ContainerIdentity(
        container_id=parts[0],
        image=parts[1],
        ownership=parts[2],
        slug=parts[3],
        running=parts[4] == "true",
    )


def require_owned(
    identity: ContainerIdentity, config: MigrationEnvConfig
) -> ContainerIdentity:
    """Refuse any container this environment did not create.

    Ownership requires the exact ``hermes.disposable`` label value this
    tooling stamps at creation and the environment's exact image; a
    missing, foreign, or relabeled container — even under the expected
    name — is never started, mutated, or removed.
    """

    if identity.ownership != OWNERSHIP_VALUE:
        raise MigrationEnvRefusal(
            "the container is not owned by this tooling (missing or "
            "foreign ownership label); refusing"
        )
    if identity.image != config.image:
        raise MigrationEnvRefusal(
            "the container's image does not match this environment; "
            "refusing"
        )
    if identity.slug != _slug_token(config.slug):
        raise MigrationEnvRefusal(
            "the container belongs to a different environment slug; "
            "refusing"
        )
    return identity


def provision_disposable(
    config: MigrationEnvConfig,
    runner: EnvCommandRunner,
    *,
    timeout: float = 120.0,
) -> EnvActionReport:
    """Provision the disposable environment as one fail-closed
    ownership/identity transaction bound by a durable record.

    Reuse happens only through the environment's provision record and
    its recorded immutable id — never through the mutable name. With no
    usable recorded container, the name must be completely free: any
    container squatting it (a replacement, another environment's seat,
    an unrecorded look-alike) is refused untouched. Creation stamps the
    ownership and slug labels, captures the immutable id from ``docker
    run`` itself, revalidates it, and persists the record before any
    further step; the loopback proof, readiness, and both guarded
    database creations then address only that immutable id. Every
    semantic failure is a hard refusal regardless of exit codes.
    """

    records: list[tuple[str, int]] = []

    def step(
        code: str,
        argv: tuple[str, ...],
        env: dict[str, str] | None = None,
    ) -> EnvRunResult:
        result = runner.run(argv, timeout=timeout, env=env)
        records.append((code, result.returncode))
        return result

    def refuse(code: str) -> EnvActionReport:
        return EnvActionReport(
            completed=False, refusal_code=code, records=tuple(records)
        )

    docker = step(
        "docker_available",
        ("docker", "version", "--format", "{{.Server.Version}}"),
    )
    if docker.returncode != 0:
        return refuse("docker_available")

    try:
        record = load_provision_record(config)
    except MigrationEnvRefusal:
        records.append(("provision_record", 1))
        return refuse("provision_record")

    identity: ContainerIdentity | None = None
    if record is not None:
        # Reuse only through the durable binding: the recorded
        # immutable id, never the mutable name.
        try:
            identity = inspect_container(
                runner, record.container_id, timeout=timeout
            )
        except MigrationEnvRefusal:
            records.append(("container_identity", 1))
            return refuse("container_identity")
        if identity is not None:
            try:
                require_owned(identity, config)
            except MigrationEnvRefusal:
                records.append(("container_ownership", 1))
                return refuse("container_ownership")
            records.append(("container_ownership", 0))
            if not identity.running:
                started = step(
                    "container_start",
                    ("docker", "start", identity.container_id),
                )
                if started.returncode != 0:
                    return refuse("container_start")

    if identity is None:
        # No usable recorded container. The name must be free: any
        # container squatting it — a replacement, another environment's
        # seat, or an unrecorded look-alike — is refused untouched.
        try:
            by_name = inspect_container(
                runner, config.container, timeout=timeout
            )
        except MigrationEnvRefusal:
            records.append(("container_identity", 1))
            return refuse("container_identity")
        if by_name is not None:
            records.append(("container_conflict", 1))
            return refuse("container_conflict")
        created = step(
            "container_run",
            _run_argv(config),
            env=provision_environment(),
        )
        if created.returncode != 0:
            return refuse("container_run")
        # docker run prints the immutable id; from here on the name is
        # never used again.
        run_output = created.stdout.strip().splitlines()
        container_id = run_output[-1].strip() if run_output else ""
        if not _CONTAINER_ID.fullmatch(container_id):
            records.append(("container_identity", 1))
            return refuse("container_identity")
        try:
            identity = inspect_container(
                runner, container_id, timeout=timeout
            )
        except MigrationEnvRefusal:
            records.append(("container_identity", 1))
            return refuse("container_identity")
        if identity is None:
            records.append(("container_identity", 1))
            return refuse("container_identity")
        try:
            require_owned(identity, config)
        except MigrationEnvRefusal:
            records.append(("container_ownership", 1))
            return refuse("container_ownership")
        write_provision_record(config, identity.container_id)
        records.append(("provision_record", 0))
    records.append(("container_identity", 0))

    container_id = identity.container_id
    port = step("loopback_bindings", ("docker", "port", container_id))
    if port.returncode != 0:
        return refuse("loopback_bindings")
    proof = loopback_proof(port.stdout)
    records.append(("loopback_proof", 0 if proof.proven else 1))
    if not proof.proven:
        return refuse("loopback_proof")

    ready = step("postgres_ready", _ready_argv(container_id))
    if ready.returncode != 0:
        return refuse("postgres_ready")
    for role, database in (
        ("source", config.source_database),
        ("target", config.target_database),
    ):
        created = step(
            f"create_database_{role}",
            _create_argv(container_id, require_disposable(database)),
        )
        if created.returncode != 0:
            return refuse(f"create_database_{role}")
    return EnvActionReport(
        completed=True,
        refusal_code=None,
        records=tuple(records),
        container_id=container_id,
    )


def teardown_disposable(
    config: MigrationEnvConfig,
    runner: EnvCommandRunner,
    *,
    timeout: float = 120.0,
) -> EnvActionReport:
    """Remove exactly the recorded owned container, or nothing.

    Teardown consumes the environment's durable provision record and
    inspects only its recorded immutable id — a missing or foreign
    record, a vanished container, or any ownership/slug/image drift is
    refused and ``docker rm`` is never invoked. Removal targets the
    exact immutable id, and only a completed removal clears the record,
    so a same-name replacement or another environment's container can
    never be deleted.
    """

    records: list[tuple[str, int]] = []
    try:
        record = require_provision_record(config)
    except MigrationEnvRefusal:
        records.append(("provision_record", 1))
        return EnvActionReport(
            completed=False,
            refusal_code="provision_record",
            records=tuple(records),
        )
    try:
        identity = inspect_container(
            runner, record.container_id, timeout=timeout
        )
    except MigrationEnvRefusal:
        records.append(("container_identity", 1))
        return EnvActionReport(
            completed=False,
            refusal_code="container_identity",
            records=tuple(records),
        )
    if identity is None:
        records.append(("container_missing", 1))
        return EnvActionReport(
            completed=False,
            refusal_code="container_missing",
            records=tuple(records),
        )
    try:
        require_owned(identity, config)
    except MigrationEnvRefusal:
        records.append(("container_ownership", 1))
        return EnvActionReport(
            completed=False,
            refusal_code="container_ownership",
            records=tuple(records),
        )
    records.append(("container_ownership", 0))
    removed = runner.run(
        ("docker", "rm", "--force", "--volumes", identity.container_id),
        timeout=timeout,
    )
    records.append(("container_remove", removed.returncode))
    if removed.returncode == 0:
        clear_provision_record(config)
    return EnvActionReport(
        completed=removed.returncode == 0,
        refusal_code=(
            None if removed.returncode == 0 else "container_remove"
        ),
        records=tuple(records),
        container_id=identity.container_id,
    )


def _idempotent_create(database: str) -> str:
    """One shell line: create the database only when it is absent.

    ``psql -c`` cannot process ``\\gexec``, so the check-then-create
    composition runs inside the container's own shell. The line is a
    constant built purely from the validated disposable name — no
    credential and no caller-controlled text.
    """

    probe = (
        "psql -U postgres -tAc "
        f"\"SELECT 1 FROM pg_database WHERE datname = '{database}'\""
        " | grep -q 1"
    )
    create = (
        "psql -U postgres -v ON_ERROR_STOP=1 -c "
        f"'CREATE DATABASE \"{database}\"'"
    )
    return f"{probe} || {create}"


def _ready_argv(name: str) -> tuple[str, ...]:
    return (
        "docker",
        "exec",
        name,
        "sh",
        "-c",
        # First-boot initialization takes a few seconds; retry inside
        # the container instead of failing closed on a single shot.
        "for attempt in $(seq 1 30); do "
        "pg_isready -U postgres && exit 0; sleep 1; done; exit 1",
    )


def _create_argv(name: str, database: str) -> tuple[str, ...]:
    return ("docker", "exec", name, "sh", "-c", _idempotent_create(database))


def _run_argv(config: MigrationEnvConfig) -> tuple[str, ...]:
    return (
        "docker",
        "run",
        "--detach",
        "--name",
        config.container,
        "--label",
        f"{OWNERSHIP_LABEL}={OWNERSHIP_VALUE}",
        "--label",
        f"{SLUG_LABEL}={_slug_token(config.slug)}",
        "-e",
        "POSTGRES_USER=postgres",
        # Bare -e: docker reads the value from the injected process
        # environment; no credential enters argv.
        "-e",
        "POSTGRES_PASSWORD",
        "-p",
        f"{LOOPBACK_BIND}:{config.port}:5432",
        config.image,
    )


def plan_provision(config: MigrationEnvConfig) -> tuple[EnvStep, ...]:
    """The dry description of the provisioning transaction, in proof
    order.

    Execution goes through :func:`provision_disposable`, which
    additionally validates ownership and identity semantically between
    these commands and refuses foreign, relabeled, ambiguous, or
    wildcard-bound containers before any mutation.
    """

    source = require_disposable(config.source_database)
    target = require_disposable(config.target_database)
    steps: list[EnvStep] = [
        EnvStep(
            argv=("docker", "version", "--format", "{{.Server.Version}}"),
            kind="probe",
            code="docker_available",
        ),
        EnvStep(
            argv=("docker", "inspect", "--format", _INSPECT_FORMAT,
                  config.container),
            kind="probe",
            code="container_identity",
        ),
        EnvStep(
            argv=_run_argv(config),
            kind="mutate",
            code="container_run",
        ),
        EnvStep(
            argv=("docker", "port", config.container),
            kind="probe",
            code="loopback_bindings",
        ),
        EnvStep(
            argv=_ready_argv(config.container),
            kind="probe",
            code="postgres_ready",
        ),
    ]
    steps.extend(
        EnvStep(
            argv=_create_argv(config.container, database),
            kind="mutate",
            code=f"create_database_{role}",
        )
        for role, database in (("source", source), ("target", target))
    )
    return tuple(steps)


def plan_teardown(config: MigrationEnvConfig) -> tuple[EnvStep, ...]:
    """The dry description of the teardown transaction.

    Execution goes through :func:`teardown_disposable`, which removes
    only the revalidated owned container by its exact immutable id.
    """

    return (
        EnvStep(
            argv=("docker", "inspect", "--format", _INSPECT_FORMAT,
                  config.container),
            kind="probe",
            code="container_identity",
        ),
        EnvStep(
            argv=(
                "docker",
                "rm",
                "--force",
                "--volumes",
                "<validated-owned-container-id>",
            ),
            kind="mutate",
            code="container_remove",
        ),
    )


@dataclass(frozen=True, slots=True)
class LoopbackProof:
    """The outcome of proving the container's published bindings."""

    proven: bool
    evidence: str


def loopback_proof(port_output: str) -> LoopbackProof:
    """Prove every published binding is loopback from ``docker port``.

    An empty listing proves nothing (the container may not exist), and
    any binding outside 127.0.0.1 disproves isolation outright.
    """

    lines = [line.strip() for line in port_output.splitlines() if line.strip()]
    if not lines:
        return LoopbackProof(
            proven=False, evidence="no published bindings observed"
        )
    offending = [
        line
        for line in lines
        if f"-> {LOOPBACK_BIND}:" not in line
    ]
    if offending:
        return LoopbackProof(proven=False, evidence="; ".join(offending))
    return LoopbackProof(proven=True, evidence="; ".join(lines))


@dataclass(frozen=True, slots=True)
class WorktreeIdentity:
    """The live git identity of one checkout, straight from git."""

    git_dir: Path
    common_dir: Path

    @property
    def linked(self) -> bool:
        # A linked secondary worktree keeps its private git dir under
        # the primary repository's common dir; in the primary checkout
        # the two are the same directory.
        return self.git_dir != self.common_dir


def _worktree_identity(worktree: Path) -> WorktreeIdentity:
    if not worktree.is_dir():
        raise MigrationEnvRefusal(
            "the isolated worktree directory does not exist"
        )
    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(worktree),
                "rev-parse",
                "--absolute-git-dir",
                "--git-common-dir",
            ],
            capture_output=True,
            text=True,
            timeout=30.0,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise MigrationEnvRefusal(
            f"git identity could not be resolved: {type(error).__name__}"
        ) from None
    lines = completed.stdout.splitlines() if completed.returncode == 0 else []
    if completed.returncode != 0 or len(lines) < 2:
        raise MigrationEnvRefusal(
            "the path is not a git checkout; refusing to treat it as an "
            "isolated worktree"
        )
    git_dir = Path(lines[0]).resolve()
    common_raw = Path(lines[1])
    common_dir = (
        common_raw if common_raw.is_absolute() else worktree / common_raw
    ).resolve()
    return WorktreeIdentity(git_dir=git_dir, common_dir=common_dir)


def mark_isolated_worktree(worktree: Path, *, slug: str) -> Path:
    """Stamp a genuinely linked secondary worktree this tooling owns.

    The primary checkout is refused outright, and the marker binds the
    slug to the exact repository and worktree git identities so a
    copied, stale, or foreign marker can never validate elsewhere.
    """

    identity = _worktree_identity(worktree)
    if not identity.linked:
        raise MigrationEnvRefusal(
            "this is the repository's primary checkout; only a linked "
            "secondary worktree may be marked"
        )
    marker = worktree / ISOLATION_MARKER
    marker.write_text(
        f"slug={_slug_token(slug)}\n"
        f"repository={identity.common_dir}\n"
        f"worktree_git_dir={identity.git_dir}\n"
        f"created_at={datetime.now(UTC).isoformat()}\n"
        "purpose=INFRA-189 disposable migration environment\n"
    )
    return marker


def require_isolated_worktree(worktree: Path, *, slug: str) -> Path:
    """Refuse any checkout that is not a marked linked worktree.

    Proof requires all of: the live git identity says this is a linked
    secondary worktree (never the primary/common checkout), the marker
    parses, and its slug, repository, and worktree git identities all
    match the live values. Anything less — a missing, malformed,
    handcrafted, stale, foreign-repository, or wrong-slug marker — is
    refused before any repository command can run.
    """

    identity = _worktree_identity(worktree)
    if not identity.linked:
        raise MigrationEnvRefusal(
            "this is the repository's primary checkout; refusing to "
            "run against it"
        )
    marker = worktree / ISOLATION_MARKER
    if not marker.is_file():
        raise MigrationEnvRefusal(
            "this checkout is not marked as an isolated migration "
            "worktree; refusing to run against it"
        )
    fields: dict[str, str] = {}
    for line in marker.read_text().splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
            fields[key.strip()] = value.strip()
    expected = {
        "slug": _slug_token(slug),
        "repository": str(identity.common_dir),
        "worktree_git_dir": str(identity.git_dir),
    }
    for key, value in expected.items():
        if fields.get(key) != value:
            raise MigrationEnvRefusal(
                f"the isolation marker's {key} does not match this "
                "checkout; refusing stale or foreign evidence"
            )
    return marker
