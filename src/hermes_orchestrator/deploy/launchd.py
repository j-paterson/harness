"""Pure launchd service specs, plist rendering, and newsyslog generation."""

import plistlib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath

LOOPBACK_HOST = "127.0.0.1"

_LABEL_NAMESPACE = "com.josystem.hermes"
_THROTTLE_FLOOR = 30
_PORT_MIN = 1024
_PORT_MAX = 65535
_FORBIDDEN_MARKERS = ("token", "password", "api_key", "secret", "keychain", "signing")

_ORCHESTRATOR_LABEL = "com.josystem.hermes-orchestrator"
_OPERATIONS_LABEL = "com.josystem.hermes-operations"
# The supervised daemon job's public label: the activation apply
# protocol kickstarts exactly this job.
ORCHESTRATOR_LABEL = _ORCHESTRATOR_LABEL

BOOTSTRAP_FILENAME = "hermes-bootstrap"

# launchd provides a minimal environment; the rendered bootstrap pins
# the operator PATH the runtime needs (claude, uv, codex, git).
BOOTSTRAP_PATH = (
    "/Applications/Codex.app/Contents/Resources:"
    "$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:"
    "/usr/bin:/bin:/usr/sbin:/sbin"
)


def render_bootstrap(
    *,
    uv_binary: PurePosixPath,
    config_repo: PurePosixPath,
    state_dir: PurePosixPath,
) -> str:
    """Render the durable, worktree-independent launchd entry script.

    The script resolves the durable active runtime activation from the
    state database READ-ONLY (never migrating anything) and execs the
    CLI from that immutable artifact; with no activation yet, the
    durable config clone is the bootstrap fallback. It never references
    any disposable issue worktree.
    """

    return (
        "#!/bin/sh\n"
        "# Rendered by hermes-orchestrator deploy-render; installed by\n"
        "# deploy-install. Do not edit by hand.\n"
        f"export PATH={BOOTSTRAP_PATH}\n"
        "ACTIVE=$(/usr/bin/sqlite3 -readonly "
        f'"{state_dir}/state.db" '
        '"SELECT checkout_root FROM runtime_activations '
        "WHERE state = 'active'\" 2>/dev/null || true)\n"
        f'PROJECT="{config_repo}"\n'
        'if [ -n "$ACTIVE" ] && [ -f "$ACTIVE/pyproject.toml" ]; then\n'
        '  PROJECT="$ACTIVE"\n'
        "fi\n"
        f'exec {uv_binary} run --project "$PROJECT" hermes-orchestrator '
        '"$@"\n'
    )


class UnsafeServiceSpec(Exception):
    """A service specification violated a safety invariant; static code only."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class LoopbackListener:
    """A loopback-only TCP listener; the host is always ``LOOPBACK_HOST``."""

    port: int

    def __post_init__(self) -> None:
        if not _PORT_MIN <= self.port <= _PORT_MAX:
            raise UnsafeServiceSpec("port_out_of_range")


@dataclass(frozen=True, slots=True, kw_only=True)
class ServiceSpec:
    """A validated launchd service specification; construction fails closed."""

    label: str
    program_arguments: tuple[str, ...]
    working_directory: PurePosixPath
    log_dir: PurePosixPath
    listener: LoopbackListener | None
    depends_on: tuple[str, ...] = ()
    throttle_seconds: int = 30
    entrypoint_subcommand: str

    def __post_init__(self) -> None:
        if not self.label.startswith(_LABEL_NAMESPACE):
            raise UnsafeServiceSpec("label_outside_namespace")
        if not self.program_arguments or not PurePosixPath(
            self.program_arguments[0]
        ).is_absolute():
            raise UnsafeServiceSpec("program_not_absolute")
        if self.throttle_seconds < _THROTTLE_FLOOR:
            raise UnsafeServiceSpec("throttle_below_floor")
        if self.listener is not None:
            quadruple = ("--host", LOOPBACK_HOST, "--port", str(self.listener.port))
            arguments = self.program_arguments
            windows = (
                arguments[index : index + 4] for index in range(len(arguments) - 3)
            )
            if quadruple not in windows:
                raise UnsafeServiceSpec("non_loopback_bind")
        for argument in self.program_arguments:
            lowered = argument.lower()
            if any(marker in lowered for marker in _FORBIDDEN_MARKERS):
                raise UnsafeServiceSpec("environment_forbidden")
        if not self.log_dir.is_absolute():
            raise UnsafeServiceSpec("log_path_outside_log_dir")
        for log_path in (self.stdout_path, self.stderr_path):
            if log_path.parent != self.log_dir:
                raise UnsafeServiceSpec("log_path_outside_log_dir")

    @property
    def stdout_path(self) -> PurePosixPath:
        return self.log_dir / f"{self.label}.out.log"

    @property
    def stderr_path(self) -> PurePosixPath:
        return self.log_dir / f"{self.label}.err.log"


def render_plist(spec: ServiceSpec) -> bytes:
    """Serialize the spec to launchd plist bytes with exactly the safe key set."""
    return plistlib.dumps(
        {
            "Label": spec.label,
            "ProgramArguments": list(spec.program_arguments),
            "WorkingDirectory": str(spec.working_directory),
            "RunAtLoad": True,
            "KeepAlive": {"SuccessfulExit": False},
            "ThrottleInterval": spec.throttle_seconds,
            "ProcessType": "Background",
            "ExitTimeOut": 15,
            "StandardOutPath": str(spec.stdout_path),
            "StandardErrorPath": str(spec.stderr_path),
            "SoftResourceLimits": {"NumberOfFiles": 256},
        }
    )


def standard_inventory(
    *,
    binary: PurePosixPath,
    config_repo: PurePosixPath,
    state_dir: PurePosixPath,
    log_dir: PurePosixPath,
    console_port: int = 8787,
) -> tuple[ServiceSpec, ServiceSpec]:
    """Return the two Hermes service specs in dependency order."""
    common = (
        str(binary),
        "--repo-root",
        str(config_repo),
        "--state-dir",
        str(state_dir),
    )
    orchestrator = ServiceSpec(
        label=_ORCHESTRATOR_LABEL,
        # The externally supervised job resolves the durable active
        # runtime activation and execs the daemon from that exact
        # checkout (INFRA-195): launchd owns the process lifetime,
        # while which code runs is decided by the validated activation
        # ledger, never by which pane or worktree launched something.
        program_arguments=(*common, "runtime-exec", "--interval", "30"),
        working_directory=state_dir,
        log_dir=log_dir,
        listener=None,
        entrypoint_subcommand="runtime-exec",
    )
    operations = ServiceSpec(
        label=_OPERATIONS_LABEL,
        program_arguments=(
            *common,
            "serve-console",
            "--host",
            LOOPBACK_HOST,
            "--port",
            str(console_port),
        ),
        working_directory=state_dir,
        log_dir=log_dir,
        listener=LoopbackListener(port=console_port),
        depends_on=(_ORCHESTRATOR_LABEL,),
        entrypoint_subcommand="serve-console",
    )
    return (orchestrator, operations)


def ordered_labels(inventory: Sequence[ServiceSpec]) -> tuple[str, ...]:
    """Topological start order (stop order is the reverse); fails closed."""
    labels = [spec.label for spec in inventory]
    if len(set(labels)) != len(labels):
        raise UnsafeServiceSpec("duplicate_label")
    known = set(labels)
    for spec in inventory:
        for dependency in spec.depends_on:
            if dependency not in known:
                raise UnsafeServiceSpec("unknown_dependency")
    remaining = {spec.label: set(spec.depends_on) for spec in inventory}
    ordered: list[str] = []
    while remaining:
        ready = [
            label
            for label in labels
            if label in remaining and not remaining[label]
        ]
        if not ready:
            raise UnsafeServiceSpec("dependency_cycle")
        for label in ready:
            ordered.append(label)
            del remaining[label]
        for dependencies in remaining.values():
            dependencies.difference_update(ready)
    return tuple(ordered)


def render_newsyslog_conf(inventory: Sequence[ServiceSpec]) -> str:
    """Render one bounded-rotation newsyslog line per service log file."""
    lines = []
    for spec in inventory:
        for log_path in (spec.stdout_path, spec.stderr_path):
            lines.append(f"{log_path} 640 5 10240 * J")
    return "\n".join(lines) + "\n"
