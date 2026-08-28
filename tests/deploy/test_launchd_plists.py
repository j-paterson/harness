"""Tests for the pure launchd spec, plist, and newsyslog generation."""

import plistlib
import subprocess
from pathlib import Path, PurePosixPath

import pytest

from hermes_orchestrator import cli
from hermes_orchestrator.deploy.launchd import (
    LOOPBACK_HOST,
    LoopbackListener,
    ServiceSpec,
    UnsafeServiceSpec,
    ordered_labels,
    render_newsyslog_conf,
    render_plist,
    standard_inventory,
)

BINARY = PurePosixPath("/opt/hermes/bin/hermes-orchestrator")
CONFIG_REPO = PurePosixPath("/srv/hermes/config")
STATE_DIR = PurePosixPath("/srv/hermes/state")
LOG_DIR = PurePosixPath("/srv/hermes/logs")

FORBIDDEN_TERMS = ("token", "password", "api_key", "secret", "keychain", "signing")

EXPECTED_PLIST_KEYS = {
    "Label",
    "ProgramArguments",
    "WorkingDirectory",
    "RunAtLoad",
    "KeepAlive",
    "ThrottleInterval",
    "ProcessType",
    "ExitTimeOut",
    "StandardOutPath",
    "StandardErrorPath",
    "SoftResourceLimits",
}


def make_spec(**overrides: object) -> ServiceSpec:
    fields: dict[str, object] = {
        "label": "com.josystem.hermes-orchestrator",
        "program_arguments": (str(BINARY), "daemon"),
        "working_directory": STATE_DIR,
        "log_dir": LOG_DIR,
        "listener": None,
        "entrypoint_subcommand": "daemon",
    }
    fields.update(overrides)
    return ServiceSpec(**fields)  # type: ignore[arg-type]


def make_inventory(
    console_port: int = 8787,
) -> tuple[ServiceSpec, ServiceSpec]:
    return standard_inventory(
        binary=BINARY,
        config_repo=CONFIG_REPO,
        state_dir=STATE_DIR,
        log_dir=LOG_DIR,
        console_port=console_port,
    )


class TestSpecValidation:
    def test_valid_spec_constructs(self) -> None:
        spec = make_spec()
        assert spec.throttle_seconds == 30
        assert spec.depends_on == ()

    def test_label_outside_namespace(self) -> None:
        with pytest.raises(UnsafeServiceSpec) as excinfo:
            make_spec(label="com.example.other")
        assert excinfo.value.code == "label_outside_namespace"

    @pytest.mark.parametrize(
        "program_arguments",
        [(), ("hermes-orchestrator", "daemon"), ("bin/hermes", "daemon")],
    )
    def test_program_not_absolute(
        self, program_arguments: tuple[str, ...]
    ) -> None:
        with pytest.raises(UnsafeServiceSpec) as excinfo:
            make_spec(program_arguments=program_arguments)
        assert excinfo.value.code == "program_not_absolute"

    def test_throttle_below_floor(self) -> None:
        with pytest.raises(UnsafeServiceSpec) as excinfo:
            make_spec(throttle_seconds=29)
        assert excinfo.value.code == "throttle_below_floor"

    def test_throttle_at_floor_accepted(self) -> None:
        assert make_spec(throttle_seconds=30).throttle_seconds == 30

    @pytest.mark.parametrize(
        "program_arguments",
        [
            (str(BINARY), "serve-console"),
            (str(BINARY), "serve-console", "--host", "0.0.0.0", "--port", "8787"),
            (str(BINARY), "serve-console", "--host", LOOPBACK_HOST, "--port", "9999"),
            (
                str(BINARY),
                "serve-console",
                "--host",
                LOOPBACK_HOST,
                "extra",
                "--port",
                "8787",
            ),
        ],
    )
    def test_non_loopback_bind(self, program_arguments: tuple[str, ...]) -> None:
        with pytest.raises(UnsafeServiceSpec) as excinfo:
            make_spec(
                program_arguments=program_arguments,
                listener=LoopbackListener(port=8787),
            )
        assert excinfo.value.code == "non_loopback_bind"

    def test_listener_with_exact_quadruple_accepted(self) -> None:
        spec = make_spec(
            program_arguments=(
                str(BINARY),
                "serve-console",
                "--host",
                LOOPBACK_HOST,
                "--port",
                "8787",
            ),
            listener=LoopbackListener(port=8787),
        )
        assert spec.listener is not None
        assert spec.listener.port == 8787

    @pytest.mark.parametrize("marker", FORBIDDEN_TERMS)
    def test_environment_forbidden_markers(self, marker: str) -> None:
        with pytest.raises(UnsafeServiceSpec) as excinfo:
            make_spec(
                program_arguments=(str(BINARY), "daemon", f"--{marker.upper()}")
            )
        assert excinfo.value.code == "environment_forbidden"

    @pytest.mark.parametrize("port", [0, 80, 1023, 65536, 70000])
    def test_port_out_of_range(self, port: int) -> None:
        with pytest.raises(UnsafeServiceSpec) as excinfo:
            LoopbackListener(port=port)
        assert excinfo.value.code == "port_out_of_range"

    @pytest.mark.parametrize("port", [1024, 8787, 65535])
    def test_port_in_range_accepted(self, port: int) -> None:
        assert LoopbackListener(port=port).port == port

    def test_log_path_outside_log_dir_relative_log_dir(self) -> None:
        with pytest.raises(UnsafeServiceSpec) as excinfo:
            make_spec(log_dir=PurePosixPath("logs"))
        assert excinfo.value.code == "log_path_outside_log_dir"

    def test_log_path_outside_log_dir_label_traversal(self) -> None:
        with pytest.raises(UnsafeServiceSpec) as excinfo:
            make_spec(label="com.josystem.hermes-x/../escape")
        assert excinfo.value.code == "log_path_outside_log_dir"


class TestRenderPlist:
    def test_exact_key_set(self) -> None:
        document = plistlib.loads(render_plist(make_spec()))
        assert set(document) == EXPECTED_PLIST_KEYS

    def test_no_environment_variables_or_sockets(self) -> None:
        document = plistlib.loads(render_plist(make_spec()))
        assert "EnvironmentVariables" not in document
        assert "Sockets" not in document

    def test_keep_alive_is_failure_only_dict(self) -> None:
        document = plistlib.loads(render_plist(make_spec()))
        keep_alive = document["KeepAlive"]
        assert keep_alive is not True
        assert isinstance(keep_alive, dict)
        assert keep_alive == {"SuccessfulExit": False}

    def test_throttle_interval_default_floor(self) -> None:
        document = plistlib.loads(render_plist(make_spec()))
        assert document["ThrottleInterval"] == 30

    def test_throttle_interval_reflects_spec(self) -> None:
        document = plistlib.loads(render_plist(make_spec(throttle_seconds=45)))
        assert document["ThrottleInterval"] == 45

    def test_label_arguments_and_working_directory_round_trip(self) -> None:
        spec = make_spec()
        document = plistlib.loads(render_plist(spec))
        assert document["Label"] == spec.label
        assert document["ProgramArguments"] == list(spec.program_arguments)
        assert document["WorkingDirectory"] == str(STATE_DIR)

    def test_loopback_quadruple_present_for_listener_service(self) -> None:
        spec = make_spec(
            label="com.josystem.hermes-operations",
            program_arguments=(
                str(BINARY),
                "serve-console",
                "--host",
                LOOPBACK_HOST,
                "--port",
                "8787",
            ),
            listener=LoopbackListener(port=8787),
            entrypoint_subcommand="serve-console",
        )
        arguments = plistlib.loads(render_plist(spec))["ProgramArguments"]
        quadruple = ["--host", LOOPBACK_HOST, "--port", "8787"]
        windows = [arguments[i : i + 4] for i in range(len(arguments) - 3)]
        assert quadruple in windows

    def test_log_paths_under_log_dir(self) -> None:
        spec = make_spec()
        document = plistlib.loads(render_plist(spec))
        assert document["StandardOutPath"] == f"{LOG_DIR}/{spec.label}.out.log"
        assert document["StandardErrorPath"] == f"{LOG_DIR}/{spec.label}.err.log"

    def test_static_operational_fields(self) -> None:
        document = plistlib.loads(render_plist(make_spec()))
        assert document["SoftResourceLimits"] == {"NumberOfFiles": 256}
        assert document["ExitTimeOut"] == 15
        assert document["ProcessType"] == "Background"
        assert document["RunAtLoad"] is True


class TestStandardInventory:
    def test_two_services_in_dependency_order(self) -> None:
        inventory = make_inventory()
        assert len(inventory) == 2
        labels = tuple(spec.label for spec in inventory)
        assert labels == (
            "com.josystem.hermes-orchestrator",
            "com.josystem.hermes-operations",
        )

    def test_no_gateway_service_or_parameter(self) -> None:
        assert not any("gateway" in spec.label for spec in make_inventory())
        with pytest.raises(TypeError):
            standard_inventory(
                binary=BINARY,
                config_repo=CONFIG_REPO,
                state_dir=STATE_DIR,
                log_dir=LOG_DIR,
                gateway_port=8788,  # type: ignore[call-arg]
            )

    def test_orchestrator_shape(self) -> None:
        orchestrator = make_inventory()[0]
        assert orchestrator.program_arguments == (
            str(BINARY),
            "--repo-root",
            str(CONFIG_REPO),
            "--state-dir",
            str(STATE_DIR),
            "daemon",
            "--interval",
            "30",
        )
        assert orchestrator.listener is None
        assert orchestrator.depends_on == ()
        assert orchestrator.entrypoint_subcommand == "daemon"
        assert orchestrator.log_dir == LOG_DIR

    def test_operations_shape(self) -> None:
        operations = make_inventory()[1]
        assert operations.program_arguments == (
            str(BINARY),
            "--repo-root",
            str(CONFIG_REPO),
            "--state-dir",
            str(STATE_DIR),
            "serve-console",
            "--host",
            LOOPBACK_HOST,
            "--port",
            "8787",
        )
        assert operations.listener == LoopbackListener(port=8787)
        assert operations.depends_on == ("com.josystem.hermes-orchestrator",)
        assert operations.entrypoint_subcommand == "serve-console"
        assert operations.log_dir == LOG_DIR

    def test_custom_console_port_flows_through(self) -> None:
        _, operations = make_inventory(console_port=9001)
        assert operations.listener == LoopbackListener(port=9001)
        assert operations.program_arguments[-1] == "9001"


class TestOrderedLabels:
    def test_topological_order_for_standard_inventory(self) -> None:
        assert ordered_labels(make_inventory()) == (
            "com.josystem.hermes-orchestrator",
            "com.josystem.hermes-operations",
        )

    def test_topological_order_independent_of_input_order(self) -> None:
        shuffled = tuple(reversed(make_inventory()))
        assert ordered_labels(shuffled) == (
            "com.josystem.hermes-orchestrator",
            "com.josystem.hermes-operations",
        )

    def test_duplicate_label_refused(self) -> None:
        inventory = (make_spec(), make_spec())
        with pytest.raises(UnsafeServiceSpec) as excinfo:
            ordered_labels(inventory)
        assert excinfo.value.code == "duplicate_label"

    def test_unknown_dependency_refused(self) -> None:
        inventory = (
            make_spec(depends_on=("com.josystem.hermes-missing",)),
        )
        with pytest.raises(UnsafeServiceSpec) as excinfo:
            ordered_labels(inventory)
        assert excinfo.value.code == "unknown_dependency"

    def test_dependency_cycle_refused(self) -> None:
        spec_a = make_spec(
            label="com.josystem.hermes-a",
            depends_on=("com.josystem.hermes-b",),
        )
        spec_b = make_spec(
            label="com.josystem.hermes-b",
            depends_on=("com.josystem.hermes-a",),
        )
        with pytest.raises(UnsafeServiceSpec) as excinfo:
            ordered_labels((spec_a, spec_b))
        assert excinfo.value.code == "dependency_cycle"


class TestNewsyslogConf:
    def test_one_line_per_log_file_with_bounded_rotation(self) -> None:
        inventory = make_inventory()
        lines = render_newsyslog_conf(inventory).splitlines()
        assert len(lines) == 4
        expected = []
        for spec in inventory:
            for suffix in ("out", "err"):
                path = f"{LOG_DIR}/{spec.label}.{suffix}.log"
                expected.append(f"{path} 640 5 10240 * J")
        assert lines == expected

    def test_every_plist_log_path_is_covered(self) -> None:
        inventory = make_inventory()
        conf = render_newsyslog_conf(inventory)
        for spec in inventory:
            document = plistlib.loads(render_plist(spec))
            assert document["StandardOutPath"] in conf
            assert document["StandardErrorPath"] in conf


class TestRealPlutilLint:
    def test_every_rendered_plist_passes_plutil_lint(self, tmp_path: Path) -> None:
        inventory = make_inventory()
        assert len(inventory) == 2
        for spec in inventory:
            plist_path = tmp_path / f"{spec.label}.plist"
            plist_path.write_bytes(render_plist(spec))
            completed = subprocess.run(
                ["plutil", "-lint", str(plist_path)],
                capture_output=True,
                text=True,
                check=False,
            )
            assert completed.returncode == 0, (spec.label, completed.stderr)


class TestEntrypointSubcommands:
    @pytest.mark.parametrize(
        "subcommand",
        [spec.entrypoint_subcommand for spec in make_inventory()],
    )
    def test_entrypoint_help_exits_zero_through_real_parser(
        self, subcommand: str
    ) -> None:
        with pytest.raises(SystemExit) as excinfo:
            cli.main([subcommand, "--help"])
        assert excinfo.value.code == 0


class TestForbiddenTerms:
    @pytest.mark.parametrize("term", FORBIDDEN_TERMS)
    def test_serialized_plists_never_contain_term(self, term: str) -> None:
        for spec in make_inventory():
            payload = render_plist(spec).lower()
            assert term.encode("ascii") not in payload

    @pytest.mark.parametrize("term", FORBIDDEN_TERMS)
    def test_newsyslog_conf_never_contains_term(self, term: str) -> None:
        conf = render_newsyslog_conf(make_inventory()).lower()
        assert term not in conf
