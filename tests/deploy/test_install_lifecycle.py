"""Install/serve-enable/uninstall/status plans stay fail-closed and fake-driven."""

from __future__ import annotations

import hashlib
import json
import plistlib
import subprocess
from pathlib import Path, PurePosixPath

import pytest

from hermes_orchestrator import cli
from hermes_orchestrator.deploy import lifecycle
from hermes_orchestrator.deploy.launchd import render_plist, standard_inventory
from hermes_orchestrator.deploy.lifecycle import (
    ExecutionReport,
    RunResult,
    execute_plan,
    plan_install,
    plan_status,
    plan_uninstall,
    render_artifacts,
)
from hermes_orchestrator.deploy.tailscale import (
    funnel_status_argv,
    serve_enable_argv,
    serve_reset_argv,
    serve_status_argv,
)

BINARY = PurePosixPath("/opt/hermes/bin/hermes-orchestrator")
CONFIG_REPO = PurePosixPath("/opt/hermes/config")
STATE_DIR = PurePosixPath("/opt/hermes/state")
LOG_DIR = PurePosixPath("/opt/hermes/logs")
RENDERED = PurePosixPath("/opt/hermes/rendered")
AGENTS_DIR = PurePosixPath("/opt/hermes/launch-agents")
UID = 501
LABELS = (
    "com.josystem.hermes-orchestrator",
    "com.josystem.hermes-operations",
)
ABSENT_SERVE = "null"
ACTIVE_SERVE = json.dumps(
    {
        "Web": {
            "host.tailnet.ts.net:443": {
                "Handlers": {"/": {"Proxy": "http://127.0.0.1:8787"}}
            }
        },
        "AllowFunnel": {"host.tailnet.ts.net:443": False},
    }
)
FUNNEL_SERVE = json.dumps({"AllowFunnel": {"host.tailnet.ts.net:443": True}})
FOREIGN_SERVE = json.dumps(
    {
        "Web": {
            "host.tailnet.ts.net:443": {
                "Handlers": {"/": {"Proxy": "http://127.0.0.1:8787"}}
            },
            "host.tailnet.ts.net:8443": {
                "Handlers": {"/": {"Proxy": "http://127.0.0.1:9999"}}
            },
        }
    }
)
FORBIDDEN_ARTIFACT_TERMS = (
    "token",
    "password",
    "api_key",
    "secret",
    "keychain",
    "signing",
)

# plan_install layout: 2 entrypoint probes, 2 plutil probes, 2 plist-absent
# probes, 2 label-absent probes, serve pre, funnel, then install/bootstrap
# per service in dependency order.
INSTALL_MUTATE_INDICES = (10, 11, 12, 13)


def _inventory():
    return standard_inventory(
        binary=BINARY,
        config_repo=CONFIG_REPO,
        state_dir=STATE_DIR,
        log_dir=LOG_DIR,
    )


def _install_plan():
    return plan_install(
        _inventory(),
        rendered_dir=RENDERED,
        launch_agents_dir=AGENTS_DIR,
        uid=UID,
        console_port=8787,
    )


def _serve_plan():
    return lifecycle.plan_serve_enable(console_port=8787)


class FakeRunner:
    """Records every argv; replays scripted results, defaulting to success."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.scripts: dict[tuple[str, ...], list[RunResult]] = {}

    def script(self, argv: tuple[str, ...], *results: RunResult) -> None:
        self.scripts.setdefault(argv, []).extend(results)

    def run(self, argv, *, timeout: float) -> RunResult:
        assert timeout > 0
        call = tuple(argv)
        self.calls.append(call)
        queued = self.scripts.get(call)
        if queued:
            return queued.pop(0)
        return RunResult(returncode=0, stdout="", stderr="")


class PoisonedRunner:
    """Fails the test if any command is ever executed."""

    def run(self, argv, *, timeout: float) -> RunResult:
        raise AssertionError(f"live runner invoked: {argv!r}")


class FilesystemRunner:
    """Models cp/rm/test/launchctl against a real tmp filesystem in Python.

    No subprocess is ever spawned: file commands act on real tmp paths and
    launchctl acts on an internal loaded-labels set. ``partial_cp_dst``
    injects a cp that writes half the bytes then fails; ``fail_bootstrap``
    injects a bootstrap that loads the label then fails.

    ``loaded_paths`` maps a loaded label to the plist path it was
    bootstrapped from, mirrored in ``launchctl print`` output. A label in
    ``loaded_labels`` without a path models an operator-managed job: it
    prints a foreign backing path.
    """

    def __init__(
        self,
        *,
        loaded_labels: set[str] | None = None,
        loaded_paths: dict[str, str] | None = None,
        partial_cp_dst: str | None = None,
        fail_bootstrap: str | None = None,
    ) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.loaded_paths: dict[str, str] = dict(loaded_paths or {})
        self.loaded: set[str] = set(loaded_labels or ()) | set(self.loaded_paths)
        self.partial_cp_dst = partial_cp_dst
        self.fail_bootstrap = fail_bootstrap
        self.scripts: dict[tuple[str, ...], list[RunResult]] = {}
        self.on_call = None

    def script(self, argv: tuple[str, ...], *results: RunResult) -> None:
        self.scripts.setdefault(argv, []).extend(results)

    def run(self, argv, *, timeout: float) -> RunResult:
        assert timeout > 0
        call = tuple(argv)
        self.calls.append(call)
        if self.on_call is not None:
            self.on_call(call)
        queued = self.scripts.get(call)
        if queued:
            return queued.pop(0)
        if call[0] == "test" and call[1] == "-e":
            exists = Path(call[2]).exists()
            return RunResult(returncode=0 if exists else 1, stdout="", stderr="")
        if call[0] == "cp":
            data = Path(call[1]).read_bytes()
            if call[2] == self.partial_cp_dst:
                Path(call[2]).write_bytes(data[: len(data) // 2])
                return RunResult(returncode=1, stdout="", stderr="short write")
            Path(call[2]).write_bytes(data)
            return RunResult(returncode=0, stdout="", stderr="")
        if call[0] == "rm":
            Path(call[1]).unlink()
            return RunResult(returncode=0, stdout="", stderr="")
        if call[0] == "launchctl" and call[1] == "print":
            label = call[2].rsplit("/", 1)[-1]
            if label not in self.loaded:
                return RunResult(returncode=113, stdout="", stderr="")
            path = self.loaded_paths.get(
                label, f"/Library/OperatorManaged/{label}.plist"
            )
            return RunResult(
                returncode=0,
                stdout=f"state = running\n\tpath = {path}\n",
                stderr="",
            )
        if call[0] == "launchctl" and call[1] == "bootstrap":
            label = Path(call[3]).stem
            self.loaded.add(label)
            self.loaded_paths[label] = call[3]
            if label == self.fail_bootstrap:
                return RunResult(returncode=1, stdout="", stderr="denied")
            return RunResult(returncode=0, stdout="", stderr="")
        if call[0] == "launchctl" and call[1] == "bootout":
            label = call[2].rsplit("/", 1)[-1]
            self.loaded.discard(label)
            self.loaded_paths.pop(label, None)
            return RunResult(returncode=0, stdout="", stderr="")
        if call[:3] == ("shasum", "-a", "256"):
            target = Path(call[3])
            if not target.exists():
                return RunResult(
                    returncode=1, stdout="", stderr="No such file or directory"
                )
            digest = hashlib.sha256(target.read_bytes()).hexdigest()
            return RunResult(
                returncode=0, stdout=f"{digest}  {target}\n", stderr=""
            )
        raise AssertionError(f"unhandled argv: {call!r}")


def _happy_install_runner() -> FakeRunner:
    runner = FakeRunner()
    runner.script(
        serve_status_argv(),
        RunResult(returncode=0, stdout=ABSENT_SERVE, stderr=""),
    )
    runner.script(
        funnel_status_argv(),
        RunResult(returncode=0, stdout="No serve config", stderr=""),
    )
    # The unscripted FakeRunner default of rc=0 means "exists"/"loaded" for
    # the absence probes, so the happy path scripts both as absent.
    for label in LABELS:
        runner.script(
            ("test", "-e", str(AGENTS_DIR / f"{label}.plist")),
            RunResult(returncode=1, stdout="", stderr=""),
        )
        runner.script(
            ("launchctl", "print", f"gui/{UID}/{label}"),
            RunResult(returncode=113, stdout="", stderr="not found"),
        )
    return runner


def _happy_serve_runner() -> FakeRunner:
    runner = FakeRunner()
    runner.script(
        serve_status_argv(),
        RunResult(returncode=0, stdout=ABSENT_SERVE, stderr=""),
        RunResult(returncode=0, stdout=ACTIVE_SERVE, stderr=""),
    )
    runner.script(
        funnel_status_argv(),
        RunResult(returncode=0, stdout="No serve config", stderr=""),
    )
    return runner


def _mutate_calls(runner: FakeRunner) -> list[tuple[str, ...]]:
    mutates = [
        call
        for call in runner.calls
        if call[0] in {"cp", "rm"}
        or (call[0] == "launchctl" and call[1] in {"bootstrap", "bootout"})
    ]
    if serve_enable_argv(8787) in runner.calls:
        mutates.append(serve_enable_argv(8787))
    if serve_reset_argv() in runner.calls:
        mutates.append(serve_reset_argv())
    return mutates


def test_install_plan_codes_probe_before_mutate() -> None:
    steps = _install_plan()
    assert [step.code for step in steps] == [
        "probe_entrypoint",
        "probe_entrypoint",
        "probe_plutil_lint",
        "probe_plutil_lint",
        "probe_plist_absent",
        "probe_plist_absent",
        "probe_label_absent",
        "probe_label_absent",
        "probe_serve_state_pre",
        "probe_funnel_state",
        "mutate_install_plist",
        "mutate_bootstrap",
        "mutate_install_plist",
        "mutate_bootstrap",
    ]
    kinds = [step.kind for step in steps]
    first_mutate = kinds.index("mutate")
    assert all(kind == "probe" for kind in kinds[:first_mutate])
    assert [index for index, kind in enumerate(kinds) if kind == "mutate"] == list(
        INSTALL_MUTATE_INDICES
    )


def test_install_plan_argv_details_and_compensations() -> None:
    steps = _install_plan()
    probes = [step for step in steps if step.code == "probe_entrypoint"]
    assert [step.argv for step in probes] == [
        (str(BINARY), "daemon", "--help"),
        (str(BINARY), "serve-console", "--help"),
    ]
    lints = [step.argv for step in steps if step.code == "probe_plutil_lint"]
    assert lints == [
        ("plutil", "-lint", str(RENDERED / f"{label}.plist")) for label in LABELS
    ]
    absences = [step.argv for step in steps if step.code == "probe_plist_absent"]
    assert absences == [
        ("test", "-e", str(AGENTS_DIR / f"{label}.plist")) for label in LABELS
    ]
    loaded = [step.argv for step in steps if step.code == "probe_label_absent"]
    assert loaded == [
        ("launchctl", "print", f"gui/{UID}/{label}") for label in LABELS
    ]
    for step in steps:
        if step.kind == "probe":
            assert step.compensation_argv is None
            assert step.compensation_code is None
            assert step.reconcile_argv is None
            assert step.reconcile_code is None
            assert step.resource is None
    installs = [step for step in steps if step.code == "mutate_install_plist"]
    assert [step.argv for step in installs] == [
        ("cp", str(RENDERED / f"{label}.plist"), str(AGENTS_DIR / f"{label}.plist"))
        for label in LABELS
    ]
    assert [step.compensation_argv for step in installs] == [
        ("rm", str(AGENTS_DIR / f"{label}.plist")) for label in LABELS
    ]
    assert all(
        step.compensation_code == "compensate_remove_plist" for step in installs
    )
    assert [step.reconcile_argv for step in installs] == [
        ("test", "-e", str(AGENTS_DIR / f"{label}.plist")) for label in LABELS
    ]
    assert all(
        step.reconcile_code == "reconcile_install_plist" for step in installs
    )
    assert [step.resource for step in installs] == [
        str(AGENTS_DIR / f"{label}.plist") for label in LABELS
    ]
    bootstraps = [step for step in steps if step.code == "mutate_bootstrap"]
    assert [step.argv for step in bootstraps] == [
        (
            "launchctl",
            "bootstrap",
            f"gui/{UID}",
            str(AGENTS_DIR / f"{label}.plist"),
        )
        for label in LABELS
    ]
    assert [step.compensation_argv for step in bootstraps] == [
        ("launchctl", "bootout", f"gui/{UID}/{label}") for label in LABELS
    ]
    assert all(step.compensation_code == "compensate_bootout" for step in bootstraps)
    assert [step.reconcile_argv for step in bootstraps] == [
        ("launchctl", "print", f"gui/{UID}/{label}") for label in LABELS
    ]
    assert all(step.reconcile_code == "reconcile_bootstrap" for step in bootstraps)
    assert [step.resource for step in bootstraps] == [
        f"gui/{UID}/{label}" for label in LABELS
    ]


def test_install_plan_has_no_serve_mutation() -> None:
    codes = {step.code for step in _install_plan()}
    assert "mutate_serve_enable" not in codes
    assert "probe_serve_state_post" not in codes


def test_serve_enable_plan_shape() -> None:
    steps = _serve_plan()
    assert [step.code for step in steps] == [
        "probe_serve_state_pre",
        "probe_funnel_state",
        "mutate_serve_enable",
        "probe_serve_state_post",
    ]
    assert [step.kind for step in steps] == ["probe", "probe", "mutate", "probe"]
    assert steps[0].argv == serve_status_argv()
    assert steps[1].argv == funnel_status_argv()
    assert steps[2].argv == serve_enable_argv(8787)
    assert steps[2].compensation_argv == serve_reset_argv()
    assert steps[2].compensation_code == "compensate_serve_reset"
    assert steps[3].argv == serve_status_argv()


def test_uninstall_plan_is_exact_reverse_with_serve_reset_first() -> None:
    steps = plan_uninstall(
        _inventory(),
        launch_agents_dir=AGENTS_DIR,
        uid=UID,
        console_port=8787,
    )
    assert [step.code for step in steps] == [
        "mutate_serve_reset",
        "mutate_bootout",
        "mutate_remove_plist",
        "mutate_bootout",
        "mutate_remove_plist",
    ]
    assert steps[0].argv == serve_reset_argv()
    bootouts = [step.argv for step in steps if step.code == "mutate_bootout"]
    assert bootouts == [
        ("launchctl", "bootout", f"gui/{UID}/{label}")
        for label in reversed(LABELS)
    ]
    removals = [step.argv for step in steps if step.code == "mutate_remove_plist"]
    assert removals == [
        ("rm", str(AGENTS_DIR / f"{label}.plist")) for label in reversed(LABELS)
    ]


def test_status_plan_is_read_only() -> None:
    steps = plan_status(_inventory(), uid=UID, console_port=8787)
    assert all(step.kind == "probe" for step in steps)
    assert [step.code for step in steps] == [
        "probe_service_status",
        "probe_service_status",
        "probe_serve_state_pre",
        "probe_funnel_state",
    ]
    prints = [step.argv for step in steps if step.code == "probe_service_status"]
    assert prints == [
        ("launchctl", "print", f"gui/{UID}/{label}") for label in LABELS
    ]


def test_execute_stops_on_entrypoint_probe_failure_without_mutation() -> None:
    runner = _happy_install_runner()
    runner.script(
        (str(BINARY), "serve-console", "--help"),
        RunResult(returncode=2, stdout="", stderr="unknown command"),
    )
    report = execute_plan(_install_plan(), runner, console_port=8787)
    assert isinstance(report, ExecutionReport)
    assert report.completed is False
    assert report.refusal_code == "probe_entrypoint"
    assert _mutate_calls(runner) == []
    assert report.compensations == ()
    assert report.residual_codes == ()


def test_execute_stops_on_plutil_failure_without_mutation() -> None:
    runner = _happy_install_runner()
    runner.script(
        ("plutil", "-lint", str(RENDERED / f"{LABELS[0]}.plist")),
        RunResult(returncode=1, stdout="", stderr="broken"),
    )
    report = execute_plan(_install_plan(), runner, console_port=8787)
    assert report.completed is False
    assert report.refusal_code == "probe_plutil_lint"
    assert _mutate_calls(runner) == []
    assert report.compensations == ()


def test_execute_stops_when_serve_state_unavailable() -> None:
    runner = _happy_install_runner()
    runner.scripts[serve_status_argv()] = [
        RunResult(returncode=1, stdout="", stderr="tailscaled not running")
    ]
    report = execute_plan(_install_plan(), runner, console_port=8787)
    assert report.completed is False
    assert report.refusal_code == "serve_state_unavailable"
    assert _mutate_calls(runner) == []
    assert report.compensations == ()


def test_execute_stops_when_funnel_enabled() -> None:
    runner = _happy_install_runner()
    runner.scripts[serve_status_argv()] = [
        RunResult(returncode=0, stdout=FUNNEL_SERVE, stderr="")
    ]
    report = execute_plan(_install_plan(), runner, console_port=8787)
    assert report.completed is False
    assert report.refusal_code == "funnel_enabled"
    assert _mutate_calls(runner) == []
    assert report.compensations == ()


def test_execute_stops_when_funnel_state_uncertain() -> None:
    runner = _happy_install_runner()
    runner.scripts[funnel_status_argv()] = [
        RunResult(returncode=0, stdout="# Funnel on:\nhttps://x", stderr="")
    ]
    report = execute_plan(_install_plan(), runner, console_port=8787)
    assert report.completed is False
    assert report.refusal_code == "funnel_state_uncertain"
    assert _mutate_calls(runner) == []
    assert report.compensations == ()


@pytest.mark.parametrize("fail_index", INSTALL_MUTATE_INDICES)
def test_install_mutate_failure_compensates_in_exact_reverse_order(
    fail_index: int,
) -> None:
    plan = _install_plan()
    fail_step = plan[fail_index]
    assert fail_step.kind == "mutate"
    runner = _happy_install_runner()
    runner.script(
        fail_step.argv, RunResult(returncode=1, stdout="", stderr="denied")
    )
    report = execute_plan(plan, runner, console_port=8787)
    # The reconcile probe is unscripted (rc=0), so the failed mutation is
    # deemed partially applied and its own compensation runs first.
    rolled_back = [fail_step, *reversed(
        [step for step in plan[:fail_index] if step.kind == "mutate"]
    )]
    assert runner.calls == (
        [step.argv for step in plan[: fail_index + 1]]
        + [fail_step.reconcile_argv]
        + [step.compensation_argv for step in rolled_back]
    )
    assert report.completed is False
    assert report.refusal_code == fail_step.code
    assert report.records[-1] == (fail_step.reconcile_code, 0)
    assert report.compensations == tuple(
        (step.compensation_code, 0) for step in rolled_back
    )
    assert report.residual_codes == ()


def test_serve_enable_mutate_failure_has_nothing_to_compensate() -> None:
    plan = _serve_plan()
    runner = _happy_serve_runner()
    runner.script(
        serve_enable_argv(8787),
        RunResult(returncode=1, stdout="", stderr="denied"),
    )
    report = execute_plan(plan, runner, console_port=8787)
    assert report.completed is False
    assert report.refusal_code == "mutate_serve_enable"
    assert runner.calls == [step.argv for step in plan[:3]]
    assert report.compensations == ()
    assert report.residual_codes == ()


def test_unsafe_post_enable_state_triggers_serve_reset_before_report() -> None:
    plan = _serve_plan()
    runner = FakeRunner()
    runner.script(
        serve_status_argv(),
        RunResult(returncode=0, stdout=ABSENT_SERVE, stderr=""),
        RunResult(returncode=0, stdout=FOREIGN_SERVE, stderr=""),
    )
    runner.script(
        funnel_status_argv(),
        RunResult(returncode=0, stdout="No serve config", stderr=""),
    )
    report = execute_plan(plan, runner, console_port=8787)
    assert report.completed is False
    assert report.refusal_code == "foreign_exposure"
    assert serve_enable_argv(8787) in runner.calls
    assert runner.calls[-1] == serve_reset_argv()
    assert runner.calls == [step.argv for step in plan] + [serve_reset_argv()]
    assert report.compensations == (("compensate_serve_reset", 0),)
    assert report.residual_codes == ()


def _fs_dirs(tmp_path: Path) -> tuple[Path, Path]:
    """Create rendered artifacts and an empty launch-agents dir on disk."""
    rendered = tmp_path / "rendered"
    agents = tmp_path / "agents"
    agents.mkdir()
    render_artifacts(_inventory(), rendered, console_port=8787)
    return rendered, agents


def _fs_install_plan(rendered: Path, agents: Path):
    return plan_install(
        _inventory(),
        rendered_dir=PurePosixPath(rendered),
        launch_agents_dir=PurePosixPath(agents),
        uid=UID,
        console_port=8787,
    )


def _fs_runner(rendered: Path, **kwargs) -> FilesystemRunner:
    runner = FilesystemRunner(**kwargs)
    runner.script(
        serve_status_argv(),
        RunResult(returncode=0, stdout=ABSENT_SERVE, stderr=""),
    )
    runner.script(
        funnel_status_argv(),
        RunResult(returncode=0, stdout="No serve config", stderr=""),
    )
    for label, subcommand in zip(LABELS, ("daemon", "serve-console"), strict=True):
        runner.script(
            (str(BINARY), subcommand, "--help"),
            RunResult(returncode=0, stdout="usage", stderr=""),
        )
        runner.script(
            ("plutil", "-lint", str(rendered / f"{label}.plist")),
            RunResult(returncode=0, stdout="OK", stderr=""),
        )
    return runner


def test_real_plan_refuses_preexisting_plist_before_any_mutation(
    tmp_path: Path,
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    seeded = b"operator-managed bytes that must survive untouched"
    preexisting = agents / f"{LABELS[0]}.plist"
    preexisting.write_bytes(seeded)
    runner = _fs_runner(rendered)
    report = execute_plan(
        _fs_install_plan(rendered, agents), runner, console_port=8787
    )
    assert report.completed is False
    assert report.refusal_code == "plist_preexisting"
    assert _mutate_calls(runner) == []
    assert preexisting.read_bytes() == seeded
    assert report.compensations == ()
    assert report.residual_codes == ()


def test_real_plan_refuses_already_loaded_label_without_mutation(
    tmp_path: Path,
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    runner = _fs_runner(rendered, loaded_labels={LABELS[0]})
    report = execute_plan(
        _fs_install_plan(rendered, agents), runner, console_port=8787
    )
    assert report.completed is False
    assert report.refusal_code == "label_already_loaded"
    assert _mutate_calls(runner) == []
    for label in LABELS:
        assert not (agents / f"{label}.plist").exists()
    assert report.compensations == ()


def test_real_plan_partial_cp_is_reconciled_and_removed(tmp_path: Path) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    failed_dst = str(agents / f"{LABELS[1]}.plist")
    runner = _fs_runner(rendered, partial_cp_dst=failed_dst)
    report = execute_plan(
        _fs_install_plan(rendered, agents), runner, console_port=8787
    )
    assert report.completed is False
    assert report.refusal_code == "mutate_install_plist"
    # cp truncated the destination before failing; the reconcile probe saw
    # it, so its removal ran first, then the first service rolled back too.
    assert runner.calls.count(("test", "-e", failed_dst)) == 2
    assert ("reconcile_install_plist", 0) in report.records
    assert report.compensations == (
        ("compensate_remove_plist", 0),
        ("compensate_bootout", 0),
        ("compensate_remove_plist", 0),
    )
    assert not Path(failed_dst).exists()
    assert not (agents / f"{LABELS[0]}.plist").exists()
    assert LABELS[0] not in runner.loaded
    assert report.residual_codes == ()


def test_real_plan_partial_bootstrap_boots_out_only_its_own_label(
    tmp_path: Path,
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    foreign = "com.example.foreign"
    runner = _fs_runner(
        rendered, loaded_labels={foreign}, fail_bootstrap=LABELS[1]
    )
    report = execute_plan(
        _fs_install_plan(rendered, agents), runner, console_port=8787
    )
    assert report.completed is False
    assert report.refusal_code == "mutate_bootstrap"
    assert ("reconcile_bootstrap", 0) in report.records
    # The partially loaded job was booted out; the foreign job never was.
    assert LABELS[1] not in runner.loaded
    assert foreign in runner.loaded
    for call in runner.calls:
        if call[:2] == ("launchctl", "bootout"):
            assert all(foreign not in part for part in call)
    assert report.compensations == (
        ("compensate_bootout", 0),
        ("compensate_remove_plist", 0),
        ("compensate_bootout", 0),
        ("compensate_remove_plist", 0),
    )
    for label in LABELS:
        assert not (agents / f"{label}.plist").exists()


def test_journal_persists_ownership_evidence_write_ahead(tmp_path: Path) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = tmp_path / "install-journal.json"
    failed_dst = str(agents / f"{LABELS[1]}.plist")
    runner = _fs_runner(rendered, partial_cp_dst=failed_dst)
    snapshots: list[tuple[str, list[dict[str, str]]]] = []

    def _snapshot(call: tuple[str, ...]) -> None:
        # Observed from inside the mutating command: the claim must already
        # be durable on disk before the mutation executes.
        if call[0] == "cp":
            snapshots.append((call[2], json.loads(journal_path.read_text())))

    runner.on_call = _snapshot
    report = execute_plan(
        _fs_install_plan(rendered, agents),
        runner,
        console_port=8787,
        journal=lifecycle.FileMutationJournal(journal_path),
    )
    assert report.completed is False
    first_dst = str(agents / f"{LABELS[0]}.plist")
    assert [dst for dst, _ in snapshots] == [first_dst, failed_dst]
    assert snapshots[0][1] == [
        {"code": "mutate_install_plist", "resource": first_dst, "state": "claimed"}
    ]
    # By the second cp, the first service's mutations were persisted as
    # applied and the failing one as claimed: enough evidence to recover
    # safely from an interruption at any point.
    second_states = {
        (entry["code"], entry["resource"]): entry["state"]
        for entry in snapshots[1][1]
    }
    assert second_states == {
        ("mutate_install_plist", first_dst): "applied",
        ("mutate_bootstrap", f"gui/{UID}/{LABELS[0]}"): "applied",
        ("mutate_install_plist", failed_dst): "claimed",
    }
    final_states = {
        (entry["code"], entry["resource"]): entry["state"]
        for entry in json.loads(journal_path.read_text())
    }
    assert final_states == {
        ("mutate_install_plist", first_dst): "compensated",
        ("mutate_bootstrap", f"gui/{UID}/{LABELS[0]}"): "compensated",
        ("mutate_install_plist", failed_dst): "compensated",
    }
    blob = journal_path.read_bytes().lower()
    for term in FORBIDDEN_ARTIFACT_TERMS:
        assert term.encode() not in blob, term


def test_rollback_failure_reports_residual_and_keeps_original_refusal() -> None:
    plan = _install_plan()
    runner = _happy_install_runner()
    # The second service's install fails (and reconciles as applied, so its
    # own removal runs first); rolling back the first service's bootstrap
    # fails too and must be reported as residual.
    fail_step = plan[INSTALL_MUTATE_INDICES[2]]
    assert fail_step.code == "mutate_install_plist"
    runner.script(
        fail_step.argv, RunResult(returncode=1, stdout="", stderr="denied")
    )
    bootout_compensation = ("launchctl", "bootout", f"gui/{UID}/{LABELS[0]}")
    runner.script(
        bootout_compensation, RunResult(returncode=1, stdout="", stderr="busy")
    )
    report = execute_plan(plan, runner, console_port=8787)
    assert report.completed is False
    assert report.refusal_code == "mutate_install_plist"
    assert report.compensations == (
        ("compensate_remove_plist", 0),
        ("compensate_bootout", 1),
        ("compensate_remove_plist", 0),
    )
    assert report.residual_codes == ("compensate_bootout",)
    # No later plan step ran after the refusal.
    later_bootstrap = plan[INSTALL_MUTATE_INDICES[3]]
    assert later_bootstrap.argv not in runner.calls
    assert runner.calls == [
        step.argv for step in plan[: INSTALL_MUTATE_INDICES[2] + 1]
    ] + [
        fail_step.reconcile_argv,
        ("rm", str(AGENTS_DIR / f"{LABELS[1]}.plist")),
        bootout_compensation,
        ("rm", str(AGENTS_DIR / f"{LABELS[0]}.plist")),
    ]


def test_execute_install_happy_path_runs_full_ordered_sequence() -> None:
    runner = _happy_install_runner()
    report = execute_plan(_install_plan(), runner, console_port=8787)
    assert report.completed is True
    assert report.refusal_code is None
    assert runner.calls == [step.argv for step in _install_plan()]
    assert [code for code, _ in report.records] == [
        step.code for step in _install_plan()
    ]
    assert report.compensations == ()
    assert report.residual_codes == ()


def test_execute_serve_enable_happy_path() -> None:
    runner = _happy_serve_runner()
    report = execute_plan(_serve_plan(), runner, console_port=8787)
    assert report.completed is True
    assert report.refusal_code is None
    assert runner.calls == [step.argv for step in _serve_plan()]
    assert report.compensations == ()
    assert report.residual_codes == ()


def test_render_artifacts_writes_exactly_expected_files(tmp_path: Path) -> None:
    output = tmp_path / "rendered"
    paths = render_artifacts(_inventory(), output, console_port=8787)
    expected = {
        *(f"{label}.plist" for label in LABELS),
        "hermes-newsyslog.conf",
        "serve-plan.json",
        "OPERATOR.md",
    }
    assert {path.name for path in paths} == expected
    assert {path.name for path in output.iterdir()} == expected
    for label in LABELS:
        parsed = plistlib.loads((output / f"{label}.plist").read_bytes())
        assert parsed["Label"] == label
    serve_plan = json.loads((output / "serve-plan.json").read_text())
    assert serve_plan["backend"] == "http://127.0.0.1:8787"
    assert serve_plan["enable"] == [
        "tailscale",
        "serve",
        "--bg",
        "--yes",
        "http://127.0.0.1:8787",
    ]
    assert serve_plan["enable"] == list(serve_enable_argv(8787))
    assert serve_plan["status"] == list(serve_status_argv())
    assert serve_plan["reset"] == list(serve_reset_argv())
    assert serve_plan["funnel_status"] == list(funnel_status_argv())
    for path in paths:
        blob = path.read_bytes().lower()
        for term in FORBIDDEN_ARTIFACT_TERMS:
            assert term.encode() not in blob, (path.name, term)


def test_runbook_is_complete_ordered_activation_guide(tmp_path: Path) -> None:
    output = tmp_path / "rendered"
    render_artifacts(_inventory(), output, console_port=8787)
    runbook = (output / "OPERATOR.md").read_text()
    ordered_phrases = [
        "remote-auth-init",
        "deploy-render",
        "deploy-install",
        "curl http://127.0.0.1:8787/healthz",
        "curl -i http://127.0.0.1:8787/api/status",
        "deploy-serve-enable",
        "tailscale serve --bg --yes http://127.0.0.1:8787",
        "tailscale serve status --json",
        "MagicDNS",
        "tailscale serve reset",
        "deploy-uninstall",
    ]
    indices = [runbook.index(phrase) for phrase in ordered_phrases]
    assert indices == sorted(indices)
    assert "Funnel must never be enabled" in runbook
    assert "tailscale serve reset" in runbook
    assert "tailscale serve --bg --yes http://127.0.0.1:8787" in runbook
    assert "401" in runbook
    assert "deploy-status" in runbook
    assert "launchctl print" in runbook
    assert "displayed exactly once" in runbook
    assert "secure credential manager" in runbook
    lowered = runbook.lower()
    for term in FORBIDDEN_ARTIFACT_TERMS:
        assert term not in lowered, term


def _spec_flags(rendered: Path) -> list[str]:
    return [
        "--binary",
        str(BINARY),
        "--config-repo",
        str(CONFIG_REPO),
        "--service-state-dir",
        str(STATE_DIR),
        "--log-dir",
        str(LOG_DIR),
        "--rendered-dir",
        str(rendered),
        "--launch-agents-dir",
        str(AGENTS_DIR),
        "--uid",
        str(UID),
    ]


def test_cli_deploy_spec_has_no_gateway_port(tmp_path: Path, capsys) -> None:
    with pytest.raises(SystemExit) as excinfo:
        cli._parser().parse_args(
            ["deploy-install", *_spec_flags(tmp_path), "--gateway-port", "8788"]
        )
    assert excinfo.value.code == 2
    capsys.readouterr()


def test_cli_deploy_render_writes_artifacts(tmp_path: Path, capsys) -> None:
    output = tmp_path / "out"
    code = cli.main(
        [
            "deploy-render",
            "--output-dir",
            str(output),
            "--binary",
            str(BINARY),
            "--config-repo",
            str(CONFIG_REPO),
            "--service-state-dir",
            str(STATE_DIR),
            "--log-dir",
            str(LOG_DIR),
        ]
    )
    assert code == 0
    assert (output / "OPERATOR.md").exists()
    listed = json.loads(capsys.readouterr().out)
    assert sorted(listed["artifacts"]) == sorted(
        str(output / name)
        for name in (
            *(f"{label}.plist" for label in LABELS),
            "hermes-newsyslog.conf",
            "serve-plan.json",
            "OPERATOR.md",
        )
    )


def test_cli_deploy_install_dry_run_prints_plan_only(
    tmp_path: Path, capsys
) -> None:
    code = cli.main(["deploy-install", *_spec_flags(tmp_path)])
    assert code == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["executed"] is False
    assert [step["code"] for step in plan["plan"]][:2] == [
        "probe_entrypoint",
        "probe_entrypoint",
    ]
    assert all(step["argv"] for step in plan["plan"])


def test_cli_deploy_serve_enable_dry_run_prints_plan_only(capsys) -> None:
    code = cli.main(["deploy-serve-enable"])
    assert code == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["executed"] is False
    assert [step["code"] for step in plan["plan"]] == [
        "probe_serve_state_pre",
        "probe_funnel_state",
        "mutate_serve_enable",
        "probe_serve_state_post",
    ]


def test_cli_dry_run_never_touches_injected_runner(tmp_path: Path, capsys) -> None:
    handlers = {
        "deploy-install": cli._deploy_install,
        "deploy-uninstall": cli._deploy_uninstall,
        "deploy-status": cli._deploy_status,
    }
    for command, handler in handlers.items():
        args = cli._parser().parse_args([command, *_spec_flags(tmp_path)])
        assert handler(args, runner=PoisonedRunner()) == 0
        capsys.readouterr()
    args = cli._parser().parse_args(["deploy-serve-enable"])
    assert cli._deploy_serve_enable(args, runner=PoisonedRunner()) == 0
    capsys.readouterr()


def test_cli_deploy_install_execute_uses_injected_runner(
    tmp_path: Path, capsys
) -> None:
    runner = _happy_install_runner()
    args = cli._parser().parse_args(
        ["deploy-install", *_spec_flags(tmp_path), "--execute"]
    )
    assert cli._deploy_install(args, runner=runner) == 0
    assert serve_enable_argv(8787) not in runner.calls
    report = json.loads(capsys.readouterr().out)
    assert report["executed"] is True
    assert report["completed"] is True
    assert report["compensations"] == []
    assert report["residual_codes"] == []


def test_cli_deploy_serve_enable_execute_uses_injected_runner(capsys) -> None:
    runner = _happy_serve_runner()
    args = cli._parser().parse_args(["deploy-serve-enable", "--execute"])
    assert cli._deploy_serve_enable(args, runner=runner) == 0
    assert serve_enable_argv(8787) in runner.calls
    report = json.loads(capsys.readouterr().out)
    assert report["executed"] is True
    assert report["completed"] is True
    assert report["compensations"] == []
    assert report["residual_codes"] == []


def test_cli_deploy_install_execute_refusal_exits_nonzero(
    tmp_path: Path, capsys
) -> None:
    runner = _happy_install_runner()
    runner.scripts[serve_status_argv()] = [
        RunResult(returncode=0, stdout=FUNNEL_SERVE, stderr="")
    ]
    args = cli._parser().parse_args(
        ["deploy-install", *_spec_flags(tmp_path), "--execute"]
    )
    assert cli._deploy_install(args, runner=runner) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["completed"] is False
    assert report["refusal_code"] == "funnel_enabled"
    assert report["compensations"] == []
    assert report["residual_codes"] == []
    assert _mutate_calls(runner) == []


def test_cli_deploy_serve_enable_execute_reports_compensations(capsys) -> None:
    runner = FakeRunner()
    runner.script(
        serve_status_argv(),
        RunResult(returncode=0, stdout=ABSENT_SERVE, stderr=""),
        RunResult(returncode=0, stdout=FOREIGN_SERVE, stderr=""),
    )
    runner.script(
        funnel_status_argv(),
        RunResult(returncode=0, stdout="No serve config", stderr=""),
    )
    args = cli._parser().parse_args(["deploy-serve-enable", "--execute"])
    assert cli._deploy_serve_enable(args, runner=runner) == 1
    assert runner.calls[-1] == serve_reset_argv()
    report = json.loads(capsys.readouterr().out)
    assert report["completed"] is False
    assert report["refusal_code"] == "foreign_exposure"
    assert report["compensations"] == [["compensate_serve_reset", 0]]
    assert report["residual_codes"] == []


def test_cli_deploy_uninstall_execute_runs_reversal(tmp_path: Path, capsys) -> None:
    runner = FakeRunner()
    args = cli._parser().parse_args(
        ["deploy-uninstall", *_spec_flags(tmp_path), "--execute"]
    )
    assert cli._deploy_uninstall(args, runner=runner) == 0
    assert runner.calls[0] == serve_reset_argv()
    assert runner.calls[1][:2] == ("launchctl", "bootout")


def test_cli_deploy_status_execute_is_read_only(tmp_path: Path, capsys) -> None:
    runner = FakeRunner()
    runner.script(
        serve_status_argv(), RunResult(returncode=0, stdout=ABSENT_SERVE, stderr="")
    )
    runner.script(
        funnel_status_argv(),
        RunResult(returncode=0, stdout="No serve config", stderr=""),
    )
    args = cli._parser().parse_args(
        ["deploy-status", *_spec_flags(tmp_path), "--execute"]
    )
    assert cli._deploy_status(args, runner=runner) == 0
    assert _mutate_calls(runner) == []


def test_subprocess_runner_not_constructed_without_execute(tmp_path: Path) -> None:
    with pytest.MonkeyPatch.context() as patcher:
        def _boom(*_args, **_kwargs):
            raise AssertionError("SubprocessRunner constructed in dry-run")

        patcher.setattr(
            "hermes_orchestrator.deploy.lifecycle.SubprocessRunner", _boom
        )
        assert cli.main(["deploy-install", *_spec_flags(tmp_path)]) == 0


# ---------------------------------------------------------------------------
# Round 4: journal recovery on restart + explicit probe outcome classification


class _DiedMidAttempt(Exception):
    """Models a process killed mid-attempt: no compensation ever runs."""


def _fs_cli_flags(rendered: Path, agents: Path) -> list[str]:
    return [
        "--binary",
        str(BINARY),
        "--config-repo",
        str(CONFIG_REPO),
        "--service-state-dir",
        str(STATE_DIR),
        "--log-dir",
        str(LOG_DIR),
        "--rendered-dir",
        str(rendered),
        "--launch-agents-dir",
        str(agents),
        "--uid",
        str(UID),
    ]


def _journal_states(path: Path) -> dict[tuple[str, str], str]:
    return {
        (entry["code"], entry["resource"]): entry["state"]
        for entry in json.loads(path.read_text())
    }


def _nth(predicate, n: int):
    seen = 0

    def check(call: tuple[str, ...]) -> bool:
        nonlocal seen
        if predicate(call):
            seen += 1
            return seen == n
        return False

    return check


def _interrupted_first_attempt(
    rendered: Path, agents: Path, *, interrupt_on
) -> FilesystemRunner:
    """Run the real plan until the chosen call, then die like a killed process.

    ``on_call`` fires after the write-ahead journal write and before the
    command's effect, so the surviving journal and filesystem are exactly
    what an interruption at that point leaves behind.
    """
    runner = _fs_runner(rendered)

    def _die(call: tuple[str, ...]) -> None:
        if interrupt_on(call):
            raise _DiedMidAttempt(call)

    runner.on_call = _die
    journal = lifecycle.FileMutationJournal(rendered / "install-journal.json")
    with pytest.raises(_DiedMidAttempt):
        execute_plan(
            _fs_install_plan(rendered, agents),
            runner,
            console_port=8787,
            journal=journal,
        )
    return runner


def _second_attempt(rendered: Path, agents: Path, runner, capsys):
    """Restart through the real deploy-install path with the surviving state."""
    args = cli._parser().parse_args(
        ["deploy-install", *_fs_cli_flags(rendered, agents), "--execute"]
    )
    code = cli._deploy_install(args, runner=runner)
    return code, json.loads(capsys.readouterr().out)


def test_restart_after_claimed_plist_recovers_and_completes(
    tmp_path: Path, capsys
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    first = _interrupted_first_attempt(
        rendered, agents, interrupt_on=lambda call: call[0] == "cp"
    )
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "claimed"
    }
    assert not Path(dst1).exists()
    assert first.loaded == set()
    second = _fs_runner(rendered)
    code, report = _second_attempt(rendered, agents, second, capsys)
    assert code == 0
    assert report["completed"] is True
    # Recovery probed exactly the recorded resource: provably absent, so the
    # claim resolves as unapplied and nothing is compensated.
    assert second.calls[0] == ("test", "-e", dst1)
    assert report["records"][0] == ["reconcile_install_plist", 1]
    assert report["compensations"] == []
    assert report["residual_codes"] == []
    for label in LABELS:
        assert (agents / f"{label}.plist").exists()
        assert label in second.loaded
    states = _journal_states(journal_path)
    assert len(states) == 4
    assert set(states.values()) == {"installed"}


def test_restart_after_applied_plist_compensates_exact_recorded_resource(
    tmp_path: Path, capsys
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    label1 = f"gui/{UID}/{LABELS[0]}"
    first = _interrupted_first_attempt(
        rendered,
        agents,
        interrupt_on=lambda call: call[:2] == ("launchctl", "bootstrap"),
    )
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "applied",
        ("mutate_bootstrap", label1): "claimed",
    }
    assert Path(dst1).exists()
    assert first.loaded == set()
    second = _fs_runner(rendered)
    code, report = _second_attempt(rendered, agents, second, capsys)
    assert code == 0
    assert report["completed"] is True
    # Reverse claim order: the claimed bootstrap resolves as unapplied via
    # the documented not-found probe; the applied plist is removed only
    # after its recorded content identity matches what is present.
    assert second.calls[:4] == [
        ("launchctl", "print", label1),
        ("test", "-e", dst1),
        ("shasum", "-a", "256", dst1),
        ("rm", dst1),
    ]
    assert report["records"][:3] == [
        ["reconcile_bootstrap", 113],
        ["reconcile_install_plist", 0],
        ["identity_install_plist", 0],
    ]
    assert report["compensations"] == [["compensate_remove_plist", 0]]
    assert report["residual_codes"] == []
    for label in LABELS:
        assert (agents / f"{label}.plist").exists()
        assert label in second.loaded
    states = _journal_states(journal_path)
    assert len(states) == 4
    assert set(states.values()) == {"installed"}


def test_restart_after_applied_bootstrap_recovers_and_preserves_unowned(
    tmp_path: Path, capsys
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    dst2 = str(agents / f"{LABELS[1]}.plist")
    label1 = f"gui/{UID}/{LABELS[0]}"
    unowned = agents / "com.example.operator-managed.plist"
    unowned_bytes = b"operator bytes that recovery must never touch"
    unowned.write_bytes(unowned_bytes)
    first = _interrupted_first_attempt(
        rendered, agents, interrupt_on=_nth(lambda call: call[0] == "cp", 2)
    )
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "applied",
        ("mutate_bootstrap", label1): "applied",
        ("mutate_install_plist", dst2): "claimed",
    }
    second = _fs_runner(rendered, loaded_paths=dict(first.loaded_paths))
    code, report = _second_attempt(rendered, agents, second, capsys)
    assert code == 0
    assert report["completed"] is True
    # Recovery touches exactly the recorded resources in reverse claim order
    # (each applied one only after its identity is verified), then the
    # unmodified real plan runs in full.
    plan = _fs_install_plan(rendered, agents)
    assert second.calls == [
        ("test", "-e", dst2),
        ("launchctl", "print", label1),
        ("shasum", "-a", "256", dst1),
        ("launchctl", "bootout", label1),
        ("test", "-e", dst1),
        ("shasum", "-a", "256", dst1),
        ("rm", dst1),
    ] + [step.argv for step in plan]
    assert report["compensations"] == [
        ["compensate_bootout", 0],
        ["compensate_remove_plist", 0],
    ]
    assert report["residual_codes"] == []
    assert unowned.read_bytes() == unowned_bytes
    states = _journal_states(journal_path)
    assert len(states) == 4
    assert set(states.values()) == {"installed"}


def test_restart_preserves_unresolved_entry_when_compensation_fails(
    tmp_path: Path,
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    dst2 = str(agents / f"{LABELS[1]}.plist")
    label1 = f"gui/{UID}/{LABELS[0]}"
    first = _interrupted_first_attempt(
        rendered, agents, interrupt_on=_nth(lambda call: call[0] == "cp", 2)
    )
    second = _fs_runner(rendered, loaded_paths=dict(first.loaded_paths))
    second.script(
        ("rm", dst1), RunResult(returncode=1, stdout="", stderr="busy")
    )
    report = execute_plan(
        _fs_install_plan(rendered, agents),
        second,
        console_port=8787,
        journal=lifecycle.FileMutationJournal(journal_path),
    )
    assert report.completed is False
    assert report.refusal_code == "journal_recovery_failed"
    assert report.residual_codes == ("compensate_remove_plist",)
    # Recovery stopped at the failed compensation; no fresh plan step ran.
    assert second.calls == [
        ("test", "-e", dst2),
        ("launchctl", "print", label1),
        ("shasum", "-a", "256", dst1),
        ("launchctl", "bootout", label1),
        ("test", "-e", dst1),
        ("shasum", "-a", "256", dst1),
        ("rm", dst1),
    ]
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst2): "unapplied",
        ("mutate_bootstrap", label1): "compensated",
        ("mutate_install_plist", dst1): "residual",
    }
    assert Path(dst1).exists()


def test_restart_refuses_and_preserves_on_uncertain_recovery_probe(
    tmp_path: Path,
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    dst2 = str(agents / f"{LABELS[1]}.plist")
    label1 = f"gui/{UID}/{LABELS[0]}"
    first = _interrupted_first_attempt(
        rendered, agents, interrupt_on=_nth(lambda call: call[0] == "cp", 2)
    )
    second = _fs_runner(rendered, loaded_labels=set(first.loaded))
    second.script(
        ("test", "-e", dst2),
        RunResult(returncode=2, stdout="", stderr="input/output error"),
    )
    report = execute_plan(
        _fs_install_plan(rendered, agents),
        second,
        console_port=8787,
        journal=lifecycle.FileMutationJournal(journal_path),
    )
    assert report.completed is False
    assert report.refusal_code == "journal_recovery_uncertain"
    assert report.residual_codes == ("reconcile_install_plist",)
    # Nothing ran beyond the uncertain probe; every other recorded claim is
    # preserved untouched for the next attempt.
    assert second.calls == [("test", "-e", dst2)]
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "applied",
        ("mutate_bootstrap", label1): "applied",
        ("mutate_install_plist", dst2): "residual",
    }
    assert Path(dst1).exists()
    assert LABELS[0] in second.loaded


@pytest.mark.parametrize(
    "payload",
    [
        "{not json",
        '{"code": "mutate_install_plist"}',
        '[{"code": "mutate_install_plist", "resource": "/tmp/x"}]',
        '[{"code": "mutate_install_plist", "resource": "/tmp/x",'
        ' "state": "weird"}]',
        '[{"code": "mutate_install_plist", "resource": "/tmp/x",'
        ' "state": "claimed", "extra": "y"}]',
        '[{"code": "mutate_install_plist", "resource": "/tmp/x",'
        ' "state": "applied", "identity": 5}]',
        '[{"code": "mutate_install_plist", "resource": "/tmp/x",'
        ' "state": "claimed"},'
        ' {"code": "mutate_install_plist", "resource": "/tmp/x",'
        ' "state": "applied"}]',
    ],
)
def test_malformed_journal_refuses_before_any_command(
    tmp_path: Path, capsys, payload: str
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    journal_path.write_text(payload)
    args = cli._parser().parse_args(
        ["deploy-install", *_fs_cli_flags(rendered, agents), "--execute"]
    )
    assert cli._deploy_install(args, runner=PoisonedRunner()) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["refusal_code"] == "journal_unrecoverable"
    assert report["records"] == []
    assert report["compensations"] == []
    # The untrusted journal is evidence: preserved byte-identical.
    assert journal_path.read_text() == payload


def test_foreign_journal_entries_refuse_before_any_command(
    tmp_path: Path, capsys
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    foreign_payloads = [
        json.dumps(
            [
                {
                    "code": "mutate_install_plist",
                    "resource": str(tmp_path / "elsewhere.plist"),
                    "state": "applied",
                }
            ]
        ),
        json.dumps(
            [{"code": "mutate_widget", "resource": dst1, "state": "claimed"}]
        ),
    ]
    for payload in foreign_payloads:
        journal_path.write_text(payload)
        args = cli._parser().parse_args(
            ["deploy-install", *_fs_cli_flags(rendered, agents), "--execute"]
        )
        assert cli._deploy_install(args, runner=PoisonedRunner()) == 1
        report = json.loads(capsys.readouterr().out)
        assert report["refusal_code"] == "journal_foreign_entries"
        assert report["records"] == []
        assert report["compensations"] == []
        assert journal_path.read_text() == payload


def test_completed_install_journal_is_terminal_and_rerun_never_compensates(
    tmp_path: Path, capsys
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    first = _fs_runner(rendered)
    report = execute_plan(
        _fs_install_plan(rendered, agents),
        first,
        console_port=8787,
        journal=lifecycle.FileMutationJournal(journal_path),
    )
    assert report.completed is True
    states = _journal_states(journal_path)
    assert len(states) == 4
    assert set(states.values()) == {"installed"}
    # A rerun after success must never mistake the completed installation
    # for an interrupted attempt: no recovery mutation, preflight refuses.
    second = _fs_runner(rendered, loaded_labels=set(first.loaded))
    code, rerun = _second_attempt(rendered, agents, second, capsys)
    assert code == 1
    assert rerun["refusal_code"] == "plist_preexisting"
    assert _mutate_calls(second) == []
    for label in LABELS:
        assert (agents / f"{label}.plist").exists()
        assert label in second.loaded


@pytest.mark.parametrize(
    "result_kwargs",
    [
        {"returncode": 1, "stdout": "", "stderr": "Operation not permitted"},
        {
            "returncode": 78,
            "stdout": "",
            "stderr": "Domain does not support specified action",
        },
        {
            "returncode": 1,
            "stdout": "",
            "stderr": "timed out",
            "synthesized": True,
        },
    ],
)
def test_label_preflight_uncertainty_refuses_before_mutation(
    tmp_path: Path, result_kwargs: dict
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    runner = _fs_runner(rendered)
    runner.script(
        ("launchctl", "print", f"gui/{UID}/{LABELS[0]}"),
        RunResult(**result_kwargs),
    )
    report = execute_plan(
        _fs_install_plan(rendered, agents), runner, console_port=8787
    )
    assert report.completed is False
    assert report.refusal_code == "label_state_uncertain"
    assert _mutate_calls(runner) == []
    for label in LABELS:
        assert not (agents / f"{label}.plist").exists()
    assert report.compensations == ()
    assert report.residual_codes == ()


def test_plist_preflight_synthesized_notfound_is_uncertain(
    tmp_path: Path,
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    runner = _fs_runner(rendered)
    dst1 = str(agents / f"{LABELS[0]}.plist")
    # A runner-synthesized rc=1 (timeout/OSError) collides with `test -e`'s
    # documented not-found code; it must classify as uncertainty, never as
    # proven absence.
    runner.script(
        ("test", "-e", dst1),
        RunResult(returncode=1, stdout="", stderr="timed out", synthesized=True),
    )
    report = execute_plan(
        _fs_install_plan(rendered, agents), runner, console_port=8787
    )
    assert report.completed is False
    assert report.refusal_code == "plist_state_uncertain"
    assert _mutate_calls(runner) == []


def test_cp_reconcile_uncertainty_persists_residual_without_compensation(
    tmp_path: Path,
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = tmp_path / "journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    runner = _fs_runner(rendered)
    runner.script(
        ("cp", str(rendered / f"{LABELS[0]}.plist"), dst1),
        RunResult(returncode=1, stdout="", stderr="denied"),
    )
    runner.script(
        ("test", "-e", dst1),
        RunResult(returncode=1, stdout="", stderr=""),
        RunResult(returncode=2, stdout="", stderr="input/output error"),
    )
    report = execute_plan(
        _fs_install_plan(rendered, agents),
        runner,
        console_port=8787,
        journal=lifecycle.FileMutationJournal(journal_path),
    )
    assert report.completed is False
    assert report.refusal_code == "mutate_install_plist"
    assert report.residual_codes == ("reconcile_install_plist",)
    assert ("rm", dst1) not in runner.calls
    assert report.compensations == ()
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "residual"
    }


def test_bootstrap_reconcile_uncertainty_persists_residual_without_bootout(
    tmp_path: Path,
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = tmp_path / "journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    label1 = f"gui/{UID}/{LABELS[0]}"
    runner = _fs_runner(rendered)
    runner.script(
        ("launchctl", "bootstrap", f"gui/{UID}", dst1),
        RunResult(returncode=1, stdout="", stderr="denied"),
    )
    runner.script(
        ("launchctl", "print", label1),
        RunResult(returncode=113, stdout="", stderr="not found"),
        RunResult(returncode=1, stdout="", stderr="Operation not permitted"),
    )
    report = execute_plan(
        _fs_install_plan(rendered, agents),
        runner,
        console_port=8787,
        journal=lifecycle.FileMutationJournal(journal_path),
    )
    assert report.completed is False
    assert report.refusal_code == "mutate_bootstrap"
    assert report.residual_codes == ("reconcile_bootstrap",)
    for call in runner.calls:
        assert call[:2] != ("launchctl", "bootout")
    assert report.compensations == (("compensate_remove_plist", 0),)
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "compensated",
        ("mutate_bootstrap", label1): "residual",
    }


def test_bootstrap_reconcile_documented_notfound_marks_unapplied(
    tmp_path: Path,
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = tmp_path / "journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    label1 = f"gui/{UID}/{LABELS[0]}"
    runner = _fs_runner(rendered)
    # The scripted failure bypasses the built-in bootstrap effect, so the
    # label is genuinely absent afterwards: the documented not-found code
    # proves it unapplied and nothing compensates it.
    runner.script(
        ("launchctl", "bootstrap", f"gui/{UID}", dst1),
        RunResult(returncode=1, stdout="", stderr="denied"),
    )
    report = execute_plan(
        _fs_install_plan(rendered, agents),
        runner,
        console_port=8787,
        journal=lifecycle.FileMutationJournal(journal_path),
    )
    assert report.completed is False
    assert report.refusal_code == "mutate_bootstrap"
    assert report.residual_codes == ()
    for call in runner.calls:
        assert call[:2] != ("launchctl", "bootout")
    assert report.compensations == (("compensate_remove_plist", 0),)
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "compensated",
        ("mutate_bootstrap", label1): "unapplied",
    }


def test_subprocess_runner_marks_synthesized_failures(monkeypatch) -> None:
    runner = lifecycle.SubprocessRunner()

    def _raise_timeout(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(cmd=["x"], timeout=1)

    monkeypatch.setattr(lifecycle.subprocess, "run", _raise_timeout)
    timed_out = runner.run(("x",), timeout=1)
    assert timed_out.returncode == 1
    assert timed_out.synthesized is True

    def _raise_oserror(*_args, **_kwargs):
        raise OSError("no such file")

    monkeypatch.setattr(lifecycle.subprocess, "run", _raise_oserror)
    missing = runner.run(("x",), timeout=1)
    assert missing.returncode == 1
    assert missing.synthesized is True

    def _ok(*_args, **_kwargs):
        return subprocess.CompletedProcess(
            args=["x"], returncode=0, stdout="", stderr=""
        )

    monkeypatch.setattr(lifecycle.subprocess, "run", _ok)
    real = runner.run(("x",), timeout=1)
    assert real.returncode == 0
    assert real.synthesized is False


def test_runbook_documents_real_interruption_recovery(tmp_path: Path) -> None:
    output = tmp_path / "rendered"
    render_artifacts(_inventory(), output, console_port=8787)
    runbook = (output / "OPERATOR.md").read_text()
    assert "install-journal.json" in runbook
    assert "residual" in runbook
    # The round-3 claim that retries always start clean was false for an
    # interrupted attempt and must be gone.
    assert "starts from a clean state" not in runbook
    lowered = runbook.lower()
    for term in FORBIDDEN_ARTIFACT_TERMS:
        assert term not in lowered, term


# Round 5: a claimed entry is pre-mutation intent only, never ownership proof


def test_restart_preserves_operator_file_at_claimed_plist_path(
    tmp_path: Path, capsys
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    _interrupted_first_attempt(
        rendered, agents, interrupt_on=lambda call: call[0] == "cp"
    )
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "claimed"
    }
    assert not Path(dst1).exists()
    # An operator creates a file at the claimed path between attempts. The
    # claim is pre-mutation intent only, so its mere presence must never
    # authorize recovery to touch it.
    operator_bytes = b"operator-owned file recovery must preserve"
    Path(dst1).write_bytes(operator_bytes)
    second = _fs_runner(rendered)
    code, report = _second_attempt(rendered, agents, second, capsys)
    assert code == 1
    assert report["refusal_code"] == "journal_claim_uncertain"
    assert report["records"] == [["reconcile_install_plist", 0]]
    assert report["compensations"] == []
    assert report["residual_codes"] == ["reconcile_install_plist"]
    assert second.calls == [("test", "-e", dst1)]
    assert Path(dst1).read_bytes() == operator_bytes
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "claimed"
    }


def test_restart_never_boots_out_operator_job_at_claimed_label(
    tmp_path: Path, capsys
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    label1 = f"gui/{UID}/{LABELS[0]}"
    _interrupted_first_attempt(
        rendered,
        agents,
        interrupt_on=lambda call: call[:2] == ("launchctl", "bootstrap"),
    )
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "applied",
        ("mutate_bootstrap", label1): "claimed",
    }
    assert Path(dst1).exists()
    # An operator loads a job under that label between attempts. The claim
    # is pre-mutation intent only, so its mere presence must never
    # authorize recovery to boot out someone else's job.
    second = _fs_runner(rendered, loaded_labels={LABELS[0]})
    code, report = _second_attempt(rendered, agents, second, capsys)
    assert code == 1
    assert report["refusal_code"] == "journal_claim_uncertain"
    # Recovery probes the newest claim first and stops there: no bootout
    # ever runs, and the older applied plist entry is not even probed.
    assert second.calls == [("launchctl", "print", label1)]
    assert LABELS[0] in second.loaded
    assert Path(dst1).exists()
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "applied",
        ("mutate_bootstrap", label1): "claimed",
    }
    assert report["residual_codes"] == ["reconcile_bootstrap"]
    assert report["compensations"] == []


def test_restart_compensates_applied_entries_in_reverse_order(
    tmp_path: Path, capsys
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    label1 = f"gui/{UID}/{LABELS[0]}"
    # The prior attempt installed exactly the rendered bytes and recorded
    # their digest as durable identity alongside each applied state.
    installed_bytes = (rendered / f"{LABELS[0]}.plist").read_bytes()
    identity = hashlib.sha256(installed_bytes).hexdigest()
    journal_path.write_text(
        json.dumps(
            [
                {
                    "code": "mutate_install_plist",
                    "resource": dst1,
                    "state": "applied",
                    "identity": identity,
                },
                {
                    "code": "mutate_bootstrap",
                    "resource": label1,
                    "state": "applied",
                    "identity": identity,
                },
            ]
        )
    )
    Path(dst1).write_bytes(installed_bytes)
    second = _fs_runner(rendered, loaded_paths={LABELS[0]: dst1})
    code, report = _second_attempt(rendered, agents, second, capsys)
    assert code == 0
    assert report["completed"] is True
    # Applied entries whose persisted identity matches current state are
    # compensated in reverse recorded order.
    assert report["compensations"] == [
        ["compensate_bootout", 0],
        ["compensate_remove_plist", 0],
    ]
    assert second.calls[:6] == [
        ("launchctl", "print", label1),
        ("shasum", "-a", "256", dst1),
        ("launchctl", "bootout", label1),
        ("test", "-e", dst1),
        ("shasum", "-a", "256", dst1),
        ("rm", dst1),
    ]
    # Recovery finished clean, so the fresh plan then runs to completion.
    states = _journal_states(journal_path)
    assert len(states) == 4
    assert set(states.values()) == {"installed"}


# Round 6: applied entries compensate only on durable identity match; a
# replacement resource at the same path or label is never compensated.


def test_install_plan_mutations_carry_durable_identity() -> None:
    steps = _install_plan()
    digests = {
        label: hashlib.sha256(render_plist(spec)).hexdigest()
        for label, spec in zip(LABELS, _inventory(), strict=True)
    }
    installs = [step for step in steps if step.code == "mutate_install_plist"]
    bootstraps = [step for step in steps if step.code == "mutate_bootstrap"]
    for label, install, bootstrap in zip(
        LABELS, installs, bootstraps, strict=True
    ):
        installed = str(AGENTS_DIR / f"{label}.plist")
        assert install.identity == digests[label]
        assert install.identity_argv == ("shasum", "-a", "256", installed)
        assert install.identity_code == "identity_install_plist"
        assert install.identity_source_path is None
        assert bootstrap.identity == digests[label]
        assert bootstrap.identity_argv == ("shasum", "-a", "256", installed)
        assert bootstrap.identity_code == "identity_bootstrap"
        assert bootstrap.identity_source_path == installed
    for step in steps:
        if step.kind == "probe":
            assert step.identity is None
            assert step.identity_argv is None
            assert step.identity_code is None
            assert step.identity_source_path is None


def test_restart_preserves_operator_replacement_at_applied_plist_path(
    tmp_path: Path, capsys
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    label1 = f"gui/{UID}/{LABELS[0]}"
    _interrupted_first_attempt(
        rendered,
        agents,
        interrupt_on=lambda call: call[:2] == ("launchctl", "bootstrap"),
    )
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "applied",
        ("mutate_bootstrap", label1): "claimed",
    }
    # An operator replaces the applied plist with their own file at the same
    # path between attempts. Applied state at that path is historical
    # evidence about a different file: recovery must preserve the
    # replacement and report uncertainty instead of deleting it.
    operator_bytes = b"operator replacement recovery must preserve"
    Path(dst1).write_bytes(operator_bytes)
    second = _fs_runner(rendered)
    code, report = _second_attempt(rendered, agents, second, capsys)
    assert code == 1
    assert report["refusal_code"] == "journal_identity_uncertain"
    assert report["compensations"] == []
    assert report["residual_codes"] == ["identity_install_plist"]
    # The claimed bootstrap resolves as unapplied first; the applied plist
    # is probed, fails the identity match, and nothing mutates.
    assert second.calls == [
        ("launchctl", "print", label1),
        ("test", "-e", dst1),
        ("shasum", "-a", "256", dst1),
    ]
    assert Path(dst1).read_bytes() == operator_bytes
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "applied",
        ("mutate_bootstrap", label1): "unapplied",
    }


def test_restart_never_boots_out_operator_job_at_applied_label(
    tmp_path: Path, capsys
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    dst2 = str(agents / f"{LABELS[1]}.plist")
    label1 = f"gui/{UID}/{LABELS[0]}"
    _interrupted_first_attempt(
        rendered, agents, interrupt_on=_nth(lambda call: call[0] == "cp", 2)
    )
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "applied",
        ("mutate_bootstrap", label1): "applied",
        ("mutate_install_plist", dst2): "claimed",
    }
    # Between attempts the operator replaced the loaded job: same label, but
    # backed by their own plist elsewhere. Label presence plus applied state
    # must never authorize booting out someone else's job.
    second = _fs_runner(rendered, loaded_labels={LABELS[0]})
    code, report = _second_attempt(rendered, agents, second, capsys)
    assert code == 1
    assert report["refusal_code"] == "journal_identity_uncertain"
    assert report["compensations"] == []
    assert report["residual_codes"] == ["reconcile_bootstrap"]
    # The claimed second plist resolves as unapplied; the loaded job is
    # probed, is not backed by the owned installation, and recovery stops
    # before any bootout or digest probe.
    assert second.calls == [
        ("test", "-e", dst2),
        ("launchctl", "print", label1),
    ]
    assert LABELS[0] in second.loaded
    assert Path(dst1).exists()
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "applied",
        ("mutate_bootstrap", label1): "applied",
        ("mutate_install_plist", dst2): "unapplied",
    }


def test_applied_entry_without_identity_refuses_for_operator(
    tmp_path: Path, capsys
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    # A legacy journal recorded applied without identity. Even a present
    # file with the exact rendered bytes cannot be proven to be the one this
    # attempt applied, so recovery must refuse for operator resolution.
    journal_path.write_text(
        json.dumps(
            [
                {
                    "code": "mutate_install_plist",
                    "resource": dst1,
                    "state": "applied",
                }
            ]
        )
    )
    Path(dst1).write_bytes((rendered / f"{LABELS[0]}.plist").read_bytes())
    second = _fs_runner(rendered)
    code, report = _second_attempt(rendered, agents, second, capsys)
    assert code == 1
    assert report["refusal_code"] == "journal_identity_uncertain"
    assert report["compensations"] == []
    assert report["residual_codes"] == ["reconcile_install_plist"]
    assert second.calls == [("test", "-e", dst1)]
    assert Path(dst1).exists()
    assert _journal_states(journal_path) == {
        ("mutate_install_plist", dst1): "applied"
    }


def test_identity_probe_failure_refuses_without_compensation(
    tmp_path: Path, capsys
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    journal_path = rendered / "install-journal.json"
    dst1 = str(agents / f"{LABELS[0]}.plist")
    _interrupted_first_attempt(
        rendered,
        agents,
        interrupt_on=lambda call: call[:2] == ("launchctl", "bootstrap"),
    )
    second = _fs_runner(rendered)
    # The digest probe itself fails: current identity is unknowable, so the
    # present resource must be preserved and the entry kept as recorded.
    second.script(
        ("shasum", "-a", "256", dst1),
        RunResult(returncode=1, stdout="", stderr="input/output error"),
    )
    code, report = _second_attempt(rendered, agents, second, capsys)
    assert code == 1
    assert report["refusal_code"] == "journal_identity_uncertain"
    assert report["compensations"] == []
    assert report["residual_codes"] == ["identity_install_plist"]
    assert ("rm", dst1) not in second.calls
    assert Path(dst1).exists()
    assert _journal_states(journal_path)[("mutate_install_plist", dst1)] == (
        "applied"
    )


def test_job_backed_by_superstring_path_is_not_owned(
    tmp_path: Path, capsys
) -> None:
    rendered, agents = _fs_dirs(tmp_path)
    dst1 = str(agents / f"{LABELS[0]}.plist")
    dst2 = str(agents / f"{LABELS[1]}.plist")
    label1 = f"gui/{UID}/{LABELS[0]}"
    _interrupted_first_attempt(
        rendered, agents, interrupt_on=_nth(lambda call: call[0] == "cp", 2)
    )
    # The operator's job is backed by a path that merely CONTAINS the owned
    # path; a substring match would wrongly claim it. Ownership requires the
    # exact backing path.
    second = _fs_runner(rendered, loaded_paths={LABELS[0]: dst1 + ".bak"})
    code, report = _second_attempt(rendered, agents, second, capsys)
    assert code == 1
    assert report["refusal_code"] == "journal_identity_uncertain"
    assert second.calls == [
        ("test", "-e", dst2),
        ("launchctl", "print", label1),
    ]
    assert LABELS[0] in second.loaded
