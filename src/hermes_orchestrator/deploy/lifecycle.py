"""Fail-closed install/uninstall/status command plans for generated services."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal, Protocol

from hermes_orchestrator.deploy.launchd import (
    ServiceSpec,
    render_newsyslog_conf,
    render_plist,
)
from hermes_orchestrator.deploy.tailscale import (
    UnsafeTailscaleState,
    funnel_status_argv,
    serve_enable_argv,
    serve_reset_argv,
    serve_status_argv,
    verify_serve_status,
)

_COMMAND_TIMEOUT_SECONDS = 30.0
# `tailscale funnel status` may only report an empty or unconfigured state;
# anything else is treated as uncertain and refuses fail-closed.
_FUNNEL_OFF_MARKERS = frozenset({"", "no serve config"})
# `launchctl error 113` is the documented "Could not find specified service"
# outcome; it is the only nonzero code accepted as proven absence.
_LAUNCHCTL_NOT_FOUND_RETURNCODE = 113
# Terminal journal states need no recovery; everything else is an unresolved
# ownership claim from a prior attempt.
_TERMINAL_JOURNAL_STATES = frozenset({"unapplied", "compensated", "installed"})
_JOURNAL_STATES = frozenset({"claimed", "applied", "residual"}) | (
    _TERMINAL_JOURNAL_STATES
)


@dataclass(frozen=True, slots=True)
class CommandStep:
    """One externally observable command; never executed by planners.

    ``reconcile_argv`` is a read-only post-state probe run when the mutation
    itself fails: rc==0 means the mutation partially applied and must be
    compensated. ``resource`` names what the mutation claims ownership of.

    ``identity`` is the durable identity of the exact content this step
    applies (the SHA-256 digest of the plist bytes), computed purely at plan
    time and persisted into the journal alongside the applied state.
    Recovery re-reads the current identity through ``identity_argv`` and
    compensates only on an exact match; ``identity_source_path`` (loaded
    launchd jobs only) is the plist path the reconcile probe's output must
    reference for the present job to be attributable to this installation.
    """

    argv: tuple[str, ...]
    kind: Literal["probe", "mutate"]
    code: str
    compensation_argv: tuple[str, ...] | None = None
    compensation_code: str | None = None
    reconcile_argv: tuple[str, ...] | None = None
    reconcile_code: str | None = None
    resource: str | None = None
    identity: str | None = None
    identity_argv: tuple[str, ...] | None = None
    identity_code: str | None = None
    identity_source_path: str | None = None


@dataclass(frozen=True, slots=True)
class RunResult:
    """Outcome of one command as observed through a runner.

    ``synthesized`` marks a result the runner fabricated because the command
    never produced one (timeout or spawn failure); such a result can prove
    nothing about system state.
    """

    returncode: int
    stdout: str
    stderr: str
    synthesized: bool = False


class CommandRunner(Protocol):
    """Boundary through which every external command is executed."""

    def run(self, argv: Sequence[str], *, timeout: float) -> RunResult: ...


class SubprocessRunner:
    """Real list-argv execution; constructed only behind explicit --execute."""

    def run(self, argv: Sequence[str], *, timeout: float) -> RunResult:
        try:
            completed = subprocess.run(
                list(argv),
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            return RunResult(
                returncode=1, stdout="", stderr=str(error), synthesized=True
            )
        return RunResult(
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


@dataclass(frozen=True, slots=True)
class ExecutionReport:
    """Ordered (step code, returncode) records plus the stop disposition.

    ``compensations`` records the rollback commands run after a refusal, in
    the order they ran (reverse completion order of this attempt's
    mutations); any compensation that itself failed is named in
    ``residual_codes`` as unresolved residual state.
    """

    records: tuple[tuple[str, int], ...]
    completed: bool
    refusal_code: str | None
    compensations: tuple[tuple[str, int], ...] = ()
    residual_codes: tuple[str, ...] = ()


def _rendered_plist(rendered_dir: PurePosixPath, spec: ServiceSpec) -> str:
    return str(rendered_dir / f"{spec.label}.plist")


def _installed_plist(launch_agents_dir: PurePosixPath, spec: ServiceSpec) -> str:
    return str(launch_agents_dir / f"{spec.label}.plist")


def plan_install(
    inventory: Sequence[ServiceSpec],
    *,
    rendered_dir: PurePosixPath,
    launch_agents_dir: PurePosixPath,
    uid: int,
    console_port: int,
) -> tuple[CommandStep, ...]:
    del console_port  # serve exposure moved to plan_serve_enable
    steps: list[CommandStep] = []
    for spec in inventory:
        steps.append(
            CommandStep(
                argv=(
                    str(spec.program_arguments[0]),
                    spec.entrypoint_subcommand,
                    "--help",
                ),
                kind="probe",
                code="probe_entrypoint",
            )
        )
    for spec in inventory:
        steps.append(
            CommandStep(
                argv=("plutil", "-lint", _rendered_plist(rendered_dir, spec)),
                kind="probe",
                code="probe_plutil_lint",
            )
        )
    # Ownership rule: this attempt may only mutate resources proven ABSENT
    # here; anything pre-existing is unowned and refuses fail-closed.
    for spec in inventory:
        steps.append(
            CommandStep(
                argv=("test", "-e", _installed_plist(launch_agents_dir, spec)),
                kind="probe",
                code="probe_plist_absent",
            )
        )
    for spec in inventory:
        steps.append(
            CommandStep(
                argv=("launchctl", "print", f"gui/{uid}/{spec.label}"),
                kind="probe",
                code="probe_label_absent",
            )
        )
    steps.append(
        CommandStep(serve_status_argv(), kind="probe", code="probe_serve_state_pre")
    )
    steps.append(
        CommandStep(funnel_status_argv(), kind="probe", code="probe_funnel_state")
    )
    for spec in inventory:
        installed = _installed_plist(launch_agents_dir, spec)
        # The exact bytes this plan installs are pure renderer output, so
        # their digest is knowable at plan time without any IO; it becomes
        # the durable identity recovery must match before compensating.
        identity = hashlib.sha256(render_plist(spec)).hexdigest()
        steps.append(
            CommandStep(
                argv=("cp", _rendered_plist(rendered_dir, spec), installed),
                kind="mutate",
                code="mutate_install_plist",
                compensation_argv=("rm", installed),
                compensation_code="compensate_remove_plist",
                reconcile_argv=("test", "-e", installed),
                reconcile_code="reconcile_install_plist",
                resource=installed,
                identity=identity,
                identity_argv=("shasum", "-a", "256", installed),
                identity_code="identity_install_plist",
            )
        )
        steps.append(
            CommandStep(
                argv=("launchctl", "bootstrap", f"gui/{uid}", installed),
                kind="mutate",
                code="mutate_bootstrap",
                compensation_argv=(
                    "launchctl",
                    "bootout",
                    f"gui/{uid}/{spec.label}",
                ),
                compensation_code="compensate_bootout",
                reconcile_argv=(
                    "launchctl",
                    "print",
                    f"gui/{uid}/{spec.label}",
                ),
                reconcile_code="reconcile_bootstrap",
                resource=f"gui/{uid}/{spec.label}",
                identity=identity,
                identity_argv=("shasum", "-a", "256", installed),
                identity_code="identity_bootstrap",
                identity_source_path=installed,
            )
        )
    return tuple(steps)


def plan_serve_enable(*, console_port: int) -> tuple[CommandStep, ...]:
    """Guarded tailnet-only serve enable; unsafe outcomes self-compensate."""
    return (
        CommandStep(
            serve_status_argv(), kind="probe", code="probe_serve_state_pre"
        ),
        CommandStep(funnel_status_argv(), kind="probe", code="probe_funnel_state"),
        CommandStep(
            serve_enable_argv(console_port),
            kind="mutate",
            code="mutate_serve_enable",
            compensation_argv=serve_reset_argv(),
            compensation_code="compensate_serve_reset",
        ),
        CommandStep(
            serve_status_argv(), kind="probe", code="probe_serve_state_post"
        ),
    )


def plan_uninstall(
    inventory: Sequence[ServiceSpec],
    *,
    launch_agents_dir: PurePosixPath,
    uid: int,
    console_port: int,
) -> tuple[CommandStep, ...]:
    del console_port  # reversal always resets serve state entirely
    steps: list[CommandStep] = [
        CommandStep(serve_reset_argv(), kind="mutate", code="mutate_serve_reset")
    ]
    for spec in reversed(inventory):
        steps.append(
            CommandStep(
                argv=("launchctl", "bootout", f"gui/{uid}/{spec.label}"),
                kind="mutate",
                code="mutate_bootout",
            )
        )
        steps.append(
            CommandStep(
                argv=("rm", _installed_plist(launch_agents_dir, spec)),
                kind="mutate",
                code="mutate_remove_plist",
            )
        )
    return tuple(steps)


def plan_status(
    inventory: Sequence[ServiceSpec], *, uid: int, console_port: int
) -> tuple[CommandStep, ...]:
    del console_port  # status only reads; verification uses the pre-state rule
    steps: list[CommandStep] = [
        CommandStep(
            argv=("launchctl", "print", f"gui/{uid}/{spec.label}"),
            kind="probe",
            code="probe_service_status",
        )
        for spec in inventory
    ]
    steps.append(
        CommandStep(serve_status_argv(), kind="probe", code="probe_serve_state_pre")
    )
    steps.append(
        CommandStep(funnel_status_argv(), kind="probe", code="probe_funnel_state")
    )
    return tuple(steps)


def _step_refusal(
    step: CommandStep, result: RunResult, *, console_port: int
) -> str | None:
    if step.code in {"probe_serve_state_pre", "probe_serve_state_post"}:
        raw = result.stdout if result.returncode == 0 else None
        try:
            verify_serve_status(
                raw,
                console_port=console_port,
                allow_absent=step.code == "probe_serve_state_pre",
            )
        except UnsafeTailscaleState as error:
            return error.code
        return None
    if step.code == "probe_funnel_state":
        if result.returncode != 0:
            return "funnel_state_uncertain"
        normalized = result.stdout.strip().lower().rstrip(".")
        if normalized not in _FUNNEL_OFF_MARKERS:
            return "funnel_state_uncertain"
        return None
    if step.code == "probe_plist_absent":
        outcome = _classify_absence_probe(step.argv, result)
        if outcome == "present":
            return "plist_preexisting"
        if outcome == "absent":
            return None
        return "plist_state_uncertain"
    if step.code == "probe_label_absent":
        outcome = _classify_absence_probe(step.argv, result)
        if outcome == "present":
            return "label_already_loaded"
        if outcome == "absent":
            return None
        return "label_state_uncertain"
    if result.returncode != 0:
        return step.code
    return None


def _classify_absence_probe(
    argv: Sequence[str], result: RunResult
) -> Literal["present", "absent", "uncertain"]:
    """Three-way, structural outcome of an existence probe.

    rc 0 proves presence (stdout is never parsed for values). Only the
    documented not-found code of each command proves absence; a runner-
    synthesized failure (timeout/spawn error mapped to rc 1) would otherwise
    collide with `test -e`'s not-found rc 1, so it and every other outcome
    (permission, unavailable domain, malformed invocation) are uncertainty.
    """
    if result.returncode == 0:
        return "present"
    if result.synthesized:
        return "uncertain"
    first_two = tuple(argv[:2])
    if first_two == ("test", "-e") and result.returncode == 1:
        return "absent"
    if (
        first_two == ("launchctl", "print")
        and result.returncode == _LAUNCHCTL_NOT_FOUND_RETURNCODE
    ):
        return "absent"
    return "uncertain"


class UnrecoverableJournal(Exception):
    """A prior-attempt journal cannot be trusted; refuse before any command."""

    code = "journal_unrecoverable"


class MutationJournal(Protocol):
    """Durable write-ahead record of this attempt's ownership claims."""

    def record(
        self,
        *,
        code: str,
        resource: str,
        state: str,
        identity: str | None = None,
    ) -> None: ...

    def load(self) -> tuple[dict[str, str], ...]: ...

    def clear(self) -> None: ...


class FileMutationJournal:
    """Ownership journal rewritten whole on every state transition.

    Content is only a list of ``{"code", "resource", "state"}`` entries
    plus an optional ``identity`` content digest for applied resources
    (paths, labels, static codes, and hashes — never any sensitive value), so an
    interrupted attempt leaves enough evidence to reconcile safely. Every
    write goes to a temp file and lands via ``os.replace`` after fsync: a
    crash can never leave a truncated journal, and the write-ahead ordering
    guarantees the journal never claims less than reality. The containing
    directory is also fsynced after the replace, so the rename itself is
    durable and the write-ahead claim survives a system crash, not just a
    process crash.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: list[dict[str, str]] = []

    def load(self) -> tuple[dict[str, str], ...]:
        """Strictly parse a prior attempt's journal, if one exists.

        Anything that is not exactly a list of unique
        ``{"code", "resource", "state"}`` string entries with known states
        (plus at most an optional string ``identity``) is untrusted
        evidence: raise rather than guess.
        """
        if not self._path.exists():
            return ()
        try:
            raw = json.loads(self._path.read_text())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            raise UnrecoverableJournal from None
        if not isinstance(raw, list):
            raise UnrecoverableJournal
        seen: set[tuple[str, str]] = set()
        for entry in raw:
            if (
                not isinstance(entry, dict)
                or set(entry) - {"identity"} != {"code", "resource", "state"}
                or not all(isinstance(value, str) for value in entry.values())
                or entry["state"] not in _JOURNAL_STATES
            ):
                raise UnrecoverableJournal
            key = (entry["code"], entry["resource"])
            if key in seen:
                raise UnrecoverableJournal
            seen.add(key)
        self._entries = [dict(entry) for entry in raw]
        return tuple(dict(entry) for entry in self._entries)

    def clear(self) -> None:
        self._entries = []
        self._write()

    def record(
        self,
        *,
        code: str,
        resource: str,
        state: str,
        identity: str | None = None,
    ) -> None:
        for entry in self._entries:
            if entry["code"] == code and entry["resource"] == resource:
                entry["state"] = state
                if identity is not None:
                    entry["identity"] = identity
                break
        else:
            new_entry = {"code": code, "resource": resource, "state": state}
            if identity is not None:
                new_entry["identity"] = identity
            self._entries.append(new_entry)
        self._write()

    def _write(self) -> None:
        temp_path = self._path.with_name(self._path.name + ".tmp")
        with open(temp_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(self._entries, indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, self._path)
        # The rename is only durable once the directory entry pointing at
        # it is itself fsynced; otherwise a crash right after os.replace
        # can still lose the rename on some filesystems.
        dir_fd = os.open(self._path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)


def _journal_record(
    journal: MutationJournal | None, step: CommandStep, state: str
) -> None:
    if journal is not None and step.resource is not None:
        # Identity lands in the same atomic journal write that marks the
        # resource applied, so ownership evidence is never durable without
        # the identity needed to verify it later.
        journal.record(
            code=step.code,
            resource=step.resource,
            state=state,
            identity=step.identity if state == "applied" else None,
        )


def _observed_sha256(result: RunResult) -> str | None:
    """The digest a ``shasum -a 256`` probe proved, or None.

    Anything but rc==0 from a real (non-synthesized) run with a leading
    64-hex-digit token is no evidence of the current content's identity.
    """
    if result.returncode != 0 or result.synthesized:
        return None
    tokens = result.stdout.split()
    if not tokens:
        return None
    digest = tokens[0].lower()
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        return None
    return digest


def _compensate(
    ledger: Sequence[CommandStep],
    runner: CommandRunner,
    journal: MutationJournal | None,
) -> tuple[tuple[tuple[str, int], ...], tuple[str, ...]]:
    """Run compensations for completed mutations in reverse completion order."""
    compensations: list[tuple[str, int]] = []
    residual_codes: list[str] = []
    for step in reversed(ledger):
        if step.compensation_argv is None or step.compensation_code is None:
            continue
        result = runner.run(step.compensation_argv, timeout=_COMMAND_TIMEOUT_SECONDS)
        compensations.append((step.compensation_code, result.returncode))
        if result.returncode != 0:
            residual_codes.append(step.compensation_code)
        _journal_record(
            journal,
            step,
            "compensated" if result.returncode == 0 else "residual",
        )
    return tuple(compensations), tuple(residual_codes)


def _job_backed_by(print_stdout: str, source_path: str) -> bool:
    """Whether ``launchctl print`` output proves the job's backing plist.

    Requires an exact ``path = <source_path>`` line (whitespace-stripped),
    never a substring hit: a foreign job loaded from a path that merely
    contains the owned path must not pass. An unexpected output format
    proves nothing and fails closed.
    """
    return any(
        line.strip() == f"path = {source_path}"
        for line in print_stdout.splitlines()
    )


def _recover_prior_attempt(
    steps: Sequence[CommandStep],
    runner: CommandRunner,
    journal: MutationJournal,
    records: list[tuple[str, int]],
    compensations: list[tuple[str, int]],
    residual_codes: list[str],
) -> str | None:
    """Resolve a prior attempt's journal before any fresh command runs.

    Unresolved claims are matched against the plan's exact mutate resources
    (anything else is foreign and refuses with zero commands run), then
    probed newest-first: proven absent claims resolve as unapplied. A
    claimed entry is pre-mutation intent only, recorded write-ahead before
    the mutation ran, and can never authorize compensation of a currently
    present resource — presence alone is not ownership evidence. An applied
    entry proves THIS attempt applied that resource, but says nothing about
    whatever occupies the path or label NOW: the present resource is
    compensated only when the entry's persisted identity exactly matches the
    probed current identity (and, for a loaded job, the reconcile probe
    shows it backed by the owned plist path). A claimed or residual entry
    found present, or an applied entry whose current identity is missing,
    unprovable, or different, is left exactly as recorded, for operator
    resolution, and refuses. Uncertainty or a failed compensation also
    preserves the entry as residual and refuses. A fresh attempt may begin
    only once every entry is terminal.
    """
    try:
        entries = journal.load()
    except UnrecoverableJournal as error:
        return error.code
    if not entries:
        return None
    unresolved = [
        entry
        for entry in entries
        if entry["state"] not in _TERMINAL_JOURNAL_STATES
    ]
    steps_by_claim = {
        (step.code, step.resource): step
        for step in steps
        if step.kind == "mutate" and step.resource is not None
    }
    matched: list[tuple[dict[str, str], CommandStep]] = []
    for entry in unresolved:
        step = steps_by_claim.get((entry["code"], entry["resource"]))
        if (
            step is None
            or step.reconcile_argv is None
            or step.reconcile_code is None
            or step.compensation_argv is None
            or step.compensation_code is None
        ):
            return "journal_foreign_entries"
        matched.append((entry, step))
    for entry, step in reversed(matched):
        probe = runner.run(step.reconcile_argv, timeout=_COMMAND_TIMEOUT_SECONDS)
        records.append((step.reconcile_code, probe.returncode))
        outcome = _classify_absence_probe(step.reconcile_argv, probe)
        if outcome == "absent":
            journal.record(
                code=entry["code"], resource=entry["resource"], state="unapplied"
            )
            continue
        if outcome == "uncertain":
            # Never compensate what cannot be proven applied; the claim
            # stays on record for the operator and the next attempt.
            journal.record(
                code=entry["code"], resource=entry["resource"], state="residual"
            )
            residual_codes.append(step.reconcile_code)
            return "journal_recovery_uncertain"
        if entry["state"] != "applied":
            # Proven present, but this attempt never proved it applied that
            # resource: a claimed (or already-residual) entry is
            # pre-mutation intent only, so presence alone can never
            # authorize compensating it. Leave the entry exactly as
            # recorded — nothing is written to the journal — for the
            # operator to resolve.
            residual_codes.append(step.reconcile_code)
            return "journal_claim_uncertain"
        expected = entry.get("identity")
        if (
            expected is None
            or step.identity_argv is None
            or step.identity_code is None
        ):
            # Applied without persisted identity (a legacy or foreign-era
            # record): whatever is present now cannot be proven to be the
            # resource this attempt applied. Preserve it and the entry.
            residual_codes.append(step.reconcile_code)
            return "journal_identity_uncertain"
        if step.identity_source_path is not None and not _job_backed_by(
            probe.stdout, step.identity_source_path
        ):
            # The loaded job is not backed by the owned installation: an
            # operator-managed job under the same label must never be
            # booted out on label presence alone.
            residual_codes.append(step.reconcile_code)
            return "journal_identity_uncertain"
        identity_probe = runner.run(
            step.identity_argv, timeout=_COMMAND_TIMEOUT_SECONDS
        )
        records.append((step.identity_code, identity_probe.returncode))
        if _observed_sha256(identity_probe) != expected:
            # The current content is not (provably) what this attempt
            # applied — likely an operator replacement at the same path.
            # Leave both the resource and the entry untouched and refuse.
            residual_codes.append(step.identity_code)
            return "journal_identity_uncertain"
        compensation = runner.run(
            step.compensation_argv, timeout=_COMMAND_TIMEOUT_SECONDS
        )
        compensations.append((step.compensation_code, compensation.returncode))
        if compensation.returncode != 0:
            journal.record(
                code=entry["code"], resource=entry["resource"], state="residual"
            )
            residual_codes.append(step.compensation_code)
            return "journal_recovery_failed"
        journal.record(
            code=entry["code"], resource=entry["resource"], state="compensated"
        )
    journal.clear()
    return None


def execute_plan(
    steps: Sequence[CommandStep],
    runner: CommandRunner,
    *,
    console_port: int,
    journal: MutationJournal | None = None,
) -> ExecutionReport:
    records: list[tuple[str, int]] = []
    recovery_compensations: list[tuple[str, int]] = []
    recovery_residuals: list[str] = []
    if journal is not None:
        # A prior attempt's journal must be terminally clean before this
        # attempt runs anything; preflight would otherwise refuse on our own
        # leftover resources and mask recovery forever.
        recovery_refusal = _recover_prior_attempt(
            steps,
            runner,
            journal,
            records,
            recovery_compensations,
            recovery_residuals,
        )
        if recovery_refusal is not None:
            return ExecutionReport(
                records=tuple(records),
                completed=False,
                refusal_code=recovery_refusal,
                compensations=tuple(recovery_compensations),
                residual_codes=tuple(recovery_residuals),
            )
    ledger: list[CommandStep] = []
    reconcile_residuals: list[str] = []
    for step in steps:
        if step.kind == "mutate":
            # Write-ahead: the claim is durable before the mutation runs.
            _journal_record(journal, step, "claimed")
        result = runner.run(step.argv, timeout=_COMMAND_TIMEOUT_SECONDS)
        records.append((step.code, result.returncode))
        refusal = _step_refusal(step, result, console_port=console_port)
        if refusal is not None:
            if (
                step.kind == "mutate"
                and step.reconcile_argv is not None
                and step.reconcile_code is not None
            ):
                # The failed command may have partially applied; decide from
                # post-state, not the return code. Preflight proved prior
                # absence, so compensating restores the exact prior state.
                reconcile = runner.run(
                    step.reconcile_argv, timeout=_COMMAND_TIMEOUT_SECONDS
                )
                records.append((step.reconcile_code, reconcile.returncode))
                outcome = _classify_absence_probe(step.reconcile_argv, reconcile)
                if outcome == "present":
                    _journal_record(journal, step, "applied")
                    # Appended last so its compensation runs first below.
                    ledger.append(step)
                elif outcome == "absent":
                    _journal_record(journal, step, "unapplied")
                else:
                    # Uncertain post-state: keep the ownership claim and
                    # never compensate what cannot be proven applied.
                    _journal_record(journal, step, "residual")
                    reconcile_residuals.append(step.reconcile_code)
            compensations, residual_codes = _compensate(ledger, runner, journal)
            return ExecutionReport(
                records=tuple(records),
                completed=False,
                refusal_code=refusal,
                compensations=tuple(recovery_compensations) + compensations,
                residual_codes=tuple(reconcile_residuals) + residual_codes,
            )
        if step.kind == "mutate":
            _journal_record(journal, step, "applied")
            ledger.append(step)
    for step in ledger:
        # Terminal success: a later attempt must never mistake a completed
        # installation for an interrupted one and tear it down.
        _journal_record(journal, step, "installed")
    return ExecutionReport(
        records=tuple(records),
        completed=True,
        refusal_code=None,
        compensations=tuple(recovery_compensations),
    )


def _render_runbook(
    inventory: Sequence[ServiceSpec], *, console_port: int
) -> str:
    start_order = ", ".join(spec.label for spec in inventory)
    stop_order = ", ".join(spec.label for spec in reversed(inventory))
    enable_command = " ".join(serve_enable_argv(console_port))
    labels = [spec.label for spec in inventory]
    launchctl_prints = "\n".join(
        f"- `launchctl print gui/<uid>/{label}`" for label in labels
    )
    return (
        "# Operator activation runbook\n"
        "\n"
        "Everything in this directory is a generated artifact: nothing was\n"
        "installed, loaded, or started. Activation is an explicit, reversible\n"
        "operator sequence performed with the commands below, in order.\n"
        "\n"
        "Funnel must never be enabled. The operations console is exposed to\n"
        f"the tailnet only, from loopback http://127.0.0.1:{console_port}\n"
        "behind Tailscale Serve. Every activating command refuses to change\n"
        "anything when the observed Tailscale state is not provably safe, and\n"
        "a failed attempt rolls back its own completed changes in reverse\n"
        "order.\n"
        "\n"
        f"Start order: {start_order}.\n"
        f"Stop order: {stop_order}.\n"
        "\n"
        "## Activate (in this order)\n"
        "\n"
        "1. Run `hermes-orchestrator remote-auth-init`. The remote\n"
        "   application credential is displayed exactly once; store it in a\n"
        "   secure credential manager on the phone.\n"
        "2. Run `hermes-orchestrator deploy-render ...` and review every\n"
        "   artifact (the launchd property lists and serve-plan.json).\n"
        "   Optionally install hermes-newsyslog.conf into /etc/newsyslog.d\n"
        "   (requires sudo) for bounded log rotation.\n"
        "3. Install and start the local services:\n"
        "   `hermes-orchestrator deploy-install ... --execute`. Probes gate\n"
        "   every mutation: entrypoint availability, plutil -lint, the\n"
        "   tailnet-only serve state, and funnel-off verification. A failed\n"
        "   attempt rolls back the changes it completed.\n"
        "4. Verify locally:\n"
        f"   `curl http://127.0.0.1:{console_port}/healthz` returns 200;\n"
        f"   `curl -i http://127.0.0.1:{console_port}/api/status` returns\n"
        "   401 (the authentication boundary is intact).\n"
        "5. Enable tailnet-only exposure:\n"
        "   `hermes-orchestrator deploy-serve-enable ... --execute`, which\n"
        f"   runs exactly `{enable_command}`.\n"
        "   Funnel is never enabled; an unsafe post-enable state is\n"
        "   automatically reverted before the command reports failure.\n"
        "6. Inspect `tailscale serve status --json`: exactly one loopback\n"
        f"   backend http://127.0.0.1:{console_port}, and Funnel off\n"
        "   everywhere.\n"
        "7. On the phone via MagicDNS: open the tailnet HTTPS URL and\n"
        "   authenticate at /login with the saved credential.\n"
        "8. To disable remote access run `tailscale serve reset`; the local\n"
        "   launchd jobs keep running. Full teardown is\n"
        "   `hermes-orchestrator deploy-uninstall ... --execute` (serve\n"
        "   reset, bootout in reverse start order, property lists removed;\n"
        "   log files are preserved).\n"
        "\n"
        "## Status\n"
        "\n"
        "- `hermes-orchestrator deploy-status ... --execute`\n"
        f"{launchctl_prints}\n"
        "- `tailscale serve status --json`\n"
        "- `tailscale funnel status` (must report nothing enabled)\n"
        "\n"
        "## Recovery\n"
        "\n"
        "- `tailscale serve reset` reverts all serve configuration at any\n"
        "  time; local services keep running.\n"
        "- Every executed install records its ownership claims in\n"
        "  install-journal.json (inside the rendered directory) before each\n"
        "  change, and the content identity (a SHA-256 digest of the exact\n"
        "  property list bytes) alongside every applied resource. After a\n"
        "  refusal or an interruption, re-run\n"
        "  `hermes-orchestrator deploy-install ... --execute`: the journal\n"
        "  is validated first, each recorded claim is probed, resources the\n"
        "  journal proves this attempt applied are compensated exactly —\n"
        "  only after the recorded identity matches what is currently\n"
        "  present — and provably absent ones are marked unapplied. A fresh\n"
        "  attempt begins only once every recorded claim is resolved.\n"
        "- A merely claimed resource is never removed: if something now\n"
        "  exists at a claimed path or label without applied evidence, the\n"
        "  command refuses and preserves the claim so you can determine\n"
        "  ownership by hand before retrying.\n"
        "- A replacement is never removed either: if the file at an applied\n"
        "  path no longer matches the recorded identity, or the job under\n"
        "  an applied label is not backed by the owned property list, the\n"
        "  command refuses, preserves the resource and the journal entry,\n"
        "  and leaves ownership for you to determine by hand.\n"
        "- When a probe or compensation cannot prove the state, the command\n"
        "  refuses and preserves the unresolved entries as residual;\n"
        "  inspect those resources by hand before retrying.\n"
        "- A malformed or unrecognized journal refuses before any command\n"
        "  runs; investigate it manually and never delete it until the\n"
        "  listed resources have been confirmed safe.\n"
        "- `hermes-orchestrator deploy-uninstall ... --execute` restores the\n"
        "  pre-install state entirely.\n"
    )


def render_artifacts(
    inventory: Sequence[ServiceSpec], output_dir: Path, *, console_port: int
) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for spec in inventory:
        plist_path = output_dir / f"{spec.label}.plist"
        plist_path.write_bytes(render_plist(spec))
        paths.append(plist_path)
    conf_path = output_dir / "hermes-newsyslog.conf"
    conf_path.write_text(render_newsyslog_conf(inventory))
    paths.append(conf_path)
    serve_plan_path = output_dir / "serve-plan.json"
    serve_plan_path.write_text(
        json.dumps(
            {
                "backend": f"http://127.0.0.1:{console_port}",
                "enable": list(serve_enable_argv(console_port)),
                "status": list(serve_status_argv()),
                "reset": list(serve_reset_argv()),
                "funnel_status": list(funnel_status_argv()),
            },
            indent=2,
        )
        + "\n"
    )
    paths.append(serve_plan_path)
    runbook_path = output_dir / "OPERATOR.md"
    runbook_path.write_text(_render_runbook(inventory, console_port=console_port))
    paths.append(runbook_path)
    return tuple(paths)
