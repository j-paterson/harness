"""Freeze-boundary candidate emission: immutable manifest plus queue wake.

INFRA-166: the Claude lead's pull request boundary becomes a real, typed
``FABLE_*`` wake. The candidate is proven from the local clone (clean tree,
feature branch head pushed to origin, base on the integration branch), the
manifest is published immutably, and the wake is delivered through the
argv-safe ``codex queue`` adapter with durable registration. Re-emitting the
same candidate is idempotent: the immutable manifest is reused and the
registered wake deduplicates.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from hermes_orchestrator.ci_window import MergeWindowExhausted, PriorMergeFailed
from hermes_orchestrator.codex_queue import QueueDeliveryResult
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.git import GitError, GitRunner
from hermes_orchestrator.manifests import (
    MANIFEST_VERSION,
    WAKE_STATUSES,
    CandidateManifest,
    ManifestError,
    WakeEvent,
    manifest_digest_for,
    read_manifest,
    wake_event_for,
    write_manifest,
)
from hermes_orchestrator.review_drift import DriftVerdict
from hermes_orchestrator.review_intake import CandidateRejected
from hermes_orchestrator.verdicts import CorrectionPacket


class _DriftHintedWakeEvent:
    """A ``WakeEvent`` view whose rendered text also carries an
    administrative-drift hint (INFRA-200).

    Mirrors ``codex_merger._PrefixedWakeEvent`` exactly: every field the
    delivery path reads (status, issue_id, candidate_sha, base_sha,
    manifest_path, event_id, manifest_digest) is forwarded unchanged to
    the wrapped event via ``__getattr__``, so this is transparent to
    ``codex_queue.CodexQueueDelivery.deliver``, ``register_wake``,
    manifest verification, and every other consumer -- the wake's
    content-addressed identity is never touched. Only :meth:`render`
    differs, appending one trailing advisory line to whatever the
    wrapped event already renders; nothing sends a second turn for it.
    """

    __slots__ = ("_hint", "_inner")

    def __init__(self, inner: WakeEvent, hint: str) -> None:
        self._inner = inner
        self._hint = hint

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def render(self, generation: int) -> str:
        return f"{self._inner.render(generation)}\n{self._hint}"


def wake_event_with_drift_hint(
    event: WakeEvent, drift: DriftVerdict | None
) -> WakeEvent:
    """Append the administrative-drift hint to ``event``'s rendered text.

    INFRA-200: ``drift`` -- ``review_intake.AdmittedCandidate.drift`` --
    is purely advisory (see ``review_drift.py``). ``None`` (no drift
    source configured, or nothing to compare against) and a
    ``"semantic"`` verdict both deliver ``event`` completely unwrapped:
    Sol gets no scoping hint and reviews the full diff exactly as before
    this hint existed. Only an ``"administrative"`` verdict wraps the
    event, appending one trailing line; every field a delivery or
    admission path reads -- manifest digest, candidate SHA, event id,
    status, issue id, base SHA -- is forwarded unchanged (see
    :class:`_DriftHintedWakeEvent`), so the wake's content-addressed
    identity and everything durably recorded from it are byte-identical
    with and without the hint.
    """

    if drift is None or drift.kind == "semantic":
        return event
    hint = (
        f"drift: administrative — {drift.reason}; scope replay to the "
        "changed paths, identity checks unchanged"
    )
    return _DriftHintedWakeEvent(event, hint)  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class FreezeFacts:
    """The proven identity of one candidate at the freeze boundary."""

    head: str
    branch: str
    base: str
    changed_files: tuple[str, ...]


def run_freeze_gate(
    run: Callable[..., str],
    project: ProjectConfig,
    *,
    expect_pushed: bool,
) -> FreezeFacts:
    """The one mandatory candidate gate, shared by every publisher.

    ``run(*git_args) -> stdout`` must raise :class:`EmissionBlocked`
    on git failure. With ``expect_pushed=False`` the gate proves
    everything provable before a push (clean tree, a real feature
    branch, the integration base, a non-empty diff); with
    ``expect_pushed=True`` it additionally proves the local HEAD is
    the exact pushed branch head. The candidate emitter and the
    reviewer-fix helper both invoke this same gate — there is no
    second copy to drift.
    """

    if run("status", "--porcelain").strip():
        raise EmissionBlocked("working tree is not clean at the freeze boundary")
    head = run("rev-parse", "HEAD").strip()
    branch = run("rev-parse", "--abbrev-ref", "HEAD").strip()
    if _SHA_PATTERN.match(head) is None:
        raise EmissionBlocked("HEAD is not a full commit sha")
    if branch in ("HEAD", "", project.integration_branch):
        raise EmissionBlocked("candidate must be on a pushed feature branch")
    run("fetch", "--", "origin", project.integration_branch)
    run("fetch", "--", "origin", branch)
    if expect_pushed:
        remote_head = run("rev-parse", f"origin/{branch}").strip()
        if remote_head != head:
            raise EmissionBlocked("local HEAD is not the pushed branch head")
    base = run(
        "merge-base", "HEAD", f"origin/{project.integration_branch}"
    ).strip()
    if _SHA_PATTERN.match(base) is None:
        raise EmissionBlocked("could not determine the integration base")
    changed = tuple(
        line
        for line in run("diff", "--name-only", base, head).splitlines()
        if line.strip()
    )
    if not changed:
        raise EmissionBlocked("candidate has no changes against its base")
    return FreezeFacts(head=head, branch=branch, base=base, changed_files=changed)

_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class WakeDeliverer(Protocol):
    """The queue delivery boundary (``codex queue`` only)."""

    async def deliver(
        self, project_key: str, event: WakeEvent
    ) -> QueueDeliveryResult: ...


class IntakeGatePort(Protocol):
    def validate(self, project_key: str, candidate: CandidateManifest) -> None: ...


class CorrectionSinkPort(Protocol):
    def deliver(
        self,
        issue_id: str,
        packets: tuple[CorrectionPacket, ...],
        *,
        source: str = ...,
    ) -> object: ...


class EmissionBlocked(RuntimeError):
    """The local clone is not at a valid immutable freeze boundary."""


class LeaseRow(Protocol):
    """The exact ``worktree_leases`` (migration 0018) columns L5 reads."""

    lease_id: str
    project_key: str
    issue_id: str
    path: str
    branch: str
    checkpoint_sha: str | None


class LeaseReaderPort(Protocol):
    """Read access to live worktree leases, matching ``WorktreeLeases``."""

    def active(self, project_key: str | None = None) -> tuple[LeaseRow, ...]: ...


class DurableWakePort(Protocol):
    """Read access to the durable wake registry (``wake_deliveries``).

    A manifest file surviving on disk is never sufficient evidence of a
    real publication by itself -- INFRA-219 L5 requires a matching
    durable wake row for the exact event before a stale file left over
    from a wrong-SHA publication can ever be adopted as a reused
    manifest.
    """

    def exists(self, project_key: str, event_id: str) -> bool: ...


class LeaseWriterSnapshot(Protocol):
    """The exact ``worktree_leases`` writer columns (migration 0063)."""

    writer_role: str
    writer_ref: str | None
    writer_generation: int
    submitted_candidate_sha: str | None


class LeaseWriterTransferPort(LeaseReaderPort, Protocol):
    """Read the live lease's writer identity and transfer its custody.

    INFRA-222: the immutable review artifact is the recorded candidate
    SHA; the worktree is an operational resource whose exclusive writer
    may change. ``WorktreeLeases`` (migration 0063) satisfies this in
    full -- ``active`` (inherited from :class:`LeaseReaderPort`) is what
    :func:`resolve_lane` already reads, ``get`` answers the current
    writer role/ref/generation and recorded candidate sha, and
    ``transfer_writer`` is the generation-keyed compare-and-swap that
    moves custody. Every refusal is expected to raise -- the exact
    exception type is never inspected here, only caught and translated
    to :class:`EmissionBlocked`.
    """

    def get(self, lease_id: str) -> LeaseWriterSnapshot: ...

    def transfer_writer(
        self,
        lease_id: str,
        *,
        expected_writer_role: str,
        expected_writer_ref: str | None,
        to_writer_role: str,
        to_writer_ref: str | None,
        observed_head: str,
        tree_clean: bool,
        submitted_candidate_sha: str | None = None,
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class ResolvedLane:
    """The one publishable lane for an issue: its live worktree lease.

    INFRA-219 L5: publication is explicitly targeted to ONE admitted
    issue lane and must derive branch, HEAD, and changed files from
    that lane's bound worktree -- never from whichever checkout the
    coordinating lead happens to occupy. This is the frozen, proven
    binding :meth:`CandidateEmitter.emit` freezes against.
    """

    lease_id: str
    project_key: str
    issue_id: str
    path: Path
    branch: str
    expected_head: str | None


def resolve_lane(
    leases: LeaseReaderPort, project_key: str, issue_id: str
) -> ResolvedLane:
    """Resolve the ONE live worktree lease bound to ``issue_id``.

    INFRA-219 L5 anchor: ``worktree_leases`` (migration 0018) already
    binds (project_key, issue_id, repo_path, path, branch) per lease,
    with a unique live-path index; a live lease is any row whose
    ``state`` is not ``'reclaimed'``, exactly what
    :meth:`WorktreeLeases.active` already returns. This is the read
    boundary that lets ``candidate-ready`` freeze the requested issue's
    OWN worktree instead of whatever checkout the lead currently
    occupies -- the reproduced defect (INFRA-218 manifest frozen at
    ``feature/infra-217``'s head). Fails closed, before any manifest
    write or wake, when no live lease binds the issue or when more
    than one does: an unresolvable lane is exactly the hazard this
    closes, never a case to guess through.
    """

    live = tuple(
        lease for lease in leases.active(project_key) if lease.issue_id == issue_id
    )
    if not live:
        raise EmissionBlocked(
            f"no live worktree lease binds issue {issue_id!r} in project "
            f"{project_key!r} -- candidate publication refuses to freeze "
            "an unbound checkout"
        )
    if len(live) > 1:
        raise EmissionBlocked(
            f"issue {issue_id!r} in project {project_key!r} has "
            f"{len(live)} live worktree leases -- publication cannot "
            "resolve one unambiguous lane"
        )
    lease = live[0]
    return ResolvedLane(
        lease_id=lease.lease_id,
        project_key=lease.project_key,
        issue_id=lease.issue_id,
        path=Path(lease.path),
        branch=lease.branch,
        expected_head=lease.checkpoint_sha,
    )


@dataclass(frozen=True, slots=True)
class EmissionResult:
    """One emitted candidate: its wake, manifest path, and delivery."""

    event: WakeEvent
    manifest_path: Path
    delivery: QueueDeliveryResult
    reused_manifest: bool
    intake: str = "clear"
    intake_reason: str = ""


class CandidateEmitter:
    """Publish one immutable candidate and wake the project's Merger."""

    def __init__(
        self,
        *,
        projects: Mapping[str, ProjectConfig],
        git: GitRunner,
        manifest_root: Path,
        delivery: WakeDeliverer,
        now: Callable[[], datetime] | None = None,
        intake_gate: IntakeGatePort | None = None,
        lead: CorrectionSinkPort | None = None,
        issue_for_failure: Callable[[str, str], str | None] | None = None,
        durable_wake: DurableWakePort | None = None,
        lease_transfer: LeaseWriterTransferPort | None = None,
        reviewer_ref: Callable[[str], str | None] | None = None,
    ) -> None:
        self._projects = dict(projects)
        self._git = git
        self._manifest_root = manifest_root
        self._delivery = delivery
        self._now = now or (lambda: datetime.now(UTC))
        self._intake_gate = intake_gate
        self._lead = lead
        self._issue_for_failure = issue_for_failure
        # INFRA-219 L5: when wired, a manifest file surviving on disk is
        # never adopted for reuse without a matching durable wake row for
        # the exact event -- a stale file from a wrong-SHA publication
        # (the reproduced defect) can never be reused. Optional so a
        # caller that has not wired the durable wake registry keeps
        # today's behavior rather than being forced to change at once.
        self._durable_wake = durable_wake
        # INFRA-222: when wired, the resolved lane's worktree-lease
        # writer is handed from fable to sol, atomically, right after the
        # candidate is proven and before the Merger is ever woken --
        # optional so a caller that has not wired the lease-transfer port
        # (or that never resolves a lane) keeps today's behavior exactly.
        self._lease_transfer = lease_transfer
        # project_key -> the bound Sol reviewer's thread/generation
        # identity, recorded on the lease as ``writer_ref`` purely for
        # operator visibility; never read back or compared by this class.
        self._reviewer_ref = reviewer_ref

    async def emit(
        self,
        project_key: str,
        issue_id: str,
        *,
        verification: tuple[tuple[str, str], ...],
        blockers: tuple[str, ...] = (),
        status: str = "FABLE_READY",
        lead_checkout: Path | None = None,
        resolved_lane: ResolvedLane | None = None,
    ) -> EmissionResult:
        project = self._projects.get(project_key)
        if project is None:
            raise EmissionBlocked(f"unknown project {project_key!r}")
        if status not in WAKE_STATUSES:
            raise EmissionBlocked(f"unknown wake status {status!r}")
        if not verification:
            raise EmissionBlocked("a candidate requires recorded verification")
        # INFRA-219 L5: a resolved lane -- the requested issue's OWN live
        # worktree lease -- is the targeted publication boundary and
        # replaces "whatever checkout the lead currently occupies" as the
        # default source of branch/HEAD/changed files. This is exactly
        # the reproduced defect: `candidate-ready INFRA-218` froze
        # `lead_checkout` (the lead's own, unrelated, checkout) instead
        # of INFRA-218's bound worktree. When a resolved lane is
        # supplied it wins outright -- `lead_checkout` is never
        # consulted -- so a lead sitting in the wrong lane's checkout can
        # never again leak into the frozen candidate.
        if resolved_lane is not None and (
            resolved_lane.project_key != project_key
            or resolved_lane.issue_id != issue_id
        ):
            raise EmissionBlocked(
                "resolved lane identity does not match the requested "
                f"candidate ({project_key!r}, {issue_id!r})"
            )
        # The durable project path stays anchored to the stable primary
        # checkout, so the candidate branch usually lives in the lead's
        # own linked worktree. The freeze boundary is that checkout —
        # but only after its git common dir proves it belongs to this
        # exact project repository; a foreign checkout is refused before
        # any freeze check runs.
        repo = project.repo_path
        checkout = resolved_lane.path if resolved_lane is not None else lead_checkout
        if checkout is not None and Path(checkout).resolve() != repo.resolve():
            claimed = self._run(
                Path(checkout),
                "rev-parse",
                "--path-format=absolute",
                "--git-common-dir",
            ).strip()
            anchor = self._run(
                repo,
                "rev-parse",
                "--path-format=absolute",
                "--git-common-dir",
            ).strip()
            if claimed != anchor:
                raise EmissionBlocked(
                    "the candidate checkout does not belong to the "
                    "project repository"
                )
            repo = Path(checkout)
        if resolved_lane is not None:
            # The lease row's path is a durable claim, not yet proven --
            # confirm the worktree git itself reports is the exact
            # checkout the lease names before freezing anything from it.
            toplevel = self._run(
                repo,
                "rev-parse",
                "--path-format=absolute",
                "--show-toplevel",
            ).strip()
            if Path(toplevel).resolve() != resolved_lane.path.resolve():
                raise EmissionBlocked(
                    "the resolved worktree lease path disagrees with the "
                    "checkout actually frozen"
                )
        facts = run_freeze_gate(
            lambda *args: self._run(repo, *args),
            project,
            expect_pushed=True,
        )
        head, branch = facts.head, facts.branch
        base, changed = facts.base, facts.changed_files
        if resolved_lane is not None:
            if branch != resolved_lane.branch:
                raise EmissionBlocked(
                    f"resolved lease branch {resolved_lane.branch!r} "
                    f"disagrees with the frozen branch {branch!r}"
                )
            if (
                resolved_lane.expected_head is not None
                and head != resolved_lane.expected_head
            ):
                raise EmissionBlocked(
                    "frozen HEAD disagrees with the resolved lease's "
                    "expected head"
                )
        # A fresh event per freeze boundary: a deferred candidate must be
        # re-woken as a new event, and the durable wake registry deduplicates
        # by candidate identity, so re-emission is always safe.
        stamp = self._now().astimezone(UTC).strftime("%Y%m%dT%H%M%S")
        event_id = f"{status.lower()}-{issue_id.lower()}-{head[:12]}-{stamp}"
        manifest = CandidateManifest(
            manifest_version=MANIFEST_VERSION,
            event_id=event_id,
            status=status,
            candidate_sha=head,
            base_sha=base,
            branch=branch,
            linear_issues=(issue_id,),
            changed_files=changed,
            # Advisory pass-through (operator directive, 2026-08-30):
            # verification entries are recorded exactly as supplied —
            # Sol and CI own verification, admission does not.
            verification=verification,
            blockers=blockers,
            created_at=self._now().astimezone(UTC).isoformat(),
        )
        reused = False
        try:
            path = write_manifest(self._manifest_root, manifest, head_sha=head)
        except ManifestError as error:
            if "immutable" not in str(error):
                raise EmissionBlocked(str(error)) from error
            path = self._manifest_root / f"{event_id}.json"
            existing = read_manifest(path, root=self._manifest_root)
            if (
                existing.candidate_sha != head
                or existing.base_sha != base
                or existing.branch != branch
                or existing.status != status
            ):
                raise EmissionBlocked(
                    "an immutable manifest already exists for this event with "
                    "a different candidate identity"
                ) from error
            # INFRA-219 L5: a manifest file on disk is never eligible for
            # reuse without a matching durable wake row for this exact
            # event -- a stale file surviving from a wrong-SHA
            # publication (the reproduced defect's leftover artifact)
            # must never be silently adopted.
            if self._durable_wake is not None and not self._durable_wake.exists(
                project_key, event_id
            ):
                raise EmissionBlocked(
                    "a manifest file exists for this event with no "
                    "matching durable wake -- a stale manifest is never "
                    "eligible for reuse"
                ) from error
            manifest = existing
            reused = True
        event = wake_event_for(manifest, path)
        assert event.manifest_digest == manifest_digest_for(manifest)
        # The durable intake boundary runs BEFORE the Merger is woken: CI
        # reconciliation is bound to this event, so the turn-time gate
        # replays the same decision with zero CI calls, and a deferred or
        # blocked candidate never triggers a review turn.
        intake, intake_reason = self._intake(project_key, issue_id, manifest)
        if intake != "clear":
            return EmissionResult(
                event=event,
                manifest_path=path,
                delivery=QueueDeliveryResult(
                    delivered=False,
                    attempts=0,
                    thread_id=None,
                    generation=None,
                    reason=f"intake_{intake}",
                ),
                reused_manifest=reused,
                intake=intake,
                intake_reason=intake_reason,
            )
        # INFRA-222: Fable's exclusive writer lease on the issue's
        # worktree is handed to Sol here -- after the candidate sha is
        # proven and the tree is proven clean and pushed (``facts``
        # above), but strictly before the Merger is woken, so a refused
        # hand-over never leaves a wake delivered against a lease Sol
        # does not yet hold.
        if self._lease_transfer is not None and resolved_lane is not None:
            self._hand_over_to_sol(resolved_lane, candidate_sha=head)
        delivery = await self._delivery.deliver(project_key, event)
        return EmissionResult(
            event=event,
            manifest_path=path,
            delivery=delivery,
            reused_manifest=reused,
        )

    def _intake(
        self, project_key: str, issue_id: str, manifest: CandidateManifest
    ) -> tuple[str, str]:
        if self._intake_gate is None:
            return "clear", ""
        try:
            self._intake_gate.validate(project_key, manifest)
        except PriorMergeFailed as blocked:
            packet = blocked.packet
            if self._lead is not None:
                owner = None
                if self._issue_for_failure is not None:
                    owner = self._issue_for_failure(project_key, packet.reviewed_sha)
                self._lead.deliver(owner or issue_id, (packet,), source="ci_failure")
            return "blocked_prior_failure", str(blocked)
        except MergeWindowExhausted as deferred:
            return "deferred", str(deferred)
        except CandidateRejected as rejected:
            return "rejected", str(rejected)
        return "clear", ""

    def _hand_over_to_sol(
        self, lane: ResolvedLane, *, candidate_sha: str
    ) -> None:
        """Transfer the resolved lane's writer lease from fable to sol.

        INFRA-222: a re-emission (the durable manifest and wake both
        deduplicate on identity) whose lease is already sol-owned with
        this EXACT candidate sha is a replay, not a conflict -- it is a
        no-op. Any other refusal (a dirty tree, a writer already
        replaced by something else, a stale generation, or a lease
        already sol-owned under a DIFFERENT candidate sha) raises
        :class:`EmissionBlocked` before the wake is ever delivered.
        """

        assert self._lease_transfer is not None
        try:
            current = self._lease_transfer.get(lane.lease_id)
        except Exception as error:
            raise EmissionBlocked(
                f"could not read worktree lease {lane.lease_id!r} before "
                f"transferring its writer to sol: {error}"
            ) from error
        if (
            current.writer_role == "sol"
            and current.submitted_candidate_sha == candidate_sha
        ):
            return
        to_writer_ref = (
            self._reviewer_ref(lane.project_key)
            if self._reviewer_ref is not None
            else None
        )
        try:
            self._lease_transfer.transfer_writer(
                lane.lease_id,
                expected_writer_role="fable",
                expected_writer_ref=None,
                to_writer_role="sol",
                to_writer_ref=to_writer_ref,
                observed_head=candidate_sha,
                tree_clean=True,
                submitted_candidate_sha=candidate_sha,
            )
        except Exception as error:
            raise EmissionBlocked(
                f"worktree lease {lane.lease_id!r} writer transfer to sol "
                f"was refused: {error}"
            ) from error

    def _run(self, repo: Path, *args: str) -> str:
        try:
            result = self._git.run(("git", *args), repo)
        except GitError as error:
            raise EmissionBlocked(str(error)) from error
        if result.returncode != 0:
            raise EmissionBlocked(
                f"git {args[0]} failed with exit code {result.returncode}"
            )
        return result.stdout
