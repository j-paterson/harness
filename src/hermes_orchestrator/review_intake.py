"""Admit exactly one validated immutable candidate per Merger wake."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from hermes_orchestrator.codex_merger import ReviewerChannel
from hermes_orchestrator.manifests import (
    CandidateManifest,
    ManifestError,
    ManifestSnapshot,
    WakeEvent,
    read_manifest_snapshot,
)

#: ``(project_key, branch) -> sha``. Three outcomes, represented
#: distinctly (INFRA-217, Sol correction c02dc0fe): the ref resolves, and
#: its commit SHA is returned; the ref is AUTHORITATIVELY ABSENT -- a
#: fetch that itself succeeded, followed by a local resolution failure of
#: ``origin/<branch>``, meaning the remote was reachable and genuinely
#: has no such branch -- represented by returning ``""``; or any OTHER
#: failure (a failed fetch -- network, auth, unreachable remote -- or an
#: unexpected local resolution error), which raises
#: :class:`BranchHeadUnknown` rather than returning ``""``, so it can
#: never be conflated with authoritative absence.
BranchHead = Callable[[str, str], str]
#: ``(project_key, branch, candidate_sha) -> bool``. Proves the exact
#: reviewed SHA is an already-merged, same-repository pull request
#: targeting the integration branch (INFRA-217).
MergedCandidateProof = Callable[[str, str, str], bool]
BasePolicy = Callable[[str, str], bool]


class BranchHeadUnknown(RuntimeError):
    """A ``BranchHead`` port could not determine whether the ref exists.

    Raised for a fetch failure, an authentication failure, or a local Git
    resolution error that is NOT the specific "fetch succeeded, ref does
    not resolve" case (INFRA-217, Sol correction c02dc0fe). This is never
    proof of branch absence: a transient fetch, authentication, or local
    Git failure must fail closed exactly as an unknown/mismatched head
    does, even when an exact merged pull request can be discovered
    independently -- only authoritative absence (a ``BranchHead`` port
    returning ``""``) is eligible for the merged-candidate-proof excuse.
    """


class AdmissionStore(Protocol):
    """The durable surface candidate admission depends on."""

    def read_channel(self, project_key: str) -> ReviewerChannel | None: ...

    def admit_wake(
        self,
        project_key: str,
        event: WakeEvent,
        *,
        thread_id: str,
        generation: int,
        manifest: ManifestSnapshot,
    ) -> bool: ...


class IntakeGate(Protocol):
    """Validation port for the explicit intake boundary before admission.

    The deterministic GitHub pull-request/queue adapter (INFRA-164) and the
    CircleCI reconciliation adapter (INFRA-165) implement this port; it runs
    after local candidate validation and before the admission
    compare-and-swap, and it fails closed by raising.
    """

    def validate(
        self, project_key: str, candidate: CandidateManifest
    ) -> None: ...


class CandidateRejected(RuntimeError):
    """Raised with a bounded diagnostic; no review turn, no state change."""


class CompositeIntakeGate:
    """Run ordered intake gates; the first fail-closed gate stops the intake.

    Ordering is contractual: the CircleCI reconciliation gate (INFRA-165)
    runs before the GitHub pull-request gate (INFRA-164) so every prior
    merged item is reconciled before the new candidate is considered.
    """

    def __init__(self, gates: tuple[IntakeGate, ...]) -> None:
        if not gates:
            raise ValueError("at least one intake gate is required")
        for gate in gates:
            if not hasattr(gate, "validate"):
                raise ValueError("every intake gate must expose validate")
        self._gates = tuple(gates)

    def validate(self, project_key: str, candidate: CandidateManifest) -> None:
        for gate in self._gates:
            gate.validate(project_key, candidate)


@dataclass(frozen=True, slots=True)
class AdmittedCandidate:
    """One immutable candidate admitted for exactly one review turn."""

    project_key: str
    manifest: CandidateManifest
    thread_id: str
    generation: int


class CandidateAdmission:
    """Validate a wake end to end, then admit its candidate exactly once.

    Every check runs before any state change, so a stale, malformed,
    mutable, or mismatched candidate produces only a bounded diagnostic and
    leaves the delivered wake record untouched.
    """

    def __init__(
        self,
        *,
        channels: AdmissionStore,
        manifest_root: Path,
        branch_head: BranchHead,
        base_policy: BasePolicy,
        intake_gate: IntakeGate,
        merged_candidate_proof: MergedCandidateProof | None = None,
    ) -> None:
        if branch_head is None:
            raise ValueError("candidate admission requires a branch head")
        if base_policy is None:
            raise ValueError("candidate admission requires a base policy")
        if intake_gate is None or not hasattr(intake_gate, "validate"):
            raise ValueError("candidate admission requires an intake gate")
        self._channels = channels
        self._manifest_root = manifest_root
        self._branch_head = branch_head
        self._base_policy = base_policy
        self._intake_gate = intake_gate
        # INFRA-217: GitHub deletes a merged candidate's branch, so
        # ``branch_head`` stops resolving and admission would reject a
        # candidate whose merge is already PROVEN. This optional proof
        # is the only thing that may excuse an unresolvable remote
        # branch; when absent, behavior is exactly as before.
        self._merged_candidate_proof = merged_candidate_proof

    def validate_only(
        self,
        project_key: str,
        event: WakeEvent,
        *,
        received_generation: int,
    ) -> AdmittedCandidate:
        """Run every read-only admission check with zero durable writes.

        INFRA-217, Sol correction c02dc0fe: ``submit_review`` (see
        ``merger_turns.py``) previously persisted a ``submitted_verdicts``
        row -- and only afterward, at settlement, discovered that the
        exact-head admission gate rejected the candidate -- letting a
        stale or mismatched remote branch create a durable submission
        before rejection, which then poisoned a corrected retry via
        ``_record_settled``'s terminal write. This method shares the
        identical manifest, envelope, channel, exact-remote-head (or
        proven-deleted-branch), base-policy, and intake-gate checks that
        :meth:`admit` runs, but stops before the durable
        ``admit_wake`` compare-and-swap -- so a caller can run it BEFORE
        persisting anything and raise :class:`CandidateRejected` with
        zero durable writes. The settlement-time call to :meth:`admit`
        below still re-runs the same checks and performs the actual
        (idempotent) admission, which is retained to catch races between
        prevalidation and settlement.
        """

        _snapshot, manifest, channel = self._run_checks(
            project_key, event, received_generation=received_generation
        )
        return AdmittedCandidate(
            project_key=project_key,
            manifest=manifest,
            thread_id=channel.thread_id,
            generation=channel.generation,
        )

    def admit(
        self,
        project_key: str,
        event: WakeEvent,
        *,
        received_generation: int,
    ) -> AdmittedCandidate:
        """Admit the wake's candidate or fail closed with a bounded reason."""

        snapshot, manifest, channel = self._run_checks(
            project_key, event, received_generation=received_generation
        )
        if not self._channels.admit_wake(
            project_key,
            event,
            thread_id=channel.thread_id,
            generation=channel.generation,
            manifest=snapshot,
        ):
            raise CandidateRejected(
                "event was not admissible: exactly one review per delivered "
                "candidate and one admitted candidate per project"
            )
        return AdmittedCandidate(
            project_key=project_key,
            manifest=manifest,
            thread_id=channel.thread_id,
            generation=channel.generation,
        )

    def _run_checks(
        self,
        project_key: str,
        event: WakeEvent,
        *,
        received_generation: int,
    ) -> tuple[ManifestSnapshot, CandidateManifest, ReviewerChannel]:
        """The complete read-only admission checks, shared by both entry
        points above; never performs a durable write.
        """

        try:
            snapshot = read_manifest_snapshot(
                Path(event.manifest_path),
                root=self._manifest_root,
                expected_digest=event.manifest_digest,
            )
        except ManifestError as error:
            raise CandidateRejected(str(error)) from error
        manifest = snapshot.manifest
        if (
            manifest.event_id != event.event_id
            or manifest.status != event.status
            or manifest.candidate_sha != event.candidate_sha
            or manifest.base_sha != event.base_sha
            or event.issue_id not in manifest.linear_issues
        ):
            raise CandidateRejected(
                "wake envelope does not match its immutable manifest"
            )
        channel = self._channels.read_channel(project_key)
        if channel is None or channel.state != "ready":
            raise CandidateRejected(
                f"reviewer channel for {project_key} is not ready"
            )
        if channel.generation != received_generation:
            raise CandidateRejected(
                "wake generation is stale for the current reviewer channel"
            )
        try:
            head = self._branch_head(project_key, manifest.branch)
        except BranchHeadUnknown as error:
            # INFRA-217, Sol correction c02dc0fe: a fetch failure, an
            # authentication failure, or a local resolution error is NEVER
            # branch absence -- it fails closed exactly as an unknown or
            # mismatched head does, even when an exact merged pull request
            # can be discovered. Only an authoritative "" (a successful
            # fetch, then no such ref) is eligible for the merge-proof
            # excuse below.
            raise CandidateRejected(
                f"remote branch head could not be determined: {error}"
            ) from error
        if manifest.candidate_sha != head:
            # INFRA-217: an empty head means the remote branch no longer
            # resolves at all -- the normal state AFTER a merge, since
            # GitHub deletes the branch. That alone still proves nothing,
            # so it is excused only when GitHub proves this exact reviewed
            # SHA is an already-merged, same-repository pull request
            # targeting the integration branch. Every other case is
            # unchanged and still fails closed: a branch that RESOLVES to
            # a different commit is a stale candidate (exact-head
            # validation for open candidates), and a missing branch with
            # no merge proof is rejected exactly as before.
            proven_merged = (
                head == ""
                and self._merged_candidate_proof is not None
                and self._merged_candidate_proof(
                    project_key, manifest.branch, manifest.candidate_sha
                )
            )
            if not proven_merged:
                raise CandidateRejected(
                    "candidate is not the current branch head"
                )
        if not self._base_policy(project_key, manifest.base_sha):
            raise CandidateRejected(
                "candidate base violates the active review policy"
            )
        self._intake_gate.validate(project_key, manifest)
        return snapshot, manifest, channel
