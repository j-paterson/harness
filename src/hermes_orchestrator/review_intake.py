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

BranchHead = Callable[[str, str], str]
BasePolicy = Callable[[str, str], bool]


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

    def admit(
        self,
        project_key: str,
        event: WakeEvent,
        *,
        received_generation: int,
    ) -> AdmittedCandidate:
        """Admit the wake's candidate or fail closed with a bounded reason."""

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
        head = self._branch_head(project_key, manifest.branch)
        if manifest.candidate_sha != head:
            raise CandidateRejected(
                "candidate is not the current branch head"
            )
        if not self._base_policy(project_key, manifest.base_sha):
            raise CandidateRejected(
                "candidate base violates the active review policy"
            )
        self._intake_gate.validate(project_key, manifest)
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
