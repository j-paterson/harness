"""Admit exactly one validated immutable candidate per Merger wake.

INFRA-200: this module also attaches an OPTIONAL, purely advisory
administrative-drift hint (see ``review_drift.py``) to the admitted
candidate it returns. That hint exists so a caller MAY scope a replay
narrowly when the only thing that changed since the last reviewed
candidate is documentation, ``NOTICE``/``LICENSE``/``CHANGELOG`` text, or
``.hermes/`` bookkeeping. It is never consulted by, and never weakens,
any check in :meth:`CandidateAdmission._run_checks`: candidate/base/tree
identity, durable packet integrity, generation freshness, and every other
fail-closed gate below run exactly as they always have, whether the drift
hint is "administrative", "semantic", "none", or absent entirely.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
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
from hermes_orchestrator.project_teams import ProjectTeamService, StaleTeamMember
from hermes_orchestrator.review_drift import DriftVerdict, classify_drift

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
#: ``(candidate_sha) -> (tree_sha, changed_paths_vs_previous)``. Optional
#: source for the administrative-drift hint (INFRA-200): given a
#: candidate's SHA, returns its tree SHA and the paths that changed
#: relative to whatever candidate this source considers "previous" for
#: that project. Absent by default, in which case no drift hint is
#: computed at all -- see ``review_drift.py`` and
#: :meth:`CandidateAdmission._drift_verdict`.
DescribeCandidate = Callable[[str], tuple[str, Sequence[str]]]


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
    """One immutable candidate admitted for exactly one review turn.

    ``drift`` (INFRA-200) is the administrative-drift hint attached to the
    wake payload delivered to Sol: ``None`` when no ``describe_candidate``
    source was configured (the default -- every prior caller sees this
    field simply absent-by-value and nothing else changes), otherwise a
    :class:`~hermes_orchestrator.review_drift.DriftVerdict` describing
    whether this candidate differs from the last reviewed one only in
    administrative paths, not at all, or semantically. It is advisory
    only -- see the module docstring above.
    """

    project_key: str
    manifest: CandidateManifest
    thread_id: str
    generation: int
    drift: DriftVerdict | None = None


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
        describe_candidate: DescribeCandidate | None = None,
        teams: ProjectTeamService | None = None,
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
        # INFRA-200: optional administrative-drift hint source; absent by
        # default, in which case ``_drift_verdict`` always returns None
        # and every existing caller is byte-for-byte unaffected.
        self._describe_candidate = describe_candidate
        # INFRA-187 wave 2: optional so every existing caller (none of
        # which know about project-team pairs yet) keeps today's
        # behavior exactly. Once supplied, a candidate must additionally
        # resolve through the project's *ready* Fable+Sol pair -- the
        # Sol member identity (thread + generation) admission already
        # trusts (the channel it just read) must also be the ready
        # team's bound Sol member, so no candidate, correction, or merge
        # can ever cross a project boundary through a stale or
        # cross-project team.
        self._teams = teams

    def validate_only(
        self,
        project_key: str,
        event: WakeEvent,
        *,
        received_generation: int,
        external_gates: bool = True,
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

        ``external_gates`` is documented on :meth:`_run_checks`.
        """

        _snapshot, manifest, channel, drift = self._run_checks(
            project_key,
            event,
            received_generation=received_generation,
            external_gates=external_gates,
        )
        return AdmittedCandidate(
            project_key=project_key,
            manifest=manifest,
            thread_id=channel.thread_id,
            generation=channel.generation,
            drift=drift,
        )

    def admit(
        self,
        project_key: str,
        event: WakeEvent,
        *,
        received_generation: int,
        external_gates: bool = True,
    ) -> AdmittedCandidate:
        """Admit the wake's candidate or fail closed with a bounded reason.

        ``external_gates`` is documented on :meth:`_run_checks`; the
        durable ``admit_wake`` compare-and-swap below is unconditional
        and identical either way.
        """

        snapshot, manifest, channel, drift = self._run_checks(
            project_key,
            event,
            received_generation=received_generation,
            external_gates=external_gates,
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
            drift=drift,
        )

    def _run_checks(
        self,
        project_key: str,
        event: WakeEvent,
        *,
        received_generation: int,
        external_gates: bool = True,
    ) -> tuple[
        ManifestSnapshot, CandidateManifest, ReviewerChannel, DriftVerdict | None
    ]:
        """The complete read-only admission checks, shared by both entry
        points above; never performs a durable write.

        INFRA-200: the administrative-drift hint computed below (the
        fourth element of the returned tuple) is advisory ONLY. Every
        check in this method -- the immutable manifest read, the wake
        envelope's exact match against it, the ready reviewer channel at
        the exact received generation, the remote branch head (or
        proven-deleted-branch excuse), the base policy, and the intake
        gate -- runs identically regardless of what the drift hint says.
        Nothing below ever branches on ``drift``: an "administrative"
        verdict cannot skip a check, shrink a comparison, or excuse a
        mismatch. It exists purely so a caller downstream MAY choose a
        narrower, scoped semantic replay instead of a wide one -- never
        to bypass candidate/base/tree identity or durable packet
        integrity, which fail closed exactly as before.

        INFRA-212: the checks split cleanly in two, and ``external_gates``
        selects between them.

        The LOCAL half -- the immutable manifest, the wake envelope's
        exact match against it, and the ready reviewer channel at the
        exact received generation -- is always run. It is the identity of
        the candidate under review and is proven entirely from durable
        local state.

        The EXTERNAL half -- the remote branch head (with its
        merged-pull-request excuse), the base policy's fetch, and the
        intake gate's CircleCI reconciliation and one-open-pull-request
        rule -- exists to authorize CROSSING an external boundary: it
        decides whether this candidate may be merged. Those checks
        require GitHub, CircleCI, git remote access and therefore
        credentials, and they run whenever ``external_gates`` is true,
        which is the default and remains the case for every settlement
        that can merge.

        A ``corrections_required`` verdict crosses no external boundary:
        it opens no pull request, merges nothing, and its only durable
        effects are the review row and the correction packets routed to
        the lead. ``merger_turns`` therefore passes
        ``external_gates=False`` for exactly that case, so a correction
        can be validated, persisted and settled from Sol's own workspace
        with no credential at all. Nothing is weakened for the merging
        path: the one-open-pull-request rule and the CI window still gate
        every approval, and the durable one-writer compare-and-swap in
        :meth:`admit` (exactly one review per delivered candidate, one
        admitted candidate per project) is unconditional either way.
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
        if self._teams is not None:
            # INFRA-187 wave 2: the channel this admission just read
            # must also be the project's currently *ready* pair's bound
            # Sol member -- a channel that is locally "ready" but does
            # not (or no longer) match the durable pair's own bound
            # generation is refused here, exactly like the generation
            # check above, before any external boundary is touched.
            # ``WakeEvent``/``CandidateManifest`` carry no Fable cell
            # identity today, so only the Sol member is resolved through
            # the pair; a future packet that adds a cell id to the wake
            # envelope should also pass ``fable_cell_id=`` here.
            try:
                self._teams.resolve(
                    project_key,
                    sol_thread_id=channel.thread_id,
                    sol_generation=channel.generation,
                )
            except StaleTeamMember as error:
                raise CandidateRejected(
                    "project pair not ready or member generation stale: "
                    f"{error}"
                ) from error
        drift = self._drift_verdict(channel, manifest)
        if not external_gates:
            return snapshot, manifest, channel, drift
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
        return snapshot, manifest, channel, drift

    def _drift_verdict(
        self, channel: ReviewerChannel, manifest: CandidateManifest
    ) -> DriftVerdict | None:
        """The administrative-drift hint for this candidate, or ``None``.

        Returns ``None`` -- no hint attached at all -- whenever no
        ``describe_candidate`` source was configured at construction; this
        is the default, so every existing caller and test is unaffected
        (INFRA-200).

        When a source IS configured, the durable
        ``reviewer_channels.last_delivered_candidate_sha`` column (read
        via ``channel``, never re-queried here) is the record of the last
        reviewed candidate for this project. When it is absent, there is
        no prior candidate on durable record and this is treated as a
        first review. Otherwise both the previous and current candidate's
        tree SHAs are looked up through ``describe_candidate`` and handed
        to the pure classifier in ``review_drift.py``. This method never
        raises and never affects admission -- see the module docstring
        and :meth:`_run_checks`.
        """

        if self._describe_candidate is None:
            return None
        previous_sha = channel.last_delivered_candidate_sha
        if previous_sha is None:
            return classify_drift(
                previous_tree_sha=None, current_tree_sha="", changed_paths=()
            )
        previous_tree_sha, _ = self._describe_candidate(previous_sha)
        current_tree_sha, changed_paths = self._describe_candidate(
            manifest.candidate_sha
        )
        return classify_drift(
            previous_tree_sha=previous_tree_sha,
            current_tree_sha=current_tree_sha,
            changed_paths=tuple(changed_paths),
        )
