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

import hashlib
import json
import re
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from hermes_orchestrator.ci_window import MergeWindowExhausted, PriorMergeFailed
from hermes_orchestrator.codex_queue import QueueDeliveryResult
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
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
from hermes_orchestrator.review_intake import CandidateRejected
from hermes_orchestrator.verdicts import CorrectionPacket

if TYPE_CHECKING:
    from hermes_orchestrator.subagent_packets import SubagentPackets
    from hermes_orchestrator.verifier import Verifier


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


# The ledger's own absent-file sentinel (``subagent_packets._ABSENT_BLOB``).
# Duplicated here (rather than imported) because it is a stable, documented
# string contract between the ledger and every consumer of its blobs, not
# an implementation detail of the ledger module.
_ABSENT_BLOB = "absent"

# Bounded runtime for each head-blob git invocation (INFRA-194 Sol finding,
# PR #30): a hang while proving a candidate's head content must fail
# closed, never block emission forever.
_HEAD_BLOB_TIMEOUT = 60.0


def _default_head_blob(repo: Path, head: str, path: str) -> bytes | None:
    """Prove ``path``'s content at ``head``, or its positive absence.

    This is a raw ``subprocess.run`` seam — deliberately NOT the
    text-mode ``self._run``/``GitRunner`` seam — because the credit
    decision this feeds must be binary-safe and must classify every
    outcome explicitly:

    - presence is proven with ``git ls-tree --full-tree <head> --
      <path>``: a nonzero exit (bad head, corrupt repository, unknown
      object) is repository failure, never absence, and fails closed;
      exit 0 with empty stdout is the ONLY positive proof of absence.
    - content is then read with ``git cat-file blob <head>:<path>``,
      captured with no text decoding at all so the returned bytes are
      exactly the blob's raw bytes, comparable byte-for-byte against
      the ledger's own raw-bytes sha256 measurement.

    A timeout, an ``OSError`` launching git, or a nonzero exit from
    either command raises :class:`EmissionBlocked` — this function
    never silently degrades an unproven outcome into "absent".
    """

    try:
        presence = subprocess.run(
            ("git", "-C", str(repo), "ls-tree", "--full-tree", head, "--", path),
            capture_output=True,
            timeout=_HEAD_BLOB_TIMEOUT,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise EmissionBlocked(
            f"could not determine whether {path!r} exists at {head}: "
            f"{type(error).__name__}"
        ) from error
    if presence.returncode != 0:
        stderr = presence.stderr.decode("utf-8", errors="replace")[:200]
        raise EmissionBlocked(
            f"git ls-tree failed for {path!r} at {head} with exit code "
            f"{presence.returncode}: {stderr}"
        )
    if not presence.stdout.strip():
        return None

    try:
        content = subprocess.run(
            ("git", "-C", str(repo), "cat-file", "blob", f"{head}:{path}"),
            capture_output=True,
            timeout=_HEAD_BLOB_TIMEOUT,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise EmissionBlocked(
            f"could not read {path!r} at {head}: {type(error).__name__}"
        ) from error
    if content.returncode != 0:
        stderr = content.stderr.decode("utf-8", errors="replace")[:200]
        raise EmissionBlocked(
            f"git cat-file failed for {path!r} at {head} with exit code "
            f"{content.returncode}: {stderr}"
        )
    return content.stdout


@dataclass(frozen=True, slots=True)
class EmissionResult:
    """One emitted candidate: its wake, manifest path, and delivery."""

    event: WakeEvent
    manifest_path: Path
    delivery: QueueDeliveryResult
    reused_manifest: bool
    intake: str = "clear"
    intake_reason: str = ""


def _session_chain_from_database(database: Database, cell_id: str) -> frozenset[str]:
    """The durable rotation chain of session ids recognized for one cell
    (INFRA-197 packet G1): operator rotation policy deliberately rotates
    lead sessions at safe boundaries with durable, acknowledged
    handoffs, so a legitimate candidate branch may be built by a CHAIN
    of sessions for one cell rather than just one.

    Mechanically verified as the union, non-NULL only, of every
    session_id durably recorded for ``cell_id`` across four independent
    sources — none of them consulted alone is authoritative on its
    own, but every session this cell has ever legitimately run under
    appears in at least one:

    - ``project_cells.session_id`` — the cell's own current session.
    - ``handoffs.replacement_session_id`` where ``state = 'acknowledged'``
      — a durably acknowledged rotation onto this cell.
    - ``lead_assignments.session_id`` — a recorded lead assignment for
      this cell.
    - ``events`` rows with ``event_type = 'project_cell.rotated'`` and
      ``aggregate_id = cell_id`` — the ``session_id`` key of the
      rotation event's own payload.

    A session absent from every source is not in the chain and earns
    no credit — fail-closed.
    """

    sessions: set[str] = set()
    for row in database.execute(
        "SELECT session_id FROM project_cells "
        "WHERE cell_id = ? AND session_id IS NOT NULL",
        (cell_id,),
    ).fetchall():
        sessions.add(str(row["session_id"]))
    for row in database.execute(
        "SELECT replacement_session_id FROM handoffs "
        "WHERE cell_id = ? AND state = 'acknowledged' "
        "AND replacement_session_id IS NOT NULL",
        (cell_id,),
    ).fetchall():
        sessions.add(str(row["replacement_session_id"]))
    for row in database.execute(
        "SELECT session_id FROM lead_assignments "
        "WHERE cell_id = ? AND session_id IS NOT NULL",
        (cell_id,),
    ).fetchall():
        sessions.add(str(row["session_id"]))
    for row in database.execute(
        "SELECT payload_json FROM events "
        "WHERE event_type = 'project_cell.rotated' AND aggregate_id = ?",
        (cell_id,),
    ).fetchall():
        payload = json.loads(str(row["payload_json"]))
        rotated_session_id = payload.get("session_id")
        if rotated_session_id is not None:
            sessions.add(str(rotated_session_id))
    return frozenset(sessions)


def build_session_chain_lookup(
    database: Database,
) -> Callable[[str], frozenset[str]]:
    """Bind a live ``Database`` handle into the ``session_chain`` seam
    :class:`CandidateEmitter` accepts, so a production construction can
    wire the real rotation-chain lookup with one call:
    ``CandidateEmitter(..., session_chain=build_session_chain_lookup(db))``.
    """

    return lambda cell_id: _session_chain_from_database(database, cell_id)


# The fixed decision_id under which the INFRA-197 provenance-grandfather
# binding is imported at freeze time (see the ``operator_decisions``
# table). Fixed and singular by design: this is a ONE-TIME transition
# grandfather, not a general mechanism, so there is exactly one binding
# row to ever look for.
_GRANDFATHER_BINDING_DECISION_ID = "infra-197-provenance-grandfather-binding-v1"

_BLOB_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _parse_grandfather_binding(raw: str) -> dict[str, object] | None:
    """Strictly validate one candidate binding payload, never raising.

    The shape is exactly ``{"candidate_sha": <40-hex>, "blobs": {<path>:
    <64-hex>, ...}}`` with a non-empty ``blobs`` map and no unknown
    top-level keys — anything else (malformed JSON, wrong types, extra
    keys, bad hex, an empty blob map) returns ``None`` so the caller
    falls back to zero grandfather support, never a partially-trusted
    binding.
    """

    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(parsed, dict):
        return None
    if set(parsed.keys()) != {"candidate_sha", "blobs"}:
        return None
    candidate_sha = parsed.get("candidate_sha")
    if not isinstance(candidate_sha, str) or _SHA_PATTERN.match(candidate_sha) is None:
        return None
    blobs = parsed.get("blobs")
    if not isinstance(blobs, dict) or not blobs:
        return None
    validated_blobs: dict[str, str] = {}
    for path, blob_sha in blobs.items():
        if not isinstance(path, str) or not isinstance(blob_sha, str):
            return None
        if _BLOB_SHA256_PATTERN.match(blob_sha) is None:
            return None
        validated_blobs[path] = blob_sha
    return {"candidate_sha": candidate_sha, "blobs": validated_blobs}


def _grandfather_binding_from_database(database: Database) -> dict[str, object] | None:
    row = database.execute(
        "SELECT source_message FROM operator_decisions "
        "WHERE decision_id = ? AND status = 'approved'",
        (_GRANDFATHER_BINDING_DECISION_ID,),
    ).fetchone()
    if row is None:
        return None
    raw = row["source_message"]
    if not raw:
        return None
    return _parse_grandfather_binding(str(raw))


def build_grandfather_binding_lookup(
    database: Database,
) -> Callable[[], dict[str, object] | None]:
    """Bind a live ``Database`` handle into the ``grandfather_binding``
    seam :class:`CandidateEmitter` accepts (INFRA-197 packet G2): a
    production construction wires the durable one-time transition
    grandfather with one call:
    ``CandidateEmitter(..., grandfather_binding=build_grandfather_binding_lookup(db))``.

    Reads the fixed, single-use ``operator_decisions`` row (decision_id
    ``infra-197-provenance-grandfather-binding-v1``, status
    ``approved``), parses and strictly validates its ``source_message``
    JSON, and returns the validated binding or ``None`` — missing row,
    non-approved status, and any malformed payload all collapse to
    ``None``, never a raise. The grandfather is inherently single-use:
    it can only ever match the one candidate SHA it was bound to.
    """

    return lambda: _grandfather_binding_from_database(database)


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
        packets: SubagentPackets | None = None,
        verifier: Verifier | None = None,
        candidate_gate_id: str = "candidate-full-gate",
        head_blob: Callable[[Path, str, str], bytes | None] | None = None,
        session_chain: Callable[[str], frozenset[str]] | None = None,
        grandfather_binding: Callable[[], dict[str, object] | None] | None = None,
    ) -> None:
        self._projects = dict(projects)
        self._git = git
        self._manifest_root = manifest_root
        self._delivery = delivery
        self._now = now or (lambda: datetime.now(UTC))
        self._intake_gate = intake_gate
        self._lead = lead
        self._issue_for_failure = issue_for_failure
        self._packets = packets
        self._verifier = verifier
        self._candidate_gate_id = candidate_gate_id
        self._head_blob = head_blob if head_blob is not None else _default_head_blob
        # ``None`` (the default) enforces the OLD single-identity rule —
        # one shared (session_id, worktree, generation) — so existing
        # constructions that never opted into rotation-chain evidence
        # keep their exact prior fail-closed behavior. Only a caller
        # that injects a real chain lookup (see
        # :func:`build_session_chain_lookup`) gets the rotation-chain
        # rule.
        self._session_chain = session_chain
        # ``None`` (the default) grants no general bypass whatsoever —
        # every existing construction keeps its exact prior fail-closed
        # behavior. Only a caller that injects the INFRA-197 one-time
        # transition grandfather (see
        # :func:`build_grandfather_binding_lookup`) gets the two narrow
        # rescue points in ``_enforce_delegation_evidence``, and even
        # then only for the exact candidate SHA the binding is bound to.
        self._grandfather_binding = grandfather_binding

    async def emit(
        self,
        project_key: str,
        issue_id: str,
        *,
        verification: tuple[tuple[str, str], ...],
        blockers: tuple[str, ...] = (),
        status: str = "FABLE_READY",
        lead_checkout: Path | None = None,
    ) -> EmissionResult:
        project = self._projects.get(project_key)
        if project is None:
            raise EmissionBlocked(f"unknown project {project_key!r}")
        if status not in WAKE_STATUSES:
            raise EmissionBlocked(f"unknown wake status {status!r}")
        if not verification:
            raise EmissionBlocked("a candidate requires recorded verification")
        # The durable project path stays anchored to the stable primary
        # checkout, so the candidate branch usually lives in the lead's
        # own linked worktree. The freeze boundary is that checkout —
        # but only after its git common dir proves it belongs to this
        # exact project repository; a foreign checkout is refused before
        # any freeze check runs.
        repo = project.repo_path
        if (
            lead_checkout is not None
            and Path(lead_checkout).resolve() != repo.resolve()
        ):
            claimed = self._run(
                Path(lead_checkout),
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
            repo = Path(lead_checkout)
        facts = run_freeze_gate(
            lambda *args: self._run(repo, *args),
            project,
            expect_pushed=True,
        )
        head, branch = facts.head, facts.branch
        base, changed = facts.base, facts.changed_files
        delegation_extra: tuple[tuple[str, str], ...] = ()
        if self._packets is not None:
            delegation_extra = self._enforce_delegation_evidence(
                repo, issue_id, base, head, changed
            )
        verification_extra: tuple[tuple[str, str], ...] = ()
        if self._verifier is not None:
            receipt_id = self._enforce_final_gate_receipt(repo)
            verification_extra = (
                (f"gate:{self._candidate_gate_id}", f"receipt:{receipt_id}"),
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
            verification=verification + delegation_extra + verification_extra,
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

    def _enforce_delegation_evidence(
        self,
        repo: Path,
        issue_id: str,
        base: str,
        head: str,
        changed: tuple[str, ...],
    ) -> tuple[tuple[str, str], ...]:
        """Refuse a non-trivial candidate with no delegation evidence, and
        reconcile the actual diff against accepted packet scopes.

        Triviality is proven from the same freeze-boundary diff: at most
        two changed files and at most thirty added-plus-deleted lines. A
        trivial candidate needs no reconciliation and contributes no
        verification entries.

        A non-trivial diff is reconciled path-by-path against every
        ``accepted`` subagent packet for this issue: a packet whose
        evidence carries ``exception_reason`` is a DIRECT EXCEPTION,
        every other accepted packet is REGULAR. A path claimed by one or
        more regular packets is credited to the most recently accepted
        one (regular packets may legitimately share files across
        sequential waves); a path claimed only by exceptions is credited
        to the sole claiming exception, but two or more exceptions
        claiming the same path fail closed. An uncovered path fails
        closed, naming it. Each credited exception's paths and measured
        lines must stay inside both the fixed reviewer-fix scale and its
        own declared ``expected_lines``. Every credited packet must share
        one worktree and one cell_id. Session identity is then checked
        one of two ways: with a ``session_chain`` lookup injected at
        construction, each credited packet's session_id must be durably
        recorded in that cell's rotation chain (any session a rotation
        legitimately handed the cell to); with none injected (the
        default), every credited packet must share the exact same
        ``(session_id, generation)`` — the original single-identity
        rule, unchanged and still fail-closed.

        Path coverage alone is not enough: every credited packet must
        also carry immutable, MEASURED execution provenance proving it
        actually produced the credited fragment, never a mere claim in
        its agent-authored evidence JSON. A regular packet needs both a
        ``reserved_blobs`` and a ``returned_blobs`` snapshot and is
        credited for a path only when that path's content changed inside
        its reservation window (``reserved != returned`` — otherwise the
        packet was created after its claimed changes already existed and
        the path has no valid owner). A direct exception needs at least
        its ``returned_blobs`` snapshot (inherently post-hoc, but still
        measured at record time). For every credited path, of either
        kind, the packet's returned blob must equal the sha256 of that
        path's actual content at the candidate head, proven fresh via
        ``git show``. Never inferred — always proven from the durable
        ledger and the actual git diff and tree.

        INFRA-197 packet G2: with a ``grandfather_binding`` lookup
        injected at construction (see
        :func:`build_grandfather_binding_lookup`), exactly two rescue
        points are consulted — lazily, and only once the binding's own
        ``candidate_sha`` equals this exact ``head`` — for an otherwise
        fatal path: an uncovered changed path, and a credited packet
        path whose returned blob does not match the head content.
        Either rescue requires the path be named in the binding's
        ``blobs`` map with a sha256 equal to the path's ACTUAL head
        content; a rescued path is recorded as grandfathered evidence
        instead, excluded from every per-packet check and from the
        identity/chain checks. No binding, a wrong candidate SHA, or a
        path/blob not in the binding all leave the original fail-closed
        behavior completely unchanged.
        """

        numstat = self._run(repo, "diff", "--numstat", base, head)
        total_lines = 0
        for line in numstat.splitlines():
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            try:
                total_lines += int(parts[0]) + int(parts[1])
            except ValueError:
                continue
        non_trivial = len(changed) > 2 or total_lines > 30
        if not non_trivial:
            return ()

        changed_set = set(changed)
        lines_by_path: dict[str, int] = {}
        for line in numstat.splitlines():
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            path = parts[-1]
            try:
                added = int(parts[0])
                deleted = int(parts[1])
            except ValueError as error:
                if path in changed_set:
                    raise EmissionBlocked(
                        f"could not measure the diff for {path!r}: "
                        "git numstat did not report parseable line counts "
                        "(binary or otherwise unmeasurable)"
                    ) from error
                continue
            lines_by_path[path] = lines_by_path.get(path, 0) + added + deleted

        assert self._packets is not None
        accepted = [
            packet
            for packet in self._packets.for_issue(issue_id)
            if packet.state == "accepted"
        ]
        if not accepted:
            raise EmissionBlocked(
                "delegation evidence missing: a non-trivial candidate "
                "requires accepted subagent packets or a recorded "
                "direct-work exception (record_direct_exception)"
            )

        regular = []
        exceptions = []
        for packet in accepted:
            evidence = packet.evidence or {}
            if "exception_reason" in evidence:
                exceptions.append(packet)
            else:
                regular.append(packet)

        def recency_key(packet: object) -> tuple[datetime, str]:
            return (self._packet_timestamp(packet), packet.packet_id)

        # INFRA-197 packet G2: the one-time transition grandfather. The
        # binding (durably imported at freeze time under the fixed
        # decision_id ``infra-197-provenance-grandfather-binding-v1``,
        # see :func:`build_grandfather_binding_lookup`) is consulted
        # LAZILY — never called at all when no rescue is ever needed —
        # and only ever trusted for the exact candidate SHA it names.
        # Cached after the first lookup so a wrong-candidate binding (or
        # no binding at all) is confirmed exactly once per emission.
        grandfather_cache: dict[str, dict[str, str] | None] = {}

        def grandfather_blobs() -> dict[str, str] | None:
            if "blobs" not in grandfather_cache:
                blobs: dict[str, str] | None = None
                if self._grandfather_binding is not None:
                    binding = self._grandfather_binding()
                    if (
                        isinstance(binding, dict)
                        and binding.get("candidate_sha") == head
                        and isinstance(binding.get("blobs"), dict)
                    ):
                        blobs = binding["blobs"]
                grandfather_cache["blobs"] = blobs
            return grandfather_cache["blobs"]

        # Paths accepted purely on the strength of the grandfather
        # binding, keyed to the actual (matching) head blob sha256 —
        # never packet-credited, so excluded from every per-packet
        # check (windows, lines) and from the identity/chain checks.
        grandfathered: dict[str, str] = {}

        credited_by_path: dict[str, object] = {}
        for path in changed:
            regular_claimants = [p for p in regular if path in p.allowed_files]
            exception_claimants = [p for p in exceptions if path in p.allowed_files]
            if regular_claimants:
                credited_by_path[path] = max(regular_claimants, key=recency_key)
            elif exception_claimants:
                if len(exception_claimants) > 1:
                    raise EmissionBlocked(
                        "delegation evidence conflict: "
                        f"{path!r} is claimed by more than one direct "
                        "exception"
                    )
                credited_by_path[path] = exception_claimants[0]
            else:
                binding_blobs = grandfather_blobs()
                if binding_blobs is not None and path in binding_blobs:
                    head_blob = self._head_blob_hash(repo, head, path)
                    if head_blob == binding_blobs[path]:
                        grandfathered[path] = head_blob
                        continue
                raise EmissionBlocked(
                    "delegation evidence missing: "
                    f"{path!r} is not covered by any accepted subagent "
                    "packet"
                )

        packet_paths: dict[str, list[str]] = {}
        packet_by_id: dict[str, object] = {}
        for path, packet in credited_by_path.items():
            packet_paths.setdefault(packet.packet_id, []).append(path)
            packet_by_id[packet.packet_id] = packet

        # Every credited packet must carry immutable, measured execution
        # provenance — the agent-authored evidence JSON never substitutes
        # for it. A regular packet needs BOTH its reserve-time and
        # settle-time blob snapshots; a direct exception (post-hoc by
        # nature) needs at least its settle-time snapshot. A regular
        # packet is credited for a path only when that path's content
        # actually changed inside the packet's reservation window
        # (reserved != returned) — a packet fabricated after its claimed
        # changes already existed measures identical blobs and earns
        # nothing, leaving the path with no valid owner. Every credited
        # path's returned blob must also equal the sha256 of that path's
        # ACTUAL content at the candidate head, proven fresh from git —
        # never inferred from the packet's own say-so.
        for packet_id, paths in packet_paths.items():
            packet = packet_by_id[packet_id]
            evidence = packet.evidence or {}
            is_exception = "exception_reason" in evidence
            if is_exception:
                if packet.returned_blobs is None:
                    raise EmissionBlocked(
                        f"packet {packet_id!r} lacks immutable execution "
                        "provenance: no measured returned blobs"
                    )
            elif packet.reserved_blobs is None or packet.returned_blobs is None:
                raise EmissionBlocked(
                    f"packet {packet_id!r} lacks immutable execution "
                    "provenance: no measured reserved/returned blobs"
                )
            kept_paths: list[str] = []
            for path in paths:
                returned_blob = packet.returned_blobs.get(path)
                if not is_exception:
                    reserved_blob = packet.reserved_blobs.get(path)
                    if reserved_blob == returned_blob:
                        raise EmissionBlocked(
                            "delegation evidence missing: "
                            f"{path!r} is not covered by any accepted "
                            "subagent packet"
                        )
                head_blob = self._head_blob_hash(repo, head, path)
                if returned_blob != head_blob:
                    binding_blobs = grandfather_blobs()
                    if (
                        binding_blobs is not None
                        and path in binding_blobs
                        and binding_blobs[path] == head_blob
                    ):
                        grandfathered[path] = head_blob
                        continue
                    raise EmissionBlocked(
                        f"packet {packet_id!r} returned blob for {path!r} "
                        "does not match the candidate head content"
                    )
                kept_paths.append(path)
            packet_paths[packet_id] = kept_paths

        # A packet whose every credited path was rescued into
        # grandfathered status now credits nothing at all — it drops
        # out of ``packet_paths``/``packet_by_id`` entirely, so it
        # contributes no evidence entry and is never subject to the
        # identity/chain checks below.
        packet_paths = {
            packet_id: paths for packet_id, paths in packet_paths.items() if paths
        }
        packet_by_id = {
            packet_id: packet_by_id[packet_id] for packet_id in packet_paths
        }

        for packet in exceptions:
            paths = packet_paths.get(packet.packet_id)
            if not paths:
                continue
            if len(paths) > 2:
                raise EmissionBlocked(
                    f"direct exception {packet.packet_id!r} exceeds the "
                    "reviewer-fix scale: more than two credited files"
                )
            credited_lines = sum(lines_by_path.get(path, 0) for path in paths)
            if credited_lines > 30:
                raise EmissionBlocked(
                    f"direct exception {packet.packet_id!r} exceeds the "
                    "reviewer-fix scale: more than thirty credited lines"
                )
            evidence = packet.evidence or {}
            expected_lines_raw = evidence.get("expected_lines")
            if expected_lines_raw is not None:
                try:
                    expected_lines = int(expected_lines_raw)
                except (TypeError, ValueError):
                    expected_lines = None
                if expected_lines is not None and credited_lines > expected_lines:
                    raise EmissionBlocked(
                        f"direct exception {packet.packet_id!r} exceeds its "
                        "own declared expected_lines bound"
                    )

        credited_packets = list(packet_by_id.values())
        worktrees = {packet.worktree for packet in credited_packets}
        if len(worktrees) > 1:
            raise EmissionBlocked(
                "delegation evidence identity mismatch: credited packets "
                "do not share one worktree"
            )
        cell_ids = {packet.cell_id for packet in credited_packets}
        if len(cell_ids) > 1:
            raise EmissionBlocked(
                "delegation evidence identity mismatch: credited packets "
                "do not share one cell"
            )
        if self._session_chain is None:
            # No rotation-chain lookup injected: enforce the original
            # single-identity rule unchanged (worktree already proven
            # uniform above, so only session_id and generation remain).
            identities = {
                (packet.session_id, packet.generation)
                for packet in credited_packets
            }
            if len(identities) > 1:
                raise EmissionBlocked(
                    "delegation evidence identity mismatch: credited "
                    "packets do not share one session, worktree, and "
                    "generation"
                )
        elif credited_packets:
            # ``credited_packets`` can be empty when every changed path
            # was rescued by the grandfather binding — nothing left to
            # check identity for.
            cell_id = next(iter(cell_ids))
            chain = self._session_chain(cell_id)
            for packet in credited_packets:
                if packet.session_id not in chain:
                    raise EmissionBlocked(
                        "delegation evidence identity mismatch: credited "
                        f"packet {packet.packet_id!r} session "
                        f"{packet.session_id!r} is not durably recorded "
                        f"in the rotation chain for cell {cell_id!r}"
                    )

        entries = [
            (
                f"packet:{packet_id}",
                f"files={len(paths)};lines="
                f"{sum(lines_by_path.get(path, 0) for path in paths)}",
            )
            for packet_id, paths in packet_paths.items()
        ]
        entries.extend(
            (f"grandfathered:{path}", f"blob={blob[:12]}")
            for path, blob in grandfathered.items()
        )
        return tuple(sorted(entries))

    @staticmethod
    def _packet_timestamp(packet: object) -> datetime:
        raw = getattr(packet, "updated_at", None)
        if not raw:
            return datetime.min.replace(tzinfo=UTC)
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return datetime.min.replace(tzinfo=UTC)

    def _enforce_final_gate_receipt(self, repo: Path) -> str:
        """Require a fresh, signed receipt for the mandatory complete
        gate on this EXACT proven tree before any candidate publishes.

        The gate's own command is bound INSIDE the attested receipt
        document and is reported to reviewers from there, so this
        transition validator does not restate or compare a command —
        it only needs to prove that SOME green attested run of
        ``candidate_gate_id`` happened on exactly this tree. It walks
        the newest receipts for the gate (``receipt_ids_for_gate``) and
        accepts the first that ``validate_for_tree`` reports fresh;
        never inferred, always proven from the durable, HMAC-signed
        ledger.
        """

        assert self._verifier is not None
        for receipt_id in self._verifier.receipt_ids_for_gate(
            self._candidate_gate_id
        ):
            valid, reason = self._verifier.validate_for_tree(
                receipt_id, cwd=repo, gate_id=self._candidate_gate_id
            )
            if valid and reason == "fresh":
                return receipt_id
        raise EmissionBlocked(
            "final candidate-gate receipt missing or stale: run the "
            "mandatory complete gate through `hermes-orchestrator "
            f"verify --gate {self._candidate_gate_id} -- <full test "
            "command>` on this exact tree"
        )

    def _head_blob_hash(self, repo: Path, head: str, path: str) -> str:
        """The sha256 hexdigest of ``path``'s content at ``head``, proven
        fresh from git and comparable against the ledger's raw-byte
        measurement.

        This defers entirely to the ``head_blob`` seam
        (:func:`_default_head_blob` unless overridden), which returns
        the path's raw blob bytes when present, ``None`` ONLY when git
        has positively proven the path absent at ``head``, and raises
        :class:`EmissionBlocked` for every other outcome (repository
        corruption, an invalid head, a timeout, or any other git
        failure). Only a positively-proven absence maps to the ledger's
        own ``"absent"`` sentinel; every unproven outcome propagates as
        a raised failure and never reaches this return at all, so it
        can never be mistaken for absence. The bytes are hashed as-is —
        no text decoding — so this is directly comparable, byte for
        byte, against the ledger's own raw-bytes sha256 measurement.
        """

        content = self._head_blob(repo, head, path)
        if content is None:
            return _ABSENT_BLOB
        return hashlib.sha256(content).hexdigest()

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
