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
from typing import Protocol

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

_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class WakeDeliverer(Protocol):
    """The queue delivery boundary (``codex queue`` only)."""

    async def deliver(
        self, project_key: str, event: WakeEvent
    ) -> QueueDeliveryResult: ...


class EmissionBlocked(RuntimeError):
    """The local clone is not at a valid immutable freeze boundary."""


@dataclass(frozen=True, slots=True)
class EmissionResult:
    """One emitted candidate: its wake, manifest path, and delivery."""

    event: WakeEvent
    manifest_path: Path
    delivery: QueueDeliveryResult
    reused_manifest: bool


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
    ) -> None:
        self._projects = dict(projects)
        self._git = git
        self._manifest_root = manifest_root
        self._delivery = delivery
        self._now = now or (lambda: datetime.now(UTC))

    async def emit(
        self,
        project_key: str,
        issue_id: str,
        *,
        verification: tuple[tuple[str, str], ...],
        blockers: tuple[str, ...] = (),
        status: str = "FABLE_READY",
    ) -> EmissionResult:
        project = self._projects.get(project_key)
        if project is None:
            raise EmissionBlocked(f"unknown project {project_key!r}")
        if status not in WAKE_STATUSES:
            raise EmissionBlocked(f"unknown wake status {status!r}")
        if not verification:
            raise EmissionBlocked("a candidate requires recorded verification")
        repo = project.repo_path
        if self._run(repo, "status", "--porcelain").strip():
            raise EmissionBlocked("working tree is not clean at the freeze boundary")
        head = self._run(repo, "rev-parse", "HEAD").strip()
        branch = self._run(repo, "rev-parse", "--abbrev-ref", "HEAD").strip()
        if _SHA_PATTERN.match(head) is None:
            raise EmissionBlocked("HEAD is not a full commit sha")
        if branch in ("HEAD", "", project.integration_branch):
            raise EmissionBlocked("candidate must be on a pushed feature branch")
        self._run(repo, "fetch", "--", "origin", project.integration_branch)
        self._run(repo, "fetch", "--", "origin", branch)
        remote_head = self._run(repo, "rev-parse", f"origin/{branch}").strip()
        if remote_head != head:
            raise EmissionBlocked("local HEAD is not the pushed branch head")
        base = self._run(
            repo, "merge-base", "HEAD", f"origin/{project.integration_branch}"
        ).strip()
        if _SHA_PATTERN.match(base) is None:
            raise EmissionBlocked("could not determine the integration base")
        changed = tuple(
            line
            for line in self._run(repo, "diff", "--name-only", base, head).splitlines()
            if line.strip()
        )
        if not changed:
            raise EmissionBlocked("candidate has no changes against its base")
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
            manifest = existing
            reused = True
        event = wake_event_for(manifest, path)
        assert event.manifest_digest == manifest_digest_for(manifest)
        delivery = await self._delivery.deliver(project_key, event)
        return EmissionResult(
            event=event,
            manifest_path=path,
            delivery=delivery,
            reused_manifest=reused,
        )

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
