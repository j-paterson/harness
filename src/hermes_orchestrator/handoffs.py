"""Complete, durable, acknowledged project-lead handoffs."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError

from hermes_orchestrator.db import Database

NonEmptyText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


class HandoffTest(BaseModel):
    """One reproducible test command and its observed outcome."""

    model_config = ConfigDict(extra="forbid")

    command: NonEmptyText
    outcome: NonEmptyText


class HandoffDocument(BaseModel):
    """The complete context contract required before lead rotation."""

    model_config = ConfigDict(extra="forbid")

    cell_id: NonEmptyText
    objective: NonEmptyText
    status: NonEmptyText
    decisions: list[NonEmptyText] = Field(min_length=1)
    branch: NonEmptyText
    commits: list[NonEmptyText] = Field(min_length=1)
    pull_request: NonEmptyText
    modified_files: list[NonEmptyText] = Field(min_length=1)
    tests: list[HandoffTest] = Field(min_length=1)
    blockers: list[NonEmptyText]
    remaining_steps: list[NonEmptyText] = Field(min_length=1)
    commands: list[NonEmptyText] = Field(min_length=1)
    environment_notes: list[NonEmptyText] = Field(min_length=1)
    risks: list[NonEmptyText]
    next_action: NonEmptyText


class HandoffRejected(ValueError):
    """The handoff or acknowledgement is incomplete or inconsistent."""


@dataclass(frozen=True, slots=True)
class HandoffRequest:
    """A durable request for a lead to stop at a safe boundary."""

    request_id: str
    cell_id: str
    reason: str
    state: str
    requested_at: str


@dataclass(frozen=True, slots=True)
class HandoffRecord:
    """Stored document and replacement acknowledgement state."""

    handoff_id: str
    cell_id: str
    state: str
    document: HandoffDocument
    markdown: str
    replacement_session_id: UUID | None
    replacement_profile_alias: str | None
    restated_next_action: str | None
    created_at: str
    updated_at: str


def _utc_now() -> datetime:
    return datetime.now(UTC)


class HandoffService:
    """Validate, render, persist, and acknowledge lead handoffs."""

    def __init__(
        self,
        database: Database,
        *,
        request_ids: Callable[[], str] | None = None,
        handoff_ids: Callable[[], str] | None = None,
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._database = database
        self._request_ids = request_ids or (lambda: str(uuid.uuid4()))
        self._handoff_ids = handoff_ids or (lambda: str(uuid.uuid4()))
        self._now = now
        self._listeners: list[Callable[[HandoffRecord], None]] = []

    def subscribe(self, listener: Callable[[HandoffRecord], None]) -> None:
        """Register an in-process signal for newly submitted handoffs.

        The post-commit listener idiom (see ``LeadTerminalWakes``): a
        listener runs only AFTER the submission transaction committed,
        and a listener failure never rolls back or unsubmits the durable
        row. INFRA-198 uses this to drive an awaiting lead rotation's
        continuation the moment the incumbent's fresh handoff lands
        (``LeadRotation.resume_on_submission``).
        """

        self._listeners.append(listener)

    def request(self, cell_id: str, reason: str) -> HandoffRequest:
        """Persist a safe-boundary handoff request."""

        if not cell_id.strip() or not reason.strip():
            raise HandoffRejected("cell id and handoff reason are required")
        request = HandoffRequest(
            request_id=self._request_ids(),
            cell_id=cell_id.strip(),
            reason=reason.strip(),
            state="requested",
            requested_at=self._aware_now().isoformat(),
        )
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO handoff_requests("
                "request_id, cell_id, reason, state, requested_at"
                ") VALUES (?, ?, ?, ?, ?)",
                (
                    request.request_id,
                    request.cell_id,
                    request.reason,
                    request.state,
                    request.requested_at,
                ),
            )
        return request

    def submit(self, document: HandoffDocument) -> HandoffRecord:
        """Revalidate and durably store a complete handoff snapshot."""

        try:
            validated = HandoffDocument.model_validate(document.model_dump())
        except ValidationError as error:
            field = ".".join(str(part) for part in error.errors()[0]["loc"])
            raise HandoffRejected(f"handoff field is incomplete: {field}") from error
        handoff_id = self._handoff_ids()
        now = self._aware_now().isoformat()
        markdown = self._render(validated)
        document_json = validated.model_dump_json()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO handoffs("
                "handoff_id, cell_id, state, document_json, markdown, "
                "created_at, updated_at"
                ") VALUES (?, ?, 'submitted', ?, ?, ?, ?)",
                (
                    handoff_id,
                    validated.cell_id,
                    document_json,
                    markdown,
                    now,
                    now,
                ),
            )
        record = self.get(handoff_id)
        for listener in self._listeners:
            try:
                listener(record)
            except Exception:
                # The durable row is the truth; a failed in-process
                # signal never unsubmits it (the awaiting-rotation
                # continuation recovers by rerunning rotate-lead).
                continue
        return record

    def acknowledge(
        self,
        handoff_id: str,
        session_id: UUID,
        restated_next_action: str,
        *,
        profile_alias: str,
    ) -> HandoffRecord:
        """Record the replacement session's concrete continuation commitment.

        Sol correction b4b545f3 P2: the selected replacement profile is
        persisted in the SAME durable transition as the acknowledgement, so
        an acknowledged-but-untransferred rotation can reconstruct the exact
        identities on recovery without reselecting capacity.
        """

        next_action = restated_next_action.strip()
        if not next_action:
            raise HandoffRejected("acknowledgement must restate the next action")
        profile = profile_alias.strip()
        if not profile:
            raise HandoffRejected(
                "acknowledgement must name the selected replacement profile"
            )
        current = self.get(handoff_id)
        if current.state == "acknowledged":
            if (
                current.replacement_session_id != session_id
                or current.restated_next_action != next_action
            ):
                raise HandoffRejected("handoff was acknowledged by another session")
            if current.replacement_profile_alias == profile:
                return current
            if current.replacement_profile_alias is not None:
                raise HandoffRejected(
                    "handoff was acknowledged with a different replacement "
                    "profile"
                )
            # A row acknowledged before migration 0051 carries no profile:
            # an identical re-acknowledgement backfills the identity.
            with self._database.transaction() as connection:
                connection.execute(
                    "UPDATE handoffs SET replacement_profile_alias = ?, "
                    "updated_at = ? WHERE handoff_id = ?",
                    (profile, self._aware_now().isoformat(), handoff_id),
                )
            return self.get(handoff_id)
        now = self._aware_now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE handoffs SET state = 'acknowledged', "
                "replacement_session_id = ?, replacement_profile_alias = ?, "
                "restated_next_action = ?, updated_at = ? WHERE handoff_id = ?",
                (str(session_id), profile, next_action, now, handoff_id),
            )
        return self.get(handoff_id)

    def get(self, handoff_id: str) -> HandoffRecord:
        """Read one durable handoff record."""

        row = self._database.execute(
            "SELECT * FROM handoffs WHERE handoff_id = ?",
            (handoff_id,),
        ).fetchone()
        if row is None:
            raise KeyError(handoff_id)
        replacement = row["replacement_session_id"]
        replacement_profile = row["replacement_profile_alias"]
        return HandoffRecord(
            handoff_id=str(row["handoff_id"]),
            cell_id=str(row["cell_id"]),
            state=str(row["state"]),
            document=HandoffDocument.model_validate_json(row["document_json"]),
            markdown=str(row["markdown"]),
            replacement_session_id=(UUID(str(replacement)) if replacement else None),
            replacement_profile_alias=(
                str(replacement_profile) if replacement_profile else None
            ),
            restated_next_action=row["restated_next_action"],
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    @staticmethod
    def _render(document: HandoffDocument) -> str:
        def bullets(values: list[str]) -> str:
            return "\n".join(f"- {value}" for value in values) or "- None"

        tests = "\n".join(
            f"- `{test.command}` — {test.outcome}" for test in document.tests
        )
        return (
            "# Project lead handoff\n\n"
            f"## Objective\n\n{document.objective}\n\n"
            f"## Status\n\n{document.status}\n\n"
            f"## Decisions\n\n{bullets(document.decisions)}\n\n"
            f"## Git and pull request\n\n"
            f"- Branch: `{document.branch}`\n"
            f"- Pull request: {document.pull_request}\n"
            f"- Commits:\n{bullets(document.commits)}\n\n"
            f"## Modified files\n\n{bullets(document.modified_files)}\n\n"
            f"## Tests\n\n{tests}\n\n"
            f"## Blockers\n\n{bullets(document.blockers)}\n\n"
            f"## Remaining steps\n\n{bullets(document.remaining_steps)}\n\n"
            f"## Commands\n\n{bullets(document.commands)}\n\n"
            f"## Environment notes\n\n{bullets(document.environment_notes)}\n\n"
            f"## Risks\n\n{bullets(document.risks)}\n\n"
            f"## Next action\n\n{document.next_action}\n"
        )

    def _aware_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class DerivedFacts:
    """Every additional mechanically-derived handoff fact beyond issue
    identity, branch, and head (INFRA-184): the pull request recorded
    for this issue, the files changed relative to the fetched
    integration branch, observed test/acceptance evidence, and Merger
    correction packets still pending the lead's acknowledgement.

    Every field here is read from durable rows or a read-only git
    probe by the caller — never supplied by the incumbent. The
    incumbent's own inputs to :func:`derived_handoff_document` stay
    exactly ``decisions``, ``caveats``, ``risks``, and ``next_action``
    (see ``lead_rotation.NON_DERIVABLE_HANDOFF_CONTENT``).
    """

    pull_request: str = "none recorded in durable state"
    modified_files: tuple[str, ...] = ()
    test_results: tuple[str, ...] = ()
    pending_corrections: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class LiveCell:
    """The live seat identity read from ``project_cells`` at the moment
    a handoff is about to be submitted — the ground truth
    :func:`validate_handoff_identity` checks the document and the
    requesting session against."""

    cell_id: str
    session_id: str | None


def validate_handoff_identity(
    document: HandoffDocument,
    live_cell: LiveCell,
    *,
    requesting_session_id: str,
) -> None:
    """Fail closed on a handoff whose identity does not match the live
    cell it claims to come from (INFRA-184).

    Two independent facts must hold before a handoff is ever durably
    stored: the document's own ``cell_id`` must name the live cell
    that is submitting, and that live cell's CURRENT session must be
    the exact session doing the submitting — not a session an
    in-flight rotation has already replaced. Either mismatch means the
    document was built from a stale or wrong seat snapshot, so it is
    refused (no write) before ``HandoffService.submit`` is ever
    called.
    """

    if document.cell_id != live_cell.cell_id:
        raise HandoffRejected(
            f"handoff refused: document names cell {document.cell_id!r} "
            f"but the live submitting seat is cell {live_cell.cell_id!r}"
        )
    if live_cell.session_id is None or live_cell.session_id != requesting_session_id:
        raise HandoffRejected(
            f"handoff refused: cell {live_cell.cell_id!r} is now owned by "
            f"session {live_cell.session_id!r}, not the submitting session "
            f"{requesting_session_id!r}; the seat has moved since this "
            "handoff was derived"
        )


def _handoff_test(entry: str) -> HandoffTest:
    """Split one derived ``name — outcome`` evidence line into the
    document's command/outcome pair, using the same " — " separator the
    markdown renderer already places between them. A line with no
    separator becomes the command verbatim with a generic outcome."""

    if " — " in entry:
        command, outcome = entry.split(" — ", 1)
        command = command.strip()
        outcome = outcome.strip()
        if command and outcome:
            return HandoffTest(command=command, outcome=outcome)
    return HandoffTest(command=entry, outcome="recorded in durable state")


def derived_handoff_document(
    *,
    cell_id: str,
    project_key: str,
    session_id: str,
    profile_alias: str,
    issue_id: str,
    issue_state: str,
    branch: str,
    head: str,
    decisions: list[str],
    caveats: list[str],
    risks: list[str],
    next_action: str,
    facts: DerivedFacts | None = None,
) -> HandoffDocument:
    """Assemble a complete handoff document from durable facts plus the
    incumbent's non-derivable content (INFRA-198, Sol 52d15493).

    Every mechanical field — identity, git position, issue state,
    environment, pull request, modified files, observed test evidence,
    and pending Merger corrections — comes from the caller's durable
    rows and worktree/git probes (``facts``, INFRA-184); the incumbent
    supplies ONLY decisions, caveats (blockers), risks, and the exact
    next action. Validation stays with :meth:`HandoffService.submit`:
    an unreadable worktree (empty branch or head) fails the document's
    own non-empty constraints, so an unverifiable checkpoint can never
    be dressed up as a handoff.
    """

    resolved = facts if facts is not None else DerivedFacts()
    modified_files = list(resolved.modified_files) or [
        "(derived) no files changed relative to the fetched integration "
        f"branch on {branch or '?'}"
    ]
    tests = [_handoff_test(entry) for entry in resolved.test_results] or [
        HandoffTest(
            command="durable test/acceptance evidence probe",
            outcome="none recorded in durable state",
        )
    ]
    environment_notes = [
        f"incumbent session {session_id} on profile {profile_alias}; "
        f"project {project_key}"
    ]
    if resolved.pending_corrections:
        environment_notes.extend(
            f"pending Merger correction: {entry}"
            for entry in resolved.pending_corrections
        )
    else:
        environment_notes.append("no pending Merger corrections")

    return HandoffDocument.model_construct(
        cell_id=cell_id,
        objective=(
            f"Hand off the {project_key} project lead for cell {cell_id} "
            f"(issue {issue_id})"
        ),
        status=(
            f"issue {issue_id} is {issue_state}; branch {branch} at "
            f"{head} is committed and pushed (verified by the rotation "
            "worktree gate)"
        ),
        decisions=decisions,
        branch=branch,
        commits=[head],
        pull_request=resolved.pull_request,
        modified_files=modified_files,
        tests=tests,
        blockers=caveats,
        remaining_steps=[next_action],
        commands=["hermes-orchestrator status --json"],
        environment_notes=environment_notes,
        risks=risks,
        next_action=next_action,
    )
