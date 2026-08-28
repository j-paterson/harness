"""Persistent, read-only, project-scoped Codex Merger reviewer channels."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from hermes_orchestrator.codex_rpc import (
    CodexRateLimits,
    CodexRequestFailed,
    parse_rate_limits,
)
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database

MERGER_MODEL = "gpt-5.6-sol"
_SERVICE_NAME = "hermes_orchestrator"

# The installed codex-cli 0.149.0-alpha.4.1 schema pins threads by moving them
# into the built-in "Pinned" thread section (threadSection/list +
# thread/section/move); the section id is resolved by name at configuration
# time and never hard-coded. Channel metadata such as the integration branch
# is persisted in the reviewer_channels record.
PINNED_SECTION_NAME = "Pinned"
_SECTION_PAGE_LIMIT = 16

# ThreadGoalStatus value that leaves the durable objective recorded but idle,
# matching the BLOCKED_ON_EXTERNAL_INTAKE turn contract: no active goal
# continuation until a validated explicit wake arrives.
IDLE_GOAL_STATUS = "blocked"


class RpcRequester(Protocol):
    """The stable request surface the Merger needs from the RPC client."""

    async def request(
        self, method: str, params: dict[str, Any] | None, timeout: float
    ) -> dict[str, Any]: ...


class MergerThreadUncertain(RuntimeError):
    """Raised when a persisted thread cannot be trusted without an operator."""


class MergerAuthRequired(RuntimeError):
    """Raised when the App Server is not ChatGPT-authenticated."""


class StaleChannelError(RuntimeError):
    """Raised when a compare-and-swap expectation no longer matches."""


@dataclass(frozen=True, slots=True)
class MergerThread:
    """One usable project Merger thread."""

    project_key: str
    thread_id: str


@dataclass(frozen=True, slots=True)
class MergerStatus:
    """Summary state for one project channel."""

    project_key: str
    thread_id: str | None
    state: str


@dataclass(frozen=True, slots=True)
class ReviewerChannel:
    """Durable source of truth for one project's Merger reviewer channel."""

    project_key: str
    thread_id: str
    generation: int
    state: str
    integration_branch: str
    prior_thread_id: str | None
    replacement_reason: str | None
    last_delivered_event_id: str | None
    last_delivered_candidate_sha: str | None
    last_delivery_failure_at: str | None
    heartbeat_enabled: bool


@dataclass(frozen=True, slots=True)
class CodexAuthHealth:
    """Merge-gate eligibility of the App Server account, identity-free."""

    eligible: bool
    reason: str


class CodexMerger:
    """Create, resume, and interrogate one durable Merger thread per project."""

    def __init__(
        self,
        *,
        rpc: RpcRequester,
        database: Database,
        projects: Mapping[str, ProjectConfig],
        prompt_file: Path,
        now: Callable[[], datetime] | None = None,
        request_timeout: float = 60.0,
        model: str = MERGER_MODEL,
    ) -> None:
        if request_timeout <= 0:
            raise ValueError("request timeout must be positive")
        self._rpc = rpc
        self._database = database
        self._projects = dict(projects)
        self._contract = prompt_file.read_text(encoding="utf-8")
        self._now = now or (lambda: datetime.now(UTC))
        self._timeout = request_timeout
        self._model = model

    async def ensure_thread(self, project_key: str) -> MergerThread:
        """Return a usable persisted thread, creating or resuming as needed."""

        project = self._project(project_key)
        health = await self.verify_chatgpt_auth()
        if not health.eligible:
            raise MergerAuthRequired(
                "codex merger requires chatgpt authentication before any "
                "thread operation"
            )
        channel = self.read_channel(project_key)
        if channel is None:
            return await self._create(project_key, project)
        if channel.state == "configuring":
            return await self._finish_configuration(
                project_key, channel.thread_id
            )
        if channel.state != "ready":
            raise MergerThreadUncertain(
                f"reviewer channel for {project_key} is {channel.state} and "
                "requires operator reconciliation"
            )
        return await self._resume_current(project_key)

    def read_channel(self, project_key: str) -> ReviewerChannel | None:
        """Read the durable reviewer-channel record without provider effects."""

        self._project(project_key)
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT thread_id, generation, state, integration_branch, "
                "prior_thread_id, "
                "replacement_reason, last_delivered_event_id, "
                "last_delivered_candidate_sha, last_delivery_failure_at, "
                "heartbeat_enabled FROM reviewer_channels "
                "WHERE project_key = ?",
                (project_key,),
            ).fetchone()
        if row is None:
            return None
        return ReviewerChannel(
            project_key=project_key,
            thread_id=str(row["thread_id"]),
            generation=int(row["generation"]),
            state=str(row["state"]),
            integration_branch=str(row["integration_branch"]),
            prior_thread_id=row["prior_thread_id"],
            replacement_reason=row["replacement_reason"],
            last_delivered_event_id=row["last_delivered_event_id"],
            last_delivered_candidate_sha=row["last_delivered_candidate_sha"],
            last_delivery_failure_at=row["last_delivery_failure_at"],
            heartbeat_enabled=bool(row["heartbeat_enabled"]),
        )

    def read_status(self, project_key: str) -> MergerStatus:
        """Summarize the persisted channel as missing, ready, or otherwise."""

        channel = self.read_channel(project_key)
        if channel is None:
            return MergerStatus(
                project_key=project_key, thread_id=None, state="missing"
            )
        return MergerStatus(
            project_key=project_key,
            thread_id=channel.thread_id,
            state=channel.state,
        )

    def begin_replacement(
        self,
        project_key: str,
        *,
        expected_thread_id: str,
        expected_generation: int,
        reason: str,
    ) -> ReviewerChannel:
        """Atomically mark the channel replacing when expectations still hold."""

        self._project(project_key)
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET state = 'replacing', "
                "replacement_reason = ?, updated_at = ? "
                "WHERE project_key = ? AND thread_id = ? AND generation = ? "
                "AND state != 'replacing'",
                (
                    reason,
                    self._now().isoformat(),
                    project_key,
                    expected_thread_id,
                    expected_generation,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleChannelError(
                    f"reviewer channel for {project_key} does not match the "
                    "expected thread and generation"
                )
        channel = self.read_channel(project_key)
        assert channel is not None
        return channel

    def complete_replacement(
        self,
        project_key: str,
        *,
        expected_thread_id: str,
        expected_generation: int,
        new_thread_id: str,
    ) -> ReviewerChannel:
        """Atomically install the new thread and increment the generation."""

        self._project(project_key)
        if not new_thread_id:
            raise ValueError("replacement thread id must not be empty")
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET thread_id = ?, "
                "generation = generation + 1, state = 'ready', "
                "prior_thread_id = ?, updated_at = ? "
                "WHERE project_key = ? AND thread_id = ? AND generation = ? "
                "AND state = 'replacing'",
                (
                    new_thread_id,
                    expected_thread_id,
                    self._now().isoformat(),
                    project_key,
                    expected_thread_id,
                    expected_generation,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleChannelError(
                    f"reviewer channel for {project_key} does not match the "
                    "expected replacing thread and generation"
                )
        channel = self.read_channel(project_key)
        assert channel is not None
        return channel

    def record_delivery_success(
        self,
        project_key: str,
        *,
        thread_id: str,
        generation: int,
        event_id: str | None = None,
        candidate_sha: str | None = None,
    ) -> bool:
        """Record a delivery for the exact ready channel that was invoked.

        Returns False on a compare-and-swap miss so a wake delivered to a
        replaced thread is never claimed on the new channel.
        """

        self._project(project_key)
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET "
                "last_delivered_event_id = coalesce(?, last_delivered_event_id), "
                "last_delivered_candidate_sha = "
                "coalesce(?, last_delivered_candidate_sha), "
                "heartbeat_enabled = 0, updated_at = ? "
                "WHERE project_key = ? AND thread_id = ? AND generation = ? "
                "AND state = 'ready'",
                (
                    event_id,
                    candidate_sha,
                    self._now().isoformat(),
                    project_key,
                    thread_id,
                    generation,
                ),
            )
        return cursor.rowcount == 1

    def record_delivery_failure(
        self, project_key: str, *, thread_id: str, generation: int
    ) -> bool:
        """Record a failure for the exact channel identity that was invoked."""

        self._project(project_key)
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET last_delivery_failure_at = ?, "
                "heartbeat_enabled = 1, updated_at = ? "
                "WHERE project_key = ? AND thread_id = ? AND generation = ?",
                (stamp, stamp, project_key, thread_id, generation),
            )
        return cursor.rowcount == 1

    async def verify_chatgpt_auth(self) -> CodexAuthHealth:
        """Check merge-gate auth without persisting account identity."""

        result = await self._rpc.request(
            "account/read", {"refreshToken": False}, self._timeout
        )
        account = result.get("account")
        if not isinstance(account, dict):
            account = {}
        if account.get("type") == "chatgpt":
            return CodexAuthHealth(eligible=True, reason="eligible")
        return CodexAuthHealth(eligible=False, reason="chatgpt_auth_required")

    async def read_rate_limits(self) -> CodexRateLimits:
        """Read the ChatGPT rate-limit snapshot for the Merger account."""

        result = await self._rpc.request(
            "account/rateLimits/read", None, self._timeout
        )
        return parse_rate_limits(result)

    async def interrupt(self, project_key: str, turn_id: str) -> None:
        """Interrupt one exact turn on the project's persisted thread."""

        channel = self.read_channel(project_key)
        if channel is None or channel.state != "ready":
            raise ValueError(
                f"no usable merger thread for project {project_key}"
            )
        await self._rpc.request(
            "turn/interrupt",
            {"threadId": channel.thread_id, "turnId": turn_id},
            self._timeout,
        )

    async def _resume_current(self, project_key: str) -> MergerThread:
        for _ in range(2):
            channel = self.read_channel(project_key)
            if channel is None or channel.state != "ready":
                raise MergerThreadUncertain(
                    f"reviewer channel for {project_key} is not ready and "
                    "requires operator reconciliation"
                )
            thread_id = channel.thread_id
            try:
                await self._rpc.request(
                    "thread/read", {"threadId": thread_id}, self._timeout
                )
                await self._rpc.request(
                    "thread/resume", {"threadId": thread_id}, self._timeout
                )
            except CodexRequestFailed as error:
                self._mark_uncertain(
                    project_key,
                    thread_id=thread_id,
                    generation=channel.generation,
                )
                raise MergerThreadUncertain(
                    f"merger thread for {project_key} could not be read or "
                    "resumed and requires operator reconciliation"
                ) from error
            if not self._channel_is_current(project_key, channel):
                continue
            await self._set_goal(project_key, thread_id)
            if self._channel_is_current(project_key, channel):
                return MergerThread(
                    project_key=project_key, thread_id=thread_id
                )
        raise StaleChannelError(
            f"reviewer channel for {project_key} was replaced while resuming"
        )

    def _channel_is_current(
        self, project_key: str, channel: ReviewerChannel
    ) -> bool:
        current = self.read_channel(project_key)
        return (
            current is not None
            and current.state == "ready"
            and current.thread_id == channel.thread_id
            and current.generation == channel.generation
        )

    async def _create(
        self, project_key: str, project: ProjectConfig
    ) -> MergerThread:
        stamp = self._now().isoformat()
        try:
            with self._database.transaction() as connection:
                connection.execute(
                    "INSERT INTO reviewer_channels("
                    "project_key, thread_id, generation, state, "
                    "integration_branch, created_at, updated_at"
                    ") VALUES (?, '', 0, 'creating', ?, ?, ?)",
                    (project_key, project.integration_branch, stamp, stamp),
                )
        except sqlite3.IntegrityError:
            current = self.read_channel(project_key)
            if current is not None and current.state == "ready":
                return await self._resume_current(project_key)
            if current is not None and current.state == "configuring":
                return await self._finish_configuration(
                    project_key, current.thread_id
                )
            raise MergerThreadUncertain(
                f"reviewer channel for {project_key} is being created by "
                "another caller and requires operator reconciliation if it "
                "stays reserved"
            ) from None
        try:
            started = await self._rpc.request(
                "thread/start",
                {
                    "model": self._model,
                    "cwd": str(project.repo_path),
                    "approvalPolicy": "never",
                    "sandbox": "read-only",
                    "serviceName": _SERVICE_NAME,
                },
                self._timeout,
            )
        except CodexRequestFailed:
            # The server definitively rejected the request, so no remote
            # thread exists and the empty reservation is safe to release.
            with self._database.transaction() as connection:
                connection.execute(
                    "DELETE FROM reviewer_channels "
                    "WHERE project_key = ? AND state = 'creating' "
                    "AND thread_id = ''",
                    (project_key,),
                )
            raise
        except BaseException:
            # Timeouts, disconnects, cancellation, and other transport or
            # process outcomes are ambiguous: the server may have created a
            # thread we never heard about. Keep the empty reservation as a
            # durable uncertain record so a retry cannot start a duplicate.
            with self._database.transaction() as connection:
                connection.execute(
                    "UPDATE reviewer_channels SET state = 'uncertain', "
                    "updated_at = ? "
                    "WHERE project_key = ? AND state = 'creating'",
                    (self._now().isoformat(), project_key),
                )
            raise
        thread = started.get("thread")
        thread_id = thread.get("id") if isinstance(thread, dict) else None
        if not isinstance(thread_id, str) or not thread_id:
            with self._database.transaction() as connection:
                connection.execute(
                    "UPDATE reviewer_channels SET state = 'uncertain', "
                    "updated_at = ? "
                    "WHERE project_key = ? AND state = 'creating'",
                    (self._now().isoformat(), project_key),
                )
            raise ValueError("codex did not return a thread id")
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET thread_id = ?, "
                "state = 'configuring', updated_at = ? "
                "WHERE project_key = ? AND state = 'creating'",
                (thread_id, self._now().isoformat(), project_key),
            )
            if cursor.rowcount != 1:
                raise StaleChannelError(
                    f"reviewer channel reservation for {project_key} "
                    "disappeared during creation"
                )
        return await self._finish_configuration(project_key, thread_id)

    async def _finish_configuration(
        self, project_key: str, thread_id: str
    ) -> MergerThread:
        """Complete or recover thread setup; the channel stays configuring
        (recoverable, never a duplicate) until every call succeeds."""

        await self._rpc.request(
            "thread/name/set",
            {"threadId": thread_id, "name": f"Merger: {project_key}"},
            self._timeout,
        )
        pinned_section_id = await self._resolve_pinned_section()
        await self._rpc.request(
            "thread/section/move",
            {"sectionId": pinned_section_id, "threadId": thread_id},
            self._timeout,
        )
        await self._set_goal(project_key, thread_id)
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET generation = 1, "
                "state = 'ready', updated_at = ? "
                "WHERE project_key = ? AND state = 'configuring' "
                "AND thread_id = ?",
                (self._now().isoformat(), project_key, thread_id),
            )
            if cursor.rowcount != 1:
                raise StaleChannelError(
                    f"reviewer channel for {project_key} changed while "
                    "completing configuration"
                )
        return MergerThread(project_key=project_key, thread_id=thread_id)

    async def _resolve_pinned_section(self) -> str:
        """Resolve exactly one built-in Pinned section id, fail closed."""

        matches: list[str] = []
        cursor: str | None = None
        for _ in range(_SECTION_PAGE_LIMIT):
            params: dict[str, Any] = {}
            if cursor is not None:
                params["cursor"] = cursor
            result = await self._rpc.request(
                "threadSection/list", params, self._timeout
            )
            data = result.get("data")
            if not isinstance(data, list):
                raise ValueError("threadSection/list returned no data")
            for section in data:
                if (
                    isinstance(section, dict)
                    and section.get("name") == PINNED_SECTION_NAME
                ):
                    section_id = section.get("id")
                    if not isinstance(section_id, str) or not section_id:
                        raise ValueError(
                            "Pinned thread section has no usable id"
                        )
                    matches.append(section_id)
            next_cursor = result.get("nextCursor")
            if next_cursor is None:
                break
            cursor = str(next_cursor)
        else:
            raise ValueError(
                "threadSection/list pagination did not terminate"
            )
        if len(matches) != 1:
            raise ValueError(
                "expected exactly one built-in Pinned thread section, "
                f"found {len(matches)}"
            )
        return matches[0]

    async def _set_goal(self, project_key: str, thread_id: str) -> None:
        project = self._project(project_key)
        objective = (
            f"Project: {project_key}. "
            f"Integration branch: {project.integration_branch}.\n\n"
            f"{self._contract}"
        )
        await self._rpc.request(
            "thread/goal/set",
            {
                "threadId": thread_id,
                "objective": objective,
                "status": IDLE_GOAL_STATUS,
            },
            self._timeout,
        )

    def _mark_uncertain(
        self, project_key: str, *, thread_id: str, generation: int
    ) -> bool:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE reviewer_channels SET state = 'uncertain', "
                "updated_at = ? WHERE project_key = ? AND thread_id = ? "
                "AND generation = ? AND state = 'ready'",
                (stamp, project_key, thread_id, generation),
            )
        return cursor.rowcount == 1

    def _project(self, project_key: str) -> ProjectConfig:
        project = self._projects.get(project_key)
        if project is None:
            raise ValueError(f"unknown merger project {project_key}")
        return project
