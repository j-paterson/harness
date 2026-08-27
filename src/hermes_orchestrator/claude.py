"""Resumable Claude Code lead turns and sanitized stream events."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from uuid import UUID

from hermes_orchestrator.profiles import ProfileRegistry


@dataclass(frozen=True, slots=True)
class LeadTurnRequest:
    """One new or resumed project-lead turn."""

    session_id: UUID
    cwd: Path
    prompt: str
    profile_alias: str
    resume: bool = False
    output_schema: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ClaudeEvent:
    """A bounded, identity-free observation from Claude's JSON stream."""

    kind: str
    original_type: str
    session_id: UUID | None
    parent_tool_use_id: str | None
    timestamp: str | None
    usage: dict[str, int]
    error_code: str | None = None
    restated_next_action: str | None = None


class ClaudeStreamError(ValueError):
    """Raised when Claude emits a malformed stream record."""


class ClaudeProcessError(RuntimeError):
    """Raised for a failed Claude process without exposing stderr."""

    def __init__(self, returncode: int) -> None:
        super().__init__(f"Claude process exited with status {returncode}")
        self.returncode = returncode


class ClaudeEventParser:
    """Normalize relevant Claude stream records without retaining message text."""

    def feed(self, line: bytes) -> ClaudeEvent:
        """Parse one JSON line into a sanitized orchestration event."""

        try:
            value = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ClaudeStreamError("Claude emitted malformed JSON") from error
        if not isinstance(value, dict):
            raise ClaudeStreamError("Claude stream record must be an object")

        original_type = str(value.get("type", "unknown"))
        subtype = str(value.get("subtype", ""))
        session_id = self._uuid_or_none(value.get("session_id"))
        parent_tool_use_id = self._text_or_none(value.get("parent_tool_use_id"))
        timestamp = self._text_or_none(value.get("timestamp"))
        usage = self._usage(value)

        kind = f"stream.{original_type}"
        error_code = None
        restated_next_action = None
        if original_type == "system" and subtype == "init":
            kind = "session.started"
        elif self._starts_subagent(value):
            kind = "subagent.started"
        elif action := self._handoff_acknowledgement(value):
            kind = "handoff.acknowledged"
            restated_next_action = action
        elif self._is_subscription_limit(value):
            kind = "provider.limit"
            error_code = "subscription_limit"

        return ClaudeEvent(
            kind=kind,
            original_type=original_type,
            session_id=session_id,
            parent_tool_use_id=parent_tool_use_id,
            timestamp=timestamp,
            usage=usage,
            error_code=error_code,
            restated_next_action=restated_next_action,
        )

    @staticmethod
    def _uuid_or_none(value: object) -> UUID | None:
        if not isinstance(value, str):
            return None
        try:
            return UUID(value)
        except ValueError:
            return None

    @staticmethod
    def _text_or_none(value: object) -> str | None:
        return value if isinstance(value, str) and value else None

    @staticmethod
    def _usage(value: dict[str, Any]) -> dict[str, int]:
        possible = value.get("usage")
        message = value.get("message")
        if not isinstance(possible, dict) and isinstance(message, dict):
            possible = message.get("usage")
        if not isinstance(possible, dict):
            return {}
        return {
            str(key): count
            for key, count in possible.items()
            if isinstance(count, int) and not isinstance(count, bool)
        }

    @staticmethod
    def _starts_subagent(value: dict[str, Any]) -> bool:
        if (
            value.get("type") == "system"
            and value.get("subtype") == "hook_started"
            and value.get("hook_name") == "SubagentStart"
        ):
            return True
        message = value.get("message")
        if not isinstance(message, dict):
            return False
        content = message.get("content")
        if not isinstance(content, list):
            return False
        return any(
            isinstance(block, dict)
            and block.get("type") == "tool_use"
            and block.get("name") == "Agent"
            for block in content
        )

    @classmethod
    def _is_subscription_limit(cls, value: object) -> bool:
        if isinstance(value, str):
            normalized = value.lower()
            return (
                "hit your" in normalized or "reached your" in normalized
            ) and "limit" in normalized
        if isinstance(value, list):
            return any(cls._is_subscription_limit(item) for item in value)
        if isinstance(value, dict):
            return any(cls._is_subscription_limit(item) for item in value.values())
        return False

    @staticmethod
    def _handoff_acknowledgement(value: dict[str, Any]) -> str | None:
        structured = value.get("structured_output")
        if not isinstance(structured, dict):
            return None
        next_action = structured.get("restated_next_action")
        if structured.get("acknowledged") is not True or not isinstance(
            next_action, str
        ):
            return None
        stripped = next_action.strip()
        return stripped or None

ProcessFactory = Callable[..., Awaitable[asyncio.subprocess.Process]]


class ClaudeRunner:
    """Launch profile-isolated Claude Code turns in owned process groups."""

    def __init__(
        self,
        registry: ProfileRegistry,
        *,
        prompt_file: Path,
        base_env: Mapping[str, str],
        executable: str = "claude",
        process_factory: ProcessFactory = asyncio.create_subprocess_exec,
        termination_timeout: float = 5.0,
    ) -> None:
        if termination_timeout <= 0:
            raise ValueError("termination_timeout must be positive")
        self._registry = registry
        self._prompt_file = prompt_file
        self._base_env = base_env
        self._executable = executable
        self._process_factory = process_factory
        self._termination_timeout = termination_timeout

    def build_command(
        self,
        request: LeadTurnRequest,
    ) -> tuple[list[str], dict[str, str]]:
        """Build an argument-array command and scrubbed profile environment."""

        command = [
            self._executable,
            "-p",
            "--model",
            "fable",
            "--effort",
            "high",
        ]
        if request.resume:
            command.extend(["--resume", str(request.session_id)])
        else:
            command.extend(["--session-id", str(request.session_id)])
        if request.output_schema is not None:
            command.extend(
                [
                    "--json-schema",
                    json.dumps(
                        request.output_schema,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ]
            )
        command.extend(
            [
                "--input-format",
                "text",
                "--output-format=stream-json",
                "--verbose",
                "--include-hook-events",
                "--forward-subagent-text",
                "--permission-mode",
                "auto",
                "--append-system-prompt-file",
                str(self._prompt_file),
                request.prompt,
            ]
        )
        return (
            command,
            self._registry.launch_env(request.profile_alias, self._base_env),
        )

    def start_lead(
        self,
        request: LeadTurnRequest,
    ) -> AsyncGenerator[ClaudeEvent]:
        """Start a new persistent session and stream normalized events."""

        return self._run(replace(request, resume=False))

    def resume_lead(
        self,
        session_id: UUID,
        request: LeadTurnRequest,
    ) -> AsyncGenerator[ClaudeEvent]:
        """Resume the same persistent session and stream normalized events."""

        return self._run(replace(request, session_id=session_id, resume=True))

    async def retire_session(self, session_id: UUID) -> None:
        """Retire a logical print-mode session after its final turn has exited."""

        del session_id

    async def _run(self, request: LeadTurnRequest) -> AsyncGenerator[ClaudeEvent]:
        command, environment = self.build_command(request)
        process = await self._process_factory(
            *command,
            cwd=str(request.cwd),
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        if process.stdout is None:
            await self._terminate(process)
            raise RuntimeError("Claude stdout pipe was not created")
        if process.stderr is None:
            await self._terminate(process)
            raise RuntimeError("Claude stderr pipe was not created")

        parser = ClaudeEventParser()
        stderr_task = asyncio.create_task(self._drain(process.stderr))
        try:
            while line := await process.stdout.readline():
                yield parser.feed(line)
            returncode = await process.wait()
            if returncode != 0:
                raise ClaudeProcessError(returncode)
        finally:
            if process.returncode is None:
                await self._terminate(process)
            await stderr_task

    @staticmethod
    async def _drain(stream: asyncio.StreamReader) -> None:
        """Drain and discard child diagnostics so its pipe cannot deadlock."""

        while await stream.read(65536):
            pass

    async def _terminate(self, process: asyncio.subprocess.Process) -> None:
        """Terminate only the owned process group, then force a bounded stop."""

        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            await process.wait()
            return
        try:
            await asyncio.wait_for(
                process.wait(),
                timeout=self._termination_timeout,
            )
            return
        except TimeoutError:
            pass
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        await process.wait()
