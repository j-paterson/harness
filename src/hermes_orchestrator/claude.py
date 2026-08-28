"""Resumable Claude Code lead turns and sanitized stream events."""

from __future__ import annotations

import asyncio
import json
import os
import re
import shlex
import signal
import sys
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from uuid import UUID

from hermes_orchestrator.processes import ProcessRegistry, register_spawned
from hermes_orchestrator.profiles import ProfileRegistry

_SYNTHETIC_MODEL = "<synthetic>"
_MAX_STREAM_LINE_BYTES = 1024 * 1024
_COMPACTION_SUBTYPES = frozenset({"compact_boundary", "compaction", "compact"})
_CONTEXT_ERROR_TEXT = re.compile(
    r"context window|prompt is too long|input length|context length exceeded"
)
_LIMIT_RESULT_SUBTYPES = frozenset({"error_during_execution"})
# Each family shares the same "you've reached/hit your ..." prefix and word
# boundary discipline as the original subscription-limit match; only the
# trailing cap phrase differs per family.
_LIMIT_PREFIX = "^you['\u2019]ve (?:reached|hit) your "
_LIMIT_KIND_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "fable",
        re.compile(_LIMIT_PREFIX + r"fable \d+(?:\.\d+)? limit\b"),
    ),
    (
        "session",
        re.compile(_LIMIT_PREFIX + r"(?:usage|session) limit\b"),
    ),
    (
        "monthly_spend",
        re.compile(_LIMIT_PREFIX + r"monthly spend (?:limit|cap)\b"),
    ),
)


@dataclass(frozen=True, slots=True)
class LeadTurnRequest:
    """One new or resumed project-lead turn."""

    session_id: UUID
    cwd: Path
    prompt: str
    profile_alias: str
    resume: bool = False
    output_schema: dict[str, Any] | None = None
    project_key: str | None = None


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
    limit_kind: str | None = None


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
        limit_kind = None
        if original_type == "system" and subtype == "init":
            kind = "session.started"
        elif original_type == "system" and subtype in _COMPACTION_SUBTYPES:
            kind = "context.compacted"
        elif self._is_context_error(value):
            kind = "context.error"
            error_code = "context_window"
        elif self._starts_subagent(value):
            kind = "subagent.started"
        elif action := self._handoff_acknowledgement(value):
            kind = "handoff.acknowledged"
            restated_next_action = action
        elif found := self._subscription_limit_kind(value):
            kind = "provider.limit"
            error_code = "subscription_limit"
            limit_kind = found

        return ClaudeEvent(
            kind=kind,
            original_type=original_type,
            session_id=session_id,
            parent_tool_use_id=parent_tool_use_id,
            timestamp=timestamp,
            usage=usage,
            error_code=error_code,
            restated_next_action=restated_next_action,
            limit_kind=limit_kind,
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
    def _subscription_limit_kind(cls, value: dict[str, Any]) -> str | None:
        """Classify only authoritative top-level CLI subscription-limit shapes.

        A live cap is either a synthetic assistant message (the CLI's
        ``"model": "<synthetic>"`` discriminator) or a terminal result error,
        and in both cases only the exact observed subscription wording
        counts. Child-agent records, rate-limit telemetry, generic cap prose
        such as concurrency or disk limits, and limit language in prompts,
        tool results, or result prose must never count as exhaustion. The
        Fable, session, and monthly-spend cap families all normalize to
        kind="provider.limit"/error_code="subscription_limit"; the returned
        string only distinguishes which family fired.
        """

        if value.get("parent_tool_use_id") is not None:
            return None
        record_type = value.get("type")
        if record_type == "result":
            if value.get("subtype") not in _LIMIT_RESULT_SUBTYPES:
                return None
            errors = value.get("errors")
            if not isinstance(errors, list):
                return None
            for item in errors:
                if isinstance(item, str) and (kind := cls._limit_kind(item)):
                    return kind
            return None
        if record_type == "assistant":
            message = value.get("message")
            if (
                not isinstance(message, dict)
                or message.get("model") != _SYNTHETIC_MODEL
            ):
                return None
            content = message.get("content")
            if not isinstance(content, list):
                return None
            for block in content:
                if (
                    isinstance(block, dict)
                    and block.get("type") == "text"
                    and isinstance(block.get("text"), str)
                    and (kind := cls._limit_kind(block["text"]))
                ):
                    return kind
            return None
        return None

    @staticmethod
    def _is_context_error(value: dict[str, Any]) -> bool:
        """Top-level terminal errors naming the context window only."""

        if value.get("parent_tool_use_id") is not None:
            return False
        if value.get("type") != "result":
            return False
        errors = value.get("errors")
        if not isinstance(errors, list):
            return False
        return any(
            isinstance(item, str) and _CONTEXT_ERROR_TEXT.search(item.lower())
            for item in errors
        )

    @staticmethod
    def _limit_kind(value: str) -> str | None:
        """Classify one text string against the known cap families, once."""

        normalized = value.strip().lower()
        for kind, pattern in _LIMIT_KIND_PATTERNS:
            if pattern.match(normalized):
                return kind
        return None

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
        processes: ProcessRegistry | None = None,
        freeze_dir: Path | None = None,
        gate_command: Sequence[str] | None = None,
    ) -> None:
        if termination_timeout <= 0:
            raise ValueError("termination_timeout must be positive")
        self._registry = registry
        self._processes = processes
        # Explicit lead control path: a PreToolUse hook on the Agent tool
        # consults a durable per-session freeze marker and blocks new
        # subagent assignments while a rotation is pending.
        self._freeze_dir = freeze_dir
        self._gate_command = (
            tuple(gate_command)
            if gate_command is not None
            else (sys.executable, "-m", "hermes_orchestrator.cli", "subagent-gate")
        )
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
        if self._freeze_dir is not None:
            command.extend(["--settings", self.hook_settings()])
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

    def hook_settings(self) -> str:
        """Claude Code settings JSON installing the subagent gate hook."""

        assert self._freeze_dir is not None
        gate = [*self._gate_command, "--freeze-dir", str(self._freeze_dir)]
        return json.dumps(
            {
                "hooks": {
                    "PreToolUse": [
                        {
                            "matcher": "Agent",
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": shlex.join(gate),
                                    "timeout": 10,
                                }
                            ],
                        }
                    ]
                }
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def freeze_assignments(self, session_id: UUID, reason: str) -> Path | None:
        """Durably freeze new subagent assignments for one exact session."""

        if self._freeze_dir is None:
            return None
        self._freeze_dir.mkdir(parents=True, exist_ok=True)
        marker = self._freeze_dir / f"{session_id}.frozen"
        marker.write_text(reason.strip() + "\n", encoding="utf-8")
        return marker

    def thaw_assignments(self, session_id: UUID) -> None:
        if self._freeze_dir is None:
            return
        with suppress(FileNotFoundError):
            (self._freeze_dir / f"{session_id}.frozen").unlink()

    def assignments_frozen(self, session_id: UUID) -> bool:
        return (
            self._freeze_dir is not None
            and (self._freeze_dir / f"{session_id}.frozen").exists()
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
            limit=_MAX_STREAM_LINE_BYTES,
        )
        if process.stdout is None:
            await self._terminate(process)
            raise RuntimeError("Claude stdout pipe was not created")
        if process.stderr is None:
            await self._terminate(process)
            raise RuntimeError("Claude stderr pipe was not created")
        lease_id = await register_spawned(
            self._processes,
            process,
            project_key=request.project_key or "unassigned",
            kind="claude_lead",
            worker_id=str(request.session_id),
            executable=self._executable,
            cwd=str(request.cwd),
            terminate=self._terminate,
        )

        parser = ClaudeEventParser()
        saw_subscription_limit = False
        stderr_task = asyncio.create_task(self._drain(process.stderr))
        try:
            while line := await process.stdout.readline():
                event = parser.feed(line)
                saw_subscription_limit = (
                    saw_subscription_limit or event.kind == "provider.limit"
                )
                yield event
            returncode = await process.wait()
            expected_limit_exit = returncode == 1 and saw_subscription_limit
            if returncode != 0 and not expected_limit_exit:
                raise ClaudeProcessError(returncode)
        finally:
            if process.returncode is None:
                await self._terminate(process)
            await stderr_task
            if lease_id is not None and self._processes is not None:
                self._processes.mark_exited(lease_id, exit_code=process.returncode)

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
