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
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from hermes_orchestrator.model_tiers import ModelTier, load_model_tiers
from hermes_orchestrator.processes import ProcessRegistry, register_spawned
from hermes_orchestrator.profiles import ProfileRegistry

if TYPE_CHECKING:
    from hermes_orchestrator.control_operations import ControlOperations

# INFRA-211: effort is a tier property, so every managed launch reads it
# from the one tier config instead of carrying its own literal. The path
# is the repository's own config/ (src/hermes_orchestrator/claude.py ->
# repo root); no new config key, and no new collaborator threaded
# through ClaudeRunner's callers.
_MODEL_TIERS_PATH = Path(__file__).resolve().parents[2] / "config" / "model-tiers.yaml"
# Only used when the tier config is unreadable: the launch still has to
# name an effort, and the conservative choice is the new default, never
# the amplified "high" this issue removes.
_UNCONFIGURED_EFFORT = "medium"


@lru_cache(maxsize=1)
def configured_model_tiers() -> dict[str, ModelTier]:
    """The repository's tier config; empty when it cannot be read."""

    try:
        return load_model_tiers(_MODEL_TIERS_PATH)
    except (OSError, ValueError):
        return {}


def tier_default_effort(tier_name: str) -> str:
    """Configured default effort for one tier."""

    tier = configured_model_tiers().get(tier_name)
    return tier.default_effort if tier is not None else _UNCONFIGURED_EFFORT


_SYNTHETIC_MODEL = "<synthetic>"
_MAX_STREAM_LINE_BYTES = 1024 * 1024
# INFRA-197 C1: a lead that dies before doing useful work must leave a
# diagnosable identity behind. Only a bounded stderr tail is retained.
_STDERR_TAIL_BYTES = 8192
_COMPACTION_SUBTYPES = frozenset({"compact_boundary", "compaction", "compact"})
_CONTEXT_ERROR_TEXT = re.compile(
    r"context window|prompt is too long|input length|context length exceeded"
)
_LIMIT_RESULT_SUBTYPES = frozenset({"error_during_execution"})
# Each family shares the same "you've reached/hit your ..." prefix and word
# boundary discipline as the original subscription-limit match; only the
# trailing cap phrase differs per family.
_LIMIT_PREFIX = "^you['\u2019]ve (?:reached|hit) your "
# INFRA-186: the Agent tool-use input carries an optional redacted packet
# marker in its description/name fields; only an exact 32-character
# lowercase hex id is ever accepted, never any other description text.
_PACKET_MARKER = re.compile(r"\bpacket:([0-9a-f]{32})\b")
_MODEL_TIERS = frozenset({"haiku", "sonnet", "opus", "fable"})
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
    # INFRA-186 redacted subagent orchestration metadata: only the
    # packet id carried in the Agent description and the enumerated
    # model tier — never prompt or response text.
    packet_id: str | None = None
    model_tier: str | None = None


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
        packet_id = None
        model_tier = None
        if original_type == "system" and subtype == "init":
            kind = "session.started"
        elif original_type == "system" and subtype in _COMPACTION_SUBTYPES:
            kind = "context.compacted"
        elif self._is_context_error(value):
            kind = "context.error"
            error_code = "context_window"
        elif self._completes_subagent(value):
            kind = "subagent.completed"
            packet_id = self._agent_packet_id(self._agent_tool_use(value))
        elif self._starts_subagent(value):
            kind = "subagent.started"
            agent_block = self._agent_tool_use(value)
            packet_id = self._agent_packet_id(agent_block)
            model_tier = self._agent_model_tier(agent_block)
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
            packet_id=packet_id,
            model_tier=model_tier,
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

    @staticmethod
    def _completes_subagent(value: dict[str, Any]) -> bool:
        """A terminal "result" record scoped to one child Agent tool call.

        This mirrors the top-level lead turn's own terminal "result"
        record, but scoped down via ``parent_tool_use_id`` exactly as
        every other per-child record in this stream already is. The
        ``error_during_execution`` subtype is deliberately excluded: that
        exact shape (a "result" record with parent_tool_use_id and
        subtype "error_during_execution") is already characterized, and
        pinned by existing tests, as an ambiguous case (it may just be a
        suppressed child-scoped subscription-limit message rather than a
        genuine subagent failure) that must keep falling through to the
        generic "stream.result" kind. Because that is the only
        error-shaped terminal record this stream distinguishes, and it is
        reserved, "subagent.failed" cannot be split out reliably here —
        only "subagent.completed" is emitted.
        """

        return (
            value.get("type") == "result"
            and value.get("parent_tool_use_id") is not None
            and value.get("subtype") not in _LIMIT_RESULT_SUBTYPES
        )

    @staticmethod
    def _agent_tool_use(value: dict[str, Any]) -> dict[str, Any] | None:
        """The Agent tool-use content block, when this record carries one."""

        message = value.get("message")
        if not isinstance(message, dict):
            return None
        content = message.get("content")
        if not isinstance(content, list):
            return None
        for block in content:
            if (
                isinstance(block, dict)
                and block.get("type") == "tool_use"
                and block.get("name") == "Agent"
            ):
                return block
        return None

    @classmethod
    def _agent_packet_id(cls, block: dict[str, Any] | None) -> str | None:
        """Scan only the Agent input's description/name for a packet marker.

        Never reads any other field (never prompt or response text); an
        absent or malformed marker (wrong length, wrong case, etc.)
        always yields None.
        """

        if block is None:
            return None
        tool_input = block.get("input")
        if not isinstance(tool_input, dict):
            return None
        for field in ("description", "name"):
            text = tool_input.get(field)
            if isinstance(text, str) and (match := _PACKET_MARKER.search(text)):
                return match.group(1)
        return None

    @staticmethod
    def _agent_model_tier(block: dict[str, Any] | None) -> str | None:
        """The Agent input's model, only when it is a known tier name."""

        if block is None:
            return None
        tool_input = block.get("input")
        if not isinstance(tool_input, dict):
            return None
        model = tool_input.get("model")
        return model if model in _MODEL_TIERS else None

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


@dataclass(frozen=True, slots=True)
class LeadLaunchFailure:
    """Durable identity of one lead process that exited nonzero.

    Carries exactly the launch identity a vanished terminal cannot
    reconstruct: the exact argv, the working directory, the single
    CLAUDE_CONFIG_DIR value the scrubbed profile environment sets, the
    profile alias, pid, exit code, session id, and a bounded stderr
    tail. Never the wider environment — the scrubbed provider
    credentials must never reach a durable payload.
    """

    argv: tuple[str, ...]
    cwd: str
    claude_config_dir: str
    profile_alias: str
    pid: int
    exit_code: int
    session_id: str
    stderr_tail: str
    project_key: str | None


LaunchFailureRecorder = Callable[[LeadLaunchFailure], None]


def control_launch_failure_recorder(
    control: ControlOperations,
) -> LaunchFailureRecorder:
    """Adapt the durable control-operations log to the runner's port.

    The runner knows its request's session id and profile alias but no
    cell identity, so the receipt is addressed by session (the default
    ``kind:session`` dedup key keeps at most one live receipt per lead)
    with the cell marked unassigned, mirroring how the process registry
    already records leads without a project key.
    """

    def record(failure: LeadLaunchFailure) -> None:
        control.record(
            kind="lead.launch_failed",
            project_key=failure.project_key or "unassigned",
            cell_id="unassigned",
            session_id=failure.session_id,
            result={
                "argv": list(failure.argv),
                "cwd": failure.cwd,
                "claude_config_dir": failure.claude_config_dir,
                "profile_alias": failure.profile_alias,
                "pid": failure.pid,
                "exit_code": failure.exit_code,
                "session_id": failure.session_id,
                "stderr_tail": failure.stderr_tail,
            },
            reason=(
                "the lead process exited nonzero; this receipt carries "
                "the exact launch identity (argv, cwd, config dir, pid, "
                "stderr tail) that would otherwise be lost with the "
                "terminal"
            ),
        )

    return record


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
        launch_failure_recorder: LaunchFailureRecorder | None = None,
    ) -> None:
        if termination_timeout <= 0:
            raise ValueError("termination_timeout must be positive")
        self._registry = registry
        self._processes = processes
        self._launch_failure_recorder = launch_failure_recorder
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
            # INFRA-211: the fable tier's configured default, not a literal.
            tier_default_effort("fable"),
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
                # The process has exited, so its stderr is at EOF and
                # the drain task completes without blocking.
                self._record_launch_failure(
                    request,
                    command=command,
                    environment=environment,
                    pid=process.pid,
                    returncode=returncode,
                    stderr_tail=await stderr_task,
                )
                raise ClaudeProcessError(returncode)
        finally:
            if process.returncode is None:
                await self._terminate(process)
            await stderr_task
            if lease_id is not None and self._processes is not None:
                self._processes.mark_exited(lease_id, exit_code=process.returncode)

    def _record_launch_failure(
        self,
        request: LeadTurnRequest,
        *,
        command: Sequence[str],
        environment: Mapping[str, str],
        pid: int,
        returncode: int,
        stderr_tail: bytes,
    ) -> None:
        """Record one durable launch-identity receipt, never raising.

        The payload names only launch identity plus the profile's
        CLAUDE_CONFIG_DIR — never the wider environment, so the
        scrubbed provider credentials cannot leak into durable state.
        """

        if self._launch_failure_recorder is None:
            return
        failure = LeadLaunchFailure(
            argv=tuple(command),
            cwd=str(request.cwd),
            claude_config_dir=environment.get("CLAUDE_CONFIG_DIR", ""),
            profile_alias=request.profile_alias,
            pid=pid,
            exit_code=returncode,
            session_id=str(request.session_id),
            stderr_tail=stderr_tail.decode("utf-8", errors="replace"),
            project_key=request.project_key,
        )
        with suppress(Exception):
            self._launch_failure_recorder(failure)

    @staticmethod
    async def _drain(stream: asyncio.StreamReader) -> bytes:
        """Drain child diagnostics fully so its pipe cannot deadlock.

        The pipe is always read to EOF, but only the last
        ``_STDERR_TAIL_BYTES`` bytes are retained for the durable
        launch-failure receipt; memory stays bounded no matter how
        verbose the child is.
        """

        tail = bytearray()
        while chunk := await stream.read(65536):
            tail.extend(chunk)
            if len(tail) > _STDERR_TAIL_BYTES:
                del tail[:-_STDERR_TAIL_BYTES]
        return bytes(tail)

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
