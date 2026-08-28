from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from hermes_orchestrator.claude import (
    ClaudeEventParser,
    ClaudeProcessError,
    ClaudeRunner,
    LeadTurnRequest,
)
from hermes_orchestrator.profiles import ProfileRegistry


@pytest.fixture
def registry(tmp_path: Path) -> ProfileRegistry:
    config = tmp_path / "profiles.yaml"
    config.write_text(
        "profiles:\n"
        "  - alias: max-a\n"
        f"    config_dir: {tmp_path / '.claude-max-a'}\n"
        "  - alias: max-b\n"
        f"    config_dir: {tmp_path / '.claude-max-b'}\n"
        "  - alias: max-c\n"
        f"    config_dir: {tmp_path / '.claude-max-c'}\n"
        "  - alias: max-d\n"
        f"    config_dir: {tmp_path / '.claude-max-d'}\n",
        encoding="utf-8",
    )
    return ProfileRegistry.load(config)


@pytest.fixture
def runner(registry: ProfileRegistry, tmp_path: Path) -> ClaudeRunner:
    return ClaudeRunner(
        registry,
        prompt_file=tmp_path / "claude-lead.md",
        base_env={
            "PATH": "/usr/bin",
            "CLAUDE_CODE_USE_BEDROCK": "1",
            "AWS_PROFILE": "work",
        },
    )


def new_request(tmp_path: Path, *, resume: bool = False) -> LeadTurnRequest:
    return LeadTurnRequest(
        session_id=UUID("11111111-1111-4111-8111-111111111111"),
        cwd=tmp_path,
        prompt="Plan ENG-9",
        profile_alias="max-a",
        resume=resume,
    )


def test_new_lead_uses_subscription_profile_and_persistent_session(
    runner: ClaudeRunner,
    tmp_path: Path,
) -> None:
    command, env = runner.build_command(new_request(tmp_path))

    assert command[:4] == ["claude", "-p", "--model", "fable"]
    assert command[command.index("--session-id") + 1] == (
        "11111111-1111-4111-8111-111111111111"
    )
    assert "--output-format=stream-json" in command
    assert "--verbose" in command
    assert "--include-hook-events" in command
    assert "--forward-subagent-text" in command
    assert env["CLAUDE_CONFIG_DIR"].endswith(".claude-max-a")
    assert "CLAUDE_CODE_USE_BEDROCK" not in env
    assert "AWS_PROFILE" not in env


def test_resume_uses_same_session_id(
    runner: ClaudeRunner,
    tmp_path: Path,
) -> None:
    request = new_request(tmp_path, resume=True)

    command, _ = runner.build_command(request)

    assert command[command.index("--resume") + 1] == str(request.session_id)
    assert "--session-id" not in command


def test_parser_extracts_session_subagent_and_limit_events() -> None:
    fixture = Path(__file__).parent / "fixtures" / "claude_stream.jsonl"
    parser = ClaudeEventParser()

    events = [parser.feed(line) for line in fixture.read_bytes().splitlines()]

    assert [event.kind for event in events] == [
        "session.started",
        "subagent.started",
        "stream.result",
        "provider.limit",
    ]
    assert events[1].session_id == UUID("11111111-1111-4111-8111-111111111111")
    assert events[1].usage == {"input_tokens": 120, "output_tokens": 8}
    assert events[2].parent_tool_use_id == "toolu-agent-1"
    assert events[2].error_code is None
    assert events[3].parent_tool_use_id is None
    assert events[3].error_code == "subscription_limit"


def test_parser_recognizes_reached_fable_limit_message() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "assistant",
                "session_id": None,
                "message": {
                    "type": "message",
                    "role": "assistant",
                    "model": "<synthetic>",
                    "content": [
                        {
                            "type": "text",
                            "text": "You've reached your Fable 5 limit. "
                            "Switch to another model, or manage usage credits "
                            "to continue.",
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "provider.limit"
    assert event.error_code == "subscription_limit"


def test_parser_recognizes_terminal_usage_cap_error() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "result",
                "subtype": "error_during_execution",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "errors": ["You've hit your usage limit; retry after reset."],
            }
        ).encode()
    )

    assert event.kind == "provider.limit"
    assert event.error_code == "subscription_limit"


def test_parser_ignores_result_prose_with_limit_language() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "result",
                "subtype": "success",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "result": "You've reached your Fable 5 limit. Switch to "
                "another model, or manage usage credits to continue.",
            }
        ).encode()
    )

    assert event.kind == "stream.result"
    assert event.error_code is None


def test_parser_ignores_top_level_concurrent_agent_cap() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "assistant",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "message": {
                    "role": "assistant",
                    "model": "claude-fable-5",
                    "content": [
                        {
                            "type": "text",
                            "text": "You've hit your concurrent agents "
                            "limit. Wait for a free subagent slot.",
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "stream.assistant"
    assert event.error_code is None


def test_parser_ignores_top_level_disk_usage_cap_error() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "result",
                "subtype": "error_during_execution",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "errors": [
                    "You've reached your disk usage limit for this workspace."
                ],
            }
        ).encode()
    )

    assert event.kind == "stream.result"
    assert event.error_code is None


def test_parser_ignores_mathematical_limit_prose() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "assistant",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "message": {
                    "role": "assistant",
                    "model": "claude-fable-5",
                    "content": [
                        {
                            "type": "text",
                            "text": "You've reached your limit of the "
                            "partial sums: the series converges to 1.",
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "stream.assistant"
    assert event.error_code is None


_NON_SUBSCRIPTION_FABLE_CAPS = [
    "You've reached your Fable concurrent agents limit. Wait for a free slot.",
    "You've reached your Fable disk usage limit for this workspace.",
    "You've reached your Fable sandbox limit.",
    "You've reached your Fable tool concurrency limit.",
]


@pytest.mark.parametrize("text", _NON_SUBSCRIPTION_FABLE_CAPS)
def test_parser_ignores_synthetic_non_subscription_fable_caps(
    text: str,
) -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "assistant",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "message": {
                    "role": "assistant",
                    "model": "<synthetic>",
                    "content": [{"type": "text", "text": text}],
                },
            }
        ).encode()
    )

    assert event.kind == "stream.assistant"
    assert event.error_code is None


@pytest.mark.parametrize("text", _NON_SUBSCRIPTION_FABLE_CAPS)
def test_parser_ignores_terminal_non_subscription_fable_caps(
    text: str,
) -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "result",
                "subtype": "error_during_execution",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "errors": [text],
            }
        ).encode()
    )

    assert event.kind == "stream.result"
    assert event.error_code is None


def test_parser_recognizes_numeric_fable_model_versions() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "assistant",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "message": {
                    "role": "assistant",
                    "model": "<synthetic>",
                    "content": [
                        {
                            "type": "text",
                            "text": "You've reached your Fable 4.5 limit. "
                            "Switch to another model, or manage usage "
                            "credits to continue.",
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "provider.limit"
    assert event.error_code == "subscription_limit"


def test_parser_requires_synthetic_model_for_limit_message() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "assistant",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "message": {
                    "role": "assistant",
                    "model": "claude-fable-5",
                    "content": [
                        {
                            "type": "text",
                            "text": "You've reached your Fable 5 limit. "
                            "Switch to another model, or manage usage "
                            "credits to continue.",
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "stream.assistant"
    assert event.error_code is None


def test_parser_ignores_rate_limit_telemetry() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "rate_limit_event",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "rate_limit": {
                    "status": "allowed_warning",
                    "summary": "You've reached your weekly limit for Fable 5.",
                    "resetsAt": 1767225600,
                },
            }
        ).encode()
    )

    assert event.kind == "stream.rate_limit_event"
    assert event.error_code is None


def test_parser_ignores_tool_results_with_limit_language() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "user",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "message": {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "toolu-1",
                            "content": [
                                {
                                    "type": "text",
                                    "text": "ERROR: You've hit your usage "
                                    "limit for this sandbox.",
                                }
                            ],
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "stream.user"
    assert event.error_code is None


def test_parser_ignores_assistant_text_quoting_limit_language() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "assistant",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "text",
                            "text": "The worker reported: You've hit your "
                            "usage limit, but the lead continues normally.",
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "stream.assistant"
    assert event.error_code is None


def test_parser_ignores_child_agent_cap_messages() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "assistant",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "parent_tool_use_id": "toolu-child-1",
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "text",
                            "text": "You've hit your concurrent agents "
                            "limit. Wait for a free subagent slot.",
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "stream.assistant"
    assert event.error_code is None


def test_parser_ignores_child_agent_terminal_limit_result() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "result",
                "subtype": "error_during_execution",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "parent_tool_use_id": "toolu-child-1",
                "errors": ["You've hit your usage limit; retry after reset."],
            }
        ).encode()
    )

    assert event.kind == "stream.result"
    assert event.error_code is None


def test_handoff_schema_is_passed_and_acknowledgement_is_parsed(
    runner: ClaudeRunner,
    tmp_path: Path,
) -> None:
    schema = {
        "type": "object",
        "required": ["acknowledged", "restated_next_action"],
    }
    request = new_request(tmp_path)
    request = LeadTurnRequest(
        session_id=request.session_id,
        cwd=request.cwd,
        prompt=request.prompt,
        profile_alias=request.profile_alias,
        output_schema=schema,
    )

    command, _ = runner.build_command(request)
    event = ClaudeEventParser().feed(
        b'{"type":"result","session_id":"11111111-1111-4111-8111-'
        b'111111111111","structured_output":{"acknowledged":true,'
        b'"restated_next_action":"Run the failing test."}}'
    )

    assert json.loads(command[command.index("--json-schema") + 1]) == schema
    assert event.kind == "handoff.acknowledged"
    assert event.restated_next_action == "Run the failing test."


class CapturingProcessFactory:
    def __init__(self) -> None:
        self.process: asyncio.subprocess.Process | None = None
        self.call: tuple[tuple[str, ...], dict[str, Any]] | None = None

    async def __call__(
        self,
        *command: str,
        **kwargs: Any,
    ) -> asyncio.subprocess.Process:
        self.call = (command, kwargs)
        child_code = (
            "import json,signal,time;"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
            "print(json.dumps({'type':'system','subtype':'init',"
            "'session_id':'11111111-1111-4111-8111-111111111111'}),flush=True);"
            "time.sleep(30)"
        )
        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            child_code,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        return self.process


class VerboseProcessFactory:
    async def __call__(
        self,
        *command: str,
        **kwargs: Any,
    ) -> asyncio.subprocess.Process:
        del command, kwargs
        child_code = (
            "import json,sys;"
            "sys.stderr.write('x'*16777216);sys.stderr.flush();"
            "print(json.dumps({'type':'system','subtype':'init',"
            "'session_id':'11111111-1111-4111-8111-111111111111'}),flush=True)"
        )
        return await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            child_code,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )


class CappedProcessFactory:
    def __init__(self, returncode: int = 1) -> None:
        self.returncode = returncode

    async def __call__(
        self,
        *command: str,
        **kwargs: Any,
    ) -> asyncio.subprocess.Process:
        del command, kwargs
        child_code = (
            "import json,sys;"
            "print(json.dumps({'type':'system','subtype':'init',"
            "'session_id':'11111111-1111-4111-8111-111111111111'}),flush=True);"
            "print(json.dumps({'type':'result','subtype':'error_during_execution',"
            "'session_id':'11111111-1111-4111-8111-111111111111',"
            "'errors':[\"You've reached your Fable 5 limit.\"]}),flush=True);"
            f"sys.exit({self.returncode})"
        )
        return await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            child_code,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )


@pytest.mark.asyncio
async def test_closing_stream_kills_hung_process_group(
    registry: ProfileRegistry,
    tmp_path: Path,
) -> None:
    factory = CapturingProcessFactory()
    runner = ClaudeRunner(
        registry,
        prompt_file=tmp_path / "claude-lead.md",
        base_env={"PATH": os.environ["PATH"]},
        process_factory=factory,
        termination_timeout=0.05,
    )

    stream = runner.start_lead(new_request(tmp_path))
    event = await anext(stream)
    await stream.aclose()

    assert event.kind == "session.started"
    assert factory.process is not None
    assert factory.process.returncode == -signal.SIGKILL
    assert factory.call is not None
    assert factory.call[1]["start_new_session"] is True


@pytest.mark.asyncio
async def test_verbose_stderr_cannot_block_stdout_events(
    registry: ProfileRegistry,
    tmp_path: Path,
) -> None:
    runner = ClaudeRunner(
        registry,
        prompt_file=tmp_path / "claude-lead.md",
        base_env={"PATH": os.environ["PATH"]},
        process_factory=VerboseProcessFactory(),
    )
    stream = runner.start_lead(new_request(tmp_path))

    event = await asyncio.wait_for(anext(stream), timeout=0.5)
    await stream.aclose()

    assert event.kind == "session.started"


@pytest.mark.asyncio
async def test_subscription_limit_is_terminal_even_when_claude_exits_one(
    registry: ProfileRegistry,
    tmp_path: Path,
) -> None:
    runner = ClaudeRunner(
        registry,
        prompt_file=tmp_path / "claude-lead.md",
        base_env={"PATH": os.environ["PATH"]},
        process_factory=CappedProcessFactory(),
    )

    events = [event async for event in runner.start_lead(new_request(tmp_path))]

    assert [event.kind for event in events] == [
        "session.started",
        "provider.limit",
    ]


@pytest.mark.asyncio
async def test_subscription_limit_does_not_hide_another_process_failure(
    registry: ProfileRegistry,
    tmp_path: Path,
) -> None:
    runner = ClaudeRunner(
        registry,
        prompt_file=tmp_path / "claude-lead.md",
        base_env={"PATH": os.environ["PATH"]},
        process_factory=CappedProcessFactory(returncode=2),
    )

    with pytest.raises(ClaudeProcessError, match="status 2"):
        _ = [event async for event in runner.start_lead(new_request(tmp_path))]
