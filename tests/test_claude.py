from __future__ import annotations

import asyncio
import dataclasses
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
    assert events[3].limit_kind == "fable"


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
    assert event.limit_kind == "fable"


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
    assert event.limit_kind == "session"


def test_parser_recognizes_session_limit_message() -> None:
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
                            "text": "You've hit your session limit. "
                            "Please wait for it to reset.",
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "provider.limit"
    assert event.error_code == "subscription_limit"
    assert event.limit_kind == "session"


def test_parser_recognizes_monthly_spend_limit_message() -> None:
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
                            "text": "You've reached your monthly spend "
                            "limit. Manage usage credits to continue.",
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "provider.limit"
    assert event.error_code == "subscription_limit"
    assert event.limit_kind == "monthly_spend"


def test_parser_recognizes_monthly_spend_cap_message() -> None:
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
                            "text": "You've hit your monthly spend cap. "
                            "Manage usage credits to continue.",
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "provider.limit"
    assert event.error_code == "subscription_limit"
    assert event.limit_kind == "monthly_spend"


def test_parser_recognizes_terminal_monthly_spend_cap_error() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "result",
                "subtype": "error_during_execution",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "errors": ["You've reached your monthly spend cap; "
                           "retry after reset."],
            }
        ).encode()
    )

    assert event.kind == "provider.limit"
    assert event.error_code == "subscription_limit"
    assert event.limit_kind == "monthly_spend"


def test_parser_limit_kind_is_none_on_ordinary_assistant_event() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "assistant",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "message": {
                    "role": "assistant",
                    "content": [{"type": "text", "text": "Working on it."}],
                },
            }
        ).encode()
    )

    assert event.kind == "stream.assistant"
    assert event.limit_kind is None


def test_parser_ignores_near_miss_usage_limit_mid_sentence() -> None:
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
                            "text": "You've almost hit your usage limit, "
                            "so plan accordingly.",
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "stream.assistant"
    assert event.error_code is None
    assert event.limit_kind is None


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
    assert event.limit_kind is None


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


class LargeLineProcessFactory:
    async def __call__(
        self,
        *command: str,
        **kwargs: Any,
    ) -> asyncio.subprocess.Process:
        del command
        child_code = (
            "import json;"
            "print(json.dumps({'type':'system','subtype':'init',"
            "'session_id':'11111111-1111-4111-8111-111111111111'}),flush=True);"
            "print(json.dumps({'type':'assistant','session_id':"
            "'11111111-1111-4111-8111-111111111111','message':{"
            "'role':'assistant','content':[{'type':'text','text':'x'*100000}]}}),"
            "flush=True)"
        )
        return await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            child_code,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
            limit=kwargs.get("limit", 64 * 1024),
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
async def test_large_forwarded_subagent_record_is_consumed(
    registry: ProfileRegistry,
    tmp_path: Path,
) -> None:
    runner = ClaudeRunner(
        registry,
        prompt_file=tmp_path / "claude-lead.md",
        base_env={"PATH": os.environ["PATH"]},
        process_factory=LargeLineProcessFactory(),
    )

    events = [event async for event in runner.start_lead(new_request(tmp_path))]

    assert [event.kind for event in events] == [
        "session.started",
        "stream.assistant",
    ]


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


def test_parser_classifies_compaction_and_context_errors() -> None:
    import json as _json

    from hermes_orchestrator.claude import ClaudeEventParser

    parser = ClaudeEventParser()
    compacted = parser.feed(
        _json.dumps({"type": "system", "subtype": "compact_boundary"}).encode()
    )
    assert compacted.kind == "context.compacted"
    error = parser.feed(
        _json.dumps(
            {
                "type": "result",
                "subtype": "error_during_execution",
                "errors": ["Prompt is too long: context window exceeded"],
            }
        ).encode()
    )
    assert (error.kind, error.error_code) == ("context.error", "context_window")
    child = parser.feed(
        _json.dumps(
            {
                "type": "result",
                "subtype": "error_during_execution",
                "parent_tool_use_id": "toolu-1",
                "errors": ["context window exceeded"],
            }
        ).encode()
    )
    assert child.kind == "stream.result"


def test_runner_installs_the_subagent_gate_hook_and_freeze_markers(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    import json as _json
    from uuid import UUID as _UUID

    freeze_dir = tmp_path / "freezes"
    runner = ClaudeRunner(
        registry,
        prompt_file=tmp_path / "lead.md",
        base_env={},
        freeze_dir=freeze_dir,
        gate_command=("hermes-orchestrator", "subagent-gate"),
    )
    command, _ = runner.build_command(new_request(tmp_path))
    settings = _json.loads(command[command.index("--settings") + 1])
    hook = settings["hooks"]["PreToolUse"][0]
    assert hook["matcher"] == "Agent"
    assert hook["hooks"][0]["command"] == (
        f"hermes-orchestrator subagent-gate --freeze-dir {freeze_dir}"
    )
    session = _UUID("11111111-1111-4111-8111-111111111111")
    assert runner.assignments_frozen(session) is False
    marker = runner.freeze_assignments(session, "rotation_pending: 85%")
    assert marker is not None and marker.read_text() == "rotation_pending: 85%\n"
    assert runner.assignments_frozen(session) is True
    runner.thaw_assignments(session)
    assert runner.assignments_frozen(session) is False
    runner.thaw_assignments(session)  # idempotent


def test_runner_without_freeze_dir_installs_no_hook(
    tmp_path: Path, registry: ProfileRegistry
) -> None:
    runner = ClaudeRunner(registry, prompt_file=tmp_path / "lead.md", base_env={})
    command, _ = runner.build_command(new_request(tmp_path))
    assert "--settings" not in command
    assert runner.freeze_assignments(
        __import__("uuid").UUID("11111111-1111-4111-8111-111111111111"), "x"
    ) is None


# --- INFRA-186: redacted Agent packet_id/model_tier + subagent.completed ---


def test_parser_extracts_packet_id_and_model_tier_from_agent_start() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "assistant",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "parent_tool_use_id": None,
                "message": {
                    "usage": {"input_tokens": 12, "output_tokens": 3},
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "toolu-agent-9",
                            "name": "Agent",
                            "input": {
                                "description": "Implement ENG-9 "
                                "packet:0123456789abcdef0123456789abcdef",
                                "model": "sonnet",
                            },
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "subagent.started"
    assert event.packet_id == "0123456789abcdef0123456789abcdef"
    assert event.model_tier == "sonnet"


def test_parser_agent_start_without_marker_yields_no_packet_or_tier() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "assistant",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "toolu-agent-10",
                            "name": "Agent",
                            "input": {"description": "Implement ENG-9"},
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "subagent.started"
    assert event.packet_id is None
    assert event.model_tier is None


def test_parser_agent_start_with_invalid_tier_yields_none() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "assistant",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "toolu-agent-11",
                            "name": "Agent",
                            "input": {
                                "description": "Implement ENG-9 "
                                "packet:0123456789abcdef0123456789abcdef",
                                "model": "gpt-4",
                            },
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "subagent.started"
    assert event.packet_id == "0123456789abcdef0123456789abcdef"
    assert event.model_tier is None


def test_parser_agent_start_rejects_malformed_hex_length_marker() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "assistant",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "toolu-agent-12",
                            "name": "Agent",
                            "input": {
                                # 31 hex chars: one short of the required 32.
                                "description": "Implement ENG-9 "
                                "packet:0123456789abcdef0123456789abcde",
                            },
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "subagent.started"
    assert event.packet_id is None


def test_parser_classifies_subagent_completed_result() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "result",
                "subtype": "success",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "parent_tool_use_id": "toolu-agent-9",
                "timestamp": "2026-08-26T12:00:04Z",
            }
        ).encode()
    )

    assert event.kind == "subagent.completed"
    assert event.parent_tool_use_id == "toolu-agent-9"


def test_parser_recovers_packet_id_on_subagent_completed_when_present() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "result",
                "subtype": "success",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "parent_tool_use_id": "toolu-agent-9",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "toolu-agent-9",
                            "name": "Agent",
                            "input": {
                                "description": "Implement ENG-9 "
                                "packet:0123456789abcdef0123456789abcdef",
                            },
                        }
                    ],
                },
            }
        ).encode()
    )

    assert event.kind == "subagent.completed"
    assert event.packet_id == "0123456789abcdef0123456789abcdef"


def test_parser_subagent_completed_never_leaks_prompt_or_response_text() -> None:
    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "result",
                "subtype": "success",
                "session_id": "11111111-1111-4111-8111-111111111111",
                "parent_tool_use_id": "toolu-agent-9",
                "result": "SENTINEL-RESPONSE-TEXT packet:"
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "errors": ["SENTINEL-ERROR-TEXT"],
                "structured_output": {"notes": "SENTINEL-STRUCTURED-TEXT"},
            }
        ).encode()
    )

    assert event.kind == "subagent.completed"
    for field in dataclasses.fields(event):
        assert "SENTINEL" not in repr(getattr(event, field.name))
    # A marker embedded in response/result prose (not an Agent tool-use
    # input) must never be treated as the packet id.
    assert event.packet_id != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert event.packet_id is None


def test_parser_existing_child_error_result_still_stays_stream_result() -> None:
    """Pinned characterization: this shape is reserved and must not shift.

    A "result" record with parent_tool_use_id and subtype
    "error_during_execution" is already exercised by
    test_parser_extracts_session_subagent_and_limit_events,
    test_parser_ignores_child_agent_terminal_limit_result, and
    test_parser_classifies_compaction_and_context_errors, all of which
    require it to remain "stream.result". subagent.completed/failed must
    not reclassify it.
    """

    event = ClaudeEventParser().feed(
        json.dumps(
            {
                "type": "result",
                "subtype": "error_during_execution",
                "parent_tool_use_id": "toolu-agent-9",
                "errors": ["boom"],
            }
        ).encode()
    )

    assert event.kind == "stream.result"


# --- INFRA-197 C1: durable launch-failure identity ---


class FailingLaunchProcessFactory:
    """A lead that dies before session confirmation with bytes on stderr."""

    def __init__(self, *, stderr_payload: str, returncode: int = 7) -> None:
        self.stderr_payload = stderr_payload
        self.returncode = returncode
        self.call: tuple[tuple[str, ...], dict[str, Any]] | None = None
        self.process: asyncio.subprocess.Process | None = None

    async def __call__(
        self,
        *command: str,
        **kwargs: Any,
    ) -> asyncio.subprocess.Process:
        self.call = (command, kwargs)
        child_code = (
            "import sys;"
            f"sys.stderr.write({self.stderr_payload!r});"
            "sys.stderr.flush();"
            f"sys.exit({self.returncode})"
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


class SucceedingProcessFactory:
    """A lead that confirms its session and exits cleanly."""

    async def __call__(
        self,
        *command: str,
        **kwargs: Any,
    ) -> asyncio.subprocess.Process:
        del command, kwargs
        child_code = (
            "import json;"
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


@pytest.mark.asyncio
async def test_launch_failure_records_one_durable_identity_receipt(
    registry: ProfileRegistry,
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.claude import control_launch_failure_recorder
    from hermes_orchestrator.control_operations import ControlOperations
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore

    database = Database.open(tmp_path / "control-state.db")
    try:
        control = ControlOperations(database, events=EventStore(database))
        factory = FailingLaunchProcessFactory(
            stderr_payload="claude: unknown option '--frobnicate'\n",
            returncode=7,
        )
        runner = ClaudeRunner(
            registry,
            prompt_file=tmp_path / "claude-lead.md",
            base_env={
                "PATH": os.environ["PATH"],
                "AWS_PROFILE": "work",
                "ANTHROPIC_API_KEY": "sk-secret",
            },
            process_factory=factory,
            launch_failure_recorder=control_launch_failure_recorder(control),
        )

        with pytest.raises(ClaudeProcessError, match="status 7"):
            _ = [
                event async for event in runner.start_lead(new_request(tmp_path))
            ]

        rows = database.execute(
            "SELECT operation_id FROM control_operations "
            "WHERE kind = 'lead.launch_failed'"
        ).fetchall()
        assert len(rows) == 1
        operation = control.get(str(rows[0]["operation_id"]))
        assert factory.call is not None
        assert factory.process is not None
        command, kwargs = factory.call
        assert kwargs["env"]["CLAUDE_CONFIG_DIR"].endswith(".claude-max-a")
        session = "11111111-1111-4111-8111-111111111111"
        assert operation.session_id == session
        # Content equality: the payload is exactly the launch identity —
        # never the wider environment, never any scrubbed credential.
        assert operation.result == {
            "argv": list(command),
            "cwd": str(tmp_path),
            "claude_config_dir": kwargs["env"]["CLAUDE_CONFIG_DIR"],
            "profile_alias": "max-a",
            "pid": factory.process.pid,
            "exit_code": 7,
            "session_id": session,
            "stderr_tail": "claude: unknown option '--frobnicate'\n",
        }
        serialized = json.dumps(operation.as_dict())
        assert "sk-secret" not in serialized
        assert "AWS_PROFILE" not in serialized
        assert "ANTHROPIC_API_KEY" not in serialized
    finally:
        database.close()


@pytest.mark.asyncio
async def test_launch_failure_stderr_tail_is_exactly_the_last_8192_bytes(
    registry: ProfileRegistry,
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.claude import control_launch_failure_recorder
    from hermes_orchestrator.control_operations import ControlOperations
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore

    database = Database.open(tmp_path / "control-state.db")
    try:
        control = ControlOperations(database, events=EventStore(database))
        factory = FailingLaunchProcessFactory(
            stderr_payload="h" * 1000 + "t" * 8192,
            returncode=1,
        )
        runner = ClaudeRunner(
            registry,
            prompt_file=tmp_path / "claude-lead.md",
            base_env={"PATH": os.environ["PATH"]},
            process_factory=factory,
            launch_failure_recorder=control_launch_failure_recorder(control),
        )

        with pytest.raises(ClaudeProcessError, match="status 1"):
            _ = [
                event async for event in runner.start_lead(new_request(tmp_path))
            ]

        rows = database.execute(
            "SELECT operation_id FROM control_operations "
            "WHERE kind = 'lead.launch_failed'"
        ).fetchall()
        assert len(rows) == 1
        operation = control.get(str(rows[0]["operation_id"]))
        assert operation.result["stderr_tail"] == "t" * 8192
        assert operation.result["exit_code"] == 1
    finally:
        database.close()


@pytest.mark.asyncio
async def test_successful_launch_records_no_launch_failure_receipt(
    registry: ProfileRegistry,
    tmp_path: Path,
) -> None:
    from hermes_orchestrator.claude import control_launch_failure_recorder
    from hermes_orchestrator.control_operations import ControlOperations
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.events import EventStore

    database = Database.open(tmp_path / "control-state.db")
    try:
        control = ControlOperations(database, events=EventStore(database))
        runner = ClaudeRunner(
            registry,
            prompt_file=tmp_path / "claude-lead.md",
            base_env={"PATH": os.environ["PATH"]},
            process_factory=SucceedingProcessFactory(),
            launch_failure_recorder=control_launch_failure_recorder(control),
        )

        events = [
            event async for event in runner.start_lead(new_request(tmp_path))
        ]

        assert events[0].kind == "session.started"
        assert database.scalar("SELECT COUNT(*) FROM control_operations") == 0
    finally:
        database.close()


@pytest.mark.asyncio
async def test_launch_failure_without_recorder_keeps_existing_behavior(
    registry: ProfileRegistry,
    tmp_path: Path,
) -> None:
    factory = FailingLaunchProcessFactory(
        stderr_payload="boom\n",
        returncode=7,
    )
    runner = ClaudeRunner(
        registry,
        prompt_file=tmp_path / "claude-lead.md",
        base_env={"PATH": os.environ["PATH"]},
        process_factory=factory,
    )

    with pytest.raises(ClaudeProcessError, match="status 7"):
        _ = [event async for event in runner.start_lead(new_request(tmp_path))]
