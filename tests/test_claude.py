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
        "provider.limit",
    ]
    assert events[1].session_id == UUID("11111111-1111-4111-8111-111111111111")
    assert events[1].usage == {"input_tokens": 120, "output_tokens": 8}
    assert events[2].parent_tool_use_id == "toolu-agent-1"
    assert events[2].error_code == "subscription_limit"


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
