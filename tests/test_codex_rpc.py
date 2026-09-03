"""Verify the restartable Codex App Server JSON-RPC client."""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.codex_rpc import (
    CodexRequestFailed,
    CodexRpcClient,
    CodexTimeout,
    CodexUnavailable,
    RpcNotification,
    app_server_command,
)

_SERVER_SCRIPT = '''
import json
import os
import sys
import threading
import time

rules_path, received_path, control_path = sys.argv[1:4]
open(control_path, "w", encoding="utf-8").close()
with open(rules_path, encoding="utf-8") as handle:
    rules = [json.loads(line) for line in handle if line.strip()]
received = open(received_path, "a", encoding="utf-8", buffering=1)
out_lock = threading.Lock()


def emit_raw(text):
    with out_lock:
        sys.stdout.write(text + "\\n")
        sys.stdout.flush()


def emit(message):
    emit_raw(json.dumps(message, separators=(",", ":")))


def watch_control():
    while True:
        try:
            with open(control_path, encoding="utf-8") as handle:
                text = handle.read().strip()
        except FileNotFoundError:
            text = ""
        if text.startswith("exit "):
            os._exit(int(text.split()[1]))
        time.sleep(0.005)


threading.Thread(target=watch_control, daemon=True).start()
responders = []


def matches(rule, message):
    if rule.get("method") != message.get("method"):
        return False
    expected = rule.get("match")
    if expected:
        params = message.get("params") or {}
        return all(params.get(key) == value for key, value in expected.items())
    return True


def respond(rule, message):
    if rule.get("delay"):
        time.sleep(rule["delay"])
    for index in range(rule.get("notify_count", 0)):
        emit({"method": "fake/notice", "params": {"index": index}})
    for notice in rule.get("notify", []):
        emit(notice)
    if rule.get("raw") is not None:
        emit_raw(rule["raw"])
        return
    if rule.get("oversized"):
        emit_raw("x" * rule["oversized"])
        return
    if rule.get("silent"):
        return
    if rule.get("error") is not None:
        emit({"id": message["id"], "error": rule["error"]})
        return
    emit({"id": message["id"], "result": rule.get("result", {})})


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    received.write(line + "\\n")
    message = json.loads(line)
    if "method" not in message:
        continue
    if message.get("method") == "initialize" and not any(
        matches(item, message) for item in rules
    ):
        emit(
            {
                "id": message["id"],
                "result": {
                    "codexHome": "/tmp/codex-home",
                    "platformFamily": "unix",
                    "platformOs": "macos",
                    "userAgent": "fake-codex",
                },
            }
        )
        continue
    if "id" not in message:
        continue
    rule = next((item for item in rules if matches(item, message)), None)
    if rule is None:
        params = message.get("params") or {}
        if message["method"] == "thread/read":
            result = {"thread": {"id": params.get("threadId")}}
        else:
            result = {}
        emit({"id": message["id"], "result": result})
        continue
    worker = threading.Thread(target=respond, args=(rule, message), daemon=True)
    responders.append(worker)
    worker.start()

for worker in responders:
    worker.join(timeout=2)
'''


class FakeCodexServer:
    """Scripted App Server child driven by JSONL rules."""

    def __init__(self, root: Path) -> None:
        self._script = root / "fake_codex_server.py"
        self._rules = root / "rules.jsonl"
        self._received = root / "received.jsonl"
        self._control = root / "control.txt"
        self._script.write_text(_SERVER_SCRIPT, encoding="utf-8")
        fixture = Path(__file__).parent / "fixtures" / "codex_rpc.jsonl"
        self._rules.write_text(fixture.read_text(encoding="utf-8"), encoding="utf-8")

    @property
    def command(self) -> list[str]:
        """Return the argv used to launch the fake server."""

        return [
            sys.executable,
            str(self._script),
            str(self._rules),
            str(self._received),
            str(self._control),
        ]

    def add_rule(self, rule: dict[str, Any]) -> None:
        """Prepend one behavior rule so it wins over fixture defaults."""

        existing = self._rules.read_text(encoding="utf-8")
        self._rules.write_text(json.dumps(rule) + "\n" + existing, encoding="utf-8")

    def exit(self, code: int) -> None:
        """Ask the running fake server to exit with the given code."""

        self._control.write_text(f"exit {code}\n", encoding="utf-8")

    @property
    def received(self) -> list[dict[str, Any]]:
        """Return messages the server has received once the file is stable."""

        return self._settled(minimum=0)

    def wait_received(self, minimum: int) -> list[dict[str, Any]]:
        """Wait for at least the given message count, then settle."""

        return self._settled(minimum=minimum)

    def _settled(self, minimum: int) -> list[dict[str, Any]]:
        deadline = time.monotonic() + 5.0
        previous = -1
        stable = 0
        lines: list[str] = []
        while time.monotonic() < deadline:
            if self._received.exists():
                text = self._received.read_text(encoding="utf-8")
                lines = text.splitlines()
            if len(lines) == previous and len(lines) >= minimum:
                stable += 1
                if stable >= 3:
                    break
            else:
                stable = 0
            previous = len(lines)
            time.sleep(0.02)
        return [json.loads(line) for line in lines]


@pytest.fixture
def fake_server(tmp_path: Path) -> FakeCodexServer:
    return FakeCodexServer(tmp_path)


async def started_client(
    fake_server: FakeCodexServer, **kwargs: Any
) -> CodexRpcClient:
    client = CodexRpcClient(fake_server.command, **kwargs)
    await client.start()
    return client


@pytest.mark.asyncio
async def test_initializes_before_other_requests(
    fake_server: FakeCodexServer,
) -> None:
    client = CodexRpcClient(fake_server.command)
    await client.start()
    try:
        assert fake_server.wait_received(2)[:2] == [
            {
                "method": "initialize",
                "id": 1,
                "params": {
                    "clientInfo": {
                        "name": "hermes_orchestrator",
                        "title": "Hermes Orchestrator",
                        "version": "0.1.0",
                    }
                },
            },
            {"method": "initialized", "params": {}},
        ]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_matches_out_of_order_responses_by_id(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {
            "method": "thread/read",
            "match": {"threadId": "a"},
            "delay": 0.2,
            "result": {"thread": {"id": "a"}},
        }
    )
    client = await started_client(fake_server)
    try:
        first, second = await asyncio.gather(
            client.request("thread/read", {"threadId": "a"}, 5),
            client.request("thread/read", {"threadId": "b"}, 5),
        )
        assert first["thread"]["id"] == "a"
        assert second["thread"]["id"] == "b"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_child_exit_fails_pending_requests(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule({"method": "thread/read", "silent": True})
    client = await started_client(fake_server)
    try:
        pending = asyncio.create_task(
            client.request("thread/read", {"threadId": "a"}, 5)
        )
        await asyncio.sleep(0.05)
        fake_server.exit(12)
        with pytest.raises(CodexUnavailable, match="exit code 12"):
            await pending
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_request_after_crash_is_unavailable(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule({"method": "thread/read", "silent": True})
    client = await started_client(fake_server)
    try:
        pending = asyncio.create_task(
            client.request("thread/read", {"threadId": "a"}, 5)
        )
        await asyncio.sleep(0.05)
        fake_server.exit(3)
        with pytest.raises(CodexUnavailable):
            await pending
        with pytest.raises(CodexUnavailable, match="exit code 3"):
            await client.request("thread/read", {"threadId": "b"}, 5)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_restart_after_crash_performs_fresh_handshake(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {"method": "thread/read", "match": {"threadId": "a"}, "silent": True}
    )
    client = await started_client(fake_server)
    try:
        pending = asyncio.create_task(
            client.request("thread/read", {"threadId": "a"}, 5)
        )
        await asyncio.sleep(0.05)
        fake_server.exit(9)
        with pytest.raises(CodexUnavailable):
            await pending
        await client.start()
        result = await client.request("thread/read", {"threadId": "b"}, 5)
        assert result["thread"]["id"] == "b"
        restarted = fake_server.received[-3:]
        assert restarted[0]["method"] == "initialize"
        assert restarted[0]["id"] == 1
        assert restarted[1] == {"method": "initialized", "params": {}}
        assert restarted[2]["method"] == "thread/read"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_malformed_json_line_fails_closed(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule({"method": "thread/read", "raw": "this is not json"})
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexUnavailable, match="malformed"):
            await client.request("thread/read", {"threadId": "a"}, 5)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_accepts_a_large_persistent_thread_read_response(
    fake_server: FakeCodexServer,
) -> None:
    history = "x" * 100_000
    fake_server.add_rule(
        {
            "method": "thread/read",
            "result": {"thread": {"id": "merger", "history": history}},
        }
    )
    client = await started_client(fake_server)
    try:
        result = await client.request("thread/read", {"threadId": "merger"}, 5)
        assert result == {"thread": {"id": "merger", "history": history}}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_oversized_line_fails_closed(fake_server: FakeCodexServer) -> None:
    fake_server.add_rule({"method": "thread/read", "oversized": 1_100_000})
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexUnavailable, match="oversized"):
            await client.request("thread/read", {"threadId": "a"}, 5)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_request_timeout_discards_the_late_response(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {
            "method": "thread/read",
            "match": {"threadId": "a"},
            "delay": 0.6,
            "result": {"thread": {"id": "a"}},
        }
    )
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexTimeout, match="thread/read"):
            await client.request("thread/read", {"threadId": "a"}, 0.05)
        result = await client.request("thread/read", {"threadId": "b"}, 5)
        assert result["thread"]["id"] == "b"
        await asyncio.sleep(0.7)
        result = await client.request("thread/read", {"threadId": "c"}, 5)
        assert result["thread"]["id"] == "c"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_thread_section_methods_are_stable(
    fake_server: FakeCodexServer,
) -> None:
    client = await started_client(fake_server)
    try:
        await client.request("threadSection/list", None, 5)
        await client.request(
            "thread/section/move",
            {"sectionId": "sec_pinned", "threadId": "thr_a"},
            5,
        )
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_experimental_methods_are_rejected(
    fake_server: FakeCodexServer,
) -> None:
    client = await started_client(fake_server)
    try:
        with pytest.raises(ValueError, match="stable"):
            await client.request("marketplace/add", {}, 5)
        received = fake_server.wait_received(2)
        assert [message["method"] for message in received] == [
            "initialize",
            "initialized",
        ]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_child_environment_removes_openai_api_key(
    fake_server: FakeCodexServer,
) -> None:
    captured: dict[str, Any] = {}
    real_factory = asyncio.create_subprocess_exec

    async def capturing_factory(
        *command: str, **kwargs: Any
    ) -> asyncio.subprocess.Process:
        captured["env"] = kwargs.get("env")
        captured["start_new_session"] = kwargs.get("start_new_session")
        return await real_factory(*command, **kwargs)

    client = CodexRpcClient(
        fake_server.command,
        base_env={"OPENAI_API_KEY": "sk-secret", "PATH": "/usr/bin"},
        process_factory=capturing_factory,
    )
    await client.start()
    try:
        assert captured["env"] is not None
        assert "OPENAI_API_KEY" not in captured["env"]
        assert captured["env"]["PATH"] == "/usr/bin"
        assert captured["start_new_session"] is True
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_error_response_raises_without_server_message(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {
            "method": "thread/read",
            "error": {"code": -32000, "message": "sensitive detail"},
        }
    )
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexRequestFailed, match="-32000") as info:
            await client.request("thread/read", {"threadId": "a"}, 5)
        assert "sensitive detail" not in str(info.value)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_notifications_are_streamed_in_order(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {
            "method": "thread/read",
            "notify": [
                {"method": "turn/started", "params": {"turnId": "t1"}},
                {"method": "item/completed", "params": {"itemId": "i1"}},
            ],
            "result": {"thread": {"id": "a"}},
        }
    )
    client = await started_client(fake_server)
    try:
        await client.request("thread/read", {"threadId": "a"}, 5)
        stream = client.notifications()
        first = await asyncio.wait_for(anext(stream), 2)
        second = await asyncio.wait_for(anext(stream), 2)
        assert first == RpcNotification(
            method="turn/started", params={"turnId": "t1"}
        )
        assert second == RpcNotification(
            method="item/completed", params={"itemId": "i1"}
        )
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_notification_queue_is_bounded_dropping_oldest(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {"method": "thread/read", "notify_count": 10, "result": {}}
    )
    client = await started_client(fake_server, notification_limit=4)
    try:
        await client.request("thread/read", {"threadId": "a"}, 5)
        assert client.dropped_notifications == 6
        stream = client.notifications()
        kept = [await asyncio.wait_for(anext(stream), 2) for _ in range(4)]
        assert [note.params["index"] for note in kept] == [6, 7, 8, 9]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_close_fails_pending_and_stops_the_child(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule({"method": "thread/read", "silent": True})
    client = await started_client(fake_server, termination_timeout=0.2)
    pending = asyncio.create_task(
        client.request("thread/read", {"threadId": "a"}, 30)
    )
    await asyncio.sleep(0.05)
    await client.close()
    with pytest.raises(CodexUnavailable, match="closed"):
        await pending
    with pytest.raises(CodexUnavailable, match="closed"):
        await client.request("thread/read", {"threadId": "b"}, 5)
    await client.close()


@pytest.mark.asyncio
async def test_notifications_stream_ends_after_close(
    fake_server: FakeCodexServer,
) -> None:
    client = await started_client(fake_server)
    stream = client.notifications()
    await client.close()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(anext(stream), 2)


@pytest.mark.asyncio
async def test_read_account_redacts_the_email(
    fake_server: FakeCodexServer,
) -> None:
    client = await started_client(fake_server)
    try:
        account = await client.read_account()
        assert account.account_type == "chatgpt"
        assert account.plan_type == "pro"
        assert account.requires_openai_auth is False
        assert account.is_chatgpt is True
        assert not hasattr(account, "email")
        assert "merger@example.com" not in repr(account)
        read = next(
            message
            for message in fake_server.received
            if message.get("method") == "account/read"
        )
        assert read["params"] == {"refreshToken": False}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_read_account_handles_missing_account(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {
            "method": "account/read",
            "result": {"account": None, "requiresOpenaiAuth": True},
        }
    )
    client = await started_client(fake_server)
    try:
        account = await client.read_account()
        assert account.account_type is None
        assert account.plan_type is None
        assert account.requires_openai_auth is True
        assert account.is_chatgpt is False
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_read_rate_limits_parses_the_primary_window(
    fake_server: FakeCodexServer,
) -> None:
    client = await started_client(fake_server)
    try:
        limits = await client.read_rate_limits()
        assert limits.primary_used_percent == 25
        assert limits.secondary_used_percent == 10
        assert limits.primary_resets_at == 1767225600
        assert limits.reached is False
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_reached_rate_limit_is_reported(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {
            "method": "account/rateLimits/read",
            "result": {
                "rateLimits": {
                    "primary": {"usedPercent": 100},
                    "rateLimitReachedType": "rate_limit_reached",
                }
            },
        }
    )
    client = await started_client(fake_server)
    try:
        limits = await client.read_rate_limits()
        assert limits.primary_used_percent == 100
        assert limits.secondary_used_percent is None
        assert limits.reached is True
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_interrupt_targets_the_exact_turn(
    fake_server: FakeCodexServer,
) -> None:
    client = await started_client(fake_server)
    try:
        await client.interrupt("thr_demo", "turn_7")
        interrupt = next(
            message
            for message in fake_server.received
            if message.get("method") == "turn/interrupt"
        )
        assert interrupt["params"] == {"threadId": "thr_demo", "turnId": "turn_7"}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_non_integer_response_id_fails_closed(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {"method": "thread/read", "raw": '{"id": [1], "result": {}}'}
    )
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexUnavailable, match="unprocessable"):
            await client.request("thread/read", {"threadId": "a"}, 5)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_boolean_response_id_cannot_complete_handshake(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {
            "method": "initialize",
            "raw": '{"id": true, "result": {"userAgent": "fake-codex"}}',
        }
    )
    client = CodexRpcClient(fake_server.command)
    with pytest.raises(CodexUnavailable, match="unprocessable"):
        await client.start()
    with pytest.raises(CodexUnavailable, match="unprocessable"):
        await client.request("thread/read", {"threadId": "a"}, 5)


@pytest.mark.asyncio
async def test_boolean_response_id_fails_closed(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {"method": "thread/read", "raw": '{"id": true, "result": {}}'}
    )
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexUnavailable, match="unprocessable"):
            await client.request("thread/read", {"threadId": "a"}, 30)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_response_without_result_or_error_fails_closed(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule({"method": "thread/read", "raw": '{"id": 2}'})
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexUnavailable, match="unprocessable"):
            await client.request("thread/read", {"threadId": "a"}, 30)
        with pytest.raises(CodexUnavailable, match="unprocessable"):
            await client.request("thread/read", {"threadId": "b"}, 5)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_response_with_result_and_error_fails_closed(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {
            "method": "thread/read",
            "raw": '{"id": 2, "result": {}, "error": {"code": -1}}',
        }
    )
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexUnavailable, match="unprocessable"):
            await client.request("thread/read", {"threadId": "a"}, 30)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_non_object_result_fails_closed(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {"method": "thread/read", "raw": '{"id": 2, "result": "ok"}'}
    )
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexUnavailable, match="unprocessable"):
            await client.request("thread/read", {"threadId": "a"}, 30)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_non_object_error_fails_closed(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {"method": "thread/read", "raw": '{"id": 2, "error": "denied"}'}
    )
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexUnavailable, match="unprocessable"):
            await client.request("thread/read", {"threadId": "a"}, 30)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_empty_envelope_fails_closed_until_restart(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {
            "method": "thread/read",
            "match": {"threadId": "a"},
            "notify": [{}],
            "result": {"thread": {"id": "a"}},
        }
    )
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexUnavailable, match="unprocessable"):
            await client.request("thread/read", {"threadId": "a"}, 30)
        with pytest.raises(CodexUnavailable, match="unprocessable"):
            await client.request("thread/read", {"threadId": "b"}, 5)
        await client.start()
        result = await client.request("thread/read", {"threadId": "c"}, 5)
        assert result["thread"]["id"] == "c"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_non_object_top_level_message_fails_closed(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule({"method": "thread/read", "raw": "[]"})
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexUnavailable, match="unprocessable"):
            await client.request("thread/read", {"threadId": "a"}, 30)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_non_object_notification_params_fails_closed(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {
            "method": "thread/read",
            "notify": [{"method": "turn/started", "params": None}],
            "result": {"thread": {"id": "a"}},
        }
    )
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexUnavailable, match="unprocessable"):
            await client.request("thread/read", {"threadId": "a"}, 30)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_empty_result_object_is_reported_as_success(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {"method": "thread/read", "raw": '{"id": 2, "result": {}}'}
    )
    client = await started_client(fake_server)
    try:
        result = await client.request("thread/read", {"threadId": "a"}, 5)
        assert result == {}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_notification_without_params_is_preserved(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {
            "method": "thread/read",
            "notify": [{"method": "turn/started"}],
            "result": {"thread": {"id": "a"}},
        }
    )
    client = await started_client(fake_server)
    try:
        await client.request("thread/read", {"threadId": "a"}, 5)
        stream = client.notifications()
        note = await asyncio.wait_for(anext(stream), 2)
        assert note == RpcNotification(method="turn/started", params={})
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_restart_after_protocol_violation(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {"method": "thread/read", "match": {"threadId": "a"}, "oversized": 1_100_000}
    )
    client = await started_client(fake_server)
    try:
        with pytest.raises(CodexUnavailable, match="oversized"):
            await client.request("thread/read", {"threadId": "a"}, 5)
        await client.start()
        result = await client.request("thread/read", {"threadId": "b"}, 5)
        assert result["thread"]["id"] == "b"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_restart_after_close_performs_fresh_handshake(
    fake_server: FakeCodexServer,
) -> None:
    client = await started_client(fake_server)
    await client.close()
    await client.start()
    try:
        result = await client.request("thread/read", {"threadId": "b"}, 5)
        assert result["thread"]["id"] == "b"
        restarted = fake_server.received[-3:]
        assert restarted[0]["method"] == "initialize"
        assert restarted[0]["id"] == 1
        assert restarted[1] == {"method": "initialized", "params": {}}
        assert restarted[2]["method"] == "thread/read"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_start_while_running_is_rejected(
    fake_server: FakeCodexServer,
) -> None:
    client = await started_client(fake_server)
    try:
        with pytest.raises(RuntimeError, match="already running"):
            await client.start()
        result = await client.request("thread/read", {"threadId": "a"}, 5)
        assert result["thread"]["id"] == "a"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_server_initiated_request_is_refused(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {
            "method": "thread/read",
            "notify": [{"id": 99, "method": "server/ping", "params": {}}],
            "result": {"thread": {"id": "a"}},
        }
    )
    client = await started_client(fake_server)
    try:
        result = await client.request("thread/read", {"threadId": "a"}, 5)
        assert result["thread"]["id"] == "a"
        reply = next(
            message
            for message in fake_server.wait_received(4)
            if "error" in message
        )
        assert reply["id"] == 99
        assert reply["error"]["code"] == -32601
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_close_sentinel_survives_late_notifications(
    fake_server: FakeCodexServer,
) -> None:
    fake_server.add_rule(
        {
            "method": "thread/read",
            "delay": 0.2,
            "notify_count": 5,
            "silent": True,
        }
    )
    client = await started_client(fake_server, notification_limit=2)
    pending = asyncio.create_task(
        client.request("thread/read", {"threadId": "a"}, 30)
    )
    await asyncio.sleep(0.05)
    stream = client.notifications()
    await client.close()
    with pytest.raises(CodexUnavailable, match="closed"):
        await pending
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(anext(stream), 2)
    assert client.dropped_notifications == 0


@pytest.mark.asyncio
async def test_request_before_start_is_unavailable(
    fake_server: FakeCodexServer,
) -> None:
    client = CodexRpcClient(fake_server.command)
    with pytest.raises(CodexUnavailable, match="not started"):
        await client.request("thread/read", {"threadId": "a"}, 5)


def test_app_server_command_attaches_through_the_app_control_socket_proxy() -> None:
    # INFRA-223 (2026-09-03): a spawned ``app-server --listen stdio://``
    # owned the exact queued turn it started and interrupted it on exit
    # (turn 01a0655d…, 348 ms). Every bounded client now relays through
    # the Codex app's existing control socket, so the app stays the sole
    # owner of the thread and its turns; an exact socket may be pinned.
    assert app_server_command() == [
        "/Applications/Codex.app/Contents/Resources/codex",
        "app-server",
        "proxy",
    ]
    assert app_server_command(socket_path="/tmp/codex/app-server.sock") == [
        "/Applications/Codex.app/Contents/Resources/codex",
        "app-server",
        "proxy",
        "--sock",
        "/tmp/codex/app-server.sock",
    ]
    with pytest.raises(ValueError, match="absolute"):
        app_server_command(socket_path="relative.sock")


def test_app_server_command_refuses_path_dependent_executables() -> None:
    # A bare or relative name resolves through PATH; under cmux's
    # minimal shell environment that reproduces the observed exit-127
    # launch failure, so only validated absolute paths are accepted.
    for path_dependent in ("codex", "bin/codex", ""):
        with pytest.raises(ValueError, match="absolute"):
            app_server_command(path_dependent)
