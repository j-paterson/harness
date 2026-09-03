"""INFRA-223: the Codex desktop owner-routed IPC adapter."""

from __future__ import annotations

import asyncio
import json
import struct
import tempfile
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.codex_desktop_ipc import (
    DesktopIpcClient,
    DesktopIpcUnavailable,
    OwnerTurnStarter,
    desktop_ipc_socket,
)

THREAD = "01a06486-514c-7dd3-8a7c-26e7069486b2"
OWNER = "74ff8688-ec14-4c51-8e01-c8abddbe384a"


def _frame(message: dict[str, Any]) -> bytes:
    payload = json.dumps(message).encode("utf-8")
    return struct.pack("<I", len(payload)) + payload


class FakeDesktopRouter:
    """The desktop app's IPC router, shape-for-shape: length-prefixed JSON,
    initialize -> clientId, owner discovery, versioned owner-routed requests."""

    def __init__(self, path: Path, *, owns: bool = True) -> None:
        self.path = path
        self.owns = owns
        self.registered = False
        self.requests: list[dict[str, Any]] = []
        self.start_turn_error: str | None = None
        self.snapshot: dict[str, Any] | None = {
            "resumeState": "resumed",
            "threadRuntimeStatus": {"type": "idle"},
        }
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_unix_server(self._serve, path=str(self.path))

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _serve(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                header = await reader.readexactly(4)
                (length,) = struct.unpack("<I", header)
                message = json.loads((await reader.readexactly(length)).decode())
                self.requests.append(message)
                if message.get("type") == "broadcast":
                    reply = self._on_broadcast(message)
                    if reply is not None:
                        writer.write(_frame(reply))
                        await writer.drain()
                    continue
                writer.write(_frame(self._respond(message)))
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    def _on_broadcast(self, message: dict[str, Any]) -> dict[str, Any] | None:
        if message["method"] != "thread-stream-following-status-requested":
            return None
        assert message["version"] == 1
        if self.snapshot is None:
            return None
        return {
            "type": "broadcast",
            "method": "thread-stream-state-changed",
            "sourceClientId": OWNER,
            "targetClientIds": [message["sourceClientId"]],
            "version": 11,
            "params": {
                "conversationId": message["params"]["conversationId"],
                "hostId": "local",
                "change": {
                    "type": "snapshot",
                    "revision": 7,
                    "conversationState": self.snapshot,
                },
            },
        }

    def _respond(self, message: dict[str, Any]) -> dict[str, Any]:
        request_id = message["requestId"]
        method = message["method"]
        if method == "initialize":
            assert "version" not in message
            assert message["params"]["clientType"]
            return {
                "type": "response",
                "requestId": request_id,
                "resultType": "success",
                "method": "initialize",
                "handledByClientId": "router",
                "result": {"clientId": "client-1"},
            }
        if method == "thread-owner-discovery":
            assert message["version"] == 1
            if (
                self.owns
                and self.registered
                and message["params"] == {"hostId": "local", "conversationId": THREAD}
            ):
                return {
                    "type": "response",
                    "requestId": request_id,
                    "resultType": "success",
                    "method": method,
                    "handledByClientId": OWNER,
                    "result": {},
                }
            return {
                "type": "response",
                "requestId": request_id,
                "resultType": "error",
                "error": "no-client-found",
            }
        if method == "thread-follower-start-turn":
            if message.get("version") != 2:
                return {
                    "type": "response",
                    "requestId": request_id,
                    "resultType": "error",
                    "error": "request-version-mismatch",
                }
            if message.get("targetClientId") != OWNER or self.start_turn_error:
                return {
                    "type": "response",
                    "requestId": request_id,
                    "resultType": "error",
                    "error": self.start_turn_error or "client-not-found",
                }
            return {
                "type": "response",
                "requestId": request_id,
                "resultType": "success",
                "method": method,
                "handledByClientId": OWNER,
                "result": {"result": {"turnId": "turn-9"}},
            }
        return {
            "type": "response",
            "requestId": request_id,
            "resultType": "error",
            "error": "no-handler-for-request",
        }


@pytest.fixture
def socket_path() -> Path:
    root = Path(tempfile.mkdtemp(prefix="hipc-", dir="/tmp"))
    return root / "ipc.sock"


@pytest.mark.asyncio
async def test_client_initializes_discovers_owner_and_starts_turn_by_owner(
    socket_path: Path,
) -> None:
    router = FakeDesktopRouter(socket_path)
    router.registered = True
    await router.start()
    try:
        assert desktop_ipc_socket(str(socket_path)) == socket_path
        async with DesktopIpcClient(socket_path) as client:
            assert client.client_id == "client-1"
            assert await client.discover_owner(THREAD) == OWNER
            result = await client.start_turn(
                THREAD,
                owner_client_id=OWNER,
                turn_request={
                    "threadId": THREAD,
                    "input": [{"type": "text", "text": "FABLE_READY ..."}],
                },
            )
        assert result == {"result": {"turnId": "turn-9"}}
        methods = [m["method"] for m in router.requests]
        assert methods == [
            "initialize",
            "thread-owner-discovery",
            "thread-follower-start-turn",
        ]
        start = router.requests[-1]
        assert start["params"]["conversationId"] == THREAD
        assert start["params"]["turnStart"]["request"]["threadId"] == THREAD
        assert start["sourceClientId"] == "client-1"
    finally:
        await router.stop()


@pytest.mark.asyncio
async def test_owner_starter_registers_the_owner_then_starts_without_stdio(
    socket_path: Path,
) -> None:
    router = FakeDesktopRouter(socket_path)
    await router.start()
    registered: list[str] = []

    async def register(thread_id: str) -> None:
        registered.append(thread_id)
        router.registered = True

    try:
        starter = OwnerTurnStarter(
            socket_path,
            cwd_for_thread=lambda thread_id: "/repo",
            register_owner=register,
            owner_wait_seconds=2.0,
        )
        outcome = await starter.start(THREAD, "FABLE_READY issue=ENG-1 ...")
        assert (outcome.started, outcome.owner_client_id, outcome.reason) == (
            True,
            OWNER,
            "owner_started",
        )
        assert registered == [THREAD]
        start = router.requests[-1]
        assert start["params"]["turnStart"]["request"] == {
            "threadId": THREAD,
            "input": [
                {
                    "type": "text",
                    "text": "FABLE_READY issue=ENG-1 ...",
                    "text_elements": [],
                }
            ],
            "cwd": "/repo",
        }
        assert start["params"]["turnStart"]["context"]["useAppServerPermissionDefault"]
    finally:
        await router.stop()


@pytest.mark.asyncio
async def test_owner_starter_fails_closed_when_no_window_owns_the_thread(
    socket_path: Path,
) -> None:
    router = FakeDesktopRouter(socket_path, owns=False)
    await router.start()

    async def register(thread_id: str) -> None:
        return None

    try:
        starter = OwnerTurnStarter(
            socket_path, register_owner=register, owner_wait_seconds=0.6
        )
        outcome = await starter.start(THREAD, "FABLE_READY ...")
        assert outcome.started is False
        assert outcome.reason.startswith("owner_not_found")
        assert not any(
            m["method"] == "thread-follower-start-turn" for m in router.requests
        )
    finally:
        await router.stop()


@pytest.mark.asyncio
async def test_owner_starter_reports_a_missing_socket(socket_path: Path) -> None:
    starter = OwnerTurnStarter(socket_path)
    outcome = await starter.start(THREAD, "FABLE_READY ...")
    assert outcome.started is False
    assert outcome.reason.startswith("owner_start_failed")


def test_desktop_ipc_socket_is_none_when_absent(socket_path: Path) -> None:
    assert desktop_ipc_socket(str(socket_path)) is None


@pytest.mark.asyncio
async def test_client_rejects_a_bad_frame(socket_path: Path) -> None:
    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readexactly(4)
        writer.write(struct.pack("<I", 0))
        await writer.drain()
        writer.close()

    server = await asyncio.start_unix_server(serve, path=str(socket_path))
    try:
        with pytest.raises(DesktopIpcUnavailable):
            await DesktopIpcClient(socket_path, timeout=2.0).connect()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_thread_activity_is_proven_by_the_owner_snapshot(
    socket_path: Path,
) -> None:
    # Sol correction 32f98837: ownership alone proves nothing. The owning
    # window's live snapshot decides: idle -> inactive, an in-progress
    # last turn (or active runtime) -> active, no snapshot -> unknown.
    router = FakeDesktopRouter(socket_path)
    router.registered = True
    await router.start()
    try:
        starter = OwnerTurnStarter(socket_path, timeout=3.0)
        idle = await starter.thread_activity(THREAD)
        assert (idle.owner_client_id, idle.active) == (OWNER, False)

        router.snapshot = {
            "resumeState": "resumed",
            "turns": [{"turnId": "t1", "status": "inProgress"}],
        }
        assert (await starter.thread_activity(THREAD)).active is True
        router.snapshot = {
            "resumeState": "resumed",
            "threadRuntimeStatus": {"type": "active"},
        }
        assert (await starter.thread_activity(THREAD)).active is True
        router.snapshot = None
        assert (await starter.thread_activity(THREAD)).active is None
        methods = [m["method"] for m in router.requests]
        assert "thread-stream-following-status-requested" in methods
        following = [
            m["params"]["following"]
            for m in router.requests
            if m["method"] == "thread-stream-following-changed"
        ]
        assert following[:2] == [True, False]
    finally:
        await router.stop()
